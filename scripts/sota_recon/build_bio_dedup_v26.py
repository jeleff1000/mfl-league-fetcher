"""
build_bio_dedup_v26.py  --  merge duplicate-identity rows in v26 player_bio (same human, two NFL_player_ids).

The duplicates are synthetic HIST-xxxxx placeholder rows that shadow a real player: the HIST- row is a
"ghost" (not in the super table, sparse, rarely any platform id), the canonical row is the real played /
non-HIST id (pfr_id, headshot, stats). We merge each ghost INTO its canonical row and drop the ghost, so
player_bio becomes one-row-per-person.

SAFETY (avoid false-merging two distinct people who share a name):
  - dup  = a HIST- row NOT in the super table (a placeholder ghost).
  - canonical = a same-name row that is in the super table (played) or has a non-HIST id, preferred by
    played > non-HIST > most-populated.
  - merge ONLY with corroboration: same birth_date (both non-null) OR same college (both non-null).
  - require a UNIQUE canonical; if a ghost matches >1 candidate person, SKIP (ambiguous -> left alone).
Canonical nulls are backfilled from the ghost (so no platform id / field is lost), then the ghost is dropped.

NOTE: resolution caches (yahoo/sleeper/espn -> NFL_player_id) must be rebuilt from the cleaned bio so they
don't point at a dropped HIST- id. Ghosts almost never carry platform ids, and those that do are moved to
the canonical id first, so a rebuilt-from-bio cache stays correct.

    python -m scripts.sota_recon.build_bio_dedup_v26            # dry-run (report, no write)
    python -m scripts.sota_recon.build_bio_dedup_v26 --apply
"""

from __future__ import annotations
import argparse
import os
import shutil
import sys
from datetime import datetime, timezone, UTC
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sota_recon.sources import latest_v26  # noqa: E402

BIO = Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")


_POS_FAMILY = {
    "wr": "WR",
    "wide receiver": "WR",
    "te": "TE",
    "tight end": "TE",
    "qb": "QB",
    "quarterback": "QB",
    "hb": "BACK",
    "rb": "BACK",
    "fb": "BACK",
    "b": "BACK",
    "tb": "BACK",
    "wb": "BACK",
    "running back": "BACK",
    "halfback": "BACK",
    "fullback": "BACK",
    "t": "OL",
    "g": "OL",
    "c": "OL",
    "ol": "OL",
    "ot": "OL",
    "og": "OL",
    "lt": "OL",
    "rt": "OL",
    "lg": "OL",
    "rg": "OL",
    "tackle": "OL",
    "guard": "OL",
    "center": "OL",
    "de": "DL",
    "dt": "DL",
    "dl": "DL",
    "nt": "DL",
    "e": "END",
    "end": "END",
    "lb": "LB",
    "olb": "LB",
    "ilb": "LB",
    "mlb": "LB",
    "linebacker": "LB",
    "cb": "DB",
    "db": "DB",
    "s": "DB",
    "saf": "DB",
    "fs": "DB",
    "ss": "DB",
    "safety": "DB",
    "cornerback": "DB",
    "k": "K",
    "p": "P",
    "def": "DEF",
    "dst": "DEF",
}


def _posfam(p):
    if p is None or (isinstance(p, float)):
        return None
    return _POS_FAMILY.get(str(p).strip().lower(), str(p).strip().lower())


def run(apply: bool) -> None:
    v = Path(latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='6GB'")
    super_ids = {
        r[0]
        for r in con.execute(f"SELECT DISTINCT NFL_player_id FROM '{v}' WHERE NFL_player_id IS NOT NULL").fetchall()
    }
    bio = con.execute(f"SELECT * FROM '{BIO}'").df()
    n0, cols = len(bio), list(bio.columns)

    bio["_played"] = bio["NFL_player_id"].isin(super_ids)
    bio["_hist"] = bio["NFL_player_id"].astype(str).str.startswith("HIST-")
    bio["_score"] = (
        bio["_played"].astype(int) * 1_000_000 + (~bio["_hist"]).astype(int) * 1_000 + bio.notna().sum(axis=1)
    )

    # Cluster same-person rows by name + corroboration: rows sharing a birth_date, or (both birth_date
    # null) a college. Handles HIST- ghosts, non-HIST ghosts, and all-ghost pairs uniformly.
    def subkey(row):
        if pd.notna(row["birth_date"]):
            return ("BD", row["player"], row["birth_date"])
        if pd.notna(row["college"]):
            return ("COL", row["player"], row["college"])
        return ("SELF", row["NFL_player_id"])  # no corroboration -> never clusters with another row

    bio["_subkey"] = bio.apply(subkey, axis=1)

    merges: dict[int, int] = {}  # dup index -> canonical index
    conflation = []  # clusters with >1 PLAYED id (real super-table conflation; out of scope)
    pos_skipped = 0  # college-only clusters skipped for position-family disagreement
    for key, cl in bio.groupby("_subkey"):
        if key[0] == "SELF" or len(cl) < 2:
            continue
        ci = cl["_score"].idxmax()  # canonical = best-scored row
        for di in cl.index:
            if di == ci:
                continue
            if bool(bio.at[di, "_played"]):
                conflation.append((bio.at[di, "player"], bio.at[di, "NFL_player_id"]))
                continue  # never drop a played id (needs super surgery)
            # birth_date match is definitive; college-only also requires position-family agreement
            if key[0] == "COL":
                pd_, pc_ = _posfam(bio.at[di, "nfl_position"]), _posfam(bio.at[ci, "nfl_position"])
                if pd_ is not None and pc_ is not None and pd_ != pc_:
                    pos_skipped += 1
                    continue
            merges[di] = ci
    ambiguous = len(conflation)

    # perform merge: backfill canonical nulls from ghost, then drop ghost
    data_cols = [c for c in cols if c not in ("NFL_player_id",)]
    moved_platform = 0
    for gi, ci in merges.items():
        for c in data_cols:
            if pd.isna(bio.at[ci, c]) and pd.notna(bio.at[gi, c]):
                bio.at[ci, c] = bio.at[gi, c]
                if c in ("yahoo_player_id", "sleeper_player_id", "espn_id"):
                    moved_platform += 1

    drop_idx = list(merges.keys())
    out = bio.drop(index=drop_idx)[cols]

    print(f"player_bio rows: {n0:,}")
    print(f"ghost rows (not in super): {int((~bio['_played']).sum()):,}")
    print(f"duplicates merged & dropped:     {len(merges):,}")
    print(
        f"  of which corroborated by birth_date: "
        f"{sum(1 for gi in merges if pd.notna(bio.at[gi,'birth_date'])):,}, by college: "
        f"{sum(1 for gi in merges if pd.isna(bio.at[gi,'birth_date']) and pd.notna(bio.at[gi,'college'])):,}"
    )
    print(f"college-only merges skipped for position mismatch: {pos_skipped:,}")
    print(f"played-id conflations SKIPPED (need super-table surgery): {ambiguous:,}")
    if conflation:
        for nm, pid in conflation[:6]:
            print(f"    conflation: {nm} ({pid})")
    print(f"platform ids moved to canonical: {moved_platform:,}")
    print(f"final rows: {len(out):,}  (was {n0:,})")

    if not apply:
        print("\nsample merges (ghost -> canonical):")
        for gi, ci in list(merges.items())[:8]:
            print(f"  {bio.at[gi,'player']}: {bio.at[gi,'NFL_player_id']} -> {bio.at[ci,'NFL_player_id']}")
        print("\n(dry-run -- no write)")
        return

    # ---- gated write ----
    assert out["NFL_player_id"].is_unique, "NFL_player_id not unique after merge"
    assert len(out) == n0 - len(merges), "row-count delta mismatch"
    assert set(out.columns) == set(cols), "column set changed"
    # every dropped id must be a ghost NOT in super (never drop a real/played id)
    assert not any(bio.at[gi, "_played"] for gi in drop_idx), "attempted to drop a played id"

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = BIO.with_name(f"player_bio.parquet.bak_dedup_{ts}")
    shutil.copy2(BIO, backup)
    tmp = BIO.with_suffix(".parquet.tmp")
    con.register("bio_out", out)
    con.execute(f"COPY bio_out TO '{tmp.as_posix()}' (FORMAT PARQUET)")
    os.replace(tmp, BIO)
    print(f"\nWROTE {BIO}  ({n0:,} -> {len(out):,} rows)  backup: {backup.name}")
    print("NEXT: re-run build_primary_team_bio_v26 --apply; rebuild Fly resolution caches from cleaned bio.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    run(ap.parse_args().apply)
