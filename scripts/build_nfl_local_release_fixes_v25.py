"""Build v25: apply targeted data corrections sourced from PFR boxscore PBP/kicking tables.

Corrections applied (all verified against PFR source):

  A3 — TB 1997 wk17: Karl Williams nfl_team 'TAM'→'TB'
       Williams' 47 recv yards were absent from the 'TB' team aggregate,
       making pass=94/recv=47=2:1. Fixing team code → ratio becomes 1:1.

  A4 — CHI 1968 wk7: Bobby Joe Green punt_yards NULL→0
       PFR kicking source shows punt_yds='0' (blocked punt). Pipeline
       misread the '0' string as NULL.

  A4 — DAL 1976 wk4: Danny White punt_yards NULL→0
       Same cause as above (blocked punt, source shows punt_yds='0').

  B5 — PHI 1999 wk12 Akers: fg_missed 0→1
       Source fgm='', fga='1'. One attempt unaccounted; set as missed.

  B5 — PHI 1999 wk12 Johnson: fg_missed 1→0
       Source fgm='1', fga='1' (1/1, all made). The miss was a data error.

  B5 — BUF 2018 wk14 Hauschka: fg_missed 1→2
       Source fgm='3', fga='5' (3/5 = 2 non-made). Only 1 miss recorded.

  B6 — NYG 1981 wk17 Danelo: pat_missed 0→1
       Source xpm='3', xpa='4' (3/4 = 1 missed PAT).

  B6 — CIN 1985 wk3 Breech: pat_missed 1→0
       Source xpm='5', xpa='5' (5/5, all made). The miss was a data error.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

RELEASES = Path(r"D:\league-history-data\nfl\releases")
V24_DIR = sorted(RELEASES.glob("*_v24/tables/nfl_player_stats_all.parquet"))[-1]
V24_PARQUET = V24_DIR


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# Patches: (player_week, column, old_value, new_value, reason)
PATCHES = [
    # A3: TB 1997 wk17 team code fix (Karl Williams)
    ("00-0017847_1997_17", "nfl_team", "TAM", "TB",
     "A3: Williams recv excluded from TB aggregate due to team code mismatch"),

    # A4: blocked punt — source shows punt_yds='0', pipeline read as NULL
    ("GreeBo20_1968_7",   "punt_yards", None,  0.0,
     "A4: Bobby Joe Green CHI 1968 wk7 blocked punt, source=0"),
    ("WHI578584_1976_4",  "punt_yards", None,  0.0,
     "A4: Danny White DAL 1976 wk4 blocked punt, source=0"),

    # B5: FG component accounting
    ("00-0000108_1999_12", "fg_missed",  0.0,  1.0,
     "B5: Akers PHI 1999 wk12 — 1 att 0 made, source confirms miss"),
    ("00-0008593_1999_12", "fg_missed",  1.0,  0.0,
     "B5: Johnson PHI 1999 wk12 — source fgm=1/fga=1, all made"),
    ("00-0025944_2018_14", "fg_missed",  1.0,  2.0,
     "B5: Hauschka BUF 2018 wk14 — source fgm=3/fga=5, 2 non-made"),

    # B6: PAT component accounting
    ("DAN083058_1981_17", "pat_missed",  0.0,  1.0,
     "B6: Danelo NYG 1981 wk17 — source xpm=3/xpa=4, 1 missed"),
    ("00-0001720_1985_3", "pat_missed",  1.0,  0.0,
     "B6: Breech CIN 1985 wk3 — source xpm=5/xpa=5, all made"),
]


def main(dry_run: bool = True) -> None:
    if not V24_PARQUET.exists():
        print(f"ERROR: v24 not found at {V24_PARQUET}", file=sys.stderr)
        sys.exit(1)

    db = duckdb.connect()
    db.execute("SET progress_bar_time=99999")

    print(f"Loading v24: {V24_PARQUET}")
    db.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{V24_PARQUET}')")
    total_v24 = db.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    print(f"  v24 rows: {total_v24:,}")
    print()

    # Verify each patch row exists and has the expected old value
    print("=== Patch verification ===")
    all_ok = True
    for pw, col, old_val, new_val, reason in PATCHES:
        row = db.execute(
            f"SELECT player_week, {col} FROM st WHERE player_week = ?", [pw]
        ).fetchone()
        if row is None:
            print(f"  MISSING  {pw}.{col} — row not found!")
            all_ok = False
        else:
            actual = row[1]
            if old_val is None:
                ok = actual is None
            elif isinstance(old_val, str):
                ok = actual == old_val
            else:
                ok = actual == old_val or (actual is None and old_val is None)
            status = "OK" if ok else "MISMATCH"
            print(f"  {status}  {pw}.{col}: {actual!r} -> {new_val!r}  [{reason[:60]}]")
            if not ok:
                all_ok = False

    print()
    if not all_ok:
        print("ERROR: Patch verification failed. Aborting.")
        sys.exit(1)

    if dry_run:
        print("DRY RUN — no files written. Pass --execute to apply.")
        return

    # Apply patches in-memory
    print("Applying patches...")
    for pw, col, old_val, new_val, reason in PATCHES:
        if isinstance(new_val, str):
            db.execute(f"UPDATE st SET {col} = ? WHERE player_week = ?", [new_val, pw])
        elif new_val is None:
            db.execute(f"UPDATE st SET {col} = NULL WHERE player_week = ?", [pw])
        else:
            db.execute(f"UPDATE st SET {col} = ? WHERE player_week = ?", [float(new_val), pw])
        print(f"  Applied: {pw}.{col} -> {new_val!r}")

    # Write v25
    stamp = utc_stamp()
    out_dir = RELEASES / f"nfl_local_release_audit_fixes_{stamp}_v25"
    out_dir.mkdir(parents=True)
    (out_dir / "tables").mkdir()
    out_parquet = out_dir / "tables" / "nfl_player_stats_all.parquet"

    print(f"\nWriting v25 to {out_parquet} ...")
    db.execute(f"COPY (SELECT * FROM st) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    total_v25 = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
    print(f"v25 rows: {total_v25:,}  (delta from v24: {total_v25 - total_v24})")
    print(f"\nv25 written: {out_dir}")


if __name__ == "__main__":
    dry = "--execute" not in sys.argv
    main(dry_run=dry)
