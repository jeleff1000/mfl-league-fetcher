"""
build_primary_team_bio_v26.py  --  add primary_franchise_id (+ primary_team_source) to v26 player_bio.

primary_franchise_id = the NFL FRANCHISE (super-table nfl_franchise_number -- stable across relocations,
so OAK/LV/RAI collapse to one franchise) a player is most associated with, by this fallback chain:
  1. lamar       : franchise with the MOST career LAMAR. "Best preset per position" = GREATEST() across the
                   12-team roster-type presets (flx, sflx, idp, tep, ppfd) per player-week -- whichever
                   roster type scores that position best (offense via flx/tep, defense via idp). Argmax
                   franchise by SUM of that best-per-row LAMAR.
  2. games       : if the player never scored any LAMAR (OL, P, pre-modern), fall back to the franchise
                   where they played the most weeks.
  3. dup_identity: if the player has NO super rows (a "ghost" row -- drafted/UDFA, never played) but is the
                   SAME person as a played player (same name + birth_date, or name + college), inherit that
                   player's franchise. Solves duplicate-identity rows for the same human.
  4. draft       : else normalized nfl_draft_team -> franchise.
  5. latest      : else a valid latest_team -> franchise.
  6. none        : no team signal anywhere -> primary_franchise_id stays NULL.

Franchise resolution: LAMAR/games come straight from nfl_franchise_number (unambiguous per row). The
name/abbr fallbacks map via an abbr->franchise table built from the super table (mode franchise per abbr,
since a city abbreviation like BAL/BOS/STL was reused by different franchises across eras) plus an
era-aware full-name->abbr table. primary_team_source records which tier supplied each value.

GATED WRITE: backs up player_bio.parquet, verifies row count + NFL_player_id uniqueness + no existing
column changed + every emitted id is a valid franchise number, then os.replace. Idempotent (drops any
prior primary_* columns before recomputing).

    python -m scripts.sota_recon.build_primary_team_bio_v26            # dry-run (report, no write)
    python -m scripts.sota_recon.build_primary_team_bio_v26 --apply    # write player_bio.parquet
"""

from __future__ import annotations
import argparse
import os
import shutil
import sys
from datetime import datetime, timezone, UTC
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sota_recon.sources import latest_v26  # noqa: E402

BIO = Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")
OUT_COLS = ["primary_franchise_id", "primary_team_source", "primary_team"]  # primary_team(abbrev) dropped if present

LAMAR_PRESETS = [
    "lamar_12t_flx_half_4pt",
    "lamar_12t_sflx_half_4pt",
    "lamar_12t_idp_half_4pt",
    "lamar_12t_tep_half_4pt",
    "lamar_12t_flx_ppfd_4pt",
    "lamar_12t_sflx_ppfd_4pt",
    "lamar_12t_idp_ppfd_4pt",
]

ABBR_FIX = {  # non-super abbreviations -> super-table PFR abbreviation
    "GB": "GNB",
    "KC": "KAN",
    "LA": "LAR",
    "LV": "LVR",
    "NE": "NWE",
    "NO": "NOR",
    "SD": "SDG",
    "SF": "SFO",
    "TB": "TAM",
    "WSH": "WAS",
    "NA": None,
}
NAME2ABBR = {  # era-aware full names (+ typos) -> super-table abbreviation (then abbr->franchise via mode)
    "Arizona Cardinals": "ARI",
    "Aizona Cardinals": "ARI",
    "Phoenix Cardinals": "PHO",
    "Chicago Cardinals": "CRD",
    "St. Louis Cardinals": "CRD",
    "Atlanta Falcons": "ATL",
    "Baltimore Colts": "IND",
    "Baltimore Colts NFL": "IND",
    "Indianapolis Colts": "IND",
    "Baltimore Ravens": "BAL",
    "Boston Patriots": "NWE",
    "New England Patriots": "NWE",
    "Boston Redskins": "WAS",
    "Washington Redskins": "WAS",
    "Washington Redskins NFL": "WAS",
    "Brooklyn Dodgers": "BKN",
    "Brooklyn Tigers": "BKN",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Chicgo Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cinicnnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Cleveland Rams": "RAM",
    "Los Angeles Rams": "LAR",
    "St. Louis Rams": "STL",
    "Dallas Cowboys": "DAL",
    "Dallas Texans": "DTX",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GNB",
    "Houston Oilers": "TEN",
    "Tennessee Oilers": "TEN",
    "Tennessee Titans": "TEN",
    "Houston Texans": "HOU",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KAN",
    "Los Angeles Raiders": "RAI",
    "Oakland Raiders": "OAK",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New Orleans Saints": "NOR",
    "New York Giants": "NYG",
    "New York Ginats": "NYG",
    "New York Jets": "NYJ",
    "New York Titans": "NYT",
    "New York Yanks": "NYY",
    "Philadelphia Eagles": "PHI",
    "Philaldelphia Eagles": "PHI",
    "Pittsburgh Pirates": "PIT",
    "Pittsburgh Steelers": "PIT",
    "San Diego Chargers": "SDG",
    "Sand Diego Chargers": "SDG",
    "Los Angeles Chargers": "LAC",
    "San Francisco 49ers": "SFO",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TAM",
    "Boston Yanks": "BOS",
    "New York Bulldogs": "NYB",
}


def run(apply: bool) -> None:
    v = Path(latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='8GB'")

    # abbr -> franchise (mode: a city abbr was reused by different franchises; pick the dominant one)
    abbr2fr = {
        r[0]: int(r[1])
        for r in con.execute(f"""
        SELECT nfl_team, mode(nfl_franchise_number) FROM '{v}'
        WHERE nfl_team IS NOT NULL AND nfl_franchise_number IS NOT NULL GROUP BY 1""").fetchall()
    }
    valid_fr = {
        int(r[0])
        for r in con.execute(
            f"SELECT DISTINCT nfl_franchise_number FROM '{v}' WHERE nfl_franchise_number IS NOT NULL"
        ).fetchall()
    }

    super_abbr = set(abbr2fr)

    def to_abbr(val):
        """Normalize a team string to a super-table abbreviation (era-correct), or None."""
        if val is None:
            return None
        s = str(val).strip()
        if not s:
            return None
        if s in super_abbr:
            return s
        return ABBR_FIX.get(s, NAME2ABBR.get(s))

    def name_or_abbr_to_fr(val):
        abbr = to_abbr(val)
        return abbr2fr.get(abbr) if abbr else None

    best = "GREATEST(" + ", ".join(LAMAR_PRESETS) + ")"
    # display abbrev = the abbreviation the player wore in his BEST season within a given franchise
    # (so LaDainian Tomlinson -> SDG from his best San Diego year, not the modern LAC franchise abbr).
    disp_map = {
        (r[0], int(r[1])): r[2]
        for r in con.execute(f"""
        SELECT NFL_player_id, nfl_franchise_number, nfl_team FROM (
          SELECT NFL_player_id, nfl_franchise_number, nfl_team,
                 ROW_NUMBER() OVER (PARTITION BY NFL_player_id, nfl_franchise_number
                     ORDER BY SUM({best}) DESC NULLS LAST, COUNT(*) DESC, MAX(year) DESC) rn
          FROM '{v}'
          WHERE NFL_player_id IS NOT NULL AND nfl_franchise_number IS NOT NULL AND nfl_team IS NOT NULL
          GROUP BY NFL_player_id, nfl_franchise_number, nfl_team, year
        ) WHERE rn = 1""").fetchall()
    }
    lamar_fr = {
        r[0]: int(r[1])
        for r in con.execute(f"""
        SELECT NFL_player_id, nfl_franchise_number FROM (
          SELECT NFL_player_id, nfl_franchise_number,
                 ROW_NUMBER() OVER (PARTITION BY NFL_player_id
                     ORDER BY SUM({best}) DESC, COUNT(*) DESC, nfl_franchise_number) rn
          FROM '{v}'
          WHERE NFL_player_id IS NOT NULL AND nfl_franchise_number IS NOT NULL AND {best} IS NOT NULL
          GROUP BY NFL_player_id, nfl_franchise_number
        ) WHERE rn = 1""").fetchall()
    }
    games_fr = {
        r[0]: int(r[1])
        for r in con.execute(f"""
        SELECT NFL_player_id, nfl_franchise_number FROM (
          SELECT NFL_player_id, nfl_franchise_number,
                 ROW_NUMBER() OVER (PARTITION BY NFL_player_id
                     ORDER BY COUNT(*) DESC, nfl_franchise_number) rn
          FROM '{v}'
          WHERE NFL_player_id IS NOT NULL AND nfl_franchise_number IS NOT NULL
          GROUP BY NFL_player_id, nfl_franchise_number
        ) WHERE rn = 1""").fetchall()
    }

    bio = con.execute(f"SELECT * FROM '{BIO}'").df()
    bio = bio[[c for c in bio.columns if c not in OUT_COLS]]  # idempotent: drop prior outputs
    n0, orig_cols = len(bio), list(bio.columns)

    import pandas as pd

    # base tier: lamar -> games (real NFL players). display = best-season abbrev within the chosen franchise.
    def base(pid):
        fr = lamar_fr.get(pid)
        if fr is not None:
            return fr, "lamar", disp_map.get((pid, fr))
        fr = games_fr.get(pid)
        if fr is not None:
            return fr, "games", disp_map.get((pid, fr))
        return None, None, None

    base_res = bio["NFL_player_id"].map(base)
    bio["primary_franchise_id"] = [r[0] for r in base_res]
    bio["primary_team_source"] = [r[1] for r in base_res]
    bio["primary_team"] = [r[2] for r in base_res]

    # namesake lookup built ONLY from played (lamar/games) players -> matches ghosts to real humans.
    # carries (franchise, display) so a resolved ghost inherits the real player's era-correct abbrev.
    played = bio[bio["primary_team_source"].isin(["lamar", "games"])]

    def unique_map(keycols):
        sub = played.dropna(subset=keycols + ["primary_franchise_id"])
        out = {}
        for key, g in sub.groupby(keycols):
            if g["primary_franchise_id"].nunique() == 1:
                row0 = g.iloc[0]
                out[key] = (int(row0["primary_franchise_id"]), row0["primary_team"])
        return out

    by_birth = unique_map(["player", "birth_date"])
    by_college = unique_map(["player", "college"])

    # fallback tiers for the still-unresolved -> (franchise, source, display)
    def fallback(row):
        if pd.notna(row["primary_franchise_id"]):
            return int(row["primary_franchise_id"]), row["primary_team_source"], row["primary_team"]
        nm = row.get("player")
        # 3. dup_identity by name+birth_date: certain same person -> overrides draft
        bd = row.get("birth_date")
        if pd.notna(nm) and pd.notna(bd) and (nm, bd) in by_birth:
            fr, disp = by_birth[(nm, bd)]
            return fr, "dup_identity", disp
        # 4/5. draft then latest (reliable team-of-record; beats weak name+college namesake)
        for field, tier in (("nfl_draft_team", "draft"), ("latest_team", "latest")):
            abbr = to_abbr(row.get(field))
            fr = abbr2fr.get(abbr) if abbr else None
            if fr is not None:
                return fr, tier, abbr
        # 6. dup_identity by name+college: weak, last resort before NULL
        col = row.get("college")
        if pd.notna(nm) and pd.notna(col) and (nm, col) in by_college:
            fr, disp = by_college[(nm, col)]
            return fr, "dup_identity", disp
        return None, "none", None

    res = bio.apply(fallback, axis=1, result_type="expand")
    bio["primary_franchise_id"] = res[0].astype("Int64")
    bio["primary_team_source"] = res[1]
    bio["primary_team"] = res[2]

    # ---- report ----
    src = bio["primary_team_source"].value_counts().to_dict()
    cov = bio["primary_franchise_id"].notna().sum()
    print(f"player_bio rows: {n0:,}")
    print("primary_team_source breakdown:")
    for k in ("lamar", "games", "dup_identity", "draft", "latest", "none"):
        print(f"  {k:12s} {src.get(k, 0):>8,}")
    print(f"primary_franchise_id populated: {cov:,} / {n0:,}  ({100*cov/n0:.1f}%)   NULL: {n0-cov:,}")
    emitted = {int(x) for x in bio["primary_franchise_id"].dropna().unique()}
    bad = sorted(emitted - valid_fr)
    if bad:
        print(f"WARNING: {len(bad)} emitted ids not valid franchise numbers: {bad[:20]}")
    bad_abbr = sorted(set(bio["primary_team"].dropna()) - super_abbr)
    if bad_abbr:
        print(f"WARNING: {len(bad_abbr)} display abbrevs not in super set: {bad_abbr[:20]}")
    # id populated but display missing (should be ~0)
    mismatch = int(((bio["primary_franchise_id"].notna()) & (bio["primary_team"].isna())).sum())
    print(f"franchise_id set but display NULL: {mismatch}")

    if not apply:
        print("\n(dry-run -- no write. re-run with --apply)")
        return

    # ---- gated write ----
    assert len(bio) == n0, "row count changed"
    assert bio["NFL_player_id"].is_unique, "NFL_player_id not unique"
    assert list(bio.columns)[: len(orig_cols)] == orig_cols, "existing columns reordered/changed"
    assert not bad, f"refusing to write: invalid franchise ids {bad[:20]}"
    assert not bad_abbr, f"refusing to write: non-canonical display abbrevs {bad_abbr[:20]}"

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = BIO.with_name(f"player_bio.parquet.bak_primaryfr_{ts}")
    shutil.copy2(BIO, backup)
    tmp = BIO.with_suffix(".parquet.tmp")
    con.register("bio_out", bio)
    con.execute(f"COPY bio_out TO '{tmp.as_posix()}' (FORMAT PARQUET)")
    os.replace(tmp, BIO)
    print(
        f"\nWROTE {BIO}  (+primary_franchise_id, +primary_team [display], +primary_team_source)  backup: {backup.name}"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    run(ap.parse_args().apply)
