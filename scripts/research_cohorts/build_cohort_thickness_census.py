"""THE standing cohort-thickness census: every position x year x cohort, and how many leagues.

Joe, 2026-08-03: "why don't we have a standing doc of all 9 positions for all 23 years and how
thick they are in each cohort yet?" -- because it kept getting re-derived inside whatever
analysis needed it. It is the denominator the entire pooling program keys on, so it is a table.

THE GRID -- 7 dimensions:
    teams       08tm / 10tm / 12tm / 14tm     derived capacity, POSITION-SPECIFIC (see below)
    roster      flx / idp / sflx
    ppr         std / half / ppr
    td          4pt / 6pt
    bracket     4po / 6po / 8po
    lineup_mode managed / best_ball
    league_type redraft / dynasty
= 864 cells per position-year.

TEAMS IS NOT LEAGUE SIZE. It is observed capacity for THAT position, tiered against the
literal team-count distribution: a league with ~100 WR roster spots is '10tm' for WR whether it
literally has 10, 12 or 20 teams. So the same league lands in different team tiers for QB, RB,
WR and TE, and the census must be built per position, never once and reused.

WHY SETTINGS-ONLY. Thickness is a property of the league population, not of anybody's stat
line, so this reads league_settings and never touches player_fantasy. That makes it minutes
rather than the hours a base extract costs, and it is why it can stand as a maintained table.

THE DENOMINATOR IS THE LIVE LEAGUE SET (R9). league_settings carries league-years with no
player rows at all -- 770 of them in 2024 -- and counting those as eligible inflates every
cohort and deflates every rate built on it. A league counts here only if it has player rows.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

import position_slots_contract as PS
from cohort_format_sql import cohort_league_settings_sql

# k_slots / def_slots drive the K and DEF eligibility gates and are not in the default select.
_EXTRAS = ("COALESCE(s.roster_K, 0) AS k_slots", "COALESCE(s.roster_DEF, 0) AS def_slots")

# Straight from the contract: a position added there must appear here, and its tier column
# must exist, or the census silently mislabels it.
POSITIONS = PS.TIER_POSITIONS
AXES = ("teams", "roster", "ppr", "td", "bracket", "lineup_mode", "league_type")


def census_year(snapshot: Path, ops: Path, year: int, tmp: Path) -> pd.DataFrame:
    con = duckdb.connect(config={
        "memory_limit": "1500MB", "threads": 3, "temp_directory": str(tmp / f"c{year}")})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  "PRAGMA max_temp_directory_size='10GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        # position_slots=True derives observed capacity from the super table, so ops must be
        # attached even though nothing here reads a stat line.
        con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
        # PASS THE YEAR: unscoped, the capacity CTEs aggregate every year x 9 positions in one
        # query and take the runtime down rather than merely running slowly.
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True, year=year,
                                               extra_select=_EXTRAS)
                    .replace("public.", "lake.public."))
        # LIVE only (R9) -- a settings row with no player rows is not an eligible league.
        con.execute(f"""CREATE OR REPLACE TEMP TABLE live AS
          SELECT DISTINCT db_name FROM lake.public.player_fantasy WHERE year={year}""")
        pos_union = " UNION ALL ".join(f"SELECT '{p}' AS pos" for p in POSITIONS)
        # ELIGIBILITY, NOT JUST PRESENCE. A flex league is not a denominator for a linebacker.
        # A CASE over the joined position, not a predicate inside the position subquery --
        # that subquery is CROSS JOINed and cannot see `f`.
        elig_case = "CASE p.pos " + " ".join(
            f"WHEN '{p}' THEN ({PS.position_eligibility_sql(p, 'f')})"
            for p in POSITIONS) + " ELSE FALSE END"
        # EVERY position reads its OWN tier column. The four-flex CASE this was copied from
        # predates the K/DEF/DL/LB/DB cuts and quietly fell back to the 2-level `teams` for
        # five of nine positions -- so a K cohort and a DB cohort came out identical, which is
        # exactly the collapse the contract was rewritten to stop. Built from TIER_POSITIONS so
        # a position added to the contract cannot be silently missed here.
        teams_case = "CASE p.pos " + " ".join(
            f"WHEN '{p}' THEN f.teams_{p}" for p in POSITIONS) + " END"
        return con.execute(f"""
          SELECT {year} AS year, p.pos AS position,
                 {teams_case} AS teams,
                 f.roster, f.ppr, f.td, f.bracket, f.lineup_mode, f.league_type,
                 COUNT(*) AS n_leagues
          FROM fmt f
          JOIN live l ON l.db_name = f.db_name
          CROSS JOIN ({pos_union}) p
          WHERE f.year={year} AND {elig_case}
          GROUP BY ALL""").fetchdf()
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--first-year", type=int, default=2003)
    ap.add_argument("--last-year", type=int, default=2025)
    ap.add_argument("--tmp", type=Path, default=Path("D:/tmp/ddbtmp/census"))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.tmp.mkdir(parents=True, exist_ok=True)

    frames = []
    for year in range(a.first_year, a.last_year + 1):
        df = census_year(a.snapshot, a.ops, year, a.tmp)
        frames.append(df)
        wr = df[df.position == "WR"]
        print(f"  {year}: {int(df.n_leagues.sum()/len(POSITIONS)):,} live leagues, "
              f"{len(wr)} of 864 WR cells populated", flush=True)
    out = pd.concat(frames, ignore_index=True)
    # UNKNOWN IS NOT A LEVEL. A NULL axis means the setting was never captured for that league;
    # it must not silently become its own cohort, so it is labelled and counted separately.
    for ax in AXES:
        out[ax] = out[ax].fillna("(unknown)")
    out.to_parquet(a.out, index=False)

    pd.set_option("display.width", 200)
    print(f"\ncohort thickness census -> {a.out}")
    print(f"{len(out):,} rows = year x position x cohort, "
          f"{out.year.nunique()} years, {out.position.nunique()} positions\n")
    g = out.groupby("position").agg(
        cells=("n_leagues", "size"), leagues=("n_leagues", "sum"),
        median_cell=("n_leagues", "median"),
        cells_over_90=("n_leagues", lambda s: (s >= 90).sum()),
        cells_over_140=("n_leagues", lambda s: (s >= 140).sum()))
    g["pct_over_90"] = (100 * g.cells_over_90 / g.cells).round(1)
    print(g.to_string())


if __name__ == "__main__":
    main()
