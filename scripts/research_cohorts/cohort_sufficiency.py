"""Cohort sufficiency on the CANONICAL contract: teams = position slot market.

`teams` is not team count.  It is league-wide starting slots at the position, bucketed to
the same 10t/12t alphabet (position_slots_contract).  So an 8- or 16-team league is not
excluded -- it lands in whichever slot market it actually constitutes, and one league-year
can be 12t for RB and 10t for TE.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

Z = NormalDist().inv_cdf(0.975)
CLT_FLOOR = 30
SLOT_POS = ("QB", "RB", "WR", "TE")      # K/DEF are not slot-modelled; they use `teams`


def need(p: float, e: float, zero_share: float = 0.0) -> int:
    n = Z * Z * p * (1 - p) / (e * e)
    return max(CLT_FLOOR, math.ceil(n / max(1e-9, 1.0 - zero_share)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(config={"memory_limit": "1000MB", "threads": 2})
    con.execute("SET enable_progress_bar=false")
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS lake (READ_ONLY)")
    ls = cohort_league_settings_sql(position_slots=True).replace("public.", "lake.public.")
    con.execute(f"CREATE OR REPLACE TEMP TABLE fmt AS {ls}")

    cols = {r[0] for r in con.execute("DESCRIBE fmt").fetchall()}
    print("contract columns:", sorted(c for c in cols if c.startswith("teams") or c in
                                      ("roster", "ppr", "td", "bracket", "year")))

    # One row per (league-year, position) using that position's own slot bucket.
    parts = []
    for pos in SLOT_POS:
        tcol = f"teams_{pos}" if f"teams_{pos}" in cols else "teams"
        parts.append(f"SELECT '{pos}' AS pos_grp, {tcol} AS teams, roster, ppr, td, bracket, "
                     f"year, db_name FROM fmt")
    for pos in ("K", "DEF"):
        parts.append(f"SELECT '{pos}' AS pos_grp, teams, roster, ppr, td, bracket, "
                     f"year, db_name FROM fmt")
    grid = con.execute(f"""
        SELECT pos_grp, teams, roster, ppr, td, bracket, year,
               COUNT(DISTINCT db_name) AS leagues
        FROM ({" UNION ALL ".join(parts)})
        WHERE teams IS NOT NULL AND roster IS NOT NULL AND ppr IS NOT NULL
          AND td IS NOT NULL AND bracket IS NOT NULL AND year >= 2003
        GROUP BY 1,2,3,4,5,6,7
    """).fetchdf()
    con.close()

    tiers = {"pct_1pt": need(0.5, 0.01), "pct_3pt": need(0.5, 0.03), "pct_5pt": need(0.5, 0.05),
             "clutch_tier": need(0.5, 0.30, 0.315), "champ_tier": need(0.5, 0.15),
             "playoff_tier": need(0.5, 0.10)}
    for k, n in tiers.items():
        grid[f"ok_{k}"] = grid.leagues >= n
    grid.to_parquet(a.out, index=False)

    axes = {c: sorted(grid[c].dropna().unique().tolist()) for c in
            ("teams", "roster", "ppr", "td", "bracket")}
    cells = 1
    for c, v in axes.items():
        print(f"  {c:8s} {len(v):>2} : {v}")
        cells *= len(v)
    print(f"\ncohort grid = {cells} cells x {grid.pos_grp.nunique()} position groups"
          f" = {cells * grid.pos_grp.nunique()} slices")
    print(f"league-years covered: {int(grid[grid.pos_grp=='RB'].leagues.sum()):,} (RB view)\n")
    print("thresholds:", {k: f"{v:,}" for k, v in tiers.items()}, "\n")

    per_year = grid[grid.pos_grp == "RB"].groupby("year").agg(
        cohorts=("leagues", "size"), leagues=("leagues", "sum"),
        **{f"n_{k}": (f"ok_{k}", "sum") for k in tiers}).reset_index()
    print(f"RB view, cohorts clearing each bar (of {cells} possible per year):")
    print(per_year.to_string(index=False))


if __name__ == "__main__":
    main()
