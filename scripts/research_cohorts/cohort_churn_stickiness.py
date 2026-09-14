"""League stickiness for WR roster%, measured on REDRAFT MANAGED leagues and on the players
that OSCILLATE -- not the ones that take one step.

Why redraft managed only (Joe, 2026-08-02): dynasty keeps the roster by construction and best
ball never moves it, so both are sticky by rule, not by behaviour. Measuring rho on a mix of
the three measures the rule, not the league. Redraft managed is where a manager can actually
drop and re-add, so it is the only place stickiness is an empirical question.

Why oscillation and not level: selecting near a roster% band keeps picking the wrong players.
At 82.7% you get Justin Jefferson -- rostered in ~99% of leagues, never moving, rho 1.000. At
the same band you also get a player who was picked up in week 2 and held for the rest of the
year: one transition, then flat forever. Neither teaches anything about how sticky a league is,
because neither is ever DROPPED.

So select on the flip rate directly. Over the m-1 week boundaries in a league, count how many
times the 0/1 rostered flag CHANGES:

    stints      = 1 + (rostered weeks whose previous week is absent)
    transitions = 2*stints - [rostered in week 1] - [rostered in the final week]
    churn       = transitions / (m - 1)

A one-way step-up is 1 transition regardless of when it happens. A player added, dropped, and
re-added is 4. Churn is the share of week boundaries where the player changed hands, which is
exactly "always going up and down".

The denominator is the ELIGIBLE **LIVE** league set (R9): league_settings carries league-years
with no player rows at all -- 770 of them in 2024 -- and counting those as eligible-but-never-
rostered deflates every rate by ~5%. Absence inside a live league is still a real zero.

Then, on that panel, within cohort (R6):

    rho      = between-league variance of the season rate / p(1-p)
    eff_wks  = m / (1 + (m-1)*rho)          how many independent weeks a season is worth
    N_season = N_weekly / eff_wks
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

MIN_LEAGUES_COHORT = 40    # a cohort too thin to carry a between-league variance
MIN_LEAGUES_CELL = 25      # a (player, cohort) cell too thin to carry one
WEEKLY_N = 300             # measured: +/-5% @ 95% for weekly WR roster% (A4)
CHURN_Q = 0.90             # the panel is the top decile of flip rate


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    y = a.year

    con = duckdb.connect(config={"memory_limit": "2000MB", "threads": 3,
                                 "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/cr{y}"})
    for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
              "PRAGMA max_temp_directory_size='8GB'"):
        con.execute(s)
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{a.ops.as_posix()}' AS ops (READ_ONLY)")
    con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                cohort_league_settings_sql(position_slots=True).replace("public.", "lake.public."))
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE wr AS
    SELECT NFL_player_id AS pid, MAX(player) AS pname
    FROM ops.nfl_historical.nfl_player_stats_all
    WHERE "year"={y} AND NFL_player_id IS NOT NULL AND UPPER(TRIM(position))='WR'
      AND position NOT LIKE '%,%' GROUP BY 1
    """)
    # weeks actually present in each league -- the m in (m-1) boundaries, and the rate divisor
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lw AS
    SELECT db_name, COUNT(DISTINCT week) AS wks, MIN(week) AS w_lo, MAX(week) AS w_hi
    FROM lake.public.player_fantasy WHERE year={y} GROUP BY 1
    """)
    # REDRAFT MANAGED ONLY, and live (present in lw) -- format is held fixed, so it drops out
    # of the cohort key entirely and the grid here is teams x roster x ppr x td x bracket.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lgc AS
    SELECT f.db_name, concat_ws('|', f.teams_WR, f.roster, f.ppr, f.td, f.bracket) AS cohort
    FROM fmt f JOIN lw ON lw.db_name=f.db_name
    WHERE f.year={y} AND f.teams_WR <> 'ALL' AND f.roster IS NOT NULL
      AND f.ppr IS NOT NULL AND f.td IS NOT NULL AND f.bracket IS NOT NULL
      AND COALESCE(f.lineup_mode,'') <> 'best_ball'
      AND COALESCE(f.league_type,'')  <> 'dynasty'
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE elig AS "
                "SELECT cohort, COUNT(*) AS n_elig FROM lgc GROUP BY 1")
    n_lg, n_ch = con.execute("SELECT COUNT(*), COUNT(DISTINCT cohort) FROM lgc").fetchone()
    print(f"redraft managed, live, {y}: {n_lg:,} leagues across {n_ch} cohorts\n")

    # per (player, league): stints, edge flags, weeks held
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pl AS
    SELECT cohort, pid, db_name, wks,
           SUM(CASE WHEN prev IS NULL OR week - prev > 1 THEN 1 ELSE 0 END) AS n_stints,
           MAX(CASE WHEN week = w_lo THEN 1 ELSE 0 END) AS at_start,
           MAX(CASE WHEN week = w_hi THEN 1 ELSE 0 END) AS at_end,
           COUNT(*) AS wks_held
    FROM (
      SELECT g.cohort, pf.NFL_player_id AS pid, pf.db_name, pf.week,
             w.wks, w.w_lo, w.w_hi,
             LAG(pf.week) OVER (PARTITION BY pf.db_name, pf.NFL_player_id
                                ORDER BY pf.week) AS prev
      FROM lake.public.player_fantasy pf
      JOIN wr    ON wr.pid = pf.NFL_player_id
      JOIN lgc g ON g.db_name = pf.db_name
      JOIN lw  w ON w.db_name = pf.db_name
      WHERE pf.year={y}
    ) GROUP BY 1,2,3,4
    """)
    df = con.execute(f"""
    SELECT p.cohort, p.pid, n.pname, e.n_elig, COUNT(*) AS lgs_rostered,
           AVG((2*p.n_stints - p.at_start - p.at_end)::DOUBLE
               / GREATEST(p.wks - 1, 1))                       AS churn,
           AVG(p.n_stints)                                     AS avg_stints,
           SUM(p.wks_held::DOUBLE / p.wks) / e.n_elig          AS overall_rate,
           -- between-league variance with absence as a real zero over the LIVE eligible set
           (SUM(pow(p.wks_held::DOUBLE / p.wks, 2))
            - pow(SUM(p.wks_held::DOUBLE / p.wks), 2) / e.n_elig)
             / (e.n_elig - 1)                                  AS between_var
    FROM pl p JOIN elig e ON e.cohort=p.cohort JOIN wr n ON n.pid=p.pid
    WHERE e.n_elig >= {MIN_LEAGUES_COHORT}
    GROUP BY 1,2,3,4 HAVING COUNT(*) >= {MIN_LEAGUES_CELL}
    """).fetchdf()
    con.close()

    pd.set_option("display.width", 250)
    print(f"(player, cohort) cells: {len(df):,}   players: {df.pid.nunique()}\n")
    print("flip rate -- share of week boundaries where the player changed hands:")
    print(df.churn.describe(percentiles=[.5, .75, .9, .95, .99]).round(3).to_string())

    cut = df.churn.quantile(CHURN_Q)
    panel = df[df.churn >= cut].copy()
    p = panel.overall_rate
    panel["rho"] = (panel.between_var / (p * (1 - p))).clip(0, 1)
    m = 17
    panel["eff_weeks"] = m / (1 + (m - 1) * panel.rho)
    panel["season_n"] = np.ceil(WEEKLY_N / panel.eff_weeks)
    panel.to_parquet(a.out, index=False)

    print(f"\nCHURN PANEL: flip rate >= {cut:.3f} (top {1-CHURN_Q:.0%})  ->  {len(panel):,} cells, "
          f"{panel.pid.nunique()} players, {panel.cohort.nunique()} cohorts")
    print(f"  their roster% sits at p50 {panel.overall_rate.median():.1%} "
          f"(p10 {panel.overall_rate.quantile(.1):.1%} - p90 {panel.overall_rate.quantile(.9):.1%})\n")
    print("LEAGUE STICKINESS, redraft managed, within cohort:")
    print(f"  rho        p50 {panel.rho.median():.3f}   p10 {panel.rho.quantile(.1):.3f}"
          f"   p90 {panel.rho.quantile(.9):.3f}")
    print(f"  eff weeks  p50 {panel.eff_weeks.median():.2f}   p90 {panel.eff_weeks.quantile(.9):.2f}")
    print(f"  season N   p50 {panel.season_n.median():.0f}   p90 {panel.season_n.quantile(.9):.0f}"
          f"   (weekly is {WEEKLY_N})")
    print(f"\n  => a season is worth {panel.eff_weeks.median():.1f} weeks; season roster% needs "
          f"~{panel.season_n.median():.0f} leagues vs {WEEKLY_N} weekly")

    print("\nthe panel, most-churning first:")
    top = (panel.groupby("pname")
           .agg(cells=("rho", "size"), churn=("churn", "mean"), stints=("avg_stints", "mean"),
                rate=("overall_rate", "mean"), rho=("rho", "median"), eff=("eff_weeks", "median"))
           .nlargest(20, "churn"))
    print(top.round(3).to_string())


if __name__ == "__main__":
    main()
