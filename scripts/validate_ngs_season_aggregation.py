#!/usr/bin/env python3
"""
Validate our weekly->season NGS aggregation against nflverse's PUBLISHED season value.

The user's gate: if we can reproduce the published season NGS number from the weekly rows
with a defensible rollup, we trust the method (and, by extension, similar rollups we might
build from our own PBP). NGS publishes both weekly rows (week>=1) and a season-total row
(week==0) per player-season -- so week==0 is ground truth.

For each metric we try three rollups and report which reproduces week==0:
  SUM         season = sum(weekly)                        (per-game TOTALS, e.g. RYOE)
  AVG         season = mean(weekly)                       (unweighted)
  WAVG[w]     season = sum(weekly*w)/sum(w)               (volume-weighted; w in targets/
                                                            receptions/attempts/rush_attempts)
Reports median abs error + % of players within tolerance, per method, for a sample season.

    python scripts/validate_ngs_season_aggregation.py --year 2023
"""
from __future__ import annotations

import argparse

import duckdb

BASE = "https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats"

# metric -> candidate volume-weight column present in that NGS table
TABLES = {
    "receiving": {
        "url": f"{BASE}/ngs_receiving.parquet",
        "weights": ["targets", "receptions"],
        "metrics": ["avg_cushion", "avg_separation", "avg_yac", "avg_expected_yac",
                    "avg_yac_above_expectation", "percent_share_of_intended_air_yards"],
    },
    "rushing": {
        "url": f"{BASE}/ngs_rushing.parquet",
        "weights": ["rush_attempts"],
        "metrics": ["efficiency", "percent_attempts_gte_eight_defenders", "avg_time_to_los",
                    "expected_rush_yards", "rush_yards_over_expected", "rush_pct_over_expected"],
    },
    "passing": {
        "url": f"{BASE}/ngs_passing.parquet",
        "weights": ["attempts"],
        "metrics": ["avg_time_to_throw", "aggressiveness", "avg_air_yards_to_sticks",
                    "expected_completion_percentage", "completion_percentage_above_expectation",
                    "avg_air_yards_differential"],
    },
}
TOL = {"default": 0.10}  # abs tolerance for "matches published"


def _score(con, method_sql: str, metric: str, tol: float) -> tuple[float, float, int]:
    q = f"""
        WITH wk AS (SELECT player_gsis_id gid, {method_sql} AS est
                    FROM weekly WHERE {metric} IS NOT NULL GROUP BY player_gsis_id),
             se AS (SELECT player_gsis_id gid, {metric} AS pub FROM season WHERE {metric} IS NOT NULL)
        SELECT median(abs(est-pub)) med_err,
               avg(CASE WHEN abs(est-pub)<={tol} THEN 1.0 ELSE 0.0 END)*100 pct_match,
               count(*) n
        FROM wk JOIN se USING(gid) WHERE est IS NOT NULL AND pub IS NOT NULL
    """
    r = con.execute(q).fetchone()
    return (r[0] or 9.9, r[1] or 0.0, r[2] or 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2023)
    args = ap.parse_args()
    con = duckdb.connect()
    con.execute("SET memory_limit='800MB'")
    con.execute("PRAGMA threads=2")
    con.execute("INSTALL httpfs; LOAD httpfs")

    for fam, cfg in TABLES.items():
        con.execute(f"CREATE OR REPLACE TEMP TABLE weekly AS SELECT * FROM read_parquet('{cfg['url']}') WHERE week>=1 AND season={args.year}")
        con.execute(f"CREATE OR REPLACE TEMP TABLE season AS SELECT * FROM read_parquet('{cfg['url']}') WHERE week=0 AND season={args.year}")
        print(f"\n=== {fam} {args.year} (weekly->season vs published week=0) ===")
        print(f"  {'metric':<42} {'best method':<16} {'med_err':>8} {'%match':>7} {'n':>4}")
        for m in cfg["metrics"]:
            tol = TOL.get(m, TOL["default"])
            cands = {"SUM": f"SUM({m})", "AVG": f"AVG({m})"}
            for w in cfg["weights"]:
                cands[f"WAVG[{w}]"] = f"SUM({m}*{w})/NULLIF(SUM({w}),0)"
            best = None
            for name, sql in cands.items():
                med, pct, n = _score(con, sql, m, tol)
                if best is None or pct > best[2] or (pct == best[2] and med < best[1]):
                    best = (name, med, pct, n)
            print(f"  {m:<42} {best[0]:<16} {best[1]:>8.3f} {best[2]:>6.1f}% {best[3]:>4}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
