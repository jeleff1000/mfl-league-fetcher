"""
sota_recon/recon_rate_fingerprint.py  --  LANE: aggregation-fingerprint detector for rate stats.

Phase 1a of the SOTA closeout plan (WS2b enforcement layer 2). For every registered
rate/weighted-mean column in player_nfl_season, compare the STORED season value against
three candidate aggregations of the weekly values:

    SUM    = SUM(weekly)                      <- matching this on a rate col is the
                                                 summed-average corruption (Burrow 45.98s
                                                 time-to-throw)
    MEAN   = AVG(weekly)                      <- matching this on a volume-weighted stat
                                                 is the misweight
    WMEAN  = SUM(weekly*w)/SUM(w) over weeks  <- the correct aggregation for WMEAN cols
             where weekly IS NOT NULL            (and equal to RATE's sum-num/sum-den)

Each column's DECLARED type comes from the registry below (v0; migrates into the QCL).
Verdict per column: OK / SUMMED-AVERAGE / MISWEIGHT / UNDETERMINED. SUM-declared control
columns (EPA) prove the detector discriminates. Runs on every build.

NGS published-season columns are checked directly against the published NGS season witness;
their weekly weighted means are not treated as the season authority. The denominator map is
used by the career builder.

    python -m scripts.sota_recon.recon_rate_fingerprint [--year-min 2016]
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from .sources import latest_v26

NGS_SEASON_SOURCE = Path("D:/league-history-data/nfl/raw/nextgen_stats/ngs_season_2016_2025.parquet")

TOL = 0.05          # absolute agreement tolerance per player-season
MATCH_SHARE = 0.90  # a candidate "matches" the column when >=90% of comparable rows agree

# column -> (declared_type, weight_col). RATE cols: weekly col is already the ratio,
# weight = its denominator, correct season value == WMEAN. SUM rows are controls.
REGISTRY = {
    # QB NGS / model metrics (per pass attempt)
    "ngs_avg_time_to_throw": ("PUBLISHED", None),
    "ngs_avg_air_yards_to_sticks": ("PUBLISHED", None),
    "ngs_avg_air_yards_differential": ("PUBLISHED", None),
    "ngs_aggressiveness": ("PUBLISHED", None),
    "ngs_completion_pct_above_expectation": ("PUBLISHED", None),
    "ngs_expected_completion_pct": ("PUBLISHED", None),
    "ngs_expected_rush_yards": ("PUBLISHED", None),
    "ngs_rush_yards_over_expected": ("PUBLISHED", None),
    "passing_cpoe": ("WMEAN", "attempts"),
    # receiver NGS (per target / reception)
    "ngs_avg_cushion": ("PUBLISHED", None),
    "ngs_avg_separation": ("PUBLISHED", None),
    "ngs_pct_share_intended_air_yards": ("PUBLISHED", None),
    "ngs_avg_yac": ("PUBLISHED", None),
    "ngs_avg_expected_yac": ("PUBLISHED", None),
    "ngs_avg_yac_above_expectation": ("PUBLISHED", None),
    # rusher NGS (per carry)
    "ngs_rush_efficiency": ("PUBLISHED", None),
    "ngs_pct_att_gte_8_defenders": ("PUBLISHED", None),
    "ngs_rush_pct_over_expected": ("PUBLISHED", None),
    # classic rates (weekly col is the ratio). Weekly and season names DIFFER for these --
    # third tuple slot = weekly column name (season name is the dict key).
    "comp_pct": ("RATE", "attempts", "completion_pct"),
    "yards_per_attempt": ("RATE", "attempts", "passing_yards_per_attempt"),
    "yards_per_carry": ("RATE", "carries", "rushing_yards_per_carry"),
    "yards_per_reception": ("RATE", "receptions", "receiving_yards_per_reception"),
    "yards_per_target": ("RATE", "targets", "receiving_yards_per_target"),
    # wave52 rates (weekly cols from build_derived_columns_v26; season cols land with
    # the cascade -- the runner SKIPs absent columns instead of crashing)
    "yards_per_touch": ("RATE", "touches", "yards_per_touch"),
    "adjusted_yards_per_attempt": ("RATE", "attempts",
                                   "passing_adjusted_yards_per_attempt"),
    "net_yards_per_attempt": ("RATE", "dropbacks", "passing_net_yards_per_attempt"),
    "adjusted_net_yards_per_attempt": ("RATE", "dropbacks",
                                       "passing_adjusted_net_yards_per_attempt"),
    # SUM controls -- correct season value IS the sum; detector must call these OK
    "passing_epa": ("SUM", "attempts"),
    "rushing_epa": ("SUM", "carries"),
    "receiving_epa": ("SUM", "targets"),
}


def _weekly_name(col: str) -> str:
    spec = REGISTRY[col]
    return spec[2] if len(spec) > 2 else col


def season_parquet() -> str:
    return Path(
        os.path.join(os.path.dirname(latest_v26()), "season_career_v26", "player_nfl_season.parquet")
    ).as_posix()


def run(year_min: int = 1999, csv: str | None = None) -> list[dict]:
    wq = Path(latest_v26()).as_posix()
    sq = season_parquet()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    # fail-soft on columns that haven't landed yet (registry declares INTENT; weekly
    # cols arrive with wave52, season cols with the cascade) -- skip, never crash
    wk_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{wq}'").fetchall()}
    sn_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{sq}'").fetchall()}
    live = {col: spec for col, spec in REGISTRY.items()
            if col in sn_cols and (
                spec[0] == "PUBLISHED"
                or (_weekly_name(col) in wk_cols and spec[1] in wk_cols)
            )}
    skipped = sorted(set(REGISTRY) - set(live))

    aggs = []
    for col, spec in live.items():
        if spec[0] == "PUBLISHED":
            continue
        w, wcol = spec[1], _weekly_name(col)
        aggs.append(f"SUM({wcol}) AS {col}__sum")
        aggs.append(f"AVG({wcol}) AS {col}__mean")
        aggs.append(
            f"SUM({wcol} * {w}) / NULLIF(SUM(CASE WHEN {wcol} IS NOT NULL THEN {w} END), 0) AS {col}__wmean"
        )
    con.execute(f"""
        CREATE TEMP TABLE wk AS
        SELECT NFL_player_id, year, {', '.join(aggs)}
        FROM '{wq}' WHERE year >= {year_min} AND season_type = 'REG'
        GROUP BY NFL_player_id, year""")

    results = [dict(column=c, declared=REGISTRY[c][0], n=0, match_sum=0,
                    match_mean=0, match_wmean=0, verdict="SKIPPED-COL-ABSENT")
               for c in skipped]
    for col, spec in live.items():
        declared = spec[0]
        if declared == "PUBLISHED":
            n, matched, mismatched = con.execute(f"""
                SELECT COUNT(*) AS expected_rows,
                       COUNT(s.NFL_player_id) AS matched_rows,
                       COUNT(*) FILTER (
                           WHERE s.NFL_player_id IS NOT NULL
                             AND NOT (s.{col} IS NOT DISTINCT FROM g.{col})
                       ) AS mismatch_rows
                FROM '{NGS_SEASON_SOURCE.as_posix()}' g
                LEFT JOIN '{sq}' s
                  ON s.NFL_player_id = g.NFL_player_id AND s.year = g.year
            """).fetchone()
            verdict = "OK" if matched == n and mismatched == 0 else "PUBLISHED-MISMATCH"
            results.append(dict(column=col, declared=declared, n=n, match_sum=0,
                                match_mean=0, match_wmean=matched - mismatched,
                                verdict=verdict))
            continue
        n, m_sum, m_mean, m_wmean = con.execute(f"""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE ABS(s.{col} - wk.{col}__sum)   <= {TOL}),
                   COUNT(*) FILTER (WHERE ABS(s.{col} - wk.{col}__mean)  <= {TOL}),
                   COUNT(*) FILTER (WHERE ABS(s.{col} - wk.{col}__wmean) <= {TOL})
            FROM '{sq}' s JOIN wk ON s.NFL_player_id = wk.NFL_player_id AND s.year = wk.year
            WHERE s.year >= {year_min} AND s.{col} IS NOT NULL AND wk.{col}__wmean IS NOT NULL
              AND ABS(wk.{col}__sum - wk.{col}__wmean) > {TOL}  -- multi-week rows only: candidates distinguishable
        """).fetchone()
        if n == 0:
            verdict = "NO-COMPARABLE-ROWS"
        else:
            best = max(("SUM", m_sum), ("MEAN", m_mean), ("WMEAN", m_wmean), key=lambda t: t[1])[0]
            share = {"SUM": m_sum, "MEAN": m_mean, "WMEAN": m_wmean}[best] / n
            if share < MATCH_SHARE:
                verdict = "UNDETERMINED"
            elif declared == "SUM":
                verdict = "OK" if best == "SUM" else f"MISAGGREGATED->{best}"
            elif best == "SUM":
                verdict = "SUMMED-AVERAGE"
            elif best == "MEAN":
                verdict = "MISWEIGHT"
            else:
                verdict = "OK"
        results.append(dict(column=col, declared=declared, n=n, match_sum=m_sum,
                            match_mean=m_mean, match_wmean=m_wmean, verdict=verdict))
    con.close()
    if csv:
        import csv as _csv
        with open(csv, "w", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=list(results[0]))
            w.writeheader(); w.writerows(results)
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-min", type=int, default=1999)
    ap.add_argument("--csv", default=None)
    a = ap.parse_args()
    rows = run(a.year_min, a.csv)
    bad = [r for r in rows if r["verdict"] not in ("OK", "NO-COMPARABLE-ROWS")]
    print(f"{'column':40s} {'declared':7s} {'n':>6s} {'sum':>6s} {'mean':>6s} {'wmean':>6s}  verdict")
    for r in rows:
        print(f"{r['column']:40s} {r['declared']:7s} {r['n']:6d} {r['match_sum']:6d} "
              f"{r['match_mean']:6d} {r['match_wmean']:6d}  {r['verdict']}")
    print(f"\nCORRUPTION SIGNATURES: {len(bad)}")
