"""coverage_gap.py -- how much MORE do we need to ingest to call a cohort complete?

Joe 2026-07-20: "we still don't know how much more we need to call it complete in terms of
seasons ingested in each cohort."

Correctness and completeness are different questions. Everything else in this directory works
on correctness; this answers completeness, in the only unit that matters (Joe, same day):
LEAGUE-SEASONS. A ten-year league is ten observations.

Joins what each cohort HAS against what ladder_stab says each metric NEEDS, and reports the
multiple still required. Run after any ladder sweep or corpus growth.

    py -3 scripts/research_cohorts/coverage_gap.py
    py -3 scripts/research_cohorts/coverage_gap.py --target 0.75   # the looser bar

IMPORTANT INTERPRETIVE LIMIT, stated up front because it is easy to over-read this table:
ladder_stab samples league-seasons from the WHOLE gated lake, so a threshold of "n=1,659"
means 1,659 league-seasons drawn across every cohort and year. Applying it per-cohort assumes
within-cohort variance resembles lake-wide variance. That is a reasonable planning assumption
and a poor precision claim -- treat the multiples as scale ("we need ~3x"), not as targets to
the nearest league.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))

OUT_DIR = Path(os.environ.get(
    "RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))

# served matchup metric -> ladder class (mirrors build_wide_bundle's TABLES)
MATCHUP_METRICS = {
    "start_rate": "srate", "won/lost": "win", "clutch": "clutch",
    "champ_wk": "champ", "playoff%": "po_started",
}

COHORT_SQL = """
WITH ls AS (
  SELECT db_name, year,
    CASE WHEN num_teams <= 11 THEN '10t' ELSE '12t' END AS teams,
    CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)
              +COALESCE(roster_DB,0)+COALESCE(roster_DB_LB,0)+COALESCE(roster_DL_LB,0) > 0 THEN 'idp'
         WHEN COALESCE(roster_SUPER_FLEX,0) > 0 THEN 'sflx' ELSE 'flx' END AS roster,
    CASE WHEN COALESCE(scoring_rec,0) = 0 THEN 'std'
         WHEN COALESCE(scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END AS ppr,
    CASE WHEN COALESCE(scoring_pass_td,4) >= 5 THEN '6pt' ELSE '4pt' END AS td
  FROM public.league_settings WHERE NOT COALESCE(sleeper_best_ball, false))
SELECT teams||'_'||roster||'_'||ppr||'_'||td AS cohort,
       COUNT(*) AS league_seasons, COUNT(DISTINCT year) AS years,
       CAST(ROUND(COUNT(*)*1.0/NULLIF(COUNT(DISTINCT year),0)) AS INTEGER) AS per_year
FROM ls GROUP BY 1 ORDER BY 2 DESC
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target", type=float, default=0.85, choices=[0.75, 0.85, 0.95])
    ap.add_argument("--grain", default="season", choices=["week", "season", "career"])
    ap.add_argument("--top", type=int, default=14)
    args = ap.parse_args()

    thr_path = (OUT_DIR / "ladder_thresholds.json" if args.grain == "season"
                else OUT_DIR / f"ladder_thresholds.{args.grain}.json")
    if not thr_path.exists():
        raise SystemExit(f"no thresholds for grain={args.grain}: {thr_path} -- run ladder_stab")
    doc = json.loads(thr_path.read_text(encoding="utf-8"))
    key = f"n_r{int(args.target * 100)}"
    need = {m: res.get(key) for m, res in doc.get("metrics", {}).items()}

    from local_reader import LocalReader
    fly = LocalReader()
    df = pd.DataFrame(fly.query(COHORT_SQL, "___leagues"))
    fly.close()
    total = int(df.league_seasons.sum())

    print(f"\nCOVERAGE GAP -- grain={args.grain}, bar r={args.target}"
          f"  (thresholds {thr_path.name}, version {doc.get('version')})")
    print(f"gated lake: {total:,} league-seasons across {len(df)} cohorts\n")

    print("what each MATCHUP metric needs, and what the lake's biggest cohorts have:")
    hdr = f"  {'cohort':<20}{'have':>7}{'/yr':>6}"
    for m in MATCHUP_METRICS:
        hdr += f"{m:>12}"
    print(hdr)
    for _, row in df.head(args.top).iterrows():
        line = f"  {row.cohort:<20}{row.league_seasons:>7,}{row.per_year:>6}"
        for m, cls in MATCHUP_METRICS.items():
            n = need.get(cls)
            if n is None:
                line += f"{'n/a':>12}"          # never crossed the bar in the tested grid
            else:
                mult = n / max(row.league_seasons, 1)
                line += f"{('OK' if mult <= 1 else f'{mult:.1f}x'):>12}"
        print(line)

    print("\n  OK   = this cohort already holds enough league-seasons for that bar")
    print("  N.Nx = multiply the cohort's league-seasons by this to reach it")
    print("  n/a  = the metric never reached this bar anywhere in the tested grid,")
    print("         so MORE INGEST DOES NOT FIX IT -- it needs a coarser rung, a")
    print("         different grain, or acceptance that it is a 'what happened'")
    print("         board rather than a ranking.\n")

    reachable = {m: c for m, c in MATCHUP_METRICS.items() if need.get(c) is not None}
    if reachable:
        worst = max(reachable.items(), key=lambda kv: need[kv[1]])
        big = int(df.league_seasons.max())
        print(f"binding constraint among reachable metrics: {worst[0]} "
              f"(needs {need[worst[1]]:,}); biggest cohort holds {big:,} "
              f"-> {need[worst[1]]/max(big,1):.1f}x")
    print(f"unreachable at this bar: "
          f"{', '.join(m for m, c in MATCHUP_METRICS.items() if need.get(c) is None) or 'none'}")


if __name__ == "__main__":
    main()
