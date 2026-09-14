#!/usr/bin/env python3
"""Rebuild Fly NFL season/career aggregate tables from the weekly supertable."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))
sys.path.insert(0, str(ROOT))

from multi_league.data_fetchers.aggregate_nfl_stats_fly import update_aggregates  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rollup-date", default="")
    parser.add_argument("--timeout-seconds", type=int, default=1200)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--current-year-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rollup_date:
        os.environ["NFL_ROLLUP_DATE"] = args.rollup_date
    os.environ["FLY_QUERY_TIMEOUT_SECONDS"] = str(args.timeout_seconds)
    counts = update_aggregates(year=args.year, rebuild_all_years=not args.current_year_only)
    print(json.dumps(counts, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
