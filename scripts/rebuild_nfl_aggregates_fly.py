#!/usr/bin/env python3
"""Run the Fly NFL season/career aggregate rebuild."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.aggregate_nfl_stats_fly import update_aggregates  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--incremental", action="store_true", help="Only rebuild the requested year for season tables.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    counts = update_aggregates(year=args.year, rebuild_all_years=not args.incremental)
    print(counts, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
