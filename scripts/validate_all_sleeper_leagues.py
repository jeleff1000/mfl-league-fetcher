#!/usr/bin/env python3
"""
Run validators on all Sleeper leagues.

This script reads leagues from ___ops.main.sleeper_leagues in MotherDuck and runs
validation on each league, showing full validation reports.

Usage:
    python scripts/validate_all_sleeper_leagues.py
    python scripts/validate_all_sleeper_leagues.py --verbose  # Show full validation reports per league
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "fantasy_football_data_scripts"))

from validate_common import run_validation


def get_sleeper_leagues_from_motherduck() -> list:
    """Fetch all Sleeper leagues from ___ops.main.sleeper_leagues table."""
    from multi_league.core.db_reader import get_reader

    reader = get_reader()
    df = reader.query_df(
        "SELECT sleeper_league_id, league_name, database_name FROM main.sleeper_leagues ORDER BY league_name",
        database="___ops",
    )
    return df.to_dict("records")


def main():
    parser = argparse.ArgumentParser(description="Validate all Sleeper leagues")
    parser.add_argument("--verbose", action="store_true", help="Show full validation reports per league")
    args = parser.parse_args()

    # Fetch leagues from MotherDuck
    print("Fetching Sleeper leagues from ___ops.main.sleeper_leagues...")
    try:
        leagues = get_sleeper_leagues_from_motherduck()
    except Exception as e:
        print(f"Error fetching leagues from MotherDuck: {e}")
        return 1

    return run_validation(leagues, platform="Sleeper", verbose=args.verbose)


if __name__ == "__main__":
    exit(main())
