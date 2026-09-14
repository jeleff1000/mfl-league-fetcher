#!/usr/bin/env python3
"""
Data Fetchers Orchestrator

Coordinates fetching raw data from Yahoo and NFL APIs.
Can be called by initial_import_v2.py or run standalone for debugging.

Usage:
    python orchestrator.py --context /path/to/league_context.json [--mode full|weekly] [--week N]
"""

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.script_runner import run_script, log
from multi_league.core.league_context import LeagueContext
from multi_league.core.import_config import (
    DATA_FETCHERS,
    YAHOO_NFL_MERGE,
    DEFAULT_FETCHER_TIMEOUT,
)

# Data fetchers for a full import (all years, all data types)
# Import from centralized config
FULL_FETCHERS = DATA_FETCHERS

# Merge scripts that run after fetchers
MERGE_SCRIPTS = [
    (YAHOO_NFL_MERGE, "Merge Yahoo + NFL"),
]

# Fetchers for a weekly update (only current season data that changes)
WEEKLY_FETCHERS = [
    ("multi_league/data_fetchers/yahoo/yahoo_matchups.py", "Matchup data"),
    ("multi_league/data_fetchers/yahoo/yahoo_rosters.py", "Yahoo player data"),
    ("multi_league/data_fetchers/yahoo/yahoo_transactions.py", "Transactions"),
]


def run_full_fetch(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run all data fetchers for a full import.

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json (for run_script)

    Returns:
        True if all fetchers succeeded, False if any failed
    """
    log("[FETCH] Starting full data fetch...")

    all_success = True

    # Run data fetchers (tuples may have 2 or 3 elements: script, label[, timeout])
    for fetcher_tuple in FULL_FETCHERS:
        script = fetcher_tuple[0]
        description = fetcher_tuple[1]
        timeout = fetcher_tuple[2] if len(fetcher_tuple) > 2 else DEFAULT_FETCHER_TIMEOUT
        ok, err = run_script(script, description, context_path, timeout=timeout)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    # Run merge scripts
    for script, description in MERGE_SCRIPTS:
        ok, err = run_script(script, description, context_path)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Full data fetch complete")
    else:
        log("[WARN] Full data fetch completed with errors")

    return all_success


def run_weekly_fetch(ctx: LeagueContext, context_path: str, week: int = None, year: int = None) -> bool:
    """
    Run only the fetchers needed for a weekly update.

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json
        week: Specific week to fetch (optional)
        year: Specific year to fetch (optional)

    Returns:
        True if all fetchers succeeded, False if any failed
    """
    log(f"[FETCH] Starting weekly data fetch (week={week}, year={year})...")

    all_success = True

    # Build additional args
    additional_args = []
    if week is not None:
        additional_args.extend(["--week", str(week)])
    if year is not None:
        additional_args.extend(["--year", str(year)])

    # Run weekly fetchers
    for script, description in WEEKLY_FETCHERS:
        ok, err = run_script(
            script, description, context_path, additional_args=additional_args if additional_args else None
        )
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    # Run merge scripts
    for script, description in MERGE_SCRIPTS:
        ok, err = run_script(
            script, description, context_path, additional_args=additional_args if additional_args else None
        )
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Weekly data fetch complete")
    else:
        log("[WARN] Weekly data fetch completed with errors")

    return all_success


def main():
    parser = argparse.ArgumentParser(description="Data Fetchers Orchestrator")
    parser.add_argument("--context", type=Path, required=True, help="Path to league_context.json")
    parser.add_argument("--mode", choices=["full", "weekly"], default="full", help="Fetch mode")
    parser.add_argument("--week", type=int, help="Specific week (for weekly mode)")
    parser.add_argument("--year", type=int, help="Specific year (for weekly mode)")
    args = parser.parse_args()

    ctx = LeagueContext.load(args.context)
    context_path = str(args.context)

    if args.mode == "full":
        success = run_full_fetch(ctx, context_path)
    else:
        success = run_weekly_fetch(ctx, context_path, week=args.week, year=args.year)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
