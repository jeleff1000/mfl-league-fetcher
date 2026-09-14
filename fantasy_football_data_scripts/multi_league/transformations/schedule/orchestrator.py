#!/usr/bin/env python3
"""
Schedule Table Orchestrator

Runs all transformations that enrich the schedule.parquet file.
Called by initial_import_v2.py or can be run standalone for debugging.

Note: schedule playoff flags are applied via SQL enrichments in MotherDuck.
      No Python scripts are currently registered here.

Usage:
    python orchestrator.py --context /path/to/league_context.json
"""

import argparse
import sys
from pathlib import Path

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

from multi_league.core.script_runner import run_script, log  # noqa: E402
from multi_league.core.league_context import LeagueContext  # noqa: E402

# Schedule transformation scripts
# (enrich_schedule_with_playoff_flags replaced by SQL enrichments in MotherDuck)
SCHEDULE_SCRIPTS: list = []


def run_schedule_enrichment(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run all schedule table transformations.

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[SCHEDULE] Starting schedule transformations...")

    all_success = True
    for script, description, timeout in SCHEDULE_SCRIPTS:
        ok, err = run_script(script, description, context_path, timeout=timeout)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Schedule enrichment complete")
    return all_success


def main():
    parser = argparse.ArgumentParser(description="Schedule Table Orchestrator")
    parser.add_argument("--context", type=Path, required=True, help="Path to league_context.json")
    args = parser.parse_args()

    ctx = LeagueContext.load(args.context)
    context_path = str(args.context)

    success = run_schedule_enrichment(ctx, context_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
