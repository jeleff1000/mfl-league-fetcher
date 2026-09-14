#!/usr/bin/env python3
"""
Draft Table Orchestrator

Runs all transformations that enrich the draft.parquet file.
Called by initial_import_v2.py or can be run standalone for debugging.

Transformation Order:
All draft enrichments are now calculated by sql_draft_enrichments.py after MotherDuck upload.
- player_to_draft_v2.py - DEPRECATED (replaced by sql_draft_enrichments.player_to_draft())
- draft_value_metrics_v3.py - DEPRECATED (replaced by sql_draft_enrichments z-score methods)

Note: keeper_economics is now a SQL enrichment on player_fantasy
      (SQLEnrichments.populate_keeper_economics, Wave 3 in run_all()).

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

from multi_league.core.script_runner import run_script, log
from multi_league.core.league_context import LeagueContext

# Draft transformation scripts in dependency order
# Early Pass 3: Before draft -> player
EARLY_SCRIPTS = [
    # DEPRECATED: player_to_draft_v2.py - replaced by sql_draft_enrichments.player_to_draft()
    # All draft enrichments now run in SQL after MotherDuck upload.
]

# Late Pass 3: No longer used - keeper economics moved to player orchestrator
LATE_SCRIPTS = []


def run_draft_early(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run early draft transformations (LAMAR calculations).

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[DRAFT] Starting early transformations (player stats + LAMAR)...")

    all_success = True
    for script, description, timeout in EARLY_SCRIPTS:
        ok, err = run_script(script, description, context_path, timeout=timeout)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Draft early transformations complete")
    return all_success


def run_draft_late(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run late draft transformations (keeper economics).

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[DRAFT] Starting late transformations (keeper economics)...")

    all_success = True
    for script, description, timeout in LATE_SCRIPTS:
        ok, err = run_script(script, description, context_path, timeout=timeout)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Draft late transformations complete")
    return all_success


def run_all_draft_transformations(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run all draft transformations (for standalone debugging).

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[DRAFT] Running ALL draft transformations...")

    success = run_draft_early(ctx, context_path)
    if not success:
        log("[WARN] Early transformations had failures, continuing...")

    success2 = run_draft_late(ctx, context_path)

    return success and success2


def main():
    parser = argparse.ArgumentParser(description="Draft Table Orchestrator")
    parser.add_argument("--context", type=Path, required=True, help="Path to league_context.json")
    parser.add_argument("--stage", choices=["early", "late"], help="Run only specific stage")
    args = parser.parse_args()

    ctx = LeagueContext.load(args.context)
    context_path = str(args.context)

    if args.stage == "early":
        success = run_draft_early(ctx, context_path)
    elif args.stage == "late":
        success = run_draft_late(ctx, context_path)
    else:
        success = run_all_draft_transformations(ctx, context_path)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
