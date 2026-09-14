#!/usr/bin/env python3
"""
Matchup Table Orchestrator

Runs all transformations that enrich the matchup.parquet file.
Called by initial_import_v2.py or can be run standalone for debugging.

Transformation Order:
1. resolve_hidden_managers.py - Unify --hidden-- manager names by GUID
2. sql_matchup_enrichments.py - Core matchup enrichment (playoff flags, records, rankings)
   (replaces cumulative_stats.py — now 5 SQL enrichments in MotherDuck)
3. expected_record_v2.py - Calculate expected records from schedule simulations
4. playoff_odds_import.py - Monte Carlo playoff odds simulation

Note: player_to_matchup_v2.py is DEPRECATED - replaced by sql_enrichments.player_to_matchup()
which runs in pure SQL after MotherDuck upload (no OOM risk).

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

# Matchup transformation scripts in dependency order
# Scripts in Pass 1: Base calculations (no dependencies on other tables)
PASS_1_SCRIPTS = [
    ("multi_league/transformations/matchup/resolve_hidden_managers.py", "Resolve Hidden Managers", 120),
    # DEPRECATED: cumulative_stats.py -> replaced by 5 SQL enrichments in sql_matchup_enrichments.py
]

# Scripts in Pass 3: After player table is enriched (need player data)
PASS_3_SCRIPTS = [
    # DEPRECATED: player_to_matchup_v2.py - replaced by sql_enrichments.player_to_matchup()
    # ("multi_league/transformations/matchup/player_to_matchup_v2.py", "Player -> Matchup", 600),
    ("multi_league/transformations/matchup/expected_record_v2.py", "Expected Record", 900),
    ("multi_league/transformations/matchup/playoff_odds_import.py", "Playoff Odds", 1800),
]


def run_matchup_pass_1(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run Pass 1 matchup transformations (no dependencies on other tables).

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[MATCHUP] Starting Pass 1 transformations (base calculations)...")

    all_success = True
    for script, description, timeout in PASS_1_SCRIPTS:
        ok, err = run_script(script, description, context_path, timeout=timeout)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Matchup Pass 1 complete")
    return all_success


def run_matchup_pass_3(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run Pass 3 matchup transformations (after player enrichment).

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[MATCHUP] Starting Pass 3 transformations (player aggregates + simulations)...")

    all_success = True
    for script, description, timeout in PASS_3_SCRIPTS:
        ok, err = run_script(script, description, context_path, timeout=timeout)
        if not ok:
            log(f"[FAIL] {description} failed")
            all_success = False

    if all_success:
        log("[OK] Matchup Pass 3 complete")
    return all_success


def run_all_matchup_transformations(ctx: LeagueContext, context_path: str) -> bool:
    """
    Run all matchup transformations (for standalone debugging).

    Note: In normal pipeline flow, Pass 1 and Pass 3 are run separately
    with player enrichment in between.

    Args:
        ctx: LeagueContext instance
        context_path: Path to league_context.json

    Returns:
        True if all transformations succeeded, False if any failed
    """
    log("[MATCHUP] Running ALL matchup transformations...")

    success = run_matchup_pass_1(ctx, context_path)
    if not success:
        log("[WARN] Pass 1 had failures, continuing to Pass 3...")

    success2 = run_matchup_pass_3(ctx, context_path)

    return success and success2


def main():
    parser = argparse.ArgumentParser(description="Matchup Table Orchestrator")
    parser.add_argument("--context", type=Path, required=True, help="Path to league_context.json")
    parser.add_argument("--pass", dest="pass_num", type=int, choices=[1, 3], help="Run only specific pass (1 or 3)")
    args = parser.parse_args()

    ctx = LeagueContext.load(args.context)
    context_path = str(args.context)

    if args.pass_num == 1:
        success = run_matchup_pass_1(ctx, context_path)
    elif args.pass_num == 3:
        success = run_matchup_pass_3(ctx, context_path)
    else:
        success = run_all_matchup_transformations(ctx, context_path)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
