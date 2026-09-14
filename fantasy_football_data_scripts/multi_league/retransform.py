"""Targeted retransform — re-run specific SQL enrichments + dependents on one league.

Reads the enrichment dependency graph and cascades: if you re-run enrichment A,
all enrichments that depend on A's output also re-run, in topological order.

Usage:
    # Re-run a single enrichment + its dependents
    python -m multi_league.retransform --db kmffl --enrichment matchup_to_player

    # Re-run all enrichments (equivalent to full retransform)
    python -m multi_league.retransform --db kmffl --enrichment all

    # Re-run only aggregation enrichments
    python -m multi_league.retransform --db kmffl --enrichment retransform_agg

    # Dry-run (print SQL, don't execute)
    python -m multi_league.retransform --db kmffl --enrichment fix_margin --dry-run

    # List enrichments and their dependents
    python -m multi_league.retransform --list

    # Show what would run for a given enrichment (without executing)
    python -m multi_league.retransform --db kmffl --enrichment fix_def_fantasy_points --plan
"""

import argparse
import logging
import sys
import threading
from collections import defaultdict, deque

# Maximum wall-clock time for a retransform run (default: 30 minutes).
# Prevents stuck processes from writing to MotherDuck indefinitely.
MAX_RUNTIME_SECONDS = 30 * 60

logger = logging.getLogger(__name__)

# =============================================================================
# ENRICHMENT DEPENDENCY GRAPH
# =============================================================================
# Maps each enrichment to the enrichments it DEPENDS ON (prerequisites).
# An enrichment with no entry (or empty list) has no prerequisites within the
# enrichment phase — it may still depend on data existing in the tables.
#
# This is the CANONICAL ordering from SQLEnrichments.run_all().
# When adding a new enrichment, add it here and in ENRICHMENT_ORDER.

ENRICHMENT_ORDER = [
    "resolve_all_nfl_player_ids",
    "detect_league_format",
    "backfill_and_normalize_positions",
    "ensure_lineup_position",
    "apply_player_bio_positions",
    "fix_zero_point_starters",
    "backfill_fantasy_points_from_super_table",
    "fix_def_fantasy_points",
    "fix_idp_fantasy_points",
    "apply_bonus_scoring",
    "apply_te_premium",
    "fix_idp_flex_positions",
    "dedup_player_fantasy",
    "ensure_manager_week",
    "populate_franchise_id",
    "populate_franchise_name",
    "normalize_manager_case",
    "sync_schedule_managers",
    "fix_margin",
    "enforce_postseason_flags",
    "shape_playoff_bracket_local",
    "shape_consolation_bracket_local",
    "compute_derived_matchup_columns",
    "matchup_to_player",
    "draft_to_player",
    "populate_position_rank",
    "calculate_lamar_for_all",
    "player_to_draft",
    "draft_manager_aggregates",
    "draft_cost_buckets",
    "draft_value_zscore",
    "draft_age_zscore",
    "draft_bench_insurance",
    "draft_starter_designation",
    "draft_failure_rates",
    "draft_bench_value_by_rank",
    "transactions_to_player",
    "player_to_transactions",
    "transaction_lamar_ros",
    "transaction_score",
    "transaction_engagement_metrics",
    # Optimal lineup wave: must run AFTER calculate_lamar_for_all (it reads
    # rank columns) and BEFORE player_to_matchup (which rolls up the
    # optimal_player flag into matchup.optimal_points / starter_points).
    "league_wide_optimal_for_all",
    "compute_manager_optimal",
    "player_to_matchup",
]

# Direct dependencies: enrichment → set of enrichments it requires.
# Enrichments not listed here have no intra-enrichment prerequisites.
DEPENDS_ON = {
    "backfill_fantasy_points_from_super_table": {"resolve_all_nfl_player_ids"},
    "fix_def_fantasy_points": {"resolve_all_nfl_player_ids", "backfill_fantasy_points_from_super_table"},
    "fix_idp_fantasy_points": {"resolve_all_nfl_player_ids", "fix_def_fantasy_points"},
    "apply_bonus_scoring": {"resolve_all_nfl_player_ids", "fix_idp_fantasy_points"},
    "apply_te_premium": {"resolve_all_nfl_player_ids", "apply_bonus_scoring"},
    "apply_player_bio_positions": {"resolve_all_nfl_player_ids"},
    "fix_idp_flex_positions": {"resolve_all_nfl_player_ids"},
    "dedup_player_fantasy": {"resolve_all_nfl_player_ids"},
    "populate_franchise_name": {"populate_franchise_id"},
    "normalize_manager_case": {"populate_franchise_id"},
    "sync_schedule_managers": {"normalize_manager_case"},
    "shape_playoff_bracket_local": {"enforce_postseason_flags"},
    "shape_consolation_bracket_local": {"shape_playoff_bracket_local"},
    "compute_derived_matchup_columns": {"fix_margin"},
    "matchup_to_player": {"populate_franchise_id", "ensure_manager_week", "compute_derived_matchup_columns"},
    "draft_to_player": {"resolve_all_nfl_player_ids"},
    "player_to_matchup": {"matchup_to_player", "ensure_manager_week", "compute_manager_optimal"},
    "compute_manager_optimal": {"resolve_all_nfl_player_ids", "calculate_lamar_for_all", "matchup_to_player"},
    "populate_position_rank": {"resolve_all_nfl_player_ids"},
    "calculate_lamar_for_all": {"resolve_all_nfl_player_ids", "dedup_player_fantasy", "apply_te_premium"},
    "player_to_draft": {"calculate_lamar_for_all", "draft_to_player"},
    "draft_manager_aggregates": {"player_to_draft"},
    "draft_cost_buckets": {"player_to_draft"},
    "draft_value_zscore": {"draft_cost_buckets", "draft_manager_aggregates"},
    "draft_age_zscore": {"player_to_draft"},
    "draft_bench_insurance": {"player_to_draft"},
    "draft_starter_designation": {"player_to_draft"},
    "draft_failure_rates": {"player_to_draft"},
    "draft_bench_value_by_rank": {"player_to_draft"},
    "transactions_to_player": {"resolve_all_nfl_player_ids"},
    "player_to_transactions": {"transactions_to_player"},
    "transaction_lamar_ros": {"calculate_lamar_for_all", "transactions_to_player"},
    "transaction_score": {"transaction_lamar_ros"},
    "transaction_engagement_metrics": {"transaction_score"},
    "league_wide_optimal_for_all": {"resolve_all_nfl_player_ids", "calculate_lamar_for_all"},
}

# Aggregation-only enrichments (for retransform_agg fix_action)
AGG_ENRICHMENTS = {
    "draft_manager_aggregates",
    "draft_cost_buckets",
    "draft_value_zscore",
    "draft_age_zscore",
    "draft_bench_insurance",
    "draft_starter_designation",
    "draft_failure_rates",
    "draft_bench_value_by_rank",
    "transaction_score",
    "transaction_engagement_metrics",
}


# =============================================================================
# DAG UTILITIES
# =============================================================================


def _build_reverse_graph() -> dict[str, set[str]]:
    """Build reverse dependency map: enrichment → set of enrichments that depend on it."""
    reverse = defaultdict(set)
    for child, parents in DEPENDS_ON.items():
        for parent in parents:
            reverse[parent].add(child)
    return dict(reverse)


def get_dependents(enrichment: str) -> list[str]:
    """Get all transitive dependents of an enrichment, in topological order.

    Returns the enrichment itself plus all downstream enrichments that
    would need to re-run if this enrichment's output changes.
    """
    reverse = _build_reverse_graph()
    visited = set()
    queue = deque([enrichment])

    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        for child in reverse.get(node, []):
            if child not in visited:
                queue.append(child)

    # Return in canonical execution order
    return [e for e in ENRICHMENT_ORDER if e in visited]


def get_cascade(enrichments: list[str]) -> list[str]:
    """Get the full cascade for multiple enrichments."""
    all_needed = set()
    for e in enrichments:
        all_needed.update(get_dependents(e))
    return [e for e in ENRICHMENT_ORDER if e in all_needed]


def get_prerequisites(enrichment: str) -> list[str]:
    """Get all transitive prerequisites of an enrichment, in topological order."""
    visited = set()
    queue = deque([enrichment])

    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        for parent in DEPENDS_ON.get(node, set()):
            if parent not in visited:
                queue.append(parent)

    return [e for e in ENRICHMENT_ORDER if e in visited]


# =============================================================================
# RETRANSFORM EXECUTOR
# =============================================================================


def retransform(
    db_name: str,
    enrichment: str,
    *,
    dry_run: bool = False,
    cascade: bool = True,
    include_prerequisites: bool = False,
) -> dict[str, int | tuple]:
    """Re-run an enrichment (and optionally its dependents) on a single league.

    Args:
        db_name: MotherDuck database name
        enrichment: Enrichment name to re-run, or "all" / "retransform_agg"
        dry_run: Print SQL without executing
        cascade: If True, also re-run all downstream dependents
        include_prerequisites: If True, also re-run all upstream prerequisites

    Returns:
        Dict mapping enrichment name → rows affected (or error tuple)
    """
    from multi_league.transformations.sql_enrichments import SQLEnrichments

    # Determine which enrichments to run
    if enrichment == "all":
        to_run = list(ENRICHMENT_ORDER)
    elif enrichment == "retransform_agg":
        to_run = [e for e in ENRICHMENT_ORDER if e in AGG_ENRICHMENTS]
    else:
        if enrichment not in set(ENRICHMENT_ORDER):
            raise ValueError(f"Unknown enrichment: {enrichment}. Valid: {', '.join(ENRICHMENT_ORDER)}")
        if include_prerequisites:
            to_run = get_prerequisites(enrichment)
        elif cascade:
            to_run = get_dependents(enrichment)
        else:
            to_run = [enrichment]

    # Everything NOT in to_run gets skipped
    skip = [e for e in ENRICHMENT_ORDER if e not in set(to_run)]

    print(f"[retransform] db={db_name}, target={enrichment}")
    print(f"[retransform] Running {len(to_run)} enrichments, skipping {len(skip)}")
    for e in to_run:
        print(f"  -> {e}")

    with SQLEnrichments(db_name, dry_run=dry_run) as enricher:
        # Load settings so scoring params are applied
        roster_by_year, scoring_params = enricher.load_settings_from_db()
        if roster_by_year:
            enricher.roster_by_year = roster_by_year
            enricher._update_scoring_params(scoring_params)
            ppr = scoring_params.get("ppr", 0.0)
            td = scoring_params.get("pass_td_pts", 4)
            print(f"[settings] {len(roster_by_year)} years, {ppr} PPR, {td}pt TD")

        results = enricher.run_all(skip=skip)

    # Filter to only the enrichments we ran
    return {k: v for k, v in results.items() if k in set(to_run)}


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="Targeted retransform — re-run SQL enrichments on a single league")
    parser.add_argument("--db", help="MotherDuck database name")
    parser.add_argument(
        "--enrichment",
        help="Enrichment to re-run (cascades to dependents). "
        "Use 'all' for full retransform, 'retransform_agg' for aggregation only.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument("--no-cascade", action="store_true", help="Only run the named enrichment, skip dependents")
    parser.add_argument(
        "--with-prerequisites",
        action="store_true",
        help="Also re-run all upstream prerequisites",
    )
    parser.add_argument("--list", action="store_true", help="List all enrichments and dependents")
    parser.add_argument("--plan", action="store_true", help="Show what would run without executing")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.list:
        reverse = _build_reverse_graph()
        print(f"{'Enrichment':<45} {'Direct Dependents'}")
        print("=" * 80)
        for e in ENRICHMENT_ORDER:
            deps = sorted(reverse.get(e, set()))
            dep_str = ", ".join(deps) if deps else "(none — leaf)"
            print(f"  {e:<43} {dep_str}")
        print(f"\nTotal: {len(ENRICHMENT_ORDER)} enrichments")
        print(f"Aggregation-only: {len(AGG_ENRICHMENTS)} ({', '.join(sorted(AGG_ENRICHMENTS))})")
        return

    if not args.db or not args.enrichment:
        parser.error("--db and --enrichment are required (unless using --list)")

    if args.plan:
        if args.enrichment == "all":
            to_run = list(ENRICHMENT_ORDER)
        elif args.enrichment == "retransform_agg":
            to_run = [e for e in ENRICHMENT_ORDER if e in AGG_ENRICHMENTS]
        elif args.with_prerequisites:
            to_run = get_prerequisites(args.enrichment)
        elif args.no_cascade:
            to_run = [args.enrichment]
        else:
            to_run = get_dependents(args.enrichment)

        print(f"[PLAN] Would run {len(to_run)} enrichments on {args.db}:")
        for i, e in enumerate(to_run, 1):
            deps = DEPENDS_ON.get(e, set())
            dep_str = f" (needs: {', '.join(sorted(deps))})" if deps else ""
            print(f"  {i:>2}. {e}{dep_str}")
        return

    # Watchdog: kill this process if it exceeds MAX_RUNTIME_SECONDS.
    # Prevents stuck retransforms from writing to MotherDuck indefinitely
    # (a stuck process ran for 3 days once, corrupting data across leagues).
    def _watchdog():
        import os

        mins = MAX_RUNTIME_SECONDS // 60
        print(f"\n[WATCHDOG] TIMEOUT — retransform exceeded {mins} minute limit. Terminating.")
        os._exit(2)

    timer = threading.Timer(MAX_RUNTIME_SECONDS, _watchdog)
    timer.daemon = True
    timer.start()

    try:
        results = retransform(
            db_name=args.db,
            enrichment=args.enrichment,
            dry_run=args.dry_run,
            cascade=not args.no_cascade,
            include_prerequisites=args.with_prerequisites,
        )
    finally:
        timer.cancel()

    print("\n" + "=" * 60)
    print("RETRANSFORM RESULTS")
    print("=" * 60)
    errors = 0
    for name, count in results.items():
        if isinstance(count, tuple):
            print(f"  {name:40s}: ERROR — {count[1][:60]}")
            errors += 1
        elif count == -1:
            print(f"  {name:40s}: SKIPPED")
        else:
            print(f"  {name:40s}: {count:,} rows")

    if errors:
        print(f"\n[WARNING] {errors} enrichment(s) failed")
        sys.exit(1)
    else:
        print("\n[SUCCESS] Retransform complete")


if __name__ == "__main__":
    main()
