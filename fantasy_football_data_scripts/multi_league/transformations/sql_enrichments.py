"""
SQL Enrichment Pipeline — Local-First Execution, Centralized-DB Compatible

All enrichments execute locally by default. The restored ___ops cache is
ATTACH'd read-only (player_bio + super_table), while the shared
___leagues path is handled through db_name-aware scoping in the base class.

6 waves, ~50 enrichments, executed in dependency order:
  Wave 1: Foundation (IDs, positions, scoring, expand to all NFL)
  Wave 2: Manager identity + matchup structure
  Wave 3: Matchup derived columns + cross-table joins
  Wave 4: LAMAR + optimal lineup
  Wave 5: Draft analytics (needs LAMAR)
  Wave 6: Transaction analytics (needs LAMAR)

Usage:
    eng = SQLEnrichments(db_name="kmffl", data_dir="/tmp/kmffl")
    eng.load_settings_from_db()
    eng.run_all()
"""

import logging
import re
import time

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.player.sql_player_enrichments import PlayerEnrichmentsMixin
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin
from multi_league.transformations.draft.sql_draft_enrichments import DraftEnrichmentsMixin
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin
from multi_league.transformations.aggregation.sql_aggregation_enrichments import AggregationEnrichmentsMixin

logger = logging.getLogger(__name__)


class SQLEnrichments(
    PlayerEnrichmentsMixin,
    MatchupEnrichmentsMixin,
    DraftEnrichmentsMixin,
    TransactionEnrichmentsMixin,
    AggregationEnrichmentsMixin,
    SQLEnrichmentsBase,
):
    """SQL-based enrichments that run directly against the active DuckDB target.

    Composes domain-specific mixins via multiple inheritance.
    All operations use UPDATE ... FROM ... WHERE syntax to modify tables in
    place without downloading data to Python. Centralized Fly runs are
    db_name-scoped by the base class.

    NOTE: DuckDB UPDATE syntax requires unqualified column names in SET clause:
    - WRONG:  UPDATE t SET t.col = s.col FROM s WHERE t.id = s.id
    - RIGHT:  UPDATE t SET col = s.col FROM s WHERE t.id = s.id
    """

    # =========================================================================
    # RUN ALL ENRICHMENTS
    # =========================================================================

    @staticmethod
    def _format_result(result) -> str:
        """Render enrichment result values for logs without changing return shape."""
        if result == -1:
            return "skipped"
        if isinstance(result, tuple):
            return f"error: {result[1]}"
        if isinstance(result, int):
            return f"{result:,} rows"
        return str(result)

    def run_all(self, skip: list[str] | None = None) -> dict[str, int]:
        """Run all SQL enrichments in the correct order.

        Args:
            skip: List of enrichment names to skip (e.g., ['transactions_to_player'])

        Returns:
            Dict mapping enrichment name to rows affected
        """
        skip = skip or []
        results = {}
        self.last_run_timings: dict[str, float] = {}

        # Critical enrichments: if one fails, downstream dependents are skipped.
        # Maps enrichment name -> set of enrichments that depend on it.
        _critical_deps = {
            "resolve_all_nfl_player_ids": {
                "expand_to_all_nfl",
                "populate_fantasy_points",
                "matchup_to_player",
                "draft_to_player",
                "player_to_matchup",
                "populate_position_rank",
                "calculate_lamar_for_all",
                "player_to_draft",
                "league_wide_optimal_for_all",
            },
        }
        # Collect names of failed critical enrichments, then build the full
        # set of enrichments to skip due to upstream failures.
        _failed_critical: set[str] = set()
        _skip_due_to_failure: set[str] = set()

        # =====================================================================
        # WAVE 1: Foundation — IDs, positions, join keys, scoring
        # All independent of each other within wave (except serial deps noted)
        # =====================================================================
        enrichments = [
            ("resolve_all_nfl_player_ids", self.resolve_all_nfl_player_ids),
            ("ensure_player_week", self.ensure_player_week),
            ("dedup_player_fantasy", self.dedup_player_fantasy),
            ("detect_league_format", self.detect_league_format),
            ("backfill_and_normalize_positions", self.backfill_and_normalize_positions),
            ("ensure_lineup_position", self.ensure_lineup_position),
            # player_bio canonical overrides must run AFTER NFL_player_id
            # resolution (needs the ID join) and BEFORE fix_idp_flex_positions
            # + downstream consumers that read p.position for scoring /
            # eligibility / optimal-lineup logic.
            ("apply_player_bio_positions", self.apply_player_bio_positions),
            # Closes the FB->RB transition that apply_player_bio_positions
            # explicitly skips (player_bio is unreliable for fullbacks). Uses
            # super_table as the source instead. Fleet-wide fix for Sleeper
            # depth-chart 'FB' leaking onto player_fantasy.position.
            ("apply_super_table_skill_positions", self.apply_super_table_skill_positions),
            ("fix_idp_flex_positions", self.fix_idp_flex_positions),
            ("expand_to_all_nfl", self.expand_to_all_nfl),
            ("dedup_player_fantasy_publish_identity_post_expand", self.dedup_player_fantasy_publish_identity),
            ("populate_fantasy_points", self.populate_fantasy_points),
            # backfill_fantasy_position runs after populate_fantasy_points so it
            # sees the recomputed points — but its logic only reads position, so
            # order vs populate_fantasy_points doesn't technically matter. It
            # MUST run before fix_zero_point_starters (which sets 'BN' on rows
            # whose points come out to 0 and could otherwise leave stale NULLs).
            ("backfill_fantasy_position_from_position", self.backfill_fantasy_position_from_position),
            # fix_zero_point_starters was previously ordered before
            # populate_fantasy_points. That was wrong: it reads fantasy_points
            # to decide who to un-start, but the recompute happens in the next
            # step, so it was making classifications based on stale platform
            # values (especially bad for ESPN pre-2019 where stored points
            # were often 0). Now runs after populate_fantasy_points on the
            # canonical-recomputed values.
            ("fix_zero_point_starters", self.fix_zero_point_starters),
            # =====================================================================
            # WAVE 2: Manager identity + matchup structure
            # =====================================================================
            ("resolve_hidden_managers", self.resolve_hidden_managers),
            ("populate_franchise_id", self.populate_franchise_id),
            ("repair_matchup_symmetry", self.repair_matchup_symmetry),
            ("ensure_missing_player_stubs", self.ensure_missing_player_stubs),
            ("ensure_manager_week", self.ensure_manager_week),
            ("dedup_matchup_publish_identity_post_symmetry", self.dedup_matchup_publish_identity),
            ("fix_margin", self.fix_margin),
            # Must run BEFORE compute_win_loss_and_projections so the
            # projection cascade (proj_wins, proj_score_error, etc.) fires.
            ("populate_team_projected_points", self.populate_team_projected_points),
            ("compute_win_loss_and_projections", self.compute_win_loss_and_projections),
            ("enforce_postseason_flags", self.enforce_postseason_flags),
            ("dedup_matchup_publish_identity_post_postseason", self.dedup_matchup_publish_identity),
            ("normalize_matchup_flags", self.normalize_matchup_flags),
            ("cumulative_records_pre_bracket", self.cumulative_records),
            ("shape_playoff_bracket_local", self.shape_playoff_bracket_local),
            ("shape_consolation_bracket_local", self.shape_consolation_bracket_local),
            ("propagate_2week_round_flags", self.propagate_2week_round_flags),
            # =====================================================================
            # WAVE 3: Matchup derived columns + cross-table joins
            # =====================================================================
            ("compute_derived_matchup_columns", self.compute_derived_matchup_columns),
            ("backfill_team_name", self.backfill_team_name),
            ("compute_season_result", self.compute_season_result),
            ("compute_league_weekly_stats", self.compute_league_weekly_stats),
            ("cumulative_records", self.cumulative_records),
            ("manager_season_ppg", self.manager_season_ppg),
            ("matchup_rankings", self.matchup_rankings),
            ("all_time_manager_stats", self.all_time_manager_stats),
            ("inflation_rate", self.inflation_rate),
            ("build_schedule_from_matchup", self.build_schedule_from_matchup),
            ("build_all_play", self.build_all_play),
            ("build_schedule_swap", self.build_schedule_swap),
            ("build_h2h_season", self.build_h2h_season),
            ("build_schedule_swap_season", self.build_schedule_swap_season),
            ("matchup_to_player", self.matchup_to_player),
            ("draft_to_player", self.draft_to_player),
            ("transactions_to_player", self.transactions_to_player),
            ("populate_keeper_economics", self.populate_keeper_economics),
            ("populate_max_keepers", self.populate_max_keepers),
            # Sync centralized keeper rules from Fly, then override base_cost +
            # write keeper_price if the league owner has configured rules.
            # Sync MUST run before apply so apply can read the local table.
            ("_sync_keeper_config_from_fly", self._sync_keeper_config_from_fly),
            ("apply_keeper_rules", self.apply_keeper_rules),
            # Late rows/repairs can change player_week after the first pass;
            # analytics and delta publish both require one row per player_week.
            ("ensure_player_week_pre_analytics", self.ensure_player_week),
            ("dedup_player_fantasy_publish_identity_pre_analytics", self.dedup_player_fantasy_publish_identity),
            # =====================================================================
            # WAVE 4: LAMAR + optimal lineup (needs fantasy_points + positions)
            # =====================================================================
            ("populate_position_rank", lambda: self.populate_position_rank(self.roster_by_year)),
            ("calculate_lamar_for_all", lambda: self.calculate_lamar_for_all_players(self.roster_by_year)),
            ("league_wide_optimal_for_all", lambda: self.league_wide_optimal_for_all(self.roster_by_year)),
            ("compute_manager_optimal", lambda: self.compute_manager_optimal(self.roster_by_year)),
            ("player_to_matchup", self.player_to_matchup),
            # =====================================================================
            # WAVE 5: Draft analytics (needs LAMAR from player_fantasy)
            # =====================================================================
            ("backfill_draft_positions", self.backfill_draft_positions),
            ("backfill_draft_managers", self.backfill_draft_managers),
            ("player_to_draft", self.player_to_draft),
            ("draft_manager_aggregates", self.draft_manager_aggregates),
            ("draft_cost_buckets", self.draft_cost_buckets),
            ("draft_value_zscore", self.draft_value_zscore),
            ("draft_age_zscore", self.draft_age_zscore),
            ("draft_bench_insurance", self.draft_bench_insurance),
            ("draft_starter_designation", self.draft_starter_designation),
            ("draft_failure_rates", self.draft_failure_rates),
            ("draft_bench_value_by_rank", self.draft_bench_value_by_rank),
            # =====================================================================
            # WAVE 6: Transaction analytics (needs LAMAR)
            # =====================================================================
            ("fix_unknown_managers", self.fix_unknown_managers),
            ("draft_pick_conveyances", self.draft_pick_conveyances),
            ("player_to_transactions", self.player_to_transactions),
            ("transaction_lamar_ros", self.transaction_lamar_ros),
            ("transaction_score", self.transaction_score),
            ("transaction_engagement_metrics", self.transaction_engagement_metrics),
            # NOTE: clutch_equity requires p_champ from playoff sims — called separately
        ]

        total_enrichments = len(enrichments)
        run_started_at = time.perf_counter()

        for index, (name, func) in enumerate(enrichments, start=1):
            if name in skip:
                logger.info(f"[ENRICHMENT {index}/{total_enrichments}] SKIP {name} (in skip list)")
                results[name] = -1
                continue

            if name in _skip_due_to_failure:
                logger.warning(
                    f"[ENRICHMENT {index}/{total_enrichments}] SKIP {name} (upstream critical dependency failed)"
                )
                results[name] = ("error", "skipped: upstream critical dependency failed")
                continue

            step_started_at = time.perf_counter()
            logger.info(f"[ENRICHMENT {index}/{total_enrichments}] START {name}")

            try:
                results[name] = func()
                elapsed = time.perf_counter() - step_started_at
                self.last_run_timings[name] = elapsed
                logger.info(
                    f"[ENRICHMENT {index}/{total_enrichments}] OK {name} "
                    f"in {elapsed:.2f}s ({self._format_result(results[name])})"
                )
            except Exception as e:
                elapsed = time.perf_counter() - step_started_at
                self.last_run_timings[name] = elapsed
                logger.error(f"[SQL ERROR] {name}: {e}")
                import traceback

                logger.error(traceback.format_exc())
                logger.error(f"[ENRICHMENT {index}/{total_enrichments}] FAIL {name} in {elapsed:.2f}s")
                # Return error tuple so callers can distinguish errors from "no row count"
                results[name] = ("error", str(e))
                # If this was a critical enrichment, mark its dependents for skipping
                if name in _critical_deps:
                    _failed_critical.add(name)
                    _skip_due_to_failure.update(_critical_deps[name])
                    logger.warning(
                        f"[CRITICAL] {name} failed — will skip {len(_critical_deps[name])} dependent enrichments"
                    )

        total_elapsed = time.perf_counter() - run_started_at
        logger.info(f"[SQL ENRICHMENTS] Completed {total_enrichments} enrichments in {total_elapsed:.2f}s")

        slowest = sorted(self.last_run_timings.items(), key=lambda item: item[1], reverse=True)[:10]
        if slowest:
            logger.info("[SQL ENRICHMENTS] Slowest enrichments:")
            for name, elapsed in slowest:
                logger.info(f"  - {name}: {elapsed:.2f}s")

        return results

    def close(self):
        """Close the database connection (only if we created it)."""
        if self._conn and self._owns_conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# =========================================================================
# CLI Interface
# =========================================================================


def main():
    """Command-line interface for running SQL enrichments."""
    import argparse

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

    from core.league_context import LeagueContext

    parser = argparse.ArgumentParser(description="Run SQL-based enrichments against the active DuckDB target")
    parser.add_argument("--context", help="Path to league_context.json")
    parser.add_argument("--db", help="League database name (alternative to --context)")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL queries without executing")
    parser.add_argument(
        "--enrichment",
        choices=[
            "detect_league_format",
            "ensure_manager_week",
            "populate_franchise_id",
            "ensure_missing_player_stubs",
            "shape_consolation_bracket_local",
            "matchup_to_player",
            "draft_to_player",
            "player_to_matchup",
            "player_to_draft",
            "transactions_to_player",
            "player_to_transactions",
            "league_wide_optimal_for_all",
            "all",
        ],
        default="all",
        help="Which enrichment to run (default: all)",
    )
    parser.add_argument("--skip", nargs="+", default=[], help="Enrichments to skip when running 'all'")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Get database name from --db or --context
    if args.db:
        db_name = args.db
        print(f"[Database] {db_name} (from --db)")
    elif args.context:
        # Load context
        ctx = LeagueContext.load(args.context)
        print(f"[League] {ctx.league_name} ({ctx.league_id})")

        # Derive database name - sanitize league name to valid DB identifier
        db_name = re.sub(r"[^a-zA-Z0-9]+", "_", (ctx.league_name or "").strip().lower()).strip("_")
        if not db_name:
            db_name = "l"
        if db_name[0].isdigit():
            db_name = "l_" + db_name
        db_name = db_name[:63]
        print(f"[Database] {db_name}")
    else:
        parser.error("Either --context or --db is required")

    # Run enrichments
    with SQLEnrichments(db_name, dry_run=args.dry_run) as enricher:
        # Always load settings so DEF/IDP/kicker/bonus multipliers are applied
        roster_by_year, scoring_params = enricher.load_settings_from_db()
        if roster_by_year:
            enricher.roster_by_year = roster_by_year
            enricher._update_scoring_params(scoring_params)
            print(
                f"[Settings] {len(roster_by_year)} years, {scoring_params.get('ppr', 0.0)} PPR, {scoring_params.get('pass_td_pts', 4)}pt TD"
            )

        if args.enrichment == "all":
            results = enricher.run_all(skip=args.skip)
            print("\n" + "=" * 60)
            print("SQL ENRICHMENTS SUMMARY")
            print("=" * 60)
            for name, count in results.items():
                duration = getattr(enricher, "last_run_timings", {}).get(name)
                if count == -1:
                    status = "SKIPPED"
                elif isinstance(count, tuple):
                    status = f"ERROR: {count[1][:60]}"
                else:
                    status = f"{count:,} rows"
                if duration is not None:
                    status = f"{status} | {duration:.2f}s"
                print(f"  {name:30s}: {status}")
        else:
            func = getattr(enricher, args.enrichment)
            count = func()
            print(f"\n[RESULT] {args.enrichment}: {count:,} rows affected")

    print("\n[SUCCESS] SQL enrichments complete")


if __name__ == "__main__":
    main()
