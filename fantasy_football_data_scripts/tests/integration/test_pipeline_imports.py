"""
Integration test: verify all pipeline modules import successfully.

This catches the most common failure mode — broken imports due to
sys.path changes, missing dependencies, or circular imports.
Runs in CI without any external services (no MotherDuck, no APIs).
"""

import importlib
import pytest


# Every module that gets invoked by the import pipeline or GH Actions workflows.
# If any of these fail to import, the pipeline will crash at runtime.
PIPELINE_MODULES = [
    # Core infrastructure
    "multi_league.core.data_normalization",
    "multi_league.core.data_validation",
    "multi_league.core.import_config",
    "multi_league.core.import_pipeline",
    "multi_league.core.import_utils",
    "multi_league.core.league_context",
    "multi_league.core.league_discovery",
    "multi_league.core.logging_config",
    "multi_league.core.db_utils",
    "multi_league.core.script_runner",
    "multi_league.core.yahoo_league_settings",
    # Shared utilities
    "multi_league.utils.credential_store",
    "multi_league.shared.filters",
    "multi_league.shared.import_setup",
    # Data fetchers
    "multi_league.data_fetchers.orchestrator",
    "multi_league.data_fetchers.load_nfl_from_super_table",
    "multi_league.data_fetchers.defense_stats",
    "multi_league.data_fetchers.nfl_offense_stats",
    "multi_league.data_fetchers.create_league_table",
    "multi_league.data_fetchers.combine_dst_to_nfl",
    "multi_league.data_fetchers.backfill_fantasy_points",
    "multi_league.data_fetchers.update_nfl_super_table",
    # Transformations — aggregation (invoked directly in GH Actions)
    "multi_league.transformations.aggregation.aggregate_fantasy_context",
    "multi_league.transformations.aggregation.aggregate_matchup_context",
    "multi_league.transformations.aggregation.aggregate_draft_context",
    "multi_league.transformations.aggregation.aggregate_transaction_context",
    "multi_league.transformations.aggregation.aggregate_standings",
    "multi_league.transformations.aggregation.homepage_summary",
    # Transformations — matchup
    "multi_league.transformations.matchup.expected_record_v2",
    "multi_league.transformations.matchup.orchestrator",
    # Transformations — player
    "multi_league.transformations.player.orchestrator",
    "multi_league.transformations.player.clutch_to_player",
    # Transformations — draft, transaction, schedule
    "multi_league.transformations.draft.orchestrator",
    "multi_league.transformations.transaction.orchestrator",
    "multi_league.transformations.schedule.orchestrator",
    # SQL enrichments
    "multi_league.transformations.sql_enrichments",
]


@pytest.mark.parametrize("module_path", PIPELINE_MODULES)
def test_module_imports(module_path):
    """Every pipeline module must import without error."""
    mod = importlib.import_module(module_path)
    assert mod is not None
