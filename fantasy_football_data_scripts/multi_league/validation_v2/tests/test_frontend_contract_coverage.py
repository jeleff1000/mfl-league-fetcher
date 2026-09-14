"""Guardrails for frontend table-contract coverage in validator_v2.

This is intentionally lightweight:

- It does not require MotherDuck.
- It does not assert semantic completeness.
- It makes sure frontend-used analytics tables are at least explicitly
  classified as covered, gap, exempt, or external.

The goal is to stop silent drift when the frontend starts depending on a new
table family without a matching validator conversation.
"""

from __future__ import annotations

from multi_league.validation_v2.checks import ALL_CHECKS


# Frontend-used analytics tables that should eventually have validator coverage.
# These are pipeline-produced league tables or precomputed experience tables,
# not admin writeback tables or ___ops upstream tables.
FRONTEND_ANALYTICS_TABLES: dict[str, str] = {
    "draft": "canonical draft picks for draft pages",
    "draft_manager_career": "precomputed draft manager career view",
    "draft_manager_season": "precomputed draft manager season view",
    "draft_player_career": "precomputed draft player career view",
    "homepage_current_standings": "home page standings card",
    "homepage_league_summary": "home page summary + superlatives",
    "homepage_manager_profiles": "manager profile cards / homepage profile data",
    "homepage_manager_rankings": "home page manager rankings",
    "homepage_top_rivalries": "home page rivalry teaser",
    "h2h_season": "franchise-id H2H fast path for luck UI",
    "keeper_config": "keeper wizard / config UI",
    "league_settings": "league format + rules source for UI feature flags",
    "matchup": "canonical weekly matchup truth for managers/home/recaps/sims",
    "matchup_career": "career manager aggregates",
    "matchup_h2h_career": "career H2H fast path",
    "matchup_h2h_season": "season H2H fast path / fallback",
    "matchup_season": "standings + season manager aggregates",
    "player_fantasy": "canonical weekly player truth",
    "player_fantasy_career": "career player aggregates",
    "player_fantasy_season": "season player aggregates",
    "player_nfl_career": "career fast path for player NFL stats/meta",
    "player_nfl_season": "season fast path for player NFL stats/meta",
    "schedule": "canonical schedule coverage",
    "schedule_swap_season": "schedule luck precompute table",
    "transaction_manager_career": "transaction manager career aggregates",
    "transaction_manager_season": "transaction manager season aggregates",
    "transaction_player_career": "transaction player career aggregates",
    "transaction_report_card": "transactions report-card fast path",
    "transactions": "canonical move-level truth for transaction UI",
}


# Frontend tables that are real dependencies, but should be validated via route /
# writeback / schema tests instead of fleet-wide pipeline validators.
EXEMPT_OPERATIONAL_TABLES: dict[str, str] = {
    "league_context": "context blob / writeback state, not analytics output",
    "league_rules": "user override table, not pipeline-derived analytics",
    "manager_overrides": "audit/writeback table for manager rename operations",
    "standings_config": "user-facing standings configuration table",
}


# External or upstream tables used by query fast paths.
EXTERNAL_UPSTREAM_TABLES: dict[str, str] = {
    "___ops.nfl_historical.nfl_player_stats_all": "external super table",
    "___ops.nfl_historical.player_nfl_career": "external NFL career rollup",
    "___ops.nfl_historical.player_nfl_season": "external NFL season rollup",
}


# Frontend analytics tables whose correctness depends materially on league
# settings. This is not a validator implementation test; it's a planning
# guardrail so we keep settings in view when adding contracts/checks.
SETTINGS_SENSITIVE_FRONTEND_TABLES: dict[str, tuple[str, ...]] = {
    "matchup": ("platform", "uses_median", "num_teams", "playoff_start_week", "playoff_teams", "end_week"),
    "matchup_season": ("uses_median", "num_teams", "playoff_start_week", "playoff_teams", "end_week"),
    "matchup_h2h_season": ("num_teams", "playoff_start_week", "playoff_teams", "end_week"),
    "h2h_season": ("num_teams", "playoff_start_week", "playoff_teams", "end_week"),
    "player_fantasy": ("platform", "full_import", "roster_settings"),
    "player_fantasy_season": ("platform", "full_import", "roster_settings"),
    "draft": ("platform", "draft_type", "max_keepers", "is_dynasty"),
    "draft_manager_season": ("platform", "draft_type", "max_keepers", "is_dynasty"),
    "transactions": ("platform", "waiver_budget", "is_dynasty"),
    "transaction_report_card": ("platform", "waiver_budget", "is_dynasty"),
    "schedule": ("num_teams", "playoff_start_week", "playoff_teams", "end_week"),
    "schedule_swap_season": ("uses_median", "num_teams", "playoff_teams", "end_week"),
    "homepage_current_standings": ("uses_median", "num_teams", "playoff_start_week", "playoff_teams", "end_week"),
    "homepage_league_summary": ("inherits_source_settings",),
    "keeper_config": ("platform", "max_keepers", "is_dynasty"),
}

PLATFORM_QUIRK_SENSITIVE_FRONTEND_TABLES: dict[str, tuple[str, ...]] = {
    "matchup": ("yahoo_grade_column", "yahoo_auction_budget_columns"),
    "player_fantasy": ("fantasy_position_is_lineup_slot", "weekly_manager_singular"),
    "player_fantasy_season": ("season_managers_plural",),
    "draft": ("keeper_columns_vary_by_platform", "pick_quality_column_varies_by_platform"),
    "transactions": ("trade_and_faab_columns_vary_by_platform",),
    "transaction_report_card": ("trade_and_faab_columns_vary_by_platform",),
}


KNOWN_SETTINGS_AXES = {
    "platform",
    "uses_median",
    "num_teams",
    "playoff_start_week",
    "playoff_teams",
    "end_week",
    "draft_type",
    "max_keepers",
    "is_dynasty",
    "waiver_budget",
    "full_import",
    "roster_settings",
    "inherits_source_settings",
}

KNOWN_PLATFORM_QUIRKS = {
    "yahoo_grade_column",
    "yahoo_auction_budget_columns",
    "fantasy_position_is_lineup_slot",
    "weekly_manager_singular",
    "season_managers_plural",
    "keeper_columns_vary_by_platform",
    "pick_quality_column_varies_by_platform",
    "trade_and_faab_columns_vary_by_platform",
}


# Known gaps: frontend analytics tables that are used today but do not yet have
# meaningful dedicated validator coverage in validation_v2.
KNOWN_FRONTEND_GAPS: dict[str, str] = {
    "matchup_h2h_career": "career H2H fast path lacks dedicated checks",
    "player_nfl_career": "career fast-path player NFL table lacks dedicated checks",
    "player_nfl_season": "season fast-path player NFL table lacks dedicated checks",
}


MINIMUM_COVERED_CORE_TABLES = {
    "league_settings",
    "matchup",
    "matchup_season",
    "matchup_career",
    "matchup_h2h_season",
    "player_fantasy",
    "player_fantasy_season",
    "player_fantasy_career",
    "draft",
    "draft_manager_season",
    "transactions",
    "transaction_report_card",
    "schedule",
    "schedule_swap_season",
    "homepage_league_summary",
    "homepage_current_standings",
    "homepage_manager_profiles",
    "homepage_manager_rankings",
    "homepage_top_rivalries",
    "h2h_season",
    "keeper_config",
}


def test_frontend_analytics_tables_are_explicitly_accounted_for():
    """No frontend analytics table should be silently unclassified."""
    validator_tables = {check.table for check in ALL_CHECKS}
    accounted = validator_tables | set(KNOWN_FRONTEND_GAPS)
    unaccounted = set(FRONTEND_ANALYTICS_TABLES) - accounted

    assert not unaccounted, (
        "Frontend analytics tables missing both validator coverage and explicit gap classification: "
        f"{sorted(unaccounted)}"
    )


def test_core_frontend_tables_have_minimum_validator_presence():
    """High-value frontend tables should never lose all validator coverage."""
    validator_tables = {check.table for check in ALL_CHECKS}
    missing = MINIMUM_COVERED_CORE_TABLES - validator_tables

    assert not missing, "Core frontend tables lost validator coverage: " f"{sorted(missing)}"


def test_frontend_table_classifications_are_disjoint():
    """A table should not be classified as both gap and exempt/external."""
    gap_tables = set(KNOWN_FRONTEND_GAPS)
    exempt_tables = set(EXEMPT_OPERATIONAL_TABLES)
    external_tables = set(EXTERNAL_UPSTREAM_TABLES)

    assert gap_tables.isdisjoint(exempt_tables)
    assert gap_tables.isdisjoint(external_tables)
    assert exempt_tables.isdisjoint(external_tables)


def test_settings_sensitive_tables_are_known_frontend_analytics():
    """Settings-sensitive table inventory should only reference frontend analytics tables."""
    settings_tables = set(SETTINGS_SENSITIVE_FRONTEND_TABLES)
    analytics_tables = set(FRONTEND_ANALYTICS_TABLES)

    assert settings_tables <= analytics_tables, (
        "Settings-sensitive tables must be a subset of frontend analytics tables: "
        f"{sorted(settings_tables - analytics_tables)}"
    )


def test_settings_axes_are_from_known_inventory():
    """Keep settings vocabulary tight so the contract list stays readable."""
    used_axes = {axis for axes in SETTINGS_SENSITIVE_FRONTEND_TABLES.values() for axis in axes}

    assert used_axes <= KNOWN_SETTINGS_AXES, (
        "Unknown settings axes in table-contract inventory: " f"{sorted(used_axes - KNOWN_SETTINGS_AXES)}"
    )


def test_platform_quirk_sensitive_tables_are_known_frontend_analytics():
    """Platform-sensitive inventory should only reference frontend analytics tables."""
    platform_tables = set(PLATFORM_QUIRK_SENSITIVE_FRONTEND_TABLES)
    analytics_tables = set(FRONTEND_ANALYTICS_TABLES)

    assert platform_tables <= analytics_tables, (
        "Platform-sensitive tables must be a subset of frontend analytics tables: "
        f"{sorted(platform_tables - analytics_tables)}"
    )


def test_platform_quirk_axes_are_from_known_inventory():
    """Keep platform-quirk vocabulary explicit and finite."""
    used_quirks = {quirk for quirks in PLATFORM_QUIRK_SENSITIVE_FRONTEND_TABLES.values() for quirk in quirks}

    assert used_quirks <= KNOWN_PLATFORM_QUIRKS, (
        "Unknown platform quirks in table-contract inventory: " f"{sorted(used_quirks - KNOWN_PLATFORM_QUIRKS)}"
    )
