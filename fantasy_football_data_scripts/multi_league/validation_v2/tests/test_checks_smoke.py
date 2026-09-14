"""Smoke tests + integration tests for validator registry health.

Validates:
- All registered checks pass validate() (correct severity, batch_group, etc.)
- No duplicate check names in the combined registry
- Core validator modules maintain minimum coverage floors
- Named high-value contract checks remain present
- Integration checks still catch representative bad data
"""

from __future__ import annotations

from importlib import import_module

import duckdb

from multi_league.validation_v2.models import Manifest
from multi_league.validation_v2.executor import build_batch_sql, run_batch, run_sql_full


MODULE_PATHS = {
    "completeness": "multi_league.validation_v2.checks.completeness",
    "manager_identity": "multi_league.validation_v2.checks.manager_identity",
    "manifest_checks": "multi_league.validation_v2.checks.manifest_checks",
    "matchups": "multi_league.validation_v2.checks.matchups",
    "standings": "multi_league.validation_v2.checks.standings",
    "playoffs": "multi_league.validation_v2.checks.playoffs",
    "simulations": "multi_league.validation_v2.checks.simulations",
    "luck": "multi_league.validation_v2.checks.luck",
    "players_weekly": "multi_league.validation_v2.checks.players_weekly",
    "players_agg": "multi_league.validation_v2.checks.players_agg",
    "optimal_lineup": "multi_league.validation_v2.checks.optimal_lineup",
    "draft": "multi_league.validation_v2.checks.draft",
    "keepers": "multi_league.validation_v2.checks.keepers",
    "transactions": "multi_league.validation_v2.checks.transactions",
    "overview": "multi_league.validation_v2.checks.overview",
    "recaps": "multi_league.validation_v2.checks.recaps",
    "team_names": "multi_league.validation_v2.checks.team_names",
    "schedules": "multi_league.validation_v2.checks.schedules",
    "league_settings": "multi_league.validation_v2.checks.league_settings",
    "system": "multi_league.validation_v2.checks.system",
    "pipeline_health": "multi_league.validation_v2.checks.pipeline_health",
    "super_table": "multi_league.validation_v2.checks.super_table",
}

MINIMUM_MODULE_FLOORS = {
    "completeness": 8,
    "manager_identity": 5,
    "matchups": 15,
    "standings": 8,
    "simulations": 20,
    "luck": 10,
    "players_weekly": 20,
    "players_agg": 10,
    "optimal_lineup": 10,
    "draft": 15,
    "keepers": 5,
    "transactions": 20,
    "overview": 12,
    "recaps": 1,
    "team_names": 2,
    "schedules": 2,
    "league_settings": 10,
    "system": 5,
    "pipeline_health": 10,
    "super_table": 10,
}

REQUIRED_CHECK_NAMES = {
    "completeness_has_matchup",
    "identity_franchise_id_not_null",
    "standings_records_match_weekly",
    "sim_probability_sums",
    "sim_bye_probability_sums",
    "luck_h2h_season_matches_all_play_rollup",
    "luck_schedule_swap_season_matches_rollup",
    "overview_career_wins_match",
    "overview_summary_high_score_match",
    "overview_current_standings_match_latest_matchup_season",
    "draft_manager_season_matches_draft_totals",
    "draft_manager_career_matches_season_rollup",
    "draft_player_career_matches_draft_rollup",
    "txn_manager_season_matches_transactions_summary",
    "txn_manager_career_matches_season_rollup",
    "txn_player_career_matches_transactions_rollup",
    "txn_report_card_matches_manager_season",
    "players_agg_season_games_match_weekly",
    "players_agg_career_matches_season_totals",
    "keepers_config_global_row_exists",
    "keepers_config_required_scalars_populated",
}

MINIMUM_TOTAL_CHECKS = 275


def _module_checks(module_key: str):
    return import_module(MODULE_PATHS[module_key]).CHECKS


# ---------------------------------------------------------------------------
# 1. test_all_checks_valid
#    Every check in the combined registry passes validate().
# ---------------------------------------------------------------------------


def test_all_checks_valid():
    """Every check in the registry passes validate()."""
    from multi_league.validation_v2.checks.completeness import CHECKS as comp
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident
    from multi_league.validation_v2.checks.manifest_checks import CHECKS as manifest
    from multi_league.validation_v2.checks.matchups import CHECKS as matchup_checks
    from multi_league.validation_v2.checks.standings import CHECKS as standings_checks
    from multi_league.validation_v2.checks.playoffs import CHECKS as playoff_checks
    from multi_league.validation_v2.checks.simulations import CHECKS as simulation_checks
    from multi_league.validation_v2.checks.luck import CHECKS as luck_checks
    from multi_league.validation_v2.checks.players_weekly import CHECKS as players_weekly_checks
    from multi_league.validation_v2.checks.players_agg import CHECKS as players_agg_checks
    from multi_league.validation_v2.checks.optimal_lineup import CHECKS as optimal_lineup_checks
    from multi_league.validation_v2.checks.draft import CHECKS as draft_checks
    from multi_league.validation_v2.checks.keepers import CHECKS as keeper_checks
    from multi_league.validation_v2.checks.transactions import CHECKS as transaction_checks
    from multi_league.validation_v2.checks.overview import CHECKS as overview_checks
    from multi_league.validation_v2.checks.recaps import CHECKS as recap_checks
    from multi_league.validation_v2.checks.team_names import CHECKS as team_name_checks
    from multi_league.validation_v2.checks.schedules import CHECKS as schedule_checks
    from multi_league.validation_v2.checks.league_settings import CHECKS as league_settings_checks
    from multi_league.validation_v2.checks.system import CHECKS as system_checks
    from multi_league.validation_v2.checks.pipeline_health import CHECKS as pipeline_health_checks
    from multi_league.validation_v2.checks.super_table import CHECKS as super_table_checks

    all_checks = (
        comp
        + ident
        + manifest
        + matchup_checks
        + standings_checks
        + playoff_checks
        + simulation_checks
        + luck_checks
        + players_weekly_checks
        + players_agg_checks
        + optimal_lineup_checks
        + draft_checks
        + keeper_checks
        + transaction_checks
        + overview_checks
        + recap_checks
        + team_name_checks
        + schedule_checks
        + league_settings_checks
        + system_checks
        + pipeline_health_checks
        + super_table_checks
    )
    for check in all_checks:
        check.validate()


# ---------------------------------------------------------------------------
# 2. test_no_duplicate_names
# ---------------------------------------------------------------------------


def test_no_duplicate_names():
    from multi_league.validation_v2.checks.completeness import CHECKS as comp
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident
    from multi_league.validation_v2.checks.manifest_checks import CHECKS as manifest
    from multi_league.validation_v2.checks.matchups import CHECKS as matchup_checks
    from multi_league.validation_v2.checks.standings import CHECKS as standings_checks
    from multi_league.validation_v2.checks.playoffs import CHECKS as playoff_checks
    from multi_league.validation_v2.checks.simulations import CHECKS as simulation_checks
    from multi_league.validation_v2.checks.luck import CHECKS as luck_checks
    from multi_league.validation_v2.checks.players_weekly import CHECKS as players_weekly_checks
    from multi_league.validation_v2.checks.players_agg import CHECKS as players_agg_checks
    from multi_league.validation_v2.checks.optimal_lineup import CHECKS as optimal_lineup_checks
    from multi_league.validation_v2.checks.draft import CHECKS as draft_checks
    from multi_league.validation_v2.checks.keepers import CHECKS as keeper_checks
    from multi_league.validation_v2.checks.transactions import CHECKS as transaction_checks
    from multi_league.validation_v2.checks.overview import CHECKS as overview_checks
    from multi_league.validation_v2.checks.recaps import CHECKS as recap_checks
    from multi_league.validation_v2.checks.team_names import CHECKS as team_name_checks
    from multi_league.validation_v2.checks.schedules import CHECKS as schedule_checks
    from multi_league.validation_v2.checks.league_settings import CHECKS as league_settings_checks
    from multi_league.validation_v2.checks.system import CHECKS as system_checks
    from multi_league.validation_v2.checks.pipeline_health import CHECKS as pipeline_health_checks
    from multi_league.validation_v2.checks.super_table import CHECKS as super_table_checks

    all_checks = (
        comp
        + ident
        + manifest
        + matchup_checks
        + standings_checks
        + playoff_checks
        + simulation_checks
        + luck_checks
        + players_weekly_checks
        + players_agg_checks
        + optimal_lineup_checks
        + draft_checks
        + keeper_checks
        + transaction_checks
        + overview_checks
        + recap_checks
        + team_name_checks
        + schedule_checks
        + league_settings_checks
        + system_checks
        + pipeline_health_checks
        + super_table_checks
    )
    names = [c.name for c in all_checks]
    assert len(names) == len(set(names)), f"Duplicates: {[n for n in names if names.count(n) > 1]}"


# ---------------------------------------------------------------------------
# 3. test_all_checks_aggregator
#    ALL_CHECKS in __init__ contains the union of all modules.
# ---------------------------------------------------------------------------


def test_all_checks_aggregator():
    from multi_league.validation_v2.checks import ALL_CHECKS
    from multi_league.validation_v2.checks.completeness import CHECKS as comp
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident
    from multi_league.validation_v2.checks.manifest_checks import CHECKS as manifest
    from multi_league.validation_v2.checks.matchups import CHECKS as matchup_checks
    from multi_league.validation_v2.checks.standings import CHECKS as standings_checks
    from multi_league.validation_v2.checks.playoffs import CHECKS as playoff_checks
    from multi_league.validation_v2.checks.simulations import CHECKS as simulation_checks
    from multi_league.validation_v2.checks.luck import CHECKS as luck_checks
    from multi_league.validation_v2.checks.players_weekly import CHECKS as players_weekly_checks
    from multi_league.validation_v2.checks.players_agg import CHECKS as players_agg_checks
    from multi_league.validation_v2.checks.optimal_lineup import CHECKS as optimal_lineup_checks
    from multi_league.validation_v2.checks.draft import CHECKS as draft_checks
    from multi_league.validation_v2.checks.keepers import CHECKS as keeper_checks
    from multi_league.validation_v2.checks.transactions import CHECKS as transaction_checks
    from multi_league.validation_v2.checks.overview import CHECKS as overview_checks
    from multi_league.validation_v2.checks.recaps import CHECKS as recap_checks
    from multi_league.validation_v2.checks.team_names import CHECKS as team_name_checks
    from multi_league.validation_v2.checks.schedules import CHECKS as schedule_checks
    from multi_league.validation_v2.checks.league_settings import CHECKS as league_settings_checks
    from multi_league.validation_v2.checks.system import CHECKS as system_checks
    from multi_league.validation_v2.checks.pipeline_health import CHECKS as pipeline_health_checks
    from multi_league.validation_v2.checks.super_table import CHECKS as super_table_checks

    all_modules = (
        comp
        + ident
        + manifest
        + matchup_checks
        + standings_checks
        + playoff_checks
        + simulation_checks
        + luck_checks
        + players_weekly_checks
        + players_agg_checks
        + optimal_lineup_checks
        + draft_checks
        + keeper_checks
        + transaction_checks
        + overview_checks
        + recap_checks
        + team_name_checks
        + schedule_checks
        + league_settings_checks
        + system_checks
        + pipeline_health_checks
        + super_table_checks
    )
    assert len(ALL_CHECKS) == len(all_modules)
    all_names = {c.name for c in ALL_CHECKS}
    for check in all_modules:
        assert check.name in all_names


# ---------------------------------------------------------------------------
# 4. minimum coverage floors and named-contract presence
# ---------------------------------------------------------------------------


def test_core_validator_modules_have_minimum_floor():
    for module_key, floor in MINIMUM_MODULE_FLOORS.items():
        checks = _module_checks(module_key)
        assert len(checks) >= floor, f"{module_key} dropped below minimum coverage floor: {len(checks)} < {floor}"


def test_named_high_value_checks_present():
    from multi_league.validation_v2.checks import ALL_CHECKS

    all_names = {check.name for check in ALL_CHECKS}
    missing = REQUIRED_CHECK_NAMES - all_names

    assert not missing, f"Missing required high-value checks: {sorted(missing)}"


def test_total_registry_has_minimum_size():
    from multi_league.validation_v2.checks import ALL_CHECKS

    assert len(ALL_CHECKS) >= MINIMUM_TOTAL_CHECKS


# ---------------------------------------------------------------------------
# 6. Integration: identity_franchise_id_not_null catches bad_league
# ---------------------------------------------------------------------------


def test_identity_franchise_id_not_null_catches_bad_league(local_db, manifest):
    """bad_league has 1 row with NULL franchise_id → check must fail for bad_league."""
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident

    check = next(c for c in ident if c.name == "identity_franchise_id_not_null")
    assert check.sql_expr is not None, "Expected sql_expr check"

    sql = build_batch_sql(
        checks=[check],
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets={},
        table_prefix="public.",
    )
    results = run_batch(local_db, sql, [check], manifest, skip_sets={})
    result_map = {r.db_name: r for r in results}

    # bad_league: Charlie has franchise_id=NULL, is_bye_week=0, is_placeholder=0
    assert "bad_league" in result_map, "bad_league must appear in results"
    assert result_map["bad_league"].passed is False, "bad_league should fail"
    assert result_map["bad_league"].fail_count == 1, "Exactly 1 NULL franchise_id row"

    # good_league: all franchise_ids populated
    assert result_map["good_league"].passed is True
    assert result_map["good_league"].fail_count == 0

    # median_league: all franchise_ids populated
    assert result_map["median_league"].passed is True


# ---------------------------------------------------------------------------
# 7. Integration: completeness_has_matchup catches missing league
# ---------------------------------------------------------------------------


def test_completeness_has_matchup_catches_missing_league():
    """A league in league_inventory but absent from matchup must be flagged."""
    from multi_league.validation_v2.checks.completeness import CHECKS as comp

    check = next(c for c in comp if c.name == "completeness_has_matchup")

    # Build a local DB with league_inventory containing 4 leagues,
    # but matchup only has data for 3 of them. "missing_league" is absent.
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES
            ('good_league', TRUE),
            ('bad_league', TRUE),
            ('median_league', TRUE),
            ('missing_league', TRUE)
        ) AS t(database_name, in_centralized)
    """)
    conn.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('good_league'),
            ('bad_league'),
            ('median_league')
        ) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            ('good_league'),
            ('bad_league'),
            ('median_league')
        ) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 1),
            ('bad_league', 2024, 1),
            ('median_league', 2024, 1)
        ) AS t(db_name, year, week)
    """)

    manifest_4 = Manifest(
        all_leagues=["good_league", "bad_league", "median_league", "missing_league"],
    )

    results = run_sql_full(conn, check, manifest_4, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    # missing_league is in inventory but not in matchup → must fail
    assert "missing_league" in result_map, "missing_league must appear in results"
    assert result_map["missing_league"].passed is False, "missing_league should fail"
    assert result_map["missing_league"].fail_count == 1

    # Leagues with data should pass
    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is True
    assert result_map["median_league"].passed is True

    conn.close()


# ---------------------------------------------------------------------------
# 8. Integration: completeness_has_matchup passes when all leagues present
# ---------------------------------------------------------------------------


def test_completeness_has_matchup_passes_when_complete():
    """When every league has matchup data, the check passes for all."""
    from multi_league.validation_v2.checks.completeness import CHECKS as comp

    check = next(c for c in comp if c.name == "completeness_has_matchup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES
            ('league_a', TRUE),
            ('league_b', TRUE)
        ) AS t(database_name, in_centralized)
    """)
    conn.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('league_a'),
            ('league_b')
        ) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            ('league_a'),
            ('league_b')
        ) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('league_a', 2024, 1),
            ('league_b', 2024, 1)
        ) AS t(db_name, year, week)
    """)

    manifest_2 = Manifest(all_leagues=["league_a", "league_b"])
    results = run_sql_full(conn, check, manifest_2, table_prefix="public.")

    for r in results:
        assert r.passed is True, f"{r.db_name} should pass but failed"

    conn.close()


def test_overview_career_wins_match_allows_playoff_wins():
    """homepage rankings include playoff wins; matchup_career is regular-season scoped."""
    from multi_league.validation_v2.checks.overview import CHECKS as overview_checks

    check = next(c for c in overview_checks if c.name == "overview_career_wins_match")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.homepage_manager_rankings AS
        SELECT * FROM (VALUES
            ('league_a', 'alice_fid', 12),
            ('league_b', 'bob_fid', 13)
        ) AS t(db_name, franchise_id, wins)
    """)
    conn.execute("""
        CREATE TABLE public.matchup_career AS
        SELECT * FROM (VALUES
            ('league_a', 'alice_fid', 10),
            ('league_b', 'bob_fid', 10)
        ) AS t(db_name, franchise_id, wins)
    """)
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('league_a', 'alice_fid', 0, 1, 0, 1),
            ('league_a', 'alice_fid', 0, 1, 0, 1),
            ('league_a', 'alice_fid', 0, 1, 1, 1),
            ('league_b', 'bob_fid', 0, 1, 0, 1)
        ) AS t(db_name, franchise_id, is_bye_week, is_playoffs, is_consolation, win)
    """)

    manifest = Manifest(all_leagues=["league_a", "league_b"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["league_a"].passed is True
    assert result_map["league_b"].passed is False
    assert result_map["league_b"].fail_count == 1

    conn.close()


# ---------------------------------------------------------------------------
# 9. Integration: completeness_has_career_agg respects multi_year feature gate
# ---------------------------------------------------------------------------


def test_completeness_has_career_agg_feature_gate():
    """career_agg check only runs for multi_year leagues."""
    from multi_league.validation_v2.checks.completeness import CHECKS as comp

    check = next(c for c in comp if c.name == "completeness_has_career_agg")
    assert check.feature == "multi_year", "career_agg must have feature='multi_year'"

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES ('multi_year_league', TRUE)) AS t(database_name, in_centralized)
    """)
    conn.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES ('multi_year_league')) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES ('multi_year_league', 2024, 1)) AS t(db_name, year, week)
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES ('multi_year_league')) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.matchup_career AS
        SELECT * FROM (VALUES ('multi_year_league', 'Alice')) AS t(db_name, manager)
    """)

    manifest_my = Manifest(
        all_leagues=["multi_year_league"],
        multi_year_leagues=["multi_year_league"],
    )
    results = run_sql_full(conn, check, manifest_my, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["multi_year_league"].passed is True

    conn.close()


def test_completeness_has_matchup_uses_live_scope_when_inventory_flag_is_false():
    """Live leagues should still validate even when inventory has stale FALSE flags."""
    from multi_league.validation_v2.checks.completeness import CHECKS as comp

    check = next(c for c in comp if c.name == "completeness_has_matchup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES ('live_but_stale', FALSE)) AS t(database_name, in_centralized)
    """)
    conn.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES ('live_but_stale')) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES ('live_but_stale')) AS t(db_name)
    """)
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('other_league', 2024, 1)
        ) AS t(db_name, year, week)
    """)

    manifest = Manifest(all_leagues=["live_but_stale"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert "live_but_stale" in result_map
    assert result_map["live_but_stale"].passed is False
    assert result_map["live_but_stale"].fail_count == 1

    conn.close()


# ---------------------------------------------------------------------------
# 10. Integration: identity_guid_one_franchise catches duplicate mapping
# ---------------------------------------------------------------------------


def test_identity_guid_one_franchise_catches_duplicate():
    """A manager_guid mapped to 2 franchise_ids in the same league must fail."""
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident

    check = next(c for c in ident if c.name == "identity_guid_one_franchise")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            -- clean_league: each guid maps to exactly one franchise_id
            ('clean_league', 2024, 1, 'Alice', 'guid_a', 'fid_a', 'Alices Team'),
            ('clean_league', 2024, 1, 'Bob',   'guid_b', 'fid_b', 'Bobs Team'),
            -- split_league: guid_x maps to two different franchise_ids
            ('split_league', 2024, 1, 'Team1', 'guid_x', 'fid_1', 'Team One'),
            ('split_league', 2024, 2, 'Team1', 'guid_x', 'fid_2', 'Team Two'),
            -- multi_team_league: same account owns two teams in the same week
            ('multi_team_league', 2024, 1, 'Owner', 'guid_multi', 'fid_1', 'First Team'),
            ('multi_team_league', 2024, 1, 'Owner', 'guid_multi', 'fid_2', 'Second Team')
        ) AS t(db_name, year, week, manager, manager_guid, franchise_id, team_name)
    """)

    manifest_2 = Manifest(all_leagues=["clean_league", "split_league", "multi_team_league"])
    results = run_sql_full(conn, check, manifest_2, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["split_league"].passed is False, "split_league should fail"
    assert result_map["split_league"].fail_count >= 1

    assert result_map["clean_league"].passed is True
    assert result_map["multi_team_league"].passed is True

    conn.close()


def test_identity_yahoo_guid_unexplained_franchise_split_ignores_legitimate_promotions():
    """Yahoo GUID splits fail only for same-season splits without multi-team evidence."""
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident

    check = next(c for c in ident if c.name == "identity_yahoo_guid_unexplained_franchise_split")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('clean_league', 'yahoo', 2024, 1, 'Alice', 'guid_a', 'fid_a', 'Alices Team'),
            ('clean_league', 'yahoo', 2024, 1, 'Bob',   'guid_b', 'fid_b', 'Bobs Team'),
            -- Changed display name should not hide a real same-season GUID split.
            ('split_league', 'yahoo', 2024, 1, 'Old Name', 'guid_x', 'fid_1', 'Old Team'),
            ('split_league', 'yahoo', 2024, 2, 'New Name', 'guid_x', 'fid_2', 'New Team'),
            -- Same account, two actual teams in the same week: legitimate branch split.
            ('multi_team_league', 'yahoo', 2024, 1, 'Owner', 'guid_multi', 'fid_1', 'First Team'),
            ('multi_team_league', 'yahoo', 2024, 1, 'Owner', 'guid_multi', 'fid_2', 'Second Team'),
            -- Clean year-to-year promotion: old Yahoo GUID rows merged onto a primary platform fid.
            ('promoted_league', 'yahoo', 2008, 1, 'Old Name', 'guid_promoted', 'guid_promoted', 'Old Team'),
            ('promoted_league', 'yahoo', 2009, 1, 'New Name', 'guid_promoted', 'sleeper_primary_fid', 'New Team'),
            ('promoted_league', 'sleeper', 2025, 1, 'New Name', 'sleeper_guid', 'sleeper_primary_fid', 'New Team')
        ) AS t(db_name, platform, year, week, manager, manager_guid, franchise_id, team_name)
    """)

    manifest_2 = Manifest(
        all_leagues=["clean_league", "split_league", "multi_team_league", "promoted_league"],
        yahoo_leagues=["clean_league", "split_league", "multi_team_league", "promoted_league"],
    )
    results = run_sql_full(conn, check, manifest_2, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["split_league"].passed is False
    assert result_map["split_league"].fail_count == 1
    assert result_map["clean_league"].passed is True
    assert result_map["multi_team_league"].passed is True
    assert result_map["promoted_league"].passed is True

    conn.close()


def test_identity_franchise_id_cross_table_scopes_to_matchup_years():
    """Roster-only years should not create orphan franchise_id errors."""
    from multi_league.validation_v2.checks.manager_identity import CHECKS as ident

    check = next(c for c in ident if c.name == "identity_franchise_id_cross_table")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('covered_league', 2024, 'fid_a'),
            ('covered_league', 2024, 'fid_b'),
            ('roster_only_league', 2024, 'fid_existing')
        ) AS t(db_name, year, franchise_id)
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            -- Covered year: this is a real cross-table identity mismatch.
            ('covered_league', 2024, 'fid_orphan'),
            -- Covered year with a matching franchise passes.
            ('covered_league', 2024, 'fid_a'),
            -- Roster-only current year: no matchup rows exist for 2025 yet,
            -- so this check should not treat the new franchise as an orphan.
            ('roster_only_league', 2025, 'fid_new_roster_only')
        ) AS t(db_name, year, franchise_id)
    """)

    manifest_2 = Manifest(all_leagues=["covered_league", "roster_only_league"])
    results = run_sql_full(conn, check, manifest_2, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["covered_league"].passed is False
    assert result_map["covered_league"].fail_count == 1
    assert result_map["roster_only_league"].passed is True

    conn.close()
