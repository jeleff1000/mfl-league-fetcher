"""Tests for manifest.py — build_manifest + load_manifest."""

from __future__ import annotations

import pytest

from multi_league.validation_v2.manifest import build_manifest, load_manifest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def local_db_with_settings(local_db):
    """Extend the local_db fixture with league_settings and league_inventory tables."""
    # Add above_league_median, x0_win, is_consolation, player_lamar, is_keeper columns
    # to the matchup table (needed for manifest checks SQL)
    local_db.execute("""
        ALTER TABLE public.matchup
        ADD COLUMN IF NOT EXISTS above_league_median DOUBLE
    """)
    local_db.execute("""
        ALTER TABLE public.matchup
        ADD COLUMN IF NOT EXISTS x0_win DOUBLE
    """)

    # Update median_league to have above_league_median data
    local_db.execute("""
        UPDATE public.matchup
        SET above_league_median = 1.0
        WHERE db_name = 'median_league'
    """)

    # Update good_league to have x0_win data (sim)
    local_db.execute("""
        UPDATE public.matchup
        SET x0_win = 0.5
        WHERE db_name = 'good_league'
    """)

    # Create league_settings table
    local_db.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('good_league',  'yahoo',   2024, 10, 14, 4,  FALSE, 16, 0,   0, FALSE, 'snake'),
            ('median_league','sleeper', 2024, 12, 15, 6,  TRUE,  17, 100, 0, FALSE, 'snake'),
            ('median_league','sleeper', 2023, 12, 15, 6,  TRUE,  17, 100, 0, FALSE, 'snake'),
            ('bad_league',   'espn',    2024, 8,  13, 4,  FALSE, 16, 0,   3, FALSE, 'auction')
        ) AS t(
            db_name, platform, year, num_teams, playoff_start_week, playoff_teams,
            uses_median, end_week, waiver_budget, max_keepers, is_dynasty, draft_type
        )
    """)

    # Create league_inventory table (lives in ___ops.accounts in production)
    local_db.execute("ATTACH ':memory:' AS ___ops")
    local_db.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    local_db.execute("""
        CREATE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES
            ('good_league', TRUE),
            ('median_league', TRUE),
            ('bad_league', TRUE),
            ('settings_missing_league', TRUE)
        ) AS t(database_name, in_centralized)
    """)

    # Create player_fantasy table for manifest checks
    local_db.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            ('good_league',  'Alice', 100.0, 50.0, NULL, 0),
            ('good_league',  'Unrostered', 80.0, NULL, NULL, 0),
            ('median_league','Eve',   110.0, 60.0, NULL, 0),
            ('bad_league',   'Charlie', 90.0, NULL, NULL, 1)
        ) AS t(db_name, manager, fantasy_points, player_lamar, is_keeper, is_started)
    """)

    return local_db


# ---------------------------------------------------------------------------
# 1. test_build_manifest_from_settings
# ---------------------------------------------------------------------------


def test_build_manifest_from_settings():
    settings_rows = [
        {
            "db_name": "league_a",
            "platform": "sleeper",
            "uses_median": True,
            "num_teams": 12,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "waiver_budget": 100,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 17,
            "year": 2024,
            "draft_type": "snake",
        },
        {
            "db_name": "league_a",
            "platform": "sleeper",
            "uses_median": True,
            "num_teams": 12,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "waiver_budget": 100,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 17,
            "year": 2023,
            "draft_type": "snake",
        },
        {
            "db_name": "league_b",
            "platform": "yahoo",
            "uses_median": False,
            "num_teams": 10,
            "playoff_start_week": 14,
            "playoff_teams": 4,
            "waiver_budget": 0,
            "max_keepers": 3,
            "is_dynasty": False,
            "end_week": 16,
            "year": 2024,
            "draft_type": "snake",
        },
    ]
    inventory = ["league_a", "league_b", "league_c"]
    m = build_manifest(settings_rows, inventory)

    assert set(m.all_leagues) == {"league_a", "league_b", "league_c"}
    assert m.median_leagues == ["league_a"]
    assert m.keeper_leagues == ["league_b"]
    assert m.faab_leagues == ["league_a"]
    assert m.sleeper_leagues == ["league_a"]
    assert m.yahoo_leagues == ["league_b"]
    assert m.multi_year_leagues == ["league_a"]  # has 2023 + 2024
    assert "league_a" in m.settings
    assert m.settings["league_a"]["year"] == 2024  # most recent


# ---------------------------------------------------------------------------
# 2. test_build_manifest_dynasty
# ---------------------------------------------------------------------------


def test_build_manifest_dynasty():
    settings_rows = [
        {
            "db_name": "dyn_league",
            "platform": "sleeper",
            "uses_median": False,
            "num_teams": 12,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": True,
            "end_week": 17,
            "year": 2024,
            "draft_type": "snake",
        },
    ]
    m = build_manifest(settings_rows, ["dyn_league"])
    assert "dyn_league" in m.dynasty_leagues
    # dynasty does NOT imply keeper — keeper_leagues requires max_keepers >= 2
    # or actual is_keeper data (discovered in load_manifest, not build_manifest)
    assert "dyn_league" not in m.keeper_leagues


# ---------------------------------------------------------------------------
# 3. test_build_manifest_espn
# ---------------------------------------------------------------------------


def test_build_manifest_espn():
    settings_rows = [
        {
            "db_name": "espn_lg",
            "platform": "espn",
            "uses_median": False,
            "num_teams": 10,
            "playoff_start_week": 14,
            "playoff_teams": 4,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 16,
            "year": 2024,
            "draft_type": "auction",
        },
    ]
    m = build_manifest(settings_rows, ["espn_lg"])
    assert "espn_lg" in m.espn_leagues
    assert "espn_lg" not in m.yahoo_leagues
    assert "espn_lg" not in m.sleeper_leagues


# ---------------------------------------------------------------------------
# 4. test_build_manifest_empty
# ---------------------------------------------------------------------------


def test_build_manifest_empty():
    m = build_manifest([], [])
    assert m.all_leagues == []


# ---------------------------------------------------------------------------
# 5. test_build_manifest_inventory_only_league
# ---------------------------------------------------------------------------


def test_build_manifest_inventory_only_league():
    """Leagues in inventory but no settings rows still appear in all_leagues."""
    settings_rows = [
        {
            "db_name": "league_a",
            "platform": "sleeper",
            "uses_median": False,
            "num_teams": 12,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 17,
            "year": 2024,
            "draft_type": "snake",
        },
    ]
    inventory = ["league_a", "no_settings_league"]
    m = build_manifest(settings_rows, inventory)
    assert "no_settings_league" in m.all_leagues
    # no_settings_league has no settings, so no feature flags
    assert "no_settings_league" not in m.median_leagues
    assert "no_settings_league" not in m.settings


# ---------------------------------------------------------------------------
# 6. test_build_manifest_settings_most_recent_year
# ---------------------------------------------------------------------------


def test_build_manifest_settings_most_recent_year():
    """settings dict should contain the most recent year's row."""
    settings_rows = [
        {
            "db_name": "league_x",
            "platform": "yahoo",
            "uses_median": False,
            "num_teams": 10,
            "playoff_start_week": 14,
            "playoff_teams": 4,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 16,
            "year": 2022,
            "draft_type": "snake",
        },
        {
            "db_name": "league_x",
            "platform": "yahoo",
            "uses_median": True,
            "num_teams": 12,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "waiver_budget": 50,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 17,
            "year": 2024,
            "draft_type": "snake",
        },
        {
            "db_name": "league_x",
            "platform": "yahoo",
            "uses_median": False,
            "num_teams": 10,
            "playoff_start_week": 14,
            "playoff_teams": 4,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 16,
            "year": 2023,
            "draft_type": "snake",
        },
    ]
    m = build_manifest(settings_rows, ["league_x"])
    assert m.settings["league_x"]["year"] == 2024


# ---------------------------------------------------------------------------
# 7. test_build_manifest_consolation_from_settings
# ---------------------------------------------------------------------------


def test_build_manifest_consolation_from_settings():
    """consolation_leagues should only include leagues with consolation settings."""
    settings_rows = [
        {
            "db_name": "lg1",
            "platform": "sleeper",
            "uses_median": False,
            "num_teams": 10,
            "playoff_start_week": 14,
            "playoff_teams": 4,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 16,
            "year": 2024,
            "draft_type": "snake",
            "has_consolation_bracket": True,
            "num_playoff_consolation_teams": 4,
        },
        {
            "db_name": "lg2",
            "platform": "sleeper",
            "uses_median": False,
            "num_teams": 4,
            "playoff_start_week": 15,
            "playoff_teams": 4,
            "waiver_budget": 0,
            "max_keepers": 0,
            "is_dynasty": False,
            "end_week": 17,
            "year": 2024,
            "draft_type": "snake",
            "has_consolation_bracket": False,
            "num_playoff_consolation_teams": 0,
        },
    ]
    m = build_manifest(settings_rows, ["lg1", "lg2", "inventory_only"])
    assert m.consolation_leagues == ["lg1"]


# ---------------------------------------------------------------------------
# 8. test_manifest_checks_all_valid
# ---------------------------------------------------------------------------


def test_manifest_checks_all_valid():
    from multi_league.validation_v2.checks.manifest_checks import CHECKS

    for c in CHECKS:
        c.validate()
    assert len(CHECKS) == 6


# ---------------------------------------------------------------------------
# 9. test_load_manifest_from_local_db
# ---------------------------------------------------------------------------


def test_load_manifest_from_local_db(local_db_with_settings):
    """load_manifest should build the manifest from SQL queries."""
    m = load_manifest(local_db_with_settings, table_prefix="public.")

    # all_leagues comes from league_inventory
    assert set(m.all_leagues) == {"good_league", "median_league", "bad_league", "settings_missing_league"}

    # median_league uses_median = True
    assert "median_league" in m.median_leagues
    assert "good_league" not in m.median_leagues

    # good_league is yahoo
    assert "good_league" in m.yahoo_leagues

    # median_league is sleeper
    assert "median_league" in m.sleeper_leagues

    # bad_league is espn
    assert "bad_league" in m.espn_leagues

    # median_league has 2 years -> multi_year
    assert "median_league" in m.multi_year_leagues
    assert "good_league" not in m.multi_year_leagues

    # bad_league has max_keepers=3 -> keeper
    assert "bad_league" in m.keeper_leagues

    # median_league has waiver_budget=100 -> faab
    assert "median_league" in m.faab_leagues

    # settings should be populated for leagues that have settings rows
    assert "good_league" in m.settings
    assert "median_league" in m.settings

    # good_league has x0_win data -> sim_leagues
    assert "good_league" in m.sim_leagues

    # good_league has Unrostered player -> full_import_leagues
    assert "good_league" in m.full_import_leagues


def test_load_manifest_uses_live_settings_scope_when_inventory_flags_are_stale(local_db):
    """Live matchup/player rows should still define validator scope."""
    local_db.execute("CREATE SCHEMA IF NOT EXISTS public")
    local_db.execute("ATTACH ':memory:' AS ___ops")
    local_db.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    local_db.execute("""
        CREATE OR REPLACE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('live_league', 'sleeper', 2024, 12, 15, 6, FALSE, 17, 0, 0, FALSE, 'snake')
        ) AS t(
            db_name, platform, year, num_teams, playoff_start_week, playoff_teams,
            uses_median, end_week, waiver_budget, max_keepers, is_dynasty, draft_type
        )
    """)
    local_db.execute("""
        CREATE OR REPLACE TABLE public.matchup AS
        SELECT * FROM (VALUES ('live_league', 2024, 1, 0.5, 0)) AS t(
            db_name, year, week, x0_win, is_consolation
        )
    """)
    local_db.execute("""
        CREATE OR REPLACE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES ('live_league', 'Unrostered', 1.0, 1.0, NULL, 0)) AS t(
            db_name, manager, fantasy_points, player_lamar, is_keeper, is_started
        )
    """)
    local_db.execute("""
        CREATE OR REPLACE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES
            ('live_league', FALSE),
            ('stale_inventory_only', FALSE)
        ) AS t(database_name, in_centralized)
    """)

    m = load_manifest(local_db, table_prefix="public.")

    assert "live_league" in m.all_leagues
    assert "stale_inventory_only" not in m.all_leagues
    assert "live_league" in m.sleeper_leagues
    assert "live_league" in m.sim_leagues
    assert "live_league" in m.full_import_leagues


def test_load_manifest_excludes_settings_only_stale_partials(local_db):
    """Settings-only rows from failed partial imports are not app-published leagues."""
    local_db.execute("CREATE SCHEMA IF NOT EXISTS public")
    local_db.execute("ATTACH ':memory:' AS ___ops")
    local_db.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    local_db.execute("""
        CREATE OR REPLACE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('settings_only_partial', 'sleeper', 2024, 12, 15, 6, FALSE, 17, 0, 0, FALSE, 'snake')
        ) AS t(
            db_name, platform, year, num_teams, playoff_start_week, playoff_teams,
            uses_median, end_week, waiver_budget, max_keepers, is_dynasty, draft_type
        )
    """)
    local_db.execute(
        "CREATE OR REPLACE TABLE public.matchup AS SELECT * FROM (VALUES ('visible_league', 2024, 1, 0.5, 0)) AS t(db_name, year, week, x0_win, is_consolation)"
    )
    local_db.execute(
        "CREATE OR REPLACE TABLE public.player_fantasy AS SELECT * FROM (VALUES ('visible_league', 'Unrostered')) AS t(db_name, manager)"
    )
    local_db.execute("""
        CREATE OR REPLACE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES
            ('settings_only_partial', FALSE),
            ('visible_league', FALSE)
        ) AS t(database_name, in_centralized)
    """)

    m = load_manifest(local_db, table_prefix="public.")

    assert "visible_league" in m.all_leagues
    assert "settings_only_partial" not in m.all_leagues
