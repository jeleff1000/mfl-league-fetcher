from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_sim_probability_sums_uses_settings_and_sleeper_2017_leniency():
    from multi_league.validation_v2.checks.simulations import CHECKS

    check = next(c for c in CHECKS if c.name == "sim_probability_sums")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 10, 'yahoo',   100.0, 0),
            ('good_league', 2024, 10, 'yahoo',   100.0, 0),
            ('good_league', 2024, 10, 'yahoo',   100.0, 0),
            ('good_league', 2024, 10, 'yahoo',   100.0, 0),
            ('bad_league',  2024, 10, 'yahoo',   100.0, 0),
            ('bad_league',  2024, 10, 'yahoo',   100.0, 0),
            ('bad_league',  2024, 10, 'yahoo',   100.0, 0),
            ('bad_league',  2024, 10, 'yahoo',   100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper',  50.0, 0)
        ) AS t(db_name, year, week, platform, p_playoffs, is_bye_week)
    """)
    conn.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 4, 2, 13, 'yahoo'),
            ('bad_league', 2024, 6, 2, 13, 'yahoo'),
            ('sleeper_2017', 2017, 6, 2, 13, 'sleeper')
        ) AS t(db_name, year, playoff_teams, bye_teams, regular_season_weeks, platform)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league", "sleeper_2017"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1
    assert result_map["sleeper_2017"].passed is True

    conn.close()


def test_sim_bye_probability_sums_uses_settings_fallback_and_sleeper_2017_leniency():
    from multi_league.validation_v2.checks.simulations import CHECKS

    check = next(c for c in CHECKS if c.name == "sim_bye_probability_sums")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 10, 'yahoo',   100.0, 0),
            ('good_league', 2024, 10, 'yahoo',   100.0, 0),
            ('good_league', 2024, 10, 'yahoo',     0.0, 0),
            ('good_league', 2024, 10, 'yahoo',     0.0, 0),
            ('bad_league',  2024, 10, 'yahoo',   100.0, 0),
            ('bad_league',  2024, 10, 'yahoo',     0.0, 0),
            ('bad_league',  2024, 10, 'yahoo',     0.0, 0),
            ('bad_league',  2024, 10, 'yahoo',     0.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper', 100.0, 0),
            ('sleeper_2017', 2017, 10, 'sleeper',  50.0, 0)
        ) AS t(db_name, year, week, platform, p_bye, is_bye_week)
    """)
    conn.execute("""
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 6, 2,    13, 'yahoo'),
            ('bad_league', 2024, 6, NULL,  13, 'yahoo'),
            ('sleeper_2017', 2017, 6, 2,   13, 'sleeper')
        ) AS t(db_name, year, playoff_teams, bye_teams, regular_season_weeks, platform)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league", "sleeper_2017"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1
    assert result_map["sleeper_2017"].passed is True

    conn.close()


def test_sim_all_managers_every_week_allows_real_bye_week():
    from multi_league.validation_v2.checks.simulations import CHECKS

    check = next(c for c in CHECKS if c.name == "sim_all_managers_every_week")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('bye_week_ok', 2024, 1, 'a', 50.0, 0, 0, 0),
            ('bye_week_ok', 2024, 1, 'b', 50.0, 0, 0, 0),
            ('bye_week_ok', 2024, 1, 'c', 50.0, 0, 0, 0),
            ('bye_week_ok', 2024, 1, 'd', 50.0, 0, 0, 0),
            ('bye_week_ok', 2024, 2, 'a', 50.0, 0, 0, 0),
            ('bye_week_ok', 2024, 2, 'b', 50.0, 0, 0, 0),
            ('missing_sim_bug', 2024, 1, 'a', 50.0, 0, 0, 0),
            ('missing_sim_bug', 2024, 1, 'b', NULL, 0, 0, 0),
            ('missing_sim_bug', 2024, 1, 'c', 50.0, 0, 0, 0),
            ('missing_sim_bug', 2024, 1, 'd', 50.0, 0, 0, 0)
        ) AS t(db_name, year, week, manager, p_playoffs, is_bye_week, is_playoffs, is_consolation)
    """)

    manifest = Manifest(all_leagues=["bye_week_ok", "missing_sim_bug"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["bye_week_ok"].passed is True
    assert result_map["missing_sim_bug"].passed is False
    assert result_map["missing_sim_bug"].fail_count == 1

    conn.close()
