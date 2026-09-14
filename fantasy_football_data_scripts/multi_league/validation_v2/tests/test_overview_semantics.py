from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_overview_summary_high_score_match_catches_summary_drift():
    from multi_league.validation_v2.checks.overview import CHECKS

    check = next(c for c in CHECKS if c.name == "overview_summary_high_score_match")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 13, 'Alice', 'Bob', 142.7, 0, 0),
            ('good_league', 2024, 13, 'Bob', 'Alice', 118.4, 0, 0),
            ('bad_league', 2024, 12, 'Carol', 'Dave', 151.2, 0, 0),
            ('bad_league', 2024, 12, 'Dave', 'Carol', 103.5, 0, 0)
        ) AS t(db_name, year, week, manager, opponent, team_points, is_bye_week, is_consolation)
    """)
    conn.execute("""
        CREATE TABLE public.homepage_league_summary AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 142.7, 2024, 13),
            ('bad_league', 'Carol', 149.0, 2024, 12)
        ) AS t(db_name, highest_score_manager, highest_score_points, highest_score_year, highest_score_week)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_overview_current_standings_match_latest_matchup_season_uses_latest_year():
    from multi_league.validation_v2.checks.overview import CHECKS

    check = next(c for c in CHECKS if c.name == "overview_current_standings_match_latest_matchup_season")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup_season AS
        SELECT * FROM (VALUES
            ('history_league', 2023, 'Alice', 8, 6, 0, 1310.0, 97.4),
            ('history_league', 2023, 'Bob',   6, 8, 0, 1255.0, 91.2),
            ('history_league', 2024, 'Alice', 10, 4, 0, 1450.0, 103.2),
            ('history_league', 2024, 'Bob',    9, 5, 0, 1395.0, 99.8),
            ('bad_league',     2024, 'Carol', 11, 3, 0, 1502.5, 105.6),
            ('bad_league',     2024, 'Dave',   7, 7, 0, 1333.1, 95.1)
        ) AS t(db_name, year, manager, wins, losses, ties, points_scored_to_date, power_rating)
    """)
    conn.execute("""
        CREATE TABLE public.homepage_current_standings AS
        SELECT * FROM (VALUES
            ('history_league', 'Alice', 10, 4, 0, 1450.0, 103.2, 1),
            ('history_league', 'Bob',    9, 5, 0, 1395.0, 99.8, 2),
            ('bad_league',     'Carol', 11, 3, 0, 1490.0, 105.6, 1)
        ) AS t(db_name, manager, wins, losses, ties, points_for, power_rating, standings_rank)
    """)

    manifest = Manifest(all_leagues=["history_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["history_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_overview_points_for_allows_zero_but_rejects_negative_values():
    from multi_league.validation_v2.checks.overview import CHECKS

    check = next(c for c in CHECKS if c.name == "overview_points_for_positive")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.homepage_current_standings AS
        SELECT * FROM (VALUES
            ('scoreless_live_week', 'Alice', 0.0),
            ('bad_league', 'Bob', -1.0)
        ) AS t(db_name, manager, points_for)
        """
    )

    manifest = Manifest(all_leagues=["scoreless_live_week", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["scoreless_live_week"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()
