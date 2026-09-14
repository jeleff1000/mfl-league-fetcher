from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_players_one_manager_per_week_ignores_phantom_bye_rows():
    from multi_league.validation_v2.checks.players_weekly import CHECKS

    check = next(c for c in CHECKS if c.name == "players_one_manager_per_week")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            ('bye_league', 2024, 17, '00-0032764', 'Active Manager', 0, 0),
            ('bye_league', 2024, 17, '00-0032764', 'Phantom Bye Manager', 1, 0)
        ) AS t(db_name, year, week, NFL_player_id, manager, is_bye_week, optimal_player)
    """)

    manifest = Manifest(all_leagues=["bye_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "bye_league"
    assert results[0].passed is True

    conn.close()


def test_optimal_no_duplicates_ignores_phantom_bye_rows():
    from multi_league.validation_v2.checks.optimal_lineup import CHECKS

    check = next(c for c in CHECKS if c.name == "optimal_no_duplicates")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            ('bye_league', 2024, 17, '00-0032764', 1, 0),
            ('bye_league', 2024, 17, '00-0032764', 1, 1)
        ) AS t(db_name, year, week, NFL_player_id, optimal_player, is_bye_week)
    """)

    manifest = Manifest(all_leagues=["bye_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "bye_league"
    assert results[0].passed is True

    conn.close()


def test_optimal_gte_team_points_ignores_bye_rows():
    from multi_league.validation_v2.checks.optimal_lineup import CHECKS
    from multi_league.validation_v2.executor import build_batch_sql, run_batch

    check = next(c for c in CHECKS if c.name == "optimal_gte_team_points")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('bye_league', 2024, 15, 'Manager A', 0.0, 181.24, 1, 'espn')
        ) AS t(db_name, year, week, manager, optimal_points, starter_points, is_bye_week, platform)
    """)

    manifest = Manifest(all_leagues=["bye_league"])
    sql = build_batch_sql(
        checks=[check],
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets={},
        table_prefix="public.",
    )
    results = run_batch(conn, sql, [check], manifest, {})

    assert len(results) == 1
    assert results[0].db_name == "bye_league"
    assert results[0].passed is True

    conn.close()
