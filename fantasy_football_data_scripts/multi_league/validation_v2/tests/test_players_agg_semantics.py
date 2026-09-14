from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_aggregate_player_rates_use_started_rows_only():
    from multi_league.transformations.aggregation.aggregate_fantasy_context import _build_select_clauses

    clauses = _build_select_clauses(
        {
            "franchise_id",
            "clutch_equity",
            "win",
            "loss",
            "is_started",
            "is_playoffs",
            "is_championship",
        },
        "season",
    )

    for metric in ("clutch", "win", "loss", "championship"):
        assert "is_started" in clauses[metric]


def test_players_agg_season_games_match_weekly_catches_count_drift():
    from multi_league.validation_v2.checks.players_agg import CHECKS

    check = next(c for c in CHECKS if c.name == "players_agg_season_games_match_weekly")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.player_fantasy AS
        SELECT * FROM (VALUES
            ('good_league', 'p1', 2024, 1, 1),
            ('good_league', 'p1', 2024, 2, 0),
            ('bad_league',  'p2', 2024, 1, 1),
            ('bad_league',  'p2', 2024, 2, 0)
        ) AS t(db_name, NFL_player_id, year, week, is_started)
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy_season AS
        SELECT * FROM (VALUES
            ('good_league', 'p1', 2024, 1, 2),
            ('bad_league',  'p2', 2024, 2, 2)
        ) AS t(db_name, NFL_player_id, year, games_started, games_rostered)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_players_agg_career_matches_season_totals_catches_rollup_drift():
    from multi_league.validation_v2.checks.players_agg import CHECKS

    check = next(c for c in CHECKS if c.name == "players_agg_career_matches_season_totals")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.player_fantasy_season AS
        SELECT * FROM (VALUES
            ('good_league', 'p1', 2023, 120.0, 10.0, 8.0, 1.0, 5, 8, 5, 3, 1, 1, 0, 1, 2, 3),
            ('good_league', 'p1', 2024, 130.0, 12.0, 9.0, 2.0, 6, 9, 6, 3, 0, 0, 0, 0, 1, 2),
            ('bad_league',  'p2', 2023, 140.0, 11.0, 7.0, 0.5, 7, 9, 7, 2, 1, 1, 0, 1, 1, 2),
            ('bad_league',  'p2', 2024, 150.0, 13.0, 8.0, 0.7, 8, 10, 8, 2, 1, 0, 1, 0, 2, 1)
        ) AS t(
            db_name, NFL_player_id, year, fantasy_points, player_lamar, manager_lamar, clutch_equity,
            games_started, games_rostered, wins, losses, playoff_games, playoff_wins, playoff_losses,
            championships, optimal_player_count, league_wide_optimal_count
        )
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy_career AS
        SELECT * FROM (VALUES
            ('good_league', 'p1', 2023, 2024, 2, 250.0, 22.0, 17.0, 3.0, 11, 17, 11, 6, 1, 1, 0, 1, 3, 5),
            ('bad_league',  'p2', 2023, 2024, 2, 280.0, 24.0, 15.0, 1.2, 15, 19, 15, 4, 2, 1, 1, 1, 3, 3)
        ) AS t(
            db_name, NFL_player_id, first_year, last_year, years_active, fantasy_points, player_lamar, manager_lamar,
            clutch_equity, games_started, games_rostered, wins, losses, playoff_games, playoff_wins,
            playoff_losses, championships, optimal_player_count, league_wide_optimal_count
        )
    """)

    manifest = Manifest(
        all_leagues=["good_league", "bad_league"],
        multi_year_leagues=["good_league", "bad_league"],
    )
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_players_agg_season_vs_weekly_ignores_subcent_float_residue():
    from multi_league.validation_v2.checks.players_agg import CHECKS

    check = next(c for c in CHECKS if c.name == "players_agg_season_vs_weekly")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            fantasy_points DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE public.player_fantasy_season AS
        SELECT * FROM (VALUES
            ('good_league', 'p1', 2024, 0.0000000000000001)
        ) AS t(db_name, NFL_player_id, year, fantasy_points)
    """)

    manifest = Manifest(all_leagues=["good_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "good_league"
    assert results[0].passed is True

    conn.close()
