from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def _check(name: str):
    from multi_league.validation_v2.checks.players_weekly import CHECKS

    return next(c for c in CHECKS if c.name == name)


def _setup_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            position VARCHAR,
            fantasy_points DOUBLE,
            season_ppg DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER,
            roster_DB INTEGER,
            roster_DL INTEGER,
            roster_LB INTEGER,
            roster_IDP INTEGER,
            scoring_def_st_yd DOUBLE
        )
    """)


def test_players_extreme_points_allows_def_return_yard_league():
    conn = duckdb.connect(":memory:")
    _setup_tables(conn)
    conn.execute("""
        INSERT INTO public.player_fantasy VALUES
            ('return_yard_league', 2016, 'DEF', 179.5, NULL),
            ('regular_league', 2016, 'DEF', 179.5, NULL)
    """)
    conn.execute("""
        INSERT INTO public.league_settings VALUES
            ('return_yard_league', 2016, 0, 0, 0, 0, 0.5),
            ('regular_league', 2016, 0, 0, 0, 0, 0.0)
    """)

    manifest = Manifest(all_leagues=["return_yard_league", "regular_league"])
    results = run_sql_full(conn, _check("players_extreme_points"), manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["return_yard_league"].passed is True
    assert result_map["regular_league"].passed is False

    conn.close()


def test_players_ppg_range_allows_def_return_yard_league():
    conn = duckdb.connect(":memory:")
    _setup_tables(conn)
    conn.execute("""
        INSERT INTO public.player_fantasy VALUES
            ('return_yard_league', 2016, 'DEF', NULL, 59.18),
            ('regular_league', 2016, 'DEF', NULL, 59.18)
    """)
    conn.execute("""
        INSERT INTO public.league_settings VALUES
            ('return_yard_league', 2016, 0, 0, 0, 0, 0.5),
            ('regular_league', 2016, 0, 0, 0, 0, 0.0)
    """)

    manifest = Manifest(all_leagues=["return_yard_league", "regular_league"])
    results = run_sql_full(conn, _check("players_ppg_range"), manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["return_yard_league"].passed is True
    assert result_map["regular_league"].passed is False

    conn.close()


def test_players_fantasy_position_populated_ignores_stub_rows():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            manager VARCHAR,
            position VARCHAR,
            fantasy_position VARCHAR,
            is_started INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('stub_league', 2012, 'Hidden Manager', 'STUB', NULL, 0),
            ('stub_league', 2012, 'Real Manager', 'RB', 'RB', 1),
            ('real_gap_league', 2012, 'Real Manager', 'RB', 'RB', 1),
            ('real_gap_league', 2012, 'Other Manager', 'WR', NULL, 0)
        """
    )

    manifest = Manifest(all_leagues=["stub_league", "real_gap_league"])
    results = run_sql_full(conn, _check("players_fantasy_position_populated"), manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["stub_league"].passed is True
    assert result_map["real_gap_league"].passed is False

    conn.close()
