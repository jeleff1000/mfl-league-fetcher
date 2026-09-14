from __future__ import annotations

import duckdb

from multi_league.validation_v2.checks.system import CHECKS
from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def _system_check():
    return next(check for check in CHECKS if check.name == "system_team_points_vs_player_sum")


def _manifest(*leagues: str) -> Manifest:
    return Manifest(
        all_leagues=list(leagues),
        median_leagues=[],
        consolation_leagues=[],
        skip_lists={},
    )


def _setup_system_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            adjustment DOUBLE,
            is_bye_week INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            platform VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            is_started INTEGER,
            fantasy_points DOUBLE
        )
        """
    )


def test_system_team_points_vs_player_sum_allows_espn_adjustment():
    conn = duckdb.connect(":memory:")
    _setup_system_tables(conn)
    conn.execute(
        """
        INSERT INTO public.matchup VALUES
            ('sparty_party', 'Garnier Fructis', 2024, 5, 176.5, 12.44, 0, 0, 0, 'espn')
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('sparty_party', 'Garnier Fructis', 2024, 5, 1, 164.06)
        """
    )

    results = run_sql_full(conn, _system_check(), _manifest("sparty_party"), table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "sparty_party"
    assert results[0].fail_count == 0
    assert results[0].passed is True


def test_system_team_points_vs_player_sum_still_flags_real_gap_after_adjustment():
    conn = duckdb.connect(":memory:")
    _setup_system_tables(conn)
    conn.execute(
        """
        INSERT INTO public.matchup VALUES
            ('still_bad', 'Commissioner Chaos', 2024, 6, 150.0, 5.0, 0, 0, 0, 'espn'),
            ('still_bad', 'Commissioner Chaos', 2024, 7, 150.0, 5.0, 0, 0, 0, 'espn'),
            ('still_bad', 'Commissioner Chaos', 2024, 8, 150.0, 5.0, 0, 0, 0, 'espn'),
            ('still_bad', 'Commissioner Chaos', 2024, 9, 150.0, 5.0, 0, 0, 0, 'espn')
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('still_bad', 'Commissioner Chaos', 2024, 6, 1, 130.0),
            ('still_bad', 'Commissioner Chaos', 2024, 7, 1, 130.0),
            ('still_bad', 'Commissioner Chaos', 2024, 8, 1, 130.0),
            ('still_bad', 'Commissioner Chaos', 2024, 9, 1, 130.0)
        """
    )

    results = run_sql_full(conn, _system_check(), _manifest("still_bad"), table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "still_bad"
    assert results[0].fail_count == 4
    assert results[0].passed is False
