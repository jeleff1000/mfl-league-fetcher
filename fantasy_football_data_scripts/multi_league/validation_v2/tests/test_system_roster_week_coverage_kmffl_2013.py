"""KMFFL 2013 scope-out for system_roster_week_coverage.

KMFFL 2013 was uploaded externally with matchup-level data only — no
draft_data_2013, no yahoo_player_stats_2013, no transactions_2013. The
matchup parquet exists with 11 managers' weekly scores, but only 2 of
those managers ever appear in player_fantasy (carryover). The remaining
9-managers × ~14-weeks = ~84 (manager, week) keys legitimately have no
roster data and shouldn't fire the system_roster_week_coverage ERROR.

Mirrors the pre-2019 ESPN scope-out pattern in
system_team_points_vs_player_sum.
"""

from __future__ import annotations

import duckdb

from multi_league.validation_v2.checks.system import CHECKS
from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def _check():
    return next(c for c in CHECKS if c.name == "system_roster_week_coverage")


def _manifest(*leagues: str) -> Manifest:
    return Manifest(
        all_leagues=list(leagues),
        median_leagues=[],
        consolation_leagues=[],
        skip_lists={},
    )


def _setup_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            is_bye_week INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )


def test_kmffl_2013_matchup_only_external_upload_is_scoped_out():
    """KMFFL 2013 has matchup data but missing roster data — known external
    upload gap, not a pipeline bug. Must not fire."""
    conn = duckdb.connect(":memory:")
    _setup_tables(conn)
    conn.execute(
        """
        INSERT INTO public.matchup VALUES
            ('kmffl', 'Alex', 2013, 1, 100.0, 0),
            ('kmffl', 'Alex', 2013, 2, 110.0, 0),
            ('kmffl', 'Bob', 2013, 1, 105.0, 0),
            ('kmffl', 'Bob', 2013, 2, 115.0, 0)
        """
    )
    # No player_fantasy rows for kmffl 2013 — that's the data gap

    results = run_sql_full(conn, _check(), _manifest("kmffl"), table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "kmffl"
    assert results[0].fail_count == 0
    assert results[0].passed is True


def test_kmffl_2014_plus_still_validated():
    """KMFFL 2014+ should still fire if roster data is genuinely missing —
    only 2013 is the matchup-only external upload."""
    conn = duckdb.connect(":memory:")
    _setup_tables(conn)
    conn.execute(
        """
        INSERT INTO public.matchup VALUES
            ('kmffl', 'Alex', 2014, 1, 100.0, 0),
            ('kmffl', 'Alex', 2014, 2, 110.0, 0)
        """
    )
    # No player_fantasy for kmffl 2014 — this would be a real pipeline bug

    results = run_sql_full(conn, _check(), _manifest("kmffl"), table_prefix="public.")

    assert len(results) == 1
    assert results[0].db_name == "kmffl"
    assert results[0].fail_count == 2
    assert results[0].passed is False


def test_other_leagues_still_validated_for_2013():
    """Scope-out is keyed on (db_name, year), not year alone. A different
    league's 2013 with missing roster is still a real failure."""
    conn = duckdb.connect(":memory:")
    _setup_tables(conn)
    conn.execute(
        """
        INSERT INTO public.matchup VALUES
            ('other_league', 'Carol', 2013, 1, 100.0, 0)
        """
    )

    results = run_sql_full(conn, _check(), _manifest("other_league"), table_prefix="public.")

    assert len(results) == 1
    assert results[0].fail_count == 1
    assert results[0].passed is False
