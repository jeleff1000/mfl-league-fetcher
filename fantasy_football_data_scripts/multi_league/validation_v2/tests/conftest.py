"""Shared fixtures for validation_v2 tests."""

from __future__ import annotations

import pytest
import duckdb

from multi_league.validation_v2.models import Manifest


@pytest.fixture
def local_db():
    """Local DuckDB with 3 synthetic leagues for testing."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('good_league', 2024, 1, 'Alice', 'f_alice', 'Bob', 100.0, 90.0, 1, 0, 0, 10.0, 0, 0, NULL, NULL, 0),
            ('good_league', 2024, 1, 'Bob', 'f_bob', 'Alice', 90.0, 100.0, 0, 1, 0, -10.0, 0, 0, NULL, NULL, 0),
            ('good_league', 2024, 2, 'Alice', 'f_alice', 'Bob', 80.0, 85.0, 0, 1, 0, -5.0, 0, 0, NULL, NULL, 0),
            ('good_league', 2024, 2, 'Bob', 'f_bob', 'Alice', 85.0, 80.0, 1, 0, 0, 5.0, 0, 0, NULL, NULL, 0),
            ('bad_league', 2024, 1, 'Charlie', NULL, 'Dave', 100.0, 90.0, 1, 0, 0, 10.0, 0, 0, NULL, NULL, 0),
            ('bad_league', 2024, 1, 'Dave', 'f_dave', 'Charlie', 90.0, 100.0, 0, 1, 0, -10.0, 0, 0, NULL, NULL, 0),
            ('median_league', 2024, 1, 'Eve', 'f_eve', 'Frank', 110.0, 95.0, 1, 0, 0, 15.0, 0, 0, NULL, NULL, 0),
            ('median_league', 2024, 1, 'Frank', 'f_frank', 'Eve', 95.0, 110.0, 0, 1, 0, -15.0, 0, 0, NULL, NULL, 0)
        ) AS t(db_name, year, week, manager, franchise_id, opponent, team_points, opponent_points,
               win, loss, tie, margin, is_playoffs, is_consolation, champion, sacko, is_bye_week)
    """)
    yield conn
    conn.close()


@pytest.fixture
def manifest():
    """Manifest with 3 leagues and feature scoping."""
    return Manifest(
        all_leagues=["good_league", "bad_league", "median_league"],
        median_leagues=["median_league"],
        consolation_leagues=["good_league"],
        skip_lists={},
    )
