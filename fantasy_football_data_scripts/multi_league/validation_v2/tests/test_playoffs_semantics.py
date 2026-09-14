from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_playoffs_sacko_has_consolation_game_allows_excluded_bottom_seed():
    from multi_league.validation_v2.checks.playoffs import CHECKS

    check = next(c for c in CHECKS if c.name == "playoffs_sacko_has_consolation_game")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('excluded_seed_ok', 2024, 15, 'fid_12', 12, 1, 0, 0, 'opp', 72.0, 91.0),
            ('missing_consolation_bug', 2024, 15, 'fid_11', 11, 1, 0, 0, 'opp', 72.0, 91.0),
            ('good_consolation_seed', 2024, 15, 'fid_10', 10, 1, 0, 1, 'opp', 72.0, 91.0)
        ) AS t(
            db_name, year, week, franchise_id, final_playoff_seed, sacko,
            is_playoffs, is_consolation, opponent, team_points, opponent_points
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('excluded_seed_ok', 2024, 12, 7, 4, 15),
            ('missing_consolation_bug', 2024, 12, 7, 4, 15),
            ('good_consolation_seed', 2024, 12, 7, 4, 15)
        ) AS t(db_name, year, num_teams, playoff_teams, num_playoff_consolation_teams, playoff_start_week)
        """
    )

    manifest = Manifest(
        all_leagues=["excluded_seed_ok", "missing_consolation_bug", "good_consolation_seed"],
        consolation_leagues=["excluded_seed_ok", "missing_consolation_bug", "good_consolation_seed"],
    )
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["excluded_seed_ok"].passed is True
    assert result_map["excluded_seed_ok"].fail_count == 0

    assert result_map["missing_consolation_bug"].passed is False
    assert result_map["missing_consolation_bug"].fail_count == 1

    assert result_map["good_consolation_seed"].passed is True
    assert result_map["good_consolation_seed"].fail_count == 0

    conn.close()


def test_playoffs_sacko_has_consolation_game_allows_no_real_postseason_row():
    from multi_league.validation_v2.checks.playoffs import CHECKS

    check = next(c for c in CHECKS if c.name == "playoffs_sacko_has_consolation_game")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('phantom_sacko_ok', 2024, 14, 'fid_6', 6, 1, 0, 0, NULL, NULL, NULL),
            ('real_missing_flag_bug', 2024, 14, 'fid_6', 6, 1, 0, 0, 'opp', 72.0, 91.0),
            ('good_consolation_seed', 2024, 14, 'fid_6', 6, 1, 0, 1, 'opp', 72.0, 91.0)
        ) AS t(
            db_name, year, week, franchise_id, final_playoff_seed, sacko,
            is_playoffs, is_consolation, opponent, team_points, opponent_points
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('phantom_sacko_ok', 2024, 10, 4, 4, 14),
            ('real_missing_flag_bug', 2024, 10, 4, 4, 14),
            ('good_consolation_seed', 2024, 10, 4, 4, 14)
        ) AS t(db_name, year, num_teams, playoff_teams, num_playoff_consolation_teams, playoff_start_week)
        """
    )

    manifest = Manifest(
        all_leagues=["phantom_sacko_ok", "real_missing_flag_bug", "good_consolation_seed"],
        consolation_leagues=["phantom_sacko_ok", "real_missing_flag_bug", "good_consolation_seed"],
    )
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["phantom_sacko_ok"].passed is True
    assert result_map["real_missing_flag_bug"].passed is False
    assert result_map["real_missing_flag_bug"].fail_count == 1
    assert result_map["good_consolation_seed"].passed is True

    conn.close()
