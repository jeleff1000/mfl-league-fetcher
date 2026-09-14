from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_luck_h2h_season_matches_all_play_rollup_catches_drift():
    from multi_league.validation_v2.checks.luck import CHECKS

    check = next(c for c in CHECKS if c.name == "luck_h2h_season_matches_all_play_rollup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.all_play AS
        SELECT * FROM (VALUES
            ('good_league', 'fid_a', 'fid_b', 2024, 'W'),
            ('good_league', 'fid_a', 'fid_b', 2024, 'L'),
            ('good_league', 'fid_a', 'fid_b', 2024, 'T'),
            ('bad_league',  'fid_c', 'fid_d', 2024, 'W'),
            ('bad_league',  'fid_c', 'fid_d', 2024, 'W'),
            ('bad_league',  'fid_c', 'fid_d', 2024, 'L')
        ) AS t(db_name, franchise_id, opponent_franchise_id, year, result)
    """)
    conn.execute("""
        CREATE TABLE public.h2h_season AS
        SELECT * FROM (VALUES
            ('good_league', 'fid_a', 'fid_b', 2024, 1, 1, 1, 3),
            ('bad_league',  'fid_c', 'fid_d', 2024, 1, 1, 0, 2)
        ) AS t(db_name, franchise_id, opponent_franchise_id, year, wins, losses, ties, games)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_luck_schedule_swap_season_matches_rollup_catches_drift():
    from multi_league.validation_v2.checks.luck import CHECKS

    check = next(c for c in CHECKS if c.name == "luck_schedule_swap_season_matches_rollup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.schedule_swap AS
        SELECT * FROM (VALUES
            ('good_league', 'fid_a', 'fid_b', 2024, 'W'),
            ('good_league', 'fid_a', 'fid_b', 2024, 'L'),
            ('bad_league',  'fid_c', 'fid_d', 2024, 'W'),
            ('bad_league',  'fid_c', 'fid_d', 2024, 'L'),
            ('bad_league',  'fid_c', 'fid_d', 2024, 'T')
        ) AS t(db_name, franchise_id, schedule_of_franchise_id, year, result)
    """)
    conn.execute("""
        CREATE TABLE public.schedule_swap_season AS
        SELECT * FROM (VALUES
            ('good_league', 'fid_a', 'fid_b', 2024, 1, 1, 0, 2),
            ('bad_league',  'fid_c', 'fid_d', 2024, 2, 0, 0, 2)
        ) AS t(db_name, franchise_id, schedule_of_franchise_id, year, wins, losses, ties, games)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()
