from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_txn_no_duplicates_uses_player_asset_fallback_for_null_nfl_ids():
    from multi_league.validation_v2.checks.transactions import CHECKS

    check = next(c for c in CHECKS if c.name == "txn_no_duplicates")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.transactions AS
        SELECT * FROM (VALUES
            ('good_league', 't1', NULL, NULL, NULL, NULL, 'Austin Jones', 'Alice', NULL, 'commish'),
            ('good_league', 't1', NULL, NULL, NULL, NULL, 'Ronald Jones', 'Alice', NULL, 'commish'),
            ('bad_league',  't2', NULL, NULL, NULL, NULL, 'Duplicate Jones', 'Bob', 'sent', 'trade'),
            ('bad_league',  't2', NULL, NULL, NULL, NULL, 'Duplicate Jones', 'Bob', 'sent', 'trade')
        ) AS t(
            db_name, transaction_id, NFL_player_id, yahoo_player_id, sleeper_player_id,
            espn_player_id, player, manager, trade_direction, transaction_type
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_txn_manager_season_matches_transactions_summary_catches_drift():
    from multi_league.validation_v2.checks.transactions import CHECKS

    check = next(c for c in CHECKS if c.name == "txn_manager_season_matches_transactions_summary")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.transactions AS
        SELECT * FROM (VALUES
            ('good_league', 'fid_a', 'Alice', 2024, 'add',  10.0, 3.0, 't1', 'P1', 'QB', NULL, NULL, NULL, NULL),
            ('good_league', 'fid_a', 'Alice', 2024, 'pickup', 5.0, 5.0, 't1b', 'P1b', 'WR', NULL, NULL, NULL, NULL),
            ('good_league', 'fid_a', 'Alice', 2024, 'drop',  0.0, -1.0, 't2', 'P2', 'RB', NULL, NULL, NULL, NULL),
            ('good_league', 'fid_a', 'Alice', 2024, 'trade', 0.0, 0.0, 't3', 'P3', 'WR', 'Bob', 2025, 1, 'Carol'),
            ('bad_league',  'fid_c', 'Carol', 2024, 'add',  20.0, 4.0, 't4', 'P4', 'QB', NULL, NULL, NULL, NULL),
            ('bad_league',  'fid_c', 'Carol', 2024, 'drop',  0.0, -2.0, 't5', 'P5', 'RB', NULL, NULL, NULL, NULL),
            ('bad_league',  'fid_c', 'Carol', 2024, 'trade', 0.0, 0.0, 't6', 'P6', 'WR', 'Dave', 2025, 1, 'Eve')
        ) AS t(
            db_name, franchise_id, manager, year, transaction_type, faab_bid, transaction_score,
            transaction_id, player, position, source_manager, traded_pick_season, traded_pick_round,
            traded_pick_original_owner
        )
    """)
    conn.execute("""
        CREATE TABLE public.transaction_manager_season AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 2024, 'fid_a', 2, 1, 3, 15.0, 2.3333333333333335, 1),
            ('bad_league',  'Carol', 2024, 'fid_c', 1, 1, 2, 18.0, 2.0, 1)
        ) AS t(
            db_name, manager, year, franchise_id, adds, drops, total_moves,
            total_faab_bid, total_transaction_score, trades
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_txn_manager_career_matches_season_rollup_catches_drift():
    from multi_league.validation_v2.checks.transactions import CHECKS

    check = next(c for c in CHECKS if c.name == "txn_manager_career_matches_season_rollup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.transaction_manager_season AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 2023, 'fid_a', 2, 1, 3, 20.0, 5.0, 1),
            ('good_league', 'Alice', 2024, 'fid_a', 1, 2, 3, 10.0, 4.0, 2),
            ('bad_league',  'Carol', 2023, 'fid_c', 2, 1, 3, 25.0, 6.0, 1),
            ('bad_league',  'Carol', 2024, 'fid_c', 1, 2, 3, 15.0, 5.0, 2)
        ) AS t(
            db_name, manager, year, franchise_id, adds, drops, total_moves,
            total_faab_bid, total_transaction_score, trades
        )
    """)
    conn.execute("""
        CREATE TABLE public.transaction_manager_career AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 'fid_a', 2, 3, 3, 6, 30.0, 4.5, 3),
            ('bad_league',  'Carol', 'fid_c', 1, 3, 3, 6, 35.0, 11.0, 3)
        ) AS t(
            db_name, manager, franchise_id, seasons, total_adds, total_drops,
            total_moves, total_faab_bid, total_transaction_score, total_trades
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_txn_player_career_matches_transactions_rollup_catches_drift():
    from multi_league.validation_v2.checks.transactions import CHECKS

    check = next(c for c in CHECKS if c.name == "txn_player_career_matches_transactions_rollup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.transactions AS
        SELECT * FROM (VALUES
            ('good_league', 'Mahomes', 'QB', 2023, 'add',       12.0, 't1', 'Alice', 'Bob',   2025, 1, 'Carol'),
            ('good_league', 'Mahomes', 'QB', 2024, 'pickup',     8.0, 't1b', 'Alice', 'Bob',   2025, 1, 'Carol'),
            ('good_league', 'Mahomes', 'QB', 2024, 'drop',       0.0, 't2', 'Alice', 'Bob',   2025, 1, 'Carol'),
            ('good_league', 'Mahomes', 'QB', 2024, 'trade',      0.0, 't3', 'Alice', 'Bob',   2025, 1, 'Carol'),
            ('good_league', 'Mahomes', 'QB', 2024, 'trade',      0.0, 't3', 'Charlie', 'Bob', 2025, 1, 'Carol'),
            ('bad_league',  'Jefferson', 'WR', 2023, 'add',     18.0, 't4', 'Carol', 'Dave',  2026, 2, 'Eve'),
            ('bad_league',  'Jefferson', 'WR', 2024, 'trade',    0.0, 't5', 'Carol', 'Dave',  2026, 2, 'Eve'),
            ('bad_league',  'Jefferson', 'WR', 2024, 'trade',    0.0, 't5', 'Frank', 'Dave',  2026, 2, 'Eve')
        ) AS t(
            db_name, player, position, year, transaction_type, faab_bid, transaction_id,
            manager, source_manager, traded_pick_season, traded_pick_round, traded_pick_original_owner
        )
    """)
    conn.execute("""
        CREATE TABLE public.transaction_player_career AS
        SELECT * FROM (VALUES
            ('good_league', 'Mahomes', 'QB', 2, 1, 1, 20.0, 2),
            ('bad_league',  'Jefferson', 'WR', 1, 0, 1, 15.0, 2)
        ) AS t(
            db_name, player, position, times_added, times_dropped,
            times_traded, total_faab_spent, years_active
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_txn_report_card_matches_manager_season_catches_drift():
    from multi_league.validation_v2.checks.transactions import CHECKS

    check = next(c for c in CHECKS if c.name == "txn_report_card_matches_manager_season")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.transaction_manager_season AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 2024, 'fid_a', 3, 2, 1, 25.0, 7.5, 'A', 3.8),
            ('bad_league',  'Carol', 2024, 'fid_c', 2, 1, 1, 20.0, 5.0, 'B', 3.0)
        ) AS t(
            db_name, manager, year, franchise_id, adds, drops, trades,
            total_faab_bid, net_lamar, transaction_grade, transaction_gpa
        )
    """)
    conn.execute("""
        CREATE TABLE public.transaction_report_card AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 2024, 'fid_a', 3, 2, 1, 25.0, 7.5, 'A', 3.8),
            ('bad_league',  'Carol', 2024, 'fid_c', 2, 1, 1, 18.0, 5.0, 'B', 3.0)
        ) AS t(
            db_name, manager, year, franchise_id, adds, drops, trades,
            total_faab_bid, total_lamar, transaction_grade, transaction_gpa
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()
