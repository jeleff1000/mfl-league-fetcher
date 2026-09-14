from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_draft_manager_season_matches_draft_totals_catches_rollup_drift():
    from multi_league.validation_v2.checks.draft import CHECKS

    check = next(c for c in CHECKS if c.name == "draft_manager_season_matches_draft_totals")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.draft AS
        SELECT * FROM (VALUES
            ('good_league', 'fid_a', 'Alice', 2024, 'standard', 0, 25.0, 'P1', 'QB', 'QB'),
            ('good_league', 'fid_a', 'Alice', 2024, 'standard', 1, 12.0, 'P2', 'RB', 'RB'),
            ('good_league', 'fid_b', 'Bob',   2024, 'standard', 0, 10.0, 'P3', 'WR', 'WR'),
            ('bad_league',  'fid_c', 'Carol', 2024, 'standard', 0, 30.0, 'P4', 'QB', 'QB'),
            ('bad_league',  'fid_c', 'Carol', 2024, 'standard', 1, 15.0, 'P5', 'RB', 'RB')
        ) AS t(
            db_name, franchise_id, manager, year, draft_category, is_keeper, cost,
            player, yahoo_position, position
        )
    """)
    conn.execute("""
        CREATE TABLE public.draft_manager_season AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 2024, 'fid_a', 'standard', 1, 1, 25.0),
            ('good_league', 'Bob',   2024, 'fid_b', 'standard', 1, 0, 10.0),
            ('bad_league',  'Carol', 2024, 'fid_c', 'standard', 1, 1, 28.0)
        ) AS t(db_name, manager, year, franchise_id, draft_category, picks, keeper_picks, total_cost)
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_draft_manager_career_matches_season_rollup_catches_drift():
    from multi_league.validation_v2.checks.draft import CHECKS

    check = next(c for c in CHECKS if c.name == "draft_manager_career_matches_season_rollup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.draft_manager_season AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 2023, 'fid_a', 'standard', 2, 0, 45.0),
            ('good_league', 'Alice', 2024, 'fid_a', 'standard', 1, 1, 25.0),
            ('bad_league',  'Carol', 2023, 'fid_c', 'standard', 2, 0, 50.0),
            ('bad_league',  'Carol', 2024, 'fid_c', 'standard', 1, 1, 35.0)
        ) AS t(db_name, manager, year, franchise_id, draft_category, picks, keeper_picks, total_cost)
    """)
    conn.execute("""
        CREATE TABLE public.draft_manager_career AS
        SELECT * FROM (VALUES
            ('good_league', 'Alice', 'fid_a', 'standard', 2, 3, 1, 70.0),
            ('bad_league',  'Carol', 'fid_c', 'standard', 1, 3, 1, 85.0)
        ) AS t(
            db_name, manager, franchise_id, draft_category, years_active,
            total_picks, total_keeper_picks, total_cost
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_draft_player_career_matches_draft_rollup_catches_drift():
    from multi_league.validation_v2.checks.draft import CHECKS

    check = next(c for c in CHECKS if c.name == "draft_player_career_matches_draft_rollup")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.draft AS
        SELECT * FROM (VALUES
            ('good_league', 'Mahomes', 'QB', 'QB', 'standard', 0, 30.0),
            ('good_league', 'Mahomes', 'QB', 'QB', 'standard', 1, 18.0),
            ('bad_league',  'Jefferson', 'WR', 'WR', 'standard', 0, 40.0),
            ('bad_league',  'Jefferson', 'WR', 'WR', 'standard', 1, 20.0)
        ) AS t(db_name, player, yahoo_position, position, draft_category, is_keeper, cost)
    """)
    conn.execute("""
        CREATE TABLE public.draft_player_career AS
        SELECT * FROM (VALUES
            ('good_league', 'Mahomes', 'QB', 'standard', 1, 1, 30.0, 18.0),
            ('bad_league',  'Jefferson', 'WR', 'standard', 1, 1, 35.0, 20.0)
        ) AS t(
            db_name, player, position, draft_category, times_drafted,
            times_kept, total_cost, keeper_cost
        )
    """)

    manifest = Manifest(all_leagues=["good_league", "bad_league"])
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_league"].passed is True
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    conn.close()


def test_draft_category_populated_ignores_non_sleeper_dynasty_leagues():
    from multi_league.validation_v2.checks.draft import CHECKS
    from multi_league.validation_v2.executor import build_batch_sql, run_batch

    check = next(c for c in CHECKS if c.name == "draft_category_populated")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.draft AS
        SELECT * FROM (VALUES
            ('espn_dynasty', 'espn', NULL),
            ('espn_dynasty', 'espn', NULL),
            ('yahoo_dynasty', 'yahoo', NULL),
            ('sleeper_dynasty', 'sleeper', NULL),
            ('sleeper_dynasty', 'sleeper', 'rookie')
        ) AS t(db_name, platform, draft_category)
    """)

    manifest = Manifest(
        all_leagues=["espn_dynasty", "yahoo_dynasty", "sleeper_dynasty"],
        dynasty_leagues=["espn_dynasty", "yahoo_dynasty", "sleeper_dynasty"],
    )
    sql = build_batch_sql(
        [check],
        table="draft",
        manifest=manifest,
        feature="dynasty",
        skip_sets={},
        table_prefix="public.",
    )
    results = run_batch(conn, sql, [check], manifest, skip_sets={})
    result_map = {r.db_name: r for r in results}

    assert result_map["espn_dynasty"].passed is True
    assert result_map["yahoo_dynasty"].passed is True
    assert result_map["sleeper_dynasty"].passed is False
    assert result_map["sleeper_dynasty"].fail_count == 1

    conn.close()
