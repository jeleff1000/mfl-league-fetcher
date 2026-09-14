from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def test_keepers_config_global_row_exists_catches_missing_year_zero():
    from multi_league.validation_v2.checks.keepers import CHECKS

    check = next(c for c in CHECKS if c.name == "keepers_config_global_row_exists")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.accounts")
    conn.execute("""
        CREATE TABLE ___ops.accounts.league_inventory AS
        SELECT * FROM (VALUES
            ('good_keeper', TRUE),
            ('bad_keeper', TRUE)
        ) AS t(database_name, in_centralized)
    """)
    conn.execute(
        "CREATE TABLE public.league_settings AS SELECT * FROM (VALUES ('good_keeper'), ('bad_keeper')) AS t(db_name)"
    )
    conn.execute("CREATE TABLE public.matchup AS SELECT * FROM (VALUES ('good_keeper'), ('bad_keeper')) AS t(db_name)")
    conn.execute(
        "CREATE TABLE public.player_fantasy AS SELECT * FROM (VALUES ('good_keeper'), ('bad_keeper')) AS t(db_name)"
    )
    conn.execute("""
        CREATE TABLE public.keeper_config AS
        SELECT * FROM (VALUES
            ('good_keeper', 0, '{"enabled": true}'),
            ('bad_keeper',  2024, '{"enabled": true}')
        ) AS t(db_name, year, rules_json)
    """)

    manifest = Manifest(
        all_leagues=["good_keeper", "bad_keeper"],
        keeper_leagues=["good_keeper", "bad_keeper"],
    )
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_keeper"].passed is True
    assert result_map["bad_keeper"].passed is False
    assert result_map["bad_keeper"].fail_count == 1

    conn.close()


def test_keepers_config_required_scalars_populated_catches_null_required_fields():
    from multi_league.validation_v2.checks.keepers import CHECKS

    check = next(c for c in CHECKS if c.name == "keepers_config_required_scalars_populated")

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.keeper_config (
            db_name VARCHAR, year INT, enabled BOOLEAN, draft_type VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO public.keeper_config VALUES
            ('good_keeper', 0, TRUE, 'snake'),
            ('bad_keeper',  0, NULL, NULL)
    """)

    manifest = Manifest(
        all_leagues=["good_keeper", "bad_keeper"],
        keeper_leagues=["good_keeper", "bad_keeper"],
    )
    results = run_sql_full(conn, check, manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["good_keeper"].passed is True
    assert result_map["bad_keeper"].passed is False
    assert result_map["bad_keeper"].fail_count == 1

    conn.close()
