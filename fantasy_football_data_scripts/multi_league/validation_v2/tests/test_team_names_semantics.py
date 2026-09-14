from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import build_batch_sql, run_batch
from multi_league.validation_v2.models import Manifest


def _run_team_name_checks(conn: duckdb.DuckDBPyConnection):
    from multi_league.validation_v2.checks.team_names import CHECKS

    manifest = Manifest(all_leagues=["fallback_ok", "missing_name"])
    sql = build_batch_sql(
        CHECKS,
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets={},
        table_prefix="public.",
    )
    return run_batch(conn, sql, CHECKS, manifest, skip_sets={})


def test_team_name_checks_allow_manager_or_franchise_fallback():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('fallback_ok', 'fid_1', NULL, 'Franchise Label', 'Alice'),
            ('fallback_ok', 'fid_2', '',   'Franchise Label', 'Alice')
        ) AS t(db_name, franchise_id, team_name, franchise_name, manager)
    """)

    results = _run_team_name_checks(conn)
    result_map = {(r.db_name, r.check.name): r for r in results}

    assert result_map[("fallback_ok", "teamnames_every_franchise_has_name")].passed is True
    assert result_map[("fallback_ok", "teamnames_name_not_empty")].passed is True

    conn.close()


def test_team_name_checks_still_flag_missing_display_identity():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup AS
        SELECT * FROM (VALUES
            ('missing_name', 'fid_1', NULL, NULL, NULL),
            ('missing_name', 'fid_2', '',   '',   '')
        ) AS t(db_name, franchise_id, team_name, franchise_name, manager)
    """)

    results = _run_team_name_checks(conn)
    result_map = {(r.db_name, r.check.name): r for r in results}

    assert result_map[("missing_name", "teamnames_every_franchise_has_name")].passed is False
    assert result_map[("missing_name", "teamnames_every_franchise_has_name")].fail_count == 1
    assert result_map[("missing_name", "teamnames_name_not_empty")].passed is False
    assert result_map[("missing_name", "teamnames_name_not_empty")].fail_count == 1

    conn.close()
