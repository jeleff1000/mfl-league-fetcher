"""Test for identity_external_synthetic_guids check."""

from __future__ import annotations

import duckdb

from multi_league.validation_v2.checks.manager_identity import CHECKS
from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def _check_by_name(name: str):
    for c in CHECKS:
        if c.name == name:
            return c
    raise KeyError(name)


def test_check_registered_with_valid_shape():
    check = _check_by_name("identity_external_synthetic_guids")
    check.validate()  # raises on bad shape
    assert check.severity == "INFO"
    assert check.table == "matchup"
    assert check.sql_full is not None


def test_check_flags_synthetic_external_guids():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE matchup (db_name VARCHAR, manager_guid VARCHAR, franchise_id VARCHAR)")
    conn.execute(
        """
        INSERT INTO matchup VALUES
            ('demo', 'real_guid_1', 'f_1'),
            ('demo', 'real_guid_2', 'f_2'),
            ('demo', 'external_abc123def456', 'f_3'),
            ('demo', 'external_xyz789ghi012', 'f_4')
    """
    )
    check = _check_by_name("identity_external_synthetic_guids")
    manifest = Manifest(all_leagues=["demo"])
    results = run_sql_full(conn, check, manifest, table_prefix="")
    by_db = {r.db_name: r.fail_count for r in results}
    assert by_db.get("demo") == 2  # 2 distinct external_* guids


def test_check_reports_nothing_when_no_external_guids():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE matchup (db_name VARCHAR, manager_guid VARCHAR, franchise_id VARCHAR)")
    conn.execute(
        """
        INSERT INTO matchup VALUES
            ('demo', 'real_guid_1', 'f_1'),
            ('demo', 'real_guid_2', 'f_2')
    """
    )
    check = _check_by_name("identity_external_synthetic_guids")
    manifest = Manifest(all_leagues=["demo"])
    results = run_sql_full(conn, check, manifest, table_prefix="")
    by_db = {r.db_name: r.fail_count for r in results}
    # When no external_* guids exist, GROUP BY returns empty set
    # and fail_count defaults to 0
    assert by_db.get("demo", 0) == 0


def test_check_ignores_external_guids_with_null_franchise_id():
    """Synthetic external_* guids without a franchise_id should not be counted."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE matchup (db_name VARCHAR, manager_guid VARCHAR, franchise_id VARCHAR)")
    conn.execute(
        """
        INSERT INTO matchup VALUES
            ('demo', 'real_guid_1', 'f_1'),
            ('demo', 'external_abc123def456', NULL)
    """
    )
    check = _check_by_name("identity_external_synthetic_guids")
    manifest = Manifest(all_leagues=["demo"])
    results = run_sql_full(conn, check, manifest, table_prefix="")
    by_db = {r.db_name: r.fail_count for r in results}
    # Only count external_* guids that have franchise_id IS NOT NULL
    assert by_db.get("demo", 0) == 0
