from __future__ import annotations

from dataclasses import dataclass

import duckdb

from multi_league.core.offseason_update_status import (
    classify_offseason_update_result,
    record_offseason_update_status,
)


@dataclass
class Result:
    status: str
    updated: bool = False
    error: str | None = None


class Writer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute(self, sql: str, *, database: str):
        self.calls.append((sql, database))
        return []


def _provision_dispatch_table(connection) -> None:
    connection.execute("""
        CREATE SCHEMA accounts;
        CREATE TABLE accounts.offseason_draft_update_dispatches (
            database_name VARCHAR NOT NULL,
            draft_year INTEGER NOT NULL,
            platform VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            workflow_file VARCHAR,
            workflow_run_id BIGINT,
            dispatch_token VARCHAR,
            dispatched_at TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            lease_expires_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT NOW(),
            error VARCHAR,
            PRIMARY KEY (database_name, draft_year)
        )
    """)


def test_classifies_real_updates_no_changes_and_provider_failures():
    assert classify_offseason_update_result(Result(status="changed", updated=True)) == "succeeded"
    assert classify_offseason_update_result(Result(status="up_to_date")) == "no_change"
    assert classify_offseason_update_result(Result(status="auth_missing")) == "failed"
    assert classify_offseason_update_result(Result(status="no_api_draft")) == "failed"


def test_records_a_terminal_worker_status_in_the_shared_fly_dispatch_row():
    writer = Writer()

    record_offseason_update_status(
        writer,
        database_name="o'hare_league",
        draft_year=2026,
        platform="espn",
        status="failed",
        workflow_run_id=12345,
        error="expired 'cookie'",
    )

    sql, database = writer.calls[-1]
    assert database == "___ops"
    assert "ON CONFLICT (database_name, draft_year) DO UPDATE" in sql
    assert "status = excluded.status" in sql
    assert "completed_at = excluded.completed_at" in sql
    assert "'o''hare_league'" in sql
    assert "'expired ''cookie'''" in sql


def test_running_status_sets_started_at_without_completing_the_job():
    writer = Writer()

    record_offseason_update_status(
        writer,
        database_name="kmffl",
        draft_year=2026,
        platform="yahoo",
        status="running",
        workflow_run_id=777,
        dispatch_token="current-claim",
    )

    sql, _database = writer.calls[-1]
    assert "'running'" in sql
    assert "NOW(), NULL" in sql
    assert "lease_expires_at = excluded.lease_expires_at" in sql


def test_status_upsert_executes_in_duckdb_and_preserves_started_at():
    connection = duckdb.connect()
    _provision_dispatch_table(connection)

    class DuckDBWriter:
        def execute(self, sql: str, *, database: str):
            assert database == "___ops"
            return connection.execute(sql)

    writer = DuckDBWriter()
    record_offseason_update_status(
        writer,
        database_name="kmffl",
        draft_year=2026,
        platform="yahoo",
        status="running",
        workflow_run_id=777,
        dispatch_token="current-claim",
    )
    record_offseason_update_status(
        writer,
        database_name="kmffl",
        draft_year=2026,
        platform="yahoo",
        status="no_change",
        workflow_run_id=777,
        dispatch_token="current-claim",
    )

    row = connection.execute(
        "SELECT status, started_at IS NOT NULL, completed_at IS NOT NULL "
        "FROM accounts.offseason_draft_update_dispatches"
    ).fetchone()
    assert row == ("no_change", True, True)


def test_stale_worker_cannot_overwrite_a_newer_claim():
    connection = duckdb.connect()
    _provision_dispatch_table(connection)

    class DuckDBWriter:
        def execute(self, sql: str, *, database: str):
            return connection.execute(sql)

    writer = DuckDBWriter()
    record_offseason_update_status(
        writer,
        database_name="kmffl",
        draft_year=2026,
        platform="yahoo",
        status="running",
        workflow_run_id=888,
        dispatch_token="new-claim",
    )
    stale_claimed = record_offseason_update_status(
        writer,
        database_name="kmffl",
        draft_year=2026,
        platform="yahoo",
        status="failed",
        workflow_run_id=777,
        dispatch_token="old-claim",
        error="late old worker",
    )

    row = connection.execute(
        "SELECT status, workflow_run_id, dispatch_token, error "
        "FROM accounts.offseason_draft_update_dispatches"
    ).fetchone()
    assert row == ("running", 888, "new-claim", None)
    assert stale_claimed is False
