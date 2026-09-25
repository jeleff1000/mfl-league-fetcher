"""Shared DuckDB connection guardrails stay compatible during disk churn."""

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
import importlib.util


@pytest.mark.parametrize("failure", [
    duckdb.IOException("Corrupt database file: checksum mismatch"),
    duckdb.FatalException("database has been invalidated"),
    duckdb.ConnectionException("cannot open database with a different configuration"),
])
def test_open_failure_is_not_retried_with_weaker_guardrails(tmp_path, monkeypatch, failure):
    import db as db_mod

    calls = []

    def fail_open(path, **kwargs):
        calls.append(kwargs)
        raise failure

    monkeypatch.setattr(db_mod.duckdb, "connect", fail_open)
    with pytest.raises(type(failure)) as caught:
        db_mod.connect_database(tmp_path / "broken.duckdb", data_dir=tmp_path)
    assert caught.value is failure
    assert len(calls) == 1
    assert calls[0]["config"]["checkpoint_threshold"] == db_mod.DEFAULT_DUCKDB_CHECKPOINT_THRESHOLD
    assert "memory_limit" in calls[0]["config"]
    assert "max_temp_directory_size" in calls[0]["config"]


def test_default_checkpoint_threshold_is_bounded_for_shared_database():
    import db as db_mod

    assert db_mod.DEFAULT_DUCKDB_CHECKPOINT_THRESHOLD == "512MB"


def test_storage_recovery_mode_applies_to_every_new_connection(tmp_path):
    import db as db_mod

    baseline = db_mod.connect_database(tmp_path / "recovery.duckdb", data_dir=tmp_path)
    db_mod.set_storage_recovery_mode(True)
    try:
        assert (
            db_mod.duckdb_connection_config(tmp_path)["checkpoint_threshold"]
            == db_mod.DEFAULT_DUCKDB_CHECKPOINT_THRESHOLD
        )
        conn = db_mod.connect_database(tmp_path / "recovery.duckdb", data_dir=tmp_path)
        try:
            assert "TiB" in conn.execute(
                "SELECT current_setting('checkpoint_threshold')"
            ).fetchone()[0]
        finally:
            conn.close()
    finally:
        db_mod.set_storage_recovery_mode(False)
        baseline.close()


def test_production_fly_config_does_not_leave_wal_checkpointing_disabled():
    fly_toml = (Path(__file__).parents[1] / "fly.toml").read_text(encoding="utf-8")

    assert "DUCKDB_CHECKPOINT_THRESHOLD = '512MB'" in fly_toml
    assert "DUCKDB_CHECKPOINT_WAL_MB = '512'" in fly_toml
    assert "DUCKDB_CHECKPOINT_WAL_MB = '0'" not in fly_toml


def test_ops_application_schema_is_provisioned_once_and_then_read_only(tmp_path):
    import db as db_mod

    conn = db_mod.connect_database(tmp_path / "___ops.duckdb", data_dir=tmp_path)
    try:
        first = db_mod.ensure_ops_credential_schema(conn)
        second = db_mod.ensure_ops_credential_schema(conn)
        credential_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = 'league_credentials'"
            ).fetchall()
        }
        inventory_columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'accounts' AND table_name = 'league_inventory'"
            ).fetchall()
        }
        application_tables = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema IN ('main', 'accounts')"
            ).fetchall()
        }
    finally:
        conn.close()

    assert first
    assert second == []
    assert {
        "league_id",
        "league_name",
        "database_name",
        "encrypted_refresh_token",
        "updated_at",
    } <= credential_columns
    assert {
        "database_name",
        "platform",
        "league_name",
        "league_id",
        "tier",
        "entitled_mode",
        "has_credentials",
        "last_import_at",
        "created_at",
        "updated_at",
    } <= inventory_columns
    assert {
        ("main", "league_credentials"),
        ("main", "yahoo_web_credentials"),
        ("main", "espn_leagues"),
        ("main", "sleeper_leagues"),
        ("main", "research_extraction_cache"),
        ("accounts", "league_inventory"),
        ("accounts", "pending_paid_imports"),
        ("accounts", "stripe_processed_sessions"),
        ("accounts", "paid_import_dispatches"),
        ("accounts", "league_update_manifests"),
        ("accounts", "league_update_dispatches"),
        ("accounts", "league_update_rate_buckets"),
        ("accounts", "league_update_probe_leases"),
        ("accounts", "offseason_draft_update_dispatches"),
    } <= application_tables


def test_init_pool_rechecks_ops_schema_after_existing_reader_is_closed(tmp_path, monkeypatch):
    import db as db_mod

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DB_POOL_SIZE", "1")
    try:
        db_mod.init_pool()
        db_mod.init_pool()
        assert db_mod.get_ops_connection() is not None
    finally:
        db_mod.close_all()


def test_init_pool_attaches_ops_before_exposing_public_pool(tmp_path, monkeypatch):
    """A pool reopen must establish the shared OPS attachment first.

    Opening public pool handles before attaching ``___ops`` leaves multiple
    handles on the shared DuckDB instance while the attachment is recreated.
    DuckDB can then reject the attach with a unique-file-handle conflict after
    an otherwise successful narrow write.
    """
    import db as db_mod

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DB_POOL_SIZE", "1")
    original_refill = db_mod._refill_pool
    observed: list[set[str]] = []

    def assert_ops_attached_then_refill(leagues_path):
        control = db_mod.get_ops_connection()
        assert control is not None
        catalogs = {
            row[0]
            for row in control.execute(
                "SELECT database_name FROM duckdb_databases()"
            ).fetchall()
        }
        observed.append(catalogs)
        assert "___ops" in catalogs
        original_refill(leagues_path)

    monkeypatch.setattr(db_mod, "_refill_pool", assert_ops_attached_then_refill)
    try:
        db_mod.init_pool()
        assert len(observed) == 1
        assert {"___leagues", "___ops"} <= observed[0]
    finally:
        db_mod.close_all()


def test_temp_limit_is_fixed_for_connections_to_same_database(tmp_path, monkeypatch):
    import db as db_mod

    free_bytes = [9 * 1024**3]
    monkeypatch.setattr(
        db_mod.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=free_bytes[0]),
    )

    db_path = tmp_path / "shared.duckdb"
    first = db_mod.connect_database(db_path, data_dir=tmp_path)
    try:
        first.execute("CREATE TABLE witness (value INTEGER)")
        free_bytes[0] = 5 * 1024**3
        second = db_mod.connect_database(db_path, data_dir=tmp_path)
        try:
            assert second.execute("SELECT COUNT(*) FROM witness").fetchone() == (0,)
        finally:
            second.close()
    finally:
        first.close()


def test_reader_and_writer_threads_do_not_change_database_open_configuration(tmp_path):
    import db as db_mod

    path = tmp_path / "shared_read_write.duckdb"
    reader = db_mod.connect_database(path, data_dir=tmp_path, threads=1)
    try:
        reader.execute("CREATE TABLE witness (value INTEGER)")
        writer = db_mod.connect_database(path, data_dir=tmp_path, threads=2)
        try:
            writer.execute("INSERT INTO witness VALUES (7)")
            assert reader.execute("SELECT value FROM witness").fetchall() == [(7,)]
        finally:
            writer.close()
    finally:
        reader.close()


@pytest.mark.parametrize("read_threads,write_threads,expected", [(1, 2, 2), (4, 2, 4)])
def test_shared_connection_keeps_configured_write_parallelism(tmp_path, monkeypatch, read_threads, write_threads, expected):
    import db as db_mod

    monkeypatch.setenv("DUCKDB_THREADS", str(read_threads))
    monkeypatch.setenv("DUCKDB_WRITE_THREADS", str(write_threads))
    spec = importlib.util.spec_from_file_location("isolated_db_thread_config", db_mod.__file__)
    configured = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(configured)
    conn = configured.connect_database(tmp_path / "thread_witness.duckdb", data_dir=tmp_path)
    try:
        assert conn.execute("SELECT current_setting('threads')").fetchone() == (expected,)
    finally:
        conn.close()
