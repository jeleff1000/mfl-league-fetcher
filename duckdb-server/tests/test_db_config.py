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


def test_production_fly_config_does_not_leave_wal_checkpointing_disabled():
    fly_toml = (Path(__file__).parents[1] / "fly.toml").read_text(encoding="utf-8")

    assert "DUCKDB_CHECKPOINT_THRESHOLD = '512MB'" in fly_toml
    assert "DUCKDB_CHECKPOINT_WAL_MB = '512'" in fly_toml
    assert "DUCKDB_CHECKPOINT_WAL_MB = '0'" not in fly_toml


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
