"""Tests for swap module."""

import pytest
import duckdb
from swap import validate_db_file, atomic_swap, startup_recovery, SwapError


def test_validate_db_file_accepts_valid_db(tmp_path):
    path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE x (id INT)")
    conn.execute("INSERT INTO x VALUES (1)")
    conn.close()
    assert validate_db_file(path) is True


def test_validate_db_file_rejects_corrupt(tmp_path):
    path = tmp_path / "bad.duckdb"
    path.write_bytes(b"not a database")
    with pytest.raises(SwapError, match="integrity"):
        validate_db_file(path)


def test_atomic_swap_replaces_file(tmp_path):
    # Create "old" DB
    old = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(old))
    conn.execute("CREATE TABLE t (v VARCHAR)")
    conn.execute("INSERT INTO t VALUES ('old')")
    conn.close()

    # Create "new" DB
    incoming = tmp_path / "___leagues.duckdb.incoming"
    conn = duckdb.connect(str(incoming))
    conn.execute("CREATE TABLE t (v VARCHAR)")
    conn.execute("INSERT INTO t VALUES ('new')")
    conn.close()

    result = atomic_swap(tmp_path, "___leagues")

    # Verify new content
    conn = duckdb.connect(str(old), read_only=True)
    val = conn.execute("SELECT v FROM t").fetchone()[0]
    conn.close()
    assert val == "new"

    # Verify .prev exists
    assert (tmp_path / "___leagues.duckdb.prev").exists()
    assert result["status"] == "swapped"


def test_atomic_swap_no_incoming_raises(tmp_path):
    with pytest.raises(SwapError, match="No incoming"):
        atomic_swap(tmp_path, "___leagues")


def test_atomic_swap_corrupt_incoming_raises(tmp_path):
    # Create valid live
    live = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(live))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    # Create corrupt incoming
    incoming = tmp_path / "___leagues.duckdb.incoming"
    incoming.write_bytes(b"corrupt data here")

    with pytest.raises(SwapError, match="integrity"):
        atomic_swap(tmp_path, "___leagues")

    # Live should still be intact
    assert live.exists()


def test_startup_recovery_cleans_stale_incoming(tmp_path):
    incoming = tmp_path / "___leagues.duckdb.incoming"
    incoming.write_bytes(b"stale upload")

    live = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(live))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    startup_recovery(tmp_path)
    assert not incoming.exists()
    assert live.exists()


def test_cleanup_stale_uploads(tmp_path):
    """Stale upload files are removed and duckdb_tmp dir is created."""
    from main import cleanup_stale_uploads

    # Create fake stale files
    (tmp_path / "league_upload_the_league.duckdb").write_bytes(b"stale1")
    (tmp_path / "league_upload_the_league.duckdb.wal").write_bytes(b"stale2")
    (tmp_path / "league_upload_nyu_ffl.duckdb").write_bytes(b"stale3")
    # This file should NOT be removed (not matching the pattern)
    (tmp_path / "___leagues.duckdb").write_bytes(b"keep")

    cleanup_stale_uploads(tmp_path)

    assert not (tmp_path / "league_upload_the_league.duckdb").exists()
    assert not (tmp_path / "league_upload_the_league.duckdb.wal").exists()
    assert not (tmp_path / "league_upload_nyu_ffl.duckdb").exists()
    assert (tmp_path / "___leagues.duckdb").exists()
    assert (tmp_path / "duckdb_tmp").is_dir()


def test_startup_recovery_restores_prev_when_live_missing(tmp_path):
    prev = tmp_path / "___leagues.duckdb.prev"
    conn = duckdb.connect(str(prev))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    startup_recovery(tmp_path)
    assert (tmp_path / "___leagues.duckdb").exists()
    assert not prev.exists()
