from pathlib import Path
import os
import time

import duckdb
import pytest


def test_second_storage_owner_is_rejected_until_first_releases(tmp_path):
    from storage_guard import StorageOwner, StorageOwnershipError

    first = StorageOwner(tmp_path)
    second = StorageOwner(tmp_path)

    first.acquire()
    try:
        with pytest.raises(StorageOwnershipError, match="already owned"):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()


def test_storage_owner_keeps_lock_file_for_diagnostics(tmp_path):
    from storage_guard import StorageOwner

    owner = StorageOwner(tmp_path)
    owner.acquire()
    lock_path = Path(tmp_path) / ".duckdb-server.lock"
    assert lock_path.exists()
    owner.release()
    assert str(owner.pid) in lock_path.read_text(encoding="utf-8")


def test_runtime_contract_accepts_matching_effective_settings(monkeypatch):
    from storage_guard import RuntimeContract

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("SET checkpoint_threshold='512MB'")
        conn.execute("SET memory_limit='1024MB'")
        conn.execute("SET threads=2")
        conn.execute("SET max_temp_directory_size='20GiB'")
        RuntimeContract(
            duckdb_version=f"v{duckdb.__version__}",
            checkpoint_threshold="512MB",
            memory_limit="1024MB",
            threads=2,
            max_temp_directory_size="20GiB",
        ).assert_matches(conn)
    finally:
        conn.close()


def test_runtime_contract_rejects_version_or_setting_drift():
    from storage_guard import RuntimeContract, RuntimeContractError

    conn = duckdb.connect(":memory:")
    try:
        effective = RuntimeContract.read(conn)
        bad = RuntimeContract(
            duckdb_version="v0.0.0",
            checkpoint_threshold=effective.checkpoint_threshold,
            memory_limit=effective.memory_limit,
            threads=effective.threads + 1,
            max_temp_directory_size=effective.max_temp_directory_size,
        )
        with pytest.raises(RuntimeContractError) as caught:
            bad.assert_matches(conn)
    finally:
        conn.close()

    message = str(caught.value)
    assert "duckdb_version" in message
    assert "threads" in message


def test_wal_policy_uses_only_file_metadata(tmp_path):
    from storage_guard import WalPolicy, wal_state

    wal_path = tmp_path / "___leagues.duckdb.wal"
    wal_path.write_bytes(b"x" * 1024)
    now = time.time()
    os.utime(wal_path, (now - 301, now - 301))

    state = wal_state(wal_path, now=wal_path.stat().st_mtime + 301)
    policy = WalPolicy(soft_bytes=2048, max_age_seconds=300, hard_bytes=4096)

    assert state.bytes == 1024
    assert 300 <= state.age_seconds <= 302
    assert policy.checkpoint_due(state) is True
    assert policy.write_blocked(state) is False


def test_wal_policy_blocks_at_hard_limit_and_ignores_missing_file(tmp_path):
    from storage_guard import WalPolicy, wal_state

    policy = WalPolicy(soft_bytes=128, max_age_seconds=300, hard_bytes=512)
    missing = wal_state(tmp_path / "missing.wal")
    assert missing.bytes == 0
    assert policy.checkpoint_due(missing) is False
    assert policy.write_blocked(missing) is False

    wal_path = tmp_path / "live.wal"
    wal_path.write_bytes(b"x" * 512)
    full = wal_state(wal_path)
    assert policy.checkpoint_due(full) is True
    assert policy.write_blocked(full) is True


def test_storage_health_fails_closed_and_preserves_first_fatal_error():
    from storage_guard import StorageHealth, StorageUnhealthyError

    health = StorageHealth()
    health.assert_writable()
    health.mark_unhealthy("checkpoint", OSError("checksum mismatch"))
    health.mark_unhealthy("later", RuntimeError("secondary"))

    with pytest.raises(StorageUnhealthyError, match="checksum mismatch"):
        health.assert_writable()
    snapshot = health.snapshot()
    assert snapshot["healthy"] is False
    assert snapshot["stage"] == "checkpoint"
    assert "secondary" not in snapshot["error"]


@pytest.mark.parametrize(
    "message",
    [
        "FATAL Error: database has been invalidated because of a previous fatal error",
        "IO Error: Corrupt database file: computed checksum 1 does not match stored checksum 2 in block",
        "Failed to create checkpoint: WAL replay failed",
    ],
)
def test_fatal_storage_error_classifies_database_integrity_failures(message):
    from storage_guard import is_fatal_storage_error

    assert is_fatal_storage_error(RuntimeError(message))


def test_fatal_storage_error_does_not_classify_bundle_checksum_validation():
    from storage_guard import is_fatal_storage_error

    assert not is_fatal_storage_error(ValueError("Parquet sha256 checksum mismatch"))
