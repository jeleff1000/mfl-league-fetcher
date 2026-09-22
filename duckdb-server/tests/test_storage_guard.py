from pathlib import Path

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
            duckdb_version="v1.5.5",
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
