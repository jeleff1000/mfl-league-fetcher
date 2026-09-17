"""Shared DuckDB connection guardrails stay compatible during disk churn."""

from pathlib import Path
from types import SimpleNamespace


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
