import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[5] / "scripts" / "sleeper_corpus" / "batch_ingest_corpus.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("batch_ingest_corpus_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_load_env_enforces_offline_corpus_dependencies(tmp_path, monkeypatch):
    ops = tmp_path / "ops_cache.duckdb"
    draft = tmp_path / "draft_global_source.parquet"
    ops.write_bytes(b"ops")
    draft.write_bytes(b"draft")

    monkeypatch.setenv("OPS_CACHE_PATH", str(ops))
    monkeypatch.setenv("DRAFT_GLOBAL_SOURCE_PATH", str(draft))
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://production.example")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin")
    monkeypatch.setenv("FLY_API_TOKEN", "fly")

    module = _load_module()
    env = module.load_env(workers=4)

    assert env["CORPUS_MODE"] == "1"
    assert env["OPS_CACHE_PATH"] == str(ops)
    assert env["DRAFT_GLOBAL_SOURCE_PATH"] == str(draft)
    assert env["SLEEPER_RATE_LIMIT_PER_MIN"] == "250"
    for key in (
        "DATABASE_SERVER_URL",
        "DATABASE_READ_TOKEN",
        "DATABASE_ADMIN_TOKEN",
        "DATABASE_WRITE_TOKEN",
        "FLY_API_TOKEN",
        "FLY_PRIMARY_MACHINE_ID",
        "MOTHERDUCK_TOKEN",
    ):
        assert key not in env
