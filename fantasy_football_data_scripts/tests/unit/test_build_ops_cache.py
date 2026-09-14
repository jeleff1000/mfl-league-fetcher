import json
import sys
import urllib.error
from pathlib import Path

import duckdb
import pytest

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import build_ops_cache


def test_build_cache_fly_scopes_schema_to_current_catalog(monkeypatch, tmp_path):
    """Duplicate attached catalogs must not duplicate every remote column."""
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")

    def fake_query(_url, _token, sql, *, timeout, max_retries=4):
        if "information_schema.columns" in sql and "nfl_player_stats_all" in sql:
            rows = [
                {"column_name": "year", "data_type": "INTEGER"},
                {"column_name": "xp%", "data_type": "DOUBLE"},
            ]
            return rows if "table_catalog = current_database()" in sql else rows + rows
        if "information_schema.columns" in sql and "player_bio" in sql:
            rows = [{"column_name": "NFL_player_id", "data_type": "VARCHAR"}]
            return rows if "table_catalog = current_database()" in sql else rows + rows
        if "SELECT DISTINCT year FROM nfl_historical.nfl_player_stats_all" in sql:
            return [{"year": 2025}]
        if "FROM nfl_historical.nfl_player_stats_all WHERE year = 2025" in sql:
            return [{"year": 2025, "xp%": 0.95}]
        if "FROM nfl_historical.player_bio" in sql:
            return [{"NFL_player_id": "00-1"}]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fake_query)

    output = tmp_path / "ops_cache.duckdb"
    build_ops_cache.build_cache_fly(str(output))

    conn = duckdb.connect(str(output), read_only=True)
    try:
        columns = conn.execute("DESCRIBE nfl_historical.nfl_player_stats_all").fetchall()
    finally:
        conn.close()
    assert [row[0] for row in columns].count("xp%") == 1


def test_build_cache_fly_replaces_required_tables_and_writes_metadata(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")

    def fake_query(_url, _token, sql, *, timeout, max_retries=4):
        if "information_schema.columns" in sql and "nfl_player_stats_all" in sql:
            return [
                {"column_name": "year", "data_type": "INTEGER"},
                {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
                {"column_name": "player_week", "data_type": "VARCHAR"},
            ]
        if "information_schema.columns" in sql and "player_bio" in sql:
            return [
                {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
                {"column_name": "player", "data_type": "VARCHAR"},
            ]
        if "SELECT DISTINCT year FROM nfl_historical.nfl_player_stats_all" in sql:
            return [{"year": 2025}]
        if "FROM nfl_historical.nfl_player_stats_all WHERE year = 2025" in sql:
            return [{"year": 2025, "NFL_player_id": "00-1", "player_week": "00-1_2025_1"}]
        if "FROM nfl_historical.player_bio" in sql:
            return [{"NFL_player_id": "00-1", "player": "Test Player"}]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fake_query)
    output = tmp_path / "ops_cache.duckdb"

    build_ops_cache.build_cache_fly(str(output))

    conn = duckdb.connect(str(output), read_only=True)
    try:
        stats_count = conn.execute("SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all").fetchone()[0]
        bio_count = conn.execute("SELECT COUNT(*) FROM nfl_historical.player_bio").fetchone()[0]
        metadata = conn.execute(
            "SELECT source_backend, table_name, rows, cols FROM ops_cache_metadata ORDER BY table_name"
        ).fetchall()
    finally:
        conn.close()

    assert stats_count == 1
    assert bio_count == 1
    assert metadata == [
        ("fly", "nfl_player_stats_all", 1, 3),
        ("fly", "player_bio", 1, 2),
    ]


def test_build_cache_fly_uses_fly_schema_not_first_chunk_inference(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")

    repaired_col = "def_identity_repaired_at_20260504"

    def fake_query(_url, _token, sql, *, timeout, max_retries=4):
        if "information_schema.columns" in sql and "nfl_player_stats_all" in sql:
            return [
                {"column_name": "year", "data_type": "INTEGER"},
                {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
                {"column_name": repaired_col, "data_type": "TIMESTAMP"},
            ]
        if "information_schema.columns" in sql and "player_bio" in sql:
            return [
                {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
                {"column_name": "player", "data_type": "VARCHAR"},
            ]
        if "SELECT DISTINCT year FROM nfl_historical.nfl_player_stats_all" in sql:
            return [{"year": 1920}, {"year": 2025}]
        if "FROM nfl_historical.nfl_player_stats_all WHERE year = 1920" in sql:
            return [{"year": 1920, "NFL_player_id": "old-1", repaired_col: None}]
        if "FROM nfl_historical.nfl_player_stats_all WHERE year = 2025" in sql:
            return [{"year": 2025, "NFL_player_id": "new-1", repaired_col: "2026-05-04T03:53:13.252663"}]
        if "FROM nfl_historical.player_bio" in sql:
            return [{"NFL_player_id": "new-1", "player": "Test Player"}]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fake_query)
    output = tmp_path / "ops_cache.duckdb"

    build_ops_cache.build_cache_fly(str(output))

    conn = duckdb.connect(str(output), read_only=True)
    try:
        data_type = conn.execute(
            f"SELECT column_type FROM (DESCRIBE nfl_historical.nfl_player_stats_all) WHERE column_name = '{repaired_col}'"
        ).fetchone()[0]
        repaired_value = conn.execute(
            f"SELECT {repaired_col} FROM nfl_historical.nfl_player_stats_all WHERE year = 2025"
        ).fetchone()[0]
    finally:
        conn.close()

    assert data_type == "TIMESTAMP"
    assert repaired_value.isoformat() == "2026-05-04T03:53:13.252663"


def test_build_cache_fly_fails_loudly_when_required_table_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")
    monkeypatch.setattr(build_ops_cache, "_fly_query_json", lambda *_args, **_kwargs: [])

    with pytest.raises(RuntimeError, match="No schema found for required ops table"):
        build_ops_cache.build_cache_fly(str(tmp_path / "ops_cache.duckdb"))


def test_update_cache_fly_prunes_legacy_overlay_tables(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")

    output = tmp_path / "ops_cache.duckdb"
    conn = duckdb.connect(str(output))
    try:
        conn.execute("CREATE SCHEMA nfl_historical")
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (year INTEGER, NFL_player_id VARCHAR)")
        conn.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES (2024, 'old')")
        conn.execute("CREATE TABLE nfl_historical.player_bio (NFL_player_id VARCHAR, player VARCHAR)")
        conn.execute("INSERT INTO nfl_historical.player_bio VALUES ('old', 'Old Player')")
        conn.execute("CREATE TABLE public.yahoo_nfl_player_map (yahoo_player_id VARCHAR)")
        conn.execute("INSERT INTO public.yahoo_nfl_player_map VALUES ('legacy')")
    finally:
        conn.close()

    def fake_query(_url, _token, sql, *, timeout, max_retries=4):
        if "information_schema.columns" in sql and "nfl_player_stats_all" in sql:
            return [
                {"column_name": "year", "data_type": "INTEGER"},
                {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
            ]
        if "information_schema.columns" in sql and "player_bio" in sql:
            return [
                {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
                {"column_name": "player", "data_type": "VARCHAR"},
            ]
        if "COUNT(*) AS rows FROM nfl_historical.nfl_player_stats_all" in sql:
            return [{"year": 2024, "rows": 1}, {"year": 2025, "rows": 1}]
        if "FROM nfl_historical.nfl_player_stats_all WHERE year = 2025" in sql:
            return [{"year": 2025, "NFL_player_id": "new"}]
        if "FROM nfl_historical.player_bio" in sql:
            return [{"NFL_player_id": "new", "player": "New Player"}]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fake_query)

    build_ops_cache.update_cache_fly(str(output))

    conn = duckdb.connect(str(output), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM nfl_historical.player_bio").fetchone()[0] == 1
        assert (
            conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'yahoo_nfl_player_map'
                """
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


def test_fly_query_json_retries_transient_url_errors(monkeypatch):
    calls = {"count": 0}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps([{"ok": True}]).encode()

    def fake_urlopen(_req, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise urllib.error.URLError("temporary reset")
        return FakeResponse()

    monkeypatch.setattr(build_ops_cache.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(build_ops_cache.time, "sleep", lambda _delay: None)

    rows = build_ops_cache._fly_query_json("https://fly.test/query", "read-token", "SELECT 1", timeout=1)

    assert rows == [{"ok": True}]
    assert calls["count"] == 2


def test_checkpoint_manifest_accepts_matching_chunk_and_rejects_count_mismatch(tmp_path):
    schema_rows = [
        {"column_name": "year", "data_type": "INTEGER"},
        {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
    ]
    rows = [
        {"year": 2024, "NFL_player_id": "a"},
        {"year": 2025, "NFL_player_id": "b"},
    ]
    checkpoint = tmp_path / "batch.duckdb"

    manifest = build_ops_cache.write_checkpoint(
        checkpoint,
        table="nfl_player_stats_all",
        batch_id="nfl_player_stats_all-batch-00",
        schema_rows=schema_rows,
        rows=rows,
        years=[2024, 2025],
    )

    assert build_ops_cache.validate_checkpoint(checkpoint, manifest)
    invalid = {**manifest, "row_count": 3}
    assert not build_ops_cache.validate_checkpoint(checkpoint, invalid)


def test_download_checkpoint_batch_reuses_valid_checkpoint_without_fly(monkeypatch, tmp_path):
    schema_rows = [
        {"column_name": "year", "data_type": "INTEGER"},
        {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
    ]
    checkpoint = tmp_path / "nfl_player_stats_all-batch-00.duckdb"
    build_ops_cache.write_checkpoint(
        checkpoint,
        table="nfl_player_stats_all",
        batch_id="nfl_player_stats_all-batch-00",
        schema_rows=schema_rows,
        rows=[{"year": 2025, "NFL_player_id": "a"}],
        years=[2025],
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("valid checkpoint should not contact Fly")

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fail_if_called)
    result = build_ops_cache.download_checkpoint_batch(
        tmp_path,
        "nfl_player_stats_all-batch-00",
        "https://fly.test/query",
        "read-token",
        expected_manifest={
            "table": "nfl_player_stats_all",
            "batch_id": "nfl_player_stats_all-batch-00",
            "schema_rows": schema_rows,
            "years": [2025],
        },
    )
    assert result == checkpoint


def test_single_checkpoint_plan_omits_empty_future_year(monkeypatch):
    schema_rows = [
        {"column_name": "year", "data_type": "INTEGER"},
        {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
    ]

    def fake_query(_url, _token, sql, **_kwargs):
        if "information_schema.columns" in sql:
            return schema_rows
        return [{"rows": 0}]

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fake_query)
    plan = build_ops_cache.build_single_checkpoint_plan(
        "https://fly.test/query", "read-token", "nfl_player_stats_all-batch-106"
    )

    assert plan["years"] == []
    assert plan["year_counts"] == {}
    assert plan["row_count"] == 0


def test_download_checkpoint_batch_pages_large_year(monkeypatch, tmp_path):
    schema_rows = [
        {"column_name": "year", "data_type": "INTEGER"},
        {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
    ]
    monkeypatch.setattr(build_ops_cache, "_YEAR_PAGE_SIZE", 2)
    calls = []

    def fake_query(_url, _token, sql, **_kwargs):
        calls.append(sql)
        offset = int(sql.rsplit(" OFFSET ", 1)[1])
        rows = [
            {"year": 2025, "NFL_player_id": "a"},
            {"year": 2025, "NFL_player_id": "b"},
            {"year": 2025, "NFL_player_id": "c"},
        ]
        return rows[offset : offset + 2]

    monkeypatch.setattr(build_ops_cache, "_fly_query_json", fake_query)
    checkpoint = build_ops_cache.download_checkpoint_batch(
        tmp_path,
        "nfl_player_stats_all-batch-00",
        "https://fly.test/query",
        "read-token",
        expected_manifest={
            "table": "nfl_player_stats_all",
            "batch_id": "nfl_player_stats_all-batch-00",
            "schema_rows": schema_rows,
            "years": [2025],
            "year_counts": {"2025": 3},
            "row_count": 3,
        },
    )

    assert checkpoint.is_file()
    assert [" OFFSET 0" in sql for sql in calls] == [True, False]
    conn = duckdb.connect(str(checkpoint), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all").fetchone()[0] == 3
    finally:
        conn.close()


def test_assemble_checkpoints_is_atomic_and_validates_all_chunks(tmp_path):
    schema_rows = [
        {"column_name": "year", "data_type": "INTEGER"},
        {"column_name": "NFL_player_id", "data_type": "VARCHAR"},
    ]
    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()
    first = build_ops_cache.write_checkpoint(
        checkpoint_dir / "nfl_player_stats_all-batch-00.duckdb",
        table="nfl_player_stats_all",
        batch_id="nfl_player_stats_all-batch-00",
        schema_rows=schema_rows,
        rows=[{"year": 2024, "NFL_player_id": "a"}],
        years=[2024],
    )
    second = build_ops_cache.write_checkpoint(
        checkpoint_dir / "nfl_player_stats_all-batch-01.duckdb",
        table="nfl_player_stats_all",
        batch_id="nfl_player_stats_all-batch-01",
        schema_rows=schema_rows,
        rows=[{"year": 2025, "NFL_player_id": "b"}],
        years=[2025],
    )
    output = tmp_path / "ops_cache.duckdb"

    build_ops_cache.assemble_checkpoints(
        output,
        checkpoint_dir,
        {"nfl_player_stats_all-batch-00": first, "nfl_player_stats_all-batch-01": second},
    )
    conn = duckdb.connect(str(output), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all").fetchone()[0] == 2
    finally:
        conn.close()

    output.write_bytes(b"known-good")
    second["row_count"] = 99
    with pytest.raises(RuntimeError, match="Invalid checkpoint"):
        build_ops_cache.assemble_checkpoints(
            output,
            checkpoint_dir,
            {"nfl_player_stats_all-batch-00": first, "nfl_player_stats_all-batch-01": second},
        )
    assert output.read_bytes() == b"known-good"
