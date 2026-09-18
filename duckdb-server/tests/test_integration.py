"""Integration tests for the full FastAPI app."""

import hashlib
import json
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Event
from unittest.mock import patch

import duckdb
import pytest

from fastapi.testclient import TestClient


def _json_hash(payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_hash(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _make_delta_bundle(
    tmp_path,
    main_mod,
    *,
    db_name="test_league",
    rows=None,
    bundle_id=None,
    import_run_id="1000",
    publish_sequence=1,
    base_generation=None,
):
    rows = rows or [
        (db_name, 2026, 1, "alice_2026_1", "Alice"),
        (db_name, 2026, 1, "bob_2026_1", "Bob"),
    ]
    base_dir = tmp_path / f"delta_{db_name}_{bundle_id or import_run_id}_{publish_sequence}"
    tables_dir = base_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = tables_dir / "matchup.parquet"
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager_week VARCHAR,
            manager VARCHAR
        )
        """
    )
    conn.executemany("INSERT INTO matchup VALUES (?, ?, ?, ?, ?)", rows)
    conn.execute(f"COPY matchup TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    columns = [
        {"name": "db_name", "type": "VARCHAR"},
        {"name": "year", "type": "INTEGER"},
        {"name": "week", "type": "INTEGER"},
        {"name": "manager_week", "type": "VARCHAR"},
        {"name": "manager", "type": "VARCHAR"},
    ]
    table_entry = {
        "table": "matchup",
        "included": True,
        "path": "tables/matchup.parquet",
        "format": "parquet",
        "compression": "default",
        "sha256": _file_hash(parquet_path),
        "row_count": len(rows),
        "columns": columns,
        "partition_keys": ["db_name"],
        "identity_keys": ["db_name", "manager_week"],
        "primary_keys": ["db_name", "manager_week"],
        "merge_mode": "replace_league",
        "scope": {"db_name": db_name},
        "kind": "core",
        "source_tables": [],
        "empty_reason": None,
        "fingerprints": {
            "row_count": len(rows),
            "content_hash": hashlib.sha256(repr(rows).encode("utf-8")).hexdigest(),
            "primary_key_hash": hashlib.sha256(repr([(row[0], row[3]) for row in rows]).encode("utf-8")).hexdigest(),
            "duplicate_primary_keys": 0,
            "key_null_counts": {"db_name": 0, "manager_week": 0},
            "min_year": 2026,
            "max_year": 2026,
            "min_week": 1,
            "max_week": 1,
        },
    }
    omitted_tables = [
        {"table": table, "reason": "table_not_present_locally", "required_for_full": False}
        for table in sorted(main_mod._DELTA_ALLOWED_TABLES - {"matchup"})
    ]
    logical_payload = {
        "manifest_version": main_mod._DELTA_MANIFEST_VERSION,
        "schema_version": main_mod._DELTA_SCHEMA_VERSION,
        "db_name": db_name,
        "league_id": None,
        "platform": "sleeper",
        "import_mode": "quick",
        "import_run_id": str(import_run_id),
        "publish_sequence": publish_sequence,
        "tables": [
            {
                "table": table_entry["table"],
                "row_count": table_entry["row_count"],
                "columns": table_entry["columns"],
                "partition_keys": table_entry["partition_keys"],
                "primary_keys": table_entry["primary_keys"],
                "merge_mode": table_entry["merge_mode"],
                "scope": table_entry["scope"],
                "fingerprints": table_entry["fingerprints"],
            }
        ],
        "omitted_tables": omitted_tables,
    }
    if base_generation is not None:
        logical_payload["base_generation"] = base_generation
    bundle_hash = _json_hash(logical_payload)
    manifest = {
        **logical_payload,
        "tables": [table_entry],
        "producer": "test",
        "producer_version": "test",
        "created_at": "2026-05-06T00:00:00+00:00",
        "bundle_id": bundle_id or f"{db_name}-{import_run_id}-{publish_sequence}-{bundle_hash[:12]}",
        "bundle_hash": bundle_hash,
    }
    manifest_path = base_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
    archive_path = base_dir / f"{manifest['bundle_id']}.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(manifest_path, arcname="manifest.json")
        archive.add(parquet_path, arcname="tables/matchup.parquet")
    return archive_path, manifest


@pytest.fixture
def data_dir(tmp_path):
    """Create a temp data dir with a test database."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (year INT, week INT, manager VARCHAR)")
    conn.execute("INSERT INTO public.matchup VALUES (2024, 1, 'Alice'), (2024, 2, 'Bob')")
    conn.close()
    return tmp_path


@pytest.fixture
def client(data_dir):
    """Create a test client with the app configured to use tmp data."""
    env = {
        "DATA_DIR": str(data_dir),
        "DB_POOL_SIZE": "2",
        "DATABASE_READ_TOKEN": "test-read",
        "DATABASE_ADMIN_TOKEN": "test-admin",
    }
    with patch.dict("os.environ", env):
        # Must import after patching env
        import importlib
        import db as db_mod
        import main as main_mod

        importlib.reload(db_mod)
        importlib.reload(main_mod)
        db_mod.close_all()
        with TestClient(main_mod.app) as tc:
            try:
                yield tc
            finally:
                db_mod.close_all()
                main_mod._state["status"] = "serving"
                main_mod._ops_write_count = 0


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["state"] == "serving"


def test_ready_endpoint(client):
    resp = client.get("/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "serving"


def test_ready_endpoint_returns_503_until_serving(client):
    import main as main_mod

    main_mod._state["status"] = "starting"
    try:
        resp = client.get("/ready")
    finally:
        main_mod._state["status"] = "serving"

    assert resp.status_code == 503
    assert resp.headers["retry-after"] == "2"
    body = resp.json()
    assert body["status"] == "starting"
    assert body["ready"] is False
    assert body["accepting_queries"] is False


def test_rename_league_requires_admin_auth(client):
    response = client.post(
        "/rename-league",
        json={
            "source_db": "old_league",
            "target_db": "new_league",
            "display_name": "New League",
            "operation_id": "rename-new-league",
        },
    )

    assert response.status_code == 401


def test_rename_league_runs_one_server_side_operation(client, monkeypatch):
    import main as main_mod

    calls = []
    handoff = []
    real_close_all = main_mod.db.close_all

    def drain_ops():
        handoff.append("drain_ops")

    def close_all():
        handoff.append("close_all")
        real_close_all()

    def fake_rename(*, data_dir, source_db, target_db, display_name, operation_id):
        calls.append((data_dir, source_db, target_db, display_name, operation_id))
        return {
            "status": "COMMITTED",
            "source_db": source_db,
            "target_db": target_db,
            "target_years": [2009, 2026],
        }

    monkeypatch.setattr(main_mod, "_rename_league_server_side", fake_rename)
    monkeypatch.setattr(main_mod, "_drain_ops_attachments_for_snapshot", drain_ops)
    monkeypatch.setattr(main_mod.db, "close_all", close_all)
    response = client.post(
        "/rename-league",
        headers={"Authorization": "Bearer test-admin"},
        json={
            "source_db": "agustafantasyleague",
            "target_db": "agusta_fantasy_league",
            "display_name": "Agusta Fantasy League",
            "operation_id": "rename-agusta",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "COMMITTED",
        "source_db": "agustafantasyleague",
        "target_db": "agusta_fantasy_league",
        "target_years": [2009, 2026],
    }
    assert calls == [
        (
            main_mod.db.get_data_dir(),
            "agustafantasyleague",
            "agusta_fantasy_league",
            "Agusta Fantasy League",
            "rename-agusta",
        )
    ]
    assert handoff[:2] == ["drain_ops", "close_all"]


def test_rename_league_rejects_invalid_target_before_write(client, monkeypatch):
    import main as main_mod

    monkeypatch.setattr(main_mod, "_rename_league_server_side", lambda **_: None)
    response = client.post(
        "/rename-league",
        headers={"Authorization": "Bearer test-admin"},
        json={
            "source_db": "old_league",
            "target_db": "Not A Slug",
            "display_name": "New League",
            "operation_id": "rename-new-league",
        },
    )

    assert response.status_code == 400


def test_server_side_rename_reuses_one_ops_connection(data_dir, monkeypatch):
    import main as main_mod

    leagues_path = data_dir / "___leagues.duckdb"
    leagues = duckdb.connect(str(leagues_path))
    leagues.execute("ALTER TABLE public.matchup ADD COLUMN db_name VARCHAR")
    leagues.execute("UPDATE public.matchup SET db_name = 'old_league'")
    leagues.close()

    ops_path = data_dir / "___ops.duckdb"
    ops = duckdb.connect(str(ops_path))
    ops.execute("CREATE SCHEMA IF NOT EXISTS main")
    ops.execute("CREATE TABLE main.league_credentials(database_name VARCHAR, token VARCHAR)")
    ops.execute("INSERT INTO main.league_credentials VALUES ('old_league', 'encrypted')")
    ops.close()

    real_connect = main_mod.db.connect_database
    ops_opens = 0

    def connect_once(path, **kwargs):
        nonlocal ops_opens
        if str(path) == str(ops_path):
            ops_opens += 1
            if ops_opens > 1:
                raise RuntimeError("ops database opened more than once")
        return real_connect(path, **kwargs)

    monkeypatch.setattr(main_mod.db, "connect_database", connect_once)
    result = main_mod._rename_league_server_side(
        data_dir=data_dir,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-new-league",
    )

    assert result["status"] == "COMMITTED"
    assert ops_opens == 1
    leagues = duckdb.connect(str(leagues_path), read_only=True)
    assert leagues.execute(
        "SELECT DISTINCT db_name FROM public.matchup"
    ).fetchall() == [("new_league",)]
    leagues.close()
    ops = duckdb.connect(str(ops_path), read_only=True)
    assert ops.execute(
        "SELECT database_name FROM main.league_credentials"
    ).fetchall() == [("new_league",)]
    ops.close()


def test_server_side_rename_retry_skips_noop_checkpoints(data_dir, monkeypatch):
    import main as main_mod

    leagues_path = data_dir / "___leagues.duckdb"
    leagues = duckdb.connect(str(leagues_path))
    leagues.execute("ALTER TABLE public.matchup ADD COLUMN db_name VARCHAR")
    leagues.execute("UPDATE public.matchup SET db_name = 'old_league'")
    leagues.close()

    ops_path = data_dir / "___ops.duckdb"
    ops = duckdb.connect(str(ops_path))
    ops.execute("CREATE SCHEMA IF NOT EXISTS main")
    ops.execute("CREATE TABLE main.league_credentials(database_name VARCHAR, token VARCHAR)")
    ops.execute("INSERT INTO main.league_credentials VALUES ('old_league', 'encrypted')")
    ops.close()

    first = main_mod._rename_league_server_side(
        data_dir=data_dir,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-new-league",
    )
    assert first["data_status"] == "CONSOLIDATED"
    assert first["control_status"] == "COMMITTED"

    def reject_checkpoint(_conn):
        raise AssertionError("an idempotent rename retry must not checkpoint")

    monkeypatch.setattr(main_mod, "_checkpoint_result", reject_checkpoint)
    second = main_mod._rename_league_server_side(
        data_dir=data_dir,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-new-league",
    )

    assert second["data_status"] == "ALREADY_CONSOLIDATED"
    assert second["control_status"] == "ALREADY_COMMITTED"
    assert second["data_checkpointed"] is False
    assert second["ops_checkpointed"] is False


def test_server_state_exposes_runtime_capacity(client):
    resp = client.get("/internal/server-state")
    assert resp.status_code == 200
    body = resp.json()

    assert body["active_queries"] == 0
    assert body["query_capacity"] >= 1
    assert body["query_queue_timeout_seconds"] > 0
    assert body["query_ops_write_wait_seconds"] >= 0
    assert body["pool_size"] == 2
    assert body["delta_publish_capacity"] >= 1
    assert body["delta_admission_timeout_seconds"] > 0
    assert body["delta_busy_retry_after_seconds"] > 0
    assert body["duckdb_config"]["pool_size"] == 2
    assert body["duckdb_config"]["memory_limit"]
    assert body["derived_recovery"]["stage"] == "idle"


def test_derived_recovery_status_does_not_touch_database(client, monkeypatch):
    import main as main_mod

    monkeypatch.setattr(
        main_mod,
        "_derived_recovery_snapshot",
        lambda: {"stage": "reaggregating", "completed": 7, "total": 20},
    )
    monkeypatch.setattr(
        main_mod.db,
        "get_metadata",
        lambda: (_ for _ in ()).throw(AssertionError("status must not touch DuckDB")),
    )

    resp = client.get("/reaggregate-damaged-derived/status")

    assert resp.status_code == 200
    assert resp.json() == {"stage": "reaggregating", "completed": 7, "total": 20}


def test_query_requires_auth(client):
    resp = client.post("/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 401


def test_query_rejects_bad_token(client):
    resp = client.post(
        "/query",
        json={"sql": "SELECT 1"},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401


def test_query_success(client):
    resp = client.post(
        "/query",
        json={"sql": "SELECT * FROM public.matchup ORDER BY week"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 2
    assert rows[0]["manager"] == "Alice"
    assert rows[1]["manager"] == "Bob"


def test_query_compresses_large_json_payloads(client):
    resp = client.post(
        "/query",
        json={"sql": "SELECT repeat('x', 5000) AS payload", "database": "___leagues"},
        headers={"Authorization": "Bearer test-read", "Accept-Encoding": "gzip"},
    )

    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "gzip"
    assert resp.json() == [{"payload": "x" * 5000}]


def test_ops_query_parquet_returns_the_same_rows_without_json_materialization(client):
    import pandas as pd

    resp = client.post(
        "/query-parquet",
        json={"sql": "SELECT 2026 AS year, 'player-1' AS NFL_player_id", "database": "___ops"},
        headers={"Authorization": "Bearer test-read"},
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/vnd.apache.parquet"
    assert pd.read_parquet(BytesIO(resp.content)).to_dict("records") == [
        {"year": 2026, "NFL_player_id": "player-1"}
    ]


def test_query_returns_retryable_busy_when_primary_writing(client):
    import main as main_mod

    main_mod._state["status"] = "writing"
    try:
        resp = client.post(
            "/query",
            json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
            headers={"Authorization": "Bearer test-read"},
        )
    finally:
        main_mod._state["status"] = "serving"

    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "1"
    assert resp.json()["reason"] == "writing"


def test_query_busy_ignores_old_replay_headers(client):
    import main as main_mod

    main_mod._state["status"] = "writing"
    try:
        resp = client.post(
            "/query",
            json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
            headers={"Authorization": "Bearer test-read", "fly-replay-failed": "1"},
        )
    finally:
        main_mod._state["status"] = "serving"

    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "1"


def test_query_blocks_writes(client):
    resp = client.post(
        "/query",
        json={"sql": "INSERT INTO public.matchup VALUES (2024, 3, 'Eve')"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 403


def test_query_rw_requires_admin(client):
    resp = client.post(
        "/query-rw",
        json={"sql": "INSERT INTO public.matchup VALUES (2024, 3, 'Eve')"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 401


def test_query_rw_rejects_while_startup_is_still_running(client, monkeypatch):
    import main as main_mod

    monkeypatch.setattr(main_mod, "QUERY_RW_STARTUP_WAIT_SECONDS", 0.0)
    main_mod._state["status"] = "starting"

    resp = client.post(
        "/query-rw",
        json={"database": "___ops", "sql": "CREATE TABLE IF NOT EXISTS main.startup_guard (id INTEGER)"},
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 503
    assert resp.json()["reason"] == "starting"
    main_mod._state["status"] = "serving"


def test_query_rw_allows_admin_writes(client):
    resp = client.post(
        "/query-rw",
        json={"sql": "INSERT INTO public.matchup VALUES (2024, 3, 'Eve')"},
        headers={"Authorization": "Bearer test-admin"},
    )
    assert resp.status_code == 200
    # DuckDB INSERT returns a Count result
    assert resp.json() == [{"Count": 1}]

    resp = client.post(
        "/query",
        json={"sql": "SELECT manager FROM public.matchup WHERE week = 3"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json() == [{"manager": "Eve"}]


def test_query_converts_non_finite_duckdb_values_to_json_null(client):
    resp = client.post(
        "/query-rw",
        json={
            "sql": "CREATE TABLE public.non_finite_smoke (value DOUBLE); "
            "INSERT INTO public.non_finite_smoke VALUES "
            "(CAST('NaN' AS DOUBLE)), (CAST('Infinity' AS DOUBLE)), (-CAST('Infinity' AS DOUBLE))",
        },
        headers={"Authorization": "Bearer test-admin"},
    )
    assert resp.status_code == 200

    resp = client.post(
        "/query",
        json={"sql": "SELECT value FROM public.non_finite_smoke ORDER BY rowid"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json() == [{"value": None}, {"value": None}, {"value": None}]


def test_query_rw_retries_pool_reopen_file_handle_conflict(client, monkeypatch):
    import db as db_mod
    import main as main_mod

    original_init_pool = db_mod.init_pool
    calls = {"count": 0}

    def flaky_init_pool():
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError(
                'Binder Error: Unique file handle conflict: Cannot attach "___ops" - '
                'the database file "/data/___ops.duckdb" is in the process of being detached'
            )
        return original_init_pool()

    monkeypatch.setattr(main_mod, "POOL_REOPEN_RETRY_BASE_SECONDS", 0.001)
    monkeypatch.setattr(db_mod, "init_pool", flaky_init_pool)

    resp = client.post(
        "/query-rw",
        json={"sql": "INSERT INTO public.matchup VALUES (2024, 4, 'Frank')"},
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 200
    assert calls["count"] == 2
    assert main_mod._state["status"] == "serving"

    resp = client.post(
        "/query",
        json={"sql": "SELECT manager FROM public.matchup WHERE week = 4"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json() == [{"manager": "Frank"}]


def test_query_waits_through_brief_ops_write_state(client, monkeypatch):
    import main as main_mod
    import threading

    monkeypatch.setattr(main_mod, "QUERY_OPS_WRITE_WAIT_SECONDS", 1.0)
    main_mod._begin_ops_write_state()

    def release_ops_write():
        time.sleep(0.05)
        main_mod._end_ops_write_state()

    releaser = threading.Thread(target=release_ops_write)
    releaser.start()
    try:
        resp = client.post(
            "/query",
            json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
            headers={"Authorization": "Bearer test-read"},
        )
    finally:
        releaser.join(timeout=1)
        main_mod._ops_write_count = 0
        main_mod._state["status"] = "serving"

    assert resp.status_code == 200
    assert resp.json() == [{"cnt": 2}]


def test_query_and_ready_stay_online_during_ops_snapshot(client):
    import main as main_mod

    main_mod._state["status"] = "ops_snapshotting"
    main_mod._ops_write_count = 1
    try:
        ready = client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["ready"] is True
        assert ready.json()["status"] == "ops_snapshotting"

        resp = client.post(
            "/query",
            json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
            headers={"Authorization": "Bearer test-read"},
        )
        assert resp.status_code == 200
        assert resp.json() == [{"cnt": 2}]
    finally:
        main_mod._ops_write_count = 0
        main_mod._state["status"] = "serving"


def test_merge_ops_hands_off_staged_snapshot(data_dir, client, tmp_path):
    import duckdb

    bundle = tmp_path / "incoming_ops.duckdb"
    conn = duckdb.connect(str(bundle))
    conn.execute("CREATE SCHEMA nfl_historical")
    conn.execute(
        "CREATE TABLE nfl_historical.research_matchup "
        "(NFL_player_id VARCHAR, year INTEGER, points DOUBLE)"
    )
    conn.execute("INSERT INTO nfl_historical.research_matchup VALUES ('p1', 2025, 42.5)")
    conn.close()

    with bundle.open("rb") as fh:
        resp = client.post(
            "/merge-ops",
            files={"file": (bundle.name, fh, "application/octet-stream")},
            headers={"Authorization": "Bearer test-admin"},
        )

    assert resp.status_code == 200
    assert resp.json()["tables"] == {"research_matchup": 1}
    query = client.post(
        "/query",
        json={
            "database": "___ops",
            "sql": "SELECT NFL_player_id, points FROM nfl_historical.research_matchup",
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert query.status_code == 200
    assert query.json() == [{"NFL_player_id": "p1", "points": 42.5}]
    assert not list(data_dir.glob("___ops.merge.*.duckdb"))


def test_merge_ops_hands_off_when_public_pool_has_ops_attachment(data_dir, client, tmp_path):
    """Snapshot handoff waits for, then survives, an active public-pool ops attachment."""
    import duckdb
    import db as db_mod
    import main as main_mod
    from threading import Event, Thread
    import time

    bundle = tmp_path / "incoming_ops_with_public_attachment.duckdb"
    conn = duckdb.connect(str(bundle))
    conn.execute("CREATE SCHEMA nfl_historical")
    conn.execute(
        "CREATE TABLE nfl_historical.research_matchup "
        "(NFL_player_id VARCHAR, year INTEGER, points DOUBLE)"
    )
    conn.execute("INSERT INTO nfl_historical.research_matchup VALUES ('p1', 2025, 42.5)")
    conn.close()

    public_conn = db_mod.acquire_connection()
    main_mod._acquire_ops_attachment(public_conn)
    released = Event()

    def release_at_exclusive_handoff():
        deadline = time.monotonic() + 5
        while main_mod._state["status"] != "ops_writing" and time.monotonic() < deadline:
            time.sleep(0.005)
        if main_mod._state["status"] == "ops_writing":
            main_mod._release_ops_attachment(public_conn)
            db_mod.release_connection(public_conn)
            released.set()

    releaser = Thread(target=release_at_exclusive_handoff)
    releaser.start()
    try:
        with bundle.open("rb") as fh:
            resp = client.post(
                "/merge-ops",
                files={"file": (bundle.name, fh, "application/octet-stream")},
                headers={"Authorization": "Bearer test-admin"},
            )

        assert resp.status_code == 200
        assert resp.json()["tables"] == {"research_matchup": 1}
        query = client.post(
            "/query",
            json={
                "database": "___ops",
                "sql": "SELECT NFL_player_id, points FROM nfl_historical.research_matchup",
            },
            headers={"Authorization": "Bearer test-read"},
        )
        assert query.status_code == 200
        assert query.json() == [{"NFL_player_id": "p1", "points": 42.5}]
    finally:
        releaser.join(timeout=5)
        if not released.is_set():
            main_mod._release_ops_attachment(public_conn)
            db_mod.release_connection(public_conn)


def test_merge_ops_keeps_old_snapshot_queryable_during_rebuild(data_dir, client, tmp_path, monkeypatch):
    import duckdb
    import main as main_mod
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    seed = client.post(
        "/query-rw",
        json={
            "database": "___ops",
            "sql": "CREATE SCHEMA IF NOT EXISTS nfl_historical; "
            "CREATE TABLE nfl_historical.research_matchup "
            "(NFL_player_id VARCHAR, year INTEGER, points DOUBLE); "
            "INSERT INTO nfl_historical.research_matchup VALUES ('old', 2024, 10.0)",
        },
        headers={"Authorization": "Bearer test-admin"},
    )
    assert seed.status_code == 200

    bundle = tmp_path / "replacement_ops.duckdb"
    conn = duckdb.connect(str(bundle))
    conn.execute("CREATE SCHEMA nfl_historical")
    conn.execute(
        "CREATE TABLE nfl_historical.research_matchup "
        "(NFL_player_id VARCHAR, year INTEGER, points DOUBLE)"
    )
    conn.execute("INSERT INTO nfl_historical.research_matchup VALUES ('new', 2025, 42.5)")
    conn.close()

    entered_rebuild = Event()
    release_rebuild = Event()
    original_execute = main_mod._execute_script_with_timeout

    def pause_staging_rebuild(conn, sql, timeout_seconds):
        if "CREATE OR REPLACE TABLE" in sql:
            entered_rebuild.set()
            assert release_rebuild.wait(timeout=5)
        return original_execute(conn, sql, timeout_seconds)

    monkeypatch.setattr(main_mod, "_execute_script_with_timeout", pause_staging_rebuild)

    def publish():
        with bundle.open("rb") as fh:
            return client.post(
                "/merge-ops",
                files={"file": (bundle.name, fh, "application/octet-stream")},
                headers={"Authorization": "Bearer test-admin"},
            )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(publish)
        assert entered_rebuild.wait(timeout=5)
        old_read = client.post(
            "/query",
            json={
                "database": "___ops",
                "sql": "SELECT NFL_player_id, points FROM nfl_historical.research_matchup",
            },
            headers={"Authorization": "Bearer test-read"},
        )
        assert old_read.status_code == 200
        assert old_read.json() == [{"NFL_player_id": "old", "points": 10.0}]
        release_rebuild.set()
        published = future.result(timeout=10)

    assert published.status_code == 200


def test_query_rw_ops_writes_keep_ops_reads_out_of_busy_state(client, monkeypatch):
    import main as main_mod

    original_script = main_mod._execute_script_with_timeout
    seen_states = []
    monkeypatch.setattr(main_mod, "QUERY_OPS_WRITE_WAIT_SECONDS", 0.001)

    def fake_ops_script(conn, sql, timeout_seconds):
        seen_states.append(main_mod._state["status"])
        busy_resp = client.post(
            "/query",
            json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
            headers={"Authorization": "Bearer test-read"},
        )
        assert busy_resp.status_code == 503
        assert busy_resp.json()["reason"] == "ops_writing"
        return original_script(conn, sql, timeout_seconds)

    monkeypatch.setattr(main_mod, "_execute_script_with_timeout", fake_ops_script)

    resp = client.post(
        "/query-rw",
        json={
            "database": "___ops",
            "sql": """
            CREATE SCHEMA IF NOT EXISTS main;
            CREATE TABLE IF NOT EXISTS main.ops_smoke (name VARCHAR);
            DELETE FROM main.ops_smoke;
            INSERT INTO main.ops_smoke VALUES ('ok')
            """,
        },
        headers={"Authorization": "Bearer test-admin"},
    )
    assert resp.status_code == 200
    assert seen_states == ["ops_writing"]

    resp = client.post(
        "/query",
        json={"database": "___ops", "sql": "SELECT name FROM main.ops_smoke"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json() == [{"name": "ok"}]


def test_query_rw_ops_write_checkpoints_wal(client, data_dir, monkeypatch):
    import main as main_mod

    monkeypatch.setattr(main_mod, "DUCKDB_CHECKPOINT_WAL_MB", 0.001)

    resp = client.post(
        "/query-rw",
        json={
            "database": "___ops",
            "sql": """
            CREATE SCHEMA IF NOT EXISTS main;
            CREATE TABLE IF NOT EXISTS main.ops_checkpoint_smoke (name VARCHAR);
            DELETE FROM main.ops_checkpoint_smoke;
            INSERT INTO main.ops_checkpoint_smoke VALUES ('ok')
            """,
        },
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 200
    wal_path = data_dir / "___ops.duckdb.wal"
    assert not wal_path.exists() or wal_path.stat().st_size == 0


def test_startup_quarantines_ops_wal_when_replay_fails(data_dir, monkeypatch):
    import main as main_mod

    (data_dir / "___leagues.duckdb").write_bytes(b"leagues")
    (data_dir / "___ops.duckdb").write_bytes(b"ops")
    wal_path = data_dir / "___ops.duckdb.wal"
    wal_path.write_bytes(b"bad wal")

    checkpoint_ops_calls = []

    def fake_checkpoint_large(*args, **kwargs):
        return False

    def fake_checkpoint_ops(db_path, **kwargs):
        if db_path.name == "___ops.duckdb":
            checkpoint_ops_calls.append(db_path)
            raise RuntimeError("forced WAL replay failure")
        return False

    monkeypatch.setattr(main_mod, "startup_recovery", lambda path: None)
    monkeypatch.setattr(main_mod, "cleanup_stale_uploads", lambda path: None)
    monkeypatch.setattr(main_mod, "_checkpoint_database_if_wal_large", fake_checkpoint_large)
    monkeypatch.setattr(main_mod, "_checkpoint_database_if_wal_exists", fake_checkpoint_ops)
    monkeypatch.setattr(main_mod.db, "init_pool", lambda: None)

    main_mod._startup_db_sync(data_dir)

    assert len(checkpoint_ops_calls) == 1
    assert not wal_path.exists()
    quarantined = list(data_dir.glob("___ops.duckdb.wal.quarantine.*"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"bad wal"


def test_startup_quarantines_leagues_wal_when_replay_fails(data_dir, monkeypatch):
    import main as main_mod

    wal_path = data_dir / "___leagues.duckdb.wal"
    wal_path.write_bytes(b"bad leagues wal")
    checkpoint_leagues_calls = []

    def fake_checkpoint(db_path, **kwargs):
        if db_path.name == "___leagues.duckdb":
            checkpoint_leagues_calls.append(db_path)
            raise RuntimeError("forced leagues WAL replay failure")
        return False

    monkeypatch.setattr(main_mod, "startup_recovery", lambda path: None)
    monkeypatch.setattr(main_mod, "cleanup_stale_uploads", lambda path: None)
    monkeypatch.setattr(main_mod, "_checkpoint_database_if_wal_exists", fake_checkpoint)
    monkeypatch.setattr(main_mod.db, "init_pool", lambda: None)

    main_mod._startup_db_sync(data_dir)

    assert len(checkpoint_leagues_calls) == 1
    assert not wal_path.exists()
    quarantined = list(data_dir.glob("___leagues.duckdb.wal.quarantine.*"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"bad leagues wal"


def test_query_rw_ops_write_resets_state_when_reopen_fails(client, monkeypatch):
    import db as db_mod
    import main as main_mod

    reopened_pool = {"value": False}

    def fail_reopen_ops_connection():
        raise RuntimeError("forced reopen failure")

    def track_reopen_pool():
        reopened_pool["value"] = True

    monkeypatch.setattr(db_mod, "reopen_ops_connection", fail_reopen_ops_connection)
    monkeypatch.setattr(db_mod, "reopen_pool", track_reopen_pool)

    with pytest.raises(RuntimeError, match="forced reopen failure"):
        main_mod._execute_ops_query_rw(
            """
            CREATE SCHEMA IF NOT EXISTS main;
            CREATE TABLE IF NOT EXISTS main.ops_state_reset (name VARCHAR);
            """
        )

    assert reopened_pool["value"] is False
    assert main_mod._ops_write_count == 0
    assert main_mod._state["status"] == "serving"


def test_query_rw_ops_write_skips_public_pool_reopen_by_default(client, monkeypatch):
    import db as db_mod

    reopened_pool = {"value": False}

    def track_reopen_pool():
        reopened_pool["value"] = True
        raise AssertionError("___ops write should not reopen the public pool")

    monkeypatch.setattr(db_mod, "reopen_pool", track_reopen_pool)

    resp = client.post(
        "/query-rw",
        json={
            "database": "___ops",
            "sql": """
            CREATE TABLE IF NOT EXISTS main.ops_no_pool_reopen (name VARCHAR);
            DELETE FROM main.ops_no_pool_reopen;
            INSERT INTO main.ops_no_pool_reopen VALUES ('ok')
            """,
        },
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 200
    assert reopened_pool["value"] is False


def test_query_rw_ops_write_does_not_hold_league_merge_lock(data_dir, client, monkeypatch):
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(
        data_dir,
        main_mod,
        db_name="ops_lock_parallel",
        import_run_id="1002",
    )
    merge_states = []

    def fake_delta_merge(leagues_path, parsed_manifest, extract_dir):
        merge_states.append(main_mod._state["status"])
        return {
            "status": "COMMITTED",
            "db_name": parsed_manifest["db_name"],
            "bundle_id": parsed_manifest["bundle_id"],
            "bundle_hash": parsed_manifest["bundle_hash"],
            "tables": {"matchup": 2},
            "table_count": 1,
            "row_count": 2,
            "timings": {},
            "elapsed_seconds": 0,
        }

    def fake_ops_write(sql):
        main_mod._begin_ops_write_state()
        try:
            with open(archive_path, "rb") as fh:
                resp = client.post(
                    "/merge-league-delta",
                    headers={
                        "Authorization": "Bearer test-admin",
                        "x-db-name": manifest["db_name"],
                        "x-bundle-id": manifest["bundle_id"],
                        "x-bundle-hash": manifest["bundle_hash"],
                    },
                    files={"file": (archive_path.name, fh, "application/gzip")},
                )
            assert resp.status_code == 200
            return []
        finally:
            main_mod._end_ops_write_state()

    monkeypatch.setattr(main_mod, "_merge_delta_bundle", fake_delta_merge)
    monkeypatch.setattr(main_mod, "_execute_ops_query_rw", fake_ops_write)

    resp = client.post(
        "/query-rw",
        json={"database": "___ops", "sql": "UPDATE accounts.fake SET value = 1"},
        headers={"Authorization": "Bearer test-admin"},
    )
    assert resp.status_code == 200
    assert merge_states == ["ops_writing"]


def test_replace_db_requires_admin(client):
    resp = client.post(
        "/replace-db",
        headers={
            "Authorization": "Bearer test-read",
            "x-db-name": "___leagues",
        },
        files={"file": ("db.duckdb", b"fake", "application/octet-stream")},
    )
    assert resp.status_code == 401


def test_full_database_upload_limit_accommodates_verified_ops_artifacts():
    """The full Ops snapshot is intentionally larger than the 5 GiB legacy cap."""
    import main as main_mod

    assert main_mod.MAX_UPLOAD_BYTES >= 8 * 1024 * 1024 * 1024


def test_replace_db_rejects_invalid_name(client):
    resp = client.post(
        "/replace-db",
        headers={
            "Authorization": "Bearer test-admin",
            "x-db-name": "evil_db",
        },
        files={"file": ("db.duckdb", b"fake", "application/octet-stream")},
    )
    assert resp.status_code == 400


def _create_league_settings_bundle(path, rows, *, duplicate=False, extra_table=False, wrong_schema=False):
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    if wrong_schema:
        conn.execute(
            "CREATE TABLE public.league_settings "
            "(db_name VARCHAR NOT NULL, year INTEGER NOT NULL, wrong_value INTEGER, "
            "PRIMARY KEY (db_name, year))"
        )
        for db_name, year, _setting in rows:
            conn.execute(
                "INSERT INTO public.league_settings VALUES (?, ?, ?)",
                [db_name, year, 1],
            )
    else:
        conn.execute(
            "CREATE TABLE public.league_settings "
            "(db_name VARCHAR NOT NULL, year INTEGER NOT NULL, setting VARCHAR, "
            "PRIMARY KEY (db_name, year))"
        )
        for row in rows:
            conn.execute("INSERT INTO public.league_settings VALUES (?, ?, ?)", row)
        if duplicate:
            conn.execute("ALTER TABLE public.league_settings DROP CONSTRAINT league_settings_db_name_year_pkey")
            conn.execute("INSERT INTO public.league_settings VALUES (?, ?, ?)", rows[0])
    if extra_table:
        conn.execute("CREATE TABLE public.unexpected (value INTEGER)")
    conn.close()


def _install_target_league_settings(data_dir):
    import db as db_mod

    db_mod.close_all()
    conn = duckdb.connect(str(data_dir / "___leagues.duckdb"))
    conn.execute(
        "CREATE TABLE public.league_settings "
        "(db_name VARCHAR NOT NULL, year INTEGER NOT NULL, setting VARCHAR, "
        "PRIMARY KEY (db_name, year))"
    )
    conn.execute("INSERT INTO public.league_settings VALUES ('old_league', 2025, 'old')")
    conn.close()
    db_mod.init_pool()


def test_replace_canonical_table_requires_admin(data_dir, client):
    _install_target_league_settings(data_dir)
    bundle = data_dir / "settings_bundle_auth.duckdb"
    _create_league_settings_bundle(bundle, [("new_league", 2026, "new")])

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-read",
                "x-db-name": "___leagues",
                "x-table-name": "league_settings",
                "x-expected-rows": "1",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 401


def test_reaggregate_damaged_derived_requires_admin(client):
    resp = client.post(
        "/reaggregate-damaged-derived",
        json={"mode": "quarantine_and_rebuild", "confirm_targets": []},
    )
    assert resp.status_code == 401


def test_reaggregate_damaged_derived_uses_exact_allowlist(client, monkeypatch):
    import main as main_mod

    calls = []

    def fake_reaggregate(database_path, *, mode, db_name=None):
        calls.append((database_path, mode, db_name))
        return {
            "status": "COMMITTED",
            "mode": mode,
            "leagues": 2,
            "targets": list(main_mod._DAMAGED_DERIVED_TARGETS),
        }

    monkeypatch.setattr(main_mod, "_reaggregate_damaged_derived_from_sources", fake_reaggregate)
    payload = {
        "mode": "quarantine_and_rebuild",
        "confirm_targets": list(main_mod._DAMAGED_DERIVED_TARGETS),
    }
    resp = client.post(
        "/reaggregate-damaged-derived",
        json=payload,
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 200
    assert resp.json()["targets"] == list(main_mod._DAMAGED_DERIVED_TARGETS)
    assert calls and calls[0][1] == "quarantine_and_rebuild"


def test_reaggregate_damaged_derived_supports_one_league_scope(client, monkeypatch):
    import main as main_mod

    calls = []

    def fake_reaggregate(database_path, *, mode, db_name=None):
        calls.append((database_path, mode, db_name))
        return {
            "status": "COMMITTED",
            "mode": mode,
            "db_name": db_name,
            "leagues": 1,
            "targets": list(main_mod._DAMAGED_DERIVED_TARGETS),
            "checkpointed": False,
        }

    monkeypatch.setattr(main_mod, "_reaggregate_damaged_derived_from_sources", fake_reaggregate)
    monkeypatch.setattr(
        main_mod.db,
        "close_pool",
        lambda: (_ for _ in ()).throw(AssertionError("scoped rebuild must not drain the pool")),
    )
    resp = client.post(
        "/reaggregate-damaged-derived",
        json={
            "mode": "scoped_rebuild",
            "db_name": "northern_league_xxx",
            "confirm_targets": list(main_mod._DAMAGED_DERIVED_TARGETS),
        },
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 200
    assert resp.json()["db_name"] == "northern_league_xxx"
    assert calls == [
        (
            main_mod.db.get_data_dir() / "___leagues.duckdb",
            "scoped_rebuild",
            "northern_league_xxx",
        )
    ]


def test_reaggregate_damaged_derived_scoped_mode_requires_valid_db_name(client):
    import main as main_mod

    resp = client.post(
        "/reaggregate-damaged-derived",
        json={
            "mode": "scoped_rebuild",
            "db_name": "not/valid",
            "confirm_targets": list(main_mod._DAMAGED_DERIVED_TARGETS),
        },
        headers={"Authorization": "Bearer test-admin"},
    )

    assert resp.status_code == 400


def test_reaggregate_damaged_derived_rejects_broader_target_set(client):
    import main as main_mod

    resp = client.post(
        "/reaggregate-damaged-derived",
        json={
            "mode": "quarantine_and_rebuild",
            "confirm_targets": [*main_mod._DAMAGED_DERIVED_TARGETS, "matchup"],
        },
        headers={"Authorization": "Bearer test-admin"},
    )
    assert resp.status_code == 400


def test_replace_canonical_table_swaps_only_allowlisted_table(data_dir, client):
    _install_target_league_settings(data_dir)
    bundle = data_dir / "settings_bundle.duckdb"
    _create_league_settings_bundle(
        bundle,
        [("alpha", 2025, "a"), ("alpha", 2026, "b"), ("beta", 2026, "c")],
    )

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "___leagues",
                "x-table-name": "league_settings",
                "x-expected-rows": "3",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "replaced"
    assert resp.json()["rows"] == 3

    settings = client.post(
        "/query",
        json={
            "sql": "SELECT db_name, year, setting FROM public.league_settings ORDER BY db_name, year"
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert settings.status_code == 200
    assert settings.json() == [
        {"db_name": "alpha", "year": 2025, "setting": "a"},
        {"db_name": "alpha", "year": 2026, "setting": "b"},
        {"db_name": "beta", "year": 2026, "setting": "c"},
    ]

    matchup = client.post(
        "/query",
        json={"sql": "SELECT COUNT(*) AS count FROM public.matchup"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert matchup.status_code == 200
    assert matchup.json() == [{"count": 2}]

    db_mod = __import__("db")
    db_mod.close_all()
    conn = duckdb.connect(str(data_dir / "___leagues.duckdb"))
    with pytest.raises(duckdb.ConstraintException):
        conn.execute("INSERT INTO public.league_settings VALUES ('alpha', 2026, 'duplicate')")
    conn.close()
    db_mod.init_pool()


def test_replace_canonical_table_allows_verified_derived_recovery(data_dir, client):
    import db as db_mod

    db_mod.close_all()
    target = data_dir / "___leagues.duckdb"
    conn = duckdb.connect(str(target))
    conn.execute("DROP TABLE IF EXISTS public.homepage_manager_rankings")
    conn.execute(
        "CREATE TABLE public.homepage_manager_rankings "
        "(db_name VARCHAR NOT NULL, franchise_id VARCHAR NOT NULL, wins INTEGER, "
        "PRIMARY KEY (db_name, franchise_id))"
    )
    conn.execute(
        "INSERT INTO public.homepage_manager_rankings VALUES ('old', 'old-1', 1)"
    )
    conn.close()
    db_mod.init_pool()

    bundle = data_dir / "homepage_rankings_recovery.duckdb"
    incoming = duckdb.connect(str(bundle))
    incoming.execute("CREATE SCHEMA public")
    incoming.execute(
        "CREATE TABLE public.homepage_manager_rankings "
        "(db_name VARCHAR NOT NULL, franchise_id VARCHAR NOT NULL, wins INTEGER, "
        "PRIMARY KEY (db_name, franchise_id))"
    )
    incoming.execute(
        "INSERT INTO public.homepage_manager_rankings VALUES "
        "('alpha', 'a-1', 3), ('beta', 'b-1', 4)"
    )
    incoming.close()

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "___leagues",
                "x-table-name": "homepage_manager_rankings",
                "x-expected-rows": "2",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"] == 2
    rows = client.post(
        "/query",
        json={
            "sql": "SELECT db_name, franchise_id, wins "
            "FROM public.homepage_manager_rankings ORDER BY db_name"
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert rows.status_code == 200
    assert rows.json() == [
        {"db_name": "alpha", "franchise_id": "a-1", "wins": 3},
        {"db_name": "beta", "franchise_id": "b-1", "wins": 4},
    ]


def test_replace_derived_table_preserves_leagues_published_after_snapshot(data_dir, client):
    import db as db_mod

    db_mod.close_all()
    target = data_dir / "___leagues.duckdb"
    conn = duckdb.connect(str(target))
    conn.execute("DROP TABLE IF EXISTS public.homepage_manager_rankings")
    conn.execute(
        "CREATE TABLE public.homepage_manager_rankings "
        "(db_name VARCHAR NOT NULL, franchise_id VARCHAR NOT NULL, wins INTEGER, "
        "PRIMARY KEY (db_name, franchise_id))"
    )
    conn.execute(
        "INSERT INTO public.homepage_manager_rankings VALUES "
        "('alpha', 'a-1', 30), ('gamma', 'g-1', 40)"
    )
    conn.execute(
        "CREATE TABLE public.league_context "
        "(db_name VARCHAR, updated_at TIMESTAMP)"
    )
    conn.execute(
        "INSERT INTO public.league_context VALUES "
        "('alpha', TIMESTAMP '2026-09-16 01:00:00'), "
        "('gamma', TIMESTAMP '2026-09-16 02:00:00'), "
        "('beta', TIMESTAMP '2026-09-14 00:00:00')"
    )
    conn.close()
    db_mod.init_pool()

    bundle = data_dir / "homepage_rankings_snapshot_bundle.duckdb"
    incoming = duckdb.connect(str(bundle))
    incoming.execute("CREATE SCHEMA public")
    incoming.execute(
        "CREATE TABLE public.homepage_manager_rankings "
        "(db_name VARCHAR NOT NULL, franchise_id VARCHAR NOT NULL, wins INTEGER, "
        "PRIMARY KEY (db_name, franchise_id))"
    )
    incoming.execute(
        "INSERT INTO public.homepage_manager_rankings VALUES "
        "('alpha', 'a-1', 3), ('beta', 'b-1', 4)"
    )
    incoming.close()

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "___leagues",
                "x-table-name": "homepage_manager_rankings",
                "x-expected-rows": "3",
                "x-recovery-since": "2026-09-15T21:18:28Z",
                "x-expected-overlay-leagues": "2",
                "x-expected-overlay-rows": "2",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["overlay_leagues"] == 2
    assert resp.json()["overlay_rows"] == 2
    rows = client.post(
        "/query",
        json={
            "sql": "SELECT db_name, franchise_id, wins "
            "FROM public.homepage_manager_rankings ORDER BY db_name"
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert rows.status_code == 200
    assert rows.json() == [
        {"db_name": "alpha", "franchise_id": "a-1", "wins": 30},
        {"db_name": "beta", "franchise_id": "b-1", "wins": 4},
        {"db_name": "gamma", "franchise_id": "g-1", "wins": 40},
    ]


def test_derived_recovery_retry_is_idempotent(data_dir, client, monkeypatch):
    import db as db_mod
    import fly_reaggregate_derived as recovery_helper
    import main as main_mod

    attached = []
    monkeypatch.setattr(
        recovery_helper,
        "_attach_if_present",
        lambda _conn, path, catalog: attached.append((path.name, catalog)),
    )

    db_mod.close_all()
    database_path = data_dir / "___leagues.duckdb"
    conn = duckdb.connect(str(database_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS merge_admin")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS merge_admin.league_publish_generations ("
        "db_name VARCHAR PRIMARY KEY, generation BIGINT NOT NULL, lane VARCHAR, "
        "run_id VARCHAR, updated_at TIMESTAMP)"
    )
    conn.execute(
        "INSERT OR REPLACE INTO merge_admin.league_publish_generations "
        "VALUES ('alpha', 7, 'derived_recovery', 'repair-run', current_timestamp)"
    )
    conn.close()

    result = main_mod._rebuild_league_derived_from_sources(
        database_path,
        db_name="alpha",
        run_id="repair-run",
    )

    assert result == {
        "status": "ALREADY_COMMITTED",
        "db_name": "alpha",
        "generation": 7,
        "run_id": "repair-run",
        "checkpointed": True,
    }
    assert attached == [
        ("___ops_nfl.duckdb", "___ops_nfl"),
        ("___ops.duckdb", "___ops"),
    ]


def test_checkpoint_failure_is_reported_without_hiding_committed_state(client):
    import main as main_mod

    class BrokenCheckpoint:
        def execute(self, sql):
            assert sql == "CHECKPOINT"
            raise OSError("checkpoint storage failure")

    assert main_mod._checkpoint_result(BrokenCheckpoint()) == (
        False,
        "checkpoint storage failure",
    )


def test_scoped_recovery_defers_checkpoint_when_wal_checkpointing_is_disabled(monkeypatch):
    import main as main_mod

    class MustNotCheckpoint:
        def execute(self, _sql):
            raise AssertionError("known-corrupt base must not be checkpointed")

    monkeypatch.setattr(main_mod, "DUCKDB_CHECKPOINT_WAL_MB", 0)
    assert main_mod._scoped_recovery_checkpoint_result(MustNotCheckpoint()) == (
        False,
        "checkpoint deferred while WAL checkpointing is disabled",
    )


def test_post_merge_checkpoint_disarms_merge_kill_timer_first(tmp_path, monkeypatch):
    import main as main_mod

    events = []

    class FakeTimer:
        def cancel(self):
            events.append("timer_cancelled")

    def fake_checkpoint(conn, db_path, *, reason, force=False):
        assert events == ["timer_cancelled"]
        events.append("checkpoint")
        return True

    monkeypatch.setattr(main_mod, "_checkpoint_connection_if_wal_large", fake_checkpoint)

    assert main_mod._checkpoint_after_merge(
        object(),
        tmp_path / "___leagues.duckdb",
        reason="delta merge test_league",
        hard_exit_timer=FakeTimer(),
    ) is True
    assert events == ["timer_cancelled", "checkpoint"]


def test_checkpoint_admin_sql_is_not_subject_to_process_kill_watchdog():
    import main as main_mod

    assert main_mod._sql_requests_checkpoint("CHECKPOINT") is True
    assert main_mod._sql_requests_checkpoint("force checkpoint; select 1") is True
    assert main_mod._sql_requests_checkpoint("SELECT 'checkpoint' AS label") is False


@pytest.mark.parametrize(
    ("bundle_options", "expected_detail"),
    [
        ({"wrong_schema": True}, "schema"),
        ({"extra_table": True}, "exactly one"),
    ],
)
def test_replace_canonical_table_rejects_invalid_bundle(
    data_dir, client, bundle_options, expected_detail
):
    _install_target_league_settings(data_dir)
    bundle = data_dir / f"settings_bundle_invalid_{expected_detail}.duckdb"
    _create_league_settings_bundle(
        bundle,
        [("new_league", 2026, "new")],
        **bundle_options,
    )

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "___leagues",
                "x-table-name": "league_settings",
                "x-expected-rows": "1",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 400
    assert expected_detail in resp.json()["detail"].lower()

    unchanged = client.post(
        "/query",
        json={"sql": "SELECT * FROM public.league_settings"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert unchanged.status_code == 200
    assert unchanged.json() == [{"db_name": "old_league", "year": 2025, "setting": "old"}]


def test_replace_canonical_table_rejects_non_allowlisted_target(data_dir, client):
    _install_target_league_settings(data_dir)
    bundle = data_dir / "settings_bundle_wrong_target.duckdb"
    _create_league_settings_bundle(bundle, [("new_league", 2026, "new")])

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "___leagues",
                "x-table-name": "matchup",
                "x-expected-rows": "1",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 400
    assert "allowlisted" in resp.json()["detail"].lower()


def test_replace_canonical_table_preserves_rows_changed_after_snapshot(data_dir, client):
    import db as db_mod

    _install_target_league_settings(data_dir)
    db_mod.close_all()
    conn = duckdb.connect(str(data_dir / "___leagues.duckdb"))
    conn.execute("DELETE FROM public.league_settings")
    conn.execute(
        "INSERT INTO public.league_settings VALUES "
        "('alpha', 2026, 'current-alpha'), ('gamma', 2026, 'current-gamma')"
    )
    conn.execute(
        "CREATE TABLE public.league_context "
        "(db_name VARCHAR, updated_at TIMESTAMP)"
    )
    conn.execute(
        "INSERT INTO public.league_context VALUES "
        "('alpha', TIMESTAMP '2026-09-16 01:00:00'), "
        "('gamma', TIMESTAMP '2026-09-16 02:00:00'), "
        "('beta', TIMESTAMP '2026-09-14 00:00:00')"
    )
    conn.close()
    db_mod.init_pool()

    bundle = data_dir / "settings_snapshot_bundle.duckdb"
    _create_league_settings_bundle(
        bundle,
        [("alpha", 2026, "snapshot-alpha"), ("beta", 2026, "snapshot-beta")],
    )

    with open(bundle, "rb") as file_handle:
        resp = client.post(
            "/replace-canonical-table",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "___leagues",
                "x-table-name": "league_settings",
                "x-expected-rows": "3",
                "x-recovery-since": "2026-09-15T21:18:28Z",
                "x-expected-overlay-leagues": "2",
                "x-expected-overlay-rows": "2",
            },
            files={"file": (bundle.name, file_handle, "application/octet-stream")},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["overlay_leagues"] == 2
    assert resp.json()["overlay_rows"] == 2

    settings = client.post(
        "/query",
        json={
            "sql": "SELECT db_name, year, setting FROM public.league_settings ORDER BY db_name"
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert settings.status_code == 200
    assert settings.json() == [
        {"db_name": "alpha", "year": 2026, "setting": "current-alpha"},
        {"db_name": "beta", "year": 2026, "setting": "snapshot-beta"},
        {"db_name": "gamma", "year": 2026, "setting": "current-gamma"},
    ]


def test_merge_league(data_dir, client):
    """Merge a per-league .duckdb file into ___leagues."""
    import db as db_mod

    # Close pool so we can write to ___leagues
    db_mod.close_all()

    # Add db_name column to existing table
    leagues_path = data_dir / "___leagues.duckdb"
    conn = duckdb.connect(str(leagues_path))
    conn.execute("ALTER TABLE public.matchup ADD COLUMN db_name VARCHAR")
    conn.execute("UPDATE public.matchup SET db_name = 'old_league'")
    conn.close()

    # Reopen pool
    db_mod.init_pool()

    # Create a league .duckdb file to merge
    league_path = data_dir / "test_league.duckdb"
    conn = duckdb.connect(str(league_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (year INT, week INT, manager VARCHAR, new_metric DOUBLE)")
    conn.execute("INSERT INTO public.matchup VALUES (2025, 1, 'Charlie', 7.5), (2025, 2, 'Dave', 8.5)")
    conn.close()

    with open(league_path, "rb") as f:
        resp = client.post(
            "/merge-league",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
            },
            files={"file": ("test_league.duckdb", f, "application/octet-stream")},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "merged"
    assert body["tables"]["matchup"] == 2

    # Verify old data is still there and new data was added
    resp = client.post(
        "/query",
        json={"sql": "SELECT db_name, COUNT(*) as cnt FROM public.matchup GROUP BY db_name ORDER BY db_name"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 2
    assert rows[0] == {"db_name": "old_league", "cnt": 2}
    assert rows[1] == {"db_name": "test_league", "cnt": 2}

    resp = client.post(
        "/query",
        json={"sql": "SELECT SUM(new_metric) AS total FROM public.matchup WHERE db_name = 'test_league'"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"total": 16.0}


def test_merge_league_skip_delete_replaces_partial_retry(data_dir, client):
    """A cancelled fast-path merge must not duplicate rows on retry."""
    import db as db_mod

    # Close pool so we can write to ___leagues
    db_mod.close_all()

    leagues_path = data_dir / "___leagues.duckdb"
    conn = duckdb.connect(str(leagues_path))
    conn.execute("ALTER TABLE public.matchup ADD COLUMN db_name VARCHAR")
    conn.execute("ALTER TABLE public.matchup ADD COLUMN new_metric DOUBLE")
    conn.execute("UPDATE public.matchup SET db_name = 'old_league', new_metric = 0")
    # Simulate an earlier cancelled first-import attempt that committed rows
    # before the client timed out and before inventory could be marked complete.
    conn.execute(
        """
        INSERT INTO public.matchup (year, week, manager, db_name, new_metric)
        VALUES
            (2025, 1, 'Partial A', 'test_league', 1.0),
            (2025, 2, 'Partial B', 'test_league', 2.0)
        """
    )
    conn.close()

    db_mod.init_pool()

    league_path = data_dir / "test_league_retry.duckdb"
    conn = duckdb.connect(str(league_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (year INT, week INT, manager VARCHAR, new_metric DOUBLE)")
    conn.execute("INSERT INTO public.matchup VALUES (2025, 1, 'Charlie', 7.5), (2025, 2, 'Dave', 8.5)")
    conn.close()

    with open(league_path, "rb") as f:
        resp = client.post(
            "/merge-league",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-skip-delete": "1",
            },
            files={"file": ("test_league_retry.duckdb", f, "application/octet-stream")},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["skip_delete_requested"] is True
    assert body["skip_delete_used"] is False

    resp = client.post(
        "/query",
        json={
            "sql": """
            SELECT COUNT(*) AS cnt, SUM(new_metric) AS total
            FROM public.matchup
            WHERE db_name = 'test_league'
            """
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"cnt": 2, "total": 16.0}


def test_merge_league_handles_parallel_legacy_uploads(data_dir, client):
    """Legacy rollback uploads serialize cleanly without loss.

    The production pressure target lives on /merge-league-delta below. Keep this
    legacy-path regression smaller because TestClient + 30 concurrent multipart
    uploads is noisy on Windows and the old endpoint is operational rollback,
    not the default publish protocol.
    """
    import db as db_mod

    db_mod.close_all()

    leagues_path = data_dir / "___leagues.duckdb"
    conn = duckdb.connect(str(leagues_path))
    conn.execute("ALTER TABLE public.matchup ADD COLUMN db_name VARCHAR")
    conn.execute("UPDATE public.matchup SET db_name = 'old_league'")
    conn.close()

    db_mod.init_pool()

    upload_paths = []
    upload_count = 8
    for i in range(upload_count):
        league_path = data_dir / f"parallel_league_{i}.duckdb"
        conn = duckdb.connect(str(league_path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.execute("CREATE TABLE public.matchup (year INT, week INT, manager VARCHAR, points DOUBLE)")
        conn.execute(
            "INSERT INTO public.matchup VALUES (2025, ?, ?, ?), (2025, ?, ?, ?)",
            [1, f"Manager {i}A", float(i), 2, f"Manager {i}B", float(i) + 0.5],
        )
        conn.close()
        upload_paths.append(league_path)

    def upload_one(i: int):
        with open(upload_paths[i], "rb") as f:
            resp = client.post(
                "/merge-league",
                headers={
                    "Authorization": "Bearer test-admin",
                    "x-db-name": f"parallel_league_{i}",
                },
                files={"file": (upload_paths[i].name, f, "application/octet-stream")},
            )
        return resp.status_code, resp.json()

    with ThreadPoolExecutor(max_workers=upload_count) as pool:
        results = list(pool.map(upload_one, range(upload_count)))

    assert all(status == 200 for status, _body in results)
    assert all(body["status"] == "merged" for _status, body in results)
    assert all(body["tables"]["matchup"] == 2 for _status, body in results)

    resp = client.post(
        "/query",
        json={
            "sql": """
            SELECT COUNT(DISTINCT db_name) AS leagues, COUNT(*) AS rows
            FROM public.matchup
            WHERE db_name LIKE 'parallel_league_%'
            """
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"leagues": upload_count, "rows": upload_count * 2}


def test_delta_allowed_tables_match_worker_registry(client):
    import main as main_mod
    from multi_league.core.delta_publish import canonical_table_registry

    assert main_mod._DELTA_ALLOWED_TABLES == set(canonical_table_registry())


def test_merge_league_delta_accepts_franchise_identity_tables(data_dir, client, tmp_path):
    from multi_league.core.delta_publish import build_delta_bundle
    from multi_league.core.franchise_identity_schema import (
        FRANCHISE_IDENTITY_AUDIT_DDL,
        FRANCHISE_IDENTITY_REGISTRY_DDL,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        FRANCHISE_IDENTITY_REGISTRY_DDL.replace(
            "franchise_identity_registry",
            "public.franchise_identity_registry",
            1,
        )
    )
    conn.execute(
        FRANCHISE_IDENTITY_AUDIT_DDL.replace(
            "franchise_identity_audit",
            "public.franchise_identity_audit",
            1,
        )
    )
    conn.execute(
        """
        INSERT INTO public.franchise_identity_registry (
            db_name, platform, base_franchise_id, manager_guid, identity_key, branch_key,
            resolved_franchise_id, team_index, anchor_year, anchor_week, anchor_team_name,
            anchor_manager, anchor_team_key, anchor_team_slot, known_team_names,
            known_manager_names, known_team_slots, active_years, created_at, updated_at
        )
        VALUES (
            'identity_table_test', 'yahoo', 'guid', 'guid', 'team_slot:1', 'team_slot:1',
            'guid_1', 1, 2025, 1, 'Aces', 'Alice', '461.l.1.t.1', '1',
            'Aces', 'Alice', '1', '2025', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.franchise_identity_audit (
            db_name, platform, year, week, manager_guid, base_franchise_id,
            resolved_franchise_id, assignment_key, identity_key, branch_key, team_index,
            team_key, team_slot, team_name, manager, reason, identity_score,
            name_score, slot_score, total_score, created_at
        )
        VALUES (
            'identity_table_test', 'yahoo', 2025, 1, 'guid', 'guid', 'guid_1',
            'audit-key-1', 'team_slot:1', 'team_slot:1', 1, '461.l.1.t.1',
            '1', 'Aces', 'Alice', 'slot', 0, 100, 30, 130, CURRENT_TIMESTAMP
        )
        """
    )
    bundle = build_delta_bundle(
        conn,
        db_name="identity_table_test",
        import_mode="quick",
        platform="yahoo",
        output_dir=tmp_path / "identity_bundle",
    )
    conn.close()

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "identity_table_test",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "COMMITTED"
    assert body["tables"]["franchise_identity_registry"] == 1
    assert body["tables"]["franchise_identity_audit"] == 1


def test_merge_league_delta_commits_and_replays(data_dir, client):
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(data_dir, main_mod)
    with open(archive_path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-bundle-id": manifest["bundle_id"],
                "x-bundle-hash": manifest["bundle_hash"],
            },
            files={"file": (archive_path.name, fh, "application/gzip")},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "COMMITTED"
    assert body["tables"]["matchup"] == 2
    assert body["lock_wait_seconds"] >= 0
    assert body["merge_seconds"] >= 0

    with open(archive_path, "rb") as fh:
        replay = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-bundle-id": manifest["bundle_id"],
                "x-bundle-hash": manifest["bundle_hash"],
            },
            files={"file": (archive_path.name, fh, "application/gzip")},
        )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True

    status = client.get(
        "/merge-league-delta/status",
        params={"db_name": "test_league", "bundle_id": manifest["bundle_id"]},
        headers={"Authorization": "Bearer test-admin"},
    )
    assert status.status_code == 200
    assert status.json()["status"] == "COMMITTED"

    resp = client.post(
        "/query",
        json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup WHERE db_name = 'test_league'"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"cnt": 2}


def test_merge_league_delta_uses_pool_compatible_connection_config(data_dir, client, monkeypatch):
    import main as main_mod

    monkeypatch.setattr(main_mod, "WRITE_DUCKDB_THREADS", 1)
    archive_path, manifest = _make_delta_bundle(
        data_dir,
        main_mod,
        db_name="config_compat",
        import_run_id="1001",
    )

    with open(archive_path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "config_compat",
                "x-bundle-id": manifest["bundle_id"],
                "x-bundle-hash": manifest["bundle_hash"],
            },
            files={"file": (archive_path.name, fh, "application/gzip")},
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "COMMITTED"


def test_merge_league_delta_keeps_public_reads_online(data_dir, client, monkeypatch):
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(data_dir, main_mod)
    seen_states = []

    def fake_merge(leagues_path, parsed_manifest, extract_dir):
        seen_states.append(main_mod._state["status"])
        read_resp = client.post(
            "/query",
            json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
            headers={"Authorization": "Bearer test-read"},
        )
        assert read_resp.status_code == 200
        return {
            "status": "COMMITTED",
            "db_name": parsed_manifest["db_name"],
            "bundle_id": parsed_manifest["bundle_id"],
            "bundle_hash": parsed_manifest["bundle_hash"],
            "tables": {"matchup": 2},
            "table_count": 1,
            "row_count": 2,
            "timings": {},
            "elapsed_seconds": 0,
        }

    monkeypatch.setattr(main_mod, "_merge_delta_bundle", fake_merge)
    with open(archive_path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-bundle-id": manifest["bundle_id"],
                "x-bundle-hash": manifest["bundle_hash"],
            },
            files={"file": (archive_path.name, fh, "application/gzip")},
        )

    assert resp.status_code == 200
    assert seen_states == ["serving"]


def test_merge_league_delta_admission_control_returns_retryable_busy(data_dir, client, monkeypatch):
    import main as main_mod

    original_timeout = main_mod.DELTA_ADMISSION_TIMEOUT_SECONDS
    original_retry_after = main_mod.DELTA_BUSY_RETRY_AFTER_SECONDS
    main_mod.DELTA_ADMISSION_TIMEOUT_SECONDS = 0.1
    main_mod.DELTA_BUSY_RETRY_AFTER_SECONDS = 1.0
    merge_entered = Event()
    release_merge = Event()

    def slow_merge(leagues_path, parsed_manifest, extract_dir):
        merge_entered.set()
        assert release_merge.wait(timeout=30), "admission-control challengers did not release the merge"
        return {
            "status": "COMMITTED",
            "db_name": parsed_manifest["db_name"],
            "bundle_id": parsed_manifest["bundle_id"],
            "bundle_hash": parsed_manifest["bundle_hash"],
            "tables": {"matchup": 2},
            "table_count": 1,
            "row_count": 2,
            "timings": {},
            "elapsed_seconds": 0,
        }

    monkeypatch.setattr(main_mod, "_merge_delta_bundle", slow_merge)

    upload_paths = []
    manifests = []
    for i in range(3):
        path, manifest = _make_delta_bundle(
            data_dir,
            main_mod,
            db_name=f"admission_parallel_{i}",
            import_run_id=str(3000 + i),
        )
        upload_paths.append(path)
        manifests.append(manifest)

    # Archive validation has its own tests. Keep this admission test independent
    # of concurrent tar/Parquet work, which can delay requests past the held merge.
    by_bundle_id = {manifest["bundle_id"]: manifest for manifest in manifests}

    def validated_archive(_path, *, db_name, expected_bundle_id, expected_bundle_hash):
        manifest = by_bundle_id[expected_bundle_id]
        assert manifest["db_name"] == db_name
        assert manifest["bundle_hash"] == expected_bundle_hash
        return manifest, data_dir

    monkeypatch.setattr(main_mod, "_validate_delta_archive", validated_archive)

    def upload_one(i: int):
        with open(upload_paths[i], "rb") as fh:
            resp = client.post(
                "/merge-league-delta",
                headers={
                    "Authorization": "Bearer test-admin",
                    "x-db-name": manifests[i]["db_name"],
                    "x-bundle-id": manifests[i]["bundle_id"],
                    "x-bundle-hash": manifests[i]["bundle_hash"],
                },
                files={"file": (upload_paths[i].name, fh, "application/gzip")},
            )
        return resp.status_code, resp.headers.get("Retry-After"), resp.json()

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(upload_one, 0)
            assert merge_entered.wait(timeout=10), "first upload did not acquire the merge slot"
            challengers = [pool.submit(upload_one, i) for i in (1, 2)]
            busy_results = [future.result(timeout=10) for future in challengers]
            release_merge.set()
            results = [first.result(timeout=10), *busy_results]
    finally:
        release_merge.set()
        main_mod.DELTA_ADMISSION_TIMEOUT_SECONDS = original_timeout
        main_mod.DELTA_BUSY_RETRY_AFTER_SECONDS = original_retry_after

    statuses = [status for status, _retry_after, _body in results]
    assert statuses.count(429) == 2
    assert statuses.count(200) == 1
    busy = next(result for result in results if result[0] == 429)
    assert busy[1] == "1"
    assert busy[2]["detail"] == "Delta publish server busy; retry later"
    assert main_mod._delta_publish_inflight == 0
    assert main_mod._active_delta_publish_snapshot() == []


def test_merge_league_delta_validation_failure_does_not_acquire_publish_slot(client, monkeypatch):
    import main as main_mod

    calls = []

    async def fail_if_called(db_name, bundle_id=None):
        calls.append((db_name, bundle_id))
        raise AssertionError("publish slot should only be acquired after validation")

    monkeypatch.setattr(main_mod, "_acquire_delta_publish_slot", fail_if_called)

    resp = client.post(
        "/merge-league-delta",
        headers={
            "Authorization": "Bearer test-admin",
            "x-db-name": "invalid_bundle_probe",
            "x-bundle-id": "invalid-bundle",
            "x-bundle-hash": "not-a-real-hash",
        },
        files={"file": ("bad.tar.gz", b"not a gzip archive", "application/gzip")},
    )

    assert resp.status_code == 400
    assert "Invalid delta archive" in resp.json()["detail"]
    assert calls == []
    assert main_mod._delta_publish_inflight == 0


def test_merge_league_delta_accepts_unrostered_player_fantasy_rows(data_dir, client):
    from multi_league.core.delta_publish import build_delta_bundle

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            franchise_id VARCHAR,
            fantasy_position VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        VALUES
            ('pf_nulls', '00-0033873_2026_1', 'alice_0', 'QB', 2026, 1),
            ('pf_nulls', '00-0035228_2026_1', NULL, NULL, 2026, 1)
        """
    )
    bundle = build_delta_bundle(
        conn,
        db_name="pf_nulls",
        import_mode="quick",
        platform="sleeper",
        output_dir=data_dir / "pf_nulls_delta",
    )
    conn.close()

    player_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "player_fantasy")
    assert player_entry["primary_keys"] == ["db_name", "player_week"]

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "pf_nulls",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )
    assert resp.status_code == 200
    assert resp.json()["tables"]["player_fantasy"] == 2

    resp = client.post(
        "/query",
        json={
            "sql": """
            SELECT COUNT(*) AS cnt,
                   SUM(CASE WHEN franchise_id IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_franchise_rows
            FROM public.player_fantasy
            WHERE db_name = 'pf_nulls'
            """
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"cnt": 2, "null_franchise_rows": 1}


def test_merge_league_delta_allows_empty_full_sources_without_derived_tables(data_dir, client):
    from multi_league.core.delta_publish import build_delta_bundle

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager_week VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER
        )
        """
    )

    bundle = build_delta_bundle(
        conn,
        db_name="empty_2026_shell",
        import_mode="full",
        platform="sleeper",
        output_dir=data_dir / "empty_2026_shell_delta",
    )
    conn.close()

    included = {entry["table"]: entry["row_count"] for entry in bundle.manifest["tables"]}
    assert included["matchup"] == 0
    assert included["player_fantasy"] == 0
    assert included["league_settings"] == 0

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "empty_2026_shell",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "COMMITTED"


def test_merge_league_delta_accepts_schedule_rows_without_opponent_identity(data_dir, client):
    from multi_league.core.delta_publish import build_delta_bundle

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.schedule (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager_week VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.schedule
        VALUES
            ('schedule_nulls', 2026, 1, 'alice_2026_1', 'Alice', 'alice_0', 'Bob', 'bob_0'),
            ('schedule_nulls', 2026, 2, 'alice_2026_2', 'Alice', 'alice_0', NULL, NULL)
        """
    )
    bundle = build_delta_bundle(
        conn,
        db_name="schedule_nulls",
        import_mode="quick",
        platform="sleeper",
        output_dir=data_dir / "schedule_nulls_delta",
    )
    conn.close()

    schedule_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "schedule")
    assert schedule_entry["primary_keys"] == ["db_name", "manager_week"]

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "schedule_nulls",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )
    assert resp.status_code == 200
    assert resp.json()["tables"]["schedule"] == 2

    resp = client.post(
        "/query",
        json={
            "sql": """
            SELECT COUNT(*) AS cnt,
                   SUM(CASE WHEN opponent_franchise_id IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_opponent_rows
            FROM public.schedule
            WHERE db_name = 'schedule_nulls'
            """
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"cnt": 2, "null_opponent_rows": 1}


def test_merge_league_delta_accepts_luck_tables(data_dir, client):
    from multi_league.core.delta_publish import build_delta_bundle

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.all_play (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            result VARCHAR,
            points DOUBLE,
            opponent_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.h2h_season (
            db_name VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            wins BIGINT,
            losses BIGINT,
            ties BIGINT,
            games BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.schedule_swap (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            schedule_of_franchise_id VARCHAR,
            result VARCHAR,
            my_points DOUBLE,
            their_opponent_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.schedule_swap_season (
            db_name VARCHAR,
            franchise_id VARCHAR,
            schedule_of_franchise_id VARCHAR,
            year INTEGER,
            wins BIGINT,
            losses BIGINT,
            ties BIGINT,
            games BIGINT
        )
        """
    )
    conn.execute("INSERT INTO public.all_play VALUES ('luck_delta', 2026, 1, 'alice', 'bob', 'W', 101.0, 99.0)")
    conn.execute("INSERT INTO public.h2h_season VALUES ('luck_delta', 'alice', 'bob', 2026, 1, 0, 0, 1)")
    conn.execute("INSERT INTO public.schedule_swap VALUES ('luck_delta', 2026, 1, 'alice', 'bob', 'W', 101.0, 99.0)")
    conn.execute("INSERT INTO public.schedule_swap_season VALUES ('luck_delta', 'alice', 'bob', 2026, 1, 0, 0, 1)")

    bundle = build_delta_bundle(
        conn,
        db_name="luck_delta",
        import_mode="quick",
        platform="sleeper",
        output_dir=data_dir / "luck_delta_bundle",
    )
    conn.close()

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "luck_delta",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "COMMITTED"
    assert body["tables"]["all_play"] == 1
    assert body["tables"]["h2h_season"] == 1
    assert body["tables"]["schedule_swap"] == 1
    assert body["tables"]["schedule_swap_season"] == 1


def test_merge_league_delta_accepts_multi_player_transactions_with_sequence(data_dir, client):
    from multi_league.core.delta_publish import build_delta_bundle

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            transaction_id VARCHAR,
            transaction_sequence INTEGER,
            player VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.transactions
        VALUES
            ('txn_sequence', 'txn-1', 0, 'Player A', 2026, 1),
            ('txn_sequence', 'txn-1', 1, 'Player B', 2026, 1)
        """
    )
    bundle = build_delta_bundle(
        conn,
        db_name="txn_sequence",
        import_mode="quick",
        platform="sleeper",
        output_dir=data_dir / "txn_sequence_delta",
    )
    conn.close()

    txn_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "transactions")
    assert txn_entry["primary_keys"] == ["db_name", "transaction_id", "transaction_sequence"]

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "txn_sequence",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )
    assert resp.status_code == 200
    assert resp.json()["tables"]["transactions"] == 2

    resp = client.post(
        "/query",
        json={
            "sql": """
            SELECT COUNT(*) AS cnt,
                   COUNT(DISTINCT transaction_id || ':' || CAST(transaction_sequence AS VARCHAR)) AS distinct_keys
            FROM public.transactions
            WHERE db_name = 'txn_sequence'
            """
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"cnt": 2, "distinct_keys": 2}


def test_merge_league_delta_accepts_same_pick_across_draft_ids(data_dir, client):
    from multi_league.core.delta_publish import build_delta_bundle

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            year INTEGER,
            draft_id VARCHAR,
            round INTEGER,
            pick INTEGER,
            player VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.draft
        VALUES
            ('draft_ids', 2026, 'startup-1', 1, 1, 'Player A'),
            ('draft_ids', 2026, 'rookie-1', 1, 1, 'Player B')
        """
    )
    bundle = build_delta_bundle(
        conn,
        db_name="draft_ids",
        import_mode="quick",
        platform="sleeper",
        output_dir=data_dir / "draft_ids_delta",
    )
    conn.close()

    draft_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "draft")
    assert draft_entry["primary_keys"] == ["db_name", "year", "draft_id", "round", "pick"]

    with open(bundle.path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "draft_ids",
                "x-bundle-id": bundle.bundle_id,
                "x-bundle-hash": bundle.bundle_hash,
            },
            files={"file": (bundle.path.name, fh, "application/gzip")},
        )
    assert resp.status_code == 200
    assert resp.json()["tables"]["draft"] == 2


def test_merge_league_delta_rejects_same_bundle_id_different_hash(data_dir, client):
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(data_dir, main_mod, bundle_id="fixed-bundle")
    with open(archive_path, "rb") as fh:
        first = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-bundle-id": manifest["bundle_id"],
                "x-bundle-hash": manifest["bundle_hash"],
            },
            files={"file": (archive_path.name, fh, "application/gzip")},
        )
    assert first.status_code == 200

    changed_archive, changed_manifest = _make_delta_bundle(
        data_dir,
        main_mod,
        rows=[
            ("test_league", 2026, 1, "alice_2026_1", "Alice Changed"),
            ("test_league", 2026, 1, "bob_2026_1", "Bob"),
        ],
        bundle_id="fixed-bundle",
    )
    assert changed_manifest["bundle_hash"] != manifest["bundle_hash"]
    with open(changed_archive, "rb") as fh:
        conflict = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-bundle-id": changed_manifest["bundle_id"],
                "x-bundle-hash": changed_manifest["bundle_hash"],
            },
            files={"file": (changed_archive.name, fh, "application/gzip")},
        )
    assert conflict.status_code == 409


def test_delta_rollout_gate_rejects_new_unfenced_imports_but_replays_prior_commit(
    data_dir, client, monkeypatch
):
    import main as main_mod

    def post(path, manifest):
        with open(path, "rb") as fh:
            return client.post(
                "/merge-league-delta",
                headers={
                    "Authorization": "Bearer test-admin",
                    "x-db-name": "test_league",
                    "x-bundle-id": manifest["bundle_id"],
                    "x-bundle-hash": manifest["bundle_hash"],
                },
                files={"file": (path.name, fh, "application/gzip")},
            )

    old_path, old_manifest = _make_delta_bundle(data_dir, main_mod, import_run_id="1001")
    assert post(old_path, old_manifest).status_code == 200
    monkeypatch.setenv("REQUIRE_DELTA_BASE_GENERATION", "1")

    replay = post(old_path, old_manifest)
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True

    unfenced_path, unfenced_manifest = _make_delta_bundle(
        data_dir, main_mod, import_run_id="1002"
    )
    unfenced = post(unfenced_path, unfenced_manifest)
    assert unfenced.status_code == 409
    assert "base_generation" in unfenced.text

    result = client.post(
        "/query",
        headers={"Authorization": "Bearer test-read"},
        json={"sql": "SELECT COUNT(*) AS n FROM public.matchup WHERE db_name = 'test_league'"},
    )
    assert result.status_code == 200
    assert result.json()[0]["n"] == 2


def test_delta_snapshot_generation_rejects_stale_import_after_other_publication(
    data_dir, client
):
    import main as main_mod

    def post(archive_path, manifest):
        with open(archive_path, "rb") as fh:
            return client.post(
                "/merge-league-delta",
                headers={
                    "Authorization": "Bearer test-admin",
                    "x-db-name": "test_league",
                    "x-bundle-id": manifest["bundle_id"],
                    "x-bundle-hash": manifest["bundle_hash"],
                },
                files={"file": (archive_path.name, fh, "application/gzip")},
            )

    first_path, first_manifest = _make_delta_bundle(
        data_dir, main_mod, import_run_id="1001", base_generation=0
    )
    assert post(first_path, first_manifest).status_code == 200

    stale_path, stale_manifest = _make_delta_bundle(
        data_dir, main_mod, import_run_id="1002", base_generation=0,
        rows=[
            ("test_league", 2026, 1, "alice_2026_1", "Stale Alice"),
            ("test_league", 2026, 1, "bob_2026_1", "Bob"),
        ],
    )
    stale = post(stale_path, stale_manifest)
    assert stale.status_code == 409, stale.text
    assert "generation" in stale.text.lower()

    current_path, current_manifest = _make_delta_bundle(
        data_dir, main_mod, import_run_id="1003", base_generation=1,
        rows=[
            ("test_league", 2026, 1, "alice_2026_1", "Current Alice"),
            ("test_league", 2026, 1, "bob_2026_1", "Bob"),
        ],
    )
    assert post(current_path, current_manifest).status_code == 200
    result = client.post(
        "/query",
        headers={"Authorization": "Bearer test-read"},
        json={
            "sql": "SELECT manager FROM public.matchup "
                   "WHERE db_name = 'test_league' AND manager_week = 'alice_2026_1'",
            "database": "___leagues",
        },
    )
    assert result.status_code == 200
    assert result.json()[0]["manager"] == "Current Alice"


def test_merge_league_delta_validation_failure_preserves_old_state(data_dir, client):
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(
        data_dir,
        main_mod,
        rows=[
            ("test_league", 2026, 1, "duplicate_key", "Alice"),
            ("test_league", 2026, 1, "duplicate_key", "Bob"),
        ],
    )
    with open(archive_path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "test_league",
                "x-bundle-id": manifest["bundle_id"],
                "x-bundle-hash": manifest["bundle_hash"],
            },
            files={"file": (archive_path.name, fh, "application/gzip")},
        )
    assert resp.status_code == 400
    assert "duplicate identity" in resp.json()["detail"]

    resp = client.post(
        "/query",
        json={"sql": "SELECT COUNT(*) AS cnt FROM public.matchup"},
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"cnt": 2}


def test_merge_league_delta_handles_thirty_parallel_uploads(data_dir, client):
    import main as main_mod

    upload_paths = []
    manifests = []
    for i in range(30):
        path, manifest = _make_delta_bundle(
            data_dir,
            main_mod,
            db_name=f"delta_parallel_{i}",
            import_run_id=str(2000 + i),
        )
        upload_paths.append(path)
        manifests.append(manifest)

    def upload_one(i: int):
        last_resp = None
        for attempt in range(30):
            with open(upload_paths[i], "rb") as fh:
                resp = client.post(
                    "/merge-league-delta",
                    headers={
                        "Authorization": "Bearer test-admin",
                        "x-db-name": manifests[i]["db_name"],
                        "x-bundle-id": manifests[i]["bundle_id"],
                        "x-bundle-hash": manifests[i]["bundle_hash"],
                    },
                    files={"file": (upload_paths[i].name, fh, "application/gzip")},
                )
            last_resp = resp
            if resp.status_code not in {429, 503}:
                break
            time.sleep(0.05 * (attempt + 1))
        assert last_resp is not None
        return last_resp.status_code, last_resp.json()

    with ThreadPoolExecutor(max_workers=30) as pool:
        results = list(pool.map(upload_one, range(30)))

    assert all(status == 200 for status, _body in results)
    assert all(body["status"] == "COMMITTED" for _status, body in results)
    assert all(body["tables"]["matchup"] == 2 for _status, body in results)

    resp = client.post(
        "/query",
        json={
            "sql": """
            SELECT COUNT(DISTINCT db_name) AS leagues, COUNT(*) AS rows
            FROM public.matchup
            WHERE db_name LIKE 'delta_parallel_%'
            """
        },
        headers={"Authorization": "Bearer test-read"},
    )
    assert resp.status_code == 200
    assert resp.json()[0] == {"leagues": 30, "rows": 60}


def test_merge_league_rejects_invalid_db_name(client):
    resp = client.post(
        "/merge-league",
        headers={
            "Authorization": "Bearer test-admin",
            "x-db-name": "../evil",
        },
        files={"file": ("evil.duckdb", b"fake", "application/octet-stream")},
    )
    assert resp.status_code == 400
