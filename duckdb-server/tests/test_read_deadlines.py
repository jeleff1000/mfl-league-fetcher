"""Short worker reads must stop before their caller retries, without touching writes."""

import asyncio
import threading
import time

import duckdb
import pytest
from fastapi import HTTPException
from starlette.requests import Request


@pytest.fixture
def local_ops(monkeypatch):
    import main
    import pyarrow.parquet  # Warm the codec before measuring native SQL interruption.

    conn = duckdb.connect(":memory:", config={"threads": 1})
    conn.execute("SELECT sum(sin(i)) FROM range(2) tbl(i)").fetchone()
    monkeypatch.setattr(main, "validate_read_token", lambda _token: None)
    monkeypatch.setitem(main._state, "status", "serving")
    monkeypatch.setattr(main.db, "get_ops_connection", lambda: conn)
    monkeypatch.setattr(main, "_ops_rebuild_lock", threading.RLock())
    monkeypatch.setattr(main.db, "_ops_lock", threading.RLock())
    yield main, conn
    conn.close()


def request():
    return Request({"type": "http", "headers": [(b"authorization", b"Bearer test"),
                                                (b"x-request-id", b"a1b2c3-1")]})


@pytest.mark.parametrize("parquet", [False, True])
def test_short_ops_read_interrupts_and_releases_connection_before_retry(local_ops, parquet):
    main, conn = local_ops

    async def run():
        # Measure native request execution, not cold Windows executor startup.
        await asyncio.to_thread(lambda: None)
        safety = threading.Timer(0.6, conn.interrupt)
        safety.start()
        start = time.monotonic()
        endpoint = main.query_parquet_endpoint if parquet else main.query_endpoint
        try:
            with pytest.raises(HTTPException) as exc:
                await endpoint(main.QueryRequest(
                    sql="SELECT sum(sin(i)) FROM range(1000000000) tbl(i)",
                    database="___ops", timeout_seconds=0.05,
                ), request())
            assert exc.value.status_code == 504
            assert time.monotonic() - start < 0.4, exc.value.headers
            assert conn.execute("SELECT 216 AS rank_alltime_qb_4pt").fetchone() == (216,)
            assert main.db._ops_lock.acquire(timeout=0.01)
            main.db._ops_lock.release()
        finally:
            safety.cancel()

    asyncio.run(run())


def test_short_ops_read_bounds_wait_for_existing_writer(local_ops):
    main, conn = local_ops
    gate = threading.Lock()
    gate.acquire()
    main._ops_rebuild_lock = gate
    safety = threading.Timer(0.6, gate.release)
    safety.start()
    start = time.monotonic()
    try:
        with pytest.raises(HTTPException) as exc:
            asyncio.run(main.query_parquet_endpoint(main.QueryRequest(
                sql="SELECT 216 AS rank_alltime_qb_4pt", database="___ops", timeout_seconds=0.05,
            ), request()))
        assert exc.value.status_code == 504
        assert time.monotonic() - start < 0.4
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        safety.cancel()
        if gate.locked():
            gate.release()


@pytest.mark.parametrize("budget", [0, -1, float("nan"), float("inf")])
def test_invalid_read_deadline_is_rejected_before_query(local_ops, budget, monkeypatch):
    main, _conn = local_ops

    def forbidden(*_args, **_kwargs):
        raise AssertionError("invalid timeout must not start SQL")

    monkeypatch.setattr(main, "_execute_ops_query_parquet", forbidden)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.query_parquet_endpoint(main.QueryRequest(
            sql="SELECT 1", database="___ops", timeout_seconds=budget,
        ), request()))
    assert exc.value.status_code == 400


def test_successful_parquet_read_preserves_ranks_and_reports_timings(local_ops):
    from io import BytesIO
    import pyarrow.parquet as pq

    main, _conn = local_ops
    response = asyncio.run(main.query_parquet_endpoint(main.QueryRequest(
        sql="SELECT 216 AS rank_alltime_qb_4pt", database="___ops", timeout_seconds=1,
    ), request()))
    assert pq.read_table(BytesIO(response.body)).to_pylist() == [{"rank_alltime_qb_4pt": 216}]
    assert response.headers["x-request-id"] == "a1b2c3-1"
    assert "queue;dur=" in response.headers["server-timing"]
    assert "execute;dur=" in response.headers["server-timing"]


def test_ops_json_keeps_fastapi_date_decimal_encoding(local_ops):
    import json
    main, _conn = local_ops
    response = asyncio.run(main.query_endpoint(main.QueryRequest(
        sql="SELECT DATE '2026-10-16' AS game_date, 24.50::DECIMAL(6,2) AS points",
        database="___ops", timeout_seconds=1,
    ), request()))
    assert json.loads(response.body) == [{"game_date": "2026-10-16", "points": 24.5}]


def test_short_ops_budget_includes_admission_wait(local_ops, monkeypatch):
    main, _conn = local_ops
    monkeypatch.setattr(main, "_ops_read_uses_pool", lambda _sql: True)
    # Real exhausted admission, not a stub that can ignore the caller deadline.
    monkeypatch.setattr(main, "_query_semaphore", asyncio.Semaphore(0))
    start = time.monotonic()
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.query_parquet_endpoint(main.QueryRequest(
            sql="SELECT 1", database="___ops", timeout_seconds=0.02,
        ), request()))
    assert exc.value.status_code == 504
    assert time.monotonic() - start < 0.2


@pytest.mark.parametrize("parquet", [False, True])
def test_disconnected_read_retains_ownership_until_native_worker_exits(local_ops, monkeypatch, parquet):
    main, conn = local_ops
    entered = threading.Event()
    finished = threading.Event()
    name = "_execute_ops_query_parquet" if parquet else "_execute_ops_query"
    original = getattr(main, name)

    def native_read(*args, **kwargs):
        entered.set()
        try:
            return original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(main, name, native_read)

    async def disconnect():
        endpoint = main.query_parquet_endpoint if parquet else main.query_endpoint
        task = asyncio.create_task(endpoint(main.QueryRequest(
            sql="SELECT sum(sin(i)) FROM range(1000000000) tbl(i)",
            database="___ops", timeout_seconds=0.1,
        ), request()))
        while not entered.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(disconnect())
    assert conn.execute("SELECT 216 AS rank_alltime_qb_4pt").fetchone() == (216,)
