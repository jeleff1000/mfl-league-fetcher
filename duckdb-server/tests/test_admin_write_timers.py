"""Admin autocommit writes may checkpoint without an explicit CHECKPOINT SQL."""

import threading

import pytest

from tests.test_integration import client, data_dir  # noqa: F401


def _write(http_client, database, sql):
    return http_client.post("/query-rw", headers={"Authorization": "Bearer test-admin"},
                       json={"database": database, "sql": sql})


@pytest.mark.parametrize("database", ["___leagues", "___ops"])
def test_autocommit_checkpoint_does_not_arm_process_exit(client, data_dir, monkeypatch, database):  # noqa: F811
    import main

    create = _write(client, database,
                    "CREATE TABLE public.timer_canary (db_name VARCHAR, value INTEGER)")
    assert create.status_code == 200, create.text
    timers, exits, wal_sizes = [], [], []
    real_start = threading.Timer.start

    def start(timer):
        if timer.function.__name__ == "hard_exit":
            timer.interval = 0
            timers.append(timer)  # Fire deterministically at the autocommit boundary.
        else:
            real_start(timer)

    real_connect = main.db.connect_database
    wrapped = False
    insert = "INSERT INTO public.timer_canary VALUES ('timer_canary', 7)"

    class Connection:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def execute(self, sql, *args, **kwargs):
            if sql == insert:
                for timer in timers:
                    timer.run()
            result = self.conn.execute(sql, *args, **kwargs)
            if sql == insert:
                wal = data_dir / f"{database}.duckdb.wal"
                wal_sizes.append(wal.stat().st_size if wal.exists() else 0)
            return result

    def connect(path, **kwargs):
        nonlocal wrapped
        conn = real_connect(path, **kwargs)
        if not wrapped and path.name == f"{database}.duckdb":
            wrapped = True
            conn.execute("SET checkpoint_threshold='1B'")
            return Connection(conn)
        return conn

    monkeypatch.setattr(threading.Timer, "start", start)
    monkeypatch.setattr(main.os, "_exit", exits.append)
    monkeypatch.setattr(main.db, "connect_database", connect)
    if database == "___ops":
        main.db.get_ops_connection().execute("SET checkpoint_threshold='1B'")
    response = _write(client, database, insert)
    assert response.status_code == 200, response.text
    if database == "___ops":
        wal = data_dir / "___ops.duckdb.wal"
        wal_sizes.append(wal.stat().st_size if wal.exists() else 0)
    assert wal_sizes == [0], "The insert must actually checkpoint, not just leave a WAL"
    persisted = _write(client, database,
                       "SELECT value FROM public.timer_canary WHERE db_name='timer_canary'")
    assert persisted.status_code == 200, persisted.text
    assert persisted.json() == [{"value": 7}]
    assert exits == []
    assert timers == []


@pytest.mark.parametrize("database", ["___leagues", "___ops"])
def test_admin_write_interrupt_still_returns_timeout_and_rolls_back(client, monkeypatch, database):  # noqa: F811
    import main

    create = _write(client, database,
                    "CREATE TABLE public.interrupt_canary (db_name VARCHAR, value HUGEINT)")
    assert create.status_code == 200, create.text
    monkeypatch.setattr(main, "ADMIN_QUERY_TIMEOUT", 0.02)
    response = _write(client, database,
                      "INSERT INTO public.interrupt_canary "
                      "SELECT 'interrupt_canary', SUM(i) FROM range(1000000000000) t(i)")
    assert response.status_code == 504, response.text
    monkeypatch.setattr(main, "ADMIN_QUERY_TIMEOUT", 5)
    persisted = _write(client, database,
                       "SELECT COUNT(*) AS n FROM public.interrupt_canary "
                       "WHERE db_name='interrupt_canary'")
    assert persisted.status_code == 200, persisted.text
    assert persisted.json() == [{"n": 0}]
    assert client.get("/ready").json()["ready"] is True
