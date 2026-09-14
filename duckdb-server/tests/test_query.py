"""Tests for query endpoint and db module."""

import pytest
import duckdb
from unittest.mock import patch


def test_pool_init_and_query(tmp_path):
    """Connection pool initializes and serves queries."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
    conn.execute("INSERT INTO test VALUES (1, 'alice'), (2, 'bob')")
    conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "2"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        c = db.acquire_connection()
        result = c.execute("SELECT * FROM test ORDER BY id").fetchall()
        db.release_connection(c)
        assert result == [(1, "alice"), (2, "bob")]
        db.close_all()


def test_pool_exhaustion(tmp_path):
    """Pool raises when all connections are checked out."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "1"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        c = db.acquire_connection()
        with pytest.raises(RuntimeError, match="exhausted"):
            db.acquire_connection(timeout=0.1)
        db.release_connection(c)
        db.close_all()


def test_is_read_only_sql():
    """Write statements are blocked at validation level."""
    from main import is_read_only_sql

    assert is_read_only_sql("SELECT 1") is True
    assert is_read_only_sql("WITH x AS (SELECT 1) SELECT * FROM x") is True
    assert is_read_only_sql("DESCRIBE tablename") is True
    assert is_read_only_sql("SHOW TABLES") is True
    assert is_read_only_sql("  select * from t") is True
    assert is_read_only_sql("SELECT 'DROP TABLE public.matchup' AS text") is True
    assert is_read_only_sql("SELECT 1 -- DROP TABLE public.matchup") is True
    assert is_read_only_sql("INSERT INTO foo VALUES (1)") is False
    assert is_read_only_sql("DROP TABLE foo") is False
    assert is_read_only_sql("  delete FROM foo") is False
    assert is_read_only_sql("CREATE TABLE x (id INT)") is False
    assert is_read_only_sql("ALTER TABLE x ADD col INT") is False
    assert is_read_only_sql("TRUNCATE TABLE x") is False
    assert is_read_only_sql("ATTACH 'file.db'") is False
    assert is_read_only_sql("COPY t TO 'out.csv'") is False
    assert is_read_only_sql("SELECT 1; DROP TABLE foo") is False
    assert is_read_only_sql("WITH x AS (DELETE FROM foo RETURNING *) SELECT * FROM x") is False
    assert is_read_only_sql("PRAGMA database_list") is False
    assert is_read_only_sql("") is False


def test_is_read_only_sql_blocks_table_functions():
    """File/URL/env reader functions start with SELECT and carry no mutation token,
    so they need their own denylist (2026-07-11 audit P0)."""
    from main import is_read_only_sql

    assert is_read_only_sql("SELECT * FROM read_csv('/etc/passwd')") is False
    assert is_read_only_sql("SELECT * FROM read_csv_auto('http://evil/x.csv')") is False
    assert is_read_only_sql("select * from READ_PARQUET('s3://bucket/x')") is False
    assert is_read_only_sql("SELECT * FROM parquet_scan('x.parquet')") is False
    assert is_read_only_sql("SELECT * FROM read_json_objects('x.json')") is False
    assert is_read_only_sql("SELECT * FROM read_text('/proc/self/environ')") is False
    assert is_read_only_sql("SELECT * FROM glob('/data/*')") is False
    assert is_read_only_sql("SELECT getenv('HOME')") is False
    assert is_read_only_sql("WITH x AS (SELECT * FROM read_blob('f')) SELECT * FROM x") is False
    # The names as plain identifiers (no call parens) stay legal.
    assert is_read_only_sql("SELECT read_csv FROM t") is True
    assert is_read_only_sql("SELECT glob_pattern FROM settings") is True
    assert is_read_only_sql("SELECT 'read_csv(x)' AS label") is True


def test_access_hardening_applied(tmp_path):
    """Engine-level lockdown: external reads outside DATA_DIR die even if a query
    slipped past the validators; ATTACH under DATA_DIR keeps working."""
    db_path = tmp_path / "___leagues.duckdb"
    side_db = tmp_path / "___ops.duckdb"
    for p in (db_path, side_db):
        conn = duckdb.connect(str(p))
        conn.execute("CREATE TABLE t (x INT)")
        conn.close()

    outside = tmp_path.parent / "outside.csv"
    outside.write_text("a,b\n1,2\n")

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "1"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        c = db.acquire_connection()
        try:
            assert c.execute("SELECT current_setting('enable_external_access')").fetchone()[0] is False
            # Reads outside DATA_DIR are dead at the engine.
            with pytest.raises(duckdb.Error):
                c.execute(f"SELECT * FROM read_csv('{outside.as_posix()}')")
            # The lazy ___ops attach path (inside DATA_DIR) still works.
            c.execute(f"ATTACH IF NOT EXISTS '{side_db.as_posix()}' AS side (READ_ONLY)")
            assert c.execute("SELECT COUNT(*) FROM side.main.t").fetchone()[0] == 0
        finally:
            db.release_connection(c)
            db.close_all()


def test_duckdb_guardrails_applied(tmp_path):
    """DuckDB memory_limit, threads, and temp_directory are set on pool init."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "2"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        c = db.acquire_connection()
        c2 = db.acquire_connection()
        try:
            # memory_limit should be set (not the default 80%)
            # On Windows (dev), falls back to hardcoded 1536MB.
            # On Linux (Fly), computes ~38% of system RAM.
            for conn in (c, c2):
                limit = conn.execute("SELECT value FROM duckdb_settings() WHERE name = 'memory_limit'").fetchone()[0]
                assert limit != "80%", f"memory_limit should not be default 80%, got {limit}"

                threads = conn.execute("SELECT value FROM duckdb_settings() WHERE name = 'threads'").fetchone()[0]
                assert threads == "2", f"Expected threads=2, got {threads}"

                temp_dir = conn.execute("SELECT value FROM duckdb_settings() WHERE name = 'temp_directory'").fetchone()[
                    0
                ]
                assert "duckdb_tmp" in temp_dir, f"Expected duckdb_tmp in temp_directory, got {temp_dir}"
        finally:
            db.release_connection(c)
            db.release_connection(c2)
            db.close_all()


def test_execute_with_timeout_succeeds(tmp_path):
    """Fast query returns expected result within timeout."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INT, name VARCHAR)")
    conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

    from main import _execute_with_timeout

    result = _execute_with_timeout(conn, "SELECT * FROM t ORDER BY id", 5.0)
    conn.close()
    assert result == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


def test_execute_with_timeout_ddl(tmp_path):
    """DDL with no result set returns empty list."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    from main import _execute_with_timeout

    result = _execute_with_timeout(conn, "CREATE TABLE t (id INT)", 5.0)
    conn.close()
    assert result == []


def test_execute_with_timeout_interrupts(tmp_path):
    """Slow cross-join query with short timeout raises TimeoutError."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE big AS SELECT range AS x FROM range(10000)")

    from main import _execute_with_timeout

    with pytest.raises(TimeoutError, match="exceeded"):
        # Cross-join 10k x 10k x 10k = 1 trillion rows — will never finish
        _execute_with_timeout(
            conn,
            "SELECT COUNT(*) FROM big a, big b, big c",
            0.5,
        )
    conn.close()


def test_check_db_health_returns_true_when_pool_works(tmp_path):
    """_check_db_health returns True when pool is functional."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "2"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()

        from main import _check_db_health

        assert _check_db_health() is True
        db.close_all()


def test_check_db_health_returns_false_when_pool_broken(tmp_path):
    """_check_db_health returns False when pool is closed."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "2"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        db.close_all()

        from main import _check_db_health

        assert _check_db_health() is False


def test_active_count_tracking(tmp_path):
    """Active count increments/decrements correctly."""
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE t (x INT)")
    conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "3"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        assert db.get_active_count() == 0
        c1 = db.acquire_connection()
        assert db.get_active_count() == 1
        c2 = db.acquire_connection()
        assert db.get_active_count() == 2
        db.release_connection(c1)
        assert db.get_active_count() == 1
        db.release_connection(c2)
        assert db.get_active_count() == 0
        db.close_all()


def test_ops_attaches_only_for_cross_database_query(tmp_path):
    """Public pool connections attach ___ops only while a query needs it."""
    leagues_path = tmp_path / "___leagues.duckdb"
    ops_path = tmp_path / "___ops.duckdb"
    leagues_conn = duckdb.connect(str(leagues_path))
    leagues_conn.execute("CREATE TABLE matchup (id INT)")
    leagues_conn.close()
    ops_conn = duckdb.connect(str(ops_path))
    ops_conn.execute("CREATE TABLE main.ops_smoke (name VARCHAR)")
    ops_conn.execute("INSERT INTO main.ops_smoke VALUES ('ok')")
    ops_conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "1"}):
        import importlib
        import db

        importlib.reload(db)
        db.init_pool()
        conn = db.acquire_connection()
        try:
            attached_before = conn.execute(
                "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = '___ops'"
            ).fetchone()[0]
            assert attached_before == 0

            from main import _execute_query

            result = _execute_query(conn, "SELECT name FROM ___ops.main.ops_smoke", "___leagues")
            assert result == [{"name": "ok"}]

            quoted_result = _execute_query(
                conn,
                'SELECT name FROM "___ops".main.ops_smoke',
                "___leagues",
            )
            assert quoted_result == [{"name": "ok"}]

            attached_after = conn.execute(
                "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = '___ops'"
            ).fetchone()[0]
            assert attached_after == 0
        finally:
            db.release_connection(conn)
            db.close_all()


def test_concurrent_cross_database_queries_keep_ops_attached_until_all_finish(tmp_path, monkeypatch):
    """One query must not detach ___ops while another query is still using it."""
    import importlib
    import threading
    from concurrent.futures import ThreadPoolExecutor

    leagues_path = tmp_path / "___leagues.duckdb"
    ops_path = tmp_path / "___ops.duckdb"
    leagues_conn = duckdb.connect(str(leagues_path))
    leagues_conn.execute("CREATE TABLE matchup (id INT)")
    leagues_conn.close()
    ops_conn = duckdb.connect(str(ops_path))
    ops_conn.execute("CREATE TABLE main.ops_smoke (name VARCHAR)")
    ops_conn.execute("INSERT INTO main.ops_smoke VALUES ('ok')")
    ops_conn.close()

    with patch.dict("os.environ", {"DATA_DIR": str(tmp_path), "DB_POOL_SIZE": "2"}):
        import db
        import main as main_mod

        importlib.reload(db)
        db.init_pool()
        first_conn = db.acquire_connection()
        second_conn = db.acquire_connection()
        original_execute = main_mod._execute_with_timeout
        both_started = threading.Barrier(2)
        first_finished = threading.Event()
        call_lock = threading.Lock()
        call_count = 0

        def coordinated_execute(conn, sql, timeout_seconds):
            nonlocal call_count
            with call_lock:
                call_index = call_count
                call_count += 1
            both_started.wait(timeout=5)
            if call_index == 0:
                return [{"name": "first"}]
            assert first_finished.wait(timeout=5)
            return original_execute(conn, sql, timeout_seconds)

        monkeypatch.setattr(main_mod, "_execute_with_timeout", coordinated_execute)
        sql = "SELECT name FROM ___ops.main.ops_smoke"

        def run_first():
            try:
                return main_mod._execute_query(first_conn, sql, "___leagues")
            finally:
                first_finished.set()

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first_future = pool.submit(run_first)
                second_future = pool.submit(main_mod._execute_query, second_conn, sql, "___leagues")
                assert first_future.result(timeout=10) == [{"name": "first"}]
                assert second_future.result(timeout=10) == [{"name": "ok"}]
        finally:
            db.release_connection(first_conn)
            db.release_connection(second_conn)
            db.close_all()
