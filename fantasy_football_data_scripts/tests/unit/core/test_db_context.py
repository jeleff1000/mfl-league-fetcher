import pandas as pd
import pytest

from multi_league.core.db_context import DbContext


class _FakeCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, describe_rows=None):
        self.describe_rows = describe_rows or []
        self.sql = []
        self.registered = {}

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if sql.startswith("DESCRIBE public."):
            return _FakeCursor(self.describe_rows)
        return _FakeCursor()

    def register(self, name, df):
        self.registered[name] = df

    def unregister(self, name):
        self.registered.pop(name, None)


def _make_db(fake_conn):
    db = DbContext.__new__(DbContext)
    db.db_name = "test_db"
    db.conn = fake_conn
    db._is_local = True
    return db


def test_write_table_uses_canonical_ddl_for_core_tables():
    fake_conn = _FakeConn()
    db = _make_db(fake_conn)
    df = pd.DataFrame({"year": [2025], "week": [1], "manager": ["Alice"], "avg_seed": [2.5]})

    db.write_table("matchup", df, mode="replace")

    assert any("CREATE TABLE IF NOT EXISTS public.matchup" in sql for sql in fake_conn.sql)
    assert any("DELETE FROM public.matchup" in sql for sql in fake_conn.sql)
    assert any("INSERT INTO public.matchup" in sql and '"avg_seed"' in sql for sql in fake_conn.sql)
    assert not any("DROP TABLE IF EXISTS public.matchup" in sql for sql in fake_conn.sql)


def test_update_columns_adds_canonical_column_types():
    fake_conn = _FakeConn(describe_rows=[("year", "INTEGER"), ("week", "INTEGER"), ("manager", "VARCHAR")])
    db = _make_db(fake_conn)
    df = pd.DataFrame({"year": [2025], "week": [1], "manager": ["Alice"], "avg_seed": [2.5]})

    db.update_columns("matchup", df, key_cols=["year", "week", "manager"], update_cols=["avg_seed"])

    assert any('ALTER TABLE public.matchup ADD COLUMN IF NOT EXISTS "avg_seed" DOUBLE' in sql for sql in fake_conn.sql)
    assert any("UPDATE public.matchup t" in sql for sql in fake_conn.sql)


def test_update_columns_rejects_noncanonical_core_columns():
    fake_conn = _FakeConn()
    db = _make_db(fake_conn)
    df = pd.DataFrame({"year": [2025], "week": [1], "manager": ["Alice"], "mystery_metric": [7]})

    with pytest.raises(ValueError, match="mystery_metric"):
        db.update_columns("matchup", df, key_cols=["year", "week", "manager"], update_cols=["mystery_metric"])
