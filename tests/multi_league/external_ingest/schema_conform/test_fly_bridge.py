"""Unit tests for PHASE 1.7 Fly bridge (pull/push helpers).

All Fly I/O is mocked — these tests run without DATABASE_SERVER_URL or
DATABASE_ADMIN_TOKEN being set.
"""

import duckdb
import pandas as pd
from unittest.mock import MagicMock, patch

from multi_league.external_ingest.schema_conform._fly_bridge import (
    pull_staging_to_local,
    push_conformed_to_fly,
)


# ---------------------------------------------------------------------------
# pull_staging_to_local
# ---------------------------------------------------------------------------


def test_pull_staging_to_local_copies_rows():
    """Three matchup rows on Fly -> materialised in local staging.staging_matchup."""
    conn = duckdb.connect(":memory:")
    fake_reader = MagicMock()
    # Fly information_schema reports staging_matchup exists.
    fake_reader.query.return_value = [{"table_name": "staging_matchup"}]
    fake_reader.query_df.return_value = pd.DataFrame(
        {
            "db_name": ["kmffl"] * 3,
            "year": ["2014"] * 3,
            "manager": ["Adin", "Marc", "Tani"],
        }
    )

    counts = pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)

    assert counts["matchup"] == 3
    n = conn.execute("SELECT count(*) FROM staging.staging_matchup").fetchone()[0]
    assert n == 3


def test_pull_staging_to_local_other_tables_zero():
    """Tables not present on Fly get count=0."""
    conn = duckdb.connect(":memory:")
    fake_reader = MagicMock()
    # Only staging_matchup exists.
    fake_reader.query.return_value = [{"table_name": "staging_matchup"}]
    fake_reader.query_df.return_value = pd.DataFrame({"db_name": ["kmffl"], "year": ["2014"], "manager": ["Adin"]})

    counts = pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)

    assert counts["matchup"] == 1
    assert counts["draft"] == 0
    assert counts["transactions"] == 0
    assert counts["player_fantasy"] == 0


def test_pull_no_op_when_no_fly_tables():
    """Returns all zeros when Fly has no staging tables."""
    conn = duckdb.connect(":memory:")
    fake_reader = MagicMock()
    fake_reader.query.return_value = []  # no tables

    counts = pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)

    assert all(n == 0 for n in counts.values())


def test_pull_idempotent():
    """Calling pull twice does not raise and keeps only latest rows."""
    conn = duckdb.connect(":memory:")
    fake_reader = MagicMock()
    fake_reader.query.return_value = [{"table_name": "staging_matchup"}]
    fake_reader.query_df.return_value = pd.DataFrame({"db_name": ["kmffl"], "year": ["2014"], "manager": ["Adin"]})

    pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)
    pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)

    n = conn.execute("SELECT count(*) FROM staging.staging_matchup").fetchone()[0]
    assert n == 1  # idempotent — not doubled


def test_pull_treats_reader_failure_as_no_staging():
    """When Fly is briefly 5xx (reader raises RuntimeError), Phase 1.7 must
    degrade gracefully: pull returns all-zero counts and does NOT raise.

    Phase 1.7 is optional/conditional — staging tables only matter if they
    exist. A transient probe failure must not abort the entire import.
    """
    conn = duckdb.connect(":memory:")
    fake_reader = MagicMock()
    fake_reader.query.side_effect = RuntimeError("Query failed (503): ")

    counts = pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)

    assert all(n == 0 for n in counts.values())
    fake_reader.query_df.assert_not_called()


def test_pull_treats_502_504_as_no_staging():
    """Same graceful degradation for 502/504 (proxy hiccups, gateway timeouts)."""
    conn = duckdb.connect(":memory:")

    for status in (502, 504):
        fake_reader = MagicMock()
        fake_reader.query.side_effect = RuntimeError(f"Query failed ({status}): ")

        counts = pull_staging_to_local(conn, db_name="kmffl", reader=fake_reader)

        assert all(n == 0 for n in counts.values()), f"status={status} should degrade gracefully"


# ---------------------------------------------------------------------------
# push_conformed_to_fly
# ---------------------------------------------------------------------------


def test_push_conformed_to_fly_calls_writer():
    """Two conformed matchup rows -> writer called for CREATE / ALTER / DELETE / INSERT."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute(
        """
        CREATE TABLE staging.conformed_matchup
        (db_name VARCHAR, year INTEGER, manager VARCHAR, manager_guid VARCHAR)
        """
    )
    conn.execute(
        """
        INSERT INTO staging.conformed_matchup VALUES
        ('kmffl', 2014, 'Adin', 'GUID_ADIN'),
        ('kmffl', 2014, 'Marc', 'GUID_MARC')
        """
    )

    fake_writer = MagicMock()
    fake_reader = MagicMock()

    counts = push_conformed_to_fly(conn, db_name="kmffl", writer=fake_writer, reader=fake_reader)

    assert counts["matchup"] == 2
    # Non-matchup tables have no local conformed data.
    assert counts["draft"] == 0
    assert counts["transactions"] == 0
    assert counts["player_fantasy"] == 0

    sqls = [c.args[0] for c in fake_writer.execute.call_args_list]
    assert any("CREATE TABLE" in s for s in sqls), "Missing CREATE TABLE call"
    assert any("DELETE FROM" in s and "kmffl" in s for s in sqls), "Missing DELETE call"
    assert any("INSERT INTO" in s for s in sqls), "Missing INSERT call"
    # At least one ALTER per column (4 columns -> 4 ALTER calls for matchup).
    assert fake_writer.execute.call_count >= 4


def test_push_inserts_correct_values():
    """Inserted rows contain the actual data values."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute("CREATE TABLE staging.conformed_matchup (db_name VARCHAR, manager VARCHAR)")
    conn.execute("INSERT INTO staging.conformed_matchup VALUES ('kmffl', 'Adin')")

    fake_writer = MagicMock()
    fake_reader = MagicMock()

    push_conformed_to_fly(conn, db_name="kmffl", writer=fake_writer, reader=fake_reader)

    sqls = [c.args[0] for c in fake_writer.execute.call_args_list]
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    assert insert_sqls, "No INSERT statement found"
    assert "Adin" in insert_sqls[0], "Manager value missing from INSERT"
    assert "kmffl" in insert_sqls[0], "db_name missing from INSERT"


def test_push_no_op_when_no_local_rows():
    """Writer is never called when no local conformed tables exist."""
    conn = duckdb.connect(":memory:")
    fake_writer = MagicMock()
    fake_reader = MagicMock()

    counts = push_conformed_to_fly(conn, db_name="kmffl", writer=fake_writer, reader=fake_reader)

    assert all(n == 0 for n in counts.values())
    fake_writer.execute.assert_not_called()


def test_push_null_values_become_sql_null():
    """NULL cells in the conformed DataFrame become SQL NULL in the INSERT."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute("CREATE TABLE staging.conformed_matchup (db_name VARCHAR, manager VARCHAR, team_key VARCHAR)")
    conn.execute("INSERT INTO staging.conformed_matchup VALUES ('kmffl', 'Adin', NULL)")

    fake_writer = MagicMock()
    fake_reader = MagicMock()

    push_conformed_to_fly(conn, db_name="kmffl", writer=fake_writer, reader=fake_reader)

    sqls = [c.args[0] for c in fake_writer.execute.call_args_list]
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    assert insert_sqls
    # The NULL column should appear as literal NULL, not the string 'None'.
    assert "NULL" in insert_sqls[0]
    assert "'None'" not in insert_sqls[0]


def test_push_batches_large_dataframe():
    """200+ rows are split across multiple INSERT statements."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute("CREATE TABLE staging.conformed_matchup (db_name VARCHAR, manager VARCHAR)")
    for i in range(250):
        conn.execute(f"INSERT INTO staging.conformed_matchup VALUES ('kmffl', 'mgr{i}')")

    fake_writer = MagicMock()
    fake_reader = MagicMock()

    counts = push_conformed_to_fly(conn, db_name="kmffl", writer=fake_writer, reader=fake_reader)

    assert counts["matchup"] == 250
    sqls = [c.args[0] for c in fake_writer.execute.call_args_list]
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    # 250 rows with BATCH=200 -> at least 2 INSERT statements.
    assert len(insert_sqls) >= 2


def test_push_only_rows_for_requested_db_name():
    """Rows belonging to a different db_name are not pushed."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute("CREATE TABLE staging.conformed_matchup (db_name VARCHAR, manager VARCHAR)")
    conn.execute("INSERT INTO staging.conformed_matchup VALUES ('kmffl', 'Adin')")
    conn.execute("INSERT INTO staging.conformed_matchup VALUES ('other_league', 'Bob')")

    fake_writer = MagicMock()
    fake_reader = MagicMock()

    counts = push_conformed_to_fly(conn, db_name="kmffl", writer=fake_writer, reader=fake_reader)

    # Only kmffl rows counted.
    assert counts["matchup"] == 1
    sqls = [c.args[0] for c in fake_writer.execute.call_args_list]
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    assert insert_sqls
    assert "Bob" not in insert_sqls[0], "other_league row leaked into INSERT"


# ---------------------------------------------------------------------------
# clear_staging_tables (staging_reader)
# ---------------------------------------------------------------------------


def test_clear_staging_tables_includes_conformed():
    """clear_staging_tables must DELETE from conformed_* tables too, not just staging_*."""
    from multi_league.data_fetchers.shared import staging_reader

    fake_reader = MagicMock()
    fake_writer = MagicMock()
    fake_reader.query.return_value = [
        {"table_name": "staging_matchup"},
        {"table_name": "conformed_matchup"},
        {"table_name": "conformed_player"},
    ]

    with (
        patch("multi_league.data_fetchers.shared.staging_reader._get_reader", return_value=fake_reader),
        patch("multi_league.data_fetchers.shared.staging_reader._get_writer", return_value=fake_writer),
    ):
        staging_reader.clear_staging_tables("kmffl", log_func=lambda *_: None)

    sqls = [call.args[0] for call in fake_writer.execute.call_args_list]
    assert any(
        "DELETE FROM" in s and "conformed_matchup" in s for s in sqls
    ), f"clear_staging_tables didn't DELETE conformed_matchup; saw: {sqls}"
    assert any(
        "DELETE FROM" in s and "conformed_player" in s for s in sqls
    ), f"clear_staging_tables didn't DELETE conformed_player; saw: {sqls}"
