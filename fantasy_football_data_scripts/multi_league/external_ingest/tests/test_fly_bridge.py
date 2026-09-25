from __future__ import annotations

import duckdb

from multi_league.external_ingest.schema_conform._fly_bridge import (
    push_conformed_to_fly,
)


class RecordingWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute(self, sql: str, *, database: str):
        self.calls.append((sql, database))
        return []


def test_push_conformed_uses_one_atomic_fly_write() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute(
        """
        CREATE TABLE staging.conformed_matchup AS
        SELECT * FROM (VALUES
            ('kmffl', '2013', '13', 'Gavi', '1', '0'),
            ('kmffl', '2013', '13', 'Rubinstein', '0', '1')
        ) AS t(db_name, year, week, manager, win, loss)
        """
    )
    writer = RecordingWriter()

    counts = push_conformed_to_fly(
        conn,
        "kmffl",
        writer=writer,
        reader=object(),
    )

    assert counts["matchup"] == 2
    assert len(writer.calls) == 1
    sql, database = writer.calls[0]
    assert database == "___leagues"
    assert sql.startswith("BEGIN TRANSACTION;")
    assert "CREATE TABLE IF NOT EXISTS ___leagues.staging.conformed_matchup" in sql
    assert "DELETE FROM ___leagues.staging.conformed_matchup WHERE db_name = 'kmffl'" in sql
    assert "INSERT INTO ___leagues.staging.conformed_matchup" in sql
    assert sql.rstrip().endswith("COMMIT;")

