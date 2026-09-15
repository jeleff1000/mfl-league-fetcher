from __future__ import annotations

import duckdb
import pytest

from multi_league.core.league_update_manifest import NFL_SCORING_INPUT_COLUMNS
from scripts.materialize_league_update_nfl_revisions import materialize_nfl_revisions


class ConnectionReader:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.connection = connection

    def query(self, sql: str, database: str) -> list[dict]:
        assert database == "___ops"
        result = self.connection.execute(sql)
        columns = [value[0] for value in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]


class ConnectionWriter:
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.connection = connection

    def execute(self, sql: str, database: str = "___ops") -> list[dict]:
        assert database == "___ops"
        self.connection.execute(sql)
        return []


def _source(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA nfl_historical")
    scoring_ddl = ", ".join(f'"{column}" DOUBLE' for column in NFL_SCORING_INPUT_COLUMNS)
    connection.execute(
        f"""
        CREATE TABLE nfl_historical.nfl_player_stats_all (
            NFL_player_id VARCHAR,
            game_date DATE,
            year INTEGER,
            week INTEGER,
            season_type VARCHAR,
            nfl_team VARCHAR,
            opponent_nfl_team VARCHAR,
            {scoring_ddl},
            pts_idp_pd DOUBLE
        )
        """
    )
    scoring_values = ", ".join("0" for _ in NFL_SCORING_INPUT_COLUMNS)
    connection.execute(
        "INSERT INTO nfl_historical.nfl_player_stats_all VALUES "
        f"('00-1', DATE '2026-09-13', 2026, 1, 'REG', 'BUF', 'MIA', {scoring_values}, 0)"
    )


def test_materializer_replaces_one_scope_and_changes_revision_for_idp_correction():
    """A corrected registered stat must replace the compact week/game receipt."""
    connection = duckdb.connect(":memory:")
    _source(connection)
    reader = ConnectionReader(connection)
    writer = ConnectionWriter(connection)

    first = materialize_nfl_revisions(reader, writer, year=2026, week=1)
    first_revision = connection.execute(
        "SELECT revision FROM accounts.league_update_nfl_revisions"
    ).fetchone()[0]
    connection.execute(
        "UPDATE nfl_historical.nfl_player_stats_all SET def_pass_defended = 2"
    )
    second = materialize_nfl_revisions(reader, writer, year=2026, week=1)
    rows = connection.execute(
        "SELECT season, week, game_key, schema_version, revision "
        "FROM accounts.league_update_nfl_revisions"
    ).fetchall()

    assert first == {"season": 2026, "week": 1, "games": 1}
    assert second == {"season": 2026, "week": 1, "games": 1}
    assert rows[0][:4] == (2026, 1, "BUF@MIA", 1)
    assert rows[0][4] != first_revision


def test_materializer_refuses_to_replace_scope_when_source_is_empty():
    """A missing NFL response cannot erase a previously materialized revision."""
    connection = duckdb.connect(":memory:")
    _source(connection)
    reader = ConnectionReader(connection)
    writer = ConnectionWriter(connection)
    materialize_nfl_revisions(reader, writer, year=2026, week=1)

    with pytest.raises(RuntimeError, match="no NFL source rows"):
        materialize_nfl_revisions(reader, writer, year=2026, week=2)

    assert connection.execute(
        "SELECT COUNT(*) FROM accounts.league_update_nfl_revisions WHERE season = 2026 AND week = 1"
    ).fetchone()[0] == 1


def test_materializer_discovers_precomputed_scoring_columns_from_source_schema():
    """A new pts_* source column must affect freshness without widening every browser poll."""
    connection = duckdb.connect(":memory:")
    _source(connection)
    reader = ConnectionReader(connection)
    writer = ConnectionWriter(connection)
    materialize_nfl_revisions(reader, writer, year=2026, week=1)
    original = connection.execute(
        "SELECT revision FROM accounts.league_update_nfl_revisions"
    ).fetchone()[0]

    connection.execute("UPDATE nfl_historical.nfl_player_stats_all SET pts_idp_pd = 4")
    materialize_nfl_revisions(reader, writer, year=2026, week=1)
    corrected = connection.execute(
        "SELECT revision FROM accounts.league_update_nfl_revisions"
    ).fetchone()[0]

    assert corrected != original
