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
            position VARCHAR,
            {scoring_ddl},
            pts_idp_pd DOUBLE
        )
        """
    )
    scoring_values = ", ".join("0" for _ in NFL_SCORING_INPUT_COLUMNS)
    connection.execute(
        "INSERT INTO nfl_historical.nfl_player_stats_all VALUES "
        f"('00-1', DATE '2026-09-13', 2026, 1, 'REG', 'BUF', 'MIA', 'QB', {scoring_values}, 0),"
        f"('00-2', DATE '2026-09-13', 2026, 1, 'REG', 'MIA', 'BUF', 'QB', {scoring_values}, 0),"
        f"('DEF-1', DATE '2026-09-13', 2026, 1, 'REG', 'BUF', 'MIA', 'DEF', {scoring_values}, 0),"
        f"('DEF-2', DATE '2026-09-13', 2026, 1, 'REG', 'MIA', 'BUF', 'DEF', {scoring_values}, 0)"
    )


def _witness(_year: int, _week: int) -> dict[str, set[tuple[str, str]]]:
    return {"BUF@MIA": {("00-1", "BUF"), ("00-2", "MIA")}}


def test_materializer_replaces_one_scope_and_changes_revision_for_idp_correction():
    """A corrected registered stat must replace the compact week/game receipt."""
    connection = duckdb.connect(":memory:")
    _source(connection)
    reader = ConnectionReader(connection)
    writer = ConnectionWriter(connection)

    first = materialize_nfl_revisions(reader, writer, year=2026, week=1, official_week_witness=_witness)
    first_revision = connection.execute(
        "SELECT revision FROM accounts.league_update_nfl_revisions"
    ).fetchone()[0]
    connection.execute(
        "UPDATE nfl_historical.nfl_player_stats_all SET def_pass_defended = 2"
    )
    second = materialize_nfl_revisions(reader, writer, year=2026, week=1, official_week_witness=_witness)
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
    materialize_nfl_revisions(reader, writer, year=2026, week=1, official_week_witness=_witness)

    with pytest.raises(RuntimeError, match="no NFL source rows"):
        materialize_nfl_revisions(reader, writer, year=2026, week=2, official_week_witness=_witness)

    assert connection.execute(
        "SELECT COUNT(*) FROM accounts.league_update_nfl_revisions WHERE season = 2026 AND week = 1"
    ).fetchone()[0] == 1


def test_materializer_discovers_precomputed_scoring_columns_from_source_schema():
    """A new pts_* source column must affect freshness without widening every browser poll."""
    connection = duckdb.connect(":memory:")
    _source(connection)
    reader = ConnectionReader(connection)
    writer = ConnectionWriter(connection)
    materialize_nfl_revisions(reader, writer, year=2026, week=1, official_week_witness=_witness)
    original = connection.execute(
        "SELECT revision FROM accounts.league_update_nfl_revisions"
    ).fetchone()[0]

    connection.execute("UPDATE nfl_historical.nfl_player_stats_all SET pts_idp_pd = 4")
    materialize_nfl_revisions(reader, writer, year=2026, week=1, official_week_witness=_witness)
    corrected = connection.execute(
        "SELECT revision FROM accounts.league_update_nfl_revisions"
    ).fetchone()[0]

    assert corrected != original


def test_materializer_refuses_a_partial_final_game_without_replacing_a_receipt():
    """One canonical player row cannot declare a two-team official game current."""
    connection = duckdb.connect(":memory:")
    _source(connection)
    reader = ConnectionReader(connection)
    writer = ConnectionWriter(connection)
    connection.execute("DELETE FROM nfl_historical.nfl_player_stats_all WHERE NFL_player_id='00-2'")
    expected = {"BUF@MIA": {("00-1", "BUF"), ("00-2", "MIA")}}

    with pytest.raises(RuntimeError, match="NFL game.*incomplete"):
        materialize_nfl_revisions(
            reader, writer, year=2026, week=1,
            official_week_witness=lambda _year, _week: expected,
        )
    assert connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='accounts' AND table_name='league_update_nfl_revisions'"
    ).fetchone()[0] == 0


def test_official_week_witness_requires_final_scores_and_both_player_sides():
    """A scored schedule row without released player identities cannot authorize a revision."""
    import gzip
    from scripts.materialize_league_update_nfl_revisions import load_nflverse_week_witness

    schedule = gzip.compress(
        b"game_id,season,week,game_type,away_team,home_team,away_score,home_score\n"
        b"2026_01_BUF_MIA,2026,1,REG,BUF,MIA,21,24\n"
    )
    stats = gzip.compress(
        b"season,week,season_type,game_id,team,opponent_team,player_id\n"
        b"2026,1,REG,2026_01_BUF_MIA,BUF,MIA,00-1\n"
    )
    def fetch(url: str) -> bytes:
        return schedule if "/schedules/" in url else stats

    with pytest.raises(RuntimeError, match="NFL game.*incomplete"):
        load_nflverse_week_witness(2026, 1, fetch_bytes=fetch)


def test_official_week_witness_skips_anonymous_team_penalty_not_a_named_player():
    """NFLverse can report anonymous team penalties without a player identity."""
    import gzip
    from scripts.materialize_league_update_nfl_revisions import load_nflverse_week_witness

    schedule = gzip.compress(
        b"game_id,season,week,game_type,away_team,home_team,away_score,home_score\n"
        b"2026_01_BUF_MIA,2026,1,REG,BUF,MIA,21,24\n"
    )
    stats = gzip.compress(
        b"season,week,season_type,game_id,team,opponent_team,player_id,player_name,position,penalties\n"
        b"2026,1,REG,2026_01_BUF_MIA,BUF,MIA,00-1,Player One,QB,0\n"
        b"2026,1,REG,2026_01_BUF_MIA,MIA,BUF,00-2,Player Two,QB,0\n"
        b"2026,1,REG,2026_01_BUF_MIA,MIA,BUF,,,,1\n"
    )
    def fetch(url: str) -> bytes:
        return schedule if "/schedules/" in url else stats

    assert load_nflverse_week_witness(2026, 1, fetch_bytes=fetch) == {
        "BUF@MIA": {("00-1", "BUF"), ("00-2", "MIA")}
    }
