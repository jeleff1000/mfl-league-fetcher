"""Canonical long-distance field-goal aggregation at season and career grain."""

from __future__ import annotations

import duckdb
import pytest

from multi_league.data_fetchers import aggregate_nfl_stats_fly as agg


class LocalWriter:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def execute(self, sql: str, database: str = "___ops") -> list[dict]:
        cur = self.con.execute(sql)
        if cur.description is None:
            return []
        names = [description[0] for description in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


@pytest.fixture()
def writer(tmp_path):
    con = duckdb.connect()
    con.execute(f"ATTACH '{(tmp_path / 'ops.duckdb').as_posix()}' AS \"___ops\"")
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".nfl_historical')
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".public')
    con.execute(
        'CREATE TABLE "___ops".nfl_historical.nfl_player_stats_all ('
        "NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, "
        "player_week VARCHAR, fg_att DOUBLE, fg_made_60_plus_canonical DOUBLE)"
    )
    yield LocalWriter(con)
    con.close()


def build(writer: LocalWriter, *, season: bool) -> list[dict]:
    table = agg.SEASON_TABLE if season else agg.CAREER_TABLE
    chunk_table, _ = agg.create_aggregate_chunk_table(
        writer,
        table,
        idx=1,
        cols=["fg_made_60_plus_canonical"],
        include_playoffs=False,
        season=season,
    )
    return writer.execute(f"SELECT * FROM {chunk_table} ORDER BY NFL_player_id")


def test_canonical_60_plus_collapses_duplicate_player_games(writer):
    writer.con.executemany(
        'INSERT INTO "___ops".nfl_historical.nfl_player_stats_all VALUES (?,?,?,?,?,?,?)',
        [
            ("K1", 2024, 1, "REG", "K1_2024_1", 1.0, 1.0),
            ("K1", 2024, 1, "REG", "K1_2024_1", 1.0, 1.0),
            ("K1", 2024, 2, "REG", "K1_2024_2", 1.0, 1.0),
        ],
    )

    assert build(writer, season=True) == [
        {"NFL_player_id": "K1", "year": 2024, "fg_made_60_plus_canonical": 2.0}
    ]


def test_canonical_60_plus_preserves_covered_zero_and_uncovered_null(writer):
    writer.con.executemany(
        'INSERT INTO "___ops".nfl_historical.nfl_player_stats_all VALUES (?,?,?,?,?,?,?)',
        [
            # Mirrors the current live transition shape: the canonical bucket
            # is sparse, while ordinary FG attempts prove kicker-stat coverage.
            ("COVERED", 2024, 1, "REG", "COVERED_2024_1", 1.0, None),
            ("UNTRACKED", 1990, 1, "REG", "UNTRACKED_1990_1", None, None),
        ],
    )

    rows = {row["NFL_player_id"]: row for row in build(writer, season=False)}
    assert rows["COVERED"]["fg_made_60_plus_canonical"] == 0.0
    assert rows["UNTRACKED"]["fg_made_60_plus_canonical"] is None
