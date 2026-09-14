"""Canonical fumbles-lost aggregation at season and career grain."""

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
        "player_week VARCHAR, fumbles_lost DOUBLE)"
    )
    yield LocalWriter(con)
    con.close()


def insert_rows(writer: LocalWriter, rows: list[tuple]) -> None:
    writer.con.executemany(
        'INSERT INTO "___ops".nfl_historical.nfl_player_stats_all VALUES (?,?,?,?,?,?)',
        rows,
    )


def build(writer: LocalWriter, *, season: bool) -> list[dict]:
    table = agg.SEASON_TABLE if season else agg.CAREER_TABLE
    chunk_table, _ = agg.create_aggregate_chunk_table(
        writer,
        table,
        idx=1,
        cols=["fumbles_lost"],
        include_playoffs=False,
        season=season,
    )
    return writer.execute(f"SELECT * FROM {chunk_table} ORDER BY NFL_player_id")


def test_season_counts_one_canonical_total_per_player_week(writer):
    insert_rows(
        writer,
        [
            ("QB1", 2024, 1, "REG", "QB1_2024_1", 1.0),
            # Duplicate source row for the same canonical player-game.
            ("QB1", 2024, 1, "REG", "QB1_2024_1", 1.0),
            ("QB1", 2024, 2, "REG", "QB1_2024_2", 0.0),
            ("QB1", 2024, 3, "REG", "QB1_2024_3", 2.0),
        ],
    )

    assert build(writer, season=True) == [
        {"NFL_player_id": "QB1", "year": 2024, "fumbles_lost": 3.0}
    ]


def test_covered_zero_stays_zero_and_uncovered_span_stays_null(writer):
    insert_rows(
        writer,
        [
            ("COVERED", 2024, 1, "REG", "COVERED_2024_1", 0.0),
            ("COVERED", 2024, 2, "REG", "COVERED_2024_2", 0.0),
            ("UNTRACKED", 1998, 1, "REG", "UNTRACKED_1998_1", None),
            ("UNTRACKED", 1998, 2, "REG", "UNTRACKED_1998_2", None),
        ],
    )

    rows = {(row["NFL_player_id"], row["year"]): row for row in build(writer, season=True)}
    assert rows[("COVERED", 2024)]["fumbles_lost"] == 0.0
    assert rows[("UNTRACKED", 1998)]["fumbles_lost"] is None


def test_career_sums_canonical_season_values_without_coalescing_null(writer):
    insert_rows(
        writer,
        [
            ("QB1", 2023, 1, "REG", "QB1_2023_1", 1.0),
            ("QB1", 2024, 1, "REG", "QB1_2024_1", 2.0),
            ("OLD", 1998, 1, "REG", "OLD_1998_1", None),
        ],
    )

    rows = {row["NFL_player_id"]: row for row in build(writer, season=False)}
    assert rows["QB1"]["fumbles_lost"] == 3.0
    assert rows["OLD"]["fumbles_lost"] is None
