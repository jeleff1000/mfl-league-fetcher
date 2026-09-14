"""Season/career aggregation of per-game NGS averages (ngs_avg_separation / yac+).

Pins the 2026-07-09 fix: these columns are per-game AVERAGES in the weekly table, and the
default SUM aggregation stored 17x-inflated season values (Kupp 2021 ngs_avg_separation =
62.50 vs true per-game 3.68). They now aggregate as volume-weighted means — separation
weighted by targets, YAC over expectation by receptions — with NULL weeks excluded from
both numerator and denominator (NULL = not tracked, never 0).
"""

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
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def make_row(
    player_id: str,
    year: int,
    week: int,
    targets: float | None,
    receptions: float | None,
    separation: float | None,
    yac_above: float | None,
) -> tuple:
    return (player_id, year, week, "REG", targets, receptions, separation, yac_above)


@pytest.fixture()
def writer(tmp_path):
    con = duckdb.connect()
    con.execute(f"ATTACH '{(tmp_path / 'ops.duckdb').as_posix()}' AS \"___ops\"")
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".nfl_historical')
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".public')
    con.execute(
        'CREATE TABLE "___ops".nfl_historical.nfl_player_stats_all ('
        "NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, "
        "targets DOUBLE, receptions DOUBLE, "
        "ngs_avg_separation DOUBLE, ngs_avg_yac_above_expectation DOUBLE)"
    )
    yield LocalWriter(con)
    con.close()


def insert_rows(writer: LocalWriter, rows: list[tuple]) -> None:
    writer.con.executemany(
        'INSERT INTO "___ops".nfl_historical.nfl_player_stats_all VALUES (?,?,?,?,?,?,?,?)',
        rows,
    )


COLS = ["ngs_avg_separation", "ngs_avg_yac_above_expectation"]


def build(writer: LocalWriter, *, season: bool = True) -> dict:
    chunk_table, _ = agg.create_aggregate_chunk_table(
        writer,
        agg.SEASON_TABLE if season else agg.CAREER_TABLE,
        idx=1,
        cols=COLS,
        include_playoffs=False,
        season=season,
        year=None,
    )
    key = "NFL_player_id, year" if season else "NFL_player_id"
    rows = writer.execute(f"SELECT {key}, {', '.join(COLS)} FROM {chunk_table}")
    if season:
        return {(r["NFL_player_id"], r["year"]): r for r in rows}
    return {r["NFL_player_id"]: r for r in rows}


def test_classification_is_weighted_not_summed():
    assert agg.WEIGHTED_AVG_COLS["ngs_avg_separation"] == "targets"
    assert agg.WEIGHTED_AVG_COLS["ngs_avg_yac_above_expectation"] == "receptions"


@pytest.mark.parametrize("column", ["fumbles_lost", "def_blk_kick", "fg_made_60_plus_canonical"])
def test_discovery_promotes_required_precomputed_totals(monkeypatch, column):
    weekly = [agg.ColumnInfo(column, "DOUBLE", 1)]

    def fake_fetch_columns(_writer, table_name):
        return weekly if table_name == agg.SUPER_TABLE else []

    monkeypatch.setattr(agg, "fetch_columns", fake_fetch_columns)

    aggregate_cols, _lamar_cols, _fpts_cols, _bonus_cols = agg.discover_aggregate_columns(object())

    assert column in aggregate_cols


def test_discovery_promotes_new_numeric_weekly_stats(monkeypatch):
    """A newly supported sortable weekly stat must reach season/career fast paths."""
    weekly = [
        agg.ColumnInfo("kickoff_return_tds", "DOUBLE", 1),
        agg.ColumnInfo("yds_allow_0_99", "DOUBLE", 2),
    ]

    def fake_fetch_columns(_writer, table_name):
        return weekly if table_name == agg.SUPER_TABLE else []

    monkeypatch.setattr(agg, "fetch_columns", fake_fetch_columns)

    aggregate_cols, _lamar_cols, _fpts_cols, _bonus_cols = agg.discover_aggregate_columns(object())

    assert {"kickoff_return_tds", "yds_allow_0_99"}.issubset(aggregate_cols)


def test_season_separation_is_target_weighted_mean(writer):
    insert_rows(writer, [
        make_row("WR1", 2021, 1, 10.0, 5.0, 4.0, 2.0),
        make_row("WR1", 2021, 2, 2.0, 1.0, 1.0, -1.0),
    ])
    out = build(writer)
    row = out[("WR1", 2021)]
    # target-weighted: (4.0*10 + 1.0*2) / 12 = 3.5 (plain AVG would be 2.5, SUM 5.0)
    assert row["ngs_avg_separation"] == pytest.approx(3.5)
    # reception-weighted: (2.0*5 + -1.0*1) / 6 = 1.5
    assert row["ngs_avg_yac_above_expectation"] == pytest.approx(1.5)


def test_untracked_weeks_are_excluded_not_zeroed(writer):
    insert_rows(writer, [
        make_row("WR1", 2021, 1, 10.0, 5.0, 4.0, 2.0),
        make_row("WR1", 2021, 2, 2.0, 1.0, 1.0, -1.0),
        # 8 targets but NGS value NULL (pre-tracking / untracked game): must not drag the mean
        make_row("WR1", 2021, 3, 8.0, 4.0, None, None),
    ])
    out = build(writer)
    row = out[("WR1", 2021)]
    assert row["ngs_avg_separation"] == pytest.approx(3.5)
    assert row["ngs_avg_yac_above_expectation"] == pytest.approx(1.5)


def test_player_with_no_tracked_weeks_is_null(writer):
    insert_rows(writer, [
        make_row("OLD1", 1998, 1, 6.0, 3.0, None, None),
        make_row("OLD1", 1998, 2, 4.0, 2.0, None, None),
    ])
    out = build(writer)
    row = out[("OLD1", 1998)]
    assert row["ngs_avg_separation"] is None
    assert row["ngs_avg_yac_above_expectation"] is None


def test_career_grain_weights_across_seasons(writer):
    insert_rows(writer, [
        make_row("WR1", 2020, 1, 10.0, 5.0, 4.0, 2.0),
        make_row("WR1", 2021, 1, 2.0, 1.0, 1.0, -1.0),
    ])
    out = build(writer, season=False)
    row = out["WR1"]
    assert row["ngs_avg_separation"] == pytest.approx(3.5)
    assert row["ngs_avg_yac_above_expectation"] == pytest.approx(1.5)
