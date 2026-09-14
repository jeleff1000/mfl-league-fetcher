"""Season/career share-family aggregation (target_share / air_yards_share / wopr).

The share denominators are summed DIRECTLY from team-game volume (team_vol CTE) — the weekly
share columns are never read. This pins the 2026-07-09 fix for the season share corruption:
in the 2003-2008 availability hole the weekly table carries sparse garbage shares (e.g. Welker
2008 week 5 target_share=1.0), and the old volume/share denominator reconstruction turned one
garbage week into a 1.0 season share.
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
    team: str,
    targets: float | None,
    air_yards: float | None,
    target_share: float | None = None,
) -> tuple:
    return (
        player_id,
        f"{player_id}_{year}_{week}",
        year,
        week,
        "REG",
        team,
        targets,
        air_yards,
        target_share,
        None,  # air_yards_share
        None,  # wopr
    )


@pytest.fixture()
def writer(tmp_path):
    con = duckdb.connect()
    con.execute(f"ATTACH '{(tmp_path / 'ops.duckdb').as_posix()}' AS \"___ops\"")
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".nfl_historical')
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".public')
    con.execute(
        'CREATE TABLE "___ops".nfl_historical.nfl_player_stats_all ('
        "NFL_player_id VARCHAR, player_week VARCHAR, year INTEGER, week INTEGER, "
        "season_type VARCHAR, nfl_team VARCHAR, targets DOUBLE, receiving_air_yards DOUBLE, "
        "target_share DOUBLE, air_yards_share DOUBLE, wopr DOUBLE)"
    )
    yield LocalWriter(con)
    con.close()


def insert_rows(writer: LocalWriter, rows: list[tuple]) -> None:
    writer.con.executemany(
        'INSERT INTO "___ops".nfl_historical.nfl_player_stats_all VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        rows,
    )


def build_shares(writer: LocalWriter, *, season: bool = True) -> dict:
    table, cols = (agg.SEASON_TABLE, ["target_share", "air_yards_share", "wopr"])
    chunk_table, _ = agg.create_aggregate_chunk_table(
        writer,
        table,
        idx=1,
        cols=cols,
        include_playoffs=False,
        season=season,
        year=None,
    )
    key = "NFL_player_id, year" if season else "NFL_player_id"
    rows = writer.execute(f"SELECT {key}, target_share, air_yards_share, wopr FROM {chunk_table}")
    if season:
        return {(r["NFL_player_id"], r["year"]): r for r in rows}
    return {r["NFL_player_id"]: r for r in rows}


def test_clean_era_share_is_volume_over_team_volume(writer):
    # 4 tracked games: WR1 has 30 of 100 team targets, 300 of 800 team air yards
    rows = []
    for week in (1, 2, 3, 4):
        rows.append(make_row("WR1", 2020, week, "NE", 7.5, 75.0, 0.99))  # garbage weekly share ignored
        rows.append(make_row("WR2", 2020, week, "NE", 17.5, 125.0))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("WR1", 2020)]["target_share"] == 0.3
    assert out[("WR1", 2020)]["air_yards_share"] == 0.375
    assert out[("WR1", 2020)]["wopr"] == round(1.5 * 0.3 + 0.7 * 0.375, 4)
    assert out[("WR2", 2020)]["target_share"] == 0.7


def test_zero_target_player_on_tracked_team_gets_true_zero(writer):
    rows = []
    for week in (1, 2, 3):
        rows.append(make_row("WR1", 2020, week, "NE", 10.0, 100.0))
        rows.append(make_row("K1", 2020, week, "NE", 0.0, 0.0))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("K1", 2020)]["target_share"] == 0.0  # tracked and zero, not NULL


def test_pre_availability_era_is_null(writer):
    rows = []
    for week in (1, 2, 3, 4):
        rows.append(make_row("RB1", 1960, week, "CHI", None, None))
        rows.append(make_row("RB2", 1960, week, "CHI", None, None))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("RB1", 1960)]["target_share"] is None
    assert out[("RB1", 1960)]["wopr"] is None


def test_partial_season_tracking_is_gated_to_null(writer):
    # A team-year where the volume is tracked in fewer than half its games (e.g. the 1999-2002
    # air-yards seasons: ~1-5 tracked games of 16) must not produce a share -- partial
    # denominators minted impossible values like air_yards_share > 1.0 on 150-target players.
    rows = []
    for week in range(1, 17):
        air = 100.0 if week <= 4 else 0.0  # tracked in only 4 of 16 games
        rows.append(make_row("WR1", 2000, week, "DEN", 8.0, air * 0.6))
        rows.append(make_row("WR2", 2000, week, "DEN", 8.0, air * 0.4))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("WR1", 2000)]["air_yards_share"] is None
    # targets tracked all 16 games -> target_share still real
    assert out[("WR1", 2000)]["target_share"] == 0.5


def test_within_game_partial_air_tracking_is_gated_by_year_coverage(writer):
    # 1999-2002 shape: air yards present in most games but for only ONE targeted player, so the
    # team denominator is a sliver of the truth (Keyshawn 2002 air_yards_share = 1.036 pre-fix).
    # Year-level coverage (air rows / targeted rows = 1/3 < 0.5) must gate the whole year.
    rows = []
    for week in range(1, 17):
        rows.append(make_row("WR1", 2001, week, "NYJ", 10.0, 80.0))  # only tracked player
        rows.append(make_row("WR2", 2001, week, "NYJ", 10.0, 0.0))
        rows.append(make_row("TE1", 2001, week, "NYJ", 10.0, 0.0))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("WR1", 2001)]["air_yards_share"] is None
    assert out[("WR1", 2001)]["target_share"] == 0.3333


def test_current_season_speakable_from_three_tracked_games(writer):
    # In-season rebuild early in the year: 3 games played, all tracked -> shares available.
    rows = []
    for week in (1, 2, 3):
        rows.append(make_row("WR1", 2026, week, "NE", 6.0, 60.0))
        rows.append(make_row("WR2", 2026, week, "NE", 18.0, 90.0))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("WR1", 2026)]["target_share"] == 0.25
    assert out[("WR1", 2026)]["air_yards_share"] == 0.4


def test_single_stray_tracked_game_is_gated_to_null(writer):
    # One stray game with targets in an otherwise untracked season (the pre-1978 stray rows):
    # without the team-season gate this player would get target_share = 1.0
    rows = [make_row("WR1", 1933, 1, "PRT", 1.0, None)]
    for week in (2, 3, 4, 5):
        rows.append(make_row("WR1", 1933, week, "PRT", None, None))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("WR1", 1933)]["target_share"] is None


def test_hole_era_garbage_weekly_share_does_not_poison_season(writer):
    # Welker-2008 shape: targets tracked all season, weekly share garbage in one week.
    # Old formula: denominator = 11/1.0 from the garbage week alone -> season share 1.0.
    rows = []
    for week in (1, 2, 3, 4):
        share = 1.0 if week == 2 else None
        rows.append(make_row("WR1", 2008, week, "NE", 10.0, None, share))
        rows.append(make_row("WR2", 2008, week, "NE", 30.0, None))
    insert_rows(writer, rows)
    out = build_shares(writer)
    assert out[("WR1", 2008)]["target_share"] == 0.25
    # air yards untracked -> share NULL, wopr degrades to 1.5 * target_share
    assert out[("WR1", 2008)]["air_yards_share"] is None
    assert out[("WR1", 2008)]["wopr"] == round(1.5 * 0.25, 4)


def test_career_grain_spans_tracked_years_only(writer):
    rows = []
    # 1970: untracked era (counts nothing)
    for week in (1, 2, 3):
        rows.append(make_row("WR1", 1970, week, "OAK", None, None))
        rows.append(make_row("WR2", 1970, week, "OAK", None, None))
    # 1980: tracked, WR1 has 20 of 80 team targets
    for week in (1, 2, 3, 4):
        rows.append(make_row("WR1", 1980, week, "OAK", 5.0, None))
        rows.append(make_row("WR2", 1980, week, "OAK", 15.0, None))
    insert_rows(writer, rows)
    out = build_shares(writer, season=False)
    assert out["WR1"]["target_share"] == 0.25


def test_share_dependencies_no_longer_require_weekly_share_columns():
    assert agg.DERIVED_RATE_DEPENDENCIES["target_share"] == ("targets",)
    assert agg.DERIVED_RATE_DEPENDENCIES["air_yards_share"] == ("receiving_air_yards",)
    assert agg.DERIVED_RATE_DEPENDENCIES["wopr"] == ("targets", "receiving_air_yards")
    assert agg.SHARE_FAMILY_COLS <= agg.DERIVED_RATE_COLS
