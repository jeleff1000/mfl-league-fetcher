"""Focused arithmetic tests for shared research matchup rollups."""

import duckdb
import pytest

from matchup_metric_sql import (
    is_research_week,
    season_expected_outcome_sql,
    season_metric_select,
    weekly_metric_select,
)


@pytest.mark.parametrize(
    ("year", "week", "included"),
    [
        (2020, 16, True),
        (2020, 17, False),
        (2021, 17, True),
        (2021, 18, False),
    ],
)
def test_research_calendar_excludes_only_the_final_nfl_week(year, week, included):
    assert is_research_week(year, week) is included


@pytest.fixture()
def con():
    connection = duckdb.connect()
    connection.execute("""
        CREATE TABLE weekly_primitives AS
        SELECT * FROM (VALUES
          (1, 20, 10, 10, 10, 10, 10, 6, 4, 200.0, 20.0, 1.5),
          -- Ten starts have no matchup result. Win% still uses all 50 starts;
          -- Expected W-L uses the full start basis, with Expected L as the
          -- complement of Expected W.
          (2, 100, 100, 60, 60, 50, 45, 20, 20, 800.0, 10.0, -0.5)
        ) t(week, roster_eligible_leagues, team_game_eligible_leagues,
            healthy_eligible_leagues, rostered_leagues, started_leagues, healthy_started_leagues,
            wins_started, losses_started,
            points_started, lamar_weighted, clutch_weighted)
    """)
    yield connection
    connection.close()


def test_season_rollup_uses_weighted_weekly_denominators(con):
    row = con.execute(
        f"SELECT {season_metric_select('w')} FROM weekly_primitives w"
    ).fetchone()

    (rostered, start, healthy, rostered_lw, wins, losses, starts, win_rate,
     starts_healthy, lamar, clutch) = row
    assert rostered == pytest.approx(70 / 120 * 100)
    assert start == pytest.approx(60 / 110 * 100)
    assert healthy == pytest.approx(55 / 70 * 100)
    assert rostered_lw == 70
    assert win_rate == pytest.approx(0.80 / 1.5 * 100)
    assert wins == pytest.approx(0.80)  # 100%*60% + 50%*40%
    assert losses == pytest.approx(0.70)  # Expected Starts (1.5) - Expected W (0.8)
    assert starts == pytest.approx(10 / 10 + 50 / 100)
    assert starts_healthy == pytest.approx(1.0 + 45 / 60)
    assert lamar == pytest.approx(30.0)
    assert clutch == pytest.approx(1.0)


def test_weekly_win_rate_uses_all_started_leagues(con):
    row = con.execute(
        f"SELECT {weekly_metric_select('w')} FROM weekly_primitives w WHERE week=2"
    ).fetchone()

    rostered, start, healthy, win_rate, wins, losses, starts = row
    assert rostered == pytest.approx(60.0)
    assert start == pytest.approx(50.0)
    assert healthy == pytest.approx(45 / 60 * 100)
    assert win_rate == pytest.approx(40.0)  # 20 wins / all 50 starts
    assert wins == pytest.approx(0.20)  # 50% start rate * 40% win rate
    assert losses == pytest.approx(0.30)  # complement of Expected W on all starts
    assert starts == pytest.approx(0.5)


def test_expected_weekly_record_is_start_share_times_win_rate(con):
    con.execute("""INSERT INTO weekly_primitives VALUES
        (4, 100, 100, 100, 80, 80, 80, 64, 16, 0.0, 0.0, 0.0)
    """)

    row = con.execute(
        f"SELECT {weekly_metric_select('w')} FROM weekly_primitives w WHERE week=4"
    ).fetchone()

    _, start, _, win_rate, wins, losses, starts = row
    assert start == pytest.approx(80.0)
    assert win_rate == pytest.approx(80.0)
    assert wins == pytest.approx(0.64)
    assert losses == pytest.approx(0.16)
    assert starts == pytest.approx(0.80)


def test_season_expected_record_sums_equal_weight_weekly_contributions(con):
    con.execute("DELETE FROM weekly_primitives")
    con.execute("""INSERT INTO weekly_primitives VALUES
        -- Regular season: 80% start share and an 80-20 record.
        (1, 100, 100, 100, 80, 80, 80, 64, 16, 0.0, 0.0, 0.0),
        -- Playoff week: smaller league support, same weekly performance.
        (15, 25, 25, 25, 20, 20, 20, 16, 4, 0.0, 0.0, 0.0)
    """)

    row = con.execute(
        f"SELECT {season_metric_select('w')} FROM weekly_primitives w"
    ).fetchone()

    _, _, _, _, wins, losses, starts, win_rate, *_ = row
    assert win_rate == pytest.approx(80.0)
    assert wins == pytest.approx(1.28)
    assert losses == pytest.approx(0.32)
    assert starts == pytest.approx(1.60)


def test_season_expected_record_complements_unknown_starts(con):
    con.execute("""INSERT INTO weekly_primitives VALUES
        (3, 10, 10, 8, 8, 5, 5, 0, 0, 100.0, 5.0, 0.0)
    """)

    row = con.execute(f"""SELECT
        {season_expected_outcome_sql('w', 'wins_started')} AS expected_wins,
        {season_expected_outcome_sql('w', 'losses_started')} AS expected_losses,
        SUM(1.0 * started_leagues / team_game_eligible_leagues) AS expected_starts,
        100.0 * {season_expected_outcome_sql('w', 'wins_started')} / COUNT(*) AS won_pct,
        100.0 * {season_expected_outcome_sql('w', 'losses_started')} / COUNT(*) AS lost_pct
      FROM weekly_primitives w""").fetchone()

    expected_wins, expected_losses, expected_starts, won_pct, lost_pct = row
    # Week 3 has no known outcome. Its 0.5 expected start remains in Exp Starts,
    # and Expected L is the complement required by the display contract.
    assert expected_wins == pytest.approx(0.80)
    assert expected_losses == pytest.approx(1.20)
    assert expected_wins + expected_losses == pytest.approx(expected_starts)
