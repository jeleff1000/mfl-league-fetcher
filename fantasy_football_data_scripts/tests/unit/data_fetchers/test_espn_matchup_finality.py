"""Canonical ESPN results must not materialize an unfinished scoring period."""

from copy import deepcopy
from types import SimpleNamespace

import duckdb
import pytest

from multi_league.data_fetchers.espn import espn_matchups


def schedule_row(winner="HOME", *, period=1, scores=(110.0, 90.0), home=1, away=2, playoff=False):
    row = {
        "matchupPeriodId": period,
        "playoffTierType": "WINNERS_BRACKET" if playoff else "NONE",
        "home": {"teamId": home, "totalPoints": scores[0]},
    }
    if away is not None:
        row["away"] = {"teamId": away, "totalPoints": scores[1]}
    if winner is not None:
        row["winner"] = winner
    return row


@pytest.fixture
def provider(monkeypatch):
    def install(schedules, *, archive=False, current_period=2, scoring_period=2, periods=None):
        source = deepcopy(schedules)
        teams = {
            side["teamId"]: SimpleNamespace(team_id=side["teamId"], team_name=f"Team {side['teamId']}")
            for rows in schedules.values() for row in rows
            for side in (row.get("home"), row.get("away")) if side
        }

        def boxes(week):
            return [SimpleNamespace(
                home_team=teams[row["home"]["teamId"]],
                away_team=teams[row["away"]["teamId"]] if row.get("away") else None,
                home_score=row["home"]["totalPoints"],
                away_score=row.get("away", {}).get("totalPoints", 0),
                is_playoff=row["playoffTierType"] != "NONE", matchup_type=row["playoffTierType"],
            ) for row in schedules.get(week, [])]

        league = SimpleNamespace(
            _uses_league_history=archive, teams=list(teams.values()),
            currentMatchupPeriod=current_period, scoringPeriodId=scoring_period,
            settings=SimpleNamespace(matchup_periods=periods or {}), box_scores=boxes, scoreboard=boxes,
        )
        calls = []

        class Client:
            def __init__(self, *_args):
                pass

            def get_league(self, _year):
                return league

            def get_raw_schedule(self, _year, week):
                calls.append(week)
                return deepcopy(schedules.get(week, []))

        ctx = SimpleNamespace(
            espn_s2=None, swid=None, playoff_start_week=14,
            get_league_id_for_year=lambda year: 123,
            get_manager_name=lambda team_id, **kw: f"Manager {team_id}",
            get_manager_guid=lambda team_id, **kw: f"owner-{team_id}",
            get_franchise_id=lambda team_id, **kw: f"fid-{team_id}",
        )
        monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", Client)
        monkeypatch.setattr(
            "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
            lambda *_: {"end_week": max(schedules), "playoff_start_week": 14,
                        "playoff_matchup_period_length": 2 if periods else 1},
        )
        return ctx, league, source, calls
    return install


@pytest.mark.parametrize("archive", [False, True])
def test_full_import_keeps_final_week_only_and_raw_live_scores(provider, archive):
    schedule = {1: [schedule_row()], 2: [schedule_row("UNDECIDED", period=2, scores=(27.5, 3.2))]}
    ctx, _, original, calls = provider(schedule, archive=archive)
    frame = espn_matchups.fetch_espn_matchups(ctx, 2026)
    assert frame["week"].tolist() == [1, 1]
    with duckdb.connect(":memory:") as conn:
        # The canonical rows handed to common completed-game metrics/sims.
        assert conn.execute("SELECT COUNT(*), SUM(win), SUM(loss), SUM(tie) FROM frame").fetchone() == (2, 1, 1, 0)
    assert schedule == original
    assert schedule[2][0]["home"]["totalPoints"] == 27.5
    assert calls == [1, 2]


@pytest.mark.parametrize("archive", [False, True])
def test_scoped_quick_import_holds_undecided_week(provider, archive):
    ctx, _, _, _ = provider({2: [schedule_row("UNDECIDED", period=2, scores=(27.5, 3.2))]}, archive=archive)
    assert espn_matchups.fetch_espn_matchups(ctx, 2026, weeks=[2]) is None


@pytest.mark.parametrize("archive", [False, True])
@pytest.mark.parametrize("winner, expected", [
    ("HOME", [[1, 0, 0], [0, 1, 0]]),
    ("AWAY", [[0, 1, 0], [1, 0, 0]]),
    ("TIE", [[0, 0, 1], [0, 0, 1]]),
])
def test_explicit_final_outcomes_unchanged(provider, archive, winner, expected):
    ctx, _, _, _ = provider({2: [schedule_row(winner, period=2, scores=(10.0, 10.0))]}, archive=archive)
    frame = espn_matchups.fetch_espn_matchups(ctx, 2026, weeks=[2])
    assert frame is not None
    assert frame[["win", "loss", "tie"]].values.tolist() == expected
    assert frame.team_points.tolist() == [10.0, 10.0]


@pytest.mark.parametrize("archive", [False, True])
def test_final_playoff_period_allows_declared_bye_without_awarding_win(provider, archive):
    ctx, _, _, _ = provider({14: [schedule_row(period=14, playoff=True), schedule_row(
        "UNDECIDED", period=14, home=3, away=None, scores=(0, 0), playoff=True,
    )]}, archive=archive, current_period=14, scoring_period=14)
    frame = espn_matchups.fetch_espn_matchups(ctx, 2026, weeks=[14])
    assert len(frame) == 3
    bye = frame.loc[frame.team_key == "3"].iloc[0]
    assert bye.is_bye_week
    assert frame.win.sum() == 1
    assert frame.loc[frame.team_key == "3", ["team_points", "win", "loss", "tie"]].isna().all().all()


@pytest.mark.parametrize("archive", [False, True])
def test_missing_historical_winner_retains_existing_fallback(provider, archive):
    ctx, _, _, _ = provider({1: [schedule_row(None)]}, archive=archive,
                             current_period=1, scoring_period=1)
    frame = espn_matchups.fetch_espn_matchups(ctx, 2019, weeks=[1])
    assert frame is not None
    assert frame.win.tolist() == [1, 0]


def test_multiweek_playoff_finality_preserves_existing_cumulative_deltas(provider):
    periods = {14: [14, 15]}
    live = {14: [schedule_row("UNDECIDED", period=14, scores=(120, 100), playoff=True)],
            15: [schedule_row("UNDECIDED", period=15, scores=(145, 103), playoff=True)]}
    ctx, _, _, _ = provider(live, current_period=14, scoring_period=15, periods=periods)
    assert espn_matchups.fetch_espn_matchups(ctx, 2026, weeks=[14, 15]) is None
    final = {14: [schedule_row(period=14, scores=(120, 100), playoff=True)],
             15: [schedule_row(period=15, scores=(250, 180), playoff=True)]}
    ctx, _, _, _ = provider(final, current_period=15, scoring_period=16, periods=periods)
    frame = espn_matchups.fetch_espn_matchups(ctx, 2026, weeks=[14, 15])
    assert frame.week.tolist() == [14, 14, 15, 15]
    assert frame.team_points.tolist() == [120, 100, 130, 80]


@pytest.mark.parametrize("winner, final", [("HOME", True), ("UNDECIDED", False)])
def test_championship_subset_uses_outcomes_not_entire_league_inventory(provider, winner, final):
    ctx, league, _, _ = provider({17: [schedule_row(winner, period=17, playoff=True)]},
                                  current_period=17, scoring_period=17)
    league.teams.extend(SimpleNamespace(team_id=i, team_name=f"Eliminated {i}") for i in range(3, 15))
    frame = espn_matchups.fetch_espn_matchups(ctx, 2026, weeks=[17])
    assert (frame is not None) is final
    if final:
        assert len(frame) == 2
