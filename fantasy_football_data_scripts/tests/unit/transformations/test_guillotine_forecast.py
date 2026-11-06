"""Native Yahoo survivor scores must use weekly cuts, never H2H pairings."""

from pathlib import Path
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from multi_league.transformations.matchup import playoff_odds_import as poi


def native_fixture(*, completed=7, start=1):
    source = pd.read_csv(
        Path(__file__).parents[1] / "core" / "fixtures" / "yahoo_guillotine_scoreboard.csv"
    ).drop_duplicates(["week", "team_slot"])
    source["franchise_id"] = "owner-" + source.team_slot.astype(str)
    source["opponent_franchise_id"] = "owner-" + source.opponent_slot.astype(str)
    source["manager"] = source.franchise_id
    source["opponent"] = source.opponent_franchise_id
    source["year"] = 2026
    source["is_playoffs"] = 0
    source["is_consolation"] = 0
    played = source.loc[source.week.le(7)].copy()
    if completed == 8:
        scores = {1: 170, 2: 80, 3: 100, 4: 130, 11: 160, 13: 180, 14: 140}
        next_week = source.loc[source.week.eq(8)].copy()
        next_week["team_points"] = next_week.team_slot.map(scores)
        played = pd.concat([played, next_week], ignore_index=True)
        source = source.loc[~(source.week.ge(9) & source.team_slot.eq(2))].copy()
    future = source.week.gt(completed)
    source.loc[future, "opponent"] = "TBD"
    source.loc[future, "opponent_franchise_id"] = None
    source["week"] += start - 1
    played["week"] += start - 1
    settings = {
        "is_guillotine": True, "platform": "yahoo", "num_teams": 14,
        "num_playoff_teams": 0, "bye_teams": 0, "start_week": start,
        "end_week": start + 12, "playoff_start_week": start + 13,
    }
    return played, source, settings


def forecast(played, schedule, settings, week):
    return poi.calc_regular_week_outputs(
        played, schedule, 2026, week, pd.DataFrame(), settings_by_year={2026: settings}
    )


@pytest.mark.parametrize("completed,survivors", [(7, 7), (8, 6)])
def test_native_fixture_forecasts_one_cut_per_week_for_odd_and_even_survivors(monkeypatch, completed, survivors):
    monkeypatch.setattr(poi, "N_SIMS", 2048)
    played, schedule, settings = native_fixture(completed=completed)

    odds, seeds, wins = forecast(played, schedule, settings, completed)

    alive = set(schedule.loc[schedule.week.eq(completed + 1), "franchise_id"])
    assert len(alive) == survivors
    assert odds.index.is_unique and len(odds) == 14
    assert odds.loc[~odds.index.isin(alive), "P_Champ"].eq(0).all()
    assert odds.loc[list(alive), "P_Champ"].gt(0).sum() > 1
    assert odds.P_Champ.sum() == pytest.approx(100)
    assert seeds[1].equals(odds.P_Champ.rename(1))
    np.testing.assert_allclose(seeds.sum(axis=0), 100)
    np.testing.assert_allclose(seeds.sum(axis=1), 100)
    # Finishing places lock exactly once per completed cut: week 1's loser is 14th.
    assert seeds.loc["owner-10", 14] == 100
    assert seeds.loc["owner-7", 8] == 100
    assert odds[["P_Playoffs", "P_Bye", "P_Semis", "P_Final", "Exp_Final_Wins"]].eq(0).all().all()
    assert wins.x0_win.eq(100).all()
    assert wins.drop(columns="x0_win").eq(0).all().all()
    # Every remaining cut awards one new finishing place, including 2 -> 1.
    assert seeds.loc[list(alive), list(range(1, survivors + 1))].sum().sum() == pytest.approx(survivors * 100)
    repeat = forecast(played, schedule, settings, completed)
    for actual, rerun in zip((odds, seeds, wins), repeat):
        pd.testing.assert_frame_equal(actual, rerun)


def test_late_start_native_league_uses_elapsed_cuts_not_calendar_week(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 512)
    played, schedule, settings = native_fixture(start=4)
    odds, seeds, _ = forecast(played, schedule, settings, 10)
    assert odds.P_Champ.sum() == pytest.approx(100)
    assert odds.P_Champ.gt(0).sum() == 7
    assert seeds.loc["owner-10", 14] == 100


@pytest.mark.parametrize("fault", ["missing_identity", "missing_future_week", "changed_future_inventory", "missing_survivor"])
def test_native_forecast_rejects_incomplete_or_changing_native_inventory(monkeypatch, fault):
    monkeypatch.setattr(poi, "N_SIMS", 32)
    played, schedule, settings = native_fixture()
    if fault == "missing_identity":
        schedule.loc[schedule.index[0], "franchise_id"] = None
    elif fault == "missing_future_week":
        schedule = schedule.loc[schedule.week.ne(10)]
    elif fault == "changed_future_inventory":
        schedule.loc[schedule.week.eq(10) & schedule.team_slot.eq(1), "franchise_id"] = "owner-phantom"
    else:
        schedule = schedule.loc[~(schedule.week.eq(9) & schedule.team_slot.eq(1))]
    with pytest.raises(ValueError, match="[Gg]uillotine"):
        forecast(played, schedule, settings, 7)


def small_final_fixture(*, finished):
    settings = {"is_guillotine": True, "num_teams": 3, "num_playoff_teams": 0, "bye_teams": 0,
                "start_week": 10, "end_week": 11, "playoff_start_week": 12}
    rows = [
        {"year": 2026, "week": week, "franchise_id": fid, "manager": fid,
         "opponent": "TBD", "opponent_franchise_id": None, "team_points": score,
         "is_playoffs": 0, "is_consolation": 0}
        for week, scores in [(10, {"a": 70, "b": 120, "c": 100}), (11, {"b": 110, "c": 100})]
        for fid, score in scores.items()
    ]
    schedule = pd.DataFrame(rows)
    played = schedule.copy() if finished else schedule.loc[schedule.week.eq(10)].copy()
    return played, schedule, settings


@pytest.mark.parametrize("finished", [False, True])
def test_native_final_cut_has_one_champion_and_no_playoff_or_win_metrics(monkeypatch, finished):
    monkeypatch.setattr(poi, "N_SIMS", 2048)
    played, schedule, settings = small_final_fixture(finished=finished)
    odds, seeds, wins = forecast(played, schedule, settings, 11 if finished else 10)
    assert odds.P_Champ.sum() == pytest.approx(100)
    assert odds.loc["a", "P_Champ"] == 0
    assert seeds.loc["a", 3] == 100
    if finished:
        assert odds.loc["b", "P_Champ"] == 100
        assert odds.loc["c", "P_Champ"] == 0
        assert seeds.loc["c", 2] == 100
        assert odds.Exp_Final_PF.to_dict() == {"a": 70.0, "b": 230.0, "c": 200.0}
    else:
        assert 0 < odds.loc["c", "P_Champ"] < odds.loc["b", "P_Champ"] < 100
        # Independent analytic check for the last two normal score draws.
        z = (odds.loc["b", "Team_Mu"] - odds.loc["c", "Team_Mu"]) / math.sqrt(
            odds.loc["b", "Team_Sigma"] ** 2 + odds.loc["c", "Team_Sigma"] ** 2
        )
        expected = 50 * (1 + math.erf(z / math.sqrt(2)))
        assert odds.loc["b", "P_Champ"] == pytest.approx(expected, abs=3)
    assert odds.Exp_Final_Wins.eq(0).all()
    assert wins.x0_win.eq(100).all()


def test_completed_final_exact_tie_retains_seeded_model_uncertainty(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 4096)
    played, schedule, settings = small_final_fixture(finished=True)
    played.loc[played.franchise_id.eq("c") & played.week.eq(10), "team_points"] = 120
    played.loc[played.franchise_id.eq("c") & played.week.eq(11), "team_points"] = 110
    original = played.copy(deep=True)
    odds, seeds, _ = forecast(played, schedule, settings, 11)
    assert odds.P_Champ.sum() == 100
    assert odds.loc["a", "P_Champ"] == 0
    assert odds.loc["b", "P_Champ"] == pytest.approx(50, abs=3)
    assert odds.loc["c", "P_Champ"] == pytest.approx(50, abs=3)
    assert seeds.loc["b", [1, 2]].sum() == 100
    pd.testing.assert_frame_equal(played, original)


def test_completed_final_exact_tie_uses_explicit_native_winner_when_available(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 128)
    played, schedule, settings = small_final_fixture(finished=True)
    played.loc[played.franchise_id.eq("c") & played.week.eq(10), "team_points"] = 120
    played.loc[played.franchise_id.eq("c") & played.week.eq(11), "team_points"] = 110
    played["team_key"] = "470.l.1.t." + played.franchise_id
    played["winner_team_key"] = None
    played.loc[played.week.eq(11), "winner_team_key"] = "470.l.1.t.c"
    odds, seeds, _ = forecast(played, schedule, settings, 11)
    assert odds.P_Champ.to_dict() == {"a": 0.0, "b": 0.0, "c": 100.0}
    assert seeds.loc["b", 2] == 100


def test_native_final_cumulative_tie_ignores_floating_point_accumulation_noise(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 4096)
    played = pd.DataFrame([
        {"year": 2026, "week": week, "franchise_id": team, "manager": team,
         "opponent": "TBD", "opponent_franchise_id": None, "is_playoffs": 0,
         "is_consolation": 0, "team_points": score}
        for week, scores in [(10, {"a": 100.99, "b": 100.93, "c": 50, "d": 110}),
                             (11, {"a": 99.34, "b": 99.40, "d": 50}), (12, {"a": 50, "b": 50})]
        for team, score in scores.items()
    ])
    settings = {"is_guillotine": True, "num_teams": 4, "start_week": 10, "end_week": 12,
                "playoff_start_week": 13, "num_playoff_teams": 0, "bye_teams": 0}
    odds, _, _ = forecast(played, played.copy(), settings, 12)
    assert odds.loc["a", "P_Champ"] == pytest.approx(50, abs=3)
    assert odds.loc["b", "P_Champ"] == pytest.approx(50, abs=3)


def test_native_repeated_perspectives_count_each_team_score_once(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 512)
    played, schedule, settings = native_fixture()
    expected = forecast(played, schedule, settings, 7)
    # The raw Yahoo graph repeats its hub many times. Neither strength nor PF
    # should change when a canonicalization stage has not yet removed repeats.
    repeated = pd.concat([played, played.loc[played.team_slot.eq(13)]] * 3, ignore_index=True)
    actual = forecast(repeated, pd.concat([schedule, schedule.iloc[:2]]), settings, 7)
    for canonical, raw in zip(expected, actual):
        pd.testing.assert_frame_equal(canonical, raw)


def test_historical_forecast_does_not_leak_later_native_eliminations(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 512)
    played, schedule, settings = native_fixture()
    with_later_results = forecast(played, schedule, settings, 3)
    frontier = schedule.loc[schedule.week.eq(4)].copy()
    future = pd.concat([frontier.assign(week=w) for w in range(4, 14)], ignore_index=True)
    future["opponent"] = "TBD"
    future["opponent_franchise_id"] = None
    at_week_three = forecast(
        played.loc[played.week.le(3)],
        pd.concat([schedule.loc[schedule.week.le(3)], future], ignore_index=True), settings, 3,
    )
    for full_history, snapshot in zip(with_later_results, at_week_three):
        pd.testing.assert_frame_equal(full_history, snapshot)


def test_native_historical_model_ignores_full_season_future_strength_statistics(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 512)
    played, schedule, settings = native_fixture()
    outputs = []
    for extra in (0, 100):
        source = played.copy()
        source.loc[source.week.ge(4), "team_points"] += extra
        outputs.append(poi.calc_regular_week_outputs(
            source, schedule, 2026, 3, pd.DataFrame(),
            season_stats={"mean": source.team_points.mean(), "sd": source.team_points.std()},
            settings_by_year={2026: settings},
        ))
    for original, changed_future in zip(*outputs):
        pd.testing.assert_frame_equal(original, changed_future)


@pytest.mark.parametrize("native_flag", [False, "0"])
def test_ordinary_unassigned_odd_schedule_keeps_even_pair_guard(monkeypatch, native_flag):
    monkeypatch.setattr(poi, "N_SIMS", 32)
    played, schedule, settings = native_fixture()
    settings["is_guillotine"] = native_flag
    with pytest.raises(ValueError, match="Unassigned schedule week 8 has incomplete team identities"):
        forecast(played, schedule, settings, 7)


def test_native_process_writeback_does_not_turn_legacy_pair_records_into_forecast_wins(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB

    monkeypatch.setattr(poi, "N_SIMS", 1000)  # Current production default, across every observed snapshot.
    for name in ("PLAYOFF_SLOTS", "BYE_SLOTS", "BRACKET_RESEED", "TIEBREAKER_ORDER", "NUM_TEAMS",
                 "REGULAR_SEASON_WEEKS", "TARGET_COLS", "ACTIVE_TABLE_CATALOG"):
        monkeypatch.setattr(poi, name, getattr(poi, name))
    played, schedule, settings = native_fixture()
    with LocalLeagueDB(tmp_path, "native_forecast") as db:
        db.save_table("matchup", played, platform="yahoo", league_id="470.l.1")
        db.save_table("schedule", schedule, platform="yahoo", league_id="470.l.1")
        db.save_table("league_settings", pd.DataFrame([dict(settings, year=2026)]))
        conn = db.connect()
        # Earlier common enrichments still carry a legacy pair record. It must
        # not rewrite the native solver's neutral no-H2H forecast distribution.
        conn.execute("UPDATE public.matchup SET wins_to_date=3")
        poi.load_playoff_config_from_settings(SimpleNamespace(data_directory=tmp_path), settings_by_year={2026: settings})
        poi.process_parquet_files(conn=conn, db_name="native_forecast", data_directory=str(tmp_path),
                                  settings_by_year={2026: settings}, target_year=2026)
        actual = conn.execute("SELECT exp_final_wins,x0_win,p_champ FROM public.matchup WHERE week=7").fetchdf()
        assert len(actual) == 8
        assert actual.exp_final_wins.eq(0).all()
        assert actual.x0_win.eq(100).all()
        assert actual.p_champ.sum() == pytest.approx(100, abs=0.05)


def test_yahoo_bulk_fetch_excludes_live_and_pending_native_rows_from_solver_facts(monkeypatch, tmp_path):
    from multi_league.data_fetchers.yahoo import yahoo_matchups

    source = pd.read_csv(
        Path(__file__).parents[1] / "core" / "fixtures" / "yahoo_guillotine_scoreboard.csv"
    )
    matchups = []
    for row in source.iloc[::2].itertuples():
        cards = "".join(
            f"<team><team_key>470.l.1.t.{slot}</team_key><name>Team {slot}</name>"
            f"<managers><manager><guid>owner-{slot}</guid><nickname>Owner {slot}</nickname></manager></managers>"
            f"<team_points><total>{points}</total></team_points></team>"
            for slot, points in [(row.team_slot, row.team_points), (row.opponent_slot, row.opponent_points)]
        )
        status = "postevent" if row.week <= 7 else "inevent" if row.week == 8 else "preevent"
        matchups.append(f"<matchup><week>{row.week}</week><status>{status}</status><teams>{cards}</teams></matchup>")
    xml = "<fantasy_content><league><scoreboard><matchups>" + "".join(matchups) + "</matchups></scoreboard></league></fantasy_content>"
    oauth = SimpleNamespace(session=SimpleNamespace(get=lambda _: SimpleNamespace(
        text=xml, status_code=200, raise_for_status=lambda: None,
    )))
    league = SimpleNamespace(current_week=8, settings=lambda: {
        "start_week": 1, "end_week": 13, "playoff_start_week": 14, "num_playoff_teams": 0,
    })
    monkeypatch.setattr(yahoo_matchups, "YFA_AVAILABLE", True)
    monkeypatch.setattr(yahoo_matchups, "RUN_LOGGER_AVAILABLE", False)
    monkeypatch.setattr(yahoo_matchups, "get_current_nfl_season_year", lambda: 2026)
    monkeypatch.setattr(yahoo_matchups, "yfa", SimpleNamespace(Game=lambda *_: SimpleNamespace(to_league=lambda _: league)))
    ctx = SimpleNamespace(league_id="470.l.1", matchup_data_directory=tmp_path,
                          manager_name_overrides={}, get_oauth_session=lambda: oauth,
                          get_league_id_for_year=lambda _: "470.l.1")
    facts, failures = yahoo_matchups.weekly_matchup_data(ctx=ctx, year=2026)
    assert failures == []
    assert sorted(facts.week.unique()) == [1, 2, 3, 4, 5, 6, 7]
    assert sorted(facts.attrs["schedule_df"].week.unique()) == list(range(1, 14))
    assert facts.attrs["schedule_df"].loc[lambda frame: frame.week.eq(8), "team_points"].gt(0).any()
