"""Result cutoffs must not truncate the native remaining-season schedule."""
from types import SimpleNamespace

import pandas as pd
import pytest

from multi_league.data_fetchers.sleeper import sleeper_matchups
from multi_league.data_fetchers.espn import espn_schedules, espn_api_client


@pytest.fixture(autouse=True)
def season_clock(monkeypatch):
    from multi_league.core import date_utils
    monkeypatch.setattr(date_utils, "get_current_nfl_season_year", lambda: 2026)


def context(tmp_path):
    return SimpleNamespace(
        league_id="native", data_directory=tmp_path, manager_name_overrides={},
        espn_s2="cookie", swid="swid", playoff_start_week=15,
        get_league_id_for_year=lambda year: "native",
        get_manager_name=lambda team, *args, **kwargs: f"Preferred {team}",
        get_manager_guid=lambda team, *args: f"owner-{team}",
        get_team_name=lambda team, *args: f"Team {team}",
        get_franchise_id=lambda team, *args: f"saved-{team}",
    )


class DB:
    league_name = "league"
    def __init__(self): self.tables = {}
    def save_table(self, name, frame, **kwargs): self.tables[name] = frame.copy()
    def table_exists(self, name): return name in self.tables
    def read_table(self, name, year=None):
        frame = self.tables[name].copy()
        return frame if year is None else frame.loc[frame.year.eq(year)].copy()


def played():
    return pd.DataFrame([
        {"year": 2026, "week": week, "team_key": str(team), "manager": f"Preferred {team}",
         "manager_guid": f"owner-{team}", "franchise_id": f"saved-{team}",
         "team_name": f"Team {team}", "team_points": 100 + team}
        for week in range(1, 7) for team in range(1, 13)
    ])


class Sleeper:
    def __init__(self, status="in_season"):
        self.status = status
        self.requested = []
    def get_league(self, league_id):
        return {"season": "2026", "status": self.status, "total_rosters": 12,
                "settings": {"last_scored_leg": 6, "leg": 7, "playoff_week_start": 15,
                             "playoff_teams": 6}}
    def get_league_users(self, league_id):
        return [{"user_id": f"owner-{i}", "display_name": f"Preferred {i}"} for i in range(1, 13)]
    def get_league_rosters(self, league_id):
        return [{"roster_id": i, "owner_id": f"owner-{i}"} for i in range(1, 13)]
    def get_league_matchups(self, league_id, week):
        self.requested.append(week)
        return [{"roster_id": i, "matchup_id": (i+1)//2, "points": 0} for i in range(1, 13)]


@pytest.mark.parametrize("mode", ["quick", "full"])
def test_sleeper_import_stages_only_remaining_native_regular_fixtures(monkeypatch, tmp_path, mode):
    from multi_league.core.local_db import LocalLeagueDB

    ctx = context(tmp_path)
    ctx.import_mode = mode
    client = Sleeper()
    facts = played()
    monkeypatch.setattr(sleeper_matchups.SleeperMatchupFetcher, "fetch_matchups_for_year", lambda *a: facts)
    with LocalLeagueDB(tmp_path, "league") as db:
        sleeper_matchups.fetch_sleeper_matchups(ctx, 2026, client=client, db=db)
        assert client.requested == list(range(7, 15))
        future = db.read_table("schedule")
        assert len(future) == 96
        assert future.groupby("week").franchise_id.nunique().to_dict() == {w: 12 for w in range(7, 15)}
        assert future.loc[future.franchise_id.eq("saved-1"), "opponent_franchise_id"].eq("saved-2").all()
        assert future[["team_points", "opponent_points", "win", "loss"]].isna().all().all()
        pd.testing.assert_frame_equal(db.read_table("matchup")[facts.columns].reset_index(drop=True), facts,
                                      check_dtype=False)


def test_sleeper_explicit_recovery_and_completed_seasons_do_not_refetch_future(monkeypatch, tmp_path):
    monkeypatch.setattr(sleeper_matchups.SleeperMatchupFetcher, "fetch_matchups_for_year", lambda *a: played())
    for status, weeks in [("complete", None), ("in_season", [1])]:
        client, db = Sleeper(status), DB()
        sleeper_matchups.fetch_sleeper_matchups(context(tmp_path), 2026, weeks=weeks, client=client, db=db)
        assert client.requested == []
        assert "schedule" not in db.tables


@pytest.mark.parametrize("mode", ["quick", "full"])
def test_espn_import_uses_one_native_season_schedule_not_played_results(monkeypatch, tmp_path, mode):
    payload = {
        "status": {"currentMatchupPeriod": 7}, "teams": [{"id": i} for i in range(1, 13)],
        "settings": {"scheduleSettings": {"matchupPeriodCount": 14,
            "matchupPeriods": {str(w): [w] for w in range(1, 15)}}},
        "schedule": [{"matchupPeriodId": w, "home": {"teamId": i}, "away": {"teamId": i+1},
                      "winner": "UNDECIDED", "playoffTierType": "NONE"}
                     for w in range(1, 15) for i in range(1, 13, 2)],
    }
    calls = []
    class Client:
        def __init__(self, *args): pass
        def get_raw_league(self, year, views):
            calls.append((year, set(views)))
            return payload
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
    ctx = context(tmp_path)
    ctx.import_mode = mode
    future = espn_schedules.fetch_espn_schedule(ctx, 2026)
    assert calls == [(2026, {"mSettings", "mTeam", "mScoreboard", "mMatchupScore"})]
    assert len(future) == 168
    assert future.groupby("week").franchise_id.nunique().to_dict() == {w: 12 for w in range(1, 15)}
    assert future.loc[future.franchise_id.eq("saved-1"), "opponent_franchise_id"].eq("saved-2").all()
    assert future.loc[future.week.gt(6), ["win", "loss"]].isna().all().all()


@pytest.mark.parametrize("variant", ["ownerless", "shared-owner", "unpublished"])
def test_sleeper_future_identity_and_coverage_survive_real_normalization(monkeypatch, tmp_path, variant):
    from multi_league.core.local_db import LocalLeagueDB

    client = Sleeper()
    facts = played().drop(columns="franchise_id")
    if variant == "ownerless":
        client.get_league_rosters = lambda league: [
            {"roster_id": i, "owner_id": None if i == 1 else f"owner-{i}"} for i in range(1, 13)]
        facts.loc[facts.team_key.eq("1"), ["manager", "manager_guid"]] = ["Team 1", "orp000100"]
    elif variant == "shared-owner":
        client.get_league_rosters = lambda league: [
            {"roster_id": i, "owner_id": f"owner-{1 if i == 2 else i}"} for i in range(1, 13)]
        facts.loc[facts.team_key.eq("2"), ["manager", "manager_guid"]] = ["Preferred 1", "owner-1"]
    else:
        original = client.get_league_matchups
        def pending(league, week):
            rows = original(league, week)
            if week == 8: return []
            if week == 9:
                for row in rows: row["matchup_id"] = None
            return rows
        client.get_league_matchups = pending
    monkeypatch.setattr(sleeper_matchups.SleeperMatchupFetcher, "fetch_matchups_for_year", lambda *a: facts)
    with LocalLeagueDB(tmp_path, "league") as db:
        sleeper_matchups.fetch_sleeper_matchups(context(tmp_path), 2026, client=client, db=db)
        from multi_league.transformations.sql_enrichments import SQLEnrichments
        # The ordinary import runs this same pass later, after the other
        # fetchers. It must not split future identities from played history.
        SQLEnrichments("league", conn=db.connect(), data_dir=str(tmp_path)).resolve_hidden_managers()
        schedule = db.read_table("schedule")
        assert schedule.groupby("week").franchise_id.nunique().to_dict() == {w: 12 for w in range(7, 15)}
        assert set(schedule.franchise_id) == set(db.read_table("matchup").franchise_id)
        if variant == "ownerless":
            assert "orp000100" in set(schedule.franchise_id)
        elif variant == "shared-owner":
            assert len(set(schedule.franchise_id)) == 12
        else:
            pending = schedule.loc[schedule.week.isin([8, 9])]
            assert len(pending) == 24
            assert pending.opponent.eq("TBD").all()
            assert pending.opponent_franchise_id.isna().all()
        known = schedule.loc[~schedule.opponent.eq("TBD")]
        assert known.opponent_franchise_id.notna().all()
        assert len(db.read_table("matchup")) == 72
