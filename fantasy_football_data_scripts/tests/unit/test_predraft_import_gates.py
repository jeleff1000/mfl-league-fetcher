"""Native pre-draft state must not require a completed draft or played facts."""
from types import SimpleNamespace

import pytest

import espn_initial_import
import sleeper_initial_import


class Sleeper:
    def __init__(self, status="pre_draft", scored=False, missing_roster=False):
        self.status, self.scored, self.missing_roster = status, scored, missing_roster

    def get_league(self, league_id):
        return {"league_id": league_id, "season": "2026", "sport": "nfl", "status": self.status,
                "settings": {"num_teams": 2, "start_week": 1, "last_scored_leg": 0}}

    def get_nfl_state(self):
        return {"season": "2026", "week": 7, "season_type": "regular"}

    def get_league_users(self, league_id):
        return [{"user_id": "a"}, {"user_id": "b"}]

    def get_league_rosters(self, league_id):
        rows = [{"roster_id": i, "owner_id": name, "players": None,
                 "settings": {"wins": 0, "losses": 0, "ties": 0,
                              "fpts": 10 if self.scored else 0, "fpts_decimal": 0}}
                for i, name in [(1, "a"), (2, "b")]]
        return rows[:1] if self.missing_roster else rows

    def get_league_drafts(self, league_id):
        return [{"draft_id": "draft", "season": "2026", "status": self.status,
                 "settings": {"teams": 2, "rounds": 1}}]

    def get_draft_picks(self, draft_id):
        return []

    def get_league_matchups(self, league_id, week):
        return []


@pytest.mark.parametrize("status", ["pre_draft", "drafting"])
def test_sleeper_native_predraft_does_not_require_players_or_completed_picks(status):
    assert sleeper_initial_import._sleeper_has_verified_unplayed_shell(
        Sleeper(status=status), year=2026, league_id="native") is True


def test_sleeper_quick_predraft_does_not_require_local_draft_rows():
    counts = {"league_settings": 1, "draft": 0, "matchup": 0, "player_fantasy": 0}
    db = SimpleNamespace(table_exists=lambda table: table in counts, row_count=lambda table: counts[table])
    ctx = SimpleNamespace(import_mode="quick", league_id="native", get_league_id_for_year=lambda year: "native")
    assert sleeper_initial_import._validate_sleeper_quick_source(ctx, Sleeper(), db, [2026]) is True


def test_native_sleeper_predraft_does_not_depend_on_the_global_nfl_clock():
    class NextSeason(Sleeper):
        def get_nfl_state(self):
            raise ConnectionError("NFL clock unavailable; league itself is verified pre-draft")
    assert sleeper_initial_import._sleeper_has_verified_unplayed_shell(
        NextSeason(), year=2026, league_id="native") is True


@pytest.mark.parametrize("owners", [[None, "b"], [None, None], ["a", "a"]])
def test_predraft_sleeper_ownership_does_not_require_every_slot_to_be_claimed(owners):
    class Unfilled(Sleeper):
        def get_league_users(self, league_id):
            return [{"user_id": owner} for owner in set(owners) if owner is not None]
        def get_league_rosters(self, league_id):
            rows = super().get_league_rosters(league_id)
            for row, owner in zip(rows, owners): row["owner_id"] = owner
            return rows
    assert sleeper_initial_import._sleeper_has_verified_unplayed_shell(
        Unfilled(), year=2026, league_id="native") is True


@pytest.mark.parametrize("client", [Sleeper(status="in_season"), Sleeper(scored=True), Sleeper(missing_roster=True)])
def test_sleeper_predraft_exception_does_not_hide_missing_or_scored_data(client):
    assert sleeper_initial_import._sleeper_has_verified_unplayed_shell(client, year=2026, league_id="native") is False


def espn_payload():
    return {"id": 17, "seasonId": 2026, "status": {"isActive": True, "currentMatchupPeriod": 7},
            "draftDetail": {"drafted": False, "picks": []},
            "teams": [{"id": team, "roster": {"entries": []},
                       "record": {"overall": {"wins": 0, "losses": 0, "ties": 0, "pointsFor": 0}}}
                      for team in [1, 2]],
            "schedule": [{"matchupPeriodId": week, "winner": "UNDECIDED",
                          "home": {"teamId": 1, "totalPoints": 0}, "away": {"teamId": 2, "totalPoints": 0}}
                         for week in range(1,15)]}


def test_espn_native_predraft_does_not_require_completed_picks_or_roster_entries():
    client = SimpleNamespace(get_raw_league=lambda year, views: espn_payload())
    assert espn_initial_import._espn_has_verified_unplayed_shell(client, year=2026, league_id=17) is True


@pytest.mark.parametrize("kind", ["scored", "winner", "missing_teams", "missing_draft_state"])
def test_espn_predraft_exception_does_not_hide_real_or_missing_data(kind):
    payload = espn_payload()
    if kind == "scored": payload["teams"][0]["record"]["overall"]["pointsFor"] = 10
    elif kind == "winner": payload["schedule"][0]["winner"] = "HOME"
    elif kind == "missing_teams": payload["teams"] = []
    else: payload["draftDetail"] = {}
    client = SimpleNamespace(get_raw_league=lambda year, views: payload)
    assert espn_initial_import._espn_has_verified_unplayed_shell(client, year=2026, league_id=17) is False
