"""Archive routing must work without treating every 2019 season as archived."""

import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from espn_api.football import League
from espn_api.football.player import Player

from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient, ESPNAPIError


def _roster(year, week, player_id):
    return {
        "entries": [
            {
                "lineupSlotId": 0,
                "playerId": player_id,
                "playerPoolEntry": {
                    "player": {
                        "id": player_id,
                        "fullName": f"Player {player_id}",
                        "proTeamId": 1,
                        "defaultPositionId": 1,
                        "eligibleSlots": [0, 20],
                        "stats": [
                            {
                                "seasonId": year,
                                "scoringPeriodId": week,
                                "statSplitTypeId": 1,
                                "statSourceId": 0,
                                "appliedTotal": 12.5 if week == 1 else 20.0,
                                "appliedStats": {"3": 12.5},
                            }
                        ],
                    }
                },
            }
        ]
    }


@pytest.fixture
def provider(monkeypatch):
    """HTTP boundary only; League, BoxScore and BoxPlayer parsing stay real."""
    state = SimpleNamespace(history=True, deny=False, calls=[], transactions=True)

    def request(_session, method, url, **kwargs):
        assert method.lower() == "get"
        params = {k: v[-1] for k, v in parse_qs(urlsplit(url).query).items()}
        params.update(kwargs.get("params") or {})
        views = params.get("view", [])
        views = [views] if isinstance(views, str) else views
        history = "/leagueHistory/" in url
        year = int(params["seasonId"]) if history else int(url.split("/seasons/")[1].split("/")[0])
        week = int(params.get("scoringPeriodId", 1))
        state.calls.append((history, year, list(views), week, kwargs.get("headers")))
        response = requests.Response()
        response.url = url
        response.status_code = 200
        if "proTeamSchedules_wl" in views:
            payload = {"settings": {"proTeams": []}}
        elif state.deny or history != state.history:
            response.status_code = 401
            payload = {}
        else:
            teams = [{"id": i, "playoffSeed": i, "roster": _roster(year, week, i * 100)} for i in (1, 2)]
            schedule = [
                {
                    "id": week,
                    "matchupPeriodId": week,
                    "winner": "HOME",
                    "home": {"teamId": 1, "totalPoints": 115.0},
                    "away": {"teamId": 2, "totalPoints": 90.0},
                }
            ]
            if not history:
                for side, team in zip(("home", "away"), teams):
                    schedule[0][side]["rosterForCurrentScoringPeriod"] = team["roster"]
            if "mRoster" not in views:
                for team in teams:
                    team.pop("roster")
            payload = {
                "seasonId": year,
                "scoringPeriodId": week,
                "settings": {"name": "Archive League"},
                "teams": teams,
                "schedule": schedule,
                "transactions": [{"id": "txn-1"}],
                "draftDetail": {"drafted": True, "picks": [{"playerId": 100}]},
            }
            if not state.transactions:
                payload.pop("transactions")
            if history:
                payload = [payload]
        response._content = json.dumps(payload).encode()
        return response

    monkeypatch.setattr(requests.sessions.Session, "request", request)
    return state


@pytest.mark.parametrize("year", [2018, 2019, 2020])
def test_accessible_modern_season_stays_modern(provider, year):
    provider.history = False
    client = ESPNAPIClient(123)
    assert client.get_league_settings_raw(year) == {"name": "Archive League"}
    assert len(provider.calls) == 1
    assert provider.calls[0][0] is False


@pytest.mark.parametrize("year", [2019, 2020])
def test_archive_fallback_is_shared_by_raw_views(provider, year):
    client = ESPNAPIClient(123)
    assert client.get_league_settings_raw(year) == {"name": "Archive League"}
    assert client.get_raw_team_playoff_seed_map(year) == {1: 1, 2: 2}
    assert len(client.get_raw_schedule(year, 1)) == 1
    assert client.get_raw_transactions(year, 1, strict=True) == [{"id": "txn-1"}]
    assert [call[0] for call in provider.calls] == [False, True, True, True, True]
    schedule_call = provider.calls[3]
    assert schedule_call[3] == 1
    assert json.loads(schedule_call[4]["x-fantasy-filter"]) == {"schedule": {"filterMatchupPeriodIds": {"value": [1]}}}


def test_pre_2018_archive_list_is_unwrapped_for_transactions(provider):
    assert ESPNAPIClient(123).get_raw_transactions(2017, 1, strict=True) == [{"id": "txn-1"}]
    assert [call[0] for call in provider.calls] == [True]


def test_failed_archive_fallback_preserves_authorization_error(provider):
    provider.deny = True
    client = ESPNAPIClient(123)
    with pytest.raises(ESPNAPIError) as exc:
        client.get_raw_transactions(2019, 1, strict=True)
    assert exc.value.status_code == 401
    assert [call[0] for call in provider.calls] == [False, True]
    provider.deny = False
    provider.history = False
    assert client.get_league_settings_raw(2019) == {"name": "Archive League"}
    assert provider.calls[-1][0] is False  # Don't retain an unverified route.


@pytest.fixture
def minimal_league_init(monkeypatch):
    # Skip unrelated season-wide team/player enrichment; retain real provider
    # discovery and every request/parser used by box_scores and the fetchers.
    def fetch_league(league):
        data = league.espn_request.get_league()
        league.settings = SimpleNamespace(name=data["settings"]["name"], matchup_periods={1: [1], 2: [2]})
        league.teams = [
            SimpleNamespace(
                team_id=t["id"],
                team_name=f"Team {t['id']}",
                roster=[Player(entry, league.year) for entry in t["roster"]["entries"]],
            )
            for t in data["teams"]
        ]
        league.current_week = league.currentMatchupPeriod = 2
        league.draft = league.espn_request.get_league_draft()["draftDetail"]["picks"]

    monkeypatch.setattr(League, "_fetch_league", fetch_league)


@pytest.mark.parametrize("year", [2019, 2020])
def test_modern_league_preserves_weekly_box_players_and_draft(provider, minimal_league_init, year):
    provider.history = False
    client = ESPNAPIClient(123)
    league = client.get_league(year)
    assert league.draft == [{"playerId": 100}]
    assert client.get_league(year) is league
    for week, points in [(1, 12.5), (2, 20.0)]:
        box = league.box_scores(week)[0]
        assert (box.home_team.team_id, box.away_team.team_id) == (1, 2)
        assert (box.home_score, box.away_score) == (115.0, 90.0)
        assert box.home_lineup[0].playerId == 100
        assert box.home_lineup[0].points == points
        assert box.home_lineup[0].slot_position == "QB"
    before = len(provider.calls)
    assert client.get_league_settings_raw(year) == {"name": "Archive League"}
    assert len(provider.calls) == before + 1
    assert provider.calls[-1][0] is False


@pytest.fixture
def ctx(tmp_path):
    return SimpleNamespace(
        espn_s2=None,
        swid=None,
        playoff_start_week=14,
        data_directory=tmp_path,
        get_league_id_for_year=lambda year: 123,
        get_manager_name=lambda team_id, **kw: f"Manager {team_id}",
        get_team_name=lambda team_id, year: f"Team {team_id}",
        get_manager_guid=lambda team_id, **kw: f"owner-{team_id}",
        get_franchise_id=lambda team_id, **kw: f"franchise-{team_id}",
    )


@pytest.mark.parametrize("year", [2019, 2020])
@pytest.mark.parametrize("history", [False, True])
def test_existing_matchup_fetcher_emits_week(provider, minimal_league_init, ctx, year, history):
    from multi_league.data_fetchers.espn.espn_matchups import fetch_espn_matchups

    provider.history = history
    rows = fetch_espn_matchups(ctx, year, weeks=[1])
    assert rows is not None
    assert rows[["year", "week", "team_key", "team_points"]].to_dict("records") == [
        {"year": year, "week": 1, "team_key": "1", "team_points": 115.0},
        {"year": year, "week": 1, "team_key": "2", "team_points": 90.0},
    ]


@pytest.mark.parametrize("year", [2019, 2020])
@pytest.mark.parametrize("history", [False, True])
def test_archive_roster_snapshot_does_not_become_weekly_lineup(provider, minimal_league_init, ctx, year, history):
    from multi_league.data_fetchers.espn.espn_rosters import fetch_espn_rosters

    provider.history = history
    rows = fetch_espn_rosters(ctx, year, weeks=[1])
    assert rows is not None and len(rows) == 2
    assert rows["year"].eq(year).all() and rows["week"].eq(1).all()
    if history:
        assert rows[["fantasy_points", "projected_points", "fantasy_position", "is_started"]].isna().all().all()
    else:
        assert rows["fantasy_points"].tolist() == [12.5, 12.5]
        assert rows["fantasy_position"].tolist() == ["QB", "QB"]


@pytest.mark.parametrize("year", [2019, 2020])
def test_archive_transactions_reuse_estimates_only_for_full_import(provider, minimal_league_init, ctx, year):
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions

    provider.transactions = False  # Live archive: HTTP 200, no transactions field.
    client = ESPNAPIClient(123)
    league = client.get_league(year)
    league.draft = [SimpleNamespace(playerId=100, team=league.teams[0], playerName="Player 100")]
    league.teams[1].roster = league.teams[0].roster
    league.teams[0].roster = []
    with pytest.raises(ESPNAPIError, match="malformed"):
        client.get_raw_transactions(year, 1, strict=True)

    before = len(provider.calls)
    rows = fetch_espn_transactions(ctx, year, client=client, league=league)
    assert rows is not None
    assert rows[["transaction_type", "team_key", "espn_player_id"]].to_dict("records") == [
        {"transaction_type": "drop", "team_key": "1", "espn_player_id": 100},
        {"transaction_type": "add", "team_key": "2", "espn_player_id": 100},
    ]
    assert rows["is_estimated"].all() and rows["week"].eq(6).all()
    assert rows["timestamp"].isna().all()
    assert len(provider.calls) == before  # Reuse draft/rosters; no 17-week scan.

    # The same missing archive surface must still fail an active refresh.
    with pytest.raises(RuntimeError, match="waivers.*could not be verified"):
        fetch_espn_transactions(ctx, year, max_week=1, client=client, league=league)

    # An accessible modern season must never enter the estimator.
    provider.history = False
    provider.transactions = True
    modern_client = ESPNAPIClient(123)
    modern_league = modern_client.get_league(year)
    modern_league.draft = league.draft
    modern_league.teams = league.teams
    assert fetch_espn_transactions(ctx, year, max_week=1, client=modern_client, league=modern_league) is None
