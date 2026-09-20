from types import SimpleNamespace

from multi_league.data_fetchers.espn.espn_api_client import (
    ESPNAPIClient,
    _fetch_legacy_draft_with_list_ids,
    _fetch_legacy_players_with_list_ids,
    _find_legacy_key,
    _legacy_draft_scalar,
    _normalize_legacy_espn_ids,
)


def test_legacy_draft_scalar_unwraps_espn_list_ids():
    assert _legacy_draft_scalar([42]) == 42
    assert _legacy_draft_scalar([[[42]]]) == 42
    assert _legacy_draft_scalar([]) is None
    assert _legacy_draft_scalar(42) == 42


def test_legacy_draft_parser_normalizes_list_valued_ids():
    class FakeLeague:
        def __init__(self):
            self.espn_request = SimpleNamespace(
                get_league_draft=lambda: {
                    "draftDetail": {
                        "drafted": True,
                        "picks": [
                            {
                                "teamId": [1],
                                "playerId": [99],
                                "nominatingTeamId": [2],
                                "roundId": 1,
                                "roundPickNumber": 1,
                                "bidAmount": 0,
                                "keeper": False,
                            }
                        ],
                    }
                }
            )
            self.player_map = {99: "Legacy Player"}
            self.draft = []

        @staticmethod
        def get_team_data(team_id):
            return f"team-{team_id}"

    league = FakeLeague()

    _fetch_legacy_draft_with_list_ids(league)

    assert len(league.draft) == 1
    assert league.draft[0].playerId == 99
    assert league.draft[0].playerName == "Legacy Player"
    assert league.draft[0].team == "team-1"
    assert league.draft[0].nominatingTeam == "team-2"


def test_raw_draft_parser_keeps_complete_picks_when_espn_drafted_flag_is_stale():
    class FakeLeague:
        def __init__(self):
            self.espn_request = SimpleNamespace(
                get_league_draft=lambda: {
                    "draftDetail": {
                        "drafted": False,
                        "inProgress": False,
                        "picks": [
                            {
                                "teamId": 1,
                                "playerId": 99,
                                "nominatingTeamId": 1,
                                "roundId": 1,
                                "roundPickNumber": 1,
                                "bidAmount": 0,
                                "keeper": False,
                            }
                        ],
                    }
                }
            )
            self.player_map = {99: "Current Player"}
            self.draft = []

        @staticmethod
        def get_team_data(team_id):
            return f"team-{team_id}"

    league = FakeLeague()

    _fetch_legacy_draft_with_list_ids(league)

    assert [(pick.playerId, pick.playerName) for pick in league.draft] == [
        (99, "Current Player")
    ]


def test_get_league_populates_complete_raw_draft_when_espn_flag_is_stale(monkeypatch):
    import espn_api.football

    class FakeLeague:
        def __init__(self, **_kwargs):
            self.espn_request = SimpleNamespace(
                LEAGUE_ENDPOINT="https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026",
                get_league_draft=lambda: {
                    "draftDetail": {
                        "drafted": False,
                        "inProgress": False,
                        "picks": [
                            {
                                "teamId": 1,
                                "playerId": 99,
                                "nominatingTeamId": 1,
                                "roundId": 1,
                                "roundPickNumber": 1,
                                "bidAmount": 0,
                                "keeper": False,
                            }
                        ],
                    }
                },
            )
            self.player_map = {99: "Current Player"}
            self.draft = []
            self.finalScoringPeriod = 17
            self.current_week = 2
            self.scoringPeriodId = 2

        @staticmethod
        def get_team_data(team_id):
            return f"team-{team_id}"

    monkeypatch.setattr(espn_api.football, "League", FakeLeague)

    league = ESPNAPIClient(12345).get_league(2026)

    assert [(pick.playerId, pick.playerName) for pick in league.draft] == [
        (99, "Current Player")
    ]


def test_raw_draft_parser_resolves_players_from_team_rosters_when_player_map_lags():
    class FakeLeague:
        def __init__(self):
            self.espn_request = SimpleNamespace(
                get_league_draft=lambda: {
                    "draftDetail": {
                        "drafted": False,
                        "inProgress": False,
                        "picks": [
                            {
                                "teamId": 1,
                                "playerId": 99,
                                "nominatingTeamId": 1,
                                "roundId": 1,
                                "roundPickNumber": 1,
                                "bidAmount": 0,
                                "keeper": False,
                            }
                        ],
                    }
                }
            )
            self.player_map = {}
            self.teams = [
                SimpleNamespace(
                    team_id=1,
                    roster=[SimpleNamespace(playerId=99, name="Roster Player")],
                )
            ]
            self.draft = []

        @staticmethod
        def get_team_data(team_id):
            return f"team-{team_id}"

    league = FakeLeague()

    _fetch_legacy_draft_with_list_ids(league)

    assert league.draft[0].playerName == "Roster Player"


def test_draft_fetch_accepts_the_verified_roster_name_map(monkeypatch):
    from multi_league.data_fetchers.espn import espn_api_client, espn_draft

    team = SimpleNamespace(team_id=1, team_name="One", roster=[])
    pick = SimpleNamespace(
        playerName="",
        playerId=99,
        round_num=1,
        round_pick=1,
        bid_amount=0,
        keeper_status=False,
        team=team,
    )
    league = SimpleNamespace(teams=[team], draft=[pick])

    class Client:
        def __init__(self, *_args):
            pass

        def get_league(self, _year):
            return league

    ctx = SimpleNamespace(
        espn_s2=None,
        swid=None,
        get_league_id_for_year=lambda _year: 12345,
        get_manager_name=lambda *_args, **_kwargs: "Manager",
        get_manager_guid=lambda *_args, **_kwargs: "owner",
        get_franchise_id=lambda *_args, **_kwargs: "franchise",
    )
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
    monkeypatch.setattr(espn_draft, "_resolve_espn_nfl_id", lambda _player_id: "00-0000099")

    frame = espn_draft.fetch_espn_draft(
        ctx,
        2026,
        player_names_by_id={"99": "Roster Player"},
    )

    assert frame[["espn_player_id", "player", "NFL_player_id"]].to_dict("records") == [{
        "espn_player_id": 99,
        "player": "Roster Player",
        "NFL_player_id": "00-0000099",
    }]


def test_raw_draft_parser_resolves_players_from_espn_player_pool():
    class FakeLeague:
        def __init__(self):
            self.espn_request = SimpleNamespace(
                get_league_draft=lambda: {
                    "draftDetail": {
                        "drafted": False,
                        "inProgress": False,
                        "picks": [{
                            "teamId": 1,
                            "playerId": 99,
                            "nominatingTeamId": 1,
                            "roundId": 1,
                            "roundPickNumber": 1,
                            "bidAmount": 0,
                            "keeper": False,
                        }],
                    }
                },
                get_pro_players=lambda: [{"id": 99, "fullName": "Pool Player"}],
            )
            self.player_map = {}
            self.teams = []
            self.draft = []

        @staticmethod
        def get_team_data(team_id):
            return f"team-{team_id}"

    league = FakeLeague()

    _fetch_legacy_draft_with_list_ids(league)

    assert league.draft[0].playerName == "Pool Player"


def test_raw_draft_parser_ignores_undrafted_placeholder_slots():
    league = SimpleNamespace(
        espn_request=SimpleNamespace(
            get_league_draft=lambda: {
                "draftDetail": {
                    "drafted": False,
                    "inProgress": False,
                    "picks": [{
                        "teamId": 1,
                        "playerId": -1,
                        "nominatingTeamId": 1,
                        "roundId": 1,
                        "roundPickNumber": 1,
                        "bidAmount": 0,
                        "keeper": False,
                    }],
                }
            },
            get_pro_players=lambda: [],
        ),
        player_map={},
        teams=[],
        draft=[],
        get_team_data=lambda team_id: f"team-{team_id}",
    )

    _fetch_legacy_draft_with_list_ids(league)

    assert league.draft == []


def test_legacy_player_parser_normalizes_list_valued_ids():
    league = SimpleNamespace(
        espn_request=SimpleNamespace(
            get_pro_players=lambda: [{"id": [99], "fullName": "Legacy Player"}]
        ),
        player_map={},
    )

    _fetch_legacy_players_with_list_ids(league)

    assert league.player_map == {99: "Legacy Player", "Legacy Player": 99}


def test_legacy_payload_normalizer_only_unwraps_singular_id_fields():
    payload = {"teams": [{"id": [1], "owners": ["owner-a"]}], "schedule": [1, 2]}

    assert _normalize_legacy_espn_ids(payload) == {
        "teams": [{"id": 1, "owners": ["owner-a"]}],
        "schedule": [1, 2],
    }


def test_legacy_key_finder_skips_empty_top_level_legacy_ids():
    payload = {
        "proTeamId": [],
        "playerPoolEntry": {"player": {"proTeamId": [[11]]}},
    }

    assert _legacy_draft_scalar(_find_legacy_key(payload, "proTeamId")) == 11


def test_legacy_payload_normalizer_repairs_null_player_pool_entry():
    payload = {"fullName": "Legacy Player", "playerPoolEntry": None}

    assert _normalize_legacy_espn_ids(payload)["playerPoolEntry"] == {"player": {}}
