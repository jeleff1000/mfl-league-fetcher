from types import SimpleNamespace

from multi_league.data_fetchers.espn.espn_api_client import (
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
