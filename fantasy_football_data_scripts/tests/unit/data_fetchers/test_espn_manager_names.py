from types import SimpleNamespace

from multi_league.data_fetchers.espn.espn_context import build_manager_names


def test_public_espn_owner_ids_fall_back_to_distinct_team_names():
    teams = [
        SimpleNamespace(team_id=1, team_name="Da slim reaper", owners=["{OWNER-1}"]),
        SimpleNamespace(team_id=2, team_name="Dak shots", owners=["{OWNER-2}"]),
    ]

    assert build_manager_names(teams) == {1: "Da slim reaper", 2: "Dak shots"}


def test_visible_espn_owner_names_still_use_manager_deduplication():
    teams = [
        SimpleNamespace(
            team_id=1,
            team_name="Team One",
            owners=[SimpleNamespace(firstName="Alex", lastName="Smith")],
        ),
        SimpleNamespace(
            team_id=2,
            team_name="Team Two",
            owners=[SimpleNamespace(firstName="Alex", lastName="Jones")],
        ),
    ]

    assert build_manager_names(teams) == {1: "Alex S.", 2: "Alex J."}
