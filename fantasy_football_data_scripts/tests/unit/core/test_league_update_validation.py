from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from multi_league.core.league_update_validation import (
    IncompleteSourceError,
    ProviderSnapshotExpectations,
    validate_active_roster_frame,
    validate_espn_final_matchup_frame,
    validate_provider_team_inventory,
    validate_tabular_active_scope,
    validate_provider_snapshot,
)


def test_provider_team_inventory_requires_settings_count_and_exact_unique_ids():
    assert validate_provider_team_inventory(
        provider="espn", settings_team_count=2, team_ids=("1", "2")
    ) == ("1", "2")
    with pytest.raises(IncompleteSourceError, match="team count mismatch"):
        validate_provider_team_inventory(
            provider="espn", settings_team_count=3, team_ids=("1", "2")
        )
    with pytest.raises(IncompleteSourceError, match="identities are incomplete"):
        validate_provider_team_inventory(
            provider="yahoo", settings_team_count=2, team_ids=("1", "1")
        )


def _espn_final_graph_fixture():
    raw = [
        {"home": {"teamId": 1}, "away": {"teamId": 2}, "winner": "HOME", "playoffTierType": "NONE"},
        {"home": {"teamId": 3}, "away": {"teamId": 4}, "winner": "AWAY", "playoffTierType": "NONE"},
        {"home": {"teamId": 5}, "away": None, "winner": "UNDECIDED", "playoffTierType": "WINNERS_BRACKET"},
    ]
    frame = pd.DataFrame([
        {"year": 2026, "week": 15, "team_key": "1", "matchup_id": 0, "is_bye_week": False, "team_points": 10.0},
        {"year": 2026, "week": 15, "team_key": "2", "matchup_id": 0, "is_bye_week": False, "team_points": 9.0},
        {"year": 2026, "week": 15, "team_key": "3", "matchup_id": 1, "is_bye_week": False, "team_points": 8.0},
        {"year": 2026, "week": 15, "team_key": "4", "matchup_id": 1, "is_bye_week": False, "team_points": 11.0},
        {"year": 2026, "week": 15, "team_key": "5", "matchup_id": 2, "is_bye_week": True, "team_points": None},
    ])
    return raw, frame


def test_espn_final_matchup_frame_matches_raw_pairs_and_declared_byes():
    raw, frame = _espn_final_graph_fixture()
    assert validate_espn_final_matchup_frame(
        season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
        raw_schedule=raw, matchups=frame,
    ) == 5


def test_espn_final_matchup_frame_rejects_missing_pair_member():
    raw, frame = _espn_final_graph_fixture()
    with pytest.raises(IncompleteSourceError, match="coverage mismatch"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=frame.loc[frame["team_key"] != "4"],
        )


def test_espn_final_matchup_frame_rejects_broken_pair_or_blank_score():
    raw, frame = _espn_final_graph_fixture()
    wrong_pair = frame.assign(matchup_id=[0, 1, 1, 1, 2])
    with pytest.raises(IncompleteSourceError, match="pair"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=wrong_pair,
        )
    blank_score = frame.copy()
    blank_score.loc[blank_score["team_key"] == "2", "team_points"] = None
    with pytest.raises(IncompleteSourceError, match="score"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=blank_score,
        )


def test_espn_raw_roster_scope_rejects_missing_team_even_when_other_rows_exist():
    rosters = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1"},
    ])
    with pytest.raises(IncompleteSourceError, match="missing.*2"):
        validate_active_roster_frame(
            provider="espn", season=2026, expected_team_ids=("1", "2"),
            requested_weeks=(1,), player_id_column="espn_player_id", rosters=rosters,
        )


def test_espn_raw_roster_scope_admits_complete_live_week_without_final_scores():
    rosters = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1", "fantasy_points": 1.0},
        {"year": 2026, "week": 1, "team_key": "2", "espn_player_id": "p2", "fantasy_points": None},
    ])
    assert validate_active_roster_frame(
        provider="espn", season=2026, expected_team_ids=("1", "2"),
        requested_weeks=(1,), player_id_column="espn_player_id", rosters=rosters,
    ) == 2


def test_raw_roster_scope_rejects_duplicate_provider_player_ownership():
    rosters = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1"},
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1"},
        {"year": 2026, "week": 1, "team_key": "2", "espn_player_id": "p2"},
    ])
    with pytest.raises(IncompleteSourceError, match="duplicate provider player"):
        validate_active_roster_frame(
            provider="espn", season=2026, expected_team_ids=("1", "2"),
            requested_weeks=(1,), player_id_column="espn_player_id", rosters=rosters,
        )


def snapshot():
    return {
        "provider": "sleeper",
        "league_id": "s26",
        "season": 2026,
        "resource_status": {
            "settings": "ok",
            "teams": "ok",
            "rosters": "ok",
            "matchups": "ok",
            "transactions": "ok_empty",
            "draft": "ok_empty",
        },
        "teams": [
            {"team_id": "1", "franchise_id": "f1"},
            {"team_id": "2", "franchise_id": "f2"},
        ],
        "rosters": [
            {"week": 1, "team_id": "1", "players": [{"player_id": "p1", "NFL_player_id": "n1"}]},
            {"week": 1, "team_id": "2", "players": [{"player_id": "p2", "NFL_player_id": "n2"}]},
        ],
        "matchups": [
            {"week": 1, "team_id": "1", "opponent_team_id": "2", "matchup_id": "m1"},
            {"week": 1, "team_id": "2", "opponent_team_id": "1", "matchup_id": "m1"},
        ],
        "transactions": [],
        "draft": [],
    }


def expectations(**changes):
    values = dict(
        provider="sleeper",
        league_id="s26",
        season=2026,
        weeks=(1,),
        expected_team_ids=("1", "2"),
        allow_byes=False,
        uses_median=False,
    )
    values.update(changes)
    return ProviderSnapshotExpectations(**values)


def test_complete_snapshot_passes_with_explicit_valid_empty_resources():
    receipt = validate_provider_snapshot(snapshot(), expectations())
    assert receipt.healthy is True
    assert receipt.expected_teams == receipt.observed_teams == 2
    assert receipt.valid_empty_resources == ("draft", "transactions")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(league_id="wrong"), "league identity"),
        (lambda value: value.update(season=2025), "active season"),
        (lambda value: value["teams"].pop(), "teams"),
        (lambda value: value["rosters"].pop(), "roster"),
        (lambda value: value["matchups"].pop(), "reciprocal"),
        (lambda value: value["rosters"][0]["players"][0].update(NFL_player_id=None), "NFL mapping"),
        (lambda value: value["teams"].append(deepcopy(value["teams"][0])), "duplicate team"),
        (lambda value: value["rosters"].append(deepcopy(value["rosters"][0])), "duplicate roster"),
        (lambda value: value["matchups"].append(deepcopy(value["matchups"][0])), "duplicate matchup"),
    ],
)
def test_incomplete_or_duplicate_provider_snapshots_are_rejected(mutate, message):
    value = snapshot()
    mutate(value)
    with pytest.raises(IncompleteSourceError, match=message):
        validate_provider_snapshot(value, expectations())

def test_bye_is_valid_only_when_declared_and_still_requires_a_roster():
    value = snapshot()
    value["matchups"] = [
        {"week": 1, "team_id": "1", "opponent_team_id": None, "matchup_id": None, "is_bye": True},
        {"week": 1, "team_id": "2", "opponent_team_id": None, "matchup_id": None, "is_bye": True},
    ]
    with pytest.raises(IncompleteSourceError, match="bye"):
        validate_provider_snapshot(value, expectations())
    assert validate_provider_snapshot(value, expectations(allow_byes=True)).healthy is True


def test_median_scoring_requires_one_median_row_per_team_week():
    value = snapshot()
    with pytest.raises(IncompleteSourceError, match="median"):
        validate_provider_snapshot(value, expectations(uses_median=True))
    value["median_matchups"] = [
        {"week": 1, "team_id": "1"},
        {"week": 1, "team_id": "2"},
    ]
    assert validate_provider_snapshot(value, expectations(uses_median=True)).healthy is True


def test_empty_resource_without_explicit_ok_empty_status_is_unknown_not_healthy():
    value = snapshot()
    value["resource_status"]["transactions"] = "ok"
    with pytest.raises(IncompleteSourceError, match="transactions.*unknown"):
        validate_provider_snapshot(value, expectations())


def test_unknown_resource_completeness_is_rejected_even_when_other_tables_are_nonempty():
    value = snapshot()
    del value["resource_status"]["rosters"]
    with pytest.raises(IncompleteSourceError, match="rosters.*unknown"):
        validate_provider_snapshot(value, expectations())


def _tabular_scope():
    return {
        "rosters": pd.DataFrame([
            {"year": 2026, "week": 1, "team_key": "1", "sleeper_player_id": "p1"},
            {"year": 2026, "week": 1, "team_key": "2", "sleeper_player_id": "p2"},
        ]),
        "matchups": pd.DataFrame([
            {"year": 2026, "week": 1, "team_key": "1", "matchup_id": "m1"},
            {"year": 2026, "week": 1, "team_key": "2", "matchup_id": "m1"},
        ]),
        "schedule": pd.DataFrame([
            {"year": 2026, "week": 1, "team_key": "1"},
            {"year": 2026, "week": 1, "team_key": "2"},
        ]),
        "draft": pd.DataFrame([
            {"year": 2026, "draft_id": "d1", "round": 1, "pick": 1},
            {"year": 2026, "draft_id": "d1", "round": 1, "pick": 2},
        ]),
    }


def _validate_tabular(**changes):
    values = _tabular_scope() | changes
    return validate_tabular_active_scope(
        provider="sleeper", league_id="s26", season=2026,
        expected_team_ids=("1", "2"), requested_weeks=(1,),
        finalized_weeks=(1,), player_id_column="sleeper_player_id",
        **values,
    )


def test_actual_weekly_tabular_payload_requires_every_team_week_and_draft_key():
    assert _validate_tabular()["observed_team_weeks"] == 2
    with pytest.raises(IncompleteSourceError, match="roster coverage"):
        _validate_tabular(rosters=_tabular_scope()["rosters"].iloc[:1])
    with pytest.raises(IncompleteSourceError, match="matchup coverage"):
        _validate_tabular(matchups=_tabular_scope()["matchups"].iloc[:1])
    duplicate = pd.concat([_tabular_scope()["draft"], _tabular_scope()["draft"].iloc[:1]])
    with pytest.raises(IncompleteSourceError, match="draft.*duplicate"):
        _validate_tabular(draft=duplicate)


def test_actual_tabular_payload_allows_a_legitimately_live_unfinalized_week():
    values = _tabular_scope()
    assert validate_tabular_active_scope(
        provider="sleeper", league_id="s26", season=2026,
        expected_team_ids=("1", "2"), requested_weeks=(1,),
        finalized_weeks=(), player_id_column="sleeper_player_id",
        rosters=values["rosters"], matchups=pd.DataFrame(),
        schedule=pd.DataFrame(), draft=values["draft"],
    )["observed_final_matchup_weeks"] == 0


def test_sleeper_schedule_fetch_only_names_map_to_exact_matchup_team_keys():
    values = _tabular_scope()
    matchups = values["matchups"].assign(
        manager_week=["A_2026_1", "B_2026_1"],
        team_name=["Team A", "Team B"],
    )
    schedule = values["schedule"].drop(columns=["team_key"]).assign(
        manager_week=["A_2026_1", "B_2026_1"],
        team_name=["Team A", "Team B"],
    )
    assert _validate_tabular(matchups=matchups, schedule=schedule)["schedule_team_weeks"] == 2
    with pytest.raises(IncompleteSourceError, match="schedule.*team identity"):
        _validate_tabular(
            matchups=matchups,
            schedule=schedule.assign(team_name=["Team A", "Wrong Team"]),
        )
