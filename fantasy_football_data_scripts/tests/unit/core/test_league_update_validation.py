from __future__ import annotations

from copy import deepcopy

import pytest

from multi_league.core.league_update_validation import (
    IncompleteSourceError,
    ProviderSnapshotExpectations,
    validate_provider_snapshot,
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
