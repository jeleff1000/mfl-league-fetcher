import pytest
from multi_league.external_ingest.manifests import (
    MATCHUP_MANIFEST,
    DRAFT_MANIFEST,
    TRANSACTIONS_MANIFEST,
    PLAYER_FANTASY_MANIFEST,
    get_manifest,
)


def test_matchup_manifest_has_required_identity_slots():
    slots_by_name = {s.name: s for s in MATCHUP_MANIFEST.slots}
    for name in ("year", "week", "manager", "opponent"):
        assert name in slots_by_name
        assert slots_by_name[name].group == "identity"


def test_matchup_manifest_satisfaction_passes_with_points():
    filled = {"year", "week", "manager", "opponent", "team_points", "opponent_points"}
    assert all(rule(filled) for rule in MATCHUP_MANIFEST.satisfaction_rules)


def test_matchup_manifest_satisfaction_passes_with_winner_only():
    filled = {"year", "week", "manager", "opponent", "win"}
    assert all(rule(filled) for rule in MATCHUP_MANIFEST.satisfaction_rules)


def test_matchup_manifest_satisfaction_fails_no_outcome():
    filled = {"year", "week", "manager", "opponent"}
    assert not all(rule(filled) for rule in MATCHUP_MANIFEST.satisfaction_rules)


def test_matchup_manifest_satisfaction_fails_missing_identity():
    filled = {"year", "week", "manager", "team_points", "opponent_points"}  # no opponent
    assert not all(rule(filled) for rule in MATCHUP_MANIFEST.satisfaction_rules)


def test_manager_guid_is_optional_slot():
    slots_by_name = {s.name: s for s in MATCHUP_MANIFEST.slots}
    assert slots_by_name["manager_guid"].group == "optional"


def test_manager_aliases_include_owner_and_name():
    slots_by_name = {s.name: s for s in MATCHUP_MANIFEST.slots}
    aliases = set(slots_by_name["manager"].aliases)
    assert "owner" in aliases
    assert "name" in aliases


def test_get_manifest_returns_correct_table():
    assert get_manifest("matchup") is MATCHUP_MANIFEST
    assert get_manifest("draft") is DRAFT_MANIFEST
    assert get_manifest("transactions") is TRANSACTIONS_MANIFEST
    assert get_manifest("player_fantasy") is PLAYER_FANTASY_MANIFEST


def test_get_manifest_unknown_table_raises():
    with pytest.raises(KeyError):
        get_manifest("unknown_table")


def test_win_derivable_from_points():
    slots_by_name = {s.name: s for s in MATCHUP_MANIFEST.slots}
    win_slot = slots_by_name["win"]
    assert win_slot.derivable_from is not None
    # win = team_points > opponent_points
    assert win_slot.derivable_from({"team_points": 100, "opponent_points": 90}) == 1
    assert win_slot.derivable_from({"team_points": 80, "opponent_points": 90}) == 0
