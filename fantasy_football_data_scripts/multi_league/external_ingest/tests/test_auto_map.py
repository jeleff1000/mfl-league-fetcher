"""Tests for column auto-mapping."""

from multi_league.external_ingest.auto_map import (
    normalize,
    auto_map,
    column_set_hash,
)
from multi_league.external_ingest.manifests import MATCHUP_MANIFEST


def test_normalize_lowercases():
    assert normalize("Manager") == "manager"
    assert normalize("YEAR") == "year"


def test_normalize_collapses_separators():
    assert normalize("manager_guid") == "manager_guid"
    assert normalize("manager-guid") == "manager_guid"
    assert normalize("manager guid") == "manager_guid"
    assert normalize("manager  guid") == "manager_guid"
    assert normalize("manager/guid") == "manager_guid"


def test_normalize_strips_whitespace():
    assert normalize("  year  ") == "year"


def test_auto_map_canonical_name_wins():
    result = auto_map(["year", "week", "manager", "opponent", "team_points", "opponent_points"], MATCHUP_MANIFEST)
    assert result.column_map["manager"] == "manager"
    assert result.column_map["year"] == "year"


def test_auto_map_alias_resolves_to_canonical_slot():
    # File uses "owner" instead of "manager"
    result = auto_map(["year", "week", "owner", "opponent", "team_points", "opponent_points"], MATCHUP_MANIFEST)
    assert result.column_map["manager"] == "owner"


def test_auto_map_canonical_beats_alias():
    # File has BOTH "manager" and "owner". Canonical wins; "owner" is dropped (no slot collision).
    result = auto_map(
        ["year", "week", "manager", "owner", "opponent", "team_points", "opponent_points"], MATCHUP_MANIFEST
    )
    assert result.column_map["manager"] == "manager"
    assert "owner" not in result.column_map.values()


def test_auto_map_two_aliases_same_slot_is_ambiguous():
    # File has two non-canonical aliases for `manager`: "owner" and "name".
    result = auto_map(["year", "week", "owner", "name", "opponent", "team_points", "opponent_points"], MATCHUP_MANIFEST)
    assert "manager" in result.ambiguous_slots
    assert "manager" not in result.column_map


def test_auto_map_unfilled_slots_reported():
    result = auto_map(["year", "week", "manager"], MATCHUP_MANIFEST)
    assert "opponent" in result.unfilled_slots


def test_auto_map_satisfaction_status():
    full = auto_map(["year", "week", "manager", "opponent", "team_points", "opponent_points"], MATCHUP_MANIFEST)
    assert full.satisfied is True

    short = auto_map(["year", "week", "manager"], MATCHUP_MANIFEST)
    assert short.satisfied is False


def test_column_set_hash_is_order_invariant():
    h1 = column_set_hash(["year", "week", "manager"])
    h2 = column_set_hash(["manager", "year", "week"])
    assert h1 == h2


def test_column_set_hash_is_case_invariant():
    h1 = column_set_hash(["Year", "Week", "Manager"])
    h2 = column_set_hash(["year", "week", "manager"])
    assert h1 == h2


def test_column_set_hash_separator_invariant():
    h1 = column_set_hash(["manager_guid"])
    h2 = column_set_hash(["manager-guid"])
    h3 = column_set_hash(["manager guid"])
    assert h1 == h2 == h3


def test_column_set_hash_dedupes():
    h1 = column_set_hash(["year", "year", "week"])
    h2 = column_set_hash(["year", "week"])
    assert h1 == h2


def test_column_set_hash_distinguishes_different_sets():
    assert column_set_hash(["year", "week"]) != column_set_hash(["year", "week", "manager"])
