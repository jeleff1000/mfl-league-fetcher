from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.sota_recon.witness_gate.position_taxonomy import (
    BROAD_POSITIONS,
    POSITION_TAXONOMY_VERSION,
    UnknownPositionToken,
    normalize_position,
    positions_compatible,
    validate_position_pair,
)


def test_long_snapper_maps_to_ol_without_erasing_detail():
    value = normalize_position("LS")
    assert value.position == "OL"
    assert value.nfl_position == "LS"
    assert value.source_position_raw == "LS"
    assert value.taxonomy_version == "position-taxonomy.v1"


@pytest.mark.parametrize(
    ("raw", "broad", "detailed"),
    [
        ("B", "RB", "B"),
        ("WB", "RB", "WB"),
        ("E", "WR", "E"),
        ("LE", "DL", "LE"),
        ("MG", "DL", "MG"),
        ("S", "DB", "S"),
    ],
)
def test_historical_roles_keep_detailed_semantics(raw, broad, detailed):
    value = normalize_position(raw)
    assert (value.position, value.nfl_position) == (broad, detailed)


def test_unknown_nonempty_token_fails_loudly():
    with pytest.raises(UnknownPositionToken, match="MYSTERY"):
        normalize_position("MYSTERY")


@pytest.mark.parametrize("raw", ["AbdulSalaam", "USC", "23", "BB-LDH-FB-"])
def test_malformed_or_nonposition_observations_remain_loud_unknowns(raw: str) -> None:
    with pytest.raises(UnknownPositionToken, match=raw):
        normalize_position(raw)


def test_empty_values_remain_null() -> None:
    assert normalize_position(None).position is None
    assert normalize_position("").nfl_position is None
    assert normalize_position("  ").source_position_raw is None


def test_case_whitespace_and_compound_alternatives_are_normalized() -> None:
    value = normalize_position(" rb - fb ")
    assert value.position == "RB"
    assert value.nfl_position == "FB/RB"
    assert value.source_position_raw == " rb - fb "


@pytest.mark.parametrize(
    ("raw", "broad"),
    [
        ("Center", "OL"),
        ("Defensive tackle", "DL"),
        ("Defensive back", "DB"),
        ("Defensive end", "DL"),
        ("End", "WR"),
        ("Guard", "OL"),
        ("Kicker", "K"),
        ("Linebacker", "LB"),
        ("Offensive tackle", "OL"),
        ("Punter", "P"),
        ("Quarterback", "QB"),
        ("Running back", "RB"),
        ("Tackle", "OL"),
        ("Tight end", "TE"),
        ("Wide receiver", "WR"),
    ],
)
def test_observed_player_bio_position_words_are_contracted(raw: str, broad: str) -> None:
    assert normalize_position(raw).position == broad


def test_non_strict_unknown_preserves_raw_but_makes_no_position_claim() -> None:
    value = normalize_position("MYSTERY", strict=False)
    assert value.position is None
    assert value.nfl_position is None
    assert value.source_position_raw == "MYSTERY"


def test_cross_family_compound_fails_loudly() -> None:
    with pytest.raises(UnknownPositionToken, match="RB/WR"):
        normalize_position("RB/WR")


def test_non_strict_cross_family_compound_makes_no_position_claim() -> None:
    value = normalize_position("RB/WR", strict=False)
    assert value.position is None
    assert value.nfl_position is None
    assert value.source_position_raw == "RB/WR"


@pytest.mark.parametrize("raw", ["QB", "LS", "WB", "LE", "RB-FB", "CB/S"])
def test_every_nonnull_normalized_position_is_a_broad_position(raw: str) -> None:
    value = normalize_position(raw)
    assert value.position in BROAD_POSITIONS


def test_position_compatibility_uses_broad_families() -> None:
    assert positions_compatible("RB", "WB")
    assert positions_compatible("DL", "LE")
    assert not positions_compatible("QB", "S")
    assert not positions_compatible(None, "QB")


def test_position_pair_must_match_the_taxonomy() -> None:
    validate_position_pair("OL", "LS")
    with pytest.raises(ValueError, match="incompatible"):
        validate_position_pair("RB", "LS")
    with pytest.raises(ValueError, match="broad position"):
        validate_position_pair("LS", "LS")


def test_contract_declares_the_versioned_finite_map() -> None:
    contract_path = (
        Path(__file__).parents[6]
        / "scripts"
        / "sota_recon"
        / "witness_gate"
        / "contracts"
        / "position_taxonomy.v1.json"
    )
    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "1"
    assert payload["taxonomy_version"] == POSITION_TAXONOMY_VERSION
    assert frozenset(payload["broad_positions"]) == BROAD_POSITIONS
    assert payload["detailed_to_broad"]["LS"] == "OL"
