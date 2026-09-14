"""Tests for flatten_rules_to_columns — JSON blob -> flat dict."""

import json
from pathlib import Path

import pytest

from multi_league.core.keeper_config_flatten import (
    UnknownPositionKeysError,
    flatten_rules_to_columns,
)
from multi_league.core.keeper_config_schema import KEEPER_CONFIG_FIELD_COLUMNS

# parents[0] = core/, parents[1] = unit/, parents[2] = tests/, parents[3] = ffd_scripts/, parents[4] = repo root
FIXTURES = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "keeper_config"


def _load(name):
    return json.loads((FIXTURES / name).read_text())


def test_returns_dict_keyed_by_field_columns():
    flat = flatten_rules_to_columns(_load("snake_basic.json"))
    assert set(flat.keys()) == set(KEEPER_CONFIG_FIELD_COLUMNS)


def test_snake_basic_extracts_top_level():
    flat = flatten_rules_to_columns(_load("snake_basic.json"))
    assert flat["enabled"] is True
    assert flat["draft_type"] == "snake"
    assert flat["max_keepers"] == 3
    assert flat["min_price"] == 1
    assert flat["num_rounds"] == 15
    assert flat["min_round"] == 1
    assert flat["round_up"] is True


def test_snake_basic_extracts_nested_drafted():
    flat = flatten_rules_to_columns(_load("snake_basic.json"))
    assert flat["snake_drafted_round_offset"] == -1
    assert flat["snake_fa_pickup_source"] == "last"
    assert flat["snake_fa_pickup_round"] is None


def test_auction_basic_extracts_auction_subkeys():
    flat = flatten_rules_to_columns(_load("auction_basic.json"))
    assert flat["draft_type"] == "auction"
    assert flat["budget"] == 200
    assert flat["auction_drafted_mult"] == 1.0
    assert flat["auction_drafted_flat"] == 5
    assert flat["auction_faab_mult"] == 1.0
    assert flat["auction_faab_flat"] == 0
    assert flat["auction_fa_value"] == 1


def test_snake_with_escalation_extracts_year_2_plus():
    flat = flatten_rules_to_columns(_load("snake_with_escalation.json"))
    assert flat["escalation_type"] == "round_escalation"
    assert flat["escalation_rounds_per_year"] == 1
    assert flat["snake_fa_pickup_source"] == "fixed"
    assert flat["snake_fa_pickup_round"] == 16


def test_position_limits_mapped_to_seven_slots():
    flat = flatten_rules_to_columns(_load("auction_with_position_limits.json"))
    assert flat["position_limit_qb"] == 1
    assert flat["position_limit_rb"] == 2
    assert flat["position_limit_wr"] == 2
    assert flat["position_limit_te"] == 1
    assert flat["position_limit_k"] == 1
    assert flat["position_limit_def"] == 1
    assert flat["position_limit_flex"] == 1


def test_franchise_tag_and_rookie_discount_extracted():
    flat = flatten_rules_to_columns(_load("auction_with_position_limits.json"))
    assert flat["franchise_tag_enabled"] is True
    assert flat["franchise_tag_cost_mult"] == 1.5
    assert flat["rookie_discount_enabled"] is True
    assert flat["rookie_discount_rounds"] == 5
    assert flat["rookie_discount_mult"] == 0.5
    assert flat["keeper_payroll_cap"] == 100


def test_partial_blob_returns_none_for_missing_fields():
    flat = flatten_rules_to_columns(_load("partial_no_auction.json"))
    assert flat["snake_drafted_round_offset"] == -1
    assert flat["auction_drafted_mult"] is None
    assert flat["auction_faab_mult"] is None
    assert flat["max_years"] is None


def test_empty_blob_returns_all_none_except_required_defaults():
    flat = flatten_rules_to_columns(_load("empty.json"))
    # All field columns present, all None
    for col in KEEPER_CONFIG_FIELD_COLUMNS:
        assert flat[col] is None, f"{col} should be None for empty blob"


def test_exotic_position_keys_raises_with_collected_keys():
    with pytest.raises(UnknownPositionKeysError) as exc_info:
        flatten_rules_to_columns(_load("exotic_position_keys.json"))
    assert set(exc_info.value.unknown_keys) == {"DB", "DL", "LB"}


def test_exotic_position_keys_lenient_mode_drops_silently_and_logs():
    flat = flatten_rules_to_columns(_load("exotic_position_keys.json"), strict=False)
    assert flat["position_limit_qb"] == 2
    # DB/DL/LB silently dropped
    assert all(flat[f"position_limit_{p}"] is None for p in ("rb", "wr", "te", "k", "def", "flex"))
