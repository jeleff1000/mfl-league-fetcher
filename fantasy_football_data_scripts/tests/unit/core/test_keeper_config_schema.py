"""Tests for the shared keeper_config schema (single source of truth)."""

from multi_league.core.keeper_config_schema import (
    KEEPER_CONFIG_COLUMNS,
    KEEPER_CONFIG_DDL,
    KEEPER_CONFIG_KEY_COLUMNS,
    KEEPER_CONFIG_FIELD_COLUMNS,
)


def test_total_column_count_is_43():
    assert len(KEEPER_CONFIG_COLUMNS) == 43


def test_keys_are_three():
    assert KEEPER_CONFIG_KEY_COLUMNS == ["db_name", "year", "updated_at"]


def test_field_count_is_40():
    assert len(KEEPER_CONFIG_FIELD_COLUMNS) == 40


def test_no_rules_json_in_schema():
    assert "rules_json" not in KEEPER_CONFIG_COLUMNS


def test_top_level_toggles_present():
    expected = {
        "enabled",
        "draft_type",
        "max_keepers",
        "max_years",
        "budget",
        "min_price",
        "max_price",
        "num_rounds",
        "min_round",
        "round_up",
    }
    assert expected.issubset(set(KEEPER_CONFIG_COLUMNS))


def test_snake_columns_present():
    expected = {"snake_drafted_round_offset", "snake_fa_pickup_source", "snake_fa_pickup_round"}
    assert expected.issubset(set(KEEPER_CONFIG_COLUMNS))


def test_auction_columns_present():
    expected = {
        "auction_drafted_mult",
        "auction_drafted_flat",
        "auction_faab_mult",
        "auction_faab_flat",
        "auction_fa_value",
    }
    assert expected.issubset(set(KEEPER_CONFIG_COLUMNS))


def test_escalation_columns_present():
    expected = {
        "escalation_type",
        "escalation_mult",
        "escalation_flat_add",
        "escalation_flat_per_year",
        "escalation_rounds_per_year",
    }
    assert expected.issubset(set(KEEPER_CONFIG_COLUMNS))


def test_position_limit_columns_present():
    expected = {f"position_limit_{p}" for p in ("qb", "rb", "wr", "te", "k", "def", "flex")}
    assert expected.issubset(set(KEEPER_CONFIG_COLUMNS))


def test_ddl_starts_with_create_table():
    assert KEEPER_CONFIG_DDL.strip().upper().startswith("CREATE TABLE IF NOT EXISTS KEEPER_CONFIG")


def test_ddl_contains_all_columns():
    for col in KEEPER_CONFIG_COLUMNS:
        assert col in KEEPER_CONFIG_DDL, f"Column {col} missing from DDL"
