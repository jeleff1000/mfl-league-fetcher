from __future__ import annotations

import pandas as pd
import pytest

from multi_league.core.league_update_ownership import (
    OwnershipContractError,
    PreservationError,
    _frame_fingerprint,
    assert_refresh_preservation,
    overlay_provider_columns,
    table_ownership,
)
import multi_league.core.league_update_ownership as ownership


def test_overlay_ignores_legacy_null_ownership_keys_that_cannot_match_provider_rows():
    """A historical bye shell with no key must not block a later active refresh."""
    contract = table_ownership("matchup")
    existing = pd.DataFrame(
        [
            {"db_name": "league", "manager_week": None, "manager": "legacy"},
            {"db_name": "league", "manager_week": None, "manager": "legacy-two"},
        ]
    )
    incoming = pd.DataFrame(
        [{"db_name": "league", "manager_week": "manager_2026_1", "manager": "current"}]
    )

    actual = overlay_provider_columns(existing, incoming, contract)

    assert actual["manager_week"].tolist() == ["manager_2026_1"]


def _optimal_week_frames():
    old = pd.DataFrame([
        {"db_name": "afi_data", "year": 2026, "week": 1, "player_week": "p1_2026_1",
         "league_wide_optimal_player": 1, "league_wide_optimal_position": "FLX", "clutch_equity": 2.0},
        {"db_name": "afi_data", "year": 2026, "week": 1, "player_week": "p2_2026_1",
         "league_wide_optimal_player": 0, "league_wide_optimal_position": None, "clutch_equity": 1.0},
    ])
    new = old.copy()
    new.loc[0, "league_wide_optimal_player"] = 0
    new.loc[0, "league_wide_optimal_position"] = None
    new.loc[1, "league_wide_optimal_player"] = 1
    new.loc[1, "league_wide_optimal_position"] = "FLX"
    return old, new


def test_recomputed_optimal_label_may_clear_only_after_verified_deselection():
    old, new = _optimal_week_frames()
    receipt = assert_refresh_preservation(
        {"player_fantasy": old}, {"player_fantasy": new}, active_year=2026,
    )
    assert receipt["historical_rows_preserved"] is True
    assert receipt["semantic_optimal_deselections"] == 1
    assert set(receipt["validation_seconds"]) == {
        "user_configuration",
        "active_aliases",
        "source_facts",
        "career_rollups",
        "homepage_outputs",
        "total",
    }
    assert set(receipt["source_fact_seconds"]) == {"player_fantasy"}
    assert set(receipt["source_fact_operation_seconds"]["player_fantasy"]) == {
        "historical_fingerprint",
        "derived_columns",
    }


def test_player_fantasy_preservation_uses_indexed_comparison_not_row_iteration(monkeypatch):
    old, new = _optimal_week_frames()

    def fail_row_iteration(*_args, **_kwargs):
        raise AssertionError("player preservation must not iterate rows")

    monkeypatch.setattr(pd.DataFrame, "iterrows", fail_row_iteration)
    receipt = assert_refresh_preservation(
        {"player_fantasy": old}, {"player_fantasy": new}, active_year=2026,
    )

    assert receipt["semantic_optimal_deselections"] == 1


@pytest.mark.parametrize("mutation", ["still_selected", "all_deselected", "lost_clutch"])
def test_optimal_deselection_exception_cannot_hide_incomplete_enrichment(mutation):
    old, new = _optimal_week_frames()
    if mutation == "still_selected":
        new.loc[0, "league_wide_optimal_player"] = 1
    elif mutation == "all_deselected":
        new.loc[1, "league_wide_optimal_player"] = 0
        new.loc[1, "league_wide_optimal_position"] = None
    else:
        new.loc[0, "clutch_equity"] = None
    with pytest.raises(PreservationError):
        assert_refresh_preservation(
            {"player_fantasy": old}, {"player_fantasy": new}, active_year=2026,
        )


def test_provider_refresh_preserves_existing_derived_player_values():
    existing = pd.DataFrame([{
        "db_name": "league_a",
        "player_week": "p1_2026_1",
        "fantasy_points": 10.0,
        "manager_lamar": 3.0,
        "clutch_equity": 4.5,
    }])
    incoming = pd.DataFrame([{
        "db_name": "league_a",
        "player_week": "p1_2026_1",
        "fantasy_points": 12.0,
        "manager_lamar": None,
        "clutch_equity": None,
    }])

    actual = overlay_provider_columns(
        existing,
        incoming,
        table_ownership("player_fantasy"),
    )

    assert actual.loc[0, "fantasy_points"] == 12.0
    assert actual.loc[0, "manager_lamar"] == 3.0
    assert actual.loc[0, "clutch_equity"] == 4.5


def test_preservation_fingerprint_handles_nullable_integer_historical_witnesses():
    historical = pd.DataFrame(
        {
            "db_name": ["league_a"],
            "year": pd.Series([2025], dtype="Int32"),
            "week": pd.Series([pd.NA], dtype="Int32"),
            "manager_week": ["manager_2025_16"],
            "manager": ["Legacy manager"],
        }
    )

    receipt = assert_refresh_preservation(
        {"matchup": historical},
        {"matchup": historical.copy()},
        active_year=2026,
    )

    assert receipt["historical_rows_preserved"] is True


def test_preservation_keeps_legacy_matchups_without_manager_week_identity():
    """Manual historical finish rows have no provider manager_week key."""
    legacy = pd.DataFrame(
        [
            {
                "db_name": "league_a",
                "year": 2018,
                "week": 16,
                "manager": "LargoRyan",
                "team_name": "Historical finish (user supplied)",
                "opponent": None,
                "manager_week": None,
                "team_points": None,
            },
            {
                "db_name": "league_a",
                "year": 2019,
                "week": 16,
                "manager": "Dak",
                "team_name": "Historical finish (user supplied)",
                "opponent": None,
                "manager_week": None,
                "team_points": None,
            },
        ]
    )

    receipt = assert_refresh_preservation(
        {"matchup": legacy}, {"matchup": legacy.copy()}, active_year=2026,
    )

    assert receipt["historical_rows_preserved"] is True


def test_preservation_uses_both_franchises_for_head_to_head_careers():
    """One franchise can have multiple all-time opponents."""
    careers = pd.DataFrame(
        [
            {
                "db_name": "league_a",
                "franchise_id": "alpha",
                "opponent_franchise_id": "bravo",
                "manager": "Alpha",
                "opponent": "Bravo",
                "games": 12,
            },
            {
                "db_name": "league_a",
                "franchise_id": "alpha",
                "opponent_franchise_id": "charlie",
                "manager": "Alpha",
                "opponent": "Charlie",
                "games": 9,
            },
        ]
    )

    receipt = assert_refresh_preservation(
        {"matchup_h2h_career": careers},
        {"matchup_h2h_career": careers.copy()},
        active_year=2026,
    )

    assert receipt["historical_rows_preserved"] is True


def test_preservation_accepts_structured_non_null_career_values():
    """Fly can decode a historical JSON field as a Python list."""
    careers = pd.DataFrame(
        [
            {
                "db_name": "league_a",
                "franchise_id": "alpha",
                "manager": "Alpha",
                "games": 12,
                "championship_years": [2020, 2021],
            }
        ]
    )

    receipt = assert_refresh_preservation(
        {"matchup_career": careers},
        {"matchup_career": careers.copy()},
        active_year=2026,
    )

    assert receipt["historical_rows_preserved"] is True


def test_preservation_allows_recomputed_current_standings_to_drop_departed_manager():
    """Current standings are a live projection, not an immutable career table."""
    before = pd.DataFrame(
        [{"db_name": "league_a", "franchise_id": "departed", "manager": "Former"}]
    )
    after = pd.DataFrame(
        [{"db_name": "league_a", "franchise_id": "current", "manager": "Current"}]
    )

    receipt = assert_refresh_preservation(
        {"homepage_current_standings": before},
        {"homepage_current_standings": after},
        active_year=2026,
    )

    assert receipt["historical_rows_preserved"] is True


def test_preservation_fingerprint_ignores_database_integer_dtype_normalization():
    source = pd.DataFrame(
        {
            "year": pd.Series([2025], dtype="int64"),
            "games_played": pd.Series([17.0], dtype="float64"),
            "draft_age": pd.Series([24.0], dtype="float64"),
        }
    )
    local = pd.DataFrame(
        {
            "year": pd.Series([2025], dtype="Int32"),
            "games_played": pd.Series([17], dtype="Int32"),
            "draft_age": pd.Series([24], dtype="Int32"),
        }
    )

    assert _frame_fingerprint(source) == _frame_fingerprint(local)


def test_preservation_fingerprint_normalizes_numeric_values_stored_in_object_columns():
    source = pd.DataFrame({"keeper_cost": pd.Series([1.0, None], dtype="object")})
    local = pd.DataFrame({"keeper_cost": pd.Series([1, None], dtype="Int32")})

    assert _frame_fingerprint(source) == _frame_fingerprint(local)


def test_user_configuration_preservation_uses_the_legacy_canonical_comparison(monkeypatch):
    frame = pd.DataFrame({"db_name": ["league_a"], "keeper_cost": [1]})

    def fail_fast_source_fingerprint(*_args, **_kwargs):
        raise AssertionError("user configuration must retain its legacy comparison")

    monkeypatch.setattr(ownership, "_frame_fingerprint", fail_fast_source_fingerprint)
    receipt = ownership.assert_refresh_preservation(
        {"keeper_config": frame}, {"keeper_config": frame.copy()}, active_year=2026,
    )

    assert receipt["user_configuration_preserved"] is True


def test_preservation_fingerprint_uses_columnar_serialization_not_cell_mapping(monkeypatch):
    frame = pd.DataFrame({"year": [2025, 2024], "points": [17.0, None]})

    def fail_cell_mapping(*_args, **_kwargs):
        raise AssertionError("fingerprinting must not map each cell in Python")

    monkeypatch.setattr(pd.DataFrame, "map", fail_cell_mapping)
    fingerprint = _frame_fingerprint(frame)

    assert fingerprint == _frame_fingerprint(frame.iloc[::-1].reset_index(drop=True))


def test_preservation_fingerprint_rejects_a_real_historical_value_change():
    before = pd.DataFrame({"year": [2025], "player_week": ["p_2025_1"], "points": [17.0]})
    after = before.copy()
    after.loc[0, "points"] = 17.01

    assert _frame_fingerprint(before) != _frame_fingerprint(after)


def test_new_provider_row_does_not_copy_another_rows_enrichment():
    existing = pd.DataFrame([{
        "db_name": "league_a",
        "player_week": "p1_2026_1",
        "clutch_equity": 4.5,
    }])
    incoming = pd.DataFrame([{
        "db_name": "league_a",
        "player_week": "p2_2026_1",
        "fantasy_points": 8.0,
    }])

    actual = overlay_provider_columns(
        existing,
        incoming,
        table_ownership("player_fantasy"),
    )

    assert pd.isna(actual.loc[0, "clutch_equity"])


def test_user_configuration_tables_are_never_provider_writable():
    contract = table_ownership("manager_overrides")
    assert contract.user_owned_columns
    assert not contract.provider_owned_columns


def test_unknown_table_has_no_implicit_ownership():
    with pytest.raises(OwnershipContractError, match="unregistered publish table"):
        table_ownership("new_unclassified_table")


@pytest.mark.parametrize(
    "table_name",
    ["player_fantasy", "matchup", "draft", "transactions"],
)
def test_canonical_source_columns_have_explicit_schema_owners(table_name):
    contract = table_ownership(table_name)

    assert contract.provider_owned_columns
    assert contract.derived_columns
    assert not (contract.provider_owned_columns & contract.derived_columns)
    assert contract.classified_columns
