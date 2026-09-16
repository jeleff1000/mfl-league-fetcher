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
