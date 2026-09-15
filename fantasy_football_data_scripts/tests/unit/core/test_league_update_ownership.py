from __future__ import annotations

import pandas as pd
import pytest

from multi_league.core.league_update_ownership import (
    OwnershipContractError,
    PreservationError,
    assert_refresh_preservation,
    overlay_provider_columns,
    table_ownership,
)


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
