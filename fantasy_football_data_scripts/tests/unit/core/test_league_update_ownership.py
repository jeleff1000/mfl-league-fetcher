from __future__ import annotations

import pandas as pd
import pytest

from multi_league.core.league_update_ownership import (
    OwnershipContractError,
    overlay_provider_columns,
    table_ownership,
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
