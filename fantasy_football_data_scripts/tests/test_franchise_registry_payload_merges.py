"""
Test FranchiseRegistry integration with franchise_merges payload.

Verifies that the franchise_merges parameter (from the wizard) correctly
populates _guid_merges, causing duplicate GUIDs to collapse into a single franchise.
"""

from __future__ import annotations

import pandas as pd

from multi_league.core.franchise_registry import FranchiseRegistry
from multi_league.transformations.matchup.discover_franchises import (
    _apply_transaction_party_franchise_columns,
    _derive_franchise_merges_from_name_overrides,
)


def test_franchise_merges_payload_consumed():
    """franchise_merges payload should populate _guid_merges so two GUIDs collapse to one franchise."""
    matchup = pd.DataFrame(
        {
            "year": [2018, 2018],
            "week": [1, 1],
            "manager_guid": ["guid_a", "guid_b"],
            "manager": ["David", "David"],
            "team_name": ["Team A", "Team B"],
            "team_key": ["k.t.1", "k.t.2"],
        }
    )
    registry = FranchiseRegistry.from_data(
        matchup_df=matchup,
        franchise_merges=[{"display_name": "David", "owner_ids": ["guid_a", "guid_b"]}],
    )
    # Both guids should resolve to a single franchise_id
    franchise_ids = {f.franchise_id for f in registry.franchises.values()}
    assert len(franchise_ids) == 1


def test_name_only_manager_override_derives_stable_owner_merge():
    """Older name-only overrides should still become owner-ID franchise merges."""
    matchup = pd.DataFrame(
        {
            "year": [2018, 2019, 2020, 2021],
            "week": [1, 1, 1, 1],
            "manager_guid": ["old_owner", "old_owner", "new_owner", "new_owner"],
            "manager": ["Old Greg", "Old Greg", "Greg", "Greg"],
        }
    )

    merges = _derive_franchise_merges_from_name_overrides(matchup, {"Old Greg": "Greg"})

    assert merges == [{"display_name": "Greg", "owner_ids": ["new_owner", "old_owner"]}]


def test_name_only_manager_override_does_not_merge_overlapping_owners():
    """If owner IDs overlap in the same season, require an explicit merge payload."""
    matchup = pd.DataFrame(
        {
            "year": [2020, 2020],
            "week": [1, 1],
            "manager_guid": ["old_owner", "new_owner"],
            "manager": ["Old Greg", "Greg"],
        }
    )

    merges = _derive_franchise_merges_from_name_overrides(matchup, {"Old Greg": "Greg"})

    assert merges == []


def test_name_only_manager_override_derives_hidden_owner_merge():
    """Hidden rows use the same pseudo-owner ID that FranchiseRegistry will scan."""
    matchup = pd.DataFrame(
        {
            "year": [2018, 2019],
            "week": [1, 1],
            "manager_guid": ["--", "new_owner"],
            "manager": ["Hidden Greg", "Greg"],
            "team_name": ["Old Team", "New Team"],
        }
    )

    merges = _derive_franchise_merges_from_name_overrides(matchup, {"Hidden Greg": "Greg"})

    assert merges == [{"display_name": "Greg", "owner_ids": ["new_owner", "hidden_hidden_greg"]}]


def test_name_only_manager_override_skips_hidden_rows_that_need_team_disambiguation():
    matchup = pd.DataFrame(
        {
            "year": [2018, 2018, 2019],
            "week": [1, 1, 1],
            "manager_guid": ["--", "--", "new_owner"],
            "manager": ["Hidden Greg", "Hidden Greg", "Greg"],
            "team_name": ["Old Team A", "Old Team B", "New Team"],
        }
    )

    merges = _derive_franchise_merges_from_name_overrides(matchup, {"Hidden Greg": "Greg"})

    assert merges == []


def test_transaction_party_franchise_columns_resolve_hidden_placeholders_from_registry():
    matchup = pd.DataFrame(
        {
            "year": [2003, 2003],
            "week": [1, 1],
            "manager_guid": ["--hidden--", "--hidden--"],
            "manager": ["Zain", "Ali"],
            "team_name": ["Zain Team", "Ali Team"],
            "team_key": ["461.l.1.t.1", "461.l.1.t.2"],
        }
    )
    registry = FranchiseRegistry.from_data(matchup_df=matchup)
    tx = pd.DataFrame(
        {
            "year": [2003],
            "source_manager": ["Zain"],
            "source_manager_guid": ["--hidden--"],
            "source_team_name": ["Zain Team"],
            "source_franchise_id": ["--hidden--"],
            "destination_manager": ["Ali"],
            "destination_manager_guid": ["--hidden--"],
            "destination_team_name": ["Ali Team"],
            "destination_franchise_id": ["--hidden--"],
        }
    )

    resolved = _apply_transaction_party_franchise_columns(tx, registry)
    owner_to_fid = {franchise.owner_guid: fid for fid, franchise in registry.franchises.items()}

    assert resolved.loc[0, "source_franchise_id"] == owner_to_fid["hidden_zain"]
    assert resolved.loc[0, "destination_franchise_id"] == owner_to_fid["hidden_ali"]
