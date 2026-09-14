"""Regression tests for Yahoo transaction parsing edge cases."""

import pandas as pd

from multi_league.data_fetchers.yahoo.yahoo_transactions import (
    _resolve_transaction_team_keys,
    backfill_unknown_drop_managers,
    process_transactions_chronologically,
)


def test_resolve_transaction_team_keys_falls_back_to_parent_drop_team():
    manager_key, source_key, destination_key = _resolve_transaction_team_keys(
        transaction_type="drop",
        destination="waivers",
        player_source_key="",
        player_destination_key="",
        transaction_team_keys={
            "source_team_key": "461.l.90939.t.7",
            "destination_team_key": "",
            "waiver_team_key": "",
            "trader_team_key": "",
            "tradee_team_key": "",
        },
    )

    assert manager_key == "461.l.90939.t.7"
    assert source_key == "461.l.90939.t.7"
    assert destination_key == ""


def test_resolve_transaction_team_keys_falls_back_to_parent_add_team():
    manager_key, source_key, destination_key = _resolve_transaction_team_keys(
        transaction_type="add",
        destination="team",
        player_source_key="",
        player_destination_key="",
        transaction_team_keys={
            "source_team_key": "",
            "destination_team_key": "461.l.90939.t.4",
            "waiver_team_key": "",
            "trader_team_key": "",
            "tradee_team_key": "",
        },
    )

    assert manager_key == "461.l.90939.t.4"
    assert source_key == ""
    assert destination_key == "461.l.90939.t.4"


def test_backfill_unknown_drop_managers_uses_last_known_owner():
    df = pd.DataFrame(
        {
            "transaction_id": ["tx-1", "tx-2"],
            "year": [2025, 2025],
            "week": [2, 4],
            "timestamp": [1_000, 2_000],
            "transaction_type": ["add", "drop"],
            "manager": ["Alice", "Unknown"],
            "manager_guid": ["guid-a", None],
            "team_name": ["Alpha", None],
            "yahoo_player_id": ["12345", "12345"],
            "player_key": ["461.p.12345", "461.p.12345"],
            "player": ["Player One", "Player One"],
            "destination": ["team", "waivers"],
        }
    )

    ordered = process_transactions_chronologically(df)
    repaired = backfill_unknown_drop_managers(ordered)

    repaired_drop = repaired.loc[repaired["transaction_id"] == "tx-2"].iloc[0]
    assert repaired_drop["manager"] == "Alice"
    assert repaired_drop["manager_guid"] == "guid-a"
    assert repaired_drop["team_name"] == "Alpha"
