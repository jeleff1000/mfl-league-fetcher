import pandas as pd

from multi_league.core.canonical_transaction import normalize_transaction_df


def test_normalize_transaction_df_expands_raw_trade_rows_once():
    raw = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "transaction_type": "trade",
                "player": "PlayerX",
                "year": 2025,
                "week": 5,
                "timestamp": 1_000,
                "yahoo_player_id": "101",
                "source_manager": "Alice",
                "source_manager_guid": "guid-alice",
                "source_team_name": "Team Alice",
                "destination_manager": "Bob",
                "destination_manager_guid": "guid-bob",
                "destination_team_name": "Team Bob",
            }
        ]
    )

    normalized = normalize_transaction_df(raw, platform="yahoo", league_id="461.l.90939")

    assert len(normalized) == 2
    assert set(normalized["manager"]) == {"Alice", "Bob"}
    assert set(normalized["source_manager"]) == {"Alice", "Bob"}
    assert set(normalized["trade_direction"]) == {"received", "sent"}
    assert normalized["destination_manager"].isna().all()


def test_normalize_transaction_df_does_not_redouble_expanded_trade_rows():
    raw = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "transaction_type": "trade",
                "player": "PlayerX",
                "year": 2025,
                "week": 5,
                "timestamp": 1_000,
                "yahoo_player_id": "101",
                "source_manager": "Alice",
                "source_manager_guid": "guid-alice",
                "source_team_name": "Team Alice",
                "destination_manager": "Bob",
                "destination_manager_guid": "guid-bob",
                "destination_team_name": "Team Bob",
            }
        ]
    )

    once = normalize_transaction_df(raw, platform="yahoo", league_id="461.l.90939")
    twice = normalize_transaction_df(once, platform="yahoo", league_id="461.l.90939")

    assert len(once) == 2
    assert len(twice) == 2
    assert set(twice["manager"]) == {"Alice", "Bob"}


def test_normalize_transaction_df_assigns_unique_sequence_for_multi_player_transactions():
    raw = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "transaction_sequence": 0,
                "transaction_type": "add",
                "player": "Player A",
                "year": 2025,
                "week": 5,
                "timestamp": 1_000,
                "sleeper_player_id": "101",
                "manager": "Alice",
            },
            {
                "transaction_id": "t1",
                "transaction_sequence": 0,
                "transaction_type": "drop",
                "player": "Player B",
                "year": 2025,
                "week": 5,
                "timestamp": 1_000,
                "sleeper_player_id": "102",
                "manager": "Alice",
            },
            {
                "transaction_id": "t2",
                "transaction_sequence": None,
                "transaction_type": "add",
                "player": "Player C",
                "year": 2025,
                "week": 5,
                "timestamp": 2_000,
                "sleeper_player_id": "103",
                "manager": "Bob",
            },
        ]
    )

    normalized = normalize_transaction_df(raw, platform="sleeper", league_id="123")

    keys = normalized[["transaction_id", "transaction_sequence"]].astype(str)
    assert keys.duplicated().sum() == 0
    assert normalized.loc[normalized["transaction_id"] == "t1", "transaction_sequence"].tolist() == [0, 1]
    assert normalized.loc[normalized["transaction_id"] == "t2", "transaction_sequence"].tolist() == [0]


def test_normalize_transaction_df_treats_hidden_yahoo_guid_as_missing_franchise_id():
    raw = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "transaction_type": "add",
                "player": "Player A",
                "year": 2003,
                "week": 1,
                "timestamp": 1_000,
                "manager": "Hidden Team",
                "manager_guid": "--hidden--",
                "franchise_id": "--hidden--",
                "source_manager": "Hidden Team",
                "source_manager_guid": "--hidden--",
                "source_franchise_id": "--hidden--",
                "destination_manager": "Known Manager",
                "destination_manager_guid": "known-guid",
                "destination_franchise_id": None,
            }
        ]
    )

    normalized = normalize_transaction_df(raw, platform="yahoo", league_id="461.l.90939")

    assert pd.isna(normalized.loc[0, "franchise_id"])
    assert pd.isna(normalized.loc[0, "source_franchise_id"])
    assert normalized.loc[0, "destination_franchise_id"] == "known-guid"
