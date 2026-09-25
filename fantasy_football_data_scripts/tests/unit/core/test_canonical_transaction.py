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


def test_normalize_transaction_df_expands_legacy_manager_scoped_trade_rows():
    raw = pd.DataFrame(
        [
            {
                "transaction_id": "331.l.381581.tr.167",
                "transaction_type": "trade",
                "player": "Darrin Reaves",
                "yahoo_player_id": "27943",
                "year": 2014,
                "week": 11,
                "manager": "Ilan",
                "manager_guid": "guid-ilan",
                "franchise_id": "franchise-ilan",
                "team_name": "Ilan Team",
            },
            {
                "transaction_id": "331.l.381581.tr.167",
                "transaction_type": "trade",
                "player": "DeSean Jackson",
                "yahoo_player_id": "8826",
                "year": 2014,
                "week": 11,
                "manager": "Ilan",
                "manager_guid": "guid-ilan",
                "franchise_id": "franchise-ilan",
                "team_name": "Ilan Team",
            },
            {
                "transaction_id": "331.l.381581.tr.167",
                "transaction_type": "trade",
                "player": "Fred Jackson",
                "yahoo_player_id": "8063",
                "year": 2014,
                "week": 11,
                "manager": "Tani",
                "manager_guid": "guid-tani",
                "franchise_id": "franchise-tani",
                "team_name": "Tani Team",
            },
            {
                "transaction_id": "331.l.381581.tr.167",
                "transaction_type": "trade",
                "player": "Hakeem Nicks",
                "yahoo_player_id": "9293",
                "year": 2014,
                "week": 11,
                "manager": "Tani",
                "manager_guid": "guid-tani",
                "franchise_id": "franchise-tani",
                "team_name": "Tani Team",
            },
        ]
    )

    normalized = normalize_transaction_df(raw, platform="yahoo", league_id="331.l.381581")

    assert len(normalized) == 8
    assert normalized.groupby(["yahoo_player_id", "trade_direction"]).size().eq(1).all()
    assert set(normalized["trade_direction"]) == {"received", "sent"}
    assert normalized["source_franchise_id"].notna().all()
    assert set(
        normalized.loc[
            normalized["yahoo_player_id"] == "27943",
            ["manager", "source_manager", "trade_direction"],
        ].itertuples(index=False, name=None)
    ) == {
        ("Ilan", "Tani", "received"),
        ("Tani", "Ilan", "sent"),
    }


def test_normalize_transaction_df_does_not_redouble_legacy_manager_scoped_trade_rows():
    raw = pd.DataFrame(
        [
            {
                "transaction_id": "legacy-trade",
                "transaction_type": "trade",
                "player": "Player A",
                "year": 2014,
                "week": 11,
                "manager": "Alice",
                "franchise_id": "alice-id",
            },
            {
                "transaction_id": "legacy-trade",
                "transaction_type": "trade",
                "player": "Player B",
                "year": 2014,
                "week": 11,
                "manager": "Bob",
                "franchise_id": "bob-id",
            },
        ]
    )

    once = normalize_transaction_df(raw, platform="yahoo", league_id="331.l.381581")
    twice = normalize_transaction_df(once, platform="yahoo", league_id="331.l.381581")

    assert len(once) == 4
    assert len(twice) == 4
    assert set(twice["trade_direction"]) == {"received", "sent"}


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
