import pandas as pd

from multi_league.core.trade_utils import TRADE_DEDUP_FILTER, duplicate_trade_rows


class TestTradeDeduplicateFilter:
    def test_filter_is_no_longer_needed_for_per_party_schema(self):
        assert TRADE_DEDUP_FILTER == ""


class TestDuplicateTradeRows:
    def _make_df(self, rows):
        return pd.DataFrame(rows)

    def test_two_party_trade_creates_one_row_per_party_perspective(self):
        df = self._make_df(
            [
                {
                    "transaction_id": "t1",
                    "transaction_type": "trade",
                    "player": "PlayerX",
                    "source_manager": "Alice",
                    "source_manager_guid": "g1",
                    "source_team_name": "Team A",
                    "source_franchise_id": "f1",
                    "destination_manager": "Bob",
                    "destination_manager_guid": "g2",
                    "destination_team_name": "Team B",
                    "destination_franchise_id": "f2",
                },
                {
                    "transaction_id": "t1",
                    "transaction_type": "trade",
                    "player": "PlayerY",
                    "source_manager": "Bob",
                    "source_manager_guid": "g2",
                    "source_team_name": "Team B",
                    "source_franchise_id": "f2",
                    "destination_manager": "Alice",
                    "destination_manager_guid": "g1",
                    "destination_team_name": "Team A",
                    "destination_franchise_id": "f1",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        assert len(result) == 4
        player_x = result[result["player"] == "PlayerX"][["manager", "source_manager", "trade_direction"]]
        assert {tuple(row) for row in player_x.itertuples(index=False, name=None)} == {
            ("Alice", "Bob", "sent"),
            ("Bob", "Alice", "received"),
        }

    def test_trade_rows_clear_destination_columns_after_perspective_expansion(self):
        df = self._make_df(
            [
                {
                    "transaction_id": "t1",
                    "transaction_type": "trade",
                    "player": "PlayerX",
                    "source_manager": "Alice",
                    "source_manager_guid": "g1",
                    "source_team_name": "Team A",
                    "source_franchise_id": "f1",
                    "destination_manager": "Bob",
                    "destination_manager_guid": "g2",
                    "destination_team_name": "Team B",
                    "destination_franchise_id": "f2",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        assert set(result["manager"]) == {"Alice", "Bob"}
        assert set(result["source_manager"]) == {"Alice", "Bob"}
        assert result["destination_manager"].isna().all()
        assert result["destination_franchise_id"].isna().all()

    def test_trade_rows_keep_counterparty_metadata_in_source_columns(self):
        df = self._make_df(
            [
                {
                    "transaction_id": "t1",
                    "transaction_type": "trade",
                    "player": "PlayerX",
                    "source_manager": "Alice",
                    "source_manager_guid": "g1",
                    "source_team_name": "Team A",
                    "source_franchise_id": "f1",
                    "destination_manager": "Bob",
                    "destination_manager_guid": "g2",
                    "destination_team_name": "Team B",
                    "destination_franchise_id": "f2",
                },
            ]
        )

        result = duplicate_trade_rows(df).sort_values(["manager"]).reset_index(drop=True)

        alice = result[result["manager"] == "Alice"].iloc[0]
        bob = result[result["manager"] == "Bob"].iloc[0]

        assert alice["source_manager"] == "Bob"
        assert alice["source_manager_guid"] == "g2"
        assert alice["source_team_name"] == "Team B"
        assert alice["source_franchise_id"] == "f2"

        assert bob["source_manager"] == "Alice"
        assert bob["source_manager_guid"] == "g1"
        assert bob["source_team_name"] == "Team A"
        assert bob["source_franchise_id"] == "f1"

    def test_non_trade_rows_pass_through_and_fill_manager_from_destination(self):
        df = self._make_df(
            [
                {
                    "transaction_id": "a1",
                    "transaction_type": "add",
                    "player": "Gould",
                    "manager": None,
                    "manager_guid": None,
                    "team_name": None,
                    "franchise_id": None,
                    "source_manager": None,
                    "source_manager_guid": None,
                    "source_team_name": None,
                    "source_franchise_id": None,
                    "destination_manager": "Alice",
                    "destination_manager_guid": "g1",
                    "destination_team_name": "Team A",
                    "destination_franchise_id": "f1",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["manager"] == "Alice"
        assert row["manager_guid"] == "g1"
        assert row["team_name"] == "Team A"
        assert row["franchise_id"] == "f1"

    def test_trade_pick_uses_same_per_party_shape(self):
        df = self._make_df(
            [
                {
                    "transaction_id": "t2",
                    "transaction_type": "trade_pick",
                    "player": "2026 2nd (from Alice)",
                    "source_manager": "Alice",
                    "source_manager_guid": "g1",
                    "source_team_name": "Team A",
                    "source_franchise_id": "f1",
                    "destination_manager": "Bob",
                    "destination_manager_guid": "g2",
                    "destination_team_name": "Team B",
                    "destination_franchise_id": "f2",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        assert len(result) == 2
        assert {
            tuple(row)
            for row in result[["manager", "source_manager", "trade_direction"]].itertuples(index=False, name=None)
        } == {
            ("Alice", "Bob", "sent"),
            ("Bob", "Alice", "received"),
        }

    def test_three_party_trade_keeps_two_rows_per_asset(self):
        df = self._make_df(
            [
                {
                    "transaction_id": "t3",
                    "transaction_type": "trade",
                    "player": "PlayerX",
                    "source_manager": "Alice",
                    "source_manager_guid": "g1",
                    "source_team_name": "Team A",
                    "source_franchise_id": "f1",
                    "destination_manager": "Bob",
                    "destination_manager_guid": "g2",
                    "destination_team_name": "Team B",
                    "destination_franchise_id": "f2",
                },
                {
                    "transaction_id": "t3",
                    "transaction_type": "trade",
                    "player": "PlayerY",
                    "source_manager": "Bob",
                    "source_manager_guid": "g2",
                    "source_team_name": "Team B",
                    "source_franchise_id": "f2",
                    "destination_manager": "Carol",
                    "destination_manager_guid": "g3",
                    "destination_team_name": "Team C",
                    "destination_franchise_id": "f3",
                },
                {
                    "transaction_id": "t3",
                    "transaction_type": "trade",
                    "player": "PlayerZ",
                    "source_manager": "Carol",
                    "source_manager_guid": "g3",
                    "source_team_name": "Team C",
                    "source_franchise_id": "f3",
                    "destination_manager": "Alice",
                    "destination_manager_guid": "g1",
                    "destination_team_name": "Team A",
                    "destination_franchise_id": "f1",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        assert len(result) == 6
        assert set(result["manager"]) == {"Alice", "Bob", "Carol"}

    def test_orphan_rows_with_null_manager_are_filtered(self):
        """If source_manager is null, the sent-perspective row would have manager=null.
        These orphans must be dropped."""
        df = self._make_df(
            [
                {
                    "transaction_id": "t1",
                    "transaction_type": "trade",
                    "player": "PlayerX",
                    "source_manager": None,  # missing source
                    "source_manager_guid": None,
                    "source_team_name": None,
                    "source_franchise_id": None,
                    "destination_manager": "Bob",
                    "destination_manager_guid": "g2",
                    "destination_team_name": "Team B",
                    "destination_franchise_id": "f2",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        # Only the received row survives (manager=Bob); the sent row
        # (manager=null) is dropped.
        assert len(result) == 1
        assert result.iloc[0]["manager"] == "Bob"
        assert result.iloc[0]["trade_direction"] == "received"

    def test_self_referencing_rows_are_filtered(self):
        """A row where source_manager == manager (self-trade) is nonsensical."""
        df = self._make_df(
            [
                {
                    "transaction_id": "t1",
                    "transaction_type": "trade",
                    "player": "PlayerX",
                    "source_manager": "Alice",
                    "source_manager_guid": "g1",
                    "source_team_name": "Team A",
                    "source_franchise_id": "f1",
                    "destination_manager": "Alice",  # same as source!
                    "destination_manager_guid": "g1",
                    "destination_team_name": "Team A",
                    "destination_franchise_id": "f1",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        # Both perspectives would produce source_manager == manager → both dropped.
        assert len(result) == 0

    def test_four_party_trade_produces_clean_rows(self):
        """4-party trade: A→B, B→C, C→D, D→A — each asset gets 2 rows."""
        df = self._make_df(
            [
                {
                    "transaction_id": "t4",
                    "transaction_type": "trade",
                    "player": "PlayerA",
                    "source_manager": "Alice",
                    "destination_manager": "Bob",
                    "source_manager_guid": "g1",
                    "destination_manager_guid": "g2",
                    "source_team_name": "A",
                    "destination_team_name": "B",
                    "source_franchise_id": "f1",
                    "destination_franchise_id": "f2",
                },
                {
                    "transaction_id": "t4",
                    "transaction_type": "trade",
                    "player": "PlayerB",
                    "source_manager": "Bob",
                    "destination_manager": "Carol",
                    "source_manager_guid": "g2",
                    "destination_manager_guid": "g3",
                    "source_team_name": "B",
                    "destination_team_name": "C",
                    "source_franchise_id": "f2",
                    "destination_franchise_id": "f3",
                },
                {
                    "transaction_id": "t4",
                    "transaction_type": "trade",
                    "player": "PlayerC",
                    "source_manager": "Carol",
                    "destination_manager": "Dave",
                    "source_manager_guid": "g3",
                    "destination_manager_guid": "g4",
                    "source_team_name": "C",
                    "destination_team_name": "D",
                    "source_franchise_id": "f3",
                    "destination_franchise_id": "f4",
                },
                {
                    "transaction_id": "t4",
                    "transaction_type": "trade_pick",
                    "player": "2027 1st (from Dave)",
                    "source_manager": "Dave",
                    "destination_manager": "Alice",
                    "source_manager_guid": "g4",
                    "destination_manager_guid": "g1",
                    "source_team_name": "D",
                    "destination_team_name": "A",
                    "source_franchise_id": "f4",
                    "destination_franchise_id": "f1",
                },
            ]
        )

        result = duplicate_trade_rows(df)

        # 4 assets × 2 perspectives = 8 rows
        assert len(result) == 8
        assert set(result["manager"]) == {"Alice", "Bob", "Carol", "Dave"}
        # Each manager appears exactly 2 times (received 1 + sent 1)
        assert result.groupby("manager").size().tolist() == [2, 2, 2, 2]
        # Every row has trade_direction
        assert set(result["trade_direction"]) == {"received", "sent"}
        # No self-referencing rows
        for _, row in result.iterrows():
            assert row["manager"] != row["source_manager"]

    def test_empty_dataframe_returns_empty_copy(self):
        df = pd.DataFrame(columns=["transaction_id", "transaction_type", "player"])

        result = duplicate_trade_rows(df)

        assert result.empty
        assert list(result.columns) == list(df.columns)
