from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from multi_league.data_fetchers.yahoo.yahoo_schedules import (
    _derive_schedule_df_from_matchup_df,
    build_and_save_for_year,
    coerce_dtypes,
    fetch_schedule_for_year,
)


class TestFetchScheduleForYear:
    @patch("multi_league.data_fetchers.yahoo.yahoo_schedules.parse_week_schedule")
    def test_prefers_local_matchup_rows(self, mock_parse_week_schedule):
        local_db = MagicMock()
        local_db.read_table.return_value = pd.DataFrame(
            {
                "year": [2021, 2021],
                "week": [1, 1],
                "manager": ["Team A", "Team B"],
                "manager_guid": ["ga", "gb"],
                "team_name": ["A", "B"],
                "opponent": ["Team B", "Team A"],
                "team_points": [100.0, 90.0],
                "opponent_points": [90.0, 100.0],
                "win": [1, 0],
                "loss": [0, 1],
                "is_playoffs": [0, 0],
                "is_consolation": [0, 0],
            }
        )
        ctx = SimpleNamespace(manager_name_overrides={}, get_oauth_session=MagicMock())

        df = fetch_schedule_for_year(
            ctx=ctx,
            year=2021,
            manager_overrides={},
            local_db=local_db,
        )

        assert not df.empty
        assert set(["manager", "opponent", "week", "year"]).issubset(df.columns)
        mock_parse_week_schedule.assert_not_called()

    @patch("multi_league.data_fetchers.yahoo.yahoo_schedules.build_and_save_for_year")
    def test_fetches_only_missing_weeks_after_local_derivation(self, mock_build_and_save):
        local_db = MagicMock()

        def read_table(table_name, year=None):
            if table_name == "matchup":
                return pd.DataFrame(
                    {
                        "year": [2021, 2021],
                        "week": [1, 1],
                        "manager": ["Team A", "Team B"],
                        "manager_guid": ["ga", "gb"],
                        "team_name": ["A", "B"],
                        "opponent": ["Team B", "Team A"],
                        "team_points": [100.0, 90.0],
                        "opponent_points": [90.0, 100.0],
                        "win": [1, 0],
                        "loss": [0, 1],
                        "is_playoffs": [0, 0],
                        "is_consolation": [0, 0],
                    }
                )
            if table_name == "league_settings":
                return pd.DataFrame({"year": [2021], "start_week": [1], "end_week": [3]})
            return pd.DataFrame()

        local_db.read_table.side_effect = read_table
        mock_build_and_save.return_value = (
            None,
            None,
            4,
            pd.DataFrame(
                {
                    "is_playoffs": [0, 0, 0, 0],
                    "is_consolation": [0, 0, 0, 0],
                    "manager": ["Team A", "Team B", "Team A", "Team B"],
                    "manager_guid": ["ga", "gb", "ga", "gb"],
                    "team_name": ["A", "B", "A", "B"],
                    "cumulative_week": [202102, 202102, 202103, 202103],
                    "manager_week": ["TeamA202102", "TeamB202102", "TeamA202103", "TeamB202103"],
                    "manager_year": ["TeamA2021", "TeamB2021", "TeamA2021", "TeamB2021"],
                    "opponent": ["Team B", "Team A", "Team B", "Team A"],
                    "opponent_week": [2, 2, 3, 3],
                    "opponent_year": [2021, 2021, 2021, 2021],
                    "week": [2, 2, 3, 3],
                    "year": [2021, 2021, 2021, 2021],
                    "team_points": [0.0, 0.0, 0.0, 0.0],
                    "opponent_points": [0.0, 0.0, 0.0, 0.0],
                    "win": [0, 0, 0, 0],
                    "loss": [0, 0, 0, 0],
                }
            ),
        )
        ctx = SimpleNamespace(manager_name_overrides={}, get_oauth_session=MagicMock())

        df = fetch_schedule_for_year(
            ctx=ctx,
            year=2021,
            manager_overrides={},
            local_db=local_db,
        )

        assert sorted(df["week"].unique().tolist()) == [1, 2, 3]
        assert mock_build_and_save.call_args.kwargs["weeks_to_fetch"] == [2, 3]

    @patch("multi_league.data_fetchers.yahoo.yahoo_schedules.parse_week_schedule", return_value=[])
    @patch("multi_league.data_fetchers.yahoo.yahoo_schedules.league_weeks", return_value=[1])
    @patch("multi_league.data_fetchers.yahoo.yahoo_schedules.get_league_for_year")
    def test_build_and_save_returns_empty_frame_when_yahoo_has_no_schedule_rows(
        self,
        mock_get_league_for_year,
        _mock_league_weeks,
        _mock_parse_week_schedule,
    ):
        mock_get_league_for_year.return_value = ("414.l.558986", object())

        _path, _parquet_path, rows, df = build_and_save_for_year(2022, ctx=SimpleNamespace(data_directory="."))

        assert rows == 0
        assert df.empty
        assert set(["manager", "opponent", "week", "year"]).issubset(df.columns)


class TestScheduleCoercion:
    def test_derived_schedule_uses_team_key_when_one_owner_has_multiple_teams(self):
        matchup = pd.DataFrame(
            {
                "year": [2026, 2026],
                "week": [1, 1],
                "manager": ["Shared Owner", "Shared Owner"],
                "manager_guid": ["same-guid", "same-guid"],
                "team_key": ["470.l.1.t.3", "470.l.1.t.9"],
                "team_name": ["Alpha", "Omega"],
                "opponent": ["Shared Owner", "Shared Owner"],
                "team_points": [101.0, 99.0],
                "opponent_points": [99.0, 101.0],
                "win": [1, 0],
                "loss": [0, 1],
            }
        )

        result = _derive_schedule_df_from_matchup_df(matchup, 2026, {})

        assert result["manager_week"].is_unique
        assert result["manager_week"].tolist() == ["470.l.1.t.3_2026_1", "470.l.1.t.9_2026_1"]
        assert result["manager_year"].tolist() == ["470.l.1.t.3_2026", "470.l.1.t.9_2026"]

    def test_coerce_dtypes_accepts_nullable_boolean_columns(self):
        df = pd.DataFrame(
            {
                "is_playoffs": pd.Series([True, False], dtype="boolean"),
                "is_consolation": pd.Series([False, None], dtype="boolean"),
                "manager": ["Team A", "Team B"],
                "manager_guid": ["ga", "gb"],
                "team_name": ["A", "B"],
                "cumulative_week": [202101, 202101],
                "manager_week": ["TeamA202101", "TeamB202101"],
                "manager_year": ["TeamA2021", "TeamB2021"],
                "opponent": ["Team B", "Team A"],
                "opponent_week": [1, 1],
                "opponent_year": [2021, 2021],
                "week": [1, 1],
                "year": [2021, 2021],
                "team_points": [100.0, 90.0],
                "opponent_points": [90.0, 100.0],
                "win": [1, 0],
                "loss": [0, 1],
            }
        )

        result = coerce_dtypes(df)

        assert result["is_playoffs"].tolist() == [1, 0]
        assert result["is_consolation"].tolist() == [0, 0]
