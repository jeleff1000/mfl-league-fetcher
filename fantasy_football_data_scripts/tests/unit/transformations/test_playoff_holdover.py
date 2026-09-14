"""Tests for playoff value holdover functions."""

import numpy as np
import pandas as pd
import sys
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))
# Also add multi_league dir so lazy 'from transformations...' imports resolve
sys.path.insert(0, str(SCRIPTS_DIR / "multi_league"))

from multi_league.transformations.matchup.modules.playoff_holdover import (
    ensure_all_managers_have_playoff_rows,
    hold_playoff_values_for_eliminated,
)

# ---------------------------------------------------------------------------
# Shared mock for load_league_settings (called inside both functions)
# ---------------------------------------------------------------------------
MOCK_SETTINGS = {"num_playoff_teams": 4, "num_playoff_rounds": 2}


def _fake_load_league_settings(year, data_directory=None, df=None):
    return MOCK_SETTINGS


PATCH_LOAD = patch(
    "transformations.matchup.modules.playoff_bracket.load_league_settings",
    side_effect=_fake_load_league_settings,
)


# ---------------------------------------------------------------------------
# Helpers to build test DataFrames
# ---------------------------------------------------------------------------


def _base_rows():
    """Build a minimal 4-manager, 14-week regular season + 2 playoff weeks."""
    rows = []
    managers = ["Alice", "Bob", "Carol", "Dave"]
    for week in range(1, 15):  # weeks 1-14 regular season
        for mgr in managers:
            rows.append(
                {
                    "year": 2024,
                    "week": week,
                    "franchise_id": mgr,
                    "manager": mgr,
                    "team_points": 100.0,
                    "opponent_points": 95.0,
                    "opponent": "Someone",
                    "opponent_franchise_id": "Someone",
                    "win": 1,
                    "loss": 0,
                    "is_playoffs": 0,
                    "is_consolation": 0,
                    "is_bye_week": 0,
                    "matchup_key": f"{mgr}__vs__Someone__2024__{week}",
                    "cumulative_week": 2024 * 100 + week,
                    "manager_week": f"{mgr}{2024 * 100 + week}",
                }
            )
    # Playoff week 15 — only Alice & Bob play (semifinal)
    for mgr, opp, win, loss in [("Alice", "Bob", 1, 0), ("Bob", "Alice", 0, 1)]:
        rows.append(
            {
                "year": 2024,
                "week": 15,
                "franchise_id": mgr,
                "manager": mgr,
                "team_points": 110.0,
                "opponent_points": 105.0,
                "opponent": opp,
                "opponent_franchise_id": opp,
                "win": win,
                "loss": loss,
                "is_playoffs": 1,
                "is_consolation": 0,
                "is_bye_week": 0,
                "playoff_round": "semifinal",
                "matchup_key": f"{mgr}__vs__{opp}__2024__15",
                "cumulative_week": 202415,
                "manager_week": f"{mgr}202415",
            }
        )
    # Playoff week 16 — only Alice & Carol play (championship)
    for mgr, opp, win, loss in [("Alice", "Carol", 1, 0), ("Carol", "Alice", 0, 1)]:
        rows.append(
            {
                "year": 2024,
                "week": 16,
                "franchise_id": mgr,
                "manager": mgr,
                "team_points": 120.0,
                "opponent_points": 115.0,
                "opponent": opp,
                "opponent_franchise_id": opp,
                "win": win,
                "loss": loss,
                "is_playoffs": 1,
                "is_consolation": 0,
                "is_bye_week": 0,
                "playoff_round": "championship",
                "matchup_key": f"{mgr}__vs__{opp}__2024__16",
                "cumulative_week": 202416,
                "manager_week": f"{mgr}202416",
            }
        )
    # Carol also has a semifinal row (week 15)
    rows.append(
        {
            "year": 2024,
            "week": 15,
            "franchise_id": "Carol",
            "manager": "Carol",
            "team_points": 108.0,
            "opponent_points": 112.0,
            "opponent": "Dave",
            "opponent_franchise_id": "Dave",
            "win": 0,
            "loss": 1,  # Carol loses semifinal but still plays championship (hypothetical bracket)
            "is_playoffs": 1,
            "is_consolation": 0,
            "is_bye_week": 0,
            "playoff_round": "semifinal",
            "matchup_key": "Carol__vs__Dave__2024__15",
            "cumulative_week": 202415,
            "manager_week": "Carol202415",
        }
    )
    return pd.DataFrame(rows)


# ===========================================================================
# Tests for ensure_all_managers_have_playoff_rows
# ===========================================================================


class TestEnsureAllManagersHavePlayoffRows:
    @PATCH_LOAD
    def test_empty_df_returns_empty(self, _mock):
        df = pd.DataFrame(columns=["year", "week", "manager", "is_playoffs", "is_consolation"])
        result = ensure_all_managers_have_playoff_rows(df)
        assert result.empty

    @PATCH_LOAD
    def test_adds_missing_manager_rows_for_playoff_weeks(self, _mock):
        """Dave has no playoff rows — function should add placeholders for weeks 15 and 16."""
        df = _base_rows()
        # Dave has no week-15 or week-16 row at all
        assert df[(df["manager"] == "Dave") & (df["week"].isin([15, 16]))].shape[0] == 0
        # Carol already has weeks 15 (semifinal) and 16 (championship) and should stay unchanged.

        result = ensure_all_managers_have_playoff_rows(df)

        # Dave should now have rows for playoff weeks 15 and 16
        dave_playoff = result[(result["manager"] == "Dave") & (result["week"].isin([15, 16]))]
        assert len(dave_playoff) == 2
        # They should be placeholders
        assert (dave_playoff["is_bye_week"] == 1).all()
        # opponent should be None/NaN
        assert dave_playoff["opponent"].isna().all()
        # game-specific cols should be NaN
        assert dave_playoff["win"].isna().all()
        assert dave_playoff["team_points"].isna().all()

    @PATCH_LOAD
    def test_does_not_duplicate_existing_rows(self, _mock):
        """Managers who already have playoff rows should NOT get duplicates."""
        df = _base_rows()
        alice_before = df[(df["manager"] == "Alice") & (df["week"] == 15)].shape[0]

        result = ensure_all_managers_have_playoff_rows(df)

        alice_after = result[(result["manager"] == "Alice") & (result["week"] == 15)].shape[0]
        assert alice_after == alice_before

    @PATCH_LOAD
    def test_cumulative_week_set_correctly(self, _mock):
        df = _base_rows()
        result = ensure_all_managers_have_playoff_rows(df)
        dave_w15 = result[(result["manager"] == "Dave") & (result["week"] == 15)]
        assert len(dave_w15) == 1
        assert dave_w15.iloc[0]["cumulative_week"] == 202415


# ===========================================================================
# Tests for hold_playoff_values_for_eliminated
# ===========================================================================


def _holdover_df():
    """Build a DF with playoff odds columns, suitable for hold_playoff_values testing."""
    df = _base_rows()
    # Add probability columns
    for col in ["p_playoffs", "p_bye", "p_semis", "p_final", "p_champ", "power_rating", "avg_seed"]:
        df[col] = 50.0  # arbitrary starting value
    return df


class TestHoldPlayoffValuesForEliminated:
    @PATCH_LOAD
    def test_empty_df_returns_empty(self, _mock):
        result = hold_playoff_values_for_eliminated(pd.DataFrame())
        assert result.empty

    @PATCH_LOAD
    def test_none_returns_empty(self, _mock):
        result = hold_playoff_values_for_eliminated(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @PATCH_LOAD
    def test_missing_columns_returns_unchanged(self, _mock):
        df = pd.DataFrame({"a": [1, 2]})
        result = hold_playoff_values_for_eliminated(df)
        assert result.equals(df)

    @PATCH_LOAD
    def test_eliminated_teams_get_p_champ_zero(self, _mock):
        """Bob lost in the semifinal — p_champ should be 0 for all playoff weeks."""
        df = _holdover_df()
        # First add placeholder rows so everyone has playoff week rows
        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        bob_playoffs = result[(result["manager"] == "Bob") & (result["week"].isin([15, 16]))]
        assert (bob_playoffs["p_champ"] == 0.0).all()

    @PATCH_LOAD
    def test_non_playoff_teams_all_odds_zero(self, _mock):
        """Dave didn't make playoffs — all odds should be 0 during postseason."""
        df = _holdover_df()
        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        dave_playoffs = result[(result["manager"] == "Dave") & (result["week"].isin([15, 16]))]
        if not dave_playoffs.empty:
            for col in ["p_playoffs", "p_bye", "p_semis", "p_final", "p_champ"]:
                assert (dave_playoffs[col] == 0.0).all(), f"Dave's {col} should be 0"

    @PATCH_LOAD
    def test_champion_gets_100_p_champ(self, _mock):
        """Alice won the championship — p_champ should be 100 on championship week."""
        df = _holdover_df()
        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        alice_champ_week = result[
            (result["manager"] == "Alice") & (result["week"] == 16) & (result["is_playoffs"] == 1)
        ]
        assert len(alice_champ_week) >= 1
        assert alice_champ_week.iloc[0]["p_champ"] == 100.0

    @PATCH_LOAD
    def test_playoff_managers_get_100_p_playoffs(self, _mock):
        """All playoff managers should have p_playoffs=100 during postseason."""
        df = _holdover_df()
        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        for mgr in ["Alice", "Bob", "Carol"]:
            mgr_playoffs = result[
                (result["manager"] == mgr) & (result["week"].isin([15, 16])) & (result["is_playoffs"] == 1)
            ]
            if not mgr_playoffs.empty:
                assert (mgr_playoffs["p_playoffs"] == 100.0).all(), f"{mgr} p_playoffs should be 100"

    @PATCH_LOAD
    def test_hierarchy_preserved(self, _mock):
        """p_champ <= p_final <= p_semis <= p_playoffs for all rows."""
        df = _holdover_df()
        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        # Check hierarchy on rows that have all columns populated
        for _, row in result.iterrows():
            if all(pd.notna(row.get(c)) for c in ["p_champ", "p_final", "p_semis", "p_playoffs"]):
                assert (
                    row["p_champ"] <= row["p_final"] + 0.01
                ), f"p_champ ({row['p_champ']}) > p_final ({row['p_final']}) for {row['manager']} week {row['week']}"
                assert (
                    row["p_final"] <= row["p_semis"] + 0.01
                ), f"p_final ({row['p_final']}) > p_semis ({row['p_semis']}) for {row['manager']} week {row['week']}"
                assert (
                    row["p_semis"] <= row["p_playoffs"] + 0.01
                ), f"p_semis ({row['p_semis']}) > p_playoffs ({row['p_playoffs']}) for {row['manager']} week {row['week']}"

    @PATCH_LOAD
    def test_no_playoff_champion_odds_are_not_clipped_by_bracket_hierarchy(self, _mock):
        """No-playoff seasons use p_champ as seed-1 odds while bracket odds stay at 0."""
        df = pd.DataFrame(
            [
                {
                    "year": 2018,
                    "week": 12,
                    "franchise_id": "fid_a",
                    "manager": "Patrick",
                    "is_playoffs": 0,
                    "is_consolation": 0,
                    "opponent": "Mark",
                    "p_playoffs": 0.0,
                    "p_bye": 0.0,
                    "p_semis": 0.0,
                    "p_final": 0.0,
                    "p_champ": 100.0,
                },
                {
                    "year": 2018,
                    "week": 12,
                    "franchise_id": "fid_b",
                    "manager": "Mark",
                    "is_playoffs": 0,
                    "is_consolation": 0,
                    "opponent": "Patrick",
                    "p_playoffs": 0.0,
                    "p_bye": 0.0,
                    "p_semis": 0.0,
                    "p_final": 0.0,
                    "p_champ": 0.0,
                },
            ]
        )

        result = hold_playoff_values_for_eliminated(df)

        assert result.loc[result["manager"] == "Patrick", "p_champ"].iloc[0] == 100.0
        assert result["p_playoffs"].sum() == 0.0

    @PATCH_LOAD
    def test_postseason_rows_keep_final_regular_record_distribution(self, _mock):
        """Postseason rows should freeze the actual final regular-season record distribution."""
        df = _holdover_df()

        # Seed explicit final-regular-season record distributions on week 14.
        final_reg = {
            "Alice": {
                "exp_final_wins": 10.0,
                "x10_win": 100.0,
                "x9_win": 0.0,
                "x8_win": 0.0,
                "x4_win": 0.0,
                "x14_win": 0.0,
            },
            "Bob": {
                "exp_final_wins": 9.0,
                "x10_win": 0.0,
                "x9_win": 100.0,
                "x8_win": 0.0,
                "x4_win": 0.0,
                "x14_win": 0.0,
            },
            "Carol": {
                "exp_final_wins": 8.0,
                "x10_win": 0.0,
                "x9_win": 0.0,
                "x8_win": 100.0,
                "x4_win": 0.0,
                "x14_win": 0.0,
            },
            "Dave": {
                "exp_final_wins": 4.0,
                "x10_win": 0.0,
                "x9_win": 0.0,
                "x8_win": 0.0,
                "x4_win": 100.0,
                "x14_win": 0.0,
            },
        }
        for col in ["exp_final_wins", "x10_win", "x9_win", "x8_win", "x4_win", "x14_win"]:
            df[col] = np.nan

        for manager, values in final_reg.items():
            mask = (df["manager"] == manager) & (df["week"] == 14) & (df["is_playoffs"] == 0)
            for col, value in values.items():
                df.loc[mask, col] = value

        # Simulate the broken live-state shape on playoff rows to make sure holdover fixes it.
        playoff_mask = (df["week"].isin([15, 16])) & (df["is_playoffs"] == 1)
        df.loc[playoff_mask, "exp_final_wins"] = 14.0
        df.loc[playoff_mask, "x10_win"] = 0.0
        df.loc[playoff_mask, "x9_win"] = 0.0
        df.loc[playoff_mask, "x8_win"] = 0.0
        df.loc[playoff_mask, "x4_win"] = 0.0
        df.loc[playoff_mask, "x14_win"] = 100.0

        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        for manager, expected in final_reg.items():
            postseason_rows = result[(result["manager"] == manager) & (result["week"].isin([15, 16]))]
            if postseason_rows.empty:
                continue
            for _, row in postseason_rows.iterrows():
                assert row["exp_final_wins"] == expected["exp_final_wins"]
                assert row["x14_win"] == expected["x14_win"]
                assert row["x10_win"] == expected["x10_win"]
                assert row["x9_win"] == expected["x9_win"]
                assert row["x8_win"] == expected["x8_win"]
                assert row["x4_win"] == expected["x4_win"]

    @PATCH_LOAD
    def test_consolation_rows_clear_regular_season_x_win_buckets(self, _mock):
        """Consolation rows should not keep frozen x-win distributions after postseason placement games."""
        df = _holdover_df()
        df["x8_win"] = 25.0
        df["x9_win"] = 50.0
        df["x10_win"] = 25.0

        # Convert Carol into a real consolation row and add the missing Dave-side
        # row so we do not accidentally assert against a phantom-bye placeholder.
        consolation_mask = (df["manager"] == "Carol") & (df["week"] == 15)
        df.loc[consolation_mask, "is_playoffs"] = 0
        df.loc[consolation_mask, "is_consolation"] = 1
        df.loc[consolation_mask, "opponent"] = "Dave"
        df.loc[consolation_mask, "opponent_franchise_id"] = "Dave"

        dave_row = df.loc[consolation_mask].iloc[0].copy()
        dave_row["franchise_id"] = "Dave"
        dave_row["manager"] = "Dave"
        dave_row["opponent"] = "Carol"
        dave_row["opponent_franchise_id"] = "Carol"
        dave_row["team_points"] = 104.0
        dave_row["opponent_points"] = 108.0
        dave_row["win"] = 0
        dave_row["loss"] = 1
        df = pd.concat([df, pd.DataFrame([dave_row])], ignore_index=True)

        df = ensure_all_managers_have_playoff_rows(df)
        result = hold_playoff_values_for_eliminated(df)

        consolation_rows = result[(result["manager"].isin(["Carol", "Dave"])) & (result["week"] == 15)]
        assert not consolation_rows.empty
        assert consolation_rows["is_consolation"].eq(1).all()
        assert consolation_rows["x8_win"].isna().all()
        assert consolation_rows["x9_win"].isna().all()
        assert consolation_rows["x10_win"].isna().all()

        alice_playoff = result[(result["manager"] == "Alice") & (result["week"] == 15) & (result["is_playoffs"] == 1)]
        assert len(alice_playoff) == 1
        assert alice_playoff.iloc[0]["x8_win"] == 25.0
        assert alice_playoff.iloc[0]["x9_win"] == 50.0
        assert alice_playoff.iloc[0]["x10_win"] == 25.0
