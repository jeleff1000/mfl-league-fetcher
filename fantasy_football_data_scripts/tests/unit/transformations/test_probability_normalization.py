"""Tests for probability normalization and hierarchy enforcement."""

import pytest
import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.transformations.matchup.modules.probability_normalization import (
    enforce_hierarchy,
    compress_and_normalize_probabilities,
)


@pytest.fixture
def raw_odds_df():
    """Raw simulation output for 4 teams, 6-team bracket, 2 byes."""
    return pd.DataFrame(
        {
            "P_Playoffs": [95.0, 80.0, 60.0, 40.0],
            "P_Bye": [60.0, 30.0, 5.0, 2.0],
            "P_Semis": [90.0, 70.0, 50.0, 30.0],
            "P_Final": [60.0, 40.0, 25.0, 15.0],
            "P_Champ": [35.0, 25.0, 20.0, 20.0],
        },
        index=["Alice", "Bob", "Carol", "Dave"],
    )


class TestEnforceHierarchy:
    def test_champ_le_final(self):
        odds = pd.DataFrame(
            {
                "P_Playoffs": [100.0],
                "P_Semis": [80.0],
                "P_Final": [30.0],
                "P_Champ": [50.0],  # violation
            }
        )
        result = enforce_hierarchy(odds)
        assert result["P_Champ"].iloc[0] <= result["P_Final"].iloc[0]

    def test_final_le_semis(self):
        odds = pd.DataFrame(
            {
                "P_Playoffs": [100.0],
                "P_Semis": [40.0],
                "P_Final": [60.0],
                "P_Champ": [20.0],  # violation
            }
        )
        result = enforce_hierarchy(odds)
        assert result["P_Final"].iloc[0] <= result["P_Semis"].iloc[0]

    def test_semis_le_playoffs(self):
        odds = pd.DataFrame(
            {
                "P_Playoffs": [50.0],
                "P_Semis": [70.0],
                "P_Final": [30.0],
                "P_Champ": [10.0],
            }
        )
        result = enforce_hierarchy(odds)
        assert result["P_Semis"].iloc[0] <= result["P_Playoffs"].iloc[0]

    def test_bye_le_semis(self):
        odds = pd.DataFrame(
            {
                "P_Playoffs": [100.0],
                "P_Bye": [80.0],
                "P_Semis": [60.0],
                "P_Final": [30.0],
                "P_Champ": [10.0],
            }
        )
        result = enforce_hierarchy(odds)
        assert result["P_Bye"].iloc[0] <= result["P_Semis"].iloc[0]

    def test_no_violations_unchanged(self, raw_odds_df):
        result = enforce_hierarchy(raw_odds_df)
        pd.testing.assert_frame_equal(
            result[["P_Champ", "P_Final", "P_Semis", "P_Playoffs"]],
            raw_odds_df[["P_Champ", "P_Final", "P_Semis", "P_Playoffs"]],
        )


class TestCompressAndNormalize:
    def test_week5_no_compression(self, raw_odds_df):
        result = compress_and_normalize_probabilities(
            raw_odds_df.copy(), week=5, num_teams=4, num_playoff_teams=4, bye_teams=2
        )
        assert result["P_Champ"].sum() == pytest.approx(100.0, abs=0.5)

    def test_week1_compresses(self):
        """Week 1 compression should pull extremes toward the base rate."""
        # Use 10 teams, 6 playoff spots -> base rate = 60%
        # Teams above 60% should be compressed down, teams below should be pushed up
        odds = pd.DataFrame(
            {
                "P_Playoffs": [95.0, 80.0, 60.0, 40.0, 30.0, 20.0, 15.0, 10.0, 8.0, 5.0],
                "P_Bye": [60.0, 30.0, 5.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "P_Semis": [90.0, 70.0, 50.0, 30.0, 20.0, 15.0, 10.0, 5.0, 3.0, 2.0],
                "P_Final": [60.0, 40.0, 25.0, 15.0, 10.0, 8.0, 5.0, 3.0, 2.0, 1.0],
                "P_Champ": [35.0, 25.0, 20.0, 20.0, 10.0, 5.0, 3.0, 2.0, 1.0, 0.5],
            },
            index=[f"T{i}" for i in range(10)],
        )
        result = compress_and_normalize_probabilities(
            odds.copy(), week=1, num_teams=10, num_playoff_teams=6, bye_teams=2
        )
        assert result.loc["T0", "P_Playoffs"] < 95.0
        assert result.loc["T9", "P_Playoffs"] > 5.0

    def test_hierarchy_preserved(self, raw_odds_df):
        for week in [1, 2, 3, 5, 10]:
            result = compress_and_normalize_probabilities(
                raw_odds_df.copy(), week=week, num_teams=4, num_playoff_teams=4, bye_teams=2
            )
            for _, row in result.iterrows():
                assert row["P_Champ"] <= row["P_Final"] + 0.01
                assert row["P_Final"] <= row["P_Semis"] + 0.01
                assert row["P_Semis"] <= row["P_Playoffs"] + 0.01

    def test_champ_sums_to_100(self, raw_odds_df):
        result = compress_and_normalize_probabilities(
            raw_odds_df.copy(), week=3, num_teams=4, num_playoff_teams=4, bye_teams=2
        )
        assert result["P_Champ"].sum() == pytest.approx(100.0, abs=1.0)

    def test_no_playoff_champ_is_regular_season_top_seed(self):
        odds = pd.DataFrame(
            {
                "P_Playoffs": [0.0, 0.0, 0.0, 0.0],
                "P_Bye": [0.0, 0.0, 0.0, 0.0],
                "P_Semis": [0.0, 0.0, 0.0, 0.0],
                "P_Final": [0.0, 0.0, 0.0, 0.0],
                "P_Champ": [80.0, 15.0, 5.0, 0.0],
            },
            index=["A", "B", "C", "D"],
        )

        result = compress_and_normalize_probabilities(
            odds.copy(), week=5, num_teams=4, num_playoff_teams=0, bye_teams=0
        )

        assert result["P_Champ"].sum() == pytest.approx(100.0, abs=0.01)
        assert result.loc["A", "P_Champ"] > result.loc["B", "P_Champ"]
        assert result["P_Playoffs"].sum() == 0
        assert result["P_Final"].sum() == 0

    def test_final_sums_to_200(self, raw_odds_df):
        result = compress_and_normalize_probabilities(
            raw_odds_df.copy(), week=3, num_teams=4, num_playoff_teams=4, bye_teams=2
        )
        assert result["P_Final"].sum() == pytest.approx(200.0, abs=1.0)
