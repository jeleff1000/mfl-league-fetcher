"""
Shared test fixtures for fantasy football data scripts.

These fixtures provide reusable test data for unit and integration tests.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path


# =============================================================================
# Path Configuration
# =============================================================================


@pytest.fixture
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def multi_league_root(project_root):
    """Return the multi_league module root."""
    return project_root / "multi_league"


# =============================================================================
# Sample Player Data
# =============================================================================


@pytest.fixture
def sample_player_df():
    """
    Sample player DataFrame with standard columns.

    Contains 3 weeks of data for 3 players across 2 managers.
    """
    return pd.DataFrame(
        {
            "yahoo_player_id": [1001, 1001, 1001, 1002, 1002, 1002, 1003, 1003, 1003],
            "player": [
                "Patrick Mahomes",
                "Patrick Mahomes",
                "Patrick Mahomes",
                "Travis Kelce",
                "Travis Kelce",
                "Travis Kelce",
                "Tyreek Hill",
                "Tyreek Hill",
                "Tyreek Hill",
            ],
            "position": ["QB", "QB", "QB", "TE", "TE", "TE", "WR", "WR", "WR"],
            "year": [2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024],
            "week": [1, 2, 3, 1, 2, 3, 1, 2, 3],
            "manager": ["Team A", "Team A", "Team A", "Team A", "Team A", "Team A", "Team B", "Team B", "Team B"],
            "fantasy_points": [25.5, 30.2, 18.7, 15.3, 22.1, 8.5, 28.4, 12.6, 21.3],
            "fantasy_position": ["QB", "QB", "QB", "TE", "TE", "BN", "WR", "WR", "WR"],
            "league_id": ["123", "123", "123", "123", "123", "123", "123", "123", "123"],
        }
    )


@pytest.fixture
def sample_player_df_mixed_types():
    """
    Player DataFrame with mixed/inconsistent types.

    Used to test type normalization functions.
    """
    return pd.DataFrame(
        {
            "yahoo_player_id": ["1001", "1002", "1003", None],  # String IDs
            "NFL_player_id": [2001.0, 2002.0, np.nan, 2004.0],  # Float IDs
            "year": ["2024", "2024", "2024", "2024"],  # String years
            "week": [1.0, 2.0, 3.0, 4.0],  # Float weeks
            "league_id": [123, 123, 123, 123],  # Int league_id (should be string)
            "player": ["Player A", "Player B", "Player C", "Player D"],
            "fantasy_points": [25.5, 30.2, 18.7, 15.3],
        }
    )


# =============================================================================
# Sample Matchup Data
# =============================================================================


@pytest.fixture
def sample_matchup_df():
    """
    Sample matchup DataFrame with standard columns.

    Contains 3 weeks of matchup data for 4 managers.
    """
    return pd.DataFrame(
        {
            "year": [2024] * 12,
            "week": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
            "manager": ["Team A", "Team B", "Team C", "Team D"] * 3,
            "team_points": [120.5, 115.2, 98.7, 105.3, 110.2, 125.8, 112.4, 88.9, 130.1, 95.6, 118.7, 122.4],
            "opponent": ["Team B", "Team A", "Team D", "Team C"] * 3,
            "league_id": ["123"] * 12,
        }
    )


# =============================================================================
# Sample Replacement Level Data
# =============================================================================


@pytest.fixture
def sample_replacement_df():
    """
    Sample weekly replacement levels for LAMAR calculation.

    Contains replacement PPG by position for each week.
    """
    positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
    weeks = [1, 2, 3]

    data = []
    for week in weeks:
        for pos in positions:
            # Simulate varying replacement levels by position
            base_ppg = {"QB": 12.0, "RB": 8.0, "WR": 7.5, "TE": 5.0, "K": 6.0, "DEF": 5.5}
            data.append(
                {
                    "year": 2024,
                    "week": week,
                    "position": pos,
                    "replacement_ppg": base_ppg[pos] + np.random.uniform(-1, 1),
                }
            )

    return pd.DataFrame(data)


# =============================================================================
# Sample Draft Data
# =============================================================================


@pytest.fixture
def sample_draft_df():
    """
    Sample draft DataFrame.

    Contains draft picks for 2 managers.
    """
    return pd.DataFrame(
        {
            "yahoo_player_id": [1001, 1002, 1003, 1004, 1005, 1006],
            "player": ["Patrick Mahomes", "Travis Kelce", "Tyreek Hill", "Josh Allen", "Stefon Diggs", "Derrick Henry"],
            "position": ["QB", "TE", "WR", "QB", "WR", "RB"],
            "manager": ["Team A", "Team A", "Team B", "Team B", "Team A", "Team B"],
            "year": [2024] * 6,
            "pick": [1, 12, 13, 24, 25, 36],
            "round": [1, 1, 2, 2, 3, 3],
            "cost": [None, None, None, None, None, None],  # Snake draft, no cost
            "is_keeper": [False, True, False, False, True, False],
            "league_id": ["123"] * 6,
        }
    )


# =============================================================================
# Empty DataFrames for Edge Case Testing
# =============================================================================


@pytest.fixture
def empty_player_df():
    """Empty player DataFrame with correct schema."""
    return pd.DataFrame(
        {
            "yahoo_player_id": pd.Series(dtype="Int64"),
            "player": pd.Series(dtype="string"),
            "position": pd.Series(dtype="string"),
            "year": pd.Series(dtype="Int64"),
            "week": pd.Series(dtype="Int64"),
            "manager": pd.Series(dtype="string"),
            "fantasy_points": pd.Series(dtype="float64"),
            "league_id": pd.Series(dtype="string"),
        }
    )


@pytest.fixture
def empty_matchup_df():
    """Empty matchup DataFrame with correct schema."""
    return pd.DataFrame(
        {
            "year": pd.Series(dtype="Int64"),
            "week": pd.Series(dtype="Int64"),
            "manager": pd.Series(dtype="string"),
            "team_points": pd.Series(dtype="float64"),
            "opponent": pd.Series(dtype="string"),
            "league_id": pd.Series(dtype="string"),
        }
    )


# =============================================================================
# Utility Fixtures
# =============================================================================


@pytest.fixture
def temp_parquet_path(tmp_path):
    """Provide a temporary path for parquet file testing."""
    return tmp_path / "test_data.parquet"


@pytest.fixture
def sample_league_context_dict():
    """
    Sample LeagueContext configuration dictionary.

    Used for testing context loading.
    """
    return {
        "league_id": "449.l.198278",
        "league_name": "Test League",
        "data_directory": "/tmp/test_data",
        "oauth_file_path": "/tmp/oauth.json",
        "start_year": 2020,
        "end_year": 2024,
    }
