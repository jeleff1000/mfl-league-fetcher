"""Unit tests for the pts_sack_taken calculation.

Regression test for the L1.b audit M2 finding: calculator at line 391 read
`safe_col(result, "sacks")` but super_table col is `sacks_suffered`. Bug
caused 13,441 rows of pts_sack_taken drift across 1999-2025.
"""

import pandas as pd

from multi_league.data_fetchers.fantasy_points_calculator import calculate_all_fantasy_points


def test_pts_sack_taken_uses_sacks_suffered():
    """A QB with sacks_suffered = 4 should get pts_sack_taken = -4."""
    df = pd.DataFrame(
        [
            {
                "position": "QB",
                "year": 2020,
                "week": 1,
                "nfl_team": "TB",
                "sacks_suffered": 4,
                # Other base stats — must be present so calculator doesn't error
                "passing_yards": 250,
                "passing_tds": 2,
                "passing_interceptions": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_sack_taken"].iloc[0] == -4.0


def test_pts_sack_taken_zero_when_no_sacks_suffered():
    """QB with no sacks gets 0."""
    df = pd.DataFrame(
        [
            {
                "position": "QB",
                "year": 2020,
                "week": 1,
                "nfl_team": "KC",
                "sacks_suffered": 0,
                "passing_yards": 350,
                "passing_tds": 3,
                "passing_interceptions": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_sack_taken"].iloc[0] == 0.0


def test_pts_sack_taken_does_not_read_phantom_sacks_col():
    """Pre-fix regression: the calculator used to read 'sacks' (which does not
    exist in super_table) and silently zero pts_sack_taken for every row. This
    verifies the fix reads sacks_suffered, not sacks."""
    # Row has 'sacks' but no 'sacks_suffered' — must produce 0 (not -5)
    df = pd.DataFrame(
        [
            {
                "position": "QB",
                "year": 2020,
                "week": 1,
                "nfl_team": "TB",
                "sacks": 5,  # phantom col — must NOT be read
                # sacks_suffered is missing
                "passing_yards": 250,
                "passing_tds": 2,
                "passing_interceptions": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    # Without sacks_suffered, pts_sack_taken should be 0 (not -5 from phantom col)
    assert out["pts_sack_taken"].iloc[0] == 0.0
