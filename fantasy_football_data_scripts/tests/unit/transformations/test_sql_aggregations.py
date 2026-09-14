"""Tests for sql_aggregations.py — Bug #1.7 franchise_id no-fallback residuals."""

import sys
from pathlib import Path

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def _matchup_df_with_franchise_id() -> pd.DataFrame:
    """Two managers, one regular-season week, with franchise_id populated."""
    return pd.DataFrame(
        [
            {
                "manager": "Alice",
                "franchise_id": "fid_alice",
                "year": 2024,
                "week": 1,
                "p_champ": 50.0,
                "team_points": 100.0,
                "opponent_points": 90.0,
            },
            {
                "manager": "Bob",
                "franchise_id": "fid_bob",
                "year": 2024,
                "week": 1,
                "p_champ": 50.0,
                "team_points": 90.0,
                "opponent_points": 100.0,
            },
            {
                "manager": "Alice",
                "franchise_id": "fid_alice",
                "year": 2024,
                "week": 2,
                "p_champ": 60.0,
                "team_points": 110.0,
                "opponent_points": 80.0,
            },
            {
                "manager": "Bob",
                "franchise_id": "fid_bob",
                "year": 2024,
                "week": 2,
                "p_champ": 40.0,
                "team_points": 80.0,
                "opponent_points": 110.0,
            },
        ]
    )


def test_get_weekly_odds_delta_fast_returns_franchise_id_slow_path():
    """Bug #1.7: producer must include franchise_id in output so consumers
    can merge on stable identity (else ['manager', ...] fallback fires every
    time and drifts on disambiguated managers)."""
    from multi_league.transformations.matchup.modules.sql_aggregations import (
        get_weekly_odds_delta_fast,
    )

    matchup_df = _matchup_df_with_franchise_id()  # no p_champ_change → slow path
    result = get_weekly_odds_delta_fast(matchup_df, odds_col="p_champ")

    assert "franchise_id" in result.columns
    # franchise_id should be the identity for each row (Alice → fid_alice, etc.)
    alice_rows = result[result["manager"] == "Alice"]
    assert (alice_rows["franchise_id"] == "fid_alice").all()
    bob_rows = result[result["manager"] == "Bob"]
    assert (bob_rows["franchise_id"] == "fid_bob").all()


def test_get_weekly_odds_delta_fast_returns_franchise_id_fast_path():
    """Bug #1.7: fast path (when {odds_col}_change is pre-calculated) must
    also include franchise_id in output."""
    from multi_league.transformations.matchup.modules.sql_aggregations import (
        get_weekly_odds_delta_fast,
    )

    matchup_df = _matchup_df_with_franchise_id()
    matchup_df["p_champ_change"] = [0.0, 0.0, 10.0, -10.0]
    result = get_weekly_odds_delta_fast(matchup_df, odds_col="p_champ")

    assert "franchise_id" in result.columns
    alice_rows = result[result["manager"] == "Alice"]
    assert (alice_rows["franchise_id"] == "fid_alice").all()
