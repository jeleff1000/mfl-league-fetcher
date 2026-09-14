"""
Unit tests for the franchise_id no-fallback migration (2026-04-28).

Spec: docs/superpowers/specs/2026-04-28-franchise-id-no-fallback-design.md

Each test enforces a "loud failure on missing franchise_id" invariant
at one of the 8 sites identified in the spec, plus the bye-aware
filtering of _games_played_by_identity and calculate_standings_stats.
"""

import sys
from pathlib import Path

import numpy as np  # noqa: F401  -- used by tasks appending tests
import pandas as pd  # noqa: F401  -- used by tasks appending tests
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


# Task 3: _faab_owner_column no-fallback collapse
from multi_league.transformations.transaction.sql_transaction_enrichments import _faab_owner_column


def test_faab_owner_column_returns_franchise_id_when_present_in_both_sides():
    result = _faab_owner_column(
        player_cols={"franchise_id", "manager", "NFL_player_id"},
        trans_cols={"franchise_id", "manager", "NFL_player_id"},
    )
    assert result == "franchise_id"


def test_faab_owner_column_raises_when_franchise_id_missing_from_player_cols():
    with pytest.raises(KeyError, match="franchise_id is required"):
        _faab_owner_column(
            player_cols={"manager", "NFL_player_id"},
            trans_cols={"franchise_id", "manager", "NFL_player_id"},
        )


def test_faab_owner_column_raises_when_franchise_id_missing_from_trans_cols():
    with pytest.raises(KeyError, match="franchise_id is required"):
        _faab_owner_column(
            player_cols={"franchise_id", "manager", "NFL_player_id"},
            trans_cols={"manager", "NFL_player_id"},
        )


def test_faab_owner_column_raises_when_franchise_id_missing_from_both():
    with pytest.raises(KeyError, match="franchise_id is required"):
        _faab_owner_column(
            player_cols={"manager"},
            trans_cols={"manager"},
        )


# Task 5: _build_select_clauses no-fallback
from multi_league.transformations.aggregation.aggregate_fantasy_context import _build_select_clauses


def test_build_select_clauses_emits_max_franchise_id_when_column_present():
    available = {
        "franchise_id",
        "team_points",
        "opponent_points",
        "win",
        "loss",
        "is_playoffs",
        "is_championship",
        "position",
    }
    clauses = _build_select_clauses(available, granularity="season")
    assert clauses["franchise_id"] == "MAX(f.franchise_id) AS franchise_id,"


def test_build_select_clauses_raises_when_franchise_id_missing():
    available = {"team_points", "opponent_points", "win", "loss"}
    with pytest.raises(KeyError, match="franchise_id is required"):
        _build_select_clauses(available, granularity="season")


# Task 6: calculate_weekly_odds_changes no-fallback
from multi_league.transformations.matchup.modules.playoff_scenarios import calculate_weekly_odds_changes


def _make_minimal_odds_df(with_franchise_id: bool):
    rows = [
        {"year": 2024, "week": 1, "manager": "Alice", "p_playoffs": 0.5, "p_champ": 0.1, "p_bye": 0.0},
        {"year": 2024, "week": 2, "manager": "Alice", "p_playoffs": 0.6, "p_champ": 0.15, "p_bye": 0.0},
        {"year": 2024, "week": 1, "manager": "Bob", "p_playoffs": 0.5, "p_champ": 0.1, "p_bye": 0.0},
        {"year": 2024, "week": 2, "manager": "Bob", "p_playoffs": 0.4, "p_champ": 0.05, "p_bye": 0.0},
    ]
    if with_franchise_id:
        for r in rows:
            r["franchise_id"] = "fid_a" if r["manager"] == "Alice" else "fid_b"
    return pd.DataFrame(rows)


def test_calculate_weekly_odds_changes_uses_franchise_id_when_present():
    df = _make_minimal_odds_df(with_franchise_id=True)
    result = calculate_weekly_odds_changes(df)
    alice_w2 = result[(result["manager"] == "Alice") & (result["week"] == 2)].iloc[0]
    assert alice_w2["p_playoffs_change"] == pytest.approx(0.1)


def test_calculate_weekly_odds_changes_raises_when_franchise_id_missing():
    df = _make_minimal_odds_df(with_franchise_id=False)
    with pytest.raises(KeyError, match="franchise_id is required"):
        calculate_weekly_odds_changes(df)


# Task 7a: calc_playoff_odds_for_week no-fallback at line 431
from multi_league.core.identity import get_manager_col


def test_playoff_odds_future_canon_without_franchise_id_raises():
    """Site 7a — future_canon missing franchise_id must raise via get_manager_col.

    The fix at line 431 swaps a ternary fallback for get_manager_col(future_canon),
    relying on get_manager_col's KeyError contract.
    """
    future_canon = pd.DataFrame(
        [
            {"year": 2024, "week": 14, "manager": "Alice", "opponent": "Bob"},
        ]
    )
    with pytest.raises(KeyError, match="franchise_id is required"):
        get_manager_col(future_canon)


def test_playoff_odds_future_canon_with_franchise_id_returns_franchise_id():
    """Site 7a happy path — future_canon with franchise_id returns "franchise_id"."""
    future_canon = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 14,
                "franchise_id": "fid_alice",
                "manager": "Alice",
                "opponent": "Bob",
                "opponent_franchise_id": "fid_bob",
            },
        ]
    )
    assert get_manager_col(future_canon) == "franchise_id"


from multi_league.transformations.matchup.modules.playoff_helpers import _identity_columns


def test_identity_columns_returns_franchise_id_tuple_for_canonical_df():
    df = pd.DataFrame(
        [
            {"franchise_id": "fid_a", "opponent_franchise_id": "fid_b", "manager": "Alice", "opponent": "Bob"},
        ]
    )
    assert _identity_columns(df) == ("franchise_id", "opponent_franchise_id")


def test_identity_columns_raises_on_none():
    with pytest.raises(KeyError, match="franchise_id is required"):
        _identity_columns(None)


def test_identity_columns_raises_on_empty_df():
    with pytest.raises(KeyError, match="franchise_id is required"):
        _identity_columns(pd.DataFrame())


def test_identity_columns_raises_when_franchise_id_column_missing():
    df = pd.DataFrame([{"manager": "Alice", "opponent": "Bob"}])
    with pytest.raises(KeyError, match="franchise_id is required"):
        _identity_columns(df)


def test_identity_columns_raises_when_opponent_franchise_id_missing():
    df = pd.DataFrame([{"franchise_id": "fid_a", "manager": "Alice", "opponent": "Bob"}])
    with pytest.raises(KeyError, match="opponent_franchise_id is required"):
        _identity_columns(df)


from multi_league.transformations.matchup.modules.playoff_helpers import rank_and_seed


def test_rank_and_seed_raises_when_played_raw_is_none():
    wins = pd.Series({"fid_a": 2.0, "fid_b": 0.0})
    points = pd.Series({"fid_a": 210.0, "fid_b": 195.0})
    with pytest.raises(ValueError, match="rank_and_seed requires non-empty played_raw"):
        rank_and_seed(wins, points, playoff_slots=1, bye_slots=0, played_raw=None)


def test_rank_and_seed_raises_when_played_raw_is_empty():
    wins = pd.Series({"fid_a": 2.0, "fid_b": 0.0})
    points = pd.Series({"fid_a": 210.0, "fid_b": 195.0})
    with pytest.raises(ValueError, match="rank_and_seed requires non-empty played_raw"):
        rank_and_seed(wins, points, playoff_slots=1, bye_slots=0, played_raw=pd.DataFrame())


from multi_league.transformations.matchup.modules.playoff_helpers import _games_played_by_identity


def test_games_played_excludes_phantom_bye_rows():
    """A row with team_points=NaN and opponent_franchise_id=NaN must not count as a game played."""
    df = pd.DataFrame(
        [
            # Played game in week 1
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_a",
                "opponent_franchise_id": "fid_b",
                "manager": "Alice",
                "opponent": "Bob",
                "team_points": 100.0,
            },
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_b",
                "opponent_franchise_id": "fid_a",
                "manager": "Bob",
                "opponent": "Alice",
                "team_points": 90.0,
            },
            # Phantom bye row in week 2 — should be excluded
            {
                "year": 2024,
                "week": 2,
                "franchise_id": "fid_a",
                "opponent_franchise_id": None,
                "manager": "Alice",
                "opponent": None,
                "team_points": None,
            },
        ]
    )

    result = _games_played_by_identity(df)
    assert (
        result.loc["fid_a"] == 1
    ), f"fid_a should have 1 game played (the bye doesn't count); got {result.loc['fid_a']}"
    assert result.loc["fid_b"] == 1


def test_games_played_excludes_row_with_display_opponent_but_no_opponent_franchise_id():
    """Identity-safe predicate: opponent_franchise_id (not display opponent) is the source of truth."""
    df = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_a",
                "opponent_franchise_id": "fid_b",
                "manager": "Alice",
                "opponent": "Bob",
                "team_points": 100.0,
            },
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_b",
                "opponent_franchise_id": "fid_a",
                "manager": "Bob",
                "opponent": "Alice",
                "team_points": 90.0,
            },
            # Stale display opponent but no opponent_franchise_id — should be excluded
            {
                "year": 2024,
                "week": 2,
                "franchise_id": "fid_a",
                "opponent_franchise_id": None,
                "manager": "Alice",
                "opponent": "GhostManager",
                "team_points": 50.0,
            },
        ]
    )

    result = _games_played_by_identity(df)
    assert result.loc["fid_a"] == 1
    assert result.loc["fid_b"] == 1


def test_games_played_counts_real_games_correctly():
    """Sanity: two real games for each franchise == 2 games played."""
    df = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_a",
                "opponent_franchise_id": "fid_b",
                "manager": "Alice",
                "opponent": "Bob",
                "team_points": 100.0,
            },
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_b",
                "opponent_franchise_id": "fid_a",
                "manager": "Bob",
                "opponent": "Alice",
                "team_points": 90.0,
            },
            {
                "year": 2024,
                "week": 2,
                "franchise_id": "fid_a",
                "opponent_franchise_id": "fid_b",
                "manager": "Alice",
                "opponent": "Bob",
                "team_points": 110.0,
            },
            {
                "year": 2024,
                "week": 2,
                "franchise_id": "fid_b",
                "opponent_franchise_id": "fid_a",
                "manager": "Bob",
                "opponent": "Alice",
                "team_points": 105.0,
            },
        ]
    )

    result = _games_played_by_identity(df)
    assert result.loc["fid_a"] == 2
    assert result.loc["fid_b"] == 2


# ---------------------------------------------------------------------------
# Task 11: calculate_standings_stats bye-aware filter
# ---------------------------------------------------------------------------

from multi_league.transformations.matchup.modules.playoff_helpers import (  # noqa: E402
    calculate_standings_stats,
)


def test_calculate_standings_stats_excludes_phantom_bye_rows():
    """Phantom bye rows must not contribute to PF/PA/avg_margin."""
    df = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_a",
                "opponent_franchise_id": "fid_b",
                "manager": "Alice",
                "opponent": "Bob",
                "team_points": 100.0,
                "opponent_points": 90.0,
            },
            {
                "year": 2024,
                "week": 1,
                "franchise_id": "fid_b",
                "opponent_franchise_id": "fid_a",
                "manager": "Bob",
                "opponent": "Alice",
                "team_points": 90.0,
                "opponent_points": 100.0,
            },
            # Phantom bye for fid_a in week 2 — must not affect PF/PA
            {
                "year": 2024,
                "week": 2,
                "franchise_id": "fid_a",
                "opponent_franchise_id": None,
                "manager": "Alice",
                "opponent": None,
                "team_points": None,
                "opponent_points": None,
            },
        ]
    )

    stats = calculate_standings_stats(df)
    assert stats.loc["fid_a", "PF"] == 100.0  # only week 1 game counted
    assert stats.loc["fid_b", "PF"] == 90.0
    assert stats.loc["fid_a", "PA"] == 90.0
    assert stats.loc["fid_b", "PA"] == 100.0
    assert stats.loc["fid_a", "avg_margin"] == 10.0
    assert stats.loc["fid_b", "avg_margin"] == -10.0
