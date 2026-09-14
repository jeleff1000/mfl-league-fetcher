"""Tests for playoff_helpers.py — Bug #1.7 franchise_id no-fallback residuals."""

import sys
from pathlib import Path

import pandas as pd
import pytest


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_display_name_lookup_returns_franchise_id_to_manager_mapping():
    """Happy path: a normalized df returns franchise_id -> latest manager name."""
    from multi_league.transformations.matchup.modules.playoff_helpers import _display_name_lookup

    df = pd.DataFrame(
        [
            {"franchise_id": "fid_alice", "manager": "Alice", "year": 2023, "week": 1},
            {"franchise_id": "fid_alice", "manager": "Alice-Renamed", "year": 2024, "week": 5},
            {"franchise_id": "fid_bob", "manager": "Bob", "year": 2024, "week": 5},
        ]
    )

    result = _display_name_lookup(df)

    # Latest week wins for each franchise_id
    assert result["fid_alice"] == "Alice-Renamed"
    assert result["fid_bob"] == "Bob"


def test_display_name_lookup_raises_when_franchise_id_missing():
    """Bug #1.7: silent {} return on missing franchise_id replaced by loud
    KeyError. Callers (rank_and_seed, history_snapshots) validate
    franchise_id via _identity_columns upstream, so the silent path was
    dead defensive code."""
    from multi_league.transformations.matchup.modules.playoff_helpers import _display_name_lookup

    df = pd.DataFrame(
        [
            {"manager": "Alice", "year": 2024, "week": 1},
        ]
    )

    with pytest.raises(KeyError):
        _display_name_lookup(df)


def test_display_name_lookup_raises_when_manager_missing():
    """Bug #1.7: silent {} return on missing manager column replaced by loud
    KeyError."""
    from multi_league.transformations.matchup.modules.playoff_helpers import _display_name_lookup

    df = pd.DataFrame(
        [
            {"franchise_id": "fid_alice", "year": 2024, "week": 1},
        ]
    )

    with pytest.raises(KeyError):
        _display_name_lookup(df)


def test_canonicalize_uses_franchise_id_columns():
    """Happy path: canonicalize prefers franchise_id / opponent_franchise_id
    over manager name when both are present."""
    from multi_league.transformations.matchup.modules.playoff_helpers import canonicalize

    df = pd.DataFrame(
        [
            {
                "manager": "Alice",
                "franchise_id": "fid_alice",
                "opponent": "Bob",
                "opponent_franchise_id": "fid_bob",
                "year": 2024,
                "week": 1,
                "team_points": 100.0,
            },
            {
                "manager": "Bob",
                "franchise_id": "fid_bob",
                "opponent": "Alice",
                "opponent_franchise_id": "fid_alice",
                "year": 2024,
                "week": 1,
                "team_points": 90.0,
            },
        ]
    )

    result = canonicalize(df)

    # canonicalize reduces 2 rows (one per perspective) to 1 row using the
    # alphabetically-first identity. With franchise_id, "fid_alice" < "fid_bob"
    # so Alice's row is the canonical one.
    assert len(result) == 1
    assert result.iloc[0]["franchise_id"] == "fid_alice"
    assert "match_key" in result.columns


def test_canonicalize_raises_when_franchise_id_missing():
    """Bug #1.7: defensive ternary that fell back to manager/opponent strings
    is replaced by trusting the precondition. Schedule data path
    empirically always has franchise_id (verified on dingleberry_derby /
    the_tfl), so the fallback was dead defensive code."""
    from multi_league.transformations.matchup.modules.playoff_helpers import canonicalize

    df = pd.DataFrame(
        [
            {"manager": "Alice", "opponent": "Bob", "year": 2024, "week": 1, "team_points": 100.0},
            {"manager": "Bob", "opponent": "Alice", "year": 2024, "week": 1, "team_points": 90.0},
        ]
    )

    with pytest.raises(KeyError):
        canonicalize(df)
