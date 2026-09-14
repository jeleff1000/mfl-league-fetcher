"""External-ingest is_keeper coalesce — null-safe handling.

Regression: KMFFL 2014 (and 6 other league-years) showed 100% is_keeper=1
because the conform pipeline ran in two stages:

1. `normalize_columns` converts empty-string `is_keeper_status` to None
   (empty string is a null-literal per `_NULL_LITERALS`).
2. `_coalesce_is_keeper` then did `status.astype(str).str.strip().ne("")` —
   on a None value, `astype(str)` produces the literal string "None", which
   `.ne("")` evaluates True, so every row was flagged is_keeper=1.

The fix: `.fillna("")` before `.astype(str)` so None/NaN map to "" (the
right semantic for a missing keeper flag).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from multi_league.external_ingest.schema_conform._coalesce import _coalesce_is_keeper


def test_coalesce_is_keeper_returns_zero_for_none_status():
    """Post-normalize state: empty-string is_keeper_status becomes None.

    Coalesce must return 0 — None means "no keeper flag set", not "set".
    """
    df = pd.DataFrame(
        {
            "is_keeper_status": [None, None, None],
            "is_keeper_cost": [None, None, None],
        }
    )

    result = _coalesce_is_keeper(df)

    assert list(result) == [0, 0, 0]


def test_coalesce_is_keeper_returns_zero_for_nan_status():
    """np.nan should also coalesce to 0, same semantic as None."""
    df = pd.DataFrame(
        {
            "is_keeper_status": [np.nan, np.nan],
            "is_keeper_cost": [np.nan, np.nan],
        }
    )

    result = _coalesce_is_keeper(df)

    assert list(result) == [0, 0]


def test_coalesce_is_keeper_returns_one_for_truthy_status():
    """Non-empty status string OR positive cost should flag is_keeper=1."""
    df = pd.DataFrame(
        {
            "is_keeper_status": ["1", "", None, "0"],
            "is_keeper_cost": [None, 5.0, None, None],
        }
    )

    result = _coalesce_is_keeper(df)

    # row 0: status="1" (non-empty) → 1
    # row 1: cost=5 (>0) → 1
    # row 2: both None → 0
    # row 3: status="0" (non-empty literal) → 1
    assert list(result) == [1, 1, 0, 1]


def test_coalesce_is_keeper_handles_empty_string_status():
    """Pre-normalize state: empty string status → 0."""
    df = pd.DataFrame(
        {
            "is_keeper_status": ["", "", ""],
            "is_keeper_cost": [None, None, None],
        }
    )

    result = _coalesce_is_keeper(df)

    assert list(result) == [0, 0, 0]


def test_coalesce_is_keeper_handles_nullable_integer_status():
    """Fly/conform paths can carry nullable integer keeper status columns."""
    df = pd.DataFrame(
        {
            "is_keeper_status": pd.Series([pd.NA, 1, 0], dtype="Int32"),
            "is_keeper_cost": [None, None, None],
        }
    )

    result = _coalesce_is_keeper(df)

    assert list(result) == [0, 1, 1]
