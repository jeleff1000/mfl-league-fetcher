"""Many-to-one coalescing rules. Constituent source cols are removed from the
assignment pool BEFORE Pass 1 (so cosine doesn't try to map them individually).
"""

from __future__ import annotations
import numpy as np
import pandas as pd


def _coalesce_is_keeper(df: pd.DataFrame) -> pd.Series:
    """Combine is_keeper_status and is_keeper_cost into single is_keeper flag."""
    status = df.get("is_keeper_status", pd.Series("", index=df.index))
    cost = df.get("is_keeper_cost", pd.Series(0, index=df.index))

    # Ensure both are Series (handle missing columns)
    if not isinstance(status, pd.Series):
        status = pd.Series(status, index=df.index)
    if not isinstance(cost, pd.Series):
        cost = pd.Series(cost, index=df.index)

    # fillna("") before astype(str): pd.Series([None]).astype(str) yields
    # the literal "None", which ne("") would falsely flag as keeper. Empty
    # string is the correct semantic for a missing keeper signal — empty
    # strings are converted to None upstream by normalize_columns, so this
    # restores the original semantic.
    return np.logical_or(
        status.astype("string").fillna("").str.strip().ne(""),
        pd.to_numeric(cost, errors="coerce").fillna(0).gt(0),
    ).astype(int)


COALESCE_RULES = {
    "is_keeper": _coalesce_is_keeper,
}

# Source cols consumed by each coalesce rule (used to remove from assignment pool).
COALESCE_INPUT_COLS = {
    "is_keeper": ["is_keeper_status", "is_keeper_cost"],
}


def apply_coalesce(df: pd.DataFrame, slot: str) -> pd.Series:
    """Returns the coalesced series for slot, or empty series if no rule registered."""
    rule = COALESCE_RULES.get(slot)
    if rule is None:
        return pd.Series([None] * len(df), index=df.index)
    return rule(df)
