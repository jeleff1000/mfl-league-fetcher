"""Per-slot invariants. Hard pre-conditions independent of cosine score.

Failure → binding rejected (status='invariant_failed'), slot treated as unmapped.
"""

from __future__ import annotations
from collections.abc import Callable
import pandas as pd

from ._scoring import _shannon_entropy_per_char


# Each invariant is (name, predicate). Predicate returns True if invariant holds.
SLOT_INVARIANTS: dict[str, list[tuple[str, Callable]]] = {
    "manager_guid": [
        (
            "cardinality_matches_manager",
            lambda src, ctx: ctx.df is not None
            and "manager" in ctx.df.columns
            and src.nunique() >= 0.9 * max(ctx.df["manager"].nunique(), 1),
        ),
        (
            "guid_shape",
            lambda src, ctx: src.dropna().astype(str).str.match(r"^[A-Z2-7]{26}$|^\d{4,}$|^\{[0-9A-F-]+\}$").mean()
            >= 0.9,
        ),
    ],
    "year": [
        ("in_range_1990_2050", lambda src, ctx: src.dropna().astype(int).between(1990, 2050).all()),
    ],
    "week": [
        ("in_range_0_25", lambda src, ctx: src.dropna().astype(int).between(0, 25).all()),
    ],
    "yahoo_player_id": [
        ("uniqueness_floor", lambda src, ctx: (src.nunique() / max(len(src), 1)) >= 0.05),
        ("digit_shape", lambda src, ctx: src.dropna().astype(str).str.match(r"^\d+$").mean() >= 0.9),
    ],
    "transaction_id": [
        ("presence", lambda src, ctx: src.notna().mean() >= 0.95),
        ("median_length", lambda src, ctx: float(src.dropna().astype(str).str.len().median() or 0) >= 6),
        (
            "entropy_per_char",
            lambda src, ctx: len(src) < 50 or _shannon_entropy_per_char(src.dropna().astype(str)) >= 2.5,
        ),
    ],
}


def validate_invariants(slot: str, src: pd.Series, ctx) -> list[str]:
    """Returns list of failed invariant names; empty list = all pass."""
    failures = []
    for name, predicate in SLOT_INVARIANTS.get(slot, []):
        try:
            if not predicate(src, ctx):
                failures.append(name)
        except Exception as e:
            failures.append(f"{name}_error_{type(e).__name__}")
    return failures
