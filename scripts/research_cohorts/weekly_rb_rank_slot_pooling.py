"""Rank-slot pooling helpers for the 2025 weekly RB study.

The unit is a rank slot within (cohort, week), not a player.  A player may occupy RB1
in one cohort/week and another player may occupy RB1 elsewhere or in the next week.
"""
from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def assign_rank_slots(
    atoms: pd.DataFrame,
    *,
    rank_by: str,
    max_rank: int = 50,
    cohort_cols: Sequence[str] = ("cohort",),
    period_cols: Sequence[str] = ("week",),
) -> pd.DataFrame:
    """Calculate period-level player metrics and assign ranks per cohort/period.

    ``atoms`` must contain one row per ``cohort_cols + period_cols + pid`` with aggregate counts:
    ``eligible``, ``started``, ``wins`` and ``losses``.  ``rank_by`` is either ``start_pct``,
    ``expected_wins`` or ``expected_wl``.  Rows without a valid denominator are omitted.
    """
    if not period_cols:
        raise ValueError("period_cols must contain at least one column")
    required = {*cohort_cols, *period_cols, "pid", "eligible", "started", "wins", "losses"}
    missing = required - set(atoms.columns)
    if missing:
        raise ValueError(f"missing rank-slot columns: {sorted(missing)}")
    if rank_by not in {"start_pct", "expected_wins", "expected_wl"}:
        raise ValueError(f"unsupported rank metric: {rank_by}")
    if max_rank < 1:
        raise ValueError("max_rank must be positive")

    out = atoms.copy()
    decided = out["wins"] + out["losses"]
    out = out[(out["eligible"] > 0) & (decided > 0)].copy()
    out["start_pct"] = 100.0 * out["started"] / out["eligible"]
    out["win_pct"] = 100.0 * out["wins"] / decided
    out["expected_wins"] = (out["started"] / out["eligible"]) * out["wins"] / decided
    out["expected_wl"] = (out["started"] / out["eligible"]) * (
        (out["wins"] - out["losses"]) / decided
    )

    group_cols = [*cohort_cols, *period_cols]
    out["slot"] = (
        out.groupby(group_cols, sort=False, dropna=False)[rank_by]
        .rank(method="first", ascending=False)
        .astype("int64")
    )
    return out[out["slot"] <= max_rank].sort_values([*group_cols, "slot", "pid"]).reset_index(
        drop=True
    )


def rank_slot_spread(
    ranked: pd.DataFrame,
    *,
    cohort_col: str = "cohort",
    stat_cols: Sequence[str] = ("start_pct", "win_pct", "expected_wl"),
    period_cols: Sequence[str] = ("week",),
) -> pd.DataFrame:
    """Return max-minus-min spread per (period, rank slot) across cohort levels."""
    if not period_cols:
        raise ValueError("period_cols must contain at least one column")
    required = {cohort_col, *period_cols, "slot", *stat_cols}
    missing = required - set(ranked.columns)
    if missing:
        raise ValueError(f"missing spread columns: {sorted(missing)}")

    rows: list[dict[str, float | int]] = []
    group_keys = [*period_cols, "slot"]
    for key, group in ranked.groupby(group_keys, sort=True):
        if group[cohort_col].nunique() < 2:
            continue
        key_values = key if isinstance(key, tuple) else (key,)
        row: dict[str, float | int] = dict(zip(group_keys, key_values))
        for stat in stat_cols:
            values = group[stat].dropna()
            row[f"{stat}_spread"] = float(values.max() - values.min()) if len(values) >= 2 else float("nan")
        rows.append(row)
    return pd.DataFrame(rows, columns=[*period_cols, "slot", *(f"{s}_spread" for s in stat_cols)])
