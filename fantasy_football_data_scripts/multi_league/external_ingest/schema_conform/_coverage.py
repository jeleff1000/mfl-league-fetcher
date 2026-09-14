"""Coverage gates + cross-table consistency + required-data null guard."""

from __future__ import annotations
from dataclasses import dataclass

import pandas as pd

from ._config import (
    COVERAGE_THRESHOLDS,
    REQUIRED_DATA_SLOTS,
    REQUIRED_DATA_NULL_FLOOR,
    CROSS_TABLE_OVERLAP_FLOOR,
)
from ._normalize import _norm


@dataclass
class CoverageFailure:
    gate: str
    detail: str
    table: str | None = None


def check_manager_guid_coverage(df: pd.DataFrame, table: str) -> CoverageFailure | None:
    """Gate (a): manager_guid populated for ≥ table-specific threshold."""
    if "manager_guid" not in df.columns:
        return CoverageFailure(gate="manager_guid_missing", detail="column not present", table=table)
    threshold = COVERAGE_THRESHOLDS.get(table, 0.95)
    coverage = df["manager_guid"].notna().mean()
    if coverage < threshold:
        return CoverageFailure(
            gate="coverage_below_threshold",
            detail=f"manager_guid populated for {coverage:.0%} of rows in {table}; " f"threshold is {threshold:.0%}",
            table=table,
        )
    return None


def check_known_managers_resolve_canonical(df: pd.DataFrame, ctx) -> CoverageFailure | None:
    """Gate (b): every "known" manager (name in canonical) must resolve to canonical guid."""
    if "manager" not in df.columns or "manager_guid" not in df.columns:
        return None
    name_to_guid = ctx.name_to_guid

    for name in df["manager"].dropna().unique():
        normed = _norm(name)
        if normed not in name_to_guid:
            continue  # new manager — handled by gate (c)
        canonical = name_to_guid[normed]
        rows = df[df["manager"] == name]
        bad = rows[rows["manager_guid"].notna() & (rows["manager_guid"] != canonical)]
        if not bad.empty:
            return CoverageFailure(
                gate="known_manager_unresolved",
                detail=f"manager {name!r} canonical guid is {canonical!r} " f"but {len(bad)} rows have different value",
            )
    return None


def check_new_managers_stable_synthetic(df: pd.DataFrame, ctx) -> CoverageFailure | None:
    """Gate (c): every "new" manager must get exactly one stable external_<hash> id."""
    if "manager" not in df.columns or "manager_guid" not in df.columns:
        return None
    name_to_guid = ctx.name_to_guid

    for name in df["manager"].dropna().unique():
        normed = _norm(name)
        if normed in name_to_guid:
            continue  # known — gate (b) handles
        rows = df[df["manager"] == name]
        guids = rows["manager_guid"].dropna().unique()
        if len(guids) > 1:
            return CoverageFailure(
                gate="new_manager_inconsistent_id",
                detail=f"new manager {name!r} has {len(guids)} different ids: {list(guids)}",
            )
    return None


def check_cross_table_consistency(
    table_dfs: dict[str, pd.DataFrame],
    slot: str,
    threshold: float = CROSS_TABLE_OVERLAP_FLOOR,
) -> CoverageFailure | None:
    """For each year, build the reference (richest) set of slot values, check every
    table's keys are largely a SUBSET of that reference. NOT union equality.
    """
    from collections import defaultdict

    sets_by_year: dict[int, dict[str, set]] = defaultdict(dict)

    for table, df in table_dfs.items():
        if slot not in df.columns or "year" not in df.columns:
            continue
        for year, sub in df.groupby("year"):
            keys = set(sub[slot].dropna().astype(str).unique())
            if keys:
                sets_by_year[year][table] = keys

    for year, table_sets in sets_by_year.items():
        if len(table_sets) < 2:
            continue
        reference = max(table_sets.values(), key=len)
        for table, keys in table_sets.items():
            if not keys:
                continue
            overlap = len(keys & reference) / len(keys)
            if overlap < threshold:
                return CoverageFailure(
                    gate="cross_table_inconsistent",
                    detail=f"slot {slot!r} in {table} year {year}: "
                    f"only {overlap:.0%} of values appear in reference set",
                    table=table,
                )
    return None


def apply_required_data_null_guard(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Demote required-DATA slots that are < REQUIRED_DATA_NULL_FLOOR populated to all-NULL.
    Modifies df in place AND returns it for chaining.
    """
    out = df.copy()
    for slot in REQUIRED_DATA_SLOTS.get(table, []):
        if slot not in out.columns:
            continue
        if out[slot].notna().mean() < REQUIRED_DATA_NULL_FLOOR:
            out[slot] = None
    return out
