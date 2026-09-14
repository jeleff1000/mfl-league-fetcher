"""Safety warnings for ignored-resurfaces and manual-mapping conflicts.

Both fire as non-blocking advisories — user can override but the design goal
is to make accidental data corruption impossible to silently miss."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd


@dataclass
class IgnoreSnapshot:
    row_count: int
    tables: list[str]
    years: list[int]


@dataclass
class ResurfaceWarning:
    manager: str
    message: str


@dataclass
class ConflictWarning:
    manager: str
    franchise_id: str
    message: str
    conflicting_rows: pd.DataFrame  # for UI rendering


def check_ignored_resurfaced(
    manager: str,
    snapshot: IgnoreSnapshot,
    current_row_count: int,
    current_tables: Sequence[str],
    current_years: Sequence[int],
) -> list[ResurfaceWarning]:
    """Warn if an ignored manager now appears with significantly more data.

    Threshold: (current > 2x snapshot row count) OR (new tables/years appeared).
    """
    if not snapshot or not snapshot.row_count:
        return []
    grew = current_row_count > 2 * snapshot.row_count
    new_tables = set(current_tables) - set(snapshot.tables)
    new_years = set(current_years) - set(snapshot.years)
    if grew or new_tables or new_years:
        msg = (
            f"Manager '{manager}' was previously ignored "
            f"(row_count={snapshot.row_count}, tables={snapshot.tables}, years={snapshot.years}) "
            f"but now appears with row_count={current_row_count}, tables={list(current_tables)}, "
            f"years={list(current_years)}. Re-confirm or re-map?"
        )
        return [ResurfaceWarning(manager=manager, message=msg)]
    return []


def check_mapping_conflict(
    external_manager: str,
    target_franchise_id: str,
    external_rows: pd.DataFrame,
    canonical_matchup: pd.DataFrame,
    points_tolerance: float = 0.01,
) -> list[ConflictWarning]:
    """Warn when mapping would attach external rows to a franchise that already has
    DIFFERENT data at the same (year, week)."""
    if external_rows.empty or canonical_matchup.empty:
        return []
    canonical_for_target = canonical_matchup[canonical_matchup["franchise_id"] == target_franchise_id]
    if canonical_for_target.empty:
        return []
    merged = external_rows.merge(
        canonical_for_target[["year", "week", "team_points"]],
        on=["year", "week"],
        suffixes=("_ext", "_can"),
        how="inner",
    )
    if merged.empty:
        return []
    diff = (merged["team_points_ext"] - merged["team_points_can"]).abs() > points_tolerance
    conflicts = merged[diff]
    if conflicts.empty:
        return []
    msg = (
        f"Mapping '{external_manager}' to franchise {target_franchise_id} would create duplicate "
        f"matchup rows at {len(conflicts)} (year, week) positions with conflicting points. "
        f"Continue anyway?"
    )
    return [
        ConflictWarning(
            manager=external_manager,
            franchise_id=target_franchise_id,
            message=msg,
            conflicting_rows=conflicts,
        )
    ]
