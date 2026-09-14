"""Shared year-range contract for local and GitHub-sharded research builds."""

from __future__ import annotations

import os


LAKE_MIN_YEAR = 1997
LAKE_MAX_YEAR = 2025
EXCLUDED_RESEARCH_YEARS = frozenset(range(1999, 2003))


def configured_years(default_start: int = LAKE_MIN_YEAR, default_end: int = LAKE_MAX_YEAR) -> tuple[int, ...]:
    """Return the inclusive configured range, failing closed on invalid shard bounds."""
    start = int(os.environ.get("RESEARCH_YEAR_START", default_start))
    end = int(os.environ.get("RESEARCH_YEAR_END", default_end))
    if start > end:
        raise ValueError(f"RESEARCH_YEAR_START {start} exceeds RESEARCH_YEAR_END {end}")
    if start < LAKE_MIN_YEAR or end > LAKE_MAX_YEAR:
        raise ValueError(
            f"research years {start}-{end} exceed the supported lake range "
            f"{LAKE_MIN_YEAR}-{LAKE_MAX_YEAR}"
        )
    return tuple(year for year in range(start, end + 1) if year not in EXCLUDED_RESEARCH_YEARS)


def year_predicate(column: str = "year", *, default_start: int = LAKE_MIN_YEAR,
                   default_end: int = LAKE_MAX_YEAR) -> str:
    years = configured_years(default_start, default_end)
    if not years:
        return "FALSE"
    if years == tuple(range(years[0], years[-1] + 1)):
        return f"{column} BETWEEN {years[0]} AND {years[-1]}"
    return f"{column} IN ({','.join(str(year) for year in years)})"
