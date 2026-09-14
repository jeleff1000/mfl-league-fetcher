"""
Shared rostered/unrostered player filters.

SINGLE SOURCE OF TRUTH for determining whether a manager value represents
a real rostered player vs an unrostered/free agent placeholder.

Used in:
- Python DataFrame filtering (validators, transformations)
- SQL WHERE clauses (enrichments, aggregations, API queries)
- Frontend API routes (TypeScript equivalent in frontend/src/lib/query-helpers.ts)

The canonical set of unrostered values:
    {'unrostered', 'fa', 'free agent', 'waivers', ''}

IMPORTANT: This set must stay in sync with the frontend's managerFilter()
and managersFilter() functions in query-helpers.ts.
"""

import pandas as pd

# Canonical set of values that indicate a player is NOT on a real roster
UNROSTERED_VALUES = frozenset(
    {
        "unrostered",
        "fa",
        "free agent",
        "waivers",
        "",
    }
)


def is_rostered_value(manager) -> bool:
    """Check if a single manager value represents a real rostered player.

    Args:
        manager: Manager name (str, None, NaN, etc.)

    Returns:
        True if the manager is a real person, False if unrostered/placeholder.
    """
    if manager is None or (isinstance(manager, float) and pd.isna(manager)):
        return False
    try:
        if pd.isna(manager):
            return False
    except (TypeError, ValueError):
        pass

    s = str(manager).lower().strip()
    return s not in UNROSTERED_VALUES


def rostered_mask(df: pd.DataFrame, col: str = "manager") -> pd.Series:
    """Return a boolean mask for rostered players in a DataFrame.

    This is the canonical way to filter a DataFrame to rostered-only rows.
    Handles None, NaN, empty strings, and all unrostered placeholder values.

    Args:
        df: DataFrame with a manager/managers column
        col: Column name to filter on (default "manager", use "managers" for season/career)

    Returns:
        Boolean Series — True for rostered rows

    Example:
        rostered_df = df[rostered_mask(df)]
        rostered_df = df[rostered_mask(df, col="managers")]
    """
    if col not in df.columns:
        return pd.Series(False, index=df.index)

    return (
        df[col].notna()
        & (df[col].astype(str).str.strip() != "")
        & (~df[col].astype(str).str.lower().str.strip().isin(UNROSTERED_VALUES))
    )


def unrostered_mask(df: pd.DataFrame, col: str = "manager") -> pd.Series:
    """Return a boolean mask for unrostered players. Inverse of rostered_mask."""
    return ~rostered_mask(df, col)


def rostered_filter_sql(
    alias: str = "",
    col: str = "manager",
    include_coalesce: bool = True,
) -> str:
    """Return a SQL WHERE clause fragment for filtering to rostered players.

    Args:
        alias: Table alias prefix (e.g., "p" → "p.manager"). Empty for no alias.
        col: Column name ("manager" for weekly, "managers" for season/career)
        include_coalesce: Whether to wrap in COALESCE for NULL safety (default True)

    Returns:
        SQL fragment suitable for use in a WHERE clause.

    Example:
        sql = f"SELECT * FROM player_fantasy WHERE {rostered_filter_sql('p')}"
        sql = f"SELECT * FROM player_fantasy_season WHERE {rostered_filter_sql('fs', 'managers')}"
    """
    qualified = f"{alias}.{col}" if alias else col
    coalesced = f"COALESCE({qualified}, '')" if include_coalesce else qualified

    values = ", ".join(f"'{v}'" for v in sorted(UNROSTERED_VALUES))

    return (
        f"{qualified} IS NOT NULL\n"
        f"    AND TRIM({coalesced}) != ''\n"
        f"    AND LOWER(TRIM({qualified})) NOT IN ({values})"
    )


def unrostered_filter_sql(
    alias: str = "",
    col: str = "manager",
) -> str:
    """Return a SQL WHERE clause fragment for filtering to unrostered players.

    Example:
        sql = f"SELECT * FROM player_fantasy WHERE {unrostered_filter_sql('p')}"
    """
    qualified = f"{alias}.{col}" if alias else col
    values = ", ".join(f"'{v}'" for v in sorted(UNROSTERED_VALUES) if v)  # exclude empty

    return f"LOWER(TRIM(COALESCE({qualified}, ''))) IN ({values})"
