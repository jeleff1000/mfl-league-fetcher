"""Helpers for canonical cross-table join keys.

The pipeline historically built ``manager_week`` from display names and raw
``cumulative_week`` values. That is fragile for two reasons:

1. Display names can collide (for example, two managers named Dave).
2. ``cumulative_week`` can arrive as floats (``202517.0``), which breaks
   string joins against integer-formatted keys.

These helpers centralize the canonical behavior:

- Prefer stable franchise identity when available.
- Fall back to manager GUID, then display name only when needed.
- Normalize cumulative_week to an integer string before concatenation.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
import polars as pl

from multi_league.core.manager_identity import HIDDEN_MANAGER_GUID_TOKENS


_INVALID_STRING_TOKENS = {"", "None", "<NA>", "nan", "NaT"}
_INVALID_MANAGER_GUID_TOKENS = {"--", "--hidden--"}


def _clean_pandas_string(
    series: pd.Series,
    *,
    strip_spaces: bool = False,
    block_unrostered: bool = False,
    extra_invalid_tokens: set[str] | None = None,
) -> pd.Series:
    """Normalize a pandas string series for use in join keys."""
    values = series.astype("string").str.strip()
    if strip_spaces:
        values = values.str.replace(" ", "", regex=False)

    invalid = values.isna() | values.isin(_INVALID_STRING_TOKENS)
    if extra_invalid_tokens:
        invalid = invalid | values.isin(extra_invalid_tokens)
    if block_unrostered:
        invalid = invalid | (values.str.lower() == "unrostered")

    return values.mask(invalid, pd.NA)


def canonical_cumulative_week_series(series: pd.Series) -> pd.Series:
    """Convert cumulative_week values to canonical integer strings."""
    numeric = pd.to_numeric(series, errors="coerce")
    rounded = numeric.round().astype("Int64")
    return rounded.astype("string").mask(rounded.isna(), pd.NA)


def ensure_cumulative_week_column(
    df: pd.DataFrame,
    *,
    year_col: str = "year",
    week_col: str = "week",
    cumulative_week_col: str = "cumulative_week",
) -> pd.DataFrame:
    """Ensure a DataFrame has canonical numeric cumulative_week values.

    The canonical rule is: ``cumulative_week = year * 100 + week``.
    Existing values are normalized first and only backfilled when missing.
    """
    if cumulative_week_col in df.columns:
        existing = pd.to_numeric(df[cumulative_week_col], errors="coerce").round().astype("Int64")
    else:
        existing = pd.Series(pd.NA, index=df.index, dtype="Int64")

    if year_col not in df.columns or week_col not in df.columns:
        df[cumulative_week_col] = existing
        return df

    year = pd.to_numeric(df[year_col], errors="coerce").round().astype("Int64")
    week = pd.to_numeric(df[week_col], errors="coerce").round().astype("Int64")
    computed = (year * 100 + week).astype("Int64")
    df[cumulative_week_col] = existing.fillna(computed)
    return df


def manager_identity_series(
    df: pd.DataFrame,
    *,
    franchise_id_col: str = "franchise_id",
    manager_guid_col: str = "manager_guid",
    team_name_col: str = "team_name",
    manager_col: str = "manager",
) -> pd.Series:
    """Return the best available stable identity for a manager/team row."""
    identity = pd.Series(pd.NA, index=df.index, dtype="string")

    if franchise_id_col in df.columns:
        identity = identity.fillna(
            _clean_pandas_string(df[franchise_id_col], extra_invalid_tokens=_INVALID_MANAGER_GUID_TOKENS)
        )

    if manager_guid_col in df.columns:
        identity = identity.fillna(
            _clean_pandas_string(df[manager_guid_col], extra_invalid_tokens=_INVALID_MANAGER_GUID_TOKENS)
        )

    if team_name_col in df.columns:
        identity = identity.fillna(_clean_pandas_string(df[team_name_col], strip_spaces=True))

    if manager_col in df.columns:
        identity = identity.fillna(_clean_pandas_string(df[manager_col], strip_spaces=True, block_unrostered=True))

    return identity


def build_manager_week_series(
    df: pd.DataFrame,
    *,
    cumulative_week_col: str = "cumulative_week",
    franchise_id_col: str = "franchise_id",
    manager_guid_col: str = "manager_guid",
    team_name_col: str = "team_name",
    manager_col: str = "manager",
) -> pd.Series:
    """Build the canonical manager_week key for pandas DataFrames."""
    identity = manager_identity_series(
        df,
        franchise_id_col=franchise_id_col,
        manager_guid_col=manager_guid_col,
        team_name_col=team_name_col,
        manager_col=manager_col,
    )
    cumulative = canonical_cumulative_week_series(df[cumulative_week_col])

    key = (identity + cumulative).astype("string")
    return key.mask(identity.isna() | cumulative.isna(), pd.NA)


def _clean_polars_string_expr(
    column_name: str,
    *,
    strip_spaces: bool = False,
    block_unrostered: bool = False,
    extra_invalid_tokens: set[str] | None = None,
) -> pl.Expr:
    """Normalize a polars string expression for use in join keys."""
    raw = pl.col(column_name).cast(pl.Utf8, strict=False)
    cleaned = raw.str.strip_chars()
    if strip_spaces:
        cleaned = cleaned.str.replace_all(" ", "")

    invalid = raw.is_null() | cleaned.is_in(list(_INVALID_STRING_TOKENS))
    if extra_invalid_tokens:
        invalid = invalid | cleaned.is_in(list(extra_invalid_tokens))
    if block_unrostered:
        invalid = invalid | (cleaned.str.to_lowercase() == "unrostered")

    return pl.when(invalid).then(pl.lit(None)).otherwise(cleaned)


def canonical_cumulative_week_expr(column_name: str = "cumulative_week") -> pl.Expr:
    """Return a polars expression for canonical cumulative_week strings."""
    numeric = pl.col(column_name).cast(pl.Float64, strict=False)
    normalized = numeric.round(0).cast(pl.Int64, strict=False).cast(pl.Utf8)
    return pl.when(numeric.is_null()).then(pl.lit(None)).otherwise(normalized)


def manager_identity_expr(
    columns: Iterable[str],
    *,
    franchise_id_col: str = "franchise_id",
    manager_guid_col: str = "manager_guid",
    team_name_col: str = "team_name",
    manager_col: str = "manager",
) -> pl.Expr:
    """Return the best available stable identity for a polars row."""
    available = set(columns)
    candidates: list[pl.Expr] = []

    if franchise_id_col in available:
        candidates.append(_clean_polars_string_expr(franchise_id_col))
    if manager_guid_col in available:
        candidates.append(
            _clean_polars_string_expr(manager_guid_col, extra_invalid_tokens=_INVALID_MANAGER_GUID_TOKENS)
        )
    if team_name_col in available:
        candidates.append(_clean_polars_string_expr(team_name_col, strip_spaces=True))
    if manager_col in available:
        candidates.append(_clean_polars_string_expr(manager_col, strip_spaces=True, block_unrostered=True))

    if not candidates:
        raise ValueError("No manager identity columns available for manager_week generation")

    return pl.coalesce(candidates)


def manager_week_expr(
    columns: Iterable[str],
    *,
    cumulative_week_col: str = "cumulative_week",
    franchise_id_col: str = "franchise_id",
    manager_guid_col: str = "manager_guid",
    team_name_col: str = "team_name",
    manager_col: str = "manager",
) -> pl.Expr:
    """Return a polars expression for the canonical manager_week key."""
    identity = manager_identity_expr(
        columns,
        franchise_id_col=franchise_id_col,
        manager_guid_col=manager_guid_col,
        team_name_col=team_name_col,
        manager_col=manager_col,
    )
    cumulative = canonical_cumulative_week_expr(cumulative_week_col)

    return (
        pl.when(identity.is_not_null() & cumulative.is_not_null()).then(identity + cumulative).otherwise(pl.lit(None))
    )


def canonical_cumulative_week_sql(column_name: str = "cumulative_week") -> str:
    """Return SQL that normalizes cumulative_week to an integer string."""
    numeric_expr = f"TRY_CAST(ROUND(TRY_CAST({column_name} AS DOUBLE), 0) AS BIGINT)"
    return f"CAST({numeric_expr} AS VARCHAR)"


def manager_identity_sql(
    available_columns: Iterable[str],
    *,
    franchise_id_col: str = "franchise_id",
    manager_guid_col: str = "manager_guid",
    team_name_col: str = "team_name",
    manager_col: str = "manager",
) -> str:
    """Return SQL for the best available stable identity."""
    available = set(available_columns)
    candidates: list[str] = []

    if franchise_id_col in available:
        hidden_tokens = ", ".join(f"'{token}'" for token in sorted(HIDDEN_MANAGER_GUID_TOKENS) if token)
        candidates.append(
            "CASE "
            f"WHEN LOWER(TRIM({franchise_id_col})) IN ({hidden_tokens}) THEN NULL "
            f"ELSE NULLIF(TRIM({franchise_id_col}), '') "
            "END"
        )
    if manager_guid_col in available:
        hidden_tokens = ", ".join(f"'{token}'" for token in sorted(HIDDEN_MANAGER_GUID_TOKENS) if token)
        candidates.append(
            "CASE "
            f"WHEN LOWER(TRIM({manager_guid_col})) IN ({hidden_tokens}) THEN NULL "
            f"ELSE NULLIF(TRIM({manager_guid_col}), '') "
            "END"
        )
    if team_name_col in available:
        candidates.append(f"NULLIF(REPLACE(TRIM({team_name_col}), ' ', ''), '')")
    if manager_col in available:
        candidates.append(
            "CASE "
            f"WHEN LOWER(TRIM({manager_col})) = 'unrostered' THEN NULL "
            f"ELSE NULLIF(REPLACE(TRIM({manager_col}), ' ', ''), '') "
            "END"
        )

    if not candidates:
        raise ValueError("No manager identity columns available for manager_week generation")

    return f"COALESCE({', '.join(candidates)})"


def franchise_identity_column_name(
    has_franchise_id: bool,
    *,
    franchise_id_col: str = "franchise_id",
    manager_col: str = "manager",
) -> str:
    """Return the stable identity column name for SQL joins/grouping."""
    return franchise_id_col


def franchise_identity_sql_ref(
    alias: str | None,
    has_franchise_id: bool,
    *,
    franchise_id_col: str = "franchise_id",
    manager_col: str = "manager",
) -> str:
    """Return a qualified SQL reference to the preferred identity column."""
    column_name = franchise_identity_column_name(
        has_franchise_id,
        franchise_id_col=franchise_id_col,
        manager_col=manager_col,
    )
    return f"{alias}.{column_name}" if alias else column_name


def franchise_identity_sql_select(
    alias: str | None,
    has_franchise_id: bool,
    *,
    output_alias: str = "identity_key",
    franchise_id_col: str = "franchise_id",
    manager_col: str = "manager",
) -> str:
    """Return a SQL SELECT fragment for the preferred identity column."""
    return (
        f"{franchise_identity_sql_ref(alias, has_franchise_id, franchise_id_col=franchise_id_col, manager_col=manager_col)} "
        f"AS {output_alias}"
    )


def franchise_identity_join_sql(
    left_alias: str,
    right_alias: str,
    *,
    left_has_franchise_id: bool,
    right_has_franchise_id: bool,
    left_franchise_id_col: str = "franchise_id",
    right_franchise_id_col: str = "franchise_id",
    left_manager_col: str = "manager",
    right_manager_col: str = "manager",
    extra_conditions: Iterable[str] | None = None,
) -> str:
    """Return a SQL join condition keyed on franchise_id."""
    conditions = [
        (
            f"{franchise_identity_sql_ref(left_alias, left_has_franchise_id, franchise_id_col=left_franchise_id_col, manager_col=left_manager_col)} = "
            f"{franchise_identity_sql_ref(right_alias, right_has_franchise_id, franchise_id_col=right_franchise_id_col, manager_col=right_manager_col)}"
        )
    ]
    if extra_conditions:
        conditions.extend(extra_conditions)
    return " AND ".join(conditions)
