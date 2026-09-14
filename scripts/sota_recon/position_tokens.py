"""Canonical compact fantasy-position token helpers for v26 builders."""

from __future__ import annotations

FANTASY_POSITION_ORDER = ("QB", "RB", "WR", "TE", "K", "DB", "LB", "DL", "P", "OL", "DEF")

EMPTY_VARCHAR_LIST_SQL = "CAST([] AS VARCHAR[])"


def canonical_position_list_sql(list_expr: str) -> str:
    """Return a DuckDB expression that de-dupes and orders compact position tokens."""

    tokens = f"list_transform(COALESCE({list_expr}, {EMPTY_VARCHAR_LIST_SQL}), x -> UPPER(TRIM(CAST(x AS VARCHAR))))"
    parts = [
        f"CASE WHEN list_has_any({tokens}, ['{token}']) THEN ['{token}'] ELSE {EMPTY_VARCHAR_LIST_SQL} END"
        for token in FANTASY_POSITION_ORDER
    ]
    return f"list_concat({', '.join(parts)})"


def canonical_position_string_sql(list_expr: str) -> str:
    """Return a DuckDB expression for a comma-joined canonical compact position string."""

    return f"NULLIF(array_to_string({canonical_position_list_sql(list_expr)}, ','), '')"


def canonicalize_position(value: str | None) -> str | None:
    """Python equivalent used by tests and small audits."""

    if value is None:
        return None
    tokens = {token.strip().upper() for token in str(value).split(",") if token.strip()}
    ordered = [token for token in FANTASY_POSITION_ORDER if token in tokens]
    return ",".join(ordered) if ordered else None
