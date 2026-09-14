"""
Shared identity resolution utilities.

franchise_id and opponent_franchise_id are now guaranteed NOT NULL in all
pipeline tables. These helpers always return the franchise_id columns —
no fallback to manager/opponent display names.

Usage:
    # Python — get the identity column name
    id_col = get_manager_col(df)           # "franchise_id"
    opp_col = get_opponent_col(df)         # "opponent_franchise_id"

    # SQL helpers
    sql_group_by("m", ["m.year"])          # "GROUP BY m.franchise_id, m.year"
    sql_partition_by("m", ["m.year"])      # "PARTITION BY m.franchise_id, m.year"
    sql_join_on("m", "wr", ["year"])       # "ON m.franchise_id = wr.franchise_id AND m.year = wr.year"
    sql_mgr_select("m")                   # "m.franchise_id, MAX(m.manager) AS manager"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# ── Constants ────────────────────────────────────────────────────────────────
MANAGER_ID = "franchise_id"
OPPONENT_ID = "opponent_franchise_id"


# ── Python / pandas helpers ──────────────────────────────────────────────────


def get_manager_col(df: pd.DataFrame) -> str:
    """The manager identity column. Requires franchise_id."""
    if MANAGER_ID not in df.columns:
        raise KeyError("franchise_id is required for manager identity")
    return MANAGER_ID


def get_opponent_col(df: pd.DataFrame) -> str:
    """The opponent identity column. Requires opponent_franchise_id."""
    if OPPONENT_ID not in df.columns:
        raise KeyError("opponent_franchise_id is required for opponent identity")
    return OPPONENT_ID


def sql_manager_expr(alias: str = "", **_kwargs) -> str:
    """Return qualified franchise_id reference."""
    prefix = f"{alias}." if alias else ""
    return f"{prefix}franchise_id"


def sql_opponent_expr(alias: str = "", **_kwargs) -> str:
    """Return qualified opponent_franchise_id reference."""
    prefix = f"{alias}." if alias else ""
    return f"{prefix}opponent_franchise_id"


# ── SQL helpers ──────────────────────────────────────────────────────────────


def sql_group_by(alias: str = "", extra_cols: list[str] | None = None) -> str:
    """GROUP BY clause with franchise_id as identity key."""
    prefix = f"{alias}." if alias else ""
    cols = [f"{prefix}franchise_id"] + (extra_cols or [])
    return "GROUP BY " + ", ".join(cols)


def sql_partition_by(alias: str = "", extra_cols: list[str] | None = None) -> str:
    """PARTITION BY clause with franchise_id as identity key."""
    prefix = f"{alias}." if alias else ""
    cols = [f"{prefix}franchise_id"] + (extra_cols or [])
    return "PARTITION BY " + ", ".join(cols)


def sql_join_on(left: str, right: str, extra_cols: list[str] | None = None, *, use_opponent: bool = False) -> str:
    """JOIN ON clause matching on franchise_id (or opponent_franchise_id)."""
    id_col = OPPONENT_ID if use_opponent else MANAGER_ID
    parts = [f"{left}.{id_col} = {right}.{id_col}"]
    for col in extra_cols or []:
        parts.append(f"{left}.{col} = {right}.{col}")
    return "ON " + " AND ".join(parts)


def sql_mgr_select(alias: str = "") -> str:
    """SELECT fragment: franchise_id + MAX(manager) AS manager for display."""
    prefix = f"{alias}." if alias else ""
    return f"{prefix}franchise_id, MAX({prefix}manager) AS manager"


def sql_opp_select(alias: str = "") -> str:
    """SELECT fragment: opponent_franchise_id + MAX(opponent) AS opponent for display."""
    prefix = f"{alias}." if alias else ""
    return f"{prefix}opponent_franchise_id, MAX({prefix}opponent) AS opponent"
