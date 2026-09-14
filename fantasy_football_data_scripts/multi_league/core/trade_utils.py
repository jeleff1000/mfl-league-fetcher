"""Shared trade row expansion helpers for the flat transaction schema.

All three platform normalizers produce raw rows with ``source_manager`` and
``destination_manager`` populated.  The canonical normalizer
(``normalize_transaction_df``) calls :func:`duplicate_trade_rows` once to
expand every trade asset into one row per party perspective.  Each row stores
the current manager in ``manager``, the counterparty in ``source_manager``,
and a canonical ``trade_direction`` flag (``received`` / ``sent``).
"""

import pandas as pd

# The per-party schema already encodes the trade perspective, so callers no
# longer need an extra SQL filter to suppress synthetic sent/received rows.
TRADE_DEDUP_FILTER = ""

_TRADE_TYPES = {"trade", "trade_pick"}


def duplicate_trade_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Expand trade assets into one row per party perspective.

    Trade rows are duplicated so both managers involved in an asset exchange
    see the asset in their own perspective.  The resulting rows follow the flat
    DDL contract:

    - ``manager`` = the perspective manager for this row
    - ``source_manager`` = that manager's counterparty
    - ``trade_direction`` = ``received`` when the current manager got the asset,
      ``sent`` when the current manager gave it away
    - ``destination_*`` columns are cleared because the row is already scoped
      to a single manager perspective

    Non-trade rows pass through unchanged, except we backfill ``manager`` /
    ``franchise_id`` / ``team_name`` from ``destination_*`` when the raw row
    only populated the destination side.
    """
    if df.empty:
        return df.copy()

    if "transaction_type" not in df.columns:
        return df.copy()

    trade_mask = df["transaction_type"].fillna("").astype(str).str.lower().isin(_TRADE_TYPES)
    trades = df[trade_mask].copy()
    non_trades = df[~trade_mask].copy()

    if trades.empty:
        _fill_non_trade_manager_context(non_trades)
        return non_trades

    received_pov = trades.copy()
    _apply_trade_perspective(received_pov, current_side="destination", counterparty_side="source")

    sent_pov = trades.copy()
    _apply_trade_perspective(sent_pov, current_side="source", counterparty_side="destination")

    expanded = pd.concat([received_pov, sent_pov], ignore_index=True)

    # --- Enforcement: remove malformed rows produced by incomplete API data ---

    # 1. Drop rows where manager resolved to null/empty (orphans from missing
    #    source or destination in the raw API data).
    mgr = expanded["manager"].fillna("").astype(str).str.strip()
    expanded = expanded[mgr != ""].copy()

    # 2. Drop self-referencing rows where source_manager == manager (a manager
    #    cannot trade an asset to themselves).
    src = expanded["source_manager"].fillna("").astype(str).str.strip()
    mgr = expanded["manager"].fillna("").astype(str).str.strip()
    expanded = expanded[src != mgr].copy()

    # 3. Dedup: if the same (transaction_id, player, manager, trade_direction)
    #    appears more than once, keep only the first row.
    dedup_cols = ["transaction_id", "player", "manager", "trade_direction"]
    if all(c in expanded.columns for c in dedup_cols):
        expanded = expanded.drop_duplicates(subset=dedup_cols, keep="first")

    _fill_non_trade_manager_context(non_trades)
    return pd.concat([expanded, non_trades], ignore_index=True)


def _apply_trade_perspective(df: pd.DataFrame, *, current_side: str, counterparty_side: str) -> None:
    """Rewrite trade rows so they reflect a single manager's perspective."""
    _ensure_column(df, "manager")
    _ensure_column(df, "manager_guid")
    _ensure_column(df, "team_name")
    _ensure_column(df, "franchise_id")

    df["manager"] = _get_side_column(df, current_side, "manager")
    df["manager_guid"] = _get_side_column(df, current_side, "manager_guid")
    df["team_name"] = _get_side_column(df, current_side, "team_name")
    df["franchise_id"] = _get_side_column(df, current_side, "franchise_id")

    df["source_manager"] = _get_side_column(df, counterparty_side, "manager")
    df["source_manager_guid"] = _get_side_column(df, counterparty_side, "manager_guid")
    df["source_team_name"] = _get_side_column(df, counterparty_side, "team_name")
    df["source_franchise_id"] = _get_side_column(df, counterparty_side, "franchise_id")
    df["trade_direction"] = "received" if current_side == "destination" else "sent"

    for col in [
        "destination_manager",
        "destination_manager_guid",
        "destination_team_name",
        "destination_franchise_id",
    ]:
        if col in df.columns:
            df[col] = None


def _fill_non_trade_manager_context(df: pd.DataFrame) -> None:
    """Backfill manager-facing columns for non-trade rows when needed."""
    if df.empty:
        return

    for target, source in [
        ("manager", "destination_manager"),
        ("manager_guid", "destination_manager_guid"),
        ("team_name", "destination_team_name"),
        ("franchise_id", "destination_franchise_id"),
    ]:
        if source not in df.columns:
            continue
        _ensure_column(df, target)
        mask = df[target].isna() | (df[target].astype(str).str.strip() == "")
        df.loc[mask, target] = df.loc[mask, source]


def _get_side_column(df: pd.DataFrame, side: str, base: str) -> pd.Series:
    col = f"{side}_{base}"
    if col in df.columns:
        return df[col]
    return pd.Series([None] * len(df), index=df.index)


def _ensure_column(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = None
