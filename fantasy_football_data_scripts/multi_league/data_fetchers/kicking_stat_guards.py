"""Guards against kicker stat leakage onto non-kicker player rows."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import pandas as pd


KICKING_SIGNAL_COLUMNS = (
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_yards",
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
    "fg_made_60_plus_canonical",
    "pat_att",
    "pat_made",
    "pat_missed",
    "xpa",
    "xpm",
    "xp_a",
    "xp_m",
    "fga",
    "fgm",
    "fg_a",
    "fg_m",
    "xp_attempts",
    "xp_made",
    "fg_attempts",
)

KICKING_ZERO_COLUMNS = (
    *KICKING_SIGNAL_COLUMNS,
    "fg_blocked",
    "pat_blocked",
    "fg_pct",
    "fg_yds_over_30",
    "fg_yards_canonical",
    "fg_yds_over_30_canonical",
    "pts_k_std",
    "pts_k_yds",
    "pts_k_flat",
    "pts_k_fgm_0_19_3",
    "pts_k_fgm_20_29_3",
    "pts_k_fgm_30_39_3",
    "pts_k_fgm_40_49_4",
    "pts_k_fgm_40_49_3",
    "pts_k_fgm_50_59_5",
    "pts_k_fgm_60p_6",
    "pts_k_fgm_60p_5",
    "pts_k_xpm_1",
    "pts_k_xpmiss_n1",
    "pts_k_fgmiss_n1",
    "pts_k_fgm_yd_p1",
    "pts_k_fgm_yd_over30_p1",
)

KICKING_POSITIONS = {"K", "P"}
EXCLUDED_POSITIONS = {"DEF"}


def _log(log_fn: Callable[[str], None] | None, message: str) -> None:
    if log_fn:
        log_fn(message)


def _first_existing(df: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def _position_tokens(value: Any) -> set[str]:
    if pd.isna(value):
        return set()
    return {token for token in re.split(r"[^A-Za-z0-9]+", str(value).upper()) if token}


def _position_has(value: Any, allowed: set[str]) -> bool:
    return bool(_position_tokens(value) & allowed)


def _build_player_week_key(df: pd.DataFrame) -> pd.Series | None:
    if "player_week" in df.columns:
        return df["player_week"].astype("string")

    player_col = _first_existing(df, ("NFL_player_id", "player_id", "gsis_id", "nfl_player_id"))
    year_col = _first_existing(df, ("year", "season"))
    if not player_col or not year_col or "week" not in df.columns:
        return None

    year = pd.to_numeric(df[year_col], errors="coerce").astype("Int64").astype("string")
    week = pd.to_numeric(df["week"], errors="coerce").astype("Int64").astype("string")
    player = df[player_col].astype("string")
    valid = player.notna() & year.notna() & week.notna()
    key = pd.Series(pd.NA, index=df.index, dtype="string")
    key.loc[valid] = player.loc[valid] + "_" + year.loc[valid] + "_" + week.loc[valid]
    return key


def _kicking_signal(df: pd.DataFrame) -> pd.Series:
    cols = [col for col in KICKING_SIGNAL_COLUMNS if col in df.columns]
    if not cols:
        return pd.Series(0.0, index=df.index)
    total = pd.Series(0.0, index=df.index)
    for col in cols:
        total = total + pd.to_numeric(df[col], errors="coerce").fillna(0.0).abs()
    return total


def _year_mask(df: pd.DataFrame, min_year: int) -> pd.Series:
    year_col = _first_existing(df, ("year", "season"))
    if not year_col:
        return pd.Series(True, index=df.index)
    return pd.to_numeric(df[year_col], errors="coerce").ge(min_year).fillna(False)


def _non_kicker_mask(df: pd.DataFrame) -> pd.Series:
    pos_col = _first_existing(df, ("position", "nfl_position"))
    if not pos_col:
        return pd.Series(False, index=df.index)

    positions = df[pos_col]
    has_position = positions.notna() & positions.astype("string").str.strip().ne("")
    has_kicking_position = positions.map(lambda value: _position_has(value, KICKING_POSITIONS))
    is_excluded = positions.map(lambda value: _position_has(value, EXCLUDED_POSITIONS))
    return has_position & has_kicking_position.eq(False) & is_excluded.eq(False)


def raw_confirmed_non_kicker_kick_keys(source_df: pd.DataFrame, min_year: int = 1999) -> set[str]:
    """Return raw nflverse player_week keys where a non-K/P row really kicked."""
    keys = _build_player_week_key(source_df)
    if keys is None:
        return set()

    mask = _year_mask(source_df, min_year) & _non_kicker_mask(source_df) & _kicking_signal(source_df).gt(0)
    return set(keys.loc[mask].dropna().astype(str))


def sanitize_non_kicker_kicking_leaks(
    df: pd.DataFrame,
    *,
    source_df: pd.DataFrame | None = None,
    min_year: int = 1999,
    zero_without_source: bool = False,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Zero leaked kicking stats on non-K/P rows unless raw source confirms them.

    Raw nflverse weekly stats include rare emergency non-kicker kicks. Those
    exact player-week rows are preserved when ``source_df`` is supplied. Any
    post-processed non-K/P row with kicking stats that does not exist in that
    raw allow-list is treated as stat leakage and zeroed before scoring/ranks.
    """
    if df.empty:
        return df

    keys = _build_player_week_key(df)
    if keys is None:
        _log(log_fn, "  Kicking stat guard skipped: no player/week key available")
        return df

    signal = _kicking_signal(df)
    suspect = _year_mask(df, min_year) & _non_kicker_mask(df) & signal.gt(0)
    if not suspect.any():
        _log(log_fn, "  Kicking stat guard: no non-kicker kicking leaks found")
        return df

    if source_df is None and not zero_without_source:
        _log(
            log_fn,
            "  Kicking stat guard skipped: found "
            f"{int(suspect.sum()):,} non-kicker kicking rows but no raw source allow-list",
        )
        return df

    allowed_keys = raw_confirmed_non_kicker_kick_keys(source_df, min_year) if source_df is not None else set()
    zero_mask = suspect & keys.astype("string").isin(allowed_keys).eq(False)
    preserve_count = int(suspect.sum() - zero_mask.sum())

    if not zero_mask.any():
        _log(log_fn, f"  Kicking stat guard: preserved {preserve_count:,} raw-confirmed non-kicker kick rows")
        return df

    out = df.copy()
    zero_cols = [col for col in KICKING_ZERO_COLUMNS if col in out.columns]
    out.loc[zero_mask, zero_cols] = 0

    sample_cols = [
        col
        for col in (
            "player",
            "player_display_name",
            "NFL_player_id",
            "player_id",
            "position",
            "nfl_team",
            "team",
            "year",
            "season",
            "week",
        )
        if col in out.columns
    ]
    sample = out.loc[zero_mask, sample_cols].head(8).to_dict("records") if sample_cols else []
    _log(
        log_fn,
        "  Kicking stat guard: zeroed "
        f"{int(zero_mask.sum()):,} leaked non-kicker rows; preserved {preserve_count:,} raw-confirmed rows",
    )
    if sample:
        _log(log_fn, f"  Kicking stat guard sample: {sample}")
    return out
