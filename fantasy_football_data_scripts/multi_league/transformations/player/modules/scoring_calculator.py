"""
Scoring Calculator Module

Handles fantasy points calculation from scoring rules.

This module:
- Loads year-specific scoring rules from JSON
- Builds Polars expressions from scoring rules
- Applies scoring to player stats
- Handles stat column detection and normalization
- Uses position-aware column mapping to handle ambiguous stats
- Supports pre-calculated fantasy points from super_table (pts_* columns)
"""

import polars as pl
from pathlib import Path
from typing import Any
import json
import re

from multi_league.core.scoring_config import (
    YAHOO_STAT_MODIFIER_BONUS_KEY_MAP,
    yahoo_stat_modifier_bonus_source_threshold,
    yahoo_stat_modifier_bonus_stat_threshold,
)


# Track which unmapped stat warnings have been printed (to avoid spam)
_warned_unmapped_stats: set[frozenset] = set()


# ============================================================================
# PRE-CALCULATED FANTASY POINTS SUPPORT
# ============================================================================
# The super_table now has modular pre-calculated fantasy points by category:
#   pts_pass_4pt, pts_pass_6pt, pts_rush,
#   pts_rec_0ppr, pts_rec_half, pts_rec_ppr,
#   pts_misc, pts_k_std, pts_k_yds, pts_k_flat,
#   pts_def_std, pts_def_ya
#
# During league import, we analyze settings to determine which columns to sum.
# ============================================================================


# Precalc default values for each stat category.
# When a league's value differs from the default, a correction is applied at import time.
# Format: stat_key -> (nflverse_column, default_value)
# Note: pass_int and pass_yds defaults depend on which precalc column was chosen,
# so they are handled dynamically in get_scoring_columns().
PRECALC_STAT_DEFAULTS = {
    # Rushing/Receiving stats (same defaults for all precalc variants)
    "rush_td": ("rushing_tds", 6.0),
    "rush_yds": ("rushing_yards", 0.1),
    "rec_td": ("receiving_tds", 6.0),
    "rec_yds": ("receiving_yards", 0.1),
    # Fumbles lost: split across 3 component columns, each at -2 per fumble lost.
    # Corrections are applied per-component so the delta propagates correctly.
    # (No single 'total_fumbles_lost' column exists in super_table)
    "fum_lost_rush": ("rushing_fumbles_lost", -2.0),
    "fum_lost_sack": ("sack_fumbles_lost", -2.0),
    "fum_lost_rec": ("receiving_fumbles_lost", -2.0),
    # Completions: default 0.25 in pts_pass_cmp
    "cmp": ("completions", 0.25),
    # Rush attempts: default 0.1 in pts_rush_att
    "rush_att": ("carries", 0.1),
    # Sacks taken: default -1.0 in pts_sack_taken
    "sack_taken": ("sacks_suffered", -1.0),
    # Pass attempts: no precalc column, always a correction
    "pass_att": ("attempts", 0.0),
}


# === L1.b.1 component dispatch ===
# Maps (atomic_stat_key, multiplier) -> precompute col name OR runtime expression.
# Float fuzz from JSON serialization is snapped to canonical values within 1e-6.
# Per docs/superpowers/specs/2026-05-02-component-precompute-design.md.

_FLOAT_FUZZ_CANONICAL = [
    0.04,
    0.05,
    0.1,
    0.0667,
    0.0333,
    0.65,
    0.15,
    0.4,
    0.25,
    0.5,
    1.0,
    1.5,
    2.0,
    0.2,
    0.3,
    0.6,
    0.7,
    0.8,
    0.9,
]


def _snap_float_fuzz(mult: float, tol: float = 1e-6) -> float:
    """Snap a multiplier to its canonical value if within `tol`.

    Yahoo / Sleeper API JSON serialization causes 0.04 to be stored as
    0.03999999910593033, etc. Snapping keeps top-mult leagues on the
    precompute path instead of diverting to atomic-runtime fallback.
    See spec Appendix B.
    """
    for canonical in _FLOAT_FUZZ_CANONICAL:
        if abs(mult - canonical) < tol:
            return canonical
    return mult


# Maps (atomic_stat_key, snapped_mult) -> precompute col name. Anything not here
# falls through to atomic-runtime: `<atomic_col> * <mult>`.
_COMPONENT_LOOKUP: dict[tuple[str, float], str] = {
    ("pass_yd", 0.04): "pts_pass_yd_p04",
    ("pass_td", 4): "pts_pass_td_4",
    ("pass_td", 6): "pts_pass_td_6",
    ("pass_int", -2): "pts_pass_int_n2",
    ("pass_int", -1): "pts_pass_int_n1",
    ("rush_yd", 0.1): "pts_rush_yd_p1",
    ("rush_td", 6): "pts_rush_td_6",
    ("rec_yd", 0.1): "pts_rec_yd_p1",
    ("rec_td", 6): "pts_rec_td_6",
    ("rec", 1.0): "pts_rec_1",
    ("rec", 0.5): "pts_rec_p5",
    ("rec_te_bonus", 0.5): "pts_rec_te_bonus_p5",
    ("pass_2pt", 2): "pts_pass_2pt_2",
    ("rush_2pt", 2): "pts_rush_2pt_2",
    ("rec_2pt", 2): "pts_rec_2pt_2",
    ("fum_lost", -2): "pts_fum_lost_n2",
    ("fum_lost", -1): "pts_fum_lost_n1",
    ("pass_cmp", 0.25): "pts_pass_cmp_p25",
    ("pass_cmp", 0.1): "pts_pass_cmp_p1",
    ("pass_cmp", 0.5): "pts_pass_cmp_p5",
    ("rush_att", 0.1): "pts_rush_att_p1",
    ("rush_att", 0.2): "pts_rush_att_p2",
    ("rush_att", 0.25): "pts_rush_att_p25",
    ("pass_fd", 0.5): "pts_pass_fd_p5",
    ("pass_fd", 0.25): "pts_pass_fd_p25",
    ("rush_fd", 0.5): "pts_rush_fd_p5",
    ("rush_fd", 0.25): "pts_rush_fd_p25",
    ("rec_fd", 0.5): "pts_rec_fd_p5",
    ("rec_fd", 0.25): "pts_rec_fd_p25",
    ("pick6", -2): "pts_pick6_n2",
    ("pick6", -1): "pts_pick6_n1",
    ("sack_taken", -1): "pts_sack_taken_n1",
    ("sack_taken", -0.5): "pts_sack_taken_np5",
    ("st_td", 6): "pts_st_td_6",
    ("fum_ret_td", 6): "pts_fum_ret_td_6",
    ("kr_yd", 0.04): "pts_kr_yd_p04",
    ("pr_yd", 0.04): "pts_pr_yd_p04",
    ("pass_td_40plus", 2): "pts_pass_td_40plus_2",
    ("pass_td_50plus", 1): "pts_pass_td_50plus_1",
    ("rush_td_40plus", 2): "pts_rush_td_40plus_2",
    ("rush_td_50plus", 1): "pts_rush_td_50plus_1",
    ("rec_td_40plus", 1): "pts_rec_td_40plus_1",
    ("rec_td_50plus", 1): "pts_rec_td_50plus_1",
    ("pass_cmp_40plus", 1): "pts_pass_cmp_40plus_1",
    ("rush_40plus", 1): "pts_rush_40plus_1",
    ("rec_40plus", 1): "pts_rec_40plus_1",
    ("ret_yd", 0.04): "pts_ret_yds",
}


# Atomic source col(s) for runtime fallback when no precompute matches.
# Multi-source 'fum_lost' uses the parenthesized 3-col sum.
_ATOMIC_SOURCE: dict[str, str] = {
    "pass_yd": "passing_yards",
    "pass_att": "attempts",
    "pass_inc": "(COALESCE(attempts, 0) - COALESCE(completions, 0))",
    "pass_td": "passing_tds",
    "pass_int": "passing_interceptions",
    "rush_yd": "rushing_yards",
    "rush_td": "rushing_tds",
    "rec_yd": "receiving_yards",
    "rec_td": "receiving_tds",
    "rec": "receptions",
    "rec_te_bonus": "(CASE WHEN UPPER(COALESCE(position, '')) = 'TE' THEN COALESCE(receptions, 0) ELSE 0 END)",
    "rec_rb_bonus": "(CASE WHEN UPPER(COALESCE(position, '')) = 'RB' THEN COALESCE(receptions, 0) ELSE 0 END)",
    "rec_wr_bonus": "(CASE WHEN UPPER(COALESCE(position, '')) = 'WR' THEN COALESCE(receptions, 0) ELSE 0 END)",
    "rec_targets": "targets",
    "pass_2pt": "passing_2pt_conversions",
    "rush_2pt": "rushing_2pt_conversions",
    "rec_2pt": "receiving_2pt_conversions",
    "fum_lost": "(COALESCE(rushing_fumbles_lost, 0) + COALESCE(sack_fumbles_lost, 0) + COALESCE(receiving_fumbles_lost, 0))",
    "fum": "(COALESCE(rushing_fumbles, 0) + COALESCE(sack_fumbles, 0) + COALESCE(receiving_fumbles, 0))",
    "pass_cmp": "completions",
    "rush_att": "carries",
    "pass_fd": "passing_first_downs",
    "rush_fd": "rushing_first_downs",
    "rec_fd": "receiving_first_downs",
    "pick6": "pick6",
    "sack_taken": "sacks_suffered",
    "ret_yd": "(COALESCE(kickoff_return_yards, 0) + COALESCE(punt_return_yards, 0))",
    "def_st_yd": "dst_return_yards",
    "st_td": "special_teams_tds",
    "fum_ret_td": "fum_ret_td",
    "fum_ret_yd": "(COALESCE(fumble_recovery_yards_own, 0) + COALESCE(fumble_recovery_yards_opp, 0))",
    "int_ret_yd": "def_interception_yards",
    "kr_yd": "kickoff_return_yards",
    "pr_yd": "punt_return_yards",
    "pass_td_40plus": "passing_tds_40plus",
    "pass_td_50plus": "passing_tds_50plus",
    "rush_td_40plus": "rushing_tds_40plus",
    "rush_td_50plus": "rushing_tds_50plus",
    "rec_td_40plus": "receiving_tds_40plus",
    "rec_td_50plus": "receiving_tds_50plus",
    "pass_cmp_40plus": "completions_40plus",
    "pass_cmp_50plus": "completions_50plus",
    "rush_40plus": "rushing_40plus",
    "rec_40plus": "receptions_40plus",
    "rec_0_4": "receptions_0_4",
    "rec_5_9": "receptions_5_9",
    "rec_10_19": "receptions_10_19",
    "rec_20_29": "receptions_20_29",
    "rec_30_39": "receptions_30_39",
    "st_tkl_solo": "special_teams_tackles_solo",
}


_ATOMIC_SOURCE_COLUMNS = frozenset(
    {
        "attempts",
        "carries",
        "completions",
        "completions_40plus",
        "completions_50plus",
        "def_interception_yards",
        "dst_return_yards",
        "fumble_recovery_yards_opp",
        "fumble_recovery_yards_own",
        "fum_ret_td",
        "kickoff_return_yards",
        "passing_2pt_conversions",
        "passing_first_downs",
        "passing_interceptions",
        "passing_tds",
        "passing_tds_40plus",
        "passing_tds_50plus",
        "passing_yards",
        "pick6",
        "punt_return_yards",
        "receiving_2pt_conversions",
        "receiving_first_downs",
        "receiving_fumbles",
        "receiving_fumbles_lost",
        "receiving_tds",
        "receiving_tds_40plus",
        "receiving_tds_50plus",
        "receiving_yards",
        "receptions",
        "receptions_0_4",
        "receptions_5_9",
        "receptions_10_19",
        "receptions_20_29",
        "receptions_30_39",
        "receptions_40plus",
        "rush_40plus",
        "rushing_2pt_conversions",
        "rushing_40plus",
        "rushing_first_downs",
        "rushing_fumbles",
        "rushing_fumbles_lost",
        "rushing_tds",
        "rushing_tds_40plus",
        "rushing_tds_50plus",
        "rushing_yards",
        "sack_fumbles",
        "sack_fumbles_lost",
        "sacks_suffered",
        "special_teams_tackles_solo",
        "special_teams_tds",
        "targets",
    }
)


def _qualify_sql_columns(expr: str, table_alias: str | None = None, position_sql: str | None = None) -> str:
    """Qualify known super-table columns in a generated scoring SQL fragment."""
    qualified = expr
    if position_sql:
        qualified = re.sub(r"(?<![\.\w])position(?!\w)", position_sql, qualified)
    if not table_alias:
        return qualified
    for col in sorted(_ATOMIC_SOURCE_COLUMNS, key=len, reverse=True):
        qualified = re.sub(rf"(?<![\.\w]){re.escape(col)}(?!\w)", f"{table_alias}.{col}", qualified)
    return qualified


def _source_columns_in_expr(expr: str) -> set[str]:
    return {col for col in _ATOMIC_SOURCE_COLUMNS if re.search(rf"(?<![\.\w]){re.escape(col)}(?!\w)", expr)}


def _zero_missing_sql_columns(expr: str, available_columns: set[str] | None = None) -> str:
    """Replace missing optional `COALESCE(col, 0)` terms with literal zero."""
    if available_columns is None:
        return expr
    adjusted = expr
    for col in sorted(_source_columns_in_expr(expr) - available_columns, key=len, reverse=True):
        adjusted = re.sub(rf"COALESCE\(\s*{re.escape(col)}\s*,\s*0\s*\)", "0", adjusted)
    return adjusted


def _coalesce_column(col: str, table_alias: str | None = None) -> str:
    qualified = f"{table_alias}.{col}" if table_alias else col
    return f"COALESCE({qualified}, 0)"


def _pick_component(
    stat_key: str,
    mult: float,
    *,
    table_alias: str | None = None,
    position_sql: str | None = None,
    available_columns: set[str] | None = None,
) -> str | None:
    """Resolve `(stat_key, mult)` into a SQL expression.

    If the snapped multiplier matches a precompute, return ``COALESCE(<col>, 0)``.
    Otherwise return the runtime expression ``COALESCE(<atomic_col>, 0) * <mult>``.
    Multi-source atomics ('fum_lost') use a pre-COALESCE'd parenthesized sum.

    Raises ValueError for unknown stat_key.
    """
    snapped = _snap_float_fuzz(float(mult))
    hit = _COMPONENT_LOOKUP.get((stat_key, snapped))
    if hit is not None and (available_columns is None or hit in available_columns):
        return _coalesce_column(hit, table_alias)
    src = _ATOMIC_SOURCE.get(stat_key)
    if src is None:
        raise ValueError(f"Unknown stat_key for component dispatch: {stat_key}")
    if src.startswith("("):
        # Multi-source expressions are already wrapped in COALESCEs. If the
        # current super table lacks one optional source, score that source as 0.
        adjusted = _zero_missing_sql_columns(src, available_columns)
        if available_columns is not None and not (_source_columns_in_expr(adjusted) & available_columns):
            return None
        return f"{_qualify_sql_columns(adjusted, table_alias, position_sql)} * {snapped}"
    if available_columns is not None and src not in available_columns:
        return None
    return f"{_coalesce_column(src, table_alias)} * {snapped}"


# Sleeper scoring key -> _pick_component stat_key. Some keys differ slightly
# in name, e.g., pass_int_td (Sleeper) vs pick6 (component dispatch).
_SLEEPER_KEY_TO_STAT: dict[str, str] = {
    "pass_yd": "pass_yd",
    "pass_att": "pass_att",
    "pass_inc": "pass_inc",
    "pass_td": "pass_td",
    "pass_int": "pass_int",
    "rush_yd": "rush_yd",
    "rush_td": "rush_td",
    "rec_yd": "rec_yd",
    "rec_td": "rec_td",
    "rec": "rec",
    "bonus_rec_te": "rec_te_bonus",  # position-conditional
    "bonus_rec_rb": "rec_rb_bonus",  # position-conditional
    "bonus_rec_wr": "rec_wr_bonus",  # position-conditional
    "rec_targets": "rec_targets",
    "pass_2pt": "pass_2pt",
    "rush_2pt": "rush_2pt",
    "rec_2pt": "rec_2pt",
    "fum_lost": "fum_lost",
    "fum": "fum",
    "pass_cmp": "pass_cmp",
    "rush_att": "rush_att",
    "pass_fd": "pass_fd",
    "rush_fd": "rush_fd",
    "rec_fd": "rec_fd",
    "pass_int_td": "pick6",
    "pass_sack": "sack_taken",
    "st_yd": "ret_yd",
    "st_td": "st_td",
    "def_st_td": "st_td",  # alias
    "fum_rec_td": "fum_ret_td",
    "fum_ret_yd": "fum_ret_yd",
    "int_ret_yd": "int_ret_yd",
    "kr_yd": "kr_yd",
    "pr_yd": "pr_yd",
    "pass_td_40p": "pass_td_40plus",
    "pass_td_50p": "pass_td_50plus",
    "rush_td_40p": "rush_td_40plus",
    "rush_td_50p": "rush_td_50plus",
    "rec_td_40p": "rec_td_40plus",
    "rec_td_50p": "rec_td_50plus",
    "pass_cmp_40p": "pass_cmp_40plus",
    "pass_cmp_50p": "pass_cmp_50plus",
    "rush_40p": "rush_40plus",
    "rec_40p": "rec_40plus",
    "rec_40p_alt": "rec_40plus",
    "rec_0_4": "rec_0_4",
    "rec_5_9": "rec_5_9",
    "rec_10_19": "rec_10_19",
    "rec_20_29": "rec_20_29",
    "rec_30_39": "rec_30_39",
    "st_tkl_solo": "st_tkl_solo",
}


# Bonus flag: Sleeper scoring key -> super_table precompute col.
# Per-league SQL multiplies the binary 0/1 flag by the league-configured value.
_BONUS_KEY_TO_COL: dict[str, str] = {
    "bonus_pass_yd_300": "bonus_pass_300yd",
    "bonus_pass_yd_400": "bonus_pass_400yd",
    "bonus_rush_yd_100": "bonus_rush_100yd",
    "bonus_rush_yd_200": "bonus_rush_200yd",
    "bonus_rec_yd_100": "bonus_rec_100yd",
    "bonus_rec_yd_200": "bonus_rec_200yd",
    "bonus_rush_rec_yd_100": "bonus_rush_rec_100yd",
    "bonus_rush_rec_yd_200": "bonus_rush_rec_200yd",
    "bonus_pass_cmp_25": "bonus_pass_25cmp",
    "bonus_rush_att_20": "bonus_rush_20att",
    "bonus_rec_10": "bonus_rec_10rec",
}

_BONUS_THRESHOLD_KEY_TO_SOURCE: dict[str, tuple[str, float]] = {
    canonical_key: parsed
    for canonical_key in YAHOO_STAT_MODIFIER_BONUS_KEY_MAP.values()
    if canonical_key not in _BONUS_KEY_TO_COL
    if (parsed := yahoo_stat_modifier_bonus_source_threshold(canonical_key)) is not None
}


_YAHOO_KEY_TO_STAT_ID: dict[str, str] = {
    "pass_att": "1",
    "pass_cmp": "2",
    "pass_inc": "3",
    "pass_yd": "4",
    "pass_td": "5",
    "pass_int": "6",
    "pass_sack": "7",
    "rush_att": "8",
    "rush_yd": "9",
    "rush_td": "10",
    "rec": "11",
    "rec_yd": "12",
    "rec_td": "13",
    "st_yd": "14",
    "st_td": "15",
    # Yahoo exposes a single untyped 2-point conversion stat.
    "pass_2pt": "16",
    "rush_2pt": "16",
    "rec_2pt": "16",
    "fum": "17",
    "fum_lost": "18",
    "fgm_0_19": "19",
    "fgm_20_29": "20",
    "fgm_30_39": "21",
    "fgm_40_49": "22",
    "fgm_50p": "23",
    "fgmiss_0_19": "24",
    "fgmiss_20_29": "25",
    "fgmiss_30_39": "26",
    "fgmiss_40_49": "27",
    "fgmiss_50p": "28",
    "xpm": "29",
    "xpmiss": "30",
    "sack": "32",
    "int": "33",
    "fum_rec": "34",
    "def_td": "35",
    "safe": "36",
    "blk_kick": "37",
    "idp_tkl_solo": "38",
    "idp_tkl_ast": "39",
    "idp_sack": "40",
    "idp_int": "41",
    "idp_ff": "42",
    "idp_fum_rec": "43",
    "idp_def_td": "44",
    "idp_safe": "45",
    "idp_pass_def": "46",
    "idp_blk_kick": "47",
    "def_st_yd": "48",
    "def_st_td": "49",
    "pts_allow_0": "50",
    "pts_allow_1_6": "51",
    "pts_allow_7_13": "52",
    "pts_allow_14_20": "53",
    "pts_allow_21_27": "54",
    "pts_allow_28_34": "55",
    "pts_allow_35p": "56",
    "fum_rec_td": "57",
    "pass_int_td": "58",
    "pass_cmp_40p": "59",
    "pass_td_40p": "60",
    "rush_40p": "61",
    "rush_td_40p": "62",
    "rec_40p": "63",
    "rec_td_40p": "64",
    "idp_tkl_loss": "65",
    "int_ret_yd": "66",
    "fum_ret_yd": "66",
    "def_4_and_stop": "67",
    "tkl_loss": "68",
    "yds_allow_neg": "70",
    "yds_allow_0_100": "71",
    "yds_allow_100_199": "72",
    "yds_allow_200_299": "73",
    "yds_allow_300_349": "74",
    "yds_allow_400_449": "75",
    "yds_allow_500_549": "76",
    "def_3_and_out": "77",
    "rec_targets": "78",
    "pass_fd": "79",
    "rec_fd": "80",
    "rush_fd": "81",
    "def_2pt": "82",
    "idp_xpr": "83",
    "fgm_yds": "84",
}

_YAHOO_BONUS_KEY_TO_STAT_THRESHOLD: dict[str, tuple[str, int]] = {
    canonical_key: (stat_id, target) for (stat_id, target), canonical_key in YAHOO_STAT_MODIFIER_BONUS_KEY_MAP.items()
}

_YAHOO_POSITION_BONUS_KEY_TO_POSITION: dict[str, str] = {
    "bonus_rec_te": "TE",
    "bonus_rec_rb": "RB",
    "bonus_rec_wr": "WR",
}


def _yahoo_stat_col(stat_id: str) -> str:
    return f"yahoo_stat_{stat_id}"


def _threshold_bonus_sources_for_settings(settings: dict) -> dict[str, tuple[str, int]]:
    """Return observed and custom threshold bonuses required by these settings."""
    threshold_keys = dict(_BONUS_THRESHOLD_KEY_TO_SOURCE)
    for sleeper_key, value in settings.items():
        if value in (None, 0) or sleeper_key in _BONUS_KEY_TO_COL:
            continue
        parsed = yahoo_stat_modifier_bonus_source_threshold(sleeper_key)
        if parsed is not None:
            threshold_keys[sleeper_key] = parsed
    return threshold_keys


def _yahoo_bonus_stat_thresholds_for_settings(settings: dict) -> dict[str, tuple[str, int]]:
    """Return observed and custom Yahoo native stat bonus thresholds for settings."""
    threshold_keys = dict(_YAHOO_BONUS_KEY_TO_STAT_THRESHOLD)
    for sleeper_key, value in settings.items():
        if value in (None, 0):
            continue
        parsed = yahoo_stat_modifier_bonus_stat_threshold(sleeper_key)
        if parsed is not None:
            threshold_keys[sleeper_key] = parsed
    return threshold_keys


def _pick_threshold_bonus(
    stat_key: str,
    threshold: float,
    bonus_value: float,
    *,
    table_alias: str | None = None,
    position_sql: str | None = None,
    available_columns: set[str] | None = None,
) -> str | None:
    stat_expr = _pick_component(
        stat_key,
        1.0,
        table_alias=table_alias,
        position_sql=position_sql,
        available_columns=available_columns,
    )
    if stat_expr is None:
        return None
    return f"CASE WHEN {stat_expr} >= {threshold} THEN {float(bonus_value)} ELSE 0.0 END"


def build_yahoo_stat_id_fantasy_points_sql(
    scoring_rules: dict,
    *,
    table_alias: str | None = None,
    position_sql: str | None = None,
    available_columns: set[str] | None = None,
) -> str | None:
    """Build fantasy_points SQL from Yahoo's roster `player_stats` stat IDs.

    Yahoo team roster rows expose platform-native stat IDs alongside official
    player_points. For rostered Yahoo players, those stat IDs are the closest
    raw substrate to Yahoo's team total, especially for IDP and return stats
    where nflverse/super_table frequently disagrees with Yahoo's feed.

    This returns None when the current player_fantasy table does not contain
    Yahoo stat columns, letting callers fall back to the super_table scorer.
    """
    settings = scoring_rules.get("scoring_settings") or scoring_rules
    parts: list[str] = []
    seen_stat_ids: set[str] = set()

    for sleeper_key, stat_id in _YAHOO_KEY_TO_STAT_ID.items():
        value = settings.get(sleeper_key)
        if value is None or value == 0:
            continue
        col = _yahoo_stat_col(stat_id)
        if available_columns is not None and col not in available_columns:
            continue
        # Several canonical keys can map to one Yahoo stat ID, notably 2-PT.
        if stat_id in seen_stat_ids:
            continue
        seen_stat_ids.add(stat_id)
        parts.append(f"{_coalesce_column(col, table_alias)} * {float(value)}")

    for sleeper_key, (stat_id, threshold) in _yahoo_bonus_stat_thresholds_for_settings(settings).items():
        value = settings.get(sleeper_key)
        if value is None or value == 0:
            continue
        col = _yahoo_stat_col(stat_id)
        if available_columns is not None and col not in available_columns:
            continue
        stat_expr = _coalesce_column(col, table_alias)
        parts.append(f"CASE WHEN {stat_expr} >= {threshold} THEN {float(value)} ELSE 0.0 END")

    rec_col = _yahoo_stat_col("11")
    if available_columns is None or rec_col in available_columns:
        pos_expr = position_sql or ("position" if not table_alias else f"{table_alias}.position")
        rec_expr = _coalesce_column(rec_col, table_alias)
        for sleeper_key, position in _YAHOO_POSITION_BONUS_KEY_TO_POSITION.items():
            value = settings.get(sleeper_key)
            if value is None or value == 0:
                continue
            parts.append(
                f"CASE WHEN UPPER(COALESCE({pos_expr}, '')) = '{position}' "
                f"THEN {rec_expr} * {float(value)} ELSE 0.0 END"
            )

    if not parts:
        return None
    return "(\n  " + "\n  + ".join(parts) + "\n)"


def build_components_fantasy_points_sql(
    scoring_rules: dict,
    *,
    table_alias: str | None = None,
    position_sql: str | None = None,
    include_bonus_flags: bool = True,
    include_position_bonuses: bool = True,
    available_columns: set[str] | None = None,
) -> str:
    """Build the per-league `fantasy_points` SQL expression as a sum of resolved
    component expressions.

    Returns a SQL fragment like:
        (
          COALESCE(pts_pass_yd_p04, 0)
          + COALESCE(pts_pass_td_4, 0)
          + COALESCE(pts_pass_int_n2, 0)
          + ...
        )

    Empty rules return "0.0".

    **Input contract:** `scoring_rules` is a Sleeper-flat dict (or has a
    `scoring_settings` sub-dict containing one). Format:
        {"pass_td": 4, "pass_yd": 0.04, "rec": 0.5, ...}

    Callers reading from `{league}.public.league_settings` flat DDL must
    strip the `scoring_` prefix from column names before calling. Yahoo
    `stat_id`-based and ESPN-native scoring data are normalized into the
    same DDL `scoring_*` columns at import time, so by the time this
    function runs the platform distinction is gone.
    """
    settings = scoring_rules.get("scoring_settings") or scoring_rules

    parts: list[str] = []

    # Component-dispatchable scoring keys. Dedupe by resolved stat_key so aliases
    # (e.g., st_td and def_st_td both -> st_td) emit the term once. First value
    # wins on multiplier conflicts, which protects imports from duplicated DDL
    # aliases without double-counting the underlying NFL event.
    seen_dispatch: set[str] = set()
    for sleeper_key, stat_key in _SLEEPER_KEY_TO_STAT.items():
        if sleeper_key in {"kr_yd", "pr_yd"} and settings.get("st_yd") not in (None, 0):
            continue
        if not include_position_bonuses and sleeper_key in {"bonus_rec_te", "bonus_rec_rb", "bonus_rec_wr"}:
            continue
        v = settings.get(sleeper_key)
        if v is None or v == 0:
            continue
        if stat_key in seen_dispatch:
            continue
        seen_dispatch.add(stat_key)
        expr = _pick_component(
            stat_key,
            v,
            table_alias=table_alias,
            position_sql=position_sql,
            available_columns=available_columns,
        )
        if expr is not None:
            parts.append(expr)

    # Bonus flags — direct precompute_col * league_value
    # Threshold bonuses that are not globally precomputed.
    for sleeper_key, (stat_key, threshold) in _threshold_bonus_sources_for_settings(settings).items():
        v = settings.get(sleeper_key)
        if v is None or v == 0:
            continue
        expr = _pick_threshold_bonus(
            stat_key,
            threshold,
            v,
            table_alias=table_alias,
            position_sql=position_sql,
            available_columns=available_columns,
        )
        if expr is not None:
            parts.append(expr)

    if include_bonus_flags:
        for sleeper_key, bonus_col in _BONUS_KEY_TO_COL.items():
            v = settings.get(sleeper_key)
            if v is None or v == 0:
                continue
            if available_columns is not None and bonus_col not in available_columns:
                continue
            parts.append(f"{_coalesce_column(bonus_col, table_alias)} * {v}")

    if not parts:
        return "0.0"

    # Whole-point-bucket scoring (uses_fractional_points = 0, the pre-~2014 Yahoo
    # era): each stat's point contribution is floored to a whole number before
    # summing — e.g. 181 passing yards at 0.02/yd scores FLOOR(3.62) = 3, not 3.62.
    # The settings rate is identical to the decimal era, so this flag is the only
    # thing that distinguishes the two rulesets. Flooring per component matches
    # Yahoo's API points exactly (verified to the cent on real seasons); integer
    # components (TDs, INTs) are unchanged by FLOOR, so this is safe to apply to
    # every part. Default decimal (no floor) when the flag is absent.
    fractional = settings.get("uses_fractional_points", True)
    if isinstance(fractional, str):
        fractional = fractional.strip().lower() not in {"0", "false", "no", ""}
    if not fractional:
        parts = [f"FLOOR({p})" for p in parts]

    return "(\n  " + "\n  + ".join(parts) + "\n)"


def compute_fantasy_points_sql_from_settings_row(
    row: dict,
    *,
    table_alias: str | None = None,
    position_sql: str | None = None,
    include_bonus_flags: bool = True,
    include_position_bonuses: bool = True,
    available_columns: set[str] | None = None,
) -> str:
    """Build the per-league fantasy_points SQL expression from a league_settings DDL row.

    The flat-DDL `___leagues.public.league_settings` (or `{league}.public.league_settings`)
    has keys like:
        scoring_pass_yd, scoring_pass_td, scoring_rec, scoring_bonus_rec_te, ...
        bonus_pass_yd_300, bonus_rec_yd_100, ...
        year, platform, league_key, num_teams, roster_qb, scoring_type, uses_median, ...

    This helper:
      1. Strips the `scoring_` prefix from every key starting with it.
      2. Passes `bonus_*` keys through unchanged (already in Sleeper-flat form).
      3. Drops every other key (year, platform, num_teams, roster_*, etc.).
      4. Drops NULL / NaN / zero values (no contribution to fantasy_points).
      5. Calls build_components_fantasy_points_sql with the cleaned Sleeper-flat dict.

    Returns a SQL fragment ready to embed in `fantasy_points = <expr>`.
    Empty / no-scoring rows return "0.0".

    Yahoo, Sleeper, and ESPN imports all normalize their platform-native scoring
    formats into the same `scoring_*` DDL columns at import time, so this helper
    is platform-agnostic.
    """
    import math

    from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row

    sleeper_flat = extract_scoring_settings_from_flat_row(row)
    for key, val in row.items():
        if not isinstance(key, str) or not key.startswith("bonus_"):
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if math.isnan(v) or v == 0:
            continue
        sleeper_flat[key] = v

    return build_components_fantasy_points_sql(
        sleeper_flat,
        table_alias=table_alias,
        position_sql=position_sql,
        include_bonus_flags=include_bonus_flags,
        include_position_bonuses=include_position_bonuses,
        available_columns=available_columns,
    )


def get_scoring_columns(scoring_rules: dict[str, Any]) -> dict[str, Any]:
    """
    Analyze league settings and return which pre-calc columns to sum,
    plus any corrections needed for edge-case scoring values.

    This examines the scoring rules to detect:
    - Passing TD points (4pt vs 5pt vs 6pt)
    - INT penalty (-1 vs -2)
    - PPR value (0, 0.5, or 1.0)
    - Optional categories (completions, rush attempts, first downs, sacks taken)
    - Kicker scoring style (Yahoo/bucket vs per-yard)
    - Defense yards allowed scoring
    - Corrections for non-standard multipliers

    Args:
        scoring_rules: Dict containing either:
            - Yahoo format: "scoring" key with list of rules
            - Sleeper format: "scoring_settings" key with flat dict like {'rec': 0.5, 'pass_td': 4}

    Returns:
        Dict mapping category to column name, plus 'corrections' list:
        {
            'pass': 'pts_pass_4pt',
            'rush': 'pts_rush',
            'rec': 'pts_rec_half',
            'misc': 'pts_misc',
            'kick': 'pts_k_std',
            'def': 'pts_def_std',
            'idp': 'pts_idp_std',
            'ret_yds': 'pts_ret_yds' or None,
            'rec_tep': 'pts_rec_tep' or None,
            'cmp': 'pts_pass_cmp' or None,
            'rush_att': 'pts_rush_att' or None,
            'first_downs': 'pts_first_downs' or None,
            'sack_taken': 'pts_sack_taken' or None,
            'corrections': [(nflverse_col, delta_multiplier), ...]
        }
    """
    columns = {}

    raw_yahoo_rules = scoring_rules.get("scoring")
    if isinstance(raw_yahoo_rules, list) and any(rule.get("stat_id") for rule in raw_yahoo_rules):
        from multi_league.core.scoring_config import normalize_yahoo

        canonical_scoring = normalize_yahoo(raw_yahoo_rules)
        return get_scoring_columns({"scoring_settings": canonical_scoring})

    # Check for Sleeper format (flat dict with 'scoring_settings' or direct 'rec'/'pass_td' keys)
    sleeper_settings = scoring_rules.get("scoring_settings", {})
    if sleeper_settings or "rec" in scoring_rules or "pass_td" in scoring_rules:
        # Use Sleeper format - flat dict with stat names as keys
        settings = sleeper_settings if sleeper_settings else scoring_rules

        # === Detect passing TD points and INT value (Sleeper uses 'pass_td', 'pass_int') ===
        pass_td_pts = settings.get("pass_td", 4)
        pass_int_pts = settings.get("pass_int", -2)

        # Choose pass column based on TD pts AND INT value
        if pass_int_pts >= -1.5:
            # Use INT -1 variant (closer to -1 than -2)
            if pass_td_pts >= 6:
                columns["pass"] = "pts_pass_6pt_int1"
                pass_td_precalc_default = 6.0
            elif pass_td_pts >= 5:
                columns["pass"] = "pts_pass_5pt_int1"
                pass_td_precalc_default = 5.0
            else:
                columns["pass"] = "pts_pass_4pt_int1"
                pass_td_precalc_default = 4.0
            int_precalc_default = -1.0
        else:
            # Use standard INT -2 variant
            if pass_td_pts >= 6:
                columns["pass"] = "pts_pass_6pt"
                pass_td_precalc_default = 6.0
            elif pass_td_pts >= 5:
                columns["pass"] = "pts_pass_5pt"
                pass_td_precalc_default = 5.0
            else:
                columns["pass"] = "pts_pass_4pt"
                pass_td_precalc_default = 4.0
            int_precalc_default = -2.0

        # === Rushing is always the same ===
        columns["rush"] = "pts_rush"

        # === Detect PPR value (Sleeper uses 'rec') ===
        ppr = settings.get("rec", 0.0)
        if ppr >= 0.75:
            columns["rec"] = "pts_rec_ppr"
            ppr_precalc_default = 1.0
        elif ppr >= 0.25:
            columns["rec"] = "pts_rec_half"
            ppr_precalc_default = 0.5
        else:
            columns["rec"] = "pts_rec_0ppr"
            ppr_precalc_default = 0.0

        # === Misc is always the same ===
        columns["misc"] = "pts_misc"

        # === Detect kicker scoring style ===
        # Sleeper uses fg_yds for per-yard scoring
        if "fg_yds" in settings or "fgm_yds_over_30" in settings:
            columns["kick"] = "pts_k_yds"
        else:
            columns["kick"] = "pts_k_std"

        # === Detect return yards scoring ===
        # Unified ST yards can use the precomputed aggregate at the common
        # 0.04/yard multiplier. Split KR/PR yards stay in runtime corrections
        # so we do not accidentally apply a KR-only rule to punt returns.
        st_yd_setting = settings.get("st_yd", settings.get("ret_yd"))
        columns["ret_yds"] = (
            "pts_ret_yds" if st_yd_setting is not None and abs(float(st_yd_setting or 0) - 0.04) <= 0.001 else None
        )

        # === Detect TE Premium ===
        # Sleeper uses 'bonus_rec_te' or higher rec value for TEs
        te_bonus = settings.get("bonus_rec_te", 0)
        te_ppr = settings.get("rec_te", settings.get("rec", 0))
        has_te_premium = te_bonus >= 0.25 or te_ppr >= 1.25
        columns["rec_tep"] = "pts_rec_tep" if has_te_premium else None
        columns["rb_premium"] = float(settings.get("bonus_rec_rb", 0) or 0)
        columns["wr_premium"] = float(settings.get("bonus_rec_wr", 0) or 0)

        # === Detect optional category columns ===
        # Completions
        cmp_pts = settings.get("pass_cmp", 0)
        columns["cmp"] = "pts_pass_cmp" if cmp_pts != 0 else None

        # Rush attempts
        rush_att_pts = settings.get("rush_att", 0)
        columns["rush_att"] = "pts_rush_att" if rush_att_pts != 0 else None

        # First downs (rush + receiving)
        rush_fd_pts = settings.get("rush_fd", 0)
        rec_fd_pts = settings.get("rec_fd", 0)
        columns["first_downs"] = "pts_first_downs" if (rush_fd_pts != 0 or rec_fd_pts != 0) else None

        # Sacks taken
        sack_taken_pts = settings.get("pass_sack", 0)
        columns["sack_taken"] = "pts_sack_taken" if sack_taken_pts != 0 else None

        # === Defense scoring ===
        # Always use pts_def_std as base; corrections handle non-standard values.
        # pts_def_std defaults: sack×1, INT×2, FF×1, FR×2, TD×6, FumTD×6, Safety×2, Block×2
        # + PA tiers: 10/7/4/1/0/-1/-4
        # Stats NOT in pts_def_std (default 0): TFL, 3-and-outs, 4th-down stops
        columns["def"] = "pts_def_std"

        # === IDP scoring (if league has IDP positions) ===
        # Detect IDP scoring variant from tackle/sack/int point values
        idp_tackle = settings.get("idp_tkl", settings.get("tkl", 1.0))
        idp_sack = settings.get("idp_sack", settings.get("sack", 2.0))
        idp_int = settings.get("idp_int", settings.get("def_int", 3.0))

        # Classify IDP scoring variant
        if idp_tackle <= 0.75 and idp_sack >= 3.5:
            columns["idp"] = "pts_idp_big_play"
        elif idp_tackle >= 1.75:
            columns["idp"] = "pts_idp_tackle_heavy"
        elif idp_tackle >= 1.25 or idp_sack >= 2.5 or idp_int >= 3.5:
            columns["idp"] = "pts_idp_premium"
        else:
            columns["idp"] = "pts_idp_std"

        # === Build corrections for non-standard multipliers ===
        # Collect league values that have precalc defaults
        league_values = {}

        # Pass INT: correct if league value != precalc default (-1 or -2)
        if pass_int_pts != int_precalc_default:
            league_values["pass_int_correction"] = pass_int_pts
        if pass_td_pts != pass_td_precalc_default:
            league_values["pass_td_correction"] = pass_td_pts
        # Pass yards: precalc uses 0.04/yd
        pass_yds_pts = settings.get("pass_yd", 0.04)
        if abs(pass_yds_pts - 0.04) > 0.001:
            league_values["pass_yds_correction"] = pass_yds_pts

        # PPR: correct if league value != precalc bucket
        if abs(ppr - ppr_precalc_default) > 0.001:
            league_values["ppr_correction"] = ppr

        # Standard stats from PRECALC_STAT_DEFAULTS
        sleeper_to_stat_key = {
            "rush_td": "rush_td",
            "rush_yd": "rush_yds",
            "rec_td": "rec_td",
            "rec_yd": "rec_yds",
            "pass_cmp": "cmp",
            "rush_att": "rush_att",
            "pass_sack": "sack_taken",
            "pass_att": "pass_att",
        }
        for sleeper_key, stat_key in sleeper_to_stat_key.items():
            val = settings.get(sleeper_key)
            if val is not None and stat_key in PRECALC_STAT_DEFAULTS:
                nflverse_col, default_val = PRECALC_STAT_DEFAULTS[stat_key]
                # Only add correction if this stat has a precalc column AND value differs,
                # OR if it has no precalc column (default 0) and league uses it
                has_precalc = stat_key in ("cmp", "rush_att", "sack_taken")
                if has_precalc and columns.get(stat_key) is not None:
                    # Precalc column is being summed; correct if value differs from default
                    if abs(val - default_val) > 0.001:
                        league_values[stat_key] = val
                elif not has_precalc:
                    # No precalc column; correct if value differs from default
                    if abs(val - default_val) > 0.001:
                        league_values[stat_key] = val

        # Fumbles lost: applied to all 3 component columns if value differs from -2
        fum_lost_val = settings.get("fum_lost", -2.0)
        if abs(fum_lost_val - (-2.0)) > 0.001:
            league_values["fum_lost_rush"] = fum_lost_val
            league_values["fum_lost_sack"] = fum_lost_val
            league_values["fum_lost_rec"] = fum_lost_val

        # Fumble (any, not just lost): no precalc, needs separate correction per column
        fum_any_val = settings.get("fum", 0)

        # Build the corrections list
        corrections = []
        # Handle special cases first
        if "pass_int_correction" in league_values:
            corrections.append(("passing_interceptions", league_values["pass_int_correction"] - int_precalc_default))
        if "pass_td_correction" in league_values:
            corrections.append(("passing_tds", league_values["pass_td_correction"] - pass_td_precalc_default))
        if "pass_yds_correction" in league_values:
            corrections.append(("passing_yards", league_values["pass_yds_correction"] - 0.04))
        if "ppr_correction" in league_values:
            corrections.append(("receptions", league_values["ppr_correction"] - ppr_precalc_default))

        # Handle standard stats
        for stat_key in (
            "rush_td",
            "rush_yds",
            "rec_td",
            "rec_yds",
            "fum_lost_rush",
            "fum_lost_sack",
            "fum_lost_rec",
            "cmp",
            "rush_att",
            "sack_taken",
            "pass_att",
        ):
            if stat_key in league_values:
                nflverse_col, default_val = PRECALC_STAT_DEFAULTS[stat_key]
                corrections.append((nflverse_col, league_values[stat_key] - default_val))

        # Fumble (any): apply to all offensive fumble counters.
        if abs(fum_any_val) > 0.001:
            corrections.append(("rushing_fumbles", fum_any_val))
            corrections.append(("sack_fumbles", fum_any_val))
            corrections.append(("receiving_fumbles", fum_any_val))

        runtime_atomic_sources = {
            "pass_inc": [("attempts", 1.0), ("completions", -1.0)],
            "rec_targets": [("targets", 1.0)],
            "pass_cmp_40p": [("completions_40plus", 1.0)],
            "pass_cmp_50p": [("completions_50plus", 1.0)],
            "pass_td_40p": [("passing_tds_40plus", 1.0)],
            "pass_td_50p": [("passing_tds_50plus", 1.0)],
            "rush_40p": [("rushing_40plus", 1.0)],
            "rush_td_40p": [("rushing_tds_40plus", 1.0)],
            "rush_td_50p": [("rushing_tds_50plus", 1.0)],
            "rec_40p": [("receptions_40plus", 1.0)],
            "rec_40p_alt": [("receptions_40plus", 1.0)],
            "rec_td_40p": [("receiving_tds_40plus", 1.0)],
            "rec_td_50p": [("receiving_tds_50plus", 1.0)],
            "rec_0_4": [("receptions_0_4", 1.0)],
            "rec_5_9": [("receptions_5_9", 1.0)],
            "rec_10_19": [("receptions_10_19", 1.0)],
            "rec_20_29": [("receptions_20_29", 1.0)],
            "rec_30_39": [("receptions_30_39", 1.0)],
            "pass_fd": [("passing_first_downs", 1.0)],
            "pass_int_td": [("pick6", 1.0)],
            "int_ret_yd": [("def_interception_yards", 1.0)],
            "fum_ret_yd": [("fumble_recovery_yards_own", 1.0), ("fumble_recovery_yards_opp", 1.0)],
            "st_tkl_solo": [("special_teams_tackles_solo", 1.0)],
        }
        seen_runtime_dispatch: set[tuple[tuple[tuple[str, float], ...], float]] = set()
        for sleeper_key, sources in runtime_atomic_sources.items():
            val = settings.get(sleeper_key)
            if val is None or abs(float(val)) <= 0.001:
                continue
            dispatch_key = (tuple(sources), _snap_float_fuzz(float(val)))
            if dispatch_key in seen_runtime_dispatch:
                continue
            seen_runtime_dispatch.add(dispatch_key)
            for col, factor in sources:
                corrections.append((col, float(val) * factor))

        for sleeper_key, col, default in (
            ("pass_2pt", "passing_2pt_conversions", 2.0),
            ("rush_2pt", "rushing_2pt_conversions", 2.0),
            ("rec_2pt", "receiving_2pt_conversions", 2.0),
            ("st_td", "special_teams_tds", 6.0),
            ("fum_rec_td", "fum_ret_td", 6.0),
        ):
            if sleeper_key == "st_td":
                val = settings.get("st_td", settings.get("def_st_td", 0.0))
            else:
                val = settings.get(sleeper_key, 0.0)
            if abs(float(val) - default) > 0.001:
                corrections.append((col, float(val) - default))

        st_yd_val = settings.get("st_yd", settings.get("ret_yd"))
        if st_yd_val is not None and abs(float(st_yd_val)) > 0.001:
            delta = float(st_yd_val) - (0.04 if columns.get("ret_yds") is not None else 0.0)
            if abs(delta) > 0.001:
                corrections.append(("kickoff_return_yards", delta))
                corrections.append(("punt_return_yards", delta))
        else:
            kr_yd_val = settings.get("kr_yd")
            pr_yd_val = settings.get("pr_yd")
            if kr_yd_val is not None and abs(float(kr_yd_val)) > 0.001:
                corrections.append(("kickoff_return_yards", float(kr_yd_val)))
            if pr_yd_val is not None and abs(float(pr_yd_val)) > 0.001:
                corrections.append(("punt_return_yards", float(pr_yd_val)))

        # First downs: special handling (two stats combined into one precalc)
        if columns.get("first_downs") is not None:
            # First downs precalc uses 1.0 per FD. If league differs, correct each separately.
            if abs(rush_fd_pts - 1.0) > 0.001:
                corrections.append(("rushing_first_downs", rush_fd_pts - 1.0))
            if abs(rec_fd_pts - 1.0) > 0.001:
                corrections.append(("receiving_first_downs", rec_fd_pts - 1.0))

        # === DEF modular multipliers ===
        # Each pts_def_* component column stores the raw event count (×1).
        # def_multipliers maps each component to the league's point value.
        # SQL/pandas multiplies each component by its multiplier — no corrections needed.
        #
        # ST TD aliases (def_st_td / st_td) map to pts_def_ret_td which is the
        # aggregated special_teams_tds column at the team/week level (see
        # fantasy_points_calculator.py ~line 510 for the aggregation). Leagues
        # that set BOTH def_td and def_st_td want ST TDs scored separately from
        # defensive INT/fumble return TDs. We honor whichever is set.
        DEF_COMPONENT_DEFAULTS = {
            # sleeper_key -> (pts_def_component_col, default_multiplier)
            "sack": ("pts_def_sack", 1.0),
            "int": ("pts_def_int", 2.0),
            "ff": ("pts_def_ff", 0.0),
            "fum_rec": ("pts_def_fr", 2.0),
            "def_td": ("pts_def_td", 6.0),
            "safe": ("pts_def_safety", 2.0),
            "one_pt_safe": ("pts_def_safety", 1.0),
            "blk_kick": ("pts_def_block", 2.0),
            "tfl": ("pts_def_tfl", 0.0),
            "tkl_loss": ("pts_def_tfl", 0.0),  # Sleeper alias for tfl
            "def_3_and_out": ("pts_def_3out", 0.0),
            "def_4_and_stop": ("pts_def_4stop", 0.0),
            # Special teams TD scoring (common in dynasty/IDP leagues)
            # Sleeper uses both def_st_td and st_td — treat as aliases.
            # Both map to pts_def_ret_td which stores team-aggregated ST TDs.
            "def_st_td": ("pts_def_ret_td", 0.0),
            "st_td": ("pts_def_ret_td", 0.0),
            # ESPN team result / score / margin scoring for DST rows.
            "team_win": ("pts_def_team_win", 0.0),
            "team_loss": ("pts_def_team_loss", 0.0),
            "team_tie": ("pts_def_team_tie", 0.0),
            "team_pts": ("pts_def_team_pts", 0.0),
            "team_margin": ("pts_def_team_margin", 0.0),
            "team_win_margin_25p": ("pts_def_team_win_margin_25p", 0.0),
            "team_win_margin_20_24": ("pts_def_team_win_margin_20_24", 0.0),
            "team_win_margin_15_19": ("pts_def_team_win_margin_15_19", 0.0),
            "team_win_margin_10_14": ("pts_def_team_win_margin_10_14", 0.0),
            "team_win_margin_5_9": ("pts_def_team_win_margin_5_9", 0.0),
            "team_win_margin_1_4": ("pts_def_team_win_margin_1_4", 0.0),
            "team_loss_margin_1_4": ("pts_def_team_loss_margin_1_4", 0.0),
            "team_loss_margin_5_9": ("pts_def_team_loss_margin_5_9", 0.0),
            "team_loss_margin_10_14": ("pts_def_team_loss_margin_10_14", 0.0),
            "team_loss_margin_15_19": ("pts_def_team_loss_margin_15_19", 0.0),
            "team_loss_margin_20_24": ("pts_def_team_loss_margin_20_24", 0.0),
            "team_loss_margin_25p": ("pts_def_team_loss_margin_25p", 0.0),
        }
        # Points-allowed scoring supports TWO mutually exclusive modes:
        #   1. Bracket mode: pts_allow_0 / pts_allow_1_6 / ... / pts_allow_35p
        #      (integer points awarded per bracket)
        #   2. Cumulative mode: pts_allow (float points per point allowed;
        #      typically negative like -0.1 or -0.25)
        # Sleeper leagues can use either. If the league sets `pts_allow`
        # (cumulative) we zero out bracket defaults to avoid double-counting.
        # If the league sets any bracket explicitly, we use brackets only.
        PA_TIER_DEFAULTS = {
            "pts_allow_0": ("pts_allow_0", 10.0),
            "pts_allow_1_6": ("pts_allow_1_6", 7.0),
            "pts_allow_7_13": ("pts_allow_7_13", 4.0),
            "pts_allow_14_20": ("pts_allow_14_20", 1.0),
            "pts_allow_21_27": ("pts_allow_21_27", 0.0),
            "pts_allow_28_34": ("pts_allow_28_34", -1.0),
            "pts_allow_35p": ("pts_allow_35_plus", -4.0),
        }
        YA_TIER_DEFAULTS = {
            # Fine-grained tiers (map directly to super table columns)
            "yds_allow_0_100": ("yds_allow_0_99", 0.0),
            "yds_allow_0_99": ("yds_allow_0_99", 0.0),
            "yds_allow_100_199": ("yds_allow_100_199", 0.0),
            "yds_allow_200_299": ("yds_allow_200_299", 0.0),
            "yds_allow_300_349": ("yds_allow_300_349", 0.0),
            "yds_allow_350_399": ("yds_allow_350_399", 0.0),
            "yds_allow_400_449": ("yds_allow_400_449", 0.0),
            "yds_allow_450_499": ("yds_allow_450_499", 0.0),
            "yds_allow_500_549": ("yds_allow_500_549", 0.0),
            "yds_allow_550p": ("yds_allow_550_plus", 0.0),
            # Coarse tiers (Sleeper leagues may still define these — expanded below)
            "yds_allow_300_399": ("yds_allow_300_399", 0.0),
            "yds_allow_400_499": ("yds_allow_400_499", 0.0),
            "yds_allow_500_plus": ("yds_allow_500_plus", 0.0),
        }

        def_multipliers = {}
        for sleeper_key, (col, default) in DEF_COMPONENT_DEFAULTS.items():
            def_multipliers[col] = settings.get(sleeper_key, default)

        # PA scoring: detect whether the league uses cumulative (`pts_allow`)
        # or brackets (`pts_allow_0`..`pts_allow_35p`). Sleeper's API returns
        # all bracket keys even when a league uses cumulative scoring (they
        # come back as 0.0, not missing), so we detect mode by checking for
        # any *non-zero* bracket value rather than key presence.
        pts_allow_cumulative = settings.get("pts_allow")
        has_explicit_bracket = any(float(settings.get(k, 0) or 0) != 0 for k in PA_TIER_DEFAULTS)
        if pts_allow_cumulative is not None and float(pts_allow_cumulative) != 0 and not has_explicit_bracket:
            # Pure cumulative scoring — map to super_table `pts_allow` column
            # and zero out all bracket columns.
            def_multipliers["pts_allow"] = float(pts_allow_cumulative)
            for _sleeper_key, (col, _default) in PA_TIER_DEFAULTS.items():
                def_multipliers[col] = 0.0
        else:
            # Bracket scoring (or defaults if nothing set).
            # Mixed-mode leagues (both cumulative AND brackets) get both: the
            # cumulative baseline plus bracket adjustments. Rare but valid.
            if pts_allow_cumulative is not None and float(pts_allow_cumulative) != 0:
                def_multipliers["pts_allow"] = float(pts_allow_cumulative)
            for sleeper_key, (col, default) in PA_TIER_DEFAULTS.items():
                def_multipliers[col] = settings.get(sleeper_key, default)

        for sleeper_key, (col, default) in YA_TIER_DEFAULTS.items():
            val = settings.get(sleeper_key, default)
            if val != 0:
                def_multipliers[col] = val

        columns["def_multipliers"] = def_multipliers
        columns["def_corrections"] = []  # backward compat

        # === IDP modular multipliers (Sleeper) ===
        # Each pts_idp_* component column stores the raw event count (×1).
        # idp_multipliers maps each component to the league's point value.
        IDP_COMPONENT_DEFAULTS = {
            # sleeper_key -> (pts_idp_component_col, default_multiplier)
            "idp_tkl": ("pts_idp_tackle_solo", 1.0),
            "idp_ast": ("pts_idp_tackle_assist", 0.5),
            "idp_sack": ("pts_idp_sack", 2.0),
            "idp_int": ("pts_idp_int", 3.0),
            "idp_ff": ("pts_idp_ff", 2.0),
            "idp_fum_rec": ("pts_idp_fr", 2.0),
            "idp_pass_def": ("pts_idp_pd", 1.0),
            "idp_qb_hit": ("pts_idp_qb_hit", 0.5),
            "idp_tkl_loss": ("pts_idp_tfl", 1.0),
            "idp_safe": ("pts_idp_safety", 2.0),
            "idp_def_td": ("pts_idp_td", 6.0),
        }

        idp_multipliers = {}
        for sleeper_key, (col, default) in IDP_COMPONENT_DEFAULTS.items():
            idp_multipliers[col] = settings.get(sleeper_key, default)
        columns["idp_multipliers"] = idp_multipliers

        columns["corrections"] = corrections

        # Expand any remaining coarse YA brackets to fine-grained super table columns
        COARSE_TO_FINE = {
            "yds_allow_neg": [],  # removed from super table
            "yds_allow_300_399": ["yds_allow_300_349", "yds_allow_350_399"],
            "yds_allow_400_499": ["yds_allow_400_449", "yds_allow_450_499"],
            "yds_allow_500_plus": ["yds_allow_500_549", "yds_allow_550_plus"],
        }
        if "def_multipliers" in columns:
            dm = columns["def_multipliers"]
            for coarse, fines in COARSE_TO_FINE.items():
                if coarse in dm:
                    val = dm.pop(coarse)
                    for fine in fines:
                        if fine not in dm:
                            dm[fine] = val

        return columns

    # Yahoo format - list of rules in "scoring" key
    rules = scoring_rules.get("scoring", [])

    # === Detect passing TD points and INT value ===
    # Use stat_id-based matching (Yahoo stat_id 5 = Pass TD, 6 = Offense INT)
    # Name-based matching is fallback only for non-Yahoo formats.
    # Known bonus stat_ids to exclude from base scoring detection:
    BONUS_STAT_IDS = {"58", "59", "60", "61", "62", "63", "64", "78", "79", "80"}
    # Known DEF stat_ids that should NOT affect offense scoring:
    DEF_STAT_IDS = {
        "32",
        "33",
        "34",
        "35",
        "36",
        "37",
        "48",
        "49",
        "50",
        "51",
        "52",
        "53",
        "54",
        "55",
        "56",
        "57",
        "67",
        "74",
        "75",
        "76",
        "77",
        "82",
    }
    pass_td_pts = 4  # default
    pass_int_pts = -2  # default
    for rule in rules:
        stat_id = str(rule.get("stat_id", ""))
        name = (rule.get("stat", "") or rule.get("name", "")).lower()
        pts = rule.get("points", 0)

        # Pass TD: prefer stat_id=5, fallback to name (excluding bonus/DEF rules)
        if stat_id == "5":
            pass_td_pts = pts
        elif not stat_id and ("pass" in name and "td" in name or "passing touchdown" in name):
            # Name fallback only for non-stat_id rules (non-Yahoo formats)
            pass_td_pts = pts

        # Offense INT: prefer stat_id=6, fallback to name (excluding DEF stat_ids)
        if stat_id == "6":
            pass_int_pts = pts
        elif stat_id not in DEF_STAT_IDS and stat_id not in BONUS_STAT_IDS:
            if name in ["int", "interceptions thrown", "interception"] or ("pass" in name and "int" in name):
                pos_types = rule.get("position_types", [])
                # Make sure this is an offensive INT (thrown), not defensive
                if not pos_types or "O" in pos_types or "QB" in pos_types:
                    pass_int_pts = pts

    # Choose pass column based on TD pts AND INT value
    if pass_int_pts >= -1.5:
        if pass_td_pts >= 6:
            columns["pass"] = "pts_pass_6pt_int1"
        elif pass_td_pts >= 5:
            columns["pass"] = "pts_pass_5pt_int1"
        else:
            columns["pass"] = "pts_pass_4pt_int1"
        int_precalc_default = -1.0
    else:
        if pass_td_pts >= 6:
            columns["pass"] = "pts_pass_6pt"
        elif pass_td_pts >= 5:
            columns["pass"] = "pts_pass_5pt"
        else:
            columns["pass"] = "pts_pass_4pt"
        int_precalc_default = -2.0

    # === Rushing is always the same ===
    columns["rush"] = "pts_rush"

    # === Detect PPR value ===
    ppr = 0.0  # default
    for rule in rules:
        stat = (rule.get("stat", "") or rule.get("name", "")).lower()
        # Look for receptions stat
        if stat in ["rec", "receptions", "reception"]:
            ppr = rule.get("points", 0.0)
            break

    if ppr >= 0.75:
        columns["rec"] = "pts_rec_ppr"
        ppr_precalc_default = 1.0
    elif ppr >= 0.25:
        columns["rec"] = "pts_rec_half"
        ppr_precalc_default = 0.5
    else:
        columns["rec"] = "pts_rec_0ppr"
        ppr_precalc_default = 0.0

    # === Misc is always the same ===
    columns["misc"] = "pts_misc"

    # === Detect kicker scoring style ===
    columns["kick"] = "pts_k_std"  # default (bucket-based)
    for rule in rules:
        name = (rule.get("stat", "") or rule.get("name", "")).lower()
        # Sleeper-style uses per-yard scoring
        if "fg yds" in name or "field goal yards" in name or "fg_yds" in name:
            columns["kick"] = "pts_k_yds"
            break

    # === Detect defense scoring variant ===
    # Always use pts_def_std as base; corrections handle non-standard values.
    # pts_def_std defaults: sack×1, INT×2, FF×1, FR×2, TD×6, FumTD×6, Safety×2, Block×2
    # + PA tiers: 10/7/4/1/0/-1/-4
    columns["def"] = "pts_def_std"

    # Extract DEF stat values from Yahoo rules (for corrections)
    # Defaults match pts_def_std baked-in values
    def_sack_pts = 1.0
    def_int_pts = 2.0
    def_ff_pts = 0.0
    def_fr_pts = 2.0
    def_td_pts = 6.0
    safety_pts = 2.0
    block_pts = 2.0
    tfl_pts = 0.0
    three_out_pts = 0.0
    fourth_down_pts = 0.0
    def_ret_yd_pts = 0.0
    def_ret_td_pts = 0.0
    def_xpr_pts = 0.0
    has_ya_tiers = False
    # PA tier defaults
    pa_0_pts = 10.0
    pa_1_6_pts = 7.0
    pa_7_13_pts = 4.0
    pa_14_20_pts = 1.0
    pa_21_27_pts = 0.0
    pa_28_34_pts = -1.0
    pa_35_pts = -4.0
    # YA (Yards Allowed) tier defaults
    ya_neg_pts = 5.0
    ya_0_99_pts = 4.0
    ya_100_199_pts = 3.0
    ya_200_299_pts = 2.0
    ya_300_399_pts = 0.0
    ya_400_499_pts = -2.0
    ya_500_plus_pts = -4.0

    # Known Yahoo DEF stat_ids (team defense, not IDP)
    # 32-37: Sack/Int/FR/TD/Safety/Block, 48-49: Ret Yd/TD, 50-56: PA tiers,
    # 57: Fum Ret TD, 67: 4th Down Stops, 74-76: YA tiers, 77: 3-and-Outs, 82: XPR
    YAHOO_DEF_STAT_IDS = {
        "32",
        "33",
        "34",
        "35",
        "36",
        "37",
        "48",
        "49",
        "50",
        "51",
        "52",
        "53",
        "54",
        "55",
        "56",
        "57",
        "67",
        "74",
        "75",
        "76",
        "77",
        "82",
    }

    for rule in rules:
        name = (rule.get("stat", "") or rule.get("name", "")).lower()
        pts = rule.get("points", 0)
        pos_types = rule.get("position_types", [])
        stat_id = str(rule.get("stat_id", ""))

        # Only consider defense rules: by position_types OR known DEF stat_ids
        # (some Yahoo leagues have empty position_types for all rules)
        is_def_rule = "D" in pos_types or "DT" in pos_types or stat_id in YAHOO_DEF_STAT_IDS
        if not is_def_rule:
            continue

        if name == "int" or "intercept" in name:
            def_int_pts = pts
        elif name in ["safe", "safety"]:
            safety_pts = pts
        elif name in ["sack", "sacks"]:
            def_sack_pts = pts
        elif name in ["fumble recovery", "fum rec", "fumbles recovered"]:
            def_fr_pts = pts
        elif name in ["fumble forced", "ff", "fumbles forced"]:
            def_ff_pts = pts
        elif name in ["td", "defensive td", "def td", "defensive touchdown"]:
            def_td_pts = pts
        elif name in ["blocked kick", "block", "blk kick"]:
            block_pts = pts
        elif "tfl" in name or "tackles for loss" in name:
            tfl_pts = pts
        elif "3 and out" in name or "three" in name:
            three_out_pts = pts
        elif "4 dwn" in name or "4th down" in name:
            fourth_down_pts = pts
        elif ("return" in name and "yard" in name and stat_id == "48") or (stat_id == "48"):
            def_ret_yd_pts = pts
        elif ("return" in name and "td" in name and stat_id == "49") or (stat_id == "49"):
            def_ret_td_pts = pts
        elif name in ["xpr", "extra point returned"] or stat_id == "82":
            def_xpr_pts = pts
        elif "yds allow" in name or "yards allowed" in name:
            has_ya_tiers = True  # Tentative; validated below after parsing all tiers
            # Parse the specific tier value
            if "neg" in name:
                ya_neg_pts = pts
            elif "0-99" in name or "0_99" in name:
                ya_0_99_pts = pts
            elif "100" in name:
                ya_100_199_pts = pts
            elif "200" in name:
                ya_200_299_pts = pts
            elif "300" in name:
                ya_300_399_pts = pts
            elif "400" in name:
                ya_400_499_pts = pts
            elif "500" in name:
                ya_500_plus_pts = pts
        # PA tiers: use stat_id for Yahoo (50-56), fallback to name
        # Note: name matching must check longer prefixes first to avoid
        # "pts allow 1" matching "pts allow 14-20"
        elif stat_id == "50" or "pts allow 0" in name or "points allowed 0" in name:
            pa_0_pts = pts
        elif stat_id == "51":
            pa_1_6_pts = pts
        elif stat_id == "52":
            pa_7_13_pts = pts
        elif stat_id == "53":
            pa_14_20_pts = pts
        elif stat_id == "54":
            pa_21_27_pts = pts
        elif stat_id == "55":
            pa_28_34_pts = pts
        elif stat_id == "56":
            pa_35_pts = pts
        # Name fallback for non-Yahoo (must check longer prefixes first)
        elif "pts allow 35" in name or "points allowed 35" in name:
            pa_35_pts = pts
        elif "pts allow 28" in name or "points allowed 28" in name:
            pa_28_34_pts = pts
        elif "pts allow 21" in name or "points allowed 21" in name:
            pa_21_27_pts = pts
        elif "pts allow 14" in name or "points allowed 14" in name:
            pa_14_20_pts = pts
        elif "pts allow 7" in name or "points allowed 7" in name:
            pa_7_13_pts = pts
        elif "pts allow 1" in name or "points allowed 1" in name:
            pa_1_6_pts = pts

    # Validate YA tiers are truly enabled: Yahoo API returns YA stat_ids with default
    # values even when the league hasn't enabled YA scoring. When truly enabled, at least
    # one low-yardage tier (0-99 or 100-199) has POSITIVE points (rewarding good defense).
    # When disabled/default, all tiers are <= 0 (only penalties, no rewards).
    if has_ya_tiers:
        ya_truly_enabled = ya_0_99_pts > 0 or ya_100_199_pts > 0
        if not ya_truly_enabled:
            has_ya_tiers = False

    if has_ya_tiers:
        columns["def"] = "pts_def_ya"

    # === Detect return yards scoring (Yahoo format) ===
    # Yahoo stat_id 14 = Ret Yds (offense), name may be "Ret Yds" or "Return Yards"
    has_return_yds = False
    for rule in rules:
        stat_id = str(rule.get("stat_id", ""))
        name = (rule.get("stat", "") or rule.get("name", "")).lower()
        pts = rule.get("points", 0)
        if stat_id == "14" and pts > 0:
            has_return_yds = True
            break
        if (
            ("return" in name and "yard" in name)
            or ("ret" in name and "yd" in name)
            or "kr_yd" in name
            or "pr_yd" in name
        ):
            if pts > 0:
                has_return_yds = True
                break
    columns["ret_yds"] = "pts_ret_yds" if has_return_yds else None

    # === Detect TE Premium (Yahoo format) ===
    # Check for TE-specific reception bonus or higher PPR for TEs
    te_bonus = 0
    for rule in rules:
        name = (rule.get("stat", "") or rule.get("name", "")).lower()
        pos_types = rule.get("position_types", [])
        pts = rule.get("points", 0)
        # Look for TE-specific reception bonus
        if ("reception" in name or name == "rec") and "TE" in pos_types and len(pos_types) == 1:
            te_bonus = pts
            break
    has_te_premium = te_bonus >= 1.25 or (te_bonus >= 0.25 and ppr >= 1.0)
    columns["rec_tep"] = "pts_rec_tep" if has_te_premium else None

    # === Optional category columns (Yahoo format) ===
    # Yahoo uses stat_id numbers, but scoring rules are pre-parsed into named stats
    cmp_pts = 0
    rush_att_pts = 0
    rush_fd_pts = 0
    rec_fd_pts = 0
    sack_taken_pts = 0
    pass_yds_pts = 0.04  # default
    rush_td_league = 6.0  # default
    rec_td_league = 6.0  # default
    rush_yds_league = 0.1  # default
    rec_yds_league = 0.1  # default
    fum_lost_league = -2.0  # default
    pass_att_league = 0  # default (not scored)
    fum_any_league = 0  # default (not scored)

    for rule in rules:
        name = (rule.get("stat", "") or rule.get("name", "")).lower()
        pts = rule.get("points", 0)
        pos_types = rule.get("position_types", [])

        # Skip defense-only rules
        if pos_types and all(pt in ("D", "DT") for pt in pos_types):
            continue

        if name in ["completions", "pass cmp", "comp"]:
            cmp_pts = pts
        elif name in ["rushing attempts", "rush att", "rush attempts", "carries"]:
            rush_att_pts = pts
        elif name in ["rushing first downs", "rush fd", "rush first down"]:
            rush_fd_pts = pts
        elif name in ["receiving first downs", "rec fd", "rec first down"]:
            rec_fd_pts = pts
        elif name in ["sacks taken", "times sacked", "pass sack", "sacked"]:
            sack_taken_pts = pts
        elif name in ["passing yards", "pass yds"]:
            pass_yds_pts = pts
        elif name in ["rushing touchdowns", "rush td"]:
            rush_td_league = pts
        elif name in ["receiving touchdowns", "rec td"]:
            rec_td_league = pts
        elif name in ["rushing yards", "rush yds"]:
            rush_yds_league = pts
        elif name in ["receiving yards", "rec yds"]:
            rec_yds_league = pts
        elif name in ["fumbles lost", "fum lost"]:
            fum_lost_league = pts
        elif name in ["passing attempts", "pass att"]:
            pass_att_league = pts
        elif name in ["fumbles", "fum"] and "lost" not in name:
            fum_any_league = pts

    columns["cmp"] = "pts_pass_cmp" if cmp_pts != 0 else None
    columns["rush_att"] = "pts_rush_att" if rush_att_pts != 0 else None
    columns["first_downs"] = "pts_first_downs" if (rush_fd_pts != 0 or rec_fd_pts != 0) else None
    columns["sack_taken"] = "pts_sack_taken" if sack_taken_pts != 0 else None

    # === Build corrections for non-standard multipliers (Yahoo) ===
    corrections = []

    # Pass INT correction
    if abs(pass_int_pts - int_precalc_default) > 0.001:
        corrections.append(("passing_interceptions", pass_int_pts - int_precalc_default))

    # Pass yards correction
    if abs(pass_yds_pts - 0.04) > 0.001:
        corrections.append(("passing_yards", pass_yds_pts - 0.04))

    # PPR correction
    if abs(ppr - ppr_precalc_default) > 0.001:
        corrections.append(("receptions", ppr - ppr_precalc_default))

    # Rush/Rec stats corrections
    stat_corrections = [
        (rush_td_league, "rushing_tds", 6.0),
        (rush_yds_league, "rushing_yards", 0.1),
        (rec_td_league, "receiving_tds", 6.0),
        (rec_yds_league, "receiving_yards", 0.1),
        (pass_att_league, "attempts", 0.0),
    ]
    for league_val, nflverse_col, default_val in stat_corrections:
        if abs(league_val - default_val) > 0.001:
            corrections.append((nflverse_col, league_val - default_val))

    # Fumbles lost: apply delta to all 3 component columns
    if abs(fum_lost_league - (-2.0)) > 0.001:
        fum_delta = fum_lost_league - (-2.0)
        corrections.append(("rushing_fumbles_lost", fum_delta))
        corrections.append(("sack_fumbles_lost", fum_delta))
        corrections.append(("receiving_fumbles_lost", fum_delta))

    # Fumble (any, not just lost): apply to rushing_fumbles + receiving_fumbles
    if abs(fum_any_league) > 0.001:
        corrections.append(("rushing_fumbles", fum_any_league))
        corrections.append(("receiving_fumbles", fum_any_league))

    # Optional category corrections (only when precalc column is being used)
    if columns.get("cmp") is not None and abs(cmp_pts - 0.25) > 0.001:
        corrections.append(("completions", cmp_pts - 0.25))
    if columns.get("rush_att") is not None and abs(rush_att_pts - 0.1) > 0.001:
        corrections.append(("carries", rush_att_pts - 0.1))
    if columns.get("sack_taken") is not None and abs(sack_taken_pts - (-1.0)) > 0.001:
        corrections.append(("sacks_suffered", sack_taken_pts - (-1.0)))

    # First downs corrections
    if columns.get("first_downs") is not None:
        if abs(rush_fd_pts - 1.0) > 0.001:
            corrections.append(("rushing_first_downs", rush_fd_pts - 1.0))
        if abs(rec_fd_pts - 1.0) > 0.001:
            corrections.append(("receiving_first_downs", rec_fd_pts - 1.0))

    # === DEF modular multipliers (Yahoo) ===
    # Each pts_def_* component stores raw event count (×1).
    # def_multipliers maps each component to the league's point value.
    def_multipliers = {
        "pts_def_sack": def_sack_pts,
        "pts_def_int": def_int_pts,
        "pts_def_ff": def_ff_pts,
        "pts_def_fr": def_fr_pts,
        "pts_def_td": def_td_pts,
        "pts_def_safety": safety_pts,
        "pts_def_block": block_pts,
        "pts_def_tfl": tfl_pts,
        "pts_def_3out": three_out_pts,
        "pts_def_4stop": fourth_down_pts,
        "pts_def_ret_yd": def_ret_yd_pts,
        "pts_def_ret_td": def_ret_td_pts,
        # pts_def_xpr removed 2026-05-02 per L1.b Phase 0 Task 0.4 — col doesn't
        # exist in super_table. Yahoo stat 82 (XP Returned) is unsupported until
        # a precomputed col is added. The def_xpr_pts variable is still parsed at
        # line ~662 to surface non-zero league configs in logs (don't silently drop).
        # PA tiers
        "pts_allow_0": pa_0_pts,
        "pts_allow_1_6": pa_1_6_pts,
        "pts_allow_7_13": pa_7_13_pts,
        "pts_allow_14_20": pa_14_20_pts,
        "pts_allow_21_27": pa_21_27_pts,
        "pts_allow_28_34": pa_28_34_pts,
        "pts_allow_35_plus": pa_35_pts,
    }

    # Add YA tiers if league uses them (values parsed from rules above)
    # Yahoo uses coarse tiers (300-399, etc.) — expand to fine-grained
    # for super table compatibility. yds_allow_neg no longer exists.
    if has_ya_tiers:
        def_multipliers["yds_allow_0_99"] = ya_0_99_pts
        def_multipliers["yds_allow_100_199"] = ya_100_199_pts
        def_multipliers["yds_allow_200_299"] = ya_200_299_pts
        def_multipliers["yds_allow_300_349"] = ya_300_399_pts
        def_multipliers["yds_allow_350_399"] = ya_300_399_pts
        def_multipliers["yds_allow_400_449"] = ya_400_499_pts
        def_multipliers["yds_allow_450_499"] = ya_400_499_pts
        def_multipliers["yds_allow_500_549"] = ya_500_plus_pts
        def_multipliers["yds_allow_550_plus"] = ya_500_plus_pts

    columns["def_multipliers"] = def_multipliers
    columns["def_corrections"] = []  # backward compat

    # === IDP modular multipliers (Yahoo) ===
    # Yahoo stat_ids 38-47,65 are IDP (individual defensive players).
    # These are separate from team DEF stat_ids 32-37.
    YAHOO_IDP_STAT_MAP = {
        "38": ("pts_idp_tackle_solo", 1.0),  # Tackle Solo
        "39": ("pts_idp_tackle_assist", 0.5),  # Tackle Assist
        "40": ("pts_idp_sack", 2.0),  # Sack
        "41": ("pts_idp_int", 3.0),  # Interception
        "42": ("pts_idp_ff", 2.0),  # Fumble Forced
        "43": ("pts_idp_fr", 2.0),  # Fumble Recovery
        "44": ("pts_idp_td", 6.0),  # Defensive TD
        "45": ("pts_idp_safety", 2.0),  # Safety
        "46": ("pts_idp_pd", 1.0),  # Pass Defended
        "65": ("pts_idp_tfl", 1.0),  # Tackle for Loss
    }

    idp_multipliers = {}
    for rule in rules:
        stat_id = str(rule.get("stat_id", ""))
        if stat_id in YAHOO_IDP_STAT_MAP:
            col, default = YAHOO_IDP_STAT_MAP[stat_id]
            idp_multipliers[col] = float(rule.get("points", default))

    # If no IDP rules found in scoring_rules, check if league has IDP positions
    # and use defaults
    if not idp_multipliers:
        # Check position_types for IDP indicators
        has_idp = False
        for rule in rules:
            pos_types = rule.get("position_types", [])
            if any(p in pos_types for p in ("LB", "DB", "DL", "DE", "DT", "CB", "S")):
                has_idp = True
                break
        if has_idp:
            for stat_id, (col, default) in YAHOO_IDP_STAT_MAP.items():
                idp_multipliers[col] = default

    columns["idp_multipliers"] = idp_multipliers

    columns["corrections"] = corrections

    # Expand any remaining coarse YA brackets to fine-grained super table columns
    COARSE_TO_FINE = {
        "yds_allow_neg": [],  # removed from super table
        "yds_allow_300_399": ["yds_allow_300_349", "yds_allow_350_399"],
        "yds_allow_400_499": ["yds_allow_400_449", "yds_allow_450_499"],
        "yds_allow_500_plus": ["yds_allow_500_549", "yds_allow_550_plus"],
    }
    if "def_multipliers" in columns:
        dm = columns["def_multipliers"]
        for coarse, fines in COARSE_TO_FINE.items():
            if coarse in dm:
                val = dm.pop(coarse)
                for fine in fines:
                    if fine not in dm:
                        dm[fine] = val

    return columns


def detect_scoring_variant(scoring_rules_by_year: dict[int, dict[str, Any]]) -> dict[str, str]:
    """
    Detect scoring variant from the earliest year's rules.

    For consistency, we use the earliest year's rules to determine the variant.
    Most leagues don't change their fundamental scoring (PPR, pass TD pts).

    Args:
        scoring_rules_by_year: Dict mapping year to scoring rules

    Returns:
        Dict mapping category to column name
    """
    if not scoring_rules_by_year:
        # Default to standard scoring
        return {
            "pass": "pts_pass_4pt",
            "rush": "pts_rush",
            "rec": "pts_rec_0ppr",
            "rec_tep": "pts_rec_tep",  # TE Premium (TEs get 1.5 PPR)
            "ret_yds": "pts_ret_yds",  # Return yards
            "misc": "pts_misc",
            "kick": "pts_k_std",
            "def": "pts_def_std",
            "idp": "pts_idp_std",
            "cmp": None,
            "rush_att": None,
            "first_downs": None,
            "sack_taken": None,
            "corrections": [],
            "def_corrections": [],
            "def_multipliers": {
                "pts_def_sack": 1.0,
                "pts_def_int": 2.0,
                "pts_def_ff": 0.0,
                "pts_def_fr": 2.0,
                "pts_def_td": 6.0,
                "pts_def_safety": 2.0,
                "pts_def_block": 2.0,
                "pts_def_tfl": 0.0,
                "pts_def_3out": 0.0,
                "pts_def_4stop": 0.0,
                "pts_allow_0": 10.0,
                "pts_allow_1_6": 7.0,
                "pts_allow_7_13": 4.0,
                "pts_allow_14_20": 1.0,
                "pts_allow_21_27": 0.0,
                "pts_allow_28_34": -1.0,
                "pts_allow_35_plus": -4.0,
            },
            "idp_multipliers": {},
        }

    # Use earliest year's rules
    earliest_year = min(scoring_rules_by_year.keys())
    return get_scoring_columns(scoring_rules_by_year[earliest_year])


def extract_scoring_params(scoring_columns: dict[str, str]) -> dict[str, Any]:
    """
    Extract PPR, pass TD, IDP scoring, and TE premium values from scoring columns dict.

    This is used to pass the right parameters to V2 optimal lineup calculation
    which needs numeric values rather than column names.

    Args:
        scoring_columns: Dict from get_scoring_columns() or detect_scoring_variant()
                         e.g., {'pass': 'pts_pass_4pt', 'rec': 'pts_rec_half', 'idp': 'pts_idp_std', ...}

    Returns:
        Dict with:
        - 'ppr' (float: 0, 0.5, or 1.0)
        - 'pass_td_pts' (int: 4, 5, or 6)
        - 'idp_scoring' (str: 'std', 'premium', 'tackle_heavy', 'big_play')
        - 'te_premium' (bool: True if TEs get bonus PPR)

    Example:
        scoring_cols = detect_scoring_variant(scoring_rules)
        params = extract_scoring_params(scoring_cols)
        # params = {'ppr': 0.5, 'pass_td_pts': 4, 'idp_scoring': 'std', 'te_premium': False}
    """
    # Extract PPR from reception column name
    rec_col = scoring_columns.get("rec", "pts_rec_0ppr")
    if "ppr" in rec_col and "half" not in rec_col and "0ppr" not in rec_col:
        ppr = 1.0  # pts_rec_ppr
    elif "half" in rec_col:
        ppr = 0.5  # pts_rec_half
    else:
        ppr = 0.0  # pts_rec_0ppr

    # Extract pass TD points from passing column name
    pass_col = scoring_columns.get("pass", "pts_pass_4pt")
    if "6pt" in pass_col:
        pass_td_pts = 6
    elif "5pt" in pass_col:
        pass_td_pts = 5
    else:
        pass_td_pts = 4  # default to 4pt

    # Extract IDP scoring variant from IDP column name
    idp_col = scoring_columns.get("idp", "pts_idp_std")
    if "big_play" in idp_col:
        idp_scoring = "big_play"
    elif "tackle_heavy" in idp_col:
        idp_scoring = "tackle_heavy"
    elif "premium" in idp_col:
        idp_scoring = "premium"
    else:
        idp_scoring = "std"

    # Detect TE Premium from rec_tep column presence or specific settings
    rec_tep_col = scoring_columns.get("rec_tep", "")
    te_premium = rec_tep_col is not None and ("tep" in str(rec_tep_col) or rec_col == "pts_rec_tep")

    # Detect return yards from ret_yds column presence
    ret_yds_col = scoring_columns.get("ret_yds", "")
    has_return_yards = ret_yds_col is not None and ret_yds_col != ""

    return {
        "ppr": ppr,
        "pass_td_pts": pass_td_pts,
        "idp_scoring": idp_scoring,
        "te_premium": te_premium,
        "return_yards": has_return_yards,
    }


def calculate_fantasy_points_from_precalc(df: pl.DataFrame, scoring_columns: dict[str, str]) -> pl.DataFrame:
    """
    Sum the appropriate pre-calculated columns based on league settings.

    This is the FAST PATH - instead of evaluating scoring rules for each stat,
    we just sum the pre-calculated category columns.

    Args:
        df: DataFrame with pts_* columns from super_table
        scoring_columns: Dict from get_scoring_columns()

    Returns:
        DataFrame with fantasy_points column added

    Usage:
        scoring_cols = get_scoring_columns(scoring_rules)
        df = calculate_fantasy_points_from_precalc(df, scoring_cols)
    """
    # Build the sum expression
    pts_cols = [
        scoring_columns.get("pass", "pts_pass_4pt"),
        scoring_columns.get("rush", "pts_rush"),
        scoring_columns.get("rec", "pts_rec_0ppr"),
        scoring_columns.get("misc", "pts_misc"),
        scoring_columns.get("kick", "pts_k_std"),
        scoring_columns.get("def", "pts_def_std"),
        scoring_columns.get("idp", "pts_idp_std"),  # IDP for individual defensive players
    ]

    # Add optional category columns when detected by get_scoring_columns()
    for optional_key in ("ret_yds", "rec_tep", "cmp", "rush_att", "first_downs", "sack_taken"):
        optional_col = scoring_columns.get(optional_key)
        if optional_col is not None:
            pts_cols.append(optional_col)

    # Check which columns exist in the DataFrame
    available_cols = [col for col in pts_cols if col in df.columns]

    if not available_cols:
        # No pre-calc columns available, return with NULL points (not 0!)
        # NULL indicates player didn't play, 0 would incorrectly trigger LAMAR calculation
        print("[scoring] Warning: No pre-calculated pts_* columns found")
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias("fantasy_points"))

    # Check if player actually played (at least one pts_* column is non-null)
    # If ALL pts columns are NULL, player didn't play that week (bye, inactive, injured)
    played_condition = pl.any_horizontal([pl.col(col).is_not_null() for col in available_cols])

    # Sum all available pre-calc columns (with null -> 0 for the sum)
    sum_expr = pl.lit(0.0)
    for col in available_cols:
        sum_expr = sum_expr + pl.col(col).fill_null(0)

    # Apply corrections for edge-case scoring differences (e.g., -1 INT, 0.375 PPR)
    corrections = scoring_columns.get("corrections", [])
    for stat_col, multiplier in corrections:
        if stat_col in df.columns:
            sum_expr = sum_expr + pl.col(stat_col).fill_null(0) * multiplier

    # Only set fantasy_points for players who actually played; otherwise NULL
    # NULL fantasy_points = player didn't play (LAMAR calculator will skip these)
    # 0 or negative fantasy_points = player played but scored poorly (valid LAMAR)
    return df.with_columns(
        pl.when(played_condition).then(sum_expr).otherwise(pl.lit(None).cast(pl.Float64)).alias("fantasy_points")
    )


def has_precalc_columns(df: pl.DataFrame) -> bool:
    """
    Check if DataFrame has pre-calculated fantasy points columns.

    Args:
        df: DataFrame to check

    Returns:
        True if at least one pts_* column exists
    """
    pts_cols = [
        "pts_pass_4pt",
        "pts_pass_5pt",
        "pts_pass_6pt",
        "pts_pass_4pt_int1",
        "pts_pass_5pt_int1",
        "pts_pass_6pt_int1",
        "pts_rush",
        "pts_rec_0ppr",
        "pts_rec_half",
        "pts_rec_ppr",
        "pts_rec_tep",
        "pts_ret_yds",
        "pts_misc",
        "pts_k_std",
        "pts_k_yds",
        "pts_k_flat",
        "pts_def_std",
        "pts_def_ya",
        # pts_def_high removed 2026-04-30 (KMFFL-specific; computed per-league)
        # IDP columns
        "pts_idp_std",
        "pts_idp_premium",
        "pts_idp_tackle_heavy",
        "pts_idp_big_play",
        # Optional category columns
        "pts_pass_cmp",
        "pts_rush_att",
        "pts_first_downs",
        "pts_sack_taken",
    ]
    return any(col in df.columns for col in pts_cols)


def load_scoring_rules(scoring_dir: Path) -> dict[int, dict[str, Any]]:
    """
    Load scoring rules from JSON files.

    Args:
        scoring_dir: Directory containing scoring_rules_YYYY.json or
            league_settings_YYYY_*.json files

    Returns:
        Dict mapping year to scoring rules (with "scoring" key containing list of rules)
    """
    rules_by_year = {}

    if not scoring_dir.exists():
        return rules_by_year

    # First try to load scoring_rules_*.json files
    scoring_files = list(scoring_dir.glob("scoring_rules_*.json"))

    # If no scoring_rules files found, try league_settings_*.json files
    if not scoring_files:
        scoring_files = list(scoring_dir.glob("league_settings_*.json"))
        print(f"[scoring] No scoring_rules_*.json found, using {len(scoring_files)} league_settings files")

    for file_path in sorted(scoring_files):
        try:
            # Extract year from filename
            # Format: scoring_rules_2014.json OR league_settings_2014_449_l_198278.json
            parts = file_path.stem.split("_")
            if "league" in file_path.stem:
                # league_settings_YYYY_... format
                year = int(parts[2])
            else:
                # scoring_rules_YYYY format
                year = int(parts[-1])

            with open(file_path) as f:
                data = json.load(f)
                from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row

                flat_scoring = extract_scoring_settings_from_flat_row(data)

                # If this is a league_settings file, extract scoring_rules and wrap in "scoring" key
                if "scoring_rules" in data:
                    rules_by_year[year] = {"scoring": data["scoring_rules"]}
                # If data already has "scoring" key, use as-is
                elif "scoring" in data:
                    rules_by_year[year] = data
                # Sleeper format: scoring_settings is a flat dict like {'rec': 0.5, 'pass_td': 4}
                elif "scoring_settings" in data:
                    rules_by_year[year] = {"scoring_settings": data["scoring_settings"]}
                # Canonical scoring (all platforms): normalized dict from canonical_scoring project
                elif "canonical_scoring" in data:
                    cs = data["canonical_scoring"]
                    if isinstance(cs, str):
                        cs = json.loads(cs)
                    rules_by_year[year] = {"scoring_settings": cs}
                # Canonical flat settings file: scoring_* columns directly on the row
                elif flat_scoring:
                    rules_by_year[year] = {"scoring_settings": flat_scoring}
                else:
                    # Unknown format, skip
                    print(
                        f"[scoring] Warning: {file_path} has no scoring_rules, scoring_settings, canonical_scoring, or flat scoring_* columns"
                    )
                    continue

        except (ValueError, json.JSONDecodeError, KeyError) as e:
            print(f"[scoring] Warning: Could not load {file_path}: {e}")
            continue

    return rules_by_year


def detect_column_patterns(df_columns: list[str]) -> dict[str, bool]:
    """
    Detect column naming patterns in DataFrame.

    This helps determine which naming convention the data uses.

    Args:
        df_columns: List of column names in DataFrame

    Returns:
        Dict with pattern detection results
    """
    cols_lower = {c.lower() for c in df_columns}

    return {
        "has_def_prefix": any(c.startswith("def_") for c in cols_lower),
        "has_pass_prefix": any(c.startswith("pass_") for c in cols_lower),
        "has_rush_prefix": any(c.startswith("rush") for c in cols_lower),
        "has_rec_prefix": any(c.startswith("rec") for c in cols_lower),
        "has_offensive_stats": any(s in cols_lower for s in ["passing_yards", "rushing_yards", "receiving_yards"]),
        "has_defensive_stats": any(s in cols_lower for s in ["def_sacks", "def_interceptions", "def_tds"]),
        "has_kicker_stats": any(s in cols_lower for s in ["fg_made", "pat_made"]),
    }


def build_position_aware_mappings(df_columns: list[str], position_types: list[str] | None = None) -> dict[str, str]:
    """
    Build column mappings based on actual DataFrame columns and position context.

    This resolves ambiguous stat names by:
    1. Detecting what columns actually exist in the DataFrame
    2. Using position context to disambiguate (e.g., "Int" for QB vs DEF)
    3. Preferring exact matches over fuzzy matches

    Args:
        df_columns: List of column names available in DataFrame
        position_types: Position types from scoring rule (e.g., ['O'], ['D'], ['K'])

    Returns:
        Dict mapping stat names to actual DataFrame columns
    """
    cols_lower = {c.lower(): c for c in df_columns}
    patterns = detect_column_patterns(df_columns)

    is_defense = position_types and any(pt in ["D", "DT"] for pt in position_types)
    is_offense = position_types and "O" in position_types
    is_kicker = position_types and "K" in position_types

    mappings = {}

    # Offensive mappings with intelligent fallbacks
    if is_offense or not position_types:
        offensive_stats = {
            # Passing stats
            "Passing Touchdowns": ["passing_tds", "pass_td", "passing_touchdowns"],
            "Pass TD": ["passing_tds", "pass_td"],
            "Passing Yards": ["passing_yards", "pass_yds", "passing_yds"],
            "Pass Yds": ["passing_yards", "pass_yds"],
            "Interceptions": ["pass_int", "interceptions", "passing_int"],
            "Int": ["pass_int", "interceptions"] if patterns["has_pass_prefix"] else ["interceptions", "pass_int"],
            # Rushing stats
            "Rushing Yards": ["rushing_yards", "rush_yds", "rushing_yds"],
            "Rush Yds": ["rushing_yards", "rush_yds"],
            "Rushing Touchdowns": ["rushing_tds", "rush_td", "rushing_touchdowns"],
            "Rush TD": ["rushing_tds", "rush_td"],
            "Rushing Attempts": ["carries", "rushing_attempts", "rush_att"],
            "Rush Att": ["carries", "rushing_attempts"],
            # Receiving stats
            "Receptions": ["receptions", "rec", "receiving_rec"],
            "Rec": ["receptions", "rec"],
            "Receiving Yards": ["receiving_yards", "rec_yds", "receiving_yds"],
            "Rec Yds": ["receiving_yards", "rec_yds"],
            "Receiving Touchdowns": ["receiving_tds", "rec_td", "receiving_touchdowns"],
            "Rec TD": ["receiving_tds", "rec_td"],
            # Special teams / other
            "Return Touchdowns": ["special_teams_tds", "return_tds", "ret_td"],
            "Ret TD": ["special_teams_tds", "return_tds"],
            "2-Point Conversions": ["passing_2pt_conversions", "two_pt_conversions", "2pt_conversions"],
            "2-PT": ["passing_2pt_conversions", "two_pt_conversions"],
            "Fumbles Lost": ["sack_fumbles_lost", "fumbles_lost", "fum_lost"],
            "Fum Lost": ["sack_fumbles_lost", "fumbles_lost"],
            "Offensive Fumble Return TD": ["fum_ret_td", "fumble_return_td"],
            "Fum Ret TD": ["fum_ret_td", "fumble_return_td"],
        }

        for stat_name, candidates in offensive_stats.items():
            for candidate in candidates:
                if candidate.lower() in cols_lower:
                    mappings[stat_name] = cols_lower[candidate.lower()]
                    break

    # Kicker mappings
    if is_kicker or not position_types:
        kicker_stats = {
            "PAT Made": ["pat_made", "xp_made", "extra_point_made"],
            "Point After Attempt Made": ["pat_made", "xp_made"],
            "FG Yds": ["fg_made_distance", "fg_yds", "field_goal_yards"],
            "Field Goals Total Yards": ["fg_made_distance", "fg_yds"],
            "FG Made": ["fg_made", "field_goals_made"],
            "Field Goals Made": ["fg_made", "field_goals_made"],
            "FG Miss": ["fg_missed", "field_goals_missed"],
            "PAT Miss": ["pat_missed", "xp_missed"],
        }

        for stat_name, candidates in kicker_stats.items():
            if stat_name not in mappings:  # Don't override offensive mappings
                for candidate in candidates:
                    if candidate.lower() in cols_lower:
                        mappings[stat_name] = cols_lower[candidate.lower()]
                        break

    # Defensive/DST mappings with prefix awareness
    if is_defense or not position_types:
        defensive_stats = {
            "Sack": ["def_sacks", "sacks", "defensive_sacks"],
            "Int": ["def_interceptions", "def_int", "interceptions"]
            if patterns["has_def_prefix"]
            else ["interceptions", "def_interceptions"],
            "Fum Rec": ["fum_rec", "fumble_recoveries", "fumbles_recovered"],
            "Fumble Recovery": ["fum_rec", "fumble_recoveries"],
            # DEF TDs: Include both INT return TDs (def_tds) and fumble return TDs (fum_ret_td)
            # All defensive TDs should be scored at 6 pts each
            "TD": ["def_tds", "defensive_tds", "touchdowns"],
            "Touchdown": ["def_tds", "defensive_tds"],
            "Safe": ["def_safeties", "safeties", "safety"],
            "Safety": ["def_safeties", "safeties"],
            "Blk Kick": ["fg_blocked", "blocked_kicks", "blocks"],
            "Block Kick": ["fg_blocked", "blocked_kicks"],
            # "Ret TD" is for SPECIAL TEAMS return TDs (kickoff/punt returns), NOT defensive fumble returns
            # Defensive fumble return TDs are in fum_ret_td and should be scored separately as "Fum Ret TD"
            "Ret TD": ["special_teams_tds", "kickoff_return_td", "punt_return_td"],
            "Kickoff and Punt Return Touchdowns": ["special_teams_tds", "kickoff_return_td"],
            # Fumble return TDs are defensive TDs, scored at same rate as other DEF TDs
            "Fum Ret TD": ["fum_ret_td", "fumble_return_tds"],
            "Defensive Fumble Return TD": ["fum_ret_td", "fumble_return_tds"],
            "TFL": ["def_tackles_for_loss", "tackles_for_loss", "tfl"],
            "Tackles for Loss": ["def_tackles_for_loss", "tackles_for_loss"],
            # Fourth down stops - database uses singular "fourth_down_stop"
            "4 Dwn Stops": ["fourth_down_stop", "def_fourth_down_stops", "fourth_down_stops"],
            "4th Down Stops": ["fourth_down_stop", "def_fourth_down_stops", "fourth_down_stops"],
            "3 and Outs": ["three_out", "def_three_out", "three_and_outs"],
            "Three and Outs Forced": ["three_out", "def_three_out"],
            "XPR": ["extra_point_return_tds", "xp_return_td"],
            "Extra Point Returned": ["extra_point_return_tds", "xp_return_td"],
            # Points Allowed buckets
            "Pts Allow 0": ["pts_allow_0", "points_allowed_0"],
            "Points Allowed 0 points": ["pts_allow_0", "points_allowed_0"],
            "Pts Allow 1-6": ["pts_allow_1_6", "points_allowed_1_6"],
            "Points Allowed 1-6 points": ["pts_allow_1_6", "points_allowed_1_6"],
            "Pts Allow 7-13": ["pts_allow_7_13", "points_allowed_7_13"],
            "Points Allowed 7-13 points": ["pts_allow_7_13", "points_allowed_7_13"],
            "Pts Allow 14-20": ["pts_allow_14_20", "points_allowed_14_20"],
            "Points Allowed 14-20 points": ["pts_allow_14_20", "points_allowed_14_20"],
            "Pts Allow 21-27": ["pts_allow_21_27", "points_allowed_21_27"],
            "Points Allowed 21-27 points": ["pts_allow_21_27", "points_allowed_21_27"],
            "Pts Allow 28-34": ["pts_allow_28_34", "points_allowed_28_34"],
            "Points Allowed 28-34 points": ["pts_allow_28_34", "points_allowed_28_34"],
            "Pts Allow 35+": ["pts_allow_35_plus", "points_allowed_35_plus"],
            "Points Allowed 35+ points": ["pts_allow_35_plus", "points_allowed_35_plus"],
            "Pts Allow Neg": ["pts_allow_neg", "points_allowed_neg"],
            # Yards Allowed buckets
            "Yds Allow Neg": ["yds_allow_neg", "yards_allowed_neg"],
            "Defensive Yards Allowed - Negative": ["yds_allow_neg", "yards_allowed_neg"],
            "Yds Allow 0-99": ["yds_allow_0_99", "yards_allowed_0_99"],
            "Defensive Yards Allowed 0-99": ["yds_allow_0_99", "yards_allowed_0_99"],
            "Yds Allow 100-199": ["yds_allow_100_199", "yards_allowed_100_199"],
            "Defensive Yards Allowed 100-199": ["yds_allow_100_199", "yards_allowed_100_199"],
            "Yds Allow 200-299": ["yds_allow_200_299", "yards_allowed_200_299"],
            "Defensive Yards Allowed 200-299": ["yds_allow_200_299", "yards_allowed_200_299"],
            "Yds Allow 300-399": ["yds_allow_300_399", "yards_allowed_300_399"],
            "Defensive Yards Allowed 300-399": ["yds_allow_300_399", "yards_allowed_300_399"],
            "Yds Allow 400-499": ["yds_allow_400_499", "yards_allowed_400_499"],
            "Defensive Yards Allowed 400-499": ["yds_allow_400_499", "yards_allowed_400_499"],
            "Yds Allow 500+": ["yds_allow_500_plus", "yards_allowed_500_plus"],
            "Defensive Yards Allowed 500+": ["yds_allow_500_plus", "yards_allowed_500_plus"],
        }

        for stat_name, candidates in defensive_stats.items():
            if stat_name not in mappings:  # Don't override kicker/offensive mappings
                for candidate in candidates:
                    if candidate.lower() in cols_lower:
                        mappings[stat_name] = cols_lower[candidate.lower()]
                        break

    return mappings


def normalize_stat_name(stat: str, position_types: list[str] | None = None) -> str:
    """
    Normalize stat names to match DataFrame columns.

    DEPRECATED: Use build_position_aware_mappings() instead for better accuracy.
    This function is kept for backward compatibility.

    Handles common variations and disambiguates based on position_types:
    - Passing Touchdowns -> passing_tds
    - Rushing Yards -> rushing_yds
    - Sack (defense) -> def_sacks
    - Int (offense vs defense) -> interceptions vs def_interceptions
    - etc.

    Args:
        stat: Stat name from scoring rules (e.g., "Int", "Sack", "Pass TD")
        position_types: List of position types from scoring rule (e.g., ['O'], ['D'], ['DT'])
                       'O' = Offense, 'D' = Defense, 'DT' = Defense Tackles

    Returns:
        Normalized column name
    """
    is_defense = position_types and any(pt in ["D", "DT"] for pt in position_types)
    is_offense = position_types and "O" in position_types

    # Offensive stats
    offensive_mappings = {
        "Passing Touchdowns": "passing_tds",
        "Pass TD": "passing_tds",
        "Passing Yards": "passing_yards",
        "Pass Yds": "passing_yards",
        "Interceptions": "pass_int",  # QB throwing INT (negative)
        "Int": "pass_int",  # Offensive INT
        "Rushing Yards": "rushing_yards",
        "Rush Yds": "rushing_yards",
        "Rushing Touchdowns": "rushing_tds",
        "Rush TD": "rushing_tds",
        "Rushing Attempts": "carries",
        "Rush Att": "carries",
        "Receptions": "receptions",
        "Rec": "receptions",
        "Receiving Yards": "receiving_yards",
        "Rec Yds": "receiving_yards",
        "Receiving Touchdowns": "receiving_tds",
        "Rec TD": "receiving_tds",
        "Return Touchdowns": "special_teams_tds",
        "Ret TD": "special_teams_tds",
        "2-Point Conversions": "passing_2pt_conversions",  # Will handle multiple types below
        "2-PT": "passing_2pt_conversions",
        "Fumbles Lost": "sack_fumbles_lost",  # Will sum multiple fumble types
        "Fum Lost": "sack_fumbles_lost",
        "Offensive Fumble Return TD": "fum_ret_td",
        "Fum Ret TD": "fum_ret_td",
    }

    # Kicker stats
    kicker_mappings = {
        "PAT Made": "pat_made",
        "Point After Attempt Made": "pat_made",
        "FG Yds": "fg_made_distance",
        "Field Goals Total Yards": "fg_made_distance",
        "FG Made": "fg_made",
        "Field Goals Made": "fg_made",
    }

    # Defensive/DST stats (Yahoo stat names → NFL defense columns)
    defensive_mappings = {
        "Sack": "def_sacks",
        "Int": "def_interceptions",  # Defense catching INT (positive)
        "Fum Rec": "fum_rec",
        "Fumble Recovery": "fum_rec",
        "TD": "def_tds",
        "Touchdown": "def_tds",
        "Safe": "def_safeties",
        "Safety": "def_safeties",
        "Blk Kick": "fg_blocked",
        "Block Kick": "fg_blocked",
        "Ret TD": "fum_ret_td",  # Defensive/special teams return TD
        "Kickoff and Punt Return Touchdowns": "fum_ret_td",
        "TFL": "def_tackles_for_loss",
        "Tackles for Loss": "def_tackles_for_loss",
        "4 Dwn Stops": "def_fourth_down_stops",
        "4th Down Stops": "def_fourth_down_stops",
        "3 and Outs": "def_three_out",
        "Three and Outs Forced": "def_three_out",
        "XPR": "extra_point_return_tds",
        "Extra Point Returned": "extra_point_return_tds",
        # Points Allowed buckets
        "Pts Allow 0": "pts_allow_0",
        "Points Allowed 0 points": "pts_allow_0",
        "Pts Allow 1-6": "pts_allow_1_6",
        "Points Allowed 1-6 points": "pts_allow_1_6",
        "Pts Allow 7-13": "pts_allow_7_13",
        "Points Allowed 7-13 points": "pts_allow_7_13",
        "Pts Allow 14-20": "pts_allow_14_20",
        "Points Allowed 14-20 points": "pts_allow_14_20",
        "Pts Allow 21-27": "pts_allow_21_27",
        "Points Allowed 21-27 points": "pts_allow_21_27",
        "Pts Allow 28-34": "pts_allow_28_34",
        "Points Allowed 28-34 points": "pts_allow_28_34",
        "Pts Allow 35+": "pts_allow_35_plus",
        "Points Allowed 35+ points": "pts_allow_35_plus",
        "Pts Allow Neg": "pts_allow_neg",
        # Yards Allowed buckets
        "Yds Allow Neg": "yds_allow_neg",
        "Defensive Yards Allowed - Negative": "yds_allow_neg",
        "Yds Allow 0-99": "yds_allow_0_99",
        "Defensive Yards Allowed 0-99": "yds_allow_0_99",
        "Yds Allow 100-199": "yds_allow_100_199",
        "Defensive Yards Allowed 100-199": "yds_allow_100_199",
        "Yds Allow 200-299": "yds_allow_200_299",
        "Defensive Yards Allowed 200-299": "yds_allow_200_299",
        "Yds Allow 300-399": "yds_allow_300_399",
        "Defensive Yards Allowed 300-399": "yds_allow_300_399",
        "Yds Allow 400-499": "yds_allow_400_499",
        "Defensive Yards Allowed 400-499": "yds_allow_400_499",
        "Yds Allow 500+": "yds_allow_500_plus",
        "Defensive Yards Allowed 500+": "yds_allow_500_plus",
    }

    # Determine if this is a kicker stat
    is_kicker = position_types and "K" in position_types

    # Choose the right mapping based on position_types
    if is_defense:
        mapped = defensive_mappings.get(stat)
        if mapped:
            return mapped
    elif is_kicker:
        mapped = kicker_mappings.get(stat)
        if mapped:
            return mapped
    elif is_offense:
        mapped = offensive_mappings.get(stat)
        if mapped:
            return mapped

    # Fallback: check all mappings (offensive first, then kicker, then defensive)
    mapped = offensive_mappings.get(stat) or kicker_mappings.get(stat) or defensive_mappings.get(stat)
    if mapped:
        return mapped

    # Final fallback: normalize by replacing spaces with underscores
    return stat.lower().replace(" ", "_")


def build_points_expression(rules: list[dict[str, Any]], df_columns: list[str]) -> pl.Expr:
    """
    Build Polars expression for fantasy points calculation.

    Uses position-aware column mapping to handle ambiguous stat names correctly.

    Args:
        rules: List of scoring rules [{"stat": "Passing Yards", "points": 0.04, "position_types": ["O"]}, ...]
        df_columns: Available columns in DataFrame

    Returns:
        Polars expression that calculates total fantasy points
    """
    expr = pl.lit(0.0)

    # Create case-insensitive column lookup
    df_cols_lower = {col.lower(): col for col in df_columns}

    # Track which stats were successfully mapped
    mapped_stats = []
    unmapped_stats = []

    for rule in rules:
        stat_name = rule.get("stat", "") or rule.get("name", "")
        points_per = rule.get("points", 0.0)
        position_types = rule.get("position_types", [])

        # Use position-aware mapping system
        position_mappings = build_position_aware_mappings(df_columns, position_types)

        # Check if this needs special multi-column handling
        is_defense = position_types and any(pt in ["D", "DT"] for pt in position_types)

        # DST TDs need special handling - skip direct mapping and use summing logic
        if is_defense and stat_name in ["TD", "Touchdown"]:
            pass  # Will be handled below in special DST TD section
        # Check if this stat has a direct mapping
        elif stat_name in position_mappings:
            mapped_col = position_mappings[stat_name]
            expr = expr + (pl.col(mapped_col).cast(pl.Float64, strict=False).fill_null(0) * points_per)
            mapped_stats.append(f"{stat_name} -> {mapped_col}")
            continue

        # Special handling for stats that sum multiple columns
        if stat_name in ["2-Point Conversions", "2-PT"]:
            # Sum all 2-point conversion types
            two_pt_cols = ["passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"]
            two_pt_expr = pl.lit(0.0)
            found_any = False
            for col_name in two_pt_cols:
                if col_name.lower() in df_cols_lower:
                    two_pt_expr = two_pt_expr + pl.col(df_cols_lower[col_name.lower()]).cast(
                        pl.Float64, strict=False
                    ).fill_null(0)
                    found_any = True
            if found_any:
                expr = expr + (two_pt_expr * points_per)
                mapped_stats.append(f"{stat_name} -> [2PT conversions sum]")
            else:
                unmapped_stats.append(stat_name)
            continue

        if stat_name in ["Fumbles Lost", "Fum Lost"]:
            # Sum all fumble types
            fumble_cols = ["sack_fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost", "fumbles_lost"]
            fumble_expr = pl.lit(0.0)
            found_any = False
            for col_name in fumble_cols:
                if col_name.lower() in df_cols_lower:
                    fumble_expr = fumble_expr + pl.col(df_cols_lower[col_name.lower()]).cast(
                        pl.Float64, strict=False
                    ).fill_null(0)
                    found_any = True
            if found_any:
                expr = expr + (fumble_expr * points_per)
                mapped_stats.append(f"{stat_name} -> [fumbles sum]")
            else:
                unmapped_stats.append(stat_name)
            continue

        # Special handling for DST Touchdowns
        # The Yahoo "TD" stat for defense should include BOTH:
        # - def_tds (INT return TDs, pick-sixes)
        # - fum_ret_td (fumble return TDs)
        # These are tracked separately in NFLverse but should be summed for fantasy scoring
        # Note: is_defense is defined above
        if is_defense and stat_name in ["TD", "Touchdown"]:
            td_cols = ["def_tds", "fum_ret_td", "defensive_tds"]
            td_expr = pl.lit(0.0)
            found_any = False
            for col_name in td_cols:
                if col_name.lower() in df_cols_lower:
                    td_expr = td_expr + pl.col(df_cols_lower[col_name.lower()]).cast(
                        pl.Float64, strict=False
                    ).fill_null(0)
                    found_any = True
            if found_any:
                expr = expr + (td_expr * points_per)
                mapped_stats.append(f"{stat_name} -> [DST TDs sum: def_tds + fum_ret_td]")
            else:
                unmapped_stats.append(stat_name)
            continue

        # Fallback: try legacy normalize_stat_name
        norm_stat = normalize_stat_name(stat_name, position_types)
        matching_col = df_cols_lower.get(norm_stat.lower())

        if matching_col:
            expr = expr + (pl.col(matching_col).cast(pl.Float64, strict=False).fill_null(0) * points_per)
            mapped_stats.append(f"{stat_name} -> {matching_col} (fallback)")
        else:
            unmapped_stats.append(stat_name)

    # Log mapping results for debugging (only once per unique set of unmapped stats)
    # Note: This is expected when using pts_* columns from super_table instead of raw stats
    if unmapped_stats:
        unmapped_key = frozenset(unmapped_stats)
        if unmapped_key not in _warned_unmapped_stats:
            _warned_unmapped_stats.add(unmapped_key)
            # Only log as debug - expected when super_table uses pre-calculated pts_* columns
            import os

            if os.environ.get("DEBUG_SCORING"):
                print(
                    f"[scoring] Debug: {len(unmapped_stats)} stats not in DataFrame (using pts_* columns): {', '.join(unmapped_stats[:5])}"
                )

    # SAFETY: Ensure the expression never returns NULL
    return expr.fill_null(0.0)


def calculate_fantasy_points(
    df: pl.DataFrame,
    scoring_rules_by_year: dict[int, dict[str, Any]],
    year_col: str = "year",
    league_start_year: int | None = None,
) -> pl.DataFrame:
    """
    Calculate fantasy points for each player based on year-specific scoring rules.

    Args:
        df: DataFrame with player stats
        scoring_rules_by_year: Dict mapping year to scoring rules
        year_col: Name of year column
        league_start_year: The year the league started (from context). If provided,
                          years before this will use earliest available scoring rules.

    Returns:
        DataFrame with fantasy_points column added
    """
    if not scoring_rules_by_year:
        # No scoring rules - preserve existing fantasy_points if present, otherwise set to 0
        if "fantasy_points" not in df.columns:
            return df.with_columns(pl.lit(0.0).alias("fantasy_points"))
        else:
            # Keep existing fantasy_points column (from Yahoo data)
            return df

    df_cols = df.columns
    result_frames = []

    # Get all unique years in the data
    all_years = df.select(pl.col(year_col).unique()).to_series().to_list()
    available_rule_years = sorted(scoring_rules_by_year.keys())

    # Use league_start_year from context if provided, otherwise try to detect from data
    league_start = league_start_year

    if league_start is None and "manager" in df.columns:
        # Fallback: try to detect from data (for backwards compatibility)
        # Filter for ACTUAL managers (not null and not "Unrostered")
        league_years = (
            df.filter(pl.col("manager").is_not_null() & (pl.col("manager") != "Unrostered"))
            .select(year_col)
            .unique()
            .sort(year_col)
        )

        if len(league_years) > 0:
            league_start = league_years[year_col].min()
            print(f"[scoring] League years detected from data: {league_start} to {league_years[year_col].max()}")
        else:
            # No rostered players found - all years are pre-league
            print("[scoring] No rostered players found - treating all years as pre-league")

    if league_start is not None:
        print(f"[scoring] League start year: {league_start}")

    # Track pre-league years for consolidated logging
    pre_league_years = []
    fallback_years = []

    # Process each year in the data
    for year in all_years:
        df_year = df.filter(pl.col(year_col) == year)

        if df_year.is_empty():
            continue

        # Find rules for this year (exact match or nearest year)
        if year in scoring_rules_by_year:
            rules = scoring_rules_by_year[year]
            settings_source = year
        else:
            # Fallback: use nearest year's rules (handles pre-league and missing years)
            if available_rule_years:
                # For pre-league years (or when league_start unknown), prefer earliest available settings
                if league_start is None or year < league_start:
                    settings_source = min(available_rule_years)
                    pre_league_years.append(year)
                else:
                    # Find closest year
                    closest_year = min(available_rule_years, key=lambda y: abs(y - year))
                    settings_source = closest_year
                    if settings_source != year:
                        fallback_years.append((year, closest_year))

                rules = scoring_rules_by_year[settings_source]
            else:
                # No rules at all, set to 0
                df_year = df_year.with_columns(pl.lit(0.0).alias("fantasy_points"))
                result_frames.append(df_year)
                continue

        # Build points expression for this year
        scoring_list = rules.get("scoring", [])
        points_expr = build_points_expression(scoring_list, df_cols)

        # Add fantasy_points column
        df_year = df_year.with_columns(points_expr.alias("fantasy_points"))
        result_frames.append(df_year)

    # Log consolidated summary for pre-league/fallback years
    if pre_league_years:
        earliest_rules = min(available_rule_years) if available_rule_years else "N/A"
        print(
            f"[scoring] {len(pre_league_years)} pre-league years ({min(pre_league_years)}-{max(pre_league_years)}): Using {earliest_rules} rules"
        )
    if fallback_years:
        print(f"[scoring] {len(fallback_years)} years used fallback rules")

    if not result_frames:
        return df.with_columns(pl.lit(0.0).alias("fantasy_points"))

    # Combine all years
    return pl.concat(result_frames, how="vertical")
