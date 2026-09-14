"""Canonical scoring configuration for cross-platform normalization.

All scoring settings (Yahoo, ESPN, Sleeper) are normalized into a single
canonical dict format using Sleeper's key names. This is the single source
of truth for cross-platform stat mapping.

Usage:
    from multi_league.core.scoring_config import (
        normalize_yahoo, normalize_espn, normalize_sleeper,
        normalize_scoring_settings, get_fpts_column, is_standard_scoring,
    )
"""

import math
import re
from collections.abc import Mapping

# ---------------------------------------------------------------------------
# Yahoo stat_id -> canonical Sleeper key (82 entries)
# ---------------------------------------------------------------------------
YAHOO_STAT_ID_MAP: dict[int, str] = {
    # Passing
    1: "pass_att",
    2: "pass_cmp",
    3: "pass_inc",
    4: "pass_yd",
    5: "pass_td",
    6: "pass_int",
    7: "pass_sack",
    # Rushing
    8: "rush_att",  # display only in most leagues
    9: "rush_yd",
    10: "rush_td",
    # Receiving
    11: "rec",
    12: "rec_yd",
    13: "rec_td",
    # Misc offense
    14: "st_yd",
    15: "st_td",
    16: "pass_2pt",  # Expanded below: Yahoo uses 16 for all 2pt conversions
    17: "fum",
    18: "fum_lost",
    # Kicker (19-30, 84)
    19: "fgm_0_19",
    20: "fgm_20_29",
    21: "fgm_30_39",
    22: "fgm_40_49",
    23: "fgm_50p",
    24: "fgmiss_0_19",
    25: "fgmiss_20_29",
    26: "fgmiss_30_39",
    27: "fgmiss_40_49",
    28: "fgmiss_50p",
    29: "xpm",
    30: "xpmiss",
    84: "fgm_yds",
    # DST (32-37, 48-49, 50-57, 67-68, 70-77, 82)
    32: "sack",
    33: "int",
    34: "fum_rec",
    35: "def_td",
    36: "safe",
    37: "blk_kick",
    48: "def_st_yd",
    49: "def_st_td",
    50: "pts_allow_0",
    51: "pts_allow_1_6",
    52: "pts_allow_7_13",
    53: "pts_allow_14_20",
    54: "pts_allow_21_27",
    55: "pts_allow_28_34",
    56: "pts_allow_35p",
    57: "fum_rec_td",
    67: "def_4_and_stop",
    68: "tkl_loss",
    70: "yds_allow_neg",
    71: "yds_allow_0_100",
    72: "yds_allow_100_199",
    73: "yds_allow_200_299",
    74: "yds_allow_300_349",
    75: "yds_allow_400_449",
    76: "yds_allow_500_549",
    77: "def_3_and_out",
    82: "def_2pt",
    # IDP (38-47, 65, 83)
    38: "idp_tkl_solo",
    39: "idp_tkl_ast",
    40: "idp_sack",
    41: "idp_int",
    42: "idp_ff",
    43: "idp_fum_rec",
    44: "idp_def_td",
    45: "idp_safe",
    46: "idp_pass_def",
    47: "idp_blk_kick",
    65: "idp_tkl_loss",
    83: "idp_xpr",
    # Bonus / Big Play (58-64, 78-81)
    58: "pass_int_td",
    59: "pass_cmp_40p",
    60: "pass_td_40p",
    61: "rush_40p",
    62: "rush_td_40p",
    63: "rec_40p",
    64: "rec_td_40p",
    # Display / aggregate
    78: "rec_targets",  # display only
    79: "pass_fd",
    80: "rec_fd",
    81: "rush_fd",
}


# Yahoo exposes milestone bonuses as nested modifiers on the base stat ID.
# Treat the observed targets as a regression corpus, not a whitelist: Yahoo can
# emit arbitrary numeric thresholds and the pipeline should preserve them
# without adding a new league_settings column for every target.
YAHOO_STAT_MODIFIER_BONUS_PREFIXES: dict[str, str] = {
    "2": "bonus_pass_cmp",
    "4": "bonus_pass_yd",
    "9": "bonus_rush_yd",
    "12": "bonus_rec_yd",
    "14": "bonus_st_yd",
    "48": "bonus_def_st_yd",
}

YAHOO_STAT_MODIFIER_BONUS_SOURCES: dict[str, str] = {
    "2": "pass_cmp",
    "4": "pass_yd",
    "9": "rush_yd",
    "12": "rec_yd",
    "14": "ret_yd",
    "48": "def_st_yd",
}

YAHOO_STAT_MODIFIER_BONUS_OBSERVED_TARGETS: dict[str, tuple[int, ...]] = {
    "2": (25,),
    "4": (200, 250, 300, 325, 350, 375, 400, 425, 450, 500, 555),
    "9": (75, 85, 100, 125, 130, 150, 160, 165, 175, 200, 250, 296, 297, 300),
    "12": (75, 100, 125, 130, 140, 150, 160, 175, 180, 200, 250, 300, 337),
    "14": (30, 75, 100, 125, 175, 200, 225),
    "48": (200, 250, 300),
}

YAHOO_STAT_MODIFIER_BONUS_KEY_MAP: dict[tuple[str, int], str] = {
    (stat_id, target): f"{YAHOO_STAT_MODIFIER_BONUS_PREFIXES[stat_id]}_{target}"
    for stat_id, targets in YAHOO_STAT_MODIFIER_BONUS_OBSERVED_TARGETS.items()
    for target in targets
}


def _modifier_bonus_target_int(target: str | int | float) -> int | None:
    try:
        target_float = float(target)
    except (TypeError, ValueError):
        return None
    target_int = int(target_float)
    if target_int <= 0 or target_float != target_int:
        return None
    return target_int


def yahoo_stat_modifier_bonus_key(stat_id: str | int, target: str | int | float) -> str | None:
    """Return the canonical scoring key for a Yahoo nested bonus modifier."""
    stat_key = str(stat_id)
    prefix = YAHOO_STAT_MODIFIER_BONUS_PREFIXES.get(stat_key)
    target_int = _modifier_bonus_target_int(target)
    if prefix is None or target_int is None:
        return None
    return f"{prefix}_{target_int}"


def yahoo_stat_modifier_bonus_stat_threshold(key: str) -> tuple[str, int] | None:
    """Parse a canonical Yahoo modifier bonus key into (Yahoo stat_id, target)."""
    for stat_id, prefix in YAHOO_STAT_MODIFIER_BONUS_PREFIXES.items():
        marker = f"{prefix}_"
        if not key.startswith(marker):
            continue
        target = _modifier_bonus_target_int(key.removeprefix(marker))
        if target is not None:
            return stat_id, target
    return None


def yahoo_stat_modifier_bonus_source_threshold(key: str) -> tuple[str, int] | None:
    """Parse a canonical Yahoo modifier bonus key into (atomic source, target)."""
    parsed = yahoo_stat_modifier_bonus_stat_threshold(key)
    if parsed is None:
        return None
    stat_id, target = parsed
    source = YAHOO_STAT_MODIFIER_BONUS_SOURCES.get(stat_id)
    if source is None:
        return None
    return source, target


# Yahoo stat_ids that represent a single platform setting but need to expand
# into multiple canonical DDL keys.
YAHOO_MULTI_STAT_ID_MAP: dict[int, tuple[str, ...]] = {
    # Yahoo's single 2-point conversion setting applies to passing, rushing,
    # and receiving conversions. The weekly stat row stores the conversion as
    # an untyped stat_id=16, while super_table has typed columns.
    16: ("pass_2pt", "rush_2pt", "rec_2pt"),
    # Turnover Return Yards = interception return yards + fumble return yards.
    66: ("int_ret_yd", "fum_ret_yd"),
}

# ---------------------------------------------------------------------------
# ESPN stat_id -> canonical Sleeper key
#
# Source of truth: docs/superpowers/findings/2026-05-02-espn-stat-id-map-audit.md
# Verified against espn_api SETTINGS_SCORING_FORMAT_MAP (235 entries) and live
# njfl 2024 payload (docs/superpowers/findings/njfl_2024_espn_scoring_settings_raw.json).
# Net: 102 -> 103 entries after 2026-05-05 team-margin mapping. The old
# fictional IDP block 140-172 was actually punter / team-margin scoring; punter
# scoring remains deferred, while team result / margin ids now map to DEF
# super-table columns. Three real IDP stat_ids (109, 112, 113) added. Bonus
# rectangle (15, 16, 46, 56, 57) and kicker block (74, 77, 79, 80, 85, 200,
# 203, 206, 209, 214) corrected.
# ---------------------------------------------------------------------------
ESPN_STAT_ID_MAP: dict[int, str] = {
    # Passing core (verified against espn_api SETTINGS_SCORING_FORMAT_MAP)
    0: "pass_att",
    1: "pass_cmp",
    3: "pass_yd",
    4: "pass_td",
    19: "pass_2pt",
    20: "pass_int",
    # 5  = Every 5 passing yards     — skip (every-N composite; superseded by stat_id 3)
    # 6  = Every 10 passing yards    — skip (every-N composite)
    # 7  = Every 20 passing yards    — skip (every-N composite)
    # 8  = Every 25 passing yards    — skip (every-N composite)
    15: "pass_td_40p",  # 40+ yard TD pass bonus — fixed 2026-05-02 (was pass_cmp_40p)
    16: "pass_td_50p",  # 50+ yard TD pass bonus — fixed 2026-05-02 (was pass_cmp_50p)
    17: "bonus_pass_yd_300",  # 300-399 yard passing game — added 2026-05-02
    18: "bonus_pass_yd_400",  # 400+ yard passing game    — added 2026-05-02
    # Rushing core
    23: "rush_att",
    24: "rush_yd",
    25: "rush_td",
    26: "rush_2pt",
    # 28 = Every 10 rushing yards — skip (every-N composite)
    # 29 = Every 20 rushing yards — skip (every-N composite)
    # 34 = Every 10 rush attempts — skip (every-N composite)
    35: "rush_td_40p",  # 40+ yard TD rush bonus — Fixed 2026-05-02: was rush_40p (any-yardage); library says TD-only bonus
    36: "rush_td_50p",  # 50+ yard TD rush bonus    — added 2026-05-02
    37: "bonus_rush_yd_100",  # 100-199 yard rushing game — added 2026-05-02
    38: "bonus_rush_yd_200",  # 200+ yard rushing game    — added 2026-05-02
    # Receiving core
    41: "rec",
    42: "rec_yd",
    43: "rec_td",
    44: "rec_2pt",
    45: "rec_td_40p",  # 40+ yard TD rec bonus
    46: "rec_td_50p",  # 50+ yard TD rec bonus — fixed 2026-05-02 (was rec_40p)
    53: "rec",  # Each reception (alias)
    # 48 = Every 10 receiving yards — skip (every-N composite)
    # 49 = Every 20 receiving yards — skip (every-N composite)
    56: "bonus_rec_yd_100",  # 100-199 yard receiving game — fixed 2026-05-02 (was rec_td_50p)
    57: "bonus_rec_yd_200",  # 200+ yard receiving game    — fixed 2026-05-02 (was rec_40p_alt)
    # Misc offense
    63: "fum_rec_td",  # Fumble Recovered for TD
    64: "pass_sack",  # Sacked (QB sack taken)
    68: "fum",  # Total Fumbles
    72: "fum_lost",  # Total Fumbles Lost
    # Kicker (verified against espn_api; 2026-05-02 audit fixed the +3 fgmiss bug)
    74: "fgm_50p",  # FG Made (50+ yards)   — fixed 2026-05-02 (was st_td)
    76: "fgmiss_50p",  # FG Missed (50+ yards) — added 2026-05-02
    77: "fgm_40_49",  # FG Made (40-49 yards) — fixed 2026-05-02 (was fgm_0_39)
    # 78 = FG Attempted (40-49 yards) — not used in scoring (removed 2026-05-02)
    79: "fgmiss_40_49",  # FG Missed (40-49 yards) — fixed 2026-05-02 (was fgm_50p)
    80: "fgm_0_39",  # FG Made (0-39 yards)    — fixed 2026-05-02 (was 'fgmiss', the +3 bug)
    82: "fgmiss_0_39",  # FG Missed (0-39 yards)  — added 2026-05-02
    83: "fgm",  # Total FG Made           — added 2026-05-02
    85: "fgmiss",  # Total FG Missed         — fixed 2026-05-02 (was fg_pct; this is the true flat-miss key)
    86: "xpm",  # Each PAT Made
    88: "xpmiss",  # Each PAT Missed
    198: "fgm_50_59",  # FG Made (50-59 yards)
    200: "fgmiss_50_59",  # FG Missed (50-59 yards) — fixed 2026-05-02 (was fgmiss_0_39)
    201: "fgm_60p",  # FG Made (60+ yards)
    203: "fgmiss_60p",  # FG Missed (60+ yards) — fixed 2026-05-02 (was fgmiss_40_49)
    206: "def_2pt",  # 2pt Return — DST return on a failed conversion
    209: "one_pt_safe",  # 1pt Safety — fixed 2026-05-02 (was fgm_60p_alt)
    214: "fgm_yds",  # FG Made Yards — fixed 2026-05-02 (was st_yd)
    # DST (Defense / Special Teams team-level scoring)
    89: "pts_allow_0",
    90: "pts_allow_1_6",
    91: "pts_allow_7_13",
    92: "pts_allow_14_20",  # 14-17 points allowed (ESPN coarse — same canonical bucket)
    93: "def_blk_kick_td",  # Blocked Punt or FG return for TD
    94: "def_td",  # Fumble or INT Return for TD
    95: "int",  # Each Interception
    96: "fum_rec",  # Each Fumble Recovered
    97: "blk_kick",  # Blocked Punt, PAT or FG
    98: "safe",  # Each Safety
    99: "sack",  # Each Sack
    # 100 = 1/2 Sack — removed 2026-05-02 (no DDL canonical for half-sack;
    #                                       was wrongly mapped to def_st_yd)
    101: "def_kr_td",  # Kickoff Return TD
    102: "def_pr_td",  # Punt Return TD
    103: "def_int_ret_td",  # Interception Return TD
    104: "def_fum_ret_td",  # Fumble Return TD
    105: "def_st_td",  # Total Return TD
    106: "ff",  # Each Fumble Forced
    114: "def_kr_yd",  # Kickoff Return Yards
    115: "def_pr_yd",  # Punt Return Yards
    120: "pts_allow",  # Points Allowed (aggregate)
    121: "pts_allow_14_20_alt",  # 18-21 points allowed (ESPN coarse alt)
    122: "pts_allow_21_27",  # 22-27 points allowed
    123: "pts_allow_28_34",
    124: "pts_allow_35p",  # 35-45 points allowed
    125: "pts_allow_46p",  # 46+ points allowed
    # DST yards allowed
    128: "yds_allow_0_100",
    129: "yds_allow_100_199",
    130: "yds_allow_200_299",
    131: "yds_allow_300_349",
    132: "yds_allow_350_399",
    133: "yds_allow_400_449",
    134: "yds_allow_450_499",
    135: "yds_allow_500_549",
    136: "yds_allow_550p",
    # IDP — REAL stat_ids (added 2026-05-02; live njfl confirmed all three)
    109: "idp_tkl",  # Total Tackles            — added 2026-05-02
    112: "idp_tkl_loss",  # Stuffs (TFL)             — added 2026-05-02
    113: "idp_pass_def",  # Passes Defensed          — added 2026-05-02
    # ESPN team result / score / margin scoring. These are DST team-game facts.
    155: "team_win",
    156: "team_loss",
    157: "team_tie",
    158: "team_pts",
    # 159 = Points Scored Per Game — per-game display/composite; skip.
    160: "team_margin",
    161: "team_win_margin_25p",
    162: "team_win_margin_20_24",
    163: "team_win_margin_15_19",
    164: "team_win_margin_10_14",
    165: "team_win_margin_5_9",
    166: "team_win_margin_1_4",
    167: "team_loss_margin_1_4",
    168: "team_loss_margin_5_9",
    169: "team_loss_margin_10_14",
    170: "team_loss_margin_15_19",
    171: "team_loss_margin_20_24",
    172: "team_loss_margin_25p",
    # 173 = Margin of Victory Per Game — per-game display/composite; skip.
    # Offensive first downs (added 2026-05-02; library has labels)
    211: "pass_fd",  # Passing First Down — added 2026-05-02
    212: "rush_fd",  # Rushing First Down — added 2026-05-02
    213: "rec_fd",  # Receiving First Down — added 2026-05-02
}

# ESPN stat_ids that need to populate multiple canonical DDL keys. ESPN exposes
# these as a single scoring setting, but our scorer has separate individual
# player and DST contexts. The super table has total individual return TDs
# (`special_teams_tds`) plus DST return aggregates, not split individual KR/PR TD
# counts, so KR/PR TD settings also populate the individual `st_td` key.
ESPN_MULTI_STAT_ID_MAP: dict[int, tuple[str, ...]] = {
    101: ("def_kr_td", "st_td"),
    102: ("def_pr_td", "st_td"),
    105: ("def_st_td", "st_td"),
    112: ("idp_tkl_loss", "tkl_loss"),
    113: ("idp_pass_def", "def_pass_def"),
    114: ("def_kr_yd", "kr_yd"),
    115: ("def_pr_yd", "pr_yd"),
}


def canonical_keys_for_espn_stat_id(stat_id: int) -> tuple[str, ...]:
    """Return all canonical scoring keys represented by an ESPN stat ID."""
    multi_keys = ESPN_MULTI_STAT_ID_MAP.get(stat_id)
    if multi_keys:
        return multi_keys
    key = ESPN_STAT_ID_MAP.get(stat_id)
    return (key,) if key else ()


# ---------------------------------------------------------------------------
# ESPN native readable key -> canonical Sleeper key
# ---------------------------------------------------------------------------
ESPN_KEY_MAP: dict[str, str] = {
    "passing_yards": "pass_yd",
    "passing_td": "pass_td",
    "passing_interceptions": "pass_int",
    "passing_2pt_conversions": "pass_2pt",
    "rushing_yards": "rush_yd",
    "rushing_td": "rush_td",
    "rushing_2pt_conversions": "rush_2pt",
    "receiving_receptions": "rec",
    "receiving_yards": "rec_yd",
    "receiving_td": "rec_td",
    "receiving_2pt_conversions": "rec_2pt",
    "fumbles_lost": "fum_lost",
    "fumbles": "fum",
    "fumble_recovered_for_td": "fum_rec_td",
    "dst_sacks": "sack",
    "dst_interceptions": "int",
    "dst_fumble_recoveries": "fum_rec",
    "dst_forced_fumbles": "ff",
    "dst_blocked_kicks": "blk_kick",
    "dst_blocked_field_goals": "def_fg_block",
    "dst_blocked_punts": "def_punt_block",
    "dst_blocked_extra_points": "def_pat_block",
    "dst_safeties": "safe",
    "dst_blocked_kick_for_td": "def_blk_kick_td",
    "dst_td": "def_td",
    "dst_kickoff_return_td": "def_kr_td",
    "dst_punt_return_td": "def_pr_td",
    "dst_int_return_td": "def_int_ret_td",
    "dst_fumble_return_td": "def_fum_ret_td",
    "dst_special_teams_td": "def_st_td",
    "dst_points_allowed_0": "pts_allow_0",
    "dst_points_allowed_1_6": "pts_allow_1_6",
    "dst_points_allowed_7_13": "pts_allow_7_13",
    "dst_points_allowed_14_17": "pts_allow_14_20",
    "dst_points_allowed_18_21": "pts_allow_14_20",
    "dst_points_allowed_22_27": "pts_allow_21_27",
    "dst_points_allowed_28_34": "pts_allow_28_34",
    "dst_points_allowed_35_45": "pts_allow_35p",
    "dst_points_allowed_46_plus": "pts_allow_35p",
    "pat_made": "xpm",
    "pat_missed": "xpmiss",
    "fg_made_0_39": "fgm_0_39",
    "fg_made_40_49": "fgm_40_49",
    "fg_made_50_plus": "fgm_50p",
    "fg_missed": "fgmiss",
    "passing_sacks": "pass_sack",
}

ESPN_MULTI_KEY_MAP: dict[str, tuple[str, ...]] = {
    "dst_kickoff_return_td": ("def_kr_td", "st_td"),
    "dst_punt_return_td": ("def_pr_td", "st_td"),
    "dst_special_teams_td": ("def_st_td", "st_td"),
}


def canonical_keys_for_espn_key(key: str) -> tuple[str, ...]:
    """Return all canonical scoring keys represented by an ESPN readable key."""
    multi_keys = ESPN_MULTI_KEY_MAP.get(key)
    if multi_keys:
        return multi_keys
    return (ESPN_KEY_MAP.get(key, key),)


# ---------------------------------------------------------------------------
# Normalizer functions
# ---------------------------------------------------------------------------


def normalize_yahoo(scoring_rules: list[dict]) -> dict:
    """Convert Yahoo scoring_rules list to canonical dict.

    Args:
        scoring_rules: List of dicts with keys "stat_id", "name", "points".

    Returns:
        Canonical scoring dict using Sleeper key names. Zero-point rules
        are excluded.
    """
    canonical: dict[str, float] = {}
    for rule in scoring_rules:
        try:
            points = float(rule.get("points", 0))
        except (TypeError, ValueError):
            continue
        canonical_key = rule.get("canonical_key")
        if canonical_key and points != 0:
            canonical[str(canonical_key)] = points
            continue
        try:
            stat_id = int(rule.get("stat_id", 0))
        except (TypeError, ValueError):
            continue
        multi_keys = YAHOO_MULTI_STAT_ID_MAP.get(stat_id)
        if multi_keys and points != 0:
            for multi_key in multi_keys:
                canonical[multi_key] = points
            continue
        key = YAHOO_STAT_ID_MAP.get(stat_id)
        if key and points != 0:
            canonical[key] = points
    return canonical


def normalize_espn(scoring_settings: dict) -> dict:
    """Convert ESPN scoring_settings to canonical dict.

    ESPN settings may contain both ESPN-native readable keys (e.g.,
    "receiving_receptions") and opaque stat_NNN keys (e.g., "stat_206").
    Both are decoded to canonical Sleeper key names.

    Args:
        scoring_settings: Raw ESPN scoring settings dict.

    Returns:
        Canonical scoring dict using Sleeper key names.
    """
    canonical: dict[str, float] = {}
    for key, value in scoring_settings.items():
        try:
            fvalue = float(value)
        except (TypeError, ValueError):
            continue
        if key.startswith("stat_"):
            # Opaque numeric stat ID
            try:
                stat_id = int(key.split("_", 1)[1])
            except (IndexError, ValueError):
                continue
            for canon_key in canonical_keys_for_espn_stat_id(stat_id):
                canonical[canon_key] = fvalue
        else:
            # ESPN native readable key — map to canonical (fallback: keep as-is)
            for canon_key in canonical_keys_for_espn_key(key):
                canonical[canon_key] = fvalue
    return canonical


def normalize_sleeper(scoring_settings: dict) -> dict:
    """Passthrough with validation — Sleeper keys ARE the canonical format.

    Args:
        scoring_settings: Raw Sleeper scoring_settings dict.

    Returns:
        Canonical scoring dict (Sleeper keys, zero values stripped).
    """
    return {k: float(v) for k, v in scoring_settings.items() if isinstance(v, int | float) and v != 0}


def normalize_scoring_settings(platform: str, raw_settings: dict) -> dict:
    """Entry point: normalize any platform's settings to canonical.

    Args:
        platform: One of "yahoo", "espn", or "sleeper".
        raw_settings: The full settings dict for the league-year. Platform-
            specific sub-keys (e.g. "scoring_rules", "scoring_settings") are
            extracted internally.

    Returns:
        Canonical scoring dict using Sleeper key names.
    """
    if platform == "yahoo":
        return normalize_yahoo(raw_settings.get("scoring_rules", []))
    elif platform == "espn":
        return normalize_espn(raw_settings.get("scoring_settings", {}))
    else:
        return normalize_sleeper(raw_settings.get("scoring_settings", {}))


# ---------------------------------------------------------------------------
# Derived helpers
# ---------------------------------------------------------------------------

_MISSING = object()
_EPS = 1e-9


def _lookup_scoring_value(scoring_row: Mapping, key: str) -> object:
    """Read a canonical scoring key (`rec`, `pass_td`, etc.)."""
    if key in scoring_row:
        value = scoring_row[key]
        if value is not None and value != "":
            return value
    return _MISSING


def _assert_bare_scoring_row(scoring_row: Mapping) -> None:
    prefixed = sorted(str(k) for k in scoring_row if isinstance(k, str) and k.startswith("scoring_"))
    if prefixed:
        sample = ", ".join(prefixed[:3])
        raise ValueError(
            "Scoring helpers expect canonical bare keys like 'rec', not flat "
            f"league_settings columns like {sample}. Use "
            "canonical_settings.extract_scoring_settings_from_flat_row() at the DDL boundary."
        )


def _has_scoring_value(scoring_row: Mapping, key: str) -> bool:
    return _lookup_scoring_value(scoring_row, key) is not _MISSING


def _scoring_float(scoring_row: Mapping, key: str, default: float = 0.0) -> float:
    value = _lookup_scoring_value(scoring_row, key)
    if value is _MISSING:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(out):
        return default
    return out


def _fmt_num(value: float) -> str:
    if abs(value) < _EPS:
        value = 0.0
    return f"{value:.12g}"


def _col(col_name: str) -> str:
    return f"COALESCE({col_name}, 0)"


def _mult_term(mult: float, col_name: str) -> str | None:
    if abs(mult) < _EPS:
        return None
    return f"({_fmt_num(mult)} * {_col(col_name)})"


def _sum_sql(parts: list[str | None]) -> str:
    terms = [p for p in parts if p]
    if not terms:
        return "0"
    return " + ".join(terms)


def get_fpts_column(canonical: dict) -> str:
    """Derive the best precomputed fpts_* column name from canonical scoring.

    Mapping logic:
    - PPR tier: 0 -> "0ppr", 0.5 -> "half", 1.0 -> "ppr"
      Non-standard PPR with TE premium bonus -> "tep"
      Other non-standard PPR -> falls back to "half"
    - Pass TD points: rounded to nearest of 4, 5, 6
    - Return yard bonus (st_yd / kr_yd > 0) appends "_ret"

    Args:
        canonical: Canonical scoring dict from any normalizer.

    Returns:
        Column name string such as "fpts_4pt_half", "fpts_6pt_ppr", etc.
    """
    _assert_bare_scoring_row(canonical)
    ppr = _scoring_float(canonical, "rec", 0)
    pass_td = _scoring_float(canonical, "pass_td", 4)
    has_ret = (
        _scoring_float(canonical, "st_yd", 0) > 0
        or _scoring_float(canonical, "kr_yd", 0) > 0
        or _scoring_float(canonical, "pr_yd", 0) > 0
    )

    # TE premium takes precedence over standard ppr tiers
    if _scoring_float(canonical, "bonus_rec_te", 0) > 0:
        ppr_key = "tep"
    else:
        ppr_key = {0: "0ppr", 0.5: "half", 1.0: "ppr"}.get(ppr)
        if ppr_key is None:
            # Non-standard PPR value: fall back to "half"
            ppr_key = "half"

    # Round pass_td to nearest of 4, 5, 6; break ties toward higher value
    td_key = f"{min([4, 5, 6], key=lambda x: (abs(x - pass_td), -x))}pt"
    ret_suffix = "_ret" if has_ret else ""
    return f"fpts_{td_key}_{ppr_key}{ret_suffix}"


def is_standard_scoring(canonical: dict) -> bool:
    """True if league matches a precomputed fpts_* column exactly.

    "Standard" means the yardage rates, TD values, and penalty values all
    match one of the known precomputed variants in the super_table.

    Args:
        canonical: Canonical scoring dict from any normalizer.

    Returns:
        True if all key scoring parameters are at standard values.
    """
    return (
        canonical.get("pass_yd", 0.04) == 0.04
        and canonical.get("rush_yd", 0.1) == 0.1
        and canonical.get("rec_yd", 0.1) == 0.1
        and canonical.get("pass_td", 4) in (4, 5, 6)
        and canonical.get("pass_int", -1) in (-1, -2)
        and canonical.get("rush_td", 6) == 6
        and canonical.get("rec_td", 6) == 6
        and canonical.get("fum_lost", -2) in (-1, -2)
    )


# ---------------------------------------------------------------------------
# fpts_* and LAMAR delta-correction SQL builders
# ---------------------------------------------------------------------------

_FPTS_RE = re.compile(r"^fpts_(?P<td>[456])pt_(?P<ppr>0ppr|half|ppr|tep)(?P<ret>_ret)?$")
_LAMAR_RE = re.compile(r"^lamar(?:_ppg)?_(?P<size>\d+t)_(?P<roster>flx|sflx|idp)_(?P<ppr>std|half|ppr)_(?P<td>[46])pt$")

_PPR_BAKED = {"0ppr": 0.0, "half": 0.5, "ppr": 1.0}
_FPTS_PPR_COMPONENTS = {
    "0ppr": "pts_rec_0ppr",
    "half": "pts_rec_half",
    "ppr": "pts_rec_ppr",
    "tep": "pts_rec_tep",
}
_IDP_POSITIONS_SQL = "'LB','ILB','OLB','MLB','DL','DE','DT','NT','ED','DB','CB','S','SS','FS','SAF'"


def _parse_fpts_col(fpts_col_name: str) -> tuple[float, str, bool]:
    match = _FPTS_RE.match(fpts_col_name)
    if not match:
        raise ValueError(f"Unsupported fpts column: {fpts_col_name}")
    return float(match.group("td")), match.group("ppr"), bool(match.group("ret"))


def get_fpts_variant_columns() -> list[str]:
    """Return the 24 precomputed fpts_* composite column names."""
    cols: list[str] = []
    for td_key in ("4pt", "5pt", "6pt"):
        for ppr_key in ("0ppr", "half", "ppr", "tep"):
            base = f"fpts_{td_key}_{ppr_key}"
            cols.append(base)
            cols.append(f"{base}_ret")
    return cols


def compute_fpts_composite_formula(fpts_col_name: str) -> str:
    """Return the canonical SQL formula for a baked fpts_* composite column."""
    baked_td, ppr_key, has_ret = _parse_fpts_col(fpts_col_name)
    terms = [
        _col(f"pts_pass_{int(baked_td)}pt"),
        _col("pts_rush"),
        _col(_FPTS_PPR_COMPONENTS[ppr_key]),
        _col("pts_misc"),
        _col("pts_k_std"),
        _col("pts_def_std"),
    ]
    if has_ret:
        terms.append(_col("pts_ret_yds"))
    return " + ".join(terms)


def _te_receptions_sql() -> str:
    return "CASE WHEN UPPER(COALESCE(position, '')) = 'TE' THEN COALESCE(receptions, 0) ELSE 0 END"


def _offense_correction_terms(scoring_row: Mapping, fpts_col_name: str) -> list[str]:
    baked_td, ppr_key, has_ret = _parse_fpts_col(fpts_col_name)
    terms: list[str] = []

    actual_td = _scoring_float(scoring_row, "pass_td", baked_td)
    td_delta = actual_td - baked_td
    if abs(td_delta) >= _EPS:
        terms.append(f"({_fmt_num(td_delta)} * {_col('passing_tds')})")

    if ppr_key == "tep":
        baked_ppr = 1.0
        baked_te_bonus = 0.5
    else:
        baked_ppr = _PPR_BAKED[ppr_key]
        baked_te_bonus = 0.0

    actual_ppr = _scoring_float(scoring_row, "rec", baked_ppr)
    rec_delta = actual_ppr - baked_ppr
    if abs(rec_delta) >= _EPS:
        terms.append(f"({_fmt_num(rec_delta)} * {_col('receptions')})")

    actual_te_bonus = _scoring_float(scoring_row, "bonus_rec_te", baked_te_bonus)
    te_delta = actual_te_bonus - baked_te_bonus
    if abs(te_delta) >= _EPS:
        terms.append(f"({_fmt_num(te_delta)} * {_te_receptions_sql()})")

    baked_ret = 0.04 if has_ret else 0.0
    st_ret = _lookup_scoring_value(scoring_row, "st_yd")
    default_kr = _scoring_float(scoring_row, "st_yd", baked_ret) if st_ret is not _MISSING else baked_ret
    default_pr = _scoring_float(scoring_row, "st_yd", baked_ret) if st_ret is not _MISSING else baked_ret
    actual_kr = _scoring_float(scoring_row, "kr_yd", default_kr)
    actual_pr = _scoring_float(scoring_row, "pr_yd", default_pr)

    kr_delta = actual_kr - baked_ret
    pr_delta = actual_pr - baked_ret
    if abs(kr_delta) >= _EPS:
        terms.append(f"({_fmt_num(kr_delta)} * {_col('kickoff_return_yards')})")
    if abs(pr_delta) >= _EPS:
        terms.append(f"({_fmt_num(pr_delta)} * {_col('punt_return_yards')})")

    return terms


def _score_bucket(scoring_row: Mapping, key: str, default: float, *fallback_keys: str) -> float:
    if _has_scoring_value(scoring_row, key):
        return _scoring_float(scoring_row, key, default)
    for fallback in fallback_keys:
        if _has_scoring_value(scoring_row, fallback):
            return _scoring_float(scoring_row, fallback, default)
    return default


def _kicker_correction_term(scoring_row: Mapping) -> str | None:
    """Return custom K scoring minus baked pts_k_std, or None when exact."""
    yardage_mult = _scoring_float(scoring_row, "fgm_yds", 0.0)
    over30_mult = _scoring_float(scoring_row, "fgm_yds_over_30", 0.0)
    flat_fgm = _scoring_float(scoring_row, "fgm", 0.0)
    has_yardage = abs(yardage_mult) >= _EPS or abs(over30_mult) >= _EPS
    has_flat_fgm = _has_scoring_value(scoring_row, "fgm") and abs(flat_fgm) >= _EPS

    terms: list[str | None] = []
    exact_made = False
    if has_yardage:
        terms.append(_mult_term(yardage_mult, "fg_yards_canonical"))
        terms.append(_mult_term(over30_mult, "fg_yards_over_30_canonical"))
    elif has_flat_fgm:
        terms.append(_mult_term(flat_fgm, "fg_made"))
    else:
        fgm_0_19 = _score_bucket(scoring_row, "fgm_0_19", 3.0, "fgm_0_39")
        fgm_20_29 = _score_bucket(scoring_row, "fgm_20_29", 3.0, "fgm_0_39")
        fgm_30_39 = _score_bucket(scoring_row, "fgm_30_39", 3.0, "fgm_0_39")
        fgm_40_49 = _score_bucket(scoring_row, "fgm_40_49", 4.0)
        fgm_50_59 = _score_bucket(scoring_row, "fgm_50_59", 5.0, "fgm_50p", "fgm_50_59_alt")
        fgm_60p = _score_bucket(scoring_row, "fgm_60p", 6.0, "fgm_50p", "fgm_60p_alt")
        exact_made = (
            abs(fgm_0_19 - 3.0) < _EPS
            and abs(fgm_20_29 - 3.0) < _EPS
            and abs(fgm_30_39 - 3.0) < _EPS
            and abs(fgm_40_49 - 4.0) < _EPS
            and abs(fgm_50_59 - 5.0) < _EPS
            and abs(fgm_60p - 6.0) < _EPS
        )
        terms += [
            _mult_term(fgm_0_19, "fg_made_0_19"),
            _mult_term(fgm_20_29, "fg_made_20_29"),
            _mult_term(fgm_30_39, "fg_made_30_39"),
            _mult_term(fgm_40_49, "fg_made_40_49"),
            _mult_term(fgm_50_59, "fg_made_50_59"),
            _mult_term(fgm_60p, "fg_made_60_plus_canonical"),
        ]

    has_distance_miss = any(
        _has_scoring_value(scoring_row, key)
        for key in (
            "fgmiss_0_19",
            "fgmiss_0_39",
            "fgmiss_20_29",
            "fgmiss_30_39",
            "fgmiss_40_49",
            "fgmiss_50p",
            "fgmiss_50_59",
            "fgmiss_60p",
        )
    )
    if has_distance_miss:
        fgmiss_0_19 = _score_bucket(scoring_row, "fgmiss_0_19", 0.0, "fgmiss_0_39")
        fgmiss_20_29 = _score_bucket(scoring_row, "fgmiss_20_29", 0.0, "fgmiss_0_39")
        fgmiss_30_39 = _score_bucket(scoring_row, "fgmiss_30_39", 0.0, "fgmiss_0_39")
        fgmiss_40_49 = _score_bucket(scoring_row, "fgmiss_40_49", 0.0)
        fgmiss_50_59 = _score_bucket(scoring_row, "fgmiss_50_59", 0.0, "fgmiss_50p")
        fgmiss_60p = _score_bucket(scoring_row, "fgmiss_60p", 0.0, "fgmiss_50p")
        terms += [
            _mult_term(fgmiss_0_19, "fg_missed_0_19"),
            _mult_term(fgmiss_20_29, "fg_missed_20_29"),
            _mult_term(fgmiss_30_39, "fg_missed_30_39"),
            _mult_term(fgmiss_40_49, "fg_missed_40_49"),
            _mult_term(fgmiss_50_59, "fg_missed_50_59"),
            _mult_term(fgmiss_60p, "fg_missed_60_"),
        ]
        exact_miss = False
    else:
        fgmiss = _scoring_float(scoring_row, "fgmiss", -1.0)
        terms.append(_mult_term(fgmiss, "fg_missed"))
        exact_miss = abs(fgmiss - -1.0) < _EPS

    xpm = _scoring_float(scoring_row, "xpm", 1.0)
    xpmiss = _scoring_float(scoring_row, "xpmiss", 0.0)
    terms.append(_mult_term(xpm, "pat_made"))
    terms.append(_mult_term(xpmiss, "pat_missed"))
    exact_pat = abs(xpm - 1.0) < _EPS and abs(xpmiss) < _EPS

    if (not has_yardage and not has_flat_fgm and exact_made) and exact_miss and exact_pat:
        return None

    actual_sql = _sum_sql(terms)
    return f"(({actual_sql}) - {_col('pts_k_std')})"


def _defense_correction_term(scoring_row: Mapping) -> str | None:
    """Return custom DEF scoring minus baked pts_def_std, or None when exact."""
    stat_defaults = {
        "sack": ("pts_def_sack", 1.0),
        "int": ("pts_def_int", 2.0),
        "ff": ("pts_def_ff", 0.0),
        "fum_rec": ("pts_def_fr", 2.0),
        "def_td": ("pts_def_td", 6.0),
        "safe": ("pts_def_safety", 2.0),
        "blk_kick": ("pts_def_block", 2.0),
    }
    terms: list[str | None] = []
    exact_stats = True
    for key, (col_name, default) in stat_defaults.items():
        mult = _scoring_float(scoring_row, key, default)
        exact_stats = exact_stats and abs(mult - default) < _EPS
        terms.append(_mult_term(mult, col_name))

    extra_stats = {
        "tkl_loss": "pts_def_tfl",
        "def_3_and_out": "pts_def_3out",
        "def_4_and_stop": "pts_def_4stop",
        "def_st_yd": "pts_def_ret_yd",
        "def_kr_yd": "pts_def_ret_yd",
        "def_pr_yd": "pts_def_ret_yd",
        # def_st_td / def_kr_td / def_pr_td handled below with a baked +6 baseline
        # (return TDs are now folded into pts_def_std), not here as additive-from-0.
        "def_fg_block": "pts_def_fg_block",
        "fg_block": "pts_def_fg_block",
        "def_punt_block": "pts_def_punt_block",
        "punt_block": "pts_def_punt_block",
        "def_pat_block": "pts_def_pat_block",
        "pat_block": "pts_def_pat_block",
        "team_win": "pts_def_team_win",
        "team_loss": "pts_def_team_loss",
        "team_tie": "pts_def_team_tie",
        "team_pts": "pts_def_team_pts",
        "team_margin": "pts_def_team_margin",
        "team_win_margin_25p": "pts_def_team_win_margin_25p",
        "team_win_margin_20_24": "pts_def_team_win_margin_20_24",
        "team_win_margin_15_19": "pts_def_team_win_margin_15_19",
        "team_win_margin_10_14": "pts_def_team_win_margin_10_14",
        "team_win_margin_5_9": "pts_def_team_win_margin_5_9",
        "team_win_margin_1_4": "pts_def_team_win_margin_1_4",
        "team_loss_margin_1_4": "pts_def_team_loss_margin_1_4",
        "team_loss_margin_5_9": "pts_def_team_loss_margin_5_9",
        "team_loss_margin_10_14": "pts_def_team_loss_margin_10_14",
        "team_loss_margin_15_19": "pts_def_team_loss_margin_15_19",
        "team_loss_margin_20_24": "pts_def_team_loss_margin_20_24",
        "team_loss_margin_25p": "pts_def_team_loss_margin_25p",
    }
    exact_extra = True
    for key, col_name in extra_stats.items():
        mult = _scoring_float(scoring_row, key, 0.0)
        if abs(mult) >= _EPS:
            exact_extra = False
            terms.append(_mult_term(mult, col_name))

    # Return TDs (KR/PR) are baked into pts_def_std at the modal +6 (89% of league-years award a
    # DST return TD, median 6). Emit the league's FULL value here — the final `- pts_def_std` nets
    # each league to its own total, so pts_def_std cancels and totals are invariant. "Exact" now
    # means the league matches the baked +6 (not 0): a league scoring return TDs at 6 relies on the
    # baked baseline; one that doesn't score them forces the correction (0 - baked 6 = -6*ret_td).
    # Unified def_st_td is the modal key (3,491 lyr); split def_kr_td/def_pr_td (695 lyr) fall back to
    # a single credit on the combined pts_def_ret_td column (no double-count).
    ret_td_mult = 0.0
    for _ret_key in ("def_st_td", "def_kr_td", "def_pr_td"):
        if _has_scoring_value(scoring_row, _ret_key):
            _v = _scoring_float(scoring_row, _ret_key, 0.0)
            if abs(_v) >= _EPS:
                ret_td_mult = _v
                break
    exact_ret_td = abs(ret_td_mult - 6.0) < _EPS
    if abs(ret_td_mult) >= _EPS:
        terms.append(_mult_term(ret_td_mult, "pts_def_ret_td"))

    pa_defaults = {
        "pts_allow_0": ("pts_allow_0", 10.0),
        "pts_allow_1_6": ("pts_allow_1_6", 7.0),
        "pts_allow_7_13": ("pts_allow_7_13", 4.0),
        "pts_allow_14_20": ("pts_allow_14_20", 1.0),
        "pts_allow_21_27": ("pts_allow_21_27", 0.0),
        "pts_allow_28_34": ("pts_allow_28_34", -1.0),
        "pts_allow_35p": ("pts_allow_35_plus", -4.0),
    }
    exact_pa = True
    for key, (col_name, default) in pa_defaults.items():
        if key == "pts_allow_14_20":
            mult = _score_bucket(scoring_row, key, default, "pts_allow_14_20_alt")
        elif key == "pts_allow_35p":
            mult = _score_bucket(scoring_row, key, default, "pts_allow_46p")
        else:
            mult = _scoring_float(scoring_row, key, default)
        exact_pa = exact_pa and abs(mult - default) < _EPS
        terms.append(_mult_term(mult, col_name))

    ya_cols = {
        "yds_allow_neg": "yds_allow_0_99",
        "yds_allow_0_100": "yds_allow_0_99",
        "yds_allow_100_199": "yds_allow_100_199",
        "yds_allow_200_299": "yds_allow_200_299",
        "yds_allow_300_349": "yds_allow_300_349",
        "yds_allow_350_399": "yds_allow_350_399",
        "yds_allow_400_449": "yds_allow_400_449",
        "yds_allow_450_499": "yds_allow_450_499",
        "yds_allow_500_549": "yds_allow_500_549",
        "yds_allow_550p": "yds_allow_550_plus",
    }
    exact_ya = True
    for key, col_name in ya_cols.items():
        mult = _scoring_float(scoring_row, key, 0.0)
        if abs(mult) >= _EPS:
            exact_ya = False
            terms.append(_mult_term(mult, col_name))

    if exact_stats and exact_extra and exact_ret_td and exact_pa and exact_ya:
        return None

    actual_sql = _sum_sql(terms)
    return f"(({actual_sql}) - {_col('pts_def_std')})"


def _idp_correction_term(scoring_row: Mapping) -> str | None:
    idp_map = {
        "idp_tkl_solo": "pts_idp_tackle_solo",
        "idp_tkl_ast": "pts_idp_tackle_assist",
        "idp_tkl": "pts_idp_tkl_combined",
        "idp_sack": "pts_idp_sack",
        "idp_int": "pts_idp_int",
        "idp_ff": "pts_idp_ff",
        "idp_fum_rec": "pts_idp_fr",
        "idp_pass_def": "pts_idp_pd",
        "idp_qb_hit": "pts_idp_qb_hit",
        "idp_tkl_loss": "pts_idp_tfl",
        "idp_safe": "pts_idp_safety",
        "idp_def_td": "pts_idp_td",
        "idp_blk_kick": "pts_idp_blk_kick",
        "idp_blk_kick_td": "pts_idp_blk_kick_td",
        "idp_int_ret_yd": "pts_idp_int_ret_yd",
        "idp_fum_rec_yd": "pts_idp_fum_rec_yd",
        "idp_fum_ret_td": "pts_idp_fum_ret_td",
        "idp_xpr": "pts_idp_xpr",
        "idp_sack_yd": "def_sack_yards",
    }
    terms: list[str | None] = []
    for key, col_name in idp_map.items():
        terms.append(_mult_term(_scoring_float(scoring_row, key, 0.0), col_name))

    pass_def_3p = _scoring_float(scoring_row, "idp_pass_def_3p", 0.0)
    if abs(pass_def_3p) >= _EPS:
        terms.append(f"LEAST(({_fmt_num(pass_def_3p)} * {_col('pts_idp_pd')}), 3.0)")

    idp_sql = _sum_sql(terms)
    if idp_sql == "0":
        return None
    return f"(CASE WHEN UPPER(COALESCE(position, '')) IN ({_IDP_POSITIONS_SQL}) THEN ({idp_sql}) ELSE 0 END)"


def compute_fpts_with_corrections(scoring_row: dict, fpts_col_name: str) -> str:
    """Build a SQL expression for snapped fpts_* plus per-axis deltas.

    Missing settings mean "use the baked fpts_* default"; explicit zero values
    mean the league truly disables that scoring axis.
    """
    _assert_bare_scoring_row(scoring_row)
    _parse_fpts_col(fpts_col_name)
    corrections = _offense_correction_terms(scoring_row, fpts_col_name)
    for term in (
        _kicker_correction_term(scoring_row),
        _defense_correction_term(scoring_row),
        _idp_correction_term(scoring_row),
    ):
        if term:
            corrections.append(term)
    if not corrections:
        return fpts_col_name
    return f"{fpts_col_name} + " + " + ".join(corrections)


def _lamar_to_fpts_col(lamar_col_name: str) -> str:
    match = _LAMAR_RE.match(lamar_col_name)
    if not match:
        raise ValueError(f"Unsupported LAMAR column: {lamar_col_name}")
    ppr_db = {"std": "0ppr", "half": "half", "ppr": "ppr"}[match.group("ppr")]
    return f"fpts_{match.group('td')}pt_{ppr_db}"


_REPLACEMENT_ATOM_ALIASES = {
    "passing_tds": "replacement_passing_tds",
    "receptions": "replacement_receptions",
    "kickoff_return_yards": "replacement_kickoff_return_yards",
    "punt_return_yards": "replacement_punt_return_yards",
    "fg_yards_canonical": "replacement_fg_yards_canonical",
    "fg_yards_over_30_canonical": "replacement_fg_yards_over_30_canonical",
    "fg_made": "replacement_fg_made",
    "fg_made_0_19": "replacement_fg_made_0_19",
    "fg_made_20_29": "replacement_fg_made_20_29",
    "fg_made_30_39": "replacement_fg_made_30_39",
    "fg_made_40_49": "replacement_fg_made_40_49",
    "fg_made_50_59": "replacement_fg_made_50_59",
    "fg_made_60_plus_canonical": "replacement_fg_made_60_plus_canonical",
    "fg_missed": "replacement_fg_missed",
    "fg_missed_0_19": "replacement_fg_missed_0_19",
    "fg_missed_20_29": "replacement_fg_missed_20_29",
    "fg_missed_30_39": "replacement_fg_missed_30_39",
    "fg_missed_40_49": "replacement_fg_missed_40_49",
    "fg_missed_50_59": "replacement_fg_missed_50_59",
    "fg_missed_60_": "replacement_fg_missed_60_",
    "pat_made": "replacement_pat_made",
    "pat_missed": "replacement_pat_missed",
    "pts_k_std": "replacement_pts_k_std",
    "pts_def_std": "replacement_pts_def_std",
    "pts_def_sack": "replacement_pts_def_sack",
    "pts_def_int": "replacement_pts_def_int",
    "pts_def_ff": "replacement_pts_def_ff",
    "pts_def_fr": "replacement_pts_def_fr",
    "pts_def_td": "replacement_pts_def_td",
    "pts_def_safety": "replacement_pts_def_safety",
    "pts_def_block": "replacement_pts_def_block",
    "pts_def_fg_block": "replacement_pts_def_fg_block",
    "pts_def_punt_block": "replacement_pts_def_punt_block",
    "pts_def_pat_block": "replacement_pts_def_pat_block",
    "pts_def_tfl": "replacement_pts_def_tfl",
    "pts_def_3out": "replacement_pts_def_3out",
    "pts_def_4stop": "replacement_pts_def_4stop",
    "pts_def_ret_yd": "replacement_pts_def_ret_yd",
    "pts_def_ret_td": "replacement_pts_def_ret_td",
    "pts_allow_0": "replacement_pts_allow_0",
    "pts_allow_1_6": "replacement_pts_allow_1_6",
    "pts_allow_7_13": "replacement_pts_allow_7_13",
    "pts_allow_14_20": "replacement_pts_allow_14_20",
    "pts_allow_21_27": "replacement_pts_allow_21_27",
    "pts_allow_28_34": "replacement_pts_allow_28_34",
    "pts_allow_35_plus": "replacement_pts_allow_35_plus",
    "yds_allow_0_99": "replacement_yds_allow_0_99",
    "yds_allow_100_199": "replacement_yds_allow_100_199",
    "yds_allow_200_299": "replacement_yds_allow_200_299",
    "yds_allow_300_349": "replacement_yds_allow_300_349",
    "yds_allow_350_399": "replacement_yds_allow_350_399",
    "yds_allow_400_449": "replacement_yds_allow_400_449",
    "yds_allow_450_499": "replacement_yds_allow_450_499",
    "yds_allow_500_549": "replacement_yds_allow_500_549",
    "yds_allow_550_plus": "replacement_yds_allow_550_plus",
    "pts_idp_tackle_solo": "replacement_pts_idp_tackle_solo",
    "pts_idp_tackle_assist": "replacement_pts_idp_tackle_assist",
    "pts_idp_tkl_combined": "replacement_pts_idp_tkl_combined",
    "pts_idp_sack": "replacement_pts_idp_sack",
    "pts_idp_int": "replacement_pts_idp_int",
    "pts_idp_ff": "replacement_pts_idp_ff",
    "pts_idp_fr": "replacement_pts_idp_fr",
    "pts_idp_pd": "replacement_pts_idp_pd",
    "pts_idp_qb_hit": "replacement_pts_idp_qb_hit",
    "pts_idp_tfl": "replacement_pts_idp_tfl",
    "pts_idp_safety": "replacement_pts_idp_safety",
    "pts_idp_td": "replacement_pts_idp_td",
    "pts_idp_blk_kick": "replacement_pts_idp_blk_kick",
    "pts_idp_int_ret_yd": "replacement_pts_idp_int_ret_yd",
    "pts_idp_fum_rec_yd": "replacement_pts_idp_fum_rec_yd",
    "def_sack_yards": "replacement_def_sack_yards",
}


def _replace_atom_with_replacement(correction_term: str) -> str:
    out = correction_term
    for atom, alias in sorted(_REPLACEMENT_ATOM_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        out = re.sub(rf"\b{re.escape(atom)}\b", alias, out)
    return out


def compute_lamar_with_corrections(scoring_row: dict, lamar_col_name: str) -> str:
    """Build a LAMAR SQL expression with player minus replacement correction."""
    fpts_col = _lamar_to_fpts_col(lamar_col_name)
    corrected_fpts = compute_fpts_with_corrections(scoring_row, fpts_col)
    if corrected_fpts == fpts_col:
        return lamar_col_name
    prefix = f"{fpts_col} + "
    correction_terms = corrected_fpts.removeprefix(prefix)
    replacement_terms = _replace_atom_with_replacement(correction_terms)
    return f"{lamar_col_name} + (({correction_terms}) - ({replacement_terms}))"


# ---------------------------------------------------------------------------
# Irreducible scoring keys
# ---------------------------------------------------------------------------

# Scoring keys that exist in league_settings DDL but cannot be computed
# from nflverse data. Documented gaps. Each entry must include a reason.
#
# DISCIPLINE: Do not let this become a dumping ground.
# - If nflverse has the data → it does NOT belong here. Build the column.
# - Only add if the data literally doesn't exist or is unreliable
#   (e.g., def_forced_punts because drive classification is messy).
# - If this dict grows beyond 3-5 entries, that's a signal we're cutting
#   corners and need to revisit.
IRREDUCIBLE_SCORING_KEYS: dict[str, str] = {}
