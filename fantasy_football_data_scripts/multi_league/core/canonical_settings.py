"""Canonical settings schema — complete union across Yahoo, Sleeper, ESPN.

Every setting from every platform normalized into one flat dict.
Missing settings are None. No JSON nesting. No platform-specific keys
without a prefix.

Usage:
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(raw_yahoo_settings, platform="yahoo", year=2025, league_key="461.l.90939")
    # flat["num_teams"] -> 10
    # flat["scoring_rec"] -> 0.5
    # flat["roster_FLX"] -> 1
    # flat["sleeper_taxi_slots"] -> None  (Yahoo league)
"""

from __future__ import annotations

import json

from multi_league.core.espn_playoff_settings import derive_espn_playoff_metadata
from multi_league.core.roster_slots import resolve as resolve_position

# ---------------------------------------------------------------------------
# All canonical scoring keys (union across Yahoo/Sleeper/ESPN)
# Sleeper key names are the standard.
# ---------------------------------------------------------------------------
ALL_SCORING_KEYS: list[str] = [
    # Passing
    "pass_att",
    "pass_cmp",
    "pass_inc",
    "pass_yd",
    "pass_td",
    "pass_int",
    "pass_sack",
    "pass_2pt",
    "bonus_pass_yd_300",
    "bonus_pass_yd_350",
    "bonus_pass_yd_400",
    "bonus_pass_yd_450",
    "bonus_pass_yd_500",
    "pass_cmp_40p",
    "pass_cmp_50p",
    "pass_td_40p",
    "pass_td_50p",
    # Passing extras (first downs, interception TD)
    "pass_fd",
    "pass_int_td",
    # Rushing
    "rush_att",
    "rush_yd",
    "rush_td",
    "rush_2pt",
    "bonus_rush_yd_75",
    "bonus_rush_yd_100",
    "bonus_rush_yd_150",
    "bonus_rush_yd_200",
    "bonus_rush_yd_250",
    "bonus_rush_yd_300",
    "rush_40p",
    "rush_td_40p",
    "rush_td_50p",
    # Rushing extras (first downs)
    "rush_fd",
    # Receiving
    "rec",
    "rec_yd",
    "rec_td",
    "rec_2pt",
    "rec_targets",
    "rec_fd",
    "bonus_rec_yd_100",
    "bonus_rec_yd_75",
    "bonus_rec_yd_150",
    "bonus_rec_yd_200",
    "bonus_rec_yd_250",
    "bonus_rec_yd_300",
    "rec_40p",
    "rec_td_40p",
    "rec_td_50p",
    "bonus_rec_te",
    # Receiving yardage brackets (Sleeper)
    "rec_0_4",
    "rec_5_9",
    "rec_10_19",
    "rec_20_29",
    "rec_30_39",
    # Fumbles / misc offense
    "fum",
    "fum_lost",
    "fum_rec_td",
    "st_yd",
    "st_td",
    "bonus_st_yd_100",
    "bonus_st_yd_200",
    "kr_yd",
    # ST individual player stats
    "st_ff",
    "st_fum_rec",
    "st_tkl_solo",
    "pr_yd",
    "int_ret_yd",
    "fum_ret_yd",
    # Kicker
    "fgm_0_19",
    "fgm_20_29",
    "fgm_30_39",
    "fgm_40_49",
    "fgm_50p",
    "fgm_50_59",
    "fgm_60p",
    "fgm_0_39",
    "fgm_yds",
    "fgmiss",
    "fgmiss_0_19",
    "fgmiss_20_29",
    "fgmiss_30_39",
    "fgmiss_40_49",
    "fgmiss_50p",
    "fgmiss_50_59",
    "fgmiss_60p",
    "fgmiss_0_39",
    "xpm",
    "xpmiss",
    # Kicker flat/alt scoring
    "fgm",
    "fgm_yds_over_30",
    "fg_pct",
    "fgm_50_59_alt",
    "fgm_60p_alt",
    # DST
    "sack",
    "int",
    "fum_rec",
    "def_td",
    "safe",
    "blk_kick",
    "def_fg_block",
    "fg_block",
    "def_punt_block",
    "punt_block",
    "def_pat_block",
    "pat_block",
    "ff",
    "def_st_td",
    "def_kr_td",
    "def_pr_td",
    "def_int_ret_td",
    "def_fum_ret_td",
    "def_blk_kick_td",
    "def_3_and_out",
    "def_4_and_stop",
    "def_2pt",
    "one_pt_safe",
    "tkl_loss",
    # DST misc (special teams coverage, return yards, IDP-adjacent DST)
    "def_st_fum_rec",
    "def_st_tkl_solo",
    "def_st_yd",
    "def_pass_def",
    "def_forced_punts",
    "def_kr_yd",
    "def_pr_yd",
    "fg_ret_yd",
    "blk_kick_ret_yd",
    # DST team result / score / margin (ESPN team scoring)
    "team_win",
    "team_loss",
    "team_tie",
    "team_pts",
    "team_margin",
    "team_win_margin_25p",
    "team_win_margin_20_24",
    "team_win_margin_15_19",
    "team_win_margin_10_14",
    "team_win_margin_5_9",
    "team_win_margin_1_4",
    "team_loss_margin_1_4",
    "team_loss_margin_5_9",
    "team_loss_margin_10_14",
    "team_loss_margin_15_19",
    "team_loss_margin_20_24",
    "team_loss_margin_25p",
    "qb_hit",
    "sack_yd",
    "tkl",
    "tkl_ast",
    "tkl_solo",
    "yds_allow",
    # Points allowed (DST)
    "pts_allow",
    "pts_allow_0",
    "pts_allow_1_6",
    "pts_allow_7_13",
    "pts_allow_14_20",
    "pts_allow_21_27",
    "pts_allow_28_34",
    "pts_allow_35p",
    # Points allowed extras / ESPN alt bracket
    "pts_allow_46p",
    "pts_allow_14_20_alt",
    # Yards allowed (DST)
    "yds_allow_neg",
    "yds_allow_0_100",
    "yds_allow_100_199",
    "yds_allow_200_299",
    "yds_allow_300_349",
    "yds_allow_350_399",
    "yds_allow_400_449",
    "yds_allow_450_499",
    "yds_allow_500_549",
    "yds_allow_550p",
    # IDP
    "idp_tkl_solo",
    "idp_tkl_ast",
    "idp_tkl",
    "idp_tkl_loss",
    "idp_sack",
    "idp_ff",
    "idp_fum_rec",
    "idp_fum_rec_yd",
    "idp_fum_ret_td",
    "idp_qb_hit",
    "idp_def_td",
    "idp_safe",
    "idp_pass_def",
    "idp_blk_kick",
    "idp_blk_kick_td",
    "idp_int",
    "idp_int_ret_yd",
    "idp_xpr",
    # IDP extras (sack yards, pass defense threshold)
    "idp_sack_yd",
    "idp_pass_def_3p",
    # IDP points allowed brackets
    "idp_pts_allow_0",
    "idp_pts_allow_1_6",
    "idp_pts_allow_7_13",
    "idp_pts_allow_14_20",
    # Bonus scoring (Sleeper)
    "bonus_pass_cmp_25",
    "bonus_rush_att_20",
    "bonus_rush_rec_yd_100",
    "bonus_rush_rec_yd_200",
    "bonus_rec_rb",
    "bonus_rec_wr",
    "bonus_sack_2p",
    "bonus_tkl_10p",
    "bonus_def_fum_td_50p",
    "bonus_def_int_td_50p",
    # ESPN alt scoring keys
    "rec_40p_alt",
]

# ---------------------------------------------------------------------------
# All canonical roster positions (after alias resolution)
# ---------------------------------------------------------------------------
ALL_ROSTER_POSITIONS: list[str] = [
    # Offense starters
    "QB",
    "RB",
    "WR",
    "TE",
    "K",
    "DEF",
    # Flex slots
    "FLX",
    "SUPER_FLEX",
    "REC_FLEX",
    "W/R",
    "R/T",
    # IDP
    "DL",
    "LB",
    "DB",
    "IDP",
    "DL_LB",
    "DB_LB",
    # Bench/reserve
    "BN",
    "IR",
    "TAXI",
]


def _value_present(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    try:
        return value == value
    except Exception:
        return True


def _to_int_or_none(value) -> int | None:
    """Coerce platform numeric settings while treating blanks as missing."""
    if not _value_present(value):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float_or_none(value) -> float | None:
    if not _value_present(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_roster(raw_positions) -> dict[str, int | None]:
    """Normalize roster positions to canonical, return flat dict with all positions."""
    counts: dict[str, int] = {}

    if raw_positions is None:
        pass
    elif isinstance(raw_positions, list):
        for item in raw_positions:
            if isinstance(item, dict):
                pos = item.get("position", "")
                cnt = _to_int_or_none(item.get("count", 1)) or 1
                if pos:
                    resolved = resolve_position(pos)
                    counts[resolved] = counts.get(resolved, 0) + cnt
            elif isinstance(item, str):
                resolved = resolve_position(item)
                counts[resolved] = counts.get(resolved, 0) + 1
    elif isinstance(raw_positions, dict):
        for pos, cnt in raw_positions.items():
            cnt_int = _to_int_or_none(cnt)
            if cnt_int and cnt_int > 0:
                resolved = resolve_position(pos)
                counts[resolved] = counts.get(resolved, 0) + cnt_int

    # Build full dict with None for missing positions
    result = {}
    for pos in ALL_ROSTER_POSITIONS:
        result[f"roster_{pos}"] = counts.get(pos) if counts.get(pos) else None
    return result


# Aliases that merge into canonical keys before the main loop.
# These keys are NOT in ALL_SCORING_KEYS — they're normalized away.
SCORING_ALIASES: dict[str, str] = {
    "def_st_ff": "ff",
    "idp_fum_ret_yd": "idp_fum_rec_yd",
}


def _default_standard_reception(scoring_dict: dict | None) -> dict:
    """Preserve explicit 0-PPR when platform payloads omit reception scoring."""
    scoring = dict(scoring_dict or {})
    if scoring and "rec" not in scoring:
        scoring["rec"] = 0.0
    return scoring


def _is_custom_scoring_key(key: str) -> bool:
    """Return True for scoring keys supported outside the flat DDL column set."""
    from multi_league.core.scoring_config import yahoo_stat_modifier_bonus_source_threshold

    return yahoo_stat_modifier_bonus_source_threshold(key) is not None


def _parse_custom_scoring_rules(raw) -> dict[str, float]:
    if not _value_present(raw):
        return {}
    if isinstance(raw, dict):
        payload = raw
    elif isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
    else:
        return {}

    custom_rules: dict[str, float] = {}
    for key, value in payload.items():
        if not isinstance(key, str) or not _is_custom_scoring_key(key):
            continue
        val = _to_float_or_none(value)
        if val is None or val == 0:
            continue
        custom_rules[key] = val
    return custom_rules


def _normalize_scoring(
    scoring_dict: dict | None,
    league: str | None = None,
    year: int | None = None,
) -> dict[str, float | str | None]:
    """Flatten scoring dict to prefixed keys with None for missing.

    Warns via silent_drop_logger when input keys aren't in ALL_SCORING_KEYS
    or the custom-rule parser (these would be silently dropped otherwise).
    """
    scoring_dict = dict(scoring_dict or {})

    # Pre-loop: fold aliases into their canonical keys
    if scoring_dict:
        for alias, canonical in SCORING_ALIASES.items():
            if alias in scoring_dict and canonical not in scoring_dict:
                scoring_dict[canonical] = scoring_dict[alias]

        # Warn on keys not in ALL_SCORING_KEYS
        if league is not None and year is not None:
            from multi_league.transformations.player.modules import silent_drop_logger as sdl

            known = set(ALL_SCORING_KEYS) | set(SCORING_ALIASES.keys())
            for key, val in scoring_dict.items():
                if key not in known and not _is_custom_scoring_key(key) and val not in (None, 0):
                    sdl.warn("canonicalization", key, {"league": league, "year": year})

    result = {}
    for key in ALL_SCORING_KEYS:
        val = scoring_dict.get(key)
        result[f"scoring_{key}"] = _to_float_or_none(val)
    custom_rules = {
        key: val
        for key, value in scoring_dict.items()
        if key not in ALL_SCORING_KEYS
        and (val := _to_float_or_none(value)) not in (None, 0)
        and _is_custom_scoring_key(key)
    }
    result["scoring_custom_rules"] = json.dumps(custom_rules, sort_keys=True) if custom_rules else None
    return result


def extract_scoring_settings_from_flat_row(row: dict) -> dict[str, float]:
    """Convert a flat league_settings DDL row to bare canonical scoring keys.

    The table contract stores scoring fields as `scoring_*` columns. Runtime
    scoring code consumes the canonical nested shape (`rec`, `pass_td`, etc.).
    This helper is the boundary between those two shapes.
    """
    scoring: dict[str, float] = {}
    known = set(ALL_SCORING_KEYS)
    for key, value in dict(row).items():
        if not isinstance(key, str) or not key.startswith("scoring_"):
            continue
        bare_key = key.removeprefix("scoring_")
        if bare_key == "custom_rules":
            continue
        if bare_key not in known and not _is_custom_scoring_key(bare_key):
            continue
        if not _value_present(value):
            continue
        scoring[bare_key] = float(value)
    scoring.update(_parse_custom_scoring_rules(dict(row).get("scoring_custom_rules")))
    # Carry the scoring-semantics flag (not a per-stat modifier) so the recompute
    # can floor whole-point-bucket leagues. Absent/NULL -> decimal (True).
    fractional = dict(row).get("uses_fractional_points")
    scoring["uses_fractional_points"] = True if fractional is None else _to_bool(fractional)
    return scoring


def _to_bool(val) -> bool:
    """Safely convert any value to bool. Handles string '0'/'1', int 0/1, bool, None."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes")
    return bool(val)


def _detect_scoring_type(scoring: dict | None) -> str:
    """Detect scoring type from rec value."""
    if not scoring:
        return "standard"
    rec = _to_float_or_none(scoring.get("rec")) or 0
    if rec >= 1.0:
        return "ppr"
    elif rec >= 0.4:
        return "half_ppr"
    return "standard"


# ---------------------------------------------------------------------------
# Platform-specific extractors
# ---------------------------------------------------------------------------

YAHOO_DRAFT_TYPE_MAP: dict[str, str] = {
    "auction": "auction",
    "self": "snake",
    "offline": "snake",
    "offline_snake": "snake",
    "snake": "snake",
    "linear": "snake",
    # "live" is ambiguous — keep as "unknown", resolved later via cost analysis
}


def _get_any(mapping: dict | None, *keys):
    """Return the first present value across camelCase/snake_case payloads."""
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def _decimal_value(value) -> float | None:
    if isinstance(value, dict):
        return _to_float_or_none(_get_any(value, "value"))
    return _to_float_or_none(value)


def _fleaflicker_rule_points(rule: dict) -> float | None:
    points_per = _decimal_value(_get_any(rule, "pointsPer", "points_per"))
    if points_per is not None:
        return points_per

    points = _decimal_value(_get_any(rule, "points"))
    if points is None:
        return None

    for_every = _to_float_or_none(_get_any(rule, "forEvery", "for_every"))
    if for_every and for_every > 1:
        return points / for_every
    return points


FLEAFLICKER_SCORING_MAP: dict[str, str] = {
    "passing attempt": "pass_att",
    "passing completion": "pass_cmp",
    "passing incompletion": "pass_inc",
    "passing yard": "pass_yd",
    "passing yards": "pass_yd",
    "passing td": "pass_td",
    "passing tds": "pass_td",
    "passing touchdown": "pass_td",
    "passing touchdowns": "pass_td",
    "interception": "pass_int",
    "interceptions": "pass_int",
    "rushing attempt": "rush_att",
    "rushing yard": "rush_yd",
    "rushing yards": "rush_yd",
    "rushing td": "rush_td",
    "rushing tds": "rush_td",
    "rushing touchdown": "rush_td",
    "rushing touchdowns": "rush_td",
    "reception": "rec",
    "receptions": "rec",
    "receiving yard": "rec_yd",
    "receiving yards": "rec_yd",
    "receiving td": "rec_td",
    "receiving tds": "rec_td",
    "receiving touchdown": "rec_td",
    "receiving touchdowns": "rec_td",
    "target": "rec_targets",
    "targets": "rec_targets",
    "fumble": "fum",
    "fumbles": "fum",
    "fumble lost": "fum_lost",
    "fumbles lost": "fum_lost",
    "field goal made": "fgm",
    "field goals made": "fgm",
    "field goal missed": "fgmiss",
    "field goals missed": "fgmiss",
    "extra point made": "xpm",
    "extra points made": "xpm",
    "extra point missed": "xpmiss",
    "extra points missed": "xpmiss",
    "sack": "sack",
    "sacks": "sack",
    "team defense interception": "int",
    "team defense interceptions": "int",
    "safety": "safe",
    "safeties": "safe",
    "defensive touchdown": "def_td",
    "defensive touchdowns": "def_td",
}


def _fleaflicker_scoring(raw: dict) -> dict[str, float]:
    rules = raw.get("groups") or raw.get("scoringRules") or raw.get("scoring_rules") or []
    scoring: dict[str, float] = {}
    # Reception rules are position-scoped via applyTo (TE-premium leagues carry two rec
    # rules with different values). Last-wins on a flat key was order-dependent and hid
    # TE premium entirely; collect per-position and split base vs TE bonus below.
    rec_by_pos: dict[str, float] = {}

    for group in rules if isinstance(rules, list) else []:
        for rule in _get_any(group, "scoringRules", "scoring_rules") or []:
            if not isinstance(rule, dict):
                continue
            if _to_bool(_get_any(rule, "isBonus", "is_bonus")):
                continue
            category = _get_any(rule, "category") or {}
            names = [
                _get_any(category, "nameSingular", "name_singular"),
                _get_any(category, "namePlural", "name_plural"),
                _get_any(category, "abbreviation"),
            ]
            key = None
            for name in names:
                normalized = str(name or "").strip().lower()
                if normalized in FLEAFLICKER_SCORING_MAP:
                    key = FLEAFLICKER_SCORING_MAP[normalized]
                    break
            if not key:
                continue
            points = _fleaflicker_rule_points(rule)
            if points is None:
                continue
            if key == "rec":
                apply_to = _get_any(rule, "applyTo", "apply_to") or []
                positions = [str(p).upper() for p in apply_to if p] or ["RB", "WR", "TE"]
                for pos in positions:
                    rec_by_pos[pos] = points
                continue
            scoring[key] = points

    if rec_by_pos:
        base_rec = next(
            (rec_by_pos[pos] for pos in ("WR", "RB", "QB") if pos in rec_by_pos),
            next(iter(rec_by_pos.values())),
        )
        scoring["rec"] = base_rec
        te_rec = rec_by_pos.get("TE")
        if te_rec is not None and te_rec != base_rec:
            scoring["bonus_rec_te"] = te_rec - base_rec

    return _default_standard_reception(scoring)


def _fleaflicker_roster_counts(raw: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    positions = raw.get("rosterPositions") or raw.get("roster_positions") or []
    for item in positions if isinstance(positions, list) else []:
        if not isinstance(item, dict):
            continue
        label = _get_any(item, "label", "position")
        if not label:
            continue
        group = str(_get_any(item, "group") or "").upper()
        count = _to_int_or_none(_get_any(item, "start"))
        if count is None or count <= 0:
            count = _to_int_or_none(_get_any(item, "max"))
        if group == "START" and (count is None or count <= 0):
            count = 1
        if count is None or count <= 0:
            continue
        resolved = resolve_position(str(label))
        counts[resolved] = counts.get(resolved, 0) + count
    return counts


def _first_scoreboard(raw: dict) -> dict:
    scoreboards = raw.get("scoreboards")
    if isinstance(scoreboards, list) and scoreboards:
        return scoreboards[0] if isinstance(scoreboards[0], dict) else {}
    scoreboard = raw.get("scoreboard")
    return scoreboard if isinstance(scoreboard, dict) else {}


def _fleaflicker_scoreboards(raw: dict) -> list[dict]:
    scoreboards = raw.get("scoreboards")
    if isinstance(scoreboards, list):
        return [scoreboard for scoreboard in scoreboards if isinstance(scoreboard, dict)]
    first = _first_scoreboard(raw)
    return [first] if first else []


def _fleaflicker_scoreboard_week(scoreboard: dict) -> int | None:
    period = scoreboard.get("schedulePeriod") or scoreboard.get("schedule_period") or {}
    week = _to_int_or_none(_get_any(period, "value", "ordinal"))
    if week is not None:
        return week
    for game in scoreboard.get("games") or []:
        if not isinstance(game, dict):
            continue
        game_period = game.get("period") or {}
        week = _to_int_or_none(_get_any(game_period, "value", "ordinal"))
        if week is not None:
            return week
    return None


def _fleaflicker_scoreboard_weeks(raw: dict) -> list[int]:
    weeks: set[int] = set()
    for scoreboard in _fleaflicker_scoreboards(raw):
        week = _fleaflicker_scoreboard_week(scoreboard)
        if week is not None:
            weeks.add(week)
        eligible_periods = (
            scoreboard.get("eligibleSchedulePeriods") or scoreboard.get("eligible_schedule_periods") or []
        )
        if not isinstance(eligible_periods, list):
            continue
        for period in eligible_periods:
            if not isinstance(period, dict):
                continue
            period_week = _to_int_or_none(_get_any(period, "value", "ordinal"))
            if period_week is not None:
                weeks.add(period_week)
    return sorted(weeks)


def _fleaflicker_playoff_metadata(raw: dict) -> dict:
    playoff_weeks: list[int] = []
    playoff_teams: set[str] = set()
    consolation_teams: set[str] = set()

    for scoreboard in _fleaflicker_scoreboards(raw):
        week = _fleaflicker_scoreboard_week(scoreboard)
        for game in scoreboard.get("games") or []:
            if not isinstance(game, dict):
                continue
            is_playoff = _to_bool(_get_any(game, "isPlayoffs", "is_playoffs"))
            is_consolation = _to_bool(_get_any(game, "isConsolation", "is_consolation"))
            if is_playoff and week is not None:
                playoff_weeks.append(week)
            for side in ("home", "away"):
                team_id = _get_any(game.get(side) if isinstance(game.get(side), dict) else {}, "id")
                if team_id is None:
                    continue
                if is_playoff:
                    playoff_teams.add(str(team_id))
                if is_consolation:
                    consolation_teams.add(str(team_id))

    playoff_start = min(playoff_weeks) if playoff_weeks else None
    playoff_team_count = len(playoff_teams) or None
    bye_teams = None
    bracket_source = "fleaflicker_scoreboard" if playoff_start is not None else None
    if playoff_start is None:
        schedule_weeks = _fleaflicker_scoreboard_weeks(raw)
        if schedule_weeks:
            playoff_start = max(schedule_weeks) + 1
            playoff_team_count = 0
            bye_teams = 0
            bracket_source = "fleaflicker_no_playoff_flags"
    if playoff_team_count and playoff_team_count > 1:
        next_power_of_2 = 1 << (playoff_team_count - 1).bit_length()
        bye_teams = max(0, next_power_of_2 - playoff_team_count)

    return {
        "playoff_teams": playoff_team_count,
        "bye_teams": bye_teams,
        "playoff_start_week": playoff_start,
        "regular_season_weeks": playoff_start - 1 if playoff_start is not None else None,
        "playoff_bracket_source": bracket_source,
        "has_consolation_bracket": bool(consolation_teams) if playoff_start is not None else None,
        "num_playoff_consolation_teams": len(consolation_teams) or None,
    }


def _extract_fleaflicker(raw: dict, year: int, league_key: str) -> dict:
    """Extract canonical fields from Fleaflicker raw rules/league payload."""
    rules = raw.get("rules") if isinstance(raw.get("rules"), dict) else raw
    standings = raw.get("standings") if isinstance(raw.get("standings"), dict) else {}
    league = raw.get("league") or standings.get("league") or {}
    scoreboard = _first_scoreboard(raw)
    eligible_periods = scoreboard.get("eligibleSchedulePeriods") or scoreboard.get("eligible_schedule_periods") or []
    end_week = None
    if isinstance(eligible_periods, list):
        week_values = [
            _to_int_or_none(_get_any(period, "value", "ordinal"))
            for period in eligible_periods
            if isinstance(period, dict)
        ]
        week_values = [week for week in week_values if week is not None]
        if week_values:
            end_week = max(week_values)

    league_size = _to_int_or_none(_get_any(league, "size"))
    if league_size is None and isinstance(standings.get("divisions"), list):
        league_size = sum(len(div.get("teams") or []) for div in standings["divisions"] if isinstance(div, dict))

    has_playoffs = _get_any(league, "hasPlayoffs", "has_playoffs")
    max_keepers = _to_int_or_none(_get_any(league, "maxKeepers", "max_keepers"))
    playoff_meta = _fleaflicker_playoff_metadata(raw)
    has_playoffs_bool = (
        _to_bool(has_playoffs) if _value_present(has_playoffs) else bool(_to_int_or_none(playoff_meta["playoff_teams"]))
    )

    return {
        "year": year,
        "platform": "fleaflicker",
        "league_key": league_key,
        "num_teams": league_size,
        "scoring_type": _detect_scoring_type(_fleaflicker_scoring(rules)),
        "uses_median": False,
        "draft_type": "snake",
        "playoff_teams": playoff_meta["playoff_teams"],
        "bye_teams": playoff_meta["bye_teams"],
        "playoff_start_week": playoff_meta["playoff_start_week"],
        "regular_season_weeks": playoff_meta["regular_season_weeks"],
        "start_week": 1,
        "end_week": end_week,
        "has_multiweek_championship": None,
        "uses_playoff_reseeding": None,
        "playoff_bracket_source": playoff_meta["playoff_bracket_source"] if has_playoffs_bool else None,
        "playoff_seeding_rule": None,
        "playoff_seeding_rule_by": None,
        "waiver_type": _get_any(league, "waiverType", "waiver_type"),
        "waiver_budget": _get_any(league, "defaultWaiverBudget", "default_waiver_budget"),
        "trade_deadline": None,
        "veto_votes_needed": None,
        "league_type": _get_any(league, "type"),
        "max_keepers": max_keepers,
        "sleeper_best_ball": None,
        "sleeper_bench_lock": None,
        "sleeper_taxi_slots": None,
        "sleeper_taxi_years": None,
        "sleeper_taxi_allow_vets": None,
        "sleeper_pick_trading": None,
        "sleeper_daily_waivers": None,
        "sleeper_trade_review_days": None,
        "sleeper_playoff_type": None,
        "draft_rounds": None,
        "reversal_round": None,
        "has_consolation_bracket": playoff_meta["has_consolation_bracket"],
        "num_playoff_consolation_teams": playoff_meta["num_playoff_consolation_teams"],
        **_normalize_scoring(_fleaflicker_scoring(rules), league=league_key, year=year),
        **_normalize_roster(_fleaflicker_roster_counts(rules)),
        "_raw": raw,
    }


_MFL_EVENT_TO_SCORING: dict[str, str] = {
    "PY": "pass_yd",
    "#P": "pass_td",
    "IN": "pass_int",
    "P2": "pass_2pt",
    "RY": "rush_yd",
    "#R": "rush_td",
    "R2": "rush_2pt",
    "CY": "rec_yd",
    "CC": "rec",
    "#C": "rec_td",
    "C2": "rec_2pt",
    "FU": "fum_lost",
    "XP": "xpm",
}


def _mfl_rule_points(points_text: str | None) -> float | None:
    """Parse MFL point syntax: "*6" = 6/unit, "3/.5" = 3 points per 0.5 units."""
    text = str(points_text or "").strip()
    if not text:
        return None
    try:
        if text.startswith("*"):
            return float(text[1:])
        if "/" in text:
            pts, _, per = text.partition("/")
            per_val = float(per)
            return float(pts) / per_val if per_val else None
        return float(text)
    except (TypeError, ValueError):
        return None


def _mfl_text(value) -> str | None:
    if isinstance(value, dict):
        value = value.get("$t")
    return str(value).strip() if value is not None else None


def _mfl_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _mfl_scoring(raw: dict) -> dict[str, float]:
    """Flatten MFL positionRules to canonical scoring keys.

    Reception rules (CC) are position-scoped like Fleaflicker's: collect per
    position, base = WR/RB value, TE surplus becomes bonus_rec_te.
    """
    payload = raw.get("rules") if isinstance(raw.get("rules"), dict) else {}
    # the export wraps the rule set one level down: {"rules": {"positionRules": ...}}
    rules = payload.get("rules") if isinstance(payload.get("rules"), dict) else payload
    scoring: dict[str, float] = {}
    rec_by_pos: dict[str, float] = {}
    for group in _mfl_list(rules.get("positionRules")):
        if not isinstance(group, dict):
            continue
        positions = [p.strip().upper() for p in str(group.get("positions") or "").split("|") if p.strip()]
        for rule in _mfl_list(group.get("rule")):
            if not isinstance(rule, dict):
                continue
            event = _mfl_text(rule.get("event"))
            key = _MFL_EVENT_TO_SCORING.get(str(event or "").upper())
            if not key:
                continue
            points = _mfl_rule_points(_mfl_text(rule.get("points")))
            if points is None:
                continue
            if key == "rec":
                for pos in positions or ["RB", "WR", "TE"]:
                    rec_by_pos[pos] = points
                continue
            scoring.setdefault(key, points)
    if rec_by_pos:
        base_rec = next(
            (rec_by_pos[pos] for pos in ("WR", "RB", "QB") if pos in rec_by_pos),
            next(iter(rec_by_pos.values())),
        )
        scoring["rec"] = base_rec
        te_rec = rec_by_pos.get("TE")
        if te_rec is not None and te_rec != base_rec:
            scoring["bonus_rec_te"] = te_rec - base_rec
    return _default_standard_reception(scoring)


def _mfl_starter_limits(league: dict) -> list[tuple[str, int, int]]:
    """(position, min, max) from starters.position rows; limit is "n" or "a-b"."""
    starters = league.get("starters") or {}
    out: list[tuple[str, int, int]] = []
    for row in _mfl_list(starters.get("position")):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        limit = str(row.get("limit") or "").strip()
        if not name or not limit:
            continue
        try:
            if "-" in limit:
                lo_text, _, hi_text = limit.partition("-")
                lo, hi = int(lo_text), int(hi_text)
            else:
                lo = hi = int(limit)
        except ValueError:
            continue
        out.append((name, lo, hi))
    return out


_MFL_POSITION_TO_ROSTER: dict[str, str] = {
    "QB": "QB", "RB": "RB", "WR": "WR", "TE": "TE",
    "PK": "K", "K": "K",
    "DEF": "DEF", "DF": "DEF", "TMDF": "DEF", "D/ST": "DEF",
    "DT": "DL", "DE": "DL", "DL": "DL",
    "LB": "LB",
    "CB": "DB", "S": "DB", "CB+S": "DB", "DB": "DB",
}


def _mfl_roster_counts(league: dict) -> dict[str, int]:
    """Slot counts from MFL's range-style starter limits.

    min = dedicated slots (a "0-1" K still gets 1: the Class A gate asks whether
    the position CAN start, not whether it must). QB surplus (max > min) is a
    superflex slot; remaining offensive surplus becomes FLX.
    """
    limits = _mfl_starter_limits(league)
    counts: dict[str, int] = {}
    qb_surplus = 0
    offense_surplus = 0
    for name, lo, hi in limits:
        resolved = _MFL_POSITION_TO_ROSTER.get(name.upper())
        if resolved is None:
            continue
        slots = lo if lo > 0 else (1 if hi > 0 else 0)
        if slots > 0:
            counts[resolved] = counts.get(resolved, 0) + slots
        if resolved == "QB" and hi > lo:
            qb_surplus += hi - lo
        elif resolved in {"RB", "WR", "TE"} and hi > lo:
            offense_surplus += hi - lo
    if qb_surplus:
        counts["SUPER_FLEX"] = qb_surplus
    starters = league.get("starters") or {}
    total = _to_int_or_none(starters.get("count"))
    idp = _to_int_or_none(starters.get("idp_starters")) or 0
    dedicated = sum(counts.get(pos, 0) for pos in ("QB", "RB", "WR", "TE", "K", "DEF", "SUPER_FLEX"))
    idp_dedicated = sum(counts.get(pos, 0) for pos in ("DL", "LB", "DB"))
    if total is not None:
        flex = total - idp - dedicated
        if idp and idp > idp_dedicated:
            counts["IDP"] = idp - idp_dedicated
        if flex > 0 and offense_surplus > 0:
            counts["FLX"] = min(flex, offense_surplus)
        elif flex > 0:
            counts["FLX"] = flex
    roster_size = _to_int_or_none(league.get("rosterSize"))
    if roster_size is not None and total is not None and roster_size > total:
        counts["BN"] = roster_size - total
    taxi = _to_int_or_none(league.get("taxiSquad"))
    if taxi:
        counts["TAXI"] = taxi
    injured = _to_int_or_none(league.get("injuredReserve"))
    if injured:
        counts["IR"] = injured
    return counts


def _mfl_playoff_meta(league: dict, brackets_raw: dict) -> dict:
    """Playoff shape from lastRegularSeasonWeek + the championship bracket.

    lastRegularSeasonWeek can be degenerate (observed: 1 on a total-points
    league) -- fall back to the championship bracket's startWeek.
    """
    last_regular = _to_int_or_none(league.get("lastRegularSeasonWeek"))
    brackets = _mfl_list(((brackets_raw or {}).get("playoffBrackets") or {}).get("playoffBracket"))
    champ_bracket = None
    for bracket in brackets:
        if not isinstance(bracket, dict):
            continue
        title = str(bracket.get("bracketWinnerTitle") or bracket.get("name") or "").lower()
        if "champ" in title and "consolation" not in title:
            if champ_bracket is None or (
                _to_int_or_none(bracket.get("startWeek")) or 99
            ) < (_to_int_or_none(champ_bracket.get("startWeek")) or 99):
                champ_bracket = bracket
    bracket_start = _to_int_or_none(champ_bracket.get("startWeek")) if champ_bracket else None
    playoff_teams = _to_int_or_none(champ_bracket.get("teamsInvolved")) if champ_bracket else None
    if last_regular is not None and last_regular >= 4:
        playoff_start = last_regular + 1
    elif bracket_start is not None:
        playoff_start = bracket_start
    else:
        playoff_start = None
    return {
        "playoff_start_week": playoff_start,
        "regular_season_weeks": playoff_start - 1 if playoff_start is not None else None,
        "playoff_teams": playoff_teams,
        "playoff_bracket_source": "mfl_playoff_brackets" if champ_bracket else None,
    }


def _extract_mfl(raw: dict, year: int, league_key: str) -> dict:
    """Extract canonical fields from MFL league/rules/brackets payloads."""
    league = raw.get("league") if isinstance(raw.get("league"), dict) else {}
    brackets = raw.get("brackets") if isinstance(raw.get("brackets"), dict) else {}
    scoring = _mfl_scoring(raw)
    playoff_meta = _mfl_playoff_meta(league, brackets)
    num_teams = _to_int_or_none((league.get("franchises") or {}).get("count"))
    best_ball = str(league.get("bestLineup") or "").strip().lower() == "yes"

    return {
        "year": year,
        "platform": "mfl",
        "league_key": league_key,
        "num_teams": num_teams,
        "scoring_type": _detect_scoring_type(scoring),
        "uses_median": False,
        "draft_type": "snake",
        "playoff_teams": playoff_meta["playoff_teams"],
        "bye_teams": None,
        "playoff_start_week": playoff_meta["playoff_start_week"],
        "regular_season_weeks": playoff_meta["regular_season_weeks"],
        "start_week": _to_int_or_none(league.get("startWeek")) or 1,
        "end_week": _to_int_or_none(league.get("endWeek")),
        "has_multiweek_championship": None,
        "uses_playoff_reseeding": None,
        "playoff_bracket_source": playoff_meta["playoff_bracket_source"],
        "playoff_seeding_rule": None,
        "playoff_seeding_rule_by": None,
        "waiver_type": league.get("currentWaiverType"),
        "waiver_budget": _to_int_or_none(league.get("bbidSeasonLimit")),
        "trade_deadline": None,
        "veto_votes_needed": None,
        "league_type": None,
        "max_keepers": _to_int_or_none(league.get("maxKeepers")),
        "sleeper_best_ball": best_ball,
        "sleeper_bench_lock": None,
        "sleeper_taxi_slots": _to_int_or_none(league.get("taxiSquad")),
        "sleeper_taxi_years": None,
        "sleeper_taxi_allow_vets": None,
        "sleeper_pick_trading": None,
        "sleeper_daily_waivers": None,
        "sleeper_trade_review_days": None,
        "sleeper_playoff_type": None,
        "draft_rounds": None,
        "reversal_round": None,
        "has_consolation_bracket": None,
        "num_playoff_consolation_teams": None,
        **_normalize_scoring(scoring, league=league_key, year=year),
        **_normalize_roster(_mfl_roster_counts(league)),
        "_raw": raw,
    }


def _extract_yahoo(raw: dict, year: int, league_key: str) -> dict:
    """Extract canonical fields from Yahoo raw settings."""
    meta = raw.get("metadata", {})
    canonical_scoring = raw.get("canonical_scoring") or {}
    if not canonical_scoring and raw.get("scoring_rules"):
        from multi_league.core.scoring_config import normalize_yahoo

        canonical_scoring = normalize_yahoo(raw.get("scoring_rules", []))
    canonical_scoring = _default_standard_reception(canonical_scoring)
    roster_raw = raw.get("roster_position_counts", raw.get("roster_positions", []))

    # Draft type: map Yahoo raw values to canonical; "live" stays unknown for cost-analysis resolution
    draft_type_raw = str(meta.get("draft_type") or "").lower()
    draft_type = YAHOO_DRAFT_TYPE_MAP.get(draft_type_raw, "unknown")

    # Derive regular_season_weeks from playoff_start_week
    playoff_start = meta.get("playoff_start_week", raw.get("playoff_start_week", None))
    playoff_start_int = _to_int_or_none(playoff_start)
    playoff_teams = _to_int_or_none(meta.get("num_playoff_teams", meta.get("playoff_teams", None)))
    bye_teams = _to_int_or_none(meta.get("bye_teams", raw.get("bye_teams", None)))
    end_week = _to_int_or_none(meta.get("end_week", raw.get("end_week", None)))
    uses_playoff_raw = meta.get("uses_playoff")
    playoffs_disabled = _value_present(uses_playoff_raw) and not _to_bool(uses_playoff_raw)
    if playoffs_disabled:
        playoff_teams = 0
        bye_teams = 0
        if playoff_start_int is None and end_week is not None:
            playoff_start_int = end_week + 1
    regular_season_weeks = playoff_start_int - 1 if playoff_start_int is not None else None
    consolation_teams = _to_int_or_none(meta.get("num_playoff_consolation_teams"))
    if playoffs_disabled:
        consolation_teams = 0
    yahoo_waiver_type = str(meta.get("waiver_type") or "").strip().upper()
    uses_faab_raw = meta.get("uses_faab")
    uses_faab = _to_bool(uses_faab_raw) if _value_present(uses_faab_raw) else None
    if uses_faab is None and yahoo_waiver_type.startswith("F"):
        uses_faab = True
    waiver_budget = _to_int_or_none(
        meta.get("waiver_budget")
        or meta.get("faab_budget")
        or meta.get("default_waiver_budget")
        or meta.get("defaultWaiverBudget")
    )
    if waiver_budget is None and uses_faab is True:
        waiver_budget = 100
    elif waiver_budget is None and uses_faab is False:
        waiver_budget = 0
    if uses_faab is True:
        waiver_type = "faab"
    elif uses_faab is False or _value_present(yahoo_waiver_type):
        waiver_type = "normal"
    else:
        waiver_type = None

    return {
        # Core
        "year": year,
        "platform": "yahoo",
        "league_key": league_key,
        "num_teams": _to_int_or_none(meta.get("num_teams", None)),
        "scoring_type": _detect_scoring_type(canonical_scoring),
        # Yahoo scoring semantics: 0 = whole-point buckets (each stat floored,
        # the pre-~2014 era), 1 = decimal. Default True (decimal) when absent.
        "uses_fractional_points": _to_bool(meta.get("uses_fractional_points", True)),
        "uses_median": _to_bool(meta.get("uses_median_score")),
        "draft_type": draft_type,
        # Playoff
        "playoff_teams": playoff_teams,
        "bye_teams": bye_teams,
        "playoff_start_week": playoff_start_int,
        "regular_season_weeks": regular_season_weeks,
        "start_week": _to_int_or_none(meta.get("start_week", None)),
        "end_week": end_week,
        "has_multiweek_championship": _to_bool(meta.get("has_multiweek_championship")),
        "uses_playoff_reseeding": _to_bool(meta.get("uses_playoff_reseeding")),
        "playoff_bracket_source": meta.get("playoff_bracket_source"),
        "playoff_seeding_rule": meta.get("playoff_seeding_rule"),
        "playoff_seeding_rule_by": _to_int_or_none(meta.get("playoff_seeding_rule_by")),
        # Waiver/trade
        "waiver_type": waiver_type,
        "waiver_budget": waiver_budget,
        "trade_deadline": None,
        "veto_votes_needed": None,
        # Dynasty/keeper
        "league_type": meta.get("league_type", None),
        "max_keepers": None,
        # Sleeper-specific (NULL for Yahoo)
        "sleeper_best_ball": None,
        "sleeper_bench_lock": None,
        "sleeper_taxi_slots": None,
        "sleeper_taxi_years": None,
        "sleeper_taxi_allow_vets": None,
        "sleeper_pick_trading": None,
        "sleeper_daily_waivers": None,
        "sleeper_trade_review_days": None,
        "sleeper_playoff_type": None,
        "draft_rounds": None,  # Yahoo doesn't expose in settings
        "reversal_round": None,
        # Consolation bracket
        "has_consolation_bracket": consolation_teams > 0 if consolation_teams is not None else None,
        "num_playoff_consolation_teams": consolation_teams,
        # Scoring + roster (flattened below)
        **_normalize_scoring(canonical_scoring, league=league_key, year=year),
        **_normalize_roster(roster_raw),
        # Raw for debugging
        "_raw": raw,
    }


def _sleeper_consolation_columns(raw: dict) -> dict:
    """Derive has_consolation_bracket + num_playoff_consolation_teams from Sleeper raw.

    The fetcher stashes `losers_bracket_teams` in the raw dict:
      - None  → in-season, unknown (API returned empty before season complete)
      - []    → API 404 or season complete with no bracket → has=False
      - [7,8,9,...] → bracket exists → has=True, count=len(list)
    """
    losers_teams = raw.get("losers_bracket_teams")
    if losers_teams is None:
        return {"has_consolation_bracket": None, "num_playoff_consolation_teams": None}
    if len(losers_teams) == 0:
        return {"has_consolation_bracket": False, "num_playoff_consolation_teams": 0}
    return {"has_consolation_bracket": True, "num_playoff_consolation_teams": len(losers_teams)}


def _extract_sleeper(raw: dict, year: int, league_key: str) -> dict:
    """Extract canonical fields from Sleeper raw settings."""
    settings = raw.get("settings", {})
    scoring = _default_standard_reception(raw.get("scoring_settings", {}))
    roster_raw = raw.get("roster_positions", [])
    playoff_teams = raw.get("playoff_teams", settings.get("playoff_teams", None))
    bye_teams = raw.get("bye_teams", None)
    try:
        playoff_teams_int = int(playoff_teams) if playoff_teams is not None else None
    except (TypeError, ValueError):
        playoff_teams_int = None
    if bye_teams is None and playoff_teams_int is not None:
        if playoff_teams_int > 1:
            next_power_of_2 = 1 << (playoff_teams_int - 1).bit_length()
            bye_teams = next_power_of_2 - playoff_teams_int
        else:
            bye_teams = 0

    # Draft type from Sleeper's numeric type
    draft_id = settings.get("type", raw.get("type", 0))
    draft_map = {0: "snake", 1: "snake", 2: "auction", 3: "auction"}
    draft_type = draft_map.get(draft_id, "snake")

    # Waiver type
    waiver_type_id = settings.get("waiver_type", 0)
    waiver_map = {0: "normal", 1: "faab", 2: "continuous"}
    waiver_type = waiver_map.get(waiver_type_id, "normal")
    playoff_seed_type = raw.get("playoff_seed_type", settings.get("playoff_seed_type", None))

    # Derive regular_season_weeks from playoff_start_week
    sleeper_playoff_start = raw.get("playoff_start_week", settings.get("playoff_week_start", None))
    sleeper_end_week = raw.get("end_week", settings.get("leg", settings.get("last_scored_leg", None)))
    if playoff_teams_int is not None and playoff_teams_int <= 1:
        try:
            end_week_int = int(sleeper_end_week)
            start_week_int = int(sleeper_playoff_start) if sleeper_playoff_start not in (None, "") else 0
            if start_week_int <= 0 or start_week_int <= end_week_int:
                sleeper_playoff_start = end_week_int + 1
        except (TypeError, ValueError):
            pass
    try:
        sleeper_reg_weeks = int(sleeper_playoff_start) - 1 if sleeper_playoff_start is not None else None
    except (ValueError, TypeError):
        sleeper_reg_weeks = None

    return {
        # Core
        "year": year,
        "platform": "sleeper",
        "league_key": league_key,
        "num_teams": settings.get("num_teams", raw.get("total_rosters", None)),
        "scoring_type": _detect_scoring_type(scoring),
        "uses_median": _to_bool(settings.get("league_average_match")),
        "draft_type": draft_type,
        # Playoff
        "playoff_teams": playoff_teams,
        "bye_teams": bye_teams,
        "playoff_start_week": sleeper_playoff_start,
        "regular_season_weeks": sleeper_reg_weeks,
        "start_week": settings.get("start_week", 1),
        "end_week": sleeper_end_week,
        "has_multiweek_championship": _to_bool(
            raw.get("has_multiweek_championship", settings.get("playoff_round_type", settings.get("playoff_type")))
        ),
        "uses_playoff_reseeding": _to_bool(settings.get("playoff_seed_type")),
        "playoff_bracket_source": raw.get("playoff_bracket_source"),
        "playoff_seeding_rule": str(playoff_seed_type) if playoff_seed_type is not None else None,
        "playoff_seeding_rule_by": _to_int_or_none(playoff_seed_type),
        # Waiver/trade
        "waiver_type": waiver_type,
        "waiver_budget": settings.get("waiver_budget", None),
        "trade_deadline": settings.get("trade_deadline", None),
        "veto_votes_needed": settings.get("veto_votes_needed", None),
        # Dynasty/keeper
        "league_type": raw.get("status", None),
        "max_keepers": settings.get("max_keepers", None),
        # Sleeper-specific
        "sleeper_best_ball": _to_bool(settings.get("best_ball")) if "best_ball" in settings else None,
        "sleeper_bench_lock": _to_bool(settings.get("bench_lock")) if "bench_lock" in settings else None,
        "sleeper_taxi_slots": settings.get("taxi_slots", None),
        "sleeper_taxi_years": settings.get("taxi_years", None),
        "sleeper_taxi_allow_vets": _to_bool(settings.get("taxi_allow_vets")) if "taxi_allow_vets" in settings else None,
        "sleeper_pick_trading": _to_bool(settings.get("pick_trading")) if "pick_trading" in settings else None,
        "sleeper_daily_waivers": _to_bool(settings.get("daily_waivers")) if "daily_waivers" in settings else None,
        "sleeper_trade_review_days": settings.get("trade_review_days", None),
        "sleeper_playoff_type": settings.get("playoff_round_type", settings.get("playoff_type", None)),
        "draft_rounds": settings.get("draft_rounds", None),
        "reversal_round": settings.get("reversal_round", None),
        # Consolation bracket (from /losers_bracket endpoint, wired in fetcher)
        **_sleeper_consolation_columns(raw),
        # Scoring + roster
        **_normalize_scoring(scoring, league=league_key, year=year),  # Sleeper keys ARE canonical
        **_normalize_roster(roster_raw),
        # Raw for debugging
        "_raw": raw,
    }


def _espn_consolation_columns(schedule: dict, num_teams, playoff_teams) -> dict:
    """Derive has_consolation_bracket + num_playoff_consolation_teams from ESPN raw.

    ESPN exposes `consolationLadderDisabled` in scheduleSettings.
    """
    disabled = schedule.get("consolationLadderDisabled")
    if disabled is not None:
        if disabled:
            return {"has_consolation_bracket": False, "num_playoff_consolation_teams": 0}
        nt = int(num_teams or 0)
        pt = int(playoff_teams or 0)
        return {
            "has_consolation_bracket": True,
            "num_playoff_consolation_teams": max(0, nt - pt),
        }
    return {"has_consolation_bracket": None, "num_playoff_consolation_teams": None}


def _extract_espn(raw: dict, year: int, league_key: str) -> dict:
    """Extract canonical fields from ESPN raw settings.

    Handles two formats:
    - Raw ESPN API: nested under raw["settings"]["scheduleSettings"], etc.
    - Pre-processed by espn_league_settings.py: flat keys at top level
      (num_teams, playoff_teams, playoff_start_week, roster_position_counts, etc.)
    """
    # Detect pre-processed format: has top-level keys like num_teams, scoring_settings
    is_preprocessed = "scoring_settings" in raw or "roster_position_counts" in raw

    settings = raw.get("settings", {})
    schedule = settings.get("scheduleSettings", {})
    draft_settings = settings.get("draftSettings", {})
    roster_settings = settings.get("rosterSettings", {})
    scoring_settings = settings.get("scoringSettings", {})
    scoring_raw = settings.get("scoringSettings", {}).get("scoringItems", [])

    # --- Scoring ---
    scoring_dict: dict[str, float] = {}
    if scoring_raw and isinstance(scoring_raw, list):
        from multi_league.core.scoring_config import canonical_keys_for_espn_stat_id

        for item in scoring_raw:
            stat_id = item.get("statId")
            overrides = item.get("pointsOverrides", {})
            if overrides:
                pts = list(overrides.values())[0]
            else:
                pts = item.get("points", 0)
            if stat_id is not None and pts != 0:
                try:
                    numeric_stat_id = int(stat_id)
                except (TypeError, ValueError):
                    continue
                for canon_key in canonical_keys_for_espn_stat_id(numeric_stat_id):
                    scoring_dict[canon_key] = float(pts)
        scoring = scoring_dict
    elif scoring_raw and isinstance(scoring_raw, dict):
        try:
            from multi_league.core.scoring_config import normalize_espn as _espn_normalizer

            scoring = _espn_normalizer(scoring_raw)
        except Exception:
            scoring = raw.get("canonical_scoring", {})
    else:
        scoring = raw.get("canonical_scoring", {})

    # Pre-processed scoring_settings is already canonical
    if not scoring and is_preprocessed:
        ss = raw.get("scoring_settings", {})
        if isinstance(ss, dict):
            scoring = ss
    scoring = _default_standard_reception(scoring)

    # --- Roster ---
    ESPN_SLOT_MAP = {
        0: "QB",
        2: "RB",
        3: "RB/WR",
        4: "WR",
        5: "WR/TE",
        6: "TE",
        7: "OP",
        8: "DT",
        9: "DE",
        10: "LB",
        11: "DL",
        12: "CB",
        13: "S",
        14: "DB",
        15: "DP",
        16: "DEF",
        17: "K",
        20: "BN",
        21: "IR",
        22: "BN",
        23: "RB/WR/TE",
    }
    lineup_slots = roster_settings.get("lineupSlotCounts", {})
    roster_counts: dict[str, int] = {}
    for slot_id_str, count in lineup_slots.items():
        if count > 0:
            slot_id = int(slot_id_str)
            pos = ESPN_SLOT_MAP.get(slot_id)
            if pos:
                resolved = resolve_position(pos)
                roster_counts[resolved] = roster_counts.get(resolved, 0) + count

    if not roster_counts and "roster_position_counts" in raw:
        roster_counts = raw["roster_position_counts"]

    # --- Draft type ---
    draft_type_raw = draft_settings.get("type", "").lower() if isinstance(draft_settings.get("type"), str) else ""
    if not draft_type_raw:
        draft_type_raw = str(raw.get("draft_type", "")).lower()
    draft_type = "auction" if "auction" in draft_type_raw else "snake"

    # --- Playoff / schedule ---
    playoff_teams_raw = schedule.get("playoffTeamCount", None)
    regular_season_raw = schedule.get("matchupPeriodCount", None)
    pmpl_raw = schedule.get("playoffMatchupPeriodLength", None)
    matchup_periods_raw = schedule.get("matchupPeriods", None)
    playoff_seeding_rule = schedule.get("playoffSeedingRule", None)
    playoff_seeding_rule_by = schedule.get("playoffSeedingRuleBy", None)
    playoff_bracket_source = raw.get("playoff_bracket_source", "api")
    home_team_bonus = scoring_settings.get("homeTeamBonus")
    playoff_home_team_bonus = scoring_settings.get("playoffHomeTeamBonus")
    matchup_tie_rule = scoring_settings.get("matchupTieRule")
    matchup_tie_rule_by = scoring_settings.get("matchupTieRuleBy")
    playoff_matchup_tie_rule = scoring_settings.get("playoffMatchupTieRule")
    playoff_matchup_tie_rule_by = scoring_settings.get("playoffMatchupTieRuleBy")

    # Fallback to pre-processed top-level keys. Use None checks so ESPN's
    # legitimate playoffTeamCount=0 does not get replaced by a default.
    if is_preprocessed:
        if playoff_teams_raw is None:
            playoff_teams_raw = raw.get("playoff_teams")
            if playoff_teams_raw is None:
                playoff_teams_raw = raw.get("num_playoff_teams")
        if regular_season_raw is None:
            regular_season_raw = raw.get("regular_season_length", raw.get("regular_season_weeks"))
        if pmpl_raw is None:
            pmpl_raw = raw.get("playoff_matchup_period_length")
        if matchup_periods_raw is None:
            matchup_periods_raw = raw.get("matchup_periods")
        if playoff_seeding_rule is None:
            playoff_seeding_rule = raw.get("playoff_seeding_rule")
        if playoff_seeding_rule_by is None:
            playoff_seeding_rule_by = raw.get("playoff_seeding_rule_by")
        playoff_bracket_source = raw.get("playoff_bracket_source", playoff_bracket_source)
        if home_team_bonus is None:
            home_team_bonus = raw.get("home_team_bonus")
        if playoff_home_team_bonus is None:
            playoff_home_team_bonus = raw.get("playoff_home_team_bonus")
        if matchup_tie_rule is None:
            matchup_tie_rule = raw.get("matchup_tie_rule")
        if matchup_tie_rule_by is None:
            matchup_tie_rule_by = raw.get("matchup_tie_rule_by")
        if playoff_matchup_tie_rule is None:
            playoff_matchup_tie_rule = raw.get("playoff_matchup_tie_rule")
        if playoff_matchup_tie_rule_by is None:
            playoff_matchup_tie_rule_by = raw.get("playoff_matchup_tie_rule_by")

    playoff_meta = derive_espn_playoff_metadata(
        playoff_teams=playoff_teams_raw,
        regular_season_length=regular_season_raw,
        playoff_matchup_period_length=pmpl_raw,
        matchup_periods=matchup_periods_raw,
    )
    playoff_teams = playoff_meta["playoff_teams"]
    playoff_start = playoff_meta["playoff_start_week"]
    espn_reg_weeks = playoff_meta["regular_season_weeks"]

    # --- FAAB ---
    acq_settings = settings.get("acquisitionSettings", {})
    acq_budget = acq_settings.get("acquisitionBudget", None)
    if acq_budget is None and is_preprocessed:
        acq_budget = raw.get("acquisition_budget") or raw.get("faab")

    # --- Keeper ---
    keeper_count = settings.get("keeperCount", draft_settings.get("keeperCount", None))
    if keeper_count is None and is_preprocessed:
        keeper_count = raw.get("keeper_count")

    # --- num_teams ---
    num_teams = settings.get("size", None)
    if num_teams is None and is_preprocessed:
        num_teams = raw.get("num_teams")

    # --- Derived playoff fields ---
    bye_teams = playoff_meta["bye_teams"]
    end_week = playoff_meta["end_week"]
    has_multiweek = bool(playoff_meta["has_multiweek_championship"])

    # --- draft rounds ---
    draft_rounds = draft_settings.get("rounds", None) if draft_settings else None
    if draft_rounds is None and is_preprocessed:
        draft_rounds = raw.get("draft_rounds")

    return {
        # Core
        "year": year,
        "platform": "espn",
        "league_key": league_key,
        "num_teams": num_teams,
        "scoring_type": _detect_scoring_type(scoring),
        "uses_median": False,
        "draft_type": draft_type,
        # Playoff
        "playoff_teams": playoff_teams,
        "bye_teams": bye_teams,
        "playoff_start_week": playoff_start,
        "regular_season_weeks": espn_reg_weeks,
        "start_week": 1,
        "end_week": end_week,
        "has_multiweek_championship": has_multiweek,
        "uses_playoff_reseeding": None,
        "playoff_bracket_source": playoff_bracket_source,
        "playoff_seeding_rule": playoff_seeding_rule,
        "playoff_seeding_rule_by": _to_int_or_none(playoff_seeding_rule_by),
        "home_team_bonus": _to_float_or_none(home_team_bonus),
        "playoff_home_team_bonus": _to_float_or_none(playoff_home_team_bonus),
        "matchup_tie_rule": matchup_tie_rule,
        "matchup_tie_rule_by": matchup_tie_rule_by,
        "playoff_matchup_tie_rule": playoff_matchup_tie_rule,
        "playoff_matchup_tie_rule_by": playoff_matchup_tie_rule_by,
        # Waiver/trade
        "waiver_type": "faab" if acq_budget and acq_budget > 0 else "normal",
        "waiver_budget": acq_budget,
        "trade_deadline": settings.get("tradeSettings", {}).get("deadlineDate", None),
        "veto_votes_needed": settings.get("tradeSettings", {}).get("vetoVotesRequired", None),
        # Dynasty/keeper
        "league_type": None,
        "max_keepers": keeper_count,
        # Sleeper-specific (NULL for ESPN)
        "sleeper_best_ball": None,
        "sleeper_bench_lock": None,
        "sleeper_taxi_slots": None,
        "sleeper_taxi_years": None,
        "sleeper_taxi_allow_vets": None,
        "sleeper_pick_trading": None,
        "sleeper_daily_waivers": None,
        "sleeper_trade_review_days": None,
        "sleeper_playoff_type": None,
        "draft_rounds": draft_rounds,
        "reversal_round": None,
        # Consolation bracket (from ESPN scheduleSettings.consolationLadderDisabled)
        **_espn_consolation_columns(schedule, num_teams, playoff_teams),
        # Scoring + roster
        **_normalize_scoring(scoring, league=league_key, year=year),
        **_normalize_roster(roster_counts),
        # Raw for debugging
        "_raw": raw,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def flatten_settings(raw: dict, platform: str, year: int, league_key: str) -> dict:
    """Normalize raw platform settings to a fully flat canonical dict.

    Every possible setting across all platforms is present as a key.
    Missing settings are None. No nesting except _raw.

    Args:
        raw: Raw settings dict from the platform API
        platform: "yahoo" | "sleeper" | "espn" | "fleaflicker"
        year: NFL season year
        league_key: Platform-specific league key for this year

    Returns:
        Flat dict with ~170 keys, all canonical names.
    """
    # League-merge flows may stage already-canonical league_settings
    # rows rather than raw platform API payloads. In that case, preserve the
    # existing canonical values instead of trying to re-extract them.
    #
    # The canonical-detection has to be carefully scoped: Sleeper's RAW API
    # response also has top-level keys `scoring_settings` (nested dict) and
    # `roster_positions` (list of strings) which both happen to start with
    # "scoring_" / "roster_". A naive startswith() check caused the
    # passthrough to fire on every Sleeper raw payload, skipping extraction
    # entirely and leaving every `roster_{POSITION}` column NULL in the
    # flat DDL. Instead, look for SPECIFIC canonical scalar keys that only
    # exist in the flat canonical shape: `scoring_rec` (from the flat
    # schema, not nested under `scoring_settings`) and `roster_QB`.
    if isinstance(raw, dict):
        has_canonical_identity = "platform" in raw and "league_key" in raw
        # Exclude the raw container names that happen to match the prefix
        # check — they aren't canonical flat columns.
        _raw_containers = {"scoring_settings", "roster_positions", "roster_position_counts"}
        canonical_flat_keys = [
            k for k in raw if (k.startswith("scoring_") or k.startswith("roster_")) and k not in _raw_containers
        ]
        has_canonical_payload = bool(canonical_flat_keys)
        if has_canonical_identity and has_canonical_payload:
            flattened = {key: raw.get(key) for key in get_schema_keys()}
            flattened["year"] = int(raw.get("year") or year)
            flattened["platform"] = raw.get("platform") or platform
            flattened["league_key"] = raw.get("league_key") or league_key
            flattened["_raw"] = raw.get("_raw")
            return flattened

    if platform == "yahoo":
        return _extract_yahoo(raw, year, league_key)
    elif platform == "sleeper":
        return _extract_sleeper(raw, year, league_key)
    elif platform == "espn":
        return _extract_espn(raw, year, league_key)
    elif platform == "fleaflicker":
        return _extract_fleaflicker(raw, year, league_key)
    elif platform == "mfl":
        return _extract_mfl(raw, year, league_key)
    raise ValueError(f"Unknown platform: {platform}")


def get_schema_keys() -> list[str]:
    """Return all canonical settings keys (excluding _raw) in order.

    Useful for creating canonical tables or validating completeness.
    """
    # Build a dummy to get all keys
    dummy_scoring = {f"scoring_{k}": None for k in ALL_SCORING_KEYS}
    dummy_roster = {f"roster_{p}": None for p in ALL_ROSTER_POSITIONS}
    core_keys = [
        "db_name",
        "year",
        "platform",
        "league_key",
        "num_teams",
        "scoring_type",
        "uses_fractional_points",
        "uses_median",
        "draft_type",
        "playoff_teams",
        "bye_teams",
        "playoff_start_week",
        "regular_season_weeks",
        "start_week",
        "end_week",
        "has_multiweek_championship",
        "uses_playoff_reseeding",
        "playoff_bracket_source",
        "playoff_seeding_rule",
        "playoff_seeding_rule_by",
        "home_team_bonus",
        "playoff_home_team_bonus",
        "matchup_tie_rule",
        "matchup_tie_rule_by",
        "playoff_matchup_tie_rule",
        "playoff_matchup_tie_rule_by",
        "waiver_type",
        "waiver_budget",
        "trade_deadline",
        "veto_votes_needed",
        "league_type",
        "max_keepers",
        "sleeper_best_ball",
        "sleeper_bench_lock",
        "sleeper_taxi_slots",
        "sleeper_taxi_years",
        "sleeper_taxi_allow_vets",
        "sleeper_pick_trading",
        "sleeper_daily_waivers",
        "sleeper_trade_review_days",
        "sleeper_playoff_type",
        "draft_rounds",
        "reversal_round",
        "is_dynasty",
        "first_active_year",
        "last_active_year",
        "import_mode",
        "scoring_variant",
        "scoring_variant_first_year",
        "scoring_custom_rules",
        "has_consolation_bracket",
        "num_playoff_consolation_teams",
    ]
    return core_keys + sorted(dummy_scoring.keys()) + sorted(dummy_roster.keys())


# ---------------------------------------------------------------------------
# DuckDB type mapping for the flat schema
# ---------------------------------------------------------------------------


def _col_type(key: str) -> str:
    """Map a canonical key to its DuckDB column type."""
    if key == "db_name":
        return "VARCHAR"
    if key == "year":
        return "INTEGER"
    if key in ("first_active_year", "last_active_year"):
        return "INTEGER"
    if key in ("import_mode", "scoring_variant", "scoring_variant_first_year", "scoring_custom_rules"):
        return "VARCHAR"
    if key == "trade_deadline":
        return "BIGINT"
    if key in ("home_team_bonus", "playoff_home_team_bonus"):
        return "DOUBLE"
    if key in (
        "uses_median",
        "uses_fractional_points",
        "has_multiweek_championship",
        "uses_playoff_reseeding",
        "sleeper_best_ball",
        "sleeper_bench_lock",
        "sleeper_taxi_allow_vets",
        "sleeper_pick_trading",
        "sleeper_daily_waivers",
        "is_dynasty",
        "has_consolation_bracket",
    ):
        return "BOOLEAN"
    if key.startswith("scoring_") and key != "scoring_type":
        return "DOUBLE"
    if key.startswith("roster_"):
        return "INTEGER"
    if key in (
        "num_teams",
        "playoff_teams",
        "bye_teams",
        "playoff_start_week",
        "regular_season_weeks",
        "start_week",
        "end_week",
        "waiver_budget",
        "veto_votes_needed",
        "max_keepers",
        "sleeper_taxi_slots",
        "sleeper_taxi_years",
        "sleeper_trade_review_days",
        "sleeper_playoff_type",
        "draft_rounds",
        "reversal_round",
        "num_playoff_consolation_teams",
        "playoff_seeding_rule_by",
    ):
        return "INTEGER"
    return "VARCHAR"


# ---------------------------------------------------------------------------
# DuckDB persistence
# ---------------------------------------------------------------------------


def _create_table_sql(database_name: str) -> str:
    """Generate CREATE TABLE IF NOT EXISTS for the flat league_settings table."""
    cols = []
    for key in get_schema_keys():
        cols.append(f'    "{key}" {_col_type(key)} NOT NULL' if key == "db_name" else f'    "{key}" {_col_type(key)}')
    col_defs = ",\n".join(cols)
    return f"""
CREATE TABLE IF NOT EXISTS "{database_name}".public.league_settings (
{col_defs},
    PRIMARY KEY (db_name, year)
)
"""


def _ensure_settings_columns(conn, database_name: str) -> None:
    """Add missing nullable canonical settings columns to existing tables."""
    existing = {
        r[0]
        for r in conn.execute(
            f"SELECT column_name FROM duckdb_columns() "
            f"WHERE database_name = '{database_name}' AND table_name = 'league_settings'"
        ).fetchall()
    }
    for key in get_schema_keys():
        if key not in existing:
            conn.execute(f'ALTER TABLE "{database_name}".public.league_settings ADD COLUMN "{key}" {_col_type(key)}')


def upload_settings(database_name: str, settings: dict, conn=None) -> bool:
    """Upload a single year's canonical settings through the active DuckDB connection.

    Creates the table if it doesn't exist. Upserts on year (existing year
    rows are replaced, new years are inserted).

    Args:
        database_name: League database name (e.g., 'kmffl')
        settings: Flat canonical settings dict from flatten_settings()
        conn: Optional existing DuckDB connection (creates one if None)

    Returns:
        True if successful
    """
    import logging

    logger = logging.getLogger(__name__)
    close_conn = conn is None
    settings = dict(settings)
    settings.setdefault("db_name", database_name)

    if conn is None:
        raise RuntimeError("conn is required - all pipeline work uses local DuckDB files.")

    try:
        # Ensure database exists
        conn.execute(f'CREATE DATABASE IF NOT EXISTS "{database_name}"')

        # Check if old-format table exists (settings_json column) and drop it
        try:
            old_cols = conn.execute(
                f"SELECT column_name FROM duckdb_columns() "
                f"WHERE database_name = '{database_name}' AND table_name = 'league_settings'"
            ).fetchall()
            old_col_names = {r[0] for r in old_cols}
            if old_col_names and ("settings_json" in old_col_names or "draft_rounds" not in old_col_names):
                conn.execute(f'DROP TABLE "{database_name}".public.league_settings')
                logger.info(f"[SETTINGS] Dropped old-format league_settings table in {database_name}")
        except Exception:
            pass  # Table doesn't exist yet — fine

        # Create table with canonical schema
        conn.execute(_create_table_sql(database_name))
        _ensure_settings_columns(conn, database_name)

        # Build upsert: DELETE + INSERT (DuckDB doesn't have UPSERT on all versions)
        year = settings.get("year")
        if year is None:
            logger.error("Settings dict missing 'year' key")
            return False

        conn.execute(
            f'DELETE FROM "{database_name}".public.league_settings WHERE year = ?',
            [year],
        )

        schema_keys = get_schema_keys()
        placeholders = ", ".join(["?"] * len(schema_keys))
        col_names = ", ".join([f'"{k}"' for k in schema_keys])
        values = [settings.get(k) for k in schema_keys]

        conn.execute(
            f'INSERT INTO "{database_name}".public.league_settings ({col_names}) VALUES ({placeholders})',
            values,
        )

        logger.info(f"[SETTINGS] Uploaded {database_name} year {year} ({len(schema_keys)} columns)")
        return True

    except Exception as e:
        logger.error(f"[SETTINGS] Upload failed for {database_name}: {e}")
        return False
    finally:
        if close_conn:
            conn.close()


def upload_all_settings(database_name: str, settings_by_year: dict[int, dict], conn=None) -> int:
    """Upload multiple years of settings through the active DuckDB connection.

    Args:
        database_name: League database name
        settings_by_year: {year: flat_settings_dict, ...}
        conn: Optional existing DuckDB connection

    Returns:
        Number of years successfully uploaded
    """
    import logging

    logger = logging.getLogger(__name__)
    close_conn = conn is None

    if conn is None:
        raise RuntimeError("conn is required - all pipeline work uses local DuckDB files.")

    try:
        count = 0
        for year in sorted(settings_by_year):
            if upload_settings(database_name, settings_by_year[year], conn=conn):
                count += 1
        logger.info(f"[SETTINGS] Uploaded {count}/{len(settings_by_year)} years to {database_name}")
        return count
    finally:
        if close_conn:
            conn.close()


def read_settings(database_name: str, year: int | None = None, conn=None) -> dict[int, dict]:
    """Read canonical settings from the active DuckDB/Fly target.

    Args:
        database_name: League database name
        year: Specific year to read (None = all years)
        conn: Optional existing DuckDB connection (used as-is when provided)

    Returns:
        {year: flat_settings_dict, ...} — empty dict if table doesn't exist
    """
    import logging

    logger = logging.getLogger(__name__)

    # When an existing connection is passed, use it directly (may be a write conn)
    if conn is not None:
        return _read_settings_via_conn(conn, database_name, year, logger)

    # No connection passed — use the DatabaseReader protocol
    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        schema_keys = get_schema_keys()

        if year is not None:
            sql = f"SELECT * FROM public.league_settings WHERE year = {year}"
        else:
            sql = "SELECT * FROM public.league_settings ORDER BY year"

        rows = reader.query(sql, database=database_name)

        result = {}
        for row in rows:
            yr = row.get("year")
            if yr is not None:
                result[yr] = {key: row.get(key) for key in schema_keys}
        return result

    except Exception as e:
        logger.debug(f"[SETTINGS] Could not read from {database_name}: {e}")
        return {}


def _read_settings_via_conn(conn, database_name: str, year: int | None, logger) -> dict[int, dict]:
    """Read settings using an existing DuckDB connection (legacy path)."""
    try:
        schema_keys = get_schema_keys()

        if year is not None:
            sql = f'SELECT * FROM "{database_name}".public.league_settings WHERE year = ?'
            rows = conn.execute(sql, [year]).fetchall()
        else:
            sql = f'SELECT * FROM "{database_name}".public.league_settings ORDER BY year'
            rows = conn.execute(sql).fetchall()
        result_cols = [d[0] for d in conn.description]

        result = {}
        for row in rows:
            raw_row = dict(zip(result_cols, row))
            yr = raw_row.get("year")
            if yr is not None:
                result[yr] = {key: raw_row.get(key) for key in schema_keys}
        return result

    except Exception as e:
        logger.debug(f"[SETTINGS] Could not read from {database_name}: {e}")
        return {}
