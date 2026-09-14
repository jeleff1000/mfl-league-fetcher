#!/usr/bin/env python3
"""
League Settings Census - shows distribution of scoring, roster, and playoff
settings across all leagues in MotherDuck.

Usage:
    python scripts/league_settings_census.py
"""

import io
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

_FFS = Path(__file__).resolve().parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))

from multi_league.core.db_reader import get_reader

# Force UTF-8 output on Windows to handle emoji/unicode league names
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


def parse_json_field(val):
    """Normalize a DuckDB column that might be dict, JSON string, or None."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return {}
    if isinstance(val, list):
        return val
    return {}


def get_all_league_dbs(reader) -> list:
    """Get all league databases from Yahoo, Sleeper, and ESPN registry tables."""
    leagues = []

    # Yahoo
    try:
        rows = reader.query(
            """
            SELECT league_name, database_name
            FROM ___ops.league_credentials
            ORDER BY league_name
        """,
            database="",
        )
        for r in rows:
            leagues.append({"league_name": r["league_name"], "database_name": r["database_name"], "platform": "Yahoo"})
    except Exception as e:
        print(f"Warning: Could not fetch Yahoo leagues: {e}")

    # Sleeper
    try:
        rows = reader.query(
            """
            SELECT league_name, database_name
            FROM ___ops.main.sleeper_leagues
            ORDER BY league_name
        """,
            database="",
        )
        for r in rows:
            leagues.append(
                {"league_name": r["league_name"], "database_name": r["database_name"], "platform": "Sleeper"}
            )
    except Exception as e:
        print(f"Warning: Could not fetch Sleeper leagues: {e}")

    # ESPN
    try:
        rows = reader.query(
            """
            SELECT league_name, database_name
            FROM ___ops.main.espn_leagues
            ORDER BY league_name
        """,
            database="",
        )
        for r in rows:
            leagues.append({"league_name": r["league_name"], "database_name": r["database_name"], "platform": "ESPN"})
    except Exception as e:
        print(f"Warning: Could not fetch ESPN leagues: {e}")

    return leagues


def fetch_league_settings(reader, db_name: str) -> list:
    """Fetch all league_settings rows for a database. Returns list of dicts.

    The table stores settings in a `settings_json` column (JSON string).
    We parse that and merge with any other columns (year, etc.).
    """
    try:
        df = reader.query_df(
            f"""
            SELECT *
            FROM "{db_name}".public.league_settings
            ORDER BY year
        """,
            database="",
        )
        rows = df.to_dict("records")
    except Exception:
        return []

    parsed = []
    for row in rows:
        # Parse settings_json blob if present
        settings_json = row.get("settings_json", "")
        if isinstance(settings_json, str) and settings_json:
            try:
                data = json.loads(settings_json)
            except (json.JSONDecodeError, ValueError):
                data = {}
        elif isinstance(settings_json, dict):
            data = settings_json
        else:
            data = {}

        # Merge: parsed JSON is the base, row-level columns override (for year, etc.)
        merged = dict(data)
        for k, v in row.items():
            if k == "settings_json":
                continue
            # Only override if the row value is actually set (not NA/None)
            if v is not None and str(v) != "<NA>" and str(v) != "nan":
                merged[k] = v
        parsed.append(merged)

    return parsed


def get_scoring_value(row, stat_id: str, sleeper_key: str = None):
    """Get a scoring value, checking stat_modifiers then scoring_settings then scoring_rules."""
    # stat_modifiers (Yahoo-normalized format)
    mods = parse_json_field(row.get("stat_modifiers"))
    if mods and stat_id in mods:
        return float(mods[stat_id])

    # scoring_settings (Sleeper native format)
    if sleeper_key:
        settings = parse_json_field(row.get("scoring_settings"))
        if settings and sleeper_key in settings:
            return float(settings[sleeper_key])

    # scoring_rules array fallback
    rules = row.get("scoring_rules")
    if isinstance(rules, str):
        try:
            rules = json.loads(rules)
        except (json.JSONDecodeError, ValueError):
            rules = []
    if isinstance(rules, list):
        for rule in rules:
            if isinstance(rule, dict) and str(rule.get("stat_id")) == stat_id:
                return float(rule.get("points", 0))

    return None


def classify_ppr(rec_pts):
    """Classify PPR format from reception points value."""
    if rec_pts is None or rec_pts == 0:
        return "Standard (0 ppr)"
    elif rec_pts == 0.5:
        return "Half PPR (0.5 ppr)"
    elif rec_pts == 1.0:
        return "Full PPR (1.0 ppr)"
    else:
        return f"Custom ({rec_pts} ppr)"


def classify_scoring_type(row):
    """Classify H2H type from row + metadata."""
    meta = parse_json_field(row.get("metadata"))
    scoring = meta.get("scoring_type", "")

    # Sleeper: league_average_match on row or in metadata
    league_avg = row.get("league_average_match", meta.get("league_average_match"))

    # Yahoo: scoring_type describes the matchup format
    if scoring == "head_to_head_median" or league_avg == 1:
        return "H2H + Median"
    elif scoring in ("head", "head_to_head"):
        return "Head-to-Head"
    elif scoring == "points":
        return "Points Only"
    elif scoring == "roto":
        return "Rotisserie"
    # Sleeper: scoring_type is PPR format ("Half PPR", "PPR", etc), not matchup type
    # Sleeper is always H2H unless league_average_match is set
    elif meta.get("platform") == "sleeper" or row.get("_platform") == "Sleeper":
        return "Head-to-Head"
    elif scoring:
        return f"Other ({scoring})"
    else:
        return "Unknown"


def classify_draft_type(row):
    """Classify draft type from row + metadata."""
    meta = parse_json_field(row.get("metadata"))
    # Check row-level first (Sleeper), then metadata (Yahoo)
    dt = row.get("draft_type", meta.get("draft_type", ""))
    is_auction = row.get("is_auction", False)

    if dt == "dynasty":
        return "Dynasty"
    elif is_auction or dt == "auction":
        return "Auction"
    elif dt in ("snake", "linear", "offline_snake"):
        return "Snake"
    elif dt in ("live", "self", "offline"):
        # Yahoo ambiguous types — need cost data to determine
        return "Auction" if is_auction else "Unknown"
    elif dt:
        return dt.title()
    else:
        return "Unknown"


def classify_league_type(row):
    """Classify dynasty vs keeper vs redraft."""
    meta = parse_json_field(row.get("metadata"))
    dt = row.get("draft_type", meta.get("draft_type", ""))
    taxi = _safe_int(row.get("taxi_slots", 0))
    pick_trading = _safe_int(row.get("pick_trading", 0))
    max_keepers = _safe_int(row.get("max_keepers", meta.get("max_keepers", 0)))

    if dt == "dynasty" or taxi > 0 or pick_trading == 1:
        return "Dynasty"
    elif max_keepers > 0:
        return "Keeper"
    else:
        return "Redraft"


def _safe_int(val, default=0):
    """Convert to int safely."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# Canonical scoring rule mappings: raw key -> (display_name, category)
# Yahoo uses stat_id numbers, Sleeper uses descriptive keys
YAHOO_STAT_MAP = {
    # Passing
    "1": ("Pass Attempts", "Passing"),
    "2": ("Completions", "Passing"),
    "4": ("Pass Yds (per yd)", "Passing"),
    "5": ("Pass TD", "Passing"),
    "6": ("Int Thrown", "Passing"),
    "7": ("Sacks Taken (QB)", "Passing"),
    "16": ("2PT Conversion", "Passing"),
    "58": ("Pick Sixes Thrown", "Passing"),
    # Rushing
    "9": ("Rush Yds (per yd)", "Rushing"),
    "10": ("Rush TD", "Rushing"),
    # Receiving
    "11": ("Receptions (PPR)", "Receiving"),
    "12": ("Rec Yds (per yd)", "Receiving"),
    "13": ("Rec TD", "Receiving"),
    # Misc Offense
    "14": ("Off Return Yds", "Misc Offense"),
    "15": ("Off Return TD", "Misc Offense"),
    "18": ("Fum Lost", "Misc Offense"),
    "57": ("Off Fum Ret TD", "Misc Offense"),
    # Kicking
    "19": ("FG 0-19", "Kicking"),
    "20": ("FG 20-29", "Kicking"),
    "21": ("FG 30-39", "Kicking"),
    "22": ("FG 40-49", "Kicking"),
    "23": ("FG 50+", "Kicking"),
    "24": ("FG Miss 0-19", "Kicking"),
    "25": ("FG Miss 20-29", "Kicking"),
    "26": ("FG Miss 30-39", "Kicking"),
    "27": ("FG Miss 40-49", "Kicking"),
    "28": ("FG Miss 50+", "Kicking"),
    "29": ("PAT Made", "Kicking"),
    "30": ("XP Miss", "Kicking"),
    "84": ("FG Total Yards", "Kicking"),
    # DST
    "32": ("DEF Sack", "DST"),
    "33": ("DEF Int", "DST"),
    "34": ("DEF Fum Rec", "DST"),
    "35": ("DEF TD", "DST"),
    "36": ("DEF Safety", "DST"),
    "37": ("DEF Blk Kick", "DST"),
    "48": ("DEF Ret Yds (per yd)", "DST"),
    "49": ("DEF/ST Ret TD", "DST"),
    "67": ("DEF 4th Down Stops", "DST"),
    "68": ("DEF TFL", "DST"),
    "77": ("DEF 3 and Outs", "DST"),
    "82": ("XP Returned", "DST"),
    # Points Allowed
    "50": ("Pts Allow 0", "DST Pts Allowed"),
    "51": ("Pts Allow 1-6", "DST Pts Allowed"),
    "52": ("Pts Allow 7-13", "DST Pts Allowed"),
    "53": ("Pts Allow 14-20", "DST Pts Allowed"),
    "54": ("Pts Allow 21-27", "DST Pts Allowed"),
    "55": ("Pts Allow 28-34", "DST Pts Allowed"),
    "56": ("Pts Allow 35+", "DST Pts Allowed"),
    # Yards Allowed
    "70": ("DEF Yds Allow Neg", "DST Yds Allowed"),
    "71": ("Yds Allow 0-99", "DST Yds Allowed"),
    "72": ("Yds Allow 100-199", "DST Yds Allowed"),
    "73": ("Yds Allow 200-299", "DST Yds Allowed"),
    "74": ("Yds Allow 300-399", "DST Yds Allowed"),
    "75": ("Yds Allow 400-499", "DST Yds Allowed"),
    "76": ("Yds Allow 500+", "DST Yds Allowed"),
    # Bonuses
    "59": ("40+ Yd Completion", "Bonuses"),
    "60": ("40+ Yd Pass TD", "Bonuses"),
    "61": ("40+ Yd Rush", "Bonuses"),
    "62": ("40+ Yd Rush TD", "Bonuses"),
    "63": ("40+ Yd Reception", "Bonuses"),
    "64": ("40+ Yd Rec TD", "Bonuses"),
    "80": ("Rec 1st Downs", "Bonuses"),
    "81": ("Rush 1st Downs", "Bonuses"),
    # IDP
    "38": ("IDP Tackle Solo", "IDP"),
    "39": ("IDP Tackle Assist", "IDP"),
    "40": ("IDP Sack", "IDP"),
    "41": ("IDP Int", "IDP"),
    "42": ("IDP Fumble Force", "IDP"),
    "43": ("IDP Fumble Recovery", "IDP"),
    "44": ("IDP TD", "IDP"),
    "45": ("IDP Safety", "IDP"),
    "46": ("IDP Pass Defended", "IDP"),
    "47": ("IDP Block Kick", "IDP"),
    "65": ("IDP TFL", "IDP"),
    "83": ("IDP XP Returned", "IDP"),
}

SLEEPER_KEY_MAP = {
    # Passing
    "pass_yd": ("Pass Yds (per yd)", "Passing"),
    "pass_td": ("Pass TD", "Passing"),
    "pass_int": ("Int Thrown", "Passing"),
    "pass_2pt": ("Pass 2PT", "Passing"),
    "pass_td_40p": ("Bonus: 40+ yd Pass TD", "Bonuses"),
    "pass_int_td": ("Pick Sixes Thrown", "Passing"),
    "pass_cmp": ("Completions", "Passing"),
    "pass_cmp_40p": ("40+ Yd Completion", "Bonuses"),
    "pass_sack": ("Sacks Taken (QB)", "Passing"),
    "pass_fd": ("Pass 1st Downs", "Bonuses"),
    "pass_td_50p": ("50+ Yd Pass TD", "Bonuses"),
    # Rushing
    "rush_yd": ("Rush Yds (per yd)", "Rushing"),
    "rush_td": ("Rush TD", "Rushing"),
    "rush_2pt": ("Rush 2PT", "Rushing"),
    "rush_td_40p": ("Bonus: 40+ yd Rush TD", "Bonuses"),
    "rush_att": ("Rush Attempts", "Rushing"),
    "rush_fd": ("Rush 1st Downs", "Bonuses"),
    "rush_40p": ("40+ Yd Rush", "Bonuses"),
    "rush_td_50p": ("50+ Yd Rush TD", "Bonuses"),
    # Receiving
    "rec": ("Receptions (PPR)", "Receiving"),
    "rec_yd": ("Rec Yds (per yd)", "Receiving"),
    "rec_td": ("Rec TD", "Receiving"),
    "rec_2pt": ("Rec 2PT", "Receiving"),
    "rec_td_40p": ("Bonus: 40+ yd Rec TD", "Bonuses"),
    "rec_fd": ("Rec 1st Downs", "Bonuses"),
    "rec_40p": ("40+ Yd Reception", "Bonuses"),
    "rec_td_50p": ("50+ Yd Rec TD", "Bonuses"),
    "rec_0_4": ("Rec 0-4 Yds", "Receiving"),
    "rec_5_9": ("Rec 5-9 Yds", "Receiving"),
    "rec_10_19": ("Rec 10-19 Yds", "Receiving"),
    "rec_20_29": ("Rec 20-29 Yds", "Receiving"),
    "rec_30_39": ("Rec 30-39 Yds", "Receiving"),
    # Misc Offense
    "fum": ("Fumble (any)", "Misc Offense"),
    "fum_lost": ("Fum Lost", "Misc Offense"),
    "fum_rec": ("Off Fum Rec", "Misc Offense"),
    "fum_rec_td": ("Off Fum Rec TD", "Misc Offense"),
    "off_fum_ret_td": ("Off Fum Ret TD", "Misc Offense"),
    "kr_yd": ("Off Kick Ret Yds", "Misc Offense"),
    "pr_yd": ("Off Punt Ret Yds", "Misc Offense"),
    "kr_td": ("Off Kick Ret TD", "Misc Offense"),
    "pr_td": ("Off Punt Ret TD", "Misc Offense"),
    "blk_kick_ret_yd": ("Blocked Kick Ret Yds", "Misc Offense"),
    "fg_ret_yd": ("FG Ret Yds", "Misc Offense"),
    "fum_ret_yd": ("Fum Ret Yds", "Misc Offense"),
    "int_ret_yd": ("Int Ret Yds", "Misc Offense"),
    # Kicking
    "fgm": ("FG Made (flat)", "Kicking"),
    "fgm_0_19": ("FG 0-19", "Kicking"),
    "fgm_20_29": ("FG 20-29", "Kicking"),
    "fgm_30_39": ("FG 30-39", "Kicking"),
    "fgm_40_49": ("FG 40-49", "Kicking"),
    "fgm_50p": ("FG 50+", "Kicking"),
    "fgm_50_59": ("FG 50-59", "Kicking"),
    "fgm_60p": ("FG 60+", "Kicking"),
    "fgmiss": ("FG Miss (any)", "Kicking"),
    "fgmiss_0_19": ("FG Miss 0-19", "Kicking"),
    "fgmiss_20_29": ("FG Miss 20-29", "Kicking"),
    "fgmiss_30_39": ("FG Miss 30-39", "Kicking"),
    "fgmiss_40_49": ("FG Miss 40-49", "Kicking"),
    "fgmiss_50p": ("FG Miss 50+", "Kicking"),
    "fgm_yds_over_30": ("FG Yds Over 30", "Kicking"),
    "fgm_yds": ("FG Made Yds (per yd)", "Kicking"),
    "xpm": ("XP Made", "Kicking"),
    "xpmiss": ("XP Miss", "Kicking"),
    # DST
    "sack": ("DEF Sack", "DST"),
    "int": ("DEF Int", "DST"),
    "ff": ("DEF Forced Fum", "DST"),
    "safe": ("DEF Safety", "DST"),
    "blk_kick": ("DEF Blk Kick", "DST"),
    "def_td": ("DEF TD", "DST"),
    "def_st_td": ("DEF/ST TD", "DST"),
    "def_st_ff": ("DEF/ST Forced Fum", "DST"),
    "def_st_fum_rec": ("DEF/ST Fum Rec", "DST"),
    "st_td": ("ST TD", "DST"),
    "st_fum_rec": ("ST Fum Rec", "DST"),
    "st_ff": ("ST Forced Fum", "DST"),
    "def_2pt": ("DEF 2PT Return", "DST"),
    "def_4_and_stop": ("DEF 4th Down Stops", "DST"),
    "def_3_and_out": ("DEF 3 and Outs", "DST"),
    "def_pr_yd": ("DEF Punt Ret Yds", "DST"),
    "def_kr_yd": ("DEF Kick Ret Yds", "DST"),
    "def_kr_td": ("DEF Kick Ret TD", "DST"),
    "def_pr_td": ("DEF Punt Ret TD", "DST"),
    "def_pass_def": ("DEF Pass Defended", "DST"),
    "tkl_loss": ("DEF TFL", "DST"),
    "def_forced_punts": ("DEF Forced Punts", "DST"),
    "pts_allow": ("Pts Allow (per pt)", "DST Pts Allowed"),
    "qb_hit": ("DEF QB Hit", "DST"),
    # Points Allowed
    "pts_allow_0": ("Pts Allow 0", "DST Pts Allowed"),
    "pts_allow_1_6": ("Pts Allow 1-6", "DST Pts Allowed"),
    "pts_allow_7_13": ("Pts Allow 7-13", "DST Pts Allowed"),
    "pts_allow_14_20": ("Pts Allow 14-20", "DST Pts Allowed"),
    "pts_allow_21_27": ("Pts Allow 21-27", "DST Pts Allowed"),
    "pts_allow_28_34": ("Pts Allow 28-34", "DST Pts Allowed"),
    "pts_allow_35p": ("Pts Allow 35+", "DST Pts Allowed"),
    # Yards Allowed
    "yds_allow_0_100": ("Yds Allow 0-100", "DST Yds Allowed"),
    "yds_allow_100_199": ("Yds Allow 100-199", "DST Yds Allowed"),
    "yds_allow_200_299": ("Yds Allow 200-299", "DST Yds Allowed"),
    "yds_allow_300_399": ("Yds Allow 300-399", "DST Yds Allowed"),
    "yds_allow_400_449": ("Yds Allow 400-449", "DST Yds Allowed"),
    "yds_allow_450_499": ("Yds Allow 450-499", "DST Yds Allowed"),
    "yds_allow_500_549": ("Yds Allow 500-549", "DST Yds Allowed"),
    "yds_allow_550p": ("Yds Allow 550+", "DST Yds Allowed"),
    "yds_allow_300_349": ("Yds Allow 300-349", "DST Yds Allowed"),
    "yds_allow_350_399": ("Yds Allow 350-399", "DST Yds Allowed"),
    # Bonuses
    "bonus_pass_yd_300": ("Bonus: 300+ Pass Yds", "Bonuses"),
    "bonus_pass_yd_400": ("Bonus: 400+ Pass Yds", "Bonuses"),
    "bonus_pass_yd_500": ("Bonus: 500+ Pass Yds", "Bonuses"),
    "bonus_rec_yd_100": ("Bonus: 100+ Rec Yds", "Bonuses"),
    "bonus_rec_yd_200": ("Bonus: 200+ Rec Yds", "Bonuses"),
    "bonus_rec_yd_300": ("Bonus: 300+ Rec Yds", "Bonuses"),
    "bonus_rush_yd_100": ("Bonus: 100+ Rush Yds", "Bonuses"),
    "bonus_rush_yd_200": ("Bonus: 200+ Rush Yds", "Bonuses"),
    "bonus_rush_yd_300": ("Bonus: 300+ Rush Yds", "Bonuses"),
    "bonus_rec_te": ("TE Premium", "Bonuses"),
    "bonus_rush_rec_yd_100": ("Bonus: 100+ Rush+Rec Yds", "Bonuses"),
    "bonus_rush_rec_yd_200": ("Bonus: 200+ Rush+Rec Yds", "Bonuses"),
    "bonus_pass_cmp_25": ("Bonus: 25+ Completions", "Bonuses"),
    "bonus_rush_att_20": ("Bonus: 20+ Rush Att", "Bonuses"),
    "bonus_tkl_10p": ("Bonus: 10+ Tackles", "Bonuses"),
    "bonus_def_fum_td_50p": ("Bonus: 50+ Yd DEF Fum TD", "Bonuses"),
    "bonus_def_int_td_50p": ("Bonus: 50+ Yd DEF INT TD", "Bonuses"),
    "bonus_fd_rb": ("Bonus: RB 1st Down", "Bonuses"),
    "bonus_fd_te": ("Bonus: TE 1st Down", "Bonuses"),
    "bonus_fd_wr": ("Bonus: WR 1st Down", "Bonuses"),
    # IDP
    "idp_tkl": ("IDP Tackle Combined", "IDP"),
    "idp_tkl_solo": ("IDP Tackle Solo", "IDP"),
    "idp_tkl_ast": ("IDP Tackle Assist", "IDP"),
    "idp_tkl_loss": ("IDP TFL", "IDP"),
    "idp_sack": ("IDP Sack", "IDP"),
    "idp_int": ("IDP Int", "IDP"),
    "idp_ff": ("IDP Fumble Force", "IDP"),
    "idp_fum_rec": ("IDP Fumble Recovery", "IDP"),
    "idp_pass_def": ("IDP Pass Defended", "IDP"),
    "idp_qb_hit": ("IDP QB Hit", "IDP"),
    "idp_blk_kick": ("IDP Block Kick", "IDP"),
    "idp_safe": ("IDP Safety", "IDP"),
    "idp_def_td": ("IDP TD", "IDP"),
    "idp_fum_ret_yd": ("IDP Fumble Ret Yds", "IDP"),
    "idp_int_ret_yd": ("IDP Int Ret Yds", "IDP"),
    "idp_sack_yd": ("IDP Sack Yds", "IDP"),
}

CATEGORY_ORDER = [
    "Passing",
    "Rushing",
    "Receiving",
    "Misc Offense",
    "Kicking",
    "DST",
    "DST Pts Allowed",
    "DST Yds Allowed",
    "IDP",
    "Bonuses",
    "Other",
]


def extract_scoring_rules(row):
    """Extract all scoring rules from a league_settings row.

    Returns list of (canonical_name, category, value) tuples.
    """
    rules = []

    # Try stat_modifiers first (Yahoo - keyed by stat_id)
    mods = parse_json_field(row.get("stat_modifiers"))
    if mods:
        for stat_id, pts in mods.items():
            lookup = YAHOO_STAT_MAP.get(str(stat_id))
            if lookup:
                name, cat = lookup
            else:
                name, cat = f"yahoo_stat_{stat_id}", "Other"
            rules.append((name, cat, float(pts)))
        return rules

    # Try scoring_settings (Sleeper - keyed by descriptive names)
    settings = parse_json_field(row.get("scoring_settings"))
    if settings:
        for key, pts in settings.items():
            lookup = SLEEPER_KEY_MAP.get(key)
            if lookup:
                name, cat = lookup
            else:
                name, cat = key, "Other"
            rules.append((name, cat, float(pts)))
        return rules

    return rules


def format_pts(val):
    """Format a points value for display."""
    if val == int(val):
        return f"{int(val)} pts"
    # Clean up floating point noise (e.g. 0.03999999910593033 -> 0.04)
    rounded = round(val, 4)
    # Remove trailing zeros
    s = f"{rounded:.4f}".rstrip("0").rstrip(".")
    return f"{s} pts"


def get_flex_positions(roster_counts):
    """Extract FLEX-type positions from roster counts."""
    flex_names = {
        "W/R/T": "W/R/T (FLEX)",
        "W/R": "W/R (WR/RB FLEX)",
        "W/T": "W/T (WR/TE FLEX)",
        "Q/W/R/T": "Q/W/R/T (Superflex)",
        "FLEX": "FLEX",
        "SUPER_FLEX": "SUPER_FLEX",
        "WRRB_FLEX": "WRRB_FLEX",
        "REC_FLEX": "REC_FLEX",
    }
    found = {}
    for key, label in flex_names.items():
        count = roster_counts.get(key, 0)
        if isinstance(count, str):
            count = int(count) if count.isdigit() else 0
        if count > 0:
            found[label] = int(count)
    return found


def print_counter(counter, total, indent="  ", show_pct=True):
    """Print a counter dict with alignment and optional percentages."""
    if not counter:
        print(f"{indent}(none)")
        return
    max_label = max(len(str(k)) for k in counter)
    for label, count in counter.most_common():
        padded = f"{indent}{label} "
        dots = "." * max(1, 55 - len(padded))
        pct = f" ({count * 100 // total}%)" if show_pct and total > 0 else ""
        print(f"{padded}{dots} {count:3}{pct}")


def main():
    reader = get_reader()

    print("Fetching league databases...")
    leagues = get_all_league_dbs(reader)
    print(f"Found {len(leagues)} leagues")

    # Collect settings per league (use most recent year for league-level stats)
    all_settings = []  # (league_name, platform, year, parsed_settings)
    latest_per_league = {}  # league_name -> most recent year's settings
    skipped = []

    for i, league in enumerate(leagues, 1):
        name = league["league_name"]
        db = league["database_name"]
        platform = league["platform"]
        safe_name = name.encode("ascii", "replace").decode("ascii")
        print(f"  [{i:2}/{len(leagues)}] {safe_name[:45]:45}", end="\r", flush=True)

        rows = fetch_league_settings(reader, db)
        if not rows:
            skipped.append(name)
            continue

        for row in rows:
            year = row.get("year")
            if year is None:
                continue
            # Tag row with platform for cross-platform detection
            row["_platform"] = platform
            entry = {
                "league_name": name,
                "platform": platform,
                "year": int(year),
                "row": row,
            }
            all_settings.append(entry)

            # Track latest year per league
            if name not in latest_per_league or int(year) > latest_per_league[name]["year"]:
                latest_per_league[name] = entry

    print()

    if not all_settings:
        print("No league settings found!")
        return 1

    total_leagues = len(latest_per_league)
    total_league_years = len(all_settings)
    platform_counts = Counter(latest_per_league[n]["platform"] for n in latest_per_league)

    # =========================================================================
    print()
    print("=" * 70)
    print("LEAGUE SETTINGS CENSUS")
    print("=" * 70)
    print(f"\n{total_leagues} leagues ({', '.join(f'{p}: {c}' for p, c in platform_counts.most_common())})")
    print(f"{total_league_years} total league-years")
    if skipped:
        print(f"{len(skipped)} leagues skipped (no league_settings table)")

    # =========================================================================
    # SCORING FORMAT (PPR)
    print("\n" + "-" * 70)
    print("SCORING FORMAT (most recent year per league)")
    print("-" * 70)
    ppr_counter = Counter()
    ppr_leagues = defaultdict(list)
    for name, entry in latest_per_league.items():
        rec_pts = get_scoring_value(entry["row"], "11", "rec")
        fmt = classify_ppr(rec_pts)
        ppr_counter[fmt] += 1
        ppr_leagues[fmt].append(name)
    print_counter(ppr_counter, total_leagues)

    # =========================================================================
    # LEAGUE SIZE
    print("\n" + "-" * 70)
    print("LEAGUE SIZE (most recent year)")
    print("-" * 70)
    size_counter = Counter()
    for name, entry in latest_per_league.items():
        row = entry["row"]
        meta = parse_json_field(row.get("metadata"))
        num = row.get("num_teams", meta.get("num_teams", "?"))
        size_counter[f"{num} teams"] += 1
    print_counter(size_counter, total_leagues)

    # =========================================================================
    # SCORING TYPE
    print("\n" + "-" * 70)
    print("SCORING TYPE")
    print("-" * 70)
    stype_counter = Counter()
    stype_leagues = defaultdict(list)
    for name, entry in latest_per_league.items():
        st = classify_scoring_type(entry["row"])
        stype_counter[st] += 1
        stype_leagues[st].append(name)
    print_counter(stype_counter, total_leagues)
    # Show H2H+Median leagues since they're edge cases
    if stype_leagues.get("H2H + Median"):
        print("    Leagues:", ", ".join(sorted(stype_leagues["H2H + Median"])))

    # =========================================================================
    # DRAFT TYPE
    print("\n" + "-" * 70)
    print("DRAFT TYPE (most recent year)")
    print("-" * 70)
    draft_counter = Counter()
    draft_leagues = defaultdict(list)
    for name, entry in latest_per_league.items():
        dt = classify_draft_type(entry["row"])
        draft_counter[dt] += 1
        draft_leagues[dt].append(name)
    print_counter(draft_counter, total_leagues)
    # Show dynasty/auction leagues since they're edge cases
    for dt_type in ("Dynasty", "Auction"):
        if draft_leagues.get(dt_type):
            print(f"    {dt_type} leagues: {', '.join(sorted(draft_leagues[dt_type]))}")

    # =========================================================================
    # LEAGUE TYPE (Dynasty / Keeper / Redraft)
    print("\n" + "-" * 70)
    print("LEAGUE TYPE")
    print("-" * 70)
    ltype_counter = Counter()
    ltype_leagues = defaultdict(list)
    for name, entry in latest_per_league.items():
        lt = classify_league_type(entry["row"])
        ltype_counter[lt] += 1
        ltype_leagues[lt].append(name)
    print_counter(ltype_counter, total_leagues)
    for lt in ("Dynasty", "Keeper"):
        if ltype_leagues.get(lt):
            print(f"    {lt} leagues: {', '.join(sorted(ltype_leagues[lt]))}")

    # Dynasty-specific features
    dynasty_names = set(ltype_leagues.get("Dynasty", []))
    if dynasty_names:
        print(f"\n  Dynasty features (across {len(dynasty_names)} dynasty leagues):")
        taxi_counter = Counter()
        reserve_counter = Counter()
        pick_trade_counter = Counter()
        for name in dynasty_names:
            row = latest_per_league[name]["row"]
            taxi = _safe_int(row.get("taxi_slots", 0))
            reserve = _safe_int(row.get("reserve_slots", 0))
            pick_t = _safe_int(row.get("pick_trading", 0))
            taxi_counter[f"{taxi} taxi slots"] += 1
            reserve_counter[f"{reserve} reserve/IR slots"] += 1
            pick_trade_counter["Pick trading ON" if pick_t else "Pick trading OFF"] += 1
        print_counter(taxi_counter, len(dynasty_names), indent="    ", show_pct=False)
        print_counter(reserve_counter, len(dynasty_names), indent="    ", show_pct=False)
        print_counter(pick_trade_counter, len(dynasty_names), indent="    ", show_pct=False)

    # =========================================================================
    # ROSTER CONFIGURATION
    print("\n" + "-" * 70)
    print("ROSTER STARTERS (most recent year, excluding BN/IR)")
    print("-" * 70)
    starter_positions = ["QB", "WR", "RB", "TE", "K", "DEF"]
    for pos in starter_positions:
        pos_counter = Counter()
        for name, entry in latest_per_league.items():
            counts = parse_json_field(entry["row"].get("roster_position_counts"))
            n = counts.get(pos, 0)
            if isinstance(n, str):
                n = int(n) if n.isdigit() else 0
            pos_counter[n] += 1
        # Only show if there's variation or it's interesting
        vals = sorted(pos_counter.keys())
        if len(vals) == 1 and vals[0] == 0:
            continue  # Skip positions no one uses
        print(f"\n  {pos}:")
        for val in sorted(vals):
            count = pos_counter[val]
            pct = count * 100 // total_leagues
            if val == 0:
                print(f"    No {pos:4} ............. {count:3} leagues ({pct}%)")
            else:
                print(f"    {val} {pos:4} ............. {count:3} leagues ({pct}%)")

    # FLEX positions
    print("\n  FLEX slots:")
    flex_counter = Counter()
    flex_leagues = defaultdict(list)
    no_flex = []
    for name, entry in latest_per_league.items():
        counts = parse_json_field(entry["row"].get("roster_position_counts"))
        flexes = get_flex_positions(counts)
        if not flexes:
            no_flex.append(name)
            flex_counter["No FLEX"] += 1
        for flex_label, flex_count in flexes.items():
            key = f"{flex_count}x {flex_label}"
            flex_counter[key] += 1
            flex_leagues[key].append(name)
    print_counter(flex_counter, total_leagues, indent="    ", show_pct=True)
    # Show unusual FLEX types
    for key in ["SUPER_FLEX", "WRRB_FLEX", "REC_FLEX"]:
        for flex_key, names in flex_leagues.items():
            if key in flex_key:
                print(f"      Leagues: {', '.join(sorted(names))}")

    # BENCH / IR
    print("\n  Bench & IR:")
    bn_counter = Counter()
    ir_counter = Counter()
    for name, entry in latest_per_league.items():
        counts = parse_json_field(entry["row"].get("roster_position_counts"))
        bn = counts.get("BN", 0)
        ir_from_rpc = counts.get("IR", 0)
        if isinstance(bn, str):
            bn = int(bn) if bn.isdigit() else 0
        if isinstance(ir_from_rpc, str):
            ir_from_rpc = int(ir_from_rpc) if ir_from_rpc.isdigit() else 0
        # Sleeper stores IR as reserve_slots at the row level
        ir_from_reserve = _safe_int(entry["row"].get("reserve_slots", 0))
        ir = max(int(ir_from_rpc), ir_from_reserve)
        bn_counter[f"{bn} BN"] += 1
        ir_counter[ir] += 1
    print_counter(bn_counter, total_leagues, indent="    ", show_pct=False)
    print()
    ir_has = sum(c for v, c in ir_counter.items() if v > 0)
    ir_none = ir_counter.get(0, 0)
    print(f"    Has IR slots ............. {ir_has:3} leagues")
    print(f"    No IR slots .............. {ir_none:3} leagues")

    # =========================================================================
    # PLAYOFF SETTINGS
    print("\n" + "-" * 70)
    print("PLAYOFF SETTINGS (most recent year)")
    print("-" * 70)
    playoff_counter = Counter()
    bye_counter = Counter()
    mwc_counter = Counter()
    for name, entry in latest_per_league.items():
        row = entry["row"]
        meta = parse_json_field(row.get("metadata"))
        npt = meta.get("num_playoff_teams", row.get("num_playoff_teams", "?"))
        playoff_counter[f"{npt} playoff teams"] += 1
        bt = meta.get("bye_teams", row.get("bye_teams", 0))
        if isinstance(bt, str):
            bt = int(bt) if bt.isdigit() else 0
        bye_counter[f"{bt} bye teams"] += 1
        mwc = meta.get("has_multiweek_championship", row.get("has_multiweek_championship", "0"))
        if str(mwc) == "1":
            mwc_counter["Multiweek championship"] += 1
        else:
            mwc_counter["Single week championship"] += 1

    print("\n  Playoff teams:")
    print_counter(playoff_counter, total_leagues, indent="    ")
    print("\n  Bye teams:")
    print_counter(bye_counter, total_leagues, indent="    ")
    print("\n  Championship format:")
    print_counter(mwc_counter, total_leagues, indent="    ")

    # =========================================================================
    # COMPLETE SCORING RULES (every league-year)
    print("\n" + "-" * 70)
    print(f"COMPLETE SCORING RULES (all {total_league_years} league-years)")
    print("-" * 70)
    print("  Every scoring rule across every league and year.")
    print("  Non-majority values list affected league-years.")

    # Collect: {canonical_name: {category, values: {label: value}}}
    # label = "League Name (YYYY)"
    all_rules = defaultdict(lambda: {"category": "Other", "values": {}})

    for entry in all_settings:
        label = f"{entry['league_name']} ({entry['year']})"
        rules = extract_scoring_rules(entry["row"])
        for canonical_name, category, value in rules:
            all_rules[canonical_name]["category"] = category
            all_rules[canonical_name]["values"][label] = value

    # Print by category
    for category in CATEGORY_ORDER:
        rules_in_cat = [(rname, data) for rname, data in all_rules.items() if data["category"] == category]
        if not rules_in_cat:
            continue

        rules_in_cat.sort(key=lambda x: x[0])

        print(f"\n  === {category.upper()} ===")

        for rule_name, data in rules_in_cat:
            league_vals = data["values"]
            if not league_vals:
                continue

            # Skip rules that are 0 for every league-year
            if all(v == 0 for v in league_vals.values()):
                continue

            # Count value distribution
            val_counter = Counter()
            val_labels = defaultdict(list)
            for label, value in league_vals.items():
                val_counter[value] += 1
                val_labels[value].append(label)

            most_common_val, most_common_count = val_counter.most_common(1)[0]

            if len(val_counter) == 1:
                n = len(league_vals)
                print(f"    {rule_name}: {format_pts(most_common_val)} ({n} league-years)")
            else:
                print(f"    {rule_name}:")
                for val, count in sorted(val_counter.items(), key=lambda x: -x[1]):
                    fmt_val = format_pts(val)
                    marker = " *" if count < most_common_count else ""
                    # Show league-year names for non-majority values (up to 10)
                    if count <= most_common_count // 2 or count <= 10:
                        names = sorted(val_labels[val])
                        if len(names) > 10:
                            names_str = f"  [{', '.join(names[:10])}, +{len(names)-10} more]"
                        else:
                            names_str = f"  [{', '.join(names)}]"
                    else:
                        names_str = ""
                    print(f"      {fmt_val:>12} ... {count:3} league-yrs{marker}{names_str}")

    # Unique scoring configurations fingerprint (per league-year)
    print("\n" + "-" * 70)
    print("UNIQUE SCORING CONFIGURATIONS")
    print("-" * 70)
    print("  League-years grouped by identical scoring rule sets.")

    fingerprints = defaultdict(list)
    for entry in all_settings:
        label = f"{entry['league_name']} ({entry['year']})"
        rules = extract_scoring_rules(entry["row"])
        sig = tuple(sorted((rname, round(val, 4)) for rname, _cat, val in rules if val != 0))
        fingerprints[sig].append(label)

    # Sort by group size descending
    groups = sorted(fingerprints.values(), key=lambda x: -len(x))
    for i, group in enumerate(groups, 1):
        # Collapse to league names if all years of a league share the config
        league_names = sorted(set(g.rsplit(" (", 1)[0] for g in group))
        years = sorted(set(g.rsplit(" (", 1)[1].rstrip(")") for g in group))
        if len(group) > 5:
            year_range = f"{years[0]}-{years[-1]}" if len(years) > 1 else years[0]
            print(f"  Config #{i} ({len(group)} league-yrs, {len(league_names)} leagues, {year_range}):")
            print(f"    {', '.join(league_names)}")
        elif len(group) > 1:
            print(f"  Config #{i} ({len(group)} league-yrs): {', '.join(sorted(group))}")
        else:
            print(f"  Config #{i} (unique): {group[0]}")

    # =========================================================================
    # HISTORY DEPTH
    print("\n" + "-" * 70)
    print("LEAGUE HISTORY DEPTH")
    print("-" * 70)
    depth_counter = Counter()
    for name, entry in latest_per_league.items():
        years = [e["year"] for e in all_settings if e["league_name"] == name]
        n_years = len(years)
        if n_years >= 10:
            bucket = "10+ years"
        elif n_years >= 5:
            bucket = "5-9 years"
        elif n_years >= 3:
            bucket = "3-4 years"
        else:
            bucket = "1-2 years"
        depth_counter[bucket] += 1
    print_counter(depth_counter, total_leagues)

    # =========================================================================
    # YEAR RANGE
    print("\n" + "-" * 70)
    print("YEARS COVERED")
    print("-" * 70)
    year_counter = Counter()
    for entry in all_settings:
        year_counter[entry["year"]] += 1
    for year in sorted(year_counter):
        print(f"  {year}: {year_counter[year]:3} leagues")

    if skipped:
        print("\n" + "-" * 70)
        print(f"SKIPPED ({len(skipped)} leagues - no league_settings table)")
        print("-" * 70)
        for name in sorted(skipped):
            print(f"  - {name}")

    # =========================================================================
    # DATA HEALTH (SQL-based, no table downloads)
    print("\n" + "=" * 70)
    print("DATA HEALTH CHECK (SQL-based)")
    print("=" * 70)

    healthy = []
    unhealthy = []
    qa_candidates = []

    for league in leagues:
        db = league["database_name"]
        name = league["league_name"]
        platform = league["platform"]

        try:
            # Matchup years vs settings years
            m_yrs = [
                r["year"]
                for r in reader.query(f'SELECT DISTINCT year FROM "{db}".public.matchup ORDER BY year', database="")
            ]
            s_yrs = [
                r["year"]
                for r in reader.query(
                    f'SELECT DISTINCT year FROM "{db}".public.league_settings ORDER BY year', database=""
                )
            ]

            # Optimal points coverage
            opt_val = reader.query_scalar(
                f"""
                SELECT SUM(CASE WHEN optimal_points > 0 THEN 1 ELSE 0 END)*100/NULLIF(COUNT(*),0)
                FROM "{db}".public.matchup
            """,
                database="",
            )
            opt_pct = int(opt_val or 0)

            # Champion/sacko counts
            try:
                champs = (
                    reader.query_scalar(
                        f'SELECT COUNT(DISTINCT year) FROM "{db}".public.matchup WHERE champion = 1',
                        database="",
                    )
                    or 0
                )
            except Exception:
                champs = 0
            try:
                sackos = (
                    reader.query_scalar(
                        f'SELECT COUNT(DISTINCT year) FROM "{db}".public.matchup WHERE sacko = 1',
                        database="",
                    )
                    or 0
                )
            except Exception:
                sackos = 0

            # Win populated on player_fantasy
            try:
                win_val = reader.query_scalar(
                    f"""
                    SELECT SUM(CASE WHEN win IS NOT NULL THEN 1 ELSE 0 END)*100/NULLIF(COUNT(*),0)
                    FROM "{db}".public.player_fantasy
                    WHERE manager IS NOT NULL AND TRIM(manager) != ''
                    AND LOWER(TRIM(manager)) NOT IN ('unrostered')
                """,
                    database="",
                )
                win_pct = int(win_val or 0)
            except Exception:
                win_pct = -1

            # Missing matchup years
            missing_yrs = sorted(set(s_yrs) - set(m_yrs))

            # Get league type info from latest settings
            entry = latest_per_league.get(name)
            league_type = "Unknown"
            scoring_type = "Unknown"
            num_teams = 0
            if entry:
                league_type = classify_league_type(entry["row"])
                scoring_type = classify_scoring_type(entry["row"])
                num_teams = _safe_int(entry["row"].get("num_teams", entry["row"].get("total_rosters", 0)))

            issues = []
            if missing_yrs:
                issues.append(f"MISS_YRS:{missing_yrs}")
            if opt_pct < 50:
                issues.append(f"OPT={opt_pct}%")
            if win_pct >= 0 and win_pct < 50:
                issues.append(f"WIN={win_pct}%")
            if champs == 0 and len(m_yrs) > 0:
                issues.append("NO_CHAMP")
            if sackos == 0 and len(m_yrs) > 0:
                issues.append("NO_SACKO")

            record = {
                "name": name,
                "db": db,
                "platform": platform,
                "years": len(m_yrs),
                "teams": num_teams,
                "league_type": league_type,
                "scoring_type": scoring_type,
                "opt_pct": opt_pct,
                "win_pct": win_pct,
                "champs": champs,
                "sackos": sackos,
                "issues": issues,
                "missing_yrs": missing_yrs,
            }

            if not issues:
                healthy.append(record)
            else:
                unhealthy.append(record)

            # QA candidate: clean data, interesting features, multiple years
            if not issues and len(m_yrs) >= 3 and opt_pct >= 80:
                qa_candidates.append(record)

        except Exception as e:
            err_msg = str(e)[:60]
            unhealthy.append(
                {
                    "name": name,
                    "db": db,
                    "platform": platform,
                    "years": 0,
                    "teams": 0,
                    "league_type": "ERROR",
                    "scoring_type": "",
                    "opt_pct": 0,
                    "win_pct": 0,
                    "champs": 0,
                    "sackos": 0,
                    "issues": [f"ERROR:{err_msg}"],
                    "missing_yrs": [],
                }
            )

    # Print health summary
    print(f"\n  Healthy: {len(healthy)} / {len(leagues)}")
    print(f"  Unhealthy: {len(unhealthy)} / {len(leagues)}")

    if unhealthy:
        print("\n  UNHEALTHY LEAGUES:")
        # Group by issue type
        by_issue = defaultdict(list)
        for r in unhealthy:
            for iss in r["issues"]:
                key = iss.split(":")[0].split("=")[0]
                by_issue[key].append(r["name"])

        for issue_type, names in sorted(by_issue.items(), key=lambda x: -len(x[1])):
            print(f"    {issue_type}: {len(names)} leagues")
            for n in sorted(names)[:5]:
                print(f"      - {n}")
            if len(names) > 5:
                print(f"      ... and {len(names)-5} more")

    # =========================================================================
    # QA LEAGUE RECOMMENDATIONS
    print("\n" + "=" * 70)
    print("QA LEAGUE RECOMMENDATIONS")
    print("=" * 70)

    if not qa_candidates:
        print("  No qualifying QA candidates found (need 3+ years, 80%+ optimal, no issues)")
    else:
        # Categorize QA candidates by what they test
        qa_by_category = {
            "Dynasty (Sleeper)": [],
            "Dynasty (Yahoo)": [],
            "Keeper": [],
            "Redraft": [],
            "H2H + Median": [],
            "Large (12+ teams)": [],
            "Deep History (7+ yrs)": [],
            "IDP": [],
        }

        for r in qa_candidates:
            if r["league_type"] == "Dynasty" and r["platform"] == "Sleeper":
                qa_by_category["Dynasty (Sleeper)"].append(r)
            elif r["league_type"] == "Dynasty" and r["platform"] == "Yahoo":
                qa_by_category["Dynasty (Yahoo)"].append(r)
            elif r["league_type"] == "Keeper":
                qa_by_category["Keeper"].append(r)
            elif r["league_type"] == "Redraft":
                qa_by_category["Redraft"].append(r)

            if r["scoring_type"] == "H2H + Median":
                qa_by_category["H2H + Median"].append(r)
            if r["teams"] >= 12:
                qa_by_category["Large (12+ teams)"].append(r)
            if r["years"] >= 7:
                qa_by_category["Deep History (7+ yrs)"].append(r)

        for category, candidates in qa_by_category.items():
            if not candidates:
                continue
            # Pick best per category (most years, then highest optimal)
            best = sorted(candidates, key=lambda x: (-x["years"], -x["opt_pct"]))[:3]
            print(f"\n  {category}:")
            for r in best:
                safe = r["name"].encode("ascii", errors="replace").decode("ascii")
                print(
                    f"    {safe:35s} {r['platform']:8s} {r['years']}yrs {r['teams']}tm opt={r['opt_pct']}% ch={r['champs']} sk={r['sackos']}  db={r['db']}"
                )

        # Overall best QA league per platform
        print("\n  RECOMMENDED TEST LEAGUES (best overall per platform):")
        for plat in ["Yahoo", "Sleeper", "ESPN"]:
            plat_candidates = [r for r in qa_candidates if r["platform"] == plat]
            if plat_candidates:
                best = sorted(plat_candidates, key=lambda x: (-x["years"], -x["opt_pct"], -x["champs"]))[0]
                safe = best["name"].encode("ascii", errors="replace").decode("ascii")
                print(f"    {plat:8s}: {safe:35s} {best['years']}yrs {best['teams']}tm  db={best['db']}")
            else:
                print(f"    {plat:8s}: (none available)")

    print()
    return 0


if __name__ == "__main__":
    exit(main())
