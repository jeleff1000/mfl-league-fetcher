#!/usr/bin/env python3
"""
Yahoo League Settings Fetcher - Unified Module

Fetches ALL league settings from Yahoo API in a single call and saves to one comprehensive file.
This replaces the previous fragmented approach of multiple files for DST/offense/rules.

What this fetches in ONE API call:
- League metadata (name, size, draft type, etc.)
- Roster positions and requirements
- Scoring rules (offense, defense, kickers)
- Stat categories and modifiers
- Points Allowed buckets for DST
- Waiver rules and trade settings

Output: Single JSON file per year containing all settings
Location: {settings_dir}/league_settings_{year}_{league_key}.json
"""

from __future__ import annotations

import json
import math
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from datetime import datetime

import pandas as pd

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.core.scoring_config import yahoo_stat_modifier_bonus_key

# Import centralized logging
try:
    from multi_league.core.logging_config import get_logger
except ImportError:
    from logging_config import get_logger

logger = get_logger(__name__)

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

try:
    from multi_league.core.league_context import LeagueContext

    LEAGUE_CONTEXT_AVAILABLE = True
except ImportError:
    LeagueContext = None
    LEAGUE_CONTEXT_AVAILABLE = False

try:
    import yahoo_fantasy_api as yfa
except ImportError:
    yfa = None

try:
    from imports_and_utils import OAuth2
except ImportError:
    try:
        from oauth_utils import create_oauth2 as OAuth2
    except ImportError:
        OAuth2 = None


def _fetch_url_xml(url: str, oauth: OAuth2, max_retries: int = 5, backoff: float = 0.5) -> ET.Element:
    """
    Fetch XML from Yahoo API with retries.

    Args:
        url: Yahoo API URL
        oauth: OAuth2 instance
        max_retries: Maximum retry attempts
        backoff: Initial backoff delay (doubled each retry)

    Returns:
        XML Element tree root

    Raises:
        RuntimeError: If fetch fails after retries
    """
    last_err = None
    for i in range(max_retries):
        try:
            r = oauth.session.get(url, timeout=30)
            r.raise_for_status()
            txt = r.text or ""
            if "Request denied" in txt:
                raise RuntimeError("Request denied")
            # Strip default namespace for easier parsing
            txt = pd.Series(txt).str.replace(r' xmlns="[^"]+"', "", n=1, regex=True).iloc[0]
            return ET.fromstring(txt)
        except Exception as e:
            last_err = e
            if i == max_retries - 1:
                raise
            time.sleep(backoff * (2**i))
    raise last_err or RuntimeError("unknown fetch error")


def _discover_league_key(
    oauth: OAuth2,
    year: int,
    league_key_arg: str | None,
    league_name: str | None = None,
    discovered_leagues_file: Path | None = None,
) -> str | None:
    """
    Discover league key from OAuth if not provided.

    Args:
        oauth: OAuth2 instance
        year: Season year
        league_key_arg: Explicit league key (if provided)
        league_name: League name to match against (for multiple leagues)
        discovered_leagues_file: Path to discovered_leagues.json (for fast lookup)

    Returns:
        League key string or None
    """
    if league_key_arg:
        return league_key_arg.strip()

    # Try discovered_leagues.json first (fast path)
    if discovered_leagues_file and discovered_leagues_file.exists() and league_name:
        try:
            discovered = json.loads(discovered_leagues_file.read_text(encoding="utf-8"))
            for entry in discovered:
                if entry.get("year") == year and entry.get("league_name") == league_name:
                    key = entry.get("league_id")
                    if key:
                        logger.debug(f"Found league key for {year} in discovered_leagues.json: {key}")
                        return key.strip()
        except Exception as e:
            logger.warning(f"Could not read discovered_leagues.json: {e}")

    # Fall back to Yahoo API
    if yfa is None:
        return None

    try:
        gm = yfa.Game(oauth, "nfl")

        # If league_name provided, fetch all leagues and match by name
        if league_name:
            try:
                # Get all leagues for this year
                leagues = gm.to_league(str(year))
                if isinstance(leagues, list):
                    for league in leagues:
                        if hasattr(league, "settings") and hasattr(league.settings, "name"):
                            if league.settings.name == league_name:
                                key = league.league_id if hasattr(league, "league_id") else None
                                if key:
                                    logger.debug(f"Matched league by name '{league_name}': {key}")
                                    return key
            except Exception as e:
                logger.debug(f"Name matching via to_league() failed for {year}: {e}")

        # Fallback: just get league IDs (may not be correct if multiple leagues)
        keys = gm.league_ids(year=year)
        if keys:
            if len(keys) > 1 and league_name:
                logger.warning(
                    f"Multiple leagues found for {year} ({len(keys)} leagues), "
                    f"but couldn't match by name '{league_name}'. "
                    f"Using last one: {keys[-1]}. "
                    f"All keys: {keys}. "
                    f"FIX: Run discover_league_history() to populate league_ids in context."
                )
            return keys[-1]
    except Exception:
        return None

    return None


def derive_playoff_round_type(matchup_len: int, has_multiweek: str) -> int:
    """Derive canonical playoff_round_type from Yahoo settings.

    Args:
        matchup_len: playoff_matchup_length (1=single week, 2=two weeks per round)
        has_multiweek: has_multiweek_championship ("0" or "1")

    Returns:
        0 = single-week rounds, 1 = all 2-week rounds, 2 = 2-week finals only
    """
    if matchup_len >= 2:
        return 1
    elif has_multiweek == "1":
        return 2
    else:
        return 0


def _parse_league_metadata(root: ET.Element) -> dict[str, Any]:
    """
    Extract league metadata from settings XML.

    Args:
        root: XML Element tree root

    Returns:
        Dictionary with league metadata
    """
    league = root.find("league")
    if league is None:
        return {}

    # Helper to get text from settings or league node
    def get_val(path: str, default: str = "") -> str:
        # Try settings/path first, then league/path
        val = (league.findtext(f"settings/{path}") or league.findtext(path) or default).strip()
        return val

    # Extract playoff configuration. Some Yahoo leagues explicitly disable
    # playoffs; in that case Yahoo omits the playoff fields instead of
    # returning zeros.
    uses_playoff_raw = get_val("uses_playoff", "1")
    uses_playoff = uses_playoff_raw != "0"
    end_week_raw = get_val("end_week")
    playoff_start_week = get_val("playoff_start_week")
    num_playoff_teams = get_val("num_playoff_teams")
    num_playoff_consolation_teams = get_val("num_playoff_consolation_teams")
    has_multiweek_championship = get_val("has_multiweek_championship", "0")
    uses_playoff_reseeding = get_val("uses_playoff_reseeding", "0")

    if not uses_playoff:
        playoff_start_week = str(int(end_week_raw) + 1) if end_week_raw.isdigit() else ""
        num_playoff_teams = "0"
        num_playoff_consolation_teams = "0"
        has_multiweek_championship = "0"
        uses_playoff_reseeding = "0"

    # Extract playoff_matchup_length (weeks per playoff round)
    playoff_matchup_length_raw = get_val("playoff_matchup_length", "1")
    matchup_len = int(playoff_matchup_length_raw) if playoff_matchup_length_raw.isdigit() else 1

    # Derive canonical playoff_round_type
    playoff_round_type = derive_playoff_round_type(matchup_len, has_multiweek_championship)

    # Derive uses_reseeding as int
    uses_reseeding = int(uses_playoff_reseeding) if uses_playoff_reseeding.isdigit() else 0

    # Read bye_teams from Yahoo API (don't calculate - Yahoo already provides this)
    bye_teams_raw = get_val("bye_teams")
    bye_teams = int(bye_teams_raw) if bye_teams_raw.isdigit() else 0

    # Only use fallback calculation if Yahoo doesn't provide bye_teams
    # Use formula matching Sleeper: bye_teams = next_power_of_2 - num_playoff_teams
    num_playoff_teams_int = int(num_playoff_teams) if num_playoff_teams.isdigit() else 0
    if bye_teams == 0:
        if num_playoff_teams_int > 1:
            next_power_of_2 = 2 ** math.ceil(math.log2(num_playoff_teams_int))
            bye_teams = next_power_of_2 - num_playoff_teams_int

    # Derive num_rounds and championship_week
    if not uses_playoff:
        num_rounds = 0
    elif num_playoff_teams_int > 1:
        num_rounds = math.ceil(math.log2(num_playoff_teams_int))
    else:
        num_rounds = 1

    start = int(playoff_start_week) if playoff_start_week.isdigit() else 15
    if not uses_playoff:
        championship_week = None
    elif playoff_round_type == 0:
        championship_week = start + num_rounds - 1
    elif playoff_round_type == 1:
        championship_week = start + (2 * num_rounds) - 1
    else:
        championship_week = start + num_rounds

    # H2H + Median scoring: Yahoo exposes as uses_median_score (0 or 1)
    uses_median_score_raw = get_val("uses_median_score", "0")
    uses_median_score = uses_median_score_raw == "1"

    # Yahoo scoring SEMANTICS, not just the per-stat rate: when this is 0 the
    # league scored in whole-point buckets (each stat contribution floored, e.g.
    # 1 pt per 50 passing yards), which is how Yahoo ran before it switched to
    # decimal scoring (~2014). The per-yard modifier in stat_modifiers is the
    # same in both eras, so this flag is the ONLY thing that distinguishes a
    # floored ruleset from a linear one. Capturing it lets the recompute
    # reproduce the API points exactly for every era. Default to fractional (1)
    # when absent — the modern norm.
    uses_fractional_points_raw = get_val("uses_fractional_points", "1")
    uses_fractional_points = uses_fractional_points_raw != "0"

    uses_faab_raw = get_val("uses_faab")

    metadata = {
        "league_key": get_val("league_key"),
        "league_id": get_val("league_id"),
        "name": get_val("name"),
        "season": get_val("season"),
        "num_teams": get_val("num_teams"),
        "draft_type": get_val("draft_type"),
        # predraft = a renewed/created shell whose season never happened;
        # corpus discovery uses this to skip unplayed league-years.
        "draft_status": get_val("draft_status"),
        "is_finished": get_val("is_finished"),
        "scoring_type": get_val("scoring_type"),
        "uses_fractional_points": uses_fractional_points,
        "uses_median_score": uses_median_score,
        "league_type": get_val("league_type"),
        "renew": get_val("renew"),
        "renewed": get_val("renewed"),
        "start_week": get_val("start_week"),
        "start_date": get_val("start_date"),
        "end_week": get_val("end_week"),
        "end_date": get_val("end_date"),
        "current_week": get_val("current_week"),
        # Waiver/trade configuration. Yahoo exposes these in the settings
        # XML even for very old leagues; canonical_settings maps only the
        # fields whose semantics match our flat DDL.
        "waiver_type": get_val("waiver_type"),
        "waiver_rule": get_val("waiver_rule"),
        "uses_faab": uses_faab_raw,
        "waiver_time": get_val("waiver_time"),
        "trade_end_date": get_val("trade_end_date"),
        "trade_ratify_type": get_val("trade_ratify_type"),
        "trade_reject_time": get_val("trade_reject_time"),
        # Playoff configuration settings
        "uses_playoff": uses_playoff,
        "playoff_start_week": playoff_start_week,
        "num_playoff_teams": num_playoff_teams,
        "num_playoff_consolation_teams": num_playoff_consolation_teams,
        "has_multiweek_championship": has_multiweek_championship,
        "uses_playoff_reseeding": uses_playoff_reseeding,
        # Calculated fields for easier consumption
        "playoff_teams": num_playoff_teams_int if num_playoff_teams_int > 0 else None,
        "bye_teams": bye_teams,
        # Canonical playoff fields (cross-platform)
        "playoff_round_type": playoff_round_type,
        "uses_reseeding": uses_reseeding,
        "playoff_matchup_length": matchup_len,
        "num_rounds": num_rounds,
        "championship_week": championship_week,
    }

    return metadata


def _parse_roster_positions(root: ET.Element) -> list[dict[str, Any]]:
    """
    Extract roster position requirements from settings XML.

    Args:
        root: XML Element tree root

    Returns:
        List of roster positions with counts
    """
    positions = []
    for pos in root.findall("league/settings/roster_positions/roster_position"):
        position_type = (pos.findtext("position") or "").strip()
        count = (pos.findtext("count") or "0").strip()
        positions.append({"position": position_type, "count": int(count) if count.isdigit() else 0})

    return positions


def _parse_stat_categories(root: ET.Element) -> dict[str, dict[str, Any]]:
    """
    Build a comprehensive map of all stat categories.

    Args:
        root: XML Element tree root

    Returns:
        Dictionary mapping stat_id to stat details
    """
    stat_map = {}

    for stat in root.findall("league/settings/stat_categories/stats/stat"):
        stat_id = (stat.findtext("stat_id") or "").strip()
        if not stat_id:
            continue

        stat_info = {
            "stat_id": stat_id,
            "enabled": (stat.findtext("enabled") or "1").strip() == "1",
            "name": (stat.findtext("name") or "").strip(),
            "display_name": (stat.findtext("display_name") or "").strip(),
            "sort_order": (stat.findtext("sort_order") or "").strip(),
            "position_type": (stat.findtext("position_type") or "").strip(),
            "stat_position_types": [],
            "is_only_display_stat": (stat.findtext("is_only_display_stat") or "0").strip() == "1",
        }

        # Get position types this stat applies to
        for pos_type in stat.findall("stat_position_types/stat_position_type"):
            position = (pos_type.findtext("position_type") or "").strip()
            if position:
                stat_info["stat_position_types"].append(position)

        # Get buckets if they exist (for Points Allowed, etc.)
        buckets = []
        for bucket in stat.findall("stat_buckets/stat_bucket"):
            start = (bucket.findtext("range/start") or "").strip()
            end = (bucket.findtext("range/end") or "").strip()
            maxv = (bucket.findtext("range/max") or "").strip()
            points = (bucket.findtext("points") or bucket.findtext("value") or "0").strip()

            if start and end:
                rng = f"{start}-{end}"
            elif start and maxv:
                rng = f"{start}-{maxv}"
            else:
                rng = start or maxv or ""

            try:
                points_val = float(points)
            except Exception:
                points_val = 0.0

            buckets.append({"range": rng.replace(" ", ""), "points": points_val})

        if buckets:
            stat_info["buckets"] = buckets

        stat_map[stat_id] = stat_info

    return stat_map


def _parse_stat_modifiers(root: ET.Element) -> dict[str, float]:
    """
    Extract point values for each stat from modifiers section.

    Args:
        root: XML Element tree root

    Returns:
        Dictionary mapping stat_id to point value
    """
    modifiers = {}

    for mod in root.findall("league/settings/stat_modifiers/stats/stat"):
        stat_id = (mod.findtext("stat_id") or "").strip()
        value = (mod.findtext("value") or "0").strip()

        if stat_id:
            try:
                modifiers[stat_id] = float(value)
            except Exception:
                modifiers[stat_id] = 0.0

    return modifiers


def _canonical_bonus_key(stat_id: str, target: int) -> str | None:
    """Map Yahoo yardage/completion bonus modifiers to canonical DDL keys."""
    return yahoo_stat_modifier_bonus_key(stat_id, target)


def _parse_stat_modifier_bonuses(
    root: ET.Element,
    stat_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Extract Yahoo's nested stat modifier bonuses as scoring rules.

    Yahoo exposes milestone bonuses as children of stat_modifiers entries,
    not as separate stat categories. Example:

        <stat>
          <stat_id>4</stat_id>
          <value>0.04</value>
          <bonuses><bonus><target>300</target><points>5</points></bonus></bonuses>
        </stat>

    These need to flow through canonical_scoring like ordinary rules.
    """
    rules: list[dict[str, Any]] = []
    for mod in root.findall("league/settings/stat_modifiers/stats/stat"):
        stat_id = (mod.findtext("stat_id") or "").strip()
        if not stat_id:
            continue
        stat_info = stat_map.get(stat_id, {})
        for bonus in mod.findall("bonuses/bonus"):
            raw_target = (bonus.findtext("target") or "").strip()
            raw_points = (bonus.findtext("points") or bonus.findtext("value") or "").strip()
            if not raw_target or not raw_points:
                continue
            try:
                target = int(float(raw_target))
                points = float(raw_points)
            except (TypeError, ValueError):
                continue
            canonical_key = _canonical_bonus_key(stat_id, target)
            if not canonical_key:
                logger.warning(
                    "Unmapped Yahoo stat modifier bonus: stat_id=%s target=%s points=%s",
                    stat_id,
                    target,
                    points,
                )
                continue
            rules.append(
                {
                    "stat_id": f"{stat_id}:bonus:{target}",
                    "name": f"{stat_info.get('display_name') or stat_info.get('name') or stat_id} {target}+ Bonus",
                    "position_types": stat_info.get("stat_position_types", []),
                    "points": points,
                    "bonus_target": target,
                    "bonus_base_stat_id": stat_id,
                    "canonical_key": canonical_key,
                }
            )
    return rules


def _build_scoring_rules(stat_map: dict[str, dict[str, Any]], modifiers: dict[str, float]) -> list[dict[str, Any]]:
    """
    Combine stat categories and modifiers into a unified scoring rules list.

    Args:
        stat_map: Map of stat details
        modifiers: Map of stat point values

    Returns:
        List of scoring rules with all details
    """
    rules = []

    for stat_id, stat_info in stat_map.items():
        # Skip disabled stats
        if not stat_info.get("enabled", True):
            continue

        # Skip display-only stats
        if stat_info.get("is_only_display_stat", False):
            continue

        base_rule = {
            "stat_id": stat_id,
            "name": stat_info.get("display_name") or stat_info.get("name") or stat_id,
            # Yahoo's display_name is usually abbreviated ("Rec", "Pass TD").
            # Preserve the canonical long name for cross-platform key mapping.
            "canonical_name": stat_info.get("name") or stat_info.get("display_name") or stat_id,
            "position_types": stat_info.get("stat_position_types", []),
        }

        # Handle bucketed stats (like Points Allowed)
        if "buckets" in stat_info:
            for bucket in stat_info["buckets"]:
                rule = base_rule.copy()
                rule["bucket_range"] = bucket["range"]
                rule["points"] = bucket["points"]
                rules.append(rule)
        # Handle regular stats with modifiers
        elif stat_id in modifiers:
            rule = base_rule.copy()
            rule["points"] = modifiers[stat_id]
            rules.append(rule)

    return rules


def _extract_dst_scoring(scoring_rules: list[dict[str, Any]]) -> dict[str, float]:
    """
    Extract DST-specific scoring into a simple dictionary for backwards compatibility.

    Args:
        scoring_rules: Full list of scoring rules

    Returns:
        Dictionary of DST stat names to point values
    """
    dst_scoring = {}

    # DST stat names to extract
    dst_stats = {
        "Sack",
        "Interception",
        "Fumble Recovery",
        "Touchdown",
        "Safety",
        "Kickoff and Punt Return Touchdowns",
        "Blocked Punt or FG",
        "Block Kick",
    }

    # Points Allowed buckets
    pa_buckets = {}

    for rule in scoring_rules:
        name = rule.get("name", "")

        # Regular DST stats
        if name in dst_stats:
            dst_scoring[name] = rule.get("points", 0.0)

        # Points Allowed buckets
        if "points allowed" in name.lower() and "bucket_range" in rule:
            rng = rule["bucket_range"]
            points = rule.get("points", 0.0)

            # Map ranges to standard keys
            if rng in ("0", "0-0"):
                pa_buckets["PA_0"] = points
            elif rng == "1-6":
                pa_buckets["PA_1_6"] = points
            elif rng == "7-13":
                pa_buckets["PA_7_13"] = points
            elif rng == "14-20":
                pa_buckets["PA_14_20"] = points
            elif rng == "21-27":
                pa_buckets["PA_21_27"] = points
            elif rng == "28-34":
                pa_buckets["PA_28_34"] = points
            elif rng in ("35+", "35-", "35"):
                pa_buckets["PA_35_plus"] = points

    # Set defaults for missing PA buckets
    for key in ["PA_0", "PA_1_6", "PA_7_13", "PA_14_20", "PA_21_27", "PA_28_34", "PA_35_plus"]:
        pa_buckets.setdefault(key, 0.0)

    # Combine
    dst_scoring.update(pa_buckets)

    return dst_scoring


def _build_scoring_settings_dict(scoring_rules: list[dict[str, Any]]) -> dict[str, float]:
    """
    Convert Yahoo scoring_rules list to Sleeper-style scoring_settings dict.

    This enables cross-platform compatibility for sql_enrichments.py which
    expects scoring_settings dict with keys like 'rec', 'pass_td', etc.

    Args:
        scoring_rules: List of scoring rules from _build_scoring_rules()

    Returns:
        Dictionary mapping Sleeper-style keys to point values
    """
    scoring_settings = {}

    # Map Yahoo stat names to Sleeper keys
    key_mapping = {
        "Passing Yards": "pass_yd",
        "Passing Touchdowns": "pass_td",
        "Interception": "pass_int",
        "Interceptions": "pass_int",
        "Interceptions Thrown": "pass_int",
        "Rushing Yards": "rush_yd",
        "Rushing Touchdowns": "rush_td",
        "Receiving Yards": "rec_yd",
        "Receiving Touchdowns": "rec_td",
        "Reception": "rec",
        "Receptions": "rec",
        "Fumbles Lost": "fum_lost",
        "Fumble": "fum",
        "2-Point Conversions": "pass_2pt",
        "Return Touchdowns": "st_td",
        "Return Yards": "st_yds",
    }

    for rule in scoring_rules:
        if not isinstance(rule, dict):
            continue
        canonical_key = rule.get("canonical_key")
        name = rule.get("name", "")
        canonical_name = rule.get("canonical_name", name)
        points = rule.get("points", 0)

        # Skip bucketed stats (like Points Allowed) - they don't map to simple keys
        if "bucket_range" in rule:
            continue

        sleeper_key = canonical_key or key_mapping.get(canonical_name) or key_mapping.get(name)
        if sleeper_key and points:
            scoring_settings[sleeper_key] = points

        if name == "2-Point Conversions" and points:
            scoring_settings["pass_2pt"] = points
            scoring_settings["rush_2pt"] = points
            scoring_settings["rec_2pt"] = points

    return scoring_settings


def _build_roster_position_counts(roster_positions: list[dict[str, Any]]) -> dict[str, int]:
    """
    Convert Yahoo roster_positions list to counts dict for cross-platform compatibility.

    Args:
        roster_positions: List like [{"position": "QB", "count": 1}, ...]

    Returns:
        Dict like {"QB": 1, "RB": 2, "WR": 2, ...}
    """
    counts = {}
    for pos_info in roster_positions:
        pos = pos_info.get("position", "")
        count = pos_info.get("count", 1)
        if pos:
            counts[pos] = int(count) if isinstance(count, (int, float, str)) else 1
    return counts


def find_settings_file_for_year(settings_dir: Path, year: int) -> Path | None:
    """
    Find settings file for a given year, regardless of league_key in filename.

    Settings files are named: league_settings_{year}_{league_key}.json
    But league_key changes every year for the same league, so we search by year pattern only.

    Args:
        settings_dir: Directory containing settings files
        year: Season year to find

    Returns:
        Path to settings file if found, None otherwise

    Example:
        >>> find_settings_file_for_year(Path("settings"), 2024)
        Path("settings/league_settings_2024_449.l.198278.json")
    """
    if not settings_dir.exists():
        logger.debug(f"Settings directory not found: {settings_dir}")
        return None

    # Search for any file matching league_settings_{year}_*.json
    pattern = f"league_settings_{year}_*.json"
    matches = list(settings_dir.glob(pattern))

    if not matches:
        logger.debug(f"No settings file found for year {year} in {settings_dir}")
        return None

    if len(matches) > 1:
        logger.warning(f"Multiple settings files found for year {year}: {[m.name for m in matches]}")
        logger.warning(f"Using first match: {matches[0].name}")

    logger.debug(f"Found settings file for {year}: {matches[0].name}")
    return matches[0]


def load_settings_for_year(settings_dir: Path, year: int) -> dict[str, Any] | None:
    """
    Load settings from file for a given year.

    BUG FIX #7: This function finds settings files by year pattern only,
    not requiring the exact league_key. This fixes the "settings not found"
    issue when league_key changes between years.

    Args:
        settings_dir: Directory containing settings files
        year: Season year to load

    Returns:
        Dictionary with settings data, or None if not found

    Example:
        >>> settings = load_settings_for_year(Path("settings"), 2024)
        >>> print(settings['metadata']['playoff_start_week'])
        14
    """
    settings_file = find_settings_file_for_year(settings_dir, year)

    if not settings_file:
        return None

    try:
        with open(settings_file, encoding="utf-8") as f:
            settings = json.load(f)
        logger.debug(f"Loaded settings for year {year} from {settings_file.name}")
        return settings
    except Exception as e:
        logger.error(f"Error loading settings from {settings_file}: {e}")
        return None


def fetch_league_settings(
    year: int,
    league_key: str | None = None,
    oauth_file: Path | None = None,
    settings_dir: Path | None = None,
    context: str | None = None,
    oauth: OAuth2 | None = None,
) -> dict[str, Any] | None:
    """
    Fetch ALL league settings from Yahoo API in a single call.

    BUG FIX #7: Before fetching from Yahoo API, checks if settings already exist
    for this year using the new load_settings_for_year() function, which finds
    settings files by year pattern regardless of league_key.

    This function makes ONE API request and parses ALL settings:
    - League metadata
    - Roster positions
    - All scoring rules (offense, defense, special teams)
    - Stat categories
    - Waiver/trade settings

    Args:
        year: Season year
        league_key: League key (e.g., "449.l.198278"), auto-discovered if None
        oauth_file: Path to OAuth credentials file
        settings_dir: Directory to save settings JSON
        context: Path to league_context.json (alternative to individual params)
        oauth: Pre-created OAuth2 session (avoids race conditions in parallel calls)

    Returns:
        Dictionary containing all league settings, or None if fetch fails

    Example output structure:
        {
            "fetched_at": "2025-01-01T12:00:00",
            "year": 2024,
            "league_key": "449.l.198278",
            "metadata": {...},
            "roster_positions": [...],
            "scoring_rules": [...],
            "dst_scoring": {...},  # Backwards compatibility
        }
    """
    # Load from context if provided
    ctx = None
    league_name = None
    discovered_leagues_file = None

    if context and LEAGUE_CONTEXT_AVAILABLE:
        try:
            ctx = LeagueContext.load(context)
            year = year or ctx.start_year
            league_name = ctx.league_name  # Get league name for matching

            # Don't use ctx.league_id - let auto-discovery find the correct year-specific key
            # league_key = league_key or ctx.league_id  # REMOVED

            if ctx.oauth_file_path:
                oauth_file = oauth_file or Path(ctx.oauth_file_path)
            settings_dir = (
                settings_dir or Path(ctx.data_directory) / "league_settings"
            )  # League-wide config, not player-specific

            # Look for discovered_leagues.json in the parent data directory
            data_parent = Path(ctx.data_directory).parent
            discovered_leagues_file = data_parent / "discovered_leagues.json"

        except Exception as e:
            logger.warning(f"Could not load context: {e}")

    # Use provided OAuth session or create one
    if oauth is None:
        try:
            from yahoo_oauth import OAuth2
        except ImportError:
            logger.error("yahoo_oauth not installed. Install with: pip install yahoo_oauth")
            return None

        if oauth_file and oauth_file.exists():
            try:
                oauth = OAuth2(None, None, from_file=str(oauth_file))
            except Exception as e:
                logger.error(f"Error creating OAuth from file: {e}")
        elif ctx and ctx.oauth_credentials:
            try:
                import tempfile

                temp_fd, temp_path = tempfile.mkstemp(suffix=".json", text=True)
                try:
                    with os.fdopen(temp_fd, "w") as f:
                        json.dump(ctx.oauth_credentials, f, indent=2)
                    oauth = OAuth2(None, None, from_file=temp_path)
                    Path(temp_path).unlink(missing_ok=True)
                except Exception as e:
                    os.close(temp_fd)
                    Path(temp_path).unlink(missing_ok=True)
                    raise e
            except Exception as e:
                logger.error(f"Error creating OAuth from context: {e}")

        if not oauth:
            logger.error("Could not create OAuth session")
            return None

    # Discover league key if not provided
    discovered_key = _discover_league_key(
        oauth, year, league_key, league_name=league_name, discovered_leagues_file=discovered_leagues_file
    )
    if not discovered_key:
        logger.error(f"Could not determine league key for {year}")
        return None

    league_key = discovered_key
    logger.info(f"Fetching ALL settings for {league_key} (year {year})...")

    # SINGLE API CALL to get everything
    try:
        root = _fetch_url_xml(f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings", oauth)
    except Exception as e:
        logger.error(f"Error fetching settings: {e}")
        return None

    # Parse all components
    logger.debug("Parsing settings components...")
    metadata = _parse_league_metadata(root)
    roster_positions = _parse_roster_positions(root)
    stat_map = _parse_stat_categories(root)
    modifiers = _parse_stat_modifiers(root)
    scoring_rules = _build_scoring_rules(stat_map, modifiers)
    scoring_rules.extend(_parse_stat_modifier_bonuses(root, stat_map))
    dst_scoring = _extract_dst_scoring(scoring_rules)

    # Build roster_position_counts for cross-platform compatibility with Sleeper
    roster_position_counts = _build_roster_position_counts(roster_positions)

    # Build scoring_settings dict for cross-platform compatibility
    scoring_settings = _build_scoring_settings_dict(scoring_rules)

    # Build comprehensive settings object
    settings = {
        "fetched_at": datetime.now().isoformat(),
        "year": year,
        "league_key": league_key,
        "metadata": metadata,
        "roster_positions": roster_positions,
        "roster_position_counts": roster_position_counts,  # Cross-platform: pre-computed counts
        "scoring_rules": scoring_rules,
        "scoring_settings": scoring_settings,  # Cross-platform: Sleeper-style scoring dict
        "dst_scoring": dst_scoring,  # Backwards compatibility
        "stat_categories": stat_map,
        "stat_modifiers": modifiers,
        # PROMOTED from metadata for cross-platform consistency (fixes playoff detection)
        "playoff_start_week": metadata.get("playoff_start_week"),
        "num_playoff_teams": metadata.get("num_playoff_teams"),
        "bye_teams": metadata.get("bye_teams", 0),
        "end_week": metadata.get("end_week"),
        "has_multiweek_championship": metadata.get("has_multiweek_championship", "0"),
    }

    # Normalize scoring to canonical Sleeper-key format
    try:
        from multi_league.core.scoring_config import normalize_yahoo

        settings["canonical_scoring"] = normalize_yahoo(settings.get("scoring_rules", []))
    except Exception:
        pass  # Non-fatal: canonical_scoring can be backfilled during enrichments

    return settings


def load_league_settings(
    year: int, league_key: str | None = None, settings_dir: Path | None = None, context: str | None = None
) -> dict[str, Any] | None:
    """
    Load previously saved league settings from file.

    Args:
        year: Season year
        league_key: League key (used to find specific file)
        settings_dir: Directory containing settings files
        context: Path to league_context.json (alternative)

    Returns:
        Dictionary containing all league settings, or None if not found
    """
    # Load from context if provided
    if context and LEAGUE_CONTEXT_AVAILABLE:
        try:
            ctx = LeagueContext.load(context)
            league_key = league_key or ctx.league_id
            settings_dir = settings_dir or Path(ctx.data_directory) / "league_settings"  # League-wide config
        except Exception:  # noqa: broad-except
            pass
    if not settings_dir or not settings_dir.exists():
        return None

    # Find the settings file
    candidates: list[Path] = []

    if league_key:
        safe_key = league_key.replace(".", "_")
        specific_file = settings_dir / f"league_settings_{year}_{safe_key}.json"
        if specific_file.exists():
            candidates.append(specific_file)

    # Fallback: find any settings file for this year
    if not candidates:
        candidates = sorted(
            settings_dir.glob(f"league_settings_{year}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )

    if not candidates:
        logger.debug(f"No saved settings found for year {year}")
        return None

    # Load the most recent file
    try:
        settings = json.loads(candidates[0].read_text(encoding="utf-8"))
        logger.info(f"Loaded settings from {candidates[0].name}")
        return settings
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
        return None


# Backwards compatibility functions (for existing code that expects the old API)


def parse_scoring_rules(settings_root: ET.Element) -> list[dict[str, Any]]:
    """
    DEPRECATED: Use fetch_league_settings() instead.

    Parse scoring rules from XML (old API for backwards compatibility).
    """
    logger.warning("parse_scoring_rules() is deprecated. Use fetch_league_settings() instead.")

    stat_map = _parse_stat_categories(settings_root)
    modifiers = _parse_stat_modifiers(settings_root)
    return _build_scoring_rules(stat_map, modifiers)


def fetch_yahoo_dst_scoring(
    year: int, league_key_arg: str | None, oauth_file: Path | None = None, settings_dir: Path | None = None
) -> dict[str, float] | None:
    """
    DEPRECATED: Use fetch_league_settings() instead.

    Fetch only DST scoring (old API for backwards compatibility).
    """
    logger.warning("fetch_yahoo_dst_scoring() is deprecated. Use fetch_league_settings() instead.")

    full_settings = fetch_league_settings(year, league_key_arg, oauth_file, settings_dir)
    if full_settings:
        return full_settings.get("dst_scoring")
    return None


def discover_league_history(
    league_key: str,
    oauth_file: Path | None = None,
    oauth: OAuth2 | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict[str, str]:
    """
    Discover all league IDs for a league by following the renew/renewed chain.

    Yahoo creates a new league_key each season. The settings contain:
    - 'renew': points to previous season's league (e.g., "449_198278")
    - 'renewed': points to next season's league

    This function follows these links to build a complete year -> league_id mapping,
    ensuring we always fetch from the correct league for each year.

    Args:
        league_key: Current (or any) Yahoo league_key (e.g., "449.l.198278")
        oauth_file: Path to OAuth JSON file
        oauth: Pre-existing OAuth2 session (alternative to oauth_file)
        start_year: Earliest year to include (optional, will go back as far as possible)
        end_year: Latest year to include (optional, defaults to current year)

    Returns:
        Dictionary mapping year (as string) to league_key
        e.g., {"2015": "359.l.123456", "2016": "380.l.234567", ...}

    Example:
        # Discover all KMFFL league IDs
        league_ids = discover_league_history("449.l.198278", oauth_file=Path("oauth.json"))
        # Returns: {"2015": "359.l.12345", "2016": "380.l.23456", "2017": "390.l.34567", ...}
    """
    logger.info(f"Discovering league history starting from {league_key}...")

    # Get OAuth session
    if oauth is None:
        if oauth_file is None:
            logger.error("OAuth file or session required")
            return {}
        try:
            from yahoo_oauth import OAuth2 as YahooOAuth2

            oauth = YahooOAuth2(None, None, from_file=str(oauth_file))
            if not oauth.token_is_valid():
                oauth.refresh_access_token()
        except Exception as e:
            logger.error(f"Error creating OAuth session: {e}")
            return {}

    current_year = get_current_nfl_season_year()
    if end_year is None:
        end_year = current_year

    league_ids: dict[str, str] = {}
    visited: set = set()

    def _parse_renew_key(renew_str: str) -> str | None:
        """Convert renew format (e.g., '449_198278') to league_key ('449.l.198278')."""
        if not renew_str:
            return None
        # Format: "game_id_league_num" -> "game_id.l.league_num"
        parts = renew_str.split("_")
        if len(parts) >= 2:
            game_id = parts[0]
            league_num = "_".join(parts[1:])  # Handle edge case of underscores in league_num
            return f"{game_id}.l.{league_num}"
        return None

    def _fetch_league_settings_minimal(lkey: str) -> dict[str, Any] | None:
        """Fetch just metadata (season, renew, renewed) for a league key."""
        try:
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{lkey}/settings"
            root = _fetch_url_xml(url, oauth)
            metadata = _parse_league_metadata(root)
            return metadata
        except Exception as e:
            logger.warning(f"Could not fetch {lkey}: {e}")
            return None

    # Start with the provided league key
    keys_to_process = [league_key]

    while keys_to_process:
        current_key = keys_to_process.pop(0)

        if current_key in visited:
            continue
        visited.add(current_key)

        logger.debug(f"Checking {current_key}...")
        metadata = _fetch_league_settings_minimal(current_key)

        if not metadata:
            continue

        # Get the season year
        season_str = metadata.get("season", "")
        if not season_str:
            continue

        try:
            season_year = int(season_str)
        except ValueError:
            continue

        # Check if this year is within our range
        if start_year and season_year < start_year:
            # Don't record, but still follow 'renewed' to find future years
            pass
        elif season_year > end_year:
            # Don't record, but still follow 'renew' to find past years
            pass
        else:
            # Record this mapping
            league_ids[str(season_year)] = current_key
            logger.debug(f"-> {season_year}: {current_key} ({metadata.get('name', 'Unknown')})")

        # Follow the 'renew' link to previous season
        renew_raw = metadata.get("renew", "")
        if renew_raw:
            prev_key = _parse_renew_key(renew_raw)
            if prev_key and prev_key not in visited:
                keys_to_process.append(prev_key)

        # Follow the 'renewed' link to next season
        renewed_raw = metadata.get("renewed", "")
        if renewed_raw:
            next_key = _parse_renew_key(renewed_raw)
            if next_key and next_key not in visited:
                keys_to_process.append(next_key)

    # Sort by year for nice output
    league_ids = dict(sorted(league_ids.items(), key=lambda x: int(x[0])))

    logger.info(f"Discovered {len(league_ids)} league IDs:")
    for yr, lid in league_ids.items():
        logger.debug(f"  {yr}: {lid}")

    return league_ids


if __name__ == "__main__":
    """
    CLI usage for testing:

    python yahoo_league_settings.py --year 2024 --league-key 449.l.198278 --oauth path/to/oauth.json --output ./settings
    """
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Yahoo league settings")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--league-key", help="League key (e.g., 449.l.198278)")
    parser.add_argument("--oauth", type=Path, help="Path to OAuth JSON file")
    parser.add_argument("--output", type=Path, help="Output directory for settings")
    parser.add_argument("--context", help="Path to league_context.json")

    args = parser.parse_args()

    settings = fetch_league_settings(
        year=args.year,
        league_key=args.league_key,
        oauth_file=args.oauth,
        settings_dir=args.output,
        context=args.context,
    )

    if settings:
        logger.info("Successfully fetched league settings!")
        logger.info(f"  League: {settings['metadata'].get('name', 'Unknown')}")
        logger.info(f"  Teams: {settings['metadata'].get('num_teams', '?')}")
        logger.info(f"  Scoring: {settings['metadata'].get('scoring_type', 'Unknown')}")
        logger.info(f"  Rules: {len(settings['scoring_rules'])} scoring rules")
    else:
        logger.error("Failed to fetch league settings")
        exit(1)
