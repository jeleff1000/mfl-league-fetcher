#!/usr/bin/env python3
"""
Weekly Matchup Data V2 - Multi-League Edition

Fetches weekly matchup data from Yahoo Fantasy Football API including:
- Manager scores and opponents
- Win/loss records
- Projection accuracy
- Team metadata (FAAB, moves, trades, grades, etc.)

NOTE: Head-to-head records (w_vs_X, l_vs_X) and playoff flags are NOT calculated here.
They are added by the transformation pipeline (cumulative_stats_v2.py) to maintain
separation of concerns between data fetching and transformation.

Key improvements over V1:
- Multi-league support via LeagueContext
- Manager name overrides from context (no hardcoded names)
- League-specific output paths
- RunLogger integration for structured logging
- Modular helper functions
- Better error handling

Usage:
    # With LeagueContext
    from multi_league.core.league_context import LeagueContext
    ctx = LeagueContext.load("leagues/kmffl/league_context.json")
    df = weekly_matchup_data(ctx, year=2024, week=5)

    # Standalone (backward compatible)
    df = weekly_matchup_data(year=2024, week=5, oauth_file=Path("Oauth.json"))

    # CLI with context
    python weekly_matchup_data_v2.py --context leagues/kmffl/league_context.json --year 2024 --week 5

    # CLI standalone
    python weekly_matchup_data_v2.py --year 2024 --week 5 --oauth Oauth.json
"""

from __future__ import annotations

import sys
import os
import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from multi_league.core.date_utils import get_current_nfl_season_year

import numpy as np
import pandas as pd
import time

try:
    import yahoo_fantasy_api as yfa

    YFA_AVAILABLE = True
except ImportError:
    YFA_AVAILABLE = False
    yfa = None

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

# Multi-league infrastructure
try:
    from core.league_context import LeagueContext

    LEAGUE_CONTEXT_AVAILABLE = True
except ImportError:
    LeagueContext = None
    LEAGUE_CONTEXT_AVAILABLE = False

try:
    from core.run_metadata import RunLogger

    RUN_LOGGER_AVAILABLE = True
except ImportError:
    RunLogger = None
    RUN_LOGGER_AVAILABLE = False

try:
    from oauth_utils import create_oauth2
except ImportError:
    create_oauth2 = None

# Import shared norm_manager function
try:
    from ..shared.clean_names import norm_manager
except ImportError:
    from multi_league.data_fetchers.shared.clean_names import norm_manager

try:
    from ...shared.yahoo_identity import recover_yahoo_team_key
except ImportError:
    from multi_league.shared.yahoo_identity import recover_yahoo_team_key

# Default paths (for standalone mode)
THIS_FILE = Path(__file__).resolve()
SCRIPT_ROOT = (
    THIS_FILE.parent.parent.parent.parent
)  # Back to scripts root (yahoo -> data_fetchers -> multi_league -> scripts)
DEFAULT_DATA_ROOT = SCRIPT_ROOT.parent / "fantasy_football_data" / "matchup_data"

# GPA scale for matchup grades
# GPA_SCALE removed — GPA now computed by SQL enrichment (compute_win_loss_and_projections)


# =============================================================================
# Helper Functions
# =============================================================================

# norm_manager is imported from clean_names.py (single source of truth)


from multi_league.data_fetchers.shared.name_utils import safe_float


def _first_text(node: ET.Element, paths: list[str]) -> str:
    """Get first non-empty text from list of XML paths."""
    for p in paths:
        v = node.findtext(p)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _first_float(node: ET.Element, paths: list[str]) -> float:
    """Get first valid float from list of XML paths."""
    for p in paths:
        v = node.findtext(p)
        fv = safe_float(v, np.nan)
        if not (isinstance(fv, float) and np.isnan(fv)):
            return fv
    return np.nan


def derive_playoff_structure(settings: dict[str, Any]) -> dict[str, Any]:
    """
    Derive playoff structure from league settings.

    Args:
        settings: League settings dictionary

    Returns:
        Dictionary with playoff structure details
    """
    start_week = int(settings.get("start_week") or 1)
    end_week = int(settings.get("end_week") or 17)
    playoff_start_week = int(settings.get("playoff_start_week") or max(end_week - 2, start_week))
    matchup_len = int(settings.get("playoff_matchup_length") or 1)
    num_teams = int(settings.get("num_playoff_teams") or 6)

    if num_teams <= 4:
        rounds = 2
    elif num_teams in (6, 8):
        rounds = 3
    else:
        rounds = 3 if num_teams >= 6 else 2

    qf_weeks, sf_weeks, final_weeks = [], [], []
    wk = playoff_start_week
    if rounds == 3:
        qf_weeks = list(range(wk, wk + matchup_len))
        wk += matchup_len
        sf_weeks = list(range(wk, wk + matchup_len))
        wk += matchup_len
        final_weeks = list(range(wk, wk + matchup_len))
    else:
        sf_weeks = list(range(wk, wk + matchup_len))
        wk += matchup_len
        final_weeks = list(range(wk, wk + matchup_len))

    champion_week = final_weeks[-1] if final_weeks else end_week

    return {
        "start_week": start_week,
        "end_week": end_week,
        "playoff_start_week": playoff_start_week,
        "matchup_len": matchup_len,
        "num_playoff_teams": num_teams,
        "rounds": rounds,
        "qf_weeks": qf_weeks,
        "sf_weeks": sf_weeks,
        "final_weeks": final_weeks,
        "champion_week": champion_week,
    }


def get_weeks_to_fetch(league, year: int, week_input: int | None, settings: dict[str, Any]) -> list[int]:
    """
    Determine which weeks to fetch.

    Args:
        league: Yahoo league object
        year: Season year
        week_input: Specific week (None or 0 = all weeks)
        settings: League settings

    Returns:
        List of week numbers to fetch
    """
    s = derive_playoff_structure(settings)
    start_week, end_week = s["start_week"], s["end_week"]

    if week_input and week_input != 0:
        return [week_input]

    # For current year, determine completed weeks
    try:
        cw_attr = getattr(league, "current_week", None)
        current_week = int(cw_attr() if callable(cw_attr) else cw_attr) if cw_attr is not None else None
    except Exception:
        current_week = None

    if (year == get_current_nfl_season_year()) and current_week and current_week > start_week:
        # Include current week if it's the final week (championship week)
        # or if we're past the season end date (all games played)
        if current_week >= end_week:
            # Championship week - include it (games should be complete by end of Monday)
            last = end_week
            print(f"[weeks] Including championship week {current_week} (final week of season)")
        else:
            # Mid-season: only fetch completed weeks (current_week - 1)
            last = min(end_week, current_week - 1)
        return list(range(start_week, last + 1))

    return list(range(start_week, end_week + 1))


def extract_team(
    team_node: ET.Element,
    matchup_node: ET.Element = None,
    manager_overrides: dict[str, str] = None,
    league_key: str | None = None,
) -> dict[str, Any]:
    """
    Extract team data from XML node.

    Args:
        team_node: XML Element for team
        matchup_node: XML Element for matchup (for grade/felo data)
        manager_overrides: Dictionary of manager name overrides
        league_key: Yahoo league key for reconstructing hidden team_key values

    Returns:
        Dictionary with team data
    """
    # Extract team_name first so it can be used as fallback for hidden managers
    team_name_raw = team_node.findtext("name") or ""

    # Get manager guid (persistent identifier across years)
    manager_guid = team_node.findtext(".//managers/manager/guid") or None

    # Raw manager nickname and profile image are identity inputs used by the
    # Yahoo-only identity resolver. Yahoo redacts `guid` to "--hidden--" for
    # leagues the token does not own (common for older seasons), so the manager
    # profile image is often the only stable cross-year anchor. The default
    # placeholder image is shared by every photo-less / deleted account, so it
    # carries no identity and is treated as absent.
    manager_nickname_raw = team_node.findtext(".//managers/manager/nickname") or ""
    manager_image_raw = team_node.findtext(".//managers/manager/image_url") or ""
    manager_image_url = "" if "default_user_profile" in manager_image_raw else manager_image_raw

    nickname = manager_nickname_raw or team_node.findtext(".//managers/manager/name") or manager_guid or ""
    # Pass team_name as fallback for --hidden-- or missing manager names
    manager = norm_manager(nickname, manager_overrides, team_name_fallback=team_name_raw)
    team_name = team_name_raw or manager

    points = safe_float(team_node.findtext("team_points/total"), 0.0)
    projected = safe_float(team_node.findtext("team_projected_points/total"), np.nan)

    url_ = team_node.findtext("url") or ""

    # Hidden Yahoo scoreboard rows sometimes redact team_key as "--" even
    # though the stable team slot still exists in the team URL.
    team_key = recover_yahoo_team_key(team_node.findtext("team_key"), url_, league_key) or ""

    # Try to find grade - MUST match by team_key to avoid mixing up grades between teams
    grade = ""

    # First try: Look in matchup_grades at matchup level, match by team_key
    if matchup_node is not None and team_key:
        for mg in matchup_node.findall(".//matchup_grades/matchup_grade"):
            mg_team_key = mg.findtext("team_key") or ""
            if mg_team_key == team_key:
                grade = mg.findtext("grade") or ""
                break

    # Second try: Look directly within the team node
    if not grade:
        grade = _first_text(
            team_node,
            [
                ".//matchup_grade/grade",
                "matchup_grade/grade",
                "grade",
            ],
        )

    image_url = team_node.findtext("team_logos/team_logo/url") or ""
    division_id = team_node.findtext("division_id") or ""
    waiver_priority = safe_float(team_node.findtext("waiver_priority"), np.nan)
    faab_balance = safe_float(team_node.findtext("faab_balance"), np.nan)
    number_of_moves = safe_float(team_node.findtext("number_of_moves"), np.nan)
    number_of_trades = safe_float(team_node.findtext("number_of_trades"), np.nan)
    coverage_value = safe_float(team_node.findtext("coverage_value"), np.nan)
    value = safe_float(team_node.findtext("value"), np.nan)
    has_draft_grade = (team_node.findtext("has_draft_grade") or "").strip()

    # Try multiple locations for felo_score and felo_tier
    felo_score = _first_float(
        team_node,
        [
            "felo_score",
            ".//team_standings/felo_score",
            ".//team_stats/felo_score",
            "team_standings/felo_score",
            "team_stats/felo_score",
        ],
    )

    felo_tier = _first_text(
        team_node,
        [
            "felo_tier",
            ".//team_standings/felo_tier",
            ".//team_stats/felo_tier",
            "team_standings/felo_tier",
            "team_stats/felo_tier",
        ],
    )

    win_probability = safe_float(team_node.findtext("win_probability"), np.nan)

    auction_budget_total = _first_float(team_node, ["draft_results/auction_budget/total", "auction_budget_total"])
    auction_budget_spent = _first_float(team_node, ["draft_results/auction_budget/spent", "auction_budget_spent"])

    return {
        "manager": manager,
        "manager_guid": manager_guid,
        "manager_nickname_raw": manager_nickname_raw,
        "manager_image_url": manager_image_url,
        "team_key": team_key,
        "team_name": team_name,
        "team_points": points,
        "team_projected_points": projected,
        "grade": grade,
        "url": url_,
        "image_url": image_url,
        "division_id": division_id,
        "waiver_priority": waiver_priority,
        "faab_balance": faab_balance,
        "number_of_moves": number_of_moves,
        "number_of_trades": number_of_trades,
        "coverage_value": coverage_value,
        "value": value,
        "has_draft_grade": has_draft_grade,
        "auction_budget_total": auction_budget_total,
        "auction_budget_spent": auction_budget_spent,
        "felo_score": felo_score,
        "felo_tier": felo_tier,
        "win_probability": win_probability,
    }


def parse_matchups_for_week(
    oauth,
    yearid: str,
    year_val: int,
    week: int,
    manager_overrides: dict[str, str] = None,
    debug_xml: bool = False,
    max_retries: int = 3,
) -> list[dict[str, Any]]:
    """
    Parse matchups for a specific week.

    Args:
        oauth: OAuth2 instance
        yearid: Yahoo league ID
        year_val: Season year
        week: Week number
        manager_overrides: Dictionary of manager name overrides
        debug_xml: If True, save raw XML response to file for debugging
        max_retries: Maximum number of retries on token expiration

    Returns:
        List of matchup row dictionaries
    """
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{yearid}/scoreboard;week={week}"

    # Retry loop for token expiration
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = oauth.session.get(url)

            # Check for token expiration errors in response
            if (
                resp.status_code in (401, 403)
                or "Invalid cookie" in resp.text
                or "please log in again" in resp.text.lower()
            ):
                raise RuntimeError(f"Token expired: {resp.text[:200]}")

            resp.raise_for_status()
            break  # Success - exit retry loop

        except Exception as e:
            last_error = e
            error_str = str(e).lower()

            # Check if this is a token expiration error
            is_token_error = (
                "invalid cookie" in error_str
                or "please log in again" in error_str
                or "must be logged in" in error_str
                or "token expired" in error_str
                or "401" in error_str
                or "unauthorized" in error_str
            )

            if is_token_error and attempt < max_retries:
                print(f"[week {week}] Token expired (attempt {attempt}/{max_retries}), refreshing...")

                # Try to refresh the token
                try:
                    if hasattr(oauth, "refresh_access_token"):
                        oauth.refresh_access_token()
                        print(f"[week {week}] Token refreshed successfully")
                    elif hasattr(oauth, "token") and hasattr(oauth.token, "refresh"):
                        oauth.token.refresh()
                        print(f"[week {week}] Token refreshed successfully")
                    else:
                        print(f"[week {week}] Warning: No refresh method available")
                except Exception as refresh_error:
                    print(f"[week {week}] Warning: Token refresh failed: {refresh_error}")

                # Wait before retry
                time.sleep(2 * attempt)
                continue
            else:
                # Not a token error or max retries reached - re-raise
                raise

    if last_error and "resp" not in dir():
        raise last_error

    # Save raw XML for debugging if requested
    if debug_xml:
        debug_file = Path(f"debug_matchup_week_{week}.xml")
        debug_file.write_text(resp.text, encoding="utf-8")
        print(f"[debug] Saved XML to {debug_file}")

    xmlstring = re.sub(r" xmlns=\"[^\"]+\"", "", resp.text, count=1)
    root = ET.fromstring(xmlstring)

    return parse_matchups_from_xml(root, year_val, manager_overrides=manager_overrides, league_key=yearid)


def _build_matchup_row(
    a: dict[str, Any],
    b: dict[str, Any],
    year_val: int,
    week_num: int = 0,
    winner_team_key: str | None = None,
) -> dict[str, Any]:
    """Build a matchup row for manager a vs manager b.

    PURE FETCH: returns only raw API data plus the identity inputs the Yahoo
    identity resolver needs (manager_nickname_raw, manager_image_url — both
    dropped before DDL). All derived columns are computed by SQL enrichments.
    """
    return {
        "week": week_num,
        "year": int(year_val),
        "manager": a["manager"],
        "manager_guid": a.get("manager_guid"),
        # Identity inputs for the Yahoo identity resolver (dropped before DDL).
        "manager_nickname_raw": a.get("manager_nickname_raw"),
        "manager_image_url": a.get("manager_image_url"),
        "team_key": a.get("team_key"),
        "team_name": a["team_name"],
        "team_points": safe_float(a["team_points"], 0.0),
        "team_projected_points": safe_float(a["team_projected_points"], None),
        "opponent": b["manager"],
        "opponent_guid": b.get("manager_guid"),
        "opponent_team_key": b.get("team_key"),
        "winner_team_key": winner_team_key,
        "opponent_points": safe_float(b["team_points"], 0.0),
        "opponent_projected_points": safe_float(b["team_projected_points"], None),
        "grade": a.get("grade"),
        "matchup_recap_title": a.get("matchup_recap_title"),
        "matchup_recap_url": a.get("matchup_recap_url"),
        "url": a.get("url"),
        "image_url": a.get("image_url"),
        "division_id": a.get("division_id"),
        "waiver_priority": a.get("waiver_priority"),
        "faab_balance": a.get("faab_balance"),
        "number_of_moves": a.get("number_of_moves"),
        "number_of_trades": a.get("number_of_trades"),
        "auction_budget_spent": a.get("auction_budget_spent"),
        "auction_budget_total": a.get("auction_budget_total"),
        "win_probability": a.get("win_probability"),
        "coverage_value": a.get("coverage_value"),
        "value": a.get("value"),
        "has_draft_grade": a.get("has_draft_grade"),
        "felo_score": a.get("felo_score"),
        "felo_tier": a.get("felo_tier"),
        "week_start": a.get("week_start"),
        "week_end": a.get("week_end"),
    }


def parse_matchups_from_xml(
    root: ET.Element,
    year_val: int,
    manager_overrides: dict[str, str] | None = None,
    league_key: str | None = None,
) -> list[dict[str, Any]]:
    """Parse a scoreboard XML tree into matchup row dicts (pure, no network).

    Split out from the API fetch so the parse + identity-input plumbing can be
    tested against saved XML. Zero-score (unplayed) weeks are NOT skipped — they
    carry schedule data; the caller separates played games from all games.
    """
    # Yahoo reports the exact matchup cardinality on the scoreboard. A 200
    # response with fewer nodes than its own declaration, or a matchup whose
    # team card cannot be parsed, must not become a partial published week.
    for container in root.findall(".//matchups"):
        matchups = container.findall("./matchup")
        declared = container.get("count")
        if declared is not None:
            try:
                expected_count = int(declared)
            except ValueError as exc:
                raise RuntimeError("Yahoo declared matchup count is invalid") from exc
            if expected_count < 0 or expected_count != len(matchups):
                raise RuntimeError(
                    f"Yahoo declared matchup count {expected_count} disagrees with {len(matchups)} nodes"
                )
        for matchup in matchups:
            teams = matchup.findall(".//teams/team")
            if len(teams) != 2:
                teams = matchup.findall(".//team")
            if len(teams) != 2:
                raise RuntimeError("Yahoo matchup team cardinality is not a reciprocal pair")
    rows: list[dict[str, Any]] = []
    for matchup in root.findall(".//matchup"):
        wk_node = matchup.find("week")
        if wk_node is None or not wk_node.text:
            continue
        week_num = int(wk_node.text)

        # Yahoo's playoff/consolation booleans are too broad for canonical
        # semantics, but winner_team_key is authoritative matchup graph data.
        winner_team_key = matchup.findtext("winner_team_key", default="") or None
        week_start = matchup.findtext("week_start", default="") or ""
        week_end = matchup.findtext("week_end", default="") or ""
        matchup_recap_url = matchup.findtext("matchup_recap_url", default="") or ""
        matchup_recap_title = matchup.findtext("matchup_recap_title", default="") or ""

        teams = matchup.findall(".//teams/team")
        if len(teams) != 2:
            teams = matchup.findall(".//team")
        if len(teams) != 2:
            continue

        t1 = extract_team(teams[0], matchup, manager_overrides, league_key=league_key)
        t2 = extract_team(teams[1], matchup, manager_overrides, league_key=league_key)

        for a, b in ((t1, t2), (t2, t1)):
            row = _build_matchup_row(a, b, year_val, week_num, winner_team_key)
            row.update(
                {
                    "week_start": week_start,
                    "week_end": week_end,
                    "matchup_recap_url": matchup_recap_url,
                    "matchup_recap_title": matchup_recap_title,
                }
            )
            rows.append(row)

    return rows


# add_derived_metrics() and add_head_to_head_records() DELETED —
# all derived metrics now computed by SQL enrichments:
#   compute_win_loss_and_projections() — win/loss, proj errors, GPA
#   compute_league_weekly_stats() — league mean/median, all-play
#   cumulative_records() — head-to-head, streaks, cumulative W/L


# =============================================================================
# Main API Function
# =============================================================================


def weekly_matchup_data(
    ctx: LeagueContext | None = None,
    year: int | None = None,
    week: int | None = None,
    oauth_file: Path | None = None,
    league_key: str | None = None,
    data_dir: Path | None = None,
    logger: RunLogger | None = None,
) -> pd.DataFrame:
    """
    Fetch weekly matchup data from Yahoo Fantasy API.

    Args:
        ctx: Optional LeagueContext for league-specific configuration
        year: Season year
        week: Week number (None or 0 = all weeks)
        oauth_file: Path to OAuth credentials (if no context)
        league_key: Yahoo league_key (if no context)
        data_dir: Custom data directory (overrides context)
        logger: Optional RunLogger instance

    Returns:
        DataFrame with matchup data

    Raises:
        ValueError: If required parameters missing
        RuntimeError: If API calls fail
    """
    if not YFA_AVAILABLE:
        raise RuntimeError("yahoo_fantasy_api not available. Install with: pip install yahoo_fantasy_api")

    # Determine data directory
    if data_dir:
        matchup_data_dir = Path(data_dir)
    elif ctx:
        matchup_data_dir = ctx.matchup_data_directory
    else:
        matchup_data_dir = DEFAULT_DATA_ROOT

    matchup_data_dir.mkdir(parents=True, exist_ok=True)

    if year is None:
        raise ValueError("year is required")

    # Get OAuth
    if ctx:
        oauth = ctx.get_oauth_session()
        manager_overrides = ctx.manager_name_overrides
    elif oauth_file:
        if create_oauth2:
            oauth = create_oauth2(str(oauth_file))
        else:
            # Fallback to yahoo_oauth if create_oauth2 not available
            from yahoo_oauth import OAuth2

            oauth = OAuth2(None, None, from_file=str(oauth_file))
        manager_overrides = {}
    else:
        raise ValueError("Either ctx or oauth_file is required")

    # Create logger if context provided
    if logger is None and ctx and RUN_LOGGER_AVAILABLE:
        logger = RunLogger("weekly_matchup_data", year=year, week=week, league_id=ctx.league_id)
        logger.__enter__()
        close_logger = True
    else:
        close_logger = False

    try:
        # Get league
        if logger:
            logger.start_step("get_league")

        gm = yfa.Game(oauth, "nfl")

        # CRITICAL: Check context FIRST to avoid calling restricted API endpoints
        # Priority: 1) ctx.get_league_id_for_year(), 2) league_key param, 3) API discovery (last resort)
        yearid = None

        # Try context first (safest - ensures league isolation AND avoids restricted API calls)
        if ctx and hasattr(ctx, "get_league_id_for_year"):
            yearid = ctx.get_league_id_for_year(year)

        # Fallback to explicit league_key parameter
        if not yearid and league_key:
            yearid = league_key
            print(f"[league] Using explicit league_key parameter: {yearid}")

        # Last resort: use API discovery (requires users;use_login=1 scope which some users don't have)
        if not yearid:
            print(f"[league] No league_id in context for {year}, attempting API discovery...")
            league_ids = None
            for _attempt in (1, 2):
                try:
                    league_ids = gm.league_ids(year=year)
                    if league_ids:
                        break
                    else:
                        raise RuntimeError(f"No leagues found for {year}")
                except Exception as e:
                    # On first failure attempt to refresh access token and retry
                    if _attempt == 1 and hasattr(oauth, "refresh_access_token"):
                        try:
                            oauth.refresh_access_token()
                        except Exception:  # noqa: broad-except
                            pass
                        # Small backoff before retrying
                        time.sleep(1)
                        continue
                    # If second attempt fails, rethrow
                    raise RuntimeError(f"Failed to fetch league_ids for {year}: {e}")

            if not league_ids:
                raise RuntimeError(f"No leagues found for {year}")

            if len(league_ids) > 1:
                print(f"[league] WARNING: Multiple leagues found for {year}: {league_ids}")
                print(f"[league] WARNING: Using last one ({league_ids[-1]}) - this may cause data mixing!")
                print("[league] TIP: Populate ctx.league_ids to ensure correct league isolation")
            yearid = league_ids[-1]

        # Create league object with retry on token expiration (Yahoo 401)
        league = None
        for league_attempt in range(1, 4):
            try:
                league = gm.to_league(yearid)
                break
            except Exception as league_error:
                error_str = str(league_error).lower()
                is_token_error = (
                    "invalid cookie" in error_str
                    or "please log in again" in error_str
                    or "must be logged in" in error_str
                    or "401" in error_str
                    or "unauthorized" in error_str
                )
                if is_token_error and league_attempt < 3:
                    print(f"[league] Token expired creating league (attempt {league_attempt}/3), refreshing...")
                    try:
                        if hasattr(oauth, "refresh_access_token"):
                            oauth.refresh_access_token()
                            print("[league] Token refreshed successfully")
                    except Exception:  # noqa: broad-except
                        pass
                    time.sleep(2 * league_attempt)
                    continue
                raise

        if league is None:
            raise RuntimeError(f"Failed to create league object for {yearid} after 3 attempts")

        print(f"[league] Using league: {yearid}")

        if logger:
            logger.complete_step()

        # Get league settings (with retry on token expiration)
        if logger:
            logger.start_step("get_league_settings")

        settings = {}
        for settings_attempt in range(1, 4):
            try:
                settings = league.settings() if hasattr(league, "settings") else {}
                break
            except Exception as settings_error:
                error_str = str(settings_error).lower()
                is_token_error = (
                    "invalid cookie" in error_str
                    or "please log in again" in error_str
                    or "must be logged in" in error_str
                    or "401" in error_str
                    or "unauthorized" in error_str
                )
                if is_token_error and settings_attempt < 3:
                    print(f"[settings] Token expired (attempt {settings_attempt}/3), refreshing...")
                    try:
                        if hasattr(oauth, "refresh_access_token"):
                            oauth.refresh_access_token()
                            print("[settings] Token refreshed successfully")
                    except Exception:  # noqa: broad-except
                        pass
                    time.sleep(2)
                    # Re-create league object with fresh token
                    league = gm.to_league(yearid)
                    continue
                raise

        if logger:
            logger.complete_step()

        # Determine weeks to fetch
        weeks = get_weeks_to_fetch(league, year, week, settings)

        # Fetch matchups for each week
        if logger:
            logger.start_step("fetch_matchups")

        # Check for debug mode via environment variable
        debug_xml = os.environ.get("DEBUG_MATCHUP_XML", "").lower() in ("1", "true", "yes")

        all_rows = []
        failed_weeks = []
        # One bulk call for the requested scope.  Passing all 22 weeks here
        # ignored an explicit ``week`` argument, so a targeted refresh could
        # accidentally stage future scheduled matchups as its active state.
        # Yahoo silently ignores non-existent weeks (no errors, no nulls).
        all_weeks_param = ",".join(str(w) for w in weeks)
        try:
            rows = parse_matchups_for_week(oauth, yearid, year, all_weeks_param, manager_overrides, debug_xml)
            all_rows.extend(rows)
            if not rows:
                print("[matchups] Bulk fetch returned no matchups, falling back to per-week")
        except Exception as e:
            # Bulk call failed — fall back to per-week fetching
            print(f"[matchups] Bulk fetch failed ({e}), falling back to per-week")

        if not all_rows:
            for w in weeks:
                try:
                    rows = parse_matchups_for_week(oauth, yearid, year, w, manager_overrides, debug_xml)
                    all_rows.extend(rows)
                except Exception as e2:
                    print(f"[week {w}] Failed: {e2}")
                    failed_weeks.append(w)

            if failed_weeks:
                try:
                    from multi_league.data_fetchers.yahoo.fetch_failure_manifest import write_manifest

                    manifest_dir = ctx.data_directory if ctx else matchup_data_dir.parent
                    write_manifest(manifest_dir, "matchups", year, failed_weeks=failed_weeks)
                except Exception as e3:
                    print(f"  [WARN] Could not write failure manifest: {e3}")

        if not all_rows:
            raise RuntimeError("No matchup data found")

        all_df = pd.DataFrame(all_rows)

        # ── Yahoo manager identity resolution (Yahoo-only, pre-DDL) ──
        # Yahoo redacts `guid` to "--hidden--" for unowned/older leagues, and
        # each season is a separate league_key so team slots carry no cross-year
        # meaning. Resolve a stable synthetic manager_guid from the profile image
        # (survives redaction) → unique nickname → team_key. This becomes the
        # matchup franchise_id (canonical_matchup sets franchise_id = manager_guid),
        # which draft / transactions / player_fantasy inherit by team_key via the
        # franchise backfill. No-op for rows that already carry a real guid.
        #
        # This must NOT be wrapped in a broad try/except: a swallowed failure
        # leaves manager_guid="--hidden--", which silently collapses every team
        # into "--hidden--_<slot>" franchises. Fail loudly instead. The import
        # resilience for the resolver's own dependencies lives in yahoo_identity.
        try:
            from multi_league.data_fetchers.yahoo.yahoo_identity import resolve_yahoo_manager_guids
        except ImportError:
            from data_fetchers.yahoo.yahoo_identity import resolve_yahoo_manager_guids

        resolved_guid = resolve_yahoo_manager_guids(all_df)
        all_df["manager_guid"] = resolved_guid.where(resolved_guid.notna(), all_df.get("manager_guid"))

        # Transient identity-input columns are consumed above; drop so they do
        # not flow into the canonical matchup/schedule schema.
        all_df = all_df.drop(columns=["manager_nickname_raw", "manager_image_url"], errors="ignore")

        # Split: played games (have scores) → matchup table,
        # ALL games (played + unplayed) → schedule table.
        # A game is "played" if either team has non-zero points.
        played_mask = (all_df["team_points"].fillna(0) != 0) | (all_df["opponent_points"].fillna(0) != 0)
        df = all_df[played_mask].copy()
        schedule_df = all_df.copy()  # schedule gets everything

        played_weeks = df["week"].nunique() if not df.empty else 0
        total_weeks = all_df["week"].nunique()
        print(
            f"[matchups] Fetched {len(all_df)} rows across {total_weeks} weeks ({played_weeks} played, {total_weeks - played_weeks} unplayed)"
        )

        # VALIDATION: Ensure all league_ids in fetched data match expected league
        if "league_id" in df.columns and ctx:
            unique_league_ids = df["league_id"].unique()
            expected_league_id = yearid
            if len(unique_league_ids) > 1:
                print(f"[matchups] WARNING: Multiple league_ids found in data: {unique_league_ids}")
                print(f"[matchups] WARNING: Expected only: {expected_league_id}")
            elif len(unique_league_ids) == 1 and unique_league_ids[0] != expected_league_id:
                print(
                    f"[matchups] WARNING: League ID mismatch - expected {expected_league_id}, got {unique_league_ids[0]}"
                )

        if logger:
            logger.complete_step(rows_read=len(df))

        # Derived metrics (weekly_mean, league_median, all-play, GPA, win/loss)
        # are computed by SQL enrichments after upload — not in the fetcher.

        # Column ordering
        # NOTE: is_playoffs and is_consolation are NOT included here
        # They will be added by transformation pipeline (cumulative_stats_v2.py)
        # NOTE: w_vs_X and l_vs_X head-to-head columns are also NOT included here
        # Raw API columns only — all derived columns computed by SQL enrichments
        # The normalizer (normalize_matchup_df) maps these to canonical DDL
        # and db.save_table() inserts only matching columns.
        df["league_id"] = yearid

        # Output saved by caller via db.save_table()

        # Attach schedule_df for the caller to write to the schedule table
        df.attrs["schedule_df"] = schedule_df

        return df, failed_weeks

    finally:
        if close_logger and logger:
            logger.__exit__(None, None, None)


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Fantasy Football weekly matchup data (multi-league compliant)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument("--week", type=int, help="Week number (0 or omit = all weeks)")
    parser.add_argument("--oauth", type=Path, help="Path to OAuth credentials (if no context)")
    parser.add_argument("--league-key", help="Yahoo league_key (if no context)")
    parser.add_argument("--data-dir", type=Path, help="Custom data directory")

    args = parser.parse_args()

    # Load context if provided
    ctx = None
    if args.context:
        if not LEAGUE_CONTEXT_AVAILABLE:
            print("Error: league_context module not available", file=sys.stderr)
            sys.exit(1)

        try:
            ctx = LeagueContext.load(args.context)
            print(f"Loaded context: {ctx.league_name}")
        except Exception as e:
            print(f"Error loading context: {e}", file=sys.stderr)
            sys.exit(1)

    # Run
    try:
        _df, _failed = weekly_matchup_data(
            ctx=ctx,
            year=args.year,
            week=args.week,
            oauth_file=args.oauth,
            league_key=args.league_key,
            data_dir=args.data_dir,
        )
        if _failed:
            print(f"[WARN] {len(_failed)} week(s) permanently failed: {_failed}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
