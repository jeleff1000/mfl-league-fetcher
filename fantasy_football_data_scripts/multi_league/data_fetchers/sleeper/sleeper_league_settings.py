"""
Sleeper League Settings Fetcher

Fetches comprehensive league settings from Sleeper API and saves as JSON.
Parallel to yahoo_league_settings.py.

Settings include:
- Scoring rules (PPR, passing TDs, etc.)
- Roster positions (QB, RB, FLEX slots)
- League rules (playoff teams, trade deadline)
- Waiver settings (FAAB budget, waiver type)
- Division structure

Usage:
    from sleeper_league_settings import fetch_sleeper_settings

    settings = fetch_sleeper_settings(client, league_id, year)
    # Returns dict with all league configuration
"""

import json
import logging
import math
import sys
from pathlib import Path
from typing import Any
from datetime import datetime

_verbose = "--verbose" in sys.argv

from .sleeper_api_client import SleeperAPIClient
from .sleeper_context import SleeperContext
from .playoff_utils import resolve_playoff_structure

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function."""
    logger.info(msg)
    print(msg)


def fetch_sleeper_settings(client: SleeperAPIClient, league_id: str, year: int | None = None) -> dict[str, Any]:
    """
    Fetch comprehensive league settings from Sleeper API.

    Args:
        client: SleeperAPIClient instance
        league_id: Sleeper league_id
        year: Optional year (for metadata)

    Returns:
        Dict with all league settings
    """
    if _verbose:
        log(f"Fetching Sleeper league settings for {league_id}")

    # Get main league info
    league = client.get_league(league_id)
    if not league:
        raise ValueError(f"League not found: {league_id}")

    # Get users for team info
    users = client.get_league_users(league_id)

    # Get rosters for team count
    rosters = client.get_league_rosters(league_id)

    # Get NFL state for week info
    nfl_state = client.get_nfl_state()

    # Extract settings
    settings = league.get("settings", {})
    scoring_settings = league.get("scoring_settings", {})
    roster_positions = league.get("roster_positions", [])

    # Extract playoff settings for easier access
    playoff_teams = settings.get("playoff_teams", 6)
    start_week = settings.get("start_week", 1)
    league_is_complete = str(league.get("status") or "").strip().lower() == "complete"
    playoff_structure = resolve_playoff_structure(
        settings,
        season_complete=league_is_complete,
    )
    playoff_week_start = playoff_structure["playoff_week_start"]
    playoff_round_type = playoff_structure["playoff_round_type"]
    playoff_rounds = playoff_structure["playoff_rounds"]
    weeks_in_playoffs = playoff_structure["weeks_in_playoffs"]
    last_scored_leg = playoff_structure["last_scored_leg"]

    if playoff_structure["playoff_start_source"] == "inferred" and last_scored_leg and last_scored_leg > 0:
        log(
            f"  [INFERRED] playoff_week_start={playoff_week_start} "
            f"from last_scored_leg={last_scored_leg}, playoff_weeks={weeks_in_playoffs}"
        )
    elif playoff_structure["playoff_start_source"] == "default":
        log(f"  [DEFAULT] playoff_week_start={playoff_week_start} (API returned 0 and no last_scored_leg)")

    has_multiweek = 1 if playoff_rounds > 0 and playoff_round_type in (1, 2) else 0

    # Championship week calculation (depends on canonical round type)
    if playoff_rounds <= 0:
        championship_week = None
    elif playoff_round_type == 0:
        championship_week = playoff_week_start + playoff_rounds - 1
    elif playoff_round_type == 1:
        championship_week = playoff_week_start + (2 * playoff_rounds) - 1
    else:  # canonical type 2: 2-week championship only
        championship_week = playoff_week_start + playoff_rounds

    # Calculate bye_teams from bracket math (standard tournament structure)
    # NOTE: playoff_round_type is about multi-week rounds (0=single week, 1=2-week championship),
    # NOT about byes. Byes are determined by bracket structure:
    # - If playoff_teams is a power of 2 (4, 8, 16), no byes needed
    # - Otherwise, top seeds get byes until bracket is a power of 2
    # Examples: 6 teams → 2 byes, 5 teams → 3 byes, 8 teams → 0 byes
    if playoff_teams > 1:
        next_power_of_2 = 2 ** math.ceil(math.log2(playoff_teams))
        bye_teams = next_power_of_2 - playoff_teams
    else:
        bye_teams = 0

    # Calculate end_week from playoff bracket structure
    # (playoff_rounds already calculated above for validation)
    end_week = last_scored_leg if last_scored_leg else playoff_structure["playoff_week_end"]

    # Regular season weeks = playoff_start_week - start_week
    regular_season_weeks = playoff_week_start - start_week

    # Map Sleeper draft type to descriptive names
    # Sleeper: type 0=redraft, 1=keeper, 2=dynasty
    sleeper_type = settings.get("type", 0)
    draft_type_map = {0: "redraft", 1: "keeper", 2: "dynasty"}
    draft_type = draft_type_map.get(sleeper_type, "redraft")

    # Determine if auction draft based on draft data (will be updated later if draft info available)
    # For now, default to snake (Sleeper doesn't have this in league settings)
    is_auction = False  # Will be overridden by draft fetcher if auction detected

    # Get number of teams
    num_teams = league.get("total_rosters", len(rosters))

    # Losers bracket — determines has_consolation_bracket + num_playoff_consolation_teams
    # See docs/superpowers/specs/2026-04-17-bracket-tracer-generality-design.md §2
    try:
        losers_bracket = client.get_losers_bracket(league_id)
    except Exception as exc:
        logger.warning(f"[sleeper_settings] /losers_bracket call failed for {league_id}: {exc}")
        losers_bracket = None

    season_complete = league_is_complete or nfl_state.get("season_type") == "off" or (
        settings.get("last_scored_leg") and settings.get("last_scored_leg") >= 17
    )

    if losers_bracket is None:
        losers_bracket_teams = []  # API 404 — authoritatively no bracket
    elif len(losers_bracket) == 0 and not season_complete:
        losers_bracket_teams = None  # In-season, empty — unknown until season progresses
    else:
        resolved = set()
        for m in losers_bracket:
            for key in ("t1", "t2"):
                v = m.get(key)
                if v is not None:
                    resolved.add(v)
        losers_bracket_teams = sorted(resolved) if resolved else []

    # Build comprehensive settings dict
    result = {
        # Metadata
        "platform": "sleeper",
        "league_id": league_id,
        "league_name": league.get("name", "Unknown"),
        "season": league.get("season"),
        "status": league.get("status"),
        "fetched_at": datetime.now().isoformat(),
        # League Structure
        "total_rosters": num_teams,
        "num_teams": num_teams,
        "draft_id": league.get("draft_id"),
        "previous_league_id": league.get("previous_league_id"),
        "sport": league.get("sport", "nfl"),
        # TOP-LEVEL PLAYOFF SETTINGS (for compatibility with transformation pipeline)
        # These are the canonical fields that load_league_settings() expects
        "num_playoff_teams": playoff_teams,
        "playoff_teams": playoff_teams,  # Alias for compatibility
        "playoff_start_week": playoff_week_start,
        "bye_teams": bye_teams,
        "has_multiweek_championship": has_multiweek,
        "uses_playoff_reseeding": None,  # Sleeper API doesn't expose reseeding
        # SEASON STRUCTURE (for compatibility with transformation pipeline)
        "start_week": start_week,
        "end_week": end_week,
        "regular_season_weeks": regular_season_weeks,
        # DRAFT SETTINGS
        "draft_type": draft_type,  # 'redraft', 'keeper', 'dynasty'
        "is_auction": is_auction,
        "draft_rounds": settings.get("draft_rounds", 15),
        "max_keepers": settings.get("max_keepers", 0),
        # WAIVER SETTINGS (top-level for compatibility)
        "waiver_type": settings.get("waiver_type", 0),  # 0=normal, 1=FAAB, 2=continuous
        "waiver_budget": settings.get("waiver_budget", 100),  # FAAB budget
        # SLEEPER-UNIQUE SETTINGS (not available in Yahoo)
        # League format
        "league_average_match": settings.get("league_average_match", 0),  # H2H + Median (1=enabled)
        "best_ball": settings.get("best_ball", 0),  # Best ball format (auto-optimal lineup)
        "bench_lock": settings.get("bench_lock", 0),  # Lock bench during games
        # Playoff structure
        "playoff_type": settings.get("playoff_type", 0),  # 0=1 week per round, 1=2 week championship
        "playoff_seed_type": settings.get("playoff_seed_type", 0),  # Seeding method
        # IR/Reserve settings
        "reserve_slots": settings.get("reserve_slots", 0),  # Number of IR slots
        "reserve_allow_out": settings.get("reserve_allow_out", 0),
        "reserve_allow_doubtful": settings.get("reserve_allow_doubtful", 0),
        "reserve_allow_sus": settings.get("reserve_allow_sus", 0),
        "reserve_allow_cov": settings.get("reserve_allow_cov", 0),
        "reserve_allow_na": settings.get("reserve_allow_na", 0),
        # Dynasty/Keeper settings
        "taxi_slots": settings.get("taxi_slots", 0),  # Taxi squad slots
        "taxi_years": settings.get("taxi_years", 0),  # Years on taxi
        "taxi_allow_vets": settings.get("taxi_allow_vets", 0),
        "pick_trading": settings.get("pick_trading", 0),  # Draft pick trading
        # Trade settings
        "trade_deadline": settings.get("trade_deadline", 11),
        "trade_review_days": settings.get("trade_review_days", 2),
        "veto_votes_needed": settings.get("veto_votes_needed", 0),
        # Roster Positions
        "roster_positions": roster_positions,
        "roster_position_counts": _count_positions(roster_positions),
        # Scoring Settings
        "scoring_settings": scoring_settings,
        "scoring_type": _determine_scoring_type(scoring_settings),
        # League Settings (nested for full Sleeper API compatibility - kept for backwards compat)
        "settings": {
            # Playoffs
            "playoff_teams": playoff_teams,
            "playoff_week_start": playoff_week_start,
            "playoff_round_type": playoff_round_type,
            "playoff_type": settings.get("playoff_type", 0),
            "playoff_seed_type": settings.get("playoff_seed_type", 0),
            # Waivers
            "waiver_type": settings.get("waiver_type", 0),
            "waiver_budget": settings.get("waiver_budget", 100),
            "waiver_day_of_week": settings.get("waiver_day_of_week", 2),
            "daily_waivers": settings.get("daily_waivers", 0),
            # Trade
            "trade_deadline": settings.get("trade_deadline", 11),
            "trade_review_days": settings.get("trade_review_days", 2),
            "veto_votes_needed": settings.get("veto_votes_needed", 0),
            # Divisions
            "divisions": settings.get("divisions", 0),
            # Scoring
            "leg": settings.get("leg", 1),
            "start_week": settings.get("start_week", 1),
            "last_scored_leg": settings.get("last_scored_leg"),
            # Sleeper-unique formats
            "league_average_match": settings.get("league_average_match", 0),
            "best_ball": settings.get("best_ball", 0),
            "bench_lock": settings.get("bench_lock", 0),
            # Dynasty/Keeper
            "pick_trading": settings.get("pick_trading", 0),
            "max_keepers": settings.get("max_keepers", 0),
            "taxi_slots": settings.get("taxi_slots", 0),
            "taxi_years": settings.get("taxi_years", 0),
            # IR/Reserve
            "reserve_slots": settings.get("reserve_slots", 0),
            "reserve_allow_out": settings.get("reserve_allow_out", 0),
            "reserve_allow_doubtful": settings.get("reserve_allow_doubtful", 0),
            # Misc
            "offseason_adds": settings.get("offseason_adds", 0),
        },
        # NFL State
        "nfl_state": {
            "season": nfl_state.get("season"),
            "week": nfl_state.get("week"),
            "season_type": nfl_state.get("season_type"),
            "season_start_date": nfl_state.get("season_start_date"),
            "leg": nfl_state.get("leg"),
        },
        # Users/Teams
        "teams": _build_teams_list(users, rosters),
        # Losers bracket (consolation bracket detection)
        "losers_bracket_teams": losers_bracket_teams,
        # Sleeper-specific metadata (keeper_deadline, etc.)
        "sleeper_metadata": league.get("metadata", {}),
        "avatar": league.get("avatar"),
        # METADATA DICT (Yahoo-compatible format for transformation pipeline)
        # The pipeline often does: metadata = settings.get('metadata', settings)
        # This ensures consistent field names regardless of how it's accessed
        "metadata": {
            "league_id": league_id,
            "name": league.get("name", "Unknown"),
            "season": league.get("season"),
            "num_teams": num_teams,
            "draft_type": draft_type,
            "scoring_type": _determine_scoring_type(scoring_settings),
            "start_week": start_week,
            "end_week": end_week,
            "playoff_start_week": playoff_week_start,
            "num_playoff_teams": playoff_teams,
            "playoff_teams": playoff_teams,
            "bye_teams": bye_teams,
            "has_multiweek_championship": has_multiweek,
            "uses_reseeding": None,  # Sleeper doesn't expose reseeding
            "playoff_round_type": playoff_round_type,
            "num_rounds": playoff_rounds,
            "championship_week": championship_week,
            "regular_season_weeks": regular_season_weeks,
            "waiver_type": settings.get("waiver_type", 0),
            "waiver_budget": settings.get("waiver_budget", 100),
            "max_keepers": settings.get("max_keepers", 0),
            "platform": "sleeper",
            # Sleeper-unique settings (for future UI features)
            "league_average_match": settings.get("league_average_match", 0),
            "best_ball": settings.get("best_ball", 0),
            "playoff_type": settings.get("playoff_type", 0),
            "playoff_seed_type": settings.get("playoff_seed_type", 0),
            "reserve_slots": settings.get("reserve_slots", 0),
            "taxi_slots": settings.get("taxi_slots", 0),
            "pick_trading": settings.get("pick_trading", 0),
            "trade_deadline": settings.get("trade_deadline", 11),
        },
    }

    log(
        f"  {result.get('season', '?')}: {result['num_teams']} teams, {result['scoring_type']}, wk {result['start_week']}-{result['end_week']}"
    )
    if _verbose:
        log(f"    League: {result['league_name']}, Type: {result['draft_type']}")
        log(
            f"    Playoffs: {result['num_playoff_teams']} teams, {result['bye_teams']} byes, weeks {result['playoff_start_week']}-{result['end_week']}"
        )

    try:
        from multi_league.core.scoring_config import normalize_sleeper

        result["canonical_scoring"] = normalize_sleeper(result.get("scoring_settings", {}))
    except Exception:
        pass

    return result


def _count_positions(roster_positions: list[str]) -> dict[str, int]:
    """Count occurrences of each roster position."""
    counts = {}
    for pos in roster_positions:
        counts[pos] = counts.get(pos, 0) + 1
    return counts


def _determine_scoring_type(scoring_settings: dict[str, float]) -> str:
    """
    Determine scoring type (Standard, Half PPR, PPR) from settings.

    Args:
        scoring_settings: Dict of stat -> points

    Returns:
        Scoring type string
    """
    ppr = scoring_settings.get("rec", 0)

    if ppr >= 1.0:
        return "PPR"
    elif ppr >= 0.5:
        return "Half PPR"
    else:
        return "Standard"


def _build_teams_list(users: list[dict], rosters: list[dict]) -> list[dict[str, Any]]:
    """Build list of team info from users and rosters."""
    # Map user_id to user info
    user_map = {u.get("user_id"): u for u in users}

    teams = []
    for roster in rosters:
        roster_id = roster.get("roster_id")
        owner_id = roster.get("owner_id")
        user = user_map.get(owner_id, {})

        teams.append(
            {
                "roster_id": roster_id,
                "owner_id": owner_id,
                "display_name": user.get("display_name", user.get("username", "Unknown")),
                "team_name": user.get("metadata", {}).get("team_name"),
                "avatar": user.get("avatar"),
                "division": roster.get("settings", {}).get("division"),
            }
        )

    return teams


def save_sleeper_settings(
    ctx: SleeperContext, year: int, settings: dict[str, Any], output_dir: Path | None = None
) -> Path:
    """
    Save league settings to JSON file.

    Args:
        ctx: SleeperContext
        year: Season year
        settings: Settings dict from fetch_sleeper_settings
        output_dir: Optional output directory

    Returns:
        Path to saved file
    """
    if output_dir is None:
        output_dir = ctx.data_directory / "league_settings"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Use standard naming pattern that transformation scripts expect
    filename = f"league_settings_{year}_{ctx.league_id}.json"
    output_path = output_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)

    if _verbose:
        log(f"Saved settings to: {output_path}")
    return output_path


def fetch_and_save_all_settings(
    ctx: SleeperContext, client: SleeperAPIClient | None = None
) -> dict[int, dict[str, Any]]:
    """
    Fetch and save settings for all years in context range.

    Args:
        ctx: SleeperContext with league_ids mapping
        client: Optional SleeperAPIClient

    Returns:
        Dict mapping year to settings
    """
    client = client or SleeperAPIClient()
    all_settings = {}

    for year in ctx.get_processing_years():
        league_id = ctx.get_league_id_for_year(year)
        if not league_id:
            log(f"No league ID for year {year}, skipping")
            continue

        try:
            settings = fetch_sleeper_settings(client, league_id, year)
            save_sleeper_settings(ctx, year, settings)
            all_settings[year] = settings

        except Exception as e:
            log(f"Error fetching settings for {year}: {e}")
            continue

    return all_settings


def load_sleeper_settings(ctx: SleeperContext, year: int) -> dict[str, Any] | None:
    """
    Load cached settings from file.

    Args:
        ctx: SleeperContext
        year: Season year

    Returns:
        Settings dict or None if not found
    """
    settings_dir = ctx.data_directory / "league_settings"

    # Try league-specific file first (standard naming pattern)
    league_id = ctx.get_league_id_for_year(year)
    if league_id:
        path = settings_dir / f"league_settings_{year}_{league_id}.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)

    # Try generic file with standard naming
    for path in settings_dir.glob(f"league_settings_{year}_*.json"):
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    return None


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper League Settings Fetcher")
    parser.add_argument("--league-id", required=True, help="Sleeper league ID")
    parser.add_argument("--output", help="Output JSON path")

    args = parser.parse_args()

    client = SleeperAPIClient()
    settings = fetch_sleeper_settings(client, args.league_id)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(settings, f, indent=2)
        print(f"Saved to: {args.output}")
    else:
        print(json.dumps(settings, indent=2))
