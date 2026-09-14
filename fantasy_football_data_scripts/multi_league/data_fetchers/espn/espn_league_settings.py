"""
ESPN League Settings Fetcher

Fetches and persists league settings per year (scoring rules, roster slots,
playoff config). Pattern matches sleeper_league_settings.py.

Settings include: scoring_type, roster_slots, playoff_teams, playoff_start_week,
regular_season_length, keeper_count, faab_budget, trade_deadline, scoring_rules.
"""

import json
import logging

from multi_league.core.espn_playoff_settings import derive_espn_playoff_metadata
from multi_league.core.roster_slots import resolve as resolve_position
from multi_league.core.scoring_config import canonical_keys_for_espn_stat_id

logger = logging.getLogger(__name__)


def log(msg: str):
    logger.info(msg)
    print(msg)


# ESPN roster slot ID -> canonical position name
ESPN_SLOT_ID_TO_POSITION = {
    0: "QB",
    2: "RB",
    3: "RB/WR",
    4: "WR",
    5: "WR/TE",
    6: "TE",
    7: "OP",  # QB/RB/WR/TE (SUPER_FLEX)
    8: "DT",  # IDP
    9: "DE",  # IDP
    10: "LB",  # IDP
    11: "DL",  # IDP DL flex
    12: "CB",  # IDP
    13: "S",  # IDP safety
    14: "DB",  # IDP DB flex
    15: "DP",  # IDP flex
    16: "D/ST",
    17: "K",
    20: "BN",
    21: "IR",
    22: "BN",
    23: "RB/WR/TE",
}


def _normalize_roster_position_counts(raw_slots: dict) -> dict[str, int]:
    """Canonicalize ESPN slot names via the shared roster-slot resolver."""
    normalized_counts: dict[str, int] = {}
    for pos, count in raw_slots.items():
        if not isinstance(count, int | float) or count <= 0:
            continue
        canonical = resolve_position(str(pos))
        normalized_counts[canonical] = normalized_counts.get(canonical, 0) + int(count)
    return normalized_counts


def _process_scoring_format(scoring_format: list, scoring_settings: dict) -> None:
    """Map ESPN stat IDs to canonical scoring keys via scoring_config.ESPN_STAT_ID_MAP.

    Stat IDs not present in the canonical map are silently dropped here.
    They will be caught by the canonicalization-layer silent_drop_logger
    if they ever surface as a populated key downstream.
    """
    for scoring_item in scoring_format:
        stat_id = scoring_item.get("id", scoring_item.get("statId"))
        points = scoring_item.get("value", scoring_item.get("points", 0))
        if stat_id is not None:
            try:
                numeric_stat_id = int(stat_id)
            except (TypeError, ValueError):
                continue
            for canonical_key in canonical_keys_for_espn_stat_id(numeric_stat_id):
                scoring_settings[canonical_key] = points


def fetch_espn_settings(ctx: "ESPNContext", year: int, *, client=None, league=None) -> dict:
    """
    Fetch league settings from ESPN for a specific year.

    Args:
        ctx: ESPNContext with league_id and auth
        year: NFL season year

    Returns:
        Dict with normalized league settings
    """
    from .espn_api_client import ESPNAPIClient

    if client is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [SETTINGS] Failed to load league for {year}: {e}")
        return {}
    raw_settings = client.get_league_settings_raw(year)
    raw_schedule_settings = {}
    raw_scoring_settings = {}
    if isinstance(raw_settings, dict):
        raw_schedule_settings = raw_settings.get("scheduleSettings") or {}
        raw_scoring_settings = raw_settings.get("scoringSettings") or {}

    settings = {}

    # Basic league info
    settings["year"] = year
    settings["league_id"] = ctx.get_league_id_for_year(year)
    settings["league_name"] = getattr(league, "settings", None) and league.settings.name or ctx.league_name

    # Team count
    settings["num_teams"] = len(league.teams) if league.teams else ctx.num_teams

    # Scoring type detection
    # ESPN doesn't expose median scoring directly; check league settings
    scoring_settings = {}
    if hasattr(league, "settings"):
        s = league.settings

        # Roster slots - ESPN uses position_slot_counts (position name -> count dict)
        if hasattr(s, "position_slot_counts"):
            raw_slots = s.position_slot_counts  # e.g. {'QB': 1, 'RB': 2, 'WR': 3, 'RB/WR/TE': 1, 'D/ST': 1, ...}
            settings["roster_slots"] = dict(raw_slots)
            settings["roster_position_counts"] = _normalize_roster_position_counts(raw_slots)
        elif hasattr(s, "roster"):
            # Fallback to old slot ID format (older espn_api versions)
            roster_slots = {}
            for slot_id, count in s.roster.items():
                pos_name = ESPN_SLOT_ID_TO_POSITION.get(slot_id, f"SLOT_{slot_id}")
                roster_slots[pos_name] = count
            settings["roster_slots"] = roster_slots
            settings["roster_position_counts"] = _normalize_roster_position_counts(roster_slots)

        # Playoff configuration. ESPN can return playoff_team_count=0 for
        # no-playoff seasons and playoff_matchup_period_length=0 when
        # matchup_periods carries per-round lengths.
        matchup_periods = getattr(s, "matchup_periods", None)
        if isinstance(matchup_periods, dict):
            settings["matchup_periods"] = dict(matchup_periods)
        playoff_meta = derive_espn_playoff_metadata(
            playoff_teams=getattr(s, "playoff_team_count", None),
            regular_season_length=getattr(s, "reg_season_count", None),
            playoff_matchup_period_length=getattr(s, "playoff_matchup_period_length", None),
            matchup_periods=matchup_periods,
        )
        settings.update(playoff_meta)

        # Keeper settings
        settings["keeper_count"] = getattr(s, "keeper_count", 0)

        # FAAB
        settings["faab"] = getattr(s, "faab", False)
        settings["acquisition_budget"] = getattr(s, "acquisition_budget", 0)

        # Trade deadline
        settings["trade_deadline"] = getattr(s, "trade_deadline", None)

        # Scoring rules (stat_id -> canonical points per unit)
        if hasattr(s, "scoring_format"):
            _process_scoring_format(s.scoring_format, scoring_settings)

    # ESPN pre-2021 scoring_format responses don't always include a rec
    # (PPR) stat entry, even for standard 0-PPR leagues. The absence in
    # the raw API is semantically "not set" which is the same as 0 for
    # the PPR value, so default it explicitly — otherwise the flat DDL
    # ends up with scoring_rec=NULL and settings_scoring_populated flags
    # every pre-2021 ESPN year as a pipeline miss. Same reasoning
    # applies to rush_yd / pass_yd / pass_td, which tfl 2012 is missing
    # entirely: default them to their NFL-standard values (1pt/10yd rush,
    # 1pt/25yd pass, 4pt pass TD) so downstream consumers aren't tripped
    # by NULLs on a year where ESPN's API returned nothing.
    if "rec" not in scoring_settings:
        scoring_settings["rec"] = 0.0
    if "rush_yd" not in scoring_settings:
        scoring_settings["rush_yd"] = 0.1
    if "pass_yd" not in scoring_settings:
        scoring_settings["pass_yd"] = 0.04
    if "pass_td" not in scoring_settings:
        scoring_settings["pass_td"] = 4.0

    settings["scoring_settings"] = scoring_settings
    settings["playoff_bracket_source"] = "api"
    settings["playoff_seeding_rule"] = raw_schedule_settings.get("playoffSeedingRule")
    settings["playoff_seeding_rule_by"] = raw_schedule_settings.get("playoffSeedingRuleBy")
    settings["home_team_bonus"] = raw_scoring_settings.get("homeTeamBonus")
    settings["playoff_home_team_bonus"] = raw_scoring_settings.get("playoffHomeTeamBonus")
    settings["matchup_tie_rule"] = raw_scoring_settings.get("matchupTieRule")
    settings["matchup_tie_rule_by"] = raw_scoring_settings.get("matchupTieRuleBy")
    settings["playoff_matchup_tie_rule"] = raw_scoring_settings.get("playoffMatchupTieRule")
    settings["playoff_matchup_tie_rule_by"] = raw_scoring_settings.get("playoffMatchupTieRuleBy")

    # Detect scoring type from scoring settings
    rec_points = scoring_settings.get("rec", 0)
    if rec_points >= 1.0:
        settings["scoring_type"] = "ppr"
    elif rec_points > 0:
        settings["scoring_type"] = "half_ppr"
    else:
        settings["scoring_type"] = "standard"

    # Detect draft type from draft data
    try:
        if league.draft:
            auction_picks = sum(1 for pick in league.draft if getattr(pick, "bid_amount", 0) > 0)
            total_picks = len(league.draft)
            if total_picks > 0 and (auction_picks / total_picks) >= 0.25:
                settings["draft_type"] = "auction"
            else:
                settings["draft_type"] = "snake"
        else:
            settings["draft_type"] = "unknown"
    except Exception:
        settings["draft_type"] = "unknown"

    try:
        from multi_league.core.scoring_config import normalize_espn

        settings["canonical_scoring"] = normalize_espn(settings.get("scoring_settings", {}))
    except Exception:
        pass

    # Build canonical metadata dict for cross-platform compatibility.
    # ESPN does NOT expose reseeding.
    settings["metadata"] = {
        "num_playoff_teams": settings.get("num_playoff_teams"),
        "playoff_start_week": settings.get("playoff_start_week"),
        "playoff_round_type": settings.get("playoff_round_type"),
        "uses_reseeding": None,  # ESPN doesn't expose reseeding
        "num_teams": settings.get("num_teams") or 10,
        "bye_teams": settings.get("bye_teams", 0),
        "num_rounds": settings.get("num_rounds"),
        "end_week": settings.get("end_week"),
        "championship_week": settings.get("championship_week"),
        "has_multiweek_championship": settings.get("has_multiweek_championship", 0),
        "start_week": 1,
        "scoring_type": settings.get("scoring_type"),
        "draft_type": settings.get("draft_type"),
        "name": settings.get("league_name"),
    }

    return settings


def save_espn_settings(settings: dict, ctx: "ESPNContext", year: int):
    """
    Save league settings to JSON file.

    Args:
        settings: Settings dict from fetch_espn_settings
        ctx: ESPNContext
        year: NFL season year
    """
    settings_dir = ctx.data_directory / "league_settings"
    settings_dir.mkdir(parents=True, exist_ok=True)

    # Use standard league_settings naming convention for cross-platform compatibility
    path = settings_dir / f"league_settings_{year}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, default=str)

    log(f"  [SETTINGS] Saved settings for {year} to {path}")


def load_espn_settings(ctx: "ESPNContext", year: int) -> dict | None:
    """
    Load cached league settings from file.

    Args:
        ctx: ESPNContext
        year: NFL season year

    Returns:
        Settings dict or None if not cached
    """
    # Try standard naming first, fall back to legacy ESPN naming
    path = ctx.data_directory / "league_settings" / f"league_settings_{year}.json"
    if not path.exists():
        path = ctx.data_directory / "league_settings" / f"settings_{year}.json"
    if not path.exists():
        return None

    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load settings for {year}: {e}")
        return None


def fetch_and_save_all_settings(ctx: "ESPNContext", years: list[int] | None = None) -> dict[int, dict]:
    """
    Fetch and save settings for specified years (or all years from context).

    Args:
        ctx: ESPNContext with year range
        years: Explicit list of years to fetch. If None, uses ctx.get_year_range().

    Returns:
        Dict mapping year -> settings
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    target_years = list(years or ctx.get_year_range())
    all_settings = {}

    if not target_years:
        log("  [SETTINGS] No target years; skipping settings fetch")
        return all_settings

    def _fetch_one(yr):
        s = fetch_espn_settings(ctx, yr)
        if s:
            save_espn_settings(s, ctx, yr)
        return (yr, s)

    with ThreadPoolExecutor(max_workers=min(5, len(target_years))) as executor:
        futures = {executor.submit(_fetch_one, yr): yr for yr in target_years}
        for f in as_completed(futures):
            yr, s = f.result()
            if s:
                all_settings[yr] = s

    log(f"  [SETTINGS] Fetched {len(all_settings)}/{len(target_years)} years")
    _adopt_api_league_name(ctx, all_settings)
    return all_settings


def _adopt_api_league_name(ctx: "ESPNContext", all_settings: dict[int, dict]) -> None:
    """Adopt ESPN's own league name (see core.league_name_sync for the hazard)."""
    from multi_league.core.league_name_sync import adopt_from_yearly_settings

    adopt_from_yearly_settings(ctx, all_settings, key="league_name", log=log)
