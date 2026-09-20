"""
ESPN Draft Fetcher

Fetches draft pick data from ESPN Fantasy API.
Works all years via league.draft attribute.

Detects draft_type per year: if 25%+ picks have bid_amount > 0 -> 'auction', else 'snake'.
Extracts keeper_status from Pick attributes.

Output columns match CanonicalDraftColumns for downstream pipeline compatibility.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Lazy-loaded NFL ID resolver
_espn_nfl_map = None


def _resolve_espn_nfl_id(espn_player_id: str) -> str | None:
    """Resolve espn_player_id → NFL_player_id via player_bio + legacy map."""
    global _espn_nfl_map
    if _espn_nfl_map is None:
        try:
            from multi_league.data_fetchers.shared.nfl_player_mapping import get_espn_to_nfl_map

            _espn_nfl_map = get_espn_to_nfl_map()
        except Exception:
            _espn_nfl_map = {}
    return _espn_nfl_map.get(str(espn_player_id))


def log(msg: str):
    logger.info(msg)
    print(msg)


def is_unfilled_espn_draft_pick(player_id: object, player_name: object = None) -> bool:
    """Return whether ESPN emitted an unused draft slot instead of a pick.

    ESPN uses negative player IDs for real D/ST selections, so only the exact
    numeric ID ``0`` is an empty drafted-season slot.  A populated name keeps
    the row fail-closed in case ESPN ever assigns zero to a real player.
    """
    try:
        zero_id = int(str(player_id).strip()) == 0
    except (TypeError, ValueError):
        return False
    name = str(player_name or "").strip().lower()
    return zero_id and name in {"", "unknown"}


# ESPN position ID -> position abbreviation
ESPN_POSITION_MAP = {
    1: "QB",
    2: "RB",
    3: "WR",
    4: "TE",
    5: "K",
    16: "DEF",  # D/ST
}


# Normalize ESPN position strings to canonical format
POSITION_NORMALIZE = {
    "D/ST": "DEF",
    "DST": "DEF",
    "DF": "DEF",
    "DEF": "DEF",
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "K": "K",
}


def _normalize_position(pos: str | None) -> str | None:
    """Normalize an ESPN position string to canonical format."""
    if not pos:
        return None
    return POSITION_NORMALIZE.get(pos.upper().strip(), pos.upper().strip())


def _build_position_map(league) -> dict:
    """Build player_id -> position lookup from league rosters and draft picks."""
    pos_map = {}
    for team in league.teams:
        for player in team.roster or []:
            pid = getattr(player, "playerId", None)
            pos = getattr(player, "position", None)
            if pid and pos:
                pos_map[pid] = _normalize_position(pos)

    # Also extract positions from draft pick player objects
    if league.draft:
        for pick in league.draft:
            pid = getattr(pick, "playerId", None)
            if pid and pid not in pos_map:
                player_obj = getattr(pick, "player", None)
                if player_obj:
                    pos = getattr(player_obj, "position", None) or getattr(player_obj, "defaultPosition", None)
                    if pos:
                        pos_map[pid] = _normalize_position(str(pos))

    return pos_map


def fetch_espn_draft(
    ctx: "ESPNContext",
    year: int,
    *,
    player_names_by_id: dict[str, str] | None = None,
    client=None,
    league=None,
) -> pd.DataFrame | None:
    """
    Fetch draft data for a single year.

    Args:
        ctx: ESPNContext with league_id and auth
        year: NFL season year

    Returns:
        DataFrame with draft picks, or None if no data
    """
    from .espn_api_client import ESPNAPIClient

    if client is None and league is None:
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)

    try:
        if league is None:
            league = client.get_league(year)
    except Exception as e:
        log(f"  [DRAFT] Failed to load league for {year}: {e}")
        return None

    if not league.draft:
        log(f"  [DRAFT] No draft data for {year}")
        return None

    # Build position lookup from team rosters
    pos_map = _build_position_map(league)

    picks = []
    for i, pick in enumerate(league.draft, 1):
        player_id = getattr(pick, "playerId", None)
        resolved_player_name = (
            getattr(pick, "playerName", None)
            or (player_names_by_id or {}).get(str(player_id))
        )
        if is_unfilled_espn_draft_pick(player_id, resolved_player_name):
            continue
        player_name = resolved_player_name or "Unknown"
        round_num = getattr(pick, "round_num", None)
        round_pick = getattr(pick, "round_pick", None)
        bid_amount = getattr(pick, "bid_amount", 0) or 0
        keeper_status = getattr(pick, "keeper_status", False)

        # Get team info
        team = getattr(pick, "team", None)
        team_id = team.team_id if team else None
        team_name = team.team_name if team else None

        # Get player position — try multiple sources
        # 1. Roster lookup (best for current-year players)
        position = pos_map.get(player_id)

        # 2. Pick object attributes (ESPN sometimes has position on the pick itself)
        if not position:
            pick_pos = getattr(pick, "position", None)
            if pick_pos:
                position = _normalize_position(pick_pos)

        # 3. ESPN position slot ID
        if not position:
            slot_id = getattr(pick, "slot", None) or getattr(pick, "lineupSlot", None)
            if slot_id and slot_id in ESPN_POSITION_MAP:
                position = ESPN_POSITION_MAP[slot_id]

        # 3b. ESPN eligible slots (array of position slot IDs the player can fill)
        if not position:
            eligible = getattr(pick, "eligibleSlots", None) or []
            for eslot_id in eligible:
                if eslot_id in ESPN_POSITION_MAP:
                    position = ESPN_POSITION_MAP[eslot_id]
                    break

        # 3c. defaultPositionId from player object
        if not position:
            player_obj = getattr(pick, "player", None) or pick
            default_pos = getattr(player_obj, "defaultPositionId", None)
            if default_pos and default_pos in ESPN_POSITION_MAP:
                position = ESPN_POSITION_MAP[default_pos]

        # 4. Player name pattern (D/ST detection)
        if not position and player_name and ("D/ST" in player_name or "DST" in player_name):
            position = "DEF"

        # 5. Normalize whatever we got (D/ST → DEF, etc.)
        if position:
            position = _normalize_position(position)

        # Get player NFL team
        nfl_team = None
        if hasattr(pick, "proTeam") and pick.proTeam:
            nfl_team = pick.proTeam

        # Resolve NFL_player_id via player_bio (canonical source)
        nfl_id = _resolve_espn_nfl_id(str(player_id)) if player_id else None

        pick_data = {
            "year": year,
            "pick": i,  # Overall pick number
            "round": round_num,
            "pick_in_round": round_pick,
            "draft_slot": None,  # ESPN doesn't provide draft slot directly
            "manager": ctx.get_manager_name(team_id, team_name=team_name or "", year=year) if team_id else "Unknown",
            "manager_guid": ctx.get_manager_guid(team_id, year=year) if team_id else "",
            "team_key": str(team_id) if team_id else "",
            "team_name": team_name or "",
            "franchise_id": ctx.get_franchise_id(team_id, year=year) if team_id else None,
            "espn_player_id": player_id,
            "NFL_player_id": nfl_id,
            "player": player_name,
            "position": position,
            "nfl_team": nfl_team,
            "cost": bid_amount,
            "is_keeper": 1 if keeper_status else 0,
            "platform": "espn",
            "league_id": str(ctx.get_league_id_for_year(year)),
        }
        picks.append(pick_data)

    if not picks:
        return None

    df = pd.DataFrame(picks)

    # Detect draft type per year
    if len(df) > 0:
        auction_picks = (df["cost"] > 0).sum()
        total_picks = len(df)
        if total_picks > 0 and (auction_picks / total_picks) >= 0.25:
            df["draft_type"] = "auction"
        else:
            df["draft_type"] = "snake"

    log(f"  [DRAFT] {year}: {len(df)} picks ({df['draft_type'].iloc[0] if len(df) > 0 else 'unknown'} draft)")

    return df


def fetch_all_espn_drafts(ctx: "ESPNContext") -> pd.DataFrame:
    """
    Fetch draft data for all years.

    Args:
        ctx: ESPNContext with year range

    Returns:
        Combined DataFrame for all years
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    years = list(ctx.get_year_range())
    log(f"\n{'='*60}")
    log(f"Fetching ESPN draft data ({len(years)} years)")
    log(f"{'='*60}")

    all_dfs = []

    with ThreadPoolExecutor(max_workers=min(3, len(years))) as executor:
        futures = {executor.submit(fetch_espn_draft, ctx, year): year for year in years}
        for future in as_completed(futures):
            year = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                log(f"  [DRAFT] {year}: FAILED - {e}")

    if not all_dfs:
        log("[DRAFT] No draft data found for any year")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    log(f"[DRAFT] Total: {len(combined)} draft picks across {len(all_dfs)} years")

    return combined
