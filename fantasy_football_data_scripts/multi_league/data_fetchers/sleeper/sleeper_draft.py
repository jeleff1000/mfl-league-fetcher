"""
Sleeper Draft Data Fetcher

Fetches draft pick data from Sleeper leagues.
Output schema matches draft_data_v2.py for downstream compatibility.

Output columns:
- year
- pick (overall pick number)
- round
- draft_slot (position in draft order - used for linking traded picks to conveyed players)
- draft_slot_roster_id (Sleeper roster_id that owns the draft-order slot)
- team_key (roster_id as string)
- manager, manager_guid
- sleeper_player_id
- cost (for auction drafts)
- player (player name)
- yahoo_position (position, named for compatibility)
- nfl_team
- is_keeper_status (boolean)
- draft_type (snake, auction)
- draft_id (Sleeper draft ID - unique per draft)
- draft_order (1st, 2nd, etc. draft of the year)
- draft_category (rookie, startup, veteran - auto-detected)
- player_year, manager_year (composite keys)

Note: Sleeper doesn't provide ADP/percent_drafted data, so these columns are null.

Dynasty Support:
- Captures ALL drafts per year (startup + rookie + supplemental)
- Auto-detects rookie drafts (>90% rookies by years_exp)
- draft_order helps distinguish multiple drafts in same year

Usage:
    from sleeper_draft import fetch_sleeper_draft
    from sleeper_context import SleeperContext

    ctx = SleeperContext.load("path/to/sleeper_context.json")
    df = fetch_sleeper_draft(ctx, year=2024)
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .sleeper_api_client import SleeperAPIClient
from .sleeper_player_cache import SleeperPlayerCache
from .sleeper_context import SleeperContext, YearFilter, resolve_years_to_fetch
from multi_league.core.date_utils import get_current_nfl_season_year

logger = logging.getLogger(__name__)

# Lazy-loaded NFL ID resolver
_sleeper_nfl_map = None


def _as_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _resolve_sleeper_nfl_id(sleeper_player_id: str) -> str | None:
    """Resolve sleeper_player_id → NFL_player_id via player_bio + legacy map."""
    global _sleeper_nfl_map
    if _sleeper_nfl_map is None:
        try:
            from multi_league.data_fetchers.shared.nfl_player_mapping import get_sleeper_to_nfl_map

            _sleeper_nfl_map = get_sleeper_to_nfl_map()
        except Exception:
            _sleeper_nfl_map = {}
    return _sleeper_nfl_map.get(str(sleeper_player_id))


def log(msg: str):
    """Simple logging function matching Yahoo script pattern."""
    logger.info(msg)
    print(msg)


class SleeperDraftFetcher:
    """
    Fetch draft data from Sleeper API.

    Includes draft picks, keeper status, and auction costs.
    Output matches Yahoo format for downstream compatibility.
    """

    def __init__(
        self,
        ctx: SleeperContext,
        client: SleeperAPIClient | None = None,
        player_cache: SleeperPlayerCache | None = None,
    ):
        """
        Initialize the draft fetcher.

        Args:
            ctx: SleeperContext with league configuration
            client: Optional pre-configured API client
            player_cache: Optional pre-loaded player cache
        """
        self.ctx = ctx
        self.client = client or SleeperAPIClient()
        self.player_cache = player_cache or SleeperPlayerCache(ctx.cache_directory)

        # Ensure player cache is loaded
        self.player_cache.refresh_if_stale(self.client)

    def _get_roster_mappings(self, league_id: str) -> dict[int, dict[str, str]]:
        """
        Build roster_id -> manager info mapping.

        Returns:
            Dict mapping roster_id to {manager_name, manager_guid, team_name}
        """
        rosters = self.client.get_league_rosters(league_id)
        users = self.client.get_league_users(league_id)

        # Build user_id -> display_name/team name mapping. Sleeper stores
        # custom team names on the user metadata, not the roster object.
        user_names = {}
        user_team_names = {}
        for user in users:
            user_id = user.get("user_id")
            display_name = user.get("display_name") or user.get("username", "Unknown")
            if user_id:
                user_names[user_id] = display_name
                metadata = user.get("metadata") or {}
                team_name = metadata.get("team_name")
                if team_name:
                    user_team_names[user_id] = str(team_name)

        # Build roster mappings
        roster_map = {}
        for roster in rosters:
            roster_id = roster.get("roster_id")
            owner_id = roster.get("owner_id")

            if roster_id is not None:
                manager_name = user_names.get(owner_id, "Unknown")

                # Apply overrides
                if manager_name in self.ctx.manager_name_overrides:
                    manager_name = self.ctx.manager_name_overrides[manager_name]

                # For deleted accounts: owner_id is None but roster_id is valid.
                # Use roster_id as synthetic GUID so franchise registry can create
                # a stable identity (matches the orp* pattern used in matchup fetchers).
                guid = owner_id or f"orp{str(roster_id).zfill(5)}"

                roster_map[roster_id] = {
                    "manager_name": manager_name,
                    "manager_guid": guid,
                    "team_name": user_team_names.get(owner_id),
                }

        return roster_map

    def _get_drafts_for_year(self, league_id: str, year: int) -> list[dict[str, Any]]:
        """
        Find ALL drafts for a specific year.

        Dynasty leagues can have multiple drafts per year:
        - Startup draft (Year 1) - All players eligible
        - Rookie draft (subsequent years) - Only rookies eligible
        - Supplemental drafts - Additional drafts mid-season

        Args:
            league_id: Sleeper league_id
            year: Season year

        Returns:
            List of draft dicts for the year (may be empty)
        """
        drafts = self.client.get_league_drafts(league_id)

        if not drafts:
            return []

        # Find ALL drafts matching the year
        year_drafts = []
        for draft in drafts:
            draft_season = draft.get("season")
            if draft_season and int(draft_season) == year:
                year_drafts.append(draft)

        # Fallback: if no drafts match the year but only one exists, use it
        # (handles leagues where season field might be missing)
        if not year_drafts and len(drafts) == 1:
            year_drafts = drafts

        return year_drafts

    def fetch_draft_manifest_for_year(self, year: int) -> pd.DataFrame:
        """Read the immutable identity of every active-season Sleeper draft pick.

        This is intentionally narrower than :meth:`fetch_draft_for_year`: a
        weekly refresh needs only ``(draft_id, pick)`` to prove that its
        hydrated source has every pick.  It avoids roster/player resolution on
        healthy refreshes and lets the caller escalate to the full fetch only
        when a provider pick is genuinely absent locally.
        """
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            return pd.DataFrame(columns=["draft_id", "pick"])

        rows: list[dict[str, object]] = []
        for draft in self._get_drafts_for_year(str(league_id), year):
            draft_id = str(draft.get("draft_id") or "").strip()
            if not draft_id:
                raise ValueError(f"Sleeper returned a {year} draft without draft_id")
            for pick in self.client.get_draft_picks(draft_id):
                pick_no = pick.get("pick_no")
                if pick_no is None:
                    raise ValueError(f"Sleeper returned a {year} draft pick without pick_no ({draft_id})")
                rows.append({"draft_id": draft_id, "pick": pick_no})

        return pd.DataFrame(rows, columns=["draft_id", "pick"])

    def _detect_draft_category(self, draft: dict[str, Any], picks: list[dict[str, Any]], draft_year: int) -> str:
        """
        Detect if a draft is a rookie draft, startup draft, or veteran draft.

        Primary signal: Sleeper's settings.player_type field:
          0 = all players (startup/veteran draft)
          1 = rookies only (rookie draft)

        Fallback: analyze picks' years_exp to determine rookie percentage.

        Args:
            draft: Draft metadata from API
            picks: List of picks from the draft
            draft_year: The year the draft occurred

        Returns:
            'rookie' - Only rookies drafted (players in their rookie year)
            'startup' - Mix of veterans and rookies (first year of league)
            'veteran' - Supplemental/slow draft with veterans
        """
        if not picks:
            return "unknown"

        # Primary: use Sleeper's player_type setting (authoritative)
        #   player_type=1 → rookies only (rookie draft)
        #   player_type=0 → all players (startup or supplemental veteran draft)
        # Startup detection: player_type=0 with many rounds (>10) = startup
        # Typical rookie drafts: 3-5 rounds; startups: 15-30+ rounds
        settings = draft.get("settings") or {}
        player_type = settings.get("player_type")
        if player_type is not None:
            if int(player_type) == 1:
                return "rookie"
            # player_type=0: startup vs veteran — use round count as heuristic
            num_rounds = settings.get("rounds", 0)
            if num_rounds > 10:
                return "startup"
            return "veteran"

        # Fallback: analyze pick composition using years_exp
        current_season = get_current_nfl_season_year()
        season_offset = current_season - draft_year

        rookie_count = 0
        veteran_count = 0

        for pick in picks:
            player_id = pick.get("player_id")
            if not player_id:
                continue

            # Prefer years_exp from pick metadata (always present in Sleeper picks)
            pick_metadata = pick.get("metadata") or {}
            years_exp_raw = pick_metadata.get("years_exp")

            if years_exp_raw is None:
                player_data = self.player_cache.get_player(str(player_id))
                if player_data:
                    years_exp_raw = player_data.get("years_exp", 0)

            if years_exp_raw is None:
                continue

            years_exp = int(years_exp_raw) if years_exp_raw else 0
            years_exp_at_draft = max(0, years_exp - season_offset)

            if years_exp_at_draft == 0:
                rookie_count += 1
            else:
                veteran_count += 1

        total = rookie_count + veteran_count
        if total == 0:
            return "unknown"

        rookie_pct = rookie_count / total

        if rookie_pct > 0.90:
            return "rookie"
        elif draft.get("draft_order") == 1:
            return "startup"
        else:
            return "veteran"

    def _get_draft_slot_roster_ids(
        self, draft: dict[str, Any], roster_map: dict[int, dict[str, str]]
    ) -> dict[int, int]:
        """
        Map Sleeper draft-order slots to original roster ids.

        Sleeper transaction ``draft_picks[].roster_id`` stores the original
        roster whose pick was traded. Sleeper draft pick rows store
        ``draft_slot`` as the draft-order slot, not that roster id. This
        explicit mapping keeps traded-pick conveyance from treating roster ids
        as draft slots.
        """
        draft_order = draft.get("draft_order") or {}
        if not isinstance(draft_order, dict):
            return {}

        guid_to_roster_ids: dict[str, list[int]] = {}
        for roster_id, info in roster_map.items():
            guid = str(info.get("manager_guid") or "").strip()
            if guid:
                guid_to_roster_ids.setdefault(guid, []).append(roster_id)

        slot_to_roster: dict[int, int] = {}
        for user_id, raw_slot in draft_order.items():
            slot = _as_int_or_none(raw_slot)
            if slot is None:
                continue
            roster_ids = guid_to_roster_ids.get(str(user_id).strip()) or []
            if len(roster_ids) == 1:
                slot_to_roster[slot] = roster_ids[0]

        return slot_to_roster

    def fetch_draft_for_year(self, year: int) -> pd.DataFrame:
        """
        Fetch draft data for a specific season.

        Handles multiple drafts per year (common in dynasty leagues):
        - Startup drafts (first year)
        - Rookie drafts (subsequent years)
        - Supplemental/slow drafts

        Args:
            year: Season year

        Returns:
            DataFrame with draft pick data from ALL drafts for the year
        """
        log(f"\n{'='*60}")
        log(f"Fetching Sleeper draft(s) for {year}")
        log(f"{'='*60}")

        # Get league ID for year
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            log(f"No league ID found for year {year}")
            return pd.DataFrame()

        log(f"League ID: {league_id}")

        # Get roster mappings
        roster_map = self._get_roster_mappings(league_id)
        log(f"Found {len(roster_map)} teams")

        # Find ALL drafts for this year
        drafts = self._get_drafts_for_year(league_id, year)
        if not drafts:
            log(f"No drafts found for year {year}")
            return pd.DataFrame()

        log(f"Found {len(drafts)} draft(s) for {year}")

        # Process each draft
        all_picks = []

        for draft_order, draft in enumerate(drafts, start=1):
            draft_id = draft.get("draft_id")
            draft_type = draft.get("type", "snake")  # snake or auction
            draft_status = draft.get("status", "complete")

            log(f"\n  Draft {draft_order}/{len(drafts)}: {draft_id}")
            log(f"    Type: {draft_type}, Status: {draft_status}")

            # For auction drafts, get slot_to_roster_id mapping
            slot_to_roster = draft.get("slot_to_roster_id", {})
            draft_slot_roster_ids = self._get_draft_slot_roster_ids(draft, roster_map)

            # Fetch all picks for this draft
            picks = self.client.get_draft_picks(draft_id)

            if not picks:
                log("    No picks found")
                continue

            log(f"    Found {len(picks)} picks")

            # Detect draft category (rookie vs startup vs veteran)
            draft_category = self._detect_draft_category(draft, picks, year)
            log(f"    Category: {draft_category}")

            # Parse picks
            for pick in picks:
                pick_no = pick.get("pick_no", 0)
                round_num = pick.get("round", 1)
                draft_slot = pick.get("draft_slot", 1)
                draft_slot_int = _as_int_or_none(draft_slot)
                player_id = pick.get("player_id")
                roster_id = pick.get("roster_id")
                is_keeper = pick.get("is_keeper", False)
                roster_id_int = _as_int_or_none(roster_id)
                if roster_id_int is None and slot_to_roster:
                    mapped_roster_id = slot_to_roster.get(str(draft_slot), slot_to_roster.get(draft_slot))
                    roster_id_int = _as_int_or_none(mapped_roster_id)
                    if roster_id_int is None:
                        roster_id_int = _as_int_or_none(slot_to_roster.get(""))

                # Get metadata (contains auction amount)
                metadata = pick.get("metadata", {}) or {}
                amount = metadata.get("amount")  # Auction cost

                if not player_id:
                    continue

                # Get manager info
                manager_info = roster_map.get(roster_id_int, {})
                manager_name = manager_info.get("manager_name", "Unknown")
                manager_guid = manager_info.get("manager_guid", "")
                team_name = manager_info.get("team_name")

                # Get player info from cache
                player_name = self.player_cache.get_player_name(str(player_id))
                position = self.player_cache.get_player_position(str(player_id))
                nfl_team = self.player_cache.get_player_team(str(player_id))

                # Handle DEF/DST picks - Sleeper uses team abbreviations as player_id
                # e.g., "PIT", "NE", "KC" for team defenses
                player_id_str = str(player_id).upper()
                if position == "Unknown" and player_id_str in self.player_cache.VALID_NFL_TEAMS:
                    # This is a team defense pick
                    position = "DEF"
                    nfl_team = player_id_str
                    # Get full team name and convert to DST format
                    team_full_name = self.player_cache.TEAM_ABBREV_TO_NAME.get(player_id_str, player_id_str)
                    short_name = self.player_cache.NFL_TEAM_SHORT_NAMES.get(team_full_name, player_id_str)
                    player_name = f"{short_name} DST"

                # Resolve NFL_player_id via player_bio (canonical source)
                nfl_id = _resolve_sleeper_nfl_id(str(player_id))

                pick_data = {
                    "year": year,
                    "pick": pick_no,
                    "round": round_num,
                    "draft_slot": draft_slot,  # Position in draft order (used for linking traded picks)
                    "draft_slot_roster_id": draft_slot_roster_ids.get(draft_slot_int),
                    "team_key": str(roster_id_int) if roster_id_int is not None else "",
                    "manager": manager_name,
                    "manager_guid": manager_guid,
                    "team_name": team_name,
                    "sleeper_player_id": str(player_id),
                    "NFL_player_id": nfl_id,
                    "cost": int(amount) if amount else None,
                    "player": player_name,
                    "yahoo_position": position,  # Named for compatibility
                    "nfl_position": position,
                    "nfl_team": nfl_team,
                    "is_keeper_status": is_keeper,
                    "draft_type": draft_type,
                    # Dynasty-specific fields
                    "draft_id": draft_id,
                    "draft_order": draft_order,  # 1st draft, 2nd draft, etc.
                    "draft_category": draft_category,  # 'rookie', 'startup', 'veteran'
                    # Sleeper doesn't provide ADP data
                    "avg_pick": None,
                    "avg_round": None,
                    "avg_cost": None,
                    "percent_drafted": None,
                    "preseason_avg_pick": None,
                    "preseason_avg_round": None,
                    "preseason_avg_cost": None,
                    "preseason_percent_drafted": None,
                }

                all_picks.append(pick_data)

        if not all_picks:
            log("No valid picks found across all drafts")
            return pd.DataFrame()

        # Create DataFrame
        df = pd.DataFrame(all_picks)

        # CRITICAL: Ensure sleeper_player_id is clean string without .0 suffix
        # Float-to-string conversion adds .0 suffix (e.g., 10790962.0 -> "10790962.0")
        if "sleeper_player_id" in df.columns:
            df["sleeper_player_id"] = df["sleeper_player_id"].astype(str).str.replace(r"\.0$", "", regex=True)

        # Sort by draft_order, then pick number
        df = df.sort_values(["draft_order", "pick"])

        # Add composite keys
        df["player_year"] = df["sleeper_player_id"] + "_" + df["year"].astype(str)
        df["manager_year"] = df["manager"] + "_" + df["year"].astype(str)

        # Summary stats
        log(f"\n{'='*60}")
        log(f"Draft Summary for {year}")
        log(f"{'='*60}")
        log(f"Total picks: {len(df)}")
        log(f"  Drafts: {df['draft_order'].nunique()}")
        log(f"  Keepers: {df['is_keeper_status'].sum()}")

        # Show breakdown by draft category
        for category in df["draft_category"].unique():
            cat_df = df[df["draft_category"] == category]
            log(f"  {category.title()} draft(s): {len(cat_df)} picks")

        if "auction" in df["draft_type"].values and df["cost"].notna().any():
            log(f"  Total spent: ${df['cost'].sum():.0f}")

        return df


def fetch_sleeper_draft(
    ctx: SleeperContext,
    year: int,
    client: SleeperAPIClient | None = None,
    player_cache: SleeperPlayerCache | None = None,
    db=None,
) -> pd.DataFrame:
    """
    Main entry point for fetching Sleeper draft data.

    Args:
        ctx: SleeperContext with league configuration
        year: Season year to fetch
        client: Optional pre-configured API client
        player_cache: Optional pre-loaded player cache
        db: LocalLeagueDB instance (required).

    Returns:
        DataFrame with draft data
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    fetcher = SleeperDraftFetcher(ctx, client, player_cache)

    df = fetcher.fetch_draft_for_year(year)

    if df.empty:
        return df

    # Save output
    league_id = ctx.get_league_id_for_year(year) or ctx.league_id
    db.save_table("draft", df, year=year, platform="sleeper", league_id=str(league_id))
    log(f"\n  [LocalDB] draft year={year}: {len(df):,} rows")

    return df


def fetch_all_sleeper_drafts(
    ctx: SleeperContext,
    client: SleeperAPIClient | None = None,
    player_cache: SleeperPlayerCache | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> dict[int, pd.DataFrame]:
    """
    Fetch draft data for all years in context range.

    Args:
        ctx: SleeperContext with league configuration
        client: Optional pre-configured API client
        player_cache: Optional pre-loaded player cache
        year_filter: If set, only fetch this year or these years (for quick imports)
        db: LocalLeagueDB instance (required).

    Returns:
        Dict mapping year to DataFrame
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    results = {}

    # If year_filter is set, only fetch that year/those years (quick import mode).
    # Otherwise prefer discovered league_ids over the payload start/end window.
    years_to_fetch = resolve_years_to_fetch(ctx, year_filter)

    for year in years_to_fetch:
        df = fetch_sleeper_draft(ctx=ctx, year=year, client=client, player_cache=player_cache, db=db)

        if not df.empty:
            results[year] = df

    return results


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper Draft Data Fetcher")
    parser.add_argument("--context", required=True, help="Path to sleeper_context.json")
    parser.add_argument("--year", type=int, help="Specific year to fetch")
    parser.add_argument("--all-years", action="store_true", help="Fetch all years")

    args = parser.parse_args()

    ctx = SleeperContext.load(Path(args.context))

    if args.all_years:
        results = fetch_all_sleeper_drafts(ctx)
        for year, df in results.items():
            print(f"Year {year}: {len(df)} picks")
    elif args.year:
        df = fetch_sleeper_draft(ctx, args.year)
        print(f"\nFetched {len(df)} picks")
        if not df.empty:
            print(df.head())
    else:
        # Default to current NFL season year
        current_year = get_current_nfl_season_year()
        df = fetch_sleeper_draft(ctx, current_year)
        print(f"\nFetched {len(df)} picks")
