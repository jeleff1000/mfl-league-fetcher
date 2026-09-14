"""
Sleeper Traded Picks Data Fetcher

Fetches traded draft pick data from Sleeper API.
This captures draft picks that have been traded between managers.

The /league/{league_id}/traded_picks endpoint returns all traded picks
for the league, showing who originally owned the pick and who currently owns it.

Output schema:
- league_id: Sleeper league ID
- season: Year the pick is for (e.g., "2026")
- round: Draft round number
- original_owner: Manager who gave away the pick
- original_owner_guid: User ID of original owner
- current_owner: Manager who currently owns the pick
- current_owner_guid: User ID of current owner
- roster_id: Current roster_id holding the pick

Usage:
    from sleeper_traded_picks import fetch_sleeper_traded_picks
    from sleeper_context import SleeperContext

    ctx = SleeperContext.load("path/to/sleeper_context.json")
    df = fetch_sleeper_traded_picks(ctx)
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .sleeper_api_client import SleeperAPIClient
from .sleeper_context import SleeperContext, YearFilter, resolve_years_to_fetch

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function matching Yahoo script pattern."""
    logger.info(msg)
    print(msg)


class SleeperTradedPicksFetcher:
    """
    Fetch traded draft pick data from Sleeper API.

    The /league/{league_id}/traded_picks endpoint returns:
    [{
        'round': 1,
        'season': '2026',
        'roster_id': 1,          # Who currently owns the pick
        'owner_id': 2,           # User who owns the pick
        'previous_owner_id': 1   # User who gave away the pick
    }]
    """

    def __init__(self, ctx: SleeperContext, client: SleeperAPIClient | None = None):
        """
        Initialize the traded picks fetcher.

        Args:
            ctx: SleeperContext with league configuration
            client: Optional pre-configured API client
        """
        self.ctx = ctx
        self.client = client or SleeperAPIClient()

    def _get_user_mappings(self, league_id: str) -> dict[int, dict[str, str]]:
        """
        Build roster_id -> manager info mapping.

        Note: The traded_picks API uses roster_id as owner_id/previous_owner_id,
        NOT user_id. So we need to map roster_id -> manager display_name.

        Returns:
            Dict mapping roster_id (int) to {display_name, user_id}
        """
        rosters = self.client.get_league_rosters(league_id)
        users = self.client.get_league_users(league_id)

        # Build user_id -> display_name mapping first
        user_names = {}
        for user in users:
            user_id = user.get("user_id")
            if user_id:
                display_name = user.get("display_name") or user.get("username", "Unknown")

                # Apply overrides
                if display_name in self.ctx.manager_name_overrides:
                    display_name = self.ctx.manager_name_overrides[display_name]

                user_names[user_id] = display_name

        # Build roster_id -> manager info mapping
        # This is what we need for traded picks lookup
        roster_info = {}
        for roster in rosters:
            roster_id = roster.get("roster_id")
            owner_id = roster.get("owner_id")
            if roster_id is not None:
                display_name = user_names.get(owner_id, "Unknown")
                roster_info[roster_id] = {
                    "display_name": display_name,
                    "user_id": owner_id or "",
                }

        return roster_info

    def _parse_traded_pick(
        self, pick: dict[str, Any], league_id: str, roster_map: dict[int, dict[str, str]]
    ) -> dict[str, Any]:
        """
        Parse a single traded pick into output row.

        Args:
            pick: Raw traded pick from API
            league_id: Sleeper league ID
            roster_map: roster_id -> manager info mapping

        Returns:
            Dict with traded pick data
        """
        season = pick.get("season", "")
        round_num = pick.get("round", 0)
        roster_id = pick.get("roster_id")

        # Note: API returns owner_id and previous_owner_id as ROSTER IDs, not user IDs
        current_roster_id = pick.get("owner_id")  # This is actually a roster_id
        previous_roster_id = pick.get("previous_owner_id")  # This is actually a roster_id

        # Get current owner info using roster_id
        current_info = roster_map.get(current_roster_id, {})
        current_owner = current_info.get("display_name", "Unknown")
        current_user_id = current_info.get("user_id", "")

        # Get previous owner info using roster_id
        previous_info = roster_map.get(previous_roster_id, {})
        original_owner = previous_info.get("display_name", "Unknown")
        original_user_id = previous_info.get("user_id", "")

        return {
            "league_id": league_id,
            "season": season,
            "round": round_num,
            "original_owner": original_owner,
            "original_owner_guid": original_user_id,
            "current_owner": current_owner,
            "current_owner_guid": current_user_id,
            "roster_id": roster_id,
        }

    def fetch_traded_picks_for_league(self, league_id: str, year: int) -> pd.DataFrame:
        """
        Fetch all traded picks for a specific league.

        Args:
            league_id: Sleeper league ID
            year: Season year (for context/logging)

        Returns:
            DataFrame with traded pick data
        """
        log(f"\n{'='*60}")
        log(f"Fetching Sleeper traded picks for {year} (league: {league_id})")
        log(f"{'='*60}")

        # Get roster -> manager mappings for this league
        roster_map = self._get_user_mappings(league_id)
        log(f"Found {len(roster_map)} roster mappings")

        # Fetch traded picks
        try:
            traded_picks = self.client.get_league_traded_picks(league_id)

            if not traded_picks:
                log("No traded picks found")
                return pd.DataFrame()

            log(f"Found {len(traded_picks)} traded picks")

            # Parse each pick
            rows = []
            for pick in traded_picks:
                row = self._parse_traded_pick(pick, league_id, roster_map)
                rows.append(row)

            # Create DataFrame
            df = pd.DataFrame(rows)

            # Add source year column (the year we fetched from, may differ from season)
            df["source_year"] = year

            # Sort by season and round
            df = df.sort_values(["season", "round"], ascending=[True, True])

            log("\nTraded picks by season:")
            for season, count in df.groupby("season").size().items():
                log(f"  {season}: {count} picks")

            return df

        except Exception as e:
            log(f"Error fetching traded picks: {e}")
            return pd.DataFrame()

    def fetch_all_traded_picks(self) -> pd.DataFrame:
        """
        Fetch traded picks from all years in context range.

        Returns:
            Combined DataFrame with all traded picks
        """
        all_dfs = []

        for year in self.ctx.get_processing_years():
            league_id = self.ctx.get_league_id_for_year(year)
            if not league_id:
                log(f"No league ID found for year {year}")
                continue

            df = self.fetch_traded_picks_for_league(league_id, year)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            log("No traded picks found across all years")
            return pd.DataFrame()

        # Combine all years
        combined = pd.concat(all_dfs, ignore_index=True)

        # Deduplicate by (league_id, season, round, original_owner, current_owner)
        # A pick can only be traded once between the same two owners
        combined = combined.drop_duplicates(
            subset=["league_id", "season", "round", "original_owner_guid", "current_owner_guid"]
        )

        log(f"\nTotal unique traded picks: {len(combined)}")

        return combined


def fetch_sleeper_traded_picks(
    ctx: SleeperContext,
    client: SleeperAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    """
    Main entry point for fetching Sleeper traded picks data.

    Args:
        ctx: SleeperContext with league configuration
        client: Optional pre-configured API client
        year_filter: If set, only fetch traded picks for this year or these years
        db: LocalLeagueDB instance (required).

    Returns:
        DataFrame with traded picks data
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    fetcher = SleeperTradedPicksFetcher(ctx, client)

    if year_filter is not None:
        dfs = []
        for year in resolve_years_to_fetch(ctx, year_filter):
            league_id = ctx.get_league_id_for_year(year)
            if league_id:
                year_df = fetcher.fetch_traded_picks_for_league(league_id, year)
                if year_df is not None and not year_df.empty:
                    dfs.append(year_df)
            else:
                log(f"No league ID found for year {year}")
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    else:
        df = fetcher.fetch_all_traded_picks()

    if df.empty:
        return df

    # Save output
    db.save_table("traded_picks", df)
    log(f"\n  [LocalDB] traded_picks: {len(df):,} rows")

    return df


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper Traded Picks Data Fetcher")
    parser.add_argument("--context", required=True, help="Path to sleeper_context.json")

    args = parser.parse_args()

    ctx = SleeperContext.load(Path(args.context))

    df = fetch_sleeper_traded_picks(ctx)
    print(f"\nFetched {len(df)} traded picks")
    if not df.empty:
        print(df)
