"""
Sleeper Roster Data Fetcher

Fetches weekly roster data showing which manager owned which player each week.
Output schema matches yahoo_fantasy_data.py for downstream compatibility.

Output columns:
- year, week
- manager (display name)
- manager_guid (user_id)
- team_key (roster_id as string, for compatibility)
- sleeper_player_id (Sleeper's numeric ID)
- player (player name)
- nfl_team
- yahoo_position (position from Sleeper, renamed for compatibility)
- fantasy_position (starter slot or BN)
- eligible_positions
- points (fantasy points for the week)

Usage:
    from sleeper_rosters import fetch_sleeper_rosters
    from sleeper_context import SleeperContext

    ctx = SleeperContext.load("path/to/sleeper_context.json")
    df = fetch_sleeper_rosters(ctx, year=2024)
"""

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_verbose = "--verbose" in sys.argv

from .sleeper_api_client import SleeperAPIClient
from .sleeper_player_cache import SleeperPlayerCache
from .sleeper_context import SleeperContext, YearFilter, resolve_years_to_fetch
from .sleeper_data_normalizer import ABBREV_TO_FRANCHISE_ID as _ABBREV_TO_FRANCHISE_ID
from .sleeper_roster_identity import build_year_roster_map

from multi_league.core.canonical_roster import normalize_roster_df

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function matching Yahoo script pattern."""
    logger.info(msg)
    print(msg)


class SleeperRosterFetcher:
    """
    Fetch weekly roster data from Sleeper API.

    Shows which manager had which player in which roster slot each week,
    with output matching Yahoo format for downstream compatibility.
    """

    def __init__(
        self,
        ctx: SleeperContext,
        client: SleeperAPIClient | None = None,
        player_cache: SleeperPlayerCache | None = None,
    ):
        """
        Initialize the roster fetcher.

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

    def _get_roster_mappings_from_matchup(self, year: int, db=None) -> dict[int, dict[str, str]]:
        """
        Build roster_id -> manager info mapping from existing matchup data FOR A SPECIFIC YEAR.

        This is preferred over current API state because matchup data has
        historical manager names even if managers have since left the league.

        IMPORTANT: roster_id -> manager mapping is YEAR-SPECIFIC because Sleeper
        creates a new league each year, and roster_ids can map to different owners
        across years. We MUST filter by year to get correct mappings.

        Reads matchup data from the local DuckDB (saved by the matchup fetcher).

        Args:
            year: The season year to get mappings for
            db: Optional LocalLeagueDB instance to read matchup data from

        Returns:
            Dict mapping roster_id to {manager_name, manager_guid}, or empty dict if no matchup data
        """
        if db is None:
            return {}

        try:
            conn = db.connect()
            matchup_df = conn.execute(
                "SELECT DISTINCT manager, manager_guid, team_key FROM public.matchup WHERE year = ?",
                [year],
            ).fetchdf()
            if matchup_df.empty:
                log(f"  No matchup data in local DB for year {year}")
                return {}
            log(f"  Found matchup data in local DB for year {year}: {len(matchup_df):,} rows")
        except Exception as e:
            log(f"  [WARN] Could not read matchup data from local DB: {e}")
            return {}

        try:
            # Need manager, manager_guid, and team_key (which is roster_id for Sleeper)
            required_cols = ["manager", "team_key"]
            if not all(col in matchup_df.columns for col in required_cols):
                log("  [WARN] Matchup data missing required columns for roster mapping")
                return {}

            roster_map = {}
            # Group by team_key to get unique roster_id -> manager mappings
            cols_to_use = ["manager", "team_key"]
            if "manager_guid" in matchup_df.columns:
                cols_to_use.append("manager_guid")

            for _, row in matchup_df[cols_to_use].drop_duplicates().iterrows():
                team_key = row.get("team_key")
                manager = row.get("manager")
                guid = row.get("manager_guid", "") if "manager_guid" in row else ""

                if team_key is not None and manager and manager != "Unknown":
                    try:
                        roster_id = int(team_key)
                        # Apply overrides
                        if manager in self.ctx.manager_name_overrides:
                            manager = self.ctx.manager_name_overrides[manager]

                        # Only add if not already present (first occurrence wins)
                        if roster_id not in roster_map:
                            roster_map[roster_id] = {
                                "manager_name": manager,
                                "manager_guid": str(guid) if pd.notna(guid) else "",
                            }
                    except (ValueError, TypeError):
                        continue

            if roster_map:
                log(f"  Built roster map from matchup data: {len(roster_map)} teams")

            return roster_map

        except Exception as e:
            log(f"  [WARN] Could not read matchup data for roster mapping: {e}")
            return {}

    def _get_roster_mappings_from_manifest(self, year: int) -> dict[int, dict[str, str]]:
        """
        Load roster_id -> manager mappings from the manager manifest file.

        The manifest is saved by sleeper_matchups.py after fetching matchups
        and provides historical manager names for years where managers may have left.

        Args:
            year: The season year to get mappings for

        Returns:
            Dict mapping roster_id to {manager_name, manager_guid, team_name}
        """
        import json

        manifest_path = self.ctx.data_directory / "sleeper_roster_manager_manifest.json"
        if not manifest_path.exists():
            return {}

        try:
            with open(manifest_path) as f:
                manifest = json.load(f)

            roster_map = {}
            for key, info in manifest.items():
                # Key format: "{year}_{roster_id}"
                parts = key.split("_", 1)  # Split on first underscore only
                if len(parts) == 2:
                    try:
                        m_year = int(parts[0])
                        m_roster_id = parts[1]
                        rid = int(m_roster_id) if m_roster_id.isdigit() else m_roster_id
                        if m_year == year and rid not in roster_map:
                            manager_name = info.get("manager", "Unknown")
                            # Apply overrides
                            if manager_name in self.ctx.manager_name_overrides:
                                manager_name = self.ctx.manager_name_overrides[manager_name]
                            roster_map[rid] = {
                                "manager_name": manager_name,
                                "manager_guid": info.get("manager_guid", ""),
                                "team_name": info.get("team_name", manager_name),
                            }
                    except (ValueError, TypeError):
                        continue

            if roster_map:
                log(f"  Loaded roster map from manifest: {len(roster_map)} teams for year {year}")
            return roster_map

        except Exception as e:
            logger.debug(f"Could not load manager manifest: {e}")
            return {}

    def _get_roster_mappings(self, league_id: str, year: int, db=None) -> dict[int, dict[str, str]]:
        """Build roster_id -> manager info mapping FOR A SPECIFIC YEAR."""
        return build_year_roster_map(
            self.ctx,
            self.client,
            league_id,
            year,
            db=db,
            log_fn=log,
        )

    def _get_league_settings(self, league_id: str) -> dict[str, Any]:
        """Get league settings for roster slot configuration."""
        league = self.client.get_league(league_id)
        if not league:
            return {}

        settings = league.get("settings", {})
        roster_positions = league.get("roster_positions", [])

        return {
            "settings": settings,
            "roster_positions": roster_positions,
            "total_rosters": league.get("total_rosters", 12),
            "season": league.get("season"),
        }

    def _map_position_slot(self, slot_index: int, roster_positions: list[str]) -> str:
        """
        Map a slot index to a fantasy position name.

        Args:
            slot_index: Index in the starters/roster array
            roster_positions: List of position slots from league settings

        Returns:
            Fantasy position (QB, RB1, RB2, FLEX, BN, etc.)
        """
        if not roster_positions or slot_index >= len(roster_positions):
            return "BN"  # Default to bench

        return roster_positions[slot_index]

    def fetch_week_rosters(
        self,
        league_id: str,
        year: int,
        week: int,
        roster_map: dict[int, dict[str, str]],
        roster_positions: list[str],
        taxi_by_roster: dict[int, set[str]] | None = None,
        reserve_by_roster: dict[int, set[str]] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch roster data for all teams for a specific week.

        Args:
            league_id: Sleeper league_id
            year: Season year
            week: Week number (1-indexed)
            roster_map: roster_id -> manager info mapping
            roster_positions: List of position slots from league settings
            taxi_by_roster: Optional dict mapping roster_id to set of taxi player IDs
                (from rosters API - more accurate than matchups API for taxi data)
            reserve_by_roster: Optional dict mapping roster_id to set of IR/reserve player IDs
                (from rosters API - more accurate than matchups API for reserve data)

        Returns:
            List of player roster entries
        """
        matchups = self.client.get_league_matchups(league_id, week)

        if not matchups:
            log(f"  No matchups found for week {week}")
            return []

        all_roster_data = []

        # DIAGNOSTIC: Track roster_ids seen vs expected for debugging
        seen_roster_ids = set()
        expected_roster_ids = set(roster_map.keys())
        managers_seen = set()
        unknown_roster_ids = []

        for matchup in matchups:
            roster_id = matchup.get("roster_id")
            if roster_id is None:
                continue

            seen_roster_ids.add(roster_id)
            manager_info = roster_map.get(roster_id, {})
            manager_name = manager_info.get("manager_name", "Unknown")
            manager_guid = manager_info.get("manager_guid", "")

            # Track for diagnostics
            if manager_name != "Unknown":
                managers_seen.add(manager_name)
            else:
                unknown_roster_ids.append(roster_id)

            # Get all players and their points
            players = matchup.get("players", []) or []
            starters = matchup.get("starters", []) or []
            players_points = matchup.get("players_points", {}) or {}

            # Dynasty-specific roster slots (taxi squad, IR/reserve)
            # Use passed-in lookups from rosters API if available (accurate)
            # Otherwise fallback to matchup data (usually empty for taxi)
            if taxi_by_roster is not None and roster_id in taxi_by_roster:
                taxi_players = taxi_by_roster[roster_id]
            else:
                taxi_players = set(str(p) for p in (matchup.get("taxi") or []))

            if reserve_by_roster is not None and roster_id in reserve_by_roster:
                reserve_players = reserve_by_roster[roster_id]
            else:
                reserve_players = set(str(p) for p in (matchup.get("reserve") or []))

            # Build roster entries
            for player_id in players:
                if not player_id:
                    continue

                # Get player info from cache
                player_name = self.player_cache.get_player_name(str(player_id))
                position = self.player_cache.get_player_position(str(player_id))
                nfl_team = self.player_cache.get_player_team(str(player_id))

                # DEF detection: team abbreviation player_ids are DEF entries.
                # The player cache may not have historical teams (OAK, SD, STL),
                # so detect DEF by checking if the player_id IS a team abbreviation.
                pid_str = str(player_id).upper().strip()
                if pid_str in self.player_cache.VALID_NFL_TEAMS or pid_str in _ABBREV_TO_FRANCHISE_ID:
                    position = "DEF"
                    if not nfl_team or nfl_team == "Unknown":
                        nfl_team = pid_str
                    if not player_name or player_name == "Unknown":
                        # Build name from team abbreviation
                        full_name = self.player_cache.TEAM_ABBREV_TO_NAME.get(pid_str, pid_str)
                        short_name = self.player_cache.NFL_TEAM_SHORT_NAMES.get(full_name, pid_str)
                        player_name = f"{short_name} DST"

                # Get points for this week
                points = players_points.get(str(player_id), 0.0)
                if points is None:
                    points = 0.0

                # Determine fantasy position (starter slot, bench, taxi, or IR)
                # Priority: Starter > Taxi > Reserve/IR > Bench
                if player_id in starters:
                    slot_index = starters.index(player_id)
                    fantasy_position = self._map_position_slot(slot_index, roster_positions)
                elif player_id in taxi_players:
                    fantasy_position = "TAXI"
                elif player_id in reserve_players:
                    fantasy_position = "IR"
                else:
                    fantasy_position = "BN"

                # Get eligible positions from cache
                player_data = self.player_cache.get_player(str(player_id))
                if player_data:
                    fantasy_positions = player_data.get("fantasy_positions", [])
                    eligible_positions = ",".join(fantasy_positions) if fantasy_positions else position
                else:
                    eligible_positions = position

                roster_entry = {
                    "year": year,
                    "week": week,
                    "manager": manager_name,
                    "manager_guid": manager_guid,
                    "team_key": str(roster_id),  # Use roster_id as team_key equivalent
                    "sleeper_player_id": str(player_id),
                    "player": player_name,
                    "nfl_team": nfl_team,
                    "yahoo_position": position,  # Use position as yahoo_position for compatibility
                    "nfl_position": position,
                    "fantasy_position": fantasy_position,
                    "eligible_positions": eligible_positions,
                    "points": round(float(points), 2),
                }

                all_roster_data.append(roster_entry)

        # DIAGNOSTIC: Log roster coverage analysis
        missing_roster_ids = expected_roster_ids - seen_roster_ids
        extra_roster_ids = seen_roster_ids - expected_roster_ids

        if unknown_roster_ids:
            logger.warning(f"    [{year} Week {week}] Unknown roster_ids (not in roster_map): {unknown_roster_ids}")

        if missing_roster_ids:
            # Only log at WARNING level if we're losing managers - this is the progressive loss bug
            missing_managers = [roster_map.get(rid, {}).get("manager_name", f"rid={rid}") for rid in missing_roster_ids]
            logger.warning(
                f"    [{year} Week {week}] Missing rosters: {sorted(missing_roster_ids)} ({missing_managers})"
            )

        if extra_roster_ids:
            logger.debug(f"    [{year} Week {week}] Extra roster_ids (not in expected map): {extra_roster_ids}")

        # Log manager count for this week to help identify progressive loss
        logger.debug(f"    [{year} Week {week}] Managers: {len(managers_seen)}, Roster entries: {len(all_roster_data)}")

        return all_roster_data

    def fetch_season_rosters(self, year: int, weeks: list[int] | None = None, db=None) -> pd.DataFrame:
        """
        Fetch roster data for an entire season.

        Args:
            year: Season year
            weeks: Optional list of specific weeks to fetch.
                   If None, fetches all available weeks.
            db: Optional LocalLeagueDB instance for reading matchup data

        Returns:
            DataFrame with roster data for all weeks
        """
        log(f"\nFetching Sleeper rosters for {year}")

        # Get league ID for year
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            log(f"No league ID found for year {year}")
            return pd.DataFrame()

        if _verbose:
            log(f"League ID: {league_id}")

        # Get league settings
        league_settings = self._get_league_settings(league_id)
        roster_positions = league_settings.get("roster_positions", [])
        if _verbose:
            log(f"Roster positions: {roster_positions}")

        # Build roster mappings (year-specific - roster_ids map to different owners each year)
        roster_map = self._get_roster_mappings(league_id, year, db=db)
        log(f"Found {len(roster_map)} teams for year {year}")

        # Fetch current rosters to get taxi/reserve assignments
        # These are season-level (not week-level) for dynasty leagues
        # The rosters API has taxi/reserve data, but matchups API doesn't
        current_rosters = self.client.get_league_rosters(league_id)
        taxi_by_roster: dict[int, set[str]] = {}
        reserve_by_roster: dict[int, set[str]] = {}
        for roster in current_rosters:
            rid = roster.get("roster_id")
            if rid is not None:
                taxi_by_roster[rid] = set(str(p) for p in (roster.get("taxi") or []))
                reserve_by_roster[rid] = set(str(p) for p in (roster.get("reserve") or []))

        taxi_count = sum(len(v) for v in taxi_by_roster.values())
        reserve_count = sum(len(v) for v in reserve_by_roster.values())
        if taxi_count > 0:
            log(f"Found {taxi_count} taxi players across all rosters")
        if reserve_count > 0:
            log(f"Found {reserve_count} IR/reserve players across all rosters")

        # Determine weeks to fetch — respect end_week from settings
        if weeks is None:
            # Try to get end_week from league settings (via Sleeper API)
            end_wk = league_settings.get("settings", {}).get("last_scored_leg")
            playoff_wk_start = league_settings.get("settings", {}).get("playoff_week_start", 0)
            total_rosters = league_settings.get("total_rosters", 0)
            # Sleeper "last_scored_leg" = last completed week; playoff_week_start is reliable
            # Fallback: regular_season_length + playoff_rounds covers the full season
            if playoff_wk_start and total_rosters:
                # end_week = playoff_start + playoff_rounds - 1
                # But simplest: fetch up to playoff_start + 4 (max 4 playoff rounds)
                max_week = min(playoff_wk_start + 4, 22)
                weeks = list(range(1, max_week + 1))
            else:
                weeks = list(range(1, 23))

        log(f"Fetching weeks: {weeks[0]}-{weeks[-1]}")

        # Fetch all weeks
        all_roster_data = []
        consecutive_empty_weeks = 0

        valid_weeks = 0
        for week in weeks:
            if _verbose:
                log(f"  Week {week}...")

            try:
                week_data = self.fetch_week_rosters(
                    league_id=league_id,
                    year=year,
                    week=week,
                    roster_map=roster_map,
                    roster_positions=roster_positions,
                    taxi_by_roster=taxi_by_roster,
                    reserve_by_roster=reserve_by_roster,
                )

                # Skip phantom weeks (no data or all points are 0)
                if not week_data:
                    consecutive_empty_weeks += 1
                    if consecutive_empty_weeks >= 3:
                        if _verbose:
                            log("    3+ consecutive empty weeks - stopping")
                        break
                    continue

                total_points = sum(entry.get("points", 0) for entry in week_data)
                if total_points == 0:
                    consecutive_empty_weeks += 1
                    if consecutive_empty_weeks >= 3:
                        if _verbose:
                            log("    3+ consecutive empty weeks - stopping")
                        break
                    continue

                consecutive_empty_weeks = 0  # Reset on valid week
                valid_weeks += 1
                all_roster_data.extend(week_data)
                if _verbose:
                    log(f"    Found {len(week_data)} roster entries, {total_points:.1f} total points")

            except Exception as e:
                log(f"    Error fetching week {week}: {e}")
                continue

        if not all_roster_data:
            log("No roster data found")
            return pd.DataFrame()

        # Create DataFrame
        df = pd.DataFrame(all_roster_data)

        # CRITICAL: Ensure sleeper_player_id is clean string without .0 suffix
        # Float-to-string conversion adds .0 suffix (e.g., 10790962.0 -> "10790962.0")
        if "sleeper_player_id" in df.columns:
            df["sleeper_player_id"] = df["sleeper_player_id"].astype(str).str.replace(r"\.0$", "", regex=True)

        log(f"[OK] {year}: {len(df):,} entries across {valid_weeks} weeks, {df['manager'].nunique()} managers")
        if _verbose:
            log(f"  Unique players: {df['sleeper_player_id'].nunique()}")
            log(f"  Managers: {df['manager'].unique().tolist()}")

        # DIAGNOSTIC: Check for progressive manager loss (the champions_branch_out bug)
        if "week" in df.columns and "manager" in df.columns:
            managers_per_week = df.groupby("week")["manager"].nunique().to_dict()
            max_managers = max(managers_per_week.values()) if managers_per_week else 0
            for wk, count in sorted(managers_per_week.items()):
                if count < max_managers:
                    logger.warning(f"    [{year} Week {wk}] Only {count}/{max_managers} managers have roster data")

            # Calculate weekly totals for duplicate detection
            weekly_totals = df.groupby("week")["points"].sum().to_dict()
            logger.debug(f"    [{year}] Weekly point totals: {weekly_totals}")

        return df


def fetch_sleeper_rosters(
    ctx: SleeperContext,
    year: int,
    weeks: list[int] | None = None,
    client: SleeperAPIClient | None = None,
    player_cache: SleeperPlayerCache | None = None,
    db=None,
) -> pd.DataFrame:
    """
    Main entry point for fetching Sleeper roster data.

    Args:
        ctx: SleeperContext with league configuration
        year: Season year to fetch
        weeks: Optional list of specific weeks. If None, fetches all.
        client: Optional pre-configured API client
        player_cache: Optional pre-loaded player cache
        db: LocalLeagueDB instance (required).

    Returns:
        DataFrame with roster data
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    fetcher = SleeperRosterFetcher(ctx, client, player_cache)

    df = fetcher.fetch_season_rosters(year, weeks, db=db)

    if df.empty:
        return df

    # Normalize to canonical roster schema before saving to DuckDB
    league_id = ctx.get_league_id_for_year(year) or ctx.league_id
    normalized = normalize_roster_df(df, platform="sleeper", league_id=str(league_id))
    db.save_table("player_fantasy", normalized, year=year, platform="sleeper", league_id=str(league_id))
    if _verbose:
        log(f"  [LocalDB] player_fantasy year={year}: {len(normalized):,} rows (canonical)")

    return df


def fetch_all_sleeper_rosters(
    ctx: SleeperContext,
    client: SleeperAPIClient | None = None,
    player_cache: SleeperPlayerCache | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    """
    Fetch roster data for all years in context range.

    Args:
        ctx: SleeperContext with league configuration
        client: Optional pre-configured API client
        player_cache: Optional pre-loaded player cache
        year_filter: If set, only fetch this year or these years (for quick imports)
        db: LocalLeagueDB instance (required).

    Returns:
        Combined DataFrame with all years
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")
    all_dfs = []
    year_weekly_totals = {}  # For detecting duplicate year data

    # If year_filter is set, only fetch that year/those years (quick import mode).
    # Otherwise prefer discovered league_ids over the payload start/end window.
    years_to_fetch = resolve_years_to_fetch(ctx, year_filter)

    for year in years_to_fetch:
        # Read end_week from league_settings in local DB to cap the fetch range
        weeks_for_year = None
        if db is not None:
            try:
                conn = db.connect()
                row = conn.execute("SELECT end_week FROM public.league_settings WHERE year = ?", [year]).fetchone()
                if row and row[0]:
                    end_wk = int(row[0])
                    weeks_for_year = list(range(1, end_wk + 1))
                    log(f"  Fetching weeks 1-{end_wk} (from league settings)")
            except Exception:
                pass  # Fall back to default 1-22

        df = fetch_sleeper_rosters(
            ctx=ctx,
            year=year,
            weeks=weeks_for_year,
            client=client,
            player_cache=player_cache,
            db=db,
        )

        if not df.empty:
            all_dfs.append(df)

            # Track weekly totals for duplicate detection
            if "week" in df.columns and "points" in df.columns:
                weekly_totals = tuple(sorted(df.groupby("week")["points"].sum().items()))
                year_weekly_totals[year] = weekly_totals

    if not all_dfs:
        return pd.DataFrame()

    # DIAGNOSTIC: Detect duplicate year data (e.g., 2025 and 2026 having identical points)
    # This indicates data was duplicated incorrectly or is synthetic placeholder data
    if len(year_weekly_totals) > 1:
        seen_patterns = {}
        for year, pattern in year_weekly_totals.items():
            if pattern in seen_patterns:
                dup_year = seen_patterns[pattern]
                logger.warning(f"DUPLICATE DATA DETECTED: Year {year} has IDENTICAL weekly points as year {dup_year}!")
                logger.warning("  This may indicate data duplication or synthetic placeholder data.")
                log(f"[WARN] DUPLICATE DATA: Year {year} matches year {dup_year} exactly!")
            else:
                seen_patterns[pattern] = year

    return pd.concat(all_dfs, ignore_index=True)


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper Roster Data Fetcher")
    parser.add_argument("--context", required=True, help="Path to sleeper_context.json")
    parser.add_argument("--year", type=int, help="Specific year to fetch")
    parser.add_argument("--week", type=int, help="Specific week to fetch")

    args = parser.parse_args()

    ctx = SleeperContext.load(Path(args.context))

    if args.year:
        weeks = [args.week] if args.week else None
        df = fetch_sleeper_rosters(ctx, args.year, weeks)
    else:
        df = fetch_all_sleeper_rosters(ctx)

    print(f"\nFetched {len(df)} roster entries")
    if not df.empty:
        print(df.head())
