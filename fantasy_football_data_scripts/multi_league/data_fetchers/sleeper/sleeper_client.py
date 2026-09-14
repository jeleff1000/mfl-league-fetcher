"""
Sleeper Platform Client

Wraps the existing SleeperAPIClient to conform to BasePlatformClient
interface, enabling platform-agnostic data fetching.

Usage:
    from data_fetchers.sleeper.sleeper_client import SleeperPlatformClient

    client = SleeperPlatformClient()  # No auth needed

    league = client.get_league(league_id)
    matchups = client.get_week_matchups(league_id, week=1)
"""

from typing import Any
from pathlib import Path
import logging

from ..platform.base_client import BasePlatformClient, PlatformConfig
from .sleeper_api_client import SleeperAPIClient, SleeperAPIConfig
from .playoff_utils import resolve_playoff_structure
from .sleeper_player_cache import SleeperPlayerCache

logger = logging.getLogger(__name__)


class SleeperPlatformClient(BasePlatformClient):
    """
    Sleeper API client conforming to BasePlatformClient interface.

    Wraps the existing SleeperAPIClient to provide a unified interface
    for data fetching across platforms. No authentication required.
    """

    def __init__(
        self,
        config: PlatformConfig | None = None,
        cache_dir: Path | None = None,
    ):
        """
        Initialize the Sleeper client.

        Args:
            config: Optional platform configuration
            cache_dir: Optional directory for player cache
        """
        super().__init__(config or PlatformConfig(rate_limit_per_min=1000))

        # Create underlying Sleeper API client
        api_config = SleeperAPIConfig(
            rate_limit_per_min=self.config.rate_limit_per_min,
            max_retries=self.config.max_retries,
            timeout=self.config.timeout_seconds,
        )
        self._api = SleeperAPIClient(api_config)

        # Player cache for name/position lookup
        self._player_cache: SleeperPlayerCache | None = None
        if cache_dir:
            self._player_cache = SleeperPlayerCache(cache_dir)
            self._player_cache.refresh_if_stale(self._api)

        # Cache for roster mappings
        self._roster_map_cache: dict[str, dict[int, dict[str, str]]] = {}

    @property
    def platform_name(self) -> str:
        return "sleeper"

    @property
    def requires_auth(self) -> bool:
        return False  # Sleeper API is public

    def get_league(self, league_id: str) -> dict[str, Any]:
        """
        Get league metadata.

        Returns:
            Dict with league_id, name, settings, roster_positions
        """
        league = self._api.get_league(league_id)
        if not league:
            return {}

        return {
            "league_id": league_id,
            "name": league.get("name"),
            "settings": league.get("settings", {}),
            "scoring_settings": league.get("scoring_settings", {}),
            "roster_positions": league.get("roster_positions", []),
            "season": league.get("season"),
        }

    def get_league_users(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all managers/users in league.

        Returns:
            List of dicts with user_id, display_name, team_name
        """
        users = self._api.get_league_users(league_id)
        return [
            {
                "user_id": u.get("user_id"),
                "display_name": u.get("display_name") or u.get("username", "Unknown"),
                "team_name": u.get("metadata", {}).get("team_name", u.get("display_name", "Unknown")),
            }
            for u in users
        ]

    def get_league_rosters(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all rosters in league.

        Returns:
            List of dicts with roster_id, owner_id, players, starters
        """
        return self._api.get_league_rosters(league_id)

    def get_roster_mappings(self, league_id: str) -> dict[int, dict[str, str]]:
        """
        Get roster_id -> manager info mapping.

        Returns:
            Dict mapping roster_id (int) to {manager_name, manager_guid}
        """
        if league_id in self._roster_map_cache:
            return self._roster_map_cache[league_id]

        rosters = self._api.get_league_rosters(league_id)
        users = self._api.get_league_users(league_id)

        # Build user_id -> display_name mapping
        user_names = {}
        for user in users:
            user_id = user.get("user_id")
            display_name = user.get("display_name") or user.get("username", "Unknown")
            if user_id:
                user_names[user_id] = display_name

        # Build roster mappings
        mappings: dict[int, dict[str, str]] = {}
        for roster in rosters:
            roster_id = roster.get("roster_id")
            owner_id = roster.get("owner_id")

            if roster_id is not None:
                mappings[roster_id] = {
                    "manager_name": user_names.get(owner_id, "Unknown"),
                    "manager_guid": owner_id or "",
                }

        self._roster_map_cache[league_id] = mappings
        return mappings

    def get_week_matchups(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get matchups for a specific week.

        Args:
            league_id: Sleeper league_id
            week: Week number (1-indexed)

        Returns:
            List of matchup dicts with roster_id, matchup_id, points, players
        """
        return self._api.get_league_matchups(league_id, week)

    def get_week_rosters(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get roster data for a specific week.

        For Sleeper, rosters are part of matchup data.

        Args:
            league_id: Sleeper league_id
            week: Week number (1-indexed)

        Returns:
            List of roster dicts with players, starters, points
        """
        return self._api.get_league_matchups(league_id, week)

    def get_week_transactions(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get transactions for a specific week.

        Args:
            league_id: Sleeper league_id
            week: Week number (1-indexed)

        Returns:
            List of transaction dicts
        """
        return self._api.get_league_transactions(league_id, week)

    def get_drafts(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all drafts for a league.

        Returns:
            List of draft dicts with draft_id, type, status, season
        """
        return self._api.get_league_drafts(league_id)

    def get_draft_picks(self, draft_id: str) -> list[dict[str, Any]]:
        """
        Get all picks in a draft.

        Args:
            draft_id: Sleeper draft_id

        Returns:
            List of pick dicts with round, pick_no, player_id, roster_id
        """
        return self._api.get_draft_picks(draft_id)

    def get_player_info(self, player_id: str) -> dict[str, Any] | None:
        """
        Get player metadata from cache.

        Args:
            player_id: Sleeper player_id

        Returns:
            Dict with player info or None
        """
        if self._player_cache:
            return self._player_cache.get_player(player_id)
        return None

    def get_all_players(self) -> dict[str, dict[str, Any]]:
        """
        Get complete player database.

        Returns cached data if available, otherwise fetches from API.

        Returns:
            Dict mapping player_id to player info
        """
        if self._player_cache:
            return self._player_cache.players
        return self._api.get_all_players()

    def get_league_history(self, league_id: str) -> list[str]:
        """
        Get previous season league IDs.

        Sleeper leagues keep the same ID across seasons.

        Returns:
            List containing just the current league_id
        """
        return [league_id]

    def get_playoff_structure(self, league_id: str) -> dict[str, Any]:
        """Get playoff configuration from league settings."""
        league = self._api.get_league(league_id)
        if not league:
            return {"playoff_week_start": 15, "playoff_teams": 6}

        playoff_structure = resolve_playoff_structure(league.get("settings", {}))

        return {
            "playoff_week_start": playoff_structure["playoff_week_start"],
            "playoff_teams": playoff_structure["playoff_teams"],
        }

    # =========================================================================
    # Additional Sleeper-specific methods
    # =========================================================================

    def get_winners_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """Get playoff bracket."""
        return self._api.get_winners_bracket(league_id)

    def get_losers_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """Get consolation bracket."""
        return self._api.get_losers_bracket(league_id)

    def get_user(self, username_or_id: str) -> dict[str, Any] | None:
        """Get user info by username or user_id."""
        return self._api.get_user(username_or_id)

    def get_user_leagues(self, user_id: str, season: int) -> list[dict[str, Any]]:
        """Get all leagues for a user in a season."""
        return self._api.get_user_leagues(user_id, "nfl", season)

    def get_nfl_state(self) -> dict[str, Any]:
        """Get current NFL state (season, week)."""
        return self._api.get_nfl_state()

    def init_player_cache(self, cache_dir: Path):
        """
        Initialize player cache if not already done.

        Args:
            cache_dir: Directory for cache files
        """
        if self._player_cache is None:
            self._player_cache = SleeperPlayerCache(cache_dir)
            self._player_cache.refresh_if_stale(self._api)

    def get_player_name(self, player_id: str) -> str:
        """Get player name from cache."""
        if self._player_cache:
            return self._player_cache.get_player_name(player_id)
        return f"Player_{player_id}"

    def get_player_position(self, player_id: str) -> str:
        """Get player position from cache."""
        if self._player_cache:
            return self._player_cache.get_player_position(player_id)
        return "UNKNOWN"

    def get_player_team(self, player_id: str) -> str:
        """Get player team from cache."""
        if self._player_cache:
            return self._player_cache.get_player_team(player_id)
        return ""
