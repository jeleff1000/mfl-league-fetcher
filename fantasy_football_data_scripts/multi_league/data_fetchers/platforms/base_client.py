"""
Base Platform Client

Abstract base class for fantasy platform API clients.
Each platform (Yahoo, Sleeper, ESPN) implements this interface,
providing a unified way to fetch league data regardless of
the underlying API structure.
"""

from abc import ABC, abstractmethod
from typing import Any
from dataclasses import dataclass


@dataclass
class PlatformConfig:
    """Configuration for platform API client."""

    rate_limit_per_min: int = 60
    max_retries: int = 3
    timeout_seconds: int = 30
    retry_delay_seconds: float = 1.0


class BasePlatformClient(ABC):
    """
    Abstract base class for fantasy platform API clients.

    Each platform (Yahoo, Sleeper, ESPN) implements this interface,
    providing a unified way to fetch league data regardless of
    the underlying API structure.

    Example:
        class SleeperPlatformClient(BasePlatformClient):
            @property
            def platform_name(self) -> str:
                return 'sleeper'

            def get_league(self, league_id: str) -> Dict[str, Any]:
                return self._fetch(f'/league/{league_id}')
    """

    def __init__(self, config: PlatformConfig | None = None):
        self.config = config or PlatformConfig()

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Platform identifier: 'yahoo', 'sleeper', 'espn'"""

    @property
    @abstractmethod
    def requires_auth(self) -> bool:
        """Whether this platform requires authentication"""

    # === League-Level Endpoints ===

    @abstractmethod
    def get_league(self, league_id: str) -> dict[str, Any]:
        """
        Get league metadata.

        Returns:
            Dict containing league_id, name, settings, roster_positions
        """

    @abstractmethod
    def get_league_users(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all managers/users in league.

        Returns:
            List of dicts with user_id, display_name, team_name, team_key
        """

    @abstractmethod
    def get_league_rosters(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all rosters (team compositions).

        Returns:
            List of dicts with roster_id, owner_id, players
        """

    @abstractmethod
    def get_roster_mappings(self, league_id: str) -> dict[Any, dict[str, str]]:
        """
        Get roster_id -> manager info mapping.

        Returns:
            Dict mapping roster_id to {manager_name, manager_guid, team_name}
        """

    # === Weekly Data Endpoints ===

    @abstractmethod
    def get_week_matchups(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get head-to-head matchups for a week.

        Args:
            league_id: Platform-specific league identifier
            week: Week number (1-indexed)

        Returns:
            List of matchup dicts with roster_id, points, matchup_id
        """

    @abstractmethod
    def get_week_rosters(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get roster/lineup data for a week.

        Args:
            league_id: Platform-specific league identifier
            week: Week number (1-indexed)

        Returns:
            List of roster dicts with team_key, week, players
        """

    @abstractmethod
    def get_week_transactions(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get transactions (trades, waivers, drops) for a week.

        Args:
            league_id: Platform-specific league identifier
            week: Week number (1-indexed)

        Returns:
            List of transaction dicts
        """

    # === Draft Endpoints ===

    @abstractmethod
    def get_drafts(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all drafts for a league.

        Returns:
            List of draft dicts with draft_id, type, status, season
        """

    @abstractmethod
    def get_draft_picks(self, draft_id: str) -> list[dict[str, Any]]:
        """
        Get all picks in a draft.

        Args:
            draft_id: Platform-specific draft identifier

        Returns:
            List of pick dicts with pick_no, round, player_id, roster_id
        """

    # === Player Cache ===

    @abstractmethod
    def get_player_info(self, player_id: str) -> dict[str, Any] | None:
        """
        Get player metadata (name, position, team).

        Args:
            player_id: Platform-specific player identifier

        Returns:
            Dict with player info or None if not found
        """

    def get_all_players(self) -> dict[str, dict[str, Any]]:
        """
        Get complete player database.

        Optional - not all platforms have a bulk player endpoint.

        Returns:
            Dict mapping player_id to player info
        """
        return {}

    # === League History ===

    def get_league_history(self, league_id: str) -> list[str]:
        """
        Get previous season league IDs (renewal chain).

        Not all platforms support this.

        Returns:
            List of league IDs from oldest to newest
        """
        return []

    # === Playoff Detection ===

    def get_playoff_structure(self, league_id: str) -> dict[str, Any]:
        """
        Get playoff configuration.

        Returns:
            Dict with playoff_week_start, playoff_teams, etc.
        """
        league = self.get_league(league_id)
        settings = league.get("settings", {})
        return {
            "playoff_week_start": settings.get("playoff_week_start", 15),
            "playoff_teams": settings.get("playoff_teams", 6),
        }

    def is_playoff_week(self, league_id: str, week: int) -> bool:
        """Check if a week is in playoffs."""
        structure = self.get_playoff_structure(league_id)
        return week >= structure.get("playoff_week_start", 15)

    # === Utility Methods ===

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(platform={self.platform_name})"
