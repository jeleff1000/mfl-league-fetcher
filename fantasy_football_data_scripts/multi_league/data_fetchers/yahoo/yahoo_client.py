"""
Yahoo Platform Client

Wraps the yahoo_fantasy_api library to conform to BasePlatformClient
interface, enabling platform-agnostic data fetching.

Usage:
    from yahoo_oauth import OAuth2
    from data_fetchers.yahoo.yahoo_client import YahooPlatformClient

    oauth = OAuth2(consumer_key, consumer_secret)
    client = YahooPlatformClient(oauth=oauth)

    league = client.get_league(league_id)
    matchups = client.get_week_matchups(league_id, week=1)
"""

from typing import Any
import logging

from ..platform.base_client import BasePlatformClient, PlatformConfig

logger = logging.getLogger(__name__)


class YahooPlatformClient(BasePlatformClient):
    """
    Yahoo Fantasy API client conforming to BasePlatformClient interface.

    Wraps the yahoo_fantasy_api library to provide a unified interface
    for data fetching across platforms.
    """

    def __init__(
        self,
        oauth: Any | None = None,
        config: PlatformConfig | None = None,
    ):
        """
        Initialize the Yahoo client.

        Args:
            oauth: OAuth2 object from yahoo_oauth library
            config: Optional platform configuration
        """
        super().__init__(config or PlatformConfig(rate_limit_per_min=60))
        self.oauth = oauth
        self._gm = None
        self._league_cache: dict[str, Any] = {}

    @property
    def platform_name(self) -> str:
        return "yahoo"

    @property
    def requires_auth(self) -> bool:
        return True

    def _get_game_manager(self):
        """Get or create the Yahoo game manager."""
        if self._gm is None:
            import yahoo_fantasy_api as yfa

            self._gm = yfa.Game(self.oauth, "nfl")
        return self._gm

    def _get_league(self, league_id: str):
        """Get or create a league object from cache."""
        if league_id not in self._league_cache:
            gm = self._get_game_manager()
            self._league_cache[league_id] = gm.to_league(league_id)
        return self._league_cache[league_id]

    def get_league(self, league_id: str) -> dict[str, Any]:
        """
        Get league metadata.

        Returns:
            Dict with league_id, name, settings, roster_positions
        """
        lg = self._get_league(league_id)
        settings = lg.settings()
        return {
            "league_id": league_id,
            "name": settings.get("name"),
            "settings": settings,
            "roster_positions": lg.roster_positions(),
        }

    def get_league_users(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all managers/users in league.

        Returns:
            List of dicts with user_id, display_name, team_name, team_key
        """
        lg = self._get_league(league_id)
        teams = lg.teams()
        users = []
        for team in teams.values():
            managers = team.get("managers", [{}])
            manager_info = managers[0] if managers else {}
            users.append(
                {
                    "user_id": manager_info.get("guid", team.get("team_key")),
                    "display_name": manager_info.get("nickname", "Unknown"),
                    "team_name": team.get("name"),
                    "team_key": team.get("team_key"),
                }
            )
        return users

    def get_league_rosters(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all rosters (team compositions).

        Returns:
            List of dicts with roster_id, owner_id, players
        """
        lg = self._get_league(league_id)
        teams = lg.teams()
        rosters = []
        for team_key, team in teams.items():
            try:
                roster = lg.to_team(team_key).roster()
            except Exception as e:
                logger.warning(f"Failed to get roster for {team_key}: {e}")
                roster = []

            managers = team.get("managers", [{}])
            manager_info = managers[0] if managers else {}

            rosters.append(
                {
                    "roster_id": team_key,
                    "owner_id": manager_info.get("guid"),
                    "players": roster,
                }
            )
        return rosters

    def get_roster_mappings(self, league_id: str) -> dict[str, dict[str, str]]:
        """
        Get roster_id -> manager info mapping.

        Returns:
            Dict mapping team_key to {manager_name, manager_guid, team_name}
        """
        lg = self._get_league(league_id)
        teams = lg.teams()
        mappings = {}
        for team_key, team in teams.items():
            managers = team.get("managers", [{}])
            manager_info = managers[0] if managers else {}
            mappings[team_key] = {
                "manager_name": manager_info.get("nickname", "Unknown"),
                "manager_guid": manager_info.get("guid", team_key),
                "team_name": team.get("name"),
            }
        return mappings

    def get_week_matchups(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get head-to-head matchups for a week.

        Args:
            league_id: Yahoo league key (e.g., 449.l.123456)
            week: Week number (1-indexed)

        Returns:
            List of matchup dicts
        """
        lg = self._get_league(league_id)
        try:
            return lg.matchups(week=week)
        except Exception as e:
            logger.warning(f"Failed to get matchups for week {week}: {e}")
            return []

    def get_week_rosters(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get roster/lineup data for a week.

        Args:
            league_id: Yahoo league key
            week: Week number (1-indexed)

        Returns:
            List of roster dicts with team_key, week, players
        """
        lg = self._get_league(league_id)
        teams = lg.teams()
        rosters = []
        for team_key in teams:
            try:
                roster = lg.to_team(team_key).roster(week=week)
                rosters.append(
                    {
                        "team_key": team_key,
                        "week": week,
                        "players": roster,
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to get roster for {team_key} week {week}: {e}")
        return rosters

    def get_week_transactions(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get transactions for a week.

        Args:
            league_id: Yahoo league key
            week: Week number (1-indexed)

        Returns:
            List of transaction dicts
        """
        lg = self._get_league(league_id)
        try:
            return lg.transactions(tran_types=["add,drop", "trade"], week=week)
        except Exception as e:
            logger.warning(f"Failed to get transactions for week {week}: {e}")
            return []

    def get_drafts(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all drafts for a league.

        Yahoo only has one draft per league per year.

        Returns:
            List with single draft dict containing draft_id and picks
        """
        lg = self._get_league(league_id)
        try:
            draft_results = lg.draft_results()
            return [
                {
                    "draft_id": league_id,
                    "picks": draft_results,
                    "type": "snake",  # Default, actual type from settings
                }
            ]
        except Exception as e:
            logger.warning(f"Failed to get draft for {league_id}: {e}")
            return []

    def get_draft_picks(self, draft_id: str) -> list[dict[str, Any]]:
        """
        Get all picks in a draft.

        For Yahoo, draft_id is the same as league_id.

        Returns:
            List of pick dicts
        """
        lg = self._get_league(draft_id)
        try:
            return lg.draft_results()
        except Exception as e:
            logger.warning(f"Failed to get draft picks for {draft_id}: {e}")
            return []

    def get_player_info(self, player_id: str) -> dict[str, Any] | None:
        """
        Get player metadata.

        Yahoo doesn't have a bulk player endpoint - player info is
        embedded in roster responses. Returns None.
        """
        return None

    def get_league_history(self, league_id: str) -> list[str]:
        """
        Get previous season league IDs by following renewal chain.

        Returns:
            List of league IDs from oldest to newest
        """
        lg = self._get_league(league_id)
        settings = lg.settings()
        history = [league_id]

        renew_key = settings.get("renew")
        while renew_key:
            history.append(renew_key)
            try:
                prev_lg = self._get_league(renew_key)
                prev_settings = prev_lg.settings()
                renew_key = prev_settings.get("renew")
            except Exception as e:
                logger.debug(f"Reached end of renewal chain: {e}")
                break

        # Return in chronological order (oldest first)
        return list(reversed(history))

    def get_playoff_structure(self, league_id: str) -> dict[str, Any]:
        """Get playoff configuration from league settings."""
        lg = self._get_league(league_id)
        settings = lg.settings()
        return {
            "playoff_week_start": settings.get("playoff_start_week", 15),
            "playoff_teams": settings.get("num_playoff_teams", 6),
            "uses_lock": settings.get("uses_lock", False),
        }
