"""
Sleeper API Client - HTTP client for Sleeper Fantasy Football API.

Sleeper API: https://docs.sleeper.com/
- No authentication required (public read-only)
- Rate limit: 1000 calls/minute
- Base URL: https://api.sleeper.app/v1/

This client provides:
- All documented Sleeper API endpoints
- Retry logic with exponential backoff
- Rate limiting to stay under 1000 req/min
- Type-safe response handling
"""

import os
import time
import logging
from dataclasses import dataclass
from typing import Any

import requests

from ..shared.retry_utils import retry_with_backoff, RateLimiter

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using %s", name, os.getenv(name), default)
        return default


@dataclass
class SleeperAPIConfig:
    """Configuration for Sleeper API client."""

    base_url: str = "https://api.sleeper.app/v1"
    # Sleeper's documented ceiling is 1000/min PER IP. The limiter lives in this process, so
    # N concurrent ingest subprocesses on one host each get their own budget -- N x 1000/min
    # from a single IP. Parallel callers (e.g. the corpus crawler) MUST divide the ceiling by
    # the worker count via SLEEPER_RATE_LIMIT_PER_MIN so the host stays within one IP budget.
    rate_limit_per_min: int = _env_int("SLEEPER_RATE_LIMIT_PER_MIN", 1000)
    timeout: int = 30
    max_retries: int = 3


class SleeperAPIError(Exception):
    """Exception raised for Sleeper API errors."""

    def __init__(self, message: str, status_code: int | None = None, response: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class SleeperAPIClient:
    """
    HTTP client for Sleeper Fantasy Football API.

    All endpoints are public and require no authentication.
    Rate limiting is enforced to stay under 1000 requests/minute.

    Usage:
        client = SleeperAPIClient()

        # Get user info
        user = client.get_user("my_username")

        # Get leagues for a user
        leagues = client.get_user_leagues(user['user_id'], 'nfl', 2024)

        # Get league details
        league = client.get_league(leagues[0]['league_id'])

        # Get matchups for week 1
        matchups = client.get_league_matchups(league['league_id'], 1)
    """

    def __init__(self, config: SleeperAPIConfig | None = None):
        """
        Initialize the Sleeper API client.

        Args:
            config: Optional configuration. Uses defaults if not provided.
        """
        self.config = config or SleeperAPIConfig()
        self._rate_limiter = RateLimiter(
            requests_per_minute=config.rate_limit_per_min if config else SleeperAPIConfig().rate_limit_per_min
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "YahooOAuth/SleeperClient (Fantasy Football Data)",
                "Accept": "application/json",
            }
        )
        # Per-league caches — eliminates redundant API calls across fetchers.
        # Matchups, rosters, draft, and transactions all independently call
        # get_league/users/rosters/brackets for the same league_id.
        self._cache_league: dict[str, Any] = {}
        self._cache_users: dict[str, list] = {}
        self._cache_rosters: dict[str, list] = {}
        self._cache_winners: dict[str, list] = {}
        self._cache_losers: dict[str, list] = {}

    def _rate_limit_wait(self):
        """
        Enforce rate limiting (1000 req/min = ~16.67 req/sec).

        Delegates to the shared RateLimiter which uses a sliding window
        with minimum gap enforcement and thread-safe locking.
        """
        self._rate_limiter.wait()

    @retry_with_backoff(
        max_retries=3,
        retry_exceptions=(
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    )
    def _get(self, endpoint: str) -> Any:
        """
        Execute GET request with retry logic.

        Args:
            endpoint: API endpoint (without base URL)

        Returns:
            Parsed JSON response (dict or list)

        Raises:
            SleeperAPIError: If the request fails
        """
        self._rate_limit_wait()

        url = f"{self.config.base_url}{endpoint}"

        try:
            response = self.session.get(url, timeout=self.config.timeout)

            if response.status_code == 404:
                # Not found is valid (e.g., user doesn't exist)
                return None

            if response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                raise SleeperAPIError("Rate limited", status_code=429)

            if response.status_code >= 400:
                raise SleeperAPIError(
                    f"API error: {response.status_code}", status_code=response.status_code, response=response.text
                )

            # Handle empty responses
            if not response.content:
                return None

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            raise

    # =========================================================================
    # User Endpoints
    # =========================================================================

    def get_user(self, username_or_id: str) -> dict[str, Any] | None:
        """
        Get user object by username or user_id.

        GET /user/{username} or GET /user/{user_id}

        Args:
            username_or_id: Sleeper username or user_id

        Returns:
            User dict with: user_id, username, display_name, avatar
            None if user not found
        """
        return self._get(f"/user/{username_or_id}")

    def get_user_leagues(self, user_id: str, sport: str, season: int) -> list[dict[str, Any]]:
        """
        Get all leagues for a user in a specific sport/season.

        GET /user/{user_id}/leagues/{sport}/{season}

        Args:
            user_id: Sleeper user_id
            sport: Sport type (nfl, nba, mlb, nhl)
            season: Season year (e.g., 2024)

        Returns:
            List of league dicts
        """
        result = self._get(f"/user/{user_id}/leagues/{sport}/{season}")
        return result if result else []

    def get_user_drafts(self, user_id: str, sport: str, season: int) -> list[dict[str, Any]]:
        """
        Get all drafts for a user in a specific sport/season.

        GET /user/{user_id}/drafts/{sport}/{season}

        Args:
            user_id: Sleeper user_id
            sport: Sport type
            season: Season year

        Returns:
            List of draft dicts
        """
        result = self._get(f"/user/{user_id}/drafts/{sport}/{season}")
        return result if result else []

    # =========================================================================
    # League Endpoints
    # =========================================================================

    def clear_league_cache(self, league_id: str = None):
        """Clear cached data for a league (or all leagues if None)."""
        if league_id:
            for cache in (
                self._cache_league,
                self._cache_users,
                self._cache_rosters,
                self._cache_winners,
                self._cache_losers,
            ):
                cache.pop(league_id, None)
        else:
            for cache in (
                self._cache_league,
                self._cache_users,
                self._cache_rosters,
                self._cache_winners,
                self._cache_losers,
            ):
                cache.clear()

    def get_league(self, league_id: str) -> dict[str, Any] | None:
        """
        Get specific league details. Cached per league_id.

        GET /league/{league_id}

        Args:
            league_id: Sleeper league_id

        Returns:
            League dict with: league_id, name, season, settings, scoring_settings, etc.
            None if league not found
        """
        if league_id not in self._cache_league:
            self._cache_league[league_id] = self._get(f"/league/{league_id}")
        return self._cache_league[league_id]

    def get_league_rosters(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all rosters in a league with player assignments.

        GET /league/{league_id}/rosters

        Args:
            league_id: Sleeper league_id

        Returns:
            List of roster dicts with: roster_id, owner_id, players, starters, settings, etc.
        """
        if league_id not in self._cache_rosters:
            result = self._get(f"/league/{league_id}/rosters")
            self._cache_rosters[league_id] = result if result else []
        return self._cache_rosters[league_id]

    def get_league_users(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all users/members in a league. Cached per league_id.

        GET /league/{league_id}/users

        Args:
            league_id: Sleeper league_id

        Returns:
            List of user dicts with: user_id, display_name, avatar, metadata
        """
        if league_id not in self._cache_users:
            result = self._get(f"/league/{league_id}/users")
            self._cache_users[league_id] = result if result else []
        return self._cache_users[league_id]

    def get_league_matchups(self, league_id: str, week: int) -> list[dict[str, Any]]:
        """
        Get matchups for a specific week.

        GET /league/{league_id}/matchups/{week}

        Args:
            league_id: Sleeper league_id
            week: Week number (1-indexed)

        Returns:
            List of matchup dicts with: roster_id, matchup_id, points, players, starters, players_points
        """
        result = self._get(f"/league/{league_id}/matchups/{week}")
        return result if result else []

    def get_league_transactions(self, league_id: str, round_num: int) -> list[dict[str, Any]]:
        """
        Get transactions for a specific round/week.

        GET /league/{league_id}/transactions/{round}

        Args:
            league_id: Sleeper league_id
            round_num: Round number (roughly corresponds to week)

        Returns:
            List of transaction dicts with: transaction_id, type, adds, drops, waiver_budget, status, created
        """
        result = self._get(f"/league/{league_id}/transactions/{round_num}")
        return result if result else []

    def get_league_traded_picks(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all traded draft picks in a league.

        GET /league/{league_id}/traded_picks

        Args:
            league_id: Sleeper league_id

        Returns:
            List of traded pick dicts
        """
        result = self._get(f"/league/{league_id}/traded_picks")
        return result if result else []

    def get_league_drafts(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get all drafts associated with a league.

        GET /league/{league_id}/drafts

        Args:
            league_id: Sleeper league_id

        Returns:
            List of draft dicts
        """
        result = self._get(f"/league/{league_id}/drafts")
        return result if result else []

    def get_winners_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get playoff bracket (winners).

        GET /league/{league_id}/winners_bracket

        Args:
            league_id: Sleeper league_id

        Returns:
            List of matchup dicts for playoff bracket
        """
        if league_id not in self._cache_winners:
            result = self._get(f"/league/{league_id}/winners_bracket")
            self._cache_winners[league_id] = result if result else []
        return self._cache_winners[league_id]

    def get_losers_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get consolation bracket (losers). Cached per league_id.

        GET /league/{league_id}/losers_bracket

        Args:
            league_id: Sleeper league_id

        Returns:
            List of matchup dicts for consolation bracket
        """
        if league_id not in self._cache_losers:
            result = self._get(f"/league/{league_id}/losers_bracket")
            self._cache_losers[league_id] = result if result else []
        return self._cache_losers[league_id]

    # =========================================================================
    # Draft Endpoints
    # =========================================================================

    def get_draft(self, draft_id: str) -> dict[str, Any] | None:
        """
        Get specific draft details.

        GET /draft/{draft_id}

        Args:
            draft_id: Sleeper draft_id

        Returns:
            Draft dict with: draft_id, type, status, season, settings, slot_to_roster_id, etc.
            None if draft not found
        """
        return self._get(f"/draft/{draft_id}")

    def get_draft_picks(self, draft_id: str) -> list[dict[str, Any]]:
        """
        Get all picks in a draft.

        GET /draft/{draft_id}/picks

        Args:
            draft_id: Sleeper draft_id

        Returns:
            List of pick dicts with: round, draft_slot, pick_no, player_id, roster_id, is_keeper, metadata
        """
        result = self._get(f"/draft/{draft_id}/picks")
        return result if result else []

    def get_draft_traded_picks(self, draft_id: str) -> list[dict[str, Any]]:
        """
        Get all traded picks in a specific draft.

        GET /draft/{draft_id}/traded_picks

        Args:
            draft_id: Sleeper draft_id

        Returns:
            List of traded pick dicts
        """
        result = self._get(f"/draft/{draft_id}/traded_picks")
        return result if result else []

    # =========================================================================
    # Player Endpoints
    # =========================================================================

    def get_all_players(self, sport: str = "nfl") -> dict[str, dict[str, Any]]:
        """
        Get complete player database (~5MB for NFL).

        GET /players/{sport}

        This is a large response. Cache locally and refresh periodically.

        Args:
            sport: Sport type (nfl, nba, mlb, nhl)

        Returns:
            Dict mapping player_id to player info:
            {
                "4046": {
                    "player_id": "4046",
                    "first_name": "Patrick",
                    "last_name": "Mahomes",
                    "full_name": "Patrick Mahomes",
                    "position": "QB",
                    "team": "KC",
                    ...
                }
            }
        """
        result = self._get(f"/players/{sport}")
        return result if result else {}

    def get_trending_players(
        self, sport: str = "nfl", trend_type: str = "add", lookback_hours: int = 24, limit: int = 25
    ) -> list[dict[str, Any]]:
        """
        Get trending players (most added/dropped).

        GET /players/{sport}/trending/{type}

        Args:
            sport: Sport type
            trend_type: "add" or "drop"
            lookback_hours: Hours to look back (default 24)
            limit: Max players to return (default 25)

        Returns:
            List of trending player dicts with: player_id, count
        """
        result = self._get(f"/players/{sport}/trending/{trend_type}?lookback_hours={lookback_hours}&limit={limit}")
        return result if result else []

    # =========================================================================
    # State Endpoints
    # =========================================================================

    def get_nfl_state(self) -> dict[str, Any]:
        """
        Get current NFL state (season, week, etc.).

        GET /state/nfl

        Returns:
            State dict with: season, week, season_type, season_start_date, etc.
        """
        result = self._get("/state/nfl")
        return result if result else {}

    def get_sport_state(self, sport: str) -> dict[str, Any]:
        """
        Get current state for any sport.

        GET /state/{sport}

        Args:
            sport: Sport type (nfl, nba, mlb, nhl)

        Returns:
            State dict with season info
        """
        result = self._get(f"/state/{sport}")
        return result if result else {}


# Convenience function for one-off API calls
def sleeper_api_call(endpoint: str, config: SleeperAPIConfig | None = None) -> Any:
    """
    Make a one-off Sleeper API call.

    For repeated calls, prefer using SleeperAPIClient directly to reuse the session.

    Args:
        endpoint: API endpoint (e.g., "/user/my_username")
        config: Optional configuration

    Returns:
        Parsed JSON response
    """
    client = SleeperAPIClient(config)
    return client._get(endpoint)
