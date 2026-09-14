"""HTTP client for Fleaflicker's fantasy API."""

from __future__ import annotations

import datetime
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Any

import requests

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.data_fetchers.shared.retry_utils import RateLimiter, retry_with_backoff

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using %s", name, os.getenv(name), default)
        return default


@dataclass
class FleaflickerAPIConfig:
    base_url: str = "https://www.fleaflicker.com/api"
    sport: str = "NFL"
    rate_limit_per_min: int = _env_int("FLEAFLICKER_RATE_LIMIT_PER_MIN", 60)
    timeout: int = 30
    external_id_types: tuple[str, ...] = ("SPORTRADAR",)
    http_max_retries: int = 3
    http_retry_base_delay: float = 5.0
    http_retry_max_delay: float = 45.0
    auth_token: str | None = os.getenv("FLEAFLICKER_AUTH_TOKEN") or None
    cookie: str | None = os.getenv("FLEAFLICKER_COOKIE") or None


class FleaflickerAPIError(Exception):
    """Raised for non-recoverable Fleaflicker API errors."""

    def __init__(self, message: str, status_code: int | None = None, response: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class FleaflickerAPIClient:
    """Small public API client with endpoint-specific helpers."""

    def __init__(self, config: FleaflickerAPIConfig | None = None):
        self.config = config or FleaflickerAPIConfig()
        self._rate_limiter = RateLimiter(requests_per_minute=self.config.rate_limit_per_min)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36 LeagueHistory/FleaflickerClient"
                ),
            }
        )
        if self.config.auth_token:
            token = self.config.auth_token.strip()
            self.session.headers["Authorization"] = token if token.lower().startswith("bearer ") else f"Bearer {token}"
        if self.config.cookie:
            self.session.headers["Cookie"] = self.config.cookie.strip()
        self._response_cache: dict[tuple[Any, ...], Any] = {}
        self._cache_lock = threading.Lock()

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), self.config.http_retry_max_delay)
            except ValueError:
                pass
        delay = min(
            self.config.http_retry_base_delay * (2**attempt),
            self.config.http_retry_max_delay,
        )
        return delay * (0.75 + random.random() * 0.5)

    @retry_with_backoff(
        max_retries=3,
        retry_exceptions=(
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    )
    def _get(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        external_id_types: tuple[str, ...] | None = None,
        retry_statuses: tuple[int, ...] | None = (403, 429, 500, 502, 503, 504),
        max_http_retries: int | None = None,
    ) -> Any:
        url = f"{self.config.base_url}/{method.lstrip('/')}"
        query: list[tuple[str, Any]] = [("sport", self.config.sport)]
        for key, value in (params or {}).items():
            if value is None:
                continue
            if isinstance(value, list | tuple):
                query.extend((key, item) for item in value if item is not None)
            else:
                query.append((key, value))
        for external_id_type in external_id_types or ():
            query.append(("external_id_type", external_id_type))

        cache_key = (method, tuple(query))
        with self._cache_lock:
            if cache_key in self._response_cache:
                return self._response_cache[cache_key]

        retries = self.config.http_max_retries if max_http_retries is None else max_http_retries
        retryable = retry_statuses or ()
        for attempt in range(retries + 1):
            self._rate_limiter.wait()
            response = self.session.get(url, params=query, timeout=self.config.timeout)
            if response.status_code == 404:
                with self._cache_lock:
                    self._response_cache[cache_key] = None
                return None
            if response.status_code in retryable and attempt < retries:
                delay = self._retry_delay(response, attempt)
                logger.warning(
                    "Fleaflicker API %s for %s; retrying in %.1fs (attempt %s/%s)",
                    response.status_code,
                    method,
                    delay,
                    attempt + 1,
                    retries + 1,
                )
                time.sleep(delay)
                continue
            if response.status_code >= 400:
                raise FleaflickerAPIError(
                    f"Fleaflicker API error {response.status_code} for {method}",
                    status_code=response.status_code,
                    response=response.text,
                )
            if not response.content:
                with self._cache_lock:
                    self._response_cache[cache_key] = None
                return None
            payload = response.json()
            with self._cache_lock:
                self._response_cache[cache_key] = payload
            return payload
        return None

    def fetch_standings(self, league_id: str | int, season: int | None = None) -> dict[str, Any] | None:
        return self._get("FetchLeagueStandings", {"league_id": league_id, "season": season})

    def fetch_user_leagues(self, user_id: str | int, season: int | None = None) -> dict[str, Any] | None:
        """Enumerate public leagues exposed for a Fleaflicker user."""
        return self._get("FetchUserLeagues", {"user_id": user_id, "season": season})

    def discover_available_years(
        self,
        league_id: str | int,
        *,
        start_year: int = 2005,
        end_year: int | None = None,
    ) -> list[int]:
        """Discover seasons the league ACTUALLY played.

        The API never 404s a missing season -- it silently serves the league's last
        played season instead (standings included), so name/team presence reports every
        season as available. The only honest witness is the scoreboard's schedule-period
        epoch: a genuine season's week-1 period starts in that calendar year.
        """
        cap_year = end_year if end_year is not None else get_current_nfl_season_year()
        years: list[int] = []
        for season in range(int(start_year), int(cap_year) + 1):
            if self.season_was_played(league_id, season):
                years.append(season)
        return years

    def season_was_played(self, league_id: str | int, season: int) -> bool:
        """True only if the scoreboard's period epoch confirms the requested season."""
        try:
            scoreboard = self.fetch_scoreboard(league_id, season=season, scoring_period=1) or {}
        except FleaflickerAPIError as exc:
            if exc.status_code in {403, 404}:
                return False
            raise
        if not scoreboard.get("games"):
            return False
        period = scoreboard.get("schedulePeriod") or scoreboard.get("schedule_period") or {}
        low = period.get("low") or {}
        epoch_ms = low.get("startEpochMilli") or low.get("start_epoch_milli")
        if not epoch_ms:
            return False
        try:
            epoch_year = datetime.datetime.fromtimestamp(int(epoch_ms) / 1000, datetime.timezone.utc).year
        except (TypeError, ValueError, OSError):
            return False
        return epoch_year == int(season)

    def fetch_rules(self, league_id: str | int) -> dict[str, Any] | None:
        return self._get("FetchLeagueRules", {"league_id": league_id})

    def fetch_scoreboard(self, league_id: str | int, season: int, scoring_period: int) -> dict[str, Any] | None:
        return self._get(
            "FetchLeagueScoreboard",
            {"league_id": league_id, "season": season, "scoring_period": scoring_period},
        )

    def fetch_league_rosters(
        self,
        league_id: str | int,
        season: int,
        scoring_period: int,
        *,
        external_id_types: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        try:
            return self._get(
                "FetchLeagueRosters",
                {"league_id": league_id, "season": season, "scoring_period": scoring_period},
                external_id_types=external_id_types or self.config.external_id_types,
                retry_statuses=(429, 500, 502, 503, 504),
                max_http_retries=1,
            )
        except FleaflickerAPIError as exc:
            if exc.status_code == 403:
                logger.warning(
                    "FetchLeagueRosters forbidden for league=%s season=%s week=%s",
                    league_id,
                    season,
                    scoring_period,
                )
                return None
            raise

    def fetch_roster(
        self,
        league_id: str | int,
        team_id: str | int,
        season: int,
        scoring_period: int,
        *,
        external_id_types: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        try:
            return self._get(
                "FetchRoster",
                {
                    "league_id": league_id,
                    "team_id": team_id,
                    "season": season,
                    "scoring_period": scoring_period,
                },
                external_id_types=external_id_types or self.config.external_id_types,
                retry_statuses=(429, 500, 502, 503, 504),
                max_http_retries=1,
            )
        except FleaflickerAPIError as exc:
            if exc.status_code == 403:
                logger.warning(
                    "FetchRoster forbidden for league=%s team=%s season=%s week=%s",
                    league_id,
                    team_id,
                    season,
                    scoring_period,
                )
                return None
            raise

    def fetch_draft_board(
        self,
        league_id: str | int,
        season: int,
        draft_number: int | None = None,
        *,
        external_id_types: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        try:
            return self._get(
                "FetchLeagueDraftBoard",
                {"league_id": league_id, "season": season, "draft_number": draft_number},
                external_id_types=external_id_types or self.config.external_id_types,
                retry_statuses=(429, 500, 502, 503, 504),
                max_http_retries=1,
            )
        except FleaflickerAPIError as exc:
            if exc.status_code == 403:
                logger.warning(
                    "FetchLeagueDraftBoard forbidden for league=%s season=%s draft=%s",
                    league_id,
                    season,
                    draft_number,
                )
                return None
            raise

    def fetch_transactions_page(
        self,
        league_id: str | int,
        team_id: str | int | None = None,
        result_offset: int = 0,
    ) -> dict[str, Any] | None:
        try:
            return self._get(
                "FetchLeagueTransactions",
                {"league_id": league_id, "team_id": team_id, "result_offset": result_offset},
                retry_statuses=(429, 500, 502, 503, 504),
                max_http_retries=1,
            )
        except FleaflickerAPIError as exc:
            if exc.status_code == 403:
                logger.warning(
                    "FetchLeagueTransactions forbidden for league=%s team=%s offset=%s",
                    league_id,
                    team_id,
                    result_offset,
                )
                return None
            raise

    def fetch_transactions(self, league_id: str | int, team_id: str | int | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        offset = 0
        seen_offsets: set[int] = set()
        while offset not in seen_offsets:
            seen_offsets.add(offset)
            payload = self.fetch_transactions_page(league_id, team_id=team_id, result_offset=offset)
            if not payload:
                break
            items.extend(payload.get("items") or [])
            next_offset = payload.get("resultOffsetNext") or payload.get("result_offset_next")
            if next_offset is None:
                break
            try:
                offset = int(next_offset)
            except (TypeError, ValueError):
                break
        return items
