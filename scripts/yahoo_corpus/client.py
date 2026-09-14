"""Small authenticated client for the offline Yahoo settings census."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping
from typing import Any

import requests


TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
GAMES_URL = "https://fantasysports.yahooapis.com/fantasy/v2/users;use_login=1/games;game_codes=nfl?format=json"
LEAGUES_URL = (
    "https://fantasysports.yahooapis.com/fantasy/v2/"
    "users;use_login=1/games;game_keys={game_key}/leagues?format=json"
)
SETTINGS_URL = "https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/settings"
# 999 is Yahoo's "Request Denied" throttle. It is NOT a permanent error: the
# same request succeeds after a cooldown. Leaving it out made every throttled
# discovery fetch drop a league-year for good.
RETRYABLE_STATUS = {429, 500, 502, 503, 504, 999}
# Yahoo's throttle windows outlast a few seconds of exponential backoff, so
# throttle responses get their own (longer) floor.
THROTTLE_STATUS = {429, 999}
THROTTLE_MIN_DELAY = 30.0
THROTTLE_MAX_DELAY = 240.0


class RequestBudgetExceeded(RuntimeError):
    """Raised before a request would exceed the configured crawl budget."""


class YahooRequestFailure(RuntimeError):
    """Sanitized Yahoo failure that never includes provider response bodies."""

    def __init__(self, endpoint: str, status: int | None, *, retryable: bool):
        self.endpoint = endpoint
        self.status = status
        self.retryable = retryable
        status_text = str(status) if status is not None else "network"
        super().__init__(f"Yahoo {endpoint} request failed ({status_text})")


def _unwrap(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, list):
        value = value[0] if value else None
    return value if isinstance(value, Mapping) else None


def _numbered_values(container: Any) -> list[Any]:
    if not isinstance(container, Mapping):
        return []
    values: list[Any] = []
    index = 0
    while str(index) in container:
        values.append(container[str(index)])
        index += 1
    return values


def parse_football_games(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    """Extract NFL game keys from Yahoo's array/object wrapper variants."""
    try:
        user = payload["fantasy_content"]["users"]["0"]["user"]
        games = user[1]["games"]
    except (KeyError, IndexError, TypeError):
        return []
    result: list[dict[str, str]] = []
    for wrapper in _numbered_values(games):
        game = _unwrap(wrapper.get("game") if isinstance(wrapper, Mapping) else None)
        if not game or game.get("code") != "nfl" or not game.get("game_key"):
            continue
        result.append({"game_key": str(game["game_key"]), "season": str(game.get("season", ""))})
    return result


def parse_game_leagues(
    payload: Mapping[str, Any], *, game_key: str, season: str
) -> list[dict[str, Any]]:
    """Extract valid leagues while ignoring malformed Yahoo siblings."""
    try:
        user = payload["fantasy_content"]["users"]["0"]["user"]
        games = user[1]["games"]
    except (KeyError, IndexError, TypeError):
        return []
    result: list[dict[str, Any]] = []
    for game_wrapper in _numbered_values(games):
        raw_game = game_wrapper.get("game") if isinstance(game_wrapper, Mapping) else None
        parts = raw_game if isinstance(raw_game, list) else [raw_game]
        leagues: Mapping[str, Any] | None = None
        for part in parts:
            if isinstance(part, Mapping) and isinstance(part.get("leagues"), Mapping):
                leagues = part["leagues"]
                break
        for wrapper in _numbered_values(leagues):
            league = _unwrap(wrapper.get("league") if isinstance(wrapper, Mapping) else None)
            if not league or not league.get("league_key"):
                continue
            result.append(
                {
                    "league_key": str(league["league_key"]),
                    "league_id": str(league.get("league_id", "")),
                    "name": str(league.get("name", "")),
                    "season": str(league.get("season") or season),
                    "num_teams": int(league.get("num_teams") or 0),
                    "game_key": game_key,
                }
            )
    return result


class YahooCensusClient:
    """Yahoo API client with bounded retries, budget tracking, and redacted errors."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        *,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        max_retries: int = 4,
        request_budget: int | None = None,
        delay_ms: int = 0,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.session = session or requests.Session()
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.request_budget = request_budget
        self.delay_ms = delay_ms
        self.request_count = 0
        self.access_token: str | None = None

    def _check_budget(self) -> None:
        if self.request_budget is not None and self.request_count >= self.request_budget:
            raise RequestBudgetExceeded(f"Yahoo request budget exhausted at {self.request_count}")

    def _request(self, method: str, url: str, *, endpoint: str, **kwargs: Any) -> requests.Response:
        last_status: int | None = None
        for attempt in range(self.max_retries + 1):
            self._check_budget()
            self.request_count += 1
            try:
                response = getattr(self.session, method)(url, timeout=30, **kwargs)
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise YahooRequestFailure(endpoint, None, retryable=True) from None
                self.sleeper(min(30.0, (2**attempt) + random.random()))
                continue

            last_status = response.status_code
            if 200 <= response.status_code < 300:
                if self.delay_ms:
                    self.sleeper(self.delay_ms / 1000.0)
                return response
            if response.status_code not in RETRYABLE_STATUS or attempt >= self.max_retries:
                raise YahooRequestFailure(
                    endpoint,
                    response.status_code,
                    retryable=response.status_code in RETRYABLE_STATUS,
                )
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                delay = float(retry_after)
            elif response.status_code in THROTTLE_STATUS:
                delay = min(THROTTLE_MAX_DELAY, THROTTLE_MIN_DELAY * (2**attempt)) + random.random()
            else:
                delay = min(30.0, (2**attempt) + random.random())
            self.sleeper(delay)
        raise YahooRequestFailure(endpoint, last_status, retryable=True)

    def refresh(self) -> None:
        response = self._request(
            "post",
            TOKEN_URL,
            endpoint="oauth",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            self.access_token = str(response.json()["access_token"])
        except (KeyError, TypeError, ValueError):
            raise YahooRequestFailure("oauth", response.status_code, retryable=False) from None

    def _auth_headers(self) -> dict[str, str]:
        if not self.access_token:
            raise YahooRequestFailure("authentication", None, retryable=False)
        return {"Authorization": f"Bearer {self.access_token}"}

    def discover_games(self) -> list[dict[str, str]]:
        response = self._request("get", GAMES_URL, endpoint="games", headers=self._auth_headers())
        return parse_football_games(response.json())

    def discover_leagues(self, game: Mapping[str, str]) -> list[dict[str, Any]]:
        response = self._request(
            "get",
            LEAGUES_URL.format(game_key=game["game_key"]),
            endpoint="leagues",
            headers=self._auth_headers(),
        )
        return parse_game_leagues(
            response.json(), game_key=game["game_key"], season=game.get("season", "")
        )

    def fetch_settings_xml(self, league_key: str) -> str:
        response = self._request(
            "get",
            SETTINGS_URL.format(league_key=league_key),
            endpoint="settings",
            headers=self._auth_headers(),
        )
        return response.text
