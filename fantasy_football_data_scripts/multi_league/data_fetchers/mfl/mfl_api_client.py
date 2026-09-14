"""HTTP client for MyFantasyLeague's public export API.

MFL facts this client encodes:
- Base URL is per-season: https://api.myfantasyleague.com/{year}/export
  (302-redirects to a www## shard host; requests follows them).
- Invalid league/endpoint returns HTTP 200 with an {"error": ...} body.
  Every response is checked for an error payload; errors are NEVER success.
- The throttle is aggressive: HTTP 429 after ~70-90 fast requests with NO
  Retry-After header and an escalating penalty once tripped. The client is
  single-threaded, defaults to 25 requests/min, and sleeps a 120s floor
  (then exponential) on any 429.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Any

import requests

from multi_league.data_fetchers.shared.retry_utils import RateLimiter, retry_with_backoff

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using %s", name, os.getenv(name), default)
        return default


@dataclass
class MFLAPIConfig:
    base_url: str = "https://api.myfantasyleague.com"
    rate_limit_per_min: int = _env_int("MFL_RATE_LIMIT_PER_MIN", 25)
    timeout: int = 60
    http_max_retries: int = 3
    http_retry_base_delay: float = 5.0
    http_retry_max_delay: float = 45.0
    throttle_floor_seconds: float = 120.0
    throttle_max_seconds: float = 600.0


class MFLAPIError(Exception):
    """Raised for non-recoverable MFL API errors (including error payloads)."""

    def __init__(self, message: str, status_code: int | None = None, response: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


def payload_error(payload: Any) -> str | None:
    """Extract the error message from an MFL error body, if any.

    MFL returns HTTP 200 with either {"error": "message"} or
    {"error": {"$t": "message"}} for invalid leagues/endpoints.
    """
    if not isinstance(payload, dict):
        return None
    if "error" not in payload:
        return None
    err = payload.get("error")
    if isinstance(err, dict):
        return str(err.get("$t") or err)
    return str(err)


class MFLAPIClient:
    """Single-threaded public MFL export client with endpoint helpers."""

    def __init__(self, config: MFLAPIConfig | None = None):
        self.config = config or MFLAPIConfig()
        self._rate_limiter = RateLimiter(requests_per_minute=self.config.rate_limit_per_min)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "LeagueHistoryImport/1.0 (MFL client)",
            }
        )
        self._response_cache: dict[tuple[Any, ...], Any] = {}
        self._cache_lock = threading.Lock()
        # Per-(year, league) player DB cache: {(year, league_id): {player_id: record}}
        self._players_cache: dict[tuple[int, str], dict[str, dict]] = {}

    def _throttle_delay(self, attempt: int) -> float:
        """429 penalty delay: 120s floor, exponential, jittered, capped."""
        delay = min(
            self.config.throttle_floor_seconds * (2**attempt),
            self.config.throttle_max_seconds,
        )
        return max(self.config.throttle_floor_seconds, delay) * (1.0 + random.random() * 0.25)

    def _retry_delay(self, attempt: int) -> float:
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
        year: int,
        export_type: str,
        params: dict[str, Any] | None = None,
        *,
        raise_on_error_payload: bool = False,
    ) -> Any:
        url = f"{self.config.base_url}/{int(year)}/export"
        query: dict[str, Any] = {"TYPE": export_type, "JSON": "1"}
        for key, value in (params or {}).items():
            if value is not None:
                query[key] = value

        cache_key = (int(year), export_type, tuple(sorted((str(k), str(v)) for k, v in query.items())))
        with self._cache_lock:
            if cache_key in self._response_cache:
                cached = self._response_cache[cache_key]
                if raise_on_error_payload and isinstance(cached, tuple) and cached and cached[0] == "__error__":
                    raise MFLAPIError(str(cached[1]), status_code=200, response=str(cached[1]))
                if isinstance(cached, tuple) and cached and cached[0] == "__error__":
                    return None
                return cached

        retries = self.config.http_max_retries
        for attempt in range(retries + 1):
            self._rate_limiter.wait()
            response = self.session.get(url, params=query, timeout=self.config.timeout)
            if response.status_code == 429:
                if attempt >= retries:
                    raise MFLAPIError(
                        f"MFL API throttled (429) for {export_type} year={year} after {retries + 1} attempts",
                        status_code=429,
                        response=response.text[:500],
                    )
                delay = self._throttle_delay(attempt)
                logger.warning(
                    "MFL API 429 for %s year=%s; sleeping %.0fs (attempt %s/%s)",
                    export_type,
                    year,
                    delay,
                    attempt + 1,
                    retries + 1,
                )
                time.sleep(delay)
                continue
            if response.status_code in (500, 502, 503, 504) and attempt < retries:
                delay = self._retry_delay(attempt)
                logger.warning(
                    "MFL API %s for %s year=%s; retrying in %.1fs (attempt %s/%s)",
                    response.status_code,
                    export_type,
                    year,
                    delay,
                    attempt + 1,
                    retries + 1,
                )
                time.sleep(delay)
                continue
            if response.status_code >= 400:
                raise MFLAPIError(
                    f"MFL API error {response.status_code} for {export_type} year={year}",
                    status_code=response.status_code,
                    response=response.text[:500],
                )
            if not response.content:
                with self._cache_lock:
                    self._response_cache[cache_key] = None
                return None

            try:
                payload = response.json()
            except ValueError as exc:
                raise MFLAPIError(
                    f"MFL API returned non-JSON for {export_type} year={year}: {exc}",
                    status_code=response.status_code,
                    response=response.text[:500],
                ) from exc

            error = payload_error(payload)
            if error is not None:
                with self._cache_lock:
                    self._response_cache[cache_key] = ("__error__", error)
                if raise_on_error_payload:
                    raise MFLAPIError(
                        f"MFL API error payload for {export_type} year={year}: {error}",
                        status_code=response.status_code,
                        response=error,
                    )
                logger.info("MFL API error payload for %s year=%s: %s", export_type, year, error)
                return None

            with self._cache_lock:
                self._response_cache[cache_key] = payload
            return payload
        return None

    # ------------------------------------------------------------------
    # Endpoint helpers (all public, no auth)
    # ------------------------------------------------------------------

    def fetch_league(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        """League settings + franchises + history. Raises on invalid league."""
        payload = self._get(year, "league", {"L": league_id}, raise_on_error_payload=True)
        return (payload or {}).get("league")

    def fetch_rules(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        payload = self._get(year, "rules", {"L": league_id})
        return (payload or {}).get("rules")

    def fetch_weekly_results(self, league_id: str | int, year: int, week: int) -> dict[str, Any] | None:
        payload = self._get(year, "weeklyResults", {"L": league_id, "W": week})
        return (payload or {}).get("weeklyResults")

    def prefetch_weekly_results_ytd(self, league_id: str | int, year: int) -> list[int]:
        """Seed the per-week weeklyResults cache from ONE W=YTD call.

        A season is ~17 weeklyResults calls at MFL's hard 15/min throttle -- the
        dominant cost of an ingest. W=YTD returns every week's block in one payload
        (shape: allWeeklyResults.weeklyResults[], each block identical to a single-
        week response). Seeding the response cache makes the existing per-week
        fetch loops cache hits, so their logic is untouched. Returns the weeks
        seeded; [] (old years / leagues without YTD support) means callers simply
        fall back to real per-week fetches.
        """
        payload = self._get(year, "weeklyResults", {"L": league_id, "W": "YTD"})
        blocks = ((payload or {}).get("allWeeklyResults") or {}).get("weeklyResults")
        blocks = [blocks] if isinstance(blocks, dict) else (blocks or [])
        seeded: list[int] = []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            try:
                wk = int(str(block.get("week")).strip())
            except (TypeError, ValueError):
                continue
            # Must mirror _get's cache key for fetch_weekly_results(league_id, year, wk).
            query: dict[str, Any] = {"TYPE": "weeklyResults", "JSON": "1", "L": league_id, "W": wk}
            cache_key = (int(year), "weeklyResults",
                         tuple(sorted((str(k), str(v)) for k, v in query.items())))
            with self._cache_lock:
                self._response_cache[cache_key] = {"weeklyResults": block}
            seeded.append(wk)
        if seeded:
            logger.info("MFL YTD prefetch %s/%s: seeded %d weeks in 1 call", league_id, year, len(seeded))
        return seeded

    def fetch_schedule(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        payload = self._get(year, "schedule", {"L": league_id})
        return (payload or {}).get("schedule")

    def fetch_standings(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        payload = self._get(year, "leagueStandings", {"L": league_id})
        return (payload or {}).get("leagueStandings")

    def fetch_playoff_brackets(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        payload = self._get(year, "playoffBrackets", {"L": league_id})
        return (payload or {}).get("playoffBrackets")

    def fetch_playoff_bracket(
        self, league_id: str | int, year: int, bracket_id: str | int
    ) -> dict[str, Any] | None:
        payload = self._get(year, "playoffBracket", {"L": league_id, "BRACKET_ID": bracket_id})
        return (payload or {}).get("playoffBracket")

    def fetch_draft_results(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        payload = self._get(year, "draftResults", {"L": league_id})
        return (payload or {}).get("draftResults")

    def fetch_auction_results(self, league_id: str | int, year: int) -> dict[str, Any] | None:
        payload = self._get(year, "auctionResults", {"L": league_id})
        return (payload or {}).get("auctionResults")

    def fetch_transactions(self, league_id: str | int, year: int) -> list[dict[str, Any]]:
        payload = self._get(year, "transactions", {"L": league_id})
        transactions = ((payload or {}).get("transactions") or {}).get("transaction")
        if isinstance(transactions, dict):
            return [transactions]
        if isinstance(transactions, list):
            return [item for item in transactions if isinstance(item, dict)]
        return []

    def fetch_players(self, year: int, league_id: str | int | None = None) -> dict[str, dict[str, Any]]:
        """Full player DB for a season, keyed by MFL player id. Cached per season.

        When MFL_PLAYERS_CACHE_DIR is set (corpus batch runs), the GLOBAL per-year DB
        is served from disk across PROCESSES: the batch driver runs one importer
        subprocess per league, so an in-memory cache re-fetched the same ~2MB season
        DB for every league-year. The global DB is a superset of any league-scoped
        one (lookups are by player id), so sharing it is safe.
        """
        import json as _json
        import os as _os
        from pathlib import Path as _Path

        cache_dir = _os.environ.get("MFL_PLAYERS_CACHE_DIR")
        disk_path = None
        if cache_dir:
            league_id = None  # force the shared global DB
            disk_path = _Path(cache_dir) / f"mfl_players_{int(year)}.json"

        cache_key = (int(year), str(league_id) if league_id is not None else "")
        cached = self._players_cache.get(cache_key)
        if cached is not None:
            return cached
        if disk_path is not None and disk_path.is_file():
            try:
                lookup = _json.loads(disk_path.read_text(encoding="utf-8"))
                self._players_cache[cache_key] = lookup
                return lookup
            except Exception:
                pass  # unreadable/corrupt cache file -> refetch below

        params: dict[str, Any] = {"DETAILS": "1"}
        if league_id is not None:
            params["L"] = league_id
        payload = self._get(year, "players", params)
        players = ((payload or {}).get("players") or {}).get("player")
        if isinstance(players, dict):
            players = [players]
        lookup: dict[str, dict[str, Any]] = {}
        for record in players or []:
            if not isinstance(record, dict):
                continue
            player_id = str(record.get("id") or "").strip()
            if player_id:
                lookup[player_id] = record
        self._players_cache[cache_key] = lookup
        if disk_path is not None and lookup:
            try:
                disk_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = disk_path.with_suffix(f".tmp{_os.getpid()}")
                tmp.write_text(_json.dumps(lookup), encoding="utf-8")
                _os.replace(tmp, disk_path)
            except Exception:
                pass  # disk cache is best-effort
        logger.info("Fetched MFL player DB for %s: %s players", year, len(lookup))
        return lookup

    def fetch_league_search(self, search: str, year: int) -> list[dict[str, Any]]:
        payload = self._get(year, "leagueSearch", {"SEARCH": search})
        leagues = ((payload or {}).get("leagues") or {}).get("league")
        if isinstance(leagues, dict):
            return [leagues]
        return [item for item in leagues or [] if isinstance(item, dict)]
