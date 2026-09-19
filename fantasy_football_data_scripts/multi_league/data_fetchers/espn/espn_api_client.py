"""
ESPN API Client

Thin wrapper around the espn_api library that:
1. Creates League objects with auth cookies
2. Handles year-specific league instantiation
3. Provides raw API access for trades (bypassing broken library Transaction class)
4. Caches League objects per year to avoid re-fetching

The espn_api library handles most ESPN Fantasy API interactions, but has
known issues with trade transactions (KeyError on missing 'status' key).
For trades, we hit the raw ESPN API directly with mTransactions2 view.
"""

import json
import logging
import time
import traceback
from typing import Any

import requests

import sys

logger = logging.getLogger(__name__)
_verbose = "--verbose" in sys.argv

# ESPN Fantasy API base URL
ESPN_FANTASY_BASE_URL = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"


def log(msg: str):
    logger.info(msg)
    print(msg)


def _legacy_draft_scalar(value: Any) -> Any:
    """Return ESPN's scalar ID when legacy draft payloads wrap it in a list."""
    while isinstance(value, (list, tuple)):
        value = value[0] if value else None
    return value


_LEGACY_ESPN_ID_FIELDS = {
    "id",
    "teamId",
    "playerId",
    "nominatingTeamId",
    "proTeamId",
    "defaultPositionId",
    "lineupSlotId",
    "divisionId",
    "seasonId",
    "scoringPeriodId",
    "statId",
    "statSplitTypeId",
    "statSourceId",
    "positionId",
    "matchupPeriodId",
}


def _normalize_legacy_espn_ids(value: Any) -> Any:
    """Unwrap only singular ID fields in ESPN's pre-2018 payloads."""
    if isinstance(value, list):
        return [_normalize_legacy_espn_ids(item) for item in value]
    if not isinstance(value, dict):
        return value
    normalized = {}
    for key, item in value.items():
        if key in _LEGACY_ESPN_ID_FIELDS:
            normalized[key] = _legacy_draft_scalar(item)
        elif key == "eligibleSlots" and isinstance(item, list):
            # ESPN's 2006 player payload can contain [[2], [3]] here;
            # espn_api uses each value as a POSITION_MAP key.
            normalized[key] = [_legacy_draft_scalar(slot) for slot in item]
        else:
            normalized[key] = _normalize_legacy_espn_ids(item)

    # Some 2006 roster entries are structurally incomplete: ESPN includes a
    # null ``playerPoolEntry`` even though espn_api always dereferences its
    # ``player`` child.  Retain the roster entry (and its root-level fields),
    # supplying an empty player object only for that malformed legacy shape.
    if "playerPoolEntry" in normalized:
        entry = normalized["playerPoolEntry"]
        if entry is None:
            normalized["playerPoolEntry"] = {"player": normalized.get("player") or {}}
        elif isinstance(entry, dict) and not isinstance(entry.get("player"), dict):
            entry["player"] = normalized.get("player") or {}
    return normalized


def _find_legacy_key(value: Any, target_key: str) -> Any:
    """Find a key through all nested ESPN legacy list/dict shapes."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key == target_key:
                # Legacy roster entries may include an empty outer ID before
                # the populated value in playerPoolEntry.player.
                if _legacy_draft_scalar(item) is not None:
                    return item
            found = _find_legacy_key(item, target_key)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for item in value:
            found = _find_legacy_key(item, target_key)
            if found is not None:
                return found
    return None


def _fetch_legacy_draft_with_list_ids(league: Any) -> None:
    """Populate ``league.draft`` when ESPN's old payload uses list-valued IDs.

    ESPN's 2006 response for some leagues wraps draft ``playerId``/``teamId``
    values in one-element lists.  ``espn_api`` uses those values as dictionary
    keys and raises ``TypeError: unhashable type: 'list'`` before the league can
    be used by any fetcher.  This is the library's normal draft parser with
    those legacy IDs normalized first.
    """
    from espn_api.base_pick import BasePick

    data = league.espn_request.get_league_draft()
    if not data.get("draftDetail", {}).get("drafted"):
        return

    for pick in data.get("draftDetail", {}).get("picks", []):
        team_id = _legacy_draft_scalar(pick.get("teamId"))
        player_id = _legacy_draft_scalar(pick.get("playerId"))
        nominating_team_id = _legacy_draft_scalar(pick.get("nominatingTeamId"))
        team = league.get_team_data(team_id)
        player_name = league.player_map.get(player_id, "")
        league.draft.append(
            BasePick(
                team,
                player_id,
                player_name,
                pick.get("roundId"),
                pick.get("roundPickNumber"),
                pick.get("bidAmount"),
                pick.get("keeper"),
                league.get_team_data(nominating_team_id),
            )
        )


def _fetch_legacy_players_with_list_ids(league: Any) -> None:
    """Build the ESPN player map without using legacy list-valued IDs as keys."""
    for player in league.espn_request.get_pro_players():
        player_id = _legacy_draft_scalar(player.get("id"))
        player_name = player.get("fullName")
        if player_id is None or not player_name:
            continue
        league.player_map[player_id] = player_name
        league.player_map.setdefault(player_name, player_id)


def _fetch_legacy_league_with_list_ids(league: Any) -> None:
    """Run ESPN's normal league initialization with the safe legacy draft parser."""
    from espn_api.base_league import BaseLeague
    from espn_api.football import player as player_module
    from espn_api.football.settings import Settings

    # ``espn_api.football.player.json_parsing`` recursively searches a player
    # object and returns the raw value it finds.  ESPN's 2006 roster embeds
    # some singular IDs in nested arrays, so normalize at that final boundary
    # too (not just in the source payload).
    if not getattr(player_module.json_parsing, "_leaguehistory_legacy_ids", False):
        original_json_parsing = player_module.json_parsing

        def _legacy_player_json_parsing(obj: Any, key: str) -> Any:
            value = original_json_parsing(obj, key)
            if key not in _LEGACY_ESPN_ID_FIELDS:
                return value
            # espn_api returns [] when the value is nested under [[...]].
            if value in (None, []):
                value = _find_legacy_key(obj, key)
            value = _legacy_draft_scalar(value)
            # A small number of 2006 roster entries have no NFL team ID at
            # all.  espn_api represents team 0 as the unknown/None team, so
            # use that documented sentinel rather than preventing the entire
            # historical league from loading.
            if key == "proTeamId" and value is None:
                return 0
            return value

        _legacy_player_json_parsing._leaguehistory_legacy_ids = True
        player_module.json_parsing = _legacy_player_json_parsing

    raw_get_league = league.espn_request.get_league
    league.espn_request.get_league = lambda: _normalize_legacy_espn_ids(raw_get_league())
    raw_get_pro_schedule = league.espn_request.get_pro_schedule
    league.espn_request.get_pro_schedule = lambda: _normalize_legacy_espn_ids(raw_get_pro_schedule())
    data = BaseLeague._fetch_league(league, SettingsClass=Settings)
    league.nfl_week = data["status"]["latestScoringPeriod"]
    _fetch_legacy_players_with_list_ids(league)
    league._fetch_teams(data)
    _fetch_legacy_draft_with_list_ids(league)


class ESPNAPIError(Exception):
    """ESPN API error with status code tracking."""

    def __init__(self, message: str, status_code: int = None, response: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class ESPNAPIClient:
    """
    Wrapper around espn_api library with raw API access for edge cases.

    Usage:
        client = ESPNAPIClient(71580, espn_s2="...", swid="{...}")
        league = client.get_league(2024)
        trades = client.get_raw_transactions(2024, scoring_period=1)
    """

    def __init__(
        self, league_id: int, espn_s2: str = None, swid: str = None,
        *, request_timeout: float = 30,
    ):
        self.league_id = league_id
        self.espn_s2 = espn_s2
        self.swid = swid
        self._league_cache: dict[int, Any] = {}
        self._history_routes: dict[int, bool] = {}
        self._session = requests.Session()
        self._request_timeout = request_timeout

        # Set cookies for private league access
        if espn_s2 and swid:
            self._session.cookies.set("espn_s2", espn_s2)
            self._session.cookies.set("SWID", swid)

    def get_league(self, year: int) -> Any:
        """
        Get League object for a specific year (cached).

        Args:
            year: NFL season year

        Returns:
            espn_api.football.League object

        Raises:
            ESPNAPIError: If league cannot be loaded
        """
        if year in self._league_cache:
            return self._league_cache[year]

        try:
            from espn_api.football import League

            kwargs = {"league_id": self.league_id, "year": year}
            if self.espn_s2:
                kwargs["espn_s2"] = self.espn_s2
            if self.swid:
                kwargs["swid"] = self.swid

            try:
                league = League(**kwargs)
            except TypeError as e:
                # ESPN's 2006 response has two malformed roster shapes.  A
                # fresh espn_api parser sees list-valued IDs; after its player
                # helper has been normalized once, a normal retry can instead
                # reach a null playerPoolEntry.  Both must use the same legacy
                # payload adapter on every independently-created League.
                legacy_errors = (
                    "unhashable type: 'list'",
                    "'NoneType' object is not subscriptable",
                )
                if year >= 2018 or not any(message in str(e) for message in legacy_errors):
                    raise
                log(f"  [ESPN] Retrying {year} with legacy draft ID normalization")
                league = League(**kwargs, fetch_league=False)
                try:
                    _fetch_legacy_league_with_list_ids(league)
                except Exception:
                    log(f"  [ESPN] Legacy {year} initialization traceback:\n{traceback.format_exc()}")
                    raise

            # espn_api can discover an archive after a modern endpoint's 401.
            # Raw views must reuse that route; archive rosters are season
            # snapshots, so fetchers must use their existing historical paths.
            self._history_routes[year] = "/leagueHistory/" in league.espn_request.LEAGUE_ENDPOINT
            league._uses_league_history = self._history_routes[year]

            # Patch inactive/historical leagues where ESPN returns scoringPeriodId=0
            # The espn_api library sets current_week=0 for these, which causes
            # box_scores(week) to always send scoringPeriodId=0 (the guard
            # `week <= current_week` → `1 <= 0` → False), returning empty data.
            final_period = getattr(league, "finalScoringPeriod", 0) or 0
            current_week = getattr(league, "current_week", 0) or 0
            scoring_period = getattr(league, "scoringPeriodId", None)

            if _verbose:
                log(
                    f"  [ESPN] League {self.league_id} year {year}: "
                    f"current_week={current_week}, finalScoringPeriod={final_period}, "
                    f"scoringPeriodId={scoring_period}"
                )

            if current_week < 1 and final_period > 0:
                if _verbose:
                    log(
                        f"  [ESPN] Inactive league detected: current_week={current_week}, "
                        f"overriding with finalScoringPeriod={final_period}"
                    )
                league.current_week = final_period
                # Also fix currentMatchupPeriod if it's 0
                if getattr(league, "currentMatchupPeriod", 0) < 1:
                    league.currentMatchupPeriod = final_period

            self._league_cache[year] = league
            return league

        except Exception as e:
            error_msg = str(e).lower()
            if "404" in error_msg or "not found" in error_msg or "does not exist" in error_msg:
                raise ESPNAPIError(f"League {self.league_id} not found for year {year}", status_code=404)
            if "401" in error_msg or "unauthorized" in error_msg or "private" in error_msg:
                raise ESPNAPIError(f"League {self.league_id} unauthorized for year {year}", status_code=401)
            raise ESPNAPIError(f"Failed to load ESPN league {self.league_id} for {year}: {e}")

    def get_raw_transactions(
        self, year: int, scoring_period: int, *, strict: bool = False,
    ) -> list:
        """
        Fetch raw transaction data from ESPN API for a specific scoring period.

        This bypasses the espn_api library's Transaction class which crashes
        on TRADE_ACCEPT records missing the 'status' key.

        Args:
            year: NFL season year
            scoring_period: Week/scoring period number

        Returns:
            List of raw transaction dicts from ESPN API
        """
        params = self._build_league_params(year, "mTransactions2")
        params["scoringPeriodId"] = scoring_period

        try:
            data = self._request_league(year, params)
            if strict and (
                not isinstance(data, dict)
                or not isinstance(data.get("transactions"), list)
                or any(not isinstance(row, dict) for row in data["transactions"])
            ):
                raise ESPNAPIError(
                    f"ESPN raw transactions for {year} week {scoring_period} are malformed"
                )
            return data.get("transactions", [])
        except requests.exceptions.HTTPError as e:
            if not strict and e.response and e.response.status_code == 404:
                return []
            raise ESPNAPIError(
                f"Failed to fetch raw transactions for {year} week {scoring_period}: {e}",
                status_code=getattr(e.response, "status_code", None),
            )
        except Exception as e:
            if strict:
                if isinstance(e, ESPNAPIError):
                    raise
                raise ESPNAPIError(
                    f"Failed to fetch ESPN raw transactions for {year} week {scoring_period}"
                ) from e
            logger.warning(f"Error fetching raw transactions for {year} week {scoring_period}: {e}")
            return []

    def get_raw_schedule(self, year: int, scoring_period: int) -> list[dict]:
        """
        Fetch raw matchup schedule for a specific scoring period.

        This preserves ESPN-only fields like commissioner adjustments and
        tiebreak values that the BoxScore abstraction drops.
        """
        params = self._build_league_params(year)
        params["view"] = ["mScoreboard", "mMatchupScore"]
        params["scoringPeriodId"] = scoring_period
        headers = {
            "x-fantasy-filter": json.dumps({"schedule": {"filterMatchupPeriodIds": {"value": [scoring_period]}}})
        }

        try:
            data = self._request_league(year, params, headers=headers)
            if not isinstance(data, dict):
                return []
            schedule = data.get("schedule", [])
            return schedule if isinstance(schedule, list) else []
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                return []
            logger.warning(f"Error fetching raw ESPN schedule for {year} week {scoring_period}: {e}")
            return []
        except Exception as e:
            logger.warning(f"Error fetching raw ESPN schedule for {year} week {scoring_period}: {e}")
            return []

    def get_raw_trades(
        self, year: int, max_weeks: int = 18, *, strict: bool = False,
    ) -> list[dict]:
        """
        Fetch all trade transactions for a year via raw ESPN API.

        Strategy:
        1. Collect all TRADE_PROPOSAL (has items) and TRADE_ACCEPT (stub) records
        2. Try to join accept → proposal via relatedTransactionId
        3. If that fails (ESPN purges old proposals), fall back to matching
           by team pair — find any proposal involving the same teams

        Args:
            year: NFL season year
            max_weeks: Maximum scoring periods to check

        Returns:
            List of trade dicts with full player movement details
        """
        all_transactions = []
        proposals_by_id = {}  # proposal_id -> proposal record
        proposals_by_teams = {}  # frozenset(team_ids) -> [proposals]
        seen_accept_ids = set()

        for period in range(1, max_weeks + 1):
            txns = self.get_raw_transactions(year, period, strict=strict)

            for txn in txns:
                txn_type = txn.get("type")

                if txn_type == "TRADE_PROPOSAL":
                    txn_id = txn.get("id")
                    items = txn.get("items", [])
                    if txn_id:
                        proposals_by_id[txn_id] = txn
                    # Index by team pair for fallback matching
                    if items:
                        teams = frozenset({i.get("fromTeamId") for i in items} | {i.get("toTeamId") for i in items})
                        proposals_by_teams.setdefault(teams, []).append(txn)

                elif txn_type == "TRADE_ACCEPT":
                    accept_id = txn.get("id")
                    if accept_id in seen_accept_ids:
                        continue
                    seen_accept_ids.add(accept_id)

                    # Try 1: match via relatedTransactionId
                    related_id = txn.get("relatedTransactionId")
                    proposal = proposals_by_id.get(related_id, {})
                    items = proposal.get("items", txn.get("items", []))

                    # Try 2: if no items, match by team pair
                    if not items:
                        accept_team = txn.get("teamId")
                        for teams, props in proposals_by_teams.items():
                            if accept_team in teams and props:
                                # Use the latest proposal for these teams, then consume it
                                proposal = props.pop()
                                items = proposal.get("items", [])
                                break

                    trade = {
                        "id": txn.get("id"),
                        "scoringPeriodId": txn.get("scoringPeriodId"),
                        "bidAmount": txn.get("bidAmount", 0),
                        "executionType": txn.get("executionType"),
                        "proposalDate": proposal.get("proposalDate") if proposal else None,
                        "acceptDate": txn.get("processDate") or txn.get("proposalDate"),
                        "items": items,
                        "teamId": txn.get("teamId"),
                        "memberId": txn.get("memberId"),
                    }
                    all_transactions.append(trade)

            time.sleep(0.05)

        return all_transactions

    def get_raw_trades_strict(self, year: int, *, max_weeks: int) -> list[dict]:
        return self.get_raw_trades(year, max_weeks=max_weeks, strict=True)

    def get_raw_waivers(
        self, year: int, max_weeks: int = 18, *, strict: bool = False,
    ) -> list:
        """
        Fetch waiver/FA transactions via raw ESPN API (bypasses broken library).

        The library's transactions() method only returns data for the current
        scoring period. For historical years, we must hit the raw API per period.

        Args:
            year: NFL season year
            max_weeks: Maximum scoring periods to check

        Returns:
            List of raw transaction dicts for FREEAGENT and WAIVER types
        """
        all_waivers = []
        waiver_types = {"FREEAGENT", "WAIVER"}
        seen_ids = set()

        for period in range(1, max_weeks + 1):
            try:
                raw = self.get_raw_transactions(year, period, strict=strict)
                for txn in raw:
                    txn_type = txn.get("type")
                    txn_id = txn.get("id")
                    if txn_type in waiver_types and txn_id not in seen_ids:
                        seen_ids.add(txn_id)
                        all_waivers.append(txn)
            except Exception:  # noqa: broad-except
                if strict:
                    raise
                continue
            time.sleep(0.05)  # Rate limit

        return all_waivers

    def get_raw_waivers_strict(self, year: int, *, max_weeks: int) -> list:
        return self.get_raw_waivers(year, max_weeks=max_weeks, strict=True)

    def discover_available_years(
        self,
        years: list[int] | set[int] | tuple[int, ...] | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[int]:
        """
        Probe years to find all available seasons.

        With no bounds, probes backwards from current year and stops after
        repeated misses to avoid scanning every historical season for young
        leagues. With explicit years or bounds, probes only those scoped years
        and does not early-stop; recent private/gap seasons can be followed by
        accessible historical seasons in multi-platform imports.

        Returns:
            List of years with available data, sorted ascending
        """
        scoped_scan = years is not None or start_year is not None or end_year is not None
        from datetime import datetime

        now = datetime.now()
        latest_real_season = now.year if now.month >= 9 else now.year - 1
        if years is not None:
            probe_years = []
            for year in years:
                try:
                    parsed = int(year)
                except (TypeError, ValueError):
                    continue
                if 2004 <= parsed <= latest_real_season:
                    probe_years.append(parsed)
            probe_years = sorted(set(probe_years), reverse=True)
        else:
            current_year = get_current_nfl_season_year()
            # Cap to the latest season with real game data (date-based).
            # Sleeper API may return a future season year during the offseason.
            current_year = min(current_year, latest_real_season)
            if end_year is not None:
                current_year = min(current_year, int(end_year))
            lower_bound = max(2004, int(start_year)) if start_year is not None else 2004
            probe_years = list(range(current_year, lower_bound - 1, -1))
        available_years = []
        MAX_CONSECUTIVE_FAILURES = 5

        consecutive_failures = 0
        for year in probe_years:
            try:
                league = self.get_league(year)
                if league and hasattr(league, "teams") and league.teams:
                    # Verify the season has actual scoring data (not just a
                    # pre-season shell).  ESPN creates league objects for the
                    # upcoming season before any games are played.
                    has_scores = False
                    if hasattr(league, "current_week") and league.current_week and league.current_week > 1:
                        has_scores = True
                    elif hasattr(league, "scoreboard") and league.scoreboard:
                        has_scores = True
                    else:
                        # Check if any team has non-zero points
                        for team in league.teams:
                            if hasattr(team, "points_for") and team.points_for and team.points_for > 0:
                                has_scores = True
                                break
                    if not has_scores:
                        log(f"  [DISCOVERY] {year} has league shell but no scores — skipping")
                        consecutive_failures += 1
                        if not scoped_scan and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                            log(f"  [DISCOVERY] {consecutive_failures} consecutive failures — stopping")
                            break
                        continue
                    available_years.append(year)
                    consecutive_failures = 0
                    log(f"  [DISCOVERY] Found data for {year}")
                else:
                    log(f"  [DISCOVERY] No data for {year}")
                    consecutive_failures += 1
                    if not scoped_scan and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        log(f"  [DISCOVERY] {consecutive_failures} consecutive failures — stopping")
                        break
                    continue
            except ESPNAPIError as e:
                if e.status_code == 404:
                    log(f"  [DISCOVERY] 404 for {year}")
                elif e.status_code == 401:
                    log(f"  [DISCOVERY] 401 (private) for {year}")
                else:
                    log(f"  [DISCOVERY] Error for {year}: {e}")
                consecutive_failures += 1
                if not scoped_scan and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    log(f"  [DISCOVERY] {consecutive_failures} consecutive failures — stopping")
                    break
                continue
            except Exception as e:
                log(f"  [DISCOVERY] Unexpected error for {year}: {e}")
                consecutive_failures += 1
                if not scoped_scan and consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    log(f"  [DISCOVERY] {consecutive_failures} consecutive failures — stopping")
                    break
                continue

        return sorted(available_years)

    def validate_league(self) -> dict:
        """
        Lightweight league validation using raw ESPN API (no espn_api library).

        Probes recent years to find any accessible season. Returns basic info
        without going through the espn_api library's fragile parsing.

        Returns:
            Dict with 'name', 'num_teams', 'year' if league found.

        Raises:
            ESPNAPIError: If league is private (401) or not found at all.
        """
        current_year = get_current_nfl_season_year()

        for try_year in range(current_year, current_year - 5, -1):
            url = f"{ESPN_FANTASY_BASE_URL}/seasons/{try_year}/segments/0/leagues/{self.league_id}"
            params = {"view": "mSettings"}
            try:
                resp = self._session.get(url, params=params, timeout=15)
                if resp.status_code == 401:
                    raise ESPNAPIError(
                        f"League {self.league_id} is private. Authentication required.",
                        status_code=401,
                    )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                data = resp.json()
                settings = data.get("settings", {})
                name = settings.get("name", f"ESPN League {self.league_id}")
                # Count teams from the members/teams in the response
                num_teams = settings.get("size", 0)
                if num_teams == 0:
                    teams = data.get("teams", [])
                    num_teams = len(teams) if teams else 10
                return {"name": name, "num_teams": num_teams, "year": try_year}
            except ESPNAPIError:
                raise
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 401:
                    raise ESPNAPIError(
                        f"League {self.league_id} is private. Authentication required.",
                        status_code=401,
                    )
                continue
            except Exception:  # noqa: broad-except
                continue
        raise ESPNAPIError(f"League {self.league_id} not found on ESPN.", status_code=404)

    def _build_league_url(self, year: int, *, history: bool | None = None) -> str:
        """Use the verified route, with the library's era rule as the initial guess."""
        if history is None:
            history = self._history_routes.get(year, year < 2018)
        if history:
            return f"{ESPN_FANTASY_BASE_URL}/leagueHistory/{self.league_id}"
        return f"{ESPN_FANTASY_BASE_URL}/seasons/{year}/segments/0/leagues/{self.league_id}"

    def _build_league_params(self, year: int, view: str = "mSettings") -> dict:
        """Build query params for the selected league endpoint."""
        params = {"view": view}
        if self._history_routes.get(year, year < 2018):
            params["seasonId"] = str(year)
        return params

    def _request_league(self, year: int, params: dict, *, headers=None) -> dict:
        """Share ESPN's 401 route fallback and archive envelope across all views."""
        preferred = self._history_routes.get(year, year < 2018)
        for history in (preferred, not preferred):
            query = dict(params)
            query.pop("seasonId", None)
            if history:
                query["seasonId"] = str(year)
            resp = self._session.get(
                self._build_league_url(year, history=history),
                params=query, headers=headers, timeout=self._request_timeout,
            )
            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as exc:
                if history == preferred and getattr(exc.response, "status_code", None) == 401:
                    continue
                raise
            data = resp.json()
            if isinstance(data, list):
                data = data[0] if data else {}
            if not isinstance(data, dict):
                return {}
            if str(data.get("seasonId", year)) != str(year):
                raise ESPNAPIError(f"ESPN returned a different season for {year}")
            self._history_routes[year] = history
            return data

    def get_raw_league(self, year: int, views: str | list[str] | tuple[str, ...] = "mSettings", **extra_params) -> dict:
        """Fetch a raw ESPN league payload and normalize leagueHistory arrays."""
        if isinstance(views, str):
            params = self._build_league_params(year, views)
        else:
            first_view = views[0] if views else "mSettings"
            params = self._build_league_params(year, first_view)
            params["view"] = list(views)
        params.update(extra_params)

        return self._request_league(year, params)

    def get_league_settings_raw(self, year: int) -> dict:
        """
        Fetch raw league settings from ESPN API.

        Args:
            year: NFL season year

        Returns:
            Dict with raw settings data
        """
        try:
            data = self.get_raw_league(year, "mSettings")
            return data.get("settings", {})
        except Exception as e:
            logger.warning(f"Failed to fetch settings for {year}: {e}")
            return {}

    def get_raw_team_playoff_seed_map(self, year: int) -> dict[int, int]:
        """Return ESPN's team playoffSeed values keyed by team id."""
        try:
            data = self.get_raw_league(year, "mTeam")
        except Exception as e:
            logger.warning(f"Failed to fetch team playoff seeds for {year}: {e}")
            return {}

        seeds: dict[int, int] = {}
        for team in data.get("teams", []) or []:
            try:
                team_id = int(team.get("id"))
                seed = team.get("playoffSeed")
                if seed is not None:
                    seeds[team_id] = int(seed)
            except (TypeError, ValueError):
                continue
        return seeds


def get_current_nfl_season_year():
    """Get current NFL season year (imported from date_utils)."""
    from multi_league.core.date_utils import get_current_nfl_season_year as _get_year

    return _get_year()
