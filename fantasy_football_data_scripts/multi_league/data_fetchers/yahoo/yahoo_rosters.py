#!/usr/bin/env python3
"""
Yahoo Fantasy Roster Data Fetcher - Multi-League Edition

Fetches weekly roster data showing which manager owned which player each week.
Compatible with multi-league infrastructure.

Output includes:
- manager_name: Team owner
- player_name: Player name
- yahoo_position: Yahoo's position designation
- primary_position: Primary position (QB, RB, WR, TE, K, DEF)
- fantasy_position: Roster slot (QB, RB1, RB2, FLEX, BN, etc.)
- year, week
- optional Yahoo fantasy_points when fetched from the team-level endpoint

Usage:
    # Using league context (RECOMMENDED)
    python yahoo_fantasy_data.py --context path/to/league_context.json

    # Fetch specific year
    python yahoo_fantasy_data.py --context path/to/league_context.json --year 2024

    # Fetch specific year and week
    python yahoo_fantasy_data.py --context path/to/league_context.json --year 2024 --week 5
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.date_utils import get_current_nfl_season_year

import pandas as pd

_verbose = "--verbose" in sys.argv

# Import shared name normalization
try:
    from ..shared.clean_names import normalize_manager_name
except ImportError:
    from multi_league.data_fetchers.shared.clean_names import normalize_manager_name

# Import from multi_league.core
try:
    from core.league_context import LeagueContext
    from core.league_discovery import LeagueDiscovery
    from core.script_runner import log
except ImportError as e:
    print(f"ERROR: Failed to import multi_league modules: {e}")
    print("Make sure you're running from the correct directory.")
    sys.exit(1)

# Try to import Yahoo OAuth
try:
    from yahoo_oauth import OAuth2

    YAHOO_OAUTH_AVAILABLE = True
except ImportError:
    OAuth2 = None
    YAHOO_OAUTH_AVAILABLE = False
    print("Warning: yahoo_oauth not available. Install with: pip install yahoo_oauth")


def retry_with_backoff(func, max_retries=3, initial_delay=1.0, backoff_factor=2.0):
    """
    Retry a function with exponential backoff.

    Thin wrapper around the shared retry_api_call utility, preserving the
    original call signature for backward compatibility.

    Args:
        func: Function to call (should take no arguments)
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds (default: 1.0)
        backoff_factor: Multiplier for delay after each retry (default: 2.0)

    Returns:
        Result of func() if successful

    Raises:
        Last exception if all retries fail
    """
    from multi_league.data_fetchers.shared.retry_utils import retry_api_call

    return retry_api_call(func, max_retries=max_retries)


class YahooRosterFetcher:
    """
    Fetch weekly roster data from Yahoo Fantasy API.

    Shows which manager had which player in which roster slot each week.
    """

    def __init__(
        self,
        oauth_file: Path | None,
        league_id: str,
        rate_limit: float = 2.0,
        max_retries: int = 5,
        output_dir: Path | None = None,
        manager_name_overrides: dict[str, str] | None = None,
        oauth_session=None,
    ):
        """
        Initialize the roster fetcher.

        Args:
            oauth_file: Path to OAuth JSON file
            league_id: Yahoo league key (e.g., "331.l.381581")
            rate_limit: Max requests per second
            max_retries: Max retry attempts
            output_dir: Directory to save output files
            manager_name_overrides: Dict mapping team names/nicknames to real manager names
        """
        self.league_id = league_id
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manager_name_overrides = manager_name_overrides or {}

        # Cookie imports provide the already-authenticated browser transport.
        # Never re-create OAuth from a synthetic file in that case.
        self.oauth = oauth_session if oauth_session is not None else self._initialize_oauth(oauth_file)

        # Rate limiting (thread-safe)
        self.last_request_time = 0.0
        self.request_count = 0
        self._rate_lock = threading.Lock()

    def _initialize_oauth(self, oauth_file: Path) -> OAuth2:
        """Initialize OAuth session."""
        if not YAHOO_OAUTH_AVAILABLE:
            raise ImportError("yahoo_oauth is required. Install with: pip install yahoo_oauth")

        if not oauth_file.exists():
            raise FileNotFoundError(f"OAuth file not found: {oauth_file}")

        log(f"Initializing OAuth from file: {oauth_file}")
        oauth = OAuth2(None, None, from_file=str(oauth_file))

        # Validate token
        if not oauth.token_is_valid():
            log("Refreshing OAuth token")
            oauth.refresh_access_token()

        return oauth

    def _rate_limit_wait(self):
        """Wait if necessary to respect rate limit (thread-safe)."""
        if self.rate_limit <= 0:
            return

        with self._rate_lock:
            elapsed = time.time() - self.last_request_time
            min_interval = 1.0 / self.rate_limit

            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                time.sleep(wait_time)

            self.last_request_time = time.time()

    def _refresh_access_token(self, reason: str) -> bool:
        """Refresh Yahoo OAuth credentials after an auth failure."""
        if not hasattr(self.oauth, "refresh_access_token"):
            log(f"[AUTH] {reason}; OAuth session cannot refresh access tokens")
            return False

        try:
            log(f"[AUTH] {reason}; refreshing Yahoo access token...")
            self.oauth.refresh_access_token()
            log("[AUTH] Token refreshed successfully")
            return True
        except Exception as exc:  # noqa: BLE001
            log(f"[AUTH] Token refresh failed: {exc}")
            return False

    def _classify_request_failure(
        self,
        *,
        response: Any | None = None,
        error: Exception | None = None,
        body_text: str | None = None,
    ) -> str:
        """Classify Yahoo failures so logs reflect auth vs throttling vs denial."""
        status_code = getattr(response, "status_code", None)
        combined = " ".join(part for part in [body_text or "", str(error or "")]).lower()

        if (
            status_code == 401
            or "unauthorized" in combined
            or "invalid cookie" in combined
            or "please log in again" in combined
            or "must be logged in" in combined
            or "token expired" in combined
        ):
            return "auth"

        if (
            status_code == 429
            or "rate limit" in combined
            or "limit exceeded" in combined
            or "too many requests" in combined
        ):
            return "rate_limit"

        if (
            status_code == 403
            or "request denied" in combined
            or "access denied" in combined
            or "forbidden access" in combined
            or "permission error" in combined
            or "permission denied" in combined
        ):
            return "access_denied"

        return "other"

    def _fetch_url_xml(self, url: str) -> ET.Element:
        """Fetch XML from Yahoo API with retries."""
        last_error = None
        backoff = 0.5
        rate_limit_retries = 0
        auth_refresh_retries = 0
        MAX_AUTH_REFRESH_RETRIES = 2
        MAX_RATE_LIMIT_RETRIES = 2  # Two retries with cooldown — give rate limit window time to recover

        for attempt in range(self.max_retries):
            try:
                self._rate_limit_wait()

                response = self.oauth.session.get(url, timeout=30)
                text = response.text or ""
                failure_kind = self._classify_request_failure(response=response, body_text=text)

                if failure_kind == "auth":
                    if auth_refresh_retries < MAX_AUTH_REFRESH_RETRIES and self._refresh_access_token(
                        f"Yahoo API auth failure while fetching {url} (status {response.status_code})"
                    ):
                        auth_refresh_retries += 1
                        time.sleep(min(2.0, backoff * (2**attempt)))
                        continue
                    raise RuntimeError(f"Yahoo API authentication failed for {url} (status {response.status_code})")

                if failure_kind == "rate_limit":
                    raise RuntimeError("Yahoo API rate limit exceeded")

                if failure_kind == "access_denied":
                    raise RuntimeError("Yahoo API access denied")

                response.raise_for_status()

                self.request_count += 1

                failure_kind = self._classify_request_failure(body_text=text)
                if failure_kind == "rate_limit":
                    raise RuntimeError("Yahoo API rate limit exceeded")
                if failure_kind == "access_denied":
                    raise RuntimeError("Yahoo API access denied")

                # Remove XML namespace for easier parsing
                text = pd.Series(text).str.replace(r' xmlns="[^"]+"', "", n=1, regex=True).iloc[0]

                return ET.fromstring(text)

            except Exception as e:
                last_error = e
                failure_kind = self._classify_request_failure(error=e)

                if failure_kind == "auth":
                    if auth_refresh_retries < MAX_AUTH_REFRESH_RETRIES and self._refresh_access_token(
                        f"Yahoo API auth failure while fetching {url}"
                    ):
                        auth_refresh_retries += 1
                        time.sleep(min(2.0, backoff * (2**attempt)))
                        continue
                    log(f"[AUTH] Authentication retries exhausted for {url}: {e}")
                    break

                if failure_kind == "access_denied":
                    if not getattr(self, "_access_denied_logged", False):
                        log("[ACCESS DENIED] Yahoo API rate limited — skipping remaining requests until cooldown")
                        self._access_denied_logged = True
                    break

                if failure_kind == "rate_limit":
                    rate_limit_retries += 1
                    if rate_limit_retries > MAX_RATE_LIMIT_RETRIES:
                        log(f"[RATE-LIMIT] Exhausted {MAX_RATE_LIMIT_RETRIES} retries for {url}; giving up")
                        break
                    cooldown = 120
                    log(f"[RATE LIMIT] Retry {rate_limit_retries}/{MAX_RATE_LIMIT_RETRIES}: {e}")
                    log(f"Cooling down for {cooldown // 60}m...")
                    time.sleep(cooldown)
                    log("Resuming after cooldown")
                    continue

                error_msg = str(e).lower()

                # Legacy fallback for uncategorized throttling-only failures.
                is_rate_limit = (
                    "rate limit" in error_msg or "limit exceeded" in error_msg or "too many requests" in error_msg
                )
                if is_rate_limit:
                    rate_limit_retries += 1
                    if rate_limit_retries > MAX_RATE_LIMIT_RETRIES:
                        log(f"Rate limit: exhausted {MAX_RATE_LIMIT_RETRIES} retries, giving up on this URL")
                        break
                    # 2-minute cooldown per retry — enough to let Yahoo's rate window partially recover
                    cooldown = 120
                    log(f"Rate limit detected (retry {rate_limit_retries}/{MAX_RATE_LIMIT_RETRIES}): {e}")
                    log(f"Cooling down for {cooldown // 60}m...")
                    time.sleep(cooldown)
                    log("Resuming after cooldown")
                    continue

                log(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")

                if attempt < self.max_retries - 1:
                    sleep_time = backoff * (2**attempt)
                    log(f"Retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)

        raise RuntimeError(f"Failed to fetch URL after retries: {last_error}")

    def fetch_teams(self) -> dict[str, dict[str, str]]:
        """
        Fetch all teams in the league.

        Returns:
            Dict mapping team_key to dict with:
                - manager_name: Normalized manager name
                - manager_guid: Yahoo user GUID (for cross-year identification)
                - team_name: Raw team name
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{self.league_id}/teams"

        log(f"Fetching teams for league {self.league_id}")

        try:
            root = self._fetch_url_xml(url)

            teams = {}
            identity_inputs: list[dict[str, str | None]] = []
            for team_elem in root.findall(".//team"):
                team_key_elem = team_elem.find("team_key")
                if team_key_elem is None:
                    continue

                team_key = team_key_elem.text

                # Get raw nickname from manager element
                manager_elem = team_elem.find(".//manager/nickname")
                raw_nickname = manager_elem.text if manager_elem is not None else None

                # Get manager GUID (persistent identifier across years)
                guid_elem = team_elem.find(".//manager/guid")
                manager_guid = guid_elem.text if guid_elem is not None else None

                # Matchup fetching resolves Yahoo's redacted manager GUIDs into
                # stable identities before the canonical tables are written.
                # Preserve the same raw identity inputs here so roster rows join
                # to those matchup rows before recovery audits run.
                image_elem = team_elem.find(".//manager/image_url")
                manager_image_url = image_elem.text if image_elem is not None else None

                # Get team name (used as fallback for --hidden-- managers)
                name_elem = team_elem.find("name")
                team_name = name_elem.text if name_elem is not None else None

                # Normalize manager name (handles --hidden-- with team_name fallback)
                manager_name = normalize_manager_name(
                    nickname=raw_nickname, overrides=self.manager_name_overrides, team_name_fallback=team_name
                )

                teams[team_key] = {"manager_name": manager_name, "manager_guid": manager_guid, "team_name": team_name}
                identity_inputs.append(
                    {
                        "manager_guid": manager_guid,
                        "manager_nickname_raw": raw_nickname,
                        "manager_image_url": manager_image_url,
                        "team_key": team_key,
                        "manager": manager_name,
                    }
                )

            if identity_inputs:
                try:
                    from multi_league.data_fetchers.yahoo.yahoo_identity import resolve_yahoo_manager_guids
                except ImportError:
                    from data_fetchers.yahoo.yahoo_identity import resolve_yahoo_manager_guids

                resolved_guids = resolve_yahoo_manager_guids(pd.DataFrame(identity_inputs))
                for team_key, resolved_guid in zip(teams, resolved_guids, strict=True):
                    if pd.notna(resolved_guid) and str(resolved_guid).strip():
                        teams[team_key]["manager_guid"] = str(resolved_guid).strip()

            log(f"Found {len(teams)} teams")

            return teams

        except Exception as e:
            log(f"Error fetching teams: {e}")
            return {}

    def fetch_league_weeks(self) -> int | None:
        """
        Fetch the number of weeks in the fantasy season from league settings.

        Returns:
            Number of weeks in the fantasy season, or None if unable to determine
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{self.league_id}/settings"

        log("Fetching league settings to determine number of weeks")

        try:
            root = self._fetch_url_xml(url)

            # Look for playoff_start_week to determine last week of fantasy season
            playoff_start = root.find(".//playoff_start_week")
            if playoff_start is not None:
                playoff_week = int(playoff_start.text)
                log(f"  Playoff start week: {playoff_week}")

                # Also check for number of playoff weeks
                num_playoff_teams = root.find(".//num_playoff_teams")

                # Most leagues have 2-3 weeks of playoffs
                # Conservative estimate: playoff_start + 2 weeks
                last_week = playoff_week + 2

                log(f"  Estimated last fantasy week: {last_week}")
                return last_week

            # Fallback: look for current_week or end_week
            current_week = root.find(".//current_week")
            if current_week is not None:
                weeks = int(current_week.text)
                log(f"  Using current_week from settings: {weeks}")
                return weeks

            # If we can't determine, return None
            log("  Could not determine number of weeks from league settings")
            return None

        except Exception as e:
            log(f"Error fetching league weeks: {e}")
            return None

    def fetch_current_week(self) -> int | None:
        """
        Fetch the current week from Yahoo API.

        Returns:
            Current week number, or None if unable to determine
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{self.league_id}/settings"

        try:
            root = self._fetch_url_xml(url)
            current_week = root.find(".//current_week")
            if current_week is not None:
                week = int(current_week.text)
                log(f"  Yahoo API current_week: {week}")
                return week
            return None
        except Exception as e:
            log(f"Error fetching current week: {e}")
            return None

    def fetch_roster_for_week(
        self,
        year: int,
        week: int,
        team_key: str,
        manager_name: str,
        manager_guid: str = None,
        *,
        include_stats: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Fetch roster for a specific team and week.

        Args:
            year: Season year
            week: Week number
            team_key: Yahoo team key
            manager_name: Manager name
            manager_guid: Yahoo user GUID (for cross-year identification)

        Returns:
            List of roster entries (one per player)
        """
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;week={week}"
        if include_stats:
            url += f"/players/stats;type=week;week={week}"
        else:
            url += "/players"

        try:
            root = self._fetch_url_xml(url)
            player_elements = root.findall(".//player")
            if not player_elements:
                raise ValueError(
                    "Yahoo weekly roster payload omitted players "
                    f"(team={team_key}, week={week}, roster_nodes={len(root.findall('.//roster'))}, "
                    f"players_nodes={len(root.findall('.//players'))})"
                )
            roster_data = self._parse_roster_players(
                player_elements,
                year=year,
                week=week,
                team_key=team_key,
                manager_name=manager_name,
                manager_guid=manager_guid,
                include_points=include_stats,
            )

        except Exception as e:
            if not getattr(self, "_access_denied_logged", False):
                log(f"Error fetching roster for {team_key} week {week}: {e}")
            raise  # Re-raise so caller tracks this as a failed team-week

        return roster_data

    def _parse_roster_players(
        self,
        player_elements: list[ET.Element],
        *,
        year: int,
        week: int,
        team_key: str,
        manager_name: str,
        manager_guid: str | None,
        include_points: bool,
    ) -> list[dict[str, Any]]:
        """Parse Yahoo roster player nodes into canonical weekly roster rows."""
        roster_data: list[dict[str, Any]] = []

        for player_elem in player_elements:
            try:
                player_info = {
                    "year": year,
                    "week": week,
                    "manager_name": manager_name,
                    "manager_guid": manager_guid,
                    "team_key": team_key,
                }

                player_key = player_elem.find("player_key")
                if player_key is not None:
                    player_info["player_key"] = player_key.text

                player_id = player_elem.find("player_id")
                if player_id is not None:
                    player_info["player_id"] = player_id.text

                name_elem = player_elem.find("name")
                if name_elem is not None:
                    full_name = name_elem.find("full")
                    if full_name is not None:
                        player_info["player_name"] = full_name.text

                editorial_team = player_elem.find("editorial_team_abbr")
                if editorial_team is not None:
                    player_info["nfl_team"] = editorial_team.text.upper() if editorial_team.text else None

                display_position = player_elem.find("display_position")
                if display_position is not None:
                    player_info["yahoo_position"] = display_position.text

                primary_position = player_elem.find("primary_position")
                if primary_position is not None:
                    player_info["primary_position"] = primary_position.text
                else:
                    player_info["primary_position"] = player_info.get("yahoo_position")

                eligible_positions = player_elem.find("eligible_positions")
                if eligible_positions is not None:
                    positions_list = [p.text for p in eligible_positions.findall("position")]
                    player_info["eligible_positions"] = ",".join(positions_list)

                selected_position = player_elem.find("selected_position")
                if selected_position is not None:
                    position_elem = selected_position.find("position")
                    if position_elem is not None:
                        player_info["fantasy_position"] = position_elem.text

                if include_points:
                    # Treat missing Yahoo points as DNP/bye/inactive, not a real zero.
                    # A literal "0" is still a valid played game and should remain 0.0.
                    pts_node = player_elem.find("player_points/total")
                    try:
                        pts_text = pts_node.text.strip() if pts_node is not None and pts_node.text is not None else None
                        player_info["fantasy_points"] = round(float(pts_text), 2) if pts_text else None
                        player_info["yahoo_official_points"] = player_info["fantasy_points"]
                    except (ValueError, TypeError):
                        player_info["fantasy_points"] = None
                        player_info["yahoo_official_points"] = None
                else:
                    player_info["fantasy_points"] = None
                    player_info["yahoo_official_points"] = None

                yahoo_stat_count = 0
                for stat_elem in player_elem.findall("player_stats/stats/stat"):
                    stat_id = (stat_elem.findtext("stat_id") or "").strip()
                    if not stat_id:
                        continue
                    value_text = (stat_elem.findtext("value") or "").strip()
                    try:
                        value = float(value_text) if value_text else None
                    except (TypeError, ValueError):
                        value = None
                    player_info[f"yahoo_stat_{stat_id}"] = value
                    yahoo_stat_count += 1
                if yahoo_stat_count:
                    player_info["yahoo_stats_available"] = True

                roster_data.append(player_info)

            except Exception as e:
                log(f"Error parsing player: {e}")
                continue

        return roster_data

    def fetch_all_rosters_for_week(self, year: int, week: int, teams: dict[str, dict[str, str]]) -> tuple:
        """
        Fetch all rosters for all teams for a specific week.

        Yahoo's league-level ``teams;out=roster`` resource exposes the current
        roster shell but does not reliably return historical weekly players.
        The native team ``roster;week=N`` resource is the authoritative weekly
        membership source. The weekly stats expansion is required because
        Yahoo otherwise returns an empty players collection for some football
        leagues; the explicit type/week qualifiers keep it weekly-scoped.

        Args:
            year: Season year
            week: Week number
            teams: Dict mapping team_key to dict with manager_name, manager_guid, team_name

        Returns:
            Tuple of (DataFrame with all roster data, list of (team_key, team_info) failures)
        """
        return self._fetch_all_rosters_individually(year, week, teams)

    def _fetch_all_rosters_individually(self, year: int, week: int, teams: dict[str, dict[str, str]]) -> tuple:
        """
        Fetch rosters individually for each team IN PARALLEL.

        Args:
            year: Season year
            week: Week number
            teams: Dict mapping team_key to dict with manager_name, manager_guid, team_name

        Returns:
            Tuple of (DataFrame with all roster data, list of (team_key, team_info) failures)
        """
        all_rosters = []
        failed_teams = []

        def fetch_team_roster(team_key: str, team_info: dict[str, str]):
            """Fetch roster for a single team (runs in parallel)"""
            try:
                roster_data = self.fetch_roster_for_week(
                    year,
                    week,
                    team_key,
                    team_info["manager_name"],
                    team_info.get("manager_guid"),
                    include_stats=True,
                )
                return (team_key, team_info["manager_name"], True, roster_data)
            except Exception as e:
                return (team_key, team_info["manager_name"], False, str(e))

        # Parallel execution with max 3 workers (respects rate limiting with built-in _rate_limit_wait)
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_team = {
                executor.submit(fetch_team_roster, team_key, team_info): (team_key, team_info["manager_name"])
                for team_key, team_info in teams.items()
            }

            for future in as_completed(future_to_team):
                team_key, manager_name, success, result = future.result()

                if success:
                    all_rosters.extend(result)
                else:
                    if not getattr(self, "_access_denied_logged", False):
                        log(f"Error fetching roster for {manager_name}: {result}")
                    failed_teams.append((team_key, teams[team_key]))

        if not all_rosters:
            return pd.DataFrame(), failed_teams

        df = pd.DataFrame(all_rosters)

        # Validate starter counts per team
        # This catches incomplete roster data that causes lineup efficiency > 100%
        if "fantasy_position" in df.columns:
            # Count starters per manager (non-BN, non-IR positions)
            bench_positions = {"BN", "IR", "IL", "IL+", "NA", "PUP"}
            starters = df[~df["fantasy_position"].isin(bench_positions)]
            starter_counts = starters.groupby("manager_name").size()

            # Check for teams with fewer starters than expected (usually 9-10)
            min_expected = 8  # Allow some flexibility for different league formats
            low_count_teams = starter_counts[starter_counts < min_expected]
            if not low_count_teams.empty:
                log(f"[WARN] Week {week}: Teams with low starter count: {dict(low_count_teams)}")
                log("  This may cause lineup efficiency issues. Consider re-fetching.")

        # Check for NULL player names (common issue with Yahoo API)
        if "player_name" in df.columns:
            null_names = df["player_name"].isna().sum()
            if null_names > 0:
                pct = null_names / len(df) * 100
                log(
                    f"[WARN] Week {week}: {null_names} players ({pct:.1f}%) have NULL names - will attempt to fill from player map"
                )

        return df, failed_teams

    def fetch_season_rosters(
        self, year: int, weeks: list[int] | None = None, end_week: int | None = None
    ) -> pd.DataFrame:
        """
        Fetch roster data for an entire season using optimized batch API.

        Args:
            year: Season year
            weeks: List of weeks to fetch (auto-detect if None)
            end_week: Last week of fantasy season (from league settings)

        Returns:
            DataFrame with all roster data for the season
        """
        if weeks is None:
            if end_week is not None:
                # Use the end_week from league settings
                weeks = list(range(1, end_week + 1))
                log(f"Using league settings: weeks 1-{end_week}")
            else:
                # Fallback to NFL defaults if we can't determine from league settings
                # WARNING: This assumes NFL fantasy football schedule
                # - 2021+: 17 regular season weeks (NFL expanded to 17 games)
                # - Pre-2021: 16 regular season weeks
                # For other sports/leagues, ensure league settings are properly configured
                if year >= 2021:
                    weeks = list(range(1, 18))  # Weeks 1-17 (17 regular season weeks)
                else:
                    weeks = list(range(1, 17))  # Weeks 1-16 (16 regular season)
                log(f"[WARN] No league settings found, using NFL default weeks for {year}: {weeks[0]}-{weeks[-1]}")

        log(f"Fetching season {year} roster data for weeks: {weeks}")

        # Initialize failed team tracking (populated by _fetch_all_weeks_batch/_fetch_week_by_week)
        self._failed_team_weeks = {}

        # Fetch teams first
        teams = self.fetch_teams()
        self.expected_team_keys = tuple(teams)

        if not teams:
            log("No teams found!")
            return pd.DataFrame()

        # Fetch all weeks using optimized batch API
        try:
            return self._fetch_all_weeks_batch(year, weeks, teams)
        except Exception as e:
            log(f"Batch fetch for all weeks failed: {e}")
            log("Falling back to week-by-week fetching...")
            return self._fetch_week_by_week(year, weeks, teams)

    def _fetch_all_weeks_batch(self, year: int, weeks: list[int], teams: dict[str, dict[str, str]]) -> pd.DataFrame:
        """
        Fetch all weeks using optimized batch API calls.

        Yahoo's API limitation: Cannot fetch all teams+weeks+roster+stats in ONE call.
        Best approach: Fetch all teams for each week in one call (1 API call per week).

        This is significantly better than individual team calls (would be teams*weeks calls).
        """
        all_rosters = []
        errors = []
        failed_team_weeks = {}  # {week: [(team_key, team_info), ...]}
        consecutive_empty = 0
        self._access_denied_logged = False  # Reset per-year so we log once per year
        CIRCUIT_BREAKER_THRESHOLD = 3  # After 3 consecutive empty weeks, manifest and move on

        for i, week in enumerate(weeks, 1):
            try:
                # Fetch all teams' rosters for this week in ONE call
                week_df, week_failures = self.fetch_all_rosters_for_week(year, week, teams)
                if week_failures:
                    failed_team_weeks[week] = week_failures

                if not week_df.empty:
                    all_rosters.append(week_df)
                    consecutive_empty = 0  # Reset on success
                else:
                    consecutive_empty += 1

                # Circuit breaker: if N consecutive weeks returned nothing, likely rate limited.
                # Manifest remaining weeks immediately and move on — recovery pass fills gaps
                # later when the rate limit has naturally expired. No in-loop cooldowns.
                if consecutive_empty >= CIRCUIT_BREAKER_THRESHOLD:
                    first_failed = weeks[i - consecutive_empty]
                    remaining = [
                        w for w in weeks[i - consecutive_empty :] if w not in [r_w for r_w in range(1, first_failed)]
                    ]
                    log(
                        f"[RATE LIMIT] {year}: {consecutive_empty} consecutive empty weeks starting at week {first_failed} — manifesting weeks {first_failed}-{weeks[-1]} for recovery"
                    )
                    errors.append(f"weeks {first_failed}-{weeks[-1]}: rate limited (manifested for recovery)")
                    all_teams_list = list(teams.items())
                    for remaining_week in weeks[i - consecutive_empty :]:
                        if remaining_week not in failed_team_weeks:
                            failed_team_weeks[remaining_week] = all_teams_list
                    break

                # Small delay between weeks to be respectful to API
                if i < len(weeks):
                    time.sleep(0.5)

            except Exception as e:
                errors.append(f"week {week}: {e}")
                consecutive_empty += 1

                # Same circuit breaker for exceptions — manifest and move on
                if consecutive_empty >= CIRCUIT_BREAKER_THRESHOLD:
                    first_failed = weeks[i - consecutive_empty + 1]
                    log(
                        f"[RATE LIMIT] {year}: {consecutive_empty} consecutive failures starting at week {first_failed} — manifesting for recovery"
                    )
                    errors.append(f"weeks {first_failed}-{weeks[-1]}: rate limited (manifested for recovery)")
                    all_teams_list = list(teams.items())
                    for remaining_week in weeks[i - consecutive_empty + 1 :]:
                        if remaining_week not in failed_team_weeks:
                            failed_team_weeks[remaining_week] = all_teams_list
                    break

                continue

        if not all_rosters:
            self._failed_team_weeks = failed_team_weeks
            log(f"[WARN] {year}: no data returned (all weeks rate limited) — manifested for recovery")
            return pd.DataFrame()

        df = pd.concat(all_rosters, ignore_index=True)
        log(f"[OK] Fetched {len(df):,} roster records across {len(weeks)} weeks ({len(weeks)} API calls)")
        if errors:
            log(f"  [WARN] {len(errors)} week(s) failed: {', '.join(errors[:3])}" + ("..." if len(errors) > 3 else ""))

        self._failed_team_weeks = failed_team_weeks
        return df

    def _fetch_week_by_week(self, year: int, weeks: list[int], teams: dict[str, dict[str, str]]) -> pd.DataFrame:
        """
        Fallback: Fetch rosters week by week using batch API for teams.
        """
        log("Using week-by-week batch API (1 call per week)")

        all_weeks_data = []
        failed_team_weeks = {}  # {week: [(team_key, team_info), ...]}

        for week in weeks:
            try:
                week_df, week_failures = self.fetch_all_rosters_for_week(year, week, teams)
                if week_failures:
                    failed_team_weeks[week] = week_failures

                if not week_df.empty:
                    all_weeks_data.append(week_df)

                # Delay between weeks
                time.sleep(1.0)

            except Exception as e:
                log(f"Error fetching week {week}: {e}")
                continue

        if not all_weeks_data:
            log(f"No roster data fetched for season {year}")
            self._failed_team_weeks = failed_team_weeks
            return pd.DataFrame()

        df = pd.concat(all_weeks_data, ignore_index=True)
        log(f"Total roster records for {year}: {len(df)}")

        # Fill missing player names from yahoo_nfl_player_map
        # This fixes issues where Yahoo API doesn't return the name/full element
        df = self._fill_missing_player_names(df)

        self._failed_team_weeks = failed_team_weeks
        return df

    def _fill_missing_player_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill missing player names from yahoo_nfl_player_map.

        Yahoo API sometimes doesn't return player names (especially for historical data
        or multi-position players). This causes 13-17% of roster entries to have NULL names,
        which breaks optimal lineup calculations and causes efficiency > 100% issues.
        """
        if df.empty:
            return df

        # Check for player_name or player column
        name_col = "player_name" if "player_name" in df.columns else "player" if "player" in df.columns else None
        id_col = (
            "player_id" if "player_id" in df.columns else "yahoo_player_id" if "yahoo_player_id" in df.columns else None
        )

        if not name_col or not id_col:
            return df

        # Count missing names
        null_mask = df[name_col].isna() | (df[name_col] == "") | (df[name_col] == "None")
        has_id_mask = df[id_col].notna()
        fillable_mask = null_mask & has_id_mask

        if not fillable_mask.any():
            return df

        fillable_count = fillable_mask.sum()
        log(f"[NAME FIX] Found {fillable_count} rows with missing player names but valid yahoo_player_id")

        # Try to load player map from MotherDuck
        try:
            from multi_league.core.db_reader import get_reader

            reader = get_reader()
            map_df = reader.query_df(
                """
                SELECT yahoo_player_id, yahoo_name, nfl_name
                FROM public.yahoo_nfl_player_map
                WHERE yahoo_player_id IS NOT NULL
                """,
                database="___ops",
            )

            # Build lookup: yahoo_player_id -> name (prefer yahoo_name, fallback to nfl_name)
            name_lookup = {}
            for _, row in map_df.iterrows():
                yid = str(row["yahoo_player_id"]).split(".")[0]  # Handle "30120.0" format
                name = row["yahoo_name"] or row["nfl_name"]
                if name:
                    name_lookup[yid] = name

            log(f"[NAME FIX] Loaded {len(name_lookup)} player name mappings")

            # Fill missing names
            def get_name(row):
                if not fillable_mask.loc[row.name]:
                    return row[name_col]
                yid = str(row[id_col]).split(".")[0] if pd.notna(row[id_col]) else None
                return name_lookup.get(yid, row[name_col])

            df[name_col] = df.apply(get_name, axis=1)

            # Count how many we fixed
            still_null = df[name_col].isna() | (df[name_col] == "") | (df[name_col] == "None")
            fixed_count = fillable_count - (still_null & has_id_mask).sum()
            log(f"[NAME FIX] Filled {fixed_count} missing player names from yahoo_nfl_player_map")

        except Exception as e:
            log(f"[NAME FIX] Error filling player names: {e}")

        return df


# =============================================================================
# Multi-League Support Functions
# =============================================================================


def find_league_settings_files(league_dir: Path) -> dict[int, Path]:
    """
    Find all league settings JSON files for a league.

    Args:
        league_dir: Path to data directory (e.g., ctx.data_directory = .../fantasy_football_data)

    Returns:
        Dict mapping year to settings file path
    """
    # League settings are now at top level (league-wide config, not player-specific)
    settings_dir = league_dir / "league_settings"

    if not settings_dir.exists():
        log(f"League settings directory not found: {settings_dir}")
        return {}

    year_to_file = {}
    for settings_file in settings_dir.glob("league_settings_*.json"):
        try:
            # Extract year from filename: league_settings_2024_449_l_198278.json
            parts = settings_file.stem.split("_")
            if len(parts) >= 3 and parts[2].isdigit():
                year = int(parts[2])
                year_to_file[year] = settings_file
        except (ValueError, IndexError) as e:
            log(f"Could not parse year from {settings_file.name}: {e}")
            continue

    return year_to_file


def load_league_settings(settings_file: Path) -> dict[str, Any]:
    """
    Load league settings from JSON file.

    Args:
        settings_file: Path to league settings JSON file

    Returns:
        Dict with league settings including year, league_key, end_week
    """
    try:
        with open(settings_file, encoding="utf-8") as f:
            data = json.load(f)

        return {
            "year": data.get("year"),
            "league_key": data.get("league_key"),
            "end_week": data.get("metadata", {}).get("end_week"),
            "start_week": data.get("metadata", {}).get("start_week", 1),
            "num_teams": data.get("metadata", {}).get("num_teams"),
        }
    except Exception as e:
        log(f"Error loading league settings from {settings_file}: {e}")
        return {}


def load_discovered_leagues(league_dir: Path, league_name: str) -> dict[int, str]:
    """
    Load year-specific league IDs from discovered_leagues.json.

    Args:
        league_dir: Path to the parent directory of the league (e.g., .../fantasy_football_data)
        league_name: Name of the league (e.g., KMFFL)

    Returns:
        Dict mapping year to league ID (e.g., 449.l.198278)
    """
    try:
        discovered_file = league_dir / "discovered_leagues.json"

        if not discovered_file.exists():
            log(f"discovered_leagues.json not found: {discovered_file}")
            return {}

        with open(discovered_file) as f:
            data = json.load(f)

        # Extract year and league ID mappings
        # The file is a flat array, not nested under 'leagues'
        year_to_league_id = {}
        for entry in data:
            if entry.get("league_name") == league_name:
                year = entry.get("year")
                league_id = entry.get("league_id")
                if year and league_id:
                    year_to_league_id[year] = league_id

        return year_to_league_id

    except Exception as e:
        log(f"Error loading discovered leagues from {league_dir}: {e}")
        return {}


def get_weeks_from_matchup_data(data_directory: Path, year: int) -> list[int] | None:
    """
    Get observed played weeks from matchup data files.

    This allows player data to align with matchup data (only fetch weeks with actual matchups).

    Args:
        data_directory: League data directory (e.g., .../fantasy_football_data/KMFFL)
        year: Year to check

    Returns:
        Sorted week numbers found in matchup data, or None if no matchup data exists
    """
    try:
        matchup_dir = data_directory / "matchup_data"

        if not matchup_dir.exists():
            log(f"[matchup_max_week] Matchup directory not found: {matchup_dir}")
            return None

        # Try to find matchup file for this year
        # Prefer all-weeks file, fallback to individual week files
        all_weeks_file = matchup_dir / f"matchup_data_week_all_year_{year}.parquet"

        if all_weeks_file.exists():
            try:
                df = pd.read_parquet(all_weeks_file)
                if not df.empty and "week" in df.columns:
                    if {"team_points", "opponent_points"}.issubset(df.columns):
                        points = pd.to_numeric(df["team_points"], errors="coerce").fillna(0)
                        opp_points = pd.to_numeric(df["opponent_points"], errors="coerce").fillna(0)
                        df = df[(points != 0) | (opp_points != 0)]
                    elif "team_points" in df.columns:
                        points = pd.to_numeric(df["team_points"], errors="coerce").fillna(0)
                        df = df[points != 0]
                if not df.empty and "week" in df.columns:
                    weeks = sorted(
                        {
                            int(week)
                            for week in pd.to_numeric(df["week"], errors="coerce").dropna().tolist()
                            if int(week) > 0
                        }
                    )
                    if weeks:
                        log(f"[matchup_weeks] Found weeks {weeks[0]}-{weeks[-1]} from {all_weeks_file.name}")
                        return weeks
            except Exception as e:
                log(f"[matchup_max_week] Error reading {all_weeks_file.name}: {e}")

        # Fallback: check individual week files
        week_files = list(matchup_dir.glob(f"matchup_data_week_*_year_{year}.parquet"))
        if week_files:
            # Extract week numbers from filenames
            week_numbers = []
            for wf in week_files:
                try:
                    # Parse filename: matchup_data_week_05_year_2024.parquet
                    parts = wf.stem.split("_")
                    if len(parts) >= 5:
                        week_str = parts[3]  # "05"
                        if week_str != "all":
                            week_numbers.append(int(week_str))
                except (ValueError, IndexError):
                    continue

            if week_numbers:
                weeks = sorted(set(week_numbers))
                log(f"[matchup_weeks] Found weeks {weeks[0]}-{weeks[-1]} from {len(week_files)} individual week files")
                return weeks

        log(f"[matchup_max_week] No matchup data found for year {year}")
        return None

    except Exception as e:
        log(f"[matchup_max_week] Error getting max week from matchup data: {e}")
        return None


def get_max_week_from_matchup_data(data_directory: Path, year: int) -> int | None:
    """Get the maximum observed matchup week from matchup data files."""
    weeks = get_weeks_from_matchup_data(data_directory, year)
    if weeks:
        max_week = int(max(weeks))
        log(f"[matchup_max_week] Found max week {max_week} from matchup parquet data")
        return max_week
    return None


def get_weeks_from_local_matchup_data(local_db: Any | None, year: int) -> list[int] | None:
    """
    Get played matchup weeks from the in-process local DuckDB.

    In the GitHub Actions import path, matchup rows are saved directly into
    LocalLeagueDB before rosters run. Use those observed weeks to avoid asking
    Yahoo for roster weeks that do not exist for a particular league season.
    """
    if local_db is None:
        return None

    try:
        if hasattr(local_db, "table_exists") and not local_db.table_exists("matchup"):
            return None

        conn = local_db.connect()
        columns = {
            str(column_name)
            for (column_name,) in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'matchup'
                """,
            ).fetchall()
        }
        if not {"year", "week"}.issubset(columns):
            return None

        score_filter = ""
        if {"team_points", "opponent_points"}.issubset(columns):
            score_filter = """
              AND (
                COALESCE(TRY_CAST(team_points AS DOUBLE), 0) <> 0
                OR COALESCE(TRY_CAST(opponent_points AS DOUBLE), 0) <> 0
              )
            """
        elif "team_points" in columns:
            score_filter = "AND COALESCE(TRY_CAST(team_points AS DOUBLE), 0) <> 0"

        rows = conn.execute(
            f"""
            SELECT DISTINCT TRY_CAST(week AS INTEGER) AS week
            FROM public.matchup
            WHERE TRY_CAST(year AS INTEGER) = ?
              AND TRY_CAST(week AS INTEGER) IS NOT NULL
              {score_filter}
            ORDER BY week
            """,
            [int(year)],
        ).fetchall()
        weeks = [int(row[0]) for row in rows if row and row[0] is not None and int(row[0]) > 0]
        if weeks:
            log(f"[matchup_weeks] Found weeks {weeks[0]}-{weeks[-1]} from local DuckDB matchup table")
            return weeks

        return None

    except Exception as e:
        log(f"[matchup_max_week] Error getting max week from local DuckDB matchup table: {e}")
        return None


def get_max_week_from_local_matchup_data(local_db: Any | None, year: int) -> int | None:
    """Get the maximum played matchup week from the in-process local DuckDB."""
    weeks = get_weeks_from_local_matchup_data(local_db, year)
    if weeks:
        max_week = int(max(weeks))
        log(f"[matchup_max_week] Found max week {max_week} from local DuckDB matchup table")
        return max_week
    return None


def _align_week_count_to_matchups(
    default_weeks: int,
    year: int,
    data_directory: Path | None,
    local_db: Any | None = None,
) -> int:
    """Prefer observed matchup weeks over settings/default roster week counts."""
    max_week = get_max_week_from_local_matchup_data(local_db, year)
    source = "local DuckDB matchup table"

    if max_week is None and data_directory is not None:
        max_week = get_max_week_from_matchup_data(Path(data_directory), year)
        source = "matchup parquet data"

    if max_week and max_week > 0:
        if max_week != default_weeks:
            log(
                f"[matchup_max_week] Aligning roster weeks for {year}: "
                f"settings/default {default_weeks} -> observed matchup max {max_week} ({source})"
            )
        return int(max_week)

    return int(default_weeks)


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _default_roster_weeks_from_settings(settings: dict[str, Any], year: int) -> list[int]:
    default_end = 17 if year >= 2021 else 16
    start_week = _coerce_positive_int(settings.get("start_week"), 1)
    end_week = _coerce_positive_int(settings.get("end_week"), default_end)
    if end_week < start_week:
        start_week = 1
    return list(range(start_week, end_week + 1))


def _resolve_roster_weeks(
    settings: dict[str, Any],
    year: int,
    data_directory: Path | None,
    local_db: Any | None = None,
) -> list[int]:
    """Prefer observed matchup weeks, falling back to Yahoo settings range."""
    default_weeks = _default_roster_weeks_from_settings(settings, year)

    weeks = get_weeks_from_local_matchup_data(local_db, year)
    source = "local DuckDB matchup table"

    if not weeks and data_directory is not None:
        weeks = get_weeks_from_matchup_data(Path(data_directory), year)
        source = "matchup parquet data"

    if weeks:
        observed = sorted(set(int(week) for week in weeks if int(week) > 0))
        if observed and observed != default_weeks:
            log(
                f"[matchup_weeks] Aligning roster weeks for {year}: "
                f"settings/default {default_weeks[0]}-{default_weeks[-1]} -> "
                f"observed {observed[0]}-{observed[-1]} ({source})"
            )
        return observed or default_weeks

    return default_weeks


def fetch_rosters_for_year(
    ctx,
    year: int,
    oauth_session=None,
    local_db: Any | None = None,
    weeks: list[int] | None = None,
) -> tuple[pd.DataFrame, list[int]]:
    """Fetch all weekly rosters for a single year.

    This is the in-process callable equivalent of the ``main()`` CLI for a
    single year.  It creates a :class:`YahooRosterFetcher`, iterates over
    the weeks for *year*, and returns the combined DataFrame.

    Args:
        ctx: LeagueContext with league configuration
        year: Season year to fetch
        oauth_session: Pre-existing OAuth2 session (optional).  If *None*,
            one is created from the context credentials.
        local_db: Optional LocalLeagueDB containing already-fetched matchups.
        weeks: Optional explicit positive weeks to fetch.  Incremental callers
            use this to avoid reading an entire live season for one finalized
            game; normal imports retain settings-based discovery.

    Returns:
        ``(combined_df, failed_weeks)`` — DataFrame of all roster records for
        the year, plus a list of week numbers that could not be fetched.
    """
    import tempfile as _tempfile

    # --- Resolve OAuth file ------------------------------------------------
    import os as _os

    oauth_file = None
    temp_oauth_file = None

    _ctx_path = getattr(ctx, "oauth_file_path", None)
    _session_from = getattr(oauth_session, "from_file", None) if oauth_session else None

    if _ctx_path and Path(_ctx_path).exists():
        oauth_file = Path(_ctx_path)
    elif _session_from and Path(_session_from).exists():
        oauth_file = Path(_session_from)
    elif oauth_session is not None:
        fd, temp_path = _tempfile.mkstemp(suffix=".json", text=True)
        try:
            session_data = {
                "access_token": "expired",
                "consumer_key": _os.environ.get("YAHOO_CLIENT_ID", ""),
                "consumer_secret": _os.environ.get("YAHOO_CLIENT_SECRET", ""),
                "refresh_token": getattr(oauth_session, "refresh_token", ""),
                "token_time": getattr(oauth_session, "token_time", 0.0),
                "token_type": "bearer",
            }
            with _os.fdopen(fd, "w") as f:
                json.dump(session_data, f)
            oauth_file = Path(temp_path)
            temp_oauth_file = oauth_file
        except Exception as e:
            _os.close(fd)
            raise e
    elif ctx.oauth_credentials:
        fd, temp_path = _tempfile.mkstemp(suffix=".json", text=True)
        try:
            import os as _os

            with _os.fdopen(fd, "w") as f:
                json.dump(ctx.oauth_credentials, f, indent=2)
            oauth_file = Path(temp_path)
            temp_oauth_file = oauth_file
        except Exception as e:
            _os.close(fd)
            raise e
    elif ctx.oauth_file_path:
        oauth_file = Path(ctx.oauth_file_path)
    else:
        raise RuntimeError("No OAuth credentials available in context")

    try:
        # --- Resolve league_id for this year --------------------------------
        league_id = None
        if ctx.has_league_ids_mapping():
            league_id = ctx.league_ids.get(str(year)) or ctx.league_ids.get(year)
        if not league_id:
            raise ValueError(f"No league_id found for year {year} in context")

        # --- Determine weeks ------------------------------------------------
        league_dir = Path(ctx.data_directory) if ctx.data_directory else Path.cwd()
        year_to_settings = find_league_settings_files(league_dir)
        settings = {}
        if year in year_to_settings:
            settings = load_league_settings(year_to_settings[year])

        settings_end_week = settings.get("end_week")
        if weeks is None:
            resolved_weeks = _resolve_roster_weeks(
                settings,
                year,
                Path(ctx.data_directory) if ctx.data_directory else None,
                local_db=local_db,
            )
        else:
            resolved_weeks = sorted({int(week) for week in weeks if int(week) > 0})
            if not resolved_weeks:
                raise ValueError("Explicit Yahoo roster weeks must contain at least one positive week")

        # --- Create fetcher and fetch ---------------------------------------
        output_dir = Path(ctx.data_directory) / "player_data"
        output_dir.mkdir(parents=True, exist_ok=True)

        rate_limit = ctx.rate_limit_per_sec if hasattr(ctx, "rate_limit_per_sec") else 2.0
        manager_overrides = ctx.manager_name_overrides if ctx else {}

        fetcher = YahooRosterFetcher(
            oauth_file=oauth_file,
            league_id=league_id,
            rate_limit=rate_limit,
            output_dir=output_dir,
            manager_name_overrides=manager_overrides,
            oauth_session=oauth_session,
        )

        df = fetcher.fetch_season_rosters(
            year=year, weeks=resolved_weeks, end_week=int(settings_end_week) if settings_end_week else None
        )
        # The fetcher already retrieved the complete league-team set. Keep it
        # with the frame so an incremental caller can detect a partial batch
        # without adding a second Yahoo API request.
        df.attrs["expected_team_keys"] = tuple(getattr(fetcher, "expected_team_keys", ()))

        # Determine failed weeks
        failed_weeks: list[int] = []
        if not df.empty and "week" in df.columns:
            fetched = set(df["week"].unique())
            failed_weeks = sorted(set(resolved_weeks) - fetched)
        elif df.empty:
            failed_weeks = resolved_weeks

        # Apply manager name overrides
        if not df.empty and ctx.manager_name_overrides and "manager_name" in df.columns:
            df["manager_name"] = df["manager_name"].replace(ctx.manager_name_overrides)

        return df, failed_weeks

    finally:
        # Clean up temporary OAuth file
        if temp_oauth_file and temp_oauth_file.exists():
            try:
                temp_oauth_file.unlink()
            except Exception:
                pass


def main():
    """Fetch roster data for a league using LeagueContext."""

    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Fantasy roster data using multi-league infrastructure",
        epilog="""
Examples:
    # Fetch all years using league context (RECOMMENDED)
    python yahoo_fantasy_data.py --context path/to/league_context.json

    # Fetch specific year
    python yahoo_fantasy_data.py --context path/to/league_context.json --year 2024

    # Fetch specific week
    python yahoo_fantasy_data.py --context path/to/league_context.json --year 2024 --week 5
        """,
    )
    parser.add_argument("--context", type=str, required=True, help="Path to league_context.json (required)")
    parser.add_argument("--year", type=int, default=0, help="Specific year to fetch (0 = all years, default: 0)")
    parser.add_argument("--week", type=int, default=0, help="Specific week to fetch (0 = all weeks, default: 0)")
    parser.add_argument("--rate-limit", type=float, help="Max API requests per second (default: from context or 2.0)")

    args = parser.parse_args()

    log("Yahoo Fantasy Roster Data Fetcher")

    # Load league context using the proper LeagueContext class
    context_file = Path(args.context)

    if not context_file.exists():
        log(f"League context not found: {context_file}")
        sys.exit(1)

    try:
        ctx = LeagueContext.load(str(context_file))
        log(f"Loaded context for league: {ctx.league_name}")
    except Exception as e:
        log(f"Failed to load league context: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # Set rate limit from args or context
    rate_limit = args.rate_limit if args.rate_limit else ctx.rate_limit_per_sec

    # Output directory from context
    output_dir = Path(ctx.data_directory) / "player_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup OAuth - use embedded credentials or file path
    oauth_file = None
    temp_oauth_file = None

    try:
        if ctx.oauth_credentials:
            # Create temporary OAuth file from embedded credentials
            import tempfile
            import os

            temp_fd, temp_path = tempfile.mkstemp(suffix=".json", text=True)
            try:
                with os.fdopen(temp_fd, "w") as f:
                    json.dump(ctx.oauth_credentials, f, indent=2)
                oauth_file = Path(temp_path)
                temp_oauth_file = oauth_file
                log("Created temporary OAuth file from context credentials")
            except Exception as e:
                os.close(temp_fd)
                raise e

        elif ctx.oauth_file_path:
            oauth_file = Path(ctx.oauth_file_path)
            if not oauth_file.exists():
                log(f"OAuth file not found: {oauth_file}")
                sys.exit(1)
            log(f"Using OAuth file: {oauth_file}")
        else:
            log("No OAuth credentials found in context")
            sys.exit(1)

        # Use LeagueDiscovery to find league IDs for each year
        # PRIORITY ORDER:
        # 1. Context league_ids (from Phase 0 discovery) - avoids restricted API calls
        # 2. Cache file (discovered_leagues.json)
        # 3. API discovery (LAST RESORT - uses restricted endpoint)
        league_dir = Path(ctx.data_directory).parent if ctx.data_directory else Path.cwd()
        discovered_file = league_dir / "discovered_leagues.json"

        year_to_league_id = {}

        # CRITICAL: Check context league_ids FIRST to avoid calling restricted API
        # This mapping comes from Phase 0 (league settings discovery) which uses the
        # renew/renewed chain instead of the restricted users/games/teams endpoint
        if ctx.has_league_ids_mapping():
            for year_str, league_id in ctx.league_ids.items():
                year_int = int(year_str)
                year_to_league_id[year_int] = league_id
            log(f"[CONTEXT] Loaded {len(year_to_league_id)} years from context")
            if _verbose:
                for y in sorted(year_to_league_id):
                    log(f"  {y}: {year_to_league_id[y]}")
        # Try to load from cache as fallback
        elif discovered_file.exists():
            log(f"\nLoading cached league IDs from {discovered_file.name}...")
            cached_leagues = load_discovered_leagues(league_dir, ctx.league_name)

            if cached_leagues:
                year_to_league_id = cached_leagues
                log(f"Loaded {len(year_to_league_id)} years from cache")
                if _verbose:
                    for year in sorted(year_to_league_id.keys()):
                        log(f"  {year}: {year_to_league_id[year]}")
            else:
                log(f"  No cached data found for '{ctx.league_name}'")

        # Determine current year and check if cache is complete
        current_year = get_current_nfl_season_year()
        start_year = ctx.start_year or 2014
        end_year = ctx.end_year or current_year

        # Check if cache is missing years (either empty or outdated)
        # BUT: If league_ids came from context, trust it completely (no discovery needed)
        cached_max_year = max(year_to_league_id.keys()) if year_to_league_id else 0
        from_context = ctx.has_league_ids_mapping()
        needs_discovery = not from_context and (not year_to_league_id or cached_max_year < current_year)

        if needs_discovery:
            if year_to_league_id:
                log(f"\nCache is outdated (max year: {cached_max_year}, current: {current_year})")
                log(f"Discovering missing years for {ctx.league_name}...")
                # Only discover missing years (start after the max cached year)
                years = list(range(cached_max_year + 1, end_year + 1))
            else:
                log(f"\nDiscovering league IDs for {ctx.league_name}...")
                # Discover all years from start
                years = list(range(start_year, end_year + 1))

            # Initialize discovery
            discovery = LeagueDiscovery(oauth_file=oauth_file, game_code=ctx.game_code)

            for year in years:
                log(f"  [{year}] Discovering leagues...")
                try:
                    leagues = discovery.discover_leagues(year=year)

                    # Find the league matching our league name
                    for league in leagues:
                        if ctx.league_name.lower() in league.get("league_name", "").lower():
                            league_id = league.get("league_id")
                            year_to_league_id[year] = league_id
                            log(f"    Found: {league_id} ('{league['league_name']}')")
                            break

                    if year not in year_to_league_id:
                        log(f"    No league found matching '{ctx.league_name}'")

                except Exception as e:
                    log(f"    Error discovering {year}: {e}")

            if not year_to_league_id:
                log(f"No leagues found for '{ctx.league_name}'")
                sys.exit(1)

            log(f"\nDiscovered {len(year_to_league_id)} total years for {ctx.league_name}")
            if _verbose:
                for year in sorted(year_to_league_id.keys()):
                    log(f"  {year}: {year_to_league_id[year]}")

            # Save updated cache (merge with existing data for this league)
            try:
                # Load existing cache to preserve other leagues' data
                existing_cache = []
                if discovered_file.exists():
                    try:
                        with open(discovered_file) as f:
                            existing_cache = json.load(f)
                    except Exception:  # noqa: broad-except
                        pass
                # Remove old entries for this league
                other_leagues_data = [entry for entry in existing_cache if entry.get("league_name") != ctx.league_name]

                # Add current league's data
                current_league_data = [
                    {"year": year, "league_id": league_id, "league_name": ctx.league_name}
                    for year, league_id in year_to_league_id.items()
                ]

                # Merge
                merged_cache = other_leagues_data + current_league_data

                with open(discovered_file, "w") as f:
                    json.dump(merged_cache, f, indent=2)
                log(f"\nUpdated league discovery cache in {discovered_file.name}")
            except Exception as e:
                log(f"[WARN] Could not save discovery cache: {e}")
        else:
            log(f"\n[CACHE HIT] Using cached league IDs ({len(year_to_league_id)} years)")

        # Determine which years to fetch
        if args.year > 0:
            # Specific year requested
            if args.year not in year_to_league_id:
                log(f"Year {args.year} not found in discovered leagues")
                sys.exit(1)
            years_to_fetch = [args.year]
        else:
            # CRITICAL: For quick imports (single year), only fetch the target year
            # This prevents fetching all historical years during a quick import
            if ctx.is_single_year_import:
                # Quick import mode - only fetch end_year (typically current year)
                target_year = ctx.end_year or get_current_nfl_season_year()
                if target_year in year_to_league_id:
                    years_to_fetch = [target_year]
                    log(f"\n[QUICK IMPORT] Fetching only year {target_year}")
                else:
                    log(f"[ERROR] Target year {target_year} not found in discovered leagues")
                    sys.exit(1)
            else:
                # Full import mode - fetch all discovered years
                years_to_fetch = sorted(year_to_league_id.keys())
                log(f"\nFetching all {len(years_to_fetch)} years")

        # Load all league settings files upfront (for end_week lookup)
        # Settings are at {data_directory}/league_settings/ (NOT parent)
        league_dir = Path(ctx.data_directory) if ctx.data_directory else Path.cwd()
        year_to_settings_file = find_league_settings_files(league_dir)
        log(f"\nFound league settings for {len(year_to_settings_file)} years")

        # Track missed weeks for retry pass at the end
        missed_year_weeks = {}  # {year: {'weeks': [...], 'league_id': str, 'settings': dict}}
        # Track per-team failures within weeks (team was rate-limited but week has partial data)
        all_failed_team_weeks = {}  # {(year, week): [(team_key, team_info), ...]}

        # Fetch data for each year
        for i, year in enumerate(years_to_fetch, 1):
            # Ensure year is an integer (JSON may load as string)
            year = int(year)

            league_id = year_to_league_id[year] if year in year_to_league_id else year_to_league_id[str(year)]

            log(f"\n[{i}/{len(years_to_fetch)}] Year {year} (league: {league_id})")

            # Load league settings for this year to get actual end_week
            settings = {}
            if year in year_to_settings_file:
                settings = load_league_settings(year_to_settings_file[year])
                if _verbose:
                    log(f"Loaded settings from: {year_to_settings_file[year].name}")

            # Determine weeks based on league settings (NOT hardcoded by year)
            # Priority: 1) settings.end_week, 2) Yahoo API for current year, 3) fallback to NFL standard
            settings_end_week = settings.get("end_week")

            if settings_end_week:
                # Use end_week from league settings (most accurate)
                # Cast to int in case JSON stored it as string
                default_weeks = int(settings_end_week)
                if _verbose:
                    log(f"Using end_week from settings: {default_weeks} weeks")
            else:
                # Fallback to NFL standard if settings not available locally
                # Note: Settings will be fetched from Yahoo API in PHASE 0 during full import
                if year >= 2021:
                    default_weeks = 17  # 17 regular season weeks starting 2021
                else:
                    default_weeks = 16  # 16 regular season weeks before 2021
                log(
                    f"No local settings cache for {year}, using NFL standard: {default_weeks} weeks (will be updated from Yahoo API)"
                )

            # Initialize fetcher for this year
            # Pass manager_name_overrides for --hidden-- fallback to team_name
            manager_overrides = ctx.manager_name_overrides if ctx else {}
            fetcher = YahooRosterFetcher(
                oauth_file=oauth_file,
                league_id=league_id,
                rate_limit=rate_limit,
                output_dir=output_dir,
                manager_name_overrides=manager_overrides,
            )

            # Determine weeks to fetch (before try block so it's available in except)
            if args.week > 0:
                weeks = [args.week]
            else:
                weeks = _resolve_roster_weeks(
                    settings,
                    year,
                    Path(ctx.data_directory) if ctx.data_directory else None,
                )

            # Validate weeks
            if not weeks:
                log(f"[ERROR] Invalid week list for {year}, skipping")
                continue

            if _verbose:
                log(f"Fetching weeks for {year}: {weeks[0]}-{weeks[-1]}")

            try:
                # Fetch roster data
                df = fetcher.fetch_season_rosters(
                    year=year, weeks=weeks, end_week=int(settings_end_week) if settings_end_week else None
                )

                # Collect per-team failures for team-level retry pass
                if fetcher._failed_team_weeks:
                    for wk, failures in fetcher._failed_team_weeks.items():
                        all_failed_team_weeks[(year, wk)] = failures

                if not df.empty:
                    # Apply manager name overrides from context
                    if ctx.manager_name_overrides and "manager_name" in df.columns:
                        log(f"Applying {len(ctx.manager_name_overrides)} manager name overrides")
                        df["manager_name"] = df["manager_name"].replace(ctx.manager_name_overrides)

                    # Validate league format (detect unsupported formats)
                    if "fantasy_position" in df.columns:
                        unique_positions = df["fantasy_position"].dropna().unique()

                        # Standard positions
                        standard_positions = {"QB", "RB", "WR", "TE", "K", "DEF", "BN", "IR", "W/R/T", "OP"}

                        # Detect special league formats
                        unsupported_positions = set(unique_positions) - standard_positions

                        if unsupported_positions:
                            # Check for specific league types
                            if any(pos in ["DL", "LB", "DB", "DP"] for pos in unsupported_positions):
                                log("  [WARN] IDP (Individual Defensive Player) league detected!")
                                log(f"  Positions: {sorted(unsupported_positions)}")
                                log("  IDP leagues may not be fully supported by downstream transformations")
                            elif "Q/W/R/T" in unsupported_positions or "W/R/T/Q" in unsupported_positions:
                                log("  [INFO] Superflex league detected (QB in FLEX)")
                                log(f"  Position: {[p for p in unsupported_positions if 'Q' in p]}")
                            else:
                                log(f"  [WARN] Non-standard roster positions detected: {sorted(unsupported_positions)}")
                                log("  This league may have custom scoring or roster settings")
                                log("  Verify downstream transformations work correctly")

                    # Show summary of points
                    if _verbose and "fantasy_points" in df.columns:
                        total_points = df["fantasy_points"].sum()
                        avg_points = df["fantasy_points"].mean()
                        non_null_count = df["fantasy_points"].notna().sum()
                        log(f"Fantasy points stats for {year}:")
                        log(f"  Non-null values: {non_null_count}/{len(df)}")
                        log(f"  Total fantasy points: {total_points:.2f}")
                        log(f"  Average points per player-week: {avg_points:.2f}")

                    log(f"[OK] Successfully fetched {len(df)} roster records for {year}")

                    # Check for missing weeks
                    if "week" in df.columns:
                        fetched_weeks = set(df["week"].unique())
                        requested_weeks = set(weeks)
                        missing = sorted(requested_weeks - fetched_weeks)
                        if missing:
                            log(f"  [WARN] Missing weeks for {year}: {missing}")
                            missed_year_weeks[year] = {
                                "weeks": missing,
                                "league_id": league_id,
                                "all_weeks": weeks,
                            }
                else:
                    log(f"[WARN] No data fetched for {year}")
                    missed_year_weeks[year] = {
                        "weeks": weeks,
                        "league_id": league_id,
                        "all_weeks": weeks,
                    }

            except Exception as e:
                log(f"[FAIL] Error processing year {year}: {e}")
                import traceback

                traceback.print_exc()

                # Track the entire year as missed
                missed_year_weeks[year] = {
                    "weeks": weeks,
                    "league_id": league_id,
                    "all_weeks": weeks,
                }

                # Continue to next year instead of stopping
                log("Continuing to next year...")
                continue

        # =====================================================================
        # No in-fetcher retries — manifest gaps and let recovery handle them.
        # The recovery pass (Phase 1.5) runs after ALL fetchers complete,
        # giving Yahoo's rate limit window time to expire naturally.
        # =====================================================================
        if missed_year_weeks:
            log(f"[MANIFEST] {len(missed_year_weeks)} year(s) have missing weeks — manifesting for recovery")
            for year, info in sorted(missed_year_weeks.items()):
                log(f"  {year}: weeks {info['weeks']}")
        if all_failed_team_weeks:
            failed_count = sum(len(v) for v in all_failed_team_weeks.values())
            log(f"[MANIFEST] {failed_count} team-week(s) with partial failures — manifesting for recovery")

        # Write failure manifests for missing weeks and per-team failures
        try:
            from multi_league.data_fetchers.yahoo.fetch_failure_manifest import write_manifest

            for yr, info in missed_year_weeks.items():
                write_manifest(
                    Path(ctx.data_directory),
                    "rosters",
                    int(yr),
                    failed_weeks=info["weeks"],
                )

            # Also manifest per-team failures within weeks that have SOME data
            if all_failed_team_weeks:
                for yr in years_to_fetch:
                    yr = int(yr)
                    failed_weeks_for_year = []
                    for (fail_yr, fail_wk), fail_teams in all_failed_team_weeks.items():
                        if int(fail_yr) == yr and fail_teams:
                            failed_weeks_for_year.append(int(fail_wk))
                    if failed_weeks_for_year:
                        log(
                            f"[MANIFEST] Year {yr} has partial roster failures (team-level) in weeks: {sorted(failed_weeks_for_year)}"
                        )
                        write_manifest(
                            Path(ctx.data_directory),
                            "rosters",
                            yr,
                            failed_weeks=sorted(failed_weeks_for_year),
                        )
        except Exception as e:
            log(f"[WARN] Could not write failure manifests: {e}")

        log(f"\n[OK] Yahoo roster fetch complete -> {output_dir}")

    finally:
        # Clean up temporary OAuth file if created
        if temp_oauth_file and temp_oauth_file.exists():
            try:
                import os

                os.unlink(temp_oauth_file)
                log("Cleaned up temporary OAuth file")
            except Exception:  # noqa: broad-except
                pass


if __name__ == "__main__":
    main()
