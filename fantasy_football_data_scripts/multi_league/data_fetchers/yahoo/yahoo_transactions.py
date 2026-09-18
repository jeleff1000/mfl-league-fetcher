#!/usr/bin/env python3
"""
Transaction Data Fetcher V2 - Multi-League Edition

Fetches Yahoo Fantasy Football transaction data for any league using LeagueContext.
Compatible with the multi-league infrastructure.

Key improvements over V1:
- Multi-league support via LeagueContext
- RunLogger integration for structured logging
- Extracts yahoo_player_id for reliable joins
- No player name cleaning (preserves original Yahoo names)
- Backward compatible with old config.py system
- Cleaner API with context-based configuration
- Better error handling and retry logic
- Per-year file output (transactions_year_YYYY.parquet)
- Automatic caching for completed years
- Fixed deduplication (preserves multi-player trades)

Data Dictionary Compliance:
- Primary keys: (transaction_id, yahoo_player_id) composite
- Foreign keys: yahoo_player_id → player.yahoo_player_id, manager → matchup.manager
- Join keys: (yahoo_player_id, year, week) for player stats enrichment

Output Files:
- transactions_year_2024.parquet (one file per year)
- transactions_year_2024.csv
- cache/transactions_year_2024.parquet (cached for faster re-runs)

Usage:
    # With LeagueContext - single year
    from multi_league.core.league_context import LeagueContext
    ctx = LeagueContext.load("leagues/kmffl/league_context.json")
    df = fetch_transactions(ctx, year=2024)  # Saves to transactions_year_2024.parquet

    # All years (creates one file per year)
    results = fetch_all_transaction_years(ctx)  # Returns dict: {year: DataFrame}

    # CLI with context - single year
    python transactions_v2.py --context leagues/kmffl/league_context.json --year 2024

    # CLI all years (creates separate file for each year)
    python transactions_v2.py --context leagues/kmffl/league_context.json --all-years
"""

from __future__ import annotations

import sys

_verbose = "--verbose" in sys.argv
import argparse
import re
import time
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from xml.etree import ElementTree as ET

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
import requests
import yahoo_fantasy_api as yfa

# Import shared name normalization
try:
    from ..shared.clean_names import normalize_manager_name
except ImportError:
    from multi_league.data_fetchers.shared.clean_names import normalize_manager_name

# Multi-league infrastructure
try:
    from core.league_context import LeagueContext

    LEAGUE_CONTEXT_AVAILABLE = True
except ImportError:
    LeagueContext = None
    LEAGUE_CONTEXT_AVAILABLE = False

try:
    from core.run_metadata import RunLogger

    RUN_LOGGER_AVAILABLE = True
except ImportError:
    RunLogger = None
    RUN_LOGGER_AVAILABLE = False

# Default paths (for standalone mode)
THIS_FILE = Path(__file__).resolve()
SCRIPT_ROOT = THIS_FILE.parent.parent.parent  # Back to scripts root
DEFAULT_DATA_ROOT = SCRIPT_ROOT.parent / "fantasy_football_data" / "transaction_data"


# -----------------------------------------------------------------------------
# Matchup window loader (reuse the exact windows the matchup job writes)
# -----------------------------------------------------------------------------
def load_matchup_windows(ctx, year: int, local_db=None) -> pd.DataFrame:
    """
    Year-scoped window loader with independent Yahoo fallback.
    Order of attempts:
      1) year-specific cache: matchup_windows_{year}.parquet
      2) build from Yahoo API (scoreboard) -> then cache
      3) (optional) read matchup_data_week_all_year_{year}.parquet if present
    """
    if local_db is not None:
        needed_cols = ["year", "week", "week_start", "week_end", "cumulative_week"]
        try:
            df = local_db.read_table("matchup", year=year)
            available = [c for c in needed_cols if c in df.columns]
            if "year" in available and "week" in available:
                windows = df[available].drop_duplicates(subset=["year", "week"]).sort_values(["year", "week"]).copy()
                if "cumulative_week" not in windows.columns:
                    windows["year"] = pd.to_numeric(windows["year"], errors="coerce").astype("Int64")
                    windows["week"] = pd.to_numeric(windows["week"], errors="coerce").astype("Int64")
                    windows["cumulative_week"] = windows["year"].astype(str) + windows["week"].astype(str).str.zfill(2)
                    windows["cumulative_week"] = pd.to_numeric(windows["cumulative_week"], errors="coerce").astype(
                        "Int64"
                    )
                if not windows.empty:
                    return windows.reset_index(drop=True)
        except Exception:
            pass

    mdir = ctx.matchup_data_directory
    cache = mdir / f"matchup_windows_{year}.parquet"  # year-scoped!

    # 1) Use year-specific cache if valid
    if cache.exists():
        try:
            df = pd.read_parquet(cache)
            cols = ["year", "week", "week_start", "week_end", "cumulative_week"]
            df = df[[c for c in cols if c in df.columns]].drop_duplicates()
            if not df.empty:
                return df
        except Exception:  # noqa: broad-except
            pass
    # 2) Try building directly from Yahoo (independent of matchup job)
    try:
        win = build_week_windows_from_yahoo(ctx, year)
        if not win.empty:
            try:
                cache.parent.mkdir(parents=True, exist_ok=True)
                win.to_parquet(cache, index=False)
            except Exception:  # noqa: broad-except
                pass
            return win
    except Exception:  # noqa: broad-except
        pass
    # 3) Last resort: borrow from the matchup “all weeks” artifact if it exists
    all_path = mdir / f"matchup_data_week_all_year_{year}.parquet"
    if all_path.exists():
        try:
            mdf = pd.read_parquet(all_path)
            win = (
                mdf[["year", "week", "week_start", "week_end"]]
                .dropna(subset=["year", "week"])
                .drop_duplicates()
                .sort_values(["year", "week"])
                .copy()
            )
            win["year"] = pd.to_numeric(win["year"], errors="coerce").astype("Int64")
            win["week"] = pd.to_numeric(win["week"], errors="coerce").astype("Int64")
            win["cumulative_week"] = win["year"].astype("Int64").astype(str) + win["week"].astype("Int64").astype(
                str
            ).str.zfill(2)
            with pd.option_context("mode.use_inf_as_na", True):
                win["cumulative_week"] = pd.to_numeric(win["cumulative_week"], errors="coerce").astype("Int64")
            # Save to the year cache for next time
            try:
                win.to_parquet(cache, index=False)
            except Exception:  # noqa: broad-except
                pass
            return win
        except Exception:  # noqa: broad-except
            pass
    # If everything failed, return an empty frame with the right schema
    return pd.DataFrame(columns=["year", "week", "week_start", "week_end", "cumulative_week"])


def build_week_windows_from_yahoo(ctx, year: int) -> pd.DataFrame:
    """
    Build (year, week, week_start, week_end, cumulative_week=YYYYWW) directly
    from Yahoo's scoreboard API. No dependency on matchup outputs.
    """
    oauth = ctx.get_oauth_session()
    gm = yfa.Game(oauth, ctx.game_code)

    # CRITICAL: Use specific league_id from context to avoid data mixing
    league_key = None
    if hasattr(ctx, "get_league_id_for_year"):
        league_key = ctx.get_league_id_for_year(year)

    # Fallback to API discovery (may mix leagues!)
    if not league_key:
        league_ids = gm.league_ids(year=year)
        if not league_ids:
            return pd.DataFrame(columns=["year", "week", "week_start", "week_end", "cumulative_week"])
        if len(league_ids) > 1:
            print(f"[transactions] WARNING: Multiple leagues found for {year}: {league_ids} - using last one")
        league_key = league_ids[-1]

    league = gm.to_league(league_key)

    # Figure out min/max weeks from settings; fall back to 1..17
    try:
        settings = league.settings() or {}
        start_week = int(settings.get("start_week") or 1)
        end_week = int(settings.get("end_week") or 17)
    except Exception:
        start_week, end_week = 1, 17

    rows = []
    for w in range(start_week, end_week + 1):
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/scoreboard;week={w}"
        try:
            root = fetch_url(url, oauth)
        except Exception:  # noqa: broad-except
            continue
        wstarts, wends = [], []
        for m in root.findall(".//matchup"):
            ws = (m.findtext("week_start") or "").strip()
            we = (m.findtext("week_end") or "").strip()
            if ws:
                wstarts.append(ws)
            if we:
                wends.append(we)
        week_start = min(wstarts) if wstarts else None
        week_end = max(wends) if wends else None
        rows.append(
            {
                "year": int(year),
                "week": int(w),
                "week_start": week_start,
                "week_end": week_end,
            }
        )
    win = pd.DataFrame(rows)
    if win.empty:
        return pd.DataFrame(columns=["year", "week", "week_start", "week_end", "cumulative_week"])
    win["year"] = pd.to_numeric(win["year"], errors="coerce").astype("Int64")
    win["week"] = pd.to_numeric(win["week"], errors="coerce").astype("Int64")
    win["cumulative_week"] = win["year"].astype("Int64").astype(str) + win["week"].astype("Int64").astype(
        str
    ).str.zfill(2)
    with pd.option_context("mode.use_inf_as_na", True):
        win["cumulative_week"] = pd.to_numeric(win["cumulative_week"], errors="coerce").astype("Int64")
    return (
        win.dropna(subset=["year", "week"])
        .drop_duplicates(subset=["year", "week"])
        .sort_values(["year", "week"])
        .reset_index(drop=True)
    )


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class Transaction:
    """Represents a single transaction."""

    transaction_id: str
    year: int
    week: int
    timestamp: str
    status: str
    transaction_type: str
    manager: str
    manager_guid: str | None
    team_name: str | None  # Team name for franchise tracking
    player_name: str
    yahoo_player_id: str | None
    player_key: str
    faab_bid: int
    source_type: str | None
    destination: str | None
    partner: str | None  # Trade partner manager name
    source_manager: str | None = None
    source_manager_guid: str | None = None
    source_team_name: str | None = None
    source_franchise_id: str | None = None
    destination_manager: str | None = None
    destination_manager_guid: str | None = None
    destination_team_name: str | None = None
    destination_franchise_id: str | None = None
    week_start: pd.Timestamp | None = None
    week_end: pd.Timestamp | None = None
    cumulative_week: int | None = None


# =============================================================================
# API Retry Logic
# =============================================================================


class APITimeoutError(Exception):
    """Recoverable API timeout."""

    pass


class RecoverableAPIError(APITimeoutError):
    """Transient API failures (rate-limit, 403/429, 'Request denied', empty XML, etc.)."""

    pass


_XML_NS_RE = re.compile(r' xmlns="[^"]+"')


def _first_non_empty(*values) -> str:
    """Return the first non-empty string value, preserving stripped text."""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _is_unknown_manager_value(value) -> bool:
    """Treat blank/NULL/'Unknown' manager values as unresolved."""
    if pd.isna(value):
        return True
    text = str(value).strip()
    return not text or text.lower() == "unknown"


def _lookup_team_meta(
    team_key: str,
    team_mappings: dict[str, str],
    guid_mappings: dict[str, str],
    team_name_mappings: dict[str, str],
    unknown_team_keys: set[str] | None = None,
    *,
    default_manager: str | None = "Unknown",
) -> tuple[str | None, str | None, str | None]:
    """Resolve manager metadata for a Yahoo team key."""
    cleaned_key = _first_non_empty(team_key)
    if not cleaned_key:
        return default_manager, None, None

    manager = team_mappings.get(cleaned_key)
    if manager is None:
        if unknown_team_keys is not None:
            unknown_team_keys.add(cleaned_key)
        manager = default_manager

    return manager, guid_mappings.get(cleaned_key), team_name_mappings.get(cleaned_key)


def _resolve_transaction_team_keys(
    *,
    transaction_type: str,
    destination: str,
    player_source_key: str,
    player_destination_key: str,
    transaction_team_keys: dict[str, str],
) -> tuple[str, str, str]:
    """Return (manager_team_key, source_team_key, destination_team_key) for a player row."""
    source_key = _first_non_empty(
        player_source_key,
        transaction_team_keys.get("source_team_key"),
        transaction_team_keys.get("trader_team_key"),
    )
    destination_key = _first_non_empty(
        player_destination_key,
        transaction_team_keys.get("destination_team_key"),
        transaction_team_keys.get("tradee_team_key"),
        transaction_team_keys.get("waiver_team_key"),
    )

    manager_key = ""
    tx_type = (transaction_type or "").strip().lower()
    dest_type = (destination or "").strip().lower()
    if dest_type == "team":
        manager_key = destination_key
    elif dest_type == "waivers":
        manager_key = source_key
    elif tx_type == "drop":
        manager_key = source_key
    else:
        manager_key = _first_non_empty(destination_key, source_key)

    return manager_key, source_key, destination_key


def _refresh_oauth_token(oauth, reason: str) -> bool:
    """Refresh Yahoo OAuth credentials when the API reports an auth failure."""
    if not hasattr(oauth, "refresh_access_token"):
        print(f"    [AUTH] {reason}; OAuth session cannot refresh access tokens")
        return False

    try:
        print(f"    [AUTH] {reason}; refreshing Yahoo access token...")
        oauth.refresh_access_token()
        print("    [AUTH] Token refreshed successfully")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"    [AUTH] Token refresh failed: {exc}")
        return False


def _is_auth_issue(status_code: int | None, body_text: str, error_text: str) -> bool:
    combined = f"{body_text}\n{error_text}".lower()
    return (
        status_code == 401
        or "unauthorized" in combined
        or "invalid cookie" in combined
        or "please log in again" in combined
        or "must be logged in" in combined
        or "token expired" in combined
    )


def _is_rate_limit_issue(status_code: int | None, body_text: str, error_text: str) -> bool:
    combined = f"{body_text}\n{error_text}".lower()
    return (
        status_code == 429
        or "rate limit" in combined
        or "limit exceeded" in combined
        or "too many requests" in combined
    )


def _is_access_denied_issue(status_code: int | None, body_text: str, error_text: str) -> bool:
    combined = f"{body_text}\n{error_text}".lower()
    return (
        status_code == 403
        or "request denied" in combined
        or "forbidden access" in combined
        or "permission error" in combined
        or "permission denied" in combined
    )


def fetch_url(url: str, oauth, max_retries: int = 6, backoff: float = 0.5) -> ET.Element:
    """
    Fetch URL with retry logic and exponential backoff.

    Args:
        url: URL to fetch
        oauth: OAuth2 session
        max_retries: Maximum retry attempts
        backoff: Initial backoff time in seconds

    Returns:
        ET.Element: Parsed XML root element

    Raises:
        APITimeoutError: If timeout occurs after retries
        RecoverableAPIError: If recoverable error occurs after retries
    """
    last_exc = None
    auth_refreshes = 0
    max_auth_refreshes = 2
    for attempt in range(max_retries):
        try:
            r = oauth.session.get(url, timeout=30)
            status_code = getattr(r, "status_code", None)
            text = (r.text or "").strip()

            if _is_auth_issue(status_code, text, ""):
                if auth_refreshes < max_auth_refreshes and _refresh_oauth_token(
                    oauth, f"Yahoo API auth failure while fetching {url} (status {status_code})"
                ):
                    auth_refreshes += 1
                    time.sleep(min(2.0, backoff * (2**attempt)))
                    continue
                raise RuntimeError(f"Yahoo API authentication failed on {url} (HTTP {status_code})")

            if _is_rate_limit_issue(status_code, text, ""):
                if attempt == max_retries - 1:
                    raise RecoverableAPIError(f"Yahoo API rate limit on {url}")
                time.sleep(backoff * (2**attempt))
                continue

            if _is_access_denied_issue(status_code, text, ""):
                if attempt == max_retries - 1:
                    raise RecoverableAPIError(f"Yahoo API access denied on {url}")
                time.sleep(backoff * (2**attempt))
                continue

            try:
                r.raise_for_status()
            except requests.HTTPError as he:
                code = getattr(he.response, "status_code", None)
                if code in (502, 503, 504):
                    if attempt == max_retries - 1:
                        raise RecoverableAPIError(f"HTTP {code} on {url}") from he
                    time.sleep(backoff * (2**attempt))
                    continue
                raise

            if not text:
                if attempt == max_retries - 1:
                    raise RecoverableAPIError(f"Empty response from {url}")
                time.sleep(backoff * (2**attempt))
                continue

            if _is_rate_limit_issue(None, text, ""):
                if attempt == max_retries - 1:
                    raise RecoverableAPIError(f"Yahoo API rate limit on {url}")
                time.sleep(backoff * (2**attempt))
                continue

            if _is_access_denied_issue(None, text, ""):
                if attempt == max_retries - 1:
                    raise RecoverableAPIError(f"Yahoo API access denied on {url}")
                time.sleep(backoff * (2**attempt))
                continue

            xmlstring = _XML_NS_RE.sub("", text, count=1)
            return ET.fromstring(xmlstring)

        except requests.exceptions.Timeout as e:
            last_exc = e
            if attempt == max_retries - 1:
                raise APITimeoutError(f"Timeout fetching {url}") from e
            time.sleep(backoff * (2**attempt))

        except (requests.RequestException, ET.ParseError, ValueError) as e:
            last_exc = e
            if _is_auth_issue(None, "", str(e)):
                if auth_refreshes < max_auth_refreshes and _refresh_oauth_token(
                    oauth, f"Yahoo API auth failure while fetching {url}"
                ):
                    auth_refreshes += 1
                    time.sleep(min(2.0, backoff * (2**attempt)))
                    continue
                raise RecoverableAPIError(f"Yahoo API authentication failed on {url}: {e}") from e
            if attempt == max_retries - 1:
                raise RecoverableAPIError(f"API error on {url}: {e}") from e
            time.sleep(backoff * (2**attempt))

    raise RecoverableAPIError(f"Failed after {max_retries} retries") from last_exc


# =============================================================================
# Utility Functions
# =============================================================================


def extract_yahoo_player_id(player_key: str) -> str | None:
    """
    Extract yahoo_player_id from player_key.
    Player keys are in format: <game_key>.p.<player_id>
    Example: 461.p.33376 -> 33376
    """
    if not player_key:
        return None
    parts = player_key.split(".")
    if len(parts) >= 3 and parts[-2] == "p":
        return parts[-1]
    return None


def convert_timestamp(ts: str) -> str:
    """Convert Yahoo timestamp (seconds since epoch) to human readable format."""
    try:
        dt = datetime.fromtimestamp(int(ts))
        return dt.strftime("%b %d %Y %I:%M:%S %p").upper()
    except (ValueError, TypeError):
        return "UNKNOWN"


def map_transaction_to_week(ts: str, year: int, matchup_windows: pd.DataFrame) -> tuple[int, str | None, str | None]:
    """
    Map a Yahoo transaction timestamp to (week, week_start, week_end) using the
    exact windows emitted by weekly_matchup_data_v2.
    Returns string week_start/week_end to match the matchup file.
    """
    rows = matchup_windows[matchup_windows["year"] == year].sort_values("week")
    if rows.empty:
        return 1, None, None

    # Parse the transaction timestamp
    try:
        t_pd = pd.to_datetime(datetime.fromtimestamp(int(ts)))
    except (ValueError, TypeError, OverflowError):
        # Unknown timestamp → default to the first week
        r0 = rows.iloc[0]
        return int(r0["week"]), r0.get("week_start"), r0.get("week_end")

    # Filter to only rows with valid week_start and week_end dates
    # This prevents using incomplete/future weeks from matchup windows
    valid_rows = rows[rows["week_start"].notna() & rows["week_end"].notna()].copy()

    if valid_rows.empty:
        # No valid windows at all - default to week 1
        return 1, None, None

    # Prepare typed windows for comparison, but preserve original strings for return
    comp = valid_rows.copy()
    comp["_ws"] = pd.to_datetime(comp["week_start"], errors="coerce")
    comp["_we"] = pd.to_datetime(comp["week_end"], errors="coerce")

    # Add one day to week_end to include the entire end date (not just midnight)
    # This ensures transactions on the last day of the week are included
    comp["_we_inclusive"] = comp["_we"] + pd.Timedelta(days=1)

    # Remove any rows where date parsing failed
    comp = comp[comp["_ws"].notna() & comp["_we"].notna()]

    if comp.empty:
        return 1, None, None

    # Before the first window → week 1
    first = comp.iloc[0]
    if pd.notna(first["_ws"]) and t_pd < first["_ws"]:
        return int(first["week"]), first.get("week_start"), first.get("week_end")

    # Inside any [week_start, week_end] (inclusive of entire end day)
    # Use < with _we_inclusive (next day) instead of <= with _we (midnight)
    m = (comp["_ws"].notna()) & (comp["_we"].notna()) & (t_pd >= comp["_ws"]) & (t_pd < comp["_we_inclusive"])
    if m.any():
        r = comp[m].iloc[0]
        return int(r["week"]), r.get("week_start"), r.get("week_end")

    # After final valid window → estimate the week based on date
    last = comp.iloc[-1]
    last_week_num = int(last["week"])
    last_week_end_inclusive = last["_we_inclusive"]

    # Calculate days since last known week ended (using inclusive end)
    days_since_last = (t_pd - last_week_end_inclusive).days

    # Estimate week (assuming 7 days per week)
    # Add 1 because if transaction is 1-7 days after, it's the next week
    estimated_week = last_week_num + max(1, (days_since_last // 7) + 1)

    # Cap at reasonable maximum (18 weeks for NFL season including playoffs)
    estimated_week = min(estimated_week, 18)

    # Return estimated week with no date boundaries (since they don't exist yet)
    return estimated_week, None, None


# =============================================================================
# Transaction Fetching
# =============================================================================


def fetch_team_mappings(
    oauth,
    league_key: str,
    manager_name_overrides: dict[str, str] | None = None,
    historical_league_keys: list[str] | None = None,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """
    Fetch team ID to manager name mappings with proper normalization.

    Args:
        oauth: OAuth2 session
        league_key: Yahoo league key (e.g., "nfl.l.123456")
        manager_name_overrides: Dict mapping team names/nicknames to real manager names
        historical_league_keys: Optional list of league keys from prior years.
            If provided, mappings are fetched for all historical keys first,
            then the current league_key overrides on conflicts.

    Returns:
        Tuple of (team_key_to_manager, team_key_to_guid, team_key_to_team_name)
    """

    team_id_to_manager: dict[str, str] = {}
    team_id_to_guid: dict[str, str] = {}
    team_id_to_team_name: dict[str, str] = {}

    # Build ordered list: historical first, current last (current wins on conflict)
    keys_to_fetch: list[str] = []
    if historical_league_keys:
        keys_to_fetch.extend(historical_league_keys)
    # Always end with the primary league_key so it overrides historical
    if league_key not in keys_to_fetch:
        keys_to_fetch.append(league_key)
    else:
        # Move to end if already present
        keys_to_fetch.remove(league_key)
        keys_to_fetch.append(league_key)

    for lk in keys_to_fetch:
        teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{lk}/teams"
        try:
            root = fetch_url(teams_url, oauth)
            identity_rows = []

            for team_elem in root.findall(".//team"):
                # Extract team_key
                team_key_elem = team_elem.find("team_key")
                if team_key_elem is None:
                    continue
                team_key = team_key_elem.text

                # Get raw nickname from manager element
                manager_elem = team_elem.find(".//manager/nickname")
                raw_nickname = manager_elem.text if manager_elem is not None else None

                # Get manager guid (persistent identifier across years)
                guid_elem = team_elem.find(".//manager/guid")
                manager_guid = guid_elem.text if guid_elem is not None else None
                identity_rows.append(
                    {
                        "team_key": team_key,
                        "manager_guid": manager_guid,
                        "manager_nickname_raw": raw_nickname,
                        "manager_image_url": team_elem.findtext(".//manager/image_url"),
                    }
                )

                # Get team name for franchise tracking and --hidden-- fallback
                team_name_elem = team_elem.find("name")
                team_name = team_name_elem.text if team_name_elem is not None else None
                team_id_to_team_name[team_key] = team_name

                # Normalize manager name (handles --hidden-- with team_name fallback)
                manager_name = normalize_manager_name(
                    nickname=raw_nickname, overrides=manager_name_overrides, team_name_fallback=team_name
                )

                team_id_to_manager[team_key] = manager_name

            if identity_rows:
                from multi_league.data_fetchers.yahoo.yahoo_identity import resolve_yahoo_manager_guids

                # Match the matchup fetcher's identity inputs for hidden GUIDs.
                # Keeping the base GUID lets the existing SQL identity pass
                # recognize and split both trade parties consistently, even
                # when schedule still carries the pre-split franchise ID.
                identities = pd.DataFrame(identity_rows)
                resolved = resolve_yahoo_manager_guids(identities)
                team_id_to_guid.update(zip(identities["team_key"], resolved))

        except Exception as e:
            if lk == league_key:
                raise RuntimeError(f"Failed to fetch team mappings for primary league {lk}: {e}") from e
            print(f"  WARNING: Failed to fetch historical team mappings for {lk}: {e}")

    return team_id_to_manager, team_id_to_guid, team_id_to_team_name


def fetch_transactions_for_year(
    oauth,
    league_key: str,
    year: int,
    team_mappings: dict[str, str],
    guid_mappings: dict[str, str],
    team_name_mappings: dict[str, str],
    matchup_windows: pd.DataFrame,
    data_dir=None,
) -> list[Transaction]:
    """
    Fetch all transactions for a specific year.

    Args:
        oauth: OAuth2 session
        league_key: Yahoo league key
        year: Season year
        team_mappings: Team key to manager name mapping
        guid_mappings: Team key to manager GUID mapping
        team_name_mappings: Team key to team name mapping (for franchise tracking)
        matchup_windows: DataFrame with week windows for cumulative_week calculation

    Returns:
        List of Transaction objects
    """
    all_transactions = []
    start = 0
    count = 100000  # Fetch all transactions in one call (Yahoo has no practical limit)
    api_calls = 0
    failed_at_offset = None  # Track where pagination broke for retry
    unknown_team_keys: set[str] = set()  # Track team_keys that couldn't be resolved

    while True:
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/transactions;start={start};count={count}"
        api_calls += 1

        try:
            root = fetch_url(url, oauth)
            tx_elements = root.findall(".//transaction")

            if not tx_elements:
                break

            # Parse transactions
            for tr in tx_elements:
                tx_type = (tr.findtext("type") or "").strip()
                status = (tr.findtext("status") or "").strip()
                ts = tr.findtext("timestamp") or "0"
                transaction_team_keys = {
                    "destination_team_key": _first_non_empty(tr.findtext("destination_team_key")),
                    "source_team_key": _first_non_empty(tr.findtext("source_team_key")),
                    "waiver_team_key": _first_non_empty(tr.findtext("waiver_team_key")),
                    "trader_team_key": _first_non_empty(tr.findtext("trader_team_key")),
                    "tradee_team_key": _first_non_empty(tr.findtext("tradee_team_key")),
                }

                # Resolve week via matchup windows (exactly how matchup file defines them)
                week_val, week_start, week_end = map_transaction_to_week(ts, year, matchup_windows)

                faab_bid = (tr.findtext("faab_bid") or "0").strip()
                transaction_key = (tr.findtext("transaction_key") or "").strip()

                # Normalize short transaction_key — Yahoo sometimes returns just the
                # numeric ID (e.g., "112") instead of the full key ("461.l.90939.tr.112").
                # Reconstruct from league_key so all years use the same format.
                if transaction_key and ".tr." not in transaction_key:
                    transaction_key = f"{league_key}.tr.{transaction_key}"

                players_in_tx = tr.findall("players/player")

                # Parse players in transaction
                for player in players_in_tx:
                    player_key = (player.findtext("player_key") or "").strip()
                    name = player.findtext("name/full") or ""
                    # Normalize DEF names at the source: "Bears" → "Bears DST"
                    # Position hint may be absent in transaction XML, so also
                    # check single-word names (NFL players always have first+last;
                    # DEF nicknames like "Bears", "Chiefs" are always one word).
                    _pos_hint = (
                        player.findtext("display_position") or player.findtext("eligible_positions/position") or ""
                    )
                    _is_def_position = _pos_hint.upper() in ("DEF", "DST", "D/ST")
                    _is_single_word = name and " " not in name.strip()
                    if _is_def_position or _is_single_word:
                        from multi_league.data_fetchers.shared.clean_names import normalize_def_player_name

                        _norm, _ = normalize_def_player_name(name)
                        if _norm:
                            name = _norm
                    yahoo_player_id = extract_yahoo_player_id(player_key)

                    ptype = (player.findtext("transaction_data/type") or "").strip()
                    source_type = (player.findtext("transaction_data/source_type") or "").strip()
                    dest_el = player.find("transaction_data/destination_type")
                    destination = (dest_el.text if dest_el is not None else "Unknown") or "Unknown"
                    player_source_key = _first_non_empty(player.findtext("transaction_data/source_team_key"))
                    player_destination_key = _first_non_empty(player.findtext("transaction_data/destination_team_key"))
                    tkey, source_team_key, destination_team_key = _resolve_transaction_team_keys(
                        transaction_type=ptype,
                        destination=destination,
                        player_source_key=player_source_key,
                        player_destination_key=player_destination_key,
                        transaction_team_keys=transaction_team_keys,
                    )

                    nickname, manager_guid, team_name = _lookup_team_meta(
                        tkey,
                        team_mappings,
                        guid_mappings,
                        team_name_mappings,
                        unknown_team_keys,
                    )

                    # Derive trade partner and source/destination fields
                    partner = None
                    src_manager = None
                    src_manager_guid = None
                    src_team_name = None
                    src_franchise_id = None
                    dst_manager = None
                    dst_manager_guid = None
                    dst_team_name = None
                    dst_franchise_id = None
                    if ptype == "trade":
                        src_key = source_team_key
                        dst_key = destination_team_key
                        # Partner is the other side: if player goes TO manager, partner is source
                        partner_key = src_key if destination == "team" else dst_key
                        partner, _, _ = _lookup_team_meta(
                            partner_key,
                            team_mappings,
                            guid_mappings,
                            team_name_mappings,
                            unknown_team_keys,
                            default_manager=None,
                        )
                        # Populate source/destination fields for trade row duplication
                        src_manager, src_manager_guid, src_team_name = _lookup_team_meta(
                            src_key,
                            team_mappings,
                            guid_mappings,
                            team_name_mappings,
                            unknown_team_keys,
                            default_manager=None,
                        )
                        dst_manager, dst_manager_guid, dst_team_name = _lookup_team_meta(
                            dst_key,
                            team_mappings,
                            guid_mappings,
                            team_name_mappings,
                            unknown_team_keys,
                            default_manager=None,
                        )

                    # Derive cumulative_week.  Prefer to look up from
                    # matchup_windows (which contains week-start/end
                    # boundaries for cross-season alignment).  If a record
                    # isn't found or the value is missing, fallback to
                    # constructing it directly from year and week (zero-padded).
                    cumulative_week = None
                    if not matchup_windows.empty:
                        cw_match = matchup_windows[
                            (matchup_windows["year"] == year) & (matchup_windows["week"] == week_val)
                        ]
                        if len(cw_match) > 0:
                            cw_value = cw_match.iloc[0]["cumulative_week"]
                            if pd.notna(cw_value):
                                try:
                                    cumulative_week = int(cw_value)
                                except Exception:
                                    cumulative_week = None
                    # Fallback: build YYYYWW if windows missing
                    if cumulative_week is None:
                        if pd.notna(week_val) and pd.notna(year):
                            try:
                                wk_int = int(week_val)
                                cumulative_week = int(f"{int(year)}{wk_int:02d}")  # e.g., 2024 + 01 → 202401
                            except Exception:
                                cumulative_week = None

                    transaction = Transaction(
                        transaction_id=transaction_key,
                        year=year,
                        week=week_val,
                        timestamp=ts,
                        status=status,
                        transaction_type=ptype,
                        manager=nickname,
                        manager_guid=manager_guid,
                        team_name=team_name,
                        player_name=name,
                        yahoo_player_id=yahoo_player_id,
                        player_key=player_key,
                        faab_bid=int(faab_bid) if faab_bid.isdigit() and ptype == "add" else 0,
                        source_type=source_type,
                        destination=destination,
                        partner=partner,
                        source_manager=src_manager,
                        source_manager_guid=src_manager_guid,
                        source_team_name=src_team_name,
                        source_franchise_id=src_franchise_id,
                        destination_manager=dst_manager,
                        destination_manager_guid=dst_manager_guid,
                        destination_team_name=dst_team_name,
                        destination_franchise_id=dst_franchise_id,
                        # preserve string week_start/week_end to match matchup file
                        week_start=week_start if pd.notna(week_start) else None,
                        week_end=week_end if pd.notna(week_end) else None,
                        cumulative_week=cumulative_week,
                    )

                    all_transactions.append(transaction)

            if len(tx_elements) < count:
                break

            start += count

        except (APITimeoutError, RecoverableAPIError) as e:
            print(f"    ERROR: {e}")
            failed_at_offset = start
            break
        except Exception as e:
            print(f"    ERROR: Unexpected error: {e}")
            failed_at_offset = start
            break

    # Retry pass: if pagination broke, cooldown and resume from the failed offset
    if failed_at_offset is not None:
        print(f"  [{year}] Pagination failed at offset {failed_at_offset} — retrying after 30s cooldown")
        import time

        time.sleep(30)
        resume_start = failed_at_offset
        retry_recovered = False
        for _retry in range(3):  # Up to 3 retry pages
            url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_key}/transactions;start={resume_start};count={count}"
            api_calls += 1
            try:
                root = fetch_url(url, oauth)
                tx_elements = root.findall(".//transaction")
                if not tx_elements:
                    break

                for tr in tx_elements:
                    tx_type = (tr.findtext("type") or "").strip()
                    status = (tr.findtext("status") or "").strip()
                    ts = tr.findtext("timestamp") or "0"
                    week_val, week_start, week_end = map_transaction_to_week(ts, year, matchup_windows)
                    transaction_team_keys = {
                        "destination_team_key": _first_non_empty(tr.findtext("destination_team_key")),
                        "source_team_key": _first_non_empty(tr.findtext("source_team_key")),
                        "waiver_team_key": _first_non_empty(tr.findtext("waiver_team_key")),
                        "trader_team_key": _first_non_empty(tr.findtext("trader_team_key")),
                        "tradee_team_key": _first_non_empty(tr.findtext("tradee_team_key")),
                    }

                    faab_bid = (tr.findtext("faab_bid") or "0").strip()
                    transaction_key = (tr.findtext("transaction_key") or "").strip()

                    # Normalize short transaction_key (same fix as main loop)
                    if transaction_key and ".tr." not in transaction_key:
                        transaction_key = f"{league_key}.tr.{transaction_key}"

                    for player in tr.findall("players/player"):
                        player_key = (player.findtext("player_key") or "").strip()
                        name = player.findtext("name/full") or ""
                        # Normalize DEF names at the source: "Bears" → "Bears DST"
                        _pos_hint = (
                            player.findtext("display_position") or player.findtext("eligible_positions/position") or ""
                        )
                        _is_def_position = _pos_hint.upper() in ("DEF", "DST", "D/ST")
                        _is_single_word = name and " " not in name.strip()
                        if _is_def_position or _is_single_word:
                            from multi_league.data_fetchers.shared.clean_names import normalize_def_player_name

                            _norm, _ = normalize_def_player_name(name)
                            if _norm:
                                name = _norm
                        yahoo_player_id = extract_yahoo_player_id(player_key)
                        ptype = (player.findtext("transaction_data/type") or "").strip()
                        source_type = (player.findtext("transaction_data/source_type") or "").strip()
                        dest_el = player.find("transaction_data/destination_type")
                        destination = (dest_el.text if dest_el is not None else "Unknown") or "Unknown"
                        player_source_key = _first_non_empty(player.findtext("transaction_data/source_team_key"))
                        player_destination_key = _first_non_empty(
                            player.findtext("transaction_data/destination_team_key")
                        )
                        tkey, source_team_key, destination_team_key = _resolve_transaction_team_keys(
                            transaction_type=ptype,
                            destination=destination,
                            player_source_key=player_source_key,
                            player_destination_key=player_destination_key,
                            transaction_team_keys=transaction_team_keys,
                        )

                        nickname, manager_guid, team_name = _lookup_team_meta(
                            tkey,
                            team_mappings,
                            guid_mappings,
                            team_name_mappings,
                            unknown_team_keys,
                        )

                        # Derive trade partner (retry path)
                        partner = None
                        src_manager = None
                        src_manager_guid = None
                        src_team_name = None
                        src_franchise_id = None
                        dst_manager = None
                        dst_manager_guid = None
                        dst_team_name = None
                        dst_franchise_id = None
                        if ptype == "trade":
                            src_key = source_team_key
                            dst_key = destination_team_key
                            partner_key = src_key if destination == "team" else dst_key
                            partner, _, _ = _lookup_team_meta(
                                partner_key,
                                team_mappings,
                                guid_mappings,
                                team_name_mappings,
                                unknown_team_keys,
                                default_manager=None,
                            )
                            src_manager, src_manager_guid, src_team_name = _lookup_team_meta(
                                src_key,
                                team_mappings,
                                guid_mappings,
                                team_name_mappings,
                                unknown_team_keys,
                                default_manager=None,
                            )
                            dst_manager, dst_manager_guid, dst_team_name = _lookup_team_meta(
                                dst_key,
                                team_mappings,
                                guid_mappings,
                                team_name_mappings,
                                unknown_team_keys,
                                default_manager=None,
                            )

                        cumulative_week = None
                        if not matchup_windows.empty:
                            cw_match = matchup_windows[
                                (matchup_windows["year"] == year) & (matchup_windows["week"] == week_val)
                            ]
                            if len(cw_match) > 0:
                                cw_value = cw_match.iloc[0]["cumulative_week"]
                                if pd.notna(cw_value):
                                    try:
                                        cumulative_week = int(cw_value)
                                    except Exception:
                                        cumulative_week = None
                        if cumulative_week is None:
                            if pd.notna(week_val) and pd.notna(year):
                                try:
                                    wk_int = int(week_val)
                                    cumulative_week = int(f"{int(year)}{wk_int:02d}")
                                except Exception:
                                    cumulative_week = None

                        transaction = Transaction(
                            transaction_id=transaction_key,
                            year=year,
                            week=week_val,
                            timestamp=ts,
                            status=status,
                            transaction_type=ptype,
                            manager=nickname,
                            manager_guid=manager_guid,
                            team_name=team_name,
                            player_name=name,
                            yahoo_player_id=yahoo_player_id,
                            player_key=player_key,
                            faab_bid=int(faab_bid) if faab_bid.isdigit() and ptype == "add" else 0,
                            source_type=source_type,
                            destination=destination,
                            partner=partner,
                            source_manager=src_manager,
                            source_manager_guid=src_manager_guid,
                            source_team_name=src_team_name,
                            source_franchise_id=src_franchise_id,
                            destination_manager=dst_manager,
                            destination_manager_guid=dst_manager_guid,
                            destination_team_name=dst_team_name,
                            destination_franchise_id=dst_franchise_id,
                            week_start=week_start if pd.notna(week_start) else None,
                            week_end=week_end if pd.notna(week_end) else None,
                            cumulative_week=cumulative_week,
                        )
                        all_transactions.append(transaction)

                if len(tx_elements) < count:
                    print(f"  [{year}] Retry recovered {len(tx_elements)} transactions from offset {resume_start}")
                    retry_recovered = True
                    break

                resume_start += count
                print(f"  [{year}] Retry recovered page at offset {resume_start - count}")

            except Exception as e:
                print(f"  [{year}] Retry failed at offset {resume_start}: {e}")
                break

        # Write failure manifest if retry did not fully recover
        if not retry_recovered and data_dir is not None:
            try:
                from multi_league.data_fetchers.yahoo.fetch_failure_manifest import write_manifest

                write_manifest(
                    data_dir,
                    "transactions",
                    year,
                    failed_at_offset=failed_at_offset,
                    error=f"Pagination failed at offset {failed_at_offset}",
                )
            except Exception as e:
                print(f"  [WARN] Could not write failure manifest: {e}")

        if not retry_recovered:
            raise RecoverableAPIError(
                f"Transactions pagination for {year} failed at offset {failed_at_offset}; refusing partial year data"
            )

    # Log summary of unresolved team keys
    if unknown_team_keys:
        print(f"  [{year}] WARNING: {len(unknown_team_keys)} unresolved team key(s): {sorted(unknown_team_keys)}")

    print(f"  [{year}] {len(all_transactions)} transactions ({api_calls} API calls)")
    return all_transactions


def transactions_to_dataframe(transactions: list[Transaction]) -> pd.DataFrame:
    """
    Convert list of Transaction objects to DataFrame.

    Args:
        transactions: List of Transaction objects

    Returns:
        DataFrame with proper column order per data dictionary
    """
    if not transactions:
        return pd.DataFrame()

    rows = []
    for tx in transactions:
        human_ts = convert_timestamp(tx.timestamp)

        rows.append(
            {
                # Primary key
                "transaction_id": tx.transaction_id,
                # Foreign keys (for joins)
                "yahoo_player_id": tx.yahoo_player_id,
                "manager": tx.manager,
                "manager_guid": tx.manager_guid,
                "team_name": tx.team_name,  # For franchise tracking
                # Time columns
                "year": tx.year,
                "week": tx.week,
                "cumulative_week": tx.cumulative_week,
                "week_start": tx.week_start,
                "week_end": tx.week_end,
                # Player info (standardized to 'player' - Yahoo API returns player_name)
                "player": tx.player_name,
                "player_key": tx.player_key,
                # Transaction details
                "transaction_type": tx.transaction_type,
                "source_type": tx.source_type,
                "destination": tx.destination,
                "status": tx.status,
                "faab_bid": tx.faab_bid,
                # Trade source/destination
                "source_manager": tx.source_manager,
                "source_manager_guid": tx.source_manager_guid,
                "source_team_name": tx.source_team_name,
                "source_franchise_id": tx.source_franchise_id,
                "destination_manager": tx.destination_manager,
                "destination_manager_guid": tx.destination_manager_guid,
                "destination_team_name": tx.destination_team_name,
                "destination_franchise_id": tx.destination_franchise_id,
                # Timestamps
                "timestamp": tx.timestamp,
                "human_readable_timestamp": human_ts,
            }
        )

    df = pd.DataFrame(rows)

    # After df = transactions_to_dataframe(transactions)
    if "cumulative_week" in df.columns:
        # Cast to Int64, keep NA if any
        df["cumulative_week"] = pd.to_numeric(df["cumulative_week"], errors="coerce").astype("Int64")

    # Create composite keys (per data dictionary)
    df["manager_week"] = df.apply(
        lambda row: (
            f"{str(row['manager']).replace(' ', '')}{int(row['cumulative_week'])}"
            if pd.notna(row["cumulative_week"])
            else ""
        ),
        axis=1,
    )
    df["manager_year"] = df.apply(lambda row: f"{str(row['manager']).replace(' ', '')}{int(row['year'])}", axis=1)

    # Create player_week and player_year to match player table format
    # player_week: player+year+week (e.g., "LamarJackson202104")
    # player_year: player+year (to match player table)
    df["player_week"] = df.apply(
        lambda row: (
            f"{str(row['player']).replace(' ', '')}{int(row['year'])}{int(row['week']):02d}"
            if pd.notna(row["player"]) and pd.notna(row["year"]) and pd.notna(row["week"])
            else ""
        ),
        axis=1,
    )
    df["player_year"] = df.apply(
        lambda row: (
            f"{str(row['player']).replace(' ', '')}{int(row['year'])}"
            if pd.notna(row["player"]) and pd.notna(row["year"])
            else ""
        ),
        axis=1,
    )

    # Final column order (per data dictionary)
    column_order = [
        # Primary key
        "transaction_id",
        # Foreign keys
        "manager",
        "manager_guid",
        "team_name",
        "yahoo_player_id",
        "player",
        # Time columns
        "year",
        "week",
        "cumulative_week",
        "week_start",
        "week_end",
        # Transaction details
        "transaction_type",
        "source_type",
        "destination",
        "status",
        "faab_bid",
        # Trade source/destination (consumed by duplicate_trade_rows)
        "source_manager",
        "source_manager_guid",
        "source_team_name",
        "source_franchise_id",
        "destination_manager",
        "destination_manager_guid",
        "destination_team_name",
        "destination_franchise_id",
        # Metadata
        "player_key",
        "timestamp",
        "human_readable_timestamp",
        # Composite keys
        "manager_week",
        "manager_year",
        "player_week",
        "player_year",
    ]

    # Ensure all columns exist
    for col in column_order:
        if col not in df.columns:
            df[col] = pd.NA

    return df[column_order]


# =============================================================================
# Transaction Processing
# =============================================================================


def process_transactions_chronologically(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure transactions are processed in chronological order.

    This is critical for roster state accuracy - transactions must be applied
    in the exact order they occurred to correctly track player movements.

    Args:
        transactions_df: DataFrame with transaction data

    Returns:
        DataFrame sorted chronologically with transaction_sequence added
    """
    if transactions_df.empty:
        return transactions_df

    # Parse transaction dates properly
    # First, try to use timestamp (Yahoo's epoch timestamp)
    if "timestamp" in transactions_df.columns:
        transactions_df["transaction_datetime"] = pd.to_datetime(
            pd.to_numeric(transactions_df["timestamp"], errors="coerce"), unit="s", errors="coerce"
        )
    elif "transaction_date" in transactions_df.columns:
        # Fallback to transaction_date if available
        transactions_df["transaction_datetime"] = pd.to_datetime(transactions_df["transaction_date"], errors="coerce")
    else:
        # Last resort: create from year/week
        # Use week_start if available, otherwise construct from year/week
        if "week_start" in transactions_df.columns:
            transactions_df["transaction_datetime"] = pd.to_datetime(transactions_df["week_start"], errors="coerce")
        else:
            # Construct from year/week (assuming week 1 = first Monday)
            transactions_df["transaction_datetime"] = pd.to_datetime(
                transactions_df["year"].astype(str) + "-W" + transactions_df["week"].astype(str).str.zfill(2) + "-1",
                format="%Y-W%W-%w",
                errors="coerce",
            )

    # Sort chronologically by:
    # 1. Year (earliest to latest)
    # 2. Week (earliest to latest)
    # 3. Actual datetime (earliest to latest)
    # 4. Transaction ID (for stability)
    sort_cols = ["year", "week", "transaction_datetime"]
    if "transaction_id" in transactions_df.columns:
        sort_cols.append("transaction_id")

    transactions_df = transactions_df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    # Add sequence number for same-day transactions per manager
    # This helps track the order of multiple transactions on the same day
    if "manager" in transactions_df.columns:
        transactions_df["transaction_sequence"] = transactions_df.groupby(
            ["year", "week", "manager"], dropna=False
        ).cumcount()
    else:
        transactions_df["transaction_sequence"] = transactions_df.groupby(["year", "week"], dropna=False).cumcount()

    # Sorting and date range info is now tracked internally (not logged per-year)

    return transactions_df


def backfill_unknown_drop_managers(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Fill unresolved Yahoo drop rows from the player's last known owner."""
    if transactions_df.empty or "manager" not in transactions_df.columns:
        return transactions_df

    df = transactions_df.copy()
    owner_state: dict[tuple[int | None, str], dict[str, str | None]] = {}

    for idx, row in df.iterrows():
        player_identity = _first_non_empty(
            row.get("yahoo_player_id"),
            row.get("player_key"),
            row.get("player"),
        )
        if not player_identity:
            continue

        year_val = pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").iloc[0]
        owner_key = (int(year_val) if pd.notna(year_val) else None, player_identity)
        tx_type = str(row.get("transaction_type") or "").strip().lower()

        if tx_type == "drop" and _is_unknown_manager_value(row.get("manager")):
            previous_owner = owner_state.get(owner_key)
            if previous_owner and not _is_unknown_manager_value(previous_owner.get("manager")):
                df.at[idx, "manager"] = previous_owner.get("manager")
                if "manager_guid" in df.columns and previous_owner.get("manager_guid"):
                    df.at[idx, "manager_guid"] = previous_owner.get("manager_guid")
                if "team_name" in df.columns and previous_owner.get("team_name"):
                    df.at[idx, "team_name"] = previous_owner.get("team_name")

        resolved_manager = df.at[idx, "manager"] if "manager" in df.columns else row.get("manager")
        if tx_type == "drop":
            if not _is_unknown_manager_value(resolved_manager):
                owner_state.pop(owner_key, None)
            continue

        if tx_type in {"add", "trade"} and not _is_unknown_manager_value(resolved_manager):
            owner_state[owner_key] = {
                "manager": str(resolved_manager).strip(),
                "manager_guid": _first_non_empty(df.at[idx, "manager_guid"]) if "manager_guid" in df.columns else None,
                "team_name": _first_non_empty(df.at[idx, "team_name"]) if "team_name" in df.columns else None,
            }

    return df


# =============================================================================
# Main Fetcher Functions
# =============================================================================


def fetch_transactions(
    ctx: LeagueContext,
    year: int,
    matchup_windows: pd.DataFrame | None = None,
    local_db=None,
) -> pd.DataFrame:
    """
    Fetch transaction data for a specific year.

    Args:
        ctx: LeagueContext with league configuration
        year: Season year to fetch
        matchup_windows: Optional matchup windows for week calculation
        local_db: Optional LocalLeagueDB. When provided, matchup windows are built
            directly from the local DuckDB instead of parquet/cache artifacts.

    Returns:
        DataFrame with transaction data
    """
    current_year = get_current_nfl_season_year()

    # Initialize OAuth
    oauth = ctx.get_oauth_session()
    gm = yfa.Game(oauth, ctx.game_code)

    # CRITICAL: Use specific league_id from context to avoid data mixing
    league_key = None
    if hasattr(ctx, "get_league_id_for_year"):
        league_key = ctx.get_league_id_for_year(year)

    # Fallback to API discovery (may mix leagues!)
    if not league_key:
        try:
            league_ids = gm.league_ids(year=year)
            if not league_ids:
                return pd.DataFrame()
            if len(league_ids) > 1:
                print(f"[transactions] WARNING: Multiple leagues found for {year}: {league_ids} - using last one")
            league_key = league_ids[-1]
        except Exception as e:
            print(f"[transactions] ERROR: Failed to get league key for {year}: {e}")
            return pd.DataFrame()

    # Load matchup windows exactly like the matchup job (and build if missing)
    if matchup_windows is None:
        matchup_windows = load_matchup_windows(ctx, year, local_db=local_db)

    # Fetch team mappings (with manager name normalization including --hidden-- fallback)
    manager_overrides = ctx.manager_name_overrides if ctx else {}
    team_mappings, guid_mappings, team_name_mappings = fetch_team_mappings(
        oauth, league_key, manager_name_overrides=manager_overrides
    )

    # Fetch transactions
    transactions = fetch_transactions_for_year(
        oauth,
        league_key,
        year,
        team_mappings,
        guid_mappings,
        team_name_mappings,
        matchup_windows,
        data_dir=ctx.data_directory if ctx else None,
    )

    # Convert to DataFrame
    df = transactions_to_dataframe(transactions)

    # Process transactions in chronological order for roster state accuracy
    if not df.empty:
        df = process_transactions_chronologically(df)
        df = backfill_unknown_drop_managers(df)

    # Check for Unknown managers and write failure manifest
    if not df.empty and "manager" in df.columns:
        unknown_count = int((df["manager"] == "Unknown").sum() + df["manager"].isna().sum())
        if unknown_count > 0:
            print(f"  [{year}] WARNING: {unknown_count}/{len(df)} transactions have Unknown manager")
            if unknown_count == len(df):
                try:
                    from multi_league.data_fetchers.yahoo.fetch_failure_manifest import write_manifest

                    write_manifest(
                        ctx.data_directory,
                        "transactions",
                        year,
                        error=f"{unknown_count} transactions with Unknown manager (team mapping failed)",
                    )
                except Exception as e:
                    print(f"  [WARN] Could not write failure manifest for Unknown managers: {e}")
                raise RuntimeError(
                    f"All {unknown_count} transactions for {year} have Unknown managers; team mapping resolution failed"
                )
            print(f"  [{year}] Targeted recovery will use local DuckDB audit for exact transaction gaps")

    # Add league_id for multi-league isolation (use year-specific league_id)
    df["league_id"] = league_key

    # Apply manager name overrides from context (e.g., "--hidden--" -> "Ilan")
    if ctx.manager_name_overrides and "manager" in df.columns:
        df["manager"] = df["manager"].replace(ctx.manager_name_overrides)

    # Output saved by caller via db.save_table()
    return df


def fetch_all_transaction_years(
    ctx: LeagueContext, matchup_windows: pd.DataFrame | None = None
) -> dict[int, pd.DataFrame]:
    """
    Fetch transaction data for all years in the league context.

    Each year is saved to a separate file: transactions_year_YYYY.parquet

    Args:
        ctx: LeagueContext with league configuration
        matchup_windows: Optional matchup windows for week calculation

    Returns:
        Dictionary mapping year to DataFrame (for backward compatibility)
    """
    current_year = get_current_nfl_season_year()
    start_year = ctx.start_year if ctx.start_year else 2014
    end_year = ctx.end_year if ctx.end_year else current_year

    years = list(range(end_year, start_year - 1, -1))

    print(f"Fetching transaction data for {len(years)} years: {min(years)}-{max(years)}")

    results = {}
    total_records = 0

    for year in years:
        try:
            # fetch_transactions now saves to year-specific files automatically
            df = fetch_transactions(ctx, year, matchup_windows)
            if not df.empty:
                results[year] = df
                total_records += len(df)
        except Exception as e:
            print(f"ERROR: Failed to fetch transactions for {year}: {e}")
            continue

    if not results:
        print("No transaction data fetched")
        return {}

    _safe_print(f"[OK] Transactions: {total_records:,} records across {len(results)} years")

    return results


# =============================================================================
# CLI
# =============================================================================


def _safe_print(*args, **kwargs):
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        print(*(str(a).encode("ascii", "replace").decode("ascii") for a in args), **kwargs)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Fantasy Football transaction data (Multi-League V2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch specific year
  python transactions_v2.py --context /path/to/league_context.json --year 2024

  # Fetch all years
  python transactions_v2.py --context /path/to/league_context.json --all-years

  # Save to custom location
  python transactions_v2.py --context /path/to/league_context.json --year 2024 --output /path/to/output.parquet
        """,
    )

    parser.add_argument("--context", type=Path, required=True, help="Path to league_context.json")
    parser.add_argument("--year", "-y", type=int, help="Specific year to fetch")
    parser.add_argument("--all-years", "-a", action="store_true", help="Fetch all available years")

    args = parser.parse_args()

    # Validate context file
    if not LEAGUE_CONTEXT_AVAILABLE:
        print("ERROR: LeagueContext not available. Ensure multi_league package is installed.")
        return 1

    if not args.context.exists():
        print(f"ERROR: Context file not found: {args.context}")
        return 1

    # Load context
    try:
        ctx = LeagueContext.load(args.context)
    except Exception as e:
        print(f"ERROR: Failed to load league context: {e}")
        return 1

    # Determine years to fetch
    if args.all_years:
        results = fetch_all_transaction_years(ctx)
        if not results:
            print("No data fetched")
            return 1

        total_records = sum(len(df) for df in results.values())
        _safe_print(
            f"\n[OK] Transaction data processing completed successfully ({total_records:,} records across {len(results)} years)"
        )

    elif args.year:
        df = fetch_transactions(ctx, args.year)

        if df.empty:
            print("No data fetched")
            return 1

        _safe_print(f"\n[OK] Transaction data processing completed successfully ({len(df)} records)")

    else:
        print("ERROR: Must specify --year or --all-years")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
