#!/usr/bin/env python3
"""
Draft Data Fetcher V2 - Multi-League Edition

Fetches Yahoo Fantasy Football draft data for any league using LeagueContext.
Compatible with the multi-league infrastructure.

Key improvements over V1:
- Multi-league support via LeagueContext
- RunLogger integration for structured logging
- Backward compatible with old config.py system
- Cleaner API with context-based configuration
- Better error handling and retry logic

Usage:
    # With LeagueContext
    from multi_league.core.league_context import LeagueContext
    ctx = LeagueContext.load("leagues/kmffl/league_context.json")
    df = fetch_draft_data(ctx, year=2024)

    # All years
    df = fetch_all_draft_years(ctx)

    # CLI with context
    python draft_data_v2.py --context leagues/kmffl/league_context.json --year 2024

    # CLI all years
    python draft_data_v2.py --context leagues/kmffl/league_context.json --all-years
"""

from __future__ import annotations

import sys
import argparse
import re
import time
from pathlib import Path
from dataclasses import dataclass
from xml.etree import ElementTree as ET

import pandas as pd
import requests
import yahoo_fantasy_api as yfa
from yahoo_oauth import OAuth2

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
    from core.yahoo_league_settings import load_league_settings

    LOAD_SETTINGS_AVAILABLE = True
except ImportError:
    load_league_settings = None
    LOAD_SETTINGS_AVAILABLE = False

try:
    from core.run_metadata import RunLogger

    RUN_LOGGER_AVAILABLE = True
except ImportError:
    RunLogger = None
    RUN_LOGGER_AVAILABLE = False

try:
    from core.script_runner import log
except ImportError:
    # Fallback to print if script_runner not available
    def log(msg):
        print(msg)


# Default paths (for standalone mode)
THIS_FILE = Path(__file__).resolve()
SCRIPT_ROOT = THIS_FILE.parent.parent.parent  # Back to scripts root
DEFAULT_DATA_ROOT = SCRIPT_ROOT.parent / "fantasy_football_data" / "draft_data"


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class DraftPick:
    """Represents a single draft pick."""

    year: int
    pick: int
    round: int
    team_key: str
    yahoo_player_id: str
    cost: float | None
    player: str | None = None
    yahoo_position: str | None = None
    nfl_team: str | None = None
    is_keeper_status: str | None = None
    is_keeper_cost: str | None = None


DRAFT_FINAL_COLUMNS = [
    "year",
    "pick",
    "round",
    "draft_slot",
    "team_key",
    "manager",
    "manager_guid",
    "yahoo_player_id",
    "cost",
    "player",
    "yahoo_position",
    "avg_pick",
    "avg_round",
    "avg_cost",
    "percent_drafted",
    "preseason_avg_pick",
    "preseason_avg_round",
    "preseason_avg_cost",
    "preseason_percent_drafted",
    "is_keeper_status",
    "is_keeper_cost",
    "player_year",
    "manager_year",
    "nfl_team",
    "draft_type",
]


def _empty_draft_frame() -> pd.DataFrame:
    """Return an empty draft frame with the canonical raw-data schema."""
    df = pd.DataFrame(columns=DRAFT_FINAL_COLUMNS)
    df["yahoo_player_id"] = df["yahoo_player_id"].astype("string")
    return df


def _normalize_yahoo_draft_type(draft_type: str, draft_df: pd.DataFrame | None) -> str:
    """Resolve Yahoo draft type from fetched price data.

    Yahoo's settings metadata is not reliable for distinguishing some live/self
    drafts. If the fetched draft rows contain any concrete `cost` values, this
    was an auction draft and should be normalized immediately at fetch time.
    """
    if draft_df is not None and not draft_df.empty and "cost" in draft_df.columns:
        costs = pd.to_numeric(draft_df["cost"], errors="coerce")
        if costs.notna().any():
            return "auction"

    if draft_type in ("auction", "snake"):
        return draft_type
    return "snake"


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

# Default timeout for API requests (seconds) - used consistently across all functions
DEFAULT_TIMEOUT = 30

# Rate limiting - proactive throttling to avoid hitting API limits.
# State is attached to the oauth session object (oauth._yahoo_last_request_time)
# so it scopes to one league import. A previous module-level global leaked
# timestamps across league boundaries when a single Python process imported
# multiple leagues sequentially (fleet runner pattern), causing spurious sleeps.
DEFAULT_RATE_LIMIT = 2.0  # Max requests per second
_LAST_REQ_ATTR = "_yahoo_last_request_time"


def _rate_limit_wait(oauth, rate_limit: float = DEFAULT_RATE_LIMIT):
    """Wait if necessary to respect rate limit (proactive throttling)."""
    if rate_limit <= 0:
        return

    last = getattr(oauth, _LAST_REQ_ATTR, 0.0)
    elapsed = time.time() - last
    min_interval = 1.0 / rate_limit

    if elapsed < min_interval:
        wait_time = min_interval - elapsed
        time.sleep(wait_time)


def fetch_url(
    url: str, oauth, max_retries: int = 6, backoff: float = 0.5, timeout: int = DEFAULT_TIMEOUT
) -> ET.Element:
    """
    Fetch URL with retry logic and exponential backoff.

    Args:
        url: URL to fetch
        oauth: OAuth2 session
        max_retries: Maximum retry attempts
        backoff: Initial backoff time in seconds
        timeout: Request timeout in seconds

    Returns:
        ET.Element: Parsed XML root element

    Raises:
        APITimeoutError: If timeout occurs after retries
        RecoverableAPIError: If recoverable error occurs after retries
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            # Proactive rate limiting - wait before request if needed.
            # State lives on the oauth object so it doesn't leak across
            # leagues when the same Python process imports several in a row.
            _rate_limit_wait(oauth)

            r = oauth.session.get(url, timeout=timeout)
            try:
                setattr(oauth, _LAST_REQ_ATTR, time.time())
            except Exception:
                # Some oauth implementations forbid attribute mutation; the
                # rate limiter degrades to no-op for that session, which is
                # safe — Yahoo's server-side limiter is the actual gate.
                pass

            try:
                r.raise_for_status()
            except requests.HTTPError as he:
                code = getattr(he.response, "status_code", None)
                if code in (429, 403, 502, 503, 504):
                    if attempt == max_retries - 1:
                        raise RecoverableAPIError(f"HTTP {code} on {url}") from he

                    # Special handling for 429 rate limit - use 10 minute cooldown
                    if code == 429:
                        log("[RATE LIMIT] Yahoo API rate limit hit; signaling wrapper to retry...")
                        raise RecoverableAPIError("rate_limited")

                    else:
                        # Normal exponential backoff for other transient errors
                        wait_time = backoff * (2**attempt)
                        log(
                            f"[RETRY] HTTP {code} error. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}..."
                        )
                        time.sleep(wait_time)
                    continue
                raise

            text = (r.text or "").strip()
            if not text or "Request denied" in text:
                if attempt == max_retries - 1:
                    raise RecoverableAPIError(f"Empty or denied response from {url}")
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
            if attempt == max_retries - 1:
                raise RecoverableAPIError(f"Transient error fetching {url}: {e}") from e
            time.sleep(backoff * (2**attempt))

    if isinstance(last_exc, requests.exceptions.Timeout):
        raise APITimeoutError(f"Timeout fetching {url}") from last_exc
    raise RecoverableAPIError(f"Unknown fetch_url failure for {url}: {last_exc}")


# --- REMOVED: league settings are now fetched in PHASE 0 of initial_import_v2.py ---
# draft_data_v2.py now READS from saved settings files instead of making API calls


# =============================================================================
# Data Fetching
# =============================================================================


def fetch_draft_picks(oauth, league_id: str, year: int) -> list[DraftPick]:
    """
    Fetch draft picks for a league.

    Args:
        oauth: OAuth2 session
        league_id: Yahoo league key
        year: Draft year

    Returns:
        List of DraftPick objects
    """
    from multi_league.data_fetchers.shared.clean_names import normalize_def_player_name

    root = fetch_url(f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_id}/draftresults/players", oauth)

    player_meta_by_key: dict[str, dict[str, str | None]] = {}
    for player_elem in root.findall(".//player"):
        player_key = (player_elem.findtext("player_key") or "").strip()
        if not player_key:
            continue

        player_name = player_elem.findtext("name/full")
        position = player_elem.findtext("display_position") or player_elem.findtext("primary_position")

        # Normalize DEF names at the source: "Bills" -> "Bills DST"
        if position and str(position).upper() in ("DEF", "DST", "D/ST"):
            normalized, _ = normalize_def_player_name(player_name)
            if normalized:
                player_name = normalized

        player_meta_by_key[player_key] = {
            "player_id": (player_elem.findtext("player_id") or "").strip() or None,
            "player_name": player_name,
            "position": position,
            "nfl_team": (player_elem.findtext("editorial_team_abbr") or "").strip() or None,
            "is_keeper_status": (player_elem.findtext("is_keeper/status") or "").strip(),
            "is_keeper_cost": (player_elem.findtext("is_keeper/cost") or "").strip(),
        }

    player_key_re = re.compile(r".*\.p\.([0-9]+)$")

    picks = []
    for result in root.findall(".//draft_result"):
        player_key = (result.findtext("player_key") or "").strip()
        meta = player_meta_by_key.get(player_key, {})
        player_name = meta.get("player_name")
        position = meta.get("position")
        yahoo_player_id = meta.get("player_id")
        if not yahoo_player_id and player_key:
            match = player_key_re.match(player_key)
            if match:
                yahoo_player_id = match.group(1)
        # Normalize DEF names at the source: "Bears" → "Bears DST"
        if position and str(position).upper() in ("DEF", "DST", "D/ST"):
            normalized, _ = normalize_def_player_name(player_name)
            if normalized:
                player_name = normalized
        pick = DraftPick(
            year=year,
            pick=int(result.findtext("pick") or 0),
            round=int(result.findtext("round") or 0),
            team_key=(result.findtext("team_key") or "").strip(),
            yahoo_player_id=str(yahoo_player_id or ""),
            cost=float(result.findtext("cost")) if result.findtext("cost") else None,
            player=player_name,
            yahoo_position=position,
            nfl_team=meta.get("nfl_team"),
            is_keeper_status=meta.get("is_keeper_status"),
            is_keeper_cost=meta.get("is_keeper_cost"),
        )
        picks.append(pick)

    return picks


def fetch_team_and_player_mappings(
    oauth, league_id: str, timeout: int = DEFAULT_TIMEOUT, manager_name_overrides: dict[str, str] | None = None
) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    """
    Fetch team and player mappings from Yahoo API.

    Args:
        oauth: OAuth2 session
        league_id: Yahoo league key
        timeout: Request timeout in seconds
        manager_name_overrides: Dict mapping team names/nicknames to real manager names

    Returns:
        Tuple of (team_key_to_manager, team_key_to_guid, team_key_to_team_name, player_id_to_name, player_id_to_team)
    """
    team_key_to_manager = {}
    team_key_to_guid = {}
    team_key_to_team_name = {}
    player_id_to_name = {}
    player_id_to_team = {}

    # Step 1: Fetch teams from /league/{league_id}/teams endpoint
    # This endpoint reliably returns both team name and manager info
    try:
        teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_id}/teams"
        root = fetch_url(teams_url, oauth)

        for team_elem in root.findall(".//team"):
            team_key_elem = team_elem.find("team_key")
            if team_key_elem is None:
                continue
            team_key = team_key_elem.text

            # Get team name (used for franchise tracking and --hidden-- fallback)
            team_name_elem = team_elem.find("name")
            team_name = team_name_elem.text if team_name_elem is not None else None
            team_key_to_team_name[team_key] = team_name

            # Get raw nickname from manager element
            manager_elem = team_elem.find(".//manager/nickname")
            raw_nickname = manager_elem.text if manager_elem is not None else None

            # Get manager guid (persistent identifier across years)
            guid_elem = team_elem.find(".//manager/guid")
            manager_guid = guid_elem.text if guid_elem is not None else None
            team_key_to_guid[team_key] = manager_guid

            # Normalize manager name (handles --hidden-- with team_name fallback)
            manager_name = normalize_manager_name(
                nickname=raw_nickname, overrides=manager_name_overrides, team_name_fallback=team_name
            )
            team_key_to_manager[team_key] = manager_name

        log(f"[draft] Fetched {len(team_key_to_manager)} teams from /teams endpoint")
    except (APITimeoutError, RecoverableAPIError) as e:
        log(f"[draft] Warning: Could not fetch teams from /teams endpoint: {e}")

    # Draft player names, positions, and NFL teams are returned by the combined
    # draftresults/players payload. Fetching every Week 1 team roster here was
    # redundant and allowed one unavailable team to consume the refresh budget.

    return team_key_to_manager, team_key_to_guid, team_key_to_team_name, player_id_to_name, player_id_to_team


def _parse_draft_analysis_player(player: ET.Element, year: int) -> dict:
    """Parse a single player's draft analysis data from XML element."""
    # Position can be in display_position (e.g., "LB,DE") or eligible_positions
    position = player.findtext("display_position")
    if not position:
        # Fallback to first eligible position
        eligible = player.find("eligible_positions/position")
        position = eligible.text if eligible is not None else None

    player_name = player.findtext("name/full")
    # Normalize DEF names at the source: "Bears" → "Bears DST"
    if position and str(position).upper() in ("DEF", "DST", "D/ST"):
        from multi_league.data_fetchers.shared.clean_names import normalize_def_player_name

        normalized, _ = normalize_def_player_name(player_name)
        if normalized:
            player_name = normalized

    return {
        "year": year,
        "yahoo_player_id": player.findtext("player_id"),
        "player": player_name,
        "yahoo_position": position,
        "avg_pick": player.findtext("draft_analysis/average_pick"),
        "avg_round": player.findtext("draft_analysis/average_round"),
        "avg_cost": player.findtext("draft_analysis/average_cost"),
        "percent_drafted": player.findtext("draft_analysis/percent_drafted"),
        "preseason_avg_pick": player.findtext("draft_analysis/preseason_average_pick"),
        "preseason_avg_round": player.findtext("draft_analysis/preseason_average_round"),
        "preseason_avg_cost": player.findtext("draft_analysis/preseason_average_cost"),
        "preseason_percent_drafted": player.findtext("draft_analysis/preseason_percent_drafted"),
        "is_keeper_status": (player.findtext("is_keeper/status") or ""),
        "is_keeper_cost": (player.findtext("is_keeper/cost") or ""),
    }


def fetch_draft_analysis(
    oauth, league_id: str, year: int, player_ids: list[str], timeout: int = DEFAULT_TIMEOUT, batch_size: int = 25
) -> pd.DataFrame:
    """
    Fetch draft analysis data from Yahoo API for specific players.

    Args:
        oauth: OAuth2 session
        league_id: Yahoo league key (e.g., "423.l.123456")
        year: Draft year
        player_ids: List of yahoo_player_ids to fetch (required)
        timeout: Request timeout
        batch_size: Number of players per batch request (max ~25)

    Returns:
        DataFrame with draft analysis data
    """
    if not player_ids:
        log("[draft_analysis] No player_ids provided, returning empty DataFrame")
        return pd.DataFrame()

    draft_analysis = []

    # Extract game_key from league_id (e.g., "423.l.123456" -> "423")
    game_key = league_id.split(".")[0] if "." in league_id else league_id

    # Remove duplicates and None values
    unique_ids = list(set(pid for pid in player_ids if pid))
    total_players = len(unique_ids)
    total_batches = (total_players + batch_size - 1) // batch_size
    log(f"[draft_analysis] Fetching analysis for {total_players} drafted players ({total_batches} batches)")

    # Batch the player IDs
    for i in range(0, total_players, batch_size):
        batch_ids = unique_ids[i : i + batch_size]
        batch_num = i // batch_size + 1

        # Construct player_keys: {game_key}.p.{player_id}
        player_keys = [f"{game_key}.p.{pid}" for pid in batch_ids]
        player_keys_str = ",".join(player_keys)

        # Retry logic for batch fetch (up to 3 attempts with backoff)
        # IMPORTANT: Use league-specific endpoint to get keeper data
        # The general /players endpoint returns empty is_keeper fields
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{league_id}/players;player_keys={player_keys_str}/draft_analysis"
        root = None
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                root = fetch_url(url, oauth)
                break  # Success
            except (APITimeoutError, RecoverableAPIError) as e:
                if attempt < max_attempts - 1:
                    wait_time = 2**attempt  # 1s, 2s
                    log(
                        f"[draft_analysis] Batch {batch_num} attempt {attempt + 1} failed: {e}, retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    log(f"[draft_analysis] WARNING: Batch {batch_num} failed after {max_attempts} attempts: {e}")

        if root is None:
            continue  # Skip this batch after all retries failed

        players = root.findall(".//player")
        for player in players:
            draft_analysis.append(_parse_draft_analysis_player(player, year))

    log(f"[draft_analysis] Completed: {len(draft_analysis)} players fetched")
    return pd.DataFrame(draft_analysis)


# =============================================================================
# Data Processing
# =============================================================================


def merge_draft_data(
    picks: list[DraftPick],
    analysis_df: pd.DataFrame,
    team_key_to_manager: dict[str, str],
    team_key_to_guid: dict[str, str],
    player_id_to_team: dict[str, str],
    player_id_to_name: dict[str, str],
    manager_name_overrides: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Merge draft picks with analysis data and apply enrichments.

    IMPORTANT: This fetcher now outputs RAW DATA ONLY (no transformations).
    Value calculations (pick_savings, cost_savings, savings, cost_bucket) are
    handled by draft_enrichment_v2.py in the transformation layer.

    Args:
        picks: List of DraftPick objects
        analysis_df: DataFrame with draft analysis data
        team_key_to_manager: Dict mapping team keys to manager names
        team_key_to_guid: Dict mapping team keys to manager GUIDs
        player_id_to_team: Dict mapping player IDs to NFL teams
        player_id_to_name: Dict mapping player IDs to player names
        manager_name_overrides: Optional dict to override manager names

    Returns:
        Merged and enriched DataFrame with raw Yahoo API data only
    """
    if not picks:
        return _empty_draft_frame()

    # Convert picks to DataFrame
    picks_df = pd.DataFrame([p.__dict__ for p in picks])

    # Enrich picks with manager and team info
    picks_df["manager"] = picks_df["team_key"].map(team_key_to_manager).fillna("N/A")
    picks_df["manager_guid"] = picks_df["team_key"].map(team_key_to_guid)
    payload_nfl_team = picks_df["nfl_team"].replace("", pd.NA)
    picks_df["nfl_team"] = payload_nfl_team.fillna(picks_df["yahoo_player_id"].map(player_id_to_team)).fillna("N/A")

    # Backfill missing player names
    def _missing_player_name(value) -> bool:
        if pd.isna(value):
            return True
        text = str(value).strip()
        return text in {"", "N/A", "None", "nan", "<NA>"}

    picks_df["player"] = picks_df.apply(
        lambda row: (
            player_id_to_name.get(str(row["yahoo_player_id"]), row["player"])
            if _missing_player_name(row["player"])
            else row["player"]
        ),
        axis=1,
    )

    # Apply manager name overrides
    if manager_name_overrides:
        picks_df["manager"] = picks_df["manager"].apply(lambda x: manager_name_overrides.get(str(x or "").strip(), x))

    # Normalize yahoo_player_id to string
    if "yahoo_player_id" in picks_df.columns:
        picks_df["yahoo_player_id"] = picks_df["yahoo_player_id"].astype(str)

    # ADP data (avg_pick, percent_drafted, etc.) now lives in
    # ___ops.yahoo_historical.yahoo_draft_analysis — frontend joins at query time.
    # No merge with analysis_df needed.
    merged = picks_df

    # Create composite keys
    merged["player_year"] = merged["player"].str.replace(" ", "", regex=False) + merged["year"].astype(str)
    merged["manager_year"] = merged["manager"].str.replace(" ", "", regex=False) + merged["year"].astype(str)

    # Add is_keeper_status and is_keeper_cost if missing
    if "is_keeper_status" not in merged.columns:
        merged["is_keeper_status"] = ""
    if "is_keeper_cost" not in merged.columns:
        merged["is_keeper_cost"] = ""

    # DEFENSIVE: Infer missing pick/round from sequence if null
    # This handles edge cases where Yahoo API returned incomplete data
    null_picks = merged[merged["pick"].isna()]
    if len(null_picks) > 0:
        log(f"[draft] Inferring {len(null_picks)} missing pick values...")
        for idx, row in null_picks.iterrows():
            year = row["year"]
            round_num = row["round"]

            # Try to infer from same round
            if pd.notna(round_num):
                same_round = merged[(merged["year"] == year) & (merged["round"] == round_num) & merged["pick"].notna()]
                if len(same_round) > 0:
                    used_picks = set(same_round["pick"].astype(int))
                    max_pick = max(used_picks) if used_picks else 0
                    for i in range(1, int(max_pick) + 2):
                        if i not in used_picks:
                            merged.loc[idx, "pick"] = i
                            log(f"[draft] Inferred pick {i} for row {idx} (round {round_num})")
                            break

    null_rounds = merged[merged["round"].isna()]
    if len(null_rounds) > 0:
        log(f"[draft] Inferring {len(null_rounds)} missing round values...")
        for idx, row in null_rounds.iterrows():
            year = row["year"]
            pick_num = row["pick"]

            # Try to infer round from pick number (assumes consistent team count)
            if pd.notna(pick_num):
                same_year = merged[(merged["year"] == year) & merged["round"].notna()]
                if len(same_year) > 0:
                    # Find teams per round
                    round_1 = same_year[same_year["round"] == 1]
                    if len(round_1) > 0:
                        teams_per_round = len(round_1)
                        inferred_round = int((pick_num - 1) // teams_per_round) + 1
                        merged.loc[idx, "round"] = inferred_round
                        log(f"[draft] Inferred round {inferred_round} for row {idx} (pick {pick_num})")

    # Calculate draft_slot for snake drafts (position in draft order, used for linking traded picks)
    # For snake drafts: Round 1 pick 1 = slot 1, Round 1 pick 12 = slot 12
    # Round 2 snakes back: pick 13 = slot 12, pick 24 = slot 1
    merged["draft_slot"] = pd.NA
    for year in merged["year"].dropna().unique():
        year_mask = merged["year"] == year
        year_df = merged[year_mask]
        round_1 = year_df[year_df["round"] == 1]
        num_teams = len(round_1) if len(round_1) > 0 else 12  # Default to 12 if can't determine

        for idx, row in year_df.iterrows():
            pick_num = row.get("pick")
            round_num = row.get("round")
            if pd.isna(pick_num) or pd.isna(round_num):
                continue

            # Position within the round (1-indexed)
            position_in_round = ((int(pick_num) - 1) % num_teams) + 1

            # Snake drafts reverse every other round
            if int(round_num) % 2 == 0:  # Even rounds snake back
                draft_slot = num_teams - position_in_round + 1
            else:  # Odd rounds go forward
                draft_slot = position_in_round

            merged.loc[idx, "draft_slot"] = draft_slot

    # Final column order (RAW DATA ONLY - no transformations)
    final_cols = DRAFT_FINAL_COLUMNS

    # Only add columns that don't exist (don't overwrite existing ones)
    for col in final_cols:
        if col not in merged.columns:
            merged[col] = pd.NA

    # Ensure yahoo_player_id is consistently string for downstream joins
    out_df = merged[final_cols].copy()
    if "yahoo_player_id" in out_df.columns:
        try:
            out_df["yahoo_player_id"] = out_df["yahoo_player_id"].astype("string")
        except Exception:
            out_df["yahoo_player_id"] = out_df["yahoo_player_id"].astype(str).astype("string")

    return out_df


# =============================================================================
# Main API
# =============================================================================


def fetch_draft_data(
    ctx: LeagueContext | None = None,
    year: int | None = None,
    oauth_file: Path | None = None,
    league_key: str | None = None,
    data_dir: Path | None = None,
    logger: RunLogger | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """
    Fetch draft data for a single year.

    Args:
        ctx: Optional LeagueContext for league-specific configuration
        year: Draft year to fetch
        oauth_file: Path to OAuth credentials (if no context)
        league_key: Yahoo league_key (if no context)
        data_dir: Custom data directory (overrides context)
        logger: Optional RunLogger instance

    Returns:
        DataFrame with draft data

    Raises:
        ValueError: If required parameters missing
        FileNotFoundError: If OAuth file not found
    """
    # Determine data directory
    if data_dir:
        draft_data_dir = Path(data_dir)
    elif ctx:
        draft_data_dir = ctx.draft_data_directory
    else:
        draft_data_dir = DEFAULT_DATA_ROOT

    if year is None:
        raise ValueError("year is required")

    # Create logger if context provided
    # Only instantiate RunLogger if it's available. Use a safe try/except so
    # a misconfigured RunLogger (None or non-callable) won't raise at runtime
    # and static analyzers are less likely to flag the call site.
    if logger is None and ctx and RUN_LOGGER_AVAILABLE:
        try:
            if RunLogger is None or not callable(RunLogger):
                raise RuntimeError("RunLogger not available or not callable")
            logger = RunLogger("draft_data", year=year, league_id=ctx.league_id)
            # If the logger implements context manager methods, enter it.
            if hasattr(logger, "__enter__"):
                logger.__enter__()
            close_logger = True
        except Exception:
            # Failed to initialize logger; continue without logging.
            logger = None
            close_logger = False
    else:
        close_logger = False

    try:
        # Initialize OAuth
        if logger:
            logger.start_step("initialize_oauth")

        if ctx:
            oauth = ctx.get_oauth_session()
            league_key = ctx.league_id
        elif oauth_file:
            oauth_path = Path(oauth_file)
            if not oauth_path.exists():
                raise FileNotFoundError(f"OAuth file not found: {oauth_path}")
            oauth = OAuth2(None, None, from_file=str(oauth_path))
            if not oauth.token_is_valid():
                oauth.refresh_access_token()
        else:
            raise ValueError("Either ctx or oauth_file is required")

        gm = yfa.Game(oauth, "nfl")

        if logger:
            logger.complete_step()

        # Get league ID for year
        if logger:
            logger.start_step("get_league_id")

        # CRITICAL: Use specific league_id from context to avoid data mixing
        year_league_id = None

        # Try context first (safest - ensures league isolation)
        if ctx and hasattr(ctx, "get_league_id_for_year"):
            year_league_id = ctx.get_league_id_for_year(year)

        # Fallback to explicit league_key parameter
        if not year_league_id and league_key:
            year_league_id = league_key

        # Last resort: use API discovery (may mix leagues!)
        if not year_league_id:
            league_ids = gm.league_ids(year=year)
            if not league_ids:
                raise ValueError(f"No league found for year {year}")
            if len(league_ids) > 1:
                log(f"[draft] WARNING: Multiple leagues found for {year}: {league_ids} - using last one")
            year_league_id = league_ids[-1]

        # --- LOAD settings from saved file (fetched in PHASE 0 of initial_import_v2.py) ---
        if LOAD_SETTINGS_AVAILABLE and load_league_settings:
            # NEW location (as of 2025): {data_directory}/league_settings/
            # OLD location (backwards compatibility): {data_directory}/player_data/yahoo_league_settings/
            if ctx:
                # Try NEW location first
                settings_dir = Path(ctx.data_directory) / "league_settings"
                if not settings_dir.exists():
                    # Fallback to OLD location
                    settings_dir = Path(ctx.player_data_directory) / "yahoo_league_settings"
            elif data_dir:
                # Try NEW location first
                settings_dir = Path(data_dir) / "league_settings"
                if not settings_dir.exists():
                    # Fallback to OLD location
                    settings_dir = Path(data_dir) / "player_data" / "yahoo_league_settings"
            else:
                # Try NEW location first
                settings_dir = DEFAULT_DATA_ROOT / "league_settings"
                if not settings_dir.exists():
                    # Fallback to OLD location
                    settings_dir = DEFAULT_DATA_ROOT / "player_data" / "yahoo_league_settings"

            from multi_league.core.canonical_settings import YAHOO_DRAFT_TYPE_MAP

            saved_settings = load_league_settings(year, league_key, settings_dir)
            if saved_settings:
                raw_type = (saved_settings.get("metadata", {}).get("draft_type") or "").lower()
                draft_type = YAHOO_DRAFT_TYPE_MAP.get(raw_type, "unknown")
            else:
                draft_type = "unknown"
        else:
            draft_type = "unknown"

        if logger:
            logger.complete_step(league_id=year_league_id)

        # Fetch mappings
        if logger:
            logger.start_step("fetch_mappings")

        # Get manager_name_overrides from context (for --hidden-- fallback)
        manager_overrides = ctx.manager_name_overrides if ctx else {}

        team_key_to_manager, team_key_to_guid, team_key_to_team_name, player_id_to_name, player_id_to_team = (
            fetch_team_and_player_mappings(
                oauth, year_league_id, timeout=timeout, manager_name_overrides=manager_overrides
            )
        )

        if logger:
            logger.complete_step(teams=len(team_key_to_manager), players=len(player_id_to_name))

        # Fetch draft picks
        if logger:
            logger.start_step("fetch_draft_picks")

        picks = fetch_draft_picks(oauth, year_league_id, year)
        log(f"[draft] Fetched {len(picks)} draft picks")

        if logger:
            logger.complete_step(picks=len(picks))

        # Draft analysis (ADP, percent_drafted) is now in ___ops.yahoo_historical.yahoo_draft_analysis.
        # Frontend joins at query time. No API calls needed.
        analysis_df = pd.DataFrame()

        # Merge data
        if logger:
            logger.start_step("merge_data")

        final_df = merge_draft_data(
            picks,
            analysis_df,
            team_key_to_manager,
            team_key_to_guid,
            player_id_to_team,
            player_id_to_name,
            manager_overrides,
        )

        # Add team_name for franchise tracking (maps team_key -> team_name)
        if "team_key" in final_df.columns:
            final_df["team_name"] = final_df["team_key"].map(team_key_to_team_name)
            log(f"[draft] Added team_name column ({final_df['team_name'].notna().sum()} mapped)")

        draft_type = _normalize_yahoo_draft_type(draft_type, final_df)

        # Store draft_type in DataFrame for downstream transformation layer
        # (draft_enrichment_v2.py needs this to calculate appropriate value metrics)
        final_df["draft_type"] = draft_type

        if logger:
            logger.complete_step(rows=len(final_df))

        # Ensure Yahoo player ID is a string.  Downstream merges depend
        # on matching yahoo_player_id across data sources; casting to a
        # consistent string dtype avoids object/int mismatches.
        if "yahoo_player_id" in final_df.columns:
            try:
                final_df["yahoo_player_id"] = final_df["yahoo_player_id"].astype("string")
            except Exception:
                final_df["yahoo_player_id"] = final_df["yahoo_player_id"].astype(str).astype("string")

        # Add league_id for multi-league isolation (use year-specific league_id)
        final_df["league_id"] = year_league_id

        # Output saved by caller via db.save_table()
        log(f"[output] Total rows: {len(final_df):,}")

        return final_df

    finally:
        if close_logger and logger:
            logger.__exit__(None, None, None)


def fetch_all_draft_years(
    ctx: LeagueContext | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    oauth_file: Path | None = None,
    league_key: str | None = None,
    data_dir: Path | None = None,
    resume: bool = False,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """
    Fetch draft data for all years.

    Args:
        ctx: Optional LeagueContext for league-specific configuration
        start_year: First year to fetch (defaults to ctx.start_year or 2014)
        end_year: Last year to fetch (defaults to current year)
        oauth_file: Path to OAuth credentials (if no context)
        league_key: Yahoo league_key (if no context)
        data_dir: Custom data directory (overrides context)

    Returns:
        DataFrame with all years combined
    """
    from multi_league.core.date_utils import get_current_nfl_season_year

    if start_year is None:
        start_year = ctx.start_year if ctx else 2014

    if end_year is None:
        current_year = get_current_nfl_season_year()
        end_year = ctx.end_year if ctx and ctx.end_year else current_year

    # Determine data directory
    if data_dir:
        draft_data_dir = Path(data_dir)
    elif ctx:
        draft_data_dir = ctx.draft_data_directory
    else:
        draft_data_dir = DEFAULT_DATA_ROOT

    log(f"[draft] Fetching years {start_year} to {end_year}")

    all_dfs = []
    failed_years = []
    for year in range(end_year, start_year - 1, -1):
        try:
            df = fetch_draft_data(
                ctx=ctx,
                year=year,
                oauth_file=oauth_file,
                league_key=league_key,
                data_dir=data_dir,
            )
            all_dfs.append(df)
        except Exception as e:
            log(f"[warn] Failed to fetch year {year}: {e}")
            failed_years.append(year)
            continue

    # Retry pass: recover failed years after a cooldown
    if failed_years:
        import time

        log(f"[RETRY] {len(failed_years)} year(s) failed — retrying after 30s cooldown: {failed_years}")
        time.sleep(30)
        recovered_years = []
        for year in failed_years:
            try:
                df = fetch_draft_data(
                    ctx=ctx,
                    year=year,
                    oauth_file=oauth_file,
                    league_key=league_key,
                    data_dir=data_dir,
                )
                all_dfs.append(df)
                recovered_years.append(year)
                log(f"  [OK] Recovered draft data for {year}")
            except Exception as e:
                log(f"  [FAIL] Year {year} still failing: {e}")

        # Write failure manifests for years that still failed after retry
        still_failed = [y for y in failed_years if y not in recovered_years]
        if still_failed:
            log(f"  [WARN] Draft data permanently failed for years: {still_failed}")
            try:
                from multi_league.data_fetchers.yahoo.fetch_failure_manifest import write_manifest

                manifest_dir = draft_data_dir.parent
                for yr in still_failed:
                    write_manifest(
                        manifest_dir,
                        "draft",
                        yr,
                        failed_year=True,
                    )
            except Exception as e:
                log(f"  [WARN] Could not write failure manifest: {e}")

    if not all_dfs:
        raise ValueError("No draft data was successfully fetched")

    # Combine all years
    combined = pd.concat(all_dfs, ignore_index=True)

    # Deduplicate: keeper picks can appear as both keeper and regular draft entries
    before_len = len(combined)
    if all(col in combined.columns for col in ["year", "round", "pick"]):
        combined = combined.drop_duplicates(subset=["year", "round", "pick"], keep="first")
        if len(combined) < before_len:
            log(f"[dedup] Removed {before_len - len(combined):,} duplicate draft picks")

    log(f"[output] Total rows: {len(combined):,}")
    return combined


# =============================================================================
# CLI
# =============================================================================


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo Fantasy Football draft data (multi-league compliant)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # With LeagueContext
  python draft_data_v2.py --context leagues/kmffl/league_context.json --year 2024

  # All years with context
  python draft_data_v2.py --context leagues/kmffl/league_context.json --all-years

  # Standalone (backward compatible)
  python draft_data_v2.py --year 2024 --oauth Oauth.json --league-key nfl.l.123456
        """,
    )

    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--year", type=int, help="Single year to fetch (overrides --all-years if provided)")
    parser.add_argument("--all-years", action="store_true", help="Fetch all years")
    parser.add_argument("--resume", action="store_true", help="Skip years already written to output folder")
    parser.add_argument("--per-request-timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds")
    parser.add_argument("--oauth", type=Path, help="Path to OAuth credentials (if no context)")
    parser.add_argument("--league-key", help="Yahoo league_key (if no context)")
    parser.add_argument("--data-dir", type=Path, help="Custom data directory")

    args = parser.parse_args()

    # Load context if provided
    ctx = None
    if args.context:
        if not LEAGUE_CONTEXT_AVAILABLE:
            print("Error: league_context module not available", file=sys.stderr)
            sys.exit(1)

        try:
            ctx = LeagueContext.load(args.context)
            log(f"Loaded context: {ctx.league_name}")
        except Exception as e:
            print(f"Error loading context: {e}", file=sys.stderr)
            sys.exit(1)

    # Validate arguments
    if not args.all_years and not args.year:
        print("Error: Must specify either --year or --all-years", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # Run fetch
    try:
        if args.all_years:
            fetch_all_draft_years(
                ctx=ctx, oauth_file=args.oauth, league_key=args.league_key, data_dir=args.data_dir, resume=args.resume
            )
        else:
            fetch_draft_data(
                ctx=ctx, year=args.year, oauth_file=args.oauth, league_key=args.league_key, data_dir=args.data_dir
            )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
