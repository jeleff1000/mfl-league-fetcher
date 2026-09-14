#!/usr/bin/env python3
"""
Fetch team defensive stats from NFLverse for fantasy football DST scoring.

LEGACY NOTICE: This is a fallback fetcher. Primary data flow now uses the pre-built
super_table in MotherDuck (___ops.nfl_historical.nfl_player_stats_all).

For new development:
- Use load_nfl_from_super_table.py to load NFL data (includes defense)
- Use update_nfl_super_table.py to update the super_table

This fetcher is retained for:
- Local development when MotherDuck is unavailable
- Initial super_table population
- Debug/verification purposes

This script downloads team-level defensive statistics from NFLverse and transforms
them into a format suitable for merging with Yahoo fantasy DST data.

Source: https://github.com/nflverse/nflverse-data/releases/download/stats_team/stats_team_week_{year}.parquet

See: docs/PLAYER_FLOW.md for the current data pipeline architecture.
"""

import argparse
import sys
import time
from pathlib import Path
import pandas as pd
import requests

from multi_league.core.date_utils import get_current_nfl_season_year

# Import centralized logging
try:
    from multi_league.core.logging_config import get_logger
except ImportError:
    from core.logging_config import get_logger

logger = get_logger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent  # yahoo_oauth

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

# Import franchise-based display name function
try:
    from nfl_data.nfl_franchises import get_def_display_name, get_def_player_id

    FRANCHISE_FUNCTIONS_AVAILABLE = True
except ImportError:
    FRANCHISE_FUNCTIONS_AVAILABLE = False
    get_def_display_name = None
    get_def_player_id = None

try:
    from nfl_data.blocked_punts import apply_historical_dst_punt_blocks
except ImportError:
    apply_historical_dst_punt_blocks = None

try:
    from nfl_data.team_margin import apply_team_margin_stats, calculate_team_margin_stats_from_pbp
except ImportError:
    apply_team_margin_stats = None
    calculate_team_margin_stats_from_pbp = None

# Try to import LeagueContext for multi-league support
try:
    from core.league_context import LeagueContext

    LEAGUE_CONTEXT_AVAILABLE = True
except ImportError:
    try:
        from multi_league.core.league_context import LeagueContext

        LEAGUE_CONTEXT_AVAILABLE = True
    except ImportError:
        LEAGUE_CONTEXT_AVAILABLE = False
        LeagueContext = None

# Default output directory
DEFAULT_OUTPUT_DIR = REPO_ROOT / "fantasy_football_data" / "player_data"

# Default cache directory
DEFAULT_CACHE_DIR = REPO_ROOT / "fantasy_football_data" / "cache" / "nflverse"

# Cache max age in hours (1 week)
CACHE_MAX_AGE_HOURS = 168

# ---------------------------------------------------------------------------
# nflverse → super_table team code normalization
# ---------------------------------------------------------------------------
# nflverse uses some team abbreviations that differ from our super_table convention.
# Apply this map BEFORE joining to the super_table (in update_super_table).
# Also imported by scripts/backfill_super_table_dst_columns.py.
#
# Super_table stores HISTORICALLY ACCURATE codes per year — e.g., SDG for
# San Diego Chargers pre-2017, LAC after; OAK for Oakland Raiders pre-2020,
# LV after; STL for St. Louis Rams pre-2016, LAR after. Most team codes
# match between nflverse and super_table. The entries below cover known
# divergences.
NFLVERSE_TO_SUPER_TABLE_TEAM_MAP = {
    # Current teams where nflverse uses a different abbreviation
    "LA": "LAR",  # nflverse "LA" → super_table "LAR" (current Los Angeles Rams)
    "SD": "SDG",  # nflverse "SD" → super_table "SDG" (historical San Diego Chargers, pre-2017)
    # Identity mappings below are intentional no-ops — documented for clarity
    # so future maintainers don't re-add wrong entries:
    #   OAK → OAK  (Raiders pre-2020; both sources use OAK)
    #   LV  → LV   (Raiders post-2020; both sources use LV)
    #   STL → STL  (Rams pre-2016; both sources use STL)
    #   LAC → LAC  (Chargers post-2017; both sources use LAC)
    #   LAR → LAR  (Rams post-2016; super_table always uses LAR, nflverse
    #              sometimes uses LA — that's why the LA mapping above exists)
}


def retry_with_backoff(func, max_retries=3, initial_delay=1.0, backoff_factor=2.0):
    """
    Retry a function with exponential backoff for transient failures.

    Args:
        func: Function to retry (should take no arguments)
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay after each retry

    Returns:
        Result of successful function call

    Raises:
        Last exception if all retries fail
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e

            if attempt < max_retries:
                error_str = str(e).lower()

                # Check if error is retryable
                is_retryable = (
                    "rate limit" in error_str
                    or "429" in error_str
                    or ("5" in error_str[:3] if len(error_str) >= 3 else False)
                    or "timeout" in error_str
                    or "connection" in error_str
                )

                if is_retryable:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                    logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                    delay *= backoff_factor
                    continue

            # Non-retryable error or final attempt, re-raise
            raise

    # Should never reach here, but just in case
    raise last_exception


def fetch_nflverse_team_stats(year: int, cache_dir: Path = None, use_cache: bool = True) -> pd.DataFrame:
    """
    Fetch team defensive stats from NFLverse for a given year.

    IMPORTANT: For the CURRENT year, cache expires more frequently (24 hours) to ensure
    we get updated data as games complete each week. For past years, cache lasts 7 days.

    Args:
        year: NFL season year (e.g., 2014)
        cache_dir: Directory to store cached downloads
        use_cache: Whether to use cached data if available

    Returns:
        DataFrame with team defensive stats
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"nflverse_team_stats_{year}.parquet"

    # Check cache first
    if use_cache and cache_file.exists():
        current_year = get_current_nfl_season_year()

        # For current year: use shorter cache expiry (24 hours) to get fresh data as games complete
        # For past years: use longer cache expiry (168 hours = 7 days)
        max_cache_age = 24 if year == current_year else CACHE_MAX_AGE_HOURS

        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < max_cache_age:
            logger.info(f"Using cached team stats for {year} (age: {age_hours:.1f} hours, max: {max_cache_age}h)")
            return pd.read_parquet(cache_file)
        else:
            logger.info(f"Cache expired for {year} (age: {age_hours:.1f}h > max: {max_cache_age}h), re-downloading")

    # Download from NFLverse
    url = f"https://github.com/nflverse/nflverse-data/releases/download/stats_team/stats_team_week_{year}.parquet"

    logger.info(f"Downloading {year} team stats from NFLverse...")
    logger.debug(f"URL: {url}")

    import tempfile

    def download():
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ValueError(
                    f"NFLverse team stats not available for {year} (404 Not Found). Data may not exist for this year."
                )
            else:
                raise

        # Use chunked download to avoid memory spike
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    tmp.write(chunk)
            tmp_path = tmp.name

        try:
            df = pd.read_parquet(tmp_path)

            # Validate downloaded data
            if df.empty:
                raise ValueError(f"Downloaded team stats for {year} is empty")
            if "team" not in df.columns or "opponent_team" not in df.columns:
                raise ValueError("Downloaded data missing required columns")

            # Save to cache
            df.to_parquet(cache_file)
            logger.info(f"Downloaded {len(df):,} rows of team stats for {year}")
            logger.debug(f"Cached to: {cache_file}")

            return df
        finally:
            # Clean up temp file
            Path(tmp_path).unlink(missing_ok=True)

    # Retry download with exponential backoff
    return retry_with_backoff(download, max_retries=3, initial_delay=1.0)


def fetch_nflverse_pbp_data(year: int, cache_dir: Path = None, use_cache: bool = True) -> pd.DataFrame:
    """
    Fetch play-by-play data from NFLverse for calculating three_out and fourth_down_stop.

    IMPORTANT: For the CURRENT year, cache expires more frequently (24 hours) to ensure
    we get updated data as games complete each week. For past years, cache lasts 7 days.

    Args:
        year: NFL season year (e.g., 2014)
        cache_dir: Directory to store cached downloads
        use_cache: Whether to use cached data if available

    Returns:
        DataFrame with play-by-play data
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"nflverse_pbp_{year}.parquet"

    # Check cache first
    if use_cache and cache_file.exists():
        current_year = get_current_nfl_season_year()

        # For current year: use shorter cache expiry (24 hours) to get fresh data as games complete
        # For past years: use longer cache expiry (168 hours = 7 days)
        max_cache_age = 24 if year == current_year else CACHE_MAX_AGE_HOURS

        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < max_cache_age:
            logger.info(
                f"Using cached play-by-play data for {year} (age: {age_hours:.1f} hours, max: {max_cache_age}h)"
            )
            return pd.read_parquet(cache_file)
        else:
            logger.info(f"Cache expired for {year} (age: {age_hours:.1f}h > max: {max_cache_age}h), re-downloading")

    # Download from NFLverse
    url = f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet"

    logger.info(f"Downloading {year} play-by-play data from NFLverse...")
    logger.debug(f"URL: {url}")

    import tempfile

    def download():
        response = requests.get(url, stream=True, timeout=120)  # Longer timeout for large file
        response.raise_for_status()

        # Use chunked download to avoid memory spike (files can be 40-200 MB)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            total_bytes = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    tmp.write(chunk)
                    total_bytes += len(chunk)

                    # Print progress every 10 MB
                    if total_bytes % (10 * 1024 * 1024) < 8192:
                        logger.debug(f"Downloaded {total_bytes / (1024 * 1024):.1f} MB...")

            tmp_path = tmp.name

        try:
            logger.info(f"Total downloaded: {total_bytes / (1024 * 1024):.1f} MB")
            logger.debug("Reading parquet file...")
            df = pd.read_parquet(tmp_path)

            # Validate downloaded data
            if df.empty:
                raise ValueError(f"Downloaded play-by-play data for {year} is empty")
            if "play_type" not in df.columns or "defteam" not in df.columns:
                raise ValueError("Downloaded data missing required columns")

            # Save to cache
            logger.debug("Caching play-by-play data...")
            df.to_parquet(cache_file)
            logger.info(f"Downloaded {len(df):,} rows of play-by-play data for {year}")
            logger.debug(f"Cached to: {cache_file}")

            return df
        finally:
            # Clean up temp file
            Path(tmp_path).unlink(missing_ok=True)

    # Retry download with exponential backoff
    return retry_with_backoff(download, max_retries=3, initial_delay=2.0)


def calculate_three_outs(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate three-and-out stats from play-by-play data.

    A three-and-out occurs when:
    - drive_first_downs == 0 AND play_type_nfl == 'PUNT'

    Args:
        pbp_df: Play-by-play DataFrame from NFLverse

    Returns:
        DataFrame with three_out counts by defteam, week, season
    """
    logger.info(f"Calculating three-and-outs from {len(pbp_df):,} plays...")

    # Filter to plays that meet three-and-out criteria
    three_out_plays = pbp_df[(pbp_df["drive_first_downs"] == 0) & (pbp_df["play_type_nfl"] == "PUNT")].copy()

    logger.debug(f"Found {len(three_out_plays):,} three-and-out plays")

    # Group by defensive team, week, and season
    three_out_stats = (
        three_out_plays.groupby(["defteam", "week", "season"], dropna=False).size().reset_index(name="three_out")
    )

    logger.debug(f"Calculated three-and-outs for {len(three_out_stats):,} team-week combinations")

    return three_out_stats


def calculate_fourth_down_stops(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate fourth down stop stats from play-by-play data.

    A fourth down stop occurs when:
    - down == 4 AND play_type in ['pass', 'run'] AND first_down == 0

    Args:
        pbp_df: Play-by-play DataFrame from NFLverse

    Returns:
        DataFrame with fourth_down_stop counts by defteam, week, season
    """
    logger.info(f"Calculating fourth down stops from {len(pbp_df):,} plays...")

    # Filter to plays that meet fourth down stop criteria
    fourth_down_stop_plays = pbp_df[
        (pbp_df["down"] == 4) & (pbp_df["play_type"].isin(["pass", "run"])) & (pbp_df["first_down"] == 0)
    ].copy()

    logger.debug(f"Found {len(fourth_down_stop_plays):,} fourth down stop plays")

    # Group by defensive team, week, and season
    fourth_down_stop_stats = (
        fourth_down_stop_plays.groupby(["defteam", "week", "season"], dropna=False)
        .size()
        .reset_index(name="fourth_down_stop")
    )

    logger.debug(f"Calculated fourth down stops for {len(fourth_down_stop_stats):,} team-week combinations")

    return fourth_down_stop_stats


def transform_to_defensive_stats(
    df: pd.DataFrame, three_out_stats: pd.DataFrame = None, fourth_down_stop_stats: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Transform team stats to defensive-oriented format for DST fantasy scoring.

    CRITICAL TRANSFORMATION:
    - Team A's defensive stats (sacks, INTs) = what Team A's defense did
    - Team A's points/yards ALLOWED = what Team B's offense did against Team A

    This requires a SELF-JOIN to match each team's defense with opponent's offense.

    Args:
        df: Raw team stats from NFLverse
        three_out_stats: Optional DataFrame with three_out counts
        fourth_down_stop_stats: Optional DataFrame with fourth_down_stop counts

    Returns:
        DataFrame transformed for defensive fantasy scoring
    """
    # ==========================================================================
    # CRITICAL FIX: Use franchise_id for self-join instead of team abbreviations
    # ==========================================================================
    # NFLverse data has inconsistent team abbreviations for relocated franchises:
    # - Some years use historical abbrevs (OAK, SD, STL)
    # - Some opponent_team columns use modern abbrevs (LV, LAC, LAR)
    # - This causes the self-join to fail, resulting in NULL defensive stats
    #
    # Solution: Use franchise_id (stable across relocations) as the join key.
    # This preserves historical abbreviations while ensuring correct joins:
    # - STL (1995-2015) → franchise_id = 14 (Rams)
    # - LAR (2016+) → franchise_id = 14 (Rams)
    # - OAK (1960-2019) → franchise_id = 31 (Raiders)
    # - LV (2020+) → franchise_id = 31 (Raiders)
    # ==========================================================================

    df = df.copy()
    year_col = "season" if "season" in df.columns else "year"

    # Define helper function for franchise_id lookup (used for self-join and PBP merges)
    def safe_get_franchise_id(team, year):
        """Get franchise_id with fallback for unknown teams."""
        if pd.isna(team) or pd.isna(year):
            return None
        if FRANCHISE_FUNCTIONS_AVAILABLE:
            from nfl_data.nfl_franchises import get_franchise_id

            fid = get_franchise_id(str(team), int(year))
            return fid
        return None  # Fallback: no franchise_id available

    # Add franchise_id for both team and opponent using the franchise mapping system
    # This handles historical relocations correctly (STL→14, LAR→14, etc.)
    if FRANCHISE_FUNCTIONS_AVAILABLE:
        df["team_franchise_id"] = df.apply(lambda row: safe_get_franchise_id(row["team"], row[year_col]), axis=1)
        df["opponent_franchise_id"] = df.apply(
            lambda row: safe_get_franchise_id(row["opponent_team"], row[year_col]), axis=1
        )
        logger.debug(f"Added franchise_id columns for {len(df):,} rows")

        # Log any rows with missing franchise_id (indicates unmapped teams)
        missing_team_fid = df["team_franchise_id"].isna().sum()
        missing_opp_fid = df["opponent_franchise_id"].isna().sum()
        if missing_team_fid > 0:
            unmapped_teams = df[df["team_franchise_id"].isna()]["team"].unique()
            logger.warning(f"{missing_team_fid} rows missing team_franchise_id: {list(unmapped_teams)[:5]}")
        if missing_opp_fid > 0:
            unmapped_opps = df[df["opponent_franchise_id"].isna()]["opponent_team"].unique()
            logger.warning(f"{missing_opp_fid} rows missing opponent_franchise_id: {list(unmapped_opps)[:5]}")
    else:
        # Fallback: use team abbreviation directly (may fail for relocated teams)
        logger.warning(
            "Franchise functions not available, using team abbreviations for join (may have issues with relocated teams)"
        )
        df["team_franchise_id"] = df["team"]
        df["opponent_franchise_id"] = df["opponent_team"]

    # Map team abbreviations to full names (for Yahoo compatibility)
    # Yahoo uses full team names including city + mascot for LA and NY teams
    # Standardized abbreviations: LAR (Rams), LAC (Chargers), LV (Raiders)
    # Historical team relocations map to CURRENT team names
    TEAM_NAMES = {
        "ARI": "Arizona",
        "ATL": "Atlanta",
        "BAL": "Baltimore",
        "BUF": "Buffalo",
        "CAR": "Carolina",
        "CHI": "Chicago",
        "CIN": "Cincinnati",
        "CLE": "Cleveland",
        "DAL": "Dallas",
        "DEN": "Denver",
        "DET": "Detroit",
        "GB": "Green Bay",
        "HOU": "Houston",
        "IND": "Indianapolis",
        "JAX": "Jacksonville",
        "JAC": "Jacksonville",
        "KC": "Kansas City",
        "LAC": "Los Angeles Chargers",
        "LAR": "Los Angeles Rams",
        "LV": "Las Vegas",
        "MIA": "Miami",
        "MIN": "Minnesota",
        "NE": "New England",
        "NO": "New Orleans",
        "NYG": "New York Giants",
        "NYJ": "New York Jets",
        "PHI": "Philadelphia",
        "PIT": "Pittsburgh",
        "SEA": "Seattle",
        "SF": "San Francisco",
        "TB": "Tampa Bay",
        "TEN": "Tennessee",
        "WAS": "Washington",
        "WSH": "Washington",
        # Historical team relocations → Map to current names
        "STL": "Los Angeles Rams",  # St. Louis Rams (1995-2015) → LAR (2016+)
        "SD": "Los Angeles Chargers",  # San Diego Chargers (until 2016) → LAC (2017+)
        "OAK": "Las Vegas",  # Oakland Raiders (until 2019) → LV (2020+)
    }

    # Map team abbreviations to logo URLs (to match offense headshot_url column)
    # Uses Wikipedia logos for consistency and availability
    # Historical teams map to CURRENT logo (maintains data consistency)
    TEAM_LOGO_MAP = {
        "ARI": "https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Arizona_Cardinals_logo.svg/179px-Arizona_Cardinals_logo.svg.png",
        "ATL": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c5/Atlanta_Falcons_logo.svg/192px-Atlanta_Falcons_logo.svg.png",
        "BAL": "https://upload.wikimedia.org/wikipedia/en/thumb/1/16/Baltimore_Ravens_logo.svg/193px-Baltimore_Ravens_logo.svg.png",
        "BUF": "https://upload.wikimedia.org/wikipedia/en/thumb/7/77/Buffalo_Bills_logo.svg/189px-Buffalo_Bills_logo.svg.png",
        "CAR": "https://upload.wikimedia.org/wikipedia/en/thumb/1/1c/Carolina_Panthers_logo.svg/100px-Carolina_Panthers_logo.svg.png",
        "CHI": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Chicago_Bears_logo.svg/100px-Chicago_Bears_logo.svg.png",
        "CIN": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Cincinnati_Bengals_logo.svg/100px-Cincinnati_Bengals_logo.svg.png",
        "CLE": "https://upload.wikimedia.org/wikipedia/en/thumb/d/d9/Cleveland_Browns_logo.svg/100px-Cleveland_Browns_logo.svg.png",
        "DAL": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/15/Dallas_Cowboys.svg/100px-Dallas_Cowboys.svg.png",
        "DEN": "https://upload.wikimedia.org/wikipedia/en/thumb/4/44/Denver_Broncos_logo.svg/100px-Denver_Broncos_logo.svg.png",
        "DET": "https://upload.wikimedia.org/wikipedia/en/thumb/7/71/Detroit_Lions_logo.svg/100px-Detroit_Lions_logo.svg.png",
        "GB": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/Green_Bay_Packers_logo.svg/100px-Green_Bay_Packers_logo.svg.png",
        "HOU": "https://upload.wikimedia.org/wikipedia/en/thumb/2/28/Houston_Texans_logo.svg/100px-Houston_Texans_logo.svg.png",
        "IND": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Indianapolis_Colts_logo.svg/100px-Indianapolis_Colts_logo.svg.png",
        "JAX": "https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/100px-Jacksonville_Jaguars_logo.svg.png",
        "JAC": "https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/100px-Jacksonville_Jaguars_logo.svg.png",  # Alternate abbreviation
        "KC": "https://upload.wikimedia.org/wikipedia/en/thumb/e/e1/Kansas_City_Chiefs_logo.svg/100px-Kansas_City_Chiefs_logo.svg.png",
        "LAC": "https://upload.wikimedia.org/wikipedia/en/thumb/7/72/NFL_Chargers_logo.svg/100px-NFL_Chargers_logo.svg.png",
        "LAR": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/100px-Los_Angeles_Rams_logo.svg.png",
        "MIA": "https://upload.wikimedia.org/wikipedia/en/thumb/3/37/Miami_Dolphins_logo.svg/100px-Miami_Dolphins_logo.svg.png",
        "MIN": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Minnesota_Vikings_logo.svg/98px-Minnesota_Vikings_logo.svg.png",
        "NE": "https://upload.wikimedia.org/wikipedia/en/thumb/b/b9/New_England_Patriots_logo.svg/100px-New_England_Patriots_logo.svg.png",
        "NO": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/New_Orleans_Saints_logo.svg/98px-New_Orleans_Saints_logo.svg.png",
        "NYG": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/New_York_Giants_logo.svg/100px-New_York_Giants_logo.svg.png",
        "NYJ": "https://upload.wikimedia.org/wikipedia/en/thumb/6/6b/New_York_Jets_logo.svg/100px-New_York_Jets_logo.svg.png",
        "LV": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/150px-Las_Vegas_Raiders_logo.svg.png",
        "PHI": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Philadelphia_Eagles_logo.svg/100px-Philadelphia_Eagles_logo.svg.png",
        "PIT": "https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/Pittsburgh_Steelers_logo.svg/100px-Pittsburgh_Steelers_logo.svg.png",
        "SF": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/San_Francisco_49ers_logo.svg/100px-San_Francisco_49ers_logo.svg.png",
        "SEA": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Seattle_Seahawks_logo.svg/100px-Seattle_Seahawks_logo.svg.png",
        "TB": "https://upload.wikimedia.org/wikipedia/en/thumb/a/a2/Tampa_Bay_Buccaneers_logo.svg/100px-Tampa_Bay_Buccaneers_logo.svg.png",
        "TEN": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c1/Tennessee_Titans_logo.svg/100px-Tennessee_Titans_logo.svg.png",
        "WAS": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/72/Washington_football_team_wlogo.svg/1024px-Washington_football_team_wlogo.svg.png",
        "WSH": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/72/Washington_football_team_wlogo.svg/1024px-Washington_football_team_wlogo.svg.png",  # Alternate abbreviation
        # Historical team relocations → Map to current logo
        "STL": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/100px-Los_Angeles_Rams_logo.svg.png",  # St. Louis → LAR
        "SD": "https://upload.wikimedia.org/wikipedia/en/thumb/7/72/NFL_Chargers_logo.svg/100px-NFL_Chargers_logo.svg.png",  # San Diego → LAC
        "OAK": "https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/150px-Las_Vegas_Raiders_logo.svg.png",  # Oakland → LV
    }

    logger.info(f"Transforming {len(df):,} rows to defensive format...")

    # Step 1: Create offense dataset (what each team's offense did)
    offense = df.copy()
    offense = offense.rename(
        columns={
            "team": "offense_team",
            "opponent_team": "defense_team",
            "team_franchise_id": "offense_franchise_id",
            "opponent_franchise_id": "defense_franchise_id",
            "season": "year",
        }
    )

    # Step 2: Create defense dataset (what each team's defense did)
    defense = df.copy()
    defense = defense.rename(
        columns={
            "team": "defense_team",
            "opponent_team": "offense_team",
            "team_franchise_id": "defense_franchise_id",
            "opponent_franchise_id": "offense_franchise_id",
            "season": "year",
        }
    )

    # CRITICAL: Fill NULL season_type values before join
    # Historical NFLverse data (1999-2000) may have NULL season_type, causing
    # the self-join to fail (NULL != NULL in pandas). Default to 'REG' for
    # regular season as that's the most common case.
    if "season_type" in offense.columns:
        offense["season_type"] = offense["season_type"].fillna("REG")
    if "season_type" in defense.columns:
        defense["season_type"] = defense["season_type"].fillna("REG")

    # Step 3: Self-join to match each team's defense with opponent's offense
    # CRITICAL: Join on franchise_id instead of team abbreviation!
    # This handles relocated teams correctly (STL vs LAR both map to franchise_id=14)
    # Join condition: defense's opponent (offense_franchise_id) = offense's team (offense_franchise_id)
    # Ensure fg_blocked column exists in offense (may be missing in very old nflverse data)
    if "fg_blocked" not in offense.columns:
        offense["fg_blocked"] = 0
    # The opponent's NON-offensive scoring (defensive-return TDs = pick-6/fumble-6, ST-return TDs,
    # safeties) is required to reconstruct the FULL points allowed, not just their offense. Guard for
    # very old nflverse data that may lack these columns.
    for _c in ("def_tds", "special_teams_tds", "def_safeties"):
        if _c not in offense.columns:
            offense[_c] = 0

    merged = defense.merge(
        offense[
            [
                "year",
                "week",
                "season_type",
                "offense_franchise_id",
                "defense_franchise_id",
                "passing_yards",
                "rushing_yards",
                "passing_tds",
                "rushing_tds",
                "receiving_tds",
                "fg_made",
                "fg_blocked",
                "pat_made",
                "passing_2pt_conversions",
                "rushing_2pt_conversions",
                "receiving_2pt_conversions",
                "def_tds",
                "special_teams_tds",
                "def_safeties",
            ]
        ],
        left_on=["year", "week", "season_type", "defense_franchise_id", "offense_franchise_id"],
        right_on=["year", "week", "season_type", "defense_franchise_id", "offense_franchise_id"],
        how="left",
        suffixes=("", "_allowed"),
    )

    # Validate self-join merge
    missing_opponent_data = merged["passing_yards_allowed"].isna()
    if missing_opponent_data.any():
        logger.warning(f"{missing_opponent_data.sum()} rows missing opponent offensive data")
        affected_teams = merged[missing_opponent_data]["defense_team"].unique()
        logger.warning(f"Affected teams: {', '.join(str(t) for t in affected_teams)}")

    # Step 4: Build defensive DataFrame with proper schema
    defensive_df = pd.DataFrame()

    # Preserve HISTORICAL team abbreviations (STL, SD, OAK) - don't normalize to current
    defense_team = merged["defense_team"]
    offense_team = merged["offense_team"]

    # Basic identifiers - use historical abbreviations as-is
    defensive_df["nfl_team"] = defense_team
    defensive_df["opponent_nfl_team"] = offense_team
    defensive_df["year"] = merged["year"]
    defensive_df["week"] = merged["week"]
    defensive_df["season_type"] = merged["season_type"]
    # Include franchise_id for merging with PBP stats (three_out, fourth_down_stop)
    defensive_df["defense_franchise_id"] = merged["defense_franchise_id"]

    # Position
    defensive_df["nfl_position"] = "DEF"
    defensive_df["fantasy_position"] = "DEF"

    # Map team abbreviations to display names
    # Use franchise-based "Nickname DST" format (e.g., "Ravens DST", "Cardinals DST")
    if FRANCHISE_FUNCTIONS_AVAILABLE:
        # Use franchise-based display name (handles historical relocations)
        defensive_df["player"] = defensive_df.apply(
            lambda row: get_def_display_name(row["nfl_team"], row["year"])
            if pd.notna(row["nfl_team"]) and pd.notna(row["year"])
            else f"{row['nfl_team']} DST"
            if pd.notna(row["nfl_team"])
            else "Unknown DST",
            axis=1,
        )
    else:
        # Fallback to old TEAM_NAMES mapping if franchise functions not available
        defensive_df["player"] = defense_team.map(TEAM_NAMES).fillna(defense_team)

    # Map team abbreviations to logo URLs (to match offense headshot_url column)
    defensive_df["headshot_url"] = defense_team.map(TEAM_LOGO_MAP)

    # Check for unmapped teams (filter out None/NaN values)
    unmapped_names = defense_team[~defense_team.isin(TEAM_NAMES)].unique()
    unmapped_names = [str(x) for x in unmapped_names if pd.notna(x)]

    unmapped_logos = defense_team[~defense_team.isin(TEAM_LOGO_MAP)].unique()
    unmapped_logos = [str(x) for x in unmapped_logos if pd.notna(x)]

    if len(unmapped_names) > 0:
        logger.warning(f"Unmapped team names: {', '.join(unmapped_names)}")
    if len(unmapped_logos) > 0:
        logger.warning(f"Unmapped team logos: {', '.join(unmapped_logos)}")

    logger.debug(f"Mapped team abbreviations to full names for {defensive_df['player'].notna().sum()} rows")
    logger.debug(f"Mapped team logos (headshot_url) for {defensive_df['headshot_url'].notna().sum()} rows")

    # Defensive stats (from defense row - what this team's defense DID)
    defensive_df["def_sacks"] = merged["def_sacks"]
    defensive_df["def_sack_yards"] = merged["def_sack_yards"]
    defensive_df["def_qb_hits"] = merged["def_qb_hits"]
    defensive_df["def_interceptions"] = merged["def_interceptions"]
    defensive_df["def_interception_yards"] = merged["def_interception_yards"]
    defensive_df["def_pass_defended"] = merged["def_pass_defended"]
    defensive_df["def_tackles_solo"] = merged["def_tackles_solo"]
    defensive_df["def_tackles_with_assist"] = merged["def_tackles_with_assist"]
    defensive_df["def_tackle_assists"] = merged["def_tackle_assists"]
    defensive_df["def_tackles_for_loss"] = merged["def_tackles_for_loss"]
    defensive_df["def_tackles_for_loss_yards"] = merged["def_tackles_for_loss_yards"]
    defensive_df["def_fumbles_forced"] = merged["def_fumbles_forced"]
    defensive_df["def_tds"] = merged["def_tds"]
    defensive_df["def_fumbles"] = merged["def_fumbles"]
    defensive_df["def_safeties"] = merged["def_safeties"]
    defensive_df["special_teams_tds"] = merged["special_teams_tds"]
    defensive_df["fum_rec"] = merged["fumble_recovery_opp"]
    defensive_df["fum_ret_td"] = merged["fumble_recovery_tds"]

    # Blocked FGs: opponent's kicker had FG blocked = this DEF blocked a kick
    # fg_blocked_allowed comes from the self-join (opponent offense side)
    defensive_df["fg_blocked"] = merged["fg_blocked_allowed"].fillna(0).astype(int)

    # Points/Yards ALLOWED (from opponent's offense - what opponent DID against this defense)
    defensive_df["passing_yds_allowed"] = merged["passing_yards_allowed"]
    defensive_df["rushing_yds_allowed"] = merged["rushing_yards_allowed"]
    # Yahoo DST "yards allowed" follows net offensive yards, which excludes the
    # sack yardage the defense generated. NFLverse team offense totals do not
    # line up with that convention directly, so subtract sack yards from the
    # opponent's pass+rush total to match Yahoo's bucketing.
    defensive_df["total_yds_allowed"] = (
        merged["passing_yards_allowed"].fillna(0)
        + merged["rushing_yards_allowed"].fillna(0)
        - merged["def_sack_yards"].fillna(0)
    ).clip(lower=0)

    # Calculate total points allowed
    defensive_df["passing_tds_allowed"] = merged["passing_tds_allowed"]
    defensive_df["rushing_tds_allowed"] = merged["rushing_tds_allowed"]
    defensive_df["receiving_tds_allowed"] = merged["receiving_tds_allowed"]

    # Total touchdowns allowed
    # NOTE: Only count rushing_tds + receiving_tds, NOT passing_tds
    # passing_tds and receiving_tds represent the SAME touchdown from different perspectives
    # (QB throws TD = passing_td, receiver catches TD = receiving_td)
    # Including both would double-count all passing touchdowns
    total_tds_allowed = merged["rushing_tds_allowed"].fillna(0) + merged["receiving_tds_allowed"].fillna(0)

    # 2-point conversions allowed
    two_pt_allowed = (
        merged["passing_2pt_conversions_allowed"].fillna(0)
        + merged["rushing_2pt_conversions_allowed"].fillna(0)
        + merged["receiving_2pt_conversions_allowed"].fillna(0)
    )

    # Field goals and PATs allowed
    fg_allowed = merged["fg_made_allowed"].fillna(0)
    pat_allowed = merged["pat_made_allowed"].fillna(0)

    # TOTAL points allowed = the opponent's FULL score, not just their offense. Reconstruct from every
    # way they scored: offensive TDs (rush+rec; passing already counted via rec) + defensive-return TDs
    # (pick-6 / fumble-6 off THIS team's offense) + ST-return TDs + FGs + PATs (pat_made covers all TD
    # types) + 2pt + safeties. This replaces the old offensive-only reconstruction that under-counted
    # any game with a return TD or safety. (Ideal end-state is anchoring to the official game score;
    # build_pa_reconciled_v26 does exactly that for the historical super table via nfl_team_games_all.)
    opp_def_ret_tds = merged.get("def_tds_allowed", 0)
    opp_def_ret_tds = opp_def_ret_tds.fillna(0) if hasattr(opp_def_ret_tds, "fillna") else 0
    opp_st_tds = merged.get("special_teams_tds_allowed", 0)
    opp_st_tds = opp_st_tds.fillna(0) if hasattr(opp_st_tds, "fillna") else 0
    opp_safeties_scored = merged.get("def_safeties_allowed", 0)
    opp_safeties_scored = opp_safeties_scored.fillna(0) if hasattr(opp_safeties_scored, "fillna") else 0

    all_tds_allowed = total_tds_allowed + opp_def_ret_tds + opp_st_tds
    defensive_df["points_allowed"] = (
        all_tds_allowed * 6 + two_pt_allowed * 2 + fg_allowed * 3 + pat_allowed * 1 + opp_safeties_scored * 2
    )
    # Fantasy-ELIGIBLE PA: exclude points the opponent scored off OUR turnovers (pick-6 / fumble-6) and
    # safeties our offense conceded; keep PATs / 2pt / FG / ST-return-TDs. Matches wave4c / the recon.
    defensive_df["dst_points_allowed"] = (
        defensive_df["points_allowed"] - opp_def_ret_tds * 6 - opp_safeties_scored * 2
    ).clip(lower=0)
    # pts_allow drives the points-allowed tier buckets, which must score off the ELIGIBLE value.
    defensive_df["pts_allow"] = defensive_df["dst_points_allowed"]

    # Other stats
    defensive_df["misc_yards"] = merged["misc_yards"]
    defensive_df["penalties"] = merged["penalties"]
    defensive_df["penalty_yards"] = merged["penalty_yards"]
    defensive_df["timeouts"] = merged["timeouts"]

    logger.info(f"Transformation complete: {len(defensive_df):,} rows")
    logger.debug(
        f"Sample points allowed: min={defensive_df['pts_allow'].min()}, max={defensive_df['pts_allow'].max()}, mean={defensive_df['pts_allow'].mean():.1f}"
    )

    # Merge three-and-out stats if provided
    if three_out_stats is not None:
        logger.debug("Merging three-and-out stats...")
        three_out_stats = three_out_stats.copy()
        three_out_stats = three_out_stats.rename(columns={"defteam": "nfl_team", "season": "year"})
        # Add franchise_id for merging (handles relocated teams correctly)
        if FRANCHISE_FUNCTIONS_AVAILABLE and "nfl_team" in three_out_stats.columns:
            three_out_stats["defense_franchise_id"] = three_out_stats.apply(
                lambda row: safe_get_franchise_id(row["nfl_team"], row["year"])
                if "year" in row and pd.notna(row.get("year"))
                else None,
                axis=1,
            )
            # Merge on franchise_id instead of team abbreviation
            defensive_df = defensive_df.merge(
                three_out_stats[["defense_franchise_id", "week", "year", "three_out"]],
                on=["defense_franchise_id", "week", "year"],
                how="left",
            )
        else:
            # Fallback: merge on nfl_team directly (may fail for relocated teams)
            defensive_df = defensive_df.merge(
                three_out_stats[["nfl_team", "week", "year", "three_out"]], on=["nfl_team", "week", "year"], how="left"
            )
        defensive_df["three_out"] = defensive_df["three_out"].fillna(0).astype(int)
        logger.debug(f"Added three_out column (mean={defensive_df['three_out'].mean():.1f})")

    # Merge fourth down stop stats if provided
    if fourth_down_stop_stats is not None:
        logger.debug("Merging fourth down stop stats...")
        fourth_down_stop_stats = fourth_down_stop_stats.copy()
        fourth_down_stop_stats = fourth_down_stop_stats.rename(columns={"defteam": "nfl_team", "season": "year"})
        # Add franchise_id for merging (handles relocated teams correctly)
        if FRANCHISE_FUNCTIONS_AVAILABLE and "nfl_team" in fourth_down_stop_stats.columns:
            fourth_down_stop_stats["defense_franchise_id"] = fourth_down_stop_stats.apply(
                lambda row: safe_get_franchise_id(row["nfl_team"], row["year"])
                if "year" in row and pd.notna(row.get("year"))
                else None,
                axis=1,
            )
            # Merge on franchise_id instead of team abbreviation
            defensive_df = defensive_df.merge(
                fourth_down_stop_stats[["defense_franchise_id", "week", "year", "fourth_down_stop"]],
                on=["defense_franchise_id", "week", "year"],
                how="left",
            )
        else:
            # Fallback: merge on nfl_team directly (may fail for relocated teams)
            defensive_df = defensive_df.merge(
                fourth_down_stop_stats[["nfl_team", "week", "year", "fourth_down_stop"]],
                on=["nfl_team", "week", "year"],
                how="left",
            )
        defensive_df["fourth_down_stop"] = defensive_df["fourth_down_stop"].fillna(0).astype(int)
        logger.debug(f"Added fourth_down_stop column (mean={defensive_df['fourth_down_stop'].mean():.1f})")

    # Apply JAX bug fix for 2001-2002 if applicable
    if "year" in defensive_df.columns:
        years_present = defensive_df["year"].unique()
        if 2001 in years_present or 2002 in years_present:
            defensive_df = fix_jax_defensive_stats_bug(defensive_df)

    # Remove bye week rows — the LEFT JOIN produces rows where the defense side
    # exists but the offense side doesn't (no opponent on bye week). These rows
    # have opponent_nfl_team = NaN and zero stats, inflating games_played counts
    # in downstream aggregation (e.g., player_nfl_season).
    if "opponent_nfl_team" in defensive_df.columns:
        bye_mask = defensive_df["opponent_nfl_team"].isna() | (
            defensive_df["opponent_nfl_team"].astype(str).str.strip() == ""
        )
        bye_count = bye_mask.sum()
        if bye_count > 0:
            logger.info(f"Removing {bye_count} DST bye week rows (no opponent)")
            defensive_df = defensive_df[~bye_mask].copy()

    return defensive_df


# =============================================================================
# JAX Bug Fix for 2001-2002 NFLverse Data
# =============================================================================
# Known bug: All defensive stats vs JAX in 2001-2002 are incorrect in NFLverse.
# This causes pts_allow=0 and inflated def_tds for games where opponent faced JAX.

JAX_GAME_SCORES = {
    # 2001 Season - JAX scored 294 total points
    (2001, 1, "PIT"): 21,
    (2001, 2, "TEN"): 13,
    (2001, 3, "CLE"): 14,
    (2001, 4, "SEA"): 24,
    (2001, 6, "BUF"): 10,
    (2001, 7, "BAL"): 13,
    (2001, 8, "TEN"): 16,
    (2001, 9, "CIN"): 14,
    (2001, 10, "PIT"): 17,
    (2001, 11, "BAL"): 10,
    (2001, 12, "GB"): 21,
    (2001, 13, "CIN"): 20,
    (2001, 14, "CLE"): 15,
    (2001, 15, "MIN"): 12,
    (2001, 16, "KC"): 20,
    (2001, 17, "CHI"): 33,
    # 2002 Season - JAX scored 328 total points
    (2002, 1, "IND"): 25,
    (2002, 2, "KC"): 23,
    (2002, 3, "TEN"): 13,
    (2002, 4, "NYJ"): 21,
    (2002, 5, "PHI"): 25,
    (2002, 6, "TEN"): 23,
    (2002, 7, "BAL"): 17,
    (2002, 8, "HOU"): 21,
    (2002, 9, "NYG"): 24,
    (2002, 10, "WAS"): 13,
    (2002, 11, "HOU"): 24,
    (2002, 12, "DAL"): 21,
    (2002, 13, "PIT"): 17,
    (2002, 14, "CLE"): 21,
    (2002, 15, "CIN"): 20,
    (2002, 16, "TEN"): 20,
    (2002, 17, "IND"): 20,
}


def fix_jax_defensive_stats_bug(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix known NFLverse bug where defensive stats vs JAX in 2001-2002 are incorrect.

    The bug causes:
    - pts_allow = 0 for all games vs JAX
    - def_tds to be inflated (6+ TDs in some games)

    This function corrects these values using verified game-by-game data.

    Args:
        df: Defensive stats DataFrame

    Returns:
        DataFrame with corrected JAX opponent games
    """
    if "opponent_nfl_team" not in df.columns:
        return df

    # Find rows where opponent is JAX in 2001-2002
    jax_mask = (df["opponent_nfl_team"] == "JAX") & (df["year"].isin([2001, 2002]))

    if jax_mask.sum() == 0:
        return df

    corrections = 0
    result = df.copy()

    for idx in result.index[jax_mask]:
        year = int(result.at[idx, "year"])
        week = int(result.at[idx, "week"])
        team = result.at[idx, "nfl_team"]
        key = (year, week, team)

        if key in JAX_GAME_SCORES:
            jax_pts = JAX_GAME_SCORES[key]

            # Fix pts_allow (points allowed = points JAX scored)
            if "pts_allow" in result.columns:
                result.at[idx, "pts_allow"] = float(jax_pts)
            if "dst_points_allowed" in result.columns:
                result.at[idx, "dst_points_allowed"] = float(jax_pts)
            if "points_allowed" in result.columns:
                result.at[idx, "points_allowed"] = float(jax_pts)

            # Reset def_tds to 0 (the bug inflated these)
            if "def_tds" in result.columns:
                result.at[idx, "def_tds"] = 0.0

            corrections += 1

    if corrections > 0:
        logger.info(f"JAX bug fix: corrected {corrections} games in 2001-2002")

    return result


def get_max_week_from_matchup_data(data_directory: Path, year: int) -> int | None:
    """
    Get the maximum week from matchup data files.

    This allows NFLverse defense data to align with matchup data (only fetch weeks with actual matchups).

    Args:
        data_directory: League data directory (e.g., .../fantasy_football_data/KMFFL)
        year: Year to check

    Returns:
        Maximum week number found in matchup data, or None if no matchup data exists
    """
    try:
        matchup_dir = data_directory / "matchup_data"

        if not matchup_dir.exists():
            logger.debug(f"Matchup directory not found: {matchup_dir}")
            return None

        # Try to find matchup file for this year
        # Prefer all-weeks file, fallback to individual week files
        all_weeks_file = matchup_dir / f"matchup_data_week_all_year_{year}.parquet"

        if all_weeks_file.exists():
            try:
                df = pd.read_parquet(all_weeks_file)
                if not df.empty and "week" in df.columns:
                    max_week = int(df["week"].max())
                    logger.debug(f"Found max week {max_week} from {all_weeks_file.name}")
                    return max_week
            except Exception as e:
                logger.warning(f"Error reading {all_weeks_file.name}: {e}")

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
                max_week = max(week_numbers)
                logger.debug(f"Found max week {max_week} from {len(week_files)} individual week files")
                return max_week

        logger.debug(f"No matchup data found for year {year}")
        return None

    except Exception as e:
        logger.warning(f"Error getting max week from matchup data: {e}")
        return None


def compute_advanced_def_columns(pbp: pd.DataFrame, team_stats: pd.DataFrame) -> pd.DataFrame:
    """Compute advanced pts_def_* columns from nflverse pbp + team stats.

    Used by both:
    - process_one_year() in this file (weekly refresh path)
    - scripts/backfill_super_table_dst_columns.py (one-time historical backfill, imports from here)

    Args:
        pbp: Play-by-play DataFrame from nflverse (play_by_play_{year}.parquet).
             Must have columns: season, week, defteam, posteam, play_type, touchdown,
             td_team, interception, fumble, fumble_recovery_1_team, fumble_forced,
             punt_blocked, field_goal_result, extra_point_result, two_point_attempt,
             defteam_score, defteam_score_post, return_yards, drive, punt_attempt.
        team_stats: Team-week stats from nflverse (stats_team_week_{year}.parquet).
             Must have columns: season, week, team, def_pass_defended, def_sack_yards,
             def_sacks, def_tackles_solo, def_tackle_assists.

    Returns:
        DataFrame keyed by (season, week, nfl_team) with all pts_def_* columns filled
        with 0 for missing rows.
    """
    # -------------------------------------------------------------------------
    # A. Columns derived from TEAM STATS (already aggregated per team-week)
    # -------------------------------------------------------------------------

    # Filter to regular + post season only (exclude preseason where week=0)
    ts = team_stats[team_stats["week"] > 0].copy()

    # A1. Pass defended
    pass_def = ts[["season", "week", "team", "def_pass_defended"]].copy()
    pass_def = pass_def.rename(columns={"team": "nfl_team", "def_pass_defended": "pts_def_pass_def"})

    # A2. Sack yards (already positive in team stats — no negation needed)
    sack_yd = ts[["season", "week", "team", "def_sack_yards"]].copy()
    sack_yd = sack_yd.rename(columns={"team": "nfl_team", "def_sack_yards": "pts_def_sack_yd"})

    # A3. Bonus: >= 2 sacks in game
    bonus_sack = ts[["season", "week", "team", "def_sacks"]].copy()
    bonus_sack["pts_def_bonus_sack_2p"] = (bonus_sack["def_sacks"] >= 2).astype(int)
    bonus_sack = bonus_sack[["season", "week", "team", "pts_def_bonus_sack_2p"]].rename(columns={"team": "nfl_team"})

    # A4. Bonus: >= 10 combined tackles in game
    bonus_tkl = ts[["season", "week", "team", "def_tackles_solo", "def_tackle_assists"]].copy()
    bonus_tkl["combined_tkl"] = bonus_tkl["def_tackles_solo"] + bonus_tkl["def_tackle_assists"]
    bonus_tkl["pts_def_bonus_tkl_10p"] = (bonus_tkl["combined_tkl"] >= 10).astype(int)
    bonus_tkl = bonus_tkl[["season", "week", "team", "pts_def_bonus_tkl_10p"]].rename(columns={"team": "nfl_team"})

    # -------------------------------------------------------------------------
    # B. Columns derived from PBP
    # -------------------------------------------------------------------------

    # B1. INT return TDs (interception AND touchdown, group by defteam = intercepting team)
    int_ret_tds = (
        pbp[(pbp["interception"] == 1) & (pbp["touchdown"] == 1)]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_int_ret_td")
        .rename(columns={"defteam": "nfl_team"})
    )

    # B2. Fumble return TDs by defense
    fum_ret_mask = (pbp["fumble"] == 1) & (pbp["touchdown"] == 1) & (pbp["fumble_recovery_1_team"] == pbp["defteam"])
    fum_ret_tds = (
        pbp[fum_ret_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_fum_ret_td")
        .rename(columns={"defteam": "nfl_team"})
    )
    # pts_def_fum_rec_td is the same computation (Sleeper uses both names as aliases)
    fum_rec_td = fum_ret_tds.rename(columns={"pts_def_fum_ret_td": "pts_def_fum_rec_td"}).copy()

    def _string_col(name: str, default: str = "") -> pd.Series:
        if name in pbp.columns:
            return pbp[name].fillna(default).astype(str)
        return pd.Series(default, index=pbp.index)

    def _numeric_col(name: str, default: float = 0.0) -> pd.Series:
        if name in pbp.columns:
            return pd.to_numeric(pbp[name], errors="coerce").fillna(default)
        return pd.Series(default, index=pbp.index)

    def _block_counts(mask: pd.Series, column_name: str) -> pd.DataFrame:
        if not mask.any():
            return pd.DataFrame(columns=["season", "week", "nfl_team", column_name])
        return (
            pbp[mask & pbp["defteam"].notna()]
            .groupby(["season", "week", "defteam"])
            .size()
            .reset_index(name=column_name)
            .rename(columns={"defteam": "nfl_team"})
        )

    punt_block_mask = _numeric_col("punt_blocked") == 1
    fg_block_mask = _string_col("field_goal_result").str.lower() == "blocked"
    pat_block_mask = _string_col("extra_point_result").str.lower() == "blocked"

    fg_blocks = _block_counts(fg_block_mask, "pts_def_fg_block")
    punt_blocks = _block_counts(punt_block_mask, "pts_def_punt_block")
    pat_blocks = _block_counts(pat_block_mask, "pts_def_pat_block")

    block_splits = fg_blocks
    for block_part in (punt_blocks, pat_blocks):
        block_splits = block_splits.merge(block_part, on=["season", "week", "nfl_team"], how="outer")
    for col in ("pts_def_fg_block", "pts_def_punt_block", "pts_def_pat_block"):
        if col not in block_splits.columns:
            block_splits[col] = 0
        block_splits[col] = block_splits[col].fillna(0).astype(int)
    block_splits["pts_def_block"] = (
        block_splits["pts_def_fg_block"] + block_splits["pts_def_punt_block"] + block_splits["pts_def_pat_block"]
    ).astype(int)

    # B3. Blocked kick TDs
    # nflverse does NOT have 'block_kick_play'; use punt/FG/PAT block signals.
    blk_kick_mask = (punt_block_mask | fg_block_mask | pat_block_mask) & (pbp["touchdown"] == 1)
    blk_kick_tds = (
        pbp[blk_kick_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_blk_kick_td")
        .rename(columns={"defteam": "nfl_team"})
    )

    # B4. Kickoff return TDs (by RETURNER team)
    # CONVENTION (verified): on kickoff plays, posteam = RETURNER, defteam = KICKER
    # td_team = posteam when returner scores → group by td_team
    kr_td_mask = (pbp["play_type"] == "kickoff") & (pbp["touchdown"] == 1)
    kr_tds = (
        pbp[kr_td_mask]
        .groupby(["season", "week", "td_team"])
        .size()
        .reset_index(name="pts_def_kr_td")
        .rename(columns={"td_team": "nfl_team"})
    )

    # B5. Punt return TDs (by RETURNER team)
    # CONVENTION (verified): on punt plays, defteam = RETURNER, td_team = defteam when returner scores
    pr_td_mask = (pbp["play_type"] == "punt") & (pbp["touchdown"] == 1) & (pbp["td_team"] == pbp["defteam"])
    pr_tds = (
        pbp[pr_td_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_pr_td")
        .rename(columns={"defteam": "nfl_team"})
    )

    # B6. Defensive 2-point conversion (defense returns failed 2pt attempt for 2pts)
    two_pt_mask = pbp["two_point_attempt"] == 1
    two_pt_plays = pbp[two_pt_mask].copy()
    two_pt_plays["def_score_change"] = two_pt_plays["defteam_score_post"] - two_pt_plays["defteam_score"]
    def_2pt = (
        two_pt_plays[two_pt_plays["def_score_change"] > 0]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_2pt")
        .rename(columns={"defteam": "nfl_team"})
    )

    # B7. Special teams forced fumbles (kickoff/punt plays where defteam forced the fumble)
    st_ff_mask = (pbp["play_type"].isin(["kickoff", "punt"])) & (pbp["fumble_forced"] == 1)
    st_ff = (
        pbp[st_ff_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_st_ff")
        .rename(columns={"defteam": "nfl_team"})
    )

    # B8. Special teams fumble recoveries (kickoff/punt plays, defteam recovers)
    st_fum_rec_mask = (
        (pbp["play_type"].isin(["kickoff", "punt"]))
        & (pbp["fumble"] == 1)
        & (pbp["fumble_recovery_1_team"] == pbp["defteam"])
    )
    st_fum_rec = (
        pbp[st_fum_rec_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="pts_def_st_fum_rec")
        .rename(columns={"defteam": "nfl_team"})
    )

    # B9. Bonus: any INT return TD >= 50 yards (binary flag per team-week)
    int_td_50_mask = (pbp["interception"] == 1) & (pbp["touchdown"] == 1) & (pbp["return_yards"] >= 50)
    int_td_50 = (
        pbp[int_td_50_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="_count")
        .rename(columns={"defteam": "nfl_team"})
    )
    int_td_50["pts_def_bonus_int_td_50p"] = (int_td_50["_count"] > 0).astype(int)
    int_td_50 = int_td_50.drop(columns=["_count"])

    # B10. Bonus: any fumble return TD >= 50 yards (binary flag per team-week)
    fum_td_50_mask = (
        (pbp["fumble"] == 1)
        & (pbp["touchdown"] == 1)
        & (pbp["fumble_recovery_1_team"] == pbp["defteam"])
        & (pbp["return_yards"] >= 50)
    )
    fum_td_50 = (
        pbp[fum_td_50_mask]
        .groupby(["season", "week", "defteam"])
        .size()
        .reset_index(name="_count")
        .rename(columns={"defteam": "nfl_team"})
    )
    fum_td_50["pts_def_bonus_fum_td_50p"] = (fum_td_50["_count"] > 0).astype(int)
    fum_td_50 = fum_td_50.drop(columns=["_count"])

    # B11. Forced punts (drives ending in punt attributed to the defense)
    try:
        pbp_valid_drives = pbp.dropna(subset=["drive"]).copy()
        drive_agg = (
            pbp_valid_drives.groupby(["season", "week", "defteam", "drive"], dropna=False)
            .agg(ended_in_punt=("punt_attempt", "max"))
            .reset_index()
        )
        forced_punts = (
            drive_agg.groupby(["season", "week", "defteam"])["ended_in_punt"]
            .sum()
            .reset_index(name="pts_def_forced_punts")
            .rename(columns={"defteam": "nfl_team"})
        )
        forced_punts["pts_def_forced_punts"] = forced_punts["pts_def_forced_punts"].astype(int)
    except Exception as exc:
        logger.warning(f"[advanced_def] forced_punts derivation failed: {exc}. Producing zeros.")
        forced_punts = pd.DataFrame(columns=["season", "week", "nfl_team", "pts_def_forced_punts"])

    # -------------------------------------------------------------------------
    # C. Build combined ST TD (KR + PR)
    # -------------------------------------------------------------------------
    st_td = kr_tds.merge(pr_tds, on=["season", "week", "nfl_team"], how="outer").fillna(
        {"pts_def_kr_td": 0, "pts_def_pr_td": 0}
    )
    st_td["pts_def_st_td"] = (st_td["pts_def_kr_td"] + st_td["pts_def_pr_td"]).astype(int)
    st_td_combined = st_td[["season", "week", "nfl_team", "pts_def_st_td"]]

    # -------------------------------------------------------------------------
    # D. Outer-join all per-team-week DataFrames into one result
    # -------------------------------------------------------------------------
    # Start with the full team-week grid from team stats (covers all 32 teams × all weeks)
    base = pass_def[["season", "week", "nfl_team"]].copy()

    join_key = ["season", "week", "nfl_team"]

    result = base
    for df_part in [
        pass_def,
        sack_yd,
        bonus_sack,
        bonus_tkl,
        int_ret_tds,
        fum_ret_tds,
        fum_rec_td,
        block_splits,
        blk_kick_tds,
        kr_tds,
        pr_tds,
        st_td_combined,
        def_2pt,
        st_ff,
        st_fum_rec,
        int_td_50,
        fum_td_50,
        forced_punts,
    ]:
        result = result.merge(df_part, on=join_key, how="left")

    # Fill all pts_def_* columns with 0 for missing team-weeks
    pts_cols = [c for c in result.columns if c.startswith("pts_def_")]
    result[pts_cols] = result[pts_cols].fillna(0)

    # Ensure integer types for count/flag columns
    for col in pts_cols:
        result[col] = result[col].astype(int)

    logger.info(f"[advanced_def] Computed {len(result):,} team-weeks with {len(pts_cols)} new columns")
    return result


def process_one_year(
    year: int, week: int = None, cache_dir: Path = None, use_cache: bool = True, data_directory: Path = None
) -> pd.DataFrame:
    """
    Process defensive stats for a single year (used by combine_dst_to_nfl.py).

    Args:
        year: NFL season year (e.g., 2014)
        week: Optional week number (0 or None = all weeks)
        cache_dir: Directory to store cached downloads
        use_cache: Whether to use cached data if available
        data_directory: League data directory for matchup window context (optional)

    Returns:
        DataFrame with defensive stats including pts_allow, three_out, and fourth_down_stop
    """
    logger.info(f"Processing year {year}, week {week if week else 'all'}")

    # Fetch team stats from NFLverse (with caching)
    df = fetch_nflverse_team_stats(year, cache_dir=cache_dir, use_cache=use_cache)

    # Fetch play-by-play data for three-and-out, fourth down stop, and advanced pts_def_* calculations.
    # For early years (1999-2000), play-by-play data may be incomplete or have missing columns.
    three_out_stats = None
    fourth_down_stop_stats = None
    advanced_def_df = None
    pbp_df = None

    try:
        pbp_df = fetch_nflverse_pbp_data(year, cache_dir=cache_dir, use_cache=use_cache)

        # Verify required columns exist before calculating stats
        if "drive_first_downs" in pbp_df.columns and "play_type_nfl" in pbp_df.columns:
            three_out_stats = calculate_three_outs(pbp_df)
        else:
            logger.warning(f"Missing columns for three_out calculation in {year} (drive_first_downs or play_type_nfl)")

        if "down" in pbp_df.columns and "play_type" in pbp_df.columns and "first_down" in pbp_df.columns:
            fourth_down_stop_stats = calculate_fourth_down_stops(pbp_df)
        else:
            logger.warning(
                f"Missing columns for fourth_down_stop calculation in {year} (down, play_type, or first_down)"
            )

    except Exception as e:
        logger.warning(f"Could not fetch or process play-by-play data for {year}: {e}")
        logger.info(f"Continuing without three_out, fourth_down_stop, and advanced pts_def_* stats for {year}")

    # Transform to defensive format
    defensive_df = transform_to_defensive_stats(df, three_out_stats, fourth_down_stop_stats)

    if pbp_df is not None and apply_team_margin_stats is not None and calculate_team_margin_stats_from_pbp is not None:
        try:
            team_margin_df = calculate_team_margin_stats_from_pbp(pbp_df)
            defensive_df = apply_team_margin_stats(defensive_df, team_margin_df)
            logger.info(
                "[process_one_year] Applied team-score/margin DST columns "
                f"from {len(team_margin_df):,} team-game rows for {year}"
            )
        except Exception as exc:
            logger.warning(f"[process_one_year] Could not apply team-score/margin DST columns: {exc}")
    elif apply_team_margin_stats is not None:
        defensive_df = apply_team_margin_stats(defensive_df, None)

    # Calculate points allowed buckets
    defensive_df = calculate_points_allowed_buckets(defensive_df)

    # Compute advanced pts_def_* columns (new in Task 12) and merge into defensive_df.
    # These columns are needed for the weekly refresh path so the super_table stays current.
    # Required columns for compute_advanced_def_columns are checked below.
    if pbp_df is not None:
        _required_pbp_cols = {
            "interception",
            "touchdown",
            "fumble",
            "fumble_recovery_1_team",
            "play_type",
            "td_team",
            "punt_blocked",
            "field_goal_result",
            "two_point_attempt",
            "defteam_score",
            "defteam_score_post",
            "return_yards",
            "drive",
            "punt_attempt",
            "fumble_forced",
        }
        _required_ts_cols = {
            "season",
            "week",
            "team",
            "def_pass_defended",
            "def_sack_yards",
            "def_sacks",
            "def_tackles_solo",
            "def_tackles_with_assist",
        }
        _pbp_ok = _required_pbp_cols.issubset(set(pbp_df.columns))
        _ts_ok = _required_ts_cols.issubset(set(df.columns))
        if _pbp_ok and _ts_ok:
            try:
                # Filter pbp to regular season + playoffs only (same as backfill script)
                pbp_filtered = pbp_df
                if "season_type" in pbp_df.columns:
                    pbp_filtered = pbp_df[pbp_df["season_type"].isin(["REG", "POST"])].copy()

                advanced_def_df = compute_advanced_def_columns(pbp_filtered, df)

                # Apply team code normalization (e.g. 'LA' → 'LAR') so the join key matches
                # the nfl_team values in defensive_df (which uses historical abbreviations).
                advanced_def_df = advanced_def_df.copy()
                advanced_def_df["nfl_team"] = advanced_def_df["nfl_team"].replace(NFLVERSE_TO_SUPER_TABLE_TEAM_MAP)

                # Merge on (season→year, week, nfl_team) into defensive_df
                # defensive_df already uses 'year' column, advanced_def_df uses 'season'
                advanced_def_df = advanced_def_df.rename(columns={"season": "year"})
                pts_def_cols = [c for c in advanced_def_df.columns if c.startswith("pts_def_")]
                merge_cols = ["year", "week", "nfl_team"] + pts_def_cols
                defensive_df = defensive_df.merge(
                    advanced_def_df[merge_cols],
                    on=["year", "week", "nfl_team"],
                    how="left",
                )
                # Fill any unmatched rows with 0
                for col in pts_def_cols:
                    if col in defensive_df.columns:
                        defensive_df[col] = defensive_df[col].fillna(0).astype(int)
                logger.info(f"[process_one_year] Merged {len(pts_def_cols)} advanced pts_def_* columns for {year}")
            except Exception as exc:
                logger.warning(f"[process_one_year] Could not compute advanced pts_def_* columns for {year}: {exc}")
                logger.info("[process_one_year] Continuing without advanced pts_def_* columns")
        else:
            missing_pbp = _required_pbp_cols - set(pbp_df.columns)
            missing_ts = _required_ts_cols - set(df.columns)
            if missing_pbp:
                logger.warning(f"[process_one_year] Missing pbp columns for advanced pts_def_*: {missing_pbp}")
            if missing_ts:
                logger.warning(f"[process_one_year] Missing team_stats columns for advanced pts_def_*: {missing_ts}")

    if apply_historical_dst_punt_blocks is not None:
        try:
            before_blocks = 0
            if not defensive_df.empty and "pts_def_punt_block" in defensive_df.columns:
                before_blocks = pd.to_numeric(defensive_df["pts_def_punt_block"], errors="coerce").fillna(0).sum()
            defensive_df = apply_historical_dst_punt_blocks(defensive_df)
            after_blocks = 0
            if not defensive_df.empty and "pts_def_punt_block" in defensive_df.columns:
                after_blocks = pd.to_numeric(defensive_df["pts_def_punt_block"], errors="coerce").fillna(0).sum()
            if after_blocks != before_blocks:
                logger.info(
                    "[process_one_year] Applied historical DST punt blocks: " f"{before_blocks:g} -> {after_blocks:g}"
                )
        except Exception as exc:
            logger.warning(f"[process_one_year] Could not apply historical DST punt blocks: {exc}")

    # Week filtering logic:
    # - CURRENT YEAR (week=0/None): Limit to max week from matchup data to avoid incomplete weeks
    # - PAST YEARS (week=0/None): Pull ALL weeks (no matchup window limitation)
    # - ANY YEAR (specific week): Filter to that specific week only
    current_year = get_current_nfl_season_year()

    # Filter by specific week if requested (applies to any year)
    if week and week > 0:
        defensive_df = defensive_df[defensive_df["week"] == week]
        logger.info(f"Filtered to week {week}: {len(defensive_df):,} rows")
    # For current year ONLY: limit to weeks with matchup data
    elif year == current_year and data_directory:
        max_week_from_matchups = get_max_week_from_matchup_data(data_directory, year)

        if max_week_from_matchups:
            logger.info(f"Current year {year}: filtering to max week from matchup data: {max_week_from_matchups}")
            defensive_df = defensive_df[defensive_df["week"] <= max_week_from_matchups]
            logger.info(f"Filtered to weeks 1-{max_week_from_matchups}: {len(defensive_df):,} rows")
        else:
            logger.warning(f"No matchup data found for current year {year}")
            logger.info("Using all available NFLverse data (may include incomplete weeks)")
    # For past years: use all available weeks (no filtering)
    else:
        if year < current_year:
            logger.debug(f"Past year {year}: using all available weeks (no matchup window limitation)")

    return defensive_df


def calculate_points_allowed_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate points allowed buckets for DST fantasy scoring.

    Buckets (standard Yahoo/ESPN scoring):
    - 0 points allowed
    - 1-6 points allowed
    - 7-13 points allowed
    - 14-20 points allowed
    - 21-27 points allowed
    - 28-34 points allowed
    - 35+ points allowed

    Args:
        df: Defensive stats DataFrame (must have 'pts_allow' column)

    Returns:
        DataFrame with points allowed buckets added
    """
    if "pts_allow" not in df.columns:
        logger.warning("pts_allow column not found, cannot calculate buckets")
        return df

    result = df.copy()

    # Initialize all bucket columns to 0
    result["pts_allow_0"] = 0
    result["pts_allow_1_6"] = 0
    result["pts_allow_7_13"] = 0
    result["pts_allow_14_20"] = 0
    result["pts_allow_21_27"] = 0
    result["pts_allow_28_34"] = 0
    result["pts_allow_35_plus"] = 0

    # Set the appropriate bucket to 1 based on points allowed
    pts = result["pts_allow"].fillna(0)
    result.loc[pts == 0, "pts_allow_0"] = 1
    result.loc[(pts >= 1) & (pts <= 6), "pts_allow_1_6"] = 1
    result.loc[(pts >= 7) & (pts <= 13), "pts_allow_7_13"] = 1
    result.loc[(pts >= 14) & (pts <= 20), "pts_allow_14_20"] = 1
    result.loc[(pts >= 21) & (pts <= 27), "pts_allow_21_27"] = 1
    result.loc[(pts >= 28) & (pts <= 34), "pts_allow_28_34"] = 1
    result.loc[pts >= 35, "pts_allow_35_plus"] = 1

    logger.debug("Points allowed bucket distribution:")
    logger.debug(f"      0 pts: {result['pts_allow_0'].sum()}")
    logger.debug(f"      1-6: {result['pts_allow_1_6'].sum()}")
    logger.debug(f"      7-13: {result['pts_allow_7_13'].sum()}")
    logger.debug(f"      14-20: {result['pts_allow_14_20'].sum()}")
    logger.debug(f"      21-27: {result['pts_allow_21_27'].sum()}")
    logger.debug(f"      28-34: {result['pts_allow_28_34'].sum()}")
    logger.debug(f"      35+: {result['pts_allow_35_plus'].sum()}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Fetch NFL team defensive stats from NFLverse")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g., 2014)")
    parser.add_argument("--week", type=int, default=0, help="Week number (0 = all weeks)")
    parser.add_argument("--context", type=str, default=None, help="Path to league_context.json")
    parser.add_argument("--no-cache", action="store_true", help="Force re-download (skip cache)")
    parser.add_argument("--cache-dir", type=str, default=None, help="Custom cache directory")
    args = parser.parse_args()

    use_cache = not args.no_cache
    cache_dir = Path(args.cache_dir) if args.cache_dir else DEFAULT_CACHE_DIR

    # Load context if provided
    output_dir = DEFAULT_OUTPUT_DIR
    if args.context and LEAGUE_CONTEXT_AVAILABLE:
        try:
            ctx = LeagueContext.load(args.context)
            output_dir = Path(ctx.player_data_directory)
            logger.info(f"Using league: {ctx.league_name}")
            logger.info(f"Output: {output_dir}")
        except Exception as e:
            logger.warning(f"Could not load context: {e}")
            logger.info(f"Falling back to default output: {output_dir}")
    else:
        logger.info(f"Using default output: {output_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Fetch team stats from NFLverse (with caching)
    df = fetch_nflverse_team_stats(args.year, cache_dir=cache_dir, use_cache=use_cache)

    # Fetch play-by-play data for three-and-out and fourth down stop calculations (with caching)
    pbp_df = fetch_nflverse_pbp_data(args.year, cache_dir=cache_dir, use_cache=use_cache)
    three_out_stats = calculate_three_outs(pbp_df)
    fourth_down_stop_stats = calculate_fourth_down_stops(pbp_df)

    # Transform to defensive format
    defensive_df = transform_to_defensive_stats(df, three_out_stats, fourth_down_stop_stats)

    # Calculate points allowed buckets
    defensive_df = calculate_points_allowed_buckets(defensive_df)

    if apply_historical_dst_punt_blocks is not None:
        defensive_df = apply_historical_dst_punt_blocks(defensive_df)

    # Filter by week if specified
    if args.week > 0:
        defensive_df = defensive_df[defensive_df["week"] == args.week]
        logger.info(f"Filtered to week {args.week}: {len(defensive_df):,} rows")

    # Save output
    week_suffix = f"week_{args.week}" if args.week > 0 else "all_weeks"
    csv_path = output_dir / f"defense_stats_{args.year}_{week_suffix}.csv"
    parquet_path = output_dir / f"defense_stats_{args.year}_{week_suffix}.parquet"

    defensive_df.to_csv(csv_path, index=False)
    defensive_df.to_parquet(parquet_path, index=False)

    logger.info(f"Saved CSV: {csv_path}")
    logger.info(f"Saved Parquet: {parquet_path}")
    logger.info(f"Rows: {len(defensive_df):,}")
    logger.debug(f"Columns: {len(defensive_df.columns)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
