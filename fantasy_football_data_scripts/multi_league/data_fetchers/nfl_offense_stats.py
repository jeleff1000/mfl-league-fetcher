#!/usr/bin/env python3
"""
Fetch NFL player offense stats from NFLverse for fantasy football.

LEGACY NOTICE: This is a fallback fetcher. Primary data flow now uses the pre-built
super_table in MotherDuck (___ops.nfl_historical.nfl_player_stats_all).

For new development:
- Use load_nfl_from_super_table.py to load NFL data
- Use update_nfl_super_table.py to update the super_table

This fetcher is retained for:
- Local development when MotherDuck is unavailable
- Initial super_table population
- Debug/verification purposes

Source: https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{year}.parquet

See: docs/PLAYER_FLOW.md for the current data pipeline architecture.
"""

import argparse
import sys
import time
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
import requests

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.data_fetchers.pbp_scoring_enrichment import enrich_with_pbp_scoring_stats

# Make tqdm optional
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None

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
DEFAULT_CACHE_DIR = REPO_ROOT / "fantasy_football_data" / "cache" / "nflverse"

# Cache settings
CACHE_MAX_AGE_HOURS = 168  # 1 week


def sanitize_infinite_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert +/-Infinity from upstream nflverse numeric fields to SQL NULLs."""
    if df.empty:
        return df

    out = df.copy()
    numeric_cols = out.select_dtypes(include=["number"]).columns
    fixed = 0
    for col in numeric_cols:
        values = pd.to_numeric(out[col], errors="coerce").to_numpy(dtype="float64", na_value=np.nan)
        mask = np.isinf(values)
        if mask.any():
            fixed += int(mask.sum())
            out.loc[mask, col] = np.nan

    if fixed:
        print(f"[NFL] Sanitized {fixed:,} infinite numeric values from nflverse stats")
    return out


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
                    print(f"  [RETRY] Attempt {attempt + 1}/{max_retries} failed: {e}")
                    print(f"  Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                    delay *= backoff_factor
                    continue

            # Non-retryable error or final attempt, re-raise
            raise

    # Should never reach here, but just in case
    raise last_exception


def fetch_nflverse_player_stats(year: int, cache_dir: Path = None, use_cache: bool = True) -> pd.DataFrame:
    """
    Fetch player offensive stats from NFLverse for a given year with caching and chunked downloads.

    IMPORTANT: For the CURRENT year, cache expires more frequently (24 hours) to ensure
    we get updated data as games complete each week. For past years, cache lasts 7 days.

    Args:
        year: NFL season year (e.g., 2014)
        cache_dir: Directory for cached files (default: DEFAULT_CACHE_DIR)
        use_cache: Whether to use cached data if available

    Returns:
        DataFrame with player offensive stats
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"nflverse_player_stats_{year}.parquet"

    # Check cache first
    if use_cache and cache_file.exists():
        current_year = get_current_nfl_season_year()

        # For current year: use shorter cache expiry (24 hours) to get fresh data as games complete
        # For past years: use longer cache expiry (168 hours = 7 days)
        max_cache_age = 24 if year == current_year else CACHE_MAX_AGE_HOURS

        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < max_cache_age:
            print(f"[NFL] Using cached data for {year} (age: {age_hours:.1f} hours, max: {max_cache_age}h)")
            try:
                df = pd.read_parquet(cache_file)
                df = sanitize_infinite_numeric_values(df)
                print(f"[NFL] Loaded {len(df):,} rows from cache")
                return df
            except Exception as e:
                print(f"[NFL] Warning: Cache read failed ({e}), re-downloading...")
                cache_file.unlink(missing_ok=True)
        else:
            print(f"[NFL] Cache expired for {year} (age: {age_hours:.1f}h > max: {max_cache_age}h), re-downloading...")

    url = f"https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{year}.parquet"

    print(f"[NFL] Downloading {year} player stats from NFLverse...")
    print(f"[NFL] URL: {url}")

    def download_with_retry():
        """Download function wrapped for retry logic."""
        tmp_path = None
        try:
            # Stream download with progress bar to avoid memory issues
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("content-length", 0))

                # Create temporary file for chunked download
                with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                    tmp_path = Path(tmp.name)

                    # Download with progress bar (if available)
                    print(f"[NFL] Downloading {total_size / (1024*1024):.1f} MB...")

                    if TQDM_AVAILABLE:
                        # Use progress bar if tqdm is available
                        with tqdm(total=total_size, unit="iB", unit_scale=True, unit_divisor=1024) as pbar:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk:  # Filter out keep-alive chunks
                                    tmp.write(chunk)
                                    pbar.update(len(chunk))
                    else:
                        # Download without progress bar
                        downloaded = 0
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:  # Filter out keep-alive chunks
                                tmp.write(chunk)
                                downloaded += len(chunk)
                                # Print progress every 10MB
                                if downloaded % (10 * 1024 * 1024) < 8192:
                                    print(
                                        f"[NFL] Downloaded {downloaded / (1024*1024):.1f} MB / {total_size / (1024*1024):.1f} MB"
                                    )

            # Read the downloaded file
            print("[NFL] Reading parquet file...")
            df = pd.read_parquet(tmp_path)
            df = sanitize_infinite_numeric_values(df)

            # NOTE: DO NOT normalize team abbreviations here!
            # NFLverse uses modern abbreviations (LA, LAC, LV) even for historical years.
            # The conversion to historical abbreviations (STL, SD, OAK) happens in
            # update_nfl_super_table.py using year-based logic.

            # Save to cache
            print(f"[NFL] Caching data to {cache_file}...")
            df.to_parquet(cache_file)

            # Clean up temp file
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

            print(f"[NFL] Downloaded {len(df):,} rows of player stats for {year}")
            return df

        except Exception as e:
            # Clean up temp file on error
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise

    # Use retry logic for the download
    return retry_with_backoff(download_with_retry)


def get_max_week_from_matchup_data(data_directory: Path, year: int) -> int | None:
    """
    Get the maximum week from matchup data files.

    This allows NFLverse player data to align with matchup data (only fetch weeks with actual matchups).

    Args:
        data_directory: League data directory (e.g., .../fantasy_football_data/KMFFL)
        year: Year to check

    Returns:
        Maximum week number found in matchup data, or None if no matchup data exists
    """
    try:
        matchup_dir = data_directory / "matchup_data"

        if not matchup_dir.exists():
            print(f"[matchup_max_week] Matchup directory not found: {matchup_dir}")
            return None

        # Try to find matchup file for this year
        # Prefer all-weeks file, fallback to individual week files
        all_weeks_file = matchup_dir / f"matchup_data_week_all_year_{year}.parquet"

        if all_weeks_file.exists():
            try:
                df = pd.read_parquet(all_weeks_file)
                if not df.empty and "week" in df.columns:
                    max_week = int(df["week"].max())
                    print(f"[matchup_max_week] Found max week {max_week} from {all_weeks_file.name}")
                    return max_week
            except Exception as e:
                print(f"[matchup_max_week] Error reading {all_weeks_file.name}: {e}")

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
                print(f"[matchup_max_week] Found max week {max_week} from {len(week_files)} individual week files")
                return max_week

        print(f"[matchup_max_week] No matchup data found for year {year}")
        return None

    except Exception as e:
        print(f"[matchup_max_week] Error getting max week from matchup data: {e}")
        return None


def process_one_year(
    year: int, week: int = None, cache_dir: Path = None, use_cache: bool = True, data_directory: Path = None
) -> pd.DataFrame:
    """
    Process offensive stats for a single year (used by combine_dst_to_nfl.py).

    Args:
        year: NFL season year (e.g., 2014)
        week: Optional week number (0 or None = all weeks)
        cache_dir: Directory for cached files
        use_cache: Whether to use cached data if available
        data_directory: League data directory for matchup window context (optional)

    Returns:
        DataFrame with player offensive stats
    """
    print(f"[NFL] Processing year {year}, week {week if week else 'all'}")

    # Fetch player stats from NFLverse with caching
    df = fetch_nflverse_player_stats(year, cache_dir=cache_dir, use_cache=use_cache)

    # Week filtering logic:
    # - CURRENT YEAR (week=0/None): Limit to max week from matchup data to avoid incomplete weeks
    # - PAST YEARS (week=0/None): Pull ALL weeks (no matchup window limitation)
    # - ANY YEAR (specific week): Filter to that specific week only
    current_year = get_current_nfl_season_year()

    # Filter by specific week if requested (applies to any year)
    if week and week > 0:
        df = df[df["week"] == week]
        print(f"[NFL] Filtered to week {week}: {len(df):,} rows")
    # For current year ONLY: limit to weeks with matchup data
    elif year == current_year and data_directory:
        max_week_from_matchups = get_max_week_from_matchup_data(data_directory, year)

        if max_week_from_matchups:
            print(f"[NFL] Current year {year}: filtering to max week from matchup data: {max_week_from_matchups}")
            df = df[df["week"] <= max_week_from_matchups]
            print(f"[NFL] Filtered to weeks 1-{max_week_from_matchups}: {len(df):,} rows")
        else:
            print(f"[NFL] WARNING: No matchup data found for current year {year}")
            print("[NFL] Using all available NFLverse data (may include incomplete weeks)")
    # For past years: use all available weeks (no filtering)
    else:
        if year < current_year:
            print(f"[NFL] Past year {year}: using all available weeks (no matchup window limitation)")

    # Standardize column names to match defense_stats.py convention
    # This prevents duplicate columns in combine_dst_to_nfl.py
    rename_map = {
        "season": "year",
        "team": "nfl_team",
        "opponent_team": "opponent_nfl_team",
        "player_id": "NFL_player_id",
        "player_display_name": "player",  # Map NFLverse player name to 'player' column
        "position": "nfl_position",  # Map position to nfl_position
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Also copy nfl_position to position for consistency
    if "nfl_position" in df.columns and "position" not in df.columns:
        df["position"] = df["nfl_position"]

    # Remap FB to RB for fantasy purposes (fullbacks are running backs in fantasy)
    if "position" in df.columns:
        df.loc[df["position"] == "FB", "position"] = "RB"
    if "nfl_position" in df.columns:
        df.loc[df["nfl_position"] == "FB", "nfl_position"] = "RB"

    df = enrich_with_pbp_scoring_stats(df, year, week=week, cache_dir=cache_dir, use_cache=use_cache)

    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch NFL player offensive stats from NFLverse")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g., 2014)")
    parser.add_argument("--week", type=int, default=0, help="Week number (0 = all weeks)")
    parser.add_argument("--context", type=str, default=None, help="Path to league_context.json")
    parser.add_argument("--no-cache", action="store_true", help="Skip cache and force re-download")
    parser.add_argument("--cache-dir", type=str, default=None, help="Custom cache directory")
    args = parser.parse_args()

    # Load context if provided
    output_dir = DEFAULT_OUTPUT_DIR
    cache_dir = Path(args.cache_dir) if args.cache_dir else DEFAULT_CACHE_DIR

    if args.context and LEAGUE_CONTEXT_AVAILABLE:
        try:
            ctx = LeagueContext.load(args.context)
            output_dir = Path(ctx.player_data_directory)
            print(f"[NFL] Using league: {ctx.league_name}")
            print(f"[NFL] Output: {output_dir}")
        except Exception as e:
            print(f"[NFL] Warning: Could not load context: {e}")
            print(f"[NFL] Falling back to default output: {output_dir}")
    else:
        print(f"[NFL] Using default output: {output_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Fetch player stats from NFLverse with caching
    df = fetch_nflverse_player_stats(args.year, cache_dir=cache_dir, use_cache=not args.no_cache)

    # Filter by week if specified
    if args.week > 0:
        df = df[df["week"] == args.week]
        print(f"[NFL] Filtered to week {args.week}: {len(df):,} rows")

    # Standardize column names to match defense_stats.py convention
    # This prevents duplicate columns in combine_dst_to_nfl.py
    rename_map = {
        "season": "year",
        "team": "nfl_team",
        "opponent_team": "opponent_nfl_team",
        "player_id": "NFL_player_id",
        "player_display_name": "player",
        "position": "nfl_position",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Also copy nfl_position to position for consistency
    if "nfl_position" in df.columns and "position" not in df.columns:
        df["position"] = df["nfl_position"]

    # Remap FB to RB for fantasy purposes (fullbacks are running backs in fantasy)
    if "position" in df.columns:
        df.loc[df["position"] == "FB", "position"] = "RB"
    if "nfl_position" in df.columns:
        df.loc[df["nfl_position"] == "FB", "nfl_position"] = "RB"

    df = enrich_with_pbp_scoring_stats(
        df,
        args.year,
        week=args.week or None,
        cache_dir=cache_dir,
        use_cache=not args.no_cache,
    )

    print(f"[NFL] Standardized column names: {list(rename_map.values())}")

    # Save output
    week_suffix = f"week_{args.week}" if args.week > 0 else "all_weeks"
    csv_path = output_dir / f"nfl_offense_stats_{args.year}_{week_suffix}.csv"
    parquet_path = output_dir / f"nfl_offense_stats_{args.year}_{week_suffix}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"\n[NFL] Saved CSV: {csv_path}")
    print(f"[NFL] Saved Parquet: {parquet_path}")
    print(f"[NFL] Rows: {len(df):,}")
    print(f"[NFL] Columns: {len(df.columns)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
