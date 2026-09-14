#!/usr/bin/env python3
"""
Load NFL player stats from the pre-built Super Table.

This script replaces the three separate fetchers:
- nfl_offense_stats.py (offense data from NFLverse)
- defense_stats.py (defense data from NFLverse)
- combine_dst_to_nfl.py (combines offense + defense)

The Super Table contains:
- Historical data (1970-1998) from Kaggle with derived DST stats
- NFLverse data (1999-2025) with offense + defense combined
- Data quality flags for known data issues
- Team logo headshots for players without photos

Data sources (in order of preference):
1. Restored ___ops DuckDB cache from GitHub Actions (fastest in workers)
2. Local parquet file
3. Fly DuckDB API (fallback)

Usage:
    python load_nfl_from_super_table.py --year 2024 --context path/to/league_context.json
    python load_nfl_from_super_table.py --year 2024 --week 5 --context path/to/league_context.json
    python load_nfl_from_super_table.py --start-year 1999 --end-year 2024 --context path/to/league_context.json
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

from multi_league.core.date_utils import get_current_nfl_season_year

import pandas as pd

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
    from multi_league.core.league_context import LeagueContext

    LEAGUE_CONTEXT_AVAILABLE = True
except ImportError:
    try:
        from core.league_context import LeagueContext

        LEAGUE_CONTEXT_AVAILABLE = True
    except ImportError:
        LEAGUE_CONTEXT_AVAILABLE = False
        LeagueContext = None

# Default paths
DEFAULT_SUPER_TABLE_PATH = REPO_ROOT / "data" / "nflverse" / "nfl_player_stats_super_table.parquet"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "fantasy_football_data" / "player_data"

# Ops table config - tables live in the shared ops database/cache
OPS_DATABASE = "___ops"
OPS_SCHEMA = "nfl_historical"
OPS_TABLE = "nfl_player_stats_all"

# Skinny columns - minimal set needed for name matching + scoring in yahoo_nfl_merge.py
# ~99% of players are matched via yahoo_nfl_player_map cache; these columns
# are only needed for the ~1% that require fallback name matching
# NOTE: gsis_id is NOT in the super table - it's only in yahoo_nfl_player_map
MERGE_SKINNY_COLUMNS = [
    # Name matching columns
    "player",  # Player name for matching
    "player_week",  # Unique identifier (player_id_year_week)
    "NFL_player_id",  # The ID we're trying to match to
    "nfl_team",  # Team for disambiguation
    "nfl_position",  # Position for disambiguation
    "position",  # Alternative position column
    "year",  # Year filter
    "week",  # Week filter
    "headshot_url",  # Needed for player display
    # Pre-calculated scoring columns (needed for fantasy_points calculation)
    # These MUST be included so scoring_calculator.py can sum them into fantasy_points
    "pts_pass_4pt",  # Passing (4pt TD leagues)
    "pts_pass_6pt",  # Passing (6pt TD leagues)
    "pts_rush",  # Rushing
    "pts_rec_0ppr",  # Receiving (standard)
    "pts_rec_half",  # Receiving (half PPR)
    "pts_rec_ppr",  # Receiving (full PPR)
    "pts_misc",  # Misc (fumbles, 2pt conversions)
    "pts_k_std",  # Kicker (Yahoo-style buckets)
    "pts_k_yds",  # Kicker (per-yard)
    "pts_def_std",  # Defense (standard) - kept for fallback/validation
    # Modular DEF components (raw counts × 1, SQL multiplies by league's point values)
    "pts_def_sack",  # Sacks
    "pts_def_int",  # Interceptions
    "pts_def_ff",  # Fumbles forced
    "pts_def_fr",  # Fumbles recovered
    "pts_def_td",  # DEF TDs (def_tds + fum_ret_td)
    "pts_def_safety",  # Safeties
    "pts_def_block",  # Blocked kicks
    "pts_def_tfl",  # Tackles for loss
    "pts_def_3out",  # 3-and-outs forced
    "pts_def_4stop",  # 4th down stops
    # Points allowed tiers (binary 0/1 indicators)
    "pts_allow_0",
    "pts_allow_1_6",
    "pts_allow_7_13",
    "pts_allow_14_20",
    "pts_allow_21_27",
    "pts_allow_28_34",
    "pts_allow_35_plus",
    # Yards allowed tiers (binary 0/1 indicators)
    "yds_allow_0_99",
    "yds_allow_100_199",
    "yds_allow_200_299",
    "yds_allow_300_349",
    "yds_allow_350_399",
    "yds_allow_400_449",
    "yds_allow_450_499",
    "yds_allow_500_549",
    "yds_allow_550_plus",
]


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [SUPER] {msg}")


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _load_from_ops_cache(cache_path: Path, years: list | None, skinny: bool) -> pd.DataFrame:
    import duckdb

    log(f"Loading from local ops cache: {cache_path}")
    conn = duckdb.connect(str(cache_path), read_only=True)
    try:
        schema_rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [OPS_SCHEMA, OPS_TABLE],
        ).fetchall()
        available = [row[0] for row in schema_rows]
        if not available:
            raise RuntimeError(f"{OPS_SCHEMA}.{OPS_TABLE} not found in {cache_path}")

        if skinny:
            valid_cols = [col for col in MERGE_SKINNY_COLUMNS if col in set(available)]
            skipped = set(MERGE_SKINNY_COLUMNS) - set(valid_cols)
            if skipped:
                log(f"  [WARN] Skipping {len(skipped)} columns not in super table: {skipped}")
            log(f"  Skinny mode: loading {len(valid_cols)} columns for name matching")
        else:
            valid_cols = available

        cols = ", ".join(_quote_ident(col) for col in valid_cols)
        params: list[int] = []
        where_sql = ""
        if years:
            params = [int(year) for year in years]
            placeholders = ", ".join(["?"] * len(params))
            where_sql = f" WHERE year IN ({placeholders})"

        df = conn.execute(
            f"SELECT {cols} FROM {OPS_SCHEMA}.{OPS_TABLE}{where_sql}",
            params,
        ).fetchdf()
        log(f"Loaded {len(df):,} records from local ops cache")
        return df
    finally:
        conn.close()


def load_super_table(super_table_path: Path = None, years: list = None, skinny: bool = False) -> pd.DataFrame:
    """
    Load the NFL Super Table from local cache/parquet or Fly.

    Args:
        super_table_path: Path to local parquet file (optional)
        years: List of years to filter (optional, used for query optimization)
        skinny: If True, only load MERGE_SKINNY_COLUMNS (~10 columns for name matching)
                instead of all 350+ columns. Use this when yahoo_nfl_player_map provides
                99% cache hits and only fallback name matching is needed.

    Returns:
        DataFrame with all NFL player stats (or skinny subset)
    """
    # Explicit local file always wins.
    if super_table_path is not None:
        super_table_path = Path(super_table_path)
    if super_table_path is not None and super_table_path.exists():
        log(f"Loading from local parquet: {super_table_path}")
        # For local files, we can use column filtering with pyarrow
        if skinny:
            import pyarrow.parquet as pq

            schema = pq.read_schema(super_table_path)
            available = {f.name for f in schema}
            valid_cols = [c for c in MERGE_SKINNY_COLUMNS if c in available]
            skipped = set(MERGE_SKINNY_COLUMNS) - set(valid_cols)
            if skipped:
                log(f"  [WARN] Skipping {len(skipped)} columns not in super table: {skipped}")
            log(f"  Skinny mode: loading {len(valid_cols)} columns for name matching")
            df = pd.read_parquet(super_table_path, columns=valid_cols)
        else:
            df = pd.read_parquet(super_table_path)
        log(f"Loaded {len(df):,} records from local file")
        # Filter locally if years specified
        if years:
            df = df[df["year"].isin(years)]
            log(f"Filtered to {len(df):,} records for years {min(years)}-{max(years)}")
        return df

    ops_cache = os.environ.get("OPS_CACHE_PATH")
    if ops_cache and Path(ops_cache).exists():
        return _load_from_ops_cache(Path(ops_cache), years, skinny)

    # Try default local parquet next.
    super_table_path = DEFAULT_SUPER_TABLE_PATH
    if super_table_path.exists():
        log(f"Loading from local parquet: {super_table_path}")
        if skinny:
            import pyarrow.parquet as pq

            schema = pq.read_schema(super_table_path)
            available = {f.name for f in schema}
            valid_cols = [c for c in MERGE_SKINNY_COLUMNS if c in available]
            skipped = set(MERGE_SKINNY_COLUMNS) - set(valid_cols)
            if skipped:
                log(f"  [WARN] Skipping {len(skipped)} columns not in super table: {skipped}")
            log(f"  Skinny mode: loading {len(valid_cols)} columns for name matching")
            df = pd.read_parquet(super_table_path, columns=valid_cols)
        else:
            df = pd.read_parquet(super_table_path)
        log(f"Loaded {len(df):,} records from local file")
        if years:
            df = df[df["year"].isin(years)]
            log(f"Filtered to {len(df):,} records for years {min(years)}-{max(years)}")
        return df

    # Fallback to Fly
    log("Local super table not found, querying Fly...")

    try:
        from multi_league.core.db_reader import get_reader

        log("Connecting to Fly...")
        reader = get_reader()

        schema_rows = reader.query(
            f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = '{OPS_DATABASE}'
            AND table_schema = '{OPS_SCHEMA}'
            AND table_name = '{OPS_TABLE}'
            ORDER BY ordinal_position
            """,
            database=OPS_DATABASE,
        )
        available = [r["column_name"] for r in schema_rows]
        if not available:
            raise RuntimeError(f"{OPS_DATABASE}.{OPS_SCHEMA}.{OPS_TABLE} not found")

        if skinny:
            available_set = set(available)
            valid_cols = [c for c in MERGE_SKINNY_COLUMNS if c in available_set]
            skipped = set(MERGE_SKINNY_COLUMNS) - set(valid_cols)
            if skipped:
                log(f"  [WARN] Skipping {len(skipped)} columns not in super table: {skipped}")
            log(f"  Skinny mode: loading {len(valid_cols)} columns for name matching")
        else:
            valid_cols = available
        cols = ", ".join(_quote_ident(col) for col in valid_cols)

        # OPTIMIZATION: Filter by year in the query to avoid loading entire table
        # The super table has 350+ columns and spans 1920-2025, so filtering is critical
        if years:
            year_list = ", ".join(str(y) for y in years)
            query = f"""
                SELECT {cols} FROM {OPS_SCHEMA}.{OPS_TABLE}
                WHERE year IN ({year_list})
            """
            log(f"Querying Fly for years: {min(years)}-{max(years)}")
        else:
            query = f"SELECT {cols} FROM {OPS_SCHEMA}.{OPS_TABLE}"
            log("Querying Fly for all years (this may take a while)...")

        df = reader.query_df(query, database=OPS_DATABASE)

        log(f"Loaded {len(df):,} records from Fly ({OPS_DATABASE}.{OPS_SCHEMA})")
        return df

    except Exception as e:
        raise RuntimeError(f"Failed to load from Fly: {e}")


def normalize_columns(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Normalize column names and fill missing values for compatibility.

    Ensures:
    - 'position' is populated from 'nfl_position' where missing
    - Column names match what yahoo_nfl_merge_v3.py expects
    """
    result = df.copy()

    # Ensure 'position' column is populated
    # NFLverse uses 'nfl_position', historical uses 'position'
    if "position" in result.columns and "nfl_position" in result.columns:
        # Fill position from nfl_position where position is missing
        position_missing = result["position"].isna() | (result["position"] == "")
        result.loc[position_missing, "position"] = result.loc[position_missing, "nfl_position"]
    elif "nfl_position" in result.columns and "position" not in result.columns:
        # Create position column from nfl_position
        result["position"] = result["nfl_position"]

    return result


def filter_by_year_week(
    df: pd.DataFrame,
    year: int = None,
    week: int = None,
    start_year: int = None,
    end_year: int = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Filter super table by year and/or week.

    Args:
        df: Super table DataFrame
        year: Single year to filter (mutually exclusive with start_year/end_year)
        week: Week to filter (0 or None = all weeks)
        start_year: Start of year range
        end_year: End of year range
        verbose: Whether to log filter operations

    Returns:
        Filtered DataFrame
    """
    result = df.copy()

    # Filter by year range or single year
    if year is not None:
        result = result[result["year"] == year]
    elif start_year is not None or end_year is not None:
        if start_year is not None:
            result = result[result["year"] >= start_year]
        if end_year is not None:
            result = result[result["year"] <= end_year]
        if verbose:
            log(f"Filtered to years {start_year or 'min'}-{end_year or 'max'}: {len(result):,} records")

    # Filter by week
    if week and week > 0:
        result = result[result["week"] == week]
        if verbose:
            log(f"Filtered to week {week}: {len(result):,} records")

    return result


def save_nfl_stats(df: pd.DataFrame, output_dir: Path, year: int, week: int = None, verbose: bool = True) -> Path:
    """
    Save NFL stats in the format expected by yahoo_nfl_merge_v3.py.

    Args:
        df: Filtered DataFrame for this year/week
        output_dir: Directory to save output
        year: Year being saved
        week: Week being saved (0 or None = all weeks)
        verbose: Whether to log save operations

    Returns:
        Path to saved file
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use the same naming convention as combine_dst_to_nfl.py
    if week and week > 0:
        filename = f"nfl_stats_merged_{year}_week_{week}.parquet"
    else:
        filename = f"nfl_stats_merged_{year}_all_weeks.parquet"

    output_path = output_dir / filename
    df.to_parquet(output_path, index=False)

    return output_path


def process_year(
    super_table: pd.DataFrame, year: int, week: int, output_dir: Path, max_week: int = None, verbose: bool = True
) -> int:
    """
    Process a single year from the super table.

    Args:
        super_table: Full super table DataFrame
        year: Year to process
        week: Week to process (0 = all weeks)
        output_dir: Output directory
        max_week: Maximum week for current year (to exclude incomplete weeks)
        verbose: Whether to log detailed progress

    Returns:
        Number of records saved, or 0 if no data
    """
    # Filter by year
    year_df = filter_by_year_week(super_table, year=year, week=week if week else None, verbose=False)

    # Normalize columns for compatibility
    year_df = normalize_columns(year_df, verbose=False)

    if year_df.empty:
        return 0

    # For current year, limit to max_week if specified
    current_year = get_current_nfl_season_year()
    if year == current_year and max_week and not week:
        year_df = year_df[year_df["week"] <= max_week]

    # Save the data
    save_nfl_stats(year_df, output_dir, year, week, verbose=verbose)

    return len(year_df)


def main():
    parser = argparse.ArgumentParser(
        description="Load NFL stats from Super Table (replaces nfl_offense_stats + defense_stats + combine_dst_to_nfl)"
    )
    parser.add_argument("--year", type=int, help="Single year to process")
    parser.add_argument("--week", type=int, default=0, help="Week to process (0 = all weeks)")
    parser.add_argument("--start-year", type=int, help="Start year for range")
    parser.add_argument("--end-year", type=int, help="End year for range")
    parser.add_argument("--max-week", type=int, default=None, help="Max week for current year (exclude incomplete)")
    parser.add_argument("--context", type=str, help="Path to league_context.json")
    parser.add_argument("--output-dir", type=str, help="Output directory")
    parser.add_argument("--super-table", type=str, help="Path to super table parquet")
    parser.add_argument(
        "--skinny",
        action="store_true",
        help="Load only skinny columns for name matching (~10 cols instead of 350+). "
        "Use when yahoo_nfl_player_map provides 99%% cache hits.",
    )

    args = parser.parse_args()

    # Determine output directory
    output_dir = DEFAULT_OUTPUT_DIR

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif args.context:
        if not LEAGUE_CONTEXT_AVAILABLE:
            log("Warning: LeagueContext not available (import failed), using default output dir")
        else:
            try:
                ctx = LeagueContext.load(args.context)
                output_dir = Path(ctx.player_data_directory)
                log(f"Using league context: {ctx.league_name} (platform: {getattr(ctx, 'platform', 'yahoo')})")
            except Exception as e:
                log(f"Warning: Could not load context: {e}")

    log(f"Output directory: {output_dir}")

    # Determine years to process BEFORE loading super table.
    # This lets cache/Fly reads stay year-scoped.
    if args.year:
        years = [args.year]
    elif args.start_year and args.end_year:
        years = list(range(args.start_year, args.end_year + 1))
    elif args.start_year:
        # Only start year specified - load that year forward.
        years = list(range(args.start_year, 2030))  # Reasonable upper bound
    elif args.end_year:
        # Only end year specified - load from 1999 (NFLverse start) to end
        years = list(range(1999, args.end_year + 1))
    else:
        # No year filter - will load all.
        years = None

    # Load the super table (with year filter for optimization)
    super_table_path = Path(args.super_table) if args.super_table else None

    try:
        super_table = load_super_table(super_table_path, years=years, skinny=args.skinny)
    except RuntimeError as e:
        log(f"ERROR: {e}")
        return 1

    # If years wasn't specified, get all available years from loaded data
    if years is None:
        years = sorted(super_table["year"].unique())
    else:
        # Filter to years actually in the data
        available_years = set(super_table["year"].unique())
        years = [y for y in years if y in available_years]

    # Process each year - verbose only if processing a single year
    verbose = len(years) == 1
    total_records = 0
    success_count = 0
    errors = []

    for year in years:
        try:
            records = process_year(super_table, year, args.week, output_dir, args.max_week, verbose=verbose)
            if records > 0:
                success_count += 1
                total_records += records
        except Exception as e:
            errors.append(f"{year}: {e}")

    # Log summary
    if len(years) == 1:
        if success_count > 0:
            log(f"Saved {total_records:,} records for {years[0]}")
    else:
        log(f"Saved {len(years)} years ({years[0]}-{years[-1]}): {total_records:,} total records")
        if errors:
            log(f"  [WARN] {len(errors)} error(s): {', '.join(errors[:3])}" + ("..." if len(errors) > 3 else ""))

    return 0 if success_count == len(years) else 1


if __name__ == "__main__":
    sys.exit(main())
