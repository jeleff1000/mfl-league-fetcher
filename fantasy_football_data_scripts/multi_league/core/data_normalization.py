"""
Core data normalization utilities for transformation modules.
Ensures consistent data types across all transformations.
"""

import pandas as pd
from pathlib import Path
from typing import Any

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

# Import logging with fallbacks
try:
    from multi_league.core.logging_config import get_logger
except ImportError:
    try:
        from core.logging_config import get_logger
    except ImportError:
        # Fallback to a simple logger if logging_config isn't available
        import logging

        def get_logger(name):
            logger = logging.getLogger(name)
            if not logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
                logger.addHandler(handler)
                logger.setLevel(logging.INFO)
            return logger


logger = get_logger(__name__)

from multi_league.core.join_keys import build_manager_week_series, canonical_cumulative_week_series


def find_league_settings_directory(
    league_id: str | None = None,
    df: pd.DataFrame | None = None,
    data_directory: Path | None = None,
) -> Path | None:
    """
    Find the yahoo_league_settings directory for a given league.

    This function searches for league settings in the standard LeagueContext directory structure:
    - {data_directory}/player_data/yahoo_league_settings/

    Args:
        league_id: League ID (e.g., "nfl.l.123456"). If None, will try to extract from df.
        df: DataFrame with league_id column (optional, used if league_id not provided)
        data_directory: Direct path to league data directory (optional, highest priority)

    Returns:
        Path to yahoo_league_settings directory if found, None otherwise
    """
    # PRIORITY 1: Use data_directory if provided (most reliable)
    if data_directory is not None:
        data_path = Path(data_directory)

        # Check for league_settings.parquet directly in data_directory
        # This is common when data is consolidated into the main data directory
        parquet_path = data_path / "league_settings.parquet"
        if parquet_path.exists():
            return data_path  # Return the directory containing the parquet

        # Check for JSON settings files directly in data_directory
        # Sleeper files use: league_settings_YEAR.json (no league ID suffix)
        # Yahoo files use: league_settings_YEAR_LEAGUEID.json
        if any(data_path.glob("league_settings_*.json")):
            return data_path

        # NEW location (as of 2025): league_settings/ (league-wide config, not player-specific)
        settings_path_new = data_path / "league_settings"
        if settings_path_new.exists() and settings_path_new.is_dir():
            # Only return if it actually contains settings files
            has_settings = (settings_path_new / "league_settings.parquet").exists() or any(
                settings_path_new.glob("league_settings_*.json")
            )
            if has_settings:
                return settings_path_new

        # OLD location (backwards compatibility): player_data/yahoo_league_settings/
        settings_path_old = data_path / "player_data" / "yahoo_league_settings"
        if settings_path_old.exists() and settings_path_old.is_dir():
            return settings_path_old

    # Try to get league_id from df if not provided
    if league_id is None and df is not None:
        if "league_id" in df.columns:
            league_id = df["league_id"].iloc[0] if len(df) > 0 else None

    if league_id is None:
        return None

    # Sanitize league_id for filesystem (replace dots with underscores)
    sanitized_id = str(league_id).replace(".", "_")

    # Start from this file's location and navigate to common data storage locations
    current = Path(__file__).resolve().parent  # multi_league/core/
    multi_league_dir = current.parent  # multi_league/
    scripts_dir = multi_league_dir.parent  # fantasy_football_data_scripts/

    # Search in standard locations based on LeagueContext structure
    possible_base_paths = [
        Path.home() / "fantasy_football_data",  # Default LeagueContext location
        scripts_dir / "data",
        scripts_dir.parent / "fantasy_football_data",
        multi_league_dir / "data",
    ]

    # PRIORITY 2: Look for {base}/{sanitized_league_id}/league_settings/ (NEW structure)
    for base_path in possible_base_paths:
        # Try NEW location first
        settings_path_new = base_path / sanitized_id / "league_settings"
        if settings_path_new.exists() and settings_path_new.is_dir():
            return settings_path_new

        # Try OLD location for backwards compatibility
        settings_path_old = base_path / sanitized_id / "player_data" / "yahoo_league_settings"
        if settings_path_old.exists() and settings_path_old.is_dir():
            return settings_path_old

    # PRIORITY 3: Broader search - look for ANY subdirectory containing the settings
    # This handles cases where the directory is named by league name instead of ID
    for base_path in possible_base_paths:
        if not base_path.exists():
            continue
        # Search for any subdirectory with league_settings or player_data/yahoo_league_settings
        try:
            for subdir in base_path.iterdir():
                if subdir.is_dir():
                    # Try NEW location first
                    settings_path_new = subdir / "league_settings"
                    if settings_path_new.exists() and settings_path_new.is_dir():
                        # Verify this is the right league by checking for settings files with matching league_id
                        settings_files = list(settings_path_new.glob(f"league_settings_*_{sanitized_id}.json"))
                        if settings_files:
                            return settings_path_new

                    # Try OLD location for backwards compatibility
                    settings_path_old = subdir / "player_data" / "yahoo_league_settings"
                    if settings_path_old.exists() and settings_path_old.is_dir():
                        # Verify this is the right league by checking for settings files with matching league_id
                        settings_files = list(settings_path_old.glob(f"league_settings_*_{sanitized_id}.json"))
                        if settings_files:
                            return settings_path_old
        except (PermissionError, OSError):
            # Skip directories we can't read
            pass

    return None


# Columns that must remain as strings even if they look numeric
# These are IDs used for joins/lookups that would lose precision as floats
# and get unwanted .0 suffix when converted back to strings
STRING_ID_COLUMNS = {
    # Player IDs
    "player_id",
    "yahoo_player_id",
    "sleeper_player_id",
    "sleeper_player_id_original",
    "NFL_player_id",
    "gsis_id",
    "player_key",
    "espn_id",
    "rotowire_id",
    "sportradar_id",
    # Manager/user IDs
    "manager_guid",
    "owner_id",
    "user_id",
    "franchise_id",
    # League/entity IDs
    "league_id",
    "draft_id",
    "transaction_id",
    "roster_id",
    "team_key",
    "matchup_id",
}


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize numeric columns to ensure consistent data types.
    Converts string numbers to proper numeric types.

    IMPORTANT: Skips ID columns that must remain as strings to avoid
    float precision issues and .0 suffix problems.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with normalized numeric columns
    """
    df = df.copy()

    for col in df.columns:
        # Skip if already numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            continue

        # Skip ID columns - must remain strings for joins and to avoid .0 suffix
        if col in STRING_ID_COLUMNS or col.endswith("_id") or col.endswith("_key") or col.endswith("_guid"):
            continue

        # Try to convert to numeric if it looks like numbers
        try:
            # Check if column is object or string type (handles both 'object' and pandas 'string' dtypes)
            if df[col].dtype == "object" or str(df[col].dtype) == "string":
                converted = pd.to_numeric(df[col], errors="coerce")
                # Only convert if we didn't lose too much data (>50% valid conversions)
                if converted.notna().sum() > len(df) * 0.5 and converted.notna().sum() > 0:
                    df[col] = converted
        except (ValueError, TypeError, AttributeError):
            # Keep as-is if conversion fails
            pass

    return df


def ensure_league_id(df: pd.DataFrame, league_id: Any) -> pd.DataFrame:
    """
    Ensure league_id column exists and is populated.

    Args:
        df: Input DataFrame
        league_id: League ID to ensure

    Returns:
        DataFrame with league_id column
    """
    df = df.copy()

    if "league_id" not in df.columns:
        df["league_id"] = league_id
    else:
        # Fill missing league_id values
        df["league_id"] = df["league_id"].fillna(league_id)

    return df


def add_composite_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add composite key columns for common lookups.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with composite key columns
    """
    df = df.copy()

    # Add common composite keys
    if "year" in df.columns and "week" in df.columns:
        df["year_week"] = df["year"].astype(str) + "_" + df["week"].astype(str)

    if "player" in df.columns and "year" in df.columns:
        df["player_year"] = df["player"].astype(str) + "_" + df["year"].astype(str)

    # --- canonical manager_week/opponent_week using cumulative_week ---
    # Ensure cumulative_week if year/week exist
    if "cumulative_week" not in df.columns and {"year", "week"}.issubset(df.columns):
        df["cumulative_week"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int) * 100 + pd.to_numeric(
            df["week"], errors="coerce"
        ).fillna(0).astype(int)

    if "cumulative_week" in df.columns:
        df["cumulative_week"] = pd.to_numeric(df["cumulative_week"], errors="coerce").round().astype("Int64")
        cumulative_str = canonical_cumulative_week_series(df["cumulative_week"])

        # Canonical manager_week prefers stable identity (franchise_id, then manager_guid)
        # so duplicate display names do not collide.
        if {"manager", "cumulative_week"}.issubset(df.columns):
            df["manager_week"] = build_manager_week_series(df)

        # opponent_week still uses opponent display identity unless an explicit
        # opponent franchise id is available.
        if "opponent" in df.columns:
            if "opponent_franchise_id" in df.columns:
                opponent_identity = (
                    df["opponent_franchise_id"]
                    .astype("string")
                    .str.strip()
                    .mask(lambda s: s.isna() | s.isin(["", "None", "<NA>", "nan", "NaT"]), pd.NA)
                )
            else:
                opponent_identity = (
                    df["opponent"]
                    .astype("string")
                    .str.strip()
                    .str.replace(" ", "", regex=False)
                    .mask(lambda s: s.isna() | s.isin(["", "None", "<NA>", "nan", "NaT"]), pd.NA)
                )
            df["opponent_week"] = (
                (opponent_identity + cumulative_str)
                .astype("string")
                .mask(
                    opponent_identity.isna() | cumulative_str.isna(),
                    pd.NA,
                )
            )

    # Ensure BOTH manager_week and manager_year_week exist (bidirectional alias)
    # This fixes cloud/local sync issues where one might exist without the other
    if "manager_week" in df.columns and "manager_year_week" not in df.columns:
        df["manager_year_week"] = df["manager_week"]
    elif "manager_year_week" in df.columns and "manager_week" not in df.columns:
        df["manager_week"] = df["manager_year_week"]

    return df


def validate_league_isolation(
    df: pd.DataFrame, expected_league_id: Any, file_name: str = "unknown", log: Any = None, valid_league_ids: Any = None
) -> bool:
    """
    Validate that all rows in DataFrame belong to expected league(s).

    Args:
        df: Input DataFrame
        expected_league_id: Primary expected league ID (for single-year leagues)
        file_name: Name of file being validated (for logging)
        log: Optional logging function (defaults to print)
        valid_league_ids: Collection of valid league IDs (for multi-year leagues)
                         If provided, allows multiple league_ids as long as all are valid

    Returns:
        True if all rows match expected league(s), False otherwise
    """
    # Use provided log function or fall back to print
    _log = log if log is not None else print

    if "league_id" not in df.columns:
        _log(f"  [{file_name}] WARNING: league_id column not found, cannot validate isolation")
        return False

    unique_leagues = df["league_id"].dropna().unique()

    if len(unique_leagues) == 0:
        _log(f"  [{file_name}] WARNING: No league_id values found")
        return False

    # Build set of valid league IDs
    valid_set = set()
    if valid_league_ids:
        valid_set = {str(lid) for lid in valid_league_ids}
    if expected_league_id:
        valid_set.add(str(expected_league_id))

    # Check if all league_ids in data are valid
    actual_set = {str(lid) for lid in unique_leagues}
    invalid_leagues = actual_set - valid_set

    if invalid_leagues:
        _log(f"  [{file_name}] ERROR: Invalid league IDs found: {invalid_leagues}")
        _log(f"  [{file_name}]        Valid league IDs: {valid_set}")
        return False

    _log(f"  [{file_name}] OK: League isolation validated ({len(actual_set)} league IDs across years)")
    return True


def harmonize_dtypes(df1: pd.DataFrame, df2: pd.DataFrame) -> tuple:
    """
    Harmonize column dtypes between two DataFrames before concat.

    When merging external staging data with existing parquet data, columns
    may have different types (e.g., division_id as str vs int64). This
    causes pyarrow to fail when writing. Convert ID-like columns to strings
    and year/week to integers.

    Args:
        df1: First DataFrame (typically existing data)
        df2: Second DataFrame (typically external staging data)

    Returns:
        Tuple of (df1, df2) with harmonized dtypes
    """
    # Columns that should be integers (year/week are critical for joins and filtering)
    int_columns = ["year", "week", "round", "pick"]

    # Columns that should be treated as strings (IDs that may be numeric)
    id_columns = [
        "division_id",
        "manager_id",
        "team_id",
        "league_id",
        "player_id",
        "player_key",
        "roster_position",
        "status",
        "bye_week",
    ]

    # First pass: normalize year/week to integers for BOTH dataframes
    # This handles float years like 2013.0 and ensures proper integer type
    for df in [df1, df2]:
        for col in int_columns:
            if col in df.columns:
                # Convert to numeric first (handles '2013', '2013.0', 2013.0, etc.)
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Drop NA rows for critical columns like year
                if col == "year":
                    df.dropna(subset=[col], inplace=True)
                # Convert to nullable integer (handles remaining NaN)
                df[col] = df[col].astype("Int64")

    # Get common columns
    common_cols = set(df1.columns) & set(df2.columns)

    for col in common_cols:
        # Skip int columns - already handled
        if col in int_columns:
            continue

        # Check if types differ
        if df1[col].dtype != df2[col].dtype:
            # For ID columns, convert both to string
            if col in id_columns or col.endswith("_id") or col.endswith("_key"):
                df1[col] = df1[col].astype(str).replace("nan", pd.NA).replace("None", pd.NA)
                df2[col] = df2[col].astype(str).replace("nan", pd.NA).replace("None", pd.NA)
            # For numeric-like columns where one is object, try to convert object to numeric
            elif pd.api.types.is_numeric_dtype(df1[col]) and df2[col].dtype == "object":
                try:
                    df2[col] = pd.to_numeric(df2[col], errors="coerce")
                except Exception:
                    # Fall back to string
                    df1[col] = df1[col].astype(str)
                    df2[col] = df2[col].astype(str)
            elif pd.api.types.is_numeric_dtype(df2[col]) and df1[col].dtype == "object":
                try:
                    df1[col] = pd.to_numeric(df1[col], errors="coerce")
                except Exception:
                    # Fall back to string
                    df1[col] = df1[col].astype(str)
                    df2[col] = df2[col].astype(str)

    return df1, df2


def coerce_staging_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce a DataFrame downloaded from staging (all-VARCHAR) to proper types.

    Staging tables store everything as VARCHAR. Before writing to parquet or
    merging with Yahoo-fetched data, numeric columns must be converted to their
    proper types to avoid ArrowTypeError on concat/write.

    This is applied in Phase 0.3 when downloading external data files from staging.
    """
    # Integer columns (nullable Int64 to handle NaN)
    int_cols = [
        "year",
        "week",
        "round",
        "pick",
        "win",
        "loss",
        "tie",
        "is_playoffs",
        "is_consolation",
        "keeper",
        "draft_slot",
        "draft_slot_roster_id",
        "yahoo_player_id",
        "close_margin",
        "proj_wins",
        "proj_losses",
        "teams_beat_this_week",
        "opponent_teams_beat_this_week",
        "above_proj_score",
        "below_proj_score",
        "win_vs_spread",
        "lose_vs_spread",
        "underdog_wins",
        "favorite_losses",
        "above_league_median",
        "below_league_median",
        "number_of_moves",
        "number_of_trades",
        "transaction_sequence",
    ]
    # Float columns
    float_cols = [
        "team_points",
        "opponent_points",
        "points",
        "fantasy_points",
        "projected_points",
        "team_projected_points",
        "opponent_projected_points",
        "margin",
        "cost",
        "faab_bid",
        "avg_pick",
        "avg_round",
        "avg_cost",
        "percent_drafted",
        "preseason_avg_pick",
        "preseason_avg_round",
        "preseason_avg_cost",
        "preseason_percent_drafted",
        "percent_started",
        "percent_owned",
        "total_matchup_score",
        "weekly_mean",
        "weekly_median",
        "league_weekly_mean",
        "league_weekly_median",
        "proj_score_error",
        "abs_proj_score_error",
        "expected_spread",
        "expected_odds",
        "gpa",
        "win_probability",
        "coverage_value",
        "value",
        "felo_score",
        "waiver_priority",
        "faab_balance",
        "auction_budget_spent",
        "auction_budget_total",
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
