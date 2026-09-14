def ensure_normalized(func):
    """Decorator to ensure data normalization"""

    @wraps(func)
    def wrapper(df: pd.DataFrame, *args, **kwargs):
        # Normalize input
        df = normalize_numeric_columns(df)

        # Run transformation
        result = func(df, *args, **kwargs)

        # Normalize output
        result = normalize_numeric_columns(result)

        # Ensure league_id present
        if "league_id" in df.columns:
            league_id = df["league_id"].iloc[0] if len(df) > 0 else None
            if league_id:
                result = ensure_league_id(result, league_id)

        return result

    return wrapper


"""
Matchup Keys Module

Adds unique identifiers for matchups to enable self-joins and opponent lookups.

Data Dictionary Additions:
- matchup_key: Canonical key for manager-opponent pair (alphabetically sorted)
- matchup_id: Unique identifier for specific week matchup instance
- matchup_sort_key: Deterministic ordering key for the matchup
"""

from functools import wraps

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

from core.data_normalization import normalize_numeric_columns, ensure_league_id
import pandas as pd


@ensure_normalized
def add_matchup_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add matchup_key and matchup_id for joining manager and opponent as one matchup.
    matchup_key format: "{team1}__vs__{team2}__{year}__{week}"
    - Teams sorted alphabetically to ensure same key for both perspectives
    - Enables self-joins to get both sides of matchup in one row
    matchup_id format: "{matchup_key}_{manager}"
    - Unique per manager perspective
    Args:
        df: Matchup DataFrame with manager, opponent, year, week columns
    Returns:
        DataFrame with matchup_key and matchup_id added
    """
    df = df.copy()
    # Ensure required columns exist
    required = ["manager", "opponent", "year", "week"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Create matchup_key (canonical, sorted alphabetically)
    def create_matchup_key(row):
        manager = str(row["manager"]).strip()
        opponent = str(row["opponent"]).strip()

        # Handle NA values in year/week
        year_val = row["year"]
        week_val = row["week"]

        # Convert to int, handling NA/None
        if pd.isna(year_val):
            return pd.NA  # Return NA for rows with missing year
        if pd.isna(week_val):
            return pd.NA  # Return NA for rows with missing week

        year = int(year_val)
        week = int(week_val)

        # Sort teams alphabetically for canonical key
        teams = sorted([manager, opponent])
        return f"{teams[0]}__vs__{teams[1]}__{year}__{week}"

    df["matchup_key"] = df.apply(create_matchup_key, axis=1)
    # Ensure matchup_key is string type (may be object with NA values)
    df["matchup_key"] = df["matchup_key"].astype(str)
    # Create matchup_id (unique per manager perspective)
    df["matchup_id"] = df["matchup_key"] + "_" + df["manager"].astype(str)
    # Create matchup_sort_key (deterministic ordering within matchup)
    # Lower alphabetically comes first
    df["matchup_sort_key"] = df.apply(lambda row: 1 if str(row["manager"]) < str(row["opponent"]) else 2, axis=1)
    # Create manager_week and opponent_week composite keys for player joins
    # First, ensure cumulative_week exists
    if "cumulative_week" not in df.columns:
        df["cumulative_week"] = pd.NA

    # Fill any NA values in cumulative_week using year + week formula
    # This handles staging data that may not have cumulative_week populated
    if "week" in df.columns and "year" in df.columns:
        na_mask = df["cumulative_week"].isna()
        if na_mask.any():
            # Calculate cumulative_week for rows with NA: year * 100 + week
            # e.g., 2013 week 1 = 201301
            computed_cw = pd.to_numeric(df.loc[na_mask, "year"], errors="coerce").fillna(0).astype(
                int
            ) * 100 + pd.to_numeric(df.loc[na_mask, "week"], errors="coerce").fillna(0).astype(int)
            df.loc[na_mask, "cumulative_week"] = computed_cw
            print(f"  Filled {na_mask.sum():,} missing cumulative_week values from year + week")

    has_valid_cumulative_week = df["cumulative_week"].notna().any()

    if has_valid_cumulative_week:
        # Convert cumulative_week to string, handling any remaining NA values
        cw_as_str = pd.to_numeric(df["cumulative_week"], errors="coerce").fillna(0).astype(int).astype(str)
        df["manager_week"] = df["manager"].str.replace(" ", "", regex=False) + cw_as_str
        df["opponent_week"] = df["opponent"].str.replace(" ", "", regex=False) + cw_as_str
        print("  Added manager_week and opponent_week composite keys")
    else:
        print("  [WARN] Could not create manager_week/opponent_week (missing cumulative_week, year, or week)")
    # Create manager_year composite key
    if "year" in df.columns:
        df["manager_year"] = df["manager"].str.replace(" ", "", regex=False) + df["year"].astype(str)
        df["opponent_year"] = df["opponent"].str.replace(" ", "", regex=False) + df["year"].astype(str)
        print("  Added manager_year and opponent_year composite keys")
    print(f"  Added matchup keys: {df['matchup_key'].nunique()} unique matchups")
    return df


@ensure_normalized
def get_opponent_data(df: pd.DataFrame, opponent_columns: list[str]) -> pd.DataFrame:
    """
    Self-join to get opponent's data for each row.
    Example:
        opponent_columns = ['team_points', 'grade', 'projected_points']
        result = get_opponent_data(df, opponent_columns)
        # Result has opponent_team_points, opponent_grade, etc.
    Args:
        df: DataFrame with matchup_key
        opponent_columns: Columns to pull from opponent's perspective
    Returns:
        DataFrame with opponent_{column} added for each column
    """
    if "matchup_key" not in df.columns:
        raise ValueError("matchup_key not found. Run add_matchup_keys() first.")
    # Self-join on matchup_key
    opponent_df = df[["matchup_key", "manager"] + opponent_columns].copy()
    # Rename columns with opponent_ prefix
    rename_map = {col: f"opponent_{col}" for col in opponent_columns}
    rename_map["manager"] = "opponent"  # This is the join key
    opponent_df = opponent_df.rename(columns=rename_map)
    # Join
    result = df.merge(opponent_df, on=["matchup_key", "opponent"], how="left")
    return result
