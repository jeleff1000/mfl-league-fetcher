"""
Shared Type Normalization Utilities

Provides consistent type handling for join keys across all transformations.
This ensures that join keys remain consistent throughout the pipeline.

Usage:
    from modules.type_utils import normalize_join_keys, safe_merge

    # Normalize before/after transformations
    df = normalize_join_keys(df)

    # Use safe_merge instead of df.merge() for automatic type handling
    result = safe_merge(left, right, on=['yahoo_player_id', 'year'], how='left')
"""

import pandas as pd


# Define standard join key columns and their expected types.
# NFL_player_id must stay string because defensive/team IDs use values like DEF-14.
STANDARD_JOIN_KEYS = {
    "yahoo_player_id": "Int64",
    "NFL_player_id": "string",
    "year": "Int64",
    "week": "Int64",
    "cumulative_week": "Int64",
    "season": "Int64",
    "transaction_sequence": "Int64",
    "league_id": "string",  # League ID is always string
}


def normalize_join_keys(df: pd.DataFrame, keys: list[str] | None = None, verbose: bool = False) -> pd.DataFrame:
    """
    Normalize join key data types to standard types.

    This ensures consistent types across all transformations:
    - Numeric IDs, years, weeks → Int64 (nullable integer)
    - League IDs → string

    Args:
        df: DataFrame to normalize
        keys: Specific keys to normalize (default: all standard keys found in df)
        verbose: Print conversion messages

    Returns:
        DataFrame with normalized join key types
    """
    df = df.copy()

    # If no keys specified, normalize all standard keys present in the dataframe
    if keys is None:
        keys = [k for k in STANDARD_JOIN_KEYS.keys() if k in df.columns]

    for key in keys:
        if key not in df.columns:
            continue

        expected_dtype = STANDARD_JOIN_KEYS.get(key)
        if expected_dtype is None:
            continue

        current_dtype = str(df[key].dtype)

        # Skip if already correct type
        if current_dtype == expected_dtype:
            continue

        # Convert to expected type
        try:
            if expected_dtype == "Int64":
                df[key] = pd.to_numeric(df[key], errors="coerce").astype("Int64")
            elif expected_dtype == "string":
                df[key] = df[key].astype("string")

            if verbose:
                print(f"  [TYPE] {key}: {current_dtype} -> {expected_dtype}")
        except Exception as e:
            if verbose:
                print(f"  [WARNING] Could not convert {key}: {e}")

    return df


def safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    how: str = "left",
    suffixes: tuple = ("_x", "_y"),
    validate: str | None = None,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Merge two DataFrames with automatic join key type normalization.

    This is a drop-in replacement for pd.DataFrame.merge() that:
    1. Normalizes join key types before merging
    2. Ensures result has consistent types after merging
    3. Prevents type mismatch join failures

    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Column(s) to join on
        how: Join type ('left', 'right', 'inner', 'outer')
        suffixes: Suffixes for overlapping columns
        validate: Merge validation (e.g., 'one_to_one', 'many_to_one')
        verbose: Print diagnostic info

    Returns:
        Merged DataFrame with normalized types
    """
    # Ensure 'on' is a list
    if isinstance(on, str):
        on = [on]

    # Normalize join keys in both dataframes before merge
    left_norm = normalize_join_keys(left, keys=on, verbose=verbose)
    right_norm = normalize_join_keys(right, keys=on, verbose=verbose)

    # Perform merge
    result = left_norm.merge(right_norm, on=on, how=how, suffixes=suffixes, validate=validate)

    # Normalize join keys in result (in case merge changed types)
    result = normalize_join_keys(result, keys=on, verbose=verbose)

    return result


def ensure_canonical_types(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """
    Ensure all join keys in a canonical file have correct types.

    Use this at the END of any transformation that modifies canonical files
    (player.parquet, draft.parquet, transactions.parquet) to ensure types
    are consistent for downstream transformations.

    Args:
        df: DataFrame to normalize
        verbose: Print conversion messages

    Returns:
        DataFrame with all standard join keys normalized
    """
    return normalize_join_keys(df, keys=None, verbose=verbose)


def validate_join_keys(df: pd.DataFrame, required_keys: list[str], name: str = "DataFrame") -> bool:
    """
    Validate that required join keys exist and have correct types.

    Args:
        df: DataFrame to validate
        required_keys: List of required column names
        name: Name of DataFrame (for error messages)

    Returns:
        True if valid, raises ValueError if invalid

    Raises:
        ValueError: If required keys are missing or have wrong types
    """
    # Check for missing columns
    missing_keys = [k for k in required_keys if k not in df.columns]
    if missing_keys:
        raise ValueError(f"{name} missing required join keys: {missing_keys}")

    # Check for wrong types
    type_issues = []
    for key in required_keys:
        if key not in STANDARD_JOIN_KEYS:
            continue

        expected_dtype = STANDARD_JOIN_KEYS[key]
        actual_dtype = str(df[key].dtype)

        if actual_dtype != expected_dtype:
            type_issues.append(f"{key}: expected {expected_dtype}, got {actual_dtype}")

    if type_issues:
        raise ValueError(f"{name} has incorrect join key types:\n  " + "\n  ".join(type_issues))

    return True


def get_join_key_info(df: pd.DataFrame) -> dict:
    """
    Get information about join keys in a DataFrame.

    Args:
        df: DataFrame to inspect

    Returns:
        Dict with key -> (dtype, null_count, sample_values) mapping
    """
    info = {}

    for key in STANDARD_JOIN_KEYS.keys():
        if key not in df.columns:
            continue

        dtype = str(df[key].dtype)
        null_count = df[key].isna().sum()
        null_pct = 100 * null_count / len(df) if len(df) > 0 else 0

        # Get sample non-null values
        non_null = df[key].dropna()
        sample_values = list(non_null.head(3)) if len(non_null) > 0 else []

        info[key] = {
            "dtype": dtype,
            "null_count": null_count,
            "null_pct": null_pct,
            "sample_values": sample_values,
            "expected_dtype": STANDARD_JOIN_KEYS[key],
            "correct_type": dtype == STANDARD_JOIN_KEYS[key],
        }

    return info
