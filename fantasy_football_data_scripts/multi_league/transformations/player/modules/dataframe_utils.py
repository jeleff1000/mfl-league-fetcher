"""
DataFrame Optimization Utilities

Vectorized operations for large DataFrames (100K+ rows).
All operations batch multiple DataFrame passes into single passes.
"""

import polars as pl


def batch_rename_columns(
    df: pl.DataFrame, column_mappings: dict[str, str], drop_conflicts: bool = True
) -> pl.DataFrame:
    """
    Batch rename multiple columns in a single pass.

    Instead of:
        for old, new in mappings.items():
            if new in df.columns:
                df = df.drop(new)              # PASS 1
            df = df.rename({old: new})          # PASS 2
        # Result: 40-60 DataFrame passes

    Use this:
        df = batch_rename_columns(df, mappings)
        # Result: 2 DataFrame passes (1 drop, 1 rename)

    Args:
        df: DataFrame
        column_mappings: Dict mapping old_name -> new_name
        drop_conflicts: If True, drop new_name if it already exists

    Returns:
        DataFrame with renamed columns
    """
    # Batch drops
    if drop_conflicts:
        drops = [new for old, new in column_mappings.items() if old in df.columns and new in df.columns and old != new]
        if drops:
            df = df.drop(drops)

    # Batch renames
    renames = {old: new for old, new in column_mappings.items() if old in df.columns and old != new}
    if renames:
        df = df.rename(renames)

    return df


def batch_coalesce_columns(df: pl.DataFrame, coalesce_specs: list[tuple[str, list[str]]]) -> pl.DataFrame:
    """
    Batch coalesce multiple column sets in a single pass.

    Instead of:
        if "position" not in df.columns:
            if "nfl_position" in df.columns:
                df = df.with_columns(pl.col("nfl_position").alias("position"))  # PASS 1
            elif "yahoo_position" in df.columns:
                df = df.with_columns(pl.col("yahoo_position").alias("position"))  # PASS 2
        # Result: 6+ DataFrame passes

    Use this:
        df = batch_coalesce_columns(df, [
            ("position", ["position", "nfl_position", "yahoo_position"]),
            ("manager", ["manager", "team_name"]),
        ])
        # Result: 1 DataFrame pass

    Args:
        df: DataFrame
        coalesce_specs: List of (output_col, [fallback_cols]) tuples
            Example: [("position", ["position", "nfl_position", "yahoo_position"])]

    Returns:
        DataFrame with coalesced columns
    """
    exprs = []
    for output_col, source_cols in coalesce_specs:
        # Filter to columns that exist
        existing_cols = [c for c in source_cols if c in df.columns]
        if existing_cols:
            if len(existing_cols) == 1:
                exprs.append(pl.col(existing_cols[0]).alias(output_col))
            else:
                exprs.append(pl.coalesce(existing_cols).alias(output_col))

    if exprs:
        df = df.with_columns(exprs)

    return df


def deduplicate_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Remove duplicate column names in a single pass.

    Instead of:
        col_counts = {}
        for col in df.columns:
            col_counts[col] = col_counts.get(col, 0) + 1
        duplicates = {col: cnt for col, cnt in col_counts.items() if cnt > 1}
        if duplicates:
            seen = set()
            unique_cols = []
            for col in df.columns:
                if col not in seen:
                    seen.add(col)
                    unique_cols.append(col)
            df = df.select(unique_cols)
        # Result: 2 passes over columns

    Use this:
        df = deduplicate_columns(df)
        # Result: 1 pass over columns

    Args:
        df: DataFrame with potential duplicate columns

    Returns:
        DataFrame with deduplicated columns (keeps first occurrence)
    """
    seen = set()
    unique_cols = []
    for col in df.columns:
        if col not in seen:
            unique_cols.append(col)
            seen.add(col)

    if len(unique_cols) < len(df.columns):
        df = df.select(unique_cols)

    return df


def normalize_position_column(
    df: pl.DataFrame, position_col: str = "position", temp_col: str = "_pos_norm"
) -> pl.DataFrame:
    """
    Add a normalized position column (uppercase, trimmed) for efficient filtering.

    Instead of:
        group_df = player_df.filter(
            pl.col(pos_col).str.to_uppercase().str.strip_chars().is_in(group_positions)
        )
        # ... later ...
        remaining_df = player_df.filter(
            ~pl.col(pos_col).str.to_uppercase().str.strip_chars().is_in(all_group_positions)
        )
        # Result: 2-4x repeated expensive string operations

    Use this:
        player_df = normalize_position_column(player_df, position_col=pos_col, temp_col="_pos_norm")
        group_df = player_df.filter(pl.col("_pos_norm").is_in(group_positions))
        remaining_df = player_df.filter(~pl.col("_pos_norm").is_in(all_group_positions))
        player_df = player_df.drop("_pos_norm")
        # Result: 1x string operation (cached for reuse)

    Args:
        df: DataFrame
        position_col: Source position column
        temp_col: Name for normalized column

    Returns:
        DataFrame with normalized position column added
    """
    if position_col in df.columns:
        df = df.with_columns(pl.col(position_col).str.to_uppercase().str.strip_chars().alias(temp_col))
    return df


def filter_by_normalized_position(
    df: pl.DataFrame, allowed_positions: list[str], position_col: str = "position", include_null: bool = True
) -> pl.DataFrame:
    """
    Filter DataFrame by normalized position in a single pass.

    Instead of:
        df = df.with_columns(
            pl.col(pos_col).str.to_uppercase().str.strip_chars().alias("_position_normalized")
        )
        df = df.filter(pl.col("_position_normalized").is_in(list(allowed_positions)))
        df = df.drop("_position_normalized")
        # Result: 3 DataFrame passes

    Use this:
        df = filter_by_normalized_position(df, allowed_positions, position_col=pos_col)
        # Result: 1 DataFrame pass

    Args:
        df: DataFrame
        allowed_positions: List of allowed positions (already uppercase)
        position_col: Position column name
        include_null: Whether to include NULL positions

    Returns:
        Filtered DataFrame
    """
    condition = pl.col(position_col).str.to_uppercase().str.strip_chars().is_in(allowed_positions)

    if include_null:
        condition = condition | pl.col(position_col).is_null()

    return df.filter(condition)
