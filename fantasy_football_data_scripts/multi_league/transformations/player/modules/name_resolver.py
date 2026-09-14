"""
Name Resolver Module

Handles player name canonicalization and restoration after data merges.

This module:
- Tracks all name variations for each player (based on nfl_player_id and yahoo_player_id)
- Creates canonical name mappings (prefer Yahoo names when available)
- Restores original names after merges (avoiding normalized/cleaned names)
- Provides name history for debugging conflicts

Strategy:
1. For players with yahoo_player_id: Use Yahoo name (most accurate for rostered players)
2. For players without yahoo_player_id: Use NFL name (best available for unrostered)
3. For players with multiple name variations: Track all variations and choose canonical

Usage:
    import polars as pl
    from name_resolver import resolve_player_names, get_name_history

    # Resolve names in a DataFrame
    df = resolve_player_names(df, prefer_yahoo=True)

    # Get name history for a specific player
    history = get_name_history(df, nfl_player_id="12345")
"""

import polars as pl
import pandas as pd


def build_name_mapping(
    df: pl.DataFrame, nfl_id_col: str = "NFL_player_id", yahoo_id_col: str = "yahoo_player_id", name_col: str = "player"
) -> dict[str, dict[str, any]]:
    """
    Build a mapping of player IDs to canonical names and name variations.

    For each unique nfl_player_id, this finds:
    - All name variations (from different data sources and years)
    - The canonical name (prefer Yahoo when available)
    - The yahoo_player_id association (if exists)

    Args:
        df: DataFrame with player data
        nfl_id_col: Column name for NFL player ID
        yahoo_id_col: Column name for Yahoo player ID
        name_col: Column name for player name

    Returns:
        Dict mapping nfl_player_id to:
            {
                "canonical_name": str,
                "name_variations": List[str],
                "yahoo_player_id": str or None,
                "name_source": "yahoo" or "nfl"
            }
    """
    # Convert to pandas for easier grouping
    pdf = df.to_pandas() if isinstance(df, pl.DataFrame) else df.copy()

    # Ensure required columns exist
    if nfl_id_col not in pdf.columns:
        return {}
    if name_col not in pdf.columns:
        return {}

    mapping = {}

    # Group by nfl_player_id
    grouped = pdf.groupby(nfl_id_col)

    for nfl_id, group in grouped:
        if pd.isna(nfl_id) or str(nfl_id).strip() == "":
            continue

        # Get all unique names for this player (remove NaN and empty strings)
        all_names = group[name_col].dropna().astype(str)
        all_names = all_names[all_names.str.strip() != ""]
        name_variations = sorted(all_names.unique().tolist())

        # Get yahoo_player_id if available
        yahoo_ids = (
            group[yahoo_id_col].dropna().astype(str) if yahoo_id_col in group.columns else pd.Series([], dtype=str)
        )
        yahoo_ids = yahoo_ids[yahoo_ids.str.strip() != ""]
        yahoo_id = yahoo_ids.iloc[0] if len(yahoo_ids) > 0 else None

        # Determine canonical name:
        # 1. Prefer names from rows with yahoo_player_id (rostered players)
        # 2. Otherwise use most common name
        # 3. Fall back to first alphabetical name

        canonical_name = None
        name_source = "nfl"

        if yahoo_id and yahoo_id_col in group.columns:
            # Get names from rows with yahoo_player_id
            yahoo_rows = group[group[yahoo_id_col].notna() & (group[yahoo_id_col].astype(str).str.strip() != "")]
            if len(yahoo_rows) > 0:
                yahoo_names = yahoo_rows[name_col].dropna().astype(str)
                yahoo_names = yahoo_names[yahoo_names.str.strip() != ""]
                if len(yahoo_names) > 0:
                    # Use most common Yahoo name
                    canonical_name = yahoo_names.mode().iloc[0] if len(yahoo_names.mode()) > 0 else yahoo_names.iloc[0]
                    name_source = "yahoo"

        # Fall back to most common name across all rows
        if canonical_name is None and len(all_names) > 0:
            canonical_name = all_names.mode().iloc[0] if len(all_names.mode()) > 0 else all_names.iloc[0]

        # Store mapping
        if canonical_name:
            mapping[str(nfl_id)] = {
                "canonical_name": canonical_name,
                "name_variations": name_variations,
                "yahoo_player_id": yahoo_id,
                "name_source": name_source,
            }

    return mapping


def resolve_player_names(
    df: pl.DataFrame,
    nfl_id_col: str = "NFL_player_id",
    yahoo_id_col: str = "yahoo_player_id",
    name_col: str = "player",
    prefer_yahoo: bool = True,
    preserve_original: bool = True,
) -> pl.DataFrame:
    """
    Resolve player names to canonical form based on player IDs.

    This function:
    1. Builds a name mapping from all rows (tracks name variations)
    2. Restores original names (Yahoo preferred, NFL fallback)
    3. Optionally preserves the pre-merge name in a backup column

    Args:
        df: DataFrame with player data
        nfl_id_col: Column name for NFL player ID
        yahoo_id_col: Column name for Yahoo player ID
        name_col: Column name for player name to resolve
        prefer_yahoo: Whether to prefer Yahoo names over NFL names (default: True)
        preserve_original: Whether to keep the original merged name in a column (default: True)

    Returns:
        DataFrame with resolved player names
    """
    # Build name mapping
    name_mapping = build_name_mapping(df, nfl_id_col, yahoo_id_col, name_col)

    if not name_mapping:
        print("[name_resolver] No name mapping built (missing required columns)")
        return df

    # Convert to pandas for easier manipulation
    pdf = df.to_pandas() if isinstance(df, pl.DataFrame) else df.copy()

    # Preserve original merged name if requested
    if preserve_original and f"{name_col}_merged" not in pdf.columns:
        pdf[f"{name_col}_merged"] = pdf[name_col].copy()

    # Create a mapping dataframe for joining
    mapping_records = []
    for nfl_id, info in name_mapping.items():
        mapping_records.append(
            {
                nfl_id_col: nfl_id,
                f"{name_col}_canonical": info["canonical_name"],
                f"{name_col}_source": info["name_source"],
                f"{name_col}_variations": "|".join(info["name_variations"]),
            }
        )

    if not mapping_records:
        print("[name_resolver] No mapping records created")
        return df

    mapping_df = pd.DataFrame(mapping_records)

    # Ensure nfl_id_col is string type for joining
    pdf[nfl_id_col] = pdf[nfl_id_col].astype(str)
    mapping_df[nfl_id_col] = mapping_df[nfl_id_col].astype(str)

    # Join mapping to main dataframe
    pdf = pdf.merge(mapping_df, on=nfl_id_col, how="left", suffixes=("", "_mapping"))

    # Restore canonical names where mapping exists
    has_canonical = pdf[f"{name_col}_canonical"].notna()

    if prefer_yahoo:
        # Only use canonical name if it came from Yahoo source (for rostered players)
        # Otherwise keep the existing name (for unrostered NFL players)
        use_canonical = has_canonical & (pdf[f"{name_col}_source"] == "yahoo")
    else:
        # Use canonical name for all players with a mapping
        use_canonical = has_canonical

    # Update player name column
    pdf.loc[use_canonical, name_col] = pdf.loc[use_canonical, f"{name_col}_canonical"]

    # Log results
    updated_count = use_canonical.sum()
    yahoo_source_count = (pdf[f"{name_col}_source"] == "yahoo").sum() if f"{name_col}_source" in pdf.columns else 0
    nfl_source_count = (pdf[f"{name_col}_source"] == "nfl").sum() if f"{name_col}_source" in pdf.columns else 0

    print(f"[name_resolver] Resolved {updated_count:,} player names")
    print(f"[name_resolver]   From Yahoo: {yahoo_source_count:,}")
    print(f"[name_resolver]   From NFL: {nfl_source_count:,}")

    # Clean up temporary columns (keep _variations for debugging if needed)
    cleanup_cols = [f"{name_col}_canonical", f"{name_col}_source"]
    for col in cleanup_cols:
        if col in pdf.columns:
            pdf = pdf.drop(columns=[col])

    # Convert back to polars if input was polars
    if isinstance(df, pl.DataFrame):
        return pl.from_pandas(pdf)

    return pdf


def get_name_history(
    df: pl.DataFrame,
    nfl_player_id: str | None = None,
    yahoo_player_id: str | None = None,
    nfl_id_col: str = "NFL_player_id",
    yahoo_id_col: str = "yahoo_player_id",
    name_col: str = "player",
) -> pd.DataFrame:
    """
    Get name history for a specific player (all name variations across years).

    Useful for debugging name conflicts and understanding how a player's name
    has appeared in different data sources over time.

    Args:
        df: DataFrame with player data
        nfl_player_id: NFL player ID to look up
        yahoo_player_id: Yahoo player ID to look up
        nfl_id_col: Column name for NFL player ID
        yahoo_id_col: Column name for Yahoo player ID
        name_col: Column name for player name

    Returns:
        DataFrame with columns: year, name, source (yahoo/nfl), is_rostered
    """
    # Convert to pandas for easier filtering
    pdf = df.to_pandas() if isinstance(df, pl.DataFrame) else df.copy()

    # Filter to player rows
    if nfl_player_id:
        player_rows = pdf[pdf[nfl_id_col].astype(str) == str(nfl_player_id)]
    elif yahoo_player_id:
        player_rows = pdf[pdf[yahoo_id_col].astype(str) == str(yahoo_player_id)]
    else:
        raise ValueError("Must provide either nfl_player_id or yahoo_player_id")

    if player_rows.empty:
        print("[name_resolver] No rows found for player ID")
        return pd.DataFrame()

    # Build history
    history_records = []

    for _, row in player_rows.iterrows():
        year = row.get("year", "Unknown")
        name = row.get(name_col, "Unknown")
        has_yahoo_id = pd.notna(row.get(yahoo_id_col)) and str(row.get(yahoo_id_col)).strip() != ""
        is_rostered = row.get("manager", "Unrostered") != "Unrostered"

        history_records.append(
            {
                "year": year,
                "name": name,
                "source": "yahoo" if has_yahoo_id else "nfl",
                "is_rostered": is_rostered,
                "yahoo_player_id": row.get(yahoo_id_col),
                "nfl_player_id": row.get(nfl_id_col),
            }
        )

    history_df = pd.DataFrame(history_records)

    # Group by year/name to show unique combinations
    summary = history_df.groupby(["year", "name", "source"]).agg({"is_rostered": "any"}).reset_index()

    # Sort by year descending (most recent first)
    summary = summary.sort_values("year", ascending=False)

    print("\n[name_resolver] Name history for player:")
    print(f"  NFL ID: {nfl_player_id or 'N/A'}")
    print(f"  Yahoo ID: {yahoo_player_id or 'N/A'}")
    print(f"  Total variations: {len(summary)}")
    print("\nName variations by year:")
    for _, row in summary.iterrows():
        roster_status = "ROSTERED" if row["is_rostered"] else "unrostered"
        print(f"  {row['year']}: '{row['name']}' (source: {row['source']}, {roster_status})")

    return summary


def add_name_variations_column(
    df: pl.DataFrame, nfl_id_col: str = "NFL_player_id", yahoo_id_col: str = "yahoo_player_id", name_col: str = "player"
) -> pl.DataFrame:
    """
    Add a column showing all name variations for each player.

    This is useful for debugging and understanding name conflicts.
    Creates a pipe-delimited string of all names seen for each nfl_player_id.

    Args:
        df: DataFrame with player data
        nfl_id_col: Column name for NFL player ID
        yahoo_id_col: Column name for Yahoo player ID
        name_col: Column name for player name

    Returns:
        DataFrame with new column: player_name_variations
    """
    # Build name mapping
    name_mapping = build_name_mapping(df, nfl_id_col, yahoo_id_col, name_col)

    if not name_mapping:
        print("[name_resolver] No name mapping built (missing required columns)")
        return df

    # Convert to pandas for easier manipulation
    pdf = df.to_pandas() if isinstance(df, pl.DataFrame) else df.copy()

    # Create variations mapping
    variations_map = {nfl_id: "|".join(info["name_variations"]) for nfl_id, info in name_mapping.items()}

    # Add variations column
    pdf["player_name_variations"] = pdf[nfl_id_col].astype(str).map(variations_map)

    # Count unique variations
    unique_variations = pdf["player_name_variations"].notna().sum()
    avg_variations = pdf["player_name_variations"].str.count(r"\|").mean() + 1 if unique_variations > 0 else 0

    print("[name_resolver] Added name variations column")
    print(f"[name_resolver]   Players with variations: {unique_variations:,}")
    print(f"[name_resolver]   Avg variations per player: {avg_variations:.1f}")

    # Convert back to polars if input was polars
    if isinstance(df, pl.DataFrame):
        return pl.from_pandas(pdf)

    return pdf


def canonicalize_names_by_id(
    df,
    nfl_id_col: str = "NFL_player_id",
    yahoo_id_col: str = "yahoo_player_id",
    name_col: str = "player",
    add_debug_cols: bool = False,
):
    """
    Canonicalize player names across all years based on NFL_player_id.

    For players who appear with yahoo_player_id in ANY year, this applies
    that Yahoo name to ALL rows with the same NFL_player_id (including
    historical unrostered years).

    Example:
        Tom Brady rostered 2014-2020 (Yahoo name: "Tom Brady")
        → His 1999-2013 unrostered stats also use "Tom Brady"
        (instead of varying NFL names like "Thomas Brady")

    This solves the problem where the same player has different names
    in different years, making it hard to track their full history.

    Args:
        df: DataFrame (pandas or polars) with player data
        nfl_id_col: Column name for NFL player ID
        yahoo_id_col: Column name for Yahoo player ID
        name_col: Column name for player name to canonicalize
        add_debug_cols: Add debugging columns (name_source, name_variations)

    Returns:
        DataFrame with canonicalized player names
    """
    # Convert to pandas for processing
    is_polars = False
    try:
        import polars as _pl

        if isinstance(df, _pl.DataFrame):
            is_polars = True
            pdf = df.to_pandas()
        else:
            pdf = df.copy()
    except ImportError:
        pdf = df.copy()

    # Build name mapping
    name_mapping = build_name_mapping(pdf, nfl_id_col, yahoo_id_col, name_col)

    if not name_mapping:
        print("[canonicalize] No name mapping built (missing required columns)")
        return df

    # Apply canonical names to all rows with each NFL_player_id
    updates = 0
    for nfl_id, info in name_mapping.items():
        mask = pdf[nfl_id_col].astype(str) == str(nfl_id)
        if mask.sum() > 0:
            pdf.loc[mask, name_col] = info["canonical_name"]
            updates += mask.sum()

            if add_debug_cols:
                pdf.loc[mask, f"{name_col}_source"] = info["name_source"]
                pdf.loc[mask, f"{name_col}_variations"] = "|".join(info["name_variations"])

    # Log results
    yahoo_source = sum(1 for info in name_mapping.values() if info["name_source"] == "yahoo")
    nfl_source = len(name_mapping) - yahoo_source

    print(f"[canonicalize] Canonicalized {updates:,} player name records")
    print(f"[canonicalize]   {len(name_mapping):,} unique players")
    print(f"[canonicalize]   {yahoo_source:,} using Yahoo names (preferred)")
    print(f"[canonicalize]   {nfl_source:,} using NFL names (no Yahoo data)")

    # Show examples of multi-year players
    example_count = 0
    for nfl_id, info in list(name_mapping.items())[:5]:
        player_rows = pdf[pdf[nfl_id_col].astype(str) == str(nfl_id)]
        if len(player_rows) > 1 and "year" in player_rows.columns:
            years = player_rows["year"].dropna().unique()
            if len(years) > 1:
                year_range = f"{min(years)}-{max(years)}"
                num_variations = len(info["name_variations"])
                if num_variations > 1:
                    print(f"  Example: {info['canonical_name']} ({year_range}, {num_variations} name variations)")
                    example_count += 1
                if example_count >= 3:
                    break

    # Convert back to original format
    if is_polars:
        return _pl.from_pandas(pdf)
    return pdf


def merge_name_sources(
    yahoo_df: pd.DataFrame,
    nfl_df: pd.DataFrame,
    yahoo_name_col: str = "player",
    nfl_name_col: str = "player",
    yahoo_id_col: str = "yahoo_player_id",
    nfl_id_col: str = "NFL_player_id",
) -> pd.DataFrame:
    """
    Merge Yahoo and NFL data sources with intelligent name handling.

    This is a helper for the yahoo_nfl_merge.py script to better handle
    name conflicts during the initial merge.

    Strategy:
    1. Prefer Yahoo names for rostered players (more accurate)
    2. Use NFL names for unrostered players
    3. Track all name variations for debugging

    Args:
        yahoo_df: DataFrame from Yahoo API (rostered players)
        nfl_df: DataFrame from NFLverse (all players)
        yahoo_name_col: Column name for player name in Yahoo data
        nfl_name_col: Column name for player name in NFL data
        yahoo_id_col: Column name for Yahoo player ID
        nfl_id_col: Column name for NFL player ID

    Returns:
        Merged DataFrame with resolved names
    """
    # Store original names before merge
    yahoo_df[f"{yahoo_name_col}_original_yahoo"] = yahoo_df[yahoo_name_col].copy()
    nfl_df[f"{nfl_name_col}_original_nfl"] = nfl_df[nfl_name_col].copy()

    # Merge dataframes (implementation depends on merge strategy)
    # This is a placeholder - actual merge logic would go here
    # For now, just return yahoo_df as example

    merged_df = yahoo_df.copy()

    # Restore original Yahoo names (preferred)
    merged_df[yahoo_name_col] = merged_df[f"{yahoo_name_col}_original_yahoo"]

    print(f"[name_resolver] Merged {len(merged_df):,} rows with Yahoo-preferred names")

    return merged_df
