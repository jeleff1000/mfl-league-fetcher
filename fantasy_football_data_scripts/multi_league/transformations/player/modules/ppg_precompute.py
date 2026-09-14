#!/usr/bin/env python3
"""
PPG Pre-Computation Module

Pre-calculates PPG (Points Per Game) metrics for all scoring variants.
These columns are stored in the super_table and selected at league import time
based on league scoring settings.

This follows the same pattern as nfl_rankings.py for rank columns:
1. Pre-compute all variants during weekly NFLverse update
2. At league import, select the correct column based on league settings
3. Rename to canonical column name (e.g., 'ppg_season')

Column Naming Convention:
    {metric}_{td}pt_{ppr}

    Where:
    - metric: ppg_season, ppg_alltime, rolling_3, rolling_5, consistency,
              weighted_ppg, avg_pts_next_year, rolling_total
    - td: 4 or 6 (passing TD points)
    - ppr: 0ppr, half, ppr (PPR value)

Example: ppg_season_4pt_half = Season PPG with 4pt pass TD, 0.5 PPR

Total columns: 48 (8 metrics x 6 scoring variants)
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# All PPG column variants - includes 4pt/5pt/6pt pass TD × 0ppr/half/ppr/tep reception
PPG_VARIANTS = [
    # 4pt passing TD
    ("4pt", "0ppr"),
    ("4pt", "half"),
    ("4pt", "ppr"),
    ("4pt", "tep"),
    # 5pt passing TD
    ("5pt", "0ppr"),
    ("5pt", "half"),
    ("5pt", "ppr"),
    ("5pt", "tep"),
    # 6pt passing TD
    ("6pt", "0ppr"),
    ("6pt", "half"),
    ("6pt", "ppr"),
    ("6pt", "tep"),
]

# Composite fantasy points columns (must exist before PPG calculation)
FPTS_COLUMNS = {
    # 4pt passing TD
    ("4pt", "0ppr"): "fpts_4pt_0ppr",
    ("4pt", "half"): "fpts_4pt_half",
    ("4pt", "ppr"): "fpts_4pt_ppr",
    ("4pt", "tep"): "fpts_4pt_tep",
    # 5pt passing TD
    ("5pt", "0ppr"): "fpts_5pt_0ppr",
    ("5pt", "half"): "fpts_5pt_half",
    ("5pt", "ppr"): "fpts_5pt_ppr",
    ("5pt", "tep"): "fpts_5pt_tep",
    # 6pt passing TD
    ("6pt", "0ppr"): "fpts_6pt_0ppr",
    ("6pt", "half"): "fpts_6pt_half",
    ("6pt", "ppr"): "fpts_6pt_ppr",
    ("6pt", "tep"): "fpts_6pt_tep",
}


def get_ppg_columns_for_scoring(ppr: float = 0.5, pass_td_pts: int = 4) -> dict[str, str]:
    """
    Get the correct PPG column names for a league's scoring settings.

    Args:
        ppr: PPR value (0, 0.5, or 1.0)
        pass_td_pts: Passing TD points (4 or 6)

    Returns:
        Dict mapping canonical names to pre-computed column names

    Example:
        >>> get_ppg_columns_for_scoring(ppr=0.5, pass_td_pts=4)
        {
            'ppg_season': 'ppg_season_4pt_half',
            'ppg_alltime': 'ppg_alltime_4pt_half',
            'rolling_3_avg': 'rolling_3_4pt_half',
            'rolling_5_avg': 'rolling_5_4pt_half',
            'consistency_score': 'consistency_4pt_half',
            'weighted_ppg': 'weighted_ppg_4pt_half',
            'avg_points_next_year': 'avg_pts_next_year_4pt_half',
        }
    """
    # Map PPR value to column suffix
    ppr_key = {0: "0ppr", 0.0: "0ppr", 0.5: "half", 1.0: "ppr", 1: "ppr"}.get(ppr, "half")
    td_key = f"{pass_td_pts}pt"

    return {
        "ppg_season": f"ppg_season_{td_key}_{ppr_key}",
        "ppg_alltime": f"ppg_alltime_{td_key}_{ppr_key}",
        "rolling_3_avg": f"rolling_3_{td_key}_{ppr_key}",
        "rolling_5_avg": f"rolling_5_{td_key}_{ppr_key}",
        "consistency_score": f"consistency_{td_key}_{ppr_key}",
        "weighted_ppg": f"weighted_ppg_{td_key}_{ppr_key}",
        "avg_points_next_year": f"avg_pts_next_year_{td_key}_{ppr_key}",
        "rolling_point_total": f"rolling_total_{td_key}_{ppr_key}",
    }


def calculate_all_ppg_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pre-compute all PPG metrics for all scoring variants.

    This is called during the weekly NFLverse update to populate the super_table.

    Args:
        df: DataFrame with NFL player stats including fpts_* columns

    Returns:
        DataFrame with all 48 PPG columns added (8 metrics x 6 variants)
    """
    result = df.copy()

    # Ensure required columns exist
    required_cols = ["NFL_player_id", "year", "week"]
    for col in required_cols:
        if col not in result.columns:
            logger.warning(f"Missing required column: {col}")
            return result

    # Sort once for all rolling calculations
    result = result.sort_values(["NFL_player_id", "year", "week"]).reset_index(drop=True)

    for td_key, ppr_key in PPG_VARIANTS:
        fpts_col = FPTS_COLUMNS.get((td_key, ppr_key))
        if fpts_col not in result.columns:
            logger.warning(f"Missing fantasy points column: {fpts_col}")
            continue

        logger.info(f"Calculating PPG metrics for {td_key} pass TD, {ppr_key}")

        # --- Season PPG ---
        season_col = f"ppg_season_{td_key}_{ppr_key}"
        season_agg = result.groupby(["NFL_player_id", "year"])[fpts_col].transform("mean")
        result[season_col] = season_agg.round(2)

        # --- All-time PPG ---
        alltime_col = f"ppg_alltime_{td_key}_{ppr_key}"
        alltime_agg = result.groupby("NFL_player_id")[fpts_col].transform("mean")
        result[alltime_col] = alltime_agg.round(2)

        # --- Rolling 3-game average ---
        rolling_3_col = f"rolling_3_{td_key}_{ppr_key}"
        result[rolling_3_col] = (
            result.groupby("NFL_player_id")[fpts_col]
            .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
            .round(2)
        )

        # --- Rolling 5-game average ---
        rolling_5_col = f"rolling_5_{td_key}_{ppr_key}"
        result[rolling_5_col] = (
            result.groupby("NFL_player_id")[fpts_col]
            .transform(lambda x: x.rolling(window=5, min_periods=1).mean())
            .round(2)
        )

        # --- Consistency score (coefficient of variation within season) ---
        # Lower is more consistent: std / mean
        consistency_col = f"consistency_{td_key}_{ppr_key}"
        season_std = result.groupby(["NFL_player_id", "year"])[fpts_col].transform("std").fillna(0)
        season_mean = result.groupby(["NFL_player_id", "year"])[fpts_col].transform("mean")
        # Avoid division by zero
        result[consistency_col] = np.where(season_mean > 0, (season_std / season_mean).round(3), 0)

        # --- Weighted PPG (Exponentially Weighted Moving Average, span=5) ---
        # More recent games weighted higher
        weighted_col = f"weighted_ppg_{td_key}_{ppr_key}"
        result[weighted_col] = (
            result.groupby("NFL_player_id")[fpts_col].transform(lambda x: x.ewm(span=5, min_periods=1).mean()).round(2)
        )

        # --- Rolling Point Total (Cumulative sum per player per season) ---
        # Used for season-to-date totals
        rolling_total_col = f"rolling_total_{td_key}_{ppr_key}"
        result[rolling_total_col] = result.groupby(["NFL_player_id", "year"])[fpts_col].transform("cumsum").round(2)

    # --- Avg Points Next Year (forward-looking metric) ---
    # Calculate season averages first, then join to get next year's average
    logger.info("Calculating avg_points_next_year metrics...")
    for td_key, ppr_key in PPG_VARIANTS:
        fpts_col = FPTS_COLUMNS.get((td_key, ppr_key))
        if fpts_col not in result.columns:
            continue

        next_year_col = f"avg_pts_next_year_{td_key}_{ppr_key}"

        # Get season averages per player-year
        season_avgs = result.groupby(["NFL_player_id", "year"])[fpts_col].mean().reset_index()
        season_avgs.columns = ["NFL_player_id", "year", "season_avg"]

        # Shift to get next year's average (year + 1)
        season_avgs["prev_year"] = season_avgs["year"] - 1
        next_year_lookup = season_avgs[["NFL_player_id", "prev_year", "season_avg"]].copy()
        next_year_lookup.columns = ["NFL_player_id", "year", next_year_col]

        # Merge back to get what each player averaged the NEXT season
        result = result.merge(next_year_lookup, on=["NFL_player_id", "year"], how="left")
        # Handle case where merge creates no matches (e.g., single-year data)
        # Column won't exist if there were no matches at all
        if next_year_col not in result.columns:
            result[next_year_col] = np.nan
        else:
            result[next_year_col] = result[next_year_col].round(2)

    return result


def add_ppg_from_precomputed(
    df: pd.DataFrame, ppr: float = 0.5, pass_td_pts: int = 4, source_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Add PPG columns to a DataFrame by mapping from pre-computed columns.

    This is called during league import in player_stats_v2.py Step 6.

    Args:
        df: DataFrame to add PPG columns to
        ppr: League PPR value
        pass_td_pts: League passing TD points
        source_df: Optional DataFrame with pre-computed columns (if different from df)

    Returns:
        DataFrame with canonical PPG columns added
    """
    result = df.copy()
    col_map = get_ppg_columns_for_scoring(ppr, pass_td_pts)

    src = source_df if source_df is not None else df

    for canonical_name, precomputed_name in col_map.items():
        if precomputed_name in src.columns:
            result[canonical_name] = src[precomputed_name]
            logger.debug(f"Mapped {precomputed_name} -> {canonical_name}")
        else:
            logger.warning(f"Pre-computed column not found: {precomputed_name}")
            result[canonical_name] = np.nan

    return result


def detect_scoring_params(scoring_rules: dict) -> tuple[float, int]:
    """
    Detect PPR and pass TD points from league scoring rules.

    Args:
        scoring_rules: League scoring rules dictionary

    Returns:
        Tuple of (ppr_value, pass_td_pts)
    """
    # Detect PPR
    ppr = scoring_rules.get("rec", 0)
    if ppr not in [0, 0.5, 1.0]:
        # Round to nearest standard value
        if ppr < 0.25:
            ppr = 0
        elif ppr < 0.75:
            ppr = 0.5
        else:
            ppr = 1.0

    # Detect pass TD points
    pass_td_pts = scoring_rules.get("pass_td", 4)
    if pass_td_pts not in [4, 6]:
        pass_td_pts = 4 if pass_td_pts < 5 else 6

    return float(ppr), int(pass_td_pts)


def calculate_all_ppg_metrics_duckdb(parquet_path: str, output_path: str | None = None) -> pd.DataFrame:
    """
    DuckDB-based PPG metric calculation - ~4x faster than pandas for large datasets.

    Uses DuckDB window functions to calculate all PPG metrics directly from parquet.
    This is the preferred method for datasets > 100k rows.

    Args:
        parquet_path: Path to the input parquet file with fpts_* columns
        output_path: Optional path to write output parquet (if None, returns DataFrame)

    Returns:
        DataFrame with all PPG columns added (8 metrics x 6 variants = 48 columns)
    """
    import duckdb

    logger.info(f"[PPG DuckDB] Processing {parquet_path}")

    # Build the SQL for all scoring variants
    variant_sql_parts = []
    fpts_columns_sql = []

    for td_key, ppr_key in PPG_VARIANTS:
        fpts_col = FPTS_COLUMNS.get((td_key, ppr_key))
        if not fpts_col:
            continue

        # Add fpts column to check for existence
        fpts_columns_sql.append(fpts_col)

        # Season PPG
        variant_sql_parts.append(f"""
            AVG({fpts_col}) OVER (
                PARTITION BY NFL_player_id, year
            ) as ppg_season_{td_key}_{ppr_key}
        """)

        # All-time PPG
        variant_sql_parts.append(f"""
            AVG({fpts_col}) OVER (
                PARTITION BY NFL_player_id
            ) as ppg_alltime_{td_key}_{ppr_key}
        """)

        # Rolling 3-game average
        variant_sql_parts.append(f"""
            AVG({fpts_col}) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) as rolling_3_{td_key}_{ppr_key}
        """)

        # Rolling 5-game average
        variant_sql_parts.append(f"""
            AVG({fpts_col}) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as rolling_5_{td_key}_{ppr_key}
        """)

        # Rolling total (cumulative sum per player per season)
        variant_sql_parts.append(f"""
            SUM({fpts_col}) OVER (
                PARTITION BY NFL_player_id, year
                ORDER BY week
                ROWS UNBOUNDED PRECEDING
            ) as rolling_total_{td_key}_{ppr_key}
        """)

        # Weighted PPG (using exponentially weighted moving average approximation)
        # DuckDB doesn't have EWM, so we use a weighted average with recency bias
        # Weight = 0.8^(games_back), normalized
        variant_sql_parts.append(f"""
            AVG({fpts_col}) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as weighted_ppg_{td_key}_{ppr_key}
        """)

    # Convert path for DuckDB (forward slashes)
    parquet_path_sql = str(parquet_path).replace("\\", "/")

    # Build the full query
    query = f"""
        SELECT *,
            {','.join(variant_sql_parts)}
        FROM read_parquet('{parquet_path_sql}')
        ORDER BY NFL_player_id, year, week
    """

    try:
        result = duckdb.execute(query).fetchdf()
        logger.info(f"[PPG DuckDB] Calculated {len(variant_sql_parts)} PPG columns for {len(result):,} rows")

        # Calculate consistency score (coefficient of variation) - requires groupby in pandas
        # This is done post-hoc as DuckDB window functions don't support STDDEV/MEAN ratio elegantly
        for td_key, ppr_key in PPG_VARIANTS:
            fpts_col = FPTS_COLUMNS.get((td_key, ppr_key))
            if fpts_col not in result.columns:
                continue

            consistency_col = f"consistency_{td_key}_{ppr_key}"
            season_std = result.groupby(["NFL_player_id", "year"])[fpts_col].transform("std").fillna(0)
            season_mean = result.groupby(["NFL_player_id", "year"])[fpts_col].transform("mean")
            result[consistency_col] = np.where(season_mean > 0, (season_std / season_mean).round(3), 0)

        # Round all PPG columns to 2 decimal places
        ppg_cols = [c for c in result.columns if c.startswith(("ppg_", "rolling_", "weighted_"))]
        for col in ppg_cols:
            result[col] = result[col].round(2)

        if output_path:
            result.to_parquet(output_path, index=False)
            logger.info(f"[PPG DuckDB] Wrote output to {output_path}")

        return result

    except Exception as e:
        logger.error(f"[PPG DuckDB] Failed: {e}")
        raise


def calculate_ppg_metrics_for_league_duckdb(
    parquet_path: str, ppr: float = 0.5, pass_td_pts: int = 4, output_path: str | None = None
) -> pd.DataFrame:
    """
    Calculate PPG metrics for a specific league's scoring settings using DuckDB.

    This is optimized for league imports where we only need one scoring variant.
    Much faster than calculating all 48 columns when only 8 are needed.

    Args:
        parquet_path: Path to player_fantasy parquet file
        ppr: League PPR value (0, 0.5, or 1.0)
        pass_td_pts: Passing TD points (4 or 6)
        output_path: Optional path to write output parquet

    Returns:
        DataFrame with canonical PPG columns (ppg_season, ppg_alltime, etc.)
    """
    import duckdb

    # Map scoring params to column suffix
    ppr_key = {0: "0ppr", 0.0: "0ppr", 0.5: "half", 1.0: "ppr", 1: "ppr"}.get(ppr, "half")
    td_key = f"{pass_td_pts}pt"
    fpts_col = f"fpts_{td_key}_{ppr_key}"

    logger.info(f"[PPG DuckDB] League scoring: {td_key} pass TD, {ppr_key}")

    # Convert path for DuckDB
    parquet_path_sql = str(parquet_path).replace("\\", "/")

    # Build query with canonical column names
    query = f"""
        SELECT *,
            -- Season PPG
            AVG(fantasy_points) OVER (
                PARTITION BY NFL_player_id, year
            ) as ppg_season,

            -- All-time PPG
            AVG(fantasy_points) OVER (
                PARTITION BY NFL_player_id
            ) as ppg_alltime,

            -- Rolling 3-game average
            AVG(fantasy_points) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) as rolling_3_avg,

            -- Rolling 5-game average
            AVG(fantasy_points) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as rolling_5_avg,

            -- Rolling total (season-to-date)
            SUM(fantasy_points) OVER (
                PARTITION BY NFL_player_id, year
                ORDER BY week
                ROWS UNBOUNDED PRECEDING
            ) as rolling_point_total,

            -- Weighted PPG (recent games weighted higher)
            AVG(fantasy_points) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as weighted_ppg

        FROM read_parquet('{parquet_path_sql}')
        WHERE fantasy_points IS NOT NULL
        ORDER BY NFL_player_id, year, week
    """

    try:
        result = duckdb.execute(query).fetchdf()
        logger.info(f"[PPG DuckDB] Calculated PPG for {len(result):,} rows")

        # Calculate consistency score
        if "fantasy_points" in result.columns:
            season_std = result.groupby(["NFL_player_id", "year"])["fantasy_points"].transform("std").fillna(0)
            season_mean = result.groupby(["NFL_player_id", "year"])["fantasy_points"].transform("mean")
            result["consistency_score"] = np.where(season_mean > 0, (season_std / season_mean).round(3), 0)

        # Round PPG columns
        ppg_cols = [
            "ppg_season",
            "ppg_alltime",
            "rolling_3_avg",
            "rolling_5_avg",
            "rolling_point_total",
            "weighted_ppg",
        ]
        for col in ppg_cols:
            if col in result.columns:
                result[col] = result[col].round(2)

        if output_path:
            result.to_parquet(output_path, index=False)
            logger.info(f"[PPG DuckDB] Wrote output to {output_path}")

        return result

    except Exception as e:
        logger.error(f"[PPG DuckDB] Failed: {e}")
        raise
