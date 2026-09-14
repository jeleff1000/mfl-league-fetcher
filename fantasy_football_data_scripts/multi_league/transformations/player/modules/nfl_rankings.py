"""
NFL-Wide Rankings Module

Maps league scoring settings to pre-computed rank columns from super_table.
These ranks compare players across the entire NFL (all players, not just rostered).

This module handles:
- Weekly position rankings (rank_qb_4pt, rank_rb_half, etc.)
- Season position rankings (rank_season_qb_4pt, etc.)
- All-time position rankings (rank_alltime_qb_4pt, etc.)
- Flex rankings (rank_flex_half, rank_sflex_4pt_half, etc.)

The ranks are PRE-COMPUTED in the super_table via backfill_ranks.py and
weekly_nfl_update.py. This module simply maps the league's scoring settings
to the appropriate rank columns and aliases them for UI display.

Called in Pass 4 (after SQLEnrichments.expand_to_all_nfl()) on ~660k+ NFL players.
"""

import polars as pl


def get_rank_columns_for_scoring(
    ppr: float = 0.5,
    pass_td_pts: int = 4,
    idp_scoring: str = "std",
    te_premium: bool = False,
    return_yards: bool = False,
) -> dict:
    """
    Get the appropriate rank column names for a league's scoring settings.

    This is the core mapping function that determines which pre-computed
    rank columns to use based on the league's scoring rules.

    Args:
        ppr: Points per reception (0, 0.5, or 1.0)
        pass_td_pts: Points per passing TD (4, 5, or 6)
        idp_scoring: IDP scoring variant ('std', 'premium', 'tackle_heavy', 'big_play')
        te_premium: Whether TEs get bonus PPR (1.5 PPR for TEs)
        return_yards: Whether league awards return yards (1 pt per 25 yards)

    Returns:
        Dict mapping position/flex types to rank column names for:
        - weekly ranks (rank_*)
        - season ranks (rank_season_*)
        - all-time ranks (rank_alltime_*)
    """
    # Map pass TD points to key
    if pass_td_pts >= 6:
        td_key = "6pt"
    elif pass_td_pts >= 5:
        td_key = "5pt"
    else:
        td_key = "4pt"

    if ppr == 0:
        ppr_key = "0ppr"
    elif ppr >= 1.0:
        ppr_key = "ppr"
    else:
        ppr_key = "half"

    # Validate IDP scoring variant
    if idp_scoring not in ("std", "premium", "tackle_heavy", "big_play"):
        idp_scoring = "std"

    # TE and flex keys for TE Premium leagues
    te_rank_key = "tep" if te_premium else ppr_key
    flex_rank_key = "tep" if te_premium else ppr_key

    # Return yards suffix for fpts column
    ret_suffix = "_ret" if return_yards else ""

    # Build fpts column name
    if te_premium:
        fpts_col = f"fpts_{td_key}_tep{ret_suffix}"
    else:
        fpts_col = f"fpts_{td_key}_{ppr_key}{ret_suffix}"

    return {
        # Fantasy points column for this scoring
        "fpts": fpts_col,
        # ===== WEEKLY POSITION RANKS =====
        # Offense
        "weekly_QB": f"rank_qb_{td_key}",
        "weekly_RB": f"rank_rb_{ppr_key}",
        "weekly_WR": f"rank_wr_{ppr_key}",
        "weekly_TE": f"rank_te_{te_rank_key}",
        "weekly_K": "rank_k",
        "weekly_DEF": "rank_def",
        # IDP
        "weekly_LB": f"rank_lb_{idp_scoring}",
        "weekly_DL": f"rank_dl_{idp_scoring}",
        "weekly_DB": f"rank_db_{idp_scoring}",
        # Weekly Flex ranks
        "weekly_FLEX": f"rank_flex_{flex_rank_key}",
        "weekly_W/R/T": f"rank_flex_{flex_rank_key}",
        "weekly_REC_FLEX": f"rank_recflex_{flex_rank_key}",
        "weekly_W/T": f"rank_recflex_{flex_rank_key}",
        "weekly_SUPER_FLEX": f"rank_sflex_{td_key}_{ppr_key}",
        "weekly_Q/W/R/T": f"rank_sflex_{td_key}_{ppr_key}",
        "weekly_IDP_FLEX": f"rank_idp_flex_{idp_scoring}",
        "weekly_W/R": f"rank_wrflex_{ppr_key}",  # RB/WR only
        "weekly_R/T": f"rank_rtflex_{ppr_key}",  # RB/TE only
        # ===== SEASON POSITION RANKS =====
        # Offense
        "season_QB": f"rank_season_qb_{td_key}",
        "season_RB": f"rank_season_rb_{ppr_key}",
        "season_WR": f"rank_season_wr_{ppr_key}",
        "season_TE": f"rank_season_te_{te_rank_key}",
        "season_K": "rank_season_k",
        "season_DEF": "rank_season_def",
        # IDP
        "season_LB": f"rank_season_lb_{idp_scoring}",
        "season_DL": f"rank_season_dl_{idp_scoring}",
        "season_DB": f"rank_season_db_{idp_scoring}",
        # Season Flex ranks
        "season_FLEX": f"rank_season_flex_{flex_rank_key}",
        "season_W/R/T": f"rank_season_flex_{flex_rank_key}",
        "season_REC_FLEX": f"rank_season_recflex_{flex_rank_key}",
        "season_W/T": f"rank_season_recflex_{flex_rank_key}",
        "season_SUPER_FLEX": f"rank_season_sflex_{td_key}_{ppr_key}",
        "season_Q/W/R/T": f"rank_season_sflex_{td_key}_{ppr_key}",
        "season_IDP_FLEX": f"rank_season_idp_flex_{idp_scoring}",
        "season_W/R": f"rank_season_wrflex_{ppr_key}",
        "season_R/T": f"rank_season_rtflex_{ppr_key}",
        # ===== ALL-TIME POSITION RANKS =====
        # Offense
        "alltime_QB": f"rank_alltime_qb_{td_key}",
        "alltime_RB": f"rank_alltime_rb_{ppr_key}",
        "alltime_WR": f"rank_alltime_wr_{ppr_key}",
        "alltime_TE": f"rank_alltime_te_{te_rank_key}",
        "alltime_K": "rank_alltime_k",
        "alltime_DEF": "rank_alltime_def",
        # IDP
        "alltime_LB": f"rank_alltime_lb_{idp_scoring}",
        "alltime_DL": f"rank_alltime_dl_{idp_scoring}",
        "alltime_DB": f"rank_alltime_db_{idp_scoring}",
        # All-time Flex ranks
        "alltime_FLEX": f"rank_alltime_flex_{flex_rank_key}",
        "alltime_W/R/T": f"rank_alltime_flex_{flex_rank_key}",
        "alltime_REC_FLEX": f"rank_alltime_recflex_{flex_rank_key}",
        "alltime_W/T": f"rank_alltime_recflex_{flex_rank_key}",
        "alltime_SUPER_FLEX": f"rank_alltime_sflex_{td_key}_{ppr_key}",
        "alltime_Q/W/R/T": f"rank_alltime_sflex_{td_key}_{ppr_key}",
        "alltime_IDP_FLEX": f"rank_alltime_idp_flex_{idp_scoring}",
        "alltime_W/R": f"rank_alltime_wrflex_{ppr_key}",
        "alltime_R/T": f"rank_alltime_rtflex_{ppr_key}",
    }


def detect_scoring_settings(df: pl.DataFrame, settings_dir: str | None = None) -> tuple:
    """
    Detect league scoring settings from data or settings files.

    Args:
        df: DataFrame with player data
        settings_dir: Path to league settings directory

    Returns:
        Tuple of (ppr, pass_td_pts, idp_scoring)
    """
    # Default settings
    ppr = 0.5
    pass_td_pts = 4
    idp_scoring = "std"

    # Try to detect from settings files if provided
    if settings_dir:
        from pathlib import Path
        import json

        settings_path = Path(settings_dir)
        league_settings_files = list(settings_path.glob("league_settings_*.json"))

        if league_settings_files:
            # Use most recent settings file
            latest_file = sorted(league_settings_files)[-1]
            try:
                with open(latest_file) as f:
                    settings = json.load(f)

                # Sleeper format
                if "scoring_settings" in settings:
                    scoring = settings["scoring_settings"]
                    ppr = scoring.get("rec", 0.5)
                    pass_td_pts = int(scoring.get("pass_td", 4))

                    # Detect IDP variant
                    from multi_league.data_fetchers.fantasy_points_calculator import detect_idp_scoring_variant

                    idp_scoring = detect_idp_scoring_variant(settings)

                # Yahoo format
                elif "scoring_type" in settings:
                    # Yahoo uses stat_modifiers
                    mods = settings.get("stat_modifiers", {})
                    ppr = float(mods.get("11", 0))  # 11 = receptions
                    pass_td_pts = int(mods.get("4", 4))  # 4 = passing TDs

            except (json.JSONDecodeError, KeyError, TypeError):
                pass

    return ppr, pass_td_pts, idp_scoring


def add_nfl_rank_columns(
    df: pl.DataFrame, ppr: float = 0.5, pass_td_pts: int = 4, idp_scoring: str = "std", position_col: str = "position"
) -> pl.DataFrame:
    """
    Add NFL-wide rank columns by copying pre-computed ranks from super_table.

    This function maps the league's scoring settings to the appropriate
    pre-computed rank columns and creates UI-friendly aliases.

    Args:
        df: DataFrame with player data (must have rank columns from super_table)
        ppr: Points per reception (0, 0.5, or 1.0)
        pass_td_pts: Points per passing TD (4 or 6)
        idp_scoring: IDP scoring variant
        position_col: Name of position column

    Returns:
        DataFrame with aliased rank columns for UI display
    """
    rank_cols = get_rank_columns_for_scoring(ppr, pass_td_pts, idp_scoring)

    # Mapping of NFL positions to their group (for IDP)
    IDP_POSITION_MAP = {
        "ILB": "LB",
        "OLB": "LB",
        "MLB": "LB",
        "DE": "DL",
        "DT": "DL",
        "NT": "DL",
        "ED": "DL",
        "CB": "DB",
        "S": "DB",
        "SS": "DB",
        "FS": "DB",
        "SAF": "DB",
    }

    # Create expressions for each rank column we want to add
    new_columns = []

    # === WEEKLY POSITION RANK (generic alias) ===
    # Create position_week_rank by selecting the right rank column based on position
    weekly_rank_expr = pl.lit(None).cast(pl.Int64)
    for pos in ["QB", "RB", "WR", "TE", "K", "DEF", "LB", "DL", "DB"]:
        src_col = rank_cols.get(f"weekly_{pos}")
        if src_col and src_col in df.columns:
            if pos in ["LB", "DL", "DB"]:
                # For IDP, also match granular positions
                pos_list = [pos] + [k for k, v in IDP_POSITION_MAP.items() if v == pos]
                weekly_rank_expr = (
                    pl.when(pl.col(position_col).is_in(pos_list)).then(pl.col(src_col)).otherwise(weekly_rank_expr)
                )
            else:
                weekly_rank_expr = (
                    pl.when(pl.col(position_col) == pos).then(pl.col(src_col)).otherwise(weekly_rank_expr)
                )

    new_columns.append(weekly_rank_expr.alias("position_week_rank"))

    # === SEASON POSITION RANK ===
    season_rank_expr = pl.lit(None).cast(pl.Int64)
    for pos in ["QB", "RB", "WR", "TE", "K", "DEF", "LB", "DL", "DB"]:
        src_col = rank_cols.get(f"season_{pos}")
        if src_col and src_col in df.columns:
            if pos in ["LB", "DL", "DB"]:
                pos_list = [pos] + [k for k, v in IDP_POSITION_MAP.items() if v == pos]
                season_rank_expr = (
                    pl.when(pl.col(position_col).is_in(pos_list)).then(pl.col(src_col)).otherwise(season_rank_expr)
                )
            else:
                season_rank_expr = (
                    pl.when(pl.col(position_col) == pos).then(pl.col(src_col)).otherwise(season_rank_expr)
                )

    new_columns.append(season_rank_expr.alias("position_season_rank"))

    # === ALL-TIME POSITION RANK ===
    alltime_rank_expr = pl.lit(None).cast(pl.Int64)
    for pos in ["QB", "RB", "WR", "TE", "K", "DEF", "LB", "DL", "DB"]:
        src_col = rank_cols.get(f"alltime_{pos}")
        if src_col and src_col in df.columns:
            if pos in ["LB", "DL", "DB"]:
                pos_list = [pos] + [k for k, v in IDP_POSITION_MAP.items() if v == pos]
                alltime_rank_expr = (
                    pl.when(pl.col(position_col).is_in(pos_list)).then(pl.col(src_col)).otherwise(alltime_rank_expr)
                )
            else:
                alltime_rank_expr = (
                    pl.when(pl.col(position_col) == pos).then(pl.col(src_col)).otherwise(alltime_rank_expr)
                )

    new_columns.append(alltime_rank_expr.alias("position_alltime_rank"))

    # === FLEX RANKS (for flex-eligible positions) ===
    flex_positions = ["RB", "WR", "TE"]
    flex_weekly_col = rank_cols.get("weekly_FLEX")
    if flex_weekly_col and flex_weekly_col in df.columns:
        new_columns.append(
            pl.when(pl.col(position_col).is_in(flex_positions))
            .then(pl.col(flex_weekly_col))
            .otherwise(None)
            .alias("flex_week_rank")
        )

    flex_season_col = rank_cols.get("season_FLEX")
    if flex_season_col and flex_season_col in df.columns:
        new_columns.append(
            pl.when(pl.col(position_col).is_in(flex_positions))
            .then(pl.col(flex_season_col))
            .otherwise(None)
            .alias("flex_season_rank")
        )

    flex_alltime_col = rank_cols.get("alltime_FLEX")
    if flex_alltime_col and flex_alltime_col in df.columns:
        new_columns.append(
            pl.when(pl.col(position_col).is_in(flex_positions))
            .then(pl.col(flex_alltime_col))
            .otherwise(None)
            .alias("flex_alltime_rank")
        )

    # === SUPERFLEX RANKS (for superflex-eligible positions) ===
    sflex_positions = ["QB", "RB", "WR", "TE"]
    sflex_weekly_col = rank_cols.get("weekly_SUPER_FLEX")
    if sflex_weekly_col and sflex_weekly_col in df.columns:
        new_columns.append(
            pl.when(pl.col(position_col).is_in(sflex_positions))
            .then(pl.col(sflex_weekly_col))
            .otherwise(None)
            .alias("sflex_week_rank")
        )

    sflex_season_col = rank_cols.get("season_SUPER_FLEX")
    if sflex_season_col and sflex_season_col in df.columns:
        new_columns.append(
            pl.when(pl.col(position_col).is_in(sflex_positions))
            .then(pl.col(sflex_season_col))
            .otherwise(None)
            .alias("sflex_season_rank")
        )

    sflex_alltime_col = rank_cols.get("alltime_SUPER_FLEX")
    if sflex_alltime_col and sflex_alltime_col in df.columns:
        new_columns.append(
            pl.when(pl.col(position_col).is_in(sflex_positions))
            .then(pl.col(sflex_alltime_col))
            .otherwise(None)
            .alias("sflex_alltime_rank")
        )

    # Apply all new columns
    if new_columns:
        df = df.with_columns(new_columns)

    return df


def calculate_nfl_rankings(
    df: pl.DataFrame,
    ppr: float = 0.5,
    pass_td_pts: int = 4,
    idp_scoring: str = "std",
    position_col: str = "position",
    settings_dir: str | None = None,
) -> pl.DataFrame:
    """
    Add NFL-wide rankings using pre-computed rank columns from super_table.

    This is the main entry point for NFL rankings.
    Should be called AFTER SQLEnrichments.expand_to_all_nfl() on all NFL players.

    Unlike the old approach that calculated rankings at runtime, this function
    uses pre-computed rank columns from the super_table and simply aliases them
    for UI display. This is significantly faster and more consistent.

    Args:
        df: DataFrame with all NFL player data (~660k+ rows)
            Must have rank columns joined from super_table
        ppr: Points per reception (0, 0.5, or 1.0)
        pass_td_pts: Points per passing TD (4 or 6)
        idp_scoring: IDP scoring variant ('std', 'premium', 'tackle_heavy', 'big_play')
        position_col: Position column name
        settings_dir: Optional path to league settings for auto-detection

    Returns:
        DataFrame with NFL-wide ranking columns added:
        - position_week_rank: Weekly rank within position
        - position_season_rank: Season rank within position
        - position_alltime_rank: Career rank within position
        - flex_week_rank: Weekly rank among flex-eligible (RB/WR/TE)
        - flex_season_rank: Season rank among flex-eligible
        - flex_alltime_rank: Career rank among flex-eligible
        - sflex_week_rank: Weekly rank among superflex-eligible (QB/RB/WR/TE)
        - sflex_season_rank: Season rank among superflex-eligible
        - sflex_alltime_rank: Career rank among superflex-eligible
    """
    from multi_league.core.logging_config import get_logger

    logger = get_logger(__name__)

    logger.info("[NFL RANKINGS] Starting NFL-wide rankings (using pre-computed columns)...")
    logger.info(f"[NFL RANKINGS] Input: {len(df):,} rows")

    # Auto-detect scoring settings if settings_dir provided
    if settings_dir:
        detected_ppr, detected_td, detected_idp = detect_scoring_settings(df, settings_dir)
        ppr = detected_ppr
        pass_td_pts = detected_td
        idp_scoring = detected_idp
        logger.info(f"[NFL RANKINGS] Detected scoring: {ppr} PPR, {pass_td_pts}pt pass TD, {idp_scoring} IDP")
    else:
        logger.info(f"[NFL RANKINGS] Using scoring: {ppr} PPR, {pass_td_pts}pt pass TD, {idp_scoring} IDP")

    # Check if we have pre-computed rank columns
    rank_cols = get_rank_columns_for_scoring(ppr, pass_td_pts, idp_scoring)
    sample_col = rank_cols.get("weekly_QB")

    if sample_col not in df.columns:
        logger.warning(f"[NFL RANKINGS] Pre-computed rank column '{sample_col}' not found!")
        logger.warning(
            "[NFL RANKINGS] Available rank columns: " + str([c for c in df.columns if "rank" in c.lower()][:10])
        )
        logger.warning("[NFL RANKINGS] Skipping NFL rankings (columns not available)")

        # Add empty columns so downstream code doesn't break
        empty_cols = [
            "position_week_rank",
            "position_season_rank",
            "position_alltime_rank",
            "flex_week_rank",
            "flex_season_rank",
            "flex_alltime_rank",
            "sflex_week_rank",
            "sflex_season_rank",
            "sflex_alltime_rank",
            # Also add the alias column names expected by downstream SQL enrichments
            "position_rank",
            "position_rank_season",
            "position_rank_ppg",
        ]
        for col in empty_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(col))

        return df

    # Add rank columns by mapping pre-computed columns
    logger.info("[NFL RANKINGS] Mapping pre-computed rank columns...")
    df = add_nfl_rank_columns(df, ppr=ppr, pass_td_pts=pass_td_pts, idp_scoring=idp_scoring, position_col=position_col)

    # Add alternative column names expected by downstream SQL enrichments
    # (canonical player_fantasy DDL exposes position_rank, position_rank_season, position_rank_ppg)
    alias_mapping = {
        "position_week_rank": "position_rank",
        "position_season_rank": "position_rank_season",
        "position_alltime_rank": "position_rank_ppg",  # PPG-based is career/all-time rank
    }
    for src_col, alias_col in alias_mapping.items():
        if src_col in df.columns and alias_col not in df.columns:
            df = df.with_columns(pl.col(src_col).alias(alias_col))

    # Log sample of ranks for verification
    if position_col in df.columns and "position_week_rank" in df.columns:
        sample = df.filter((pl.col(position_col) == "QB") & pl.col("position_week_rank").is_not_null()).head(3)
        if len(sample) > 0:
            logger.info(
                "[NFL RANKINGS] Sample QB ranks: "
                + str(
                    sample.select(
                        [position_col, "position_week_rank", "position_season_rank", "position_alltime_rank"]
                    ).to_dicts()[:3]
                )
            )

    logger.info(f"[NFL RANKINGS] Complete: {len(df):,} rows with NFL rankings")

    return df
