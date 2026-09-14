"""
Map league scoring rules to super_table column names.
Only fetch the columns THIS league actually needs.

This module provides utilities to determine which rank and PPG columns
from the super_table are needed for a specific league's scoring rules,
enabling league-specific column selection that reduces memory usage.
"""

from typing import Optional

from multi_league.core.roster_slots import resolve, is_flex


# Supported pass-TD point values for rank columns in the super_table.
_SUPPORTED_TD_PTS = [4, 5, 6]


def _normalize_td_pts(pass_td_pts) -> int:
    """Map pass_td_pts to nearest supported rank column variant (4/5/6)."""
    td_int = int(pass_td_pts)
    if td_int in _SUPPORTED_TD_PTS:
        return td_int
    return min(_SUPPORTED_TD_PTS, key=lambda x: abs(x - td_int))


def get_position_rank_cols(ppr_key: str, td_key: str, idp_scoring: str = "std") -> dict[str, str]:
    """
    Build position → rank-column mapping for a league's scoring settings.

    This is the **single source of truth** for POSITION_RANK_COLS.
    Both sql_base.py and any other consumer should call this function
    rather than duplicating the dict.

    Args:
        ppr_key: PPR suffix — one of '0ppr', 'half', 'ppr'
        td_key: Pass-TD suffix — e.g. '4pt', '5pt', '6pt'
        idp_scoring: IDP scoring variant ('std', 'premium', etc.)

    Returns:
        Dict mapping position codes to rank column names, e.g.
        {"QB": "rank_qb_4pt", "RB": "rank_rb_half", ...}
    """
    return {
        # Offense
        "QB": f"rank_qb_{td_key}",
        "RB": f"rank_rb_{ppr_key}",
        "WR": f"rank_wr_{ppr_key}",
        "TE": f"rank_te_{ppr_key}",
        "K": "rank_k",
        "DEF": "rank_def",
        # IDP — broad categories
        "LB": f"rank_lb_{idp_scoring}",
        "DL": f"rank_dl_{idp_scoring}",
        "DB": f"rank_db_{idp_scoring}",
        # IDP — specific positions (map to parent rank column)
        "ILB": f"rank_lb_{idp_scoring}",
        "OLB": f"rank_lb_{idp_scoring}",
        "MLB": f"rank_lb_{idp_scoring}",
        "DE": f"rank_dl_{idp_scoring}",
        "DT": f"rank_dl_{idp_scoring}",
        "EDGE": f"rank_dl_{idp_scoring}",
        "NT": f"rank_dl_{idp_scoring}",
        "CB": f"rank_db_{idp_scoring}",
        "S": f"rank_db_{idp_scoring}",
        "SS": f"rank_db_{idp_scoring}",
        "FS": f"rank_db_{idp_scoring}",
    }


def get_position_rank_cols_from_scoring(
    ppr: float = 0.5, pass_td_pts: int = 4, idp_scoring: str = "std"
) -> dict[str, str]:
    """
    Convenience wrapper that accepts raw scoring params (ppr float, pass_td_pts int)
    and converts them to the ppr_key/td_key strings needed by get_position_rank_cols.

    Args:
        ppr: Points per reception (0, 0.5, 1.0)
        pass_td_pts: Points per passing TD (4, 5, or 6)
        idp_scoring: IDP scoring variant

    Returns:
        Same dict as get_position_rank_cols()
    """
    ppr_key = {0: "0ppr", 0.5: "half", 1.0: "ppr"}.get(ppr, "half")
    td_key = f"{_normalize_td_pts(pass_td_pts)}pt"
    return get_position_rank_cols(ppr_key, td_key, idp_scoring)


def get_rank_columns_for_league(
    ppr: float, pass_td_pts: int, idp_scoring: str = "std", roster_settings: dict | None = None
) -> list[str]:
    """
    Get ONLY the rank columns needed for this league's scoring rules.

    Each flex type maps to its own rank column in the super_table:
      - FLEX / W/R/T → rank_flex_*       (RB/WR/TE)
      - REC_FLEX / W/T → rank_recflex_*  (WR/TE)
      - WRRB_FLEX / W/R → rank_wrflex_*  (WR/RB)
      - R/T → rank_rtflex_*              (RB/TE)
      - SUPER_FLEX / Q/W/R/T / OP → rank_sflex_{td}_{ppr}  (QB/WR/RB/TE)
      - IDP_FLEX / D → rank_idp_flex_*   (DL/LB/DB)

    Args:
        ppr: Points per reception (0, 0.5, 1.0)
        pass_td_pts: Points per passing TD (4 or 6)
        idp_scoring: IDP scoring variant ('std', 'premium', or None)
        roster_settings: League roster config to know which positions are used

    Returns:
        List of rank column names for this league

    Example:
        ppr=1.0, pass_td_pts=6, roster has W/T + W/R → [
            'rank_qb_6pt', 'rank_rb_ppr', 'rank_wr_ppr', 'rank_te_ppr',
            'rank_recflex_ppr', 'rank_wrflex_ppr', 'rank_k', 'rank_def'
        ]
    """
    # Map PPR to suffix
    ppr_suffix_map = {0: "0ppr", 0.5: "half", 1.0: "ppr"}
    ppr_suffix = ppr_suffix_map.get(ppr, "half")
    td_key = f"{pass_td_pts}pt"

    # Build column list
    cols = []

    # QB (pass TD variant)
    if _position_used(roster_settings, "QB"):
        cols.append(f"rank_qb_{td_key}")

    # Offensive positions (PPR variant)
    for pos in ["RB", "WR", "TE"]:
        if _position_used(roster_settings, pos):
            cols.append(f"rank_{pos.lower()}_{ppr_suffix}")

    # Flex positions — each type maps to a different rank column
    for rank_col in _get_flex_rank_columns(roster_settings, ppr_suffix, td_key, idp_scoring):
        if rank_col not in cols:
            cols.append(rank_col)

    # K and DEF (no scoring variants)
    if _position_used(roster_settings, "K"):
        cols.append("rank_k")
    if _position_used(roster_settings, "DEF"):
        cols.append("rank_def")

    # IDP positions (IDP scoring variant)
    if idp_scoring and idp_scoring != "none":
        for pos in ["DL", "LB", "DB"]:
            if _position_used(roster_settings, pos):
                cols.append(f"rank_{pos.lower()}_{idp_scoring}")

    return cols


def get_ppg_columns_for_league(ppr: float, pass_td_pts: int) -> list[str]:
    """
    Get ONLY the PPG columns needed for this league's scoring variant.

    Args:
        ppr: Points per reception (0, 0.5, 1.0)
        pass_td_pts: Points per passing TD (4 or 6)

    Returns:
        List of PPG column names for this scoring variant

    Example:
        ppr=0.5, pass_td_pts=4 → [
            'ppg_season_4pt_half', 'ppg_alltime_4pt_half',
            'rolling_3_4pt_half', 'rolling_5_4pt_half',
            'consistency_4pt_half', 'weighted_ppg_4pt_half',
            'rolling_total_4pt_half'
        ] = 7 columns instead of 56
    """
    # Map PPR to suffix
    ppr_suffix_map = {0: "0ppr", 0.5: "half", 1.0: "ppr"}
    ppr_suffix = ppr_suffix_map.get(ppr, "half")

    # Build column list
    base_cols = ["ppg_season", "ppg_alltime", "rolling_3", "rolling_5", "consistency", "weighted_ppg", "rolling_total"]

    return [f"{base}_{pass_td_pts}pt_{ppr_suffix}" for base in base_cols]


# Map canonical flex slot names to super_table rank column prefixes.
# Each flex type has its own precomputed rank in the super_table.
# Platform aliases (W/R/T, FLEX, OP, etc.) are resolved via roster_slots.resolve().
_CANONICAL_RANK_PREFIX = {
    "FLX": "rank_flex",
    "REC_FLEX": "rank_recflex",
    "W/R": "rank_wrflex",
    "R/T": "rank_rtflex",
    "SUPER_FLEX": "rank_sflex",
    "IDP": "rank_idp_flex",
}


def get_flex_rank_prefix(slot_name: str) -> str | None:
    """Return the rank column prefix for a roster slot, or None if not a flex slot."""
    return _CANONICAL_RANK_PREFIX.get(resolve(slot_name))


# These prefixes need td_key + ppr_suffix (e.g., rank_sflex_4pt_half)
_SFLEX_PREFIXES = {"rank_sflex"}
# These prefixes need idp_scoring (e.g., rank_idp_flex_std)
_IDP_FLEX_PREFIXES = {"rank_idp_flex"}


def _get_flex_rank_columns(roster_settings: dict | None, ppr_suffix: str, td_key: str, idp_scoring: str) -> list[str]:
    """
    Detect which flex positions exist in roster settings and return the
    correct rank column name for each.

    Args:
        roster_settings: League roster config by year
        ppr_suffix: PPR suffix ('0ppr', 'half', 'ppr')
        td_key: Pass TD key ('4pt' or '6pt')
        idp_scoring: IDP scoring variant ('std', 'premium', etc.)

    Returns:
        List of flex rank column names (deduplicated)
    """
    if not roster_settings:
        # No roster info — default to standard flex
        return [f"rank_flex_{ppr_suffix}"]

    seen_prefixes = set()
    cols = []

    for year_settings in roster_settings.values():
        if not isinstance(year_settings, dict):
            continue
        for slot_name in year_settings:
            slot_upper = slot_name.upper()
            prefix = get_flex_rank_prefix(slot_upper)
            if prefix and prefix not in seen_prefixes:
                seen_prefixes.add(prefix)
                # Build the full column name based on the flex type
                if prefix in _SFLEX_PREFIXES:
                    cols.append(f"{prefix}_{td_key}_{ppr_suffix}")
                elif prefix in _IDP_FLEX_PREFIXES:
                    if idp_scoring and idp_scoring != "none":
                        cols.append(f"{prefix}_{idp_scoring}")
                else:
                    cols.append(f"{prefix}_{ppr_suffix}")

    return cols


def _position_used(roster_settings: dict | None, position: str) -> bool:
    """
    Check if a position is used in this league's roster.

    Args:
        roster_settings: League roster config by year
        position: Position code (e.g., 'QB', 'RB', 'FLEX')

    Returns:
        True if position is used, False otherwise
    """
    if not roster_settings:
        return True  # Assume all positions if no settings

    # Check any year's settings (they should be consistent)
    for year_settings in roster_settings.values():
        if isinstance(year_settings, dict):
            if position in year_settings or position.upper() in year_settings:
                return True
            # Handle FLEX positions — Sleeper uses 'FLEX', Yahoo uses 'W/R/T', 'W/R', 'W/T', etc.
            if position == "FLEX":
                if any(is_flex(k) for k in year_settings):
                    return True

    return False


def get_pts_columns_for_scoring(scoring_columns: dict[str, str]) -> list[str]:
    """
    Get the pts_* component columns needed for fantasy_points calculation.

    These are the pre-calculated category columns in super_table that
    get summed to produce fantasy_points.

    Args:
        scoring_columns: Dict from detect_scoring_variant() or get_scoring_columns()
                        e.g., {'pass': 'pts_pass_4pt', 'rec': 'pts_rec_half', ...}

    Returns:
        List of pts_* column names to fetch from super_table

    Example:
        scoring_cols = {'pass': 'pts_pass_4pt', 'rec': 'pts_rec_half', ...}
        pts_cols = get_pts_columns_for_scoring(scoring_cols)
        # ['pts_pass_4pt', 'pts_rush', 'pts_rec_half', 'pts_misc', 'pts_k_std', 'pts_def_std']
    """
    # Extract the actual column names from scoring_columns dict
    pts_cols = [
        scoring_columns.get("pass", "pts_pass_4pt"),
        scoring_columns.get("rush", "pts_rush"),
        scoring_columns.get("rec", "pts_rec_half"),
        scoring_columns.get("misc", "pts_misc"),
        scoring_columns.get("kick", "pts_k_std"),
        scoring_columns.get("def", "pts_def_std"),
    ]

    # Add IDP if present
    if "idp" in scoring_columns:
        pts_cols.append(scoring_columns["idp"])

    return pts_cols


def get_fpts_column_for_scoring(ppr: float, pass_td_pts: int) -> str:
    """
    Get the pre-calculated total fantasy points column name for scoring variant.

    The super_table has pre-calculated fpts_* columns that already sum
    all category points for common scoring variants.

    Args:
        ppr: Points per reception (0, 0.5, 1.0)
        pass_td_pts: Points per passing TD (4 or 6)

    Returns:
        Column name like 'fpts_4pt_half' for 4pt/0.5PPR scoring

    Example:
        col = get_fpts_column_for_scoring(0.5, 4)  # 'fpts_4pt_half'
        col = get_fpts_column_for_scoring(1.0, 6)  # 'fpts_6pt_ppr'
    """
    ppr_suffix_map = {0: "0ppr", 0.5: "half", 1.0: "ppr"}
    ppr_suffix = ppr_suffix_map.get(ppr, "half")
    return f"fpts_{pass_td_pts}pt_{ppr_suffix}"


def fetch_pts_columns_from_supertable(
    player_weeks: list[str], scoring_columns: dict[str, str], include_fpts: bool = True, logger=None
) -> Optional["pd.DataFrame"]:
    """
    Fetch pts_* and fpts_* columns from super_table for specific player_weeks.

    This is used by quick imports to get fantasy points data for rostered players
    without loading the full NFL pool.

    Args:
        player_weeks: List of player_week identifiers to fetch
        scoring_columns: Dict from detect_scoring_variant() specifying which pts_* cols to use
        include_fpts: Also fetch pre-calculated fpts_* totals (default True)
        logger: Optional logger for debug output

    Returns:
        DataFrame with player_week and pts/fpts columns, or None if fetch fails

    Example:
        scoring_cols = detect_scoring_variant(scoring_rules)
        pts_df = fetch_pts_columns_from_supertable(
            player_weeks=['00-0023459_2025_1', '00-0023460_2025_1'],
            scoring_columns=scoring_cols
        )
    """
    import os
    from pathlib import Path

    if not player_weeks:
        return None

    try:
        import duckdb
        import pandas as pd

        # Try OPS_CACHE_PATH first, then Fly reader, then legacy MotherDuck
        ops_cache = os.environ.get("OPS_CACHE_PATH", "")
        backend = os.environ.get("DATABASE_BACKEND", "fly")

        if ops_cache and Path(ops_cache).exists():
            conn = duckdb.connect()
            conn.execute(f"ATTACH '{ops_cache}' AS \"___ops\" (READ_ONLY)")
        elif backend == "fly":
            if logger:
                logger.warning("[fetch_pts] Fly backend without OPS_CACHE_PATH — cannot fetch pts columns")
            return None
        else:
            token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
            if not token:
                if logger:
                    logger.warning("[fetch_pts] No database token set")
                return None
            conn = duckdb.connect(f"md:?motherduck_token={token}")

        # Build column list
        cols_to_fetch = ["player_week"] + get_pts_columns_for_scoring(scoring_columns)

        # Add fpts columns if requested
        if include_fpts:
            cols_to_fetch.extend(
                ["fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr", "fpts_6pt_0ppr", "fpts_6pt_half", "fpts_6pt_ppr"]
            )

        # Remove duplicates while preserving order
        cols_to_fetch = list(dict.fromkeys(cols_to_fetch))

        if logger:
            logger.info(f"[fetch_pts] Fetching {len(cols_to_fetch)} columns for {len(player_weeks):,} player_weeks")

        # Batch the fetch for large datasets
        batch_size = 5000
        result_dfs = []

        for i in range(0, len(player_weeks), batch_size):
            batch = player_weeks[i : i + batch_size]
            pw_list = ", ".join(f"'{pw}'" for pw in batch)

            query = f"""
                SELECT {', '.join(cols_to_fetch)}
                FROM ___ops.nfl_historical.nfl_player_stats_all
                WHERE player_week IN ({pw_list})
            """

            batch_df = conn.execute(query).fetchdf()
            if not batch_df.empty:
                result_dfs.append(batch_df)

        conn.close()

        if result_dfs:
            result = pd.concat(result_dfs, ignore_index=True)
            if logger:
                logger.info(f"[fetch_pts] Retrieved {len(result):,} rows")
            return result
        else:
            if logger:
                logger.warning("[fetch_pts] No data found in super_table for given player_weeks")
            return None

    except Exception as e:
        if logger:
            logger.warning(f"[fetch_pts] Failed to fetch: {e}")
        return None
