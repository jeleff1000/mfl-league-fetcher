#!/usr/bin/env python3
"""
Modular Fantasy Points Calculator

Pre-calculates fantasy points BY CATEGORY so leagues can sum the relevant columns.
This handles multi-category players (e.g., Nick Foles passing + receiving in same game).

Category Columns:
- pts_pass_4pt, pts_pass_5pt, pts_pass_6pt (passing with 4/5/6pt TDs, -2 INT)
- pts_pass_4pt_int1, pts_pass_5pt_int1, pts_pass_6pt_int1 (passing with -1 INT)
- pts_rush (rushing - same across all leagues)
- pts_rec_0ppr, pts_rec_half, pts_rec_ppr, pts_rec_tep (receiving with PPR variants)
- pts_ret_yds (return yards, 1 pt per 25 yds)
- pts_misc (2pt conversions + return TDs)
- pts_k_std, pts_k_yds, pts_k_flat (kicker variants)
- pts_def_std, pts_def_ya (defense variants; pts_def_high removed 2026-04-30 — KMFFL-specific)
- pts_idp_std, pts_idp_premium, pts_idp_tackle_heavy, pts_idp_big_play (IDP variants)
- pts_pass_cmp, pts_rush_att, pts_first_downs, pts_pass_fd, pts_sack_taken (optional)
- pts_pick6 (pick sixes thrown, raw count)
- bonus_pass_300yd, bonus_pass_400yd, bonus_rush_100yd, etc. (milestone flags)
- yds_allow_300_349, yds_allow_350_399, etc. (fine-grained YA tiers)

Usage:
    from fantasy_points_calculator import calculate_all_fantasy_points, PRECALC_COLUMNS

    # During weekly update or backfill:
    df = calculate_all_fantasy_points(df)

    # During league import, sum the relevant columns:
    df['fantasy_points'] = (
        df['pts_pass_4pt'] + df['pts_rush'] + df['pts_rec_half'] +
        df['pts_misc'] + df['pts_k_std'] + df['pts_def_std']
    )
"""

import pandas as pd

try:
    from nfl_data.team_margin import TEAM_MARGIN_COLUMNS
except ImportError:  # pragma: no cover - direct-script fallback
    TEAM_MARGIN_COLUMNS = (
        "pts_def_team_win",
        "pts_def_team_loss",
        "pts_def_team_tie",
        "pts_def_team_pts",
        "pts_def_team_margin",
        "pts_def_team_win_margin_25p",
        "pts_def_team_win_margin_20_24",
        "pts_def_team_win_margin_15_19",
        "pts_def_team_win_margin_10_14",
        "pts_def_team_win_margin_5_9",
        "pts_def_team_win_margin_1_4",
        "pts_def_team_loss_margin_1_4",
        "pts_def_team_loss_margin_5_9",
        "pts_def_team_loss_margin_10_14",
        "pts_def_team_loss_margin_15_19",
        "pts_def_team_loss_margin_20_24",
        "pts_def_team_loss_margin_25p",
    )

# All category columns that will be pre-calculated (28 total)
PRECALC_COLUMNS = [
    # Passing (6) - 4pt, 5pt, 6pt variants × INT -2 and INT -1
    "pts_pass_4pt",
    "pts_pass_5pt",
    "pts_pass_6pt",
    "pts_pass_4pt_int1",
    "pts_pass_5pt_int1",
    "pts_pass_6pt_int1",
    # Rushing (1)
    "pts_rush",
    # Receiving (4) - 0ppr, half, ppr, TE premium
    "pts_rec_0ppr",
    "pts_rec_half",
    "pts_rec_ppr",
    "pts_rec_tep",
    # Return yards (1)
    "pts_ret_yds",
    # Misc (1)
    "pts_misc",
    # Kicker (3)
    "pts_k_std",
    "pts_k_yds",
    "pts_k_flat",
    # Team Defense (2 variants + 12 modular components + 2 return aggregations)
    # Note: pts_def_high removed 2026-04-30 — KMFFL-specific scoring computed
    # per-league from league_settings, not stored in league-agnostic super_table.
    "pts_def_std",
    "pts_def_ya",
    # DEF modular components (raw event counts × 1 for SQL multiplication)
    "pts_def_sack",
    "pts_def_int",
    "pts_def_ff",
    "pts_def_fr",
    "pts_def_td",
    "pts_def_safety",
    "pts_def_block",
    "pts_def_fg_block",
    "pts_def_punt_block",
    "pts_def_pat_block",
    "pts_def_tfl",
    "pts_def_3out",
    "pts_def_4stop",
    # DEF return aggregations (sum of individual player returns by team/week)
    "pts_def_ret_yd",
    "pts_def_ret_td",
    # DEF team-score/margin components (raw team game facts)
    *TEAM_MARGIN_COLUMNS,
    # IDP - Individual Defensive Players (4 variants + 11 modular components)
    "pts_idp_std",
    "pts_idp_premium",
    "pts_idp_tackle_heavy",
    "pts_idp_big_play",
    # IDP modular components (raw event counts × 1 for SQL multiplication)
    "pts_idp_tackle_solo",
    "pts_idp_tackle_assist",
    "pts_idp_sack",
    "pts_idp_int",
    "pts_idp_ff",
    "pts_idp_fr",
    "pts_idp_pd",
    "pts_idp_qb_hit",
    "pts_idp_tfl",
    "pts_idp_safety",
    "pts_idp_td",
    # IDP additions from L1.d (2026-05-03) - bug-fix cols + combined-tackle helper
    # See: docs/superpowers/specs/2026-05-03-pts-idp-component-precompute-design.md
    "pts_idp_blk_kick",
    "pts_idp_int_ret_yd",
    "pts_idp_fum_rec_yd",
    "pts_idp_tkl_combined",
    "pts_idp_blk_kick_td",
    "pts_idp_fum_ret_td",
    "pts_idp_xpr",
    "pts_idp_pass_def_3p",
    # Optional category columns (5) - only non-zero when stat is relevant
    "pts_pass_cmp",
    "pts_rush_att",
    "pts_first_downs",
    "pts_pass_fd",
    "pts_sack_taken",
    # Pick sixes thrown (QB penalty) - raw count
    "pts_pick6",
    # Bonus yardage milestones (binary flags: 1 if threshold met, 0 otherwise)
    "bonus_pass_300yd",
    "bonus_pass_400yd",
    "bonus_rush_100yd",
    "bonus_rush_200yd",
    "bonus_rec_100yd",
    "bonus_rec_200yd",
    "bonus_rush_rec_100yd",
    "bonus_rush_rec_200yd",
    "bonus_pass_25cmp",
    "bonus_rush_20att",
    "bonus_rec_10rec",
    # === L1.b.1 component cols (added 2026-05-02; calculator emits in Phase A) ===
    # Atomic-stat × multiplier products. Per-league SQL sums these instead of
    # bundled-variant cols. See docs/superpowers/specs/2026-05-02-component-precompute-design.md.
    "pts_pass_yd_p04",
    "pts_pass_td_4",
    "pts_pass_td_6",
    "pts_pass_int_n2",
    "pts_pass_int_n1",
    "pts_rush_yd_p1",
    "pts_rush_td_6",
    "pts_rec_yd_p1",
    "pts_rec_td_6",
    "pts_rec_1",
    "pts_rec_p5",
    "pts_rec_te_bonus_p5",
    "pts_pass_2pt_2",
    "pts_rush_2pt_2",
    "pts_rec_2pt_2",
    "pts_fum_lost_n2",
    "pts_fum_lost_n1",
    "pts_pass_cmp_p25",
    "pts_pass_cmp_p1",
    "pts_pass_cmp_p5",
    "pts_rush_att_p1",
    "pts_rush_att_p2",
    "pts_rush_att_p25",
    "pts_pass_fd_p5",
    "pts_pass_fd_p25",
    "pts_rush_fd_p5",
    "pts_rush_fd_p25",
    "pts_rec_fd_p5",
    "pts_rec_fd_p25",
    "pts_pick6_n2",
    "pts_pick6_n1",
    "pts_sack_taken_n1",
    "pts_sack_taken_np5",
    "pts_st_td_6",
    "pts_fum_ret_td_6",
    "pts_kr_yd_p04",
    "pts_pr_yd_p04",
    "pts_pass_td_40plus_2",
    "pts_pass_td_50plus_1",
    "pts_rush_td_40plus_2",
    "pts_rush_td_50plus_1",
    "pts_rec_td_40plus_1",
    "pts_rec_td_50plus_1",
    "pts_pass_cmp_40plus_1",
    "pts_rush_40plus_1",
    "pts_rec_40plus_1",
    # === L1.c kicker component cols (added 2026-05-02; calculator emits in Phase A) ===
    # Atomic-bucket × multiplier products. See:
    # docs/superpowers/specs/2026-05-02-pts-k-component-precompute-design.md
    "pts_k_fgm_0_19_3",
    "pts_k_fgm_20_29_3",
    "pts_k_fgm_30_39_3",
    "pts_k_fgm_40_49_4",
    "pts_k_fgm_40_49_3",
    "pts_k_fgm_50_59_5",
    "pts_k_fgm_60p_6",
    "pts_k_fgm_60p_5",
    "pts_k_xpm_1",
    "pts_k_xpmiss_n1",
    "pts_k_fgmiss_n1",
    "pts_k_fgm_yd_p1",
    "pts_k_fgm_yd_over30_p1",
    # Finer-grained Yards Allowed tiers for DEF (binary flags)
    "yds_allow_300_349",
    "yds_allow_350_399",
    "yds_allow_400_449",
    "yds_allow_450_499",
    "yds_allow_500_549",
    "yds_allow_550_plus",
]

# Pre-computed PPG/aggregate columns (120 total: 8 metrics × 15 scoring variants)
# These enable fast league imports by avoiding runtime groupby/sort operations
# Scoring variants: 4pt/5pt/6pt pass TD × 0ppr/half/ppr + 4pt/5pt/6pt × tep
PPG_COLUMNS = [
    # Season PPG - Points per game within a season (15)
    "ppg_season_4pt_0ppr",
    "ppg_season_4pt_half",
    "ppg_season_4pt_ppr",
    "ppg_season_5pt_0ppr",
    "ppg_season_5pt_half",
    "ppg_season_5pt_ppr",
    "ppg_season_6pt_0ppr",
    "ppg_season_6pt_half",
    "ppg_season_6pt_ppr",
    "ppg_season_4pt_tep",
    "ppg_season_5pt_tep",
    "ppg_season_6pt_tep",
    # All-time PPG - Career points per game (15)
    "ppg_alltime_4pt_0ppr",
    "ppg_alltime_4pt_half",
    "ppg_alltime_4pt_ppr",
    "ppg_alltime_5pt_0ppr",
    "ppg_alltime_5pt_half",
    "ppg_alltime_5pt_ppr",
    "ppg_alltime_6pt_0ppr",
    "ppg_alltime_6pt_half",
    "ppg_alltime_6pt_ppr",
    "ppg_alltime_4pt_tep",
    "ppg_alltime_5pt_tep",
    "ppg_alltime_6pt_tep",
    # Rolling 3-game average (15)
    "rolling_3_4pt_0ppr",
    "rolling_3_4pt_half",
    "rolling_3_4pt_ppr",
    "rolling_3_5pt_0ppr",
    "rolling_3_5pt_half",
    "rolling_3_5pt_ppr",
    "rolling_3_6pt_0ppr",
    "rolling_3_6pt_half",
    "rolling_3_6pt_ppr",
    "rolling_3_4pt_tep",
    "rolling_3_5pt_tep",
    "rolling_3_6pt_tep",
    # Rolling 5-game average (15)
    "rolling_5_4pt_0ppr",
    "rolling_5_4pt_half",
    "rolling_5_4pt_ppr",
    "rolling_5_5pt_0ppr",
    "rolling_5_5pt_half",
    "rolling_5_5pt_ppr",
    "rolling_5_6pt_0ppr",
    "rolling_5_6pt_half",
    "rolling_5_6pt_ppr",
    "rolling_5_4pt_tep",
    "rolling_5_5pt_tep",
    "rolling_5_6pt_tep",
    # Consistency score - std/mean ratio within season (15)
    "consistency_4pt_0ppr",
    "consistency_4pt_half",
    "consistency_4pt_ppr",
    "consistency_5pt_0ppr",
    "consistency_5pt_half",
    "consistency_5pt_ppr",
    "consistency_6pt_0ppr",
    "consistency_6pt_half",
    "consistency_6pt_ppr",
    "consistency_4pt_tep",
    "consistency_5pt_tep",
    "consistency_6pt_tep",
    # Weighted PPG - Exponentially weighted moving average, span=5 (15)
    "weighted_ppg_4pt_0ppr",
    "weighted_ppg_4pt_half",
    "weighted_ppg_4pt_ppr",
    "weighted_ppg_5pt_0ppr",
    "weighted_ppg_5pt_half",
    "weighted_ppg_5pt_ppr",
    "weighted_ppg_6pt_0ppr",
    "weighted_ppg_6pt_half",
    "weighted_ppg_6pt_ppr",
    "weighted_ppg_4pt_tep",
    "weighted_ppg_5pt_tep",
    "weighted_ppg_6pt_tep",
    # Avg points next year - What player averaged the following season (15)
    "avg_pts_next_year_4pt_0ppr",
    "avg_pts_next_year_4pt_half",
    "avg_pts_next_year_4pt_ppr",
    "avg_pts_next_year_5pt_0ppr",
    "avg_pts_next_year_5pt_half",
    "avg_pts_next_year_5pt_ppr",
    "avg_pts_next_year_6pt_0ppr",
    "avg_pts_next_year_6pt_half",
    "avg_pts_next_year_6pt_ppr",
    "avg_pts_next_year_4pt_tep",
    "avg_pts_next_year_5pt_tep",
    "avg_pts_next_year_6pt_tep",
    # Rolling point total - Cumulative sum per player per season (15 + 2 = 17)
    "rolling_total_4pt_0ppr",
    "rolling_total_4pt_half",
    "rolling_total_4pt_ppr",
    "rolling_total_5pt_0ppr",
    "rolling_total_5pt_half",
    "rolling_total_5pt_ppr",
    "rolling_total_6pt_0ppr",
    "rolling_total_6pt_half",
    "rolling_total_6pt_ppr",
    "rolling_total_4pt_tep",
    "rolling_total_5pt_tep",
    "rolling_total_6pt_tep",
    # DEF and K rolling totals for tie-breaking in rankings
    "rolling_total_def",
    "rolling_total_k",
]


def safe_col(df: pd.DataFrame, col_name: str, default: float = 0.0) -> pd.Series:
    """
    Safely get a column from DataFrame, returning default if missing.

    Converts column to numeric (handling string values) and fills NA with default.

    Args:
        df: DataFrame to get column from
        col_name: Column name to get
        default: Default value if column is missing

    Returns:
        Series with numeric column values or default
    """
    if col_name in df.columns:
        # Convert to numeric, coercing errors to NaN, then fill with default
        return pd.to_numeric(df[col_name], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index)


def calculate_all_fantasy_points(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add all 12 category columns to DataFrame.

    Handles missing columns gracefully by defaulting to 0.

    Args:
        df: DataFrame with NFL player stats

    Returns:
        DataFrame with all pts_* columns added
    """
    result = df.copy()

    # === PASSING ===
    # Base passing: yards * 0.04 + INTs * -2
    pass_yds_pts = safe_col(result, "passing_yards") * 0.04

    # INTs can be in different column names
    if "passing_interceptions" in result.columns:
        pass_ints_pts = safe_col(result, "passing_interceptions") * -2
    elif "pass_int" in result.columns:
        pass_ints_pts = safe_col(result, "pass_int") * -2
    else:
        pass_ints_pts = safe_col(result, "interceptions") * -2

    # 4pt passing TD
    result["pts_pass_4pt"] = pass_yds_pts + safe_col(result, "passing_tds") * 4 + pass_ints_pts

    # 5pt passing TD (common setting between 4pt and 6pt)
    result["pts_pass_5pt"] = pass_yds_pts + safe_col(result, "passing_tds") * 5 + pass_ints_pts

    # 6pt passing TD
    result["pts_pass_6pt"] = pass_yds_pts + safe_col(result, "passing_tds") * 6 + pass_ints_pts

    # === L1.b.1 components: passing_yards + passing_tds + passing_interceptions ===
    # Each component is a single atomic_stat * fixed_multiplier, no bundling.
    # See docs/superpowers/specs/2026-05-02-component-precompute-design.md.
    result["pts_pass_yd_p04"] = pass_yds_pts  # already computed above = passing_yards * 0.04
    result["pts_pass_td_4"] = safe_col(result, "passing_tds") * 4
    result["pts_pass_td_6"] = safe_col(result, "passing_tds") * 6
    result["pts_pass_int_n2"] = pass_ints_pts  # already computed above = passing_interceptions * -2

    # === PASSING (INT -1 variants) ===
    # 36% of leagues use -1 per INT instead of -2. Pre-calc these to avoid corrections.
    if "passing_interceptions" in result.columns:
        pass_ints_pts_neg1 = safe_col(result, "passing_interceptions") * -1
    elif "pass_int" in result.columns:
        pass_ints_pts_neg1 = safe_col(result, "pass_int") * -1
    else:
        pass_ints_pts_neg1 = safe_col(result, "interceptions") * -1

    result["pts_pass_4pt_int1"] = pass_yds_pts + safe_col(result, "passing_tds") * 4 + pass_ints_pts_neg1

    result["pts_pass_5pt_int1"] = pass_yds_pts + safe_col(result, "passing_tds") * 5 + pass_ints_pts_neg1

    result["pts_pass_6pt_int1"] = pass_yds_pts + safe_col(result, "passing_tds") * 6 + pass_ints_pts_neg1

    # === L1.b.1 component: passing_interceptions × -1 ===
    result["pts_pass_int_n1"] = pass_ints_pts_neg1  # already computed = passing_interceptions * -1

    # === RUSHING ===
    # Sum all rushing fumble types
    rush_fum_lost = (
        safe_col(result, "rushing_fumbles_lost")
        + safe_col(result, "sack_fumbles_lost")  # QB sacks count as rushing fumbles
    )

    result["pts_rush"] = (
        safe_col(result, "rushing_yards") * 0.1 + safe_col(result, "rushing_tds") * 6 + rush_fum_lost * -2
    )

    # === L1.b.1 components: rushing_yards + rushing_tds ===
    result["pts_rush_yd_p1"] = safe_col(result, "rushing_yards") * 0.1
    result["pts_rush_td_6"] = safe_col(result, "rushing_tds") * 6

    # === RECEIVING ===
    rec_fum_lost = safe_col(result, "receiving_fumbles_lost")

    # Base receiving (0 PPR)
    result["pts_rec_0ppr"] = (
        safe_col(result, "receiving_yards") * 0.1 + safe_col(result, "receiving_tds") * 6 + rec_fum_lost * -2
    )

    # Half PPR (0.5 per reception)
    result["pts_rec_half"] = result["pts_rec_0ppr"] + safe_col(result, "receptions") * 0.5

    # Full PPR (1.0 per reception)
    result["pts_rec_ppr"] = result["pts_rec_0ppr"] + safe_col(result, "receptions") * 1.0

    # TE Premium (TEs get 1.5 PPR, non-TEs get 1.0 PPR)
    # This is the standard TE premium format used in dynasty leagues
    # Position column can be 'position' or 'nfl_position'
    position_col = "nfl_position" if "nfl_position" in result.columns else "position"
    if position_col in result.columns:
        import numpy as np

        is_te = result[position_col].str.upper() == "TE"
        # TEs: 1.5 per reception, non-TEs: 1.0 per reception (full PPR baseline)
        result["pts_rec_tep"] = np.where(
            is_te,
            result["pts_rec_0ppr"] + safe_col(result, "receptions") * 1.5,
            result["pts_rec_0ppr"] + safe_col(result, "receptions") * 1.0,
        )
    else:
        # No position column, default to full PPR
        result["pts_rec_tep"] = result["pts_rec_ppr"]

    # === L1.b.1 components: receiving_yards + receiving_tds + receptions ===
    result["pts_rec_yd_p1"] = safe_col(result, "receiving_yards") * 0.1
    result["pts_rec_td_6"] = safe_col(result, "receiving_tds") * 6
    result["pts_rec_1"] = safe_col(result, "receptions") * 1
    result["pts_rec_p5"] = safe_col(result, "receptions") * 0.5
    # TE-conditional: 0.5 × receptions only when position is TE; 0 otherwise.
    # Per-league SQL adds this component when bonus_rec_te is configured.
    if position_col in result.columns:
        is_te_for_bonus = result[position_col].fillna("").str.upper() == "TE"
        result["pts_rec_te_bonus_p5"] = (safe_col(result, "receptions") * 0.5).where(is_te_for_bonus, 0.0)
    else:
        result["pts_rec_te_bonus_p5"] = 0.0

    # === MISC (2PT conversions + non-DST return TDs) ===
    # DEF rows carry team return/TD scoring through pts_def_* columns. Keeping
    # fumble/ST return TDs in pts_misc on DEF rows double-counts those scores in
    # baked fpts_* composites.
    if position_col in result.columns:
        is_team_def = result[position_col].fillna("").astype(str).str.upper().str.strip() == "DEF"
    else:
        is_team_def = pd.Series(False, index=result.index)

    two_pt_total = (
        safe_col(result, "passing_2pt_conversions")
        + safe_col(result, "rushing_2pt_conversions")
        + safe_col(result, "receiving_2pt_conversions")
    )
    non_dst_special_teams_tds = safe_col(result, "special_teams_tds").where(~is_team_def, 0.0)
    non_dst_fum_ret_tds = safe_col(result, "fum_ret_td").where(~is_team_def, 0.0)

    result["pts_misc"] = two_pt_total * 2 + non_dst_special_teams_tds * 6 + non_dst_fum_ret_tds * 6

    # === L1.b.1 components: 2PT conversions ===
    result["pts_pass_2pt_2"] = safe_col(result, "passing_2pt_conversions") * 2
    result["pts_rush_2pt_2"] = safe_col(result, "rushing_2pt_conversions") * 2
    result["pts_rec_2pt_2"] = safe_col(result, "receiving_2pt_conversions") * 2

    # === L1.b.1 components: total fumbles_lost (sum of 3 atomic sources) ===
    # NOTE: pts_fum_lost combines rush + sack + rec fumbles; existing variants
    # (pts_rush uses rush+sack only; pts_rec_* uses rec only). M12 variant-
    # identity invariant handles this asymmetry via special-case overrides.
    fum_lost_total = (
        safe_col(result, "rushing_fumbles_lost")
        + safe_col(result, "sack_fumbles_lost")
        + safe_col(result, "receiving_fumbles_lost")
    )
    result["pts_fum_lost_n2"] = fum_lost_total * -2
    result["pts_fum_lost_n1"] = fum_lost_total * -1

    # === A3: fumble residual (GREATEST(total, split) shortfall) ===
    # pts_rush uses the rush+sack split and pts_rec_* the rec split; but the `fumbles_lost` TOTAL column
    # carries return/aborted-snap lost fumbles the split MISSES (1,256 offense rows, total>split). The
    # residual = (GREATEST(total, split) - split) * -2 folds those into the standard composites WITHOUT
    # touching pts_rush/pts_rec or the per-league correction baseline (so per-league totals stay invariant).
    # GREATEST is two-way safe: where the total col under-reports (split>total) the residual is 0. Mirrors
    # offense_recipe.LOST_FUMBLES_GREATEST -> golden_points locks fpts to it exactly.
    _split_fl = fum_lost_total  # rushing + sack + receiving fumbles_lost (already summed above)
    _total_fl = safe_col(result, "fumbles_lost")
    result["pts_fum_residual"] = (_total_fl - _split_fl).clip(lower=0) * -2

    # === RETURN YARDS ===
    # Common in ~20% of leagues, 1 point per 25 return yards
    result["pts_ret_yds"] = (
        safe_col(result, "kickoff_return_yards", 0) + safe_col(result, "punt_return_yards", 0)
    ) / 25.0

    # === OPTIONAL CATEGORY COLUMNS ===
    # These cover stats that ~5-20% of leagues score but aren't in the base precalc.
    # Default multipliers chosen to match the most common setting for each stat.

    # Completions (QB accuracy reward) - default 0.25 pts/completion
    result["pts_pass_cmp"] = safe_col(result, "completions") * 0.25

    # Rush attempts (RB volume reward) - default 0.1 pts/carry
    result["pts_rush_att"] = safe_col(result, "carries") * 0.1

    # === L1.b.1 components: completions + carries ===
    result["pts_pass_cmp_p25"] = safe_col(result, "completions") * 0.25
    result["pts_pass_cmp_p1"] = safe_col(result, "completions") * 0.1
    result["pts_pass_cmp_p5"] = safe_col(result, "completions") * 0.5
    result["pts_rush_att_p1"] = safe_col(result, "carries") * 0.1
    result["pts_rush_att_p2"] = safe_col(result, "carries") * 0.2
    result["pts_rush_att_p25"] = safe_col(result, "carries") * 0.25

    # First downs (rush + receiving combined) - default 1.0 pt/first down
    result["pts_first_downs"] = (
        safe_col(result, "rushing_first_downs") + safe_col(result, "receiving_first_downs")
    ) * 1.0

    # Passing first downs (separate from rush+rec) - default 1.0 pt/first down
    result["pts_pass_fd"] = safe_col(result, "passing_first_downs") * 1.0

    # === L1.b.1 components: first downs (3 atomic × 2 mults each) ===
    result["pts_pass_fd_p5"] = safe_col(result, "passing_first_downs") * 0.5
    result["pts_pass_fd_p25"] = safe_col(result, "passing_first_downs") * 0.25
    result["pts_rush_fd_p5"] = safe_col(result, "rushing_first_downs") * 0.5
    result["pts_rush_fd_p25"] = safe_col(result, "rushing_first_downs") * 0.25
    result["pts_rec_fd_p5"] = safe_col(result, "receiving_first_downs") * 0.5
    result["pts_rec_fd_p25"] = safe_col(result, "receiving_first_downs") * 0.25

    # === L1.b.1 components: pick6 + sack_taken + ST + fum_ret + return_yd ===
    result["pts_pick6_n2"] = safe_col(result, "pick6") * -2
    result["pts_pick6_n1"] = safe_col(result, "pick6") * -1
    result["pts_sack_taken_n1"] = safe_col(result, "sacks_suffered") * -1
    result["pts_sack_taken_np5"] = safe_col(result, "sacks_suffered") * -0.5
    result["pts_st_td_6"] = non_dst_special_teams_tds * 6
    result["pts_fum_ret_td_6"] = non_dst_fum_ret_tds * 6
    result["pts_kr_yd_p04"] = safe_col(result, "kickoff_return_yards") * 0.04
    result["pts_pr_yd_p04"] = safe_col(result, "punt_return_yards") * 0.04

    # === L1.b.1 Phase 4 bracket components (ship as components from start) ===
    # TD-bracket cols: TDs scored on plays of 40+ / 50+ yards
    result["pts_pass_td_40plus_2"] = safe_col(result, "passing_tds_40plus") * 2
    result["pts_pass_td_50plus_1"] = safe_col(result, "passing_tds_50plus") * 1
    result["pts_rush_td_40plus_2"] = safe_col(result, "rushing_tds_40plus") * 2
    result["pts_rush_td_50plus_1"] = safe_col(result, "rushing_tds_50plus") * 1
    result["pts_rec_td_40plus_1"] = safe_col(result, "receiving_tds_40plus") * 1
    result["pts_rec_td_50plus_1"] = safe_col(result, "receiving_tds_50plus") * 1
    # Play-bracket cols: completions / rushes / receptions of 40+ yards
    result["pts_pass_cmp_40plus_1"] = safe_col(result, "completions_40plus") * 1
    result["pts_rush_40plus_1"] = safe_col(result, "rushing_40plus") * 1
    result["pts_rec_40plus_1"] = safe_col(result, "receptions_40plus") * 1

    # Sacks taken (QB penalty) - default -1.0 pts/sack.
    # Super_table col is sacks_suffered (verified 2026-05-02 via L1.b audit M2):
    # 'sacks' col does not exist in super_table; reading it returned 0 for every
    # row, producing 13,441 rows of pts_sack_taken drift before this fix.
    result["pts_sack_taken"] = safe_col(result, "sacks_suffered") * -1.0

    # Pick sixes thrown (QB penalty) - raw count for league-specific multiplier
    result["pts_pick6"] = safe_col(result, "pick6")

    # === BONUS YARDAGE MILESTONES ===
    # Binary flags (1/0) - leagues multiply by their bonus point value
    pass_yds = safe_col(result, "passing_yards")
    rush_yds = safe_col(result, "rushing_yards")
    rec_yds = safe_col(result, "receiving_yards")
    result["bonus_pass_300yd"] = (pass_yds >= 300).astype(float)
    result["bonus_pass_400yd"] = (pass_yds >= 400).astype(float)
    result["bonus_rush_100yd"] = (rush_yds >= 100).astype(float)
    result["bonus_rush_200yd"] = (rush_yds >= 200).astype(float)
    result["bonus_rec_100yd"] = (rec_yds >= 100).astype(float)
    result["bonus_rec_200yd"] = (rec_yds >= 200).astype(float)
    result["bonus_rush_rec_100yd"] = ((rush_yds + rec_yds) >= 100).astype(float)
    result["bonus_rush_rec_200yd"] = ((rush_yds + rec_yds) >= 200).astype(float)
    result["bonus_pass_25cmp"] = (safe_col(result, "completions") >= 25).astype(float)
    result["bonus_rush_20att"] = (safe_col(result, "carries") >= 20).astype(float)
    result["bonus_rec_10rec"] = (safe_col(result, "receptions") >= 10).astype(float)

    # === KICKER ===
    # Yahoo/ESPN style - distance buckets with 50+ bonus
    # FG 0-39 = 3pts, 40-49 = 4pts, 50-59 = 5pts, 60+ = 6pts (or 5 on some platforms)
    # PAT = 1pt, Miss = -1pt
    fg_0_19 = safe_col(result, "fg_made_0_19")
    fg_20_29 = safe_col(result, "fg_made_20_29")
    fg_30_39 = safe_col(result, "fg_made_30_39")
    fg_40_49 = safe_col(result, "fg_made_40_49")
    fg_50_59 = safe_col(result, "fg_made_50_59")
    fg_60_plus = (
        safe_col(result, "fg_made_60_")  # NFLverse uses fg_made_60_
        + safe_col(result, "fg_made_60_plus")  # Alternative naming
    )
    # L1.c canonical 60+ source (Phase 0.1; replaces dead-code fg_made_60_plus union).
    # Existing `fg_60_plus` local var still feeds legacy pts_k_std until Phase D drop.
    fg_60_plus_canonical = safe_col(result, "fg_made_60_plus_canonical")
    pat_made = safe_col(result, "pat_made")
    pat_missed = safe_col(result, "pat_missed")
    fg_missed = safe_col(result, "fg_missed")

    # Sum of bucket FGs (may be 0 for historical data where buckets are NULL)
    fg_from_buckets = fg_0_19 + fg_20_29 + fg_30_39 + fg_40_49 + fg_50_59 + fg_60_plus
    fg_made_total = safe_col(result, "fg_made")

    # For historical data (pre-1980s), buckets are often NULL but fg_made exists
    # Use fg_made * 3 as fallback (assume average ~35 yard FG = 3 pts each)
    # This is approximate but better than 0 points
    has_bucket_data = fg_from_buckets > 0

    # pts_k_std: the modal league kicker config across our DB (4,531 league-years):
    # FG 3/3/3/4/5/5 by distance, XP +1, missed FG -1, missed XP -1 (not yardage).
    # 60+ is 5 (the plurality) not 6, and the missed-XP penalty is included -- both
    # verified against public.league_settings modes. Renamed from pts_k_std (it was
    # never Yahoo-specific).
    result["pts_k_std"] = (
        # When we have bucket data, use precise scoring
        pd.Series((fg_0_19 + fg_20_29 + fg_30_39) * 3 + fg_40_49 * 4 + fg_50_59 * 5 + fg_60_plus * 5).where(
            has_bucket_data, fg_made_total * 3
        )  # Fallback: 3 pts per FG
        + pat_made * 1
        + fg_missed * -1
        + pat_missed * -1
    )

    # Sleeper/Yahoo style - per yard scoring (FG Yds × 0.1)
    # Prefer actual made-FG distance.  Historical PBP backfills store the same
    # truth in fg_yards, so use it when fg_made_distance is not populated.
    # Fall back to bucket midpoint estimation, then to 35 yards/FG.
    fg_made_distance = safe_col(result, "fg_made_distance")
    fg_yards_actual = safe_col(result, "fg_yards")
    fg_actual_distance = fg_made_distance.where(fg_made_distance > 0, fg_yards_actual)
    has_actual_distance = fg_actual_distance > 0

    fg_yards_from_buckets = (
        fg_0_19 * 17  # Midpoint of 0-19 range
        + fg_20_29 * 25  # Midpoint of 20-29 range
        + fg_30_39 * 35  # Midpoint of 30-39 range
        + fg_40_49 * 45  # Midpoint of 40-49 range
        + fg_50_59 * 54  # Midpoint of 50-59 range
        + fg_60_plus * 62  # Estimate for 60+ (most are 60-64)
    )
    fg_yards_estimated = pd.Series(fg_yards_from_buckets).where(has_bucket_data, fg_made_total * 35)

    # Use actual distance when available, fall back to estimate
    fg_yards = pd.Series(fg_actual_distance).where(has_actual_distance, fg_yards_estimated)

    result["pts_k_yds"] = fg_yards * 0.1 + pat_made * 1

    # Simple flat rate - 3pts per FG, 1pt per PAT
    result["pts_k_flat"] = fg_made_total * 3 + pat_made * 1

    # === L1.c kicker components (added 2026-05-02) ===
    # Atomic-bucket x multiplier products. See:
    #   docs/superpowers/specs/2026-05-02-pts-k-component-precompute-design.md
    # Per-league SQL sums these instead of bundled-variant cols.
    result["pts_k_fgm_0_19_3"] = fg_0_19 * 3
    result["pts_k_fgm_20_29_3"] = fg_20_29 * 3
    result["pts_k_fgm_30_39_3"] = fg_30_39 * 3
    result["pts_k_fgm_40_49_4"] = fg_40_49 * 4
    result["pts_k_fgm_40_49_3"] = fg_40_49 * 3  # alternate high-coverage
    result["pts_k_fgm_50_59_5"] = fg_50_59 * 5
    result["pts_k_fgm_60p_6"] = fg_60_plus_canonical * 6
    result["pts_k_fgm_60p_5"] = fg_60_plus_canonical * 5  # alternate high-coverage
    result["pts_k_xpm_1"] = pat_made * 1
    result["pts_k_xpmiss_n1"] = pat_missed * -1
    result["pts_k_fgmiss_n1"] = fg_missed * -1
    fg_yards_canonical = safe_col(result, "fg_yards_canonical")
    fg_yards_over_30_canonical = safe_col(result, "fg_yards_over_30_canonical")
    result["pts_k_fgm_yd_p1"] = fg_yards_canonical * 0.1
    result["pts_k_fgm_yd_over30_p1"] = fg_yards_over_30_canonical * 0.1

    # === DEFENSE ===
    # Modular DEF components (raw event counts × 1)
    # Each column stores the raw count so SQL can multiply by any league's point value.
    # No corrections framework needed — just multiply each component.
    result["pts_def_sack"] = safe_col(result, "def_sacks")
    result["pts_def_int"] = safe_col(result, "def_interceptions")
    result["pts_def_ff"] = safe_col(result, "def_fumbles_forced")
    result["pts_def_fr"] = safe_col(result, "fum_rec")
    # Canonical component sum prevents fum_ret_td double-count.
    # NFLverse `def_tds` semantics drift across years: roughly half of post-1999
    # rows include fum returns, half don't. Pre-fix `def_tds + fum_ret_td` silently
    # double-counts the inclusive case. Take GREATEST of the disjoint component sum
    # and the box-score aggregate def_tds (never add): in modern data def_tds <= the
    # component sum so the sum wins (no double-count); in pre-2000 PFR-merge data the
    # box score is authoritative but the play-by-play split is often incomplete, so
    # def_tds wins instead of silently dropping TDs (e.g. 1950 Lions wk1: box def_tds=3,
    # only 1 int_ret_td classified -> was crediting 1 TD, -12 pts).
    # See: docs/superpowers/specs/2026-04-29-pts-def-audit-design.md §M12,
    # docs/superpowers/specs/2026-05-01-pts-off-audit-design.md Phase 0 Task 0.1
    _components_sum = (
        safe_col(result, "pts_def_int_ret_td")
        + safe_col(result, "pts_def_fum_ret_td")
        + safe_col(result, "pts_def_blk_kick_td")
    )
    result["pts_def_td"] = _components_sum.where(
        _components_sum >= safe_col(result, "def_tds"), safe_col(result, "def_tds")
    )
    result["pts_def_safety"] = safe_col(result, "def_safeties")
    result["pts_def_fg_block"] = safe_col(result, "pts_def_fg_block").where(
        safe_col(result, "pts_def_fg_block") != 0,
        safe_col(result, "fg_blocked"),
    )
    result["pts_def_punt_block"] = safe_col(result, "pts_def_punt_block")
    result["pts_def_pat_block"] = safe_col(result, "pts_def_pat_block")
    result["pts_def_block"] = result["pts_def_fg_block"] + result["pts_def_punt_block"] + result["pts_def_pat_block"]
    result["pts_def_tfl"] = safe_col(result, "def_tackles_for_loss")
    result["pts_def_3out"] = safe_col(result, "three_out")
    result["pts_def_4stop"] = safe_col(result, "fourth_down_stop")
    for _team_margin_col in TEAM_MARGIN_COLUMNS:
        result[_team_margin_col] = safe_col(result, _team_margin_col)

    # === Aggregate individual player return data into DEF rows ===
    # DEF rows have NULL for return stats because return yards/TDs are individual
    # player stats. Aggregate from individual players on the same team/week to get
    # team-level return yards and TDs. These are RAW stats (not pre-multiplied) so
    # the league-specific multiplier is applied via def_multipliers at import time.
    result["pts_def_ret_yd"] = 0.0
    result["pts_def_ret_td"] = 0.0

    # Detect column names (NFLverse raw: recent_team/season, after rename: nfl_team/year)
    team_col = (
        "recent_team" if "recent_team" in result.columns else ("nfl_team" if "nfl_team" in result.columns else None)
    )
    year_col = "season" if "season" in result.columns else ("year" if "year" in result.columns else None)
    pos_col = (
        "nfl_position" if "nfl_position" in result.columns else ("position" if "position" in result.columns else None)
    )

    if pos_col and team_col and year_col:
        non_def = result[pos_col] != "DEF"
        def_mask = result[pos_col] == "DEF"

        if non_def.any() and def_mask.any():
            # Individual player return totals
            # kickoff_return_yards / punt_return_yards are per-player return yards
            # special_teams_tds is the combined return TD column in NFLverse
            player_ret_yds = safe_col(result, "kickoff_return_yards") + safe_col(result, "punt_return_yards")
            player_ret_tds = safe_col(result, "special_teams_tds")

            # Aggregate by team/week
            team_returns = (
                pd.DataFrame(
                    {
                        "team": result.loc[non_def, team_col],
                        "yr": result.loc[non_def, year_col],
                        "wk": result.loc[non_def, "week"],
                        "ret_yds": player_ret_yds[non_def],
                        "ret_tds": player_ret_tds[non_def],
                    }
                )
                .groupby(["team", "yr", "wk"], as_index=False)
                .sum()
            )

            # Map aggregated return data onto DEF rows via team/year/week lookup
            def_df = result.loc[def_mask, [team_col, year_col, "week"]].copy()
            def_df.columns = ["team", "yr", "wk"]
            def_df = def_df.merge(team_returns, on=["team", "yr", "wk"], how="left")
            result.loc[def_mask, "pts_def_ret_yd"] = def_df["ret_yds"].fillna(0).values
            result.loc[def_mask, "pts_def_ret_td"] = def_df["ret_tds"].fillna(0).values

    # Base defensive stats (aggregated from components for precomputed totals).
    # Standard DST scores fumble recoveries, not forced fumbles. Keep pts_def_ff
    # as a raw component for custom leagues that explicitly award it.
    # Kick/punt return TDs (pts_def_ret_td) ARE baked at the modal +6: 89% of
    # league-years award the DST a return TD (median 6). The baked-baseline +
    # correction-delta architecture (scoring_config._defense_correction_term)
    # shifts its return-TD baseline 0->6 in lockstep, so per-league totals are
    # invariant; only the modal pts_def_std / rank_def / DEF fpts_* reflect it.
    def_base = (
        result["pts_def_sack"] * 1
        + result["pts_def_int"] * 2
        + result["pts_def_fr"] * 2
        + result["pts_def_td"] * 6
        + result["pts_def_safety"] * 2
        + result["pts_def_block"] * 2
        + result["pts_def_ret_td"] * 6
    )

    # Points allowed tiers (standard Yahoo/ESPN scoring)
    pa_tiers = (
        safe_col(result, "pts_allow_0") * 10
        + safe_col(result, "pts_allow_1_6") * 7
        + safe_col(result, "pts_allow_7_13") * 4
        + safe_col(result, "pts_allow_14_20") * 1
        + safe_col(result, "pts_allow_21_27") * 0
        + safe_col(result, "pts_allow_28_34") * -1
        + safe_col(result, "pts_allow_35_plus") * -4
    )

    # Fine-grained yards allowed tiers (some Sleeper leagues split the
    # traditional 300-399, 400-499, and 500+ buckets into smaller bands).
    total_ya = safe_col(result, "total_yds_allowed")
    result["yds_allow_300_349"] = ((total_ya >= 300) & (total_ya <= 349)).astype(float)
    result["yds_allow_350_399"] = ((total_ya >= 350) & (total_ya <= 399)).astype(float)
    result["yds_allow_400_449"] = ((total_ya >= 400) & (total_ya <= 449)).astype(float)
    result["yds_allow_450_499"] = ((total_ya >= 450) & (total_ya <= 499)).astype(float)
    result["yds_allow_500_549"] = ((total_ya >= 500) & (total_ya <= 549)).astype(float)
    result["yds_allow_550_plus"] = (total_ya >= 550).astype(float)

    # Yards allowed tiers (some leagues use these)
    ya_tiers = (
        safe_col(result, "yds_allow_neg") * 5
        + safe_col(result, "yds_allow_0_99") * 4
        + safe_col(result, "yds_allow_100_199") * 3
        + safe_col(result, "yds_allow_200_299") * 2
        + safe_col(result, "yds_allow_300_349") * 0
        + safe_col(result, "yds_allow_350_399") * 0
        + safe_col(result, "yds_allow_400_449") * -2
        + safe_col(result, "yds_allow_450_499") * -2
        + safe_col(result, "yds_allow_500_549") * -4
        + safe_col(result, "yds_allow_550_plus") * -4
    )

    # Standard defense (PA tiers only)
    result["pts_def_std"] = def_base + pa_tiers

    # Defense with yards allowed tiers
    result["pts_def_ya"] = def_base + pa_tiers + ya_tiers

    # === DEF LEAK GUARD ===
    # pts_def_* cols are only meaningful on DEF rows. NULL them out for any other
    # position to prevent calculator-side leak on non-DEF rows where pts_allow_*
    # tier brackets fire and produce nonzero pts_def_std values. Mirrors the
    # recompute SQL Phase E cleanup from L1.a — keeps weekly calculator runs
    # from re-introducing the leak (256k+ rows in L1.a's pre-fix state).
    # See: docs/superpowers/specs/2026-05-01-pts-off-audit-design.md Phase 0 Task 0.2
    _pos_col_for_def = (
        "nfl_position" if "nfl_position" in result.columns else ("position" if "position" in result.columns else None)
    )
    if _pos_col_for_def:
        _non_def_mask = result[_pos_col_for_def] != "DEF"
        if _non_def_mask.any():
            _pts_def_cols_to_null = [
                "pts_def_sack",
                "pts_def_int",
                "pts_def_ff",
                "pts_def_fr",
                "pts_def_td",
                "pts_def_safety",
                "pts_def_block",
                "pts_def_fg_block",
                "pts_def_punt_block",
                "pts_def_pat_block",
                "pts_def_tfl",
                "pts_def_3out",
                "pts_def_4stop",
                "pts_def_ret_yd",
                "pts_def_ret_td",
                *TEAM_MARGIN_COLUMNS,
                "pts_def_std",
                "pts_def_ya",
            ]
            for _col in _pts_def_cols_to_null:
                if _col in result.columns:
                    result.loc[_non_def_mask, _col] = pd.NA

    # NOTE: pts_def_high (KMFFL-style high-reward + penalty-only) was removed
    # from super_table on 2026-04-30 per spec §3.5. The variant uses opinionated
    # multipliers (INT=3, fum_ret_td=8, safety=4, fg_blocked=3, plus tfl/3-and-out/
    # 4-down-stop bonuses and penalty-only PA brackets) that don't generalize
    # across leagues. KMFFL retains the scoring via per-league pipeline driven by
    # league_settings (verified: scoring_int/safe/blk_kick/tkl_loss/def_3_and_out/
    # def_4_and_stop/pts_allow_* present and correct).
    # Backup: ___ops.public.pts_def_high_drop_backup_20260430

    # === IDP (Individual Defensive Players) ===
    # These columns store raw individual defensive events so league DDL can apply
    # any Yahoo/Sleeper/ESPN multiplier at import time.
    #
    # Super table columns (from aggregate_nfl_stats.py):
    #   def_tackles_solo, def_tackle_assists, def_tackles_with_assist,
    #   def_sacks, def_interceptions, def_fumbles_forced, fum_rec,
    #   def_tackles_for_loss, def_pass_defended, def_qb_hits, def_safeties,
    #   def_tds, fum_ret_td

    # Tackles: score real fantasy splits when present. Only fall back to the
    # combined tackle column on rows where the split atoms are absent.
    idp_tackles_solo = safe_col(result, "def_tackles_solo")
    idp_tackle_assists = safe_col(result, "def_tackle_assists")
    idp_tackles_combined = safe_col(result, "def_tackles_with_assist")
    idp_split_tackles = idp_tackles_solo + idp_tackle_assists * 0.5
    idp_has_split_tackles = (idp_tackles_solo != 0) | (idp_tackle_assists != 0)
    idp_tackles = idp_split_tackles.where(idp_has_split_tackles, idp_tackles_combined)

    # Sacks (use def_sacks, same column as team DEF but applies to individuals too)
    idp_sacks = safe_col(result, "def_sacks")

    # Interceptions - use def_interceptions
    # For IDP players this is positive points (unlike QB interceptions thrown)
    idp_ints = safe_col(result, "def_interceptions")

    # Forced fumbles
    idp_ff = safe_col(result, "def_fumbles_forced")

    # Fumble recoveries. `def_fumbles` is not the player fumble-recovery count;
    # the populated super-table source for recoveries is `fum_rec`.
    idp_fr = safe_col(result, "fum_rec")

    # Tackles for loss
    idp_tfl = safe_col(result, "def_tackles_for_loss")

    # Passes defended
    idp_pd = safe_col(result, "def_pass_defended")

    # QB hits
    idp_qb_hits = safe_col(result, "def_qb_hits")

    # Safeties
    idp_safety = safe_col(result, "def_safeties")

    # Defensive TDs: GREATEST of the disjoint return-TD component sum and aggregate
    # def_tds (never add). This matches the SOTA parquet builder: prevents fumble-return
    # TD double-counts in modern data while not dropping box-score TDs in pre-2000 data
    # where the play-by-play split is incomplete but def_tds is authoritative.
    idp_return_td_components = safe_col(result, "def_int_ret_td") + safe_col(result, "fum_ret_td")
    idp_td = idp_return_td_components.where(
        idp_return_td_components >= safe_col(result, "def_tds"), safe_col(result, "def_tds")
    )

    # === IDP Modular Components (raw event counts × 1) ===
    # Each column stores the raw count so SQL can multiply by any league's point value.
    # Same pattern as pts_def_sack / pts_def_int for team DEF.
    result["pts_idp_tackle_solo"] = safe_col(result, "def_tackles_solo")
    result["pts_idp_tackle_assist"] = safe_col(result, "def_tackle_assists")
    result["pts_idp_sack"] = safe_col(result, "def_sacks")
    result["pts_idp_int"] = safe_col(result, "def_interceptions")
    result["pts_idp_ff"] = safe_col(result, "def_fumbles_forced")
    result["pts_idp_fr"] = safe_col(result, "fum_rec")
    result["pts_idp_pd"] = safe_col(result, "def_pass_defended")
    result["pts_idp_qb_hit"] = safe_col(result, "def_qb_hits")
    result["pts_idp_tfl"] = safe_col(result, "def_tackles_for_loss")
    result["pts_idp_safety"] = safe_col(result, "def_safeties")
    result["pts_idp_td"] = idp_td

    # === L1.d additions (2026-05-03) ===
    # Bug-fix: 3 placeholder cols never populated by calculator (super_table had
    # the cols but no formula). Plus pts_idp_tkl_combined helper for 56 lyr using
    # scoring_idp_tkl (combined-tackle field).
    # See: docs/superpowers/specs/2026-05-03-pts-idp-component-precompute-design.md
    result["pts_idp_blk_kick"] = safe_col(result, "def_blk_kick")
    result["pts_idp_int_ret_yd"] = safe_col(result, "def_interception_yards")
    fumble_recovery_yards_total = safe_col(result, "fumble_recovery_yards_own") + safe_col(
        result, "fumble_recovery_yards_opp"
    )
    result["pts_idp_fum_rec_yd"] = fumble_recovery_yards_total.where(
        fumble_recovery_yards_total != 0,
        safe_col(result, "fum_rec_yds"),
    )
    result["pts_idp_blk_kick_td"] = safe_col(result, "def_blk_kick_td")
    result["pts_idp_fum_ret_td"] = safe_col(result, "fum_ret_td")
    # No current source column exists for Yahoo IDP XPR; keep the output column
    # wired so future source enrichment flows through without a schema/code gap.
    result["pts_idp_xpr"] = safe_col(result, "def_xpr")
    result["pts_idp_pass_def_3p"] = result["pts_idp_pd"].clip(upper=3.0)

    # Combined-tackle helper: derive total tackles from split atoms when present.
    # 56 lyr use scoring_idp_tkl as a combined-tackle field.
    _solo_plus_ast = idp_tackles_solo + idp_tackle_assists
    result["pts_idp_tkl_combined"] = _solo_plus_ast.where(idp_has_split_tackles, idp_tackles_combined)

    # === IDP Standard (modal Sleeper baseline) ===
    # Tackle=1, Sack=4, INT=6, FF=3, FR=2, TFL=2, PD=3, QB hit=1,
    # Safety=2, TD=6.
    result["pts_idp_std"] = (
        idp_tackles * 1.0
        + idp_sacks * 4.0
        + idp_ints * 6.0
        + idp_ff * 3.0
        + idp_fr * 2.0
        + idp_tfl * 2.0
        + idp_pd * 3.0
        + idp_qb_hits * 1.0
        + idp_safety * 2.0
        + idp_td * 6.0
    )

    # === IDP Premium (higher value across the board) ===
    # Tackle=1.5, Sack=3, INT=4, FF=3, FR=3, TFL=1.5, PD=1, Safety=3, TD=6
    result["pts_idp_premium"] = (
        idp_tackles * 1.5
        + idp_sacks * 3.0
        + idp_ints * 4.0
        + idp_ff * 3.0
        + idp_fr * 3.0
        + idp_tfl * 1.5
        + idp_pd * 1.0
        + idp_qb_hits * 1.0
        + idp_safety * 3.0
        + idp_td * 6.0
    )

    # === IDP Tackle Heavy (LB-friendly, high tackle value) ===
    # Tackle=2, Sack=2, INT=3, FF=2, FR=2, TFL=2, PD=1, Safety=2, TD=6
    result["pts_idp_tackle_heavy"] = (
        idp_tackles * 2.0
        + idp_sacks * 2.0
        + idp_ints * 3.0
        + idp_ff * 2.0
        + idp_fr * 2.0
        + idp_tfl * 2.0
        + idp_pd * 1.0
        + idp_qb_hits * 0.5
        + idp_safety * 2.0
        + idp_td * 6.0
    )

    # === IDP Big Play (sacks/turnovers worth more, tackles worth less) ===
    # Tackle=0.5, Sack=4, INT=6, FF=4, FR=4, TFL=2, PD=1.5, Safety=4, TD=6
    result["pts_idp_big_play"] = (
        idp_tackles * 0.5
        + idp_sacks * 4.0
        + idp_ints * 6.0
        + idp_ff * 4.0
        + idp_fr * 4.0
        + idp_tfl * 2.0
        + idp_pd * 1.5
        + idp_qb_hits * 1.0
        + idp_safety * 4.0
        + idp_td * 6.0
    )

    # === Zero out IDP columns for non-IDP-eligible rows ===
    # IDP scoring is for individual defensive eligibility only. This prevents
    # team defenses and offensive/ST players from carrying tackle/sack/INT points
    # into non-IDP fantasy variants while preserving DB/DL/LB lanes for IDP mode.
    idp_position_col = (
        "position" if "position" in result.columns else ("nfl_position" if "nfl_position" in result.columns else None)
    )
    if idp_position_col:
        idp_tokens = {"DL", "LB", "DB", "ILB", "OLB", "MLB", "DE", "DT", "EDGE", "NT", "CB", "S", "SS", "FS", "SAF"}
        is_idp_eligible = (
            result[idp_position_col]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.split(",")
            .map(lambda tokens: any(token.strip() in idp_tokens for token in tokens))
        )
        non_idp = ~is_idp_eligible
        for col in [
            "pts_idp_std",
            "pts_idp_premium",
            "pts_idp_tackle_heavy",
            "pts_idp_big_play",
            "pts_idp_tackle_solo",
            "pts_idp_tackle_assist",
            "pts_idp_sack",
            "pts_idp_int",
            "pts_idp_ff",
            "pts_idp_fr",
            "pts_idp_pd",
            "pts_idp_qb_hit",
            "pts_idp_tfl",
            "pts_idp_safety",
            "pts_idp_td",
            # L1.d additions (2026-05-03) - same DEF-row guard rationale
            "pts_idp_blk_kick",
            "pts_idp_int_ret_yd",
            "pts_idp_fum_rec_yd",
            "pts_idp_tkl_combined",
            "pts_idp_blk_kick_td",
            "pts_idp_fum_ret_td",
            "pts_idp_xpr",
            "pts_idp_pass_def_3p",
        ]:
            result.loc[non_idp, col] = 0.0

    return result


def get_precalc_columns_sql() -> str:
    """
    Get SQL column definitions for ALTER TABLE.

    Returns:
        SQL string for adding columns
    """
    return ", ".join([f"ADD COLUMN IF NOT EXISTS {col} DOUBLE" for col in PRECALC_COLUMNS])


def get_precalc_columns_list() -> list[str]:
    """
    Get list of pre-calculated column names.

    Returns:
        List of column names
    """
    return PRECALC_COLUMNS.copy()


if __name__ == "__main__":
    # Test with sample data
    print("Fantasy Points Calculator - Test Mode")
    print("=" * 50)

    # Create sample data
    test_data = {
        "player": ["Josh Allen", "Derrick Henry", "Justin Jefferson", "Jake Elliott", "Bills DST"],
        "nfl_position": ["QB", "RB", "WR", "K", "DEF"],
        "passing_yards": [300, 0, 0, 0, 0],
        "passing_tds": [3, 0, 0, 0, 0],
        "interceptions": [1, 0, 0, 0, 0],
        "rushing_yards": [40, 150, 5, 0, 0],
        "rushing_tds": [1, 2, 0, 0, 0],
        "receiving_yards": [0, 10, 120, 0, 0],
        "receiving_tds": [0, 0, 1, 0, 0],
        "receptions": [0, 2, 8, 0, 0],
        "fg_made_30_39": [0, 0, 0, 2, 0],
        "fg_made_40_49": [0, 0, 0, 1, 0],
        "pat_made": [0, 0, 0, 4, 0],
        "def_sacks": [0, 0, 0, 0, 3],
        "def_interceptions": [0, 0, 0, 0, 2],
        "pts_allow_14_20": [0, 0, 0, 0, 1],
    }

    df = pd.DataFrame(test_data)
    df = calculate_all_fantasy_points(df)

    print("\nResults:")
    for _, row in df.iterrows():
        print(f"\n{row['player']} ({row['nfl_position']}):")
        for col in PRECALC_COLUMNS:
            if row[col] != 0:
                print(f"  {col}: {row[col]:.1f}")

    print("\n\nExample league combinations:")
    print("-" * 40)

    # Standard 4pt pass TD, 0 PPR
    df["fantasy_std"] = (
        df["pts_pass_4pt"]
        + df["pts_rush"]
        + df["pts_rec_0ppr"]
        + df["pts_misc"]
        + df["pts_k_std"]
        + df["pts_def_std"]
    )

    # Half PPR, 6pt pass TD
    df["fantasy_ppr_6pt"] = (
        df["pts_pass_6pt"]
        + df["pts_rush"]
        + df["pts_rec_half"]
        + df["pts_misc"]
        + df["pts_k_std"]
        + df["pts_def_std"]
    )

    for _, row in df.iterrows():
        print(f"{row['player']}: Std={row['fantasy_std']:.1f}, Half-PPR-6pt={row['fantasy_ppr_6pt']:.1f}")


# =============================================================================
# COMPOSITE FANTASY POINTS & PRE-COMPUTED RANKS
# =============================================================================
# These columns are added to the super_table to enable optimal lineup
# calculations via rank lookups instead of runtime sorting.

# Composite fantasy points columns (27 variants)
# 9 standard variants (3 pass-TD x 3 PPR)
# 3 TE premium variants (3 pass-TD x TEP)
# 3 point-per-first-down variants (3 pass-TD x PPFD)
# 9 return-yards variants (3 pass-TD x 3 PPR + ret)
# 3 TE premium + return-yards variants (3 pass-TD x TEP + ret)
COMPOSITE_POINTS_COLUMNS = [
    # 4pt passing TD variants
    "fpts_4pt_0ppr",
    "fpts_4pt_half",
    "fpts_4pt_ppr",
    # 5pt passing TD variants
    "fpts_5pt_0ppr",
    "fpts_5pt_half",
    "fpts_5pt_ppr",
    # 6pt passing TD variants
    "fpts_6pt_0ppr",
    "fpts_6pt_half",
    "fpts_6pt_ppr",
    # TE Premium variants (TEs get 1.5 PPR, non-TEs get 1.0 PPR)
    "fpts_4pt_tep",
    "fpts_5pt_tep",
    "fpts_6pt_tep",
    # Point-per-first-down variants: half PPR + 0.5 per rush/receiving first down
    "fpts_4pt_ppfd",
    "fpts_5pt_ppfd",
    "fpts_6pt_ppfd",
    # Return yards variants (includes 1 pt per 25 return yards)
    "fpts_4pt_0ppr_ret",
    "fpts_4pt_half_ret",
    "fpts_4pt_ppr_ret",
    "fpts_5pt_0ppr_ret",
    "fpts_5pt_half_ret",
    "fpts_5pt_ppr_ret",
    "fpts_6pt_0ppr_ret",
    "fpts_6pt_half_ret",
    "fpts_6pt_ppr_ret",
    # TE Premium + Return yards variants
    "fpts_4pt_tep_ret",
    "fpts_5pt_tep_ret",
    "fpts_6pt_tep_ret",
]

# Position rank columns (30 total)
POSITION_RANK_COLUMNS = [
    # QB: varies by pass TD (3)
    "rank_qb_4pt",
    "rank_qb_5pt",
    "rank_qb_6pt",
    # RB: varies by PPR (3)
    "rank_rb_0ppr",
    "rank_rb_half",
    "rank_rb_ppr",
    "rank_rb_ppfd",
    # WR: varies by PPR (3)
    "rank_wr_0ppr",
    "rank_wr_half",
    "rank_wr_ppr",
    "rank_wr_ppfd",
    # TE: varies by PPR (3) + TE Premium (1)
    "rank_te_0ppr",
    "rank_te_half",
    "rank_te_ppr",
    "rank_te_tep",
    "rank_te_ppfd",
    # K/DEF: no variants (2)
    "rank_k",
    "rank_def",
    # IDP: LB, DL, DB - 4 variants each (12)
    "rank_lb_std",
    "rank_lb_premium",
    "rank_lb_tackle_heavy",
    "rank_lb_big_play",
    "rank_dl_std",
    "rank_dl_premium",
    "rank_dl_tackle_heavy",
    "rank_dl_big_play",
    "rank_db_std",
    "rank_db_premium",
    "rank_db_tackle_heavy",
    "rank_db_big_play",
]

# Flex rank columns (41 total)
FLEX_RANK_COLUMNS = [
    # Standard flex (RB/WR/TE) - 3 + 1 TEP
    "rank_flex_0ppr",
    "rank_flex_half",
    "rank_flex_ppr",
    "rank_flex_ppfd",
    "rank_flex_tep",
    # Receiving flex (WR/TE) - 3 + 1 TEP
    "rank_recflex_0ppr",
    "rank_recflex_half",
    "rank_recflex_ppr",
    "rank_recflex_ppfd",
    "rank_recflex_tep",
    # W/R flex (RB/WR only, no TE) - 3
    "rank_wrflex_0ppr",
    "rank_wrflex_half",
    "rank_wrflex_ppr",
    "rank_wrflex_ppfd",
    # R/T flex (RB/TE only, no WR) - 3
    "rank_rtflex_0ppr",
    "rank_rtflex_half",
    "rank_rtflex_ppr",
    "rank_rtflex_ppfd",
    # SuperFlex (QB/RB/WR/TE) - 9 (needs pass TD and PPR variants)
    "rank_sflex_4pt_0ppr",
    "rank_sflex_4pt_half",
    "rank_sflex_4pt_ppr",
    "rank_sflex_5pt_0ppr",
    "rank_sflex_5pt_half",
    "rank_sflex_5pt_ppr",
    "rank_sflex_6pt_0ppr",
    "rank_sflex_6pt_half",
    "rank_sflex_6pt_ppr",
    "rank_sflex_4pt_tep",
    "rank_sflex_6pt_tep",
    "rank_sflex_4pt_ppfd",
    "rank_sflex_6pt_ppfd",
    # IDP Flex (LB/DL/DB combined) - 4 variants
    "rank_idp_flex_std",
    "rank_idp_flex_premium",
    "rank_idp_flex_tackle_heavy",
    "rank_idp_flex_big_play",
]

# Season position rank columns (30 total - same structure as weekly, aggregated by year)
SEASON_POSITION_RANK_COLUMNS = [
    # QB: varies by pass TD (3)
    "rank_season_qb_4pt",
    "rank_season_qb_5pt",
    "rank_season_qb_6pt",
    # RB: varies by PPR (3)
    "rank_season_rb_0ppr",
    "rank_season_rb_half",
    "rank_season_rb_ppr",
    "rank_season_rb_ppfd",
    # WR: varies by PPR (3)
    "rank_season_wr_0ppr",
    "rank_season_wr_half",
    "rank_season_wr_ppr",
    "rank_season_wr_ppfd",
    # TE: varies by PPR (3) + TE Premium (1)
    "rank_season_te_0ppr",
    "rank_season_te_half",
    "rank_season_te_ppr",
    "rank_season_te_tep",
    "rank_season_te_ppfd",
    # K/DEF: no variants (2)
    "rank_season_k",
    "rank_season_def",
    # IDP: LB, DL, DB - 4 variants each (12)
    "rank_season_lb_std",
    "rank_season_lb_premium",
    "rank_season_lb_tackle_heavy",
    "rank_season_lb_big_play",
    "rank_season_dl_std",
    "rank_season_dl_premium",
    "rank_season_dl_tackle_heavy",
    "rank_season_dl_big_play",
    "rank_season_db_std",
    "rank_season_db_premium",
    "rank_season_db_tackle_heavy",
    "rank_season_db_big_play",
]

# Season flex rank columns (41 total)
SEASON_FLEX_RANK_COLUMNS = [
    # Standard flex (RB/WR/TE) - 3 + 1 TEP
    "rank_season_flex_0ppr",
    "rank_season_flex_half",
    "rank_season_flex_ppr",
    "rank_season_flex_ppfd",
    "rank_season_flex_tep",
    # Receiving flex (WR/TE) - 3 + 1 TEP
    "rank_season_recflex_0ppr",
    "rank_season_recflex_half",
    "rank_season_recflex_ppr",
    "rank_season_recflex_ppfd",
    "rank_season_recflex_tep",
    # W/R flex (RB/WR only, no TE) - 3
    "rank_season_wrflex_0ppr",
    "rank_season_wrflex_half",
    "rank_season_wrflex_ppr",
    "rank_season_wrflex_ppfd",
    # R/T flex (RB/TE only, no WR) - 3
    "rank_season_rtflex_0ppr",
    "rank_season_rtflex_half",
    "rank_season_rtflex_ppr",
    "rank_season_rtflex_ppfd",
    # SuperFlex (QB/RB/WR/TE) - 9 (needs pass TD and PPR variants)
    "rank_season_sflex_4pt_0ppr",
    "rank_season_sflex_4pt_half",
    "rank_season_sflex_4pt_ppr",
    "rank_season_sflex_5pt_0ppr",
    "rank_season_sflex_5pt_half",
    "rank_season_sflex_5pt_ppr",
    "rank_season_sflex_6pt_0ppr",
    "rank_season_sflex_6pt_half",
    "rank_season_sflex_6pt_ppr",
    "rank_season_sflex_4pt_tep",
    "rank_season_sflex_6pt_tep",
    "rank_season_sflex_4pt_ppfd",
    "rank_season_sflex_6pt_ppfd",
    # IDP Flex (LB/DL/DB combined) - 4 variants
    "rank_season_idp_flex_std",
    "rank_season_idp_flex_premium",
    "rank_season_idp_flex_tackle_heavy",
    "rank_season_idp_flex_big_play",
]

# All-time position rank columns (30 total - career aggregates, no year partition)
ALLTIME_POSITION_RANK_COLUMNS = [
    # QB: varies by pass TD (3)
    "rank_alltime_qb_4pt",
    "rank_alltime_qb_5pt",
    "rank_alltime_qb_6pt",
    # RB: varies by PPR (3)
    "rank_alltime_rb_0ppr",
    "rank_alltime_rb_half",
    "rank_alltime_rb_ppr",
    "rank_alltime_rb_ppfd",
    # WR: varies by PPR (3)
    "rank_alltime_wr_0ppr",
    "rank_alltime_wr_half",
    "rank_alltime_wr_ppr",
    "rank_alltime_wr_ppfd",
    # TE: varies by PPR (3) + TE Premium (1)
    "rank_alltime_te_0ppr",
    "rank_alltime_te_half",
    "rank_alltime_te_ppr",
    "rank_alltime_te_tep",
    "rank_alltime_te_ppfd",
    # K/DEF: no variants (2)
    "rank_alltime_k",
    "rank_alltime_def",
    # IDP: LB, DL, DB - 4 variants each (12)
    "rank_alltime_lb_std",
    "rank_alltime_lb_premium",
    "rank_alltime_lb_tackle_heavy",
    "rank_alltime_lb_big_play",
    "rank_alltime_dl_std",
    "rank_alltime_dl_premium",
    "rank_alltime_dl_tackle_heavy",
    "rank_alltime_dl_big_play",
    "rank_alltime_db_std",
    "rank_alltime_db_premium",
    "rank_alltime_db_tackle_heavy",
    "rank_alltime_db_big_play",
]

# All-time flex rank columns (41 total)
ALLTIME_FLEX_RANK_COLUMNS = [
    # Standard flex (RB/WR/TE) - 3 + 1 TEP
    "rank_alltime_flex_0ppr",
    "rank_alltime_flex_half",
    "rank_alltime_flex_ppr",
    "rank_alltime_flex_ppfd",
    "rank_alltime_flex_tep",
    # Receiving flex (WR/TE) - 3 + 1 TEP
    "rank_alltime_recflex_0ppr",
    "rank_alltime_recflex_half",
    "rank_alltime_recflex_ppr",
    "rank_alltime_recflex_ppfd",
    "rank_alltime_recflex_tep",
    # W/R flex (RB/WR only, no TE) - 3
    "rank_alltime_wrflex_0ppr",
    "rank_alltime_wrflex_half",
    "rank_alltime_wrflex_ppr",
    "rank_alltime_wrflex_ppfd",
    # R/T flex (RB/TE only, no WR) - 3
    "rank_alltime_rtflex_0ppr",
    "rank_alltime_rtflex_half",
    "rank_alltime_rtflex_ppr",
    "rank_alltime_rtflex_ppfd",
    # SuperFlex (QB/RB/WR/TE) - 9 (needs pass TD and PPR variants)
    "rank_alltime_sflex_4pt_0ppr",
    "rank_alltime_sflex_4pt_half",
    "rank_alltime_sflex_4pt_ppr",
    "rank_alltime_sflex_5pt_0ppr",
    "rank_alltime_sflex_5pt_half",
    "rank_alltime_sflex_5pt_ppr",
    "rank_alltime_sflex_6pt_0ppr",
    "rank_alltime_sflex_6pt_half",
    "rank_alltime_sflex_6pt_ppr",
    "rank_alltime_sflex_4pt_tep",
    "rank_alltime_sflex_6pt_tep",
    "rank_alltime_sflex_4pt_ppfd",
    "rank_alltime_sflex_6pt_ppfd",
    # IDP Flex (LB/DL/DB combined) - 4 variants
    "rank_alltime_idp_flex_std",
    "rank_alltime_idp_flex_premium",
    "rank_alltime_idp_flex_tackle_heavy",
    "rank_alltime_idp_flex_big_play",
]

# Rank-only columns (weekly + season + alltime). These should stay nullable
# integer-shaped for DuckDB/frontend consumers.
RANK_VALUE_COLUMNS = (
    POSITION_RANK_COLUMNS
    + FLEX_RANK_COLUMNS
    + SEASON_POSITION_RANK_COLUMNS
    + SEASON_FLEX_RANK_COLUMNS
    + ALLTIME_POSITION_RANK_COLUMNS
    + ALLTIME_FLEX_RANK_COLUMNS
)

# All new columns for ranks feature (composite points + ranks)
ALL_RANK_COLUMNS = COMPOSITE_POINTS_COLUMNS + RANK_VALUE_COLUMNS


def calculate_composite_fantasy_points(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 6 composite fantasy point columns by summing component columns.

    These composite columns are used for ranking players. Each variant represents
    a different scoring configuration (4pt vs 6pt passing TDs, different PPR settings).

    Args:
        df: DataFrame with pre-calculated category columns (pts_pass_4pt, pts_rush, etc.)

    Returns:
        DataFrame with fpts_* columns added
    """
    result = df.copy()

    # Check if we have the required component columns
    required = [
        "pts_pass_4pt",
        "pts_pass_6pt",
        "pts_rush",
        "pts_rec_0ppr",
        "pts_rec_half",
        "pts_rec_ppr",
        "pts_misc",
        "pts_k_std",
        "pts_def_std",
    ]
    missing = [col for col in required if col not in result.columns]
    if missing:
        # Try to calculate them first
        result = calculate_all_fantasy_points(result)

    # 4pt passing TD variants
    result["fpts_4pt_0ppr"] = (
        safe_col(result, "pts_pass_4pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_0ppr")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_4pt_half"] = (
        safe_col(result, "pts_pass_4pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_half")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_4pt_ppr"] = (
        safe_col(result, "pts_pass_4pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_ppr")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    # 5pt passing TD variants
    result["fpts_5pt_0ppr"] = (
        safe_col(result, "pts_pass_5pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_0ppr")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_5pt_half"] = (
        safe_col(result, "pts_pass_5pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_half")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_5pt_ppr"] = (
        safe_col(result, "pts_pass_5pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_ppr")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    # 6pt passing TD variants
    result["fpts_6pt_0ppr"] = (
        safe_col(result, "pts_pass_6pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_0ppr")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_6pt_half"] = (
        safe_col(result, "pts_pass_6pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_half")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_6pt_ppr"] = (
        safe_col(result, "pts_pass_6pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_ppr")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    # TE Premium variants (TEs get 1.5 PPR, non-TEs get 1.0 PPR)
    result["fpts_4pt_tep"] = (
        safe_col(result, "pts_pass_4pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_tep")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_5pt_tep"] = (
        safe_col(result, "pts_pass_5pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_tep")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    result["fpts_6pt_tep"] = (
        safe_col(result, "pts_pass_6pt")
        + safe_col(result, "pts_rush")
        + safe_col(result, "pts_rec_tep")
        + safe_col(result, "pts_misc")
        + safe_col(result, "pts_k_std")
        + safe_col(result, "pts_def_std")
    )

    # === A3: fold the fumble residual into every BASE composite (ppfd/ret variants below derive from
    # these and inherit it). Added ONLY to the standard composites -- pts_rush/pts_rec + the per-league
    # correction baseline are untouched, so per-league totals stay invariant. ===
    _fum_resid = safe_col(result, "pts_fum_residual")
    for _fc in ("fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr",
                "fpts_5pt_0ppr", "fpts_5pt_half", "fpts_5pt_ppr",
                "fpts_6pt_0ppr", "fpts_6pt_half", "fpts_6pt_ppr",
                "fpts_4pt_tep", "fpts_5pt_tep", "fpts_6pt_tep"):
        if _fc in result.columns:
            result[_fc] = safe_col(result, _fc) + _fum_resid

    # Point-per-first-down variants. PPFD here follows the research preset:
    # half PPR plus 0.5 per rushing/receiving first down.
    first_down_bonus = 0.5 * (safe_col(result, "rushing_first_downs") + safe_col(result, "receiving_first_downs"))
    result["fpts_4pt_ppfd"] = result["fpts_4pt_half"] + first_down_bonus
    result["fpts_5pt_ppfd"] = result["fpts_5pt_half"] + first_down_bonus
    result["fpts_6pt_ppfd"] = result["fpts_6pt_half"] + first_down_bonus

    # Return yards variants (base columns + pts_ret_yds)
    # These are for leagues that award points for kick/punt return yards
    ret_yds = safe_col(result, "pts_ret_yds")

    # Standard PPR + return yards
    result["fpts_4pt_0ppr_ret"] = result["fpts_4pt_0ppr"] + ret_yds
    result["fpts_4pt_half_ret"] = result["fpts_4pt_half"] + ret_yds
    result["fpts_4pt_ppr_ret"] = result["fpts_4pt_ppr"] + ret_yds

    result["fpts_5pt_0ppr_ret"] = result["fpts_5pt_0ppr"] + ret_yds
    result["fpts_5pt_half_ret"] = result["fpts_5pt_half"] + ret_yds
    result["fpts_5pt_ppr_ret"] = result["fpts_5pt_ppr"] + ret_yds

    result["fpts_6pt_0ppr_ret"] = result["fpts_6pt_0ppr"] + ret_yds
    result["fpts_6pt_half_ret"] = result["fpts_6pt_half"] + ret_yds
    result["fpts_6pt_ppr_ret"] = result["fpts_6pt_ppr"] + ret_yds

    # TE Premium + return yards
    result["fpts_4pt_tep_ret"] = result["fpts_4pt_tep"] + ret_yds
    result["fpts_5pt_tep_ret"] = result["fpts_5pt_tep"] + ret_yds
    result["fpts_6pt_tep_ret"] = result["fpts_6pt_tep"] + ret_yds

    return result


def calculate_position_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add position rank columns within each year/week.

    Ranks players within their position group for each week. Uses min ranking
    (ties get same rank, next rank skips).

    Position rank columns:
    - QB: rank_qb_4pt, rank_qb_6pt (pass TD matters)
    - RB/WR/TE: rank_*_0ppr, rank_*_half, rank_*_ppr (PPR matters)
    - K/DEF: rank_k, rank_def (no variants)

    Args:
        df: DataFrame with composite fantasy points columns

    Returns:
        DataFrame with rank_* columns added
    """
    result = df.copy()

    # Ensure we have composite points
    if "fpts_4pt_half" not in result.columns:
        result = calculate_composite_fantasy_points(result)

    # Ensure nfl_position column exists
    if "nfl_position" not in result.columns:
        print("[WARN] calculate_position_ranks: 'nfl_position' column missing, skipping ranks")
        return result

    # QB ranks - varies by pass TD, not PPR (use half PPR as base)
    # Use rolling_total as tiebreaker to ensure unique ranks when weekly points tie
    qb_mask = result["nfl_position"] == "QB"
    if qb_mask.any():
        result.loc[qb_mask, "rank_qb_4pt"] = _rank_with_tiebreaker(
            result[qb_mask], ["year", "week"], "fpts_4pt_half", "rolling_total_4pt_half"
        )
        result.loc[qb_mask, "rank_qb_5pt"] = _rank_with_tiebreaker(
            result[qb_mask], ["year", "week"], "fpts_5pt_half", "rolling_total_5pt_half"
        )
        result.loc[qb_mask, "rank_qb_6pt"] = _rank_with_tiebreaker(
            result[qb_mask], ["year", "week"], "fpts_6pt_half", "rolling_total_6pt_half"
        )

    # RB/WR/TE ranks - varies by PPR, use 4pt pass TD as base
    # Use rolling_total as tiebreaker to ensure unique ranks when weekly points tie
    for pos in ["RB", "WR", "TE"]:
        pos_mask = result["nfl_position"] == pos
        if not pos_mask.any():
            continue

        col_prefix = f"rank_{pos.lower()}"

        for ppr_suffix, fpts_col, tiebreaker_col in [
            ("0ppr", "fpts_4pt_0ppr", "rolling_total_4pt_0ppr"),
            ("half", "fpts_4pt_half", "rolling_total_4pt_half"),
            ("ppr", "fpts_4pt_ppr", "rolling_total_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd", "rolling_total_4pt_ppfd"),
        ]:
            if fpts_col in result.columns:
                result.loc[pos_mask, f"{col_prefix}_{ppr_suffix}"] = _rank_with_tiebreaker(
                    result[pos_mask], ["year", "week"], fpts_col, tiebreaker_col
                )

    # TE Premium rank - TEs ranked with TE premium scoring
    # Use rolling_total as tiebreaker to ensure unique ranks when weekly points tie
    te_mask = result["nfl_position"] == "TE"
    if te_mask.any() and "fpts_4pt_tep" in result.columns:
        result.loc[te_mask, "rank_te_tep"] = _rank_with_tiebreaker(
            result[te_mask], ["year", "week"], "fpts_4pt_tep", "rolling_total_4pt_tep"
        )

    # K rank - single variant with tie-breaker
    # CRITICAL: Use 'first' method for fallback to ensure unique ranks
    # 'min' would give ties the same rank, breaking optimal lineup calculation
    k_mask = result["nfl_position"] == "K"
    if k_mask.any():
        if "rolling_total_k" in result.columns:
            result.loc[k_mask, "rank_k"] = _rank_with_tiebreaker(
                result[k_mask], ["year", "week"], "pts_k_yds", "rolling_total_k"
            )
        else:
            result.loc[k_mask, "rank_k"] = (
                result[k_mask].groupby(["year", "week"])["pts_k_yds"].rank(method="first", ascending=False)
            )

    # DEF rank - single variant with tie-breaker
    # CRITICAL: Use 'first' method for fallback to ensure unique ranks
    def_mask = result["nfl_position"] == "DEF"
    if def_mask.any():
        if "rolling_total_def" in result.columns:
            result.loc[def_mask, "rank_def"] = _rank_with_tiebreaker(
                result[def_mask], ["year", "week"], "pts_def_std", "rolling_total_def"
            )
        else:
            result.loc[def_mask, "rank_def"] = (
                result[def_mask].groupby(["year", "week"])["pts_def_std"].rank(method="first", ascending=False)
            )

    # === IDP Position Ranks ===
    # Use method='first' for deterministic ranking (no rolling_total for IDP)
    # LB (linebackers) - includes ILB, OLB, MLB, LB
    lb_positions = ["LB", "ILB", "OLB", "MLB"]
    lb_mask = result["nfl_position"].isin(lb_positions)
    if lb_mask.any():
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in result.columns:
                result.loc[lb_mask, f"rank_lb_{idp_suffix}"] = (
                    result[lb_mask].groupby(["year", "week"])[pts_col].rank(method="first", ascending=False)
                )

    # DL (defensive line) - includes DE, DT, NT, ED
    dl_positions = ["DL", "DE", "DT", "NT", "ED"]
    dl_mask = result["nfl_position"].isin(dl_positions)
    if dl_mask.any():
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in result.columns:
                result.loc[dl_mask, f"rank_dl_{idp_suffix}"] = (
                    result[dl_mask].groupby(["year", "week"])[pts_col].rank(method="first", ascending=False)
                )

    # DB (defensive backs) - includes CB, S, SS, FS, SAF
    db_positions = ["DB", "CB", "S", "SS", "FS", "SAF"]
    db_mask = result["nfl_position"].isin(db_positions)
    if db_mask.any():
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in result.columns:
                result.loc[db_mask, f"rank_db_{idp_suffix}"] = (
                    result[db_mask].groupby(["year", "week"])[pts_col].rank(method="first", ascending=False)
                )

    return result


def _rank_with_tiebreaker(
    df_subset: pd.DataFrame, group_cols: list, pts_col: str, tiebreaker_col: str = None
) -> pd.Series:
    """
    Rank players by points with optional tie-breaker.

    Uses rolling_total (cumulative season points) as tie-breaker when players
    have identical weekly points. This ensures consistent, deterministic rankings.

    Args:
        df_subset: Filtered DataFrame with only eligible players
        group_cols: Columns to group by (e.g., ['year', 'week'])
        pts_col: Primary points column to rank by
        tiebreaker_col: Secondary column for tie-breaking (usually rolling_total)

    Returns:
        Series with rank values aligned to df_subset index
    """
    if tiebreaker_col and tiebreaker_col in df_subset.columns:
        # Create a composite sort key for deterministic ranking
        # Negate values so sorting ascending gives us descending order
        # Multiply pts by large factor to ensure it dominates the tiebreaker
        work_df = df_subset[group_cols + [pts_col, tiebreaker_col]].copy()
        work_df["_sort_key"] = -work_df[pts_col].fillna(0) * 1e10 - work_df[tiebreaker_col].fillna(0)
        # Rank within each group by the composite sort key (ascending = best first)
        return work_df.groupby(group_cols)["_sort_key"].rank(method="first", ascending=True)
    else:
        # Fallback to simple rank without tiebreaker (use 'first' for unique ranks)
        return df_subset.groupby(group_cols)[pts_col].rank(method="first", ascending=False)


def calculate_flex_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add flex/superflex rank columns within each year/week.

    Ranks players across combined position groups for flex slot eligibility:
    - FLEX (RB/WR/TE): rank_flex_*
    - REC_FLEX (WR/TE): rank_recflex_*
    - W/R FLEX (RB/WR only): rank_wrflex_*
    - R/T FLEX (RB/TE only): rank_rtflex_*
    - SUPER_FLEX (QB/RB/WR/TE): rank_sflex_*
    - IDP_FLEX (LB/DL/DB): rank_idp_flex_*

    Uses rolling_total as tie-breaker for consistent rankings.

    Args:
        df: DataFrame with composite fantasy points columns

    Returns:
        DataFrame with flex rank columns added
    """
    result = df.copy()

    # Ensure we have composite points
    if "fpts_4pt_half" not in result.columns:
        result = calculate_composite_fantasy_points(result)

    if "nfl_position" not in result.columns:
        print("[WARN] calculate_flex_ranks: 'nfl_position' column missing, skipping ranks")
        return result

    # Define PPR variants with their tiebreaker columns
    ppr_variants = [
        ("0ppr", "fpts_4pt_0ppr", "rolling_total_4pt_0ppr"),
        ("half", "fpts_4pt_half", "rolling_total_4pt_half"),
        ("ppr", "fpts_4pt_ppr", "rolling_total_4pt_ppr"),
        ("ppfd", "fpts_4pt_ppfd", "rolling_total_4pt_ppfd"),
    ]

    # Standard FLEX (RB/WR/TE)
    flex_mask = result["nfl_position"].isin(["RB", "WR", "TE"])
    if flex_mask.any():
        for ppr_suffix, fpts_col, tiebreaker_col in ppr_variants:
            if fpts_col in result.columns:
                result.loc[flex_mask, f"rank_flex_{ppr_suffix}"] = _rank_with_tiebreaker(
                    result[flex_mask], ["year", "week"], fpts_col, tiebreaker_col
                )
        # TE Premium flex rank
        if "fpts_4pt_tep" in result.columns:
            result.loc[flex_mask, "rank_flex_tep"] = _rank_with_tiebreaker(
                result[flex_mask], ["year", "week"], "fpts_4pt_tep", "rolling_total_4pt_ppr"
            )

    # REC_FLEX (WR/TE only)
    recflex_mask = result["nfl_position"].isin(["WR", "TE"])
    if recflex_mask.any():
        for ppr_suffix, fpts_col, tiebreaker_col in ppr_variants:
            if fpts_col in result.columns:
                result.loc[recflex_mask, f"rank_recflex_{ppr_suffix}"] = _rank_with_tiebreaker(
                    result[recflex_mask], ["year", "week"], fpts_col, tiebreaker_col
                )
        # TE Premium recflex rank
        if "fpts_4pt_tep" in result.columns:
            result.loc[recflex_mask, "rank_recflex_tep"] = _rank_with_tiebreaker(
                result[recflex_mask], ["year", "week"], "fpts_4pt_tep", "rolling_total_4pt_ppr"
            )

    # W/R FLEX (RB/WR only, no TE)
    wrflex_mask = result["nfl_position"].isin(["RB", "WR"])
    if wrflex_mask.any():
        for ppr_suffix, fpts_col, tiebreaker_col in ppr_variants:
            if fpts_col in result.columns:
                result.loc[wrflex_mask, f"rank_wrflex_{ppr_suffix}"] = _rank_with_tiebreaker(
                    result[wrflex_mask], ["year", "week"], fpts_col, tiebreaker_col
                )

    # R/T FLEX (RB/TE only, no WR)
    rtflex_mask = result["nfl_position"].isin(["RB", "TE"])
    if rtflex_mask.any():
        for ppr_suffix, fpts_col, tiebreaker_col in ppr_variants:
            if fpts_col in result.columns:
                result.loc[rtflex_mask, f"rank_rtflex_{ppr_suffix}"] = _rank_with_tiebreaker(
                    result[rtflex_mask], ["year", "week"], fpts_col, tiebreaker_col
                )

    # SuperFlex (QB/RB/WR/TE) - needs all 9 variants (3 TD × 3 PPR)
    sflex_mask = result["nfl_position"].isin(["QB", "RB", "WR", "TE"])
    if sflex_mask.any():
        for td_suffix in ["4pt", "5pt", "6pt"]:
            for ppr_suffix in ["0ppr", "half", "ppr"]:
                fpts_col = f"fpts_{td_suffix}_{ppr_suffix}"
                tiebreaker_col = f"rolling_total_{td_suffix}_{ppr_suffix}"
                rank_col = f"rank_sflex_{td_suffix}_{ppr_suffix}"
                if fpts_col in result.columns:
                    result.loc[sflex_mask, rank_col] = _rank_with_tiebreaker(
                        result[sflex_mask], ["year", "week"], fpts_col, tiebreaker_col
                    )
        for td_suffix in ["4pt", "6pt"]:
            for suffix in ["tep", "ppfd"]:
                fpts_col = f"fpts_{td_suffix}_{suffix}"
                tiebreaker_col = f"rolling_total_{td_suffix}_{suffix}"
                rank_col = f"rank_sflex_{td_suffix}_{suffix}"
                if fpts_col in result.columns:
                    result.loc[sflex_mask, rank_col] = _rank_with_tiebreaker(
                        result[sflex_mask], ["year", "week"], fpts_col, tiebreaker_col
                    )

    # IDP Flex (LB/DL/DB combined) - 4 IDP scoring variants
    # Use method='first' for deterministic ranking (no rolling_total for IDP)
    idp_all_positions = ["LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "DB", "CB", "S", "SS", "FS", "SAF"]
    idp_flex_mask = result["nfl_position"].isin(idp_all_positions)
    if idp_flex_mask.any():
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in result.columns:
                # IDP doesn't have rolling_total, use method='first' for unique ranks
                result.loc[idp_flex_mask, f"rank_idp_flex_{idp_suffix}"] = (
                    result[idp_flex_mask].groupby(["year", "week"])[pts_col].rank(method="first", ascending=False)
                )

    return result


def calculate_season_position_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add season position rank columns (aggregated by year).

    Aggregates weekly fantasy points into season totals, then ranks players
    within their position group for each year.

    Args:
        df: DataFrame with composite fantasy points columns

    Returns:
        DataFrame with rank_season_* columns added
    """
    result = df.copy()

    # Ensure we have composite points
    if "fpts_4pt_half" not in result.columns:
        result = calculate_composite_fantasy_points(result)

    if "nfl_position" not in result.columns or "NFL_player_id" not in result.columns:
        print("[WARN] calculate_season_position_ranks: Required columns missing, skipping")
        return result

    # Columns to aggregate for season totals
    fpts_cols = [
        "fpts_4pt_0ppr",
        "fpts_4pt_half",
        "fpts_4pt_ppr",
        "fpts_5pt_0ppr",
        "fpts_5pt_half",
        "fpts_5pt_ppr",
        "fpts_6pt_0ppr",
        "fpts_6pt_half",
        "fpts_6pt_ppr",
        "fpts_4pt_tep",
        "fpts_5pt_tep",
        "fpts_6pt_tep",
        "fpts_4pt_ppfd",
        "fpts_5pt_ppfd",
        "fpts_6pt_ppfd",
        "pts_k_std",
        "pts_k_yds",
        "pts_def_std",
        "pts_idp_std",
        "pts_idp_premium",
        "pts_idp_tackle_heavy",
        "pts_idp_big_play",
    ]

    # Aggregate weekly points to season totals
    agg_dict = {col: "sum" for col in fpts_cols if col in result.columns}
    agg_dict["nfl_position"] = "first"  # Keep position

    season_totals = result.groupby(["NFL_player_id", "year"]).agg(agg_dict).reset_index()

    # QB season ranks - use method='first' for deterministic unique ranks
    qb_mask = season_totals["nfl_position"] == "QB"
    if qb_mask.any():
        season_totals.loc[qb_mask, "rank_season_qb_4pt"] = (
            season_totals[qb_mask].groupby("year")["fpts_4pt_half"].rank(method="first", ascending=False)
        )
        if "fpts_5pt_half" in season_totals.columns:
            season_totals.loc[qb_mask, "rank_season_qb_5pt"] = (
                season_totals[qb_mask].groupby("year")["fpts_5pt_half"].rank(method="first", ascending=False)
            )
        season_totals.loc[qb_mask, "rank_season_qb_6pt"] = (
            season_totals[qb_mask].groupby("year")["fpts_6pt_half"].rank(method="first", ascending=False)
        )

    # RB/WR/TE season ranks - use method='first' for deterministic unique ranks
    for pos in ["RB", "WR", "TE"]:
        pos_mask = season_totals["nfl_position"] == pos
        if not pos_mask.any():
            continue
        col_prefix = f"rank_season_{pos.lower()}"
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in season_totals.columns:
                season_totals.loc[pos_mask, f"{col_prefix}_{ppr_suffix}"] = (
                    season_totals[pos_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                )

    # TE Premium TE season rank
    te_mask = season_totals["nfl_position"] == "TE"
    if te_mask.any() and "fpts_4pt_tep" in season_totals.columns:
        season_totals.loc[te_mask, "rank_season_te_tep"] = (
            season_totals[te_mask].groupby("year")["fpts_4pt_tep"].rank(method="first", ascending=False)
        )

    # K season rank
    k_mask = season_totals["nfl_position"] == "K"
    if k_mask.any() and "pts_k_yds" in season_totals.columns:
        season_totals.loc[k_mask, "rank_season_k"] = (
            season_totals[k_mask].groupby("year")["pts_k_yds"].rank(method="first", ascending=False)
        )

    # DEF season rank
    def_mask = season_totals["nfl_position"] == "DEF"
    if def_mask.any() and "pts_def_std" in season_totals.columns:
        season_totals.loc[def_mask, "rank_season_def"] = (
            season_totals[def_mask].groupby("year")["pts_def_std"].rank(method="first", ascending=False)
        )

    # IDP season ranks (LB, DL, DB) - use method='first' for deterministic unique ranks
    lb_positions = ["LB", "ILB", "OLB", "MLB"]
    dl_positions = ["DL", "DE", "DT", "NT", "ED"]
    db_positions = ["DB", "CB", "S", "SS", "FS", "SAF"]

    for pos_group, pos_list, prefix in [
        ("lb", lb_positions, "rank_season_lb"),
        ("dl", dl_positions, "rank_season_dl"),
        ("db", db_positions, "rank_season_db"),
    ]:
        pos_mask = season_totals["nfl_position"].isin(pos_list)
        if not pos_mask.any():
            continue
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in season_totals.columns:
                season_totals.loc[pos_mask, f"{prefix}_{idp_suffix}"] = (
                    season_totals[pos_mask].groupby("year")[pts_col].rank(method="first", ascending=False)
                )

    # Join season ranks back to weekly data
    season_rank_cols = [c for c in season_totals.columns if c.startswith("rank_season_")]
    if season_rank_cols:
        merge_cols = ["NFL_player_id", "year"] + season_rank_cols
        result = result.merge(season_totals[merge_cols], on=["NFL_player_id", "year"], how="left")

    return result


def calculate_season_flex_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add season flex rank columns (aggregated by year).

    Args:
        df: DataFrame with composite fantasy points columns

    Returns:
        DataFrame with rank_season_flex_* columns added
    """
    result = df.copy()

    if "fpts_4pt_half" not in result.columns:
        result = calculate_composite_fantasy_points(result)

    if "nfl_position" not in result.columns or "NFL_player_id" not in result.columns:
        print("[WARN] calculate_season_flex_ranks: Required columns missing, skipping")
        return result

    # Aggregate weekly to season
    fpts_cols = [
        "fpts_4pt_0ppr",
        "fpts_4pt_half",
        "fpts_4pt_ppr",
        "fpts_5pt_0ppr",
        "fpts_5pt_half",
        "fpts_5pt_ppr",
        "fpts_6pt_0ppr",
        "fpts_6pt_half",
        "fpts_6pt_ppr",
        "fpts_4pt_tep",
        "fpts_5pt_tep",
        "fpts_6pt_tep",
        "fpts_4pt_ppfd",
        "fpts_5pt_ppfd",
        "fpts_6pt_ppfd",
        "pts_idp_std",
        "pts_idp_premium",
        "pts_idp_tackle_heavy",
        "pts_idp_big_play",
    ]
    agg_dict = {col: "sum" for col in fpts_cols if col in result.columns}
    agg_dict["nfl_position"] = "first"

    season_totals = result.groupby(["NFL_player_id", "year"]).agg(agg_dict).reset_index()

    # Standard FLEX (RB/WR/TE) - use method='first' for deterministic unique ranks
    flex_mask = season_totals["nfl_position"].isin(["RB", "WR", "TE"])
    if flex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in season_totals.columns:
                season_totals.loc[flex_mask, f"rank_season_flex_{ppr_suffix}"] = (
                    season_totals[flex_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                )
        # TE Premium flex
        if "fpts_4pt_tep" in season_totals.columns:
            season_totals.loc[flex_mask, "rank_season_flex_tep"] = (
                season_totals[flex_mask].groupby("year")["fpts_4pt_tep"].rank(method="first", ascending=False)
            )

    # REC_FLEX (WR/TE)
    recflex_mask = season_totals["nfl_position"].isin(["WR", "TE"])
    if recflex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in season_totals.columns:
                season_totals.loc[recflex_mask, f"rank_season_recflex_{ppr_suffix}"] = (
                    season_totals[recflex_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                )
        # TE Premium recflex
        if "fpts_4pt_tep" in season_totals.columns:
            season_totals.loc[recflex_mask, "rank_season_recflex_tep"] = (
                season_totals[recflex_mask].groupby("year")["fpts_4pt_tep"].rank(method="first", ascending=False)
            )

    # W/R FLEX (RB/WR only, no TE)
    wrflex_mask = season_totals["nfl_position"].isin(["RB", "WR"])
    if wrflex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in season_totals.columns:
                season_totals.loc[wrflex_mask, f"rank_season_wrflex_{ppr_suffix}"] = (
                    season_totals[wrflex_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                )

    # R/T FLEX (RB/TE only, no WR)
    rtflex_mask = season_totals["nfl_position"].isin(["RB", "TE"])
    if rtflex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in season_totals.columns:
                season_totals.loc[rtflex_mask, f"rank_season_rtflex_{ppr_suffix}"] = (
                    season_totals[rtflex_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                )

    # SuperFlex (QB/RB/WR/TE) - 9 variants (3 TD × 3 PPR)
    sflex_mask = season_totals["nfl_position"].isin(["QB", "RB", "WR", "TE"])
    if sflex_mask.any():
        for td_suffix in ["4pt", "5pt", "6pt"]:
            for ppr_suffix in ["0ppr", "half", "ppr"]:
                fpts_col = f"fpts_{td_suffix}_{ppr_suffix}"
                if fpts_col in season_totals.columns:
                    season_totals.loc[sflex_mask, f"rank_season_sflex_{td_suffix}_{ppr_suffix}"] = (
                        season_totals[sflex_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                    )
        for td_suffix in ["4pt", "6pt"]:
            for suffix in ["tep", "ppfd"]:
                fpts_col = f"fpts_{td_suffix}_{suffix}"
                if fpts_col in season_totals.columns:
                    season_totals.loc[sflex_mask, f"rank_season_sflex_{td_suffix}_{suffix}"] = (
                        season_totals[sflex_mask].groupby("year")[fpts_col].rank(method="first", ascending=False)
                    )

    # IDP Flex - use method='first' for deterministic unique ranks
    idp_positions = ["LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "DB", "CB", "S", "SS", "FS", "SAF"]
    idp_flex_mask = season_totals["nfl_position"].isin(idp_positions)
    if idp_flex_mask.any():
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in season_totals.columns:
                season_totals.loc[idp_flex_mask, f"rank_season_idp_flex_{idp_suffix}"] = (
                    season_totals[idp_flex_mask].groupby("year")[pts_col].rank(method="first", ascending=False)
                )

    # Join back
    season_rank_cols = [c for c in season_totals.columns if c.startswith("rank_season_")]
    if season_rank_cols:
        merge_cols = ["NFL_player_id", "year"] + season_rank_cols
        # Only merge columns that don't already exist
        new_cols = [c for c in season_rank_cols if c not in result.columns]
        if new_cols:
            result = result.merge(
                season_totals[["NFL_player_id", "year"] + new_cols], on=["NFL_player_id", "year"], how="left"
            )

    return result


def calculate_alltime_position_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add all-time position rank columns (career aggregates, no year partition).

    Aggregates all weekly fantasy points across entire career, then ranks
    players within their position group.

    Args:
        df: DataFrame with composite fantasy points columns

    Returns:
        DataFrame with rank_alltime_* columns added
    """
    result = df.copy()

    if "fpts_4pt_half" not in result.columns:
        result = calculate_composite_fantasy_points(result)

    if "nfl_position" not in result.columns or "NFL_player_id" not in result.columns:
        print("[WARN] calculate_alltime_position_ranks: Required columns missing, skipping")
        return result

    # Aggregate all weeks to career totals
    fpts_cols = [
        "fpts_4pt_0ppr",
        "fpts_4pt_half",
        "fpts_4pt_ppr",
        "fpts_5pt_0ppr",
        "fpts_5pt_half",
        "fpts_5pt_ppr",
        "fpts_6pt_0ppr",
        "fpts_6pt_half",
        "fpts_6pt_ppr",
        "fpts_4pt_tep",
        "fpts_5pt_tep",
        "fpts_6pt_tep",
        "fpts_4pt_ppfd",
        "fpts_5pt_ppfd",
        "fpts_6pt_ppfd",
        "pts_k_std",
        "pts_k_yds",
        "pts_def_std",
        "pts_idp_std",
        "pts_idp_premium",
        "pts_idp_tackle_heavy",
        "pts_idp_big_play",
    ]
    agg_dict = {col: "sum" for col in fpts_cols if col in result.columns}
    agg_dict["nfl_position"] = "first"

    career_totals = result.groupby("NFL_player_id").agg(agg_dict).reset_index()

    # QB alltime ranks - use method='first' for deterministic unique ranks
    qb_mask = career_totals["nfl_position"] == "QB"
    if qb_mask.any():
        career_totals.loc[qb_mask, "rank_alltime_qb_4pt"] = career_totals.loc[qb_mask, "fpts_4pt_half"].rank(
            method="first", ascending=False
        )
        if "fpts_5pt_half" in career_totals.columns:
            career_totals.loc[qb_mask, "rank_alltime_qb_5pt"] = career_totals.loc[qb_mask, "fpts_5pt_half"].rank(
                method="first", ascending=False
            )
        career_totals.loc[qb_mask, "rank_alltime_qb_6pt"] = career_totals.loc[qb_mask, "fpts_6pt_half"].rank(
            method="first", ascending=False
        )

    # RB/WR/TE alltime ranks - use method='first' for deterministic unique ranks
    for pos in ["RB", "WR", "TE"]:
        pos_mask = career_totals["nfl_position"] == pos
        if not pos_mask.any():
            continue
        col_prefix = f"rank_alltime_{pos.lower()}"
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in career_totals.columns:
                career_totals.loc[pos_mask, f"{col_prefix}_{ppr_suffix}"] = career_totals.loc[pos_mask, fpts_col].rank(
                    method="first", ascending=False
                )

    # TE Premium TE alltime rank
    te_mask = career_totals["nfl_position"] == "TE"
    if te_mask.any() and "fpts_4pt_tep" in career_totals.columns:
        career_totals.loc[te_mask, "rank_alltime_te_tep"] = career_totals.loc[te_mask, "fpts_4pt_tep"].rank(
            method="first", ascending=False
        )

    # K alltime rank
    k_mask = career_totals["nfl_position"] == "K"
    if k_mask.any() and "pts_k_yds" in career_totals.columns:
        career_totals.loc[k_mask, "rank_alltime_k"] = career_totals.loc[k_mask, "pts_k_yds"].rank(
            method="first", ascending=False
        )

    # DEF alltime rank
    def_mask = career_totals["nfl_position"] == "DEF"
    if def_mask.any() and "pts_def_std" in career_totals.columns:
        career_totals.loc[def_mask, "rank_alltime_def"] = career_totals.loc[def_mask, "pts_def_std"].rank(
            method="first", ascending=False
        )

    # IDP alltime ranks - use method='first' for deterministic unique ranks
    lb_positions = ["LB", "ILB", "OLB", "MLB"]
    dl_positions = ["DL", "DE", "DT", "NT", "ED"]
    db_positions = ["DB", "CB", "S", "SS", "FS", "SAF"]

    for pos_group, pos_list, prefix in [
        ("lb", lb_positions, "rank_alltime_lb"),
        ("dl", dl_positions, "rank_alltime_dl"),
        ("db", db_positions, "rank_alltime_db"),
    ]:
        pos_mask = career_totals["nfl_position"].isin(pos_list)
        if not pos_mask.any():
            continue
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in career_totals.columns:
                career_totals.loc[pos_mask, f"{prefix}_{idp_suffix}"] = career_totals.loc[pos_mask, pts_col].rank(
                    method="first", ascending=False
                )

    # Join career ranks back to weekly data
    alltime_rank_cols = [c for c in career_totals.columns if c.startswith("rank_alltime_")]
    if alltime_rank_cols:
        result = result.merge(career_totals[["NFL_player_id"] + alltime_rank_cols], on="NFL_player_id", how="left")

    return result


def calculate_alltime_flex_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add all-time flex rank columns (career aggregates).

    Args:
        df: DataFrame with composite fantasy points columns

    Returns:
        DataFrame with rank_alltime_flex_* columns added
    """
    result = df.copy()

    if "fpts_4pt_half" not in result.columns:
        result = calculate_composite_fantasy_points(result)

    if "nfl_position" not in result.columns or "NFL_player_id" not in result.columns:
        print("[WARN] calculate_alltime_flex_ranks: Required columns missing, skipping")
        return result

    # Aggregate to career totals
    fpts_cols = [
        "fpts_4pt_0ppr",
        "fpts_4pt_half",
        "fpts_4pt_ppr",
        "fpts_5pt_0ppr",
        "fpts_5pt_half",
        "fpts_5pt_ppr",
        "fpts_6pt_0ppr",
        "fpts_6pt_half",
        "fpts_6pt_ppr",
        "fpts_4pt_tep",
        "fpts_5pt_tep",
        "fpts_6pt_tep",
        "fpts_4pt_ppfd",
        "fpts_5pt_ppfd",
        "fpts_6pt_ppfd",
        "pts_idp_std",
        "pts_idp_premium",
        "pts_idp_tackle_heavy",
        "pts_idp_big_play",
    ]
    agg_dict = {col: "sum" for col in fpts_cols if col in result.columns}
    agg_dict["nfl_position"] = "first"

    career_totals = result.groupby("NFL_player_id").agg(agg_dict).reset_index()

    # Standard FLEX (RB/WR/TE) - use method='first' for deterministic unique ranks
    flex_mask = career_totals["nfl_position"].isin(["RB", "WR", "TE"])
    if flex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in career_totals.columns:
                career_totals.loc[flex_mask, f"rank_alltime_flex_{ppr_suffix}"] = career_totals.loc[
                    flex_mask, fpts_col
                ].rank(method="first", ascending=False)
        # TE Premium flex
        if "fpts_4pt_tep" in career_totals.columns:
            career_totals.loc[flex_mask, "rank_alltime_flex_tep"] = career_totals.loc[flex_mask, "fpts_4pt_tep"].rank(
                method="first", ascending=False
            )

    # REC_FLEX (WR/TE)
    recflex_mask = career_totals["nfl_position"].isin(["WR", "TE"])
    if recflex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in career_totals.columns:
                career_totals.loc[recflex_mask, f"rank_alltime_recflex_{ppr_suffix}"] = career_totals.loc[
                    recflex_mask, fpts_col
                ].rank(method="first", ascending=False)
        # TE Premium recflex
        if "fpts_4pt_tep" in career_totals.columns:
            career_totals.loc[recflex_mask, "rank_alltime_recflex_tep"] = career_totals.loc[
                recflex_mask, "fpts_4pt_tep"
            ].rank(method="first", ascending=False)

    # W/R FLEX (RB/WR only, no TE)
    wrflex_mask = career_totals["nfl_position"].isin(["RB", "WR"])
    if wrflex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in career_totals.columns:
                career_totals.loc[wrflex_mask, f"rank_alltime_wrflex_{ppr_suffix}"] = career_totals.loc[
                    wrflex_mask, fpts_col
                ].rank(method="first", ascending=False)

    # R/T FLEX (RB/TE only, no WR)
    rtflex_mask = career_totals["nfl_position"].isin(["RB", "TE"])
    if rtflex_mask.any():
        for ppr_suffix, fpts_col in [
            ("0ppr", "fpts_4pt_0ppr"),
            ("half", "fpts_4pt_half"),
            ("ppr", "fpts_4pt_ppr"),
            ("ppfd", "fpts_4pt_ppfd"),
        ]:
            if fpts_col in career_totals.columns:
                career_totals.loc[rtflex_mask, f"rank_alltime_rtflex_{ppr_suffix}"] = career_totals.loc[
                    rtflex_mask, fpts_col
                ].rank(method="first", ascending=False)

    # SuperFlex (QB/RB/WR/TE) - 9 variants (3 TD × 3 PPR)
    sflex_mask = career_totals["nfl_position"].isin(["QB", "RB", "WR", "TE"])
    if sflex_mask.any():
        for td_suffix in ["4pt", "5pt", "6pt"]:
            for ppr_suffix in ["0ppr", "half", "ppr"]:
                fpts_col = f"fpts_{td_suffix}_{ppr_suffix}"
                if fpts_col in career_totals.columns:
                    career_totals.loc[sflex_mask, f"rank_alltime_sflex_{td_suffix}_{ppr_suffix}"] = career_totals.loc[
                        sflex_mask, fpts_col
                    ].rank(method="first", ascending=False)
        for td_suffix in ["4pt", "6pt"]:
            for suffix in ["tep", "ppfd"]:
                fpts_col = f"fpts_{td_suffix}_{suffix}"
                if fpts_col in career_totals.columns:
                    career_totals.loc[sflex_mask, f"rank_alltime_sflex_{td_suffix}_{suffix}"] = career_totals.loc[
                        sflex_mask, fpts_col
                    ].rank(method="first", ascending=False)

    # IDP Flex - use method='first' for deterministic unique ranks
    idp_positions = ["LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "DB", "CB", "S", "SS", "FS", "SAF"]
    idp_flex_mask = career_totals["nfl_position"].isin(idp_positions)
    if idp_flex_mask.any():
        for idp_suffix, pts_col in [
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ]:
            if pts_col in career_totals.columns:
                career_totals.loc[idp_flex_mask, f"rank_alltime_idp_flex_{idp_suffix}"] = career_totals.loc[
                    idp_flex_mask, pts_col
                ].rank(method="first", ascending=False)

    # Join back
    alltime_rank_cols = [c for c in career_totals.columns if c.startswith("rank_alltime_")]
    if alltime_rank_cols:
        # Only merge columns that don't already exist
        new_cols = [c for c in alltime_rank_cols if c not in result.columns]
        if new_cols:
            result = result.merge(career_totals[["NFL_player_id"] + new_cols], on="NFL_player_id", how="left")

    return result


def calculate_all_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master function to add all composite points and rank columns.

    Calls in order:
    1. calculate_composite_fantasy_points() - 6 columns
    2. calculate_position_ranks() - 25 weekly position rank columns
    3. calculate_flex_ranks() - 16 weekly flex rank columns
    4. calculate_season_position_ranks() - 25 season position rank columns
    5. calculate_season_flex_ranks() - 16 season flex rank columns
    6. calculate_alltime_position_ranks() - 25 all-time position rank columns
    7. calculate_alltime_flex_ranks() - 16 all-time flex rank columns

    Total: 6 + 25 + 16 + 25 + 16 + 25 + 16 = 129 columns

    Args:
        df: DataFrame with base NFL stats and pre-calculated category columns

    Returns:
        DataFrame with all rank columns added
    """
    result = df.copy()

    # Ensure base category columns exist
    if "pts_pass_4pt" not in result.columns:
        result = calculate_all_fantasy_points(result)

    # Step 1: Add composite fantasy points
    print("[ranks] Step 1/7: Calculating composite fantasy points...")
    result = calculate_composite_fantasy_points(result)

    # Step 2: Add weekly position ranks
    print("[ranks] Step 2/7: Calculating weekly position ranks...")
    result = calculate_position_ranks(result)

    # Step 3: Add weekly flex ranks
    print("[ranks] Step 3/7: Calculating weekly flex ranks...")
    result = calculate_flex_ranks(result)

    # Step 4: Add season position ranks
    print("[ranks] Step 4/7: Calculating season position ranks...")
    result = calculate_season_position_ranks(result)

    # Step 5: Add season flex ranks
    print("[ranks] Step 5/7: Calculating season flex ranks...")
    result = calculate_season_flex_ranks(result)

    # Step 6: Add all-time position ranks
    print("[ranks] Step 6/7: Calculating all-time position ranks...")
    result = calculate_alltime_position_ranks(result)

    # Step 7: Add all-time flex ranks
    print("[ranks] Step 7/7: Calculating all-time flex ranks...")
    result = calculate_alltime_flex_ranks(result)

    result = _cast_rank_columns_to_nullable_int(result)

    print(f"[ranks] Complete: Added {len(ALL_RANK_COLUMNS)} rank columns")
    return result


def _cast_rank_columns_to_nullable_int(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rank outputs integer-shaped after pandas rank() calculations."""
    result = df.copy()
    for col in RANK_VALUE_COLUMNS:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce").astype("Int64")
    return result


def get_rank_columns_sql() -> str:
    """
    Get SQL column definitions for ALTER TABLE to add rank columns.

    Returns:
        SQL string for adding all rank-related columns (weekly + season + alltime)
    """
    # Composite points are DOUBLE
    composite_ddl = [f"ADD COLUMN IF NOT EXISTS {col} DOUBLE" for col in COMPOSITE_POINTS_COLUMNS]

    # All rank columns are INTEGER (weekly, season, and alltime)
    all_rank_cols = (
        POSITION_RANK_COLUMNS
        + FLEX_RANK_COLUMNS
        + SEASON_POSITION_RANK_COLUMNS
        + SEASON_FLEX_RANK_COLUMNS
        + ALLTIME_POSITION_RANK_COLUMNS
        + ALLTIME_FLEX_RANK_COLUMNS
    )
    rank_ddl = [f"ADD COLUMN IF NOT EXISTS {col} INTEGER" for col in all_rank_cols]

    return ", ".join(composite_ddl + rank_ddl)


def get_scoring_variant_for_league(ppr: float = 0.5, pass_td_pts: int = 4) -> str:
    """
    Get the scoring variant key based on league settings.

    Args:
        ppr: Points per reception (0, 0.5, or 1.0)
        pass_td_pts: Points per passing TD (4 or 6)

    Returns:
        Scoring variant key like "4pt_half" or "6pt_ppr"
    """
    td_key = "4pt" if pass_td_pts == 4 else "6pt"

    if ppr == 0:
        ppr_key = "0ppr"
    elif ppr >= 1.0:
        ppr_key = "ppr"
    else:
        ppr_key = "half"

    return f"{td_key}_{ppr_key}"


def get_rank_columns_for_league(ppr: float = 0.5, pass_td_pts: int = 4, idp_scoring: str = "std") -> dict:
    """
    Get the appropriate rank column names for a league's scoring settings.

    Args:
        ppr: Points per reception (0, 0.5, or 1.0)
        pass_td_pts: Points per passing TD (4 or 6)
        idp_scoring: IDP scoring variant ('std', 'premium', 'tackle_heavy', 'big_play')

    Returns:
        Dict mapping position/flex types to rank column names
    """
    td_key = "4pt" if pass_td_pts == 4 else "6pt"

    if ppr == 0:
        ppr_key = "0ppr"
    elif ppr >= 1.0:
        ppr_key = "ppr"
    else:
        ppr_key = "half"

    # Validate IDP scoring variant
    if idp_scoring not in ("std", "premium", "tackle_heavy", "big_play"):
        idp_scoring = "std"

    return {
        # Fantasy points column for this scoring
        "fpts": f"fpts_{td_key}_{ppr_key}",
        # Position ranks - Offense
        "QB": f"rank_qb_{td_key}",
        "RB": f"rank_rb_{ppr_key}",
        "WR": f"rank_wr_{ppr_key}",
        "TE": f"rank_te_{ppr_key}",
        "K": "rank_k",
        "DEF": "rank_def",
        # Flex ranks - Offense
        "FLEX": f"rank_flex_{ppr_key}",  # W/R/T
        "W/R/T": f"rank_flex_{ppr_key}",  # Yahoo alias
        "REC_FLEX": f"rank_recflex_{ppr_key}",  # WR/TE
        "W/T": f"rank_recflex_{ppr_key}",  # Yahoo alias
        "SUPER_FLEX": f"rank_sflex_{td_key}_{ppr_key}",  # QB/RB/WR/TE
        "Q/W/R/T": f"rank_sflex_{td_key}_{ppr_key}",  # Yahoo alias
        # IDP Position ranks
        "LB": f"rank_lb_{idp_scoring}",
        "DL": f"rank_dl_{idp_scoring}",
        "DB": f"rank_db_{idp_scoring}",
        # Individual IDP positions map to their group
        "ILB": f"rank_lb_{idp_scoring}",
        "OLB": f"rank_lb_{idp_scoring}",
        "MLB": f"rank_lb_{idp_scoring}",
        "DE": f"rank_dl_{idp_scoring}",
        "DT": f"rank_dl_{idp_scoring}",
        "NT": f"rank_dl_{idp_scoring}",
        "ED": f"rank_dl_{idp_scoring}",
        "CB": f"rank_db_{idp_scoring}",
        "S": f"rank_db_{idp_scoring}",
        "SS": f"rank_db_{idp_scoring}",
        "FS": f"rank_db_{idp_scoring}",
        "SAF": f"rank_db_{idp_scoring}",
        # IDP Flex (all defensive players combined)
        "IDP_FLEX": f"rank_idp_flex_{idp_scoring}",
        "IDP": f"rank_idp_flex_{idp_scoring}",  # Sleeper alias
        "D": f"rank_idp_flex_{idp_scoring}",  # Generic defense
        # IDP fantasy points column
        "fpts_idp": f"pts_idp_{idp_scoring}",
    }


def detect_idp_scoring_variant(scoring_rules: dict) -> str:
    """
    Detect IDP scoring variant from league settings.

    Analyzes tackle, sack, and INT point values to determine the closest variant.

    Args:
        scoring_rules: League scoring settings dict (Sleeper or Yahoo format)

    Returns:
        One of: 'std', 'premium', 'tackle_heavy', 'big_play'
    """
    # Try to get Sleeper format first
    settings = scoring_rules.get("scoring_settings", {})
    if not settings:
        settings = scoring_rules

    # Get IDP stat values (Sleeper uses these keys)
    tackle_pts = settings.get("idp_tkl", settings.get("tkl", settings.get("tackle", 1.0)))
    sack_pts = settings.get("idp_sack", settings.get("sack", 2.0))
    int_pts = settings.get("idp_int", settings.get("def_int", 3.0))

    # Classify based on tackle/sack/int balance
    # Big play: low tackle (<=0.75), high sack (>=3.5)
    if tackle_pts <= 0.75 and sack_pts >= 3.5:
        return "big_play"
    # Tackle heavy: high tackle (>=1.75)
    elif tackle_pts >= 1.75:
        return "tackle_heavy"
    # Premium: balanced but higher than standard
    elif tackle_pts >= 1.25 or sack_pts >= 2.5 or int_pts >= 3.5:
        return "premium"
    # Standard: default
    else:
        return "std"
