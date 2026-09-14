#!/usr/bin/env python3
"""
Aggregate NFL Stats from Super Table into Season/Career Tables.

This script pre-aggregates NFL stats to speed up season/career queries:
- player_nfl_season: Aggregated by NFL_player_id + year
- player_nfl_career: Aggregated by NFL_player_id (all-time)

These tables are SHARED across all leagues (stored in ___ops.nfl_historical).
NFL stats are league-independent - one aggregation serves all leagues.

TWO-TRACK ARCHITECTURE:
- Track 1 (this script): Pre-aggregate universal NFL stats
- Track 2 (aggregate_fantasy_season.py): Pre-aggregate league-specific fantasy context

Usage:
    python aggregate_nfl_stats.py --year 2024
    python aggregate_nfl_stats.py --rebuild  # Full rebuild all years

Callable from pipeline:
    from aggregate_nfl_stats import update_aggregates
    update_aggregates(year=2024, week=14)
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.data_fetchers.research_lamar import get_all_lamar_columns, get_all_lamar_ppg_columns
from nfl_data.nfl_franchises import generate_def_franchise_case_sql

SCRIPT_DIR = Path(__file__).resolve().parent

# MotherDuck configuration
MOTHERDUCK_DATABASE = "___ops"
MOTHERDUCK_SCHEMA = "nfl_historical"
SUPER_TABLE = f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.nfl_player_stats_all"

# Regular season tables (default, fast path)
SEASON_TABLE = f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.player_nfl_season"
CAREER_TABLE = f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.player_nfl_career"

# All-games tables (includes playoffs, used when "Include Playoffs" is checked)
SEASON_TABLE_ALL = f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.player_nfl_season_all"
CAREER_TABLE_ALL = f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.player_nfl_career_all"


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [NFL-AGG] {msg}")


def get_deduped_cte(where_clause: str = "") -> str:
    """
    Generate a CTE that deduplicates the super table by player_week.

    PROBLEM 1: Some players from the 1960s (19 players, 175 weeks) had duplicate
    player_week entries because they played both a skill position AND kicker.
    Each position was stored as a separate row from different Stathead parses.
    NOTE: This is now largely resolved at build time by merge_dual_position_duplicates()
    in build_nfl_super_table.py, but this CTE remains as a safety net.

    PROBLEM 2: DST/DEF players have inconsistent NFL_player_id values across
    data sources. nflverse uses numeric IDs (DEF-8 = Vikings) while synthesized
    data uses team abbreviations (DEF-MIN = Vikings). In 2025, nflverse switched
    from abbreviations (weeks 1-16) to numeric IDs (week 17), causing splits.

    SOLUTION:
    1. Normalize DST IDs using franchise-based mapping (nfl_team + year → DEF-{franchise_id})
       to create consistent grouping that respects franchise lineages. This prevents
       conflation of different franchises sharing abbreviations (e.g., BAL Colts vs BAL Ravens).
    2. Merge duplicate rows by taking MAX of stat columns
    3. SUM fantasy points across position rows (for dual-position players)

    Args:
        where_clause: Optional WHERE clause to filter data (e.g., "WHERE year = 2024")

    Returns:
        SQL CTE string that deduplicates by player_week
    """
    # First normalize DST IDs, then deduplicate
    # For DSTs with abbreviation-based IDs (DEF-ARI, DEF-MIN), map to franchise-based (DEF-13, DEF-8)
    # For DSTs already having franchise-based numeric IDs (DEF-5, DEF-13), keep as-is
    # This prevents conflation when two franchises share nfl_team (CHI = Bears + Cardinals pre-1960)
    # For non-DSTs, keep the original NFL_player_id
    def_case_sql = generate_def_franchise_case_sql()
    # Research LAMAR columns — MAX for dedup (one value per player_week)
    _lamar_cols = get_all_lamar_columns()
    lamar_dedup_sql = ",\n            ".join(f"MAX({c}) AS {c}" for c in _lamar_cols)
    return f"""
    normalized AS (
        SELECT *,
            CASE
                -- Non-DEF: keep original ID
                WHEN nfl_position != 'DEF' OR nfl_position IS NULL THEN NFL_player_id
                -- DEF with already-correct franchise-based numeric ID (DEF-5, DEF-13, etc.): keep it
                WHEN NFL_player_id LIKE 'DEF-%%'
                  AND TRY_CAST(REPLACE(NFL_player_id, 'DEF-', '') AS INTEGER) IS NOT NULL
                THEN NFL_player_id
                -- DEF with abbreviation-based ID (DEF-ARI, DEF-MIN): normalize via nfl_team + year
                WHEN nfl_team IS NOT NULL
                THEN {def_case_sql}
                ELSE NFL_player_id
            END AS normalized_id
        FROM {SUPER_TABLE}
        {where_clause}
    ),
    deduped AS (
        SELECT
            player_week,
            -- Use normalized_id for grouping but output as NFL_player_id
            normalized_id AS NFL_player_id,
            year,
            week,

            -- Player metadata: prefer non-null values
            COALESCE(MAX(CASE WHEN data_source = 'stathead' THEN player END), MAX(player)) AS player,
            -- Position already normalized in super_table (SAF->DB, MLB->LB, DE->DL, etc.)
            COALESCE(MAX(CASE WHEN data_source = 'stathead' THEN nfl_position END), MAX(nfl_position)) AS nfl_position,
            COALESCE(MAX(CASE WHEN data_source = 'stathead' THEN nfl_team END), MAX(nfl_team)) AS nfl_team,
            MAX(headshot_url) AS headshot_url,
            MAX(season_type) AS season_type,

            -- Passing stats (from skill position row)
            MAX(passing_yards) AS passing_yards,
            MAX(passing_tds) AS passing_tds,
            MAX(passing_interceptions) AS passing_interceptions,
            MAX(attempts) AS attempts,
            MAX(completions) AS completions,
            MAX(passing_air_yards) AS passing_air_yards,
            MAX(passing_yards_after_catch) AS passing_yards_after_catch,
            MAX(passing_first_downs) AS passing_first_downs,
            MAX(passing_epa) AS passing_epa,
            MAX(passing_cpoe) AS passing_cpoe,
            MAX(pacr) AS pacr,
            MAX(passing_2pt_conversions) AS passing_2pt_conversions,
            MAX(passing_long) AS passing_long,
            MAX(completions_40plus) AS completions_40plus,
            MAX(passing_tds_40plus) AS passing_tds_40plus,
            MAX(passing_tds_50plus) AS passing_tds_50plus,

            -- Rushing stats (from skill position row)
            MAX(rushing_yards) AS rushing_yards,
            MAX(carries) AS carries,
            MAX(rushing_tds) AS rushing_tds,
            MAX(rushing_fumbles) AS rushing_fumbles,
            MAX(rushing_fumbles_lost) AS rushing_fumbles_lost,
            MAX(rushing_first_downs) AS rushing_first_downs,
            MAX(rushing_epa) AS rushing_epa,
            MAX(rushing_2pt_conversions) AS rushing_2pt_conversions,
            MAX(rushing_long) AS rushing_long,
            MAX(rushing_40plus) AS rushing_40plus,
            MAX(rushing_tds_40plus) AS rushing_tds_40plus,
            MAX(rushing_tds_50plus) AS rushing_tds_50plus,

            -- Receiving stats (from skill position row)
            MAX(receptions) AS receptions,
            MAX(receiving_yards) AS receiving_yards,
            MAX(receiving_tds) AS receiving_tds,
            MAX(targets) AS targets,
            MAX(receiving_fumbles) AS receiving_fumbles,
            MAX(receiving_fumbles_lost) AS receiving_fumbles_lost,
            MAX(receiving_first_downs) AS receiving_first_downs,
            MAX(receiving_epa) AS receiving_epa,
            MAX(receiving_2pt_conversions) AS receiving_2pt_conversions,
            MAX(target_share) AS target_share,
            MAX(wopr) AS wopr,
            MAX(racr) AS racr,
            MAX(receiving_air_yards) AS receiving_air_yards,
            MAX(receiving_yards_after_catch) AS receiving_yards_after_catch,
            MAX(air_yards_share) AS air_yards_share,
            MAX(receiving_long) AS receiving_long,
            MAX(receptions_40plus) AS receptions_40plus,
            MAX(receiving_tds_40plus) AS receiving_tds_40plus,
            MAX(receiving_tds_50plus) AS receiving_tds_50plus,

            -- Kicking stats (from kicker row)
            MAX(fg_made) AS fg_made,
            MAX(fg_att) AS fg_att,
            MAX(fg_pct) AS fg_pct,
            MAX(fg_long) AS fg_long,
            MAX(fg_made_0_19) AS fg_made_0_19,
            MAX(fg_made_20_29) AS fg_made_20_29,
            MAX(fg_made_30_39) AS fg_made_30_39,
            MAX(fg_made_40_49) AS fg_made_40_49,
            MAX(fg_made_50_59) AS fg_made_50_59,
            MAX(fg_missed) AS fg_missed,
            MAX(pat_made) AS pat_made,
            MAX(pat_att) AS pat_att,
            MAX(pat_missed) AS pat_missed,
            MAX(fg_yds_over_30) AS fg_yds_over_30,

            -- Defense/IDP stats
            MAX(def_sacks) AS def_sacks,
            MAX(def_sack_yards) AS def_sack_yards,
            MAX(def_qb_hits) AS def_qb_hits,
            MAX(def_interceptions) AS def_interceptions,
            MAX(def_interception_yards) AS def_interception_yards,
            MAX(def_pass_defended) AS def_pass_defended,
            MAX(def_tackles_solo) AS def_tackles_solo,
            MAX(def_tackle_assists) AS def_tackle_assists,
            MAX(def_tackles_with_assist) AS def_tackles_with_assist,
            MAX(def_tackles_for_loss) AS def_tackles_for_loss,
            MAX(def_tackles_for_loss_yards) AS def_tackles_for_loss_yards,
            MAX(def_fumbles) AS def_fumbles,
            MAX(def_fumbles_forced) AS def_fumbles_forced,
            MAX(def_safeties) AS def_safeties,
            MAX(def_tds) AS def_tds,

            -- DST stats
            MAX(pts_allow) AS pts_allow,
            MAX(dst_points_allowed) AS dst_points_allowed,
            MAX(points_allowed) AS points_allowed,
            MAX(passing_yds_allowed) AS passing_yds_allowed,
            MAX(rushing_yds_allowed) AS rushing_yds_allowed,
            MAX(total_yds_allowed) AS total_yds_allowed,
            MAX(fum_rec) AS fum_rec,
            MAX(fum_ret_td) AS fum_ret_td,
            MAX(special_teams_tds) AS special_teams_tds,
            MAX(TRY_CAST(three_out AS DOUBLE)) AS three_out,
            MAX(TRY_CAST(fourth_down_stop AS DOUBLE)) AS fourth_down_stop,
            MAX(dst_return_yards) AS dst_return_yards,
            MAX(kickoff_return_yards) AS kickoff_return_yards,
            MAX(punt_return_yards) AS punt_return_yards,
            MAX(fum_rec_yds) AS fum_rec_yds,
            MAX(fg_blocked) AS fg_blocked,

            -- Pre-calculated fantasy points: SUM across position rows
            -- (skill position contributes rush/rec/pass, kicker contributes kicking)
            SUM(pts_pass_4pt) AS pts_pass_4pt,
            SUM(pts_pass_6pt) AS pts_pass_6pt,
            SUM(pts_rush) AS pts_rush,
            SUM(pts_rec_0ppr) AS pts_rec_0ppr,
            SUM(pts_rec_half) AS pts_rec_half,
            SUM(pts_rec_ppr) AS pts_rec_ppr,
            SUM(pts_misc) AS pts_misc,
            SUM(pts_k_std) AS pts_k_std,
            SUM(pts_k_yds) AS pts_k_yds,
            SUM(pts_k_flat) AS pts_k_flat,
            SUM(pts_def_std) AS pts_def_std,
            SUM(pts_def_ya) AS pts_def_ya,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            -- IDP fantasy points (individual defensive players only, NOT team defenses)
            -- DEF records in source data have team-wide IDP aggregates which we must exclude
            SUM(CASE WHEN nfl_position = 'DEF' THEN 0 ELSE pts_idp_std END) AS pts_idp_std,
            SUM(CASE WHEN nfl_position = 'DEF' THEN 0 ELSE pts_idp_premium END) AS pts_idp_premium,
            SUM(CASE WHEN nfl_position = 'DEF' THEN 0 ELSE pts_idp_big_play END) AS pts_idp_big_play,
            SUM(CASE WHEN nfl_position = 'DEF' THEN 0 ELSE pts_idp_tackle_heavy END) AS pts_idp_tackle_heavy,

            -- Pre-computed PPG metrics (96 columns - MAX since pre-computed per player_week)
            -- 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + 3 TEP variants
            -- Season PPG (12)
            MAX(ppg_season_4pt_0ppr) AS ppg_season_4pt_0ppr,
            MAX(ppg_season_4pt_half) AS ppg_season_4pt_half,
            MAX(ppg_season_4pt_ppr) AS ppg_season_4pt_ppr,
            MAX(ppg_season_5pt_0ppr) AS ppg_season_5pt_0ppr,
            MAX(ppg_season_5pt_half) AS ppg_season_5pt_half,
            MAX(ppg_season_5pt_ppr) AS ppg_season_5pt_ppr,
            MAX(ppg_season_6pt_0ppr) AS ppg_season_6pt_0ppr,
            MAX(ppg_season_6pt_half) AS ppg_season_6pt_half,
            MAX(ppg_season_6pt_ppr) AS ppg_season_6pt_ppr,
            MAX(ppg_season_4pt_tep) AS ppg_season_4pt_tep,
            MAX(ppg_season_5pt_tep) AS ppg_season_5pt_tep,
            MAX(ppg_season_6pt_tep) AS ppg_season_6pt_tep,
            -- Alltime PPG (12)
            MAX(ppg_alltime_4pt_0ppr) AS ppg_alltime_4pt_0ppr,
            MAX(ppg_alltime_4pt_half) AS ppg_alltime_4pt_half,
            MAX(ppg_alltime_4pt_ppr) AS ppg_alltime_4pt_ppr,
            MAX(ppg_alltime_5pt_0ppr) AS ppg_alltime_5pt_0ppr,
            MAX(ppg_alltime_5pt_half) AS ppg_alltime_5pt_half,
            MAX(ppg_alltime_5pt_ppr) AS ppg_alltime_5pt_ppr,
            MAX(ppg_alltime_6pt_0ppr) AS ppg_alltime_6pt_0ppr,
            MAX(ppg_alltime_6pt_half) AS ppg_alltime_6pt_half,
            MAX(ppg_alltime_6pt_ppr) AS ppg_alltime_6pt_ppr,
            MAX(ppg_alltime_4pt_tep) AS ppg_alltime_4pt_tep,
            MAX(ppg_alltime_5pt_tep) AS ppg_alltime_5pt_tep,
            MAX(ppg_alltime_6pt_tep) AS ppg_alltime_6pt_tep,
            -- Rolling 3 (12)
            MAX(rolling_3_4pt_0ppr) AS rolling_3_4pt_0ppr,
            MAX(rolling_3_4pt_half) AS rolling_3_4pt_half,
            MAX(rolling_3_4pt_ppr) AS rolling_3_4pt_ppr,
            MAX(rolling_3_5pt_0ppr) AS rolling_3_5pt_0ppr,
            MAX(rolling_3_5pt_half) AS rolling_3_5pt_half,
            MAX(rolling_3_5pt_ppr) AS rolling_3_5pt_ppr,
            MAX(rolling_3_6pt_0ppr) AS rolling_3_6pt_0ppr,
            MAX(rolling_3_6pt_half) AS rolling_3_6pt_half,
            MAX(rolling_3_6pt_ppr) AS rolling_3_6pt_ppr,
            MAX(rolling_3_4pt_tep) AS rolling_3_4pt_tep,
            MAX(rolling_3_5pt_tep) AS rolling_3_5pt_tep,
            MAX(rolling_3_6pt_tep) AS rolling_3_6pt_tep,
            -- Rolling 5 (12)
            MAX(rolling_5_4pt_0ppr) AS rolling_5_4pt_0ppr,
            MAX(rolling_5_4pt_half) AS rolling_5_4pt_half,
            MAX(rolling_5_4pt_ppr) AS rolling_5_4pt_ppr,
            MAX(rolling_5_5pt_0ppr) AS rolling_5_5pt_0ppr,
            MAX(rolling_5_5pt_half) AS rolling_5_5pt_half,
            MAX(rolling_5_5pt_ppr) AS rolling_5_5pt_ppr,
            MAX(rolling_5_6pt_0ppr) AS rolling_5_6pt_0ppr,
            MAX(rolling_5_6pt_half) AS rolling_5_6pt_half,
            MAX(rolling_5_6pt_ppr) AS rolling_5_6pt_ppr,
            MAX(rolling_5_4pt_tep) AS rolling_5_4pt_tep,
            MAX(rolling_5_5pt_tep) AS rolling_5_5pt_tep,
            MAX(rolling_5_6pt_tep) AS rolling_5_6pt_tep,
            -- Consistency (12)
            MAX(consistency_4pt_0ppr) AS consistency_4pt_0ppr,
            MAX(consistency_4pt_half) AS consistency_4pt_half,
            MAX(consistency_4pt_ppr) AS consistency_4pt_ppr,
            MAX(consistency_5pt_0ppr) AS consistency_5pt_0ppr,
            MAX(consistency_5pt_half) AS consistency_5pt_half,
            MAX(consistency_5pt_ppr) AS consistency_5pt_ppr,
            MAX(consistency_6pt_0ppr) AS consistency_6pt_0ppr,
            MAX(consistency_6pt_half) AS consistency_6pt_half,
            MAX(consistency_6pt_ppr) AS consistency_6pt_ppr,
            MAX(consistency_4pt_tep) AS consistency_4pt_tep,
            MAX(consistency_5pt_tep) AS consistency_5pt_tep,
            MAX(consistency_6pt_tep) AS consistency_6pt_tep,
            -- Weighted PPG (12)
            MAX(weighted_ppg_4pt_0ppr) AS weighted_ppg_4pt_0ppr,
            MAX(weighted_ppg_4pt_half) AS weighted_ppg_4pt_half,
            MAX(weighted_ppg_4pt_ppr) AS weighted_ppg_4pt_ppr,
            MAX(weighted_ppg_5pt_0ppr) AS weighted_ppg_5pt_0ppr,
            MAX(weighted_ppg_5pt_half) AS weighted_ppg_5pt_half,
            MAX(weighted_ppg_5pt_ppr) AS weighted_ppg_5pt_ppr,
            MAX(weighted_ppg_6pt_0ppr) AS weighted_ppg_6pt_0ppr,
            MAX(weighted_ppg_6pt_half) AS weighted_ppg_6pt_half,
            MAX(weighted_ppg_6pt_ppr) AS weighted_ppg_6pt_ppr,
            MAX(weighted_ppg_4pt_tep) AS weighted_ppg_4pt_tep,
            MAX(weighted_ppg_5pt_tep) AS weighted_ppg_5pt_tep,
            MAX(weighted_ppg_6pt_tep) AS weighted_ppg_6pt_tep,
            -- Avg Points Next Year (12)
            MAX(avg_pts_next_year_4pt_0ppr) AS avg_pts_next_year_4pt_0ppr,
            MAX(avg_pts_next_year_4pt_half) AS avg_pts_next_year_4pt_half,
            MAX(avg_pts_next_year_4pt_ppr) AS avg_pts_next_year_4pt_ppr,
            MAX(avg_pts_next_year_5pt_0ppr) AS avg_pts_next_year_5pt_0ppr,
            MAX(avg_pts_next_year_5pt_half) AS avg_pts_next_year_5pt_half,
            MAX(avg_pts_next_year_5pt_ppr) AS avg_pts_next_year_5pt_ppr,
            MAX(avg_pts_next_year_6pt_0ppr) AS avg_pts_next_year_6pt_0ppr,
            MAX(avg_pts_next_year_6pt_half) AS avg_pts_next_year_6pt_half,
            MAX(avg_pts_next_year_6pt_ppr) AS avg_pts_next_year_6pt_ppr,
            MAX(avg_pts_next_year_4pt_tep) AS avg_pts_next_year_4pt_tep,
            MAX(avg_pts_next_year_5pt_tep) AS avg_pts_next_year_5pt_tep,
            MAX(avg_pts_next_year_6pt_tep) AS avg_pts_next_year_6pt_tep,
            -- Research LAMAR columns (36)
            {lamar_dedup_sql}

        FROM normalized
        GROUP BY player_week, normalized_id, year, week
    )
    """


def get_connection():
    """Get database connection for ___ops (MotherDuck or local for Fly.io)."""
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        # For Fly.io, reads go through get_reader(); this function is for write paths
        # that need a DuckDB connection. Fall through to MotherDuck for now.
        pass

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN environment variable not set")

    return duckdb.connect(f"md:?motherduck_token={token}")


def ensure_schema_exists(conn) -> None:
    """Ensure the ___ops.nfl_historical schema exists."""
    log(f"Ensuring database and schema exist: {MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}")

    # Create database if not exists
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DATABASE}")

    # Create schema if not exists
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}")

    log(f"  Schema ready: {MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}")


def create_season_table(conn) -> bool:
    """Create player_nfl_season table if it doesn't exist."""
    log("Creating player_nfl_season table...")

    # Create table with aggregated stats
    # SUM for counting stats, AVG for rate stats, MAX for peak stats
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {SEASON_TABLE} (
            NFL_player_id VARCHAR,
            year INTEGER,

            -- Player metadata (take most common/recent)
            player VARCHAR,
            nfl_position VARCHAR,
            nfl_team VARCHAR,
            headshot_url VARCHAR,

            -- Game counts
            games_played INTEGER,

            -- Passing stats
            passing_yards DOUBLE,
            passing_tds DOUBLE,
            passing_interceptions DOUBLE,
            attempts DOUBLE,
            completions DOUBLE,
            passing_air_yards DOUBLE,
            passing_yards_after_catch DOUBLE,
            passing_first_downs DOUBLE,
            passing_epa DOUBLE,
            passing_cpoe DOUBLE,
            pacr DOUBLE,
            passing_2pt_conversions DOUBLE,

            -- Rushing stats
            rushing_yards DOUBLE,
            carries DOUBLE,
            rushing_tds DOUBLE,
            rushing_fumbles DOUBLE,
            rushing_fumbles_lost DOUBLE,
            rushing_first_downs DOUBLE,
            rushing_epa DOUBLE,
            rushing_2pt_conversions DOUBLE,

            -- Receiving stats
            receptions DOUBLE,
            receiving_yards DOUBLE,
            receiving_tds DOUBLE,
            targets DOUBLE,
            receiving_fumbles DOUBLE,
            receiving_fumbles_lost DOUBLE,
            receiving_first_downs DOUBLE,
            receiving_epa DOUBLE,
            receiving_2pt_conversions DOUBLE,
            target_share DOUBLE,
            wopr DOUBLE,
            racr DOUBLE,
            receiving_air_yards DOUBLE,
            receiving_yards_after_catch DOUBLE,
            air_yards_share DOUBLE,

            -- Kicking stats
            fg_made DOUBLE,
            fg_att DOUBLE,
            fg_pct DOUBLE,
            fg_long DOUBLE,
            fg_made_0_19 DOUBLE,
            fg_made_20_29 DOUBLE,
            fg_made_30_39 DOUBLE,
            fg_made_40_49 DOUBLE,
            fg_made_50_59 DOUBLE,
            fg_missed DOUBLE,
            pat_made DOUBLE,
            pat_att DOUBLE,
            pat_missed DOUBLE,

            -- Derived rate stats
            comp_pct DOUBLE,
            yards_per_attempt DOUBLE,
            passer_rating DOUBLE,
            yards_per_carry DOUBLE,
            yards_per_reception DOUBLE,
            catch_rate DOUBLE,

            -- Big play / long stats
            passing_long DOUBLE,
            completions_40plus DOUBLE,
            passing_tds_40plus DOUBLE,
            passing_tds_50plus DOUBLE,
            rushing_long DOUBLE,
            rushing_40plus DOUBLE,
            rushing_tds_40plus DOUBLE,
            rushing_tds_50plus DOUBLE,
            receiving_long DOUBLE,
            receptions_40plus DOUBLE,
            receiving_tds_40plus DOUBLE,
            receiving_tds_50plus DOUBLE,
            fg_yds_over_30 DOUBLE,

            -- Defense/IDP stats
            def_sacks DOUBLE,
            def_sack_yards DOUBLE,
            def_qb_hits DOUBLE,
            def_interceptions DOUBLE,
            def_interception_yards DOUBLE,
            def_pass_defended DOUBLE,
            def_tackles_solo DOUBLE,
            def_tackle_assists DOUBLE,
            def_tackles_with_assist DOUBLE,
            def_tackles_for_loss DOUBLE,
            def_tackles_for_loss_yards DOUBLE,
            def_fumbles DOUBLE,
            def_fumbles_forced DOUBLE,
            def_safeties DOUBLE,
            def_tds DOUBLE,

            -- DST stats
            pts_allow DOUBLE,
            dst_points_allowed DOUBLE,
            points_allowed DOUBLE,
            passing_yds_allowed DOUBLE,
            rushing_yds_allowed DOUBLE,
            total_yds_allowed DOUBLE,
            fum_rec DOUBLE,
            fum_ret_td DOUBLE,
            special_teams_tds DOUBLE,
            three_out DOUBLE,
            fourth_down_stop DOUBLE,
            dst_return_yards DOUBLE,
            kickoff_return_yards DOUBLE,
            punt_return_yards DOUBLE,
            fum_rec_yds DOUBLE,
            fg_blocked DOUBLE,

            -- Pre-calculated fantasy points by category
            pts_pass_4pt DOUBLE,
            pts_pass_6pt DOUBLE,
            pts_rush DOUBLE,
            pts_rec_0ppr DOUBLE,
            pts_rec_half DOUBLE,
            pts_rec_ppr DOUBLE,
            pts_misc DOUBLE,
            pts_k_std DOUBLE,
            pts_k_yds DOUBLE,
            pts_k_flat DOUBLE,
            pts_def_std DOUBLE,
            pts_def_ya DOUBLE,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            -- IDP fantasy points
            pts_idp_std DOUBLE,
            pts_idp_premium DOUBLE,
            pts_idp_big_play DOUBLE,
            pts_idp_tackle_heavy DOUBLE,

            -- Metadata
            last_updated TIMESTAMP,

            PRIMARY KEY (NFL_player_id, year)
        )
    """)

    # Add IDP columns if they don't exist (for existing tables)
    for col in ["pts_idp_std", "pts_idp_premium", "pts_idp_big_play", "pts_idp_tackle_heavy"]:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:
            pass  # Column already exists

    # Add derived rate stat columns if they don't exist
    for col in [
        "comp_pct",
        "yards_per_attempt",
        "passer_rating",
        "yards_per_carry",
        "yards_per_reception",
        "catch_rate",
    ]:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add big play / long stat columns if they don't exist
    for col in [
        "passing_long",
        "completions_40plus",
        "passing_tds_40plus",
        "passing_tds_50plus",
        "rushing_long",
        "rushing_40plus",
        "rushing_tds_40plus",
        "rushing_tds_50plus",
        "receiving_long",
        "receptions_40plus",
        "receiving_tds_40plus",
        "receiving_tds_50plus",
        "fg_yds_over_30",
        "dst_return_yards",
        "kickoff_return_yards",
        "punt_return_yards",
        "fum_rec_yds",
        "fg_blocked",
    ]:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add PPG columns if they don't exist (for existing tables)
    # Season tables get: ppg_season_* (12) + consistency_* (12) + weighted_ppg_* (12) + avg_pts_next_year_* (12) = 48 columns
    # 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + 3 TEP variants
    ppg_season_cols = [
        # ppg_season (12)
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
        # consistency (12)
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
        # weighted_ppg (12)
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
        # avg_pts_next_year (12)
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
    ]
    for col in ppg_season_cols:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add research LAMAR columns (36 lamar + 36 lamar_ppg = 72 columns)
    for col in get_all_lamar_columns() + get_all_lamar_ppg_columns():
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:
            pass
    log("  player_nfl_season table ready")
    return True


def create_career_table(conn) -> bool:
    """Create player_nfl_career table if it doesn't exist."""
    log("Creating player_nfl_career table...")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CAREER_TABLE} (
            NFL_player_id VARCHAR PRIMARY KEY,

            -- Player metadata
            player VARCHAR,
            nfl_position VARCHAR,
            nfl_team VARCHAR,
            headshot_url VARCHAR,

            -- Career span
            first_year INTEGER,
            last_year INTEGER,
            years_active INTEGER,
            games_played INTEGER,

            -- Same stat columns as season (career totals)
            passing_yards DOUBLE,
            passing_tds DOUBLE,
            passing_interceptions DOUBLE,
            attempts DOUBLE,
            completions DOUBLE,
            passing_air_yards DOUBLE,
            passing_yards_after_catch DOUBLE,
            passing_first_downs DOUBLE,
            passing_epa DOUBLE,
            passing_cpoe DOUBLE,
            pacr DOUBLE,
            passing_2pt_conversions DOUBLE,

            rushing_yards DOUBLE,
            carries DOUBLE,
            rushing_tds DOUBLE,
            rushing_fumbles DOUBLE,
            rushing_fumbles_lost DOUBLE,
            rushing_first_downs DOUBLE,
            rushing_epa DOUBLE,
            rushing_2pt_conversions DOUBLE,

            receptions DOUBLE,
            receiving_yards DOUBLE,
            receiving_tds DOUBLE,
            targets DOUBLE,
            receiving_fumbles DOUBLE,
            receiving_fumbles_lost DOUBLE,
            receiving_first_downs DOUBLE,
            receiving_epa DOUBLE,
            receiving_2pt_conversions DOUBLE,
            target_share DOUBLE,
            wopr DOUBLE,
            racr DOUBLE,
            receiving_air_yards DOUBLE,
            receiving_yards_after_catch DOUBLE,
            air_yards_share DOUBLE,

            fg_made DOUBLE,
            fg_att DOUBLE,
            fg_pct DOUBLE,
            fg_long DOUBLE,
            fg_made_0_19 DOUBLE,
            fg_made_20_29 DOUBLE,
            fg_made_30_39 DOUBLE,
            fg_made_40_49 DOUBLE,
            fg_made_50_59 DOUBLE,
            fg_missed DOUBLE,
            pat_made DOUBLE,
            pat_att DOUBLE,
            pat_missed DOUBLE,

            -- Derived rate stats
            comp_pct DOUBLE,
            yards_per_attempt DOUBLE,
            passer_rating DOUBLE,
            yards_per_carry DOUBLE,
            yards_per_reception DOUBLE,
            catch_rate DOUBLE,

            -- Big play / long stats
            passing_long DOUBLE,
            completions_40plus DOUBLE,
            passing_tds_40plus DOUBLE,
            passing_tds_50plus DOUBLE,
            rushing_long DOUBLE,
            rushing_40plus DOUBLE,
            rushing_tds_40plus DOUBLE,
            rushing_tds_50plus DOUBLE,
            receiving_long DOUBLE,
            receptions_40plus DOUBLE,
            receiving_tds_40plus DOUBLE,
            receiving_tds_50plus DOUBLE,
            fg_yds_over_30 DOUBLE,

            def_sacks DOUBLE,
            def_sack_yards DOUBLE,
            def_qb_hits DOUBLE,
            def_interceptions DOUBLE,
            def_interception_yards DOUBLE,
            def_pass_defended DOUBLE,
            def_tackles_solo DOUBLE,
            def_tackle_assists DOUBLE,
            def_tackles_with_assist DOUBLE,
            def_tackles_for_loss DOUBLE,
            def_tackles_for_loss_yards DOUBLE,
            def_fumbles DOUBLE,
            def_fumbles_forced DOUBLE,
            def_safeties DOUBLE,
            def_tds DOUBLE,

            pts_allow DOUBLE,
            dst_points_allowed DOUBLE,
            points_allowed DOUBLE,
            passing_yds_allowed DOUBLE,
            rushing_yds_allowed DOUBLE,
            total_yds_allowed DOUBLE,
            fum_rec DOUBLE,
            fum_ret_td DOUBLE,
            special_teams_tds DOUBLE,
            three_out DOUBLE,
            fourth_down_stop DOUBLE,
            dst_return_yards DOUBLE,
            kickoff_return_yards DOUBLE,
            punt_return_yards DOUBLE,
            fum_rec_yds DOUBLE,
            fg_blocked DOUBLE,

            -- Pre-calculated fantasy points by category
            pts_pass_4pt DOUBLE,
            pts_pass_6pt DOUBLE,
            pts_rush DOUBLE,
            pts_rec_0ppr DOUBLE,
            pts_rec_half DOUBLE,
            pts_rec_ppr DOUBLE,
            pts_misc DOUBLE,
            pts_k_std DOUBLE,
            pts_k_yds DOUBLE,
            pts_k_flat DOUBLE,
            pts_def_std DOUBLE,
            pts_def_ya DOUBLE,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            -- IDP fantasy points
            pts_idp_std DOUBLE,
            pts_idp_premium DOUBLE,
            pts_idp_big_play DOUBLE,
            pts_idp_tackle_heavy DOUBLE,

            last_updated TIMESTAMP
        )
    """)

    # Add IDP columns if they don't exist (for existing tables)
    for col in ["pts_idp_std", "pts_idp_premium", "pts_idp_big_play", "pts_idp_tackle_heavy"]:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add derived rate stat columns if they don't exist
    for col in [
        "comp_pct",
        "yards_per_attempt",
        "passer_rating",
        "yards_per_carry",
        "yards_per_reception",
        "catch_rate",
    ]:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add big play / long stat columns if they don't exist
    for col in [
        "passing_long",
        "completions_40plus",
        "passing_tds_40plus",
        "passing_tds_50plus",
        "rushing_long",
        "rushing_40plus",
        "rushing_tds_40plus",
        "rushing_tds_50plus",
        "receiving_long",
        "receptions_40plus",
        "receiving_tds_40plus",
        "receiving_tds_50plus",
        "fg_yds_over_30",
        "dst_return_yards",
        "kickoff_return_yards",
        "punt_return_yards",
        "fum_rec_yds",
        "fg_blocked",
    ]:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add PPG columns if they don't exist (for existing tables)
    # Career tables get: ppg_alltime_* (12) = 12 columns
    # 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + 3 TEP variants
    ppg_career_cols = [
        "ppg_alltime_4pt_0ppr",
        "ppg_alltime_4pt_half",
        "ppg_alltime_4pt_ppr",
        "ppg_alltime_4pt_tep",
        "ppg_alltime_5pt_0ppr",
        "ppg_alltime_5pt_half",
        "ppg_alltime_5pt_ppr",
        "ppg_alltime_5pt_tep",
        "ppg_alltime_6pt_0ppr",
        "ppg_alltime_6pt_half",
        "ppg_alltime_6pt_ppr",
        "ppg_alltime_6pt_tep",
    ]
    for col in ppg_career_cols:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add research LAMAR columns (36 lamar + 36 lamar_ppg = 72 columns)
    for col in get_all_lamar_columns() + get_all_lamar_ppg_columns():
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:
            pass
    log("  player_nfl_career table ready")
    return True


def create_season_table_all(conn) -> bool:
    """Create player_nfl_season_all table (includes playoffs) if it doesn't exist.

    Same schema as player_nfl_season but includes ALL games (regular + playoffs).
    """
    log("Creating player_nfl_season_all table...")

    # Use same schema as regular season table, just different table name
    # Copy the DDL from create_season_table but use SEASON_TABLE_ALL
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {SEASON_TABLE_ALL} (
            NFL_player_id VARCHAR,
            year INTEGER,
            player VARCHAR,
            nfl_position VARCHAR,
            nfl_team VARCHAR,
            headshot_url VARCHAR,
            games_played INTEGER,
            passing_yards DOUBLE, passing_tds DOUBLE, passing_interceptions DOUBLE,
            attempts DOUBLE, completions DOUBLE, passing_air_yards DOUBLE,
            passing_yards_after_catch DOUBLE, passing_first_downs DOUBLE,
            passing_epa DOUBLE, passing_cpoe DOUBLE, pacr DOUBLE, passing_2pt_conversions DOUBLE,
            rushing_yards DOUBLE, carries DOUBLE, rushing_tds DOUBLE,
            rushing_fumbles DOUBLE, rushing_fumbles_lost DOUBLE,
            rushing_first_downs DOUBLE, rushing_epa DOUBLE, rushing_2pt_conversions DOUBLE,
            receptions DOUBLE, receiving_yards DOUBLE, receiving_tds DOUBLE,
            targets DOUBLE, receiving_fumbles DOUBLE, receiving_fumbles_lost DOUBLE,
            receiving_first_downs DOUBLE, receiving_epa DOUBLE, receiving_2pt_conversions DOUBLE,
            target_share DOUBLE, wopr DOUBLE, racr DOUBLE,
            receiving_air_yards DOUBLE, receiving_yards_after_catch DOUBLE, air_yards_share DOUBLE,
            fg_made DOUBLE, fg_att DOUBLE, fg_pct DOUBLE, fg_long DOUBLE,
            fg_made_0_19 DOUBLE, fg_made_20_29 DOUBLE, fg_made_30_39 DOUBLE,
            fg_made_40_49 DOUBLE, fg_made_50_59 DOUBLE, fg_missed DOUBLE,
            pat_made DOUBLE, pat_att DOUBLE, pat_missed DOUBLE,
            comp_pct DOUBLE, yards_per_attempt DOUBLE, passer_rating DOUBLE,
            yards_per_carry DOUBLE, yards_per_reception DOUBLE, catch_rate DOUBLE,
            passing_long DOUBLE, completions_40plus DOUBLE, passing_tds_40plus DOUBLE, passing_tds_50plus DOUBLE,
            rushing_long DOUBLE, rushing_40plus DOUBLE, rushing_tds_40plus DOUBLE, rushing_tds_50plus DOUBLE,
            receiving_long DOUBLE, receptions_40plus DOUBLE, receiving_tds_40plus DOUBLE, receiving_tds_50plus DOUBLE,
            fg_yds_over_30 DOUBLE,
            def_sacks DOUBLE, def_sack_yards DOUBLE, def_qb_hits DOUBLE,
            def_interceptions DOUBLE, def_interception_yards DOUBLE, def_pass_defended DOUBLE,
            def_tackles_solo DOUBLE, def_tackle_assists DOUBLE, def_tackles_with_assist DOUBLE,
            def_tackles_for_loss DOUBLE, def_tackles_for_loss_yards DOUBLE,
            def_fumbles DOUBLE, def_fumbles_forced DOUBLE, def_safeties DOUBLE, def_tds DOUBLE,
            pts_allow DOUBLE, dst_points_allowed DOUBLE, points_allowed DOUBLE,
            passing_yds_allowed DOUBLE, rushing_yds_allowed DOUBLE, total_yds_allowed DOUBLE,
            fum_rec DOUBLE, fum_ret_td DOUBLE, special_teams_tds DOUBLE,
            three_out DOUBLE, fourth_down_stop DOUBLE,
            dst_return_yards DOUBLE, kickoff_return_yards DOUBLE, punt_return_yards DOUBLE,
            fum_rec_yds DOUBLE, fg_blocked DOUBLE,
            pts_pass_4pt DOUBLE, pts_pass_6pt DOUBLE, pts_rush DOUBLE,
            pts_rec_0ppr DOUBLE, pts_rec_half DOUBLE, pts_rec_ppr DOUBLE,
            pts_misc DOUBLE, pts_k_std DOUBLE, pts_k_yds DOUBLE, pts_k_flat DOUBLE,
            pts_def_std DOUBLE, pts_def_ya DOUBLE,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            pts_idp_std DOUBLE, pts_idp_premium DOUBLE, pts_idp_big_play DOUBLE, pts_idp_tackle_heavy DOUBLE,
            last_updated TIMESTAMP,
            PRIMARY KEY (NFL_player_id, year)
        )
    """)

    # Add IDP columns if they don't exist
    for col in ["pts_idp_std", "pts_idp_premium", "pts_idp_big_play", "pts_idp_tackle_heavy"]:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add derived rate stat columns if they don't exist
    for col in [
        "comp_pct",
        "yards_per_attempt",
        "passer_rating",
        "yards_per_carry",
        "yards_per_reception",
        "catch_rate",
    ]:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add big play / long stat columns if they don't exist
    for col in [
        "passing_long",
        "completions_40plus",
        "passing_tds_40plus",
        "passing_tds_50plus",
        "rushing_long",
        "rushing_40plus",
        "rushing_tds_40plus",
        "rushing_tds_50plus",
        "receiving_long",
        "receptions_40plus",
        "receiving_tds_40plus",
        "receiving_tds_50plus",
        "fg_yds_over_30",
        "dst_return_yards",
        "kickoff_return_yards",
        "punt_return_yards",
        "fum_rec_yds",
        "fg_blocked",
    ]:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add PPG columns if they don't exist (for existing tables)
    # Season tables get: ppg_season_* (12) + consistency_* (12) + weighted_ppg_* (12) + avg_pts_next_year_* (12) = 48 columns
    # 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + 3 TEP variants
    ppg_season_cols = [
        # ppg_season (12)
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
        # consistency (12)
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
        # weighted_ppg (12)
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
        # avg_pts_next_year (12)
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
    ]
    for col in ppg_season_cols:
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add research LAMAR columns (36 lamar + 36 lamar_ppg = 72 columns)
    for col in get_all_lamar_columns() + get_all_lamar_ppg_columns():
        try:
            conn.execute(f"ALTER TABLE {SEASON_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:
            pass
    log("  player_nfl_season_all table ready")
    return True


def create_career_table_all(conn) -> bool:
    """Create player_nfl_career_all table (includes playoffs) if it doesn't exist.

    Same schema as player_nfl_career but includes ALL games (regular + playoffs).
    """
    log("Creating player_nfl_career_all table...")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CAREER_TABLE_ALL} (
            NFL_player_id VARCHAR PRIMARY KEY,
            player VARCHAR,
            nfl_position VARCHAR,
            nfl_team VARCHAR,
            headshot_url VARCHAR,
            first_year INTEGER,
            last_year INTEGER,
            years_active INTEGER,
            games_played INTEGER,
            passing_yards DOUBLE, passing_tds DOUBLE, passing_interceptions DOUBLE,
            attempts DOUBLE, completions DOUBLE, passing_air_yards DOUBLE,
            passing_yards_after_catch DOUBLE, passing_first_downs DOUBLE,
            passing_epa DOUBLE, passing_cpoe DOUBLE, pacr DOUBLE, passing_2pt_conversions DOUBLE,
            rushing_yards DOUBLE, carries DOUBLE, rushing_tds DOUBLE,
            rushing_fumbles DOUBLE, rushing_fumbles_lost DOUBLE,
            rushing_first_downs DOUBLE, rushing_epa DOUBLE, rushing_2pt_conversions DOUBLE,
            receptions DOUBLE, receiving_yards DOUBLE, receiving_tds DOUBLE,
            targets DOUBLE, receiving_fumbles DOUBLE, receiving_fumbles_lost DOUBLE,
            receiving_first_downs DOUBLE, receiving_epa DOUBLE, receiving_2pt_conversions DOUBLE,
            target_share DOUBLE, wopr DOUBLE, racr DOUBLE,
            receiving_air_yards DOUBLE, receiving_yards_after_catch DOUBLE, air_yards_share DOUBLE,
            fg_made DOUBLE, fg_att DOUBLE, fg_pct DOUBLE, fg_long DOUBLE,
            fg_made_0_19 DOUBLE, fg_made_20_29 DOUBLE, fg_made_30_39 DOUBLE,
            fg_made_40_49 DOUBLE, fg_made_50_59 DOUBLE, fg_missed DOUBLE,
            pat_made DOUBLE, pat_att DOUBLE, pat_missed DOUBLE,
            comp_pct DOUBLE, yards_per_attempt DOUBLE, passer_rating DOUBLE,
            yards_per_carry DOUBLE, yards_per_reception DOUBLE, catch_rate DOUBLE,
            passing_long DOUBLE, completions_40plus DOUBLE, passing_tds_40plus DOUBLE, passing_tds_50plus DOUBLE,
            rushing_long DOUBLE, rushing_40plus DOUBLE, rushing_tds_40plus DOUBLE, rushing_tds_50plus DOUBLE,
            receiving_long DOUBLE, receptions_40plus DOUBLE, receiving_tds_40plus DOUBLE, receiving_tds_50plus DOUBLE,
            fg_yds_over_30 DOUBLE,
            def_sacks DOUBLE, def_sack_yards DOUBLE, def_qb_hits DOUBLE,
            def_interceptions DOUBLE, def_interception_yards DOUBLE, def_pass_defended DOUBLE,
            def_tackles_solo DOUBLE, def_tackle_assists DOUBLE, def_tackles_with_assist DOUBLE,
            def_tackles_for_loss DOUBLE, def_tackles_for_loss_yards DOUBLE,
            def_fumbles DOUBLE, def_fumbles_forced DOUBLE, def_safeties DOUBLE, def_tds DOUBLE,
            pts_allow DOUBLE, dst_points_allowed DOUBLE, points_allowed DOUBLE,
            passing_yds_allowed DOUBLE, rushing_yds_allowed DOUBLE, total_yds_allowed DOUBLE,
            fum_rec DOUBLE, fum_ret_td DOUBLE, special_teams_tds DOUBLE,
            three_out DOUBLE, fourth_down_stop DOUBLE,
            dst_return_yards DOUBLE, kickoff_return_yards DOUBLE, punt_return_yards DOUBLE,
            fum_rec_yds DOUBLE, fg_blocked DOUBLE,
            pts_pass_4pt DOUBLE, pts_pass_6pt DOUBLE, pts_rush DOUBLE,
            pts_rec_0ppr DOUBLE, pts_rec_half DOUBLE, pts_rec_ppr DOUBLE,
            pts_misc DOUBLE, pts_k_std DOUBLE, pts_k_yds DOUBLE, pts_k_flat DOUBLE,
            pts_def_std DOUBLE, pts_def_ya DOUBLE,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            pts_idp_std DOUBLE, pts_idp_premium DOUBLE, pts_idp_big_play DOUBLE, pts_idp_tackle_heavy DOUBLE,
            last_updated TIMESTAMP
        )
    """)

    # Add IDP columns if they don't exist
    for col in ["pts_idp_std", "pts_idp_premium", "pts_idp_big_play", "pts_idp_tackle_heavy"]:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add derived rate stat columns if they don't exist
    for col in [
        "comp_pct",
        "yards_per_attempt",
        "passer_rating",
        "yards_per_carry",
        "yards_per_reception",
        "catch_rate",
    ]:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add big play / long stat columns if they don't exist
    for col in [
        "passing_long",
        "completions_40plus",
        "passing_tds_40plus",
        "passing_tds_50plus",
        "rushing_long",
        "rushing_40plus",
        "rushing_tds_40plus",
        "rushing_tds_50plus",
        "receiving_long",
        "receptions_40plus",
        "receiving_tds_40plus",
        "receiving_tds_50plus",
        "fg_yds_over_30",
        "dst_return_yards",
        "kickoff_return_yards",
        "punt_return_yards",
        "fum_rec_yds",
        "fg_blocked",
    ]:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add PPG columns if they don't exist (for existing tables)
    # Career tables get: ppg_alltime_* (12) = 12 columns
    # 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + 3 TEP variants
    ppg_career_cols = [
        "ppg_alltime_4pt_0ppr",
        "ppg_alltime_4pt_half",
        "ppg_alltime_4pt_ppr",
        "ppg_alltime_4pt_tep",
        "ppg_alltime_5pt_0ppr",
        "ppg_alltime_5pt_half",
        "ppg_alltime_5pt_ppr",
        "ppg_alltime_5pt_tep",
        "ppg_alltime_6pt_0ppr",
        "ppg_alltime_6pt_half",
        "ppg_alltime_6pt_ppr",
        "ppg_alltime_6pt_tep",
    ]
    for col in ppg_career_cols:
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:  # noqa: broad-except
            pass
    # Add research LAMAR columns (36 lamar + 36 lamar_ppg = 72 columns)
    for col in get_all_lamar_columns() + get_all_lamar_ppg_columns():
        try:
            conn.execute(f"ALTER TABLE {CAREER_TABLE_ALL} ADD COLUMN IF NOT EXISTS {col} DOUBLE")
        except Exception:
            pass
    log("  player_nfl_career_all table ready")
    return True


def aggregate_to_season(conn, year: int = None) -> int:
    """
    Aggregate super_table to season totals.

    Only includes REGULAR SEASON games (season_type = 'REG').
    This makes the default UI fast. Playoff data is available via
    the "Include Playoffs" option which uses runtime aggregation.

    DEDUPLICATION: Uses CTE to merge duplicate player_week entries.
    Some 1960s players (19 players, 175 weeks) have duplicate entries
    because they played both a skill position AND kicker.

    Args:
        conn: DuckDB connection
        year: Specific year to aggregate, or None for all years

    Returns:
        Number of rows aggregated
    """
    # Filter to regular season only (exclude playoff games and bye/placeholder weeks)
    # IMPORTANT: Do NOT include season_type IS NULL - those are bye weeks and placeholder rows
    reg_season_filter = "season_type = 'REG'"

    if year:
        where_clause = f"WHERE {reg_season_filter} AND year = {year}"
    else:
        where_clause = f"WHERE {reg_season_filter}"

    year_desc = str(year) if year else "all years"

    log(f"Aggregating season stats for {year_desc}...")

    # Delete existing data for this year (incremental update)
    if year:
        conn.execute(f"DELETE FROM {SEASON_TABLE} WHERE year = {year}")
    else:
        conn.execute(f"DELETE FROM {SEASON_TABLE}")

    # Get the deduplication CTE
    deduped_cte = get_deduped_cte(where_clause)

    # Build LAMAR aggregation expressions (SUM for totals, AVG for per-game)
    _lamar_cols = get_all_lamar_columns()
    # All SUMs first (lamar_*), then all AVGs (lamar_ppg_*) to match DDL column order
    _lamar_sum_parts = [f"SUM({_c}) AS {_c}" for _c in _lamar_cols]
    _lamar_avg_parts = [f"AVG({_c}) AS {_c.replace('lamar_', 'lamar_ppg_', 1)}" for _c in _lamar_cols]
    lamar_agg_sql = ",\n            ".join(_lamar_sum_parts + _lamar_avg_parts)

    # Aggregate from deduped CTE (merges duplicate player_week entries)
    # SUM for counting stats, AVG for rate stats, MAX for metadata
    # Position logic: Use MODE() to get the position played most often
    result = conn.execute(f"""
        INSERT INTO {SEASON_TABLE}
        WITH {deduped_cte}
        SELECT
            NFL_player_id,
            year,

            -- Player metadata (most recent week's name/team to handle DST franchise merges)
            ARG_MAX(player, week) AS player,
            -- Use most frequent position (the position they played most games at)
            MODE(nfl_position) AS nfl_position,
            ARG_MAX(nfl_team, week) AS nfl_team,
            MAX(headshot_url) AS headshot_url,

            -- Game counts (now correctly counts unique weeks after dedup)
            COUNT(*) AS games_played,

            -- Passing stats (SUM except rates which are AVG)
            SUM(passing_yards) AS passing_yards,
            SUM(passing_tds) AS passing_tds,
            SUM(passing_interceptions) AS passing_interceptions,
            SUM(attempts) AS attempts,
            SUM(completions) AS completions,
            SUM(passing_air_yards) AS passing_air_yards,
            SUM(passing_yards_after_catch) AS passing_yards_after_catch,
            SUM(passing_first_downs) AS passing_first_downs,
            SUM(passing_epa) AS passing_epa,
            AVG(passing_cpoe) AS passing_cpoe,
            AVG(pacr) AS pacr,
            SUM(passing_2pt_conversions) AS passing_2pt_conversions,

            -- Rushing stats
            SUM(rushing_yards) AS rushing_yards,
            SUM(carries) AS carries,
            SUM(rushing_tds) AS rushing_tds,
            SUM(rushing_fumbles) AS rushing_fumbles,
            SUM(rushing_fumbles_lost) AS rushing_fumbles_lost,
            SUM(rushing_first_downs) AS rushing_first_downs,
            SUM(rushing_epa) AS rushing_epa,
            SUM(rushing_2pt_conversions) AS rushing_2pt_conversions,

            -- Receiving stats
            SUM(receptions) AS receptions,
            SUM(receiving_yards) AS receiving_yards,
            SUM(receiving_tds) AS receiving_tds,
            SUM(targets) AS targets,
            SUM(receiving_fumbles) AS receiving_fumbles,
            SUM(receiving_fumbles_lost) AS receiving_fumbles_lost,
            SUM(receiving_first_downs) AS receiving_first_downs,
            SUM(receiving_epa) AS receiving_epa,
            SUM(receiving_2pt_conversions) AS receiving_2pt_conversions,
            AVG(target_share) AS target_share,
            AVG(wopr) AS wopr,
            AVG(racr) AS racr,
            SUM(receiving_air_yards) AS receiving_air_yards,
            SUM(receiving_yards_after_catch) AS receiving_yards_after_catch,
            AVG(air_yards_share) AS air_yards_share,

            -- Kicking stats
            SUM(fg_made) AS fg_made,
            SUM(fg_att) AS fg_att,
            CASE WHEN SUM(fg_att) > 0 THEN ROUND(CAST(SUM(fg_made) AS DOUBLE) / SUM(fg_att), 4) ELSE NULL END AS fg_pct,
            MAX(fg_long) AS fg_long,
            SUM(fg_made_0_19) AS fg_made_0_19,
            SUM(fg_made_20_29) AS fg_made_20_29,
            SUM(fg_made_30_39) AS fg_made_30_39,
            SUM(fg_made_40_49) AS fg_made_40_49,
            SUM(fg_made_50_59) AS fg_made_50_59,
            SUM(fg_missed) AS fg_missed,
            SUM(pat_made) AS pat_made,
            SUM(pat_att) AS pat_att,
            SUM(pat_missed) AS pat_missed,

            -- Derived rate stats (recomputed from season/career totals)
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(completions) AS DOUBLE) / SUM(attempts), 4) ELSE NULL END AS comp_pct,
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(passing_yards) AS DOUBLE) / SUM(attempts), 2) ELSE NULL END AS yards_per_attempt,
            CASE WHEN SUM(attempts) >= 1 THEN ROUND(
                (LEAST(2.375, GREATEST(0, (CAST(SUM(completions) AS DOUBLE)/SUM(attempts) - 0.3) * 5)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_yards) AS DOUBLE)/SUM(attempts) - 3) * 0.25)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_tds) AS DOUBLE)/SUM(attempts)) * 20)) +
                 LEAST(2.375, GREATEST(0, 2.375 - (COALESCE(CAST(SUM(passing_interceptions) AS DOUBLE),0)/SUM(attempts) * 25)))
                ) / 6 * 100, 1) ELSE NULL END AS passer_rating,
            CASE WHEN SUM(carries) > 0 THEN ROUND(CAST(SUM(rushing_yards) AS DOUBLE) / SUM(carries), 2) ELSE NULL END AS yards_per_carry,
            CASE WHEN SUM(receptions) > 0 THEN ROUND(CAST(SUM(receiving_yards) AS DOUBLE) / SUM(receptions), 2) ELSE NULL END AS yards_per_reception,
            CASE WHEN SUM(targets) > 0 THEN ROUND(CAST(SUM(receptions) AS DOUBLE) / SUM(targets), 4) ELSE NULL END AS catch_rate,

            -- Big play / long stats
            MAX(passing_long) AS passing_long,
            SUM(completions_40plus) AS completions_40plus,
            SUM(passing_tds_40plus) AS passing_tds_40plus,
            SUM(passing_tds_50plus) AS passing_tds_50plus,
            MAX(rushing_long) AS rushing_long,
            SUM(rushing_40plus) AS rushing_40plus,
            SUM(rushing_tds_40plus) AS rushing_tds_40plus,
            SUM(rushing_tds_50plus) AS rushing_tds_50plus,
            MAX(receiving_long) AS receiving_long,
            SUM(receptions_40plus) AS receptions_40plus,
            SUM(receiving_tds_40plus) AS receiving_tds_40plus,
            SUM(receiving_tds_50plus) AS receiving_tds_50plus,
            SUM(fg_yds_over_30) AS fg_yds_over_30,

            -- Defense/IDP stats
            SUM(def_sacks) AS def_sacks,
            SUM(def_sack_yards) AS def_sack_yards,
            SUM(def_qb_hits) AS def_qb_hits,
            SUM(def_interceptions) AS def_interceptions,
            SUM(def_interception_yards) AS def_interception_yards,
            SUM(def_pass_defended) AS def_pass_defended,
            SUM(def_tackles_solo) AS def_tackles_solo,
            SUM(def_tackle_assists) AS def_tackle_assists,
            SUM(def_tackles_with_assist) AS def_tackles_with_assist,
            SUM(def_tackles_for_loss) AS def_tackles_for_loss,
            SUM(def_tackles_for_loss_yards) AS def_tackles_for_loss_yards,
            SUM(def_fumbles) AS def_fumbles,
            SUM(def_fumbles_forced) AS def_fumbles_forced,
            SUM(def_safeties) AS def_safeties,
            SUM(def_tds) AS def_tds,

            -- DST stats
            SUM(pts_allow) AS pts_allow,
            SUM(dst_points_allowed) AS dst_points_allowed,
            SUM(points_allowed) AS points_allowed,
            SUM(passing_yds_allowed) AS passing_yds_allowed,
            SUM(rushing_yds_allowed) AS rushing_yds_allowed,
            SUM(total_yds_allowed) AS total_yds_allowed,
            SUM(fum_rec) AS fum_rec,
            SUM(fum_ret_td) AS fum_ret_td,
            SUM(special_teams_tds) AS special_teams_tds,
            SUM(three_out) AS three_out,
            SUM(fourth_down_stop) AS fourth_down_stop,
            SUM(dst_return_yards) AS dst_return_yards,
            SUM(kickoff_return_yards) AS kickoff_return_yards,
            SUM(punt_return_yards) AS punt_return_yards,
            SUM(fum_rec_yds) AS fum_rec_yds,
            SUM(fg_blocked) AS fg_blocked,

            -- Pre-calculated fantasy points by category
            SUM(pts_pass_4pt) AS pts_pass_4pt,
            SUM(pts_pass_6pt) AS pts_pass_6pt,
            SUM(pts_rush) AS pts_rush,
            SUM(pts_rec_0ppr) AS pts_rec_0ppr,
            SUM(pts_rec_half) AS pts_rec_half,
            SUM(pts_rec_ppr) AS pts_rec_ppr,
            SUM(pts_misc) AS pts_misc,
            SUM(pts_k_std) AS pts_k_std,
            SUM(pts_k_yds) AS pts_k_yds,
            SUM(pts_k_flat) AS pts_k_flat,
            SUM(pts_def_std) AS pts_def_std,
            SUM(pts_def_ya) AS pts_def_ya,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)

            -- IDP fantasy points (in CREATE TABLE, before last_updated)
            SUM(pts_idp_std) AS pts_idp_std,
            SUM(pts_idp_premium) AS pts_idp_premium,
            SUM(pts_idp_big_play) AS pts_idp_big_play,
            SUM(pts_idp_tackle_heavy) AS pts_idp_tackle_heavy,

            -- Metadata (after IDP columns in CREATE TABLE)
            CURRENT_TIMESTAMP AS last_updated,

            -- Pre-computed PPG metrics (Season: AVG of weekly PPG, consistency, weighted_ppg, avg_pts_next_year)
            -- 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TEP variants
            AVG(ppg_season_4pt_0ppr) AS ppg_season_4pt_0ppr,
            AVG(ppg_season_4pt_half) AS ppg_season_4pt_half,
            AVG(ppg_season_4pt_ppr) AS ppg_season_4pt_ppr,
            AVG(ppg_season_4pt_tep) AS ppg_season_4pt_tep,
            AVG(ppg_season_5pt_0ppr) AS ppg_season_5pt_0ppr,
            AVG(ppg_season_5pt_half) AS ppg_season_5pt_half,
            AVG(ppg_season_5pt_ppr) AS ppg_season_5pt_ppr,
            AVG(ppg_season_5pt_tep) AS ppg_season_5pt_tep,
            AVG(ppg_season_6pt_0ppr) AS ppg_season_6pt_0ppr,
            AVG(ppg_season_6pt_half) AS ppg_season_6pt_half,
            AVG(ppg_season_6pt_ppr) AS ppg_season_6pt_ppr,
            AVG(ppg_season_6pt_tep) AS ppg_season_6pt_tep,
            AVG(consistency_4pt_0ppr) AS consistency_4pt_0ppr,
            AVG(consistency_4pt_half) AS consistency_4pt_half,
            AVG(consistency_4pt_ppr) AS consistency_4pt_ppr,
            AVG(consistency_4pt_tep) AS consistency_4pt_tep,
            AVG(consistency_5pt_0ppr) AS consistency_5pt_0ppr,
            AVG(consistency_5pt_half) AS consistency_5pt_half,
            AVG(consistency_5pt_ppr) AS consistency_5pt_ppr,
            AVG(consistency_5pt_tep) AS consistency_5pt_tep,
            AVG(consistency_6pt_0ppr) AS consistency_6pt_0ppr,
            AVG(consistency_6pt_half) AS consistency_6pt_half,
            AVG(consistency_6pt_ppr) AS consistency_6pt_ppr,
            AVG(consistency_6pt_tep) AS consistency_6pt_tep,
            -- Weighted PPG (end-of-season value, use MAX to get final week's value)
            MAX(weighted_ppg_4pt_0ppr) AS weighted_ppg_4pt_0ppr,
            MAX(weighted_ppg_4pt_half) AS weighted_ppg_4pt_half,
            MAX(weighted_ppg_4pt_ppr) AS weighted_ppg_4pt_ppr,
            MAX(weighted_ppg_4pt_tep) AS weighted_ppg_4pt_tep,
            MAX(weighted_ppg_5pt_0ppr) AS weighted_ppg_5pt_0ppr,
            MAX(weighted_ppg_5pt_half) AS weighted_ppg_5pt_half,
            MAX(weighted_ppg_5pt_ppr) AS weighted_ppg_5pt_ppr,
            MAX(weighted_ppg_5pt_tep) AS weighted_ppg_5pt_tep,
            MAX(weighted_ppg_6pt_0ppr) AS weighted_ppg_6pt_0ppr,
            MAX(weighted_ppg_6pt_half) AS weighted_ppg_6pt_half,
            MAX(weighted_ppg_6pt_ppr) AS weighted_ppg_6pt_ppr,
            MAX(weighted_ppg_6pt_tep) AS weighted_ppg_6pt_tep,
            -- Avg Points Next Year (same across all weeks in a season, use MAX)
            MAX(avg_pts_next_year_4pt_0ppr) AS avg_pts_next_year_4pt_0ppr,
            MAX(avg_pts_next_year_4pt_half) AS avg_pts_next_year_4pt_half,
            MAX(avg_pts_next_year_4pt_ppr) AS avg_pts_next_year_4pt_ppr,
            MAX(avg_pts_next_year_4pt_tep) AS avg_pts_next_year_4pt_tep,
            MAX(avg_pts_next_year_5pt_0ppr) AS avg_pts_next_year_5pt_0ppr,
            MAX(avg_pts_next_year_5pt_half) AS avg_pts_next_year_5pt_half,
            MAX(avg_pts_next_year_5pt_ppr) AS avg_pts_next_year_5pt_ppr,
            MAX(avg_pts_next_year_5pt_tep) AS avg_pts_next_year_5pt_tep,
            MAX(avg_pts_next_year_6pt_0ppr) AS avg_pts_next_year_6pt_0ppr,
            MAX(avg_pts_next_year_6pt_half) AS avg_pts_next_year_6pt_half,
            MAX(avg_pts_next_year_6pt_ppr) AS avg_pts_next_year_6pt_ppr,
            MAX(avg_pts_next_year_6pt_tep) AS avg_pts_next_year_6pt_tep,

            -- Research LAMAR columns (SUM for totals, AVG for per-game)
            {lamar_agg_sql}

        FROM deduped
        GROUP BY NFL_player_id, year
    """)

    # Get row count (use simple year filter for aggregated table)
    count_filter = f"WHERE year = {year}" if year else ""
    count = conn.execute(f"""
        SELECT COUNT(*) FROM {SEASON_TABLE}
        {count_filter}
    """).fetchone()[0]

    log(f"  Aggregated {count:,} player-seasons")
    return count


def aggregate_to_career(conn) -> int:
    """
    Aggregate super_table to career totals.

    Only includes REGULAR SEASON games (season_type = 'REG').
    This makes the default UI fast. Playoff data is available via
    the "Include Playoffs" option which uses runtime aggregation.

    DEDUPLICATION: Uses CTE to merge duplicate player_week entries.
    Some 1960s players (19 players, 175 weeks) have duplicate entries
    because they played both a skill position AND kicker.

    Args:
        conn: DuckDB connection

    Returns:
        Number of rows aggregated
    """
    log("Aggregating career stats (regular season only)...")

    # Filter to regular season only (exclude playoff games)
    reg_season_filter = "(season_type IS NULL OR season_type = 'REG')"
    where_clause = f"WHERE {reg_season_filter}"

    # Get the deduplication CTE
    deduped_cte = get_deduped_cte(where_clause)

    # Build LAMAR aggregation expressions (SUM for totals, AVG for per-game)
    _lamar_cols = get_all_lamar_columns()
    # All SUMs first (lamar_*), then all AVGs (lamar_ppg_*) to match DDL column order
    _lamar_sum_parts = [f"SUM({_c}) AS {_c}" for _c in _lamar_cols]
    _lamar_avg_parts = [f"AVG({_c}) AS {_c.replace('lamar_', 'lamar_ppg_', 1)}" for _c in _lamar_cols]
    lamar_agg_sql = ",\n            ".join(_lamar_sum_parts + _lamar_avg_parts)

    # Full rebuild of career table (all players)
    conn.execute(f"DELETE FROM {CAREER_TABLE}")

    result = conn.execute(f"""
        INSERT INTO {CAREER_TABLE}
        WITH {deduped_cte}
        SELECT
            NFL_player_id,

            -- Player metadata (most recent year's name/team to handle DST franchise merges)
            ARG_MAX(player, year * 100 + COALESCE(week, 0)) AS player,
            -- Use most frequent position (the position they played most games at)
            MODE(nfl_position) AS nfl_position,
            ARG_MAX(nfl_team, year * 100 + COALESCE(week, 0)) AS nfl_team,
            MAX(headshot_url) AS headshot_url,

            -- Career span
            MIN(year) AS first_year,
            MAX(year) AS last_year,
            COUNT(DISTINCT year) AS years_active,
            COUNT(*) AS games_played,

            -- Passing stats
            SUM(passing_yards) AS passing_yards,
            SUM(passing_tds) AS passing_tds,
            SUM(passing_interceptions) AS passing_interceptions,
            SUM(attempts) AS attempts,
            SUM(completions) AS completions,
            SUM(passing_air_yards) AS passing_air_yards,
            SUM(passing_yards_after_catch) AS passing_yards_after_catch,
            SUM(passing_first_downs) AS passing_first_downs,
            SUM(passing_epa) AS passing_epa,
            AVG(passing_cpoe) AS passing_cpoe,
            AVG(pacr) AS pacr,
            SUM(passing_2pt_conversions) AS passing_2pt_conversions,

            -- Rushing stats
            SUM(rushing_yards) AS rushing_yards,
            SUM(carries) AS carries,
            SUM(rushing_tds) AS rushing_tds,
            SUM(rushing_fumbles) AS rushing_fumbles,
            SUM(rushing_fumbles_lost) AS rushing_fumbles_lost,
            SUM(rushing_first_downs) AS rushing_first_downs,
            SUM(rushing_epa) AS rushing_epa,
            SUM(rushing_2pt_conversions) AS rushing_2pt_conversions,

            -- Receiving stats
            SUM(receptions) AS receptions,
            SUM(receiving_yards) AS receiving_yards,
            SUM(receiving_tds) AS receiving_tds,
            SUM(targets) AS targets,
            SUM(receiving_fumbles) AS receiving_fumbles,
            SUM(receiving_fumbles_lost) AS receiving_fumbles_lost,
            SUM(receiving_first_downs) AS receiving_first_downs,
            SUM(receiving_epa) AS receiving_epa,
            SUM(receiving_2pt_conversions) AS receiving_2pt_conversions,
            AVG(target_share) AS target_share,
            AVG(wopr) AS wopr,
            AVG(racr) AS racr,
            SUM(receiving_air_yards) AS receiving_air_yards,
            SUM(receiving_yards_after_catch) AS receiving_yards_after_catch,
            AVG(air_yards_share) AS air_yards_share,

            -- Kicking stats
            SUM(fg_made) AS fg_made,
            SUM(fg_att) AS fg_att,
            CASE WHEN SUM(fg_att) > 0 THEN ROUND(CAST(SUM(fg_made) AS DOUBLE) / SUM(fg_att), 4) ELSE NULL END AS fg_pct,
            MAX(fg_long) AS fg_long,
            SUM(fg_made_0_19) AS fg_made_0_19,
            SUM(fg_made_20_29) AS fg_made_20_29,
            SUM(fg_made_30_39) AS fg_made_30_39,
            SUM(fg_made_40_49) AS fg_made_40_49,
            SUM(fg_made_50_59) AS fg_made_50_59,
            SUM(fg_missed) AS fg_missed,
            SUM(pat_made) AS pat_made,
            SUM(pat_att) AS pat_att,
            SUM(pat_missed) AS pat_missed,

            -- Derived rate stats (recomputed from season/career totals)
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(completions) AS DOUBLE) / SUM(attempts), 4) ELSE NULL END AS comp_pct,
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(passing_yards) AS DOUBLE) / SUM(attempts), 2) ELSE NULL END AS yards_per_attempt,
            CASE WHEN SUM(attempts) >= 1 THEN ROUND(
                (LEAST(2.375, GREATEST(0, (CAST(SUM(completions) AS DOUBLE)/SUM(attempts) - 0.3) * 5)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_yards) AS DOUBLE)/SUM(attempts) - 3) * 0.25)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_tds) AS DOUBLE)/SUM(attempts)) * 20)) +
                 LEAST(2.375, GREATEST(0, 2.375 - (COALESCE(CAST(SUM(passing_interceptions) AS DOUBLE),0)/SUM(attempts) * 25)))
                ) / 6 * 100, 1) ELSE NULL END AS passer_rating,
            CASE WHEN SUM(carries) > 0 THEN ROUND(CAST(SUM(rushing_yards) AS DOUBLE) / SUM(carries), 2) ELSE NULL END AS yards_per_carry,
            CASE WHEN SUM(receptions) > 0 THEN ROUND(CAST(SUM(receiving_yards) AS DOUBLE) / SUM(receptions), 2) ELSE NULL END AS yards_per_reception,
            CASE WHEN SUM(targets) > 0 THEN ROUND(CAST(SUM(receptions) AS DOUBLE) / SUM(targets), 4) ELSE NULL END AS catch_rate,

            -- Big play / long stats
            MAX(passing_long) AS passing_long,
            SUM(completions_40plus) AS completions_40plus,
            SUM(passing_tds_40plus) AS passing_tds_40plus,
            SUM(passing_tds_50plus) AS passing_tds_50plus,
            MAX(rushing_long) AS rushing_long,
            SUM(rushing_40plus) AS rushing_40plus,
            SUM(rushing_tds_40plus) AS rushing_tds_40plus,
            SUM(rushing_tds_50plus) AS rushing_tds_50plus,
            MAX(receiving_long) AS receiving_long,
            SUM(receptions_40plus) AS receptions_40plus,
            SUM(receiving_tds_40plus) AS receiving_tds_40plus,
            SUM(receiving_tds_50plus) AS receiving_tds_50plus,
            SUM(fg_yds_over_30) AS fg_yds_over_30,

            -- Defense/IDP stats
            SUM(def_sacks) AS def_sacks,
            SUM(def_sack_yards) AS def_sack_yards,
            SUM(def_qb_hits) AS def_qb_hits,
            SUM(def_interceptions) AS def_interceptions,
            SUM(def_interception_yards) AS def_interception_yards,
            SUM(def_pass_defended) AS def_pass_defended,
            SUM(def_tackles_solo) AS def_tackles_solo,
            SUM(def_tackle_assists) AS def_tackle_assists,
            SUM(def_tackles_with_assist) AS def_tackles_with_assist,
            SUM(def_tackles_for_loss) AS def_tackles_for_loss,
            SUM(def_tackles_for_loss_yards) AS def_tackles_for_loss_yards,
            SUM(def_fumbles) AS def_fumbles,
            SUM(def_fumbles_forced) AS def_fumbles_forced,
            SUM(def_safeties) AS def_safeties,
            SUM(def_tds) AS def_tds,

            -- DST stats
            SUM(pts_allow) AS pts_allow,
            SUM(dst_points_allowed) AS dst_points_allowed,
            SUM(points_allowed) AS points_allowed,
            SUM(passing_yds_allowed) AS passing_yds_allowed,
            SUM(rushing_yds_allowed) AS rushing_yds_allowed,
            SUM(total_yds_allowed) AS total_yds_allowed,
            SUM(fum_rec) AS fum_rec,
            SUM(fum_ret_td) AS fum_ret_td,
            SUM(special_teams_tds) AS special_teams_tds,
            SUM(three_out) AS three_out,
            SUM(fourth_down_stop) AS fourth_down_stop,
            SUM(dst_return_yards) AS dst_return_yards,
            SUM(kickoff_return_yards) AS kickoff_return_yards,
            SUM(punt_return_yards) AS punt_return_yards,
            SUM(fum_rec_yds) AS fum_rec_yds,
            SUM(fg_blocked) AS fg_blocked,

            -- Pre-calculated fantasy points by category
            SUM(pts_pass_4pt) AS pts_pass_4pt,
            SUM(pts_pass_6pt) AS pts_pass_6pt,
            SUM(pts_rush) AS pts_rush,
            SUM(pts_rec_0ppr) AS pts_rec_0ppr,
            SUM(pts_rec_half) AS pts_rec_half,
            SUM(pts_rec_ppr) AS pts_rec_ppr,
            SUM(pts_misc) AS pts_misc,
            SUM(pts_k_std) AS pts_k_std,
            SUM(pts_k_yds) AS pts_k_yds,
            SUM(pts_k_flat) AS pts_k_flat,
            SUM(pts_def_std) AS pts_def_std,
            SUM(pts_def_ya) AS pts_def_ya,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)

            -- IDP fantasy points (in CREATE TABLE, before last_updated)
            SUM(pts_idp_std) AS pts_idp_std,
            SUM(pts_idp_premium) AS pts_idp_premium,
            SUM(pts_idp_big_play) AS pts_idp_big_play,
            SUM(pts_idp_tackle_heavy) AS pts_idp_tackle_heavy,

            -- Metadata (after IDP columns in CREATE TABLE)
            CURRENT_TIMESTAMP AS last_updated,

            -- Pre-computed PPG metrics (Career: AVG of alltime PPG)
            -- 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TEP variants
            AVG(ppg_alltime_4pt_0ppr) AS ppg_alltime_4pt_0ppr,
            AVG(ppg_alltime_4pt_half) AS ppg_alltime_4pt_half,
            AVG(ppg_alltime_4pt_ppr) AS ppg_alltime_4pt_ppr,
            AVG(ppg_alltime_4pt_tep) AS ppg_alltime_4pt_tep,
            AVG(ppg_alltime_5pt_0ppr) AS ppg_alltime_5pt_0ppr,
            AVG(ppg_alltime_5pt_half) AS ppg_alltime_5pt_half,
            AVG(ppg_alltime_5pt_ppr) AS ppg_alltime_5pt_ppr,
            AVG(ppg_alltime_5pt_tep) AS ppg_alltime_5pt_tep,
            AVG(ppg_alltime_6pt_0ppr) AS ppg_alltime_6pt_0ppr,
            AVG(ppg_alltime_6pt_half) AS ppg_alltime_6pt_half,
            AVG(ppg_alltime_6pt_ppr) AS ppg_alltime_6pt_ppr,
            AVG(ppg_alltime_6pt_tep) AS ppg_alltime_6pt_tep,

            -- Research LAMAR columns (SUM for totals, AVG for per-game)
            {lamar_agg_sql}

        FROM deduped
        GROUP BY NFL_player_id
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM {CAREER_TABLE}").fetchone()[0]
    log(f"  Aggregated {count:,} player careers")
    return count


def aggregate_to_season_all(conn, year: int = None) -> int:
    """
    Aggregate super_table to season totals INCLUDING playoffs.

    This populates player_nfl_season_all with ALL games (regular + playoffs).
    Used when "Include Playoffs" checkbox is enabled.

    DEDUPLICATION: Uses CTE to merge duplicate player_week entries.
    Some 1960s players (19 players, 175 weeks) have duplicate entries
    because they played both a skill position AND kicker.

    Args:
        conn: DuckDB connection
        year: Specific year to aggregate, or None for all years

    Returns:
        Number of rows aggregated
    """
    # No season_type filter - include ALL games
    where_clause = f"WHERE year = {year}" if year else ""
    year_desc = str(year) if year else "all years"

    log(f"Aggregating season_all stats for {year_desc} (includes playoffs)...")

    # Delete existing data for this year (incremental update)
    if year:
        conn.execute(f"DELETE FROM {SEASON_TABLE_ALL} WHERE year = {year}")
    else:
        conn.execute(f"DELETE FROM {SEASON_TABLE_ALL}")

    # Get the deduplication CTE
    deduped_cte = get_deduped_cte(where_clause)

    # Build LAMAR aggregation expressions (SUM for totals, AVG for per-game)
    _lamar_cols = get_all_lamar_columns()
    # All SUMs first (lamar_*), then all AVGs (lamar_ppg_*) to match DDL column order
    _lamar_sum_parts = [f"SUM({_c}) AS {_c}" for _c in _lamar_cols]
    _lamar_avg_parts = [f"AVG({_c}) AS {_c.replace('lamar_', 'lamar_ppg_', 1)}" for _c in _lamar_cols]
    lamar_agg_sql = ",\n            ".join(_lamar_sum_parts + _lamar_avg_parts)

    # Same aggregation as aggregate_to_season but with no season_type filter
    result = conn.execute(f"""
        INSERT INTO {SEASON_TABLE_ALL}
        WITH {deduped_cte}
        SELECT
            NFL_player_id,
            year,
            -- Player metadata (most recent week's name/team to handle DST franchise merges)
            ARG_MAX(player, week) AS player,
            -- Use most frequent position (the position they played most games at)
            MODE(nfl_position) AS nfl_position,
            ARG_MAX(nfl_team, week) AS nfl_team,
            MAX(headshot_url) AS headshot_url,
            COUNT(*) AS games_played,
            SUM(passing_yards) AS passing_yards,
            SUM(passing_tds) AS passing_tds,
            SUM(passing_interceptions) AS passing_interceptions,
            SUM(attempts) AS attempts,
            SUM(completions) AS completions,
            SUM(passing_air_yards) AS passing_air_yards,
            SUM(passing_yards_after_catch) AS passing_yards_after_catch,
            SUM(passing_first_downs) AS passing_first_downs,
            SUM(passing_epa) AS passing_epa,
            AVG(passing_cpoe) AS passing_cpoe,
            AVG(pacr) AS pacr,
            SUM(passing_2pt_conversions) AS passing_2pt_conversions,
            SUM(rushing_yards) AS rushing_yards,
            SUM(carries) AS carries,
            SUM(rushing_tds) AS rushing_tds,
            SUM(rushing_fumbles) AS rushing_fumbles,
            SUM(rushing_fumbles_lost) AS rushing_fumbles_lost,
            SUM(rushing_first_downs) AS rushing_first_downs,
            SUM(rushing_epa) AS rushing_epa,
            SUM(rushing_2pt_conversions) AS rushing_2pt_conversions,
            SUM(receptions) AS receptions,
            SUM(receiving_yards) AS receiving_yards,
            SUM(receiving_tds) AS receiving_tds,
            SUM(targets) AS targets,
            SUM(receiving_fumbles) AS receiving_fumbles,
            SUM(receiving_fumbles_lost) AS receiving_fumbles_lost,
            SUM(receiving_first_downs) AS receiving_first_downs,
            SUM(receiving_epa) AS receiving_epa,
            SUM(receiving_2pt_conversions) AS receiving_2pt_conversions,
            AVG(target_share) AS target_share,
            AVG(wopr) AS wopr,
            AVG(racr) AS racr,
            SUM(receiving_air_yards) AS receiving_air_yards,
            SUM(receiving_yards_after_catch) AS receiving_yards_after_catch,
            AVG(air_yards_share) AS air_yards_share,
            SUM(fg_made) AS fg_made,
            SUM(fg_att) AS fg_att,
            CASE WHEN SUM(fg_att) > 0 THEN ROUND(CAST(SUM(fg_made) AS DOUBLE) / SUM(fg_att), 4) ELSE NULL END AS fg_pct,
            MAX(fg_long) AS fg_long,
            SUM(fg_made_0_19) AS fg_made_0_19,
            SUM(fg_made_20_29) AS fg_made_20_29,
            SUM(fg_made_30_39) AS fg_made_30_39,
            SUM(fg_made_40_49) AS fg_made_40_49,
            SUM(fg_made_50_59) AS fg_made_50_59,
            SUM(fg_missed) AS fg_missed,
            SUM(pat_made) AS pat_made,
            SUM(pat_att) AS pat_att,
            SUM(pat_missed) AS pat_missed,
            -- Derived rate stats
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(completions) AS DOUBLE) / SUM(attempts), 4) ELSE NULL END AS comp_pct,
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(passing_yards) AS DOUBLE) / SUM(attempts), 2) ELSE NULL END AS yards_per_attempt,
            CASE WHEN SUM(attempts) >= 1 THEN ROUND(
                (LEAST(2.375, GREATEST(0, (CAST(SUM(completions) AS DOUBLE)/SUM(attempts) - 0.3) * 5)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_yards) AS DOUBLE)/SUM(attempts) - 3) * 0.25)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_tds) AS DOUBLE)/SUM(attempts)) * 20)) +
                 LEAST(2.375, GREATEST(0, 2.375 - (COALESCE(CAST(SUM(passing_interceptions) AS DOUBLE),0)/SUM(attempts) * 25)))
                ) / 6 * 100, 1) ELSE NULL END AS passer_rating,
            CASE WHEN SUM(carries) > 0 THEN ROUND(CAST(SUM(rushing_yards) AS DOUBLE) / SUM(carries), 2) ELSE NULL END AS yards_per_carry,
            CASE WHEN SUM(receptions) > 0 THEN ROUND(CAST(SUM(receiving_yards) AS DOUBLE) / SUM(receptions), 2) ELSE NULL END AS yards_per_reception,
            CASE WHEN SUM(targets) > 0 THEN ROUND(CAST(SUM(receptions) AS DOUBLE) / SUM(targets), 4) ELSE NULL END AS catch_rate,
            MAX(passing_long) AS passing_long,
            SUM(completions_40plus) AS completions_40plus,
            SUM(passing_tds_40plus) AS passing_tds_40plus,
            SUM(passing_tds_50plus) AS passing_tds_50plus,
            MAX(rushing_long) AS rushing_long,
            SUM(rushing_40plus) AS rushing_40plus,
            SUM(rushing_tds_40plus) AS rushing_tds_40plus,
            SUM(rushing_tds_50plus) AS rushing_tds_50plus,
            MAX(receiving_long) AS receiving_long,
            SUM(receptions_40plus) AS receptions_40plus,
            SUM(receiving_tds_40plus) AS receiving_tds_40plus,
            SUM(receiving_tds_50plus) AS receiving_tds_50plus,
            SUM(fg_yds_over_30) AS fg_yds_over_30,
            SUM(def_sacks) AS def_sacks,
            SUM(def_sack_yards) AS def_sack_yards,
            SUM(def_qb_hits) AS def_qb_hits,
            SUM(def_interceptions) AS def_interceptions,
            SUM(def_interception_yards) AS def_interception_yards,
            SUM(def_pass_defended) AS def_pass_defended,
            SUM(def_tackles_solo) AS def_tackles_solo,
            SUM(def_tackle_assists) AS def_tackle_assists,
            SUM(def_tackles_with_assist) AS def_tackles_with_assist,
            SUM(def_tackles_for_loss) AS def_tackles_for_loss,
            SUM(def_tackles_for_loss_yards) AS def_tackles_for_loss_yards,
            SUM(def_fumbles) AS def_fumbles,
            SUM(def_fumbles_forced) AS def_fumbles_forced,
            SUM(def_safeties) AS def_safeties,
            SUM(def_tds) AS def_tds,
            SUM(pts_allow) AS pts_allow,
            SUM(dst_points_allowed) AS dst_points_allowed,
            SUM(points_allowed) AS points_allowed,
            SUM(passing_yds_allowed) AS passing_yds_allowed,
            SUM(rushing_yds_allowed) AS rushing_yds_allowed,
            SUM(total_yds_allowed) AS total_yds_allowed,
            SUM(fum_rec) AS fum_rec,
            SUM(fum_ret_td) AS fum_ret_td,
            SUM(special_teams_tds) AS special_teams_tds,
            SUM(three_out) AS three_out,
            SUM(fourth_down_stop) AS fourth_down_stop,
            SUM(dst_return_yards) AS dst_return_yards,
            SUM(kickoff_return_yards) AS kickoff_return_yards,
            SUM(punt_return_yards) AS punt_return_yards,
            SUM(fum_rec_yds) AS fum_rec_yds,
            SUM(fg_blocked) AS fg_blocked,
            SUM(pts_pass_4pt) AS pts_pass_4pt,
            SUM(pts_pass_6pt) AS pts_pass_6pt,
            SUM(pts_rush) AS pts_rush,
            SUM(pts_rec_0ppr) AS pts_rec_0ppr,
            SUM(pts_rec_half) AS pts_rec_half,
            SUM(pts_rec_ppr) AS pts_rec_ppr,
            SUM(pts_misc) AS pts_misc,
            SUM(pts_k_std) AS pts_k_std,
            SUM(pts_k_yds) AS pts_k_yds,
            SUM(pts_k_flat) AS pts_k_flat,
            SUM(pts_def_std) AS pts_def_std,
            SUM(pts_def_ya) AS pts_def_ya,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            -- IDP fantasy points (in CREATE TABLE, before last_updated)
            SUM(pts_idp_std) AS pts_idp_std,
            SUM(pts_idp_premium) AS pts_idp_premium,
            SUM(pts_idp_big_play) AS pts_idp_big_play,
            SUM(pts_idp_tackle_heavy) AS pts_idp_tackle_heavy,
            -- Metadata (after IDP columns in CREATE TABLE)
            CURRENT_TIMESTAMP AS last_updated,
            -- Pre-computed PPG metrics (Season: AVG of weekly PPG, consistency, weighted_ppg, avg_pts_next_year)
            -- 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TEP variants
            AVG(ppg_season_4pt_0ppr) AS ppg_season_4pt_0ppr,
            AVG(ppg_season_4pt_half) AS ppg_season_4pt_half,
            AVG(ppg_season_4pt_ppr) AS ppg_season_4pt_ppr,
            AVG(ppg_season_4pt_tep) AS ppg_season_4pt_tep,
            AVG(ppg_season_5pt_0ppr) AS ppg_season_5pt_0ppr,
            AVG(ppg_season_5pt_half) AS ppg_season_5pt_half,
            AVG(ppg_season_5pt_ppr) AS ppg_season_5pt_ppr,
            AVG(ppg_season_5pt_tep) AS ppg_season_5pt_tep,
            AVG(ppg_season_6pt_0ppr) AS ppg_season_6pt_0ppr,
            AVG(ppg_season_6pt_half) AS ppg_season_6pt_half,
            AVG(ppg_season_6pt_ppr) AS ppg_season_6pt_ppr,
            AVG(ppg_season_6pt_tep) AS ppg_season_6pt_tep,
            AVG(consistency_4pt_0ppr) AS consistency_4pt_0ppr,
            AVG(consistency_4pt_half) AS consistency_4pt_half,
            AVG(consistency_4pt_ppr) AS consistency_4pt_ppr,
            AVG(consistency_4pt_tep) AS consistency_4pt_tep,
            AVG(consistency_5pt_0ppr) AS consistency_5pt_0ppr,
            AVG(consistency_5pt_half) AS consistency_5pt_half,
            AVG(consistency_5pt_ppr) AS consistency_5pt_ppr,
            AVG(consistency_5pt_tep) AS consistency_5pt_tep,
            AVG(consistency_6pt_0ppr) AS consistency_6pt_0ppr,
            AVG(consistency_6pt_half) AS consistency_6pt_half,
            AVG(consistency_6pt_ppr) AS consistency_6pt_ppr,
            AVG(consistency_6pt_tep) AS consistency_6pt_tep,
            -- Weighted PPG (end-of-season value, use MAX to get final week's value)
            MAX(weighted_ppg_4pt_0ppr) AS weighted_ppg_4pt_0ppr,
            MAX(weighted_ppg_4pt_half) AS weighted_ppg_4pt_half,
            MAX(weighted_ppg_4pt_ppr) AS weighted_ppg_4pt_ppr,
            MAX(weighted_ppg_4pt_tep) AS weighted_ppg_4pt_tep,
            MAX(weighted_ppg_5pt_0ppr) AS weighted_ppg_5pt_0ppr,
            MAX(weighted_ppg_5pt_half) AS weighted_ppg_5pt_half,
            MAX(weighted_ppg_5pt_ppr) AS weighted_ppg_5pt_ppr,
            MAX(weighted_ppg_5pt_tep) AS weighted_ppg_5pt_tep,
            MAX(weighted_ppg_6pt_0ppr) AS weighted_ppg_6pt_0ppr,
            MAX(weighted_ppg_6pt_half) AS weighted_ppg_6pt_half,
            MAX(weighted_ppg_6pt_ppr) AS weighted_ppg_6pt_ppr,
            MAX(weighted_ppg_6pt_tep) AS weighted_ppg_6pt_tep,
            -- Avg Points Next Year (same across all weeks in a season, use MAX)
            MAX(avg_pts_next_year_4pt_0ppr) AS avg_pts_next_year_4pt_0ppr,
            MAX(avg_pts_next_year_4pt_half) AS avg_pts_next_year_4pt_half,
            MAX(avg_pts_next_year_4pt_ppr) AS avg_pts_next_year_4pt_ppr,
            MAX(avg_pts_next_year_4pt_tep) AS avg_pts_next_year_4pt_tep,
            MAX(avg_pts_next_year_5pt_0ppr) AS avg_pts_next_year_5pt_0ppr,
            MAX(avg_pts_next_year_5pt_half) AS avg_pts_next_year_5pt_half,
            MAX(avg_pts_next_year_5pt_ppr) AS avg_pts_next_year_5pt_ppr,
            MAX(avg_pts_next_year_5pt_tep) AS avg_pts_next_year_5pt_tep,
            MAX(avg_pts_next_year_6pt_0ppr) AS avg_pts_next_year_6pt_0ppr,
            MAX(avg_pts_next_year_6pt_half) AS avg_pts_next_year_6pt_half,
            MAX(avg_pts_next_year_6pt_ppr) AS avg_pts_next_year_6pt_ppr,
            MAX(avg_pts_next_year_6pt_tep) AS avg_pts_next_year_6pt_tep,

            -- Research LAMAR columns (SUM for totals, AVG for per-game)
            {lamar_agg_sql}

        FROM deduped
        GROUP BY NFL_player_id, year
    """)

    count = conn.execute(
        f"SELECT COUNT(*) FROM {SEASON_TABLE_ALL}" + (f" WHERE year = {year}" if year else "")
    ).fetchone()[0]
    log(f"  Aggregated {count:,} player-seasons (all games)")
    return count


def aggregate_to_career_all(conn) -> int:
    """
    Aggregate super_table to career totals INCLUDING playoffs.

    This populates player_nfl_career_all with ALL games (regular + playoffs).
    Used when "Include Playoffs" checkbox is enabled.

    DEDUPLICATION: Uses CTE to merge duplicate player_week entries.
    Some 1960s players (19 players, 175 weeks) have duplicate entries
    because they played both a skill position AND kicker.

    Args:
        conn: DuckDB connection

    Returns:
        Number of rows aggregated
    """
    log("Aggregating career_all stats (includes playoffs)...")

    # Get the deduplication CTE (no WHERE clause = all games)
    deduped_cte = get_deduped_cte("")

    # Build LAMAR aggregation expressions (SUM for totals, AVG for per-game)
    _lamar_cols = get_all_lamar_columns()
    # All SUMs first (lamar_*), then all AVGs (lamar_ppg_*) to match DDL column order
    _lamar_sum_parts = [f"SUM({_c}) AS {_c}" for _c in _lamar_cols]
    _lamar_avg_parts = [f"AVG({_c}) AS {_c.replace('lamar_', 'lamar_ppg_', 1)}" for _c in _lamar_cols]
    lamar_agg_sql = ",\n            ".join(_lamar_sum_parts + _lamar_avg_parts)

    # Full rebuild of career table (all players)
    conn.execute(f"DELETE FROM {CAREER_TABLE_ALL}")

    result = conn.execute(f"""
        INSERT INTO {CAREER_TABLE_ALL}
        WITH {deduped_cte}
        SELECT
            NFL_player_id,
            -- Player metadata (most recent year's name/team to handle DST franchise merges)
            ARG_MAX(player, year * 100 + COALESCE(week, 0)) AS player,
            -- Use most frequent position (the position they played most games at)
            MODE(nfl_position) AS nfl_position,
            ARG_MAX(nfl_team, year * 100 + COALESCE(week, 0)) AS nfl_team,
            MAX(headshot_url) AS headshot_url,
            MIN(year) AS first_year,
            MAX(year) AS last_year,
            COUNT(DISTINCT year) AS years_active,
            COUNT(*) AS games_played,
            SUM(passing_yards) AS passing_yards,
            SUM(passing_tds) AS passing_tds,
            SUM(passing_interceptions) AS passing_interceptions,
            SUM(attempts) AS attempts,
            SUM(completions) AS completions,
            SUM(passing_air_yards) AS passing_air_yards,
            SUM(passing_yards_after_catch) AS passing_yards_after_catch,
            SUM(passing_first_downs) AS passing_first_downs,
            SUM(passing_epa) AS passing_epa,
            AVG(passing_cpoe) AS passing_cpoe,
            AVG(pacr) AS pacr,
            SUM(passing_2pt_conversions) AS passing_2pt_conversions,
            SUM(rushing_yards) AS rushing_yards,
            SUM(carries) AS carries,
            SUM(rushing_tds) AS rushing_tds,
            SUM(rushing_fumbles) AS rushing_fumbles,
            SUM(rushing_fumbles_lost) AS rushing_fumbles_lost,
            SUM(rushing_first_downs) AS rushing_first_downs,
            SUM(rushing_epa) AS rushing_epa,
            SUM(rushing_2pt_conversions) AS rushing_2pt_conversions,
            SUM(receptions) AS receptions,
            SUM(receiving_yards) AS receiving_yards,
            SUM(receiving_tds) AS receiving_tds,
            SUM(targets) AS targets,
            SUM(receiving_fumbles) AS receiving_fumbles,
            SUM(receiving_fumbles_lost) AS receiving_fumbles_lost,
            SUM(receiving_first_downs) AS receiving_first_downs,
            SUM(receiving_epa) AS receiving_epa,
            SUM(receiving_2pt_conversions) AS receiving_2pt_conversions,
            AVG(target_share) AS target_share,
            AVG(wopr) AS wopr,
            AVG(racr) AS racr,
            SUM(receiving_air_yards) AS receiving_air_yards,
            SUM(receiving_yards_after_catch) AS receiving_yards_after_catch,
            AVG(air_yards_share) AS air_yards_share,
            SUM(fg_made) AS fg_made,
            SUM(fg_att) AS fg_att,
            CASE WHEN SUM(fg_att) > 0 THEN ROUND(CAST(SUM(fg_made) AS DOUBLE) / SUM(fg_att), 4) ELSE NULL END AS fg_pct,
            MAX(fg_long) AS fg_long,
            SUM(fg_made_0_19) AS fg_made_0_19,
            SUM(fg_made_20_29) AS fg_made_20_29,
            SUM(fg_made_30_39) AS fg_made_30_39,
            SUM(fg_made_40_49) AS fg_made_40_49,
            SUM(fg_made_50_59) AS fg_made_50_59,
            SUM(fg_missed) AS fg_missed,
            SUM(pat_made) AS pat_made,
            SUM(pat_att) AS pat_att,
            SUM(pat_missed) AS pat_missed,
            -- Derived rate stats
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(completions) AS DOUBLE) / SUM(attempts), 4) ELSE NULL END AS comp_pct,
            CASE WHEN SUM(attempts) > 0 THEN ROUND(CAST(SUM(passing_yards) AS DOUBLE) / SUM(attempts), 2) ELSE NULL END AS yards_per_attempt,
            CASE WHEN SUM(attempts) >= 1 THEN ROUND(
                (LEAST(2.375, GREATEST(0, (CAST(SUM(completions) AS DOUBLE)/SUM(attempts) - 0.3) * 5)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_yards) AS DOUBLE)/SUM(attempts) - 3) * 0.25)) +
                 LEAST(2.375, GREATEST(0, (CAST(SUM(passing_tds) AS DOUBLE)/SUM(attempts)) * 20)) +
                 LEAST(2.375, GREATEST(0, 2.375 - (COALESCE(CAST(SUM(passing_interceptions) AS DOUBLE),0)/SUM(attempts) * 25)))
                ) / 6 * 100, 1) ELSE NULL END AS passer_rating,
            CASE WHEN SUM(carries) > 0 THEN ROUND(CAST(SUM(rushing_yards) AS DOUBLE) / SUM(carries), 2) ELSE NULL END AS yards_per_carry,
            CASE WHEN SUM(receptions) > 0 THEN ROUND(CAST(SUM(receiving_yards) AS DOUBLE) / SUM(receptions), 2) ELSE NULL END AS yards_per_reception,
            CASE WHEN SUM(targets) > 0 THEN ROUND(CAST(SUM(receptions) AS DOUBLE) / SUM(targets), 4) ELSE NULL END AS catch_rate,
            MAX(passing_long) AS passing_long,
            SUM(completions_40plus) AS completions_40plus,
            SUM(passing_tds_40plus) AS passing_tds_40plus,
            SUM(passing_tds_50plus) AS passing_tds_50plus,
            MAX(rushing_long) AS rushing_long,
            SUM(rushing_40plus) AS rushing_40plus,
            SUM(rushing_tds_40plus) AS rushing_tds_40plus,
            SUM(rushing_tds_50plus) AS rushing_tds_50plus,
            MAX(receiving_long) AS receiving_long,
            SUM(receptions_40plus) AS receptions_40plus,
            SUM(receiving_tds_40plus) AS receiving_tds_40plus,
            SUM(receiving_tds_50plus) AS receiving_tds_50plus,
            SUM(fg_yds_over_30) AS fg_yds_over_30,
            SUM(def_sacks) AS def_sacks,
            SUM(def_sack_yards) AS def_sack_yards,
            SUM(def_qb_hits) AS def_qb_hits,
            SUM(def_interceptions) AS def_interceptions,
            SUM(def_interception_yards) AS def_interception_yards,
            SUM(def_pass_defended) AS def_pass_defended,
            SUM(def_tackles_solo) AS def_tackles_solo,
            SUM(def_tackle_assists) AS def_tackle_assists,
            SUM(def_tackles_with_assist) AS def_tackles_with_assist,
            SUM(def_tackles_for_loss) AS def_tackles_for_loss,
            SUM(def_tackles_for_loss_yards) AS def_tackles_for_loss_yards,
            SUM(def_fumbles) AS def_fumbles,
            SUM(def_fumbles_forced) AS def_fumbles_forced,
            SUM(def_safeties) AS def_safeties,
            SUM(def_tds) AS def_tds,
            SUM(pts_allow) AS pts_allow,
            SUM(dst_points_allowed) AS dst_points_allowed,
            SUM(points_allowed) AS points_allowed,
            SUM(passing_yds_allowed) AS passing_yds_allowed,
            SUM(rushing_yds_allowed) AS rushing_yds_allowed,
            SUM(total_yds_allowed) AS total_yds_allowed,
            SUM(fum_rec) AS fum_rec,
            SUM(fum_ret_td) AS fum_ret_td,
            SUM(special_teams_tds) AS special_teams_tds,
            SUM(three_out) AS three_out,
            SUM(fourth_down_stop) AS fourth_down_stop,
            SUM(dst_return_yards) AS dst_return_yards,
            SUM(kickoff_return_yards) AS kickoff_return_yards,
            SUM(punt_return_yards) AS punt_return_yards,
            SUM(fum_rec_yds) AS fum_rec_yds,
            SUM(fg_blocked) AS fg_blocked,
            SUM(pts_pass_4pt) AS pts_pass_4pt,
            SUM(pts_pass_6pt) AS pts_pass_6pt,
            SUM(pts_rush) AS pts_rush,
            SUM(pts_rec_0ppr) AS pts_rec_0ppr,
            SUM(pts_rec_half) AS pts_rec_half,
            SUM(pts_rec_ppr) AS pts_rec_ppr,
            SUM(pts_misc) AS pts_misc,
            SUM(pts_k_std) AS pts_k_std,
            SUM(pts_k_yds) AS pts_k_yds,
            SUM(pts_k_flat) AS pts_k_flat,
            SUM(pts_def_std) AS pts_def_std,
            SUM(pts_def_ya) AS pts_def_ya,
            -- pts_def_high removed 2026-04-30 (KMFFL-specific scoring)
            -- IDP fantasy points (in CREATE TABLE, before last_updated)
            SUM(pts_idp_std) AS pts_idp_std,
            SUM(pts_idp_premium) AS pts_idp_premium,
            SUM(pts_idp_big_play) AS pts_idp_big_play,
            SUM(pts_idp_tackle_heavy) AS pts_idp_tackle_heavy,
            -- Metadata (after IDP columns in CREATE TABLE)
            CURRENT_TIMESTAMP AS last_updated,
            -- Pre-computed PPG metrics (Career: AVG of alltime PPG)
            -- 12 variants = 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TEP variants
            AVG(ppg_alltime_4pt_0ppr) AS ppg_alltime_4pt_0ppr,
            AVG(ppg_alltime_4pt_half) AS ppg_alltime_4pt_half,
            AVG(ppg_alltime_4pt_ppr) AS ppg_alltime_4pt_ppr,
            AVG(ppg_alltime_4pt_tep) AS ppg_alltime_4pt_tep,
            AVG(ppg_alltime_5pt_0ppr) AS ppg_alltime_5pt_0ppr,
            AVG(ppg_alltime_5pt_half) AS ppg_alltime_5pt_half,
            AVG(ppg_alltime_5pt_ppr) AS ppg_alltime_5pt_ppr,
            AVG(ppg_alltime_5pt_tep) AS ppg_alltime_5pt_tep,
            AVG(ppg_alltime_6pt_0ppr) AS ppg_alltime_6pt_0ppr,
            AVG(ppg_alltime_6pt_half) AS ppg_alltime_6pt_half,
            AVG(ppg_alltime_6pt_ppr) AS ppg_alltime_6pt_ppr,
            AVG(ppg_alltime_6pt_tep) AS ppg_alltime_6pt_tep,

            -- Research LAMAR columns (SUM for totals, AVG for per-game)
            {lamar_agg_sql}

        FROM deduped
        GROUP BY NFL_player_id
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM {CAREER_TABLE_ALL}").fetchone()[0]
    log(f"  Aggregated {count:,} player careers (all games)")
    return count


def refresh_kicker_yardage_points(conn, year: int = None) -> dict[str, int]:
    """
    Refresh only pts_k_yds in the aggregate tables.

    This is a targeted repair path for kicker yardage scoring fixes. It avoids
    rebuilding the full aggregate tables and only updates K rows' pts_k_yds
    values from the corrected weekly super table.

    Args:
        conn: DuckDB connection
        year: Optional single year to scope the season tables. Career tables are
            refreshed globally for affected kickers.

    Returns:
        Dict with updated row counts per aggregate table.
    """
    log("Refreshing targeted kicker yardage points in aggregate tables...")

    reg_where = "WHERE season_type = 'REG'"
    if year:
        reg_where += f" AND year = {year}"
    all_where = f"WHERE year = {year}" if year else ""

    reg_deduped_cte = get_deduped_cte(reg_where)
    all_deduped_cte = get_deduped_cte(all_where)

    season_year_filter = f"AND tgt.year = {year}" if year else ""

    season_updated = conn.execute(f"""
        UPDATE {SEASON_TABLE} AS tgt
        SET
            pts_k_yds = src.pts_k_yds,
            last_updated = CURRENT_TIMESTAMP
        FROM (
            WITH {reg_deduped_cte}
            SELECT
                NFL_player_id,
                year,
                ROUND(SUM(COALESCE(pts_k_yds, 0)), 2) AS pts_k_yds
            FROM deduped
            WHERE nfl_position = 'K'
            GROUP BY NFL_player_id, year
        ) AS src
        WHERE tgt.NFL_player_id = src.NFL_player_id
          AND tgt.year = src.year
          AND tgt.nfl_position = 'K'
          {season_year_filter}
    """).rowcount

    season_all_updated = conn.execute(f"""
        UPDATE {SEASON_TABLE_ALL} AS tgt
        SET
            pts_k_yds = src.pts_k_yds,
            last_updated = CURRENT_TIMESTAMP
        FROM (
            WITH {all_deduped_cte}
            SELECT
                NFL_player_id,
                year,
                ROUND(SUM(COALESCE(pts_k_yds, 0)), 2) AS pts_k_yds
            FROM deduped
            WHERE nfl_position = 'K'
            GROUP BY NFL_player_id, year
        ) AS src
        WHERE tgt.NFL_player_id = src.NFL_player_id
          AND tgt.year = src.year
          AND tgt.nfl_position = 'K'
          {season_year_filter}
    """).rowcount

    career_updated = conn.execute(f"""
        UPDATE {CAREER_TABLE} AS tgt
        SET
            pts_k_yds = src.pts_k_yds,
            last_updated = CURRENT_TIMESTAMP
        FROM (
            WITH {reg_deduped_cte}
            SELECT
                NFL_player_id,
                ROUND(SUM(COALESCE(pts_k_yds, 0)), 2) AS pts_k_yds
            FROM deduped
            WHERE nfl_position = 'K'
            GROUP BY NFL_player_id
        ) AS src
        WHERE tgt.NFL_player_id = src.NFL_player_id
          AND tgt.nfl_position = 'K'
    """).rowcount

    career_all_updated = conn.execute(f"""
        UPDATE {CAREER_TABLE_ALL} AS tgt
        SET
            pts_k_yds = src.pts_k_yds,
            last_updated = CURRENT_TIMESTAMP
        FROM (
            WITH {all_deduped_cte}
            SELECT
                NFL_player_id,
                ROUND(SUM(COALESCE(pts_k_yds, 0)), 2) AS pts_k_yds
            FROM deduped
            WHERE nfl_position = 'K'
            GROUP BY NFL_player_id
        ) AS src
        WHERE tgt.NFL_player_id = src.NFL_player_id
          AND tgt.nfl_position = 'K'
    """).rowcount

    log(
        "  Kicker pts_k_yds refreshed:"
        f" season={season_updated},"
        f" season_all={season_all_updated},"
        f" career={career_updated},"
        f" career_all={career_all_updated}"
    )

    return {
        "season": season_updated,
        "season_all": season_all_updated,
        "career": career_updated,
        "career_all": career_all_updated,
    }


def update_aggregates(year: int = None, week: int = None) -> dict:
    """
    Update aggregated tables after weekly import.

    This is the main entry point called from the pipeline.
    Builds BOTH regular season tables AND all-games tables.

    Args:
        year: Year to update (required for incremental updates)
        week: Week number (informational only, not used in aggregation)

    Returns:
        Dict with counts: {'season': count, 'career': count, 'season_all': count, 'career_all': count}
    """
    backend = os.environ.get("DATABASE_BACKEND", "fly").lower()
    if backend == "fly":
        from multi_league.data_fetchers.aggregate_nfl_stats_fly import update_aggregates as update_fly_aggregates

        # Fly's ops aggregate tables are shared fast-path caches. Rebuild all
        # years so career and all-games tables stay consistent after historical
        # weekly-table repairs.
        return update_fly_aggregates(year=year, week=week, rebuild_all_years=True)

    log("=" * 60)
    log("NFL STATS AGGREGATION (Regular Season + All Games)")
    log("=" * 60)

    conn = get_connection()

    try:
        # Ensure database and schema exist
        ensure_schema_exists(conn)

        # Ensure ALL tables exist (regular + _all versions)
        create_season_table(conn)
        create_career_table(conn)
        create_season_table_all(conn)
        create_career_table_all(conn)

        # --- Regular Season Tables (default, fast path) ---
        log("\n--- Regular Season Tables ---")
        season_count = aggregate_to_season(conn, year)
        career_count = aggregate_to_career(conn)

        # --- All Games Tables (includes playoffs) ---
        log("\n--- All Games Tables (includes playoffs) ---")
        season_all_count = aggregate_to_season_all(conn, year)
        career_all_count = aggregate_to_career_all(conn)

        # --- Post-processing: Fix misclassified kickers ---
        # Some historical players (e.g., Fred Cone) only exist in kicker source files
        # but were primarily skill players. Reclassify K -> RB if 100+ rushing yards.
        log("\n--- Post-processing: Fixing misclassified kickers ---")
        for table in [SEASON_TABLE, CAREER_TABLE, SEASON_TABLE_ALL, CAREER_TABLE_ALL]:
            fixed = conn.execute(f"""
                UPDATE {table}
                SET nfl_position = 'RB'
                WHERE nfl_position = 'K'
                AND COALESCE(rushing_yards, 0) >= 100
            """).rowcount
            if fixed > 0:
                log(f"  {table}: Reclassified {fixed} kicker(s) with 100+ rushing yards as RB")

        log("")
        log("Aggregation complete:")
        log(f"  Regular season: {season_count:,} seasons, {career_count:,} careers")
        log(f"  All games:      {season_all_count:,} seasons, {career_all_count:,} careers")

        return {
            "season": season_count,
            "career": career_count,
            "season_all": season_all_count,
            "career_all": career_all_count,
        }

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Aggregate NFL stats from super table into season/career tables")
    parser.add_argument("--year", type=int, help="Specific year to aggregate")
    parser.add_argument("--rebuild", action="store_true", help="Full rebuild all years")

    args = parser.parse_args()

    if args.rebuild:
        result = update_aggregates(year=None)  # All years
    elif args.year:
        result = update_aggregates(year=args.year)
    else:
        # Default: current year
        result = update_aggregates(year=get_current_nfl_season_year())

    print(f"\nResult: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
