#!/usr/bin/env python3
"""
Aggregate Matchup Context to Season/Career Tables (Per-League)

Pre-aggregates weekly matchup data into:
- matchup_season: one row per (manager, year) — regular-season aggregates + end-of-season snapshots
- matchup_career: one row per manager — derived from matchup_season
- matchup_h2h_season: one row per (manager, opponent, year)
- matchup_h2h_career: one row per (manager, opponent)

These tables make all summary views instant single-SELECT queries, avoiding
the MCP 2,048 row / 50K char limits that required 11+ batched SQL queries.

Usage:
    python -m multi_league.transformations.aggregation.aggregate_matchup_context --context path/to/context.json
    python -m multi_league.transformations.aggregation.aggregate_matchup_context --db demo_league
"""

import argparse
import json
import re
import sys

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

from multi_league.core.db_utils import get_pipeline_connection
from multi_league.core.aggregate_ddl import aggregate_insert_columns, ensure_aggregate_table
from multi_league.core.canonical_matchup import (
    SCHEDULE_LUCK_SIM_COLUMNS,
    SOS_LUCK_SIM_COLUMNS,
    PLAYOFF_SIM_COLUMNS,
)
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    get_active_catalog,
    league_db_filter,
    make_logger,
)
from multi_league.transformations.aggregation.homepage_summary import (
    build_losses_sql,
    build_wins_sql,
    detect_h2h_median_years_from_db,
)

log = make_logger("MATCHUP-AGG")


def get_available_columns(conn, db_name: str, table: str) -> set:
    """Get set of column names that exist in a table."""
    configure_table_catalog(conn)
    try:
        rows = conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog = '{get_active_catalog()}' AND table_schema = 'public' AND table_name = '{table}'"
        ).fetchall()
        if rows:
            return {r[0] for r in rows}
    except Exception:  # noqa: broad-except
        pass
    # Fallback: DESCRIBE
    try:
        rows = conn.execute(f"DESCRIBE {central_table(table)}").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def get_playoff_start_weeks(conn, db_name: str) -> dict:
    """Load per-year playoff_start_week from league_settings table.

    Returns dict of {year: last_regular_week}.
    """
    year_boundaries = {}
    configure_table_catalog(conn)

    # Flat canonical schema only: playoff_start_week is a direct column.
    try:
        rows = conn.execute(
            f"SELECT year, playoff_start_week FROM {central_table('league_settings')} WHERE {league_db_filter(db_name)} ORDER BY year"
        ).fetchall()
        for year_val, psw in rows:
            year = int(year_val)
            if psw is not None:
                year_boundaries[year] = int(psw) - 1
        if year_boundaries:
            return year_boundaries
    except Exception as e:
        log(f"  WARNING: Could not read flat league_settings.playoff_start_week: {e}")

    return year_boundaries


def discover_dynamic_columns(existing_cols: set) -> dict:
    """Discover dynamic columns by regex patterns."""
    result = {
        "shuffle_win": [],  # shuffle_N_win
        "shuffle_seed": [],  # shuffle_N_seed
        "opp_shuffle_win": [],  # opp_shuffle_N_win
        "opp_shuffle_seed": [],  # opp_shuffle_N_seed
        "x_win": [],  # xN_win
        "x_seed": [],  # xN_seed
    }

    for col in existing_cols:
        if re.match(r"^shuffle_\d+_win$", col):
            result["shuffle_win"].append(col)
        elif re.match(r"^shuffle_\d+_seed$", col):
            result["shuffle_seed"].append(col)
        elif re.match(r"^opp_shuffle_\d+_win$", col):
            result["opp_shuffle_win"].append(col)
        elif re.match(r"^opp_shuffle_\d+_seed$", col):
            result["opp_shuffle_seed"].append(col)
        elif re.match(r"^x\d+_win$", col):
            result["x_win"].append(col)
        elif re.match(r"^x\d+_seed$", col):
            result["x_seed"].append(col)

    # Sort each list by numeric value
    for key in result:
        result[key].sort(key=lambda c: int(re.search(r"\d+", c).group()))

    return result


def _build_seed_snapshot_select(
    *,
    has_final_playoff_seed: bool,
    has_is_playoffs: bool = True,
    has_is_consolation: bool = True,
    has_is_bye_week: bool = True,
) -> str:
    """Build the season-level final seed snapshot expression."""
    is_playoffs = "COALESCE(CAST(is_playoffs AS INT), 0)" if has_is_playoffs else "0"
    is_consolation = "COALESCE(CAST(is_consolation AS INT), 0)" if has_is_consolation else "0"
    is_bye_week = "COALESCE(CAST(is_bye_week AS INT), 0)" if has_is_bye_week else "0"
    regular_row_condition = f"{is_playoffs} = 0 AND {is_consolation} = 0 AND {is_bye_week} = 0"
    fallback_expr = (
        "ARG_MAX(\n"
        f"            CASE WHEN {regular_row_condition} THEN playoff_seed_to_date END,\n"
        f"            CASE WHEN {regular_row_condition} THEN week END\n"
        "        )"
    )
    seed_expr = f"COALESCE(MAX(final_playoff_seed), {fallback_expr})" if has_final_playoff_seed else fallback_expr
    return f",\n        {seed_expr} AS playoff_seed_to_date"


def aggregate_matchup_season(conn, db_name: str, dry_run: bool = False) -> int:
    """Build matchup_season table from weekly matchup data.

    One row per (manager, year) with:
    - Aggregated stats from regular-season weeks
    - End-of-season snapshot values from the last regular-season week
    - Playoff detection from post-regular-season rows
    """
    log("Building matchup_season table...")
    configure_table_catalog(conn)

    existing_cols = get_available_columns(conn, db_name, "matchup")
    if not existing_cols:
        log("  WARNING: matchup table has no columns or doesn't exist")
        return 0

    has = lambda col: col in existing_cols
    if not has("franchise_id"):
        raise KeyError("franchise_id is required for manager identity")
    year_boundaries = get_playoff_start_weeks(conn, db_name)
    dynamic_cols = discover_dynamic_columns(existing_cols)

    # Get all years from matchup table
    years = [
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT year FROM {central_table('matchup')} WHERE {league_db_filter(db_name)} AND year IS NOT NULL ORDER BY year"
        ).fetchall()
    ]

    if not years:
        log("  No years found in matchup table")
        return 0

    # Fill in missing year boundaries with fallback (max week where all managers present)
    for yr in years:
        if yr not in year_boundaries:
            try:
                result = conn.execute(f"""
                    SELECT MAX(week) as max_week FROM (
                        SELECT week, COUNT(DISTINCT franchise_id) as mgr_count
                        FROM {central_table('matchup')}
                        WHERE {league_db_filter(db_name)}
                          AND year = {yr}
                          AND COALESCE(is_playoffs, 0) = 0
                          AND COALESCE(is_consolation, 0) = 0
                          AND manager IS NOT NULL
                        GROUP BY week
                    ) sub
                """).fetchone()
                if result and result[0]:
                    year_boundaries[yr] = int(result[0])
                else:
                    # Last resort: use 14
                    year_boundaries[yr] = 14
            except Exception:
                year_boundaries[yr] = 14

    log(f"  Year boundaries: {year_boundaries}")

    # Detect H2H+Median years so wins/losses include median wins
    median_years: list[int] = []
    if has("above_league_median") and has("below_league_median") and has("is_playoffs"):
        median_years = detect_h2h_median_years_from_db(conn, db_name)
        if median_years:
            log(f"  H2H+Median years detected: {median_years}")

    ensure_aggregate_table(conn, get_active_catalog(), "matchup_season")
    if not dry_run:
        execute_scoped(
            conn,
            f"DELETE FROM {central_table('matchup_season')} WHERE db_name = '{db_name}'",
            db_name,
            label="matchup_season:delete",
        )

    total_rows = 0

    for yr in years:
        last_reg_week = year_boundaries.get(yr, 14)
        log(f"  Processing year {yr} (last_regular_week={last_reg_week})...")

        # -- A. Aggregated stats (GROUP BY manager, year on regular-season weeks)
        # -- B. Snapshot values (ARG_MAX on last regular week)
        # -- C. Playoff detection (separate subquery)

        # Build projection columns for projected stats
        proj_col = (
            "team_projected_points"
            if has("team_projected_points")
            else "manager_proj_score"
            if has("manager_proj_score")
            else None
        )
        opp_proj_col = (
            "opponent_projected_points"
            if has("opponent_projected_points")
            else "opponent_proj_score"
            if has("opponent_proj_score")
            else None
        )

        # Build dynamic snapshot columns for luck/simulations
        snapshot_cols = []

        # Snapshot buckets come from canonical schema metadata, not hand-maintained lists.
        for col in SCHEDULE_LUCK_SIM_COLUMNS:
            if has(col):
                snapshot_cols.append(col)

        for col in SOS_LUCK_SIM_COLUMNS:
            if has(col):
                snapshot_cols.append(col)

        for col in PLAYOFF_SIM_COLUMNS:
            if has(col):
                snapshot_cols.append(col)

        # Standings columns (snapshot at end of regular season)
        # playoff_seed_to_date is handled separately (regular-season-only ARG_MAX)
        for col in [
            "wins_to_date",
            "losses_to_date",
            "ties_to_date",
            "points_scored_to_date",
        ]:
            if has(col):
                snapshot_cols.append(col)

        has_playoff_seed = has("playoff_seed_to_date")

        insert_cols = ["db_name", "manager", "year"]
        if has("franchise_id"):
            insert_cols.append("franchise_id")
        insert_cols.extend(
            [
                "games",
                "wins",
                "losses",
                "ties",
                "win_pct",
                "total_team_points",
                "total_opponent_points",
                "avg_team_points",
                "avg_opponent_points",
                "avg_margin",
                "max_team_points",
                "min_team_points",
                "std_dev_team_points",
                "avg_gpa",
                "above_league_median",
                "below_league_median",
                "close_games",
                "close_wins",
                "close_losses",
                "close_win_pct",
                "blowout_wins",
                "blowout_losses",
                "max_win_streak",
                "max_loss_streak",
                "optimal_games",
                "optimal_actual_pts",
                "optimal_ceiling_pts",
                "optimal_bench_pts",
                "optimal_efficiency",
                "optimal_wins",
                "optimal_losses",
                "optimal_missed_wins",
                "optimal_lucky_wins",
                "optimal_wins_actual",
                "optimal_losses_actual",
                "optimal_outcome_changes",
                "optimal_margin",
                "proj_games",
                "proj_wins",
                "proj_losses",
                "proj_total_team_points",
                "proj_total_opponent_points",
                "proj_total_proj",
                "proj_opp_proj",
                "proj_above_proj",
                "proj_below_proj",
                "proj_beat_spread",
                "proj_margin_total",
                "proj_spread_avg",
                "proj_favored_pct",
                "proj_avg_win_pct",
                "proj_expected_wins",
                "proj_upset_wins",
                "proj_upset_losses",
                "proj_total_upsets",
                "proj_total_error",
                "proj_avg_error",
                "made_playoffs",
                "is_champion",
                "is_sacko",
                "playoff_result",
            ]
        )
        insert_cols.extend(snapshot_cols)
        if has_playoff_seed:
            insert_cols.append("playoff_seed_to_date")

        # Mean simulation columns (season-level averages across all weeks)
        mean_sim_cols = []
        for col in [
            "avg_seed",
            "p_playoffs",
            "p_bye",
            "p_semis",
            "p_final",
            "p_champ",
            "exp_final_wins",
            "exp_final_pf",
        ]:
            if has(col):
                mean_sim_cols.append(col)
        if mean_sim_cols:
            insert_cols.extend([f"mean_{col}" for col in mean_sim_cols])

        # Build snapshot SQL fragments
        snapshot_select = ""
        for col in snapshot_cols:
            snapshot_select += f",\n        ARG_MAX({col}, CASE WHEN week <= {last_reg_week} THEN week END) AS {col}"

        # playoff_seed_to_date: prefer frozen final seeds. If unavailable,
        # restrict to real regular-season rows; playoff/consolation/bye rows can
        # carry stale or reseeded values that produce duplicate season seeds.
        if has_playoff_seed:
            snapshot_select += _build_seed_snapshot_select(
                has_final_playoff_seed=has("final_playoff_seed"),
                has_is_playoffs=has("is_playoffs"),
                has_is_consolation=has("is_consolation"),
                has_is_bye_week=has("is_bye_week"),
            )

        # Build mean simulation SQL fragments
        mean_sim_select = ""
        for col in mean_sim_cols:
            mean_sim_select += f",\n        AVG(CAST({col} AS DOUBLE)) AS mean_{col}"

        # Build median-aware wins/losses expressions for this year
        yr_is_median = yr in median_years
        wins_expr = build_wins_sql(use_median=yr_is_median)
        losses_expr = build_losses_sql(use_median=yr_is_median)

        # Build aggregation SQL using a CTE so derived columns can reference base aliases
        agg_sql = f"""
        WITH base AS (
        SELECT
            MAX(manager) AS manager,
            {yr} AS year,
            franchise_id,

            -- Basic stats
            COUNT(*) AS games,
            SUM({wins_expr}) AS wins,
            SUM({losses_expr}) AS losses,
            SUM(COALESCE(CAST({'tie' if has('tie') else '0'} AS INT), 0)) AS ties,
            SUM(team_points) AS total_team_points,
            SUM(opponent_points) AS total_opponent_points,
            AVG(team_points) AS avg_team_points,
            AVG(opponent_points) AS avg_opponent_points,
            AVG(team_points) - AVG(opponent_points) AS avg_margin,
            MAX(team_points) AS max_team_points,
            MIN(CASE WHEN team_points > 0 THEN team_points END) AS min_team_points,
            STDDEV_SAMP(team_points) AS std_dev_team_points,

            -- GPA
            AVG({'CAST(gpa AS DOUBLE)' if has('gpa') else 'NULL'}) AS avg_gpa,

            -- Median (regular season only — matches build_wins_sql which excludes playoffs)
            SUM(CASE WHEN COALESCE({'above_league_median' if has('above_league_median') else '0'}, 0) = 1 AND COALESCE(CAST(is_playoffs AS INT), 0) = 0 THEN 1 ELSE 0 END) AS above_league_median,
            SUM(CASE WHEN COALESCE({'below_league_median' if has('below_league_median') else '0'}, 0) = 1 AND COALESCE(CAST(is_playoffs AS INT), 0) = 0 THEN 1 ELSE 0 END) AS below_league_median,

            -- Close games (margin <= 5)
            SUM(CASE WHEN COALESCE({'close_margin' if has('close_margin') else '0'}, 0) = 1 THEN 1 ELSE 0 END) AS close_games,
            SUM(CASE WHEN COALESCE({'close_margin' if has('close_margin') else '0'}, 0) = 1 AND COALESCE(CAST(win AS INT), 0) = 1 THEN 1 ELSE 0 END) AS close_wins,
            SUM(CASE WHEN COALESCE({'close_margin' if has('close_margin') else '0'}, 0) = 1 AND COALESCE(CAST(loss AS INT), 0) = 1 THEN 1 ELSE 0 END) AS close_losses,

            -- Blowouts (margin > 20)
            SUM(CASE WHEN COALESCE(CAST(win AS INT), 0) = 1 AND margin > 20 THEN 1 ELSE 0 END) AS blowout_wins,
            SUM(CASE WHEN COALESCE(CAST(loss AS INT), 0) = 1 AND margin < -20 THEN 1 ELSE 0 END) AS blowout_losses,

            -- Streaks
            MAX({
                'win_streak'
                if has('win_streak')
                else 'winning_streak'
                if has('winning_streak')
                else '0'
            }) AS max_win_streak,
            MAX({
                'loss_streak'
                if has('loss_streak')
                else 'losing_streak'
                if has('losing_streak')
                else '0'
            }) AS max_loss_streak,

            -- Optimal lineup
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0 THEN 1 ELSE 0 END) AS optimal_games,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0 THEN team_points ELSE 0 END) AS optimal_actual_pts,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0 THEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) ELSE 0 END) AS optimal_ceiling_pts,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > opponent_points THEN 1 ELSE 0 END) AS optimal_wins,
            SUM(CASE WHEN COALESCE(CAST(win AS INT), 0) = 0
                      AND COALESCE(CAST({'tie' if has('tie') else '0'} AS INT), 0) = 0
                      AND COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > opponent_points
                 THEN 1 ELSE 0 END) AS optimal_missed_wins,
            SUM(CASE WHEN COALESCE(CAST(win AS INT), 0) = 1
                      AND COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0
                      AND (team_points / NULLIF({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '1'}, 0)) < 0.80
                 THEN 1 ELSE 0 END) AS optimal_lucky_wins,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0
                      AND COALESCE(CAST(win AS INT), 0) = 1 THEN 1 ELSE 0 END) AS optimal_wins_actual,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0
                      AND COALESCE(CAST(loss AS INT), 0) = 1 THEN 1 ELSE 0 END) AS optimal_losses_actual,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0
                      AND (COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > opponent_points) != (COALESCE(CAST(win AS INT), 0) = 1)
                 THEN 1 ELSE 0 END) AS optimal_outcome_changes,
            SUM(CASE WHEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) > 0
                 THEN COALESCE({'CAST(optimal_points AS DOUBLE)' if has('optimal_points') else '0'}, 0) - opponent_points ELSE 0 END) AS optimal_margin,

            -- Projected stats
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0 THEN 1 ELSE 0 END) AS proj_games,
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0 AND COALESCE(CAST(win AS INT), 0) = 1 THEN 1 ELSE 0 END) AS proj_wins,
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0 AND COALESCE(CAST(loss AS INT), 0) = 1 THEN 1 ELSE 0 END) AS proj_losses,
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0 THEN team_points ELSE 0 END) AS proj_total_team_points,
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0 THEN opponent_points ELSE 0 END) AS proj_total_opponent_points,
            SUM(COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0)) AS proj_total_proj,
            SUM(COALESCE({f'CAST({opp_proj_col} AS DOUBLE)' if opp_proj_col else '0'}, 0)) AS proj_opp_proj,
            SUM(CASE WHEN team_points > COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0)
                      AND COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0
                 THEN 1 ELSE 0 END) AS proj_above_proj,
            SUM(CASE WHEN team_points <= COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0)
                      AND COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0
                 THEN 1 ELSE 0 END) AS proj_below_proj,
            SUM(CASE WHEN {'expected_spread IS NOT NULL' if has('expected_spread') else 'FALSE'}
                      AND (team_points - opponent_points) > COALESCE({'CAST(expected_spread AS DOUBLE)' if has('expected_spread') else '0'}, 0)
                 THEN 1 ELSE 0 END) AS proj_beat_spread,
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0 THEN margin ELSE 0 END) AS proj_margin_total,
            AVG(CASE WHEN {'expected_spread IS NOT NULL' if has('expected_spread') else 'FALSE'} THEN {'CAST(expected_spread AS DOUBLE)' if has('expected_spread') else '0'} END) AS proj_spread_avg,
            AVG(CASE WHEN {'expected_odds IS NOT NULL AND CAST(expected_odds AS DOUBLE) > 0.5' if has('expected_odds') else 'FALSE'} THEN 1.0 ELSE
                CASE WHEN {'expected_odds IS NOT NULL' if has('expected_odds') else 'FALSE'} THEN 0.0 END END) AS proj_favored_pct,
            AVG(CASE WHEN {'expected_odds IS NOT NULL' if has('expected_odds') else 'FALSE'} THEN {'CAST(expected_odds AS DOUBLE)' if has('expected_odds') else '0'} END) AS proj_avg_win_pct,
            SUM(CASE WHEN COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) >
                          COALESCE({f'CAST({opp_proj_col} AS DOUBLE)' if opp_proj_col else '0'}, 0)
                      AND COALESCE({f'CAST({proj_col} AS DOUBLE)' if proj_col else '0'}, 0) > 0
                 THEN 1 ELSE 0 END) AS proj_expected_wins,
            SUM(COALESCE({'CAST(underdog_wins AS INT)' if has('underdog_wins') else '0'}, 0)) AS proj_upset_wins,
            SUM(COALESCE({'CAST(favorite_losses AS INT)' if has('favorite_losses') else '0'}, 0)) AS proj_upset_losses,
            SUM(COALESCE({'CAST(proj_score_error AS DOUBLE)' if has('proj_score_error') else '0'}, 0)) AS proj_total_error,
            AVG(CASE WHEN {'abs_proj_score_error IS NOT NULL' if has('abs_proj_score_error') else 'FALSE'}
                 THEN {'CAST(abs_proj_score_error AS DOUBLE)' if has('abs_proj_score_error') else '0'} END) AS proj_avg_error,

            -- Playoff defaults (updated later via UPDATE)
            0 AS made_playoffs,
            0 AS is_champion,
            0 AS is_sacko,
            CAST(NULL AS VARCHAR) AS playoff_result

            -- Snapshot values (end-of-regular-season)
            {snapshot_select}

            -- Mean simulation values (season averages across all weeks)
            {mean_sim_select}

        FROM {central_table('matchup')}
        WHERE {league_db_filter(db_name)}
          AND year = {yr}
          AND week <= {last_reg_week}
          AND COALESCE(CAST(is_playoffs AS INT), 0) = 0
          AND COALESCE(CAST(is_consolation AS INT), 0) = 0
          AND manager IS NOT NULL AND opponent IS NOT NULL
          {'AND COALESCE(is_bye_week, 0) = 0' if has('is_bye_week') else ''}
          {'AND COALESCE(is_placeholder, 0) = 0' if has('is_placeholder') else ''}
          -- Exclude unplayed games (zero scores AND no win/loss/tie recorded)
          AND (
              COALESCE(team_points, 0) > 0
              OR COALESCE(opponent_points, 0) > 0
              OR COALESCE(CAST(win AS INT), 0) = 1
              OR COALESCE(CAST(loss AS INT), 0) = 1
              {'OR COALESCE(CAST(tie AS INT), 0) = 1' if has('tie') else ''}
          )
        GROUP BY franchise_id
        )
        SELECT
            '{db_name}' AS db_name,
            manager,
            year,
            franchise_id,
            games,
            wins,
            losses,
            ties,
            CASE WHEN games > 0 THEN (wins + 0.5 * ties) / games ELSE 0 END AS win_pct,
            total_team_points,
            total_opponent_points,
            avg_team_points,
            avg_opponent_points,
            avg_margin,
            max_team_points,
            min_team_points,
            std_dev_team_points,
            avg_gpa,
            above_league_median,
            below_league_median,
            close_games,
            close_wins,
            close_losses,
            CASE WHEN close_games > 0 THEN CAST(close_wins AS DOUBLE) / close_games ELSE 0 END AS close_win_pct,
            blowout_wins,
            blowout_losses,
            max_win_streak,
            max_loss_streak,
            optimal_games,
            optimal_actual_pts,
            optimal_ceiling_pts,
            (optimal_ceiling_pts - optimal_actual_pts) AS optimal_bench_pts,
            CASE WHEN optimal_ceiling_pts > 0 THEN (optimal_actual_pts / optimal_ceiling_pts) * 100 ELSE 0 END AS optimal_efficiency,
            optimal_wins,
            (optimal_games - optimal_wins) AS optimal_losses,
            optimal_missed_wins,
            optimal_lucky_wins,
            optimal_wins_actual,
            optimal_losses_actual,
            optimal_outcome_changes,
            optimal_margin,
            proj_games,
            proj_wins,
            proj_losses,
            proj_total_team_points,
            proj_total_opponent_points,
            proj_total_proj,
            proj_opp_proj,
            proj_above_proj,
            proj_below_proj,
            proj_beat_spread,
            proj_margin_total,
            proj_spread_avg,
            proj_favored_pct,
            proj_avg_win_pct,
            proj_expected_wins,
            proj_upset_wins,
            proj_upset_losses,
            (proj_upset_wins + proj_upset_losses) AS proj_total_upsets,
            proj_total_error,
            proj_avg_error,
            made_playoffs,
            is_champion,
            is_sacko,
            playoff_result
            {',' + ','.join(snapshot_cols) if snapshot_cols else ''}
            {',playoff_seed_to_date' if has_playoff_seed else ''}
            {',' + ','.join(f'mean_{col}' for col in mean_sim_cols) if mean_sim_cols else ''}
        FROM base
        ORDER BY year, manager
        """

        if dry_run:
            log(f"  [DRY-RUN] Would aggregate year {yr}")
            continue

        execute_scoped(
            conn,
            f"INSERT INTO {central_table('matchup_season')} ({aggregate_insert_columns('matchup_season', insert_cols)}) "
            f"{agg_sql}",
            db_name,
            label="matchup_season:insert",
        )

        # -- C. Playoff detection: UPDATE rows with playoff info
        # NULLIF handles empty strings in playoff_round/consolation_round
        pr_expr = "CAST(playoff_round AS VARCHAR)" if has("playoff_round") else "''"
        cr_expr = "CAST(consolation_round AS VARCHAR)" if has("consolation_round") else "''"
        pr_nullif = f"NULLIF(TRIM({pr_expr}), '')"
        cr_nullif = f"NULLIF(TRIM({cr_expr}), '')"
        champion_flag_expr = "COALESCE(CAST(champion AS INT), 0)" if has("champion") else "0"
        sacko_flag_expr = "COALESCE(CAST(sacko AS INT), 0)" if has("sacko") else "0"

        playoff_update_sql = f"""
        UPDATE {central_table('matchup_season')} AS ms SET
            made_playoffs = COALESCE(pdata.made_playoffs, 0),
            is_champion = COALESCE(pdata.is_champion, 0),
            is_sacko = COALESCE(pdata.is_sacko, 0),
            playoff_result = pdata.playoff_result
        FROM (
            SELECT
                MAX(manager) AS manager,
                franchise_id,
                MAX(CASE WHEN COALESCE(CAST({'is_playoffs' if has('is_playoffs') else '0'} AS INT), 0) = 1 THEN 1 ELSE 0 END) AS made_playoffs,
                MAX({champion_flag_expr}) AS is_champion,
                MAX({sacko_flag_expr}) AS is_sacko,
                -- playoff_result: pick last game that has a round name
                ARG_MAX(
                    CASE WHEN COALESCE({pr_nullif}, {cr_nullif}) IS NOT NULL
                         THEN (CASE WHEN COALESCE(CAST(win AS INT), 0) = 1 THEN 'Won ' ELSE 'Lost ' END)
                              || REPLACE(COALESCE({pr_nullif}, {cr_nullif}), '_', ' ')
                         ELSE NULL END,
                    week
                ) AS playoff_result
            FROM {central_table('matchup')}
            WHERE {league_db_filter(db_name)}
              AND year = {yr}
              AND (
                  week > {last_reg_week}
                  OR {champion_flag_expr} = 1
                  OR {sacko_flag_expr} = 1
              )
              AND manager IS NOT NULL
            GROUP BY franchise_id
        ) pdata
        WHERE ms.franchise_id = pdata.franchise_id
          AND ms.db_name = '{db_name}'
          AND ms.year = {yr}
        """

        try:
            execute_scoped(conn, playoff_update_sql, db_name, label="matchup_season:update")
        except Exception as e:
            log(f"  WARNING: Playoff update failed for {yr}: {e}")

        yr_count = conn.execute(
            f"SELECT COUNT(*) FROM {central_table('matchup_season')} WHERE db_name = '{db_name}' AND year = {yr}"
        ).fetchone()[0]
        total_rows += yr_count
        log(f"    -> {yr_count} rows for {yr}")

    log(f"  matchup_season: {total_rows} total rows")
    return total_rows


def aggregate_matchup_career(conn, db_name: str, dry_run: bool = False) -> int:
    """Build matchup_career table from matchup_season.

    One row per manager with career aggregates.
    """
    log("Building matchup_career table...")
    configure_table_catalog(conn)

    if dry_run:
        log("  [DRY-RUN] Would build matchup_career")
        return 0

    season_cols = get_available_columns(conn, db_name, "matchup_season")
    if not season_cols:
        log("  WARNING: matchup_season table not found")
        return 0

    has = lambda col: col in season_cols
    if not has("franchise_id"):
        log("  WARNING: franchise_id is required in matchup_season; skipping matchup_career aggregation")
        return 0

    # Build optional column selections
    opt_cols = []
    opt_insert_cols = []

    # Franchise info — franchise_id is the GROUP BY key when available,
    # so it doesn't need to be aggregated; just include it as-is.
    if has("franchise_id"):
        opt_cols.append("franchise_id")
        opt_insert_cols.append("franchise_id")

    # Summed columns (source name from matchup_season = alias in matchup_career)
    sum_cols = [
        ("above_league_median", "above_league_median"),
        ("below_league_median", "below_league_median"),
        ("close_games", "close_games"),
        ("close_wins", "close_wins"),
        ("close_losses", "close_losses"),
        ("blowout_wins", "blowout_wins"),
        ("blowout_losses", "blowout_losses"),
        ("optimal_games", "optimal_games"),
        ("optimal_actual_pts", "optimal_actual_pts"),
        ("optimal_ceiling_pts", "optimal_ceiling_pts"),
        ("optimal_wins", "optimal_wins"),
        ("optimal_missed_wins", "optimal_missed_wins"),
        ("optimal_lucky_wins", "optimal_lucky_wins"),
        ("optimal_wins_actual", "optimal_wins_actual"),
        ("optimal_losses_actual", "optimal_losses_actual"),
        ("optimal_outcome_changes", "optimal_outcome_changes"),
        ("optimal_margin", "optimal_margin"),
        ("proj_games", "proj_games"),
        ("proj_wins", "proj_wins"),
        ("proj_losses", "proj_losses"),
        ("proj_total_team_points", "proj_total_team_points"),
        ("proj_total_opponent_points", "proj_total_opponent_points"),
        ("proj_total_proj", "proj_total_proj"),
        ("proj_opp_proj", "proj_opp_proj"),
        ("proj_above_proj", "proj_above_proj"),
        ("proj_below_proj", "proj_below_proj"),
        ("proj_beat_spread", "proj_beat_spread"),
        ("proj_margin_total", "proj_margin_total"),
        ("proj_upset_wins", "proj_upset_wins"),
        ("proj_upset_losses", "proj_upset_losses"),
        ("proj_total_error", "proj_total_error"),
        ("proj_expected_wins", "proj_expected_wins"),
    ]
    for src, alias in sum_cols:
        if has(src):
            opt_cols.append(f"SUM({src}) AS {alias}")
            opt_insert_cols.append(alias)

    # Max columns
    max_cols = [("max_win_streak", "max_win_streak"), ("max_loss_streak", "max_loss_streak")]
    for src, alias in max_cols:
        if has(src):
            opt_cols.append(f"MAX({src}) AS {alias}")
            opt_insert_cols.append(alias)

    # Best/worst across seasons
    opt_cols.append("MAX(max_team_points) AS max_team_points")
    opt_cols.append("MIN(CASE WHEN min_team_points > 0 THEN min_team_points END) AS min_team_points")
    opt_insert_cols.extend(["max_team_points", "min_team_points"])

    # Average columns (ratings — from snapshot columns in matchup_season)
    avg_cols = [
        ("power_rating", "avg_power_rating"),
        ("avg_seed", "avg_avg_seed"),
        ("p_playoffs", "avg_p_playoffs"),
        ("p_bye", "avg_p_bye"),
        ("p_semis", "avg_p_semis"),
        ("p_final", "avg_p_final"),
        ("p_champ", "avg_p_champ"),
        ("exp_final_wins", "avg_exp_wins"),
    ]
    for src, alias in avg_cols:
        if has(src):
            opt_cols.append(f"AVG(CASE WHEN {src} IS NOT NULL THEN {src} END) AS {alias}")
            opt_insert_cols.append(alias)

    # Playoff counts
    if has("made_playoffs"):
        opt_cols.append("SUM(CASE WHEN made_playoffs = 1 THEN 1 ELSE 0 END) AS playoff_seasons")
        opt_insert_cols.append("playoff_seasons")
    if has("is_champion"):
        opt_cols.append("SUM(CASE WHEN is_champion = 1 THEN 1 ELSE 0 END) AS champion_seasons")
        opt_insert_cols.append("champion_seasons")
    if has("is_sacko"):
        opt_cols.append("SUM(CASE WHEN is_sacko = 1 THEN 1 ELSE 0 END) AS sacko_seasons")
        opt_insert_cols.append("sacko_seasons")

    # GPA
    if has("avg_gpa"):
        opt_cols.append("AVG(CASE WHEN avg_gpa > 0 THEN avg_gpa END) AS avg_gpa")
        opt_insert_cols.append("avg_gpa")

    # Derived columns (computed from sums in a CTE outer SELECT)
    # close_win_pct
    if has("close_games") and has("close_wins"):
        opt_cols.append(
            "CASE WHEN SUM(close_games) > 0 THEN CAST(SUM(close_wins) AS DOUBLE) / SUM(close_games) ELSE 0 END AS close_win_pct"
        )
        opt_insert_cols.append("close_win_pct")
    # optimal_bench_pts, optimal_efficiency, optimal_losses
    if has("optimal_ceiling_pts") and has("optimal_actual_pts"):
        opt_cols.append("(SUM(optimal_ceiling_pts) - SUM(optimal_actual_pts)) AS optimal_bench_pts")
        opt_insert_cols.append("optimal_bench_pts")
        opt_cols.append(
            "CASE WHEN SUM(optimal_ceiling_pts) > 0 THEN (SUM(optimal_actual_pts) / SUM(optimal_ceiling_pts)) * 100 ELSE 0 END AS optimal_efficiency"
        )
        opt_insert_cols.append("optimal_efficiency")
    if has("optimal_games") and has("optimal_wins"):
        opt_cols.append("(SUM(optimal_games) - SUM(optimal_wins)) AS optimal_losses")
        opt_insert_cols.append("optimal_losses")
    # proj_total_upsets
    if has("proj_upset_wins") and has("proj_upset_losses"):
        opt_cols.append("(SUM(proj_upset_wins) + SUM(proj_upset_losses)) AS proj_total_upsets")
        opt_insert_cols.append("proj_total_upsets")

    opt_cols_sql = (",\n            " + ",\n            ".join(opt_cols)) if opt_cols else ""

    ensure_aggregate_table(conn, get_active_catalog(), "matchup_career")
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('matchup_career')} WHERE db_name = '{db_name}'",
        db_name,
        label="matchup_career:delete",
    )

    career_sql = f"""
    SELECT
        '{db_name}' AS db_name,
        ARG_MAX(manager, year) AS manager,
        COUNT(DISTINCT year) AS seasons,
        SUM(games) AS games,
        SUM(wins) AS wins,
        SUM(losses) AS losses,
        SUM(ties) AS ties,
        SUM(total_team_points) AS total_team_points,
        SUM(total_opponent_points) AS total_opponent_points,
        CASE WHEN SUM(games) > 0 THEN SUM(total_team_points) / SUM(games) ELSE 0 END AS avg_team_points,
        CASE WHEN SUM(games) > 0 THEN SUM(total_opponent_points) / SUM(games) ELSE 0 END AS avg_opponent_points,
        CASE WHEN SUM(games) > 0 THEN SUM(total_team_points) / SUM(games) - SUM(total_opponent_points) / SUM(games) ELSE 0 END AS avg_margin,
        CASE WHEN SUM(games) > 0 THEN (SUM(wins) + 0.5 * SUM(ties)) / SUM(games) * 100 ELSE 0 END AS win_pct
        {opt_cols_sql}
    FROM {central_table('matchup_season')}
    WHERE {league_db_filter(db_name)}
    GROUP BY franchise_id
    ORDER BY manager
    """

    insert_cols = [
        "db_name",
        "manager",
        "seasons",
        "games",
        "wins",
        "losses",
        "ties",
        "total_team_points",
        "total_opponent_points",
        "avg_team_points",
        "avg_opponent_points",
        "avg_margin",
        "win_pct",
        *opt_insert_cols,
    ]
    execute_scoped(
        conn,
        f"INSERT INTO {central_table('matchup_career')} ({aggregate_insert_columns('matchup_career', insert_cols)}) "
        f"{career_sql}",
        db_name,
        label="matchup_career:insert",
    )

    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('matchup_career')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  matchup_career: {count} rows")
    return count


def aggregate_matchup_h2h(conn, db_name: str, dry_run: bool = False) -> tuple:
    """Build matchup_h2h_season and matchup_h2h_career tables.

    H2H season: one row per (manager, opponent, year)
    H2H career: one row per (manager, opponent) with streak info
    """
    log("Building matchup H2H tables...")
    configure_table_catalog(conn)

    existing_cols = get_available_columns(conn, db_name, "matchup")
    if not existing_cols:
        log("  WARNING: matchup table not found")
        return (0, 0)

    has = lambda col: col in existing_cols
    if not (has("franchise_id") and has("opponent_franchise_id")):
        log("  WARNING: franchise_id and opponent_franchise_id are required in matchup; skipping H2H aggregation")
        return (0, 0)
    year_boundaries = get_playoff_start_weeks(conn, db_name)

    # Get years
    years = [
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT year FROM {central_table('matchup')} WHERE {league_db_filter(db_name)} AND year IS NOT NULL ORDER BY year"
        ).fetchall()
    ]

    if not years:
        log("  No years found")
        return (0, 0)

    if dry_run:
        log("  [DRY-RUN] Would build H2H tables")
        return (0, 0)

    # Build base WHERE
    bye_filter = "AND COALESCE(is_bye_week, 0) = 0" if has("is_bye_week") else ""
    placeholder_filter = "AND COALESCE(is_placeholder, 0) = 0" if has("is_placeholder") else ""
    tie_col = "tie" if has("tie") else "0"

    # H2H Season
    ensure_aggregate_table(conn, get_active_catalog(), "matchup_h2h_season")
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('matchup_h2h_season')} WHERE db_name = '{db_name}'",
        db_name,
        label="matchup_h2h_season:delete",
    )

    # Build optional franchise columns for H2H
    # When franchise_id is available, use it as GROUP BY key; manager/opponent become MAX()
    use_fid_groupby = has("franchise_id") and has("opponent_franchise_id")
    h2h_fid_cols = []
    if has("franchise_id"):
        h2h_fid_cols.append("franchise_id")
    if has("opponent_franchise_id"):
        h2h_fid_cols.append("opponent_franchise_id")

    season_total = 0
    for yr in years:
        last_reg_week = year_boundaries.get(yr, 14)

        h2h_season_sql = f"""
        SELECT
            '{db_name}' AS db_name,
            MAX(manager) AS manager,
            MAX(opponent) AS opponent,
            franchise_id, opponent_franchise_id,
            {yr} AS year,
            COUNT(*) AS games,
            SUM(COALESCE(CAST(win AS INT), 0)) AS wins,
            SUM(COALESCE(CAST(loss AS INT), 0)) AS losses,
            SUM(COALESCE(CAST({tie_col} AS INT), 0)) AS ties,
            SUM(team_points) AS total_team_points,
            SUM(margin) AS total_margin,
            MAX(team_points) AS max_team_points,
            MIN(CASE WHEN team_points > 0 THEN team_points END) AS min_team_points
        FROM {central_table('matchup')}
        WHERE {league_db_filter(db_name)}
          AND year = {yr}
          AND week <= {last_reg_week}
          AND manager IS NOT NULL AND opponent IS NOT NULL
          AND franchise_id IS NOT NULL AND opponent_franchise_id IS NOT NULL
          {bye_filter}
          {placeholder_filter}
        GROUP BY franchise_id, opponent_franchise_id
        ORDER BY year, manager
        """

        h2h_season_insert_cols = [
            "db_name",
            "manager",
            "opponent",
            *h2h_fid_cols,
            "year",
            "games",
            "wins",
            "losses",
            "ties",
            "total_team_points",
            "total_margin",
            "max_team_points",
            "min_team_points",
        ]
        execute_scoped(
            conn,
            f"INSERT INTO {central_table('matchup_h2h_season')} "
            f"({aggregate_insert_columns('matchup_h2h_season', h2h_season_insert_cols)}) "
            f"{h2h_season_sql}",
            db_name,
            label="matchup_h2h_season:insert",
        )

        yr_count = conn.execute(
            f"SELECT COUNT(*) FROM {central_table('matchup_h2h_season')} WHERE db_name = '{db_name}' AND year = {yr}"
        ).fetchone()[0]
        season_total += yr_count

    log(f"  matchup_h2h_season: {season_total} rows")

    # H2H Career (with streaks via ARRAY_AGG and recent game via ARG_MAX)
    ensure_aggregate_table(conn, get_active_catalog(), "matchup_h2h_career")
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('matchup_h2h_career')} WHERE db_name = '{db_name}'",
        db_name,
        label="matchup_h2h_career:delete",
    )

    # Use all regular-season weeks across all years
    year_filters = " OR ".join([f"(year = {yr} AND week <= {year_boundaries.get(yr, 14)})" for yr in years])

    h2h_career_sql = f"""
    SELECT
        '{db_name}' AS db_name,
        ARG_MAX(manager, year * 100 + week) AS manager,
        ARG_MAX(opponent, year * 100 + week) AS opponent,
        franchise_id, opponent_franchise_id,
        COUNT(*) AS games,
        SUM(COALESCE(CAST(win AS INT), 0)) AS wins,
        SUM(COALESCE(CAST(loss AS INT), 0)) AS losses,
        SUM(COALESCE(CAST({tie_col} AS INT), 0)) AS ties,
        SUM(team_points) AS total_team_points,
        SUM(margin) AS total_margin,
        MAX(team_points) AS max_team_points,
        MIN(CASE WHEN team_points > 0 THEN team_points END) AS min_team_points,
        ARG_MAX(CAST(win AS INT), year * 100 + week) AS recent_win,
        ARG_MAX(team_points, year * 100 + week) AS recent_team_points,
        ARG_MAX(year, year * 100 + week) AS recent_year,
        ARG_MAX(week, year * 100 + week) AS recent_week,
        ARRAY_AGG(COALESCE(CAST(win AS INT), 0) ORDER BY year, week) AS results
    FROM {central_table('matchup')}
    WHERE {league_db_filter(db_name)}
      AND manager IS NOT NULL AND opponent IS NOT NULL
      AND franchise_id IS NOT NULL AND opponent_franchise_id IS NOT NULL
      {bye_filter}
      {placeholder_filter}
      AND ({year_filters})
    GROUP BY franchise_id, opponent_franchise_id
    ORDER BY manager, opponent
    """

    h2h_career_insert_cols = [
        "db_name",
        "manager",
        "opponent",
        *h2h_fid_cols,
        "games",
        "wins",
        "losses",
        "ties",
        "total_team_points",
        "total_margin",
        "max_team_points",
        "min_team_points",
        "recent_win",
        "recent_team_points",
        "recent_year",
        "recent_week",
        "results",
    ]
    execute_scoped(
        conn,
        f"INSERT INTO {central_table('matchup_h2h_career')} "
        f"({aggregate_insert_columns('matchup_h2h_career', h2h_career_insert_cols)}) "
        f"{h2h_career_sql}",
        db_name,
        label="matchup_h2h_career:insert",
    )
    career_count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('matchup_h2h_career')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  matchup_h2h_career: {career_count} rows")

    return (season_total, career_count)


def aggregate_luck_all_play(conn, db_name: str, dry_run: bool = False) -> tuple[int, int, int, int]:
    """Build centralized all-play and schedule-swap luck tables.

    These are the Luck-page matrices:
    - all_play / h2h_season: "if you played this manager every week"
    - schedule_swap / schedule_swap_season: "if you had this manager's schedule"
    """
    log("Building luck all-play tables...")
    configure_table_catalog(conn)

    existing_cols = get_available_columns(conn, db_name, "matchup")
    if not existing_cols:
        log("  WARNING: matchup table has no columns or doesn't exist")
        return (0, 0, 0, 0)

    has = lambda col: col in existing_cols
    required = {"year", "week", "franchise_id", "opponent_franchise_id", "team_points", "opponent_points"}
    missing = sorted(required - existing_cols)
    if missing:
        log(f"  WARNING: matchup table missing required luck columns: {missing}")
        return (0, 0, 0, 0)

    years = [
        r[0]
        for r in conn.execute(
            f"SELECT DISTINCT year FROM {central_table('matchup')} WHERE {league_db_filter(db_name)} AND year IS NOT NULL ORDER BY year"
        ).fetchall()
    ]
    if not years:
        log("  No years found in matchup table")
        return (0, 0, 0, 0)

    year_boundaries = get_playoff_start_weeks(conn, db_name)
    for yr in years:
        if yr not in year_boundaries:
            try:
                result = conn.execute(f"""
                    SELECT MAX(week) FROM (
                        SELECT week, COUNT(DISTINCT franchise_id) AS manager_count
                        FROM {central_table('matchup')}
                        WHERE {league_db_filter(db_name)}
                          AND year = {yr}
                          AND COALESCE(CAST({'is_playoffs' if has('is_playoffs') else '0'} AS INT), 0) = 0
                          AND COALESCE(CAST({'is_consolation' if has('is_consolation') else '0'} AS INT), 0) = 0
                          AND franchise_id IS NOT NULL
                        GROUP BY week
                    ) sub
                """).fetchone()
                year_boundaries[yr] = int(result[0]) if result and result[0] else 14
            except Exception:
                year_boundaries[yr] = 14

    year_filter = " OR ".join([f"(a.year = {yr} AND a.week <= {year_boundaries.get(yr, 14)})" for yr in years])
    if not year_filter:
        return (0, 0, 0, 0)

    def regular_filters(alias: str) -> str:
        pieces = []
        if has("is_playoffs"):
            pieces.append(f"COALESCE(CAST({alias}.is_playoffs AS INT), 0) = 0")
        if has("is_consolation"):
            pieces.append(f"COALESCE(CAST({alias}.is_consolation AS INT), 0) = 0")
        if has("is_bye_week"):
            pieces.append(f"COALESCE(CAST({alias}.is_bye_week AS INT), 0) = 0")
        elif has("is_bye"):
            pieces.append(f"COALESCE(CAST({alias}.is_bye AS INT), 0) = 0")
        if has("is_placeholder"):
            pieces.append(f"COALESCE(CAST({alias}.is_placeholder AS INT), 0) = 0")
        return "\n          AND " + "\n          AND ".join(pieces) if pieces else ""

    if dry_run:
        log("  [DRY-RUN] Would build all_play, h2h_season, schedule_swap, schedule_swap_season")
        return (0, 0, 0, 0)

    for table_name in ("all_play", "h2h_season", "schedule_swap", "schedule_swap_season"):
        ensure_aggregate_table(conn, get_active_catalog(), table_name)
        execute_scoped(
            conn,
            f"DELETE FROM {central_table(table_name)} WHERE db_name = '{db_name}'",
            db_name,
            label=f"{table_name}:delete",
        )

    all_play_cols = [
        "db_name",
        "year",
        "week",
        "franchise_id",
        "opponent_franchise_id",
        "result",
        "points",
        "opponent_points",
    ]
    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table('all_play')} ({aggregate_insert_columns('all_play', all_play_cols)})
        SELECT
            '{db_name}' AS db_name,
            a.year,
            a.week,
            a.franchise_id,
            b.franchise_id AS opponent_franchise_id,
            CASE
                WHEN a.team_points > b.team_points THEN 'W'
                WHEN a.team_points < b.team_points THEN 'L'
                ELSE 'T'
            END AS result,
            a.team_points AS points,
            b.team_points AS opponent_points
        FROM {central_table('matchup')} a
        CROSS JOIN {central_table('matchup')} b
        WHERE {league_db_filter(db_name, 'a')}
          AND {league_db_filter(db_name, 'b')}
          AND a.year = b.year
          AND a.week = b.week
          AND ({year_filter})
          AND a.franchise_id IS NOT NULL
          AND b.franchise_id IS NOT NULL
          AND a.franchise_id != b.franchise_id
          AND a.team_points IS NOT NULL
          AND b.team_points IS NOT NULL
          {regular_filters('a')}
          {regular_filters('b')}
        """,
        db_name,
        label="all_play:insert",
    )
    all_play_count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('all_play')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]

    h2h_cols = ["db_name", "franchise_id", "opponent_franchise_id", "year", "wins", "losses", "ties", "games"]
    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table('h2h_season')} ({aggregate_insert_columns('h2h_season', h2h_cols)})
        SELECT
            db_name,
            franchise_id,
            opponent_franchise_id,
            year,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties,
            COUNT(*) AS games
        FROM {central_table('all_play')}
        WHERE {league_db_filter(db_name)}
        GROUP BY db_name, franchise_id, opponent_franchise_id, year
        """,
        db_name,
        label="h2h_season:insert",
    )
    h2h_count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('h2h_season')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]

    swap_cols = [
        "db_name",
        "year",
        "week",
        "franchise_id",
        "schedule_of_franchise_id",
        "result",
        "my_points",
        "their_opponent_points",
    ]
    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table('schedule_swap')} ({aggregate_insert_columns('schedule_swap', swap_cols)})
        SELECT
            '{db_name}' AS db_name,
            a.year,
            a.week,
            a.franchise_id,
            b.franchise_id AS schedule_of_franchise_id,
            CASE
                WHEN a.team_points > b.opponent_points THEN 'W'
                WHEN a.team_points < b.opponent_points THEN 'L'
                ELSE 'T'
            END AS result,
            a.team_points AS my_points,
            b.opponent_points AS their_opponent_points
        FROM {central_table('matchup')} a
        CROSS JOIN {central_table('matchup')} b
        WHERE {league_db_filter(db_name, 'a')}
          AND {league_db_filter(db_name, 'b')}
          AND a.year = b.year
          AND a.week = b.week
          AND ({year_filter})
          AND a.franchise_id IS NOT NULL
          AND b.franchise_id IS NOT NULL
          AND (a.franchise_id = b.franchise_id OR b.opponent_franchise_id != a.franchise_id)
          AND a.team_points IS NOT NULL
          AND b.opponent_points IS NOT NULL
          {regular_filters('a')}
          {regular_filters('b')}
        """,
        db_name,
        label="schedule_swap:insert",
    )
    swap_count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('schedule_swap')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]

    swap_season_cols = [
        "db_name",
        "franchise_id",
        "schedule_of_franchise_id",
        "year",
        "wins",
        "losses",
        "ties",
        "games",
    ]
    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table('schedule_swap_season')} ({aggregate_insert_columns('schedule_swap_season', swap_season_cols)})
        SELECT
            db_name,
            franchise_id,
            schedule_of_franchise_id,
            year,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties,
            COUNT(*) AS games
        FROM {central_table('schedule_swap')}
        WHERE {league_db_filter(db_name)}
        GROUP BY db_name, franchise_id, schedule_of_franchise_id, year
        """,
        db_name,
        label="schedule_swap_season:insert",
    )
    swap_season_count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('schedule_swap_season')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]

    log(
        f"  all_play={all_play_count}, h2h_season={h2h_count}, "
        f"schedule_swap={swap_count}, schedule_swap_season={swap_season_count}"
    )
    return (all_play_count, h2h_count, swap_count, swap_season_count)


def main():
    parser = argparse.ArgumentParser(description="Aggregate matchup context to season/career tables")
    parser.add_argument("--context", help="Path to league context JSON file")
    parser.add_argument("--db", help="Database name (alternative to --context)")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )
    args = parser.parse_args()

    # Determine database name
    if args.db:
        db_name = args.db
    elif args.context:
        try:
            with open(args.context) as f:
                ctx_json = json.load(f)
            # Prefer pre-resolved db name from workflow, fallback to sanitize
            db_name = ctx_json.get("database_name") or ctx_json.get("motherduck_db_name", "")
            if not db_name:
                league_name = ctx_json.get("league_name", "")
                if not league_name:
                    raise ValueError("No league_name or database_name in context JSON")
                from multi_league.core.db_utils import sanitize_database_name

                db_name = sanitize_database_name(league_name)
        except Exception as e:
            log(f"FAIL: Could not read context file: {e}")
            sys.exit(1)
    else:
        log("FAIL: Must provide --context or --db")
        sys.exit(1)

    log(f"Aggregating matchup context for database: {db_name}")

    conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
    # Bind the shared aggregation catalog to this connection so
    # central_table('matchup') resolves to the local catalog when running
    # against a per-league DuckDB file on the GH runner.
    configure_table_catalog(conn)

    try:
        season_count = aggregate_matchup_season(conn, db_name, args.dry_run)
        career_count = aggregate_matchup_career(conn, db_name, args.dry_run)
        h2h_season, h2h_career = aggregate_matchup_h2h(conn, db_name, args.dry_run)
        all_play, luck_h2h_season, schedule_swap, schedule_swap_season = aggregate_luck_all_play(
            conn, db_name, args.dry_run
        )

        log(
            f"DONE: matchup_season={season_count}, matchup_career={career_count}, "
            f"h2h_season={h2h_season}, h2h_career={h2h_career}, "
            f"all_play={all_play}, luck_h2h_season={luck_h2h_season}, "
            f"schedule_swap={schedule_swap}, schedule_swap_season={schedule_swap_season}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
