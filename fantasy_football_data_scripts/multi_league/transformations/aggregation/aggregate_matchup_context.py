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
from multi_league.core.aggregate_ddl import (
    MATCHUP_SEASON_COLUMN_TYPES,
    aggregate_insert_columns,
    ensure_aggregate_table,
)
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


def aggregate_matchup_season(conn, db_name: str, dry_run: bool = False, *, year: int | None = None) -> int:
    """Build matchup_season table from weekly matchup data.

    One row per (manager, year) with:
    - Aggregated stats from regular-season weeks
    - End-of-season snapshot values from the last regular-season week
    - Playoff detection from post-regular-season rows
    """
    log("Building matchup_season table...")
    season_scope = league_db_filter(db_name, year=year)
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
            f"SELECT DISTINCT year FROM {central_table('matchup')} WHERE {season_scope} AND year IS NOT NULL ORDER BY year"
        ).fetchall()
    ]

    if not years:
        log("  No years found in matchup table")
        if year is not None and not dry_run:
            ensure_aggregate_table(conn, get_active_catalog(), "matchup_season")
            execute_scoped(
                conn,
                f"DELETE FROM {central_table('matchup_season')} WHERE {season_scope}",
                db_name,
                label="matchup_season:delete",
            )
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
            f"DELETE FROM {central_table('matchup_season')} WHERE {season_scope}",
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


def rebuild_matchup_season_fleet(conn, *, target_table: str) -> dict[str, int]:
    """Rebuild the fleet-wide matchup-season rollup in one set-based pass.

    This is a storage-recovery helper for a replaceable derived table.  It
    reads only canonical ``matchup`` and ``league_settings`` facts and writes
    only the caller-provided empty staging table.  The caller owns the atomic
    canonical-table swap after these receipts pass.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_table):
        raise ValueError("invalid matchup_season staging table name")
    configure_table_catalog(conn)
    existing_cols = get_available_columns(conn, "", "matchup")
    required = {
        "db_name", "manager", "franchise_id", "year", "week", "opponent",
        "team_points", "opponent_points", "win", "loss", "is_playoffs",
        "is_consolation",
    }
    missing = sorted(required - existing_cols)
    if missing:
        raise ValueError(f"matchup source is missing required columns: {missing}")

    target_ref = f'public."{target_table}"'
    target_cols = {
        row[0] for row in conn.execute(f"DESCRIBE {target_ref}").fetchall()
    }
    expected_cols = set(MATCHUP_SEASON_COLUMN_TYPES)
    if target_cols != expected_cols:
        raise ValueError(
            "matchup_season staging schema mismatch: "
            f"missing={sorted(expected_cols - target_cols)}, "
            f"extra={sorted(target_cols - expected_cols)}"
        )
    if conn.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0]:
        raise ValueError("matchup_season staging table must be empty")

    has = existing_cols.__contains__
    int_col = lambda col: f"COALESCE(CAST(m.{col} AS INT), 0)" if has(col) else "0"
    dbl_col = lambda col: f"COALESCE(CAST(m.{col} AS DOUBLE), 0)" if has(col) else "0"
    raw_col = lambda col, default="NULL": f"m.{col}" if has(col) else default

    proj_col = "team_projected_points" if has("team_projected_points") else "manager_proj_score" if has("manager_proj_score") else None
    opp_proj_col = "opponent_projected_points" if has("opponent_projected_points") else "opponent_proj_score" if has("opponent_proj_score") else None
    proj = f"COALESCE(CAST(m.{proj_col} AS DOUBLE), 0)" if proj_col else "0"
    opp_proj = f"COALESCE(CAST(m.{opp_proj_col} AS DOUBLE), 0)" if opp_proj_col else "0"
    optimal = dbl_col("optimal_points")
    tie = int_col("tie")
    is_playoffs = int_col("is_playoffs")
    above_median = int_col("above_league_median")
    below_median = int_col("below_league_median")

    snapshot_cols = [
        col
        for col in (*SCHEDULE_LUCK_SIM_COLUMNS, *SOS_LUCK_SIM_COLUMNS, *PLAYOFF_SIM_COLUMNS,
                    "wins_to_date", "losses_to_date", "ties_to_date", "points_scored_to_date")
        if has(col)
    ]
    mean_cols = [
        col for col in (
            "avg_seed", "p_playoffs", "p_bye", "p_semis", "p_final",
            "p_champ", "exp_final_wins", "exp_final_pf",
        ) if has(col)
    ]

    base_exprs = {
        "manager": "MAX(m.manager)",
        "games": "COUNT(*)",
        "wins": f"SUM({int_col('win')} + CASE WHEN m.uses_median THEN {above_median} ELSE 0 END)",
        "losses": f"SUM({int_col('loss')} + CASE WHEN m.uses_median THEN {below_median} ELSE 0 END)",
        "ties": f"SUM({tie})",
        "total_team_points": "SUM(m.team_points)",
        "total_opponent_points": "SUM(m.opponent_points)",
        "avg_team_points": "AVG(m.team_points)",
        "avg_opponent_points": "AVG(m.opponent_points)",
        "avg_margin": "AVG(m.team_points) - AVG(m.opponent_points)",
        "max_team_points": "MAX(m.team_points)",
        "min_team_points": "MIN(CASE WHEN m.team_points > 0 THEN m.team_points END)",
        "std_dev_team_points": "STDDEV_SAMP(m.team_points)",
        "avg_gpa": f"AVG(CAST({raw_col('gpa')} AS DOUBLE))" if has("gpa") else "CAST(NULL AS DOUBLE)",
        "above_league_median": f"SUM(CASE WHEN {above_median} = 1 AND {is_playoffs} = 0 THEN 1 ELSE 0 END)",
        "below_league_median": f"SUM(CASE WHEN {below_median} = 1 AND {is_playoffs} = 0 THEN 1 ELSE 0 END)",
        "close_games": f"SUM(CASE WHEN {int_col('close_margin')} = 1 THEN 1 ELSE 0 END)",
        "close_wins": f"SUM(CASE WHEN {int_col('close_margin')} = 1 AND {int_col('win')} = 1 THEN 1 ELSE 0 END)",
        "close_losses": f"SUM(CASE WHEN {int_col('close_margin')} = 1 AND {int_col('loss')} = 1 THEN 1 ELSE 0 END)",
        "blowout_wins": f"SUM(CASE WHEN {int_col('win')} = 1 AND m.margin > 20 THEN 1 ELSE 0 END)" if has("margin") else "0",
        "blowout_losses": f"SUM(CASE WHEN {int_col('loss')} = 1 AND m.margin < -20 THEN 1 ELSE 0 END)" if has("margin") else "0",
        "max_win_streak": f"MAX({raw_col('win_streak', raw_col('winning_streak', '0'))})",
        "max_loss_streak": f"MAX({raw_col('loss_streak', raw_col('losing_streak', '0'))})",
        "optimal_games": f"SUM(CASE WHEN {optimal} > 0 THEN 1 ELSE 0 END)",
        "optimal_actual_pts": f"SUM(CASE WHEN {optimal} > 0 THEN m.team_points ELSE 0 END)",
        "optimal_ceiling_pts": f"SUM(CASE WHEN {optimal} > 0 THEN {optimal} ELSE 0 END)",
        "optimal_wins": f"SUM(CASE WHEN {optimal} > m.opponent_points THEN 1 ELSE 0 END)",
        "optimal_missed_wins": f"SUM(CASE WHEN {int_col('win')} = 0 AND {tie} = 0 AND {optimal} > m.opponent_points THEN 1 ELSE 0 END)",
        "optimal_lucky_wins": f"SUM(CASE WHEN {int_col('win')} = 1 AND {optimal} > 0 AND (m.team_points / NULLIF({optimal}, 0)) < 0.80 THEN 1 ELSE 0 END)",
        "optimal_wins_actual": f"SUM(CASE WHEN {optimal} > 0 AND {int_col('win')} = 1 THEN 1 ELSE 0 END)",
        "optimal_losses_actual": f"SUM(CASE WHEN {optimal} > 0 AND {int_col('loss')} = 1 THEN 1 ELSE 0 END)",
        "optimal_outcome_changes": f"SUM(CASE WHEN {optimal} > 0 AND ({optimal} > m.opponent_points) != ({int_col('win')} = 1) THEN 1 ELSE 0 END)",
        "optimal_margin": f"SUM(CASE WHEN {optimal} > 0 THEN {optimal} - m.opponent_points ELSE 0 END)",
        "proj_games": f"SUM(CASE WHEN {proj} > 0 THEN 1 ELSE 0 END)",
        "proj_wins": f"SUM(CASE WHEN {proj} > 0 AND {int_col('win')} = 1 THEN 1 ELSE 0 END)",
        "proj_losses": f"SUM(CASE WHEN {proj} > 0 AND {int_col('loss')} = 1 THEN 1 ELSE 0 END)",
        "proj_total_team_points": f"SUM(CASE WHEN {proj} > 0 THEN m.team_points ELSE 0 END)",
        "proj_total_opponent_points": f"SUM(CASE WHEN {proj} > 0 THEN m.opponent_points ELSE 0 END)",
        "proj_total_proj": f"SUM({proj})",
        "proj_opp_proj": f"SUM({opp_proj})",
        "proj_above_proj": f"SUM(CASE WHEN m.team_points > {proj} AND {proj} > 0 THEN 1 ELSE 0 END)",
        "proj_below_proj": f"SUM(CASE WHEN m.team_points <= {proj} AND {proj} > 0 THEN 1 ELSE 0 END)",
        "proj_beat_spread": f"SUM(CASE WHEN {raw_col('expected_spread')} IS NOT NULL AND (m.team_points - m.opponent_points) > {dbl_col('expected_spread')} THEN 1 ELSE 0 END)" if has("expected_spread") else "0",
        "proj_margin_total": f"SUM(CASE WHEN {proj} > 0 THEN m.margin ELSE 0 END)" if has("margin") else "0",
        "proj_spread_avg": "AVG(CASE WHEN m.expected_spread IS NOT NULL THEN CAST(m.expected_spread AS DOUBLE) END)" if has("expected_spread") else "CAST(NULL AS DOUBLE)",
        "proj_favored_pct": "AVG(CASE WHEN m.expected_odds IS NOT NULL AND CAST(m.expected_odds AS DOUBLE) > 0.5 THEN 1.0 WHEN m.expected_odds IS NOT NULL THEN 0.0 END)" if has("expected_odds") else "CAST(NULL AS DOUBLE)",
        "proj_avg_win_pct": "AVG(CASE WHEN m.expected_odds IS NOT NULL THEN CAST(m.expected_odds AS DOUBLE) END)" if has("expected_odds") else "CAST(NULL AS DOUBLE)",
        "proj_expected_wins": f"SUM(CASE WHEN {proj} > {opp_proj} AND {proj} > 0 THEN 1 ELSE 0 END)",
        "proj_upset_wins": f"SUM({int_col('underdog_wins')})",
        "proj_upset_losses": f"SUM({int_col('favorite_losses')})",
        "proj_total_error": f"SUM({dbl_col('proj_score_error')})",
        "proj_avg_error": "AVG(CASE WHEN m.abs_proj_score_error IS NOT NULL THEN CAST(m.abs_proj_score_error AS DOUBLE) END)" if has("abs_proj_score_error") else "CAST(NULL AS DOUBLE)",
    }
    if has("final_playoff_seed"):
        base_exprs["_final_playoff_seed"] = "MAX(m.final_playoff_seed)"
    if has("playoff_seed_to_date"):
        snapshot_cols.append("playoff_seed_to_date")
    for col in mean_cols:
        base_exprs[f"mean_{col}"] = f"AVG(CAST(m.{col} AS DOUBLE))"

    base_select = ",\n                ".join(
        f"{expr} AS \"{column}\"" for column, expr in base_exprs.items()
    )
    insert_cols = ["db_name", "manager", "year", "franchise_id", *[c for c in base_exprs if c not in {"manager", "_final_playoff_seed"}]]
    insert_cols.extend(snapshot_cols)
    insert_cols.extend([
        "win_pct", "close_win_pct", "optimal_bench_pts", "optimal_efficiency",
        "optimal_losses", "proj_total_upsets", "made_playoffs", "is_champion",
        "is_sacko", "playoff_result",
    ])
    # Canonical order is not required by INSERT column lists, but deterministic
    # schema order makes the statement and tests easier to audit.
    insert_cols = [col for col in MATCHUP_SEASON_COLUMN_TYPES if col in set(insert_cols)]

    champion = int_col("champion")
    sacko = int_col("sacko")
    playoff_round = "NULLIF(TRIM(CAST(m.playoff_round AS VARCHAR)), '')" if has("playoff_round") else "NULL"
    consolation_round = "NULLIF(TRIM(CAST(m.consolation_round AS VARCHAR)), '')" if has("consolation_round") else "NULL"
    final_select = {
        "db_name": "a.db_name", "manager": "a.manager", "year": "a.year", "franchise_id": "a.franchise_id",
        **{col: f'a."{col}"' for col in base_exprs if col not in {"manager", "_final_playoff_seed"}},
        **{col: f's."{col}"' for col in snapshot_cols},
        "win_pct": "CASE WHEN a.games > 0 THEN (a.wins + 0.5 * a.ties) / a.games ELSE 0 END",
        "close_win_pct": "CASE WHEN a.close_games > 0 THEN CAST(a.close_wins AS DOUBLE) / a.close_games ELSE 0 END",
        "optimal_bench_pts": "a.optimal_ceiling_pts - a.optimal_actual_pts",
        "optimal_efficiency": "CASE WHEN a.optimal_ceiling_pts > 0 THEN (a.optimal_actual_pts / a.optimal_ceiling_pts) * 100 ELSE 0 END",
        "optimal_losses": "a.optimal_games - a.optimal_wins",
        "proj_total_upsets": "a.proj_upset_wins + a.proj_upset_losses",
        "made_playoffs": "COALESCE(p.made_playoffs, 0)",
        "is_champion": "COALESCE(p.is_champion, 0)",
        "is_sacko": "COALESCE(p.is_sacko, 0)",
        "playoff_result": "p.playoff_result",
    }
    if has("playoff_seed_to_date") and has("final_playoff_seed"):
        final_select["playoff_seed_to_date"] = 'COALESCE(a."_final_playoff_seed", s."playoff_seed_to_date")'
    select_list = ",\n            ".join(
        f"{final_select[col]} AS \"{col}\"" for col in insert_cols
    )
    quoted_insert_cols = ", ".join(f'"{col}"' for col in insert_cols)
    bye_filter = f"AND {int_col('is_bye_week')} = 0" if has("is_bye_week") else ""
    placeholder_filter = f"AND {int_col('is_placeholder')} = 0" if has("is_placeholder") else ""
    settings_has_uses_median = "uses_median" in get_available_columns(conn, "", "league_settings")
    uses_median_expr = "COALESCE(MAX(CAST(ls.uses_median AS INT)), 0) = 1" if settings_has_uses_median else "FALSE"
    eligible_source_columns = [
        col for col in (
            "db_name", "manager", "franchise_id", "year", "week", "opponent",
            "team_points", "opponent_points", "win", "loss", "tie",
            "is_playoffs", "is_consolation", "is_bye_week", "is_placeholder",
            "margin", "gpa", "above_league_median", "below_league_median",
            "close_margin", "win_streak", "winning_streak", "loss_streak",
            "losing_streak", "optimal_points", "team_projected_points",
            "manager_proj_score", "opponent_projected_points", "opponent_proj_score",
            "expected_spread", "expected_odds", "underdog_wins", "favorite_losses",
            "proj_score_error", "abs_proj_score_error", "final_playoff_seed",
            *mean_cols,
        ) if has(col)
    ]
    eligible_projection = ", ".join(f'm."{col}"' for col in eligible_source_columns)

    sql = f"""
        INSERT INTO {target_ref} ({quoted_insert_cols})
        WITH boundaries AS (
            SELECT
                m.db_name,
                m.year,
                COALESCE(
                    MAX(CAST(ls.playoff_start_week AS INTEGER)) - 1,
                    MAX(CASE WHEN {is_playoffs} = 0 AND {int_col('is_consolation')} = 0
                                  AND m.manager IS NOT NULL THEN m.week END),
                    14
                ) AS last_reg_week,
                {uses_median_expr} AS uses_median
            FROM public.matchup m
            LEFT JOIN public.league_settings ls
              ON ls.db_name = m.db_name AND TRY_CAST(ls.year AS INTEGER) = TRY_CAST(m.year AS INTEGER)
            WHERE m.db_name IS NOT NULL AND m.year IS NOT NULL
            GROUP BY m.db_name, m.year
        ),
        eligible AS (
            SELECT {eligible_projection}, b.uses_median
            FROM public.matchup m
            JOIN boundaries b ON b.db_name = m.db_name AND b.year = m.year
            WHERE m.week <= b.last_reg_week
              AND {is_playoffs} = 0
              AND {int_col('is_consolation')} = 0
              AND m.manager IS NOT NULL AND m.opponent IS NOT NULL
              {bye_filter}
              {placeholder_filter}
              AND (
                  COALESCE(m.team_points, 0) > 0 OR COALESCE(m.opponent_points, 0) > 0
                  OR {int_col('win')} = 1 OR {int_col('loss')} = 1 OR {tie} = 1
              )
        ),
        aggregated AS (
            SELECT
                m.db_name,
                m.year,
                m.franchise_id,
                {base_select}
            FROM eligible m
            GROUP BY m.db_name, m.year, m.franchise_id
        ),
        snapshot_weeks AS (
            SELECT db_name, year, franchise_id, MAX(week) AS week
            FROM eligible
            GROUP BY db_name, year, franchise_id
        ),
        snapshots AS (
            SELECT m.*
            FROM public.matchup m
            JOIN snapshot_weeks w
              ON w.db_name = m.db_name AND w.year = m.year
             AND w.franchise_id = m.franchise_id AND w.week = m.week
        ),
        playoffs AS (
            SELECT
                m.db_name,
                m.year,
                m.franchise_id,
                MAX(CASE WHEN {is_playoffs} = 1 THEN 1 ELSE 0 END) AS made_playoffs,
                MAX({champion}) AS is_champion,
                MAX({sacko}) AS is_sacko,
                ARG_MAX(
                    CASE WHEN COALESCE({playoff_round}, {consolation_round}) IS NOT NULL
                         THEN (CASE WHEN {int_col('win')} = 1 THEN 'Won ' ELSE 'Lost ' END)
                              || REPLACE(COALESCE({playoff_round}, {consolation_round}), '_', ' ')
                         ELSE NULL END,
                    m.week
                ) AS playoff_result
            FROM public.matchup m
            JOIN boundaries b ON b.db_name = m.db_name AND b.year = m.year
            WHERE (m.week > b.last_reg_week OR {champion} = 1 OR {sacko} = 1)
              AND m.manager IS NOT NULL
            GROUP BY m.db_name, m.year, m.franchise_id
        )
        SELECT
            {select_list}
        FROM aggregated a
        JOIN snapshots s
          ON s.db_name = a.db_name AND s.year = a.year AND s.franchise_id = a.franchise_id
        LEFT JOIN playoffs p
          ON p.db_name = a.db_name AND p.year = a.year AND p.franchise_id = a.franchise_id
    """
    conn.execute(sql)

    rows = int(conn.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0])
    distinct_keys = int(conn.execute(
        f"SELECT COUNT(*) FROM (SELECT db_name, franchise_id, year FROM {target_ref} "
        "GROUP BY db_name, franchise_id, year)"
    ).fetchone()[0])
    null_keys = int(conn.execute(
        f"SELECT COUNT(*) FROM {target_ref} WHERE db_name IS NULL OR franchise_id IS NULL OR year IS NULL"
    ).fetchone()[0])
    if not rows or rows != distinct_keys or null_keys:
        raise RuntimeError(
            "invalid rebuilt matchup_season keys: "
            f"rows={rows}, distinct={distinct_keys}, null={null_keys}"
        )
    leagues, league_years = conn.execute(
        f"SELECT COUNT(DISTINCT db_name), COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) FROM {target_ref}"
    ).fetchone()
    return {
        "rows": rows,
        "distinct_keys": distinct_keys,
        "leagues": int(leagues),
        "league_years": int(league_years),
    }


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


def aggregate_matchup_h2h(
    conn, db_name: str, dry_run: bool = False, *, season_years: set[int] | None = None
) -> tuple:
    """Build matchup_h2h_season and matchup_h2h_career tables.

    H2H season: one row per (manager, opponent, year)
    H2H career: one row per (manager, opponent) with streak info

    ``season_years`` limits season-table writes, not career inputs. An empty
    set rebuilds only careers from the full persisted matchup chain, allowing
    a weekly publish to retain already-finalized historical season outputs.
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
    selected_years = years if season_years is None else [yr for yr in years if yr in season_years]
    if selected_years:
        season_filter = (
            "" if season_years is None
            else f" AND year IN ({','.join(str(int(yr)) for yr in selected_years)})"
        )
        execute_scoped(
            conn,
            f"DELETE FROM {central_table('matchup_h2h_season')} WHERE db_name = '{db_name}'{season_filter}",
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
    for yr in selected_years:
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
