#!/usr/bin/env python3
"""
Pre-aggregate draft data into manager-season, manager-career, and player-career tables.

This transformation pre-aggregates league-specific draft data to speed up queries:
- draft_manager_season: Aggregated by manager + year
- draft_manager_career: Aggregated by manager (all-time)
- draft_player_career: Aggregated by player + position (all-time)

These tables are PER-LEAGUE (stored in each league's database).

Usage:
    python aggregate_draft_context.py --context path/to/league_context.json
    python aggregate_draft_context.py --db demo_league
    python aggregate_draft_context.py --context path/to/league_context.json --dry-run
"""

import argparse

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
from multi_league.core.join_keys import (
    franchise_identity_sql_ref,
    franchise_identity_sql_select,
)
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    get_active_catalog,
    league_db_filter,
    make_logger,
    resolve_db_name,
)

log = make_logger("DRAFT-AGG")


def pick_quality_zscore_expr(cols: set[str], alias: str) -> str | None:
    """Return the preferred pick-quality z-score expression.

    `draft_value_zscore` is the canonical blended expectation score. Keep the
    older aliases as fallbacks so aggregate rebuilds still work for leagues that
    have not been re-enriched yet.
    """
    if "draft_value_zscore" in cols:
        return f"{alias}.draft_value_zscore"
    if "pick_quality_zscore" in cols:
        return f"{alias}.pick_quality_zscore"
    if "pick_score" in cols:
        score = f"TRY_CAST({alias}.pick_score AS DOUBLE)"
        return f"CASE WHEN ABS({score}) <= 10 THEN {score} ELSE ({score} - 100.0) / 15.0 END"
    return None


def get_available_columns(conn, db_name: str, table_name: str) -> set:
    """
    Get the set of columns that exist in a table.

    This allows us to handle optional columns that may not exist
    across different league platforms (Yahoo vs Sleeper vs ESPN).
    """
    configure_table_catalog(conn)
    try:
        result = conn.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = '{table_name}'
              AND table_catalog = '{get_active_catalog()}'
        """).fetchall()
        return {row[0].lower() for row in result}
    except Exception as e:
        log(f"  [WARN] Could not detect columns in {table_name}: {e}")
        # Fallback: try to query the table directly
        try:
            sample = conn.execute(f"SELECT * FROM {central_table(table_name)} LIMIT 0").description
            return {col[0].lower() for col in sample}
        except Exception:
            return set()


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------


def create_draft_manager_season_table(conn, db_name: str) -> bool:
    """Create draft_manager_season table if it doesn't exist."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "draft_manager_season")
    log("  draft_manager_season table ready")
    return True


def create_draft_manager_career_table(conn, db_name: str) -> bool:
    """Create draft_manager_career table if it doesn't exist."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "draft_manager_career")
    log("  draft_manager_career table ready")
    return True


def create_draft_player_career_table(conn, db_name: str) -> bool:
    """Create draft_player_career table if it doesn't exist."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "draft_player_career")
    log("  draft_player_career table ready")
    return True


# ---------------------------------------------------------------------------
# Aggregation functions
# ---------------------------------------------------------------------------


def _build_keeper_expr(cols: set) -> str:
    """Build keeper detection expression from available columns.

    Uses TRY_CAST for safety — keeper columns may be VARCHAR if Yahoo
    API returned strings that weren't normalized before upload.
    """
    parts = []
    if "is_keeper" in cols:
        parts.append("COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) > 0")
    if "is_keeper_status" in cols:
        parts.append("COALESCE(TRY_CAST(d.is_keeper_status AS INTEGER), 0) > 0")
    if "is_keeper_cost" in cols:
        parts.append("COALESCE(TRY_CAST(d.is_keeper_cost AS INTEGER), 0) > 0")
    return " OR ".join(parts) if parts else "FALSE"


def aggregate_draft_manager_season(conn, db_name: str) -> int:
    """Aggregate draft table to manager-season totals."""
    configure_table_catalog(conn)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('draft_manager_season')} WHERE db_name = '{db_name}'",
        db_name,
        label="draft_manager_season:delete",
    )
    cols = get_available_columns(conn, db_name, "draft")
    if not cols:
        log("  No draft columns found, skipping")
        return 0

    # Column fallbacks
    lamar_col = "manager_lamar" if "manager_lamar" in cols else "lamar" if "lamar" in cols else None
    player_lamar_col = "player_lamar" if "player_lamar" in cols else None
    pos_col = "yahoo_position" if "yahoo_position" in cols else "position" if "position" in cols else None
    quality_expr = pick_quality_zscore_expr(cols, "nk")
    points_col = "total_fantasy_points" if "total_fantasy_points" in cols else "points" if "points" in cols else None
    ppg_col = "season_ppg" if "season_ppg" in cols else None
    games_col = "games_played" if "games_played" in cols else None
    starter_col = "drafted_as_starter" if "drafted_as_starter" in cols else None
    lpd_col = "lamar_per_dollar" if "lamar_per_dollar" in cols else None
    bust_col = "is_bust" if "is_bust" in cols else None
    breakout_col = "is_breakout" if "is_breakout" in cols else None
    grade_col = "manager_draft_grade" if "manager_draft_grade" in cols else None
    score_col = "manager_draft_score" if "manager_draft_score" in cols else None
    pct_col = "manager_draft_percentile_alltime" if "manager_draft_percentile_alltime" in cols else None
    if not lamar_col:
        log("  No LAMAR column found in draft table, skipping")
        return 0
    if "franchise_id" not in cols:
        raise KeyError("franchise_id is required for manager identity")
    fid_col = "franchise_id"

    is_keeper_expr = _build_keeper_expr(cols)

    # Build SELECT expressions for optional columns.
    # The non_keeper CTE pre-filters keepers, so no CASE WHEN keeper guard needed
    # inside the main SELECT — just aggregate directly from nk.
    player_lamar_agg = f"SUM(COALESCE(nk.{player_lamar_col}, 0))" if player_lamar_col else "0"
    points_agg = f"SUM(COALESCE(nk.{points_col}, 0))" if points_col else "0"
    ppg_agg = f"AVG(nk.{ppg_col})" if ppg_col else "0"
    quality_agg = f"AVG({quality_expr})" if quality_expr else "0"
    games_agg = f"AVG(nk.{games_col})" if games_col else "0"
    starter_agg = f"SUM(CASE WHEN COALESCE(nk.{starter_col}, 0) = 1 THEN 1 ELSE 0 END)" if starter_col else "0"
    lpd_agg = f"AVG(CASE WHEN COALESCE(nk.cost, 0) > 0 THEN nk.{lpd_col} END)" if lpd_col else "0"
    bust_agg = f"SUM(CASE WHEN COALESCE(nk.{bust_col}, 0) = 1 THEN 1 ELSE 0 END)" if bust_col else "0"
    breakout_agg = f"SUM(CASE WHEN COALESCE(nk.{breakout_col}, 0) = 1 THEN 1 ELSE 0 END)" if breakout_col else "0"
    grade_agg = f"MAX(nk.{grade_col})" if grade_col else "NULL"
    score_agg = f"MAX(nk.{score_col})" if score_col else "0"
    pct_agg = f"MAX(nk.{pct_col})" if pct_col else "0"
    fid_agg = f"MAX(nk.{fid_col})"

    has_draft_category = "draft_category" in cols
    cat_expr = "COALESCE(CAST(d.draft_category AS VARCHAR), 'standard')" if has_draft_category else "'standard'"
    cat_expr_nk = "COALESCE(CAST(nk.draft_category AS VARCHAR), 'standard')" if has_draft_category else "'standard'"

    sql = f"""
        INSERT INTO {central_table("draft_manager_season")} (
            db_name, manager, year, franchise_id, draft_category, picks, keeper_picks, total_cost,
            total_manager_lamar, avg_manager_lamar,
            total_player_lamar, total_fantasy_points, avg_season_ppg,
            hits, hit_rate, busts, breakouts,
            avg_pick_quality_zscore, starters_drafted, avg_games_played, avg_lamar_per_dollar,
            manager_draft_grade, manager_draft_score, manager_draft_percentile,
            best_pick_player, best_pick_lamar, worst_pick_player, worst_pick_lamar,
            last_updated
        )
        WITH non_keeper AS (
            SELECT d.*, {cat_expr} as _cat
            FROM {central_table("draft")} d
            WHERE NOT ({is_keeper_expr})
              AND {league_db_filter(db_name, "d")}
              AND d.manager IS NOT NULL AND TRIM(d.manager) <> ''
              AND d.franchise_id IS NOT NULL AND TRIM(CAST(d.franchise_id AS VARCHAR)) <> ''
        ),
        keeper AS (
            SELECT d.franchise_id as _grp_key,
                   MAX(d.manager) AS manager,
                   d.year, {cat_expr} as _cat, COUNT(*) as cnt
            FROM {central_table("draft")} d
            WHERE ({is_keeper_expr})
              AND {league_db_filter(db_name, "d")}
              AND d.manager IS NOT NULL AND TRIM(d.manager) <> ''
              AND d.franchise_id IS NOT NULL AND TRIM(CAST(d.franchise_id AS VARCHAR)) <> ''
            GROUP BY d.franchise_id, d.year, {cat_expr}
        ),
        best_worst AS (
            SELECT franchise_id as _grp_key,
                   MAX(manager) AS manager,
                   year, _cat,
                FIRST(player ORDER BY {lamar_col} DESC) as best_player,
                MAX({lamar_col}) as best_lamar,
                FIRST(player ORDER BY {lamar_col} ASC) as worst_player,
                MIN({lamar_col}) as worst_lamar
            FROM non_keeper
            WHERE player IS NOT NULL
            GROUP BY franchise_id, year, _cat
        )
        SELECT
            '{db_name}' AS db_name,
            MAX(nk.manager) as manager,
            nk.year,
            {fid_agg} as franchise_id,
            nk._cat as draft_category,
            COUNT(*) as picks,
            COALESCE(k.cnt, 0) as keeper_picks,
            SUM(COALESCE(nk.cost, 0)) as total_cost,
            SUM(COALESCE(nk.{lamar_col}, 0)) as total_manager_lamar,
            AVG(COALESCE(nk.{lamar_col}, 0)) as avg_manager_lamar,
            {player_lamar_agg} as total_player_lamar,
            {points_agg} as total_fantasy_points,
            {ppg_agg} as avg_season_ppg,
            SUM(CASE WHEN COALESCE(nk.{lamar_col}, 0) > 0 THEN 1 ELSE 0 END) as hits,
            CAST(SUM(CASE WHEN COALESCE(nk.{lamar_col}, 0) > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) as hit_rate,
            {bust_agg} as busts,
            {breakout_agg} as breakouts,
            {quality_agg} as avg_pick_quality_zscore,
            {starter_agg} as starters_drafted,
            {games_agg} as avg_games_played,
            {lpd_agg} as avg_lamar_per_dollar,
            {grade_agg} as manager_draft_grade,
            {score_agg} as manager_draft_score,
            {pct_agg} as manager_draft_percentile,
            bw.best_player,
            bw.best_lamar,
            bw.worst_player,
            bw.worst_lamar,
            CURRENT_TIMESTAMP
        FROM non_keeper nk
        LEFT JOIN keeper k ON nk.franchise_id = k._grp_key AND nk.year = k.year AND nk._cat = k._cat
        LEFT JOIN best_worst bw ON nk.franchise_id = bw._grp_key AND nk.year = bw.year AND nk._cat = bw._cat
        GROUP BY nk.franchise_id, nk.year, nk._cat, k.cnt,
                 bw.best_player, bw.best_lamar, bw.worst_player, bw.worst_lamar
        ORDER BY nk.year DESC, total_manager_lamar DESC
    """
    execute_scoped(conn, sql, db_name, label="draft_manager_season:insert")
    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('draft_manager_season')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  draft_manager_season: {count} rows")
    return count


def aggregate_draft_manager_career(conn, db_name: str) -> int:
    """Aggregate draft_manager_season to career totals with GPA grading."""
    configure_table_catalog(conn)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('draft_manager_career')} WHERE db_name = '{db_name}'",
        db_name,
        label="draft_manager_career:delete",
    )
    season_cols = get_available_columns(conn, db_name, "draft_manager_season")
    if not season_cols:
        log("  No draft_manager_season columns found, skipping")
        return 0
    if "franchise_id" not in season_cols:
        raise KeyError("franchise_id is required for manager identity")
    season_identity_ref = franchise_identity_sql_ref(None, True)
    season_identity_select = franchise_identity_sql_select(None, True, output_alias="_grp_key")
    career_identity_ref = franchise_identity_sql_ref("c", True)

    # GPA grade mapping
    grade_map = """
        CASE
            WHEN c.career_gpa >= 3.85 THEN 'A+'
            WHEN c.career_gpa >= 3.50 THEN 'A'
            WHEN c.career_gpa >= 3.15 THEN 'A-'
            WHEN c.career_gpa >= 2.85 THEN 'B+'
            WHEN c.career_gpa >= 2.50 THEN 'B'
            WHEN c.career_gpa >= 2.15 THEN 'B-'
            WHEN c.career_gpa >= 1.85 THEN 'C+'
            WHEN c.career_gpa >= 1.50 THEN 'C'
            WHEN c.career_gpa >= 1.15 THEN 'C-'
            WHEN c.career_gpa >= 0.85 THEN 'D+'
            WHEN c.career_gpa >= 0.50 THEN 'D'
            WHEN c.career_gpa >= 0.15 THEN 'D-'
            ELSE 'F'
        END
    """

    insert_cols = [
        "db_name",
        "manager",
        "franchise_id",
        "draft_category",
        "years_active",
        "total_picks",
        "total_keeper_picks",
        "total_cost",
        "total_manager_lamar",
        "avg_manager_lamar",
        "total_fantasy_points",
        "avg_season_ppg",
        "career_hit_rate",
        "total_busts",
        "total_breakouts",
        "avg_pick_quality_zscore",
        "avg_games_played",
        "avg_lamar_per_dollar",
        "career_gpa",
        "career_grade",
        "best_pick_player",
        "best_pick_lamar",
        "worst_pick_player",
        "worst_pick_lamar",
        "last_updated",
    ]

    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table("draft_manager_career")} ({aggregate_insert_columns("draft_manager_career", insert_cols)})
        WITH season AS (
            SELECT * FROM {central_table("draft_manager_season")}
            WHERE db_name = '{db_name}'
        ),
        career_raw AS (
            SELECT
                ARG_MAX(manager, year) as manager,
                MAX(franchise_id) as franchise_id,
                draft_category,
                COUNT(DISTINCT year) as years_active,
                SUM(picks) as total_picks,
                SUM(keeper_picks) as total_keeper_picks,
                SUM(total_cost) as total_cost,
                SUM(total_manager_lamar) as total_manager_lamar,
                SUM(total_manager_lamar) / NULLIF(SUM(picks), 0) as avg_manager_lamar,
                SUM(total_fantasy_points) as total_fantasy_points,
                SUM(avg_season_ppg * picks) / NULLIF(SUM(picks), 0) as avg_season_ppg,
                CAST(SUM(hits) AS DOUBLE) / NULLIF(SUM(picks), 0) as career_hit_rate,
                SUM(busts) as total_busts,
                SUM(breakouts) as total_breakouts,
                SUM(avg_pick_quality_zscore * picks) / NULLIF(SUM(picks), 0) as avg_pick_quality_zscore,
                SUM(avg_games_played * picks) / NULLIF(SUM(picks), 0) as avg_games_played,
                SUM(total_manager_lamar) / NULLIF(SUM(total_cost), 0) as avg_lamar_per_dollar,
                -- Convert season letter grades → GPA (0-4 scale), then weighted average
                SUM(
                    CASE manager_draft_grade
                        WHEN 'A+' THEN 4.0  WHEN 'A' THEN 3.67  WHEN 'A-' THEN 3.33
                        WHEN 'B+' THEN 3.0  WHEN 'B' THEN 2.67  WHEN 'B-' THEN 2.33
                        WHEN 'C+' THEN 2.0  WHEN 'C' THEN 1.67  WHEN 'C-' THEN 1.33
                        WHEN 'D+' THEN 1.0  WHEN 'D' THEN 0.67  WHEN 'D-' THEN 0.33
                        WHEN 'F' THEN 0.0   ELSE NULL
                    END * picks
                ) / NULLIF(SUM(CASE WHEN manager_draft_grade IS NOT NULL THEN picks ELSE 0 END), 0) as career_gpa
            FROM season
            GROUP BY {season_identity_ref}, draft_category
        ),
        best_worst AS (
            SELECT {season_identity_select},
                draft_category,
                FIRST(best_pick_player ORDER BY best_pick_lamar DESC) as best_pick_player,
                MAX(best_pick_lamar) as best_pick_lamar,
                FIRST(worst_pick_player ORDER BY worst_pick_lamar ASC) as worst_pick_player,
                MIN(worst_pick_lamar) as worst_pick_lamar
            FROM season
            WHERE best_pick_player IS NOT NULL
            GROUP BY {season_identity_ref}, draft_category
        )
        SELECT
            '{db_name}' AS db_name,
            c.manager, c.franchise_id, c.draft_category, c.years_active, c.total_picks, c.total_keeper_picks,
            c.total_cost, c.total_manager_lamar, c.avg_manager_lamar,
            c.total_fantasy_points, c.avg_season_ppg, c.career_hit_rate,
            c.total_busts, c.total_breakouts, c.avg_pick_quality_zscore,
            c.avg_games_played, c.avg_lamar_per_dollar,
            c.career_gpa,
            {grade_map} as career_grade,
            bw.best_pick_player, bw.best_pick_lamar,
            bw.worst_pick_player, bw.worst_pick_lamar,
            CURRENT_TIMESTAMP
        FROM career_raw c
        LEFT JOIN best_worst bw ON {career_identity_ref} = bw._grp_key AND c.draft_category = bw.draft_category
    """,
        db_name,
        label="draft_manager_career:insert",
    )
    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('draft_manager_career')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  draft_manager_career: {count} rows")
    return count


def aggregate_draft_player_career(conn, db_name: str) -> int:
    """Aggregate draft table to player-career totals with manager/year rollups."""
    configure_table_catalog(conn)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('draft_player_career')} WHERE db_name = '{db_name}'",
        db_name,
        label="draft_player_career:delete",
    )
    cols = get_available_columns(conn, db_name, "draft")
    lamar_col = "manager_lamar" if "manager_lamar" in cols else "lamar" if "lamar" in cols else None
    pos_col = "yahoo_position" if "yahoo_position" in cols else "position" if "position" in cols else None
    quality_expr = pick_quality_zscore_expr(cols, "d")
    points_col = "total_fantasy_points" if "total_fantasy_points" in cols else "points" if "points" in cols else None
    ppg_col = "season_ppg" if "season_ppg" in cols else None

    if not lamar_col or not pos_col:
        log("  Missing required columns for draft_player_career, skipping")
        return 0

    # Keeper detection
    is_keeper_expr = _build_keeper_expr(cols)

    quality_agg = f"AVG({quality_expr})" if quality_expr else "0"
    points_agg = f"SUM(COALESCE(d.{points_col}, 0))" if points_col else "0"
    ppg_agg = f"AVG(d.{ppg_col})" if ppg_col else "0"

    has_draft_category = "draft_category" in cols
    cat_expr = "COALESCE(CAST(d.draft_category AS VARCHAR), 'standard')" if has_draft_category else "'standard'"

    if "franchise_id" not in cols:
        raise KeyError("franchise_id is required for manager identity")
    fid_agg_player = "STRING_AGG(DISTINCT d.franchise_id, ', ' ORDER BY d.franchise_id)"

    insert_cols = [
        "db_name",
        "player",
        "position",
        "draft_category",
        "times_drafted",
        "times_kept",
        "total_cost",
        "keeper_cost",
        "total_manager_lamar",
        "avg_manager_lamar",
        "lamar_per_dollar",
        "total_fantasy_points",
        "avg_season_ppg",
        "avg_pick_quality_zscore",
        "best_manager",
        "managers",
        "franchise_ids",
        "years",
        "last_updated",
    ]

    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table("draft_player_career")} ({aggregate_insert_columns("draft_player_career", insert_cols)})
        SELECT
            '{db_name}' AS db_name,
            d.player,
            d.{pos_col} as position,
            {cat_expr} as draft_category,
            SUM(CASE WHEN NOT ({is_keeper_expr}) THEN 1 ELSE 0 END) as times_drafted,
            SUM(CASE WHEN ({is_keeper_expr}) THEN 1 ELSE 0 END) as times_kept,
            SUM(CASE WHEN NOT ({is_keeper_expr}) THEN COALESCE(d.cost, 0) ELSE 0 END) as total_cost,
            SUM(CASE WHEN ({is_keeper_expr}) THEN COALESCE(d.cost, 0) ELSE 0 END) as keeper_cost,
            SUM(COALESCE(d.{lamar_col}, 0)) as total_manager_lamar,
            AVG(COALESCE(d.{lamar_col}, 0)) as avg_manager_lamar,
            SUM(COALESCE(d.{lamar_col}, 0)) / NULLIF(SUM(CASE WHEN NOT ({is_keeper_expr}) THEN COALESCE(d.cost, 0) ELSE 0 END), 0) as lamar_per_dollar,
            {points_agg} as total_fantasy_points,
            {ppg_agg} as avg_season_ppg,
            {quality_agg} as avg_pick_quality_zscore,
            FIRST(d.manager ORDER BY COALESCE(d.{lamar_col}, 0) DESC) as best_manager,
            STRING_AGG(DISTINCT d.manager, ', ' ORDER BY d.manager) as managers,
            {fid_agg_player} as franchise_ids,
            STRING_AGG(DISTINCT CAST(d.year AS VARCHAR), ', ' ORDER BY CAST(d.year AS VARCHAR)) as years,
            CURRENT_TIMESTAMP
        FROM {central_table("draft")} d
        WHERE {league_db_filter(db_name, "d")}
          AND d.player IS NOT NULL AND TRIM(d.player) <> ''
          AND d.{pos_col} IS NOT NULL AND TRIM(d.{pos_col}) <> ''
        GROUP BY d.player, d.{pos_col}, {cat_expr}
    """,
        db_name,
        label="draft_player_career:insert",
    )
    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('draft_player_career')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  draft_player_career: {count} rows")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(args):
    """Main entry point for draft context aggregation."""
    log("=" * 60)
    log("DRAFT CONTEXT AGGREGATION")
    log("=" * 60)

    # Determine database name
    db_name, ctx_data = resolve_db_name(args)
    league_name = ctx_data.get("league_name", "")
    if league_name:
        log(f"League: {league_name}")
    log(f"Database: {db_name}")

    if args.dry_run:
        log("[DRY RUN] Would aggregate draft tables")
        log("  - draft_manager_season: GROUP BY manager, year")
        log("  - draft_manager_career: GROUP BY manager")
        log("  - draft_player_career: GROUP BY player, position")
        return

    # Connect to database (local or MotherDuck)
    conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
    # Bind the shared aggregation catalog to this connection so
    # central_table() resolves to the local catalog when running on the
    # GH runner's per-league DuckDB file.
    configure_table_catalog(conn)

    try:
        # Create tables
        create_draft_manager_season_table(conn, db_name)
        create_draft_manager_career_table(conn, db_name)
        create_draft_player_career_table(conn, db_name)

        # Aggregate (season first, career rolls up from season)
        season_count = aggregate_draft_manager_season(conn, db_name)
        career_count = aggregate_draft_manager_career(conn, db_name)
        player_count = aggregate_draft_player_career(conn, db_name)

        log("")
        log(f"Done: {season_count} season + {career_count} career + {player_count} player rows")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate draft data from draft table to manager-season/career and player-career tables"
    )
    parser.add_argument("--context", help="Path to league_context.json")
    parser.add_argument("--db", help="Database name (alternative to --context)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )

    args = parser.parse_args()
    main(args)
