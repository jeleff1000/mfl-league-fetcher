#!/usr/bin/env python3
"""
Aggregate Fantasy Context to Season/Career Tables (Per-League)

This transformation pre-aggregates league-specific fantasy context to speed up queries:
- player_fantasy_season: Aggregated by NFL_player_id + year
- player_fantasy_career: Aggregated by NFL_player_id (all-time)

These tables are PER-LEAGUE (stored in each league's database).
Fantasy context is league-specific - each league gets its own aggregation.

TWO-TRACK ARCHITECTURE:
- Track 1 (aggregate_nfl_stats.py): Pre-aggregate universal NFL stats (shared)
- Track 2 (this script): Pre-aggregate league-specific fantasy context

DUAL-PATH QUERIES:
- No filters: JOIN player_fantasy_season + player_nfl_season (instant)
- With filters: Aggregate from player_fantasy at query time (flexible)

Usage:
    python aggregate_fantasy_context.py --context path/to/league_context.json
    python aggregate_fantasy_context.py --db demo_league
    python aggregate_fantasy_context.py --context path/to/league_context.json --dry-run
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
from multi_league.core.aggregate_ddl import (
    aggregate_insert_columns,
    aggregate_table_columns,
    ensure_aggregate_table,
    recreate_aggregate_table_like,
)
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.aggregation.aggregation_utils import (
    CENTRAL_DB_NAME,
    central_table,
    configure_table_catalog,
    current_catalog,
    get_active_catalog,
    make_logger,
    resolve_db_name,
)

# Note: We don't use LeagueContext here because it requires OAuth credentials.
# This script only needs the database name, which we extract directly from JSON.

log = make_logger("FANTASY-AGG")


def table_ref(table_name: str) -> str:
    """Legacy wrapper — prefer central_table() directly in new code."""
    return central_table(table_name)


def create_fantasy_season_table(conn, db_name: str) -> bool:
    """Create player_fantasy_season table if it doesn't exist."""
    configure_table_catalog(conn)
    log("Creating player_fantasy_season table...")
    ensure_aggregate_table(conn, get_active_catalog(), "player_fantasy_season")

    log("  player_fantasy_season table ready")
    return True


def create_fantasy_career_table(conn, db_name: str) -> bool:
    """Create player_fantasy_career table if it doesn't exist."""
    configure_table_catalog(conn)
    log("Creating player_fantasy_career table...")
    ensure_aggregate_table(conn, get_active_catalog(), "player_fantasy_career")

    log("  player_fantasy_career table ready")
    return True


def get_available_columns(conn, db_name: str, table_name: str) -> set:
    """
    Get the set of columns that exist in a table.

    This allows us to handle optional columns like clutch_equity
    which may not exist until playoff odds worker runs.
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
            sample = conn.execute(f"SELECT * FROM {table_ref(table_name)} LIMIT 0").description
            return {col[0].lower() for col in sample}
        except Exception:
            return set()


def _build_select_clauses(available_cols: set, granularity: str) -> dict:
    """
    Build SQL SELECT clauses based on which columns exist in player_fantasy.

    franchise_id is required and raises KeyError if missing. Other columns
    (clutch_equity, optimal_player, win, loss, is_playoffs, is_championship,
    position, team_points, opponent_points) are optional and fall back to
    zero/NULL constants when absent.

    Args:
        available_cols: Set of column names available in the table
        granularity: "season" or "career" — controls aggregation style

    Returns:
        Dict of SQL snippet strings keyed by clause name

    Raises:
        KeyError: if franchise_id is not in available_cols.
    """
    if "franchise_id" not in available_cols:
        raise KeyError("franchise_id is required for manager identity")

    has_team_points = "team_points" in available_cols
    has_opponent_points = "opponent_points" in available_cols
    has_clutch = "clutch_equity" in available_cols
    has_optimal = "optimal_player" in available_cols
    has_league_optimal = "league_wide_optimal_player" in available_cols
    has_win = "win" in available_cols
    has_loss = "loss" in available_cols
    has_is_championship = "is_championship" in available_cols
    has_is_playoffs = "is_playoffs" in available_cols
    has_position = "position" in available_cols
    has_is_started = "is_started" in available_cols

    # Player outcome/value metrics are start-based.  A player can be rostered
    # while sitting on a bench, and the manager's weekly win/loss and clutch
    # result must not be attributed to that player in that case.  If the
    # source table cannot identify starts, fail closed instead of silently
    # reverting to the old roster-row aggregation.
    started_condition = "COALESCE(CAST(f.is_started AS INTEGER), 0) = 1" if has_is_started else "FALSE"

    c = {}
    c["clutch"] = (
        f"SUM(CASE WHEN {started_condition} THEN COALESCE(CAST(f.clutch_equity AS DOUBLE), 0) ELSE 0 END) AS clutch_equity,"
        if has_clutch
        else "0 AS clutch_equity,"
    )
    c["optimal"] = (
        "SUM(COALESCE(f.optimal_player, 0)) AS optimal_player_count," if has_optimal else "0 AS optimal_player_count,"
    )
    c["league_optimal"] = (
        "SUM(COALESCE(f.league_wide_optimal_player, 0)) AS league_wide_optimal_count,"
        if has_league_optimal
        else "0 AS league_wide_optimal_count,"
    )

    c["win"] = (
        f"SUM(CASE WHEN {started_condition} THEN COALESCE(f.win, 0) ELSE 0 END) AS wins,"
        if has_win
        else "0 AS wins,"
    )
    c["loss"] = (
        f"SUM(CASE WHEN {started_condition} THEN COALESCE(f.loss, 0) ELSE 0 END) AS losses,"
        if has_loss
        else "0 AS losses,"
    )
    c["team_points"] = "SUM(COALESCE(f.team_points, 0)) AS team_points," if has_team_points else "0 AS team_points,"
    c["opponent_points"] = (
        "SUM(COALESCE(f.opponent_points, 0)) AS opponent_points," if has_opponent_points else "0 AS opponent_points,"
    )

    if has_is_playoffs and has_win:
        c["playoff_games"] = (
            f"SUM(CASE WHEN {started_condition} AND COALESCE(f.is_playoffs, 0) = 1 "
            "THEN 1 ELSE 0 END) AS playoff_games,"
        )
        c["playoff_wins"] = (
            f"SUM(CASE WHEN {started_condition} AND COALESCE(f.is_playoffs, 0) = 1 "
            "AND COALESCE(f.win, 0) = 1 THEN 1 ELSE 0 END) AS playoff_wins,"
        )
    else:
        c["playoff_games"] = "0 AS playoff_games,"
        c["playoff_wins"] = "0 AS playoff_wins,"

    if has_is_playoffs and has_loss:
        c["playoff_losses"] = (
            f"SUM(CASE WHEN {started_condition} AND COALESCE(f.is_playoffs, 0) = 1 "
            "AND COALESCE(f.loss, 0) = 1 THEN 1 ELSE 0 END) AS playoff_losses,"
        )
    else:
        c["playoff_losses"] = "0 AS playoff_losses,"

    if has_is_championship and has_win:
        c["championship"] = (
            f"SUM(CASE WHEN {started_condition} "
            "AND COALESCE(CAST(f.is_championship AS INTEGER), 0) = 1 "
            "AND COALESCE(f.win, 0) = 1 THEN 1 ELSE 0 END) AS championships,"
        )
    else:
        c["championship"] = "0 AS championships,"

    c["position"] = "MAX(f.position) AS position," if has_position else "NULL AS position,"
    c["franchise_id"] = "MAX(f.franchise_id) AS franchise_id,"

    return c


def _log_missing_columns(available_cols: set, is_first_call: bool = True):
    """Log info about missing optional columns (only on first call per aggregation run)."""
    if not is_first_call:
        return
    if "clutch_equity" not in available_cols:
        log("  [INFO] clutch_equity column not found - will be added after playoff odds worker runs")
    if "optimal_player" not in available_cols:
        log("  [INFO] optimal_player column not found - will be added after optimal lineup transformation runs")
    if "win" not in available_cols:
        log("  [INFO] win/loss columns not found - will be added after matchup_to_player transformation runs")


def _scoped_nfl_lookup_ctes(db_name: str) -> str:
    """Build NFL lookups from only the players used by one league.

    The centralized NFL table is large.  Weekly publication used to rank and
    deduplicate the whole table four times even though a league references a
    small fraction of its keys.
    """
    return f"""
        needed_player_weeks AS MATERIALIZED (
            SELECT DISTINCT player_week
            FROM {table_ref('player_fantasy')}
            WHERE db_name = '{db_name}' AND player_week IS NOT NULL
        ),
        needed_player_ids AS MATERIALIZED (
            SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
            FROM {table_ref('player_fantasy')}
            WHERE db_name = '{db_name}' AND NFL_player_id IS NOT NULL
        ),
        super_table_dedup AS (
            SELECT DISTINCT s.player_week, s.player
            FROM ___ops.nfl_historical.nfl_player_stats_all s
            INNER JOIN needed_player_weeks n ON n.player_week = s.player_week
            WHERE s.player_week IS NOT NULL
        ),
        nfl_team_lookup AS (
            SELECT s.NFL_player_id, s.nfl_team,
                   ROW_NUMBER() OVER (
                       PARTITION BY s.NFL_player_id ORDER BY s.year DESC, s.week DESC
                   ) AS rn
            FROM ___ops.nfl_historical.nfl_player_stats_all s
            INNER JOIN needed_player_ids n
                ON n.NFL_player_id = CAST(s.NFL_player_id AS VARCHAR)
            WHERE s.nfl_team IS NOT NULL AND s.NFL_player_id IS NOT NULL
        ),
        nfl_team_dedup AS (
            SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id, nfl_team
            FROM nfl_team_lookup WHERE rn = 1
        )
    """


def _aggregate_fantasy(conn, db_name: str, granularity: str, include_playoffs: bool, year: int = None) -> int:
    """
    Shared aggregation engine for season/career tables, regular/all-games variants.

    Args:
        conn: DuckDB connection
        db_name: League database name
        granularity: "season" (GROUP BY NFL_player_id, year) or "career" (GROUP BY NFL_player_id)
        include_playoffs: False = filter to regular season weeks, True = all games
        year: (season only) Specific year to aggregate, or None for all years

    Returns:
        Number of rows aggregated
    """
    configure_table_catalog(conn)
    is_season = granularity == "season"
    suffix = "_all" if include_playoffs else ""
    table_name = f"player_fantasy_{granularity}{suffix}"
    staging_name = f"_agg_{granularity}{suffix}_{db_name}_staging"
    playlist_label = " (includes playoffs)" if include_playoffs else ""

    # Log
    if is_season:
        year_desc = str(year) if year else "all years"
        log(f"Aggregating fantasy {granularity}{suffix} stats for {year_desc}{playlist_label}...")
    else:
        log(f"Aggregating fantasy {granularity}{suffix} stats{playlist_label}...")

    # Detect available columns and build SQL snippets
    available_cols = get_available_columns(conn, db_name, "player_fantasy")
    # Only log missing columns on the very first call (season, regular season)
    _log_missing_columns(available_cols, is_first_call=(is_season and not include_playoffs))
    c = _build_select_clauses(available_cols, granularity)

    # Week filter: regular season only excludes NFL playoff weeks
    week_filter = ""
    if not include_playoffs:
        week_filter = """
        AND (
            (f.year >= 2021 AND f.week <= 18)
            OR (f.year < 2021 AND f.week <= 17)
        )
    """

    # Year filter (season only, for incremental updates)
    source_where_clauses = [f"f.db_name = '{db_name}'", "f.NFL_player_id IS NOT NULL"]
    if is_season and year:
        source_where_clauses.append(f"f.year = {year}")

    # GROUP BY / PARTITION BY / ORDER BY differ by granularity
    if is_season:
        group_by = "GROUP BY f.NFL_player_id, f.year"
        partition_by = "PARTITION BY NFL_player_id, year"
        order_by = "ORDER BY f.year, managers"
    else:
        group_by = "GROUP BY f.NFL_player_id"
        partition_by = "PARTITION BY NFL_player_id"
        order_by = "ORDER BY managers"

    # Season-specific SELECT columns (year) vs career-specific (first_year, last_year, years_active)
    if is_season:
        granularity_cols = "f.year,"
    else:
        granularity_cols = """MIN(f.year) AS first_year,
            MAX(f.year) AS last_year,
            COUNT(DISTINCT f.year) AS years_active,"""

    # Build explicit staging table with the same schema as the final aggregate.
    # We keep the staging hop to avoid PK issues on the final target while still
    # staying DDL-first. In the centralized model `recreate_aggregate_table_like`
    # creates a session-scoped TEMP table, so the staging name is unqualified
    # (not `___leagues.public.*`) and safe under concurrent imports.
    recreate_aggregate_table_like(conn, get_active_catalog(), table_name, staging_name)
    staging_ref = staging_name if get_active_catalog() == "___leagues" else table_ref(staging_name)
    insert_cols = aggregate_table_columns(table_name)
    execute_scoped(
        conn,
        f"""
        INSERT INTO {staging_ref}
        ({aggregate_insert_columns(table_name, insert_cols)})
        WITH {_scoped_nfl_lookup_ctes(db_name)},
        agg AS (
        SELECT
            '{db_name}' AS db_name,
            f.NFL_player_id,
            {granularity_cols}
            COALESCE(MAX(bio.player), MAX(s.player), MAX(f.player)) AS player,
            {c['position']}
            MAX(st.nfl_team) AS nfl_team,
            SUM(COALESCE(f.fantasy_points, 0)) AS fantasy_points,
            SUM(COALESCE(CAST(f.player_lamar AS DOUBLE), 0)) AS player_lamar,
            SUM(COALESCE(CAST(f.manager_lamar AS DOUBLE), 0)) AS manager_lamar,
            {c['clutch']}
            SUM(CASE WHEN CAST(f.is_started AS INTEGER) = 1 THEN 1 ELSE 0 END) AS games_started,
            COUNT(*) AS games_rostered,
            {c['win']}
            {c['loss']}
            COALESCE(
                STRING_AGG(DISTINCT NULLIF(TRIM(f.manager), ''), ', ')
                    FILTER (WHERE f.manager IS NOT NULL AND TRIM(f.manager) <> '' AND LOWER(TRIM(f.manager)) <> 'unrostered'),
                'Unrostered'
            ) AS managers,
            {c['franchise_id']}
            {c['team_points']}
            {c['opponent_points']}
            {c['playoff_games']}
            {c['playoff_wins']}
            {c['playoff_losses']}
            {c['championship']}
            {c['optimal']}
            {c['league_optimal']}
            MAX(f.fantasy_position) AS fantasy_position,
            CURRENT_TIMESTAMP AS last_updated
        FROM {table_ref('player_fantasy')} f
        LEFT JOIN ___ops.nfl_historical.player_bio bio
            ON CAST(bio.NFL_player_id AS VARCHAR) = CAST(f.NFL_player_id AS VARCHAR)
        LEFT JOIN super_table_dedup s ON f.player_week = s.player_week
        LEFT JOIN nfl_team_dedup st ON CAST(f.NFL_player_id AS VARCHAR) = st.NFL_player_id
        WHERE {' AND '.join(source_where_clauses)}
        {week_filter}
        {group_by}
        {order_by}
        )
        SELECT * FROM agg
        QUALIFY ROW_NUMBER() OVER ({partition_by} ORDER BY fantasy_points DESC) = 1
    """,
        db_name,
        label=f"{table_name}:stage",
    )

    # Delete existing data, then insert from staging
    if is_season and year:
        execute_scoped(
            conn,
            f"DELETE FROM {table_ref(table_name)} WHERE db_name = '{db_name}' AND year = {year}",
            db_name,
            label=f"{table_name}:delete",
        )
    else:
        execute_scoped(
            conn,
            f"DELETE FROM {table_ref(table_name)} WHERE db_name = '{db_name}'",
            db_name,
            label=f"{table_name}:delete",
        )

    col_list = aggregate_insert_columns(table_name, insert_cols)
    execute_scoped(
        conn,
        f"""
        INSERT INTO {table_ref(table_name)} ({col_list})
        SELECT {col_list} FROM {staging_ref}
        WHERE db_name = '{db_name}'
    """,
        db_name,
        label=f"{table_name}:insert",
    )
    # Session-scoped TEMP tables are auto-cleaned on disconnect, but we
    # drop explicitly to release memory between batches on long runs.
    conn.execute(f"DROP TABLE IF EXISTS {staging_ref}")

    # Get row count
    count_where = f" AND year = {year}" if is_season and year else ""
    count = conn.execute(
        f"SELECT COUNT(*) FROM {table_ref(table_name)} WHERE db_name = '{db_name}'{count_where}"
    ).fetchone()[0]

    label = "player-seasons" if is_season else "player careers"
    extra = " (all games)" if include_playoffs else ""
    log(f"  Aggregated {count:,} {label}{extra}")
    return count


def aggregate_fantasy_season(conn, db_name: str, year: int = None) -> int:
    """Aggregate player_fantasy to season totals (regular season only)."""
    return _aggregate_fantasy(conn, db_name, "season", include_playoffs=False, year=year)


def aggregate_fantasy_career(conn, db_name: str) -> int:
    """Aggregate player_fantasy to career totals (regular season only)."""
    return _aggregate_fantasy(conn, db_name, "career", include_playoffs=False)


def create_fantasy_season_table_all(conn, db_name: str) -> bool:
    """Create player_fantasy_season_all table (includes playoffs) if it doesn't exist."""
    configure_table_catalog(conn)
    log("Creating player_fantasy_season_all table...")
    ensure_aggregate_table(conn, get_active_catalog(), "player_fantasy_season_all")

    log("  player_fantasy_season_all table ready")
    return True


def create_fantasy_career_table_all(conn, db_name: str) -> bool:
    """Create player_fantasy_career_all table (includes playoffs) if it doesn't exist."""
    configure_table_catalog(conn)
    log("Creating player_fantasy_career_all table...")
    ensure_aggregate_table(conn, get_active_catalog(), "player_fantasy_career_all")

    log("  player_fantasy_career_all table ready")
    return True


def aggregate_fantasy_season_all(conn, db_name: str, year: int = None) -> int:
    """Aggregate player_fantasy to season totals INCLUDING playoffs."""
    return _aggregate_fantasy(conn, db_name, "season", include_playoffs=True, year=year)


def aggregate_fantasy_career_all(conn, db_name: str) -> int:
    """Aggregate player_fantasy to career totals INCLUDING playoffs."""
    return _aggregate_fantasy(conn, db_name, "career", include_playoffs=True)


def run_aggregation(conn, db_name: str | None = None) -> tuple[int, int, int, int]:
    """Build all fantasy aggregate tables on an already-open connection."""
    runtime_db_name = db_name or current_catalog(conn)
    configure_table_catalog(conn)
    if runtime_db_name == CENTRAL_DB_NAME:
        raise ValueError("db_name is required when aggregating fantasy tables in centralized ___leagues mode")

    # Ensure ALL tables exist (regular + _all versions)
    create_fantasy_season_table(conn, runtime_db_name)
    create_fantasy_career_table(conn, runtime_db_name)
    create_fantasy_season_table_all(conn, runtime_db_name)
    create_fantasy_career_table_all(conn, runtime_db_name)

    # Guard: check if player_fantasy table exists (preseason leagues may not have it)
    pf_exists = conn.execute(
        f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name = '{get_active_catalog()}' AND schema_name = 'public' AND table_name = 'player_fantasy'
    """
    ).fetchone()[0]
    if not pf_exists:
        for table_name in (
            "player_fantasy_season",
            "player_fantasy_career",
            "player_fantasy_season_all",
            "player_fantasy_career_all",
        ):
            execute_scoped(
                conn,
                f"DELETE FROM {table_ref(table_name)} WHERE db_name = '{runtime_db_name}'",
                runtime_db_name,
                label=f"{table_name}:delete",
            )
        log("\nplayer_fantasy table does not exist - skipping aggregation (preseason league or incomplete import)")
        return 0, 0, 0, 0

    log("\n--- Regular Season Tables ---")
    season_count = aggregate_fantasy_season(conn, runtime_db_name)
    career_count = aggregate_fantasy_career(conn, runtime_db_name)

    log("\n--- All Games Tables (includes playoffs) ---")
    season_all_count = aggregate_fantasy_season_all(conn, runtime_db_name)
    career_all_count = aggregate_fantasy_career_all(conn, runtime_db_name)

    log("")
    log("Aggregation complete:")
    log(f"  Regular season: {season_count:,} seasons, {career_count:,} careers")
    log(f"  All games:      {season_all_count:,} seasons, {career_all_count:,} careers")
    return season_count, career_count, season_all_count, career_all_count


def main(args):
    """Main entry point for fantasy context aggregation."""
    log("=" * 60)
    log("FANTASY CONTEXT AGGREGATION (Regular Season + All Games)")
    log("=" * 60)

    # Determine database name
    db_name, ctx_data = resolve_db_name(args)
    league_name = ctx_data.get("league_name", "")
    if league_name:
        log(f"League: {league_name}")
    log(f"Database: {db_name}")

    if args.dry_run:
        log("[DRY RUN] Would aggregate fantasy context")
        log("  - player_fantasy_season: GROUP BY NFL_player_id, year (regular season)")
        log("  - player_fantasy_career: GROUP BY NFL_player_id (regular season)")
        log("  - player_fantasy_season_all: GROUP BY NFL_player_id, year (all games)")
        log("  - player_fantasy_career_all: GROUP BY NFL_player_id (all games)")
        return

    # Connect (local DuckDB with ___ops attached, or MotherDuck)
    conn = get_pipeline_connection(db_name, data_dir=args.data_dir, attach_ops=True, qualified=not bool(args.data_dir))
    # Bind the shared aggregation catalog to this connection so
    # central_table() resolves to the local catalog when we're running on
    # a local DuckDB scratch file.
    configure_table_catalog(conn)

    try:
        runtime_catalog = current_catalog(conn)
        if runtime_catalog != CENTRAL_DB_NAME:
            log(f"Catalog: {runtime_catalog}")
        run_aggregation(conn, db_name)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate fantasy context from player_fantasy to season/career tables"
    )
    parser.add_argument("--context", help="Path to league_context.json")
    parser.add_argument("--db", help="Database name (alternative to --context)")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()
    main(args)
