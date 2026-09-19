"""Pre-aggregate standings by year for instant dashboard loads.

Replaces the 5-tier CTE (dedup -> base -> max_playoff -> agg -> final) that
the frontend /standings API route executes on every request.  The resulting
standings_by_year table makes the API a single SELECT ... WHERE year = ?.

The SQL logic here mirrors frontend/src/app/api/league/[db]/standings/route.ts
exactly, including:
- Dedup via ROW_NUMBER(PARTITION BY year, week, manager, opponent)
- Regular-season win/loss/points aggregation
- Playoff context (champion, sacko, placement game results)
- H2H+Median support (above_median_wins/losses, total_wins/losses)
- Seed calculation (final_playoff_seed with fallback to wins/points ranking)

Usage:
    # As a library (called from import pipeline or other aggregation modules):
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings
    aggregate_standings(conn, db_name, years)

    # Standalone:
    python -m multi_league.transformations.aggregation.aggregate_standings --db demo_league
    python -m multi_league.transformations.aggregation.aggregate_standings --context path/to/context.json
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

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

from multi_league.core.aggregate_ddl import aggregate_insert_columns, ensure_aggregate_table
from multi_league.core.join_keys import franchise_identity_column_name
from multi_league.core.db_utils import get_pipeline_connection
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    get_active_catalog,
    league_db_filter,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_matchup_columns(conn, db_name: str) -> set:
    """Return set of column names present in the matchup table."""
    configure_table_catalog(conn)
    try:
        rows = conn.execute(f"DESCRIBE {central_table('matchup')}").fetchall()
        return {row[0] for row in rows}
    except Exception:
        return set()


def _detect_h2h_median(conn, db_name: str, year: int, cols: set) -> bool:
    """Check whether the given year uses H2H+Median scoring.

    Canonical source of truth is the flat ``league_settings.uses_median``
    field for the requested year.
    """
    configure_table_catalog(conn)
    try:
        row = conn.execute(
            f"SELECT COALESCE(uses_median, FALSE) "
            f"FROM {central_table('league_settings')} "
            f"WHERE {league_db_filter(db_name)} AND year = {year} "
            f"LIMIT 1"
        ).fetchone()
        return row is not None and bool(row[0])
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Core aggregation
# ---------------------------------------------------------------------------


def _active_fantasy_season(now: datetime | None = None) -> int:
    """Return the NFL season currently in progress for a UTC timestamp."""
    current = now or datetime.now(timezone.utc)
    return current.year - 1 if current.month <= 2 else current.year


def aggregate_standings(
    conn,
    db_name: str,
    years: list,
    *,
    active_season: int | None = None,
) -> None:
    """Create ``standings_by_year`` table from matchup data.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active MotherDuck connection.
    db_name : str
        Sanitised league database name.
    years : list[int]
        Years to aggregate.  Typically all years present in the matchup table.
    """
    configure_table_catalog(conn)
    cols = _get_matchup_columns(conn, db_name)
    if not cols:
        logger.warning("  [standings_by_year] matchup table not found or empty")
        return
    if "franchise_id" not in cols:
        raise KeyError("franchise_id is required for manager identity")
    if "opponent_franchise_id" not in cols:
        raise KeyError("opponent_franchise_id is required for manager identity")
    identity_column = franchise_identity_column_name(True)
    opponent_identity_column = "opponent_franchise_id"

    # Pre-scan: determine which years use H2H+Median so the table schema is
    # consistent.  If ANY year uses median we include those columns for all
    # years (filling NULL/0 for non-median years).  This avoids INSERT schema
    # mismatches when the first year lacks median but a later year has it.
    # above_league_median is in canonical DDL (always exists); we still need
    # to detect which years actually use H2H+Median to decide output schema
    median_years: set = set()
    current_season = _active_fantasy_season() if active_season is None else int(active_season)
    for year in years:
        if _detect_h2h_median(conn, db_name, year, cols):
            median_years.add(year)
    any_median = len(median_years) > 0

    ensure_aggregate_table(conn, get_active_catalog(), "standings_by_year")
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('standings_by_year')} WHERE db_name = '{db_name}'",
        db_name,
        label="standings_by_year:delete",
    )

    for year in years:
        unfinished_result = "'In Progress'" if int(year) == current_season else "'Missed Playoffs'"
        # Determine whether this specific year uses H2H+Median
        has_median = year in median_years

        # -- Median projections (SELECT inside agg CTE) --------------------
        # When any_median is True we always emit these columns so the table
        # schema is consistent across INSERT statements.  For years that do
        # NOT use median we emit NULL placeholders.
        median_select = ""
        if any_median:
            if has_median:
                median_select = (
                    ",\n"
                    "            SUM(CASE WHEN COALESCE(b.is_playoffs, 0) = 0 "
                    "AND b.above_league_median = 1 THEN 1 ELSE 0 END) AS above_median_wins,\n"
                    "            SUM(CASE WHEN COALESCE(b.is_playoffs, 0) = 0 "
                    "AND b.above_league_median = 0 THEN 1 ELSE 0 END) AS above_median_losses"
                )
            else:
                median_select = (
                    ",\n"
                    "            CAST(NULL AS INTEGER) AS above_median_wins,\n"
                    "            CAST(NULL AS INTEGER) AS above_median_losses"
                )

        # -- Median total projections (SELECT inside final CTE) ------------
        median_total_select = ""
        if any_median:
            median_total_select = (
                ",\n"
                "            a.above_median_wins,\n"
                "            a.above_median_losses,\n"
                "            (a.wins + COALESCE(a.above_median_wins, 0)) AS total_wins,\n"
                "            (a.losses + COALESCE(a.above_median_losses, 0)) AS total_losses"
            )

        # -- ORDER BY for seed fallback ------------------------------------
        # Use total_wins only when this specific year has median data
        order_by_col = "total_wins DESC, total_losses ASC" if has_median else "wins DESC, losses ASC"

        # -- above_league_median in dedup SELECT ---------------------------
        median_dedup_col = "above_league_median," if has_median else ""
        dedup_franchise_select = "CAST(franchise_id AS VARCHAR) AS franchise_id,"
        dedup_identity_select = f"CAST({identity_column} AS VARCHAR) AS identity_key,"
        dedup_identity_partition = f"CAST({identity_column} AS VARCHAR)"
        dedup_opponent_partition = f"CAST({opponent_identity_column} AS VARCHAR)"
        agg_franchise_select = "ARG_MAX(b.franchise_id, b.week) AS franchise_id,"
        agg_identity_group = "b.franchise_id"
        final_identity_join = "a.franchise_id = mp.identity_key"

        sql = f"""
        WITH deduped AS (
            SELECT DISTINCT
                   TRY_CAST(year AS INTEGER) AS year,
                   TRY_CAST(week AS INTEGER) AS week,
                   CAST(manager AS VARCHAR) AS manager,
                   {dedup_franchise_select}
                   CAST(team_name AS VARCHAR) AS team_name,
                   CAST(opponent AS VARCHAR) AS opponent,
                   TRY_CAST(team_points AS DOUBLE) AS team_points,
                   TRY_CAST(opponent_points AS DOUBLE) AS opponent_points,
                   TRY_CAST(win AS INTEGER) AS win,
                   TRY_CAST(loss AS INTEGER) AS loss,
                   TRY_CAST(is_playoffs AS INTEGER) AS is_playoffs,
                   CAST(playoff_round AS VARCHAR) AS playoff_round,
                   CAST(consolation_round AS VARCHAR) AS consolation_round,
                   TRY_CAST(champion AS INTEGER) AS champion,
                   TRY_CAST(sacko AS INTEGER) AS sacko,
                   TRY_CAST(is_consolation AS INTEGER) AS is_consolation,
                   TRY_CAST(final_playoff_seed AS INTEGER) AS final_playoff_seed,
                   TRY_CAST(is_bye_week AS BOOLEAN) AS is_bye_week,
                   {dedup_identity_select}
                   {median_dedup_col}
            ROW_NUMBER() OVER (
                       PARTITION BY
                           TRY_CAST(year AS INTEGER),
                           TRY_CAST(week AS INTEGER),
                           {dedup_identity_partition},
                           {dedup_opponent_partition}
                       ORDER BY TRY_CAST(team_points AS DOUBLE) DESC NULLS LAST
                   ) AS rn
            FROM {central_table('matchup')}
            WHERE {league_db_filter(db_name)}
              AND TRY_CAST(year AS INTEGER) = {year}
              AND manager IS NOT NULL
              AND TRIM(CAST(manager AS VARCHAR)) != ''
        ),
        base AS (
            SELECT *
            FROM deduped
            WHERE rn = 1
              AND week IS NOT NULL
        ),
        max_playoff AS (
            SELECT
                identity_key,
                MAX(week) AS max_week,
                MAX(CASE WHEN champion = 1 THEN 1 ELSE 0 END) AS won_championship,
                MAX(CASE WHEN sacko = 1 THEN 1 ELSE 0 END) AS got_sacko,
                ARG_MAX(playoff_round, week) AS last_playoff_round,
                ARG_MAX(consolation_round, week) AS last_consolation_round,
                ARG_MAX(win, week) AS last_game_win,
                ARG_MAX(is_consolation, week) AS last_game_consolation,
                MAX(final_playoff_seed) AS final_playoff_seed
            FROM base
            WHERE is_playoffs = 1 OR is_consolation = 1
            GROUP BY identity_key
        ),
        agg AS (
            SELECT
                ARG_MAX(b.manager, b.week) AS manager,
                {year} AS year,
                {agg_franchise_select}
                MAX(b.team_name) AS team_name,
                SUM(CASE WHEN COALESCE(b.is_playoffs, 0) = 0 AND COALESCE(b.is_consolation, 0) = 0 AND COALESCE(b.is_bye_week, FALSE) = FALSE AND b.win = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN COALESCE(b.is_playoffs, 0) = 0 AND COALESCE(b.is_consolation, 0) = 0 AND COALESCE(b.is_bye_week, FALSE) = FALSE AND b.loss = 1 THEN 1 ELSE 0 END) AS losses,
                0 AS ties,
                ROUND(SUM(b.team_points), 1) AS points_for,
                ROUND(SUM(b.opponent_points), 1) AS points_against,
                MAX(b.final_playoff_seed) AS final_playoff_seed
                {median_select}
            FROM base b
            GROUP BY {agg_identity_group}
        ),
        final AS (
            SELECT
                a.manager,
                a.year,
                a.team_name,
                a.wins,
                a.losses,
                a.ties,
                a.points_for,
                a.points_against,
                ROUND(a.wins * 1.0 / NULLIF(a.wins + a.losses, 0), 3) AS win_pct,
                a.final_playoff_seed,
                a.franchise_id
                {median_total_select},
                CASE
                    WHEN COALESCE(mp.won_championship, 0) = 1 THEN 'Won Championship'
                    WHEN COALESCE(mp.got_sacko, 0) = 1 THEN 'Sacko'
                    WHEN mp.last_playoff_round = 'championship' AND mp.last_game_win = 0 THEN 'Lost Championship'
                    WHEN mp.last_consolation_round = 'third_place_game' AND mp.last_game_win = 1 THEN 'Won Third Place Game'
                    WHEN mp.last_consolation_round = 'third_place_game' AND mp.last_game_win = 0 THEN 'Lost Third Place Game'
                    WHEN mp.last_consolation_round = 'fifth_place_game' AND mp.last_game_win = 1 THEN 'Won Fifth Place Game'
                    WHEN mp.last_consolation_round = 'fifth_place_game' AND mp.last_game_win = 0 THEN 'Lost Fifth Place Game'
                    WHEN mp.last_consolation_round = 'seventh_place_game' AND mp.last_game_win = 1 THEN 'Won Seventh Place Game'
                    WHEN mp.last_consolation_round = 'seventh_place_game' AND mp.last_game_win = 0 THEN 'Lost Seventh Place Game'
                    WHEN mp.last_consolation_round IS NOT NULL AND mp.last_game_win = 1 THEN 'Won Placement Game'
                    WHEN mp.last_consolation_round IS NOT NULL AND mp.last_game_win = 0 THEN 'Lost Placement Game'
                    WHEN mp.last_game_consolation = 1 THEN 'Consolation'
                    WHEN mp.max_week IS NOT NULL THEN 'Eliminated'
                    ELSE {unfinished_result}
                END AS final_result
            FROM agg a
            LEFT JOIN max_playoff mp ON {final_identity_join}
        )
        SELECT
            '{db_name}' AS db_name,
            *,
            COALESCE(final_playoff_seed,
                ROW_NUMBER() OVER (ORDER BY {order_by_col}, points_for DESC)
            ) AS seed
        FROM final
        ORDER BY seed ASC, {order_by_col}, points_for DESC
        """

        insert_cols = [
            "db_name",
            "manager",
            "year",
            "team_name",
            "wins",
            "losses",
            "ties",
            "points_for",
            "points_against",
            "win_pct",
            "final_playoff_seed",
            "franchise_id",
        ]
        if any_median:
            insert_cols.extend(["above_median_wins", "above_median_losses", "total_wins", "total_losses"])
        insert_cols.extend(["final_result", "seed"])

        try:
            execute_scoped(
                conn,
                f"INSERT INTO {central_table('standings_by_year')} "
                f"({aggregate_insert_columns('standings_by_year', insert_cols)}) "
                f"{sql}",
                db_name,
                label="standings_by_year:insert",
            )
        except Exception as e:
            logger.warning(f"  [standings_by_year] year={year} failed: {e}")
            continue

    # Log summary
    try:
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {central_table('standings_by_year')} WHERE db_name = '{db_name}'"
        ).fetchone()[0]
        logger.info(f"  [standings_by_year] {row_count} rows across {len(years)} years")
    except Exception:
        logger.warning("  [standings_by_year] table was not created (no valid years)")


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Pre-aggregate standings by year for instant dashboard loads")
    parser.add_argument("--context", help="Path to league context JSON file")
    parser.add_argument("--db", help="Database name (alternative to --context)")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )
    args = parser.parse_args()

    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [STANDINGS-AGG] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Resolve database name
    if args.db:
        db_name = args.db
    elif args.context:
        try:
            with open(args.context) as f:
                ctx_json = json.load(f)
            db_name = ctx_json.get("database_name") or ctx_json.get("motherduck_db_name", "")
            if not db_name:
                league_name = ctx_json.get("league_name", "")
                if not league_name:
                    raise ValueError("No league_name or database_name in context JSON")
                from multi_league.core.db_utils import sanitize_database_name

                db_name = sanitize_database_name(league_name)
        except Exception as e:
            logger.error(f"Could not read context file: {e}")
            sys.exit(1)
    else:
        logger.error("Must provide --context or --db")
        sys.exit(1)

    logger.info(f"Aggregating standings for database: {db_name}")

    conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
    configure_table_catalog(conn)

    try:
        # Discover years from matchup table
        years = [
            row[0]
            for row in conn.execute(
                f"SELECT DISTINCT year FROM {central_table('matchup')} "
                f"WHERE {league_db_filter(db_name)} AND year IS NOT NULL ORDER BY year"
            ).fetchall()
        ]

        if not years:
            logger.warning("No years found in matchup table")
            sys.exit(0)

        logger.info(f"Years: {years}")
        aggregate_standings(conn, db_name, years)
        logger.info("DONE")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
