#!/usr/bin/env python3
"""
Homepage Summary Precomputation

Computes all data needed for homepage display and stores in 5 MotherDuck tables.
Runs after all LAMAR/clutch metrics are calculated.

This transformation precomputes:
1. League-wide summary data (records, highlights, player leaders)
2. Manager career rankings (Hall of Fame / leaderboard)
3. Current season standings with playoff odds
4. Top rivalries
5. Manager profiles (career stats, badges, draft/txn profiles, rivalries, timeline, player leaders)

Performance impact:
- Before: 40-50 queries, 400-800ms homepage load
- After: 5 simple SELECT queries, 40-70ms homepage load (10-15x speedup)

Usage:
    python homepage_summary.py --context league_context.json
    python homepage_summary.py --db demo_league
    python homepage_summary.py --context league_context.json --upload
    python homepage_summary.py --context league_context.json --dry-run
"""

import argparse
import gc
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from collections.abc import Mapping, Sequence

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

import pandas as pd

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS, aggregate_insert_columns, ensure_aggregate_table
from multi_league.core.player_identity import select_platform_player_id_column
from multi_league.core.sql_utils import execute_scoped
from multi_league.shared.filters import rostered_filter_sql  # noqa: F401 - used in f-strings
from multi_league.core.db_utils import detect_platform, get_pipeline_connection
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    get_active_catalog,
    league_db_filter,
    make_logger,
    set_active_catalog,
    LocalProfileContext,
    ColumnCache,
)


log = make_logger("HOMEPAGE-SUMMARY")


class IncompleteTradeMirrorError(RuntimeError):
    """A persisted trade asset is missing its valued opposite-side mirror."""


def _table_select_sql(table: str, db_name: str, alias: str = "") -> str:
    table_sql = central_table(table)
    alias_sql = f" {alias}" if alias else ""
    where_sql = league_db_filter(db_name, alias)
    return f"{table_sql}{alias_sql} WHERE {where_sql}"


class ScopedLocalProfileContext(LocalProfileContext):
    """Local profile scratch pad that only pulls rows for the active db_name."""

    def _pull_tables(self, remote_conn, db_name: str, platform: str):  # noqa: D401
        configure_table_catalog(remote_conn)

        for table in ["matchup", "draft", "transactions"]:
            try:
                df = remote_conn.execute(
                    f"SELECT * FROM {central_table(table)} WHERE {league_db_filter(db_name)}"
                ).fetchdf()
                self.local.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            except Exception as e:
                self._log(f"[WARN] Failed to pull {table}: {e}")

        try:
            pf_df = remote_conn.execute(
                f"SELECT * FROM {central_table('player_fantasy')} "
                f"WHERE {league_db_filter(db_name)} "
                "AND is_started = 1 AND manager_lamar IS NOT NULL"
            ).fetchdf()
            self.local.execute("CREATE TABLE player_fantasy AS SELECT * FROM pf_df")
        except Exception:
            self._log("[WARN] Filtered player_fantasy pull failed, trying unfiltered league-only pull")
            try:
                pf_df = remote_conn.execute(
                    f"SELECT * FROM {central_table('player_fantasy')} WHERE {league_db_filter(db_name)}"
                ).fetchdf()
                self.local.execute("CREATE TABLE player_fantasy AS SELECT * FROM pf_df")
            except Exception as e:
                self._log(f"[WARN] Could not pull player_fantasy: {e}")

        self._build_headshot_lookups(remote_conn)

        for table in ["matchup_season", "player_fantasy_season", "league_settings", "league_context"]:
            try:
                if table_exists(remote_conn, db_name, table):
                    df = remote_conn.execute(
                        f"SELECT * FROM {central_table(table)} WHERE {league_db_filter(db_name)}"
                    ).fetchdf()
                    self.local.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            except Exception:
                continue


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _normalize_platform_id_sql(expr: str) -> str:
    """Normalize platform IDs to comparable strings for SQL joins."""
    return f"NULLIF(REGEXP_REPLACE(TRIM(CAST({expr} AS VARCHAR)), '\\\\.0+$', ''), '')"


def _typed_matchup_dedupe_ctes_sql(db_name: str) -> str:
    """Return shared CTEs that cast matchup year/week and dedupe one row per team-week.

    ``base_matchup`` is restricted to played rows for record/rivalry stats.
    ``latest_matchup`` keeps bye/holdover rows so current odds can use the
    latest frozen probabilities without counting those rows as games.
    """
    return f"""
        typed_matchup AS (
            SELECT
                * EXCLUDE (year, week),
                TRY_CAST(year AS INT) AS year,
                TRY_CAST(week AS INT) AS week
            FROM {central_table('matchup')}
            WHERE franchise_id IS NOT NULL
              AND TRIM(CAST(franchise_id AS VARCHAR)) != ''
              AND {league_db_filter(db_name)}
        ),
        deduped AS (
            SELECT
                typed_matchup.*,
                ROW_NUMBER() OVER (
                    PARTITION BY franchise_id, year, week
                    ORDER BY
                        CASE
                            WHEN team_points IS NOT NULL
                              AND COALESCE(CAST(is_bye_week AS INT), 0) = 0
                            THEN 0 ELSE 1
                        END,
                        manager
                ) as rn
            FROM typed_matchup
        ),
        base_matchup AS (
            SELECT *
            FROM deduped
            WHERE rn = 1
              AND team_points IS NOT NULL
              AND COALESCE(CAST(is_bye_week AS INT), 0) = 0
        ),
        latest_matchup AS (
            SELECT * FROM deduped WHERE rn = 1
        )
    """


def _escape_sql_literal(value: str) -> str:
    """Escape a string literal for inline DuckDB SQL."""
    return str(value).replace("'", "''")


def _franchise_filter_sql(franchise_id: str, alias: str = "", column: str = "franchise_id") -> str:
    """Return a stable identity filter keyed on franchise_id."""
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column} = '{_escape_sql_literal(franchise_id)}'"


def _profile_log_label(manager: str | None, franchise_id: str) -> str:
    """Return a concise log label for a franchise-backed manager profile."""
    manager_text = str(manager).strip() if manager is not None else ""
    if manager_text:
        return f"{manager_text} [{franchise_id}]"
    return str(franchise_id)


def replace_scoped_aggregate_table_from_dataframe(conn, db_name: str, table_name: str, df: pd.DataFrame) -> None:
    """Replace one league's rows in a shared aggregate table."""
    configure_table_catalog(conn)

    prepared = df.copy()
    prepared.columns = [str(column) for column in prepared.columns]
    if prepared.columns.duplicated().any():
        dupes = prepared.columns[prepared.columns.duplicated()].tolist()
        raise ValueError(f"{table_name} aggregate dataframe has duplicate columns: {dupes}")

    expected_columns = list(AGGREGATE_TABLE_SPECS[table_name].column_types.keys())
    expected_types = AGGREGATE_TABLE_SPECS[table_name].column_types
    data_columns = [column for column in expected_columns if column != "db_name"]

    extra = sorted(set(prepared.columns) - set(data_columns))
    if "db_name" in extra:
        extra.remove("db_name")
    if extra:
        raise ValueError(f"{table_name} aggregate dataframe has non-canonical columns: {extra}")

    if "db_name" in prepared.columns:
        prepared = prepared.drop(columns=["db_name"])
    for column in data_columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[data_columns]

    ensure_aggregate_table(conn, get_active_catalog(), table_name)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table(table_name)} WHERE {league_db_filter(db_name)}",
        db_name,
        label=f"{table_name}:delete",
    )

    register_name = "_aggregate_upload"
    conn.register(register_name, prepared)
    try:
        cast_select = ", ".join(
            f"CAST({_quote_identifier(column)} AS {expected_types[column]}) AS {_quote_identifier(column)}"
            for column in data_columns
        )
        insert_cols = aggregate_insert_columns(table_name, expected_columns)
        execute_scoped(
            conn,
            f"INSERT INTO {central_table(table_name)} ({insert_cols}) "
            f"SELECT '{db_name}' AS db_name, {cast_select} FROM {register_name}",
            db_name,
            label=f"{table_name}:insert",
        )
    finally:
        conn.unregister(register_name)


def _trade_direction_filters() -> tuple[str, str]:
    """Return SQL filters for received vs sent trade rows."""
    return (
        " AND t.trade_direction = 'received'",
        " AND t.trade_direction = 'sent'",
    )


def _trade_received_lamar_expr(txn_cols: set[str], alias: str = "t") -> str:
    """Return the best available trade-value expression for received rows."""
    if "trade_asset_lamar" in txn_cols:
        return f"COALESCE({alias}.trade_asset_lamar, 0)"

    parts = []
    if "manager_lamar_ros_managed" in txn_cols:
        parts.append(f"{alias}.manager_lamar_ros_managed")
    if "player_lamar_ros_total" in txn_cols:
        parts.append(f"{alias}.player_lamar_ros_total")
    if "total_points_ros_total" in txn_cols:
        parts.append(f"{alias}.total_points_ros_total")
    parts.append("0")
    return f"COALESCE({', '.join(parts)})"


def _trade_sent_lamar_expr(txn_cols: set[str], alias: str = "t") -> str:
    """Return the best available trade-value expression for sent rows."""
    if "trade_asset_lamar" in txn_cols:
        return f"COALESCE({alias}.trade_asset_lamar, 0)"

    parts = []
    if "player_lamar_ros_total" in txn_cols:
        parts.append(f"{alias}.player_lamar_ros_total")
    if "total_points_ros_total" in txn_cols:
        parts.append(f"{alias}.total_points_ros_total")
    parts.append("0")
    return f"COALESCE({', '.join(parts)})"


def get_available_columns(conn, db_name: str, table_name: str) -> set:
    """Get the set of columns that exist in a table."""
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
        try:
            sample = conn.execute(f"SELECT * FROM {central_table(table_name)} LIMIT 0").description
            return {col[0].lower() for col in sample}
        except Exception:
            return set()


def table_exists(conn, db_name: str, table_name: str) -> bool:
    """Check if a table exists in the database."""
    configure_table_catalog(conn)
    try:
        result = conn.execute(f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = '{table_name}'
              AND table_catalog = '{get_active_catalog()}'
            LIMIT 1
        """).fetchone()
        return result is not None
    except Exception:
        try:
            conn.execute(f"DESCRIBE {central_table(table_name)}")
            return True
        except Exception:
            return False


def _late_clutch_weeks_cte(db_name: str) -> str:
    """Return a CTE that captures the last two championship-bracket weeks per year.

    The week window is derived from the flat ``league_settings`` contract:
    ``playoff_start_week`` + bracket size + ``has_multiweek_championship``.
    We only fall back to all weeks if those late-playoff weeks do not yet have
    any clutch rows in ``player_fantasy``.
    """
    return f"""
        WITH settings_weeks AS (
            SELECT
                year,
                CAST(playoff_start_week AS INTEGER) AS playoff_start_week,
                GREATEST(
                    1,
                    CAST(CEIL(LOG2(CAST(playoff_teams AS DOUBLE))) AS INTEGER)
                ) AS playoff_rounds,
                CASE
                    WHEN COALESCE(CAST(has_multiweek_championship AS INTEGER), 0) = 1 THEN 1
                    ELSE 0
                END AS extra_championship_week
            FROM {central_table('league_settings')}
            WHERE playoff_start_week IS NOT NULL
              AND playoff_teams IS NOT NULL
              AND {league_db_filter(db_name)}
        ),
        late_clutch_weeks AS (
            SELECT
                s.year,
                gs.week
            FROM settings_weeks s
            CROSS JOIN LATERAL generate_series(
                s.playoff_start_week
                    + GREATEST(s.playoff_rounds + s.extra_championship_week - 2, 0),
                s.playoff_start_week
                    + s.playoff_rounds + s.extra_championship_week - 1
            ) AS gs(week)
        ),
        has_late_clutch_rows AS (
            SELECT COUNT(*) AS n
            FROM {central_table('player_fantasy')} pf
            WHERE pf.clutch_equity IS NOT NULL
              AND pf.clutch_equity != 0
              AND pf.is_started = 1
              AND COALESCE(CAST(pf.is_playoffs AS INTEGER), 0) = 1
              AND COALESCE(CAST(pf.is_consolation AS INTEGER), 0) = 0
              AND {league_db_filter(db_name, 'pf')}
              AND EXISTS (
                  SELECT 1
                  FROM late_clutch_weeks lc
                  WHERE lc.year = pf.year
                    AND lc.week = pf.week
              )
        )
    """


def _late_clutch_filter_sql(alias: str = "f") -> str:
    """Return the WHERE predicate for late-playoff clutch leader queries."""
    return (
        f"((SELECT n FROM has_late_clutch_rows) = 0 OR "
        f"(COALESCE(CAST({alias}.is_playoffs AS INTEGER), 0) = 1 "
        f"AND COALESCE(CAST({alias}.is_consolation AS INTEGER), 0) = 0 "
        f"AND EXISTS ("
        f"    SELECT 1 FROM late_clutch_weeks lc "
        f"    WHERE lc.year = {alias}.year AND lc.week = {alias}.week"
        f")))"
    )


def detect_h2h_median_from_db(conn, db_name: str) -> bool:
    """
    Detect whether any season in the league uses H2H+Median scoring.

    Canonical source of truth is the flat, year-specific
    ``league_settings.uses_median`` column.

    Returns:
        True if league uses H2H+Median scoring, False otherwise
    """
    median_years = detect_h2h_median_years_from_db(conn, db_name)
    if median_years:
        log(f"  [INFO] Detected H2H+Median from league_settings.uses_median in years: {median_years}")
        return True
    return False


def detect_h2h_median_years_from_db(conn, db_name: str) -> list[int]:
    """
    Detect which seasons within the given league use H2H+Median scoring.

    This helper checks the flat ``league_settings`` table for year-specific
    indicators that median scoring is enabled. Only years that explicitly
    set ``uses_median`` are returned.

    Args:
        conn: Active DuckDB connection.
        db_name: The database name to query against.

    Returns:
        A sorted list of years (ints) where H2H+Median scoring was used.
        If no year-specific information can be determined, returns an empty list.
    """
    years: set[int] = set()
    configure_table_catalog(conn)
    # Ensure league_settings table exists and has year column
    if not table_exists(conn, db_name, "league_settings"):
        return []
    settings_cols = get_available_columns(conn, db_name, "league_settings")
    if "year" not in settings_cols:
        return []

    # Check the flat uses_median column (1/true means median scoring)
    if "uses_median" in settings_cols:
        try:
            rows = conn.execute(f"""
                SELECT year
                FROM {central_table('league_settings')}
                WHERE COALESCE(uses_median, FALSE)
                  AND {league_db_filter(db_name)}
            """).fetchall()
            for (y,) in rows:
                if y is not None:
                    try:
                        years.add(int(y))
                    except Exception:  # noqa: broad-except
                        continue
        except Exception:  # noqa: broad-except
            pass
    return sorted(years)


def build_wins_sql(use_median: bool, table_alias: str = "", median_years: Sequence[int] | None = None) -> str:
    """
    Build SQL expression for wins calculation based on scoring type.

    If a list of ``median_years`` is provided, the expression will only add
    median wins for rows where the ``year`` column is in that list. This allows
    per-year median scoring without incorrectly applying median wins to all
    seasons.

    Args:
        use_median: Whether the league uses H2H+Median scoring for at least
            one season. This affects the base expression but will be overridden
            if ``median_years`` is provided.
        table_alias: Optional table alias to prefix column names with.
        median_years: Optional sequence of years for which median wins should
            apply. If None or empty, the function falls back to ``use_median``.

    Returns:
        A SQL expression string that computes wins for a single row.
    """
    prefix = f"{table_alias}." if table_alias else ""

    # If a per-year list is provided, build conditional expression
    if median_years:
        years_list = ",".join(str(int(y)) for y in median_years)
        cond = f"CAST({prefix}year AS INT) IN ({years_list})"
        return f"""(
            COALESCE(CAST({prefix}win AS INT), 0) +
            CASE WHEN COALESCE({prefix}is_playoffs, 0) = 0 AND {cond}
                 THEN COALESCE(CAST({prefix}above_league_median AS INT), 0)
                 ELSE 0 END
        )"""
    # Otherwise use simple boolean flag
    if use_median:
        return f"""(
            COALESCE(CAST({prefix}win AS INT), 0) +
            CASE WHEN COALESCE({prefix}is_playoffs, 0) = 0
                 THEN COALESCE(CAST({prefix}above_league_median AS INT), 0)
                 ELSE 0 END
        )"""
    else:
        return f"COALESCE(CAST({prefix}win AS INT), 0)"


def build_losses_sql(use_median: bool, table_alias: str = "", median_years: Sequence[int] | None = None) -> str:
    """Build SQL expression for losses calculation based on scoring type.

    If ``median_years`` is provided, the expression only adds median losses
    (below_league_median) for rows where the ``year`` column is in that list.
    Otherwise it falls back to ``use_median``.
    """
    prefix = f"{table_alias}." if table_alias else ""

    if median_years:
        years_list = ",".join(str(int(y)) for y in median_years)
        cond = f"CAST({prefix}year AS INT) IN ({years_list})"
        return f"""(
            COALESCE(CAST({prefix}loss AS INT), 0) +
            CASE WHEN COALESCE({prefix}is_playoffs, 0) = 0 AND {cond}
                 THEN COALESCE(CAST({prefix}below_league_median AS INT), 0)
                 ELSE 0 END
        )"""
    if use_median:
        return f"""(
            COALESCE(CAST({prefix}loss AS INT), 0) +
            CASE WHEN COALESCE({prefix}is_playoffs, 0) = 0
                 THEN COALESCE(CAST({prefix}below_league_median AS INT), 0)
                 ELSE 0 END
        )"""
    else:
        return f"COALESCE(CAST({prefix}loss AS INT), 0)"


def build_ties_sql(table_alias: str = "") -> str:
    """Build SQL expression for ties calculation (ties have no median component)."""
    prefix = f"{table_alias}." if table_alias else ""
    return f"COALESCE(CAST({prefix}tie AS INT), 0)"


def normalize_player_name_sql(column: str) -> str:
    """Return SQL expression to normalize player name by removing suffixes like II, III, Jr., Sr."""
    return f"REGEXP_REPLACE({column}, ' (II|III|IV|V|Jr\\.?|Sr\\.?)$', '', 'gi')"


def headshot_subquery(
    table_alias: str, player_id_col: str | None = "yahoo_player_id", platform: str = "yahoo", has_nfl_id: bool = False
) -> str:
    """
    Generate SQL subquery to get headshot_url.

    Priority order:
    1. NFL_player_id → player_bio (one row per player, most reliable)
    2. NFL_player_id → super_table (platform-independent fallback)
    3. Platform player_id → mapping table (handles name variants)
    4. Normalized name match → player_bio (last resort)

    Args:
        table_alias: SQL table alias (e.g., 'd' for draft table)
        player_id_col: Column containing the player ID (e.g., 'yahoo_player_id' or 'sleeper_player_id')
        platform: 'yahoo' or 'sleeper' - determines which mapping table to use
        has_nfl_id: Whether the source table has an NFL_player_id column
    """
    # Determine player_bio column for platform-specific ID fallback
    if platform == "sleeper":
        bio_platform_col = "sleeper_player_id"
    elif platform == "espn":
        bio_platform_col = "espn_id"
    else:
        bio_platform_col = "yahoo_player_id"

    parts = []

    if has_nfl_id:
        # Preferred: NFL_player_id → player_bio (one row per player, fast)
        parts.append(f"""
            (SELECT bio.headshot_url FROM ___ops.nfl_historical.player_bio bio
             WHERE CAST(bio.NFL_player_id AS VARCHAR) = CAST({table_alias}.NFL_player_id AS VARCHAR)
               AND bio.headshot_url IS NOT NULL
             LIMIT 1)""")

        # Fallback 1: NFL_player_id → super_table

    # Fallback 2: platform player_id → player_bio (via platform-specific column)
    if player_id_col:
        parts.append(f"""
            (SELECT bio.headshot_url FROM ___ops.nfl_historical.player_bio bio
             WHERE {_normalize_platform_id_sql(f'bio.{bio_platform_col}')} = {_normalize_platform_id_sql(f'{table_alias}.{player_id_col}')}
               AND bio.headshot_url IS NOT NULL
             LIMIT 1)""")

    # Fallback 3: normalized name match → player_bio
    parts.append(f"""
            (SELECT bio.headshot_url FROM ___ops.nfl_historical.player_bio bio
             WHERE {normalize_player_name_sql('bio.player')} = {normalize_player_name_sql(f'{table_alias}.player')}
               AND bio.headshot_url IS NOT NULL
             LIMIT 1)""")

    return f"""
        COALESCE({','.join(parts)}
        )"""


def _sql_scalar_literal(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return "NULL"
    except (TypeError, ValueError):
        if value is None:
            return "NULL"
    return f"'{_escape_sql_literal(str(value))}'"


def _lookup_headshot_for_player(
    conn,
    *,
    player: Any,
    platform: str,
    player_id_col: str | None,
    platform_player_id: Any = None,
    nfl_player_id: Any = None,
) -> str | None:
    """Look up one headshot after a highlight row has already been selected."""
    selected_cols = [f"{_sql_scalar_literal(player)} AS player"]
    has_nfl_id = nfl_player_id is not None and str(nfl_player_id).strip() != ""
    if has_nfl_id:
        selected_cols.append(f"{_sql_scalar_literal(nfl_player_id)} AS NFL_player_id")
    if player_id_col:
        selected_cols.append(f"{_sql_scalar_literal(platform_player_id)} AS {_quote_identifier(player_id_col)}")

    try:
        row = conn.execute(f"""
            WITH t AS (
                SELECT {", ".join(selected_cols)}
            )
            SELECT {headshot_subquery("t", player_id_col, platform, has_nfl_id=has_nfl_id)} AS headshot_url
            FROM t
        """).fetchone()
        return row[0] if row else None
    except Exception:
        return None


# ============================================================================
# LEAGUE-WIDE SUMMARY COMPUTATION
# ============================================================================


def _latest_matchup_year_week(conn, db_name: str) -> tuple[int | None, int | None]:
    """Return the latest played matchup week from one season, not independent maxima."""
    configure_table_catalog(conn)
    row = conn.execute(f"""
        SELECT TRY_CAST(year AS INT), TRY_CAST(week AS INT)
        FROM {central_table('matchup')}
        WHERE {league_db_filter(db_name)}
          AND TRY_CAST(year AS INT) IS NOT NULL
          AND TRY_CAST(week AS INT) IS NOT NULL
        ORDER BY TRY_CAST(year AS INT) DESC, TRY_CAST(week AS INT) DESC
        LIMIT 1
    """).fetchone()
    return (int(row[0]), int(row[1])) if row else (None, None)


def compute_league_summary(
    conn,
    db_name: str,
    platform: str = "yahoo",
    cache: "ColumnCache | None" = None,
    *,
    preserved_alltime_trade: Mapping[str, Any] | None = None,
    changed_years: set[int] | None = None,
) -> pd.DataFrame:
    """
    Compute all league-wide aggregates into a single-row DataFrame.

    Returns DataFrame with ~60 columns containing:
    - Metadata (last_updated, data_year, data_week)
    - League records (highest score, closest game, biggest blowout, most championships)
    - Player performance leaders (best game/season/career LAMAR + clutch)
    - Draft highlights (best/worst picks all-time + current season)
    - Transaction highlights (best pickup, worst drop, best trade)

    Args:
        conn: Database connection
        db_name: Database name
        platform: 'yahoo' or 'sleeper' - determines which player ID mapping table to use
        cache: Optional ColumnCache for metadata queries (created if not provided)
    """
    configure_table_catalog(conn)
    if cache is None:
        cache = ColumnCache(conn, get_active_catalog())

    log("Computing league-wide summary...")

    data_year, data_week = _latest_matchup_year_week(conn, db_name)

    summary = {
        "last_updated": datetime.now(),
        "data_year": data_year,
        "data_week": data_week,
    }

    # ========== LEAGUE RECORDS ==========
    matchup_cols = cache.columns("matchup")
    records = _compute_league_records(conn, db_name, matchup_cols)
    summary.update(records)

    # ========== PLAYER PERFORMANCE LEADERS ==========
    leaders = _compute_player_leaders(conn, db_name, platform=platform, cache=cache)
    summary.update(leaders)

    # ========== DRAFT HIGHLIGHTS (all-time) ==========
    draft_alltime = _compute_draft_highlights(conn, db_name, year=None, platform=platform, cache=cache)
    summary.update({f"alltime_{k}": v for k, v in draft_alltime.items()})

    # ========== DRAFT HIGHLIGHTS (current season) ==========
    if data_year:
        draft_season = _compute_draft_highlights(conn, db_name, year=data_year, platform=platform, cache=cache)
        summary.update({f"season_{k}": v for k, v in draft_season.items()})

    # ========== TRANSACTION HIGHLIGHTS (all-time) ==========
    txn_alltime = _compute_transaction_highlights(conn, db_name, year=None, platform=platform, cache=cache)
    summary.update({f"alltime_{k}": v for k, v in txn_alltime.items()})

    # ========== TRANSACTION HIGHLIGHTS (current season) ==========
    if data_year:
        txn_season = _compute_transaction_highlights(conn, db_name, year=data_year, platform=platform, cache=cache)
        summary.update({f"season_{k}": v for k, v in txn_season.items()})

    # ========== BEST TRADE (all-time) ==========
    try:
        trade_alltime = _compute_best_trade(conn, db_name, year=None, platform=platform, cache=cache)
    except IncompleteTradeMirrorError:
        # A weekly publication must not be blocked by an untouched legacy
        # season whose trade mirrors predate the current enrichment contract.
        # Preserve the already-published all-time winner only after proving
        # every year changed by this publication is independently valid.
        changed_years = {int(value) for value in (changed_years or set())}
        old_winner = (preserved_alltime_trade or {}).get("alltime_trade_winner")
        old_year = (preserved_alltime_trade or {}).get("alltime_trade_year")
        if not changed_years or pd.isna(old_winner) or pd.isna(old_year):
            raise
        if int(old_year) in changed_years:
            raise
        for changed_year in sorted(changed_years):
            _compute_best_trade(
                conn,
                db_name,
                year=changed_year,
                platform=platform,
                cache=cache,
            )
        from multi_league.core.aggregate_ddl import HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES

        trade_alltime = {
            field: preserved_alltime_trade.get(f"alltime_trade_{field}")
            for field in HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES
            if field != "db_name"
        }
        log(
            "  Preserved validated all-time trade highlight from untouched "
            f"{int(old_year)}; refreshed years {sorted(changed_years)} are mirror-complete"
        )
    summary.update({f"alltime_trade_{k}": v for k, v in trade_alltime.items()})

    # ========== BEST TRADE (current season) ==========
    if data_year:
        trade_season = _compute_best_trade(conn, db_name, year=data_year, platform=platform, cache=cache)
        summary.update({f"season_trade_{k}": v for k, v in trade_season.items()})

    log(f"  Computed {len(summary)} summary fields")
    return pd.DataFrame([summary])


def _compute_league_records(conn, db_name: str, matchup_cols: set = None) -> dict[str, Any]:
    """Compute league record holders."""
    records = {}
    configure_table_catalog(conn)

    if matchup_cols is None:
        matchup_cols = get_available_columns(conn, db_name, "matchup")

    # Build dynamic filters for columns that may not exist (Yahoo vs ESPN)
    bye_col = "is_bye_week" if "is_bye_week" in matchup_cols else "is_bye" if "is_bye" in matchup_cols else None
    bye_filter = f"AND ({bye_col} = false OR {bye_col} IS NULL)" if bye_col else ""
    consolation_filter = "AND COALESCE(is_consolation, 0) = 0" if "is_consolation" in matchup_cols else ""
    has_champion = "champion" in matchup_cols

    # Highest single-week score
    try:
        row = conn.execute(f"""
            SELECT manager, team_points as points, year, week
            FROM {central_table('matchup')}
            WHERE manager IS NOT NULL AND team_points IS NOT NULL
              AND {league_db_filter(db_name)}
              {bye_filter}
              {consolation_filter}
            ORDER BY team_points DESC LIMIT 1
        """).fetchone()
        if row:
            records["highest_score_manager"] = row[0]
            records["highest_score_points"] = round(float(row[1]), 2) if row[1] else 0
            records["highest_score_year"] = row[2]
            records["highest_score_week"] = row[3]
    except Exception as e:
        log(f"  [WARN] Failed to compute highest score: {e}")

    # Closest game
    try:
        row = conn.execute(f"""
            SELECT manager, opponent,
                   ABS(team_points - opponent_points) as margin, year, week
            FROM {central_table('matchup')}
            WHERE win IS NOT NULL AND team_points IS NOT NULL AND opponent_points IS NOT NULL
              AND {league_db_filter(db_name)}
              {bye_filter}
              {consolation_filter}
            ORDER BY margin ASC LIMIT 1
        """).fetchone()
        if row:
            records["closest_game_manager"] = row[0]
            records["closest_game_opponent"] = row[1]
            records["closest_game_margin"] = round(float(row[2]), 2) if row[2] else 0
            records["closest_game_year"] = row[3]
            records["closest_game_week"] = row[4]
    except Exception as e:
        log(f"  [WARN] Failed to compute closest game: {e}")

    # Biggest blowout
    try:
        row = conn.execute(f"""
            SELECT manager, opponent,
                   ABS(team_points - opponent_points) as margin, year, week
            FROM {central_table('matchup')}
            WHERE win = 1 AND team_points IS NOT NULL AND opponent_points IS NOT NULL
              AND {league_db_filter(db_name)}
              {bye_filter}
              {consolation_filter}
            ORDER BY margin DESC LIMIT 1
        """).fetchone()
        if row:
            records["biggest_blowout_manager"] = row[0]
            records["biggest_blowout_opponent"] = row[1]
            records["biggest_blowout_margin"] = round(float(row[2]), 2) if row[2] else 0
            records["biggest_blowout_year"] = row[3]
            records["biggest_blowout_week"] = row[4]
    except Exception as e:
        log(f"  [WARN] Failed to compute biggest blowout: {e}")

    # Most championships
    if not has_champion:
        log("  [WARN] Skipping most championships — 'champion' column not found")
    else:
        try:
            row = conn.execute(f"""
                WITH manager_year_champs AS (
                    SELECT franchise_id, year, MAX(manager) as manager,
                           MAX(COALESCE(champion, 0)) as is_champion
                    FROM {central_table('matchup')}
                    WHERE franchise_id IS NOT NULL
                      AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
                      AND {league_db_filter(db_name)}
                    GROUP BY franchise_id, year
                )
                SELECT ARG_MAX(manager, year) as manager, SUM(is_champion) as championships
                FROM manager_year_champs
                GROUP BY franchise_id
                ORDER BY championships DESC, franchise_id ASC LIMIT 1
            """).fetchone()
            if row:
                records["most_championships_manager"] = row[0]
                records["most_championships_count"] = int(row[1])
        except Exception as e:
            log(f"  [WARN] Failed to compute most championships: {e}")

    return records


def _compute_player_leaders(
    conn, db_name: str, platform: str = "yahoo", cache: "ColumnCache | None" = None
) -> dict[str, Any]:
    """Compute player performance leaders (LAMAR and clutch)."""
    leaders = {}
    configure_table_catalog(conn)

    # Check if clutch_equity column exists
    player_cols = cache.columns("player_fantasy") if cache else get_available_columns(conn, db_name, "player_fantasy")
    has_clutch = "clutch_equity" in player_cols
    player_id_col = select_platform_player_id_column(player_cols, platform_hint=platform)
    player_has_nfl_id = "nfl_player_id" in player_cols
    season_cols = (
        cache.columns("player_fantasy_season")
        if cache and cache.exists("player_fantasy_season")
        else get_available_columns(conn, db_name, "player_fantasy_season")
    )
    career_cols = (
        cache.columns("player_fantasy_career")
        if cache and cache.exists("player_fantasy_career")
        else get_available_columns(conn, db_name, "player_fantasy_career")
    )
    season_has_nfl_id = "nfl_player_id" in season_cols
    career_has_nfl_id = "nfl_player_id" in career_cols

    # Best single-game manager_lamar
    try:
        row = conn.execute(f"""
            SELECT f.player, f.manager, f.year, f.week, f.manager_lamar,
                   {headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)} as headshot_url
            FROM {central_table('player_fantasy')} f
            WHERE f.manager_lamar IS NOT NULL AND f.is_started = 1
              AND {league_db_filter(db_name, 'f')}
            ORDER BY f.manager_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            leaders["best_game_lamar_player"] = row[0]
            leaders["best_game_lamar_manager"] = row[1]
            leaders["best_game_lamar_year"] = row[2]
            leaders["best_game_lamar_week"] = row[3]
            leaders["best_game_lamar_value"] = round(float(row[4]), 2) if row[4] else 0
            leaders["best_game_lamar_headshot"] = row[5]
    except Exception as e:
        log(f"  [WARN] Failed to compute best game LAMAR: {e}")

    # Best single-game clutch_equity (only if column exists and has non-zero values)
    if has_clutch:
        try:
            row = conn.execute(f"""
                {_late_clutch_weeks_cte(db_name)}
                SELECT f.player, f.manager, f.year, f.week, f.clutch_equity,
                       {headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)} as headshot_url
                FROM {central_table('player_fantasy')} f
                WHERE f.clutch_equity IS NOT NULL AND f.clutch_equity != 0 AND f.is_started = 1
                  AND {league_db_filter(db_name, 'f')}
                  AND {_late_clutch_filter_sql('f')}
                ORDER BY f.clutch_equity DESC LIMIT 1
            """).fetchone()
            if row:
                leaders["best_game_clutch_player"] = row[0]
                leaders["best_game_clutch_manager"] = row[1]
                leaders["best_game_clutch_year"] = row[2]
                leaders["best_game_clutch_week"] = row[3]
                leaders["best_game_clutch_value"] = round(float(row[4]), 2) if row[4] else 0
                leaders["best_game_clutch_headshot"] = row[5]
        except Exception:  # noqa: broad-except
            pass
    # Best season manager_lamar (from pre-aggregated season table)
    try:
        row = conn.execute(f"""
            SELECT fs.player, fs.managers, fs.year,
                   fs.manager_lamar as total_lamar,
                   {headshot_subquery('fs', None, platform, has_nfl_id=season_has_nfl_id)} as headshot_url
            FROM {central_table('player_fantasy_season')} fs
            WHERE fs.manager_lamar IS NOT NULL
              AND {league_db_filter(db_name, 'fs')}
              AND {rostered_filter_sql('fs', 'managers')}
            ORDER BY fs.manager_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            leaders["best_season_lamar_player"] = row[0]
            leaders["best_season_lamar_manager"] = row[1]
            leaders["best_season_lamar_year"] = row[2]
            leaders["best_season_lamar_value"] = round(float(row[3]), 2) if row[3] else 0
            leaders["best_season_lamar_headshot"] = row[4]
    except Exception as e:
        log(f"  [WARN] Failed to compute best season LAMAR: {e}")

    # Best season clutch_equity (from pre-aggregated season table)
    if has_clutch:
        try:
            row = conn.execute(f"""
                SELECT fs.player, fs.managers, fs.year,
                       fs.clutch_equity as total_clutch,
                       {headshot_subquery('fs', None, platform, has_nfl_id=season_has_nfl_id)} as headshot_url
                FROM {central_table('player_fantasy_season')} fs
                WHERE fs.clutch_equity IS NOT NULL AND fs.clutch_equity != 0
                  AND {league_db_filter(db_name, 'fs')}
                  AND {rostered_filter_sql('fs', 'managers')}
                ORDER BY fs.clutch_equity DESC LIMIT 1
            """).fetchone()
            if row:
                leaders["best_season_clutch_player"] = row[0]
                leaders["best_season_clutch_manager"] = row[1]
                leaders["best_season_clutch_year"] = row[2]
                leaders["best_season_clutch_value"] = round(float(row[3]), 2) if row[3] else 0
                leaders["best_season_clutch_headshot"] = row[4]
        except Exception:  # noqa: broad-except
            pass
    # Best career manager_lamar (from pre-aggregated career table)
    try:
        row = conn.execute(f"""
            SELECT fc.player, fc.managers,
                   fc.manager_lamar as total_lamar,
                   {headshot_subquery('fc', None, platform, has_nfl_id=career_has_nfl_id)} as headshot_url
            FROM {central_table('player_fantasy_career')} fc
            WHERE fc.manager_lamar IS NOT NULL
              AND {league_db_filter(db_name, 'fc')}
              AND {rostered_filter_sql('fc', 'managers')}
            ORDER BY fc.manager_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            leaders["best_career_lamar_player"] = row[0]
            leaders["best_career_lamar_manager"] = row[1]
            leaders["best_career_lamar_value"] = round(float(row[2]), 2) if row[2] else 0
            leaders["best_career_lamar_headshot"] = row[3]
    except Exception as e:
        log(f"  [WARN] Failed to compute best career LAMAR: {e}")

    # Best career clutch_equity (from pre-aggregated career table)
    if has_clutch:
        try:
            row = conn.execute(f"""
                SELECT fc.player, fc.managers,
                       fc.clutch_equity as total_clutch,
                       {headshot_subquery('fc', None, platform, has_nfl_id=career_has_nfl_id)} as headshot_url
                FROM {central_table('player_fantasy_career')} fc
                WHERE fc.clutch_equity IS NOT NULL AND fc.clutch_equity != 0
                  AND {league_db_filter(db_name, 'fc')}
                  AND {rostered_filter_sql('fc', 'managers')}
                ORDER BY fc.clutch_equity DESC LIMIT 1
            """).fetchone()
            if row:
                leaders["best_career_clutch_player"] = row[0]
                leaders["best_career_clutch_manager"] = row[1]
                leaders["best_career_clutch_value"] = round(float(row[2]), 2) if row[2] else 0
                leaders["best_career_clutch_headshot"] = row[3]
        except Exception:  # noqa: broad-except
            pass
    return leaders


def _compute_draft_highlights(
    conn, db_name: str, year: int = None, platform: str = "yahoo", cache: "ColumnCache | None" = None
) -> dict[str, Any]:
    """Compute draft highlights (best/worst picks).

    Uses pick_quality_zscore when available and sufficient data exists,
    otherwise falls back to LAMAR-based ranking.

    Args:
        conn: Database connection
        db_name: Database name
        year: Optional year filter
        platform: 'yahoo' or 'sleeper' - determines which player ID column and mapping table to use
        cache: Optional ColumnCache for metadata queries
    """
    highlights = {}
    configure_table_catalog(conn)

    # Check which pick quality column exists
    # Prefer draft_value_zscore (uses slot baselines, stable across years)
    # over pick_quality_zscore (per-year calculation, can be inflated)
    draft_cols = cache.columns("draft") if cache else get_available_columns(conn, db_name, "draft")
    player_id_col = select_platform_player_id_column(draft_cols, platform_hint=platform) or "yahoo_player_id"
    draft_has_nfl_id = "nfl_player_id" in draft_cols
    quality_col = None
    if "draft_value_zscore" in draft_cols:
        quality_col = "draft_value_zscore"
    elif "pick_quality_zscore" in draft_cols:
        quality_col = "pick_quality_zscore"
    elif "pick_score" in draft_cols:
        quality_col = "pick_score"
    elif "pick_quality_score" in draft_cols:
        quality_col = "pick_quality_score"

    # Check for keeper columns (TRY_CAST for safety — column may be VARCHAR)
    if "is_keeper" in draft_cols:
        keeper_filter = "AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0"
    elif "is_keeper_status" in draft_cols:
        keeper_filter = "AND COALESCE(TRY_CAST(d.is_keeper_status AS INTEGER), 0) = 0"
    else:
        keeper_filter = ""

    year_filter = f"AND d.year = {year}" if year else ""

    # Check for cost and pick columns (auction vs snake)
    has_cost = "cost" in draft_cols
    has_pick = "pick" in draft_cols
    cost_select = ", d.cost" if has_cost else ", NULL as cost"
    pick_select = ", d.pick" if has_pick else ", NULL as pick"

    # Determine if we have enough quality scores to use them.
    # Need at least 2 distinct score values for meaningful best/worst.
    use_quality_col = False
    if quality_col:
        try:
            count_result = conn.execute(f"""
                SELECT COUNT(DISTINCT ROUND(TRY_CAST(d.{quality_col} AS DOUBLE), 6))
                FROM {central_table('draft')} d
                WHERE d.player IS NOT NULL AND d.{quality_col} IS NOT NULL
                  AND {league_db_filter(db_name, 'd')}
                  {keeper_filter} {year_filter}
                  AND d.position NOT IN ('DEF', 'K', 'DST')
            """).fetchone()
            if count_result and count_result[0] >= 2:
                use_quality_col = True
        except Exception:  # noqa: broad-except
            pass
    # Build LAMAR expression based on which columns exist
    # IMPORTANT: DuckDB COALESCE validates all columns at parse time, so we can't use
    # COALESCE(d.manager_lamar, d.lamar) if one doesn't exist - check columns first
    if "manager_lamar" in draft_cols and "lamar" in draft_cols:
        lamar_expr = "COALESCE(d.manager_lamar, d.lamar, 0)"
    elif "manager_lamar" in draft_cols:
        lamar_expr = "COALESCE(d.manager_lamar, 0)"
    elif "lamar" in draft_cols:
        lamar_expr = "COALESCE(d.lamar, 0)"
    else:
        lamar_expr = "0"  # No LAMAR columns available

    # Choose ordering column: quality score if sufficient data, else LAMAR
    if use_quality_col:
        order_col = quality_col
        quality_filter = f"AND d.{quality_col} IS NOT NULL"
    else:
        if lamar_expr == "0":
            # No LAMAR columns - skip draft highlights
            return highlights
        order_col = lamar_expr
        quality_filter = f"AND {lamar_expr} IS NOT NULL"

    # Best pick
    # Use player_id -> appropriate mapping table for reliable headshot lookup
    # (handles name variants like "James Cook III" vs "James Cook")
    try:
        row = conn.execute(f"""
            SELECT d.player, d.manager, d.position, d.year, d.round,
                   {lamar_expr} as lamar,
                   {headshot_subquery('d', player_id_col, platform, has_nfl_id=draft_has_nfl_id)} as headshot_url
                   {cost_select}{pick_select}
            FROM {central_table('draft')} d
            WHERE d.player IS NOT NULL
              AND {league_db_filter(db_name, 'd')}
              {quality_filter} {keeper_filter} {year_filter}
              AND d.position NOT IN ('DEF', 'K', 'DST')
            ORDER BY {order_col} DESC LIMIT 1
        """).fetchone()
        if row:
            highlights["best_pick_player"] = row[0]
            highlights["best_pick_manager"] = row[1]
            highlights["best_pick_position"] = row[2]
            highlights["best_pick_year"] = row[3]
            highlights["best_pick_round"] = row[4]
            highlights["best_pick_lamar"] = round(float(row[5]), 2) if row[5] else 0
            highlights["best_pick_headshot"] = row[6]
            if row[7] is not None:
                highlights["best_pick_cost"] = round(float(row[7]), 0)
            if row[8] is not None:
                highlights["best_pick_pick"] = int(row[8])
    except Exception as e:
        log(f"  [WARN] Failed to compute best pick: {e}")

    # Worst pick
    try:
        row = conn.execute(f"""
            SELECT d.player, d.manager, d.position, d.year, d.round,
                   {lamar_expr} as lamar,
                   {headshot_subquery('d', player_id_col, platform, has_nfl_id=draft_has_nfl_id)} as headshot_url
                   {cost_select}{pick_select}
            FROM {central_table('draft')} d
            WHERE d.player IS NOT NULL
              AND {league_db_filter(db_name, 'd')}
              {quality_filter} {keeper_filter} {year_filter}
              AND d.position NOT IN ('DEF', 'K', 'DST')
            ORDER BY {order_col} ASC LIMIT 1
        """).fetchone()
        if row:
            highlights["worst_pick_player"] = row[0]
            highlights["worst_pick_manager"] = row[1]
            highlights["worst_pick_position"] = row[2]
            highlights["worst_pick_year"] = row[3]
            highlights["worst_pick_round"] = row[4]
            highlights["worst_pick_lamar"] = round(float(row[5]), 2) if row[5] else 0
            highlights["worst_pick_headshot"] = row[6]
            if row[7] is not None:
                highlights["worst_pick_cost"] = round(float(row[7]), 0)
            if row[8] is not None:
                highlights["worst_pick_pick"] = int(row[8])
    except Exception as e:
        log(f"  [WARN] Failed to compute worst pick: {e}")

    return highlights


def _compute_transaction_highlights(
    conn, db_name: str, year: int = None, platform: str = "yahoo", cache: "ColumnCache | None" = None
) -> dict[str, Any]:
    """Compute transaction highlights (best pickup, worst drop).

    Uses LAMAR when available, falls back to total fantasy points ROS when LAMAR
    columns are empty/zero (common for current season before full calculations run).

    Args:
        conn: Database connection
        db_name: Database name
        year: Optional year filter
        platform: 'yahoo' or 'sleeper' - determines which player ID column and mapping table to use
        cache: Optional ColumnCache for metadata queries
    """
    highlights = {}
    configure_table_catalog(conn)
    year_filter = f"AND t.year = {year}" if year else ""

    if cache:
        if not cache.exists("transactions"):
            return highlights
    else:
        if not table_exists(conn, db_name, "transactions"):
            return highlights

    # Check which columns exist
    txn_cols = cache.columns("transactions") if cache else get_available_columns(conn, db_name, "transactions")
    player_id_col = select_platform_player_id_column(txn_cols, platform_hint=platform) or "yahoo_player_id"
    txn_has_nfl_id = "nfl_player_id" in txn_cols
    nfl_id_select = "t.NFL_player_id" if txn_has_nfl_id else "NULL"
    platform_id_select = f"t.{player_id_col}" if player_id_col in txn_cols else "NULL"

    # Build LAMAR expression with fallbacks
    # Priority: manager_lamar_ros_managed > player_lamar_ros_total > total_points_ros_total
    lamar_parts = []
    if "manager_lamar_ros_managed" in txn_cols:
        lamar_parts.append("NULLIF(t.manager_lamar_ros_managed, 0)")
    if "player_lamar_ros_total" in txn_cols:
        lamar_parts.append("NULLIF(t.player_lamar_ros_total, 0)")
    if "total_points_ros_total" in txn_cols:
        lamar_parts.append("NULLIF(t.total_points_ros_total, 0)")
    lamar_parts.append("0")
    lamar_expr = f"COALESCE({', '.join(lamar_parts)})"
    # Build drop regret expression with fallbacks
    # For worst drops, we want the player's LAMAR remaining after they were dropped
    # This shows how much value was "left on the table" by dropping them
    drop_parts = []
    if "drop_regret_score" in txn_cols:
        drop_parts.append("NULLIF(t.drop_regret_score, 0)")
    if "player_lamar_ros_total" in txn_cols:
        drop_parts.append("NULLIF(t.player_lamar_ros_total, 0)")
    if "player_lamar_ros" in txn_cols:  # Fallback when _total is 0
        drop_parts.append("NULLIF(t.player_lamar_ros, 0)")
    if "total_points_ros_total" in txn_cols:
        drop_parts.append("NULLIF(t.total_points_ros_total, 0)")
    drop_parts.append("0")
    drop_expr = f"COALESCE({', '.join(drop_parts)})"

    # Best pickup - need meaningful value (> 0) to be considered
    try:
        row = conn.execute(f"""
            SELECT t.player, t.manager, t.year, t.week,
                   {lamar_expr} as lamar,
                   {nfl_id_select} AS nfl_player_id,
                   {platform_id_select} AS platform_player_id
            FROM {central_table('transactions')} t
            WHERE t.transaction_type = 'add' AND t.player IS NOT NULL {year_filter}
              AND {league_db_filter(db_name, 't')}
              AND {lamar_expr} > 0
            ORDER BY lamar DESC LIMIT 1
        """).fetchone()
        if row:
            highlights["best_pickup_player"] = row[0]
            highlights["best_pickup_manager"] = row[1]
            highlights["best_pickup_year"] = row[2]
            highlights["best_pickup_week"] = row[3]
            highlights["best_pickup_lamar"] = round(float(row[4]), 2) if row[4] else 0
            highlights["best_pickup_headshot"] = _lookup_headshot_for_player(
                conn,
                player=row[0],
                platform=platform,
                player_id_col=player_id_col,
                nfl_player_id=row[5],
                platform_player_id=row[6],
            )
    except Exception as e:
        raise RuntimeError(f"Failed to compute best pickup: {e}") from e

    # Worst drop - need meaningful value (> 0) to be considered a "bad" drop
    try:
        row = conn.execute(f"""
            SELECT t.player, t.manager, t.year, t.week,
                   {drop_expr} as lamar,
                   {nfl_id_select} AS nfl_player_id,
                   {platform_id_select} AS platform_player_id
            FROM {central_table('transactions')} t
            WHERE t.transaction_type = 'drop' AND t.player IS NOT NULL {year_filter}
              AND {league_db_filter(db_name, 't')}
              AND {drop_expr} > 0
            ORDER BY lamar DESC LIMIT 1
        """).fetchone()
        if row:
            highlights["worst_drop_player"] = row[0]
            highlights["worst_drop_manager"] = row[1]
            highlights["worst_drop_year"] = row[2]
            highlights["worst_drop_week"] = row[3]
            highlights["worst_drop_lamar"] = round(float(row[4]), 2) if row[4] else 0
            highlights["worst_drop_headshot"] = _lookup_headshot_for_player(
                conn,
                player=row[0],
                platform=platform,
                player_id_col=player_id_col,
                nfl_player_id=row[5],
                platform_player_id=row[6],
            )
    except Exception as e:
        raise RuntimeError(f"Failed to compute worst drop: {e}") from e

    return highlights


def _trade_partner_sql(txn_cols: Sequence[str]) -> tuple[str, str]:
    """Return trade-partner SQL that tolerates numeric platform manager IDs."""
    if "source_manager" not in txn_cols:
        return "NULL as partner", ""

    source_manager = "CAST(t.source_manager AS VARCHAR)"
    return (
        f"STRING_AGG(DISTINCT {source_manager}, ', ' ORDER BY {source_manager}) as partner",
        f"AND t.source_manager IS NOT NULL AND TRIM({source_manager}) <> ''",
    )


def _compute_best_trade(
    conn, db_name: str, year: int = None, platform: str = "yahoo", cache: "ColumnCache | None" = None
) -> dict[str, Any]:
    """Compute best trade by net LAMAR.

    Uses LAMAR when available, falls back to total fantasy points ROS.
    """
    highlights = {}
    configure_table_catalog(conn)
    year_filter = f"AND t.year = {year}" if year else ""

    if cache:
        if not cache.exists("transactions"):
            return highlights
    else:
        if not table_exists(conn, db_name, "transactions"):
            return highlights

    # Check which columns exist for fallback
    txn_cols = cache.columns("transactions") if cache else get_available_columns(conn, db_name, "transactions")
    if "franchise_id" not in txn_cols:
        raise KeyError("franchise_id is required for manager identity")
    player_id_col = select_platform_player_id_column(txn_cols, platform_hint=platform) or "yahoo_player_id"

    # Build separate LAMAR expressions for received vs sent sides:
    # - Received (winner): manager_lamar_ros_managed — value while on YOUR roster
    # - Sent (loser): player_lamar_ros_total — value for rest of season (you don't manage them anymore)
    received_lamar_expr = _trade_received_lamar_expr(txn_cols)
    sent_lamar_expr = _trade_sent_lamar_expr(txn_cols)

    trade_received_filter, trade_sent_filter = _trade_direction_filters()

    # Build headshot lookup — prefer NFL_player_id join, fallback to name match
    # Use ANY_VALUE to pick one headshot per NFL_player_id (DSTs can have multiple logos across years)
    has_nfl_id = "nfl_player_id" in txn_cols
    headshot_agg = (
        f"STRING_AGG({headshot_subquery('t', player_id_col, platform, has_nfl_id=has_nfl_id)}, "
        f"'|||' ORDER BY t.player) as headshots"
    )
    partner_select, partner_filter = _trade_partner_sql(txn_cols)

    try:
        if "trade_asset_lamar" in txn_cols:
            # The canonical enrichment mirrors each received asset onto its
            # sent perspective. Validate that existing contract before treating
            # a zero-net package as a legitimate empty highlight.
            from multi_league.transformations.transaction.sql_transaction_enrichments import _trade_asset_key_expr

            asset_key = _trade_asset_key_expr(txn_cols, "t")
            partner_asset_key = _trade_asset_key_expr(txn_cols, "p")
            invalid = conn.execute(f"""
                SELECT COUNT(*) FROM {central_table('transactions')} t
                WHERE {league_db_filter(db_name, 't')} {year_filter}
                  AND t.transaction_type IN ('trade', 'trade_pick')
                  AND (
                    t.trade_asset_lamar IS NULL OR NOT isfinite(t.trade_asset_lamar)
                    OR t.trade_direction IS NULL OR t.trade_direction NOT IN ('received', 'sent')
                    OR NOT EXISTS (
                        SELECT 1 FROM {central_table('transactions')} p
                        WHERE p.db_name = t.db_name AND p.year = t.year
                          AND p.transaction_id = t.transaction_id
                          AND p.transaction_type = t.transaction_type
                          AND p.franchise_id = t.source_franchise_id
                          AND p.source_franchise_id = t.franchise_id
                          AND p.trade_direction = CASE t.trade_direction
                              WHEN 'received' THEN 'sent' ELSE 'received' END
                          AND {partner_asset_key} = {asset_key}
                          AND p.trade_asset_lamar = t.trade_asset_lamar
                    )
                  )
            """).fetchone()[0]
            if invalid:
                raise IncompleteTradeMirrorError(
                    f"{invalid} trade assets lack complete mirrored valuations"
                )
        row = conn.execute(f"""
            WITH trade_received AS (
                SELECT
                    t.transaction_id,
                    t.franchise_id,
                    t.year,
                    MIN(t.week) as week,
                    MAX(t.manager) as manager,
                    STRING_AGG(t.player, ', ' ORDER BY t.player) as winner_players,
                    {headshot_agg.replace("as headshots", "as winner_headshots") if headshot_agg != "NULL as headshots" else "NULL as winner_headshots"},
                    SUM({received_lamar_expr}) as winner_lamar
                FROM {central_table('transactions')} t
                WHERE t.transaction_type IN ('trade', 'trade_pick') AND t.player IS NOT NULL {year_filter}
                  AND t.franchise_id IS NOT NULL AND TRIM(CAST(t.franchise_id AS VARCHAR)) <> ''
                  AND {league_db_filter(db_name, 't')}
                  {trade_received_filter}
                GROUP BY t.transaction_id, t.franchise_id, t.year
            ),
            trade_sent AS (
                SELECT
                    t.transaction_id,
                    t.franchise_id,
                    t.year,
                    MIN(t.week) as week,
                    MAX(t.manager) as manager,
                    STRING_AGG(t.player, ', ' ORDER BY t.player) as loser_players,
                    {headshot_agg.replace("as headshots", "as loser_headshots") if headshot_agg != "NULL as headshots" else "NULL as loser_headshots"},
                    SUM({sent_lamar_expr}) as loser_lamar
                FROM {central_table('transactions')} t
                WHERE t.transaction_type IN ('trade', 'trade_pick') AND t.player IS NOT NULL {year_filter}
                  AND t.franchise_id IS NOT NULL AND TRIM(CAST(t.franchise_id AS VARCHAR)) <> ''
                  AND {league_db_filter(db_name, 't')}
                  {trade_sent_filter}
                GROUP BY t.transaction_id, t.franchise_id, t.year
            ),
            trade_partners AS (
                SELECT
                    transaction_id,
                    franchise_id,
                    year,
                    {partner_select}
                FROM {central_table('transactions')} t
                WHERE t.transaction_type IN ('trade', 'trade_pick') AND t.player IS NOT NULL {year_filter}
                  AND t.franchise_id IS NOT NULL AND TRIM(CAST(t.franchise_id AS VARCHAR)) <> ''
                  {partner_filter}
                  AND {league_db_filter(db_name, 't')}
                GROUP BY transaction_id, franchise_id, year
            ),
            trade_summary AS (
                SELECT
                    COALESCE(r.transaction_id, s.transaction_id) as transaction_id,
                    COALESCE(r.franchise_id, s.franchise_id) as franchise_id,
                    COALESCE(r.year, s.year) as year,
                    COALESCE(r.week, s.week) as week,
                    COALESCE(r.manager, s.manager) as winner,
                    COALESCE(r.winner_players, '') as winner_players,
                    r.winner_headshots,
                    COALESCE(r.winner_lamar, 0) as winner_lamar,
                    COALESCE(p.partner, '') as loser,
                    COALESCE(s.loser_players, '') as loser_players,
                    s.loser_headshots,
                    COALESCE(s.loser_lamar, 0) as loser_lamar,
                    COALESCE(r.winner_lamar, 0) - COALESCE(s.loser_lamar, 0) as net_lamar
                FROM trade_received r
                FULL OUTER JOIN trade_sent s
                  ON r.transaction_id = s.transaction_id
                 AND r.franchise_id = s.franchise_id
                 AND r.year = s.year
                LEFT JOIN trade_partners p
                  ON COALESCE(r.transaction_id, s.transaction_id) = p.transaction_id
                 AND COALESCE(r.franchise_id, s.franchise_id) = p.franchise_id
                 AND COALESCE(r.year, s.year) = p.year
            )
            SELECT transaction_id, year, week,
                   winner, winner_players, winner_headshots, winner_lamar,
                   loser, loser_players, loser_headshots, loser_lamar,
                   net_lamar
            FROM trade_summary
            WHERE net_lamar > 0
            ORDER BY net_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            highlights["winner"] = row[3]
            highlights["winner_players"] = row[4]
            highlights["winner_headshots"] = row[5]
            highlights["winner_lamar"] = round(float(row[6]), 2) if row[6] else 0
            highlights["loser"] = row[7]
            highlights["loser_players"] = row[8]
            highlights["loser_headshots"] = row[9]
            highlights["loser_lamar"] = round(float(row[10]), 2) if row[10] else 0
            highlights["net_lamar"] = round(float(row[11]), 2) if row[11] else 0
            highlights["year"] = row[1]
            highlights["week"] = row[2]
    except IncompleteTradeMirrorError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to compute best trade for {db_name}: {e}") from e

    # Explicit nullable DDL fields mean the calculation succeeded with no
    # qualifying winner. A missing source or failed query must not produce this.
    from multi_league.core.aggregate_ddl import HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES

    return {
        **{column: None for column in HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES if column != "db_name"},
        **highlights,
    }


# ============================================================================
# MANAGER RANKINGS COMPUTATION
# ============================================================================


def compute_manager_rankings(
    conn, db_name: str, median_years: list[int] | None = None, cache: "ColumnCache | None" = None
) -> pd.DataFrame:
    """Compute manager career rankings (one row per manager)."""
    if cache is None:
        configure_table_catalog(conn)
        cache = ColumnCache(conn, get_active_catalog())

    log("Computing manager rankings...")

    # Check if power_rating, median, and optional columns exist
    matchup_cols = cache.columns("matchup")
    has_power_rating = "power_rating" in matchup_cols
    matchup_season_cols = cache.columns("matchup_season")
    has_season_power_rating = {
        "db_name",
        "year",
        "franchise_id",
        "power_rating",
    }.issubset(matchup_season_cols)
    has_above_median = "above_league_median" in matchup_cols
    has_champion = "champion" in matchup_cols

    power_rating_select = "AVG(power_rating) as avg_power_rating" if has_power_rating else "NULL as avg_power_rating"
    season_power_cte = ""
    manager_stats_from = "FROM manager_year_stats"
    manager_power_select = "AVG(avg_power_rating) as avg_power_rating"
    if has_season_power_rating:
        season_power_cte = f""",
        season_power AS (
            SELECT franchise_id, year, AVG(power_rating) AS power_rating
            FROM {central_table('matchup_season')}
            WHERE {league_db_filter(db_name)}
            GROUP BY franchise_id, year
        )"""
        manager_stats_from = (
            "FROM manager_year_stats "
            "LEFT JOIN season_power USING (franchise_id, year)"
        )
        manager_power_select = (
            "AVG(COALESCE(season_power.power_rating, avg_power_rating)) "
            "as avg_power_rating"
        )

    # Detect H2H+Median scoring per year for proper win/loss calculation
    # Only use median expressions if the columns actually exist in the matchup table
    has_below_median = "below_league_median" in matchup_cols
    if median_years is None:
        median_years = detect_h2h_median_years_from_db(conn, db_name)
    use_median_any = bool(median_years) and has_above_median and has_below_median
    wins_expr = build_wins_sql(use_median_any, median_years=median_years if use_median_any else None)
    losses_expr = build_losses_sql(use_median_any, median_years=median_years if use_median_any else None)
    ties_expr = build_ties_sql()

    scoring_type = "H2H+Median" if use_median_any else "H2H"
    log(f"  Scoring type: {scoring_type}")

    franchise_select = "MAX(franchise_id) as franchise_id,"

    df = conn.execute(f"""
        WITH {_typed_matchup_dedupe_ctes_sql(db_name)},
        manager_year_stats AS (
            SELECT franchise_id, year,
                   MAX(manager) as manager,
                   -- Include reg-season + championship playoff wins, exclude consolation.
                   -- Matches matchup_season scope (is_consolation=0) and the
                   -- overview_career_wins_match validator.
                   SUM(CASE WHEN COALESCE(is_consolation, 0) = 0 THEN {wins_expr} ELSE 0 END) as wins,
                   SUM(CASE WHEN COALESCE(is_consolation, 0) = 0 THEN {losses_expr} ELSE 0 END) as losses,
                   SUM(CASE WHEN COALESCE(is_consolation, 0) = 0 THEN {ties_expr} ELSE 0 END) as ties,
                   {"MAX(COALESCE(champion, 0))" if has_champion else "0"} as is_champion,
                   MAX(CASE WHEN is_playoffs = 1 AND COALESCE(is_consolation, 0) = 0 THEN 1 ELSE 0 END) as made_playoffs,
                   {power_rating_select}
            FROM base_matchup
            GROUP BY franchise_id, year
        ){season_power_cte},
        manager_stats AS (
            SELECT ARG_MAX(manager, year) as manager,
                   franchise_id,
                   SUM(wins) as wins, SUM(losses) as losses, SUM(ties) as ties,
                   SUM(is_champion) as championships, SUM(made_playoffs) as playoff_appearances,
                   COUNT(*) as total_years, MIN(year) as first_year, MAX(year) as last_year,
                   {manager_power_select}
            {manager_stats_from}
            GROUP BY franchise_id
        )
        SELECT manager, franchise_id, wins, losses, ties,
               ROUND(CAST(wins AS FLOAT) / NULLIF(wins + losses + ties, 0), 3) as win_pct,
               championships, playoff_appearances, total_years as seasons,
               ROUND(avg_power_rating, 1) as power_rating, first_year, last_year,
               ROW_NUMBER() OVER (ORDER BY wins DESC, championships DESC, franchise_id ASC) as career_rank
        FROM manager_stats
        ORDER BY wins DESC, championships DESC, franchise_id ASC
    """).fetchdf()

    log(f"  Computed rankings for {len(df)} managers")
    return df


# ============================================================================
# CURRENT STANDINGS COMPUTATION
# ============================================================================


def compute_current_standings(
    conn, db_name: str, median_years: list[int] | None = None, cache: "ColumnCache | None" = None
) -> pd.DataFrame:
    """Compute current season standings (one row per manager)."""
    if cache is None:
        configure_table_catalog(conn)
        cache = ColumnCache(conn, get_active_catalog())

    log("Computing current standings...")

    # Get latest year/week with actual scores (skip preseason skeleton rows)
    latest = conn.execute(f"""
        SELECT MAX(TRY_CAST(year AS INT)) as year
        FROM {central_table('matchup')}
        WHERE {league_db_filter(db_name)} AND team_points > 0
    """).fetchone()

    if not latest or not latest[0]:
        log("  No scored matchup data found")
        return pd.DataFrame()

    year = latest[0]
    week_row = conn.execute(f"""
        SELECT MAX(TRY_CAST(week AS INT)) as week
        FROM {central_table('matchup')}
        WHERE {league_db_filter(db_name)} AND TRY_CAST(year AS INT) = {int(year)}
    """).fetchone()
    week = week_row[0] if week_row else 1

    # Check available columns
    matchup_cols = cache.columns("matchup")
    has_power_rating = "power_rating" in matchup_cols
    has_p_playoffs = "p_playoffs" in matchup_cols
    has_p_champ = "p_champ" in matchup_cols

    power_select = "MAX(power_rating) as power_rating" if has_power_rating else "NULL as power_rating"
    p_playoffs_select = "MAX(p_playoffs) as p_playoffs" if has_p_playoffs else "NULL as p_playoffs"
    p_champ_select = "MAX(p_champ) as p_champ" if has_p_champ else "NULL as p_champ"

    # Detect H2H+Median scoring per year
    # Only use median expressions if the columns actually exist
    has_below_median = "below_league_median" in matchup_cols
    has_above_median = "above_league_median" in matchup_cols
    if median_years is None:
        median_years = detect_h2h_median_years_from_db(conn, db_name)
    use_median_for_season = (year in median_years if median_years else False) and has_above_median and has_below_median
    wins_expr = build_wins_sql(use_median_for_season, median_years=median_years if use_median_for_season else None)
    losses_expr = build_losses_sql(use_median_for_season, median_years=median_years if use_median_for_season else None)
    ties_expr = build_ties_sql()

    # week_stats picks the LATEST non-bye row per franchise across the season,
    # not just rows at week = max_week. Managers eliminated before the final
    # week (e.g. round-1 playoff losers) have a phantom bye row at the final
    # week that base_matchup filters out, which used to leave power_rating /
    # p_playoffs / p_champ as NULL → "—" in the standings UI. Power rating is
    # already frozen by the matchup pipeline at the team's last contention
    # game, so the latest available row carries the correct value.
    #
    # Implementation note: ROW_NUMBER() inside another CTE chained on top of
    # base_matchup misbehaves on the Fly DuckDB build (returned "week=233"
    # values from somewhere mid-stream), so we go through MAX(week)+JOIN.
    power_rating_pick = "power_rating" if has_power_rating else "NULL"
    p_playoffs_pick = "p_playoffs" if has_p_playoffs else "NULL"
    p_champ_pick = "p_champ" if has_p_champ else "NULL"
    latest_metric_filters = ["team_points IS NOT NULL"]
    if has_power_rating:
        latest_metric_filters.append("power_rating IS NOT NULL")
    if has_p_playoffs:
        latest_metric_filters.append("p_playoffs IS NOT NULL")
    if has_p_champ:
        latest_metric_filters.append("p_champ IS NOT NULL")
    latest_metric_filter = " OR ".join(latest_metric_filters)

    df = conn.execute(f"""
        WITH {_typed_matchup_dedupe_ctes_sql(db_name)},
        season_to_date AS (
            SELECT franchise_id,
                   MAX(manager) as manager,
                   SUM({wins_expr}) as wins,
                   SUM({losses_expr}) as losses,
                   SUM({ties_expr}) as ties,
                   SUM(team_points) as points_for, MAX(team_name) as team_name
            FROM base_matchup
            WHERE year = {int(year)} AND week <= {int(week)} AND COALESCE(is_consolation, 0) = 0
            GROUP BY franchise_id
        ),
        latest_played AS (
            SELECT franchise_id, MAX(week) AS max_wk
            FROM latest_matchup
            WHERE year = {int(year)}
              AND week <= {int(week)}
              AND ({latest_metric_filter})
            GROUP BY franchise_id
        ),
        week_stats AS (
            SELECT bm.franchise_id, bm.manager,
                   {power_rating_pick} AS power_rating,
                   {p_playoffs_pick} AS p_playoffs,
                   {p_champ_pick} AS p_champ
            FROM latest_matchup bm
            JOIN latest_played lp
                ON bm.franchise_id = lp.franchise_id AND bm.week = lp.max_wk
            WHERE bm.year = {int(year)}
        )
        SELECT std.manager, std.franchise_id,
               std.team_name, std.wins, std.losses, std.ties,
               ROUND(std.points_for, 1) as points_for,
               ROUND(CAST(std.wins AS FLOAT) / NULLIF(std.wins + std.losses + std.ties, 0), 3) as win_pct,
               ROUND(ws.power_rating, 1) as power_rating,
               ROUND(ws.p_playoffs, 1) as p_playoffs,
               ROUND(ws.p_champ, 1) as p_champ,
               ROW_NUMBER() OVER (ORDER BY std.wins DESC, std.points_for DESC) as standings_rank
        FROM season_to_date std
        LEFT JOIN week_stats ws ON std.franchise_id = ws.franchise_id
        ORDER BY std.wins DESC, std.points_for DESC
    """).fetchdf()

    log(f"  Computed standings for {len(df)} managers")
    return df


# ============================================================================
# TOP RIVALRIES COMPUTATION
# ============================================================================


def compute_top_rivalries(conn, db_name: str, limit: int = 20, cache: "ColumnCache | None" = None) -> pd.DataFrame:
    """Compute top rivalry matchups (one row per rivalry pair).

    Note: Rivalries are based on head-to-head wins only (not median wins),
    since median is about league-wide performance, not who beats who directly.
    """
    log("Computing top rivalries...")
    configure_table_catalog(conn)

    df = conn.execute(f"""
        WITH {_typed_matchup_dedupe_ctes_sql(db_name)},
        matchups AS (
            SELECT manager, opponent, year, week,
                   franchise_id,
                   opponent_franchise_id,
                   win, loss, COALESCE(tie, 0) as tie,
                   team_points, opponent_points,
                   ABS(team_points - opponent_points) as margin
            FROM base_matchup
            WHERE opponent_franchise_id IS NOT NULL
              AND TRIM(CAST(opponent_franchise_id AS VARCHAR)) != ''
              AND franchise_id < opponent_franchise_id  -- Ensure each matchup counted once
              AND COALESCE(is_consolation, 0) = 0
              AND opponent_points IS NOT NULL
        ),
        latest_labels AS (
            SELECT
                franchise_id,
                CASE
                    WHEN manager IS NULL
                      OR TRIM(CAST(manager AS VARCHAR)) = ''
                      OR LOWER(TRIM(CAST(manager AS VARCHAR))) LIKE '--hidden--%'
                    THEN NULLIF(TRIM(CAST(team_name AS VARCHAR)), '')
                    ELSE TRIM(CAST(manager AS VARCHAR))
                END AS manager,
                NULLIF(TRIM(CAST(team_name AS VARCHAR)), '') AS team_name,
                ROW_NUMBER() OVER (
                    PARTITION BY franchise_id
                    ORDER BY CAST(year AS BIGINT) DESC, CAST(week AS BIGINT) DESC, manager DESC
                ) AS rn
            FROM base_matchup
        ),
        rivalry_stats AS (
            SELECT franchise_id as franchise_id_1,
                   opponent_franchise_id as franchise_id_2,
                   COUNT(*) as total_games,
                   SUM(win) as manager1_wins,
                   SUM(loss) as manager1_losses,
                   SUM(tie) as ties,
                   AVG(margin) as avg_margin
            FROM matchups
            GROUP BY franchise_id, opponent_franchise_id
            HAVING COUNT(*) >= 3  -- At least 3 games to be a rivalry
        ),
        labeled_rivalries AS (
            SELECT
                COALESCE(l1.manager, rs.franchise_id_1) AS raw_manager1,
                COALESCE(l2.manager, rs.franchise_id_2) AS raw_manager2,
                l1.team_name AS team_name1,
                l2.team_name AS team_name2,
                rs.*
            FROM rivalry_stats rs
            LEFT JOIN latest_labels l1
                ON rs.franchise_id_1 = l1.franchise_id AND l1.rn = 1
            LEFT JOIN latest_labels l2
                ON rs.franchise_id_2 = l2.franchise_id AND l2.rn = 1
        )
        SELECT
               CASE
                   WHEN raw_manager1 = raw_manager2
                     AND team_name1 IS NOT NULL
                     AND team_name1 != ''
                   THEN raw_manager1 || ' - ' || team_name1
                   ELSE raw_manager1
               END AS manager1,
               CASE
                   WHEN raw_manager1 = raw_manager2
                     AND team_name2 IS NOT NULL
                     AND team_name2 != ''
                   THEN raw_manager2 || ' - ' || team_name2
                   ELSE raw_manager2
               END AS manager2,
               franchise_id_1, franchise_id_2,
               total_games, manager1_wins,
               manager1_losses as manager2_wins, ties,
               -- Competitiveness: closer to 0.5 is more competitive
               1.0 - ABS(CAST(manager1_wins AS FLOAT) / NULLIF(total_games, 0) - 0.5) * 2 as competitiveness_score,
               ROUND(avg_margin, 2) as avg_margin,
               ROW_NUMBER() OVER (ORDER BY total_games DESC,
                   (1.0 - ABS(CAST(manager1_wins AS FLOAT) / NULLIF(total_games, 0) - 0.5) * 2) DESC) as rivalry_rank
        FROM labeled_rivalries
        ORDER BY total_games DESC, competitiveness_score DESC
        LIMIT {limit}
    """).fetchdf()

    log(f"  Computed {len(df)} top rivalries")
    return df


# ============================================================================
# MANAGER PROFILES COMPUTATION
# ============================================================================


def compute_all_manager_profiles(
    conn,
    db_name: str,
    platform: str = "yahoo",
    *,
    franchise_ids: set[str] | None = None,
) -> pd.DataFrame:
    """Compute profile data for all managers (one row per manager).

    Uses LocalProfileContext to pull data once into local DuckDB (~8 remote
    queries) then run all per-manager computations locally (instant).

    Args:
        conn: Database connection (remote MotherDuck)
        db_name: Database name
        platform: 'yahoo' or 'sleeper' - determines which player ID column and mapping table to use
    """

    log("Computing manager profiles...")
    t_start = time.perf_counter()

    # Phase 1: Pull data locally (~8 remote queries)
    previous_catalog = get_active_catalog()
    ctx = ScopedLocalProfileContext(conn, db_name, platform)
    ctx.setup_aliases(db_name)
    set_active_catalog(db_name)

    try:
        local = ctx.local

        managers_df = local.execute(f"""
            WITH {_typed_matchup_dedupe_ctes_sql(db_name)},
            latest_labels AS (
                SELECT
                    franchise_id,
                    manager,
                    ROW_NUMBER() OVER (
                        PARTITION BY franchise_id
                        ORDER BY
                            CASE WHEN manager IS NOT NULL AND TRIM(manager) != '' THEN 0 ELSE 1 END,
                            year DESC,
                            week DESC,
                            manager DESC
                    ) AS rn
                FROM base_matchup
            )
            SELECT manager, franchise_id
            FROM latest_labels
            WHERE rn = 1
            ORDER BY manager, franchise_id
        """).fetchdf()

        if managers_df.empty:
            log("  No managers found")
            return pd.DataFrame()
        if franchise_ids is not None:
            wanted = {str(value) for value in franchise_ids}
            managers_df = managers_df[
                managers_df["franchise_id"].astype(str).isin(wanted)
            ].copy()
            if managers_df.empty:
                log("  No changed managers found")
                return pd.DataFrame()

        current_year_row = local.execute(f'SELECT MAX(year) FROM "{db_name}".public.matchup').fetchone()
        current_year = int(current_year_row[0]) if current_year_row and current_year_row[0] else None

        # Detect scoring years and columns once (uses local connection)
        median_years = detect_h2h_median_years_from_db(local, db_name)
        if median_years:
            log("  Using H2H+Median scoring for applicable seasons")
        matchup_cols = ctx.cache.columns("matchup")

        log(f"  Processing {len(managers_df)} managers (local)...")

        # Phase 2: Compute profiles on independent cursors over the same small,
        # league-scoped in-memory database.  Every manager runs the exact same
        # queries as before; only their independent reads overlap.
        def build_profile(profile_conn, row):
            manager = row.manager
            franchise_id = row.franchise_id
            profile_cache = ColumnCache(profile_conn, "memory", schema="main")
            profile = {
                "manager": manager,
                "franchise_id": franchise_id,
                "current_year": current_year,
            }

            career = _compute_manager_career_stats(
                profile_conn, db_name, franchise_id, manager,
                median_years=median_years, matchup_cols=matchup_cols,
            )
            profile.update(career)

            badges = _compute_manager_badges(profile_conn, db_name, manager, career)
            profile["badges_list"] = badges

            draft = _compute_manager_draft_profile(
                profile_conn, db_name, franchise_id, manager,
                platform=platform, cache=profile_cache,
            )
            profile.update(draft)

            if current_year:
                draft_season = _compute_manager_draft_profile(
                    profile_conn,
                    db_name,
                    franchise_id,
                    manager,
                    year=current_year,
                    prefix="season_",
                    platform=platform,
                    cache=profile_cache,
                )
                profile.update(draft_season)

            txn = _compute_manager_txn_profile(
                profile_conn, db_name, franchise_id, manager,
                platform=platform, cache=profile_cache,
            )
            profile.update(txn)

            if current_year:
                txn_season = _compute_manager_txn_profile(
                    profile_conn,
                    db_name,
                    franchise_id,
                    manager,
                    year=current_year,
                    prefix="season_",
                    platform=platform,
                    cache=profile_cache,
                )
                profile.update(txn_season)

            trade = _compute_manager_best_trade(
                profile_conn, db_name, franchise_id, manager,
                platform=platform, cache=profile_cache,
            )
            profile.update(trade)

            if current_year:
                trade_season = _compute_manager_best_trade(
                    profile_conn,
                    db_name,
                    franchise_id,
                    manager,
                    year=current_year,
                    prefix="season_",
                    platform=platform,
                    cache=profile_cache,
                )
                profile.update(trade_season)

            rivalries = _compute_manager_rivalries(
                profile_conn, db_name, franchise_id, manager, cache=profile_cache,
            )
            profile.update(rivalries)

            leaders = _compute_manager_player_leaders(
                profile_conn, db_name, franchise_id, manager,
                platform=platform, cache=profile_cache,
            )
            profile.update(leaders)

            timeline = _compute_manager_timeline(
                profile_conn, db_name, franchise_id, manager,
                median_years=median_years, matchup_cols=matchup_cols,
                cache=profile_cache,
            )
            profile["timeline_data"] = timeline
            return profile

        profiles = _compute_profiles_concurrently(
            local,
            list(managers_df.itertuples(index=False)),
            build_profile,
        )

        elapsed = time.perf_counter() - t_start
        log(f"  Computed profiles for {len(profiles)} managers in {elapsed:.1f}s")
        return pd.DataFrame(profiles)
    finally:
        set_active_catalog(previous_catalog)
        ctx.close()


def _compute_profiles_concurrently(
    conn,
    rows: Sequence[Any],
    build: Callable[[Any, Any], dict[str, Any] | Any],
    *,
    max_workers: int = 8,
) -> list[Any]:
    """Map independent manager reads across cursors, preserving row order."""
    if not rows:
        return []
    worker_count = max(1, min(int(max_workers), len(rows)))
    if worker_count == 1:
        return [build(conn, row) for row in rows]

    def run(row):
        cursor = conn.cursor()
        try:
            return build(cursor, row)
        finally:
            cursor.close()

    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="homepage-profile",
    ) as executor:
        return list(executor.map(run, rows))


def _compute_manager_career_stats(
    conn,
    db_name: str,
    franchise_id: str,
    manager: str | None = None,
    use_median: bool = False,
    median_years: Sequence[int] | None = None,
    matchup_cols: set = None,
) -> dict[str, Any]:
    """Compute career statistics for a single manager.

    For H2H+Median leagues, regular season wins include both H2H and median wins.
    Playoff wins are H2H only (median doesn't apply in playoffs).
    """
    if matchup_cols is None:
        matchup_cols = get_available_columns(conn, db_name, "matchup")
    if "franchise_id" not in matchup_cols:
        raise KeyError("franchise_id is required for manager identity")

    profile_label = _profile_log_label(manager, franchise_id)
    franchise_filter = _franchise_filter_sql(franchise_id)

    has_champion = "champion" in matchup_cols
    has_sacko = "sacko" in matchup_cols
    has_above_median = "above_league_median" in matchup_cols
    has_below_median = "below_league_median" in matchup_cols

    # Determine per-year median usage for regular season wins/losses
    if median_years and has_above_median and has_below_median:
        years_list = ",".join(str(int(y)) for y in median_years)
        cond = f"CAST(year AS INT) IN ({years_list})"
        reg_wins_expr = f"""SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0
                               THEN COALESCE(CAST(win AS INT), 0) + CASE WHEN {cond} THEN COALESCE(CAST(above_league_median AS INT), 0) ELSE 0 END
                               ELSE 0 END)"""
        reg_losses_expr = f"""SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0
                               THEN COALESCE(CAST(loss AS INT), 0) + CASE WHEN {cond} THEN COALESCE(CAST(below_league_median AS INT), 0) ELSE 0 END
                               ELSE 0 END)"""
    elif use_median and has_above_median and has_below_median:
        # For H2H+Median: reg wins = H2H wins + median wins, playoff wins = H2H only
        reg_wins_expr = """SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0
                               THEN COALESCE(CAST(win AS INT), 0) + COALESCE(CAST(above_league_median AS INT), 0)
                               ELSE 0 END)"""
        reg_losses_expr = """SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0
                               THEN COALESCE(CAST(loss AS INT), 0) + COALESCE(CAST(below_league_median AS INT), 0)
                               ELSE 0 END)"""
    else:
        reg_wins_expr = "SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0 AND win = 1 THEN 1 ELSE 0 END)"
        reg_losses_expr = "SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0 AND loss = 1 THEN 1 ELSE 0 END)"

    # Tie expressions (ties don't have median component)
    reg_ties_expr = "SUM(CASE WHEN is_playoffs = 0 AND is_consolation = 0 AND COALESCE(tie, 0) = 1 THEN 1 ELSE 0 END)"

    try:
        row = conn.execute(f"""
            WITH game_types AS (
                SELECT year, team_points, opponent_points,
                       COALESCE(is_playoffs, 0) as is_playoffs,
                       COALESCE(is_consolation, 0) as is_consolation,
                       {"COALESCE(champion, 0)" if has_champion else "0"} as is_champion,
                       {"COALESCE(sacko, 0)" if has_sacko else "0"} as is_sacko,
                       team_name, win, loss, COALESCE(tie, 0) as tie,
                       {"COALESCE(above_league_median, 0)" if has_above_median else "0"} as above_league_median,
                       {"COALESCE(below_league_median, 0)" if has_below_median else "0"} as below_league_median
                FROM {central_table('matchup')}
                WHERE {franchise_filter}
            ),
            records AS (
                SELECT
                    {reg_wins_expr} as reg_wins,
                    {reg_losses_expr} as reg_losses,
                    {reg_ties_expr} as reg_ties,
                    SUM(CASE WHEN is_playoffs = 1 AND is_consolation = 0 AND win = 1 THEN 1 ELSE 0 END) as playoff_wins,
                    SUM(CASE WHEN is_playoffs = 1 AND is_consolation = 0 AND loss = 1 THEN 1 ELSE 0 END) as playoff_losses,
                    SUM(CASE WHEN is_playoffs = 1 AND is_consolation = 0 AND tie = 1 THEN 1 ELSE 0 END) as playoff_ties,
                    SUM(team_points) as career_points
                FROM game_types
            ),
            season_stats AS (
                SELECT year, MAX(is_champion) as is_champion, MAX(is_sacko) as is_sacko,
                       MAX(CASE WHEN is_playoffs = 1 AND is_consolation = 0 THEN 1 ELSE 0 END) as made_playoffs,
                       MAX(team_name) as team_name
                FROM game_types GROUP BY year
            )
            SELECT r.reg_wins, r.reg_losses, r.reg_ties, r.playoff_wins, r.playoff_losses, r.playoff_ties,
                   r.career_points,
                   SUM(s.is_champion) as championships, SUM(s.is_sacko) as sacko_bowls,
                   SUM(s.made_playoffs) as playoff_appearances,
                   MIN(s.year) as first_year, MAX(s.year) as last_year,
                   COUNT(DISTINCT s.year) as seasons_played,
                   (SELECT team_name FROM season_stats WHERE year = (SELECT MAX(year) FROM season_stats)) as current_team_name
            FROM records r, season_stats s
            GROUP BY r.reg_wins, r.reg_losses, r.reg_ties, r.playoff_wins, r.playoff_losses, r.playoff_ties, r.career_points
        """).fetchone()

        if not row:
            return {}

        reg_wins = int(row[0] or 0)
        reg_losses = int(row[1] or 0)
        reg_ties = int(row[2] or 0)
        playoff_wins = int(row[3] or 0)
        playoff_losses = int(row[4] or 0)
        playoff_ties = int(row[5] or 0)
        total_wins = reg_wins + playoff_wins
        total_losses = reg_losses + playoff_losses
        total_ties = reg_ties + playoff_ties
        total_games = total_wins + total_losses + total_ties
        seasons = int(row[12] or 0)
        playoff_apps = int(row[9] or 0)

        reg_denom = reg_wins + reg_losses + reg_ties
        playoff_denom = playoff_wins + playoff_losses + playoff_ties

        return {
            "total_wins": total_wins,
            "total_losses": total_losses,
            "total_ties": total_ties,
            "total_win_pct": round(total_wins / total_games, 2) if total_games > 0 else 0,
            "reg_wins": reg_wins,
            "reg_losses": reg_losses,
            "reg_ties": reg_ties,
            "reg_win_pct": round(reg_wins / reg_denom, 2) if reg_denom > 0 else 0,
            "playoff_wins": playoff_wins,
            "playoff_losses": playoff_losses,
            "playoff_ties": playoff_ties,
            "playoff_win_pct": round(playoff_wins / playoff_denom, 2) if playoff_denom > 0 else 0,
            "championships": int(row[7] or 0),
            "sacko_bowls": int(row[8] or 0),
            "playoff_appearances": playoff_apps,
            "playoff_rate": round(playoff_apps / seasons, 2) if seasons > 0 else 0,
            "first_year": int(row[10]) if row[10] else None,
            "last_year": int(row[11]) if row[11] else None,
            "seasons_played": seasons,
            "current_team_name": row[13] or "Unknown",
            "career_points": float(row[6] or 0),
        }
    except Exception as e:
        log(f"  [WARN] Failed to compute career stats for {profile_label}: {e}")
        return {}


def _compute_manager_badges(conn, db_name: str, manager: str, career: dict[str, Any]) -> str:
    """Compute achievement badges for a manager. Returns pipe-delimited string.

    Format: name:display:emoji:color:description
    """
    badges = []

    # Championship badges
    champs = career.get("championships", 0)
    if champs >= 3:
        badges.append(f"dynasty:{champs}x Champion:👑:#d4af37:Won {champs} league championships")
    elif champs >= 1:
        display = f"{champs}x Champion" if champs > 1 else "Champion"
        badges.append(
            f"champion:{display}:🏆:#d4af37:Won {'a' if champs == 1 else str(champs)} league championship{'s' if champs > 1 else ''}"
        )

    # Playoff badges
    playoff_rate = career.get("playoff_rate", 0)
    playoff_apps = career.get("playoff_appearances", 0)
    if playoff_rate >= 0.75:
        badges.append(f"perennial_contender:Perennial Contender:🔥:#ef4444:Made playoffs {playoff_rate:.0%} of seasons")
    elif playoff_apps >= 5:
        badges.append(f"playoff_veteran:Playoff Veteran:⭐:#3b82f6:Made playoffs {playoff_apps} times")

    # Win rate badges
    win_pct = career.get("total_win_pct", 0)
    if win_pct >= 0.65:
        badges.append(f"dominant:Dominant:💪:#22c55e:Career win rate of {win_pct:.0%}")
    elif win_pct >= 0.55:
        badges.append(f"consistent_winner:Consistent Winner:✅:#22c55e:Career win rate of {win_pct:.0%}")

    # Longevity badge
    seasons = career.get("seasons_played", 0)
    if seasons >= 10:
        badges.append(f"veteran:Veteran:🛡️:#8b5cf6:{seasons} seasons in the league")

    # Sacko badge (not a good thing!)
    sackos = career.get("sacko_bowls", 0)
    if sackos >= 2:
        badges.append(f"basement_dweller:Basement Dweller:💩:#6b7280:Won the Sacko Bowl {sackos} times")
    elif sackos == 1:
        badges.append("sacko_survivor:Sacko Survivor:💩:#6b7280:Won the Sacko Bowl")

    return "|".join(badges)


def _compute_manager_draft_profile(
    conn,
    db_name: str,
    franchise_id: str,
    manager: str | None = None,
    year: int = None,
    prefix: str = "",
    platform: str = "yahoo",
    cache: "ColumnCache | None" = None,
) -> dict[str, Any]:
    """Compute draft profile for a manager, optionally for a specific year.

    Args:
        conn: Database connection
        db_name: Database name
        franchise_id: Stable franchise identity
        manager: Display manager label
        year: Optional year filter (None = all-time)
        prefix: Key prefix for results (e.g., "season_" for season-specific data)
        platform: 'yahoo' or 'sleeper' - determines which player ID column and mapping table to use
    """
    profile = {}
    year_filter = f"AND d.year = {year}" if year else ""

    # Check which pick quality column exists
    # Prefer draft_value_zscore (uses slot baselines, stable across years)
    draft_cols = cache.columns("draft") if cache else get_available_columns(conn, db_name, "draft")
    if "franchise_id" not in draft_cols:
        raise KeyError("franchise_id is required for manager identity")
    franchise_filter = _franchise_filter_sql(franchise_id, "d")
    player_id_col = select_platform_player_id_column(draft_cols, platform_hint=platform) or "yahoo_player_id"
    draft_has_nfl_id = "nfl_player_id" in draft_cols
    if "draft_value_zscore" in draft_cols:
        quality_col = "draft_value_zscore"
    elif "pick_quality_zscore" in draft_cols:
        quality_col = "pick_quality_zscore"
    elif "pick_score" in draft_cols:
        quality_col = "pick_score"
    elif "pick_quality_score" in draft_cols:
        quality_col = "pick_quality_score"
    else:
        return profile

    # Keeper filter (TRY_CAST for safety — column may be VARCHAR)
    if "is_keeper" in draft_cols:
        keeper_filter = "AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0"
    elif "is_keeper_status" in draft_cols:
        keeper_filter = "AND COALESCE(TRY_CAST(d.is_keeper_status AS INTEGER), 0) = 0"
    else:
        keeper_filter = ""

    # Build LAMAR expression based on which columns exist
    # IMPORTANT: DuckDB COALESCE validates all columns at parse time
    if "manager_lamar" in draft_cols and "lamar" in draft_cols:
        lamar_expr = "COALESCE(d.manager_lamar, d.lamar, 0)"
    elif "manager_lamar" in draft_cols:
        lamar_expr = "COALESCE(d.manager_lamar, 0)"
    elif "lamar" in draft_cols:
        lamar_expr = "COALESCE(d.lamar, 0)"
    else:
        lamar_expr = "0"

    # Check for cost and pick columns (auction vs snake)
    has_cost = "cost" in draft_cols
    has_pick = "pick" in draft_cols
    cost_select = ", d.cost" if has_cost else ", NULL as cost"
    pick_select = ", d.pick" if has_pick else ", NULL as pick"

    # Best pick
    try:
        row = conn.execute(f"""
            SELECT d.player, d.round, d.year, {lamar_expr} as lamar,
                   {headshot_subquery('d', player_id_col, platform, has_nfl_id=draft_has_nfl_id)} as headshot_url
                   {cost_select}{pick_select}
            FROM {central_table('draft')} d
            WHERE {franchise_filter} AND d.{quality_col} IS NOT NULL
              {keeper_filter} {year_filter} AND d.position NOT IN ('DEF', 'K', 'DST')
            ORDER BY d.{quality_col} DESC LIMIT 1
        """).fetchone()
        if row:
            profile[f"{prefix}best_pick_player"] = row[0]
            profile[f"{prefix}best_pick_round"] = row[1]
            profile[f"{prefix}best_pick_year"] = row[2]
            profile[f"{prefix}best_pick_lamar"] = row[3]
            profile[f"{prefix}best_pick_headshot"] = row[4]
            if row[5] is not None:
                profile[f"{prefix}best_pick_cost"] = round(float(row[5]), 0)
            if row[6] is not None:
                profile[f"{prefix}best_pick_pick"] = int(row[6])
    except Exception:  # noqa: broad-except
        pass
    # Worst pick
    try:
        row = conn.execute(f"""
            SELECT d.player, d.round, d.year, {lamar_expr} as lamar,
                   {headshot_subquery('d', player_id_col, platform, has_nfl_id=draft_has_nfl_id)} as headshot_url
                   {cost_select}{pick_select}
            FROM {central_table('draft')} d
            WHERE {franchise_filter} AND d.{quality_col} IS NOT NULL
              {keeper_filter} {year_filter} AND d.position NOT IN ('DEF', 'K', 'DST')
            ORDER BY d.{quality_col} ASC LIMIT 1
        """).fetchone()
        if row:
            profile[f"{prefix}worst_pick_player"] = row[0]
            profile[f"{prefix}worst_pick_round"] = row[1]
            profile[f"{prefix}worst_pick_year"] = row[2]
            profile[f"{prefix}worst_pick_lamar"] = row[3]
            profile[f"{prefix}worst_pick_headshot"] = row[4]
            if row[5] is not None:
                profile[f"{prefix}worst_pick_cost"] = round(float(row[5]), 0)
            if row[6] is not None:
                profile[f"{prefix}worst_pick_pick"] = int(row[6])
    except Exception:  # noqa: broad-except
        pass
    # Draft grade (career or season)
    grade_year_filter = f"AND year = {year}" if year else ""
    try:
        if "manager_draft_score" in draft_cols:
            manager_grade_expr = (
                "ARG_MAX(manager_draft_grade, manager_draft_score)" if "manager_draft_grade" in draft_cols else "NULL"
            )
            draft_category_expr = (
                "COALESCE(draft_category, 'standard')" if "draft_category" in draft_cols else "'standard'"
            )
            row = conn.execute(f"""
                WITH manager_years AS (
                    SELECT year,
                           {draft_category_expr} AS draft_category,
                           manager_draft_score,
                           {manager_grade_expr} AS manager_draft_grade
                    FROM {central_table('draft')}
                    WHERE {_franchise_filter_sql(franchise_id)}
                      AND manager_draft_score IS NOT NULL
                      {grade_year_filter}
                    GROUP BY year, {draft_category_expr}, manager_draft_score
                )
                SELECT AVG(manager_draft_score) AS avg_score,
                       ARG_MAX(manager_draft_grade, manager_draft_score) AS manager_draft_grade
                FROM manager_years
            """).fetchone()
            if row and row[0] is not None:
                grade = str(row[1]) if year and row[1] is not None else _score_to_draft_grade(float(row[0]))
                profile[f"{prefix}draft_career_grade" if not prefix else f"{prefix}draft_grade"] = grade
                return profile

        row = conn.execute(f"""
            SELECT AVG(CASE
                WHEN draft_grade IN ('A+', 'A', 'A-') THEN 4
                WHEN draft_grade IN ('B+', 'B', 'B-') THEN 3
                WHEN draft_grade = 'C' THEN 2
                WHEN draft_grade = 'D' THEN 1
                WHEN draft_grade = 'F' THEN 0 ELSE NULL END) as avg_grade
            FROM {central_table('draft')}
            WHERE {_franchise_filter_sql(franchise_id)} AND draft_grade IS NOT NULL {grade_year_filter}
        """).fetchone()
        if row and row[0] is not None:
            avg = float(row[0])
            if avg >= 3.5:
                grade = "A"
            elif avg >= 3.0:
                grade = "B+"
            elif avg >= 2.5:
                grade = "B"
            elif avg >= 2.0:
                grade = "C+"
            elif avg >= 1.5:
                grade = "C"
            elif avg >= 1.0:
                grade = "D"
            else:
                grade = "F"
            profile[f"{prefix}draft_career_grade" if not prefix else f"{prefix}draft_grade"] = grade
    except Exception:  # noqa: broad-except
        pass
    return profile


def _compute_manager_txn_profile(
    conn,
    db_name: str,
    franchise_id: str,
    manager: str | None = None,
    year: int = None,
    prefix: str = "",
    platform: str = "yahoo",
    cache: "ColumnCache | None" = None,
) -> dict[str, Any]:
    """Compute transaction profile for a manager, optionally for a specific year.

    Args:
        conn: Database connection
        db_name: Database name
        franchise_id: Stable franchise identity
        manager: Display manager label
        year: Optional year filter (None = all-time)
        prefix: Key prefix for results (e.g., "season_" for season-specific data)
        platform: 'yahoo' or 'sleeper' - determines which player ID column and mapping table to use
    """
    profile = {}
    profile_label = _profile_log_label(manager, franchise_id)
    year_filter = f"AND t.year = {year}" if year else ""

    if not (cache.exists("transactions") if cache else table_exists(conn, db_name, "transactions")):
        return profile

    # Check which columns exist in transactions table
    trans_cols = cache.columns("transactions") if cache else get_available_columns(conn, db_name, "transactions")
    if "franchise_id" not in trans_cols:
        raise KeyError("franchise_id is required for manager identity")
    franchise_filter = _franchise_filter_sql(franchise_id, "t")
    player_id_col = select_platform_player_id_column(trans_cols, platform_hint=platform) or "yahoo_player_id"
    txn_has_nfl_id = "nfl_player_id" in trans_cols

    # Build column expressions based on what exists
    # IMPORTANT: DuckDB COALESCE validates all columns at parse time
    # Prefer transaction_quality_score (numeric) over transaction_grade (letter) to avoid ties
    if "transaction_quality_score" in trans_cols:
        quality_score_col = "t.transaction_quality_score"
    else:
        quality_score_col = "NULL as transaction_quality_score"

    if "transaction_grade" in trans_cols:
        grade_col = "t.transaction_grade"
    else:
        grade_col = "NULL as transaction_grade"

    # Non-trades use transaction-style managed LAMAR; trades use package net.
    if "trade_net_lamar" in trans_cols and "manager_lamar_ros_managed" in trans_cols:
        net_lamar_expr = (
            "CASE WHEN t.transaction_type IN ('trade', 'trade_pick') "
            "THEN COALESCE(t.trade_net_lamar, 0) "
            "ELSE COALESCE(t.manager_lamar_ros_managed, 0) END"
        )
    elif "manager_lamar_ros_managed" in trans_cols:
        net_lamar_expr = "COALESCE(t.manager_lamar_ros_managed, 0)"
    elif "fa_lamar_ros" in trans_cols:
        net_lamar_expr = "COALESCE(t.fa_lamar_ros, 0)"
    else:
        net_lamar_expr = "0"

    if "player_lamar_ros_total" in trans_cols and "player_lamar_ros" in trans_cols:
        player_lamar_expr = "COALESCE(NULLIF(t.player_lamar_ros_total, 0), t.player_lamar_ros, 0)"
    elif "player_lamar_ros_total" in trans_cols:
        player_lamar_expr = "COALESCE(t.player_lamar_ros_total, 0)"
    elif "player_lamar_ros" in trans_cols:
        player_lamar_expr = "COALESCE(t.player_lamar_ros, 0)"
    else:
        player_lamar_expr = "0"

    try:
        # Load transactions for quality score calculation
        # Use player_lamar_ros_managed (LAMAR while on manager's roster) as primary metric for adds
        # net_manager_lamar_ros is often 0 due to pipeline issues
        df = conn.execute(f"""
            SELECT t.transaction_type, {grade_col}, {quality_score_col},
                   {net_lamar_expr} as net_lamar,
                   {player_lamar_expr} as player_lamar_ros,
                   t.player, t.year, t.week,
                   {headshot_subquery('t', player_id_col, platform, has_nfl_id=txn_has_nfl_id)} as headshot_url
            FROM {central_table('transactions')} t
            WHERE {franchise_filter} {year_filter}
        """).fetchdf()

        if df.empty:
            return profile

        # Primary metric: Average transaction_quality_score (numeric, avoids ties)
        # Fallback: GPA from letter grades
        if "transaction_quality_score" in trans_cols and "transaction_quality_score" in df.columns:
            scores = df["transaction_quality_score"].dropna()
            if len(scores) > 0:
                avg_quality = scores.mean()
                profile[f"{prefix}transaction_avg_quality_score"] = round(float(avg_quality), 2)
                # Store this as primary metric for rankings (numeric, no ties)
                profile[f"{prefix}transaction_quality_metric"] = round(float(avg_quality), 2)

        # Also calculate GPA for display purposes (only if transaction_grade column exists)
        grade_points = {"A": 4, "B": 3, "C": 2, "D": 1, "F": 0}
        total_points = 0
        graded_count = 0
        if "transaction_grade" in trans_cols and "transaction_grade" in df.columns:
            for grade in df["transaction_grade"].dropna():
                base_grade = str(grade)[0].upper()
                if base_grade in grade_points:
                    total_points += grade_points[base_grade]
                    graded_count += 1

            gpa = total_points / graded_count if graded_count > 0 else 0
            profile[f"{prefix}transaction_gpa"] = round(gpa, 2)

            # Convert GPA to letter grade for display
            if gpa >= 3.7:
                letter_grade = "A" if gpa >= 3.85 else "A-"
            elif gpa >= 3.0:
                letter_grade = "B+" if gpa >= 3.3 else "B" if gpa >= 2.7 else "B-"
            elif gpa >= 2.0:
                letter_grade = "C+" if gpa >= 2.3 else "C" if gpa >= 1.7 else "C-"
            elif gpa >= 1.0:
                letter_grade = "D+" if gpa >= 1.3 else "D"
            else:
                letter_grade = "F"
            profile[f"{prefix}transaction_overall_grade"] = letter_grade

            # If we don't have quality_score, use GPA as the ranking metric
            if f"{prefix}transaction_quality_metric" not in profile:
                profile[f"{prefix}transaction_quality_metric"] = gpa

        # Best add
        adds = df[df["transaction_type"] == "add"]
        if not adds.empty:
            best_add = adds.loc[adds["net_lamar"].idxmax()]
            profile[f"{prefix}transaction_best_add_player"] = best_add["player"]
            profile[f"{prefix}transaction_best_add_lamar"] = round(float(best_add["net_lamar"]), 2)
            profile[f"{prefix}transaction_best_add_year"] = int(best_add["year"])
            profile[f"{prefix}transaction_best_add_week"] = int(best_add["week"])
            profile[f"{prefix}transaction_best_add_headshot"] = best_add.get("headshot_url")

        # Worst drop
        drops = df[df["transaction_type"] == "drop"]
        if not drops.empty:
            worst_drop = drops.loc[drops["player_lamar_ros"].idxmax()]
            profile[f"{prefix}transaction_worst_drop_player"] = worst_drop["player"]
            profile[f"{prefix}transaction_worst_drop_lamar"] = round(float(worst_drop["player_lamar_ros"]), 2)
            profile[f"{prefix}transaction_worst_drop_year"] = int(worst_drop["year"])
            profile[f"{prefix}transaction_worst_drop_week"] = int(worst_drop["week"])
            profile[f"{prefix}transaction_worst_drop_headshot"] = worst_drop.get("headshot_url")

        # Best trade
        trades = df[df["transaction_type"] == "trade"]
        if not trades.empty:
            best_trade = trades.loc[trades["net_lamar"].idxmax()]
            profile[f"{prefix}transaction_best_trade_player"] = best_trade["player"]
            profile[f"{prefix}transaction_best_trade_lamar"] = round(float(best_trade["net_lamar"]), 2)
            profile[f"{prefix}transaction_best_trade_year"] = int(best_trade["year"])
            profile[f"{prefix}transaction_best_trade_headshot"] = best_trade.get("headshot_url")
    except Exception as e:
        log(f"  [WARN] Failed to compute txn profile for {profile_label}: {e}")

    return profile


def _compute_manager_best_trade(
    conn,
    db_name: str,
    franchise_id: str,
    manager: str | None = None,
    year: int = None,
    prefix: str = "",
    platform: str = "yahoo",
    cache: "ColumnCache | None" = None,
) -> dict[str, Any]:
    """Compute the best trade for a manager as a full bilateral trade card.

    Returns winner/loser/players/headshots/net_lamar like the league-wide best trade,
    but filtered to trades involving this specific manager.
    """
    result = {}
    profile_label = _profile_log_label(manager, franchise_id)
    year_filter = f"AND t.year = {year}" if year else ""

    if not (cache.exists("transactions") if cache else table_exists(conn, db_name, "transactions")):
        return result

    txn_cols = cache.columns("transactions") if cache else get_available_columns(conn, db_name, "transactions")
    if "franchise_id" not in txn_cols:
        return result
    franchise_filter = _franchise_filter_sql(franchise_id, "t")
    player_id_col = select_platform_player_id_column(txn_cols, platform_hint=platform)

    # Separate LAMAR expressions for received vs sent:
    # - Received: manager_lamar_ros_managed (value while on your roster)
    # - Sent: player_lamar_ros_total (value for rest of season, you don't manage them)
    received_lamar_expr = _trade_received_lamar_expr(txn_cols)
    sent_lamar_expr = _trade_sent_lamar_expr(txn_cols)

    trade_received_filter, trade_sent_filter = _trade_direction_filters()

    has_nfl_id = "nfl_player_id" in txn_cols
    headshot_agg = (
        f"STRING_AGG({headshot_subquery('t', player_id_col, platform, has_nfl_id=has_nfl_id)}, "
        f"'|||' ORDER BY t.player) as headshots"
    )
    partner_select, partner_filter = _trade_partner_sql(txn_cols)

    try:
        row = conn.execute(f"""
            WITH trade_received AS (
                SELECT
                    t.transaction_id,
                    t.franchise_id,
                    t.year,
                    MIN(t.week) as week,
                    MAX(t.manager) as manager,
                    STRING_AGG(t.player, ', ' ORDER BY t.player) as winner_players,
                    {headshot_agg.replace("as headshots", "as winner_headshots") if headshot_agg != "NULL as headshots" else "NULL as winner_headshots"},
                    SUM({received_lamar_expr}) as winner_lamar
                FROM {central_table('transactions')} t
                WHERE t.transaction_type IN ('trade', 'trade_pick') AND t.player IS NOT NULL {year_filter}
                  AND {franchise_filter}
                  {trade_received_filter}
                GROUP BY t.transaction_id, t.franchise_id, t.year
            ),
            trade_sent AS (
                SELECT
                    t.transaction_id,
                    t.franchise_id,
                    t.year,
                    MIN(t.week) as week,
                    MAX(t.manager) as manager,
                    STRING_AGG(t.player, ', ' ORDER BY t.player) as loser_players,
                    {headshot_agg.replace("as headshots", "as loser_headshots") if headshot_agg != "NULL as headshots" else "NULL as loser_headshots"},
                    SUM({sent_lamar_expr}) as loser_lamar
                FROM {central_table('transactions')} t
                WHERE t.transaction_type IN ('trade', 'trade_pick') AND t.player IS NOT NULL {year_filter}
                  AND {franchise_filter}
                  {trade_sent_filter}
                GROUP BY t.transaction_id, t.franchise_id, t.year
            ),
            trade_partners AS (
                SELECT
                    transaction_id,
                    franchise_id,
                    year,
                    {partner_select}
                FROM {central_table('transactions')} t
                WHERE t.transaction_type IN ('trade', 'trade_pick') AND t.player IS NOT NULL {year_filter}
                  AND {franchise_filter}
                  {partner_filter}
                GROUP BY transaction_id, franchise_id, year
            ),
            trade_summary AS (
                SELECT
                    COALESCE(r.transaction_id, s.transaction_id) as transaction_id,
                    COALESCE(r.franchise_id, s.franchise_id) as franchise_id,
                    COALESCE(r.year, s.year) as year,
                    COALESCE(r.week, s.week) as week,
                    COALESCE(r.manager, s.manager) as winner,
                    COALESCE(r.winner_players, '') as winner_players,
                    r.winner_headshots,
                    COALESCE(r.winner_lamar, 0) as winner_lamar,
                    COALESCE(p.partner, '') as loser,
                    COALESCE(s.loser_players, '') as loser_players,
                    s.loser_headshots,
                    COALESCE(s.loser_lamar, 0) as loser_lamar,
                    COALESCE(r.winner_lamar, 0) - COALESCE(s.loser_lamar, 0) as net_lamar
                FROM trade_received r
                FULL OUTER JOIN trade_sent s
                  ON r.transaction_id = s.transaction_id
                 AND r.franchise_id = s.franchise_id
                 AND r.year = s.year
                LEFT JOIN trade_partners p
                  ON COALESCE(r.transaction_id, s.transaction_id) = p.transaction_id
                 AND COALESCE(r.franchise_id, s.franchise_id) = p.franchise_id
                 AND COALESCE(r.year, s.year) = p.year
            )
            SELECT transaction_id, year, week,
                   winner, winner_players, winner_headshots, winner_lamar,
                   loser, loser_players, loser_headshots, loser_lamar,
                   net_lamar
            FROM trade_summary
            WHERE net_lamar >= 0
            ORDER BY net_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            result[f"{prefix}trade_winner"] = row[3]
            result[f"{prefix}trade_winner_players"] = row[4]
            result[f"{prefix}trade_winner_headshots"] = row[5]
            result[f"{prefix}trade_winner_lamar"] = round(float(row[6]), 2) if row[6] else 0
            result[f"{prefix}trade_loser"] = row[7]
            result[f"{prefix}trade_loser_players"] = row[8]
            result[f"{prefix}trade_loser_headshots"] = row[9]
            result[f"{prefix}trade_loser_lamar"] = round(float(row[10]), 2) if row[10] else 0
            result[f"{prefix}trade_net_lamar"] = round(float(row[11]), 2) if row[11] else 0
            result[f"{prefix}trade_year"] = row[1]
            result[f"{prefix}trade_week"] = row[2]
    except Exception as e:
        log(f"  [WARN] Failed to compute best trade for {profile_label}: {e}")

    return result


def _compute_manager_rivalries(
    conn, db_name: str, franchise_id: str, manager: str | None = None, cache: "ColumnCache | None" = None
) -> dict[str, Any]:
    """Compute rivalries for a manager (nemesis, victim, closest)."""
    profile = {}
    profile_label = _profile_log_label(manager, franchise_id)
    matchup_cols = cache.columns("matchup") if cache else get_available_columns(conn, db_name, "matchup")
    if "franchise_id" not in matchup_cols or "opponent_franchise_id" not in matchup_cols:
        return profile

    try:
        # Get all H2H records
        df = conn.execute(f"""
            SELECT opponent_franchise_id, MAX(opponent) AS opponent,
                   SUM(win) as wins, SUM(loss) as losses,
                   SUM(COALESCE(tie, 0)) as ties, COUNT(*) as games,
                   AVG(ABS(team_points - opponent_points)) as avg_margin
            FROM {central_table('matchup')}
            WHERE {_franchise_filter_sql(franchise_id)}
              AND opponent_franchise_id IS NOT NULL
              AND COALESCE(is_consolation, 0) = 0
            GROUP BY opponent_franchise_id
            HAVING COUNT(*) >= 2
        """).fetchdf()

        if df.empty:
            return profile

        df["win_pct"] = df["wins"] / df["games"]

        # Nemesis (opponent with worst record against)
        nemesis = df.loc[df["win_pct"].idxmin()]
        if nemesis["wins"] < nemesis["losses"]:
            profile["nemesis_opponent"] = nemesis["opponent"]
            profile["nemesis_wins"] = int(nemesis["wins"])
            profile["nemesis_losses"] = int(nemesis["losses"])
            nemesis_ties = int(nemesis["ties"])
            if nemesis_ties > 0:
                profile["nemesis_ties"] = nemesis_ties

        # Victim (opponent with best record against)
        victim = df.loc[df["win_pct"].idxmax()]
        if victim["wins"] > victim["losses"]:
            profile["victim_opponent"] = victim["opponent"]
            profile["victim_wins"] = int(victim["wins"])
            profile["victim_losses"] = int(victim["losses"])
            victim_ties = int(victim["ties"])
            if victim_ties > 0:
                profile["victim_ties"] = victim_ties

        # Closest rival (most even record with most games)
        df["closeness"] = abs(df["win_pct"] - 0.5)
        closest = df.sort_values(["closeness", "games"], ascending=[True, False]).iloc[0]
        profile["closest_rival_opponent"] = closest["opponent"]
        closest_ties = int(closest["ties"])
        if closest_ties > 0:
            profile["closest_rival_record"] = f"{int(closest['wins'])}-{int(closest['losses'])}-{closest_ties}"
        else:
            profile["closest_rival_record"] = f"{int(closest['wins'])}-{int(closest['losses'])}"
        profile["closest_rival_avg_margin"] = round(float(closest["avg_margin"]), 2)
    except Exception as e:
        log(f"  [WARN] Failed to compute rivalries for {profile_label}: {e}")

    return profile


def _compute_manager_player_leaders(
    conn,
    db_name: str,
    franchise_id: str,
    manager: str | None = None,
    platform: str = "yahoo",
    cache: "ColumnCache | None" = None,
) -> dict[str, Any]:
    """Compute best players for a manager (LAMAR and clutch)."""
    profile = {}

    # Check if clutch_equity exists
    player_cols = cache.columns("player_fantasy") if cache else get_available_columns(conn, db_name, "player_fantasy")
    if "franchise_id" not in player_cols:
        return profile
    franchise_filter = _franchise_filter_sql(franchise_id, "f")
    has_clutch = "clutch_equity" in player_cols
    player_id_col = select_platform_player_id_column(player_cols, platform_hint=platform)
    player_has_nfl_id = "nfl_player_id" in player_cols

    # Best game LAMAR
    try:
        row = conn.execute(f"""
            SELECT f.player, f.year, f.week, f.manager_lamar,
                   {headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)} as headshot_url
            FROM {central_table('player_fantasy')} f
            WHERE {franchise_filter} AND f.manager_lamar IS NOT NULL AND f.is_started = 1
            ORDER BY f.manager_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            profile["best_game_lamar_player"] = row[0]
            profile["best_game_lamar_year"] = row[1]
            profile["best_game_lamar_week"] = row[2]
            profile["best_game_lamar_value"] = round(float(row[3]), 2) if row[3] else 0
            profile["best_game_lamar_headshot"] = row[4]
    except Exception:  # noqa: broad-except
        pass
    # Best game clutch
    if has_clutch:
        try:
            row = conn.execute(f"""
                {_late_clutch_weeks_cte(db_name)}
                SELECT f.player, f.year, f.week, f.clutch_equity,
                       {headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)} as headshot_url
                FROM {central_table('player_fantasy')} f
                WHERE {franchise_filter} AND f.clutch_equity IS NOT NULL
                  AND f.clutch_equity != 0 AND f.is_started = 1
                  AND {_late_clutch_filter_sql('f')}
                ORDER BY f.clutch_equity DESC LIMIT 1
            """).fetchone()
            if row:
                profile["best_game_clutch_player"] = row[0]
                profile["best_game_clutch_year"] = row[1]
                profile["best_game_clutch_week"] = row[2]
                profile["best_game_clutch_value"] = round(float(row[3]), 2) if row[3] else 0
                profile["best_game_clutch_headshot"] = row[4]
        except Exception:  # noqa: broad-except
            pass
    # Best season LAMAR (GROUP BY NFL_player_id to avoid name conflation)
    try:
        row = conn.execute(f"""
            SELECT MAX(f.player) AS player, f.year, SUM(f.manager_lamar) as total_lamar,
                   MAX({headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)}) as headshot_url
            FROM {central_table('player_fantasy')} f
            WHERE {franchise_filter} AND f.manager_lamar IS NOT NULL AND f.is_started = 1
            GROUP BY f.NFL_player_id, f.year
            ORDER BY total_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            profile["best_season_lamar_player"] = row[0]
            profile["best_season_lamar_year"] = row[1]
            profile["best_season_lamar_value"] = round(float(row[2]), 2) if row[2] else 0
            profile["best_season_lamar_headshot"] = row[3]
    except Exception:  # noqa: broad-except
        pass
    # Best season clutch (GROUP BY NFL_player_id to avoid name conflation)
    if has_clutch:
        try:
            row = conn.execute(f"""
                SELECT MAX(f.player) AS player, f.year, SUM(f.clutch_equity) as total_clutch,
                       MAX({headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)}) as headshot_url
                FROM {central_table('player_fantasy')} f
                WHERE {franchise_filter} AND f.clutch_equity IS NOT NULL
                  AND f.clutch_equity != 0 AND f.is_started = 1
                GROUP BY f.NFL_player_id, f.year
                HAVING SUM(f.clutch_equity) != 0
                ORDER BY total_clutch DESC, f.NFL_player_id ASC, f.year DESC LIMIT 1
            """).fetchone()
            if row:
                profile["best_season_clutch_player"] = row[0]
                profile["best_season_clutch_year"] = row[1]
                profile["best_season_clutch_value"] = round(float(row[2]), 2) if row[2] else 0
                profile["best_season_clutch_headshot"] = row[3]
        except Exception:  # noqa: broad-except
            pass
    # Best career LAMAR (GROUP BY NFL_player_id to avoid name conflation)
    try:
        row = conn.execute(f"""
            SELECT MAX(f.player) AS player, SUM(f.manager_lamar) as total_lamar,
                   MAX({headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)}) as headshot_url
            FROM {central_table('player_fantasy')} f
            WHERE {franchise_filter} AND f.manager_lamar IS NOT NULL AND f.is_started = 1
            GROUP BY f.NFL_player_id
            ORDER BY total_lamar DESC LIMIT 1
        """).fetchone()
        if row:
            profile["best_career_lamar_player"] = row[0]
            profile["best_career_lamar_value"] = round(float(row[1]), 2) if row[1] else 0
            profile["best_career_lamar_headshot"] = row[2]
    except Exception:  # noqa: broad-except
        pass
    # Best career clutch (GROUP BY NFL_player_id to avoid name conflation)
    if has_clutch:
        try:
            row = conn.execute(f"""
                SELECT MAX(f.player) AS player, SUM(f.clutch_equity) as total_clutch,
                       MAX({headshot_subquery('f', player_id_col, platform, has_nfl_id=player_has_nfl_id)}) as headshot_url
                FROM {central_table('player_fantasy')} f
                WHERE {franchise_filter} AND f.clutch_equity IS NOT NULL
                  AND f.clutch_equity != 0 AND f.is_started = 1
                GROUP BY f.NFL_player_id
                HAVING SUM(f.clutch_equity) != 0
                ORDER BY total_clutch DESC, f.NFL_player_id ASC LIMIT 1
            """).fetchone()
            if row:
                profile["best_career_clutch_player"] = row[0]
                profile["best_career_clutch_value"] = round(float(row[1]), 2) if row[1] else 0
                profile["best_career_clutch_headshot"] = row[2]
        except Exception:  # noqa: broad-except
            pass
    return profile


def _score_to_draft_grade(avg_score: float) -> str:
    """Convert an average draft score to a readable letter grade.

    This is a fallback for aggregate/profile surfaces when a percentile-based
    `manager_draft_grade` is not already materialized. New imports store
    manager_draft_score as a 100-index, while old imports may still have a
    z-score alias; normalize both to z-score thresholds here.
    """
    z_score = (avg_score - 100.0) / 15.0 if abs(avg_score) > 10 else avg_score
    if z_score >= 1.5:
        return "A+"
    elif z_score >= 1.0:
        return "A"
    elif z_score >= 0.7:
        return "A-"
    elif z_score >= 0.45:
        return "B+"
    elif z_score >= 0.15:
        return "B"
    elif z_score >= -0.15:
        return "B-"
    elif z_score >= -0.45:
        return "C"
    elif z_score >= -0.9:
        return "D"
    else:
        return "F"


def _compute_manager_timeline(
    conn,
    db_name: str,
    franchise_id: str,
    manager: str | None = None,
    use_median: bool = False,
    median_years: Sequence[int] | None = None,
    matchup_cols: set = None,
    cache: "ColumnCache | None" = None,
) -> str:
    """Compute career timeline for a manager. Returns pipe-delimited string.

    Extended format includes PF, PA, regular season seed, final placement, and
    6 enrichment fields (all optional — empty string if unavailable):
    year:record:wins:losses:notable:team_name:is_champion:pf:pa:reg_seed:final_place:ppg:win_pct:power_rating:total_lamar:lineup_efficiency:draft_grade

    For H2H+Median leagues, regular season wins include both H2H and median wins.
    Playoff wins are H2H only (median doesn't apply in playoffs).
    """
    if matchup_cols is None:
        matchup_cols = cache.columns("matchup") if cache else get_available_columns(conn, db_name, "matchup")
    if "franchise_id" not in matchup_cols:
        return ""
    profile_label = _profile_log_label(manager, franchise_id)
    franchise_filter = _franchise_filter_sql(franchise_id)
    franchise_literal = _escape_sql_literal(franchise_id)
    has_champion = "champion" in matchup_cols
    has_sacko = "sacko" in matchup_cols
    has_playoff_round = "playoff_round" in matchup_cols
    has_consolation_round = "consolation_round" in matchup_cols
    has_above_median = "above_league_median" in matchup_cols
    has_below_median = "below_league_median" in matchup_cols

    # Build win/loss expressions based on scoring type
    # Only use median if the columns exist in the matchup table
    effective_median_years = median_years if (has_above_median and has_below_median) else None
    effective_use_median = use_median and has_above_median and has_below_median

    # For H2H+Median: reg season wins = H2H + median (only for years in median_years), playoff wins = H2H only
    if effective_median_years:
        years_list = ",".join(str(int(y)) for y in median_years)
        cond = f"CAST(year AS INT) IN ({years_list})"
        cond_m = f"CAST(m.year AS INT) IN ({years_list})"
        # Total wins/losses (including playoffs - playoffs are H2H only)
        total_wins_expr = f"""SUM(CASE
            WHEN COALESCE(is_consolation, 0) = 0 THEN
                CASE WHEN COALESCE(is_playoffs, 0) = 0 THEN
                    COALESCE(CAST(win AS INT), 0) + CASE WHEN {cond} THEN COALESCE(CAST(above_league_median AS INT), 0) ELSE 0 END
                    ELSE COALESCE(CAST(win AS INT), 0)
                END
            ELSE 0 END)"""
        total_losses_expr = f"""SUM(CASE
            WHEN COALESCE(is_consolation, 0) = 0 THEN
                CASE WHEN COALESCE(is_playoffs, 0) = 0 THEN
                    COALESCE(CAST(loss AS INT), 0) + CASE WHEN {cond} THEN COALESCE(CAST(below_league_median AS INT), 0) ELSE 0 END
                    ELSE COALESCE(CAST(loss AS INT), 0)
                END
            ELSE 0 END)"""
        # Regular season wins (for seeding) = H2H + median (per-year)
        reg_wins_expr = f"""SUM(CASE WHEN COALESCE(is_playoffs, 0) = 0 AND COALESCE(is_consolation, 0) = 0
            THEN COALESCE(CAST(win AS INT), 0) + CASE WHEN {cond} THEN COALESCE(CAST(above_league_median AS INT), 0) ELSE 0 END
            ELSE 0 END)"""
        # For all_managers_by_year (with m. prefix)
        all_mgr_reg_wins_expr = f"""SUM(CASE WHEN COALESCE(m.is_playoffs, 0) = 0 AND COALESCE(m.is_consolation, 0) = 0
            THEN COALESCE(CAST(m.win AS INT), 0) + CASE WHEN {cond_m} THEN COALESCE(CAST(m.above_league_median AS INT), 0) ELSE 0 END
            ELSE 0 END)"""
    elif effective_use_median:
        # Total wins/losses (including playoffs - but playoffs are H2H only)
        total_wins_expr = """SUM(CASE
            WHEN COALESCE(is_consolation, 0) = 0 THEN
                CASE WHEN COALESCE(is_playoffs, 0) = 0
                    THEN COALESCE(CAST(win AS INT), 0) + COALESCE(CAST(above_league_median AS INT), 0)
                    ELSE COALESCE(CAST(win AS INT), 0)
                END
            ELSE 0 END)"""
        total_losses_expr = """SUM(CASE
            WHEN COALESCE(is_consolation, 0) = 0 THEN
                CASE WHEN COALESCE(is_playoffs, 0) = 0
                    THEN COALESCE(CAST(loss AS INT), 0) + COALESCE(CAST(below_league_median AS INT), 0)
                    ELSE COALESCE(CAST(loss AS INT), 0)
                END
            ELSE 0 END)"""
        # Regular season wins (for seeding) = H2H + median
        reg_wins_expr = """SUM(CASE WHEN COALESCE(is_playoffs, 0) = 0 AND COALESCE(is_consolation, 0) = 0
            THEN COALESCE(CAST(win AS INT), 0) + COALESCE(CAST(above_league_median AS INT), 0)
            ELSE 0 END)"""
        # For all_managers_by_year (with m. prefix)
        all_mgr_reg_wins_expr = """SUM(CASE WHEN COALESCE(m.is_playoffs, 0) = 0 AND COALESCE(m.is_consolation, 0) = 0
            THEN COALESCE(CAST(m.win AS INT), 0) + COALESCE(CAST(m.above_league_median AS INT), 0)
            ELSE 0 END)"""
    else:
        total_wins_expr = "SUM(CASE WHEN COALESCE(is_consolation, 0) = 0 AND win = 1 THEN 1 ELSE 0 END)"
        total_losses_expr = "SUM(CASE WHEN COALESCE(is_consolation, 0) = 0 AND win = 0 THEN 1 ELSE 0 END)"
        reg_wins_expr = "SUM(CASE WHEN COALESCE(is_playoffs, 0) = 0 AND COALESCE(is_consolation, 0) = 0 AND win = 1 THEN 1 ELSE 0 END)"
        all_mgr_reg_wins_expr = "SUM(CASE WHEN COALESCE(m.is_playoffs, 0) = 0 AND COALESCE(m.is_consolation, 0) = 0 AND m.win = 1 THEN 1 ELSE 0 END)"

    try:
        # Get season stats with PF, PA, and final placement from playoff results
        df = conn.execute(f"""
            WITH season_data AS (
                SELECT year,
                       {total_wins_expr} as wins,
                       {total_losses_expr} as losses,
                       -- Regular season record for seeding
                       {reg_wins_expr} as reg_wins,
                       -- PF/PA (regular season only)
                       SUM(CASE WHEN COALESCE(is_playoffs, 0) = 0 AND COALESCE(is_consolation, 0) = 0 THEN team_points ELSE 0 END) as pf,
                       SUM(CASE WHEN COALESCE(is_playoffs, 0) = 0 AND COALESCE(is_consolation, 0) = 0 THEN opponent_points ELSE 0 END) as pa,
                       {"MAX(COALESCE(champion, 0))" if has_champion else "0"} as is_champion,
                       {"MAX(COALESCE(sacko, 0))" if has_sacko else "0"} as is_sacko,
                       MAX(CASE WHEN COALESCE(is_playoffs, 0) = 1 AND COALESCE(is_consolation, 0) = 0 THEN 1 ELSE 0 END) as made_playoffs,
                       MAX(team_name) as team_name
                FROM {central_table('matchup')}
                WHERE {franchise_filter}
                GROUP BY year
            ),
            -- Calculate regular season standings per year
            all_managers_by_year AS (
                SELECT m.year,
                       m.franchise_id, MAX(m.manager) as manager,
                       {all_mgr_reg_wins_expr} as reg_wins,
                       SUM(CASE WHEN COALESCE(m.is_playoffs, 0) = 0 AND COALESCE(m.is_consolation, 0) = 0 THEN m.team_points ELSE 0 END) as pf
                FROM {central_table('matchup')} m
                WHERE m.franchise_id IS NOT NULL
                  AND TRIM(CAST(m.franchise_id AS VARCHAR)) != ''
                GROUP BY m.year, m.franchise_id
            ),
            standings AS (
                SELECT year, franchise_id, manager,
                       ROW_NUMBER() OVER (PARTITION BY year ORDER BY reg_wins DESC, pf DESC) as reg_seed
                FROM all_managers_by_year
            ),
            -- Get max playoff week per manager/year for final result
            max_playoff_weeks AS (
                SELECT franchise_id, MAX(manager) as manager,
                       year, MAX(week) as max_playoff_week
                FROM {central_table('matchup')}
                WHERE (is_playoffs = 1 OR is_consolation = 1)
                  AND franchise_id IS NOT NULL
                  AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
                GROUP BY franchise_id, year
            ),
            -- Get final playoff/consolation result
            final_results AS (
                SELECT
                    m.franchise_id, MAX(m.manager) as manager,
                    m.year,
                    {"MAX(m.playoff_round)" if has_playoff_round else "NULL"} as final_playoff_round,
                    {"MAX(m.consolation_round)" if has_consolation_round else "NULL"} as final_consolation_round,
                    MAX(m.win) as final_win,
                    {"MAX(COALESCE(m.champion, 0))" if has_champion else "0"} as is_champion,
                    {"MAX(COALESCE(m.sacko, 0))" if has_sacko else "0"} as is_sacko
                FROM {central_table('matchup')} m
                INNER JOIN max_playoff_weeks mpw
                    ON m.franchise_id = mpw.franchise_id
                    AND m.year = mpw.year
                    AND m.week = mpw.max_playoff_week
                GROUP BY m.franchise_id, m.year
            ),
            -- Count teams per year for sacko placement
            teams_per_year AS (
                SELECT year, COUNT(DISTINCT franchise_id) as num_teams
                FROM {central_table('matchup')}
                WHERE franchise_id IS NOT NULL
                  AND TRIM(CAST(franchise_id AS VARCHAR)) != ''
                GROUP BY year
            )
            SELECT sd.*, st.reg_seed, fr.final_playoff_round, fr.final_consolation_round, fr.final_win, tpy.num_teams
            FROM season_data sd
            LEFT JOIN standings st ON sd.year = st.year AND st.franchise_id = '{franchise_literal}'
            LEFT JOIN final_results fr ON sd.year = fr.year AND fr.franchise_id = '{franchise_literal}'
            LEFT JOIN teams_per_year tpy ON sd.year = tpy.year
            ORDER BY sd.year DESC
        """).fetchdf()

        if df.empty:
            return ""

        # ── Gather optional enrichment data (keyed by year) ──
        # Each dict maps year -> value; missing = empty string in output
        enrich_ppg: dict[int, float] = {}
        enrich_power: dict[int, float] = {}
        enrich_opt_pts: dict[int, float] = {}
        enrich_total_pf_ms: dict[int, float] = {}
        enrich_lamar: dict[int, float] = {}
        enrich_draft_grade: dict[int, str] = {}

        # matchup_season: avg_team_points, power_rating, total_team_points, optimal_ceiling_pts
        if cache.exists("matchup_season") if cache else table_exists(conn, db_name, "matchup_season"):
            ms_cols = cache.columns("matchup_season") if cache else get_available_columns(conn, db_name, "matchup_season")
            ms_select_parts = ["year"]
            if "franchise_id" in ms_cols and "avg_team_points" in ms_cols:
                ms_select_parts.append("avg_team_points")
            if "franchise_id" in ms_cols and "power_rating" in ms_cols:
                ms_select_parts.append("power_rating")
            if "franchise_id" in ms_cols and "total_team_points" in ms_cols:
                ms_select_parts.append("total_team_points")
            if "franchise_id" in ms_cols and "optimal_ceiling_pts" in ms_cols:
                ms_select_parts.append("optimal_ceiling_pts")
            if len(ms_select_parts) > 1:
                try:
                    ms_df = conn.execute(f"""
                        SELECT {', '.join(ms_select_parts)}
                        FROM {central_table('matchup_season')}
                        WHERE {_franchise_filter_sql(franchise_id)}
                    """).fetchdf()
                    for _, msr in ms_df.iterrows():
                        yr = int(msr["year"])
                        if "avg_team_points" in ms_df.columns and not pd.isna(msr.get("avg_team_points")):
                            enrich_ppg[yr] = float(msr["avg_team_points"])
                        if "power_rating" in ms_df.columns and not pd.isna(msr.get("power_rating")):
                            enrich_power[yr] = float(msr["power_rating"])
                        if "total_team_points" in ms_df.columns and not pd.isna(msr.get("total_team_points")):
                            enrich_total_pf_ms[yr] = float(msr["total_team_points"])
                        if "optimal_ceiling_pts" in ms_df.columns and not pd.isna(msr.get("optimal_ceiling_pts")):
                            enrich_opt_pts[yr] = float(msr["optimal_ceiling_pts"])
                except Exception:
                    pass

        # player_fantasy_season: total LAMAR per franchise+year
        if cache.exists("player_fantasy_season") if cache else table_exists(conn, db_name, "player_fantasy_season"):
            pfs_cols = cache.columns("player_fantasy_season") if cache else get_available_columns(conn, db_name, "player_fantasy_season")
            if "manager_lamar" in pfs_cols and "franchise_id" in pfs_cols:
                try:
                    lamar_df = conn.execute(f"""
                        SELECT year, SUM(manager_lamar) as total_lamar
                        FROM {central_table('player_fantasy_season')}
                        WHERE {_franchise_filter_sql(franchise_id)}
                        GROUP BY year
                    """).fetchdf()
                    for _, lr in lamar_df.iterrows():
                        if not pd.isna(lr.get("total_lamar")):
                            enrich_lamar[int(lr["year"])] = float(lr["total_lamar"])
                except Exception:
                    pass
        if not enrich_lamar and (cache.exists("player_fantasy") if cache else table_exists(conn, db_name, "player_fantasy")):
            pf_cols = cache.columns("player_fantasy") if cache else get_available_columns(conn, db_name, "player_fantasy")
            if "manager_lamar" in pf_cols and "franchise_id" in pf_cols:
                pf_started_filter = "AND COALESCE(is_started, 0) = 1" if "is_started" in pf_cols else ""
                try:
                    lamar_df = conn.execute(f"""
                        SELECT year, SUM(manager_lamar) as total_lamar
                        FROM {central_table('player_fantasy')}
                        WHERE {_franchise_filter_sql(franchise_id)}
                          AND manager_lamar IS NOT NULL
                          {pf_started_filter}
                        GROUP BY year
                    """).fetchdf()
                    for _, lr in lamar_df.iterrows():
                        if not pd.isna(lr.get("total_lamar")):
                            enrich_lamar[int(lr["year"])] = float(lr["total_lamar"])
                except Exception:
                    pass

        # draft: avg manager_draft_score -> letter grade per year
        if cache.exists("draft") if cache else table_exists(conn, db_name, "draft"):
            draft_cols = cache.columns("draft") if cache else get_available_columns(conn, db_name, "draft")
            if "manager_draft_score" in draft_cols and "franchise_id" in draft_cols:
                manager_grade_expr = (
                    "ARG_MAX(manager_draft_grade, manager_draft_score) AS manager_draft_grade"
                    if "manager_draft_grade" in draft_cols
                    else "NULL AS manager_draft_grade"
                )
                try:
                    draft_df = conn.execute(f"""
                        SELECT year,
                               AVG(manager_draft_score) as avg_score,
                               {manager_grade_expr}
                        FROM {central_table('draft')}
                        WHERE {_franchise_filter_sql(franchise_id)}
                          AND manager_draft_score IS NOT NULL
                        GROUP BY year
                    """).fetchdf()
                    for _, dr in draft_df.iterrows():
                        if not pd.isna(dr.get("manager_draft_grade")) and str(dr["manager_draft_grade"]).strip():
                            enrich_draft_grade[int(dr["year"])] = str(dr["manager_draft_grade"]).strip()
                        elif not pd.isna(dr.get("avg_score")):
                            enrich_draft_grade[int(dr["year"])] = _score_to_draft_grade(float(dr["avg_score"]))
                except Exception:
                    pass

        entries = []
        for _, row in df.iterrows():
            # Helper to safely convert to int, handling NaN
            def safe_int(val, default=0):
                if pd.isna(val):
                    return default
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return default

            def safe_text(val, default=""):
                if pd.isna(val):
                    return default
                text = str(val).strip()
                return text if text else default

            wins = safe_int(row["wins"])
            losses = safe_int(row["losses"])
            is_champ = safe_int(row["is_champion"]) == 1
            is_sacko = safe_int(row["is_sacko"]) == 1
            made_playoffs = safe_int(row["made_playoffs"]) == 1
            pf = float(row.get("pf", 0) or 0) if not pd.isna(row.get("pf")) else 0.0
            pa = float(row.get("pa", 0) or 0) if not pd.isna(row.get("pa")) else 0.0
            reg_seed = safe_int(row.get("reg_seed", 0))
            num_teams = safe_int(row.get("num_teams", 0))

            # Calculate final placement from playoff results
            final_place = 0  # 0 = unknown
            final_playoff_round = safe_text(row.get("final_playoff_round"))
            final_consolation_round = safe_text(row.get("final_consolation_round"))
            final_win = safe_int(row.get("final_win", 0))

            if is_champ:
                final_place = 1
                notable = "Champion"
            elif is_sacko:
                final_place = num_teams if num_teams > 0 else 0
                notable = "Sacko"
            elif final_playoff_round == "championship" and final_win == 0:
                final_place = 2
                notable = "2nd"
            elif final_consolation_round == "third_place_game":
                final_place = 3 if final_win == 1 else 4
                notable = "3rd" if final_win == 1 else "4th"
            elif final_consolation_round == "fifth_place_game":
                final_place = 5 if final_win == 1 else 6
                notable = "5th" if final_win == 1 else "6th"
            elif final_consolation_round == "seventh_place_game":
                final_place = 7 if final_win == 1 else 8
                notable = "7th" if final_win == 1 else "8th"
            elif final_consolation_round == "ninth_place_game":
                final_place = 9 if final_win == 1 else 10
                notable = "9th" if final_win == 1 else "10th"
            elif final_playoff_round == "semifinal" and final_win == 0:
                final_place = 0  # 3rd or 4th, unknown without 3rd place game
                notable = "Semis"
            elif final_playoff_round == "quarterfinal" and final_win == 0:
                final_place = 0  # 5th-8th, unknown
                notable = "Quarters"
            elif made_playoffs:
                notable = "Playoffs"
            else:
                notable = "Missed"

            team_name = safe_text(row.get("team_name"), "Unknown").replace("|", "-").replace(":", "-")
            year_val = safe_int(row["year"])

            # ── 6 optional enrichment fields (empty string if unavailable) ──
            ppg_str = f"{enrich_ppg[year_val]:.2f}" if year_val in enrich_ppg else ""
            total_games = wins + losses
            win_pct_str = f"{wins / total_games:.4f}" if total_games > 0 else ""
            power_str = f"{enrich_power[year_val]:.1f}" if year_val in enrich_power else ""
            lamar_str = f"{enrich_lamar[year_val]:.1f}" if year_val in enrich_lamar else ""
            # lineup_efficiency = total_team_points / optimal_ceiling_pts
            eff_str = ""
            if year_val in enrich_total_pf_ms and year_val in enrich_opt_pts:
                opt_pts = enrich_opt_pts[year_val]
                if opt_pts > 0:
                    eff_str = f"{enrich_total_pf_ms[year_val] / opt_pts:.4f}"
            draft_grade_str = enrich_draft_grade.get(year_val, "")

            # Format: ...existing 11 fields...:ppg:win_pct:power_rating:total_lamar:lineup_efficiency:draft_grade
            entry = (
                f"{year_val}:{wins}-{losses}:{wins}:{losses}:{notable}:{team_name}:"
                f"{str(is_champ).lower()}:{pf:.1f}:{pa:.1f}:{reg_seed}:{final_place}:"
                f"{ppg_str}:{win_pct_str}:{power_str}:{lamar_str}:{eff_str}:{draft_grade_str}"
            )
            entries.append(entry)

        return "|".join(entries)
    except Exception as e:
        log(f"  [WARN] Failed to compute timeline for {profile_label}: {e}")
        return ""


# ============================================================================
# UPLOAD TO MOTHERDUCK
# ============================================================================


def upload_homepage_tables_to_motherduck(conn, db_name: str, data_dir: Path):
    """Upload all 5 homepage tables to MotherDuck (legacy — tables are now written directly)."""
    log("Homepage tables already uploaded directly to MotherDuck (no-op).")


def compute_homepage_frames(
    conn,
    db_name: str,
    *,
    platform: str | None = None,
    manager_profile_franchise_ids: set[str] | None = None,
    preserved_alltime_trade: Mapping[str, Any] | None = None,
    changed_years: set[int] | None = None,
) -> dict[str, pd.DataFrame]:
    """Compute all five homepage rollups from one complete league connection."""
    configure_table_catalog(conn)
    detected_platform = detect_platform(conn, db_name)
    if detected_platform != "yahoo" or not platform:
        platform = detected_platform
    platform = platform or "yahoo"

    cache = ColumnCache(conn, get_active_catalog())
    median_years = detect_h2h_median_years_from_db(conn, db_name)
    return {
        "homepage_league_summary": compute_league_summary(
            conn,
            db_name,
            platform=platform,
            cache=cache,
            preserved_alltime_trade=preserved_alltime_trade,
            changed_years=changed_years,
        ),
        "homepage_manager_rankings": compute_manager_rankings(
            conn, db_name, median_years=median_years, cache=cache
        ),
        "homepage_current_standings": compute_current_standings(
            conn, db_name, median_years=median_years, cache=cache
        ),
        "homepage_top_rivalries": compute_top_rivalries(conn, db_name, cache=cache),
        "homepage_manager_profiles": compute_all_manager_profiles(
            conn,
            db_name,
            platform=platform,
            franchise_ids=manager_profile_franchise_ids,
        ),
    }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================


def main(args):
    """Main entry point for homepage summary precomputation."""
    log("=" * 60)
    log("HOMEPAGE SUMMARY PRECOMPUTATION")
    log("=" * 60)

    # Determine database name
    platform: str | None = None
    legacy_data_dir = Path("fantasy_football_data")

    if args.db:
        db_name = args.db
    elif args.context:
        try:
            with open(args.context) as f:
                ctx_data = json.load(f)
            league_name = ctx_data.get("league_name", "unknown")
            platform = ctx_data.get("platform")
            log(f"League: {league_name}")
            # Get database name - sanitize league name to valid DB identifier
            db_name = re.sub(r"[^a-zA-Z0-9]+", "_", (league_name or "").strip().lower()).strip("_")
            if not db_name:
                db_name = "l"
            if db_name[0].isdigit():
                db_name = "l_" + db_name
            db_name = db_name[:63]
            legacy_data_dir = Path(ctx_data.get("data_directory", "fantasy_football_data"))
        except Exception as e:
            log(f"FAIL: Could not read context file: {e}")
            sys.exit(1)
    else:
        log("FAIL: Must provide --context or --db")
        sys.exit(1)

    # Legacy data directory (used for filesystem side-effects)
    legacy_data_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        log("[DRY RUN] Would compute and upload homepage tables:")
        log("  - homepage_league_summary (1 row)")
        log("  - homepage_manager_rankings (one row per manager)")
        log("  - homepage_current_standings (one row per manager)")
        log("  - homepage_top_rivalries (top 20 rivalries)")
        log("  - homepage_manager_profiles (one row per manager)")
        return

    # Connect (local DuckDB with ___ops attached, or MotherDuck)
    conn = get_pipeline_connection(db_name, data_dir=args.data_dir, attach_ops=True, qualified=True)

    try:
        configure_table_catalog(conn)
        detected_platform = detect_platform(conn, db_name)
        if detected_platform != "yahoo" or not platform:
            platform = detected_platform
        platform = platform or "yahoo"

        log(f"Database: {db_name}")
        log(f"Platform: {platform}")

        # Verify required tables exist before proceeding
        existing_tables = {
            row[0]
            for row in conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{get_active_catalog()}' AND table_schema = 'public'"
            ).fetchall()
        }
        required = {"matchup"}
        missing = required - existing_tables
        if missing:
            log(
                f"[SKIP] Required tables not yet created: {', '.join(sorted(missing))}. "
                "Homepage summary will be computed after matchup data is available."
            )
            return

        # (Removed a dead defensive-normalization block that referenced an undefined
        # helper `_build_scoped_temp_table_insert_sql`; it was gated behind
        # `if norm_sql:` where `norm_sql = ""`, so it never executed. Homepage
        # summary tolerates legacy type drift at query time.)

        # Check if matchup table has actual data (0 rows = season hasn't started)
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {central_table('matchup')} WHERE {league_db_filter(db_name)}"
        ).fetchone()[0]
        if row_count == 0:
            log(
                "[SKIP] Matchup table is empty (no games played yet). "
                "Homepage summary will be computed after matchup data is available."
            )
            return

        homepage_frames = compute_homepage_frames(conn, db_name, platform=platform)
        configure_table_catalog(conn)

        # Upload all tables directly to MotherDuck (always, not just on --upload)
        for _tbl_name, _tbl_df in homepage_frames.items():
            replace_scoped_aggregate_table_from_dataframe(conn, db_name, _tbl_name, _tbl_df)
            log(f"  Uploaded {_tbl_name} ({len(_tbl_df)} rows)")

        log("")
        log("Homepage summary computation complete:")
        log(
            "  - League summary: 1 row, "
            f"{len(homepage_frames['homepage_league_summary'].columns)} columns"
        )
        log(f"  - Manager rankings: {len(homepage_frames['homepage_manager_rankings'])} managers")
        log(f"  - Current standings: {len(homepage_frames['homepage_current_standings'])} managers")
        log(f"  - Top rivalries: {len(homepage_frames['homepage_top_rivalries'])} rivalries")
        log(f"  - Manager profiles: {len(homepage_frames['homepage_manager_profiles'])} managers")

    finally:
        # Free memory before closing connection to reduce pressure during
        # DuckDB shutdown (prevents SIGSEGV/exit code 139 on low-memory runners)
        gc.collect()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Precompute homepage summary data for faster page loads")
    parser.add_argument("--context", help="Path to league_context.json")
    parser.add_argument("--db", help="Database name (alternative to --context)")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )
    parser.add_argument(
        "--upload", action="store_true", help="(legacy, no-op) Tables are always written directly to MotherDuck"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()
    main(args)
