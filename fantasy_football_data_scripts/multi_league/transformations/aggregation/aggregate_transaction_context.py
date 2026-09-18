#!/usr/bin/env python3
"""
Pre-aggregate transaction data into manager-season, manager-career, and player-career tables.

This transformation pre-aggregates league-specific transaction data to speed up queries:
- transaction_manager_season: Aggregated by manager + year
- transaction_manager_career: Aggregated by manager (all-time)
- transaction_player_career: Aggregated by player + position (all-time)

These tables are PER-LEAGUE (stored in each league's database).

Usage:
    python aggregate_transaction_context.py --context path/to/league_context.json
    python aggregate_transaction_context.py --db demo_league
    python aggregate_transaction_context.py --context path/to/league_context.json --dry-run
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

log = make_logger("TXN-AGG")
_ADD_TRANSACTION_TYPES_SQL = "('add', 'pickup', 'claim', 'waiver', 'add/drop')"
_ADD_DROP_TRANSACTION_TYPES_SQL = "('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop')"
# Back-compat shim — runtime reads go through get_active_catalog().


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


def _trade_direction_filters() -> tuple[str, str]:
    """Return SQL filters for received vs sent trade rows."""
    return (
        " AND t.trade_direction = 'received'",
        " AND t.trade_direction = 'sent'",
    )


# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------


def create_transaction_manager_season_table(conn, db_name: str) -> bool:
    """Create transaction_manager_season table if it doesn't exist."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "transaction_manager_season")
    log("  transaction_manager_season table ready")
    return True


def create_transaction_manager_career_table(conn, db_name: str) -> bool:
    """Create transaction_manager_career table if it doesn't exist."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "transaction_manager_career")
    log("  transaction_manager_career table ready")
    return True


def create_transaction_player_career_table(conn, db_name: str) -> bool:
    """Create transaction_player_career table if it doesn't exist."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "transaction_player_career")
    log("  transaction_player_career table ready")
    return True


# ---------------------------------------------------------------------------
# Aggregation functions
# ---------------------------------------------------------------------------


def aggregate_transaction_manager_season(conn, db_name: str, *, year: int | None = None) -> int:
    """Aggregate transactions table to manager-season totals."""
    configure_table_catalog(conn)
    season_scope = league_db_filter(db_name, year=year)
    transaction_scope = league_db_filter(db_name, "t", year=year)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('transaction_manager_season')} WHERE {season_scope}",
        db_name,
        label="transaction_manager_season:delete",
    )
    cols = get_available_columns(conn, db_name, "transactions")
    if not cols:
        log("  No transaction columns found, skipping")
        return 0

    # LAMAR column cascade
    add_lamar_col = next((c for c in ["manager_lamar_ros_managed", "fa_lamar_ros", "net_lamar_ros"] if c in cols), None)
    drop_lamar_col = next((c for c in ["player_lamar_ros_total", "fa_lamar_ros"] if c in cols), None)
    score_col = "transaction_score" if "transaction_score" in cols else None
    timing_col = "score_timing_mult" if "score_timing_mult" in cols else None
    faab_col = "faab_bid" if "faab_bid" in cols else None
    ppf_col = "points_per_faab_dollar" if "points_per_faab_dollar" in cols else None
    regret_col = "drop_regret_score" if "drop_regret_score" in cols else None
    ppg_before = "ppg_before_transaction" if "ppg_before_transaction" in cols else None
    ppg_after = "ppg_after_transaction" if "ppg_after_transaction" in cols else None

    if "franchise_id" not in cols:
        raise KeyError("franchise_id is required for manager identity")
    fid_col = "franchise_id"

    # Part 1: Add/Drop aggregation expressions
    add_lamar_select = f"SUM(COALESCE(t.{add_lamar_col}, 0))" if add_lamar_col else "0"
    drop_lamar_select = f"SUM(COALESCE(t.{drop_lamar_col}, 0))" if drop_lamar_col else "0"
    # transaction_score is a 100-centered quality index. Aggregate score columns
    # use the manager's average index so cross-league comparisons are not driven
    # by transaction volume.
    score_sum = f"AVG(t.{score_col})" if score_col else "0"
    score_avg = f"AVG(t.{score_col})" if score_col else "0"
    timing_avg = f"AVG(t.{timing_col})" if timing_col else "0"
    faab_sum = (
        f"SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN COALESCE(t.{faab_col}, 0) ELSE 0 END)"
        if faab_col
        else "0"
    )
    ppf_avg = (
        f"AVG(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} AND COALESCE(t.{ppf_col}, 0) > 0 THEN t.{ppf_col} END)"
        if ppf_col
        else "0"
    )
    ppg_imp = (
        f"AVG(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} AND t.{ppg_before} IS NOT NULL AND t.{ppg_after} IS NOT NULL THEN t.{ppg_after} - t.{ppg_before} END)"
        if ppg_before and ppg_after
        else "0"
    )
    net_pts_col = "net_points_ros" if "net_points_ros" in cols else None
    net_pts_sum = (
        f"SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN COALESCE(t.{net_pts_col}, 0) ELSE 0 END)"
        if net_pts_col
        else "0"
    )

    # Best pickup / worst drop CTEs
    grp_key = f"t.{fid_col}"
    best_pickup_cte = ""
    if add_lamar_col:
        best_pickup_cte = f"""
        , best_pickup AS (
            SELECT {grp_key} as _grp_key, t.year,
                FIRST(t.player ORDER BY COALESCE(t.{add_lamar_col}, 0) DESC) as player,
                MAX(COALESCE(t.{add_lamar_col}, 0)) as lamar
            FROM {central_table("transactions")} t
            WHERE t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} AND t.player IS NOT NULL
              AND {transaction_scope}
            GROUP BY {grp_key}, t.year
        )"""
    worst_drop_cte = ""
    if regret_col:
        worst_drop_cte = f"""
        , worst_drop AS (
            SELECT {grp_key} as _grp_key, t.year,
                FIRST(t.player ORDER BY COALESCE(t.{regret_col}, 0) DESC) as player,
                MAX(COALESCE(t.{regret_col}, 0)) as regret
            FROM {central_table("transactions")} t
            WHERE t.transaction_type = 'drop' AND t.player IS NOT NULL
              AND {transaction_scope}
            GROUP BY {grp_key}, t.year
        )"""

    ad_grp_ref = "ad.franchise_id"
    bp_join = f"LEFT JOIN best_pickup bp ON {ad_grp_ref} = bp._grp_key AND ad.year = bp.year" if best_pickup_cte else ""
    wd_join = f"LEFT JOIN worst_drop wd ON {ad_grp_ref} = wd._grp_key AND ad.year = wd.year" if worst_drop_cte else ""
    bp_select = "bp.player, bp.lamar," if best_pickup_cte else "NULL, 0,"
    wd_select = "wd.player, wd.regret," if worst_drop_cte else "NULL, 0,"

    # Part 2: Trade aggregation (side-based)
    trade_pts_col = next(
        (c for c in ["total_points_ros_managed", "total_points_rest_of_season", "total_points_ros_total"] if c in cols),
        None,
    )
    trade_cte = ""
    trade_join = ""
    trade_select = "0, 0, 0, 0, 0, 0,"
    trade_lamar_value = (
        "COALESCE(t.trade_asset_lamar, 0)"
        if "trade_asset_lamar" in cols
        else f"COALESCE(t.{add_lamar_col}, 0)"
        if add_lamar_col
        else "0"
    )
    trade_points_value = f"COALESCE(t.{trade_pts_col}, 0)" if trade_pts_col else "0"
    if "source_manager" in cols:
        trade_received_filter, trade_sent_filter = _trade_direction_filters()
        trade_cte = f"""
        , trade_received AS (
            SELECT
                t.transaction_id,
                t.year,
                MIN(t.week) as week,
                {grp_key} as _grp_key,
                SUM({trade_lamar_value}) as got_lamar,
                SUM({trade_points_value}) as got_points
            FROM {central_table("transactions")} t
            WHERE t.transaction_type IN ('trade', 'trade_pick')
              AND t.manager IS NOT NULL
              AND TRIM(t.manager) <> ''
              AND {transaction_scope}
              {trade_received_filter}
            GROUP BY t.transaction_id, {grp_key}, t.year
        ),
        trade_sent AS (
            SELECT
                t.transaction_id,
                t.year,
                {grp_key} as _grp_key,
                SUM({trade_lamar_value}) as gave_lamar,
                SUM({trade_points_value}) as gave_points
            FROM {central_table("transactions")} t
            WHERE t.transaction_type IN ('trade', 'trade_pick')
              AND t.manager IS NOT NULL
              AND TRIM(t.manager) <> ''
              AND {transaction_scope}
              {trade_sent_filter}
            GROUP BY t.transaction_id, {grp_key}, t.year
        ),
        trade_perspective AS (
            SELECT
                COALESCE(r.transaction_id, s.transaction_id) as transaction_id,
                COALESCE(r.year, s.year) as year,
                COALESCE(r._grp_key, s._grp_key) as _grp_key,
                COALESCE(r.got_lamar, 0) - COALESCE(s.gave_lamar, 0) as net_lamar,
                COALESCE(r.got_points, 0) - COALESCE(s.gave_points, 0) as net_points
            FROM trade_received r
            FULL OUTER JOIN trade_sent s
              ON r.transaction_id = s.transaction_id
             AND r._grp_key = s._grp_key
             AND r.year = s.year
        ),
        trade_agg AS (
            SELECT _grp_key, year,
                COUNT(DISTINCT transaction_id) as trades,
                SUM(net_lamar) as trade_net_lamar,
                SUM(net_lamar) / NULLIF(COUNT(DISTINCT transaction_id), 0) as trade_lamar_per_trade,
                SUM(CASE WHEN net_lamar > 0 THEN 1 ELSE 0 END) as trade_wins,
                CAST(SUM(CASE WHEN net_lamar > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(DISTINCT transaction_id), 0) as trade_win_rate,
                SUM(net_points) as trade_net_points
            FROM trade_perspective
            GROUP BY _grp_key, year
        )"""
        trade_join = f"LEFT JOIN trade_agg tr ON {ad_grp_ref} = tr._grp_key AND ad.year = tr.year"
        trade_select = "COALESCE(tr.trades, 0), COALESCE(tr.trade_net_lamar, 0), COALESCE(tr.trade_lamar_per_trade, 0), COALESCE(tr.trade_wins, 0), COALESCE(tr.trade_win_rate, 0), COALESCE(tr.trade_net_points, 0),"

    # Build the lamar_added and lamar_dropped expressions for the add_drop CTE
    lamar_added_expr = (
        f"SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN COALESCE(t.{add_lamar_col}, 0) ELSE 0 END)"
        if add_lamar_col
        else "0"
    )
    drop_lamar_fallback = drop_lamar_col if drop_lamar_col else add_lamar_col
    lamar_dropped_expr = (
        f"SUM(CASE WHEN t.transaction_type = 'drop' THEN COALESCE(t.{drop_lamar_fallback}, 0) ELSE 0 END)"
        if drop_lamar_fallback
        else "0"
    )

    fid_agg = f"MAX(t.{fid_col})"

    sql = f"""
        INSERT INTO {central_table("transaction_manager_season")} (
            db_name, manager, year, franchise_id, adds, drops, total_moves, total_faab_bid,
            lamar_added, lamar_dropped, net_lamar, net_points_ros,
            total_transaction_score, avg_transaction_score, avg_faab_per_add, avg_timing_mult,
            avg_ppg_improvement, avg_points_per_faab,
            transaction_grade, transaction_gpa,
            trades, trade_net_lamar, trade_lamar_per_trade, trade_wins, trade_win_rate, trade_net_points,
            best_pickup_player, best_pickup_lamar, worst_drop_player, worst_drop_regret,
            last_updated
        )
        WITH add_drop AS (
            SELECT
                '{db_name}' AS db_name,
                MAX(t.manager) as manager, t.year,
                {fid_agg} as franchise_id,
                SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN 1 ELSE 0 END) as adds,
                SUM(CASE WHEN t.transaction_type = 'drop' THEN 1 ELSE 0 END) as drops,
                SUM(CASE WHEN t.transaction_type IN {_ADD_DROP_TRANSACTION_TYPES_SQL} THEN 1 ELSE 0 END) as total_moves,
                {faab_sum} as total_faab_bid,
                {lamar_added_expr} as lamar_added,
                {lamar_dropped_expr} as lamar_dropped,
                {net_pts_sum} as net_points_ros,
                {score_sum} as total_transaction_score,
                {score_avg} as avg_transaction_score,
                {timing_avg} as avg_timing_mult,
                {ppg_imp} as avg_ppg_improvement,
                {ppf_avg} as avg_points_per_faab
            FROM {central_table("transactions")} t
            WHERE t.transaction_type IN {_ADD_DROP_TRANSACTION_TYPES_SQL}
              AND t.manager IS NOT NULL AND TRIM(t.manager) <> ''
              AND t.{fid_col} IS NOT NULL
              AND {transaction_scope}
            GROUP BY {grp_key}, t.year
        )
        {best_pickup_cte}
        {worst_drop_cte}
        {trade_cte}
        SELECT
            ad.db_name,
            ad.manager, ad.year, ad.franchise_id, ad.adds, ad.drops, ad.total_moves, ad.total_faab_bid,
            ad.lamar_added, ad.lamar_dropped,
            ad.lamar_added - ad.lamar_dropped as net_lamar,
            ad.net_points_ros,
            ad.total_transaction_score, ad.avg_transaction_score,
            ad.total_faab_bid / NULLIF(ad.adds, 0) as avg_faab_per_add,
            ad.avg_timing_mult,
            ad.avg_ppg_improvement,
            ad.avg_points_per_faab,
            NULL as transaction_grade,
            0 as transaction_gpa,
            {trade_select}
            {bp_select}
            {wd_select}
            CURRENT_TIMESTAMP
        FROM add_drop ad
        {bp_join}
        {wd_join}
        {trade_join}
    """
    execute_scoped(conn, sql, db_name, label="transaction_manager_season:insert")

    # Now compute GPA by percentile within each year
    # Use franchise_id for UPDATE joins; do not fall back to manager display names.
    gpa_join_col = "franchise_id"
    execute_scoped(
        conn,
        f"""
        UPDATE {central_table("transaction_manager_season")} s
        SET transaction_gpa = sub.transaction_gpa,
            transaction_grade = CASE
                WHEN sub.transaction_gpa >= 3.85 THEN 'A+'  WHEN sub.transaction_gpa >= 3.50 THEN 'A'
                WHEN sub.transaction_gpa >= 3.15 THEN 'A-'  WHEN sub.transaction_gpa >= 2.85 THEN 'B+'
                WHEN sub.transaction_gpa >= 2.50 THEN 'B'   WHEN sub.transaction_gpa >= 2.15 THEN 'B-'
                WHEN sub.transaction_gpa >= 1.85 THEN 'C+'  WHEN sub.transaction_gpa >= 1.50 THEN 'C'
                WHEN sub.transaction_gpa >= 1.15 THEN 'C-'  WHEN sub.transaction_gpa >= 0.85 THEN 'D+'
                WHEN sub.transaction_gpa >= 0.50 THEN 'D'   WHEN sub.transaction_gpa >= 0.15 THEN 'D-'
                ELSE 'F'
            END
        FROM (
            SELECT {gpa_join_col} as _join_key, year,
                (1.0 - PERCENT_RANK() OVER (PARTITION BY year ORDER BY total_transaction_score DESC)) * 4.0 as transaction_gpa
            FROM {central_table("transaction_manager_season")}
            WHERE {season_scope}
        ) sub
        WHERE s.{gpa_join_col} = sub._join_key AND s.year = sub.year AND s.db_name = '{db_name}'
    """,
        db_name,
        label="transaction_manager_season:update",
    )

    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('transaction_manager_season')} WHERE {season_scope}"
    ).fetchone()[0]
    log(f"  transaction_manager_season: {count} rows")
    return count


def aggregate_transaction_manager_career(conn, db_name: str) -> int:
    """Aggregate transaction_manager_season to career totals with GPA grading."""
    configure_table_catalog(conn)
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('transaction_manager_career')} WHERE db_name = '{db_name}'",
        db_name,
        label="transaction_manager_career:delete",
    )
    season_cols = get_available_columns(conn, db_name, "transaction_manager_season")
    if not season_cols or "franchise_id" not in season_cols:
        log("  franchise_id is required in transaction_manager_season; skipping manager-career transaction aggregation")
        return 0
    season_identity_ref = franchise_identity_sql_ref(None, True)
    season_identity_select = franchise_identity_sql_select(None, True, output_alias="_join_key")
    career_identity_ref = franchise_identity_sql_ref("s", True)

    insert_cols = [
        "db_name",
        "manager",
        "franchise_id",
        "seasons",
        "total_adds",
        "total_drops",
        "total_moves",
        "total_faab_bid",
        "net_lamar",
        "net_points_ros",
        "efficiency",
        "lamar_per_season",
        "total_transaction_score",
        "avg_transaction_score",
        "avg_timing_mult",
        "transaction_grade",
        "transaction_gpa",
        "total_trades",
        "trade_net_lamar",
        "trade_avg_net",
        "trade_win_rate",
        "trade_net_points",
        "last_updated",
    ]

    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table("transaction_manager_career")} ({aggregate_insert_columns("transaction_manager_career", insert_cols)})
        WITH career AS (
            SELECT
                ARG_MAX(manager, year) as manager,
                MAX(franchise_id) as franchise_id,
                COUNT(DISTINCT year) as seasons,
                SUM(adds) as total_adds,
                SUM(drops) as total_drops,
                SUM(total_moves) as total_moves,
                SUM(total_faab_bid) as total_faab_bid,
                SUM(net_lamar) as net_lamar,
                SUM(net_points_ros) as net_points_ros,
                SUM(net_lamar) / NULLIF(SUM(total_moves), 0) as efficiency,
                SUM(net_lamar) / NULLIF(COUNT(DISTINCT year), 0) as lamar_per_season,
                SUM(total_transaction_score * total_moves) / NULLIF(SUM(total_moves), 0) as total_transaction_score,
                SUM(avg_transaction_score * total_moves) / NULLIF(SUM(total_moves), 0) as avg_transaction_score,
                SUM(avg_timing_mult * total_moves) / NULLIF(SUM(total_moves), 0) as avg_timing_mult,
                SUM(trades) as total_trades,
                SUM(trade_net_lamar) as trade_net_lamar,
                SUM(trade_net_lamar) / NULLIF(SUM(trades), 0) as trade_avg_net,
                CAST(SUM(trade_wins) AS DOUBLE) / NULLIF(SUM(trades), 0) as trade_win_rate,
                SUM(trade_net_points) as trade_net_points
            FROM {central_table("transaction_manager_season")}
            WHERE db_name = '{db_name}'
            GROUP BY {season_identity_ref}
        )
        SELECT
            '{db_name}' AS db_name,
            manager, franchise_id, seasons, total_adds, total_drops, total_moves, total_faab_bid,
            net_lamar, net_points_ros, efficiency, lamar_per_season,
            total_transaction_score, avg_transaction_score, avg_timing_mult,
            NULL as transaction_grade, 0 as transaction_gpa,
            total_trades, trade_net_lamar, trade_avg_net, trade_win_rate, trade_net_points,
            CURRENT_TIMESTAMP
        FROM career
    """,
        db_name,
        label="transaction_manager_career:insert",
    )

    # GPA by global percentile — use franchise_id when available
    execute_scoped(
        conn,
        f"""
        UPDATE {central_table("transaction_manager_career")} s
        SET transaction_gpa = sub.transaction_gpa,
            transaction_grade = CASE
                WHEN sub.transaction_gpa >= 3.85 THEN 'A+'  WHEN sub.transaction_gpa >= 3.50 THEN 'A'
                WHEN sub.transaction_gpa >= 3.15 THEN 'A-'  WHEN sub.transaction_gpa >= 2.85 THEN 'B+'
                WHEN sub.transaction_gpa >= 2.50 THEN 'B'   WHEN sub.transaction_gpa >= 2.15 THEN 'B-'
                WHEN sub.transaction_gpa >= 1.85 THEN 'C+'  WHEN sub.transaction_gpa >= 1.50 THEN 'C'
                WHEN sub.transaction_gpa >= 1.15 THEN 'C-'  WHEN sub.transaction_gpa >= 0.85 THEN 'D+'
                WHEN sub.transaction_gpa >= 0.50 THEN 'D'   WHEN sub.transaction_gpa >= 0.15 THEN 'D-'
                ELSE 'F'
            END
        FROM (
            SELECT {season_identity_select},
                (1.0 - PERCENT_RANK() OVER (ORDER BY total_transaction_score DESC)) * 4.0 as transaction_gpa
            FROM {central_table("transaction_manager_career")}
            WHERE db_name = '{db_name}'
        ) sub
        WHERE {career_identity_ref} = sub._join_key AND s.db_name = '{db_name}'
    """,
        db_name,
        label="transaction_manager_career:update",
    )

    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('transaction_manager_career')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  transaction_manager_career: {count} rows")
    return count


def aggregate_transaction_player_career(conn, db_name: str) -> int:
    """Aggregate transactions table to player-career totals."""
    configure_table_catalog(conn)
    cols = get_available_columns(conn, db_name, "transactions")
    add_lamar_col = next((c for c in ["manager_lamar_ros_managed", "fa_lamar_ros", "net_lamar_ros"] if c in cols), None)
    drop_lamar_col = next((c for c in ["player_lamar_ros_total", "fa_lamar_ros"] if c in cols), None)
    regret_col = "drop_regret_score" if "drop_regret_score" in cols else None
    faab_col = "faab_bid" if "faab_bid" in cols else None

    # Build expressions with fallbacks
    faab_sum = (
        f"SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN COALESCE(t.{faab_col}, 0) ELSE 0 END)"
        if faab_col
        else "0"
    )
    faab_avg = (
        f"AVG(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} AND COALESCE(t.{faab_col}, 0) > 0 THEN t.{faab_col} END)"
        if faab_col
        else "0"
    )
    add_lamar_agg = (
        f"SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN COALESCE(t.{add_lamar_col}, 0) ELSE 0 END)"
        if add_lamar_col
        else "0"
    )
    drop_lamar_fallback = drop_lamar_col if drop_lamar_col else add_lamar_col
    drop_lamar_agg = (
        f"SUM(CASE WHEN t.transaction_type = 'drop' THEN COALESCE(t.{drop_lamar_fallback}, 0) ELSE 0 END)"
        if drop_lamar_fallback
        else "0"
    )
    regret_agg = f"AVG(CASE WHEN t.transaction_type = 'drop' THEN t.{regret_col} END)" if regret_col else "0"

    fid_in_txn = "franchise_id" in cols
    fid_agg_player = "STRING_AGG(DISTINCT t.franchise_id, ', ' ORDER BY t.franchise_id)" if fid_in_txn else "NULL"

    # Build optional trade pick partition columns (not all leagues have these)
    pick_parts = []
    if "traded_pick_season" in cols:
        pick_parts.append("COALESCE(CAST(t.traded_pick_season AS VARCHAR), '')")
    if "traded_pick_round" in cols:
        pick_parts.append("COALESCE(CAST(t.traded_pick_round AS VARCHAR), '')")
    if "traded_pick_original_owner" in cols:
        pick_parts.append("COALESCE(t.traded_pick_original_owner, '')")
    pick_partition_cols = "".join(f",\n                            {p}" for p in pick_parts)

    execute_scoped(
        conn,
        f"DELETE FROM {central_table('transaction_player_career')} WHERE db_name = '{db_name}'",
        db_name,
        label="transaction_player_career:delete",
    )
    insert_cols = [
        "db_name",
        "player",
        "position",
        "times_added",
        "times_dropped",
        "times_traded",
        "total_faab_spent",
        "avg_faab",
        "total_lamar_when_added",
        "total_lamar_when_dropped",
        "avg_drop_regret",
        "managers",
        "franchise_ids",
        "years_active",
        "last_updated",
    ]

    execute_scoped(
        conn,
        f"""
        INSERT INTO {central_table("transaction_player_career")} ({aggregate_insert_columns("transaction_player_career", insert_cols)})
        WITH deduped_trade_rows AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT
                    t.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            t.transaction_id,
                            t.transaction_type,
                            COALESCE(t.player, ''),
                            COALESCE(t.position, ''){pick_partition_cols}
                        ORDER BY t.manager, t.source_manager
                    ) as rn
                FROM {central_table("transactions")} t
                WHERE t.transaction_type IN ('trade', 'trade_pick')
                  AND {league_db_filter(db_name, "t")}
            )
            WHERE rn = 1
        ),
        base_transactions AS (
            SELECT *
            FROM {central_table("transactions")}
            WHERE {league_db_filter(db_name)}
              AND transaction_type NOT IN ('trade', 'trade_pick')
            UNION ALL
            SELECT * FROM deduped_trade_rows
        )
        SELECT
            '{db_name}' AS db_name,
            t.player,
            t.position,
            SUM(CASE WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN 1 ELSE 0 END) as times_added,
            SUM(CASE WHEN t.transaction_type = 'drop' THEN 1 ELSE 0 END) as times_dropped,
            SUM(CASE WHEN t.transaction_type IN ('trade', 'trade_pick') THEN 1 ELSE 0 END) as times_traded,
            {faab_sum} as total_faab_spent,
            {faab_avg} as avg_faab,
            {add_lamar_agg} as total_lamar_when_added,
            {drop_lamar_agg} as total_lamar_when_dropped,
            {regret_agg} as avg_drop_regret,
            STRING_AGG(DISTINCT t.manager, ', ' ORDER BY t.manager) as managers,
            {fid_agg_player} as franchise_ids,
            COUNT(DISTINCT t.year) as years_active,
            CURRENT_TIMESTAMP
        FROM base_transactions t
        WHERE t.player IS NOT NULL AND TRIM(t.player) <> ''
          AND t.position IS NOT NULL
        GROUP BY t.player, t.position
    """,
        db_name,
        label="transaction_player_career:insert",
    )
    count = conn.execute(
        f"SELECT COUNT(*) FROM {central_table('transaction_player_career')} WHERE db_name = '{db_name}'"
    ).fetchone()[0]
    log(f"  transaction_player_career: {count} rows")
    return count


def create_transaction_report_card_table(conn, db_name: str) -> bool:
    """Create transaction_report_card table — precomputed data for report card UI."""
    configure_table_catalog(conn)
    ensure_aggregate_table(conn, get_active_catalog(), "transaction_report_card")
    log("  transaction_report_card table ready")
    return True


def aggregate_transaction_report_card(conn, db_name: str, *, year: int | None = None) -> int:
    """Build per-manager per-year report card data with JSON detail columns."""
    import json as _json

    configure_table_catalog(conn)
    season_scope = league_db_filter(db_name, year=year)
    cols = get_available_columns(conn, db_name, "transactions")
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('transaction_report_card')} WHERE {season_scope}",
        db_name,
        label="transaction_report_card:delete",
    )
    if not cols:
        log("  No transaction columns found, skipping report card")
        return 0

    add_lamar_col = next((c for c in ["manager_lamar_ros_managed", "fa_lamar_ros", "net_lamar_ros"] if c in cols), None)
    regret_col = "drop_regret_score" if "drop_regret_score" in cols else None
    grade_col = "transaction_grade" if "transaction_grade" in cols else None
    faab_col = "faab_bid" if "faab_bid" in cols else None
    fid_col = "franchise_id" if "franchise_id" in cols else None
    trade_lamar_expr = (
        "COALESCE(t.trade_asset_lamar, 0)"
        if "trade_asset_lamar" in cols
        else f"COALESCE(t.{add_lamar_col}, 0)"
        if add_lamar_col
        else "0"
    )

    if not add_lamar_col:
        log("  No LAMAR column found, skipping report card")
        return 0
    if not fid_col:
        log("  franchise_id is required in transactions; skipping report card aggregation")
        return 0

    # Build report cards from the franchise-keyed season aggregate so duplicate
    # manager display names never collapse into one identity bucket.
    manager_years = conn.execute(f"""
        SELECT franchise_id, manager, year
        FROM {central_table("transaction_manager_season")}
        WHERE {season_scope}
          AND franchise_id IS NOT NULL
          AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
        ORDER BY manager, year, franchise_id
    """).fetchall()

    rows_inserted = 0

    for franchise_id, mgr, yr in manager_years:
        fid_lit = str(franchise_id).replace("'", "''")

        # Summary stats from precomputed season table
        season_row = conn.execute(f"""
            SELECT adds, drops, total_faab_bid, net_lamar, trades, trade_wins,
                   franchise_id, transaction_grade, transaction_gpa
            FROM {central_table("transaction_manager_season")}
            WHERE db_name = '{db_name}' AND franchise_id = '{fid_lit}' AND year = {yr}
        """).fetchone()

        if not season_row:
            continue

        (
            adds,
            drops,
            total_faab_bid,
            net_lamar,
            trade_count,
            trade_wins,
            franchise_id,
            transaction_grade,
            transaction_gpa,
        ) = season_row
        add_rows = conn.execute(f"""
            SELECT COUNT(*) FROM {central_table("transactions")}
            WHERE db_name = '{db_name}' AND franchise_id = '{fid_lit}' AND year = {yr}
              AND transaction_type IN {_ADD_TRANSACTION_TYPES_SQL}
        """).fetchone()[0]
        total_moves = (add_rows or 0) + (trade_count or 0)
        add_wins = (
            conn.execute(f"""
            SELECT COUNT(*) FROM {central_table("transactions")}
            WHERE db_name = '{db_name}' AND franchise_id = '{fid_lit}' AND year = {yr}
              AND transaction_type IN {_ADD_TRANSACTION_TYPES_SQL}
              AND COALESCE({add_lamar_col}, 0) > 0
        """).fetchone()[0]
            or 0
        )
        win_rate = ((add_wins + (trade_wins or 0)) / total_moves * 100) if total_moves > 0 else 0

        # Grade distribution
        grade_counts = {}
        if grade_col:
            gc_rows = conn.execute(f"""
                SELECT {grade_col}, COUNT(*)
                FROM {central_table("transactions")}
                WHERE db_name = '{db_name}' AND franchise_id = '{fid_lit}' AND year = {yr}
                  AND transaction_type IN {_ADD_TRANSACTION_TYPES_SQL}
                  AND {grade_col} IS NOT NULL AND TRIM({grade_col}) <> ''
                GROUP BY {grade_col}
            """).fetchall()
            grade_counts = {str(r[0]): r[1] for r in gc_rows}

        # Top 4 pickups
        faab_select = f", COALESCE(t.{faab_col}, 0) as faab" if faab_col else ", 0 as faab"
        grade_select = f", t.{grade_col} as grade" if grade_col else ", NULL as grade"
        top_adds = conn.execute(f"""
            SELECT t.player, t.position, COALESCE(t.{add_lamar_col}, 0) as lamar,
                   t.week {grade_select} {faab_select}
            FROM {central_table("transactions")} t
            WHERE t.db_name = '{db_name}' AND t.franchise_id = '{fid_lit}' AND t.year = {yr}
              AND t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} AND t.player IS NOT NULL
            ORDER BY COALESCE(t.{add_lamar_col}, 0) DESC
            LIMIT 4
        """).fetchall()
        top_pickups = [
            {
                "player": r[0],
                "position": r[1] or "",
                "lamar": round(r[2], 1),
                "week": r[3],
                "grade": r[4] or "",
                "faab": round(r[5], 0),
            }
            for r in top_adds
        ]

        # Top 4 worst drops
        worst_drops_list = []
        if regret_col:
            wd_rows = conn.execute(f"""
                SELECT t.player, t.position, COALESCE(t.{regret_col}, 0) as regret,
                       t.week {grade_select}
                FROM {central_table("transactions")} t
                WHERE t.db_name = '{db_name}' AND t.franchise_id = '{fid_lit}' AND t.year = {yr}
                  AND t.transaction_type = 'drop' AND t.player IS NOT NULL
                ORDER BY COALESCE(t.{regret_col}, 0) DESC
                LIMIT 4
            """).fetchall()
            worst_drops_list = [
                {
                    "player": r[0],
                    "position": r[1] or "",
                    "drop_regret": round(r[2], 1),
                    "week": r[3],
                    "grade": r[4] or "",
                }
                for r in wd_rows
            ]

        # Best + worst trade from the manager's perspective using explicit got/gave directions.
        best_trade = None
        worst_trade = None
        trade_rows = []
        if "trade_direction" in cols:
            trade_received_filter, trade_sent_filter = _trade_direction_filters()
            trade_rows = conn.execute(f"""
                WITH trade_received AS (
                    SELECT
                        t.transaction_id,
                        t.year,
                        MIN(t.week) as week,
                        LIST(t.player ORDER BY t.player) as got,
                        SUM({trade_lamar_expr}) as got_lamar
                    FROM {central_table("transactions")} t
                    WHERE t.db_name = '{db_name}' AND t.franchise_id = '{fid_lit}'
                      AND t.year = {yr}
                      AND t.transaction_type IN ('trade', 'trade_pick')
                      {trade_received_filter}
                    GROUP BY t.transaction_id, t.year
                ),
                trade_sent AS (
                    SELECT
                        t.transaction_id,
                        t.year,
                        MIN(t.week) as week,
                        LIST(t.player ORDER BY t.player) as gave,
                        SUM({trade_lamar_expr}) as gave_lamar
                    FROM {central_table("transactions")} t
                    WHERE t.db_name = '{db_name}' AND t.franchise_id = '{fid_lit}'
                      AND t.year = {yr}
                      AND t.transaction_type IN ('trade', 'trade_pick')
                      {trade_sent_filter}
                    GROUP BY t.transaction_id, t.year
                ),
                trade_partners AS (
                    SELECT
                        transaction_id,
                        year,
                        STRING_AGG(DISTINCT t.source_manager, ', ' ORDER BY t.source_manager) as partner
                    FROM {central_table("transactions")} t
                    WHERE t.db_name = '{db_name}' AND t.franchise_id = '{fid_lit}'
                      AND t.year = {yr}
                      AND t.transaction_type IN ('trade', 'trade_pick')
                      AND t.source_manager IS NOT NULL
                    GROUP BY transaction_id, year
                ),
                trade_summary AS (
                    SELECT
                        COALESCE(r.transaction_id, s.transaction_id) as transaction_id,
                        COALESCE(r.year, s.year) as year,
                        COALESCE(r.week, s.week) as week,
                        r.got as got,
                        COALESCE(r.got_lamar, 0) as got_lamar,
                        COALESCE(p.partner, '') as partner,
                        s.gave as gave,
                        COALESCE(s.gave_lamar, 0) as gave_lamar,
                        COALESCE(r.got_lamar, 0) - COALESCE(s.gave_lamar, 0) as net_lamar
                    FROM trade_received r
                    FULL OUTER JOIN trade_sent s
                      ON r.transaction_id = s.transaction_id
                     AND r.year = s.year
                    LEFT JOIN trade_partners p
                      ON COALESCE(r.transaction_id, s.transaction_id) = p.transaction_id
                     AND COALESCE(r.year, s.year) = p.year
                )
                SELECT transaction_id, year, week, got, got_lamar, partner, gave, gave_lamar, net_lamar
                FROM trade_summary
                ORDER BY net_lamar DESC
            """).fetchall()

        if trade_rows:

            def trade_to_dict(r):
                got = [p for p in (list(r[3]) if r[3] else []) if p is not None]
                gave = [p for p in (list(r[6]) if r[6] else []) if p is not None]
                return {
                    "got": got,
                    "gave": gave,
                    "partner": r[5] or "",
                    "net_lamar": round(r[8], 1),
                    "year": r[1],
                    "week": r[2],
                }

            if trade_rows[0][8] > 0:
                best_trade = trade_to_dict(trade_rows[0])
            if len(trade_rows) > 1 and trade_rows[-1][8] < 0:
                worst_trade = trade_to_dict(trade_rows[-1])

        def esc(val):
            """Escape a value for SQL string literal."""
            if val is None:
                return "NULL"
            s = _json.dumps(val) if not isinstance(val, str) else val
            return "'" + s.replace("'", "''") + "'"

        execute_scoped(
            conn,
            f"""
            INSERT INTO {central_table("transaction_report_card")} (
                db_name, manager, year, franchise_id, adds, drops, trades, total_faab_bid,
                total_lamar, win_rate, transaction_grade, transaction_gpa,
                grade_counts, top_pickups, worst_drops, best_trade, worst_trade, last_updated
            )
            VALUES ({esc(db_name)}, {esc(mgr)}, {yr}, {esc(franchise_id)}, {adds or 0}, {drops or 0}, {trade_count or 0},
                    {total_faab_bid or 0}, {net_lamar or 0}, {round(win_rate, 1)},
                    {esc(transaction_grade or "")}, {transaction_gpa or 0},
                    {esc(grade_counts)},
                    {esc(top_pickups)},
                    {esc(worst_drops_list)},
                    {esc(best_trade)},
                    {esc(worst_trade)},
                    CURRENT_TIMESTAMP)
        """,
            db_name,
            label="transaction_report_card:insert",
        )
        rows_inserted += 1

    log(f"  transaction_report_card: {rows_inserted} rows")
    return rows_inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(args):
    """Main entry point for transaction context aggregation."""
    log("=" * 60)
    log("TRANSACTION CONTEXT AGGREGATION")
    log("=" * 60)

    # Determine database name
    db_name, ctx_data = resolve_db_name(args)
    league_name = ctx_data.get("league_name", "")
    if league_name:
        log(f"League: {league_name}")
    log(f"Database: {db_name}")

    if args.dry_run:
        log("[DRY RUN] Would aggregate transaction tables")
        log("  - transaction_manager_season: GROUP BY manager, year")
        log("  - transaction_manager_career: GROUP BY manager")
        log("  - transaction_player_career: GROUP BY player, position")
        return

    # Connect to database (local or MotherDuck)
    conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
    # Bind the shared aggregation catalog to this connection.
    configure_table_catalog(conn)

    try:
        # Create tables
        create_transaction_manager_season_table(conn, db_name)
        create_transaction_manager_career_table(conn, db_name)
        create_transaction_player_career_table(conn, db_name)
        create_transaction_report_card_table(conn, db_name)

        # Aggregate (season first, career rolls up from season, report card last)
        season_count = aggregate_transaction_manager_season(conn, db_name)
        career_count = aggregate_transaction_manager_career(conn, db_name)
        player_count = aggregate_transaction_player_career(conn, db_name)
        rc_count = aggregate_transaction_report_card(conn, db_name)

        log("")
        log(
            f"Done: {season_count} season + {career_count} career + {player_count} player + {rc_count} report-card rows"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate transaction data from transactions table to manager-season/career and player-career tables"
    )
    parser.add_argument("--context", help="Path to league_context.json")
    parser.add_argument("--db", help="Database name (alternative to --context)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )

    args = parser.parse_args()
    main(args)
