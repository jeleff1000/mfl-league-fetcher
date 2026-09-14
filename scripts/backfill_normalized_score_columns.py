#!/usr/bin/env python3
"""Backfill normalized score columns and dependent homepage tables in Fly.

This is the fleet-safe wrapper for score convention changes. It processes one
league at a time, writes only existing score/aggregate columns, and rebuilds
the scoped homepage rows after each league so the public site does not wait on
one giant table rewrite.

Default mode is dry-run.

Examples:
  python scripts/backfill_normalized_score_columns.py --db the_league --dry-run
  python scripts/backfill_normalized_score_columns.py --db the_league --apply
  python scripts/backfill_normalized_score_columns.py --apply --limit 25
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
PIPELINE_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
for path in (SCRIPTS_DIR, PIPELINE_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

LEAGUES_DB = "___leagues"


from bulk_backfill_draft_scores import aggregate_sql as draft_aggregate_sql  # noqa: E402
from bulk_backfill_draft_scores import fetch_draft_headshot_map, homepage_sql as draft_homepage_sql  # noqa: E402
from bulk_backfill_draft_scores import parse_db_names, scoring_sql as draft_scoring_sql  # noqa: E402
from update_sleeper_offseason_draft import (  # noqa: E402
    FlyDuckDBConnection,
    load_dotenv,
    rebuild_homepage_tables,
    sql_literal,
)


@dataclass
class PhaseResult:
    phase: str
    status: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None


@dataclass
class LeagueResult:
    db_name: str
    status: str
    phases: list[PhaseResult] = field(default_factory=list)
    error: str | None = None


def requested_filter_sql(requested: list[str], alias: str = "l") -> str:
    if not requested:
        return "1=1"
    values = ", ".join(sql_literal(name) for name in requested)
    return f"{alias}.db_name IN ({values})"


def inventory_sql(requested: list[str]) -> str:
    requested_filter = requested_filter_sql(requested, "l")
    return f"""
    WITH target_leagues AS (
        SELECT db_name
        FROM (
            SELECT DISTINCT db_name FROM public.league_context WHERE db_name IS NOT NULL
            UNION
            SELECT DISTINCT db_name FROM public.matchup WHERE db_name IS NOT NULL
            UNION
            SELECT DISTINCT db_name FROM public.draft WHERE db_name IS NOT NULL
            UNION
            SELECT DISTINCT db_name FROM public.transactions WHERE db_name IS NOT NULL
        )
        WHERE TRIM(CAST(db_name AS VARCHAR)) <> ''
    ),
    draft_needs AS (
        SELECT d.db_name,
               COUNT(*) FILTER (
                   WHERE d.draft_value_zscore IS NOT NULL
                     AND (
                         d.pick_quality_zscore IS DISTINCT FROM d.draft_value_zscore
                         OR d.pick_score IS DISTINCT FROM ROUND(
                             100.0 + 15.0 * CAST(d.draft_value_zscore AS DOUBLE),
                             3
                         )
                     )
               ) AS draft_score_drift_rows
        FROM public.draft d
        JOIN target_leagues l ON d.db_name = l.db_name
        WHERE {requested_filter_sql(requested, 'd')}
        GROUP BY d.db_name
    ),
    transaction_scored AS (
        SELECT t.db_name,
               t.year,
               CASE
                   WHEN t.transaction_type IN ('add', 'pickup', 'claim', 'waiver', 'add/drop') THEN 'add'
                   WHEN t.transaction_type = 'drop' THEN 'drop'
                   ELSE NULL
               END AS score_family,
               CAST(t.transaction_score AS DOUBLE) AS raw_score
        FROM public.transactions t
        JOIN target_leagues l ON t.db_name = l.db_name
        WHERE {requested_filter_sql(requested, 't')}
          AND t.transaction_score IS NOT NULL
          AND t.transaction_type IN ('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop')
    ),
    transaction_cohorts AS (
        SELECT db_name,
               year,
               score_family,
               COUNT(*) AS score_count,
               MEDIAN(raw_score) AS median_score
        FROM transaction_scored
        WHERE score_family IS NOT NULL
        GROUP BY db_name, year, score_family
    ),
    transaction_needs AS (
        SELECT db_name,
               SUM(CASE WHEN median_score BETWEEN 95.0 AND 105.0 THEN 0 ELSE score_count END) AS transaction_rows_to_normalize
        FROM transaction_cohorts
        GROUP BY db_name
    ),
    power_cohorts AS (
        SELECT m.db_name,
               m.year,
               COUNT(*) AS power_rows,
               MEDIAN(CAST(m.power_rating AS DOUBLE)) AS median_power
        FROM public.matchup m
        JOIN target_leagues l ON m.db_name = l.db_name
        WHERE {requested_filter_sql(requested, 'm')}
          AND m.power_rating IS NOT NULL
        GROUP BY m.db_name, m.year
    ),
    power_needs AS (
        SELECT db_name,
               SUM(CASE WHEN median_power BETWEEN 99.5 AND 100.5 THEN 0 ELSE power_rows END) AS power_rows_to_normalize
        FROM power_cohorts
        GROUP BY db_name
    ),
    playoff_wins AS (
        SELECT m.db_name,
               m.franchise_id,
               SUM(COALESCE(TRY_CAST(win AS INTEGER), 0)) AS wins
        FROM public.matchup m
        WHERE {requested_filter_sql(requested, 'm')}
          AND m.franchise_id IS NOT NULL
          AND COALESCE(TRY_CAST(m.is_bye_week AS INTEGER), 0) = 0
          AND COALESCE(TRY_CAST(m.is_playoffs AS INTEGER), 0) = 1
          AND COALESCE(TRY_CAST(m.is_consolation AS INTEGER), 0) = 0
        GROUP BY m.db_name, m.franchise_id
    ),
    ranking_needs AS (
        SELECT r.db_name,
               COUNT(*) AS ranking_mismatch_rows
        FROM public.homepage_manager_rankings r
        JOIN public.matchup_career c
          ON r.db_name = c.db_name
         AND r.franchise_id = c.franchise_id
        LEFT JOIN playoff_wins p
          ON r.db_name = p.db_name
         AND r.franchise_id = p.franchise_id
        WHERE {requested_filter_sql(requested, 'r')}
          AND r.wins IS NOT NULL
          AND c.wins IS NOT NULL
          AND TRY_CAST(r.wins AS INTEGER) != TRY_CAST(c.wins AS INTEGER) + TRY_CAST(COALESCE(p.wins, 0) AS INTEGER)
        GROUP BY r.db_name
    )
    SELECT l.db_name,
           COALESCE(d.draft_score_drift_rows, 0) AS draft_score_drift_rows,
           COALESCE(t.transaction_rows_to_normalize, 0) AS transaction_rows_to_normalize,
           COALESCE(p.power_rows_to_normalize, 0) AS power_rows_to_normalize,
           COALESCE(r.ranking_mismatch_rows, 0) AS ranking_mismatch_rows,
           (
               COALESCE(d.draft_score_drift_rows, 0)
               + COALESCE(t.transaction_rows_to_normalize, 0)
               + COALESCE(p.power_rows_to_normalize, 0)
               + COALESCE(r.ranking_mismatch_rows, 0)
           ) AS total_drift_rows
    FROM target_leagues l
    LEFT JOIN draft_needs d ON l.db_name = d.db_name
    LEFT JOIN transaction_needs t ON l.db_name = t.db_name
    LEFT JOIN power_needs p ON l.db_name = p.db_name
    LEFT JOIN ranking_needs r ON l.db_name = r.db_name
    WHERE {requested_filter}
    ORDER BY l.db_name;
    """


def fetch_inventory(conn: FlyDuckDBConnection, requested: list[str]) -> list[dict[str, Any]]:
    return conn.execute(inventory_sql(requested)).fetchdf().to_dict("records")


def inventory_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    drift_columns = [
        "draft_score_drift_rows",
        "transaction_rows_to_normalize",
        "power_rows_to_normalize",
        "ranking_mismatch_rows",
        "total_drift_rows",
    ]
    return {
        "event": "inventory_summary",
        "leagues": len(rows),
        "leagues_needing_backfill": sum(1 for row in rows if int(row.get("total_drift_rows") or 0) > 0),
        **{column: sum(int(row.get(column) or 0) for row in rows) for column in drift_columns},
    }


def print_inventory(rows: list[dict[str, Any]], *, max_dirty: int = 50) -> None:
    print(json.dumps(inventory_summary(rows), default=str), flush=True)
    dirty_rows = [row for row in rows if int(row.get("total_drift_rows") or 0) > 0]
    for row in sorted(dirty_rows, key=lambda item: int(item.get("total_drift_rows") or 0), reverse=True)[:max_dirty]:
        print(json.dumps({"event": "inventory_dirty_league", **row}, default=str), flush=True)
    if len(dirty_rows) > max_dirty:
        print(
            json.dumps(
                {
                    "event": "inventory_truncated",
                    "printed": max_dirty,
                    "remaining_dirty_leagues": len(dirty_rows) - max_dirty,
                }
            ),
            flush=True,
        )


def fetch_target_leagues(
    conn: FlyDuckDBConnection,
    requested: list[str],
    *,
    limit: int | None,
    only_needs_backfill: bool,
) -> list[str]:
    if only_needs_backfill:
        rows = fetch_inventory(conn, requested)
        names = [str(row["db_name"]) for row in rows if int(row.get("total_drift_rows") or 0) > 0]
        if limit is not None:
            names = names[: max(int(limit), 0)]
        return names

    if requested:
        names = requested
    else:
        rows = conn.execute(
            """
            SELECT db_name
            FROM (
                SELECT DISTINCT db_name FROM public.league_context WHERE db_name IS NOT NULL
                UNION
                SELECT DISTINCT db_name FROM public.matchup WHERE db_name IS NOT NULL
                UNION
                SELECT DISTINCT db_name FROM public.draft WHERE db_name IS NOT NULL
                UNION
                SELECT DISTINCT db_name FROM public.transactions WHERE db_name IS NOT NULL
            )
            WHERE TRIM(CAST(db_name AS VARCHAR)) <> ''
            ORDER BY db_name
            """
        ).fetchdf()
        names = [str(value) for value in rows["db_name"].tolist()] if not rows.empty else []
    if limit is not None:
        names = names[: max(int(limit), 0)]
    return names


def chunked(values: list[str], size: int) -> list[list[str]]:
    size = max(int(size), 1)
    return [values[i : i + size] for i in range(0, len(values), size)]


def run_writer_sql(conn: FlyDuckDBConnection, sql: str) -> list[dict[str, Any]]:
    return conn.writer.execute(sql, database=LEAGUES_DB) or []


def run_draft_phase(conn: FlyDuckDBConnection, db_name: str, *, apply: bool, skip_aggregates: bool) -> PhaseResult:
    rows: list[dict[str, Any]] = []
    rows.extend(run_writer_sql(conn, draft_scoring_sql([db_name], apply=apply)))
    if not skip_aggregates:
        rows.extend(run_writer_sql(conn, draft_aggregate_sql([db_name], apply=apply)))
    return PhaseResult("draft", "ok", rows=rows)


def transaction_sql(db_name: str, *, apply: bool) -> str:
    db = sql_literal(db_name)
    add_types = "('add', 'pickup', 'claim', 'waiver', 'add/drop')"
    add_drop_types = "('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop')"
    scored_cte = f"""
        scored AS (
            SELECT
                rowid AS rid,
                year,
                CASE
                    WHEN transaction_type IN {add_types} THEN 'add'
                    WHEN transaction_type = 'drop' THEN 'drop'
                    ELSE NULL
                END AS score_family,
                CAST(transaction_score AS DOUBLE) AS raw_score
            FROM public.transactions
            WHERE db_name = {db}
              AND transaction_score IS NOT NULL
              AND transaction_type IN {add_drop_types}
        ),
        cohort_stats AS (
            SELECT
                year,
                score_family,
                MEDIAN(raw_score) AS median_score,
                STDDEV_SAMP(raw_score) AS std_score,
                COUNT(*) AS score_count,
                MEDIAN(raw_score) BETWEEN 95.0 AND 105.0 AS already_indexed
            FROM scored
            WHERE score_family IS NOT NULL
            GROUP BY year, score_family
        )
    """

    if not apply:
        return f"""
        WITH {scored_cte}
        SELECT 'dry_run_transactions' AS phase,
               COUNT(*) AS cohort_count,
               SUM(score_count) AS scored_rows,
               SUM(CASE WHEN already_indexed THEN 0 ELSE score_count END) AS rows_to_normalize,
               MIN(median_score) AS min_median_score,
               MAX(median_score) AS max_median_score,
               MIN(std_score) AS min_std_score,
               MAX(std_score) AS max_std_score
        FROM cohort_stats;
        """

    return f"""
    BEGIN TRANSACTION;
    WITH {scored_cte},
    normalized AS (
        SELECT
            s.rid,
            CASE
                WHEN cs.already_indexed THEN ROUND(s.raw_score, 1)
                WHEN cs.score_count <= 1 THEN 100.0
                WHEN COALESCE(cs.std_score, 0) = 0 THEN 100.0
                ELSE ROUND(
                    100.0 + 15.0 * ((s.raw_score - cs.median_score) / GREATEST(cs.std_score, 1.0)),
                    1
                )
            END AS normalized_score
        FROM scored s
        JOIN cohort_stats cs
          ON s.year = cs.year AND s.score_family = cs.score_family
    )
    UPDATE public.transactions AS t
    SET transaction_score = n.normalized_score
    FROM normalized n
    WHERE t.rowid = n.rid
      AND t.db_name = {db};

    UPDATE public.transactions
    SET transaction_grade = NULL,
        score_percentile = NULL
    WHERE db_name = {db}
      AND transaction_type IN {add_drop_types};

    WITH ranked AS (
        SELECT
            rowid AS rid,
            CASE
                WHEN transaction_type IN {add_types} THEN 'add'
                WHEN transaction_type = 'drop' THEN 'drop'
            END AS score_family,
            COUNT(*) OVER (
                PARTITION BY CASE
                    WHEN transaction_type IN {add_types} THEN 'add'
                    WHEN transaction_type = 'drop' THEN 'drop'
                END
            ) AS pool_size,
            PERCENT_RANK() OVER (
                PARTITION BY CASE
                    WHEN transaction_type IN {add_types} THEN 'add'
                    WHEN transaction_type = 'drop' THEN 'drop'
                END
                ORDER BY transaction_score ASC
            ) * 100 AS pctile
        FROM public.transactions
        WHERE db_name = {db}
          AND transaction_score IS NOT NULL
          AND transaction_type IN {add_drop_types}
    )
    UPDATE public.transactions AS t
    SET score_percentile = CASE WHEN r.pool_size >= 30 THEN r.pctile ELSE NULL END,
        transaction_grade = CASE
            WHEN r.pool_size < 30 THEN NULL
            WHEN r.pctile >= 95 THEN 'A+'
            WHEN r.pctile >= 85 THEN 'A'
            WHEN r.pctile >= 75 THEN 'A-'
            WHEN r.pctile >= 65 THEN 'B+'
            WHEN r.pctile >= 50 THEN 'B'
            WHEN r.pctile >= 35 THEN 'B-'
            WHEN r.pctile >= 20 THEN 'C'
            WHEN r.pctile >= 10 THEN 'D'
            ELSE 'F'
        END
    FROM ranked r
    WHERE t.rowid = r.rid
      AND t.db_name = {db};
    COMMIT;

    WITH {scored_cte}
    SELECT 'transactions' AS phase,
           COUNT(*) AS cohort_count,
           SUM(score_count) AS scored_rows,
           MIN(median_score) AS min_median_score,
           MAX(median_score) AS max_median_score,
           MIN(std_score) AS min_std_score,
           MAX(std_score) AS max_std_score
    FROM cohort_stats;
    """


def transaction_batch_sql(db_names: list[str], *, apply: bool) -> str:
    target = requested_filter_sql(db_names, "t")
    season_target = requested_filter_sql(db_names, "s")
    career_target = requested_filter_sql(db_names, "c")
    add_types = "('add', 'pickup', 'claim', 'waiver', 'add/drop')"
    add_drop_types = "('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop')"
    add_lamar = "COALESCE(t.manager_lamar_ros_managed, t.fa_lamar_ros, 0)"
    drop_lamar = "COALESCE(t.player_lamar_ros_total, t.fa_lamar_ros, 0)"
    trade_lamar = "COALESCE(t.trade_asset_lamar, t.manager_lamar_ros_managed, 0)"
    trade_points = "COALESCE(t.total_points_ros_managed, t.total_points_ros_total, 0)"

    scored_cte = f"""
        scored AS (
            SELECT
                t.rowid AS rid,
                t.db_name,
                t.year,
                CASE
                    WHEN t.transaction_type IN {add_types} THEN 'add'
                    WHEN t.transaction_type = 'drop' THEN 'drop'
                    ELSE NULL
                END AS score_family,
                CAST(t.transaction_score AS DOUBLE) AS raw_score
            FROM public.transactions t
            WHERE {target}
              AND t.transaction_score IS NOT NULL
              AND t.transaction_type IN {add_drop_types}
        ),
        cohort_stats AS (
            SELECT
                db_name,
                year,
                score_family,
                MEDIAN(raw_score) AS median_score,
                STDDEV_SAMP(raw_score) AS std_score,
                COUNT(*) AS score_count,
                MEDIAN(raw_score) BETWEEN 95.0 AND 105.0 AS already_indexed
            FROM scored
            WHERE score_family IS NOT NULL
            GROUP BY db_name, year, score_family
        )
    """

    if not apply:
        return f"""
        WITH {scored_cte}
        SELECT 'dry_run_transactions_batch' AS phase,
               COUNT(DISTINCT db_name) AS league_count,
               COUNT(*) AS cohort_count,
               SUM(score_count) AS scored_rows,
               SUM(CASE WHEN already_indexed THEN 0 ELSE score_count END) AS rows_to_normalize,
               MIN(median_score) AS min_median_score,
               MAX(median_score) AS max_median_score,
               MIN(std_score) AS min_std_score,
               MAX(std_score) AS max_std_score
        FROM cohort_stats;
        """

    return f"""
    BEGIN TRANSACTION;
    WITH {scored_cte},
    normalized AS (
        SELECT
            s.rid,
            s.db_name,
            CASE
                WHEN cs.already_indexed THEN ROUND(s.raw_score, 1)
                WHEN cs.score_count <= 1 THEN 100.0
                WHEN COALESCE(cs.std_score, 0) = 0 THEN 100.0
                ELSE ROUND(
                    100.0 + 15.0 * ((s.raw_score - cs.median_score) / GREATEST(cs.std_score, 1.0)),
                    1
                )
            END AS normalized_score
        FROM scored s
        JOIN cohort_stats cs
          ON s.db_name = cs.db_name
         AND s.year = cs.year
         AND s.score_family = cs.score_family
    )
    UPDATE public.transactions AS t
    SET transaction_score = n.normalized_score
    FROM normalized n
    WHERE t.rowid = n.rid
      AND t.db_name = n.db_name
      AND {target};

    UPDATE public.transactions AS t
    SET transaction_grade = NULL,
        score_percentile = NULL
    WHERE {target}
      AND t.transaction_type IN {add_drop_types};

    WITH ranked AS (
        SELECT
            t.rowid AS rid,
            t.db_name,
            CASE
                WHEN t.transaction_type IN {add_types} THEN 'add'
                WHEN t.transaction_type = 'drop' THEN 'drop'
            END AS score_family,
            COUNT(*) OVER (
                PARTITION BY t.db_name,
                             CASE
                                 WHEN t.transaction_type IN {add_types} THEN 'add'
                                 WHEN t.transaction_type = 'drop' THEN 'drop'
                             END
            ) AS pool_size,
            PERCENT_RANK() OVER (
                PARTITION BY t.db_name,
                             CASE
                                 WHEN t.transaction_type IN {add_types} THEN 'add'
                                 WHEN t.transaction_type = 'drop' THEN 'drop'
                             END
                ORDER BY t.transaction_score ASC
            ) * 100 AS pctile
        FROM public.transactions t
        WHERE {target}
          AND t.transaction_score IS NOT NULL
          AND t.transaction_type IN {add_drop_types}
    )
    UPDATE public.transactions AS t
    SET score_percentile = CASE WHEN r.pool_size >= 30 THEN r.pctile ELSE NULL END,
        transaction_grade = CASE
            WHEN r.pool_size < 30 THEN NULL
            WHEN r.pctile >= 95 THEN 'A+'
            WHEN r.pctile >= 85 THEN 'A'
            WHEN r.pctile >= 75 THEN 'A-'
            WHEN r.pctile >= 65 THEN 'B+'
            WHEN r.pctile >= 50 THEN 'B'
            WHEN r.pctile >= 35 THEN 'B-'
            WHEN r.pctile >= 20 THEN 'C'
            WHEN r.pctile >= 10 THEN 'D'
            ELSE 'F'
        END
    FROM ranked r
    WHERE t.rowid = r.rid
      AND t.db_name = r.db_name
      AND {target};

    DELETE FROM public.transaction_manager_season AS s WHERE {season_target};

    INSERT INTO public.transaction_manager_season (
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
            t.db_name,
            ARG_MAX(t.manager, COALESCE(t.timestamp, 0)) AS manager,
            t.year,
            t.franchise_id,
            SUM(CASE WHEN t.transaction_type IN {add_types} THEN 1 ELSE 0 END) AS adds,
            SUM(CASE WHEN t.transaction_type = 'drop' THEN 1 ELSE 0 END) AS drops,
            SUM(CASE WHEN t.transaction_type IN {add_drop_types} THEN 1 ELSE 0 END) AS total_moves,
            SUM(CASE WHEN t.transaction_type IN {add_types} THEN COALESCE(t.faab_bid, 0) ELSE 0 END) AS total_faab_bid,
            SUM(CASE WHEN t.transaction_type IN {add_types} THEN {add_lamar} ELSE 0 END) AS lamar_added,
            SUM(CASE WHEN t.transaction_type = 'drop' THEN {drop_lamar} ELSE 0 END) AS lamar_dropped,
            SUM(CASE WHEN t.transaction_type IN {add_types} THEN COALESCE(t.total_points_ros_managed, 0) ELSE 0 END) AS net_points_ros,
            AVG(t.transaction_score) AS total_transaction_score,
            AVG(t.transaction_score) AS avg_transaction_score,
            0 AS avg_timing_mult,
            AVG(CASE
                WHEN t.transaction_type IN {add_types}
                 AND t.ppg_before_transaction IS NOT NULL
                 AND t.ppg_after_transaction IS NOT NULL
                THEN t.ppg_after_transaction - t.ppg_before_transaction
            END) AS avg_ppg_improvement,
            0 AS avg_points_per_faab
        FROM public.transactions t
        WHERE {target}
          AND t.transaction_type IN {add_drop_types}
          AND t.manager IS NOT NULL
          AND TRIM(t.manager) <> ''
          AND t.franchise_id IS NOT NULL
          AND TRIM(CAST(t.franchise_id AS VARCHAR)) <> ''
        GROUP BY t.db_name, t.franchise_id, t.year
    ),
    best_pickup AS (
        SELECT t.db_name,
               t.franchise_id,
               t.year,
               FIRST(t.player ORDER BY {add_lamar} DESC NULLS LAST) AS player,
               MAX({add_lamar}) AS lamar
        FROM public.transactions t
        WHERE {target}
          AND t.transaction_type IN {add_types}
          AND t.player IS NOT NULL
          AND t.franchise_id IS NOT NULL
        GROUP BY t.db_name, t.franchise_id, t.year
    ),
    worst_drop AS (
        SELECT t.db_name,
               t.franchise_id,
               t.year,
               NULL::VARCHAR AS player,
               0::DOUBLE AS regret
        FROM public.transactions t
        WHERE {target}
          AND t.transaction_type = 'drop'
          AND t.franchise_id IS NOT NULL
        GROUP BY t.db_name, t.franchise_id, t.year
    ),
    trade_received AS (
        SELECT t.db_name,
               t.transaction_id,
               t.year,
               t.franchise_id,
               SUM({trade_lamar}) AS got_lamar,
               SUM({trade_points}) AS got_points
        FROM public.transactions t
        WHERE {target}
          AND t.transaction_type IN ('trade', 'trade_pick')
          AND t.trade_direction = 'received'
          AND t.franchise_id IS NOT NULL
        GROUP BY t.db_name, t.transaction_id, t.franchise_id, t.year
    ),
    trade_sent AS (
        SELECT t.db_name,
               t.transaction_id,
               t.year,
               t.franchise_id,
               SUM({trade_lamar}) AS gave_lamar,
               SUM({trade_points}) AS gave_points
        FROM public.transactions t
        WHERE {target}
          AND t.transaction_type IN ('trade', 'trade_pick')
          AND t.trade_direction = 'sent'
          AND t.franchise_id IS NOT NULL
        GROUP BY t.db_name, t.transaction_id, t.franchise_id, t.year
    ),
    trade_perspective AS (
        SELECT
            COALESCE(r.db_name, s.db_name) AS db_name,
            COALESCE(r.transaction_id, s.transaction_id) AS transaction_id,
            COALESCE(r.year, s.year) AS year,
            COALESCE(r.franchise_id, s.franchise_id) AS franchise_id,
            COALESCE(r.got_lamar, 0) - COALESCE(s.gave_lamar, 0) AS net_lamar,
            COALESCE(r.got_points, 0) - COALESCE(s.gave_points, 0) AS net_points
        FROM trade_received r
        FULL OUTER JOIN trade_sent s
          ON r.db_name = s.db_name
         AND r.transaction_id = s.transaction_id
         AND r.franchise_id = s.franchise_id
         AND r.year = s.year
    ),
    trade_agg AS (
        SELECT db_name,
               franchise_id,
               year,
               COUNT(DISTINCT transaction_id) AS trades,
               SUM(net_lamar) AS trade_net_lamar,
               SUM(net_lamar) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS trade_lamar_per_trade,
               SUM(CASE WHEN net_lamar > 0 THEN 1 ELSE 0 END) AS trade_wins,
               CAST(SUM(CASE WHEN net_lamar > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(DISTINCT transaction_id), 0) AS trade_win_rate,
               SUM(net_points) AS trade_net_points
        FROM trade_perspective
        GROUP BY db_name, franchise_id, year
    )
    SELECT
        ad.db_name,
        ad.manager,
        ad.year,
        ad.franchise_id,
        ad.adds,
        ad.drops,
        ad.total_moves,
        ad.total_faab_bid,
        ad.lamar_added,
        ad.lamar_dropped,
        ad.lamar_added - ad.lamar_dropped AS net_lamar,
        ad.net_points_ros,
        ad.total_transaction_score,
        ad.avg_transaction_score,
        ad.total_faab_bid / NULLIF(ad.adds, 0) AS avg_faab_per_add,
        ad.avg_timing_mult,
        ad.avg_ppg_improvement,
        ad.avg_points_per_faab,
        NULL AS transaction_grade,
        0 AS transaction_gpa,
        COALESCE(tr.trades, 0),
        COALESCE(tr.trade_net_lamar, 0),
        COALESCE(tr.trade_lamar_per_trade, 0),
        COALESCE(tr.trade_wins, 0),
        COALESCE(tr.trade_win_rate, 0),
        COALESCE(tr.trade_net_points, 0),
        bp.player,
        COALESCE(bp.lamar, 0),
        wd.player,
        COALESCE(wd.regret, 0),
        CURRENT_TIMESTAMP
    FROM add_drop ad
    LEFT JOIN best_pickup bp
      ON ad.db_name = bp.db_name AND ad.franchise_id = bp.franchise_id AND ad.year = bp.year
    LEFT JOIN worst_drop wd
      ON ad.db_name = wd.db_name AND ad.franchise_id = wd.franchise_id AND ad.year = wd.year
    LEFT JOIN trade_agg tr
      ON ad.db_name = tr.db_name AND ad.franchise_id = tr.franchise_id AND ad.year = tr.year;

    UPDATE public.transaction_manager_season AS s
    SET transaction_gpa = sub.transaction_gpa,
        transaction_grade = CASE
            WHEN sub.transaction_gpa >= 3.85 THEN 'A+'
            WHEN sub.transaction_gpa >= 3.50 THEN 'A'
            WHEN sub.transaction_gpa >= 3.15 THEN 'A-'
            WHEN sub.transaction_gpa >= 2.85 THEN 'B+'
            WHEN sub.transaction_gpa >= 2.50 THEN 'B'
            WHEN sub.transaction_gpa >= 2.15 THEN 'B-'
            WHEN sub.transaction_gpa >= 1.85 THEN 'C+'
            WHEN sub.transaction_gpa >= 1.50 THEN 'C'
            WHEN sub.transaction_gpa >= 1.15 THEN 'C-'
            WHEN sub.transaction_gpa >= 0.85 THEN 'D+'
            WHEN sub.transaction_gpa >= 0.50 THEN 'D'
            WHEN sub.transaction_gpa >= 0.15 THEN 'D-'
            ELSE 'F'
        END
    FROM (
        SELECT db_name,
               franchise_id,
               year,
               (1.0 - PERCENT_RANK() OVER (PARTITION BY db_name, year ORDER BY total_transaction_score DESC)) * 4.0 AS transaction_gpa
        FROM public.transaction_manager_season s
        WHERE {season_target}
    ) sub
    WHERE s.db_name = sub.db_name
      AND s.franchise_id = sub.franchise_id
      AND s.year = sub.year
      AND {season_target};

    DELETE FROM public.transaction_manager_career AS c WHERE {career_target};

    INSERT INTO public.transaction_manager_career (
        db_name, manager, franchise_id, seasons, total_adds, total_drops, total_moves,
        total_faab_bid, net_lamar, net_points_ros, efficiency, lamar_per_season,
        total_transaction_score, avg_transaction_score, avg_timing_mult,
        transaction_grade, transaction_gpa, total_trades, trade_net_lamar,
        trade_avg_net, trade_win_rate, trade_net_points, last_updated
    )
    WITH career AS (
        SELECT
            db_name,
            ARG_MAX(manager, year) AS manager,
            franchise_id,
            COUNT(DISTINCT year) AS seasons,
            SUM(adds) AS total_adds,
            SUM(drops) AS total_drops,
            SUM(total_moves) AS total_moves,
            SUM(total_faab_bid) AS total_faab_bid,
            SUM(net_lamar) AS net_lamar,
            SUM(net_points_ros) AS net_points_ros,
            SUM(net_lamar) / NULLIF(SUM(total_moves), 0) AS efficiency,
            SUM(net_lamar) / NULLIF(COUNT(DISTINCT year), 0) AS lamar_per_season,
            SUM(total_transaction_score * total_moves) / NULLIF(SUM(total_moves), 0) AS total_transaction_score,
            SUM(avg_transaction_score * total_moves) / NULLIF(SUM(total_moves), 0) AS avg_transaction_score,
            SUM(avg_timing_mult * total_moves) / NULLIF(SUM(total_moves), 0) AS avg_timing_mult,
            SUM(trades) AS total_trades,
            SUM(trade_net_lamar) AS trade_net_lamar,
            SUM(trade_net_lamar) / NULLIF(SUM(trades), 0) AS trade_avg_net,
            CAST(SUM(trade_wins) AS DOUBLE) / NULLIF(SUM(trades), 0) AS trade_win_rate,
            SUM(trade_net_points) AS trade_net_points
        FROM public.transaction_manager_season s
        WHERE {season_target}
        GROUP BY db_name, franchise_id
    )
    SELECT
        db_name,
        manager,
        franchise_id,
        seasons,
        total_adds,
        total_drops,
        total_moves,
        total_faab_bid,
        net_lamar,
        net_points_ros,
        efficiency,
        lamar_per_season,
        total_transaction_score,
        avg_transaction_score,
        avg_timing_mult,
        NULL AS transaction_grade,
        0 AS transaction_gpa,
        total_trades,
        trade_net_lamar,
        trade_avg_net,
        trade_win_rate,
        trade_net_points,
        CURRENT_TIMESTAMP
    FROM career;

    UPDATE public.transaction_manager_career AS c
    SET transaction_gpa = sub.transaction_gpa,
        transaction_grade = CASE
            WHEN sub.transaction_gpa >= 3.85 THEN 'A+'
            WHEN sub.transaction_gpa >= 3.50 THEN 'A'
            WHEN sub.transaction_gpa >= 3.15 THEN 'A-'
            WHEN sub.transaction_gpa >= 2.85 THEN 'B+'
            WHEN sub.transaction_gpa >= 2.50 THEN 'B'
            WHEN sub.transaction_gpa >= 2.15 THEN 'B-'
            WHEN sub.transaction_gpa >= 1.85 THEN 'C+'
            WHEN sub.transaction_gpa >= 1.50 THEN 'C'
            WHEN sub.transaction_gpa >= 1.15 THEN 'C-'
            WHEN sub.transaction_gpa >= 0.85 THEN 'D+'
            WHEN sub.transaction_gpa >= 0.50 THEN 'D'
            WHEN sub.transaction_gpa >= 0.15 THEN 'D-'
            ELSE 'F'
        END
    FROM (
        SELECT db_name,
               franchise_id,
               (1.0 - PERCENT_RANK() OVER (PARTITION BY db_name ORDER BY total_transaction_score DESC)) * 4.0 AS transaction_gpa
        FROM public.transaction_manager_career c
        WHERE {career_target}
    ) sub
    WHERE c.db_name = sub.db_name
      AND c.franchise_id = sub.franchise_id
      AND {career_target};
    COMMIT;

    WITH {scored_cte}
    SELECT 'transactions_batch' AS phase,
           COUNT(DISTINCT db_name) AS league_count,
           COUNT(*) AS cohort_count,
           SUM(score_count) AS scored_rows,
           MIN(median_score) AS min_median_score,
           MAX(median_score) AS max_median_score,
           MIN(std_score) AS min_std_score,
           MAX(std_score) AS max_std_score,
           (SELECT COUNT(*) FROM public.transaction_manager_season s WHERE {season_target}) AS transaction_manager_season_rows,
           (SELECT COUNT(*) FROM public.transaction_manager_career c WHERE {career_target}) AS transaction_manager_career_rows
    FROM cohort_stats;
    """


def run_transaction_phase(
    conn: FlyDuckDBConnection,
    db_name: str,
    *,
    apply: bool,
    include_detail_aggregates: bool,
) -> PhaseResult:
    rows = run_writer_sql(conn, transaction_sql(db_name, apply=apply))
    if not apply:
        return PhaseResult("transactions", "dry-run", rows=rows)

    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_career,
        aggregate_transaction_manager_season,
        create_transaction_manager_career_table,
        create_transaction_manager_season_table,
    )

    create_transaction_manager_season_table(conn, db_name)
    create_transaction_manager_career_table(conn, db_name)
    counts = {
        "transaction_manager_season": aggregate_transaction_manager_season(conn, db_name),
        "transaction_manager_career": aggregate_transaction_manager_career(conn, db_name),
    }
    if include_detail_aggregates:
        from multi_league.transformations.aggregation.aggregate_transaction_context import (
            aggregate_transaction_player_career,
            aggregate_transaction_report_card,
            create_transaction_player_career_table,
            create_transaction_report_card_table,
        )

        create_transaction_player_career_table(conn, db_name)
        create_transaction_report_card_table(conn, db_name)
        counts["transaction_player_career"] = aggregate_transaction_player_career(conn, db_name)
        counts["transaction_report_card"] = aggregate_transaction_report_card(conn, db_name)
    return PhaseResult("transactions", "ok", rows=rows, counts=counts)


def power_sql(db_name: str, *, apply: bool) -> str:
    db = sql_literal(db_name)
    if not apply:
        return f"""
        WITH cohorts AS (
            SELECT year,
                   COUNT(*) AS rows,
                   MEDIAN(CAST(power_rating AS DOUBLE)) AS median_power,
                   MIN(CAST(power_rating AS DOUBLE)) AS min_power,
                   MAX(CAST(power_rating AS DOUBLE)) AS max_power
            FROM public.matchup
            WHERE db_name = {db}
              AND power_rating IS NOT NULL
            GROUP BY year
        )
        SELECT 'dry_run_power' AS phase,
               COUNT(*) AS season_count,
               SUM(rows) AS power_rows,
               MIN(median_power) AS min_median_power,
               MAX(median_power) AS max_median_power
        FROM cohorts;
        """

    return f"""
    BEGIN TRANSACTION;
    WITH cohorts AS (
        SELECT db_name,
               year,
               MEDIAN(CAST(power_rating AS DOUBLE)) AS median_power,
               COUNT(*) AS rows
        FROM public.matchup
        WHERE db_name = {db}
          AND power_rating IS NOT NULL
        GROUP BY db_name, year
    )
    UPDATE public.matchup AS m
    SET power_rating = CASE
        WHEN c.median_power = 0 THEN 100.0
        ELSE ROUND((CAST(m.power_rating AS DOUBLE) / c.median_power) * 100.0, 2)
    END
    FROM cohorts AS c
    WHERE m.db_name = c.db_name
      AND m.year = c.year
      AND m.db_name = {db}
      AND m.power_rating IS NOT NULL
      AND c.rows > 0
      AND c.median_power IS NOT NULL;

    WITH settings_bounds AS (
        SELECT db_name,
               TRY_CAST(year AS INTEGER) AS year,
               TRY_CAST(playoff_start_week AS INTEGER) - 1 AS last_reg_week
        FROM public.league_settings
        WHERE db_name = {db}
          AND playoff_start_week IS NOT NULL
    ),
    fallback_bounds AS (
        SELECT db_name,
               TRY_CAST(year AS INTEGER) AS year,
               MAX(TRY_CAST(week AS INTEGER)) AS last_reg_week
        FROM public.matchup
        WHERE db_name = {db}
          AND COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 0
          AND COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 0
          AND manager IS NOT NULL
        GROUP BY db_name, TRY_CAST(year AS INTEGER)
    ),
    year_bounds AS (
        SELECT COALESCE(s.db_name, f.db_name) AS db_name,
               COALESCE(s.year, f.year) AS year,
               COALESCE(s.last_reg_week, f.last_reg_week, 14) AS last_reg_week
        FROM settings_bounds s
        FULL OUTER JOIN fallback_bounds f
          ON s.db_name = f.db_name AND s.year = f.year
    ),
    season_power AS (
        SELECT m.db_name,
               m.franchise_id,
               TRY_CAST(m.year AS INTEGER) AS year,
               ARG_MAX(
                   CAST(m.power_rating AS DOUBLE),
                   CASE
                       WHEN TRY_CAST(m.week AS INTEGER) <= y.last_reg_week THEN TRY_CAST(m.week AS INTEGER)
                       ELSE NULL
                   END
               ) AS power_rating
        FROM public.matchup m
        JOIN year_bounds y
          ON m.db_name = y.db_name AND TRY_CAST(m.year AS INTEGER) = y.year
        WHERE m.db_name = {db}
          AND m.franchise_id IS NOT NULL
          AND m.power_rating IS NOT NULL
        GROUP BY m.db_name, m.franchise_id, TRY_CAST(m.year AS INTEGER)
    )
    UPDATE public.matchup_season AS ms
    SET power_rating = ROUND(sp.power_rating, 2)
    FROM season_power sp
    WHERE ms.db_name = sp.db_name
      AND ms.franchise_id = sp.franchise_id
      AND TRY_CAST(ms.year AS INTEGER) = sp.year
      AND ms.db_name = {db};

    WITH career_power AS (
        SELECT db_name,
               franchise_id,
               ROUND(AVG(CAST(power_rating AS DOUBLE)), 2) AS avg_power_rating
        FROM public.matchup_season
        WHERE db_name = {db}
          AND franchise_id IS NOT NULL
          AND power_rating IS NOT NULL
        GROUP BY db_name, franchise_id
    )
    UPDATE public.matchup_career AS mc
    SET avg_power_rating = cp.avg_power_rating
    FROM career_power cp
    WHERE mc.db_name = cp.db_name
      AND mc.franchise_id = cp.franchise_id
      AND mc.db_name = {db};
    COMMIT;

    WITH cohorts AS (
        SELECT year,
               COUNT(*) AS rows,
               MEDIAN(CAST(power_rating AS DOUBLE)) AS median_power,
               MIN(CAST(power_rating AS DOUBLE)) AS min_power,
               MAX(CAST(power_rating AS DOUBLE)) AS max_power
        FROM public.matchup
        WHERE db_name = {db}
          AND power_rating IS NOT NULL
        GROUP BY year
    )
    SELECT 'power' AS phase,
           COUNT(*) AS season_count,
           SUM(rows) AS power_rows,
           MIN(median_power) AS min_median_power,
           MAX(median_power) AS max_median_power,
           (SELECT COUNT(*) FROM public.matchup_season WHERE db_name = {db} AND power_rating IS NOT NULL) AS matchup_season_power_rows,
           (SELECT COUNT(*) FROM public.matchup_career WHERE db_name = {db} AND avg_power_rating IS NOT NULL) AS matchup_career_power_rows
    FROM cohorts;
    """


def power_batch_sql(db_names: list[str], *, apply: bool) -> str:
    target = requested_filter_sql(db_names, "m")
    season_target = requested_filter_sql(db_names, "ms")
    career_target = requested_filter_sql(db_names, "mc")
    if not apply:
        return f"""
        WITH cohorts AS (
            SELECT m.db_name,
                   m.year,
                   COUNT(*) AS rows,
                   MEDIAN(CAST(m.power_rating AS DOUBLE)) AS median_power,
                   MIN(CAST(m.power_rating AS DOUBLE)) AS min_power,
                   MAX(CAST(m.power_rating AS DOUBLE)) AS max_power
            FROM public.matchup m
            WHERE {target}
              AND m.power_rating IS NOT NULL
            GROUP BY m.db_name, m.year
        )
        SELECT 'dry_run_power_batch' AS phase,
               COUNT(DISTINCT db_name) AS league_count,
               COUNT(*) AS season_count,
               SUM(rows) AS power_rows,
               MIN(median_power) AS min_median_power,
               MAX(median_power) AS max_median_power
        FROM cohorts;
        """

    return f"""
    BEGIN TRANSACTION;
    WITH cohorts AS (
        SELECT m.db_name,
               m.year,
               MEDIAN(CAST(m.power_rating AS DOUBLE)) AS median_power,
               COUNT(*) AS rows
        FROM public.matchup m
        WHERE {target}
          AND m.power_rating IS NOT NULL
        GROUP BY m.db_name, m.year
    )
    UPDATE public.matchup AS m
    SET power_rating = CASE
        WHEN c.median_power = 0 THEN 100.0
        ELSE ROUND((CAST(m.power_rating AS DOUBLE) / c.median_power) * 100.0, 2)
    END
    FROM cohorts AS c
    WHERE m.db_name = c.db_name
      AND m.year = c.year
      AND {target}
      AND m.power_rating IS NOT NULL
      AND c.rows > 0
      AND c.median_power IS NOT NULL;

    WITH settings_bounds AS (
        SELECT db_name,
               TRY_CAST(year AS INTEGER) AS year,
               TRY_CAST(playoff_start_week AS INTEGER) - 1 AS last_reg_week
        FROM public.league_settings
        WHERE db_name IN ({", ".join(sql_literal(name) for name in db_names)})
          AND playoff_start_week IS NOT NULL
    ),
    fallback_bounds AS (
        SELECT m.db_name,
               TRY_CAST(m.year AS INTEGER) AS year,
               MAX(TRY_CAST(m.week AS INTEGER)) AS last_reg_week
        FROM public.matchup m
        WHERE {target}
          AND COALESCE(TRY_CAST(m.is_playoffs AS INTEGER), 0) = 0
          AND COALESCE(TRY_CAST(m.is_consolation AS INTEGER), 0) = 0
          AND m.manager IS NOT NULL
        GROUP BY m.db_name, TRY_CAST(m.year AS INTEGER)
    ),
    year_bounds AS (
        SELECT COALESCE(s.db_name, f.db_name) AS db_name,
               COALESCE(s.year, f.year) AS year,
               COALESCE(s.last_reg_week, f.last_reg_week, 14) AS last_reg_week
        FROM settings_bounds s
        FULL OUTER JOIN fallback_bounds f
          ON s.db_name = f.db_name AND s.year = f.year
    ),
    season_power AS (
        SELECT m.db_name,
               m.franchise_id,
               TRY_CAST(m.year AS INTEGER) AS year,
               ARG_MAX(
                   CAST(m.power_rating AS DOUBLE),
                   CASE
                       WHEN TRY_CAST(m.week AS INTEGER) <= y.last_reg_week THEN TRY_CAST(m.week AS INTEGER)
                       ELSE NULL
                   END
               ) AS power_rating
        FROM public.matchup m
        JOIN year_bounds y
          ON m.db_name = y.db_name AND TRY_CAST(m.year AS INTEGER) = y.year
        WHERE {target}
          AND m.franchise_id IS NOT NULL
          AND m.power_rating IS NOT NULL
        GROUP BY m.db_name, m.franchise_id, TRY_CAST(m.year AS INTEGER)
    )
    UPDATE public.matchup_season AS ms
    SET power_rating = ROUND(sp.power_rating, 2)
    FROM season_power sp
    WHERE ms.db_name = sp.db_name
      AND ms.franchise_id = sp.franchise_id
      AND TRY_CAST(ms.year AS INTEGER) = sp.year
      AND {season_target};

    WITH career_power AS (
        SELECT ms.db_name,
               ms.franchise_id,
               ROUND(AVG(CAST(ms.power_rating AS DOUBLE)), 2) AS avg_power_rating
        FROM public.matchup_season ms
        WHERE {season_target}
          AND ms.franchise_id IS NOT NULL
          AND ms.power_rating IS NOT NULL
        GROUP BY ms.db_name, ms.franchise_id
    )
    UPDATE public.matchup_career AS mc
    SET avg_power_rating = cp.avg_power_rating
    FROM career_power cp
    WHERE mc.db_name = cp.db_name
      AND mc.franchise_id = cp.franchise_id
      AND {career_target};
    COMMIT;

    WITH cohorts AS (
        SELECT m.db_name,
               m.year,
               COUNT(*) AS rows,
               MEDIAN(CAST(m.power_rating AS DOUBLE)) AS median_power,
               MIN(CAST(m.power_rating AS DOUBLE)) AS min_power,
               MAX(CAST(m.power_rating AS DOUBLE)) AS max_power
        FROM public.matchup m
        WHERE {target}
          AND m.power_rating IS NOT NULL
        GROUP BY m.db_name, m.year
    )
    SELECT 'power_batch' AS phase,
           COUNT(DISTINCT db_name) AS league_count,
           COUNT(*) AS season_count,
           SUM(rows) AS power_rows,
           MIN(median_power) AS min_median_power,
           MAX(median_power) AS max_median_power,
           (SELECT COUNT(*) FROM public.matchup_season ms WHERE {season_target} AND ms.power_rating IS NOT NULL) AS matchup_season_power_rows,
           (SELECT COUNT(*) FROM public.matchup_career mc WHERE {career_target} AND mc.avg_power_rating IS NOT NULL) AS matchup_career_power_rows
    FROM cohorts;
    """


def run_power_phase(conn: FlyDuckDBConnection, db_name: str, *, apply: bool) -> PhaseResult:
    rows = run_writer_sql(conn, power_sql(db_name, apply=apply))
    return PhaseResult("power", "ok" if apply else "dry-run", rows=rows)


def homepage_grade_sql(gpa_expr: str) -> str:
    return f"""CASE
        WHEN {gpa_expr} IS NULL THEN NULL
        WHEN {gpa_expr} >= 3.85 THEN 'A'
        WHEN {gpa_expr} >= 3.70 THEN 'A-'
        WHEN {gpa_expr} >= 3.30 THEN 'B+'
        WHEN {gpa_expr} >= 2.70 THEN 'B'
        WHEN {gpa_expr} >= 2.00 THEN 'B-'
        WHEN {gpa_expr} >= 1.70 THEN 'C'
        WHEN {gpa_expr} >= 1.30 THEN 'D+'
        WHEN {gpa_expr} >= 1.00 THEN 'D'
        ELSE 'F'
    END"""


def targeted_homepage_sql(db_name: str) -> str:
    db = sql_literal(db_name)
    grade_points = """CASE SUBSTR(UPPER(CAST(transaction_grade AS VARCHAR)), 1, 1)
        WHEN 'A' THEN 4.0
        WHEN 'B' THEN 3.0
        WHEN 'C' THEN 2.0
        WHEN 'D' THEN 1.0
        WHEN 'F' THEN 0.0
        ELSE NULL
    END"""
    return f"""
    BEGIN TRANSACTION;
    WITH playoff_record AS (
        SELECT
            franchise_id,
            SUM(COALESCE(TRY_CAST(win AS INTEGER), 0)) AS playoff_wins,
            SUM(COALESCE(TRY_CAST(loss AS INTEGER), 0)) AS playoff_losses,
            SUM(COALESCE(TRY_CAST(tie AS INTEGER), 0)) AS playoff_ties
        FROM public.matchup
        WHERE db_name = {db}
          AND franchise_id IS NOT NULL
          AND COALESCE(TRY_CAST(is_bye_week AS INTEGER), 0) = 0
          AND COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1
          AND COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 0
        GROUP BY franchise_id
    ),
    year_bounds AS (
        SELECT franchise_id,
               MIN(TRY_CAST(year AS INTEGER)) AS first_year,
               MAX(TRY_CAST(year AS INTEGER)) AS last_year
        FROM public.matchup_season
        WHERE db_name = {db}
          AND franchise_id IS NOT NULL
        GROUP BY franchise_id
    ),
    ranked_managers AS (
        SELECT
            c.franchise_id,
            c.manager,
            COALESCE(c.wins, 0) + COALESCE(p.playoff_wins, 0) AS wins,
            COALESCE(c.losses, 0) + COALESCE(p.playoff_losses, 0) AS losses,
            COALESCE(c.ties, 0) + COALESCE(p.playoff_ties, 0) AS ties,
            COALESCE(c.champion_seasons, 0) AS championships,
            COALESCE(c.playoff_seasons, 0) AS playoff_appearances,
            COALESCE(c.seasons, 0) AS seasons,
            y.first_year,
            y.last_year,
            ROUND(c.avg_power_rating, 1) AS power_rating,
            ROW_NUMBER() OVER (
                ORDER BY COALESCE(c.wins, 0) + COALESCE(p.playoff_wins, 0) DESC,
                         COALESCE(c.champion_seasons, 0) DESC
            ) AS career_rank
        FROM public.matchup_career c
        LEFT JOIN playoff_record p ON c.franchise_id = p.franchise_id
        LEFT JOIN year_bounds y ON c.franchise_id = y.franchise_id
        WHERE c.db_name = {db}
    ),
    ranked_with_pct AS (
        SELECT *,
               ROUND(CAST(wins AS DOUBLE) / NULLIF(wins + losses + ties, 0), 3) AS win_pct
        FROM ranked_managers
    )
    UPDATE public.homepage_manager_rankings AS h
    SET manager = r.manager,
        wins = r.wins,
        losses = r.losses,
        ties = r.ties,
        win_pct = r.win_pct,
        championships = r.championships,
        playoff_appearances = r.playoff_appearances,
        seasons = r.seasons,
        power_rating = r.power_rating,
        first_year = r.first_year,
        last_year = r.last_year,
        career_rank = r.career_rank
    FROM ranked_with_pct r
    WHERE h.db_name = {db}
      AND h.franchise_id = r.franchise_id;

    WITH manager_year AS (
        SELECT franchise_id, year, AVG(CAST(power_rating AS DOUBLE)) AS avg_power_rating
        FROM public.matchup
        WHERE db_name = {db}
          AND franchise_id IS NOT NULL
          AND power_rating IS NOT NULL
        GROUP BY franchise_id, year
    ),
    career AS (
        SELECT franchise_id, ROUND(AVG(avg_power_rating), 1) AS power_rating
        FROM manager_year
        GROUP BY franchise_id
    )
    UPDATE public.homepage_manager_rankings AS h
    SET power_rating = c.power_rating
    FROM career c
    WHERE h.db_name = {db}
      AND h.franchise_id = c.franchise_id;

    WITH latest_year AS (
        SELECT MAX(TRY_CAST(year AS INTEGER)) AS year
        FROM public.matchup
        WHERE db_name = {db}
          AND team_points > 0
    ),
    latest_power AS (
        SELECT franchise_id, ROUND(CAST(power_rating AS DOUBLE), 1) AS power_rating
        FROM (
            SELECT m.franchise_id,
                   m.power_rating,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.franchise_id
                       ORDER BY TRY_CAST(m.week AS INTEGER) DESC
                   ) AS rn
            FROM public.matchup m
            JOIN latest_year y ON TRY_CAST(m.year AS INTEGER) = y.year
            WHERE m.db_name = {db}
              AND m.franchise_id IS NOT NULL
              AND m.power_rating IS NOT NULL
        ) ranked
        WHERE rn = 1
    )
    UPDATE public.homepage_current_standings AS h
    SET power_rating = p.power_rating
    FROM latest_power p
    WHERE h.db_name = {db}
      AND h.franchise_id = p.franchise_id;

    WITH latest_year AS (
        SELECT COALESCE(
            (SELECT MAX(data_year) FROM public.homepage_league_summary WHERE db_name = {db}),
            (SELECT MAX(TRY_CAST(year AS INTEGER)) FROM public.matchup WHERE db_name = {db}),
            (SELECT MAX(TRY_CAST(year AS INTEGER)) FROM public.transactions WHERE db_name = {db})
        ) AS year
    ),
    career_grade AS (
        SELECT franchise_id,
               ROUND(AVG({grade_points}), 2) AS transaction_gpa
        FROM public.transactions
        WHERE db_name = {db}
          AND franchise_id IS NOT NULL
          AND transaction_grade IS NOT NULL
        GROUP BY franchise_id
    ),
    season_grade AS (
        SELECT t.franchise_id,
               ROUND(AVG({grade_points}), 2) AS transaction_gpa
        FROM public.transactions t
        JOIN latest_year y ON TRY_CAST(t.year AS INTEGER) = y.year
        WHERE t.db_name = {db}
          AND t.franchise_id IS NOT NULL
          AND t.transaction_grade IS NOT NULL
        GROUP BY t.franchise_id
    )
    UPDATE public.homepage_manager_profiles AS p
    SET transaction_gpa = cg.transaction_gpa,
        transaction_overall_grade = {homepage_grade_sql('cg.transaction_gpa')},
        transaction_quality_metric = cg.transaction_gpa,
        season_transaction_gpa = sg.transaction_gpa,
        season_transaction_overall_grade = {homepage_grade_sql('sg.transaction_gpa')},
        season_transaction_quality_metric = sg.transaction_gpa
    FROM career_grade cg
    LEFT JOIN season_grade sg ON cg.franchise_id = sg.franchise_id
    WHERE p.db_name = {db}
      AND p.franchise_id = cg.franchise_id;
    COMMIT;

    SELECT 'targeted_homepage' AS phase,
           (SELECT COUNT(*) FROM public.homepage_league_summary WHERE db_name = {db}) AS homepage_league_summary,
           (SELECT COUNT(*) FROM public.homepage_manager_rankings WHERE db_name = {db}) AS homepage_manager_rankings,
           (SELECT COUNT(*) FROM public.homepage_current_standings WHERE db_name = {db}) AS homepage_current_standings,
           (SELECT COUNT(*) FROM public.homepage_manager_profiles WHERE db_name = {db}) AS homepage_manager_profiles;
    """


def targeted_homepage_batch_sql(db_names: list[str]) -> str:
    matchup_target = requested_filter_sql(db_names, "m")
    season_target = requested_filter_sql(db_names, "ms")
    career_target = requested_filter_sql(db_names, "c")
    rankings_target = requested_filter_sql(db_names, "h")
    standings_target = requested_filter_sql(db_names, "h")
    profiles_target = requested_filter_sql(db_names, "p")
    transactions_target = requested_filter_sql(db_names, "t")
    summary_target = requested_filter_sql(db_names, "hls")
    db_values = ", ".join(sql_literal(name) for name in db_names)
    grade_points = """CASE SUBSTR(UPPER(CAST(transaction_grade AS VARCHAR)), 1, 1)
        WHEN 'A' THEN 4.0
        WHEN 'B' THEN 3.0
        WHEN 'C' THEN 2.0
        WHEN 'D' THEN 1.0
        WHEN 'F' THEN 0.0
        ELSE NULL
    END"""
    return f"""
    BEGIN TRANSACTION;
    WITH playoff_record AS (
        SELECT
            m.db_name,
            m.franchise_id,
            SUM(COALESCE(TRY_CAST(m.win AS INTEGER), 0)) AS playoff_wins,
            SUM(COALESCE(TRY_CAST(m.loss AS INTEGER), 0)) AS playoff_losses,
            SUM(COALESCE(TRY_CAST(m.tie AS INTEGER), 0)) AS playoff_ties
        FROM public.matchup m
        WHERE {matchup_target}
          AND m.franchise_id IS NOT NULL
          AND COALESCE(TRY_CAST(m.is_bye_week AS INTEGER), 0) = 0
          AND COALESCE(TRY_CAST(m.is_playoffs AS INTEGER), 0) = 1
          AND COALESCE(TRY_CAST(m.is_consolation AS INTEGER), 0) = 0
        GROUP BY m.db_name, m.franchise_id
    ),
    year_bounds AS (
        SELECT ms.db_name,
               ms.franchise_id,
               MIN(TRY_CAST(ms.year AS INTEGER)) AS first_year,
               MAX(TRY_CAST(ms.year AS INTEGER)) AS last_year
        FROM public.matchup_season ms
        WHERE {season_target}
          AND ms.franchise_id IS NOT NULL
        GROUP BY ms.db_name, ms.franchise_id
    ),
    ranked_managers AS (
        SELECT
            c.db_name,
            c.franchise_id,
            c.manager,
            COALESCE(c.wins, 0) + COALESCE(pr.playoff_wins, 0) AS wins,
            COALESCE(c.losses, 0) + COALESCE(pr.playoff_losses, 0) AS losses,
            COALESCE(c.ties, 0) + COALESCE(pr.playoff_ties, 0) AS ties,
            COALESCE(c.champion_seasons, 0) AS championships,
            COALESCE(c.playoff_seasons, 0) AS playoff_appearances,
            COALESCE(c.seasons, 0) AS seasons,
            y.first_year,
            y.last_year,
            ROUND(c.avg_power_rating, 1) AS power_rating,
            ROW_NUMBER() OVER (
                PARTITION BY c.db_name
                ORDER BY COALESCE(c.wins, 0) + COALESCE(pr.playoff_wins, 0) DESC,
                         COALESCE(c.champion_seasons, 0) DESC
            ) AS career_rank
        FROM public.matchup_career c
        LEFT JOIN playoff_record pr
          ON c.db_name = pr.db_name AND c.franchise_id = pr.franchise_id
        LEFT JOIN year_bounds y
          ON c.db_name = y.db_name AND c.franchise_id = y.franchise_id
        WHERE {career_target}
    ),
    ranked_with_pct AS (
        SELECT *,
               ROUND(CAST(wins AS DOUBLE) / NULLIF(wins + losses + ties, 0), 3) AS win_pct
        FROM ranked_managers
    )
    UPDATE public.homepage_manager_rankings AS h
    SET manager = r.manager,
        wins = r.wins,
        losses = r.losses,
        ties = r.ties,
        win_pct = r.win_pct,
        championships = r.championships,
        playoff_appearances = r.playoff_appearances,
        seasons = r.seasons,
        power_rating = r.power_rating,
        first_year = r.first_year,
        last_year = r.last_year,
        career_rank = r.career_rank
    FROM ranked_with_pct r
    WHERE h.db_name = r.db_name
      AND h.franchise_id = r.franchise_id
      AND {rankings_target};

    WITH manager_year AS (
        SELECT m.db_name,
               m.franchise_id,
               m.year,
               AVG(CAST(m.power_rating AS DOUBLE)) AS avg_power_rating
        FROM public.matchup m
        WHERE {matchup_target}
          AND m.franchise_id IS NOT NULL
          AND m.power_rating IS NOT NULL
        GROUP BY m.db_name, m.franchise_id, m.year
    ),
    career AS (
        SELECT db_name, franchise_id, ROUND(AVG(avg_power_rating), 1) AS power_rating
        FROM manager_year
        GROUP BY db_name, franchise_id
    )
    UPDATE public.homepage_manager_rankings AS h
    SET power_rating = c.power_rating
    FROM career c
    WHERE h.db_name = c.db_name
      AND h.franchise_id = c.franchise_id
      AND {rankings_target};

    WITH latest_year AS (
        SELECT m.db_name, MAX(TRY_CAST(m.year AS INTEGER)) AS year
        FROM public.matchup m
        WHERE {matchup_target}
          AND m.team_points > 0
        GROUP BY m.db_name
    ),
    latest_power AS (
        SELECT db_name, franchise_id, ROUND(CAST(power_rating AS DOUBLE), 1) AS power_rating
        FROM (
            SELECT m.db_name,
                   m.franchise_id,
                   m.power_rating,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.db_name, m.franchise_id
                       ORDER BY TRY_CAST(m.week AS INTEGER) DESC
                   ) AS rn
            FROM public.matchup m
            JOIN latest_year y
              ON m.db_name = y.db_name AND TRY_CAST(m.year AS INTEGER) = y.year
            WHERE {matchup_target}
              AND m.franchise_id IS NOT NULL
              AND m.power_rating IS NOT NULL
        ) ranked
        WHERE rn = 1
    )
    UPDATE public.homepage_current_standings AS h
    SET power_rating = pwr.power_rating
    FROM latest_power pwr
    WHERE h.db_name = pwr.db_name
      AND h.franchise_id = pwr.franchise_id
      AND {standings_target};

    WITH latest_year AS (
        SELECT db_name, MAX(year) AS year
        FROM (
            SELECT hls.db_name, MAX(hls.data_year) AS year
            FROM public.homepage_league_summary hls
            WHERE {summary_target}
            GROUP BY hls.db_name
            UNION ALL
            SELECT m.db_name, MAX(TRY_CAST(m.year AS INTEGER)) AS year
            FROM public.matchup m
            WHERE {matchup_target}
            GROUP BY m.db_name
            UNION ALL
            SELECT t.db_name, MAX(TRY_CAST(t.year AS INTEGER)) AS year
            FROM public.transactions t
            WHERE {transactions_target}
            GROUP BY t.db_name
        )
        GROUP BY db_name
    ),
    career_grade AS (
        SELECT t.db_name,
               t.franchise_id,
               ROUND(AVG({grade_points}), 2) AS transaction_gpa
        FROM public.transactions t
        WHERE {transactions_target}
          AND t.franchise_id IS NOT NULL
          AND t.transaction_grade IS NOT NULL
        GROUP BY t.db_name, t.franchise_id
    ),
    season_grade AS (
        SELECT t.db_name,
               t.franchise_id,
               ROUND(AVG({grade_points}), 2) AS transaction_gpa
        FROM public.transactions t
        JOIN latest_year y ON t.db_name = y.db_name AND TRY_CAST(t.year AS INTEGER) = y.year
        WHERE {transactions_target}
          AND t.franchise_id IS NOT NULL
          AND t.transaction_grade IS NOT NULL
        GROUP BY t.db_name, t.franchise_id
    )
    UPDATE public.homepage_manager_profiles AS p
    SET transaction_gpa = cg.transaction_gpa,
        transaction_overall_grade = {homepage_grade_sql('cg.transaction_gpa')},
        transaction_quality_metric = cg.transaction_gpa,
        season_transaction_gpa = sg.transaction_gpa,
        season_transaction_overall_grade = {homepage_grade_sql('sg.transaction_gpa')},
        season_transaction_quality_metric = sg.transaction_gpa
    FROM career_grade cg
    LEFT JOIN season_grade sg
      ON cg.db_name = sg.db_name AND cg.franchise_id = sg.franchise_id
    WHERE p.db_name = cg.db_name
      AND p.franchise_id = cg.franchise_id
      AND {profiles_target};
    COMMIT;

    SELECT 'targeted_homepage_batch' AS phase,
           COUNT(*) AS league_count,
           (SELECT COUNT(*) FROM public.homepage_league_summary WHERE db_name IN ({db_values})) AS homepage_league_summary,
           (SELECT COUNT(*) FROM public.homepage_manager_rankings WHERE db_name IN ({db_values})) AS homepage_manager_rankings,
           (SELECT COUNT(*) FROM public.homepage_current_standings WHERE db_name IN ({db_values})) AS homepage_current_standings,
           (SELECT COUNT(*) FROM public.homepage_manager_profiles WHERE db_name IN ({db_values})) AS homepage_manager_profiles
    FROM (SELECT DISTINCT db_name FROM public.league_context WHERE db_name IN ({db_values}));
    """


def run_homepage_phase(
    conn: FlyDuckDBConnection,
    db_name: str,
    *,
    apply: bool,
    chunk_size: int,
    full_homepage: bool,
) -> PhaseResult:
    if not apply:
        rows = (
            conn.execute(
                f"""
            SELECT 'dry_run_homepage_targeted' AS phase,
                   (SELECT COUNT(*) FROM public.homepage_league_summary WHERE db_name = {sql_literal(db_name)}) AS homepage_league_summary,
                   (SELECT COUNT(*) FROM public.homepage_manager_rankings WHERE db_name = {sql_literal(db_name)}) AS homepage_manager_rankings,
                   (SELECT COUNT(*) FROM public.homepage_current_standings WHERE db_name = {sql_literal(db_name)}) AS homepage_current_standings,
                   (SELECT COUNT(*) FROM public.homepage_top_rivalries WHERE db_name = {sql_literal(db_name)}) AS homepage_top_rivalries,
                   (SELECT COUNT(*) FROM public.homepage_manager_profiles WHERE db_name = {sql_literal(db_name)}) AS homepage_manager_profiles
            """
            )
            .fetchdf()
            .to_dict("records")
        )
        return PhaseResult("homepage", "dry-run", rows=rows)

    rows: list[dict[str, Any]] = []
    headshots = fetch_draft_headshot_map([db_name])
    rows.extend(run_writer_sql(conn, draft_homepage_sql([db_name], apply=True, headshots=headshots)))
    rows.extend(run_writer_sql(conn, targeted_homepage_sql(db_name)))
    counts: dict[str, int] = {}
    if full_homepage:
        counts = rebuild_homepage_tables(conn, db_name, chunk_size=chunk_size)
    return PhaseResult("homepage", "ok", rows=rows, counts=counts)


def run_draft_batch_phase(
    conn: FlyDuckDBConnection, db_names: list[str], *, apply: bool, skip_aggregates: bool
) -> PhaseResult:
    rows: list[dict[str, Any]] = []
    rows.extend(run_writer_sql(conn, draft_scoring_sql(db_names, apply=apply)))
    if not skip_aggregates:
        rows.extend(run_writer_sql(conn, draft_aggregate_sql(db_names, apply=apply)))
    return PhaseResult("draft_batch", "ok" if apply else "dry-run", rows=rows)


def run_transaction_batch_phase(conn: FlyDuckDBConnection, db_names: list[str], *, apply: bool) -> PhaseResult:
    rows = run_writer_sql(conn, transaction_batch_sql(db_names, apply=apply))
    return PhaseResult("transactions_batch", "ok" if apply else "dry-run", rows=rows)


def run_power_batch_phase(conn: FlyDuckDBConnection, db_names: list[str], *, apply: bool) -> PhaseResult:
    rows = run_writer_sql(conn, power_batch_sql(db_names, apply=apply))
    return PhaseResult("power_batch", "ok" if apply else "dry-run", rows=rows)


def run_homepage_batch_phase(conn: FlyDuckDBConnection, db_names: list[str], *, apply: bool) -> PhaseResult:
    if not apply:
        values = ", ".join(sql_literal(name) for name in db_names)
        rows = (
            conn.execute(
                f"""
            SELECT 'dry_run_homepage_batch' AS phase,
                   COUNT(*) AS league_count,
                   (SELECT COUNT(*) FROM public.homepage_league_summary WHERE db_name IN ({values})) AS homepage_league_summary,
                   (SELECT COUNT(*) FROM public.homepage_manager_rankings WHERE db_name IN ({values})) AS homepage_manager_rankings,
                   (SELECT COUNT(*) FROM public.homepage_current_standings WHERE db_name IN ({values})) AS homepage_current_standings,
                   (SELECT COUNT(*) FROM public.homepage_top_rivalries WHERE db_name IN ({values})) AS homepage_top_rivalries,
                   (SELECT COUNT(*) FROM public.homepage_manager_profiles WHERE db_name IN ({values})) AS homepage_manager_profiles
            FROM (SELECT DISTINCT db_name FROM public.league_context WHERE db_name IN ({values}));
            """
            )
            .fetchdf()
            .to_dict("records")
        )
        return PhaseResult("homepage_batch", "dry-run", rows=rows)

    rows: list[dict[str, Any]] = []
    headshots = fetch_draft_headshot_map(db_names)
    rows.extend(run_writer_sql(conn, draft_homepage_sql(db_names, apply=True, headshots=headshots)))
    rows.extend(run_writer_sql(conn, targeted_homepage_batch_sql(db_names)))
    return PhaseResult("homepage_batch", "ok", rows=rows)


def process_batch(
    conn: FlyDuckDBConnection,
    db_names: list[str],
    *,
    apply: bool,
    skip_draft: bool,
    skip_draft_aggregates: bool,
    skip_transactions: bool,
    skip_power: bool,
    skip_homepage: bool,
) -> list[PhaseResult]:
    phases: list[PhaseResult] = []
    if not skip_draft:
        phases.append(run_draft_batch_phase(conn, db_names, apply=apply, skip_aggregates=skip_draft_aggregates))
    if not skip_transactions:
        phases.append(run_transaction_batch_phase(conn, db_names, apply=apply))
    if not skip_power:
        phases.append(run_power_batch_phase(conn, db_names, apply=apply))
    if not skip_homepage:
        phases.append(run_homepage_batch_phase(conn, db_names, apply=apply))
    return phases


def process_league(
    conn: FlyDuckDBConnection,
    db_name: str,
    *,
    apply: bool,
    skip_draft: bool,
    skip_draft_aggregates: bool,
    skip_transactions: bool,
    include_transaction_detail_aggregates: bool,
    skip_power: bool,
    skip_homepage: bool,
    full_homepage: bool,
    chunk_size: int,
) -> LeagueResult:
    phases: list[PhaseResult] = []
    if not skip_draft:
        phases.append(run_draft_phase(conn, db_name, apply=apply, skip_aggregates=skip_draft_aggregates))
    if not skip_transactions:
        phases.append(
            run_transaction_phase(
                conn,
                db_name,
                apply=apply,
                include_detail_aggregates=include_transaction_detail_aggregates,
            )
        )
    if not skip_power:
        phases.append(run_power_phase(conn, db_name, apply=apply))
    if not skip_homepage:
        phases.append(
            run_homepage_phase(
                conn,
                db_name,
                apply=apply,
                chunk_size=chunk_size,
                full_homepage=full_homepage,
            )
        )
    return LeagueResult(db_name=db_name, status="ok", phases=phases)


def print_phase_result(db_name: str, result: PhaseResult) -> None:
    payload: dict[str, Any] = {"db_name": db_name, "phase": result.phase, "status": result.status}
    if result.counts:
        payload["counts"] = result.counts
    if result.rows:
        payload["rows"] = result.rows
    if result.error:
        payload["error"] = result.error
    print(json.dumps(payload, default=str), flush=True)


def print_batch_phase_result(db_names: list[str], result: PhaseResult) -> None:
    payload: dict[str, Any] = {
        "db_names": db_names,
        "league_count": len(db_names),
        "phase": result.phase,
        "status": result.status,
    }
    if result.counts:
        payload["counts"] = result.counts
    if result.rows:
        payload["rows"] = result.rows
    if result.error:
        payload["error"] = result.error
    print(json.dumps(payload, default=str), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill normalized score columns and homepage rows in Fly")
    parser.add_argument("--db", action="append", help="Optional db_name, comma list, or newline list. Omit for fleet.")
    parser.add_argument("--apply", action="store_true", help="Apply writes. Default is dry-run.")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run.")
    parser.add_argument(
        "--inventory",
        action="store_true",
        help="Print drift inventory and exit without running phase SQL.",
    )
    parser.add_argument(
        "--only-needs-backfill",
        action="store_true",
        help="Only target leagues whose inventory has drift rows.",
    )
    parser.add_argument(
        "--inventory-print-limit",
        type=int,
        default=50,
        help="Maximum dirty league rows to print in inventory mode.",
    )
    parser.add_argument("--limit", type=int, help="Limit number of target leagues after sorting.")
    parser.add_argument("--resume-from", help="Skip sorted targets until this db_name is reached.")
    parser.add_argument("--skip-draft", action="store_true")
    parser.add_argument("--skip-draft-aggregates", action="store_true")
    parser.add_argument("--skip-transactions", action="store_true")
    parser.add_argument(
        "--include-transaction-detail-aggregates",
        action="store_true",
        help="Also rebuild transaction_player_career and transaction_report_card. Slower and not needed for homepage refresh.",
    )
    parser.add_argument("--skip-power", action="store_true")
    parser.add_argument("--skip-homepage", action="store_true")
    parser.add_argument(
        "--full-homepage",
        action="store_true",
        help="Recompute all homepage tables after targeted homepage score updates. Much slower.",
    )
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.5, help="Pause between leagues in apply mode.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Process normal backfill phases in db_name batches. Uses per-league mode for full homepage/detail aggregates.",
    )
    parser.add_argument("--chunk-size", type=int, default=250, help="Homepage insert chunk size.")
    parser.add_argument("--timeout-seconds", type=int, default=1800, help="Fly write timeout per statement.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")

    from multi_league.core.fly_writer import FlyWriter

    FlyWriter.TIMEOUT_SECONDS = max(FlyWriter.TIMEOUT_SECONDS, int(args.timeout_seconds))

    conn = FlyDuckDBConnection()
    conn.writer.max_retries = max(conn.writer.max_retries, 2)
    requested = parse_db_names(args.db)
    if args.inventory:
        print_inventory(fetch_inventory(conn, requested), max_dirty=max(int(args.inventory_print_limit), 0))
        return 0

    targets = fetch_target_leagues(
        conn,
        requested,
        limit=None,
        only_needs_backfill=args.only_needs_backfill,
    )
    if args.resume_from:
        resume = str(args.resume_from)
        targets = [name for name in targets if name >= resume]
    if args.limit is not None:
        targets = targets[: max(int(args.limit), 0)]

    apply_writes = bool(args.apply and not args.dry_run)
    batch_size = max(int(args.batch_size or 1), 1)
    use_batch_mode = batch_size > 1 and not args.full_homepage and not args.include_transaction_detail_aggregates
    print(
        json.dumps(
            {
                "mode": "apply" if apply_writes else "dry-run",
                "targets": len(targets),
                "only_needs_backfill": args.only_needs_backfill,
                "batch_size": batch_size,
                "batch_mode": use_batch_mode,
                "skip_draft": args.skip_draft,
                "skip_transactions": args.skip_transactions,
                "skip_power": args.skip_power,
                "skip_homepage": args.skip_homepage,
            }
        ),
        flush=True,
    )

    failures: list[LeagueResult] = []
    if use_batch_mode:
        batches = chunked(targets, batch_size)
        for idx, batch in enumerate(batches, start=1):
            print(
                json.dumps(
                    {
                        "event": "start_batch",
                        "index": idx,
                        "total": len(batches),
                        "league_count": len(batch),
                        "first_db_name": batch[0],
                        "last_db_name": batch[-1],
                    }
                ),
                flush=True,
            )
            try:
                phases = process_batch(
                    conn,
                    batch,
                    apply=apply_writes,
                    skip_draft=args.skip_draft,
                    skip_draft_aggregates=args.skip_draft_aggregates,
                    skip_transactions=args.skip_transactions,
                    skip_power=args.skip_power,
                    skip_homepage=args.skip_homepage,
                )
                for phase in phases:
                    print_batch_phase_result(batch, phase)
            except Exception as exc:  # noqa: broad-except
                failure = LeagueResult(db_name=f"{batch[0]}..{batch[-1]}", status="error", error=str(exc))
                failures.append(failure)
                print(
                    json.dumps(
                        {
                            "db_names": batch,
                            "status": "error",
                            "error": str(exc),
                        }
                    ),
                    flush=True,
                )
                if not args.continue_on_error:
                    raise
            if apply_writes and idx < len(batches) and args.sleep_seconds > 0:
                time.sleep(float(args.sleep_seconds))
    else:
        for idx, db_name in enumerate(targets, start=1):
            print(
                json.dumps({"event": "start_league", "index": idx, "total": len(targets), "db_name": db_name}),
                flush=True,
            )
            try:
                result = process_league(
                    conn,
                    db_name,
                    apply=apply_writes,
                    skip_draft=args.skip_draft,
                    skip_draft_aggregates=args.skip_draft_aggregates,
                    skip_transactions=args.skip_transactions,
                    include_transaction_detail_aggregates=args.include_transaction_detail_aggregates,
                    skip_power=args.skip_power,
                    skip_homepage=args.skip_homepage,
                    full_homepage=args.full_homepage,
                    chunk_size=args.chunk_size,
                )
                for phase in result.phases:
                    print_phase_result(db_name, phase)
            except Exception as exc:  # noqa: broad-except
                failure = LeagueResult(db_name=db_name, status="error", error=str(exc))
                failures.append(failure)
                print(json.dumps({"db_name": db_name, "status": "error", "error": str(exc)}), flush=True)
                if not args.continue_on_error:
                    raise
            if apply_writes and idx < len(targets) and args.sleep_seconds > 0:
                time.sleep(float(args.sleep_seconds))

    print(
        json.dumps(
            {
                "event": "complete",
                "mode": "apply" if apply_writes else "dry-run",
                "targets": len(targets),
                "failures": [failure.db_name for failure in failures],
            }
        ),
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
