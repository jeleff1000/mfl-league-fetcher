#!/usr/bin/env python
"""Print local newspaper_review staging counts and a small run sample."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--run-id", default="")
    parser.add_argument(
        "--apply-run-id",
        default="",
        help="Promotion apply run to summarize. Defaults to the latest local apply run.",
    )
    parser.add_argument("--limit", type=int, default=12)
    return parser.parse_args()


def table_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [
        row[1]
        for row in con.execute(f"PRAGMA table_info('newspaper_review.{table}')").fetchall()
    ]


def first_existing(cols: list[str], candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return ""


def latest_apply_run(
    con: duckdb.DuckDBPyConnection,
) -> dict[str, object] | None:
    cols = table_columns(con, "review_decision_apply_run")
    run_col = first_existing(cols, ["promotion_apply_run_id", "apply_run_id", "run_id"])
    created_col = first_existing(cols, ["created_at_utc", "created_at"])
    if not run_col:
        return None
    order_sql = f"ORDER BY {created_col} DESC" if created_col else ""
    row = con.execute(
        f"""
        SELECT *
        FROM newspaper_review.review_decision_apply_run
        {order_sql}
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    return dict(zip(cols, row))


def summarize_apply_run(
    con: duckdb.DuckDBPyConnection, apply_run_id: str
) -> dict[str, object]:
    out: dict[str, object] = {"apply_run_id": apply_run_id}

    promo_cols = table_columns(con, "review_decision_applied_promotion")
    promo_run_col = first_existing(
        promo_cols, ["promotion_apply_run_id", "apply_run_id", "run_id"]
    )
    promo_target_col = first_existing(
        promo_cols, ["target_table", "resolved_target_table", "semantic_target_table"]
    )
    if promo_run_col and promo_target_col:
        record_counts = dict(
            con.execute(
                f"""
                SELECT {promo_target_col}, COUNT(*) AS row_count
                FROM newspaper_review.review_decision_applied_promotion
                WHERE {promo_run_col} = ?
                GROUP BY 1
                ORDER BY row_count DESC, 1
                """,
                [apply_run_id],
            ).fetchall()
        )
        out["applied_promotion_record_counts"] = record_counts
        out["applied_promotion_record_total"] = con.execute(
            f"""
            SELECT COUNT(*)
            FROM newspaper_review.review_decision_applied_promotion
            WHERE {promo_run_col} = ?
            """,
            [apply_run_id],
        ).fetchone()[0]
        out["applied_record_note"] = (
            "Audit records are pre-dedupe; review_decision_apply_run.promoted_row_count "
            "is the unique local promoted-row count."
        )

    route_cols = table_columns(con, "review_decision_route_queue")
    route_run_col = first_existing(
        route_cols, ["promotion_apply_run_id", "apply_run_id", "run_id"]
    )
    route_lane_col = first_existing(route_cols, ["route_to_lane", "lane"])
    route_target_col = first_existing(
        route_cols, ["target_table", "resolved_target_table", "semantic_target_table"]
    )
    route_action_col = first_existing(
        route_cols, ["recommended_next_action", "next_action", "review_action"]
    )
    if route_run_col:
        out["route_queue_total"] = con.execute(
            f"""
            SELECT COUNT(*)
            FROM newspaper_review.review_decision_route_queue
            WHERE {route_run_col} = ?
            """,
            [apply_run_id],
        ).fetchone()[0]
        if route_lane_col:
            out["route_lane_counts"] = dict(
                con.execute(
                    f"""
                    SELECT {route_lane_col}, COUNT(*) AS row_count
                    FROM newspaper_review.review_decision_route_queue
                    WHERE {route_run_col} = ?
                    GROUP BY 1
                    ORDER BY row_count DESC, 1
                    """,
                    [apply_run_id],
                ).fetchall()
            )
        if route_target_col and route_action_col:
            out["route_target_action_counts"] = [
                {
                    "target_table": row[0],
                    "recommended_next_action": row[1],
                    "row_count": row[2],
                }
                for row in con.execute(
                    f"""
                    SELECT
                      {route_target_col},
                      {route_action_col},
                      COUNT(*) AS row_count
                    FROM newspaper_review.review_decision_route_queue
                    WHERE {route_run_col} = ?
                    GROUP BY 1, 2
                    ORDER BY row_count DESC, 1, 2
                    """,
                    [apply_run_id],
                ).fetchall()
            ]
    return out


def main() -> int:
    args = parse_args()
    tables = [
        "atom_claim",
        "game_candidate",
        "scoring_event",
        "play_by_play_event",
        "player_game_box_score",
        "lineup_participation",
        "promotion_candidate",
        "conveyor_document_state",
    ]
    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        counts = {
            table: con.execute(f"SELECT COUNT(*) FROM newspaper_review.{table}").fetchone()[0]
            for table in tables
        }
        latest_apply = latest_apply_run(con)
        apply_run_id = args.apply_run_id
        if not apply_run_id and latest_apply:
            apply_run_id = str(
                latest_apply.get("promotion_apply_run_id")
                or latest_apply.get("apply_run_id")
                or latest_apply.get("run_id")
                or ""
            )
        out: dict[str, object] = {"db_path": str(args.db_path), "counts": counts}
        if latest_apply:
            out["latest_apply_run"] = latest_apply
        if apply_run_id:
            out["apply_run_summary"] = summarize_apply_run(con, apply_run_id)
        print(json.dumps(out, indent=2, default=str))
        if args.run_id:
            rows = con.execute(
                """
                SELECT
                  boxscore_id,
                  atom_type,
                  semantic_target_table,
                  semantic_target_field,
                  entity_text,
                  raw_value,
                  confidence_score
                FROM newspaper_review.atom_claim
                WHERE run_id = ?
                ORDER BY boxscore_id, atom_type, atom_claim_id
                LIMIT ?
                """,
                [args.run_id, args.limit],
            ).fetchall()
            print("sample_rows")
            for row in rows:
                print(row)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
