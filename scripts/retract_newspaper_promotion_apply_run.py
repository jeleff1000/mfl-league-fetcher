#!/usr/bin/env python
"""Retract one local newspaper promotion apply run from DuckDB.

This only touches local D-drive newspaper atom tables. It is intended for
correcting a conveyor apply run before anything is promoted to v26/Fly/live
tables. A JSON audit artifact is written before the scoped delete commits.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "review_decision_apply_retractions"

PROMOTED_TABLES = [
    "game_candidate",
    "scoring_event",
    "play_by_play_event",
    "player_game_box_score",
    "player_game_stat_claim",
    "player_game_note",
    "team_game_stat_claim",
    "source_document_note",
    "lineup_participation",
    "player_identity_candidate",
    "promotion_candidate",
]

REVIEW_TABLES = [
    "review_decision_applied_promotion",
    "review_decision_route_queue",
    "review_decision_apply_run",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def count_rows(con: duckdb.DuckDBPyConnection, table: str, run_id: str) -> int:
    try:
        return int(
            con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE promotion_apply_run_id = ?",
                [run_id],
            ).fetchone()[0]
        )
    except duckdb.CatalogException:
        return 0


def delete_rows(con: duckdb.DuckDBPyConnection, table: str, run_id: str) -> int:
    before = count_rows(con, table, run_id)
    if before:
        con.execute(f"DELETE FROM {table} WHERE promotion_apply_run_id = ?", [run_id])
    return before


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--promotion-apply-run-id", required=True)
    parser.add_argument("--label", default="local_apply_retraction")
    parser.add_argument("--reason", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run_id = args.promotion_apply_run_id
    out_dir = args.out_root / f"{stamp()}_{args.label}"
    con = duckdb.connect(str(args.db_path))

    promoted_counts = {
        table: count_rows(con, f"newspaper_promoted.{table}", run_id)
        for table in PROMOTED_TABLES
    }
    review_counts = {
        table: count_rows(con, f"newspaper_review.{table}", run_id)
        for table in REVIEW_TABLES
    }
    summary = {
        "created_at_utc": iso_now(),
        "promotion_apply_run_id": run_id,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "reason": args.reason,
        "dry_run": bool(args.dry_run),
        "promoted_table_counts_before": {k: v for k, v in promoted_counts.items() if v},
        "review_table_counts_before": {k: v for k, v in review_counts.items() if v},
        "deleted_promoted_rows": 0,
        "deleted_review_rows": 0,
    }
    write_json(out_dir / "summary_before.json", summary)

    if not args.dry_run:
        con.begin()
        deleted_promoted = 0
        deleted_review = 0
        try:
            for table in PROMOTED_TABLES:
                deleted_promoted += delete_rows(con, f"newspaper_promoted.{table}", run_id)
            for table in REVIEW_TABLES:
                deleted_review += delete_rows(con, f"newspaper_review.{table}", run_id)
            con.commit()
        except Exception:
            con.rollback()
            raise
        summary["deleted_promoted_rows"] = deleted_promoted
        summary["deleted_review_rows"] = deleted_review

    write_json(out_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
