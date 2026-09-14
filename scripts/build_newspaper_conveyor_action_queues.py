#!/usr/bin/env python
"""Build actionable queues from the local newspaper LLM conveyor state.

This station turns reviewed documents, follow-ups, materialized rows, promotion
packages, and conflicts into work queues that can be consumed by the next OCR,
visual review, semantic reconciliation, or promotion-review pass.

It writes only to the local D-drive newspaper atom database and artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "conveyor_action_queues"
DEFAULT_V26_COMPARISON_ROOT = DEFAULT_ROOT / "v26_comparison_reports"

ACTION_FIELDS = [
    "action_queue_run_id",
    "action_id",
    "priority",
    "action_type",
    "action_lane",
    "status",
    "source_document_id",
    "packet_id",
    "boxscore_id",
    "target_table",
    "target_entity_key",
    "promotion_package_id",
    "promotion_conflict_id",
    "followup_type",
    "confidence_bar",
    "max_confidence_score",
    "evidence_document_count",
    "item_count",
    "novelty_vs_v26",
    "v26_match_status",
    "v26_score_status",
    "v26_value_class",
    "v26_comparison_run_id",
    "reason",
    "proposed_fields_json",
    "source_documents_json",
    "artifact_path",
    "created_at_utc",
]

RUN_FIELDS = [
    "action_queue_run_id",
    "ingest_run_id",
    "package_run_id",
    "output_dir",
    "action_count",
    "followup_action_count",
    "promotion_ready_count",
    "promotion_conflict_count",
    "v26_comparison_run_id",
    "status",
    "created_at_utc",
    "summary_json_path",
]

OCR_FOLLOWUP_TYPES = {
    "better_ocr",
    "larger_crop",
    "narrower_crop",
    "column_split",
    "deeper_article_ocr",
    "full_page_visual_review",
    "visual_score_check",
    "ocr_region_missing_text",
}

SEMANTIC_FOLLOWUP_TYPES = {
    "boxscore_reconciliation",
    "team_label_resolution",
    "duplicate_source_route",
    "semantic_route_review",
    "cross_game_result_reconciliation",
}


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def parse_int(value: Any) -> int:
    try:
        return int(float(clean(value)))
    except (TypeError, ValueError):
        return 0


def parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [clean(item) for item in value if clean(item)]
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if isinstance(parsed, list):
        return [clean(item) for item in parsed if clean(item)]
    return [clean(parsed)] if clean(parsed) else []


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in rows:
        values = [clean(row.get(field)).replace("\n", " ") for field in fields]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def latest_value(con: duckdb.DuckDBPyConnection, table: str, id_field: str, created_field: str = "created_at_utc") -> str:
    try:
        row = con.execute(
            f"""
            SELECT {id_field}
            FROM newspaper_review.{table}
            ORDER BY {created_field} DESC, {id_field} DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        return ""
    return clean(row[0]) if row else ""


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def latest_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        [path for path in root.iterdir() if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def load_v26_comparison(
    package_run_id: str,
    comparison_root: Path,
    comparison_csv: Path | None = None,
) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    """Load package-level v26 comparison rows, if a matching artifact exists."""
    csv_path = comparison_csv if comparison_csv and comparison_csv.exists() else None
    summary: dict[str, Any] = {}
    if csv_path:
        summary = read_json(csv_path.parent / "summary.json")
    else:
        for run_dir in latest_dirs(comparison_root):
            candidate_summary = read_json(run_dir / "summary.json")
            if package_run_id and clean(candidate_summary.get("package_run_id")) != package_run_id:
                continue
            candidate_csv = run_dir / "package_comparison.csv"
            if candidate_csv.exists():
                csv_path = candidate_csv
                summary = candidate_summary
                break

    if not csv_path:
        return {}, {}

    rows = read_csv_dicts(csv_path)
    by_package = {
        clean(row.get("promotion_package_id")): row
        for row in rows
        if clean(row.get("promotion_package_id"))
    }
    summary = {
        **summary,
        "comparison_csv_path": str(csv_path),
        "comparison_run_id": clean(summary.get("comparison_run_id")) or csv_path.parent.name,
    }
    return by_package, summary


def fallback_novelty_for_target(target_table: str) -> str:
    target_table = clean(target_table)
    if target_table == "game_candidate":
        return "needs_v26_comparison"
    if target_table == "scoring_event":
        return "new_scoring_event"
    if target_table == "play_by_play_event":
        return "new_pbp"
    if target_table == "player_game_box_score":
        return "new_player_stat"
    if target_table == "lineup_participation":
        return "new_lineup"
    if target_table == "team_game_stat_claim":
        return "new_team_stat"
    if target_table == "player_identity_candidate":
        return "new_identity"
    return "newspaper_atom_candidate"


def novelty_from_value_class(value_class: str, target_table: str) -> str:
    value_class = clean(value_class)
    if value_class == "corroborates_existing_v26_score":
        return "confirms_existing_score"
    if value_class == "fills_v26_missing_game_date":
        return "fills_v26_missing_game_date"
    if value_class == "fills_v26_missing_date_or_score":
        return "fills_v26_missing_date_or_score"
    if value_class == "fills_v26_missing_score_side":
        return "fills_v26_missing_score_side"
    if value_class == "possible_v26_score_delta_or_mapping_issue":
        return "possible_v26_correction"
    if value_class == "score_followup_needed_no_newspaper_score":
        return "needs_ocr_or_semantic_resolution"
    if value_class == "score_candidate_v26_missing_game":
        return "possible_new_game_or_missing_v26_game"
    if value_class == "not_v26_score_comparable":
        return "needs_v26_comparison"
    return fallback_novelty_for_target(target_table)


def comparison_fields_for_package(
    package: dict[str, Any],
    comparison_rows: dict[str, dict[str, str]],
    comparison_summary: dict[str, Any],
) -> dict[str, str]:
    package_id = clean(package.get("promotion_package_id"))
    target_table = clean(package.get("target_table"))
    comparison = comparison_rows.get(package_id, {})
    if comparison:
        value_class = clean(comparison.get("value_class"))
        return {
            "novelty_vs_v26": novelty_from_value_class(value_class, target_table),
            "v26_match_status": clean(comparison.get("v26_match_status")),
            "v26_score_status": clean(comparison.get("v26_score_status")),
            "v26_value_class": value_class,
            "v26_comparison_run_id": clean(comparison.get("comparison_run_id"))
            or clean(comparison_summary.get("comparison_run_id")),
        }
    return {
        "novelty_vs_v26": fallback_novelty_for_target(target_table),
        "v26_match_status": "comparison_not_run",
        "v26_score_status": "comparison_not_run" if target_table == "game_candidate" else "not_score_package",
        "v26_value_class": "",
        "v26_comparison_run_id": clean(comparison_summary.get("comparison_run_id")),
    }


def followup_lane(followup_type: str) -> tuple[str, int, str]:
    kind = followup_type.lower().strip()
    if kind in OCR_FOLLOWUP_TYPES:
        return "ocr_visual_followup", 20, "ocr_or_visual_pass"
    if kind in SEMANTIC_FOLLOWUP_TYPES:
        return "semantic_followup", 30, "semantic_reconciliation"
    return "general_followup", 40, "followup_review"


def package_priority(row: dict[str, Any]) -> int:
    status = clean(row.get("package_status"))
    if status == "ready_for_promotion_review":
        return 10
    if status == "needs_conflict_review":
        return 15
    if status == "needs_quality_review":
        return 25
    return 35


def load_followups(con: duckdb.DuckDBPyConnection, ingest_run_id: str) -> list[dict[str, Any]]:
    if not ingest_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT
          f.ingest_run_id,
          f.packet_id,
          f.source_document_id,
          f.followup_type,
          f.reason,
          f.status AS followup_status,
          f.review_output_path,
          d.doc_disposition,
          d.document_next_action,
          d.boxscore_id,
          d.publication,
          d.issue_date,
          d.page
        FROM newspaper_review.llm_review_followup f
        LEFT JOIN newspaper_review.llm_document_review d
          ON f.ingest_run_id = d.ingest_run_id
         AND f.source_document_id = d.source_document_id
        WHERE f.ingest_run_id = ?
        ORDER BY f.source_document_id, f.followup_index
        """,
        [ingest_run_id],
    )


def load_packages(con: duckdb.DuckDBPyConnection, package_run_id: str) -> list[dict[str, Any]]:
    if not package_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_promotion_package
        WHERE package_run_id = ?
        ORDER BY
          CASE package_status
            WHEN 'ready_for_promotion_review' THEN 1
            WHEN 'needs_conflict_review' THEN 2
            WHEN 'needs_quality_review' THEN 3
            ELSE 4
          END,
          target_table,
          target_entity_key
        """,
        [package_run_id],
    )


def load_conflicts(con: duckdb.DuckDBPyConnection, package_run_id: str) -> list[dict[str, Any]]:
    if not package_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_promotion_conflict
        WHERE package_run_id = ?
        ORDER BY target_table, target_entity_key, field_name
        """,
        [package_run_id],
    )


def action_from_followup(action_queue_run_id: str, row: dict[str, Any], created_at: str) -> dict[str, Any]:
    followup_type = clean(row.get("followup_type"))
    action_type, priority, lane = followup_lane(followup_type)
    source_document_id = clean(row.get("source_document_id"))
    return {
        "action_queue_run_id": action_queue_run_id,
        "action_id": stable_id(action_queue_run_id, "followup", source_document_id, followup_type, row.get("reason")),
        "priority": priority,
        "action_type": action_type,
        "action_lane": lane,
        "status": "open",
        "source_document_id": source_document_id,
        "packet_id": clean(row.get("packet_id")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "target_table": "",
        "target_entity_key": "",
        "promotion_package_id": "",
        "promotion_conflict_id": "",
        "followup_type": followup_type,
        "confidence_bar": "",
        "max_confidence_score": "",
        "evidence_document_count": "",
        "item_count": "",
        "novelty_vs_v26": "needs_ocr_or_semantic_resolution" if action_type != "general_followup" else "followup_not_atom",
        "v26_match_status": "",
        "v26_score_status": "",
        "v26_value_class": "",
        "v26_comparison_run_id": "",
        "reason": clean(row.get("reason")),
        "proposed_fields_json": "",
        "source_documents_json": json.dumps([source_document_id], ensure_ascii=False),
        "artifact_path": clean(row.get("review_output_path")),
        "created_at_utc": created_at,
    }


def action_from_package(
    action_queue_run_id: str,
    row: dict[str, Any],
    created_at: str,
    comparison_rows: dict[str, dict[str, str]],
    comparison_summary: dict[str, Any],
) -> dict[str, Any] | None:
    status = clean(row.get("package_status"))
    if status not in {"ready_for_promotion_review", "needs_review", "needs_quality_review"}:
        return None
    package_id = clean(row.get("promotion_package_id"))
    action_type = "promotion_review" if status == "ready_for_promotion_review" else "quality_review"
    comparison_fields = comparison_fields_for_package(row, comparison_rows, comparison_summary)
    return {
        "action_queue_run_id": action_queue_run_id,
        "action_id": stable_id(action_queue_run_id, "package", package_id, status),
        "priority": package_priority(row),
        "action_type": action_type,
        "action_lane": "promotion_review" if action_type == "promotion_review" else "materialized_row_review",
        "status": "open",
        "source_document_id": "",
        "packet_id": "",
        "boxscore_id": clean(row.get("boxscore_id")),
        "target_table": clean(row.get("target_table")),
        "target_entity_key": clean(row.get("target_entity_key")),
        "promotion_package_id": package_id,
        "promotion_conflict_id": "",
        "followup_type": "",
        "confidence_bar": clean(row.get("confidence_bar")),
        "max_confidence_score": clean(row.get("max_confidence_score")),
        "evidence_document_count": clean(row.get("evidence_document_count")),
        "item_count": clean(row.get("item_count")),
        **comparison_fields,
        "reason": f"Package status is {status}.",
        "proposed_fields_json": clean(row.get("proposed_fields_json")),
        "source_documents_json": clean(row.get("source_documents_json")),
        "artifact_path": "",
        "created_at_utc": created_at,
    }


def action_from_conflict(
    action_queue_run_id: str,
    row: dict[str, Any],
    created_at: str,
    comparison_rows: dict[str, dict[str, str]],
    comparison_summary: dict[str, Any],
) -> dict[str, Any]:
    conflict_id = clean(row.get("promotion_conflict_id"))
    values = parse_json_list(row.get("values_json"))
    comparison_fields = comparison_fields_for_package(row, comparison_rows, comparison_summary)
    return {
        "action_queue_run_id": action_queue_run_id,
        "action_id": stable_id(action_queue_run_id, "conflict", conflict_id),
        "priority": 5,
        "action_type": "conflict_review",
        "action_lane": "promotion_conflict_resolution",
        "status": "open",
        "source_document_id": "",
        "packet_id": "",
        "boxscore_id": "",
        "target_table": clean(row.get("target_table")),
        "target_entity_key": clean(row.get("target_entity_key")),
        "promotion_package_id": clean(row.get("promotion_package_id")),
        "promotion_conflict_id": conflict_id,
        "followup_type": "",
        "confidence_bar": "conflict",
        "max_confidence_score": "",
        "evidence_document_count": "",
        "item_count": "",
        **comparison_fields,
        "reason": f"Conflicting values for {clean(row.get('field_name'))}: {len(values)} value groups.",
        "proposed_fields_json": "",
        "source_documents_json": clean(row.get("source_documents_json")),
        "artifact_path": "",
        "created_at_utc": created_at,
    }


def sort_actions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            parse_int(row.get("priority")),
            clean(row.get("action_type")),
            clean(row.get("target_table")),
            clean(row.get("boxscore_id")),
            clean(row.get("source_document_id")),
        ),
    )


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_conveyor_action_queue (
          action_queue_run_id VARCHAR,
          action_id VARCHAR,
          priority INTEGER,
          action_type VARCHAR,
          action_lane VARCHAR,
          status VARCHAR,
          source_document_id VARCHAR,
          packet_id VARCHAR,
          boxscore_id VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          promotion_package_id VARCHAR,
          promotion_conflict_id VARCHAR,
          followup_type VARCHAR,
          confidence_bar VARCHAR,
          max_confidence_score VARCHAR,
          evidence_document_count VARCHAR,
          item_count VARCHAR,
          reason VARCHAR,
          proposed_fields_json VARCHAR,
          source_documents_json VARCHAR,
          artifact_path VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    existing = {
        row[0] for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='llm_conveyor_action_queue'
            """
        ).fetchall()
    }
    for field in ACTION_FIELDS:
        if field in existing:
            continue
        column_type = "INTEGER" if field == "priority" else "VARCHAR"
        con.execute(
            f"ALTER TABLE newspaper_review.llm_conveyor_action_queue "
            f"ADD COLUMN IF NOT EXISTS {field} {column_type}"
        )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_conveyor_action_queue_run (
          action_queue_run_id VARCHAR,
          ingest_run_id VARCHAR,
          package_run_id VARCHAR,
          output_dir VARCHAR,
          action_count INTEGER,
          followup_action_count INTEGER,
          promotion_ready_count INTEGER,
          promotion_conflict_count INTEGER,
          v26_comparison_run_id VARCHAR,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )
    run_existing = {
        row[0] for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='llm_conveyor_action_queue_run'
            """
        ).fetchall()
    }
    integer_fields = {
        "action_count",
        "followup_action_count",
        "promotion_ready_count",
        "promotion_conflict_count",
    }
    for field in RUN_FIELDS:
        if field in run_existing:
            continue
        column_type = "INTEGER" if field in integer_fields else "VARCHAR"
        con.execute(
            f"ALTER TABLE newspaper_review.llm_conveyor_action_queue_run "
            f"ADD COLUMN IF NOT EXISTS {field} {column_type}"
        )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ",".join(["?"] * len(fields))
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({','.join(fields)}) VALUES ({placeholders})",
        [[row.get(field) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    action_queue_run_id: str,
    ingest_run_id: str,
    package_run_id: str,
    v26_comparison_run_id: str,
    out_dir: Path,
    action_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.llm_conveyor_action_queue WHERE action_queue_run_id = ?",
            [action_queue_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.llm_conveyor_action_queue_run WHERE action_queue_run_id = ?",
            [action_queue_run_id],
        )
        insert_rows(con, "llm_conveyor_action_queue", action_rows, ACTION_FIELDS)
        insert_rows(con, "llm_conveyor_action_queue_run", [{
            "action_queue_run_id": action_queue_run_id,
            "ingest_run_id": ingest_run_id,
            "package_run_id": package_run_id,
            "output_dir": str(out_dir),
            "action_count": len(action_rows),
            "followup_action_count": sum(1 for row in action_rows if row["action_type"].endswith("followup")),
            "promotion_ready_count": sum(1 for row in action_rows if row["action_type"] == "promotion_review"),
            "promotion_conflict_count": sum(1 for row in action_rows if row["action_type"] == "conflict_review"),
            "v26_comparison_run_id": v26_comparison_run_id,
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], action_rows: list[dict[str, Any]]) -> None:
    top_rows = action_rows[:25]
    lines = [
        "# Newspaper Conveyor Action Queues",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Action queue run: `{summary['action_queue_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Total actions: `{summary['action_count']}`",
        f"- Actions by type: `{summary['action_type_counts']}`",
        f"- Followups by type: `{summary['followup_type_counts']}`",
        f"- Promotion package statuses: `{summary['package_status_counts']}`",
        f"- Novelty vs v26: `{summary['novelty_vs_v26_counts']}`",
        f"- v26 comparison run: `{summary.get('v26_comparison_run_id', '')}`",
        "",
        "## Highest Priority Actions",
        "",
    ]
    lines.extend(markdown_table(top_rows, [
        "priority",
        "action_type",
        "followup_type",
        "target_table",
        "boxscore_id",
        "novelty_vs_v26",
        "source_document_id",
        "reason",
    ]))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--ingest-run-id", default="")
    parser.add_argument("--package-run-id", default="")
    parser.add_argument("--v26-comparison-root", type=Path, default=DEFAULT_V26_COMPARISON_ROOT)
    parser.add_argument("--v26-comparison-csv", type=Path, default=None)
    parser.add_argument("--label", default="newspaper_conveyor_action_queues")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    action_queue_run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / action_queue_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        ingest_run_id = args.ingest_run_id or latest_value(con, "llm_review_ingest_run", "ingest_run_id")
        package_run_id = args.package_run_id or latest_value(con, "llm_promotion_package_run", "package_run_id")
        followups = load_followups(con, ingest_run_id)
        packages = load_packages(con, package_run_id)
        conflicts = load_conflicts(con, package_run_id)
    finally:
        con.close()
    comparison_rows, comparison_summary = load_v26_comparison(
        package_run_id,
        args.v26_comparison_root,
        args.v26_comparison_csv,
    )
    v26_comparison_run_id = clean(comparison_summary.get("comparison_run_id"))

    action_rows: list[dict[str, Any]] = []
    action_rows.extend(action_from_followup(action_queue_run_id, row, created_at) for row in followups)
    action_rows.extend(
        action_from_conflict(action_queue_run_id, row, created_at, comparison_rows, comparison_summary)
        for row in conflicts
    )
    for package in packages:
        action = action_from_package(action_queue_run_id, package, created_at, comparison_rows, comparison_summary)
        if action:
            action_rows.append(action)
    action_rows = sort_actions(action_rows)

    ocr_actions = [row for row in action_rows if row["action_type"] == "ocr_visual_followup"]
    semantic_actions = [row for row in action_rows if row["action_type"] == "semantic_followup"]
    ready_actions = [row for row in action_rows if row["action_type"] == "promotion_review"]
    conflict_actions = [row for row in action_rows if row["action_type"] == "conflict_review"]
    review_actions = [row for row in action_rows if row["action_type"] in {"quality_review", "general_followup"}]

    write_csv(out_dir / "priority_action_queue.csv", action_rows, ACTION_FIELDS)
    write_csv(out_dir / "next_ocr_visual_followup_queue.csv", ocr_actions, ACTION_FIELDS)
    write_csv(out_dir / "next_semantic_followup_queue.csv", semantic_actions, ACTION_FIELDS)
    write_csv(out_dir / "next_promotion_ready_queue.csv", ready_actions, ACTION_FIELDS)
    write_csv(out_dir / "next_promotion_conflict_queue.csv", conflict_actions, ACTION_FIELDS)
    write_csv(out_dir / "next_quality_review_queue.csv", review_actions, ACTION_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "action_queue_run_id": action_queue_run_id,
        "ingest_run_id": ingest_run_id,
        "package_run_id": package_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "action_count": len(action_rows),
        "action_type_counts": dict(Counter(row["action_type"] for row in action_rows)),
        "followup_type_counts": dict(Counter(row["followup_type"] for row in action_rows if row["followup_type"])),
        "package_status_counts": dict(Counter(clean(row.get("package_status")) for row in packages)),
        "novelty_vs_v26_counts": dict(Counter(row["novelty_vs_v26"] for row in action_rows if row["novelty_vs_v26"])),
        "v26_score_status_counts": dict(Counter(row["v26_score_status"] for row in action_rows if row["v26_score_status"])),
        "v26_comparison_run_id": v26_comparison_run_id,
        "v26_comparison_csv_path": clean(comparison_summary.get("comparison_csv_path")),
        "ocr_visual_followup_count": len(ocr_actions),
        "semantic_followup_count": len(semantic_actions),
        "promotion_ready_count": len(ready_actions),
        "promotion_conflict_count": len(conflict_actions),
        "quality_review_count": len(review_actions),
    }
    summary_path = out_dir / "summary.json"
    write_json(summary_path, summary)
    write_markdown(out_dir / "action_queue_report.md", summary, action_rows)
    persist(
        args.db_path,
        action_queue_run_id,
        ingest_run_id,
        package_run_id,
        v26_comparison_run_id,
        out_dir,
        action_rows,
        summary_path,
    )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
