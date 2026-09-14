#!/usr/bin/env python
"""Build a consolidated decision ledger for newspaper conveyor handoffs.

This station does not promote rows to live tables. It gathers the latest OCR,
semantic, conflict, quality-review, and promotion-review prep outputs into one
auditable ledger. A reviewer or LLM can later edit/export decisions from this
ledger, and the conveyor can ingest those decisions without losing source
evidence.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "review_decision_ledgers"

DECISION_FIELDS = [
    "decision_ledger_run_id",
    "decision_id",
    "lane",
    "source_prep_run_id",
    "action_queue_run_id",
    "action_id",
    "promotion_package_id",
    "source_document_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "confidence_bar",
    "confidence_score",
    "evidence_document_count",
    "item_count",
    "recommended_next_action",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_boxscore_id",
    "resolved_target_table",
    "resolved_target_entity_key",
    "reason",
    "proposed_fields_json",
    "source_documents_json",
    "artifact_path",
    "notes",
    "created_at_utc",
]

RUN_FIELDS = [
    "decision_ledger_run_id",
    "output_dir",
    "decision_count",
    "open_decision_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]


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


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in rows:
        values = [clean(row.get(field)).replace("\n", " ") for field in fields]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema='newspaper_review'
          AND table_name=?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def latest_value(con: duckdb.DuckDBPyConnection, table: str, id_field: str) -> str:
    if not table_exists(con, table):
        return ""
    row = con.execute(
        f"""
        SELECT {id_field}
        FROM newspaper_review.{table}
        ORDER BY created_at_utc DESC, {id_field} DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_rows(con: duckdb.DuckDBPyConnection, table: str, id_field: str, run_id: str) -> list[dict[str, Any]]:
    if not run_id or not table_exists(con, table):
        return []
    return query_dicts(
        con,
        f"SELECT * FROM newspaper_review.{table} WHERE {id_field} = ?",
        [run_id],
    )


def load_conflict_actions(con: duckdb.DuckDBPyConnection, action_queue_run_id: str) -> list[dict[str, Any]]:
    if not action_queue_run_id or not table_exists(con, "llm_conveyor_action_queue"):
        return []
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_conveyor_action_queue
        WHERE action_queue_run_id = ?
          AND action_type = 'conflict_review'
        ORDER BY priority, target_table, target_entity_key, promotion_conflict_id
        """,
        [action_queue_run_id],
    )


def load_decision_overrides(path: Path | None) -> dict[str, dict[str, str]]:
    if not path or not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {clean(row.get("decision_id")): row for row in rows if clean(row.get("decision_id"))}


def overlay_decision(row: dict[str, Any], overrides: dict[str, dict[str, str]]) -> dict[str, Any]:
    override = overrides.get(clean(row.get("decision_id")))
    if not override:
        return row
    for field in [
        "decision_status",
        "decision_value",
        "route_to_lane",
        "resolved_boxscore_id",
        "resolved_target_table",
        "resolved_target_entity_key",
        "proposed_fields_json",
        "notes",
    ]:
        value = clean(override.get(field))
        if value:
            row[field] = value
    return row


def is_open_decision(row: dict[str, Any]) -> bool:
    status = clean(row.get("decision_status")).lower()
    value = clean(row.get("decision_value")).lower()
    return status in {"", "pending", "queued_for_ocr", "pending_followup"} or value in {"", "pending"}


def ocr_route_from_followup_type(followup_type: str) -> str:
    kind = followup_type.lower().strip()
    if kind in {"ocr_region_missing_text", "full_page_visual_review"}:
        return "render_page_and_reocr"
    if kind in {"column_split", "narrower_crop"}:
        return "column_crop_then_reocr"
    if kind == "larger_crop":
        return "expand_crop_then_reocr"
    if kind == "visual_score_check":
        return "visual_verify_score"
    if kind in {"deeper_article_ocr", "better_ocr"}:
        return "high_resolution_article_ocr"
    return "ocr_followup_review"


def decision_row(
    decision_ledger_run_id: str,
    lane: str,
    source_prep_run_id: str,
    row: dict[str, Any],
    created_at: str,
    decision_status: str,
    decision_value: str,
    route_to_lane: str,
    reason: str = "",
) -> dict[str, Any]:
    package_id = clean(row.get("promotion_package_id"))
    source_document_id = clean(row.get("source_document_id"))
    action_id = clean(row.get("action_id"))
    decision_id = stable_id(lane, source_prep_run_id, action_id, package_id, source_document_id, row.get("target_entity_key"))
    return {
        "decision_ledger_run_id": decision_ledger_run_id,
        "decision_id": decision_id,
        "lane": lane,
        "source_prep_run_id": source_prep_run_id,
        "action_queue_run_id": clean(row.get("action_queue_run_id")),
        "action_id": action_id,
        "promotion_package_id": package_id,
        "source_document_id": source_document_id,
        "target_table": clean(row.get("target_table")),
        "target_entity_key": clean(row.get("target_entity_key")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "confidence_bar": clean(row.get("confidence_bar")),
        "confidence_score": clean(row.get("max_confidence_score")) or clean(row.get("confidence_score")),
        "evidence_document_count": clean(row.get("evidence_document_count")),
        "item_count": clean(row.get("item_count")),
        "recommended_next_action": clean(row.get("recommended_next_action")) or clean(row.get("recommended_resolution_pass")) or clean(row.get("recommended_next_pass")),
        "decision_status": decision_status,
        "decision_value": decision_value,
        "route_to_lane": route_to_lane,
        "resolved_boxscore_id": clean(row.get("resolved_boxscore_id")),
        "resolved_target_table": clean(row.get("resolved_target_table")),
        "resolved_target_entity_key": clean(row.get("resolved_target_entity_key")),
        "reason": reason or clean(row.get("reason")),
        "proposed_fields_json": clean(row.get("proposed_fields_json")),
        "source_documents_json": clean(row.get("source_documents_json")),
        "artifact_path": clean(row.get("review_output_path")) or clean(row.get("asset_pdf_path")),
        "notes": clean(row.get("review_notes")) or clean(row.get("resolution_notes")),
        "created_at_utc": created_at,
    }


def build_decisions(
    decision_ledger_run_id: str,
    ocr_rows: list[dict[str, Any]],
    semantic_rows: list[dict[str, Any]],
    conflict_rows: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
    promotion_rows: list[dict[str, Any]],
    generated_rows: list[dict[str, Any]],
    overrides: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    created_at = iso_now()
    decisions: list[dict[str, Any]] = []
    for row in ocr_rows:
        route = clean(row.get("recommended_next_pass")) or ocr_route_from_followup_type(clean(row.get("followup_type")))
        decisions.append(decision_row(
            decision_ledger_run_id,
            "ocr_visual_followup",
            clean(row.get("ocr_followup_prep_run_id")),
            row,
            created_at,
            "queued_for_ocr",
            "pending_ocr_followup",
            route,
        ))
    for row in semantic_rows:
        status = clean(row.get("resolution_status")) or "pending"
        decisions.append(decision_row(
            decision_ledger_run_id,
            "semantic_followup",
            clean(row.get("semantic_followup_prep_run_id")),
            row,
            created_at,
            status,
            "pending",
            clean(row.get("recommended_resolution_pass")),
        ))
    for row in conflict_rows:
        decisions.append(decision_row(
            decision_ledger_run_id,
            "conflict_review",
            clean(row.get("action_queue_run_id")),
            row,
            created_at,
            "pending",
            "pending",
            "resolve_conflict_then_promote",
        ))
    for row in quality_rows:
        status = clean(row.get("quality_decision")) or "pending"
        decisions.append(decision_row(
            decision_ledger_run_id,
            "quality_review",
            clean(row.get("quality_review_prep_run_id")),
            row,
            created_at,
            status,
            "pending",
            clean(row.get("recommended_next_action")),
        ))
    for row in promotion_rows:
        status = clean(row.get("review_decision")) or "pending"
        decisions.append(decision_row(
            decision_ledger_run_id,
            "promotion_review",
            clean(row.get("promotion_review_prep_run_id")),
            row,
            created_at,
            status,
            "pending",
            "promotion_review",
        ))
    for row in generated_rows:
        generated = {field: clean(row.get(field)) for field in DECISION_FIELDS}
        generated["decision_ledger_run_id"] = decision_ledger_run_id
        if not generated["decision_id"]:
            generated["decision_id"] = stable_id(
                generated.get("lane"),
                generated.get("source_prep_run_id"),
                generated.get("source_document_id"),
                generated.get("target_table"),
                generated.get("target_entity_key"),
                generated.get("proposed_fields_json"),
            )
        if not generated["lane"]:
            generated["lane"] = "generated_atom_decision"
        if not generated["source_prep_run_id"]:
            generated["source_prep_run_id"] = clean(row.get("generated_atom_decision_run_id"))
        if not generated["created_at_utc"]:
            generated["created_at_utc"] = created_at
        decisions.append(generated)
    return [overlay_decision(row, overrides) for row in decisions]


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    defs = ", ".join(f"{field} VARCHAR" for field in DECISION_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.llm_review_decision_ledger ({defs})")
    existing = {
        row[0] for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='llm_review_decision_ledger'
            """
        ).fetchall()
    }
    for field in DECISION_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.llm_review_decision_ledger ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_review_decision_ledger_run (
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          decision_count INTEGER,
          open_decision_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ",".join(["?"] * len(fields))
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({','.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    decision_ledger_run_id: str,
    out_dir: Path,
    decision_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.llm_review_decision_ledger WHERE decision_ledger_run_id = ?",
            [decision_ledger_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.llm_review_decision_ledger_run WHERE decision_ledger_run_id = ?",
            [decision_ledger_run_id],
        )
        insert_rows(con, "llm_review_decision_ledger", decision_rows, DECISION_FIELDS)
        insert_rows(con, "llm_review_decision_ledger_run", [{
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "decision_count": len(decision_rows),
            "open_decision_count": sum(1 for row in decision_rows if is_open_decision(row)),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], decision_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Review Decision Ledger",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Ledger run: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Decisions: `{summary['decision_count']}`",
        f"- Open decisions: `{summary['open_decision_count']}`",
        f"- Lanes: `{summary['lane_counts']}`",
        f"- Routes: `{summary['route_to_lane_counts']}`",
        "",
        "## Open Queue",
        "",
    ]
    open_rows = [row for row in decision_rows if is_open_decision(row)]
    lines.extend(markdown_table(open_rows[:120], [
        "lane",
        "route_to_lane",
        "target_table",
        "boxscore_id",
        "source_document_id",
        "confidence_bar",
        "decision_status",
        "recommended_next_action",
        "reason",
    ]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_review_decision_ledger")
    parser.add_argument("--ocr-followup-prep-run-id", default="")
    parser.add_argument("--semantic-followup-prep-run-id", default="")
    parser.add_argument("--action-queue-run-id", default="")
    parser.add_argument("--quality-review-prep-run-id", default="")
    parser.add_argument("--promotion-review-prep-run-id", default="")
    parser.add_argument("--generated-atom-decision-run-id", default="")
    parser.add_argument("--decision-input-csv", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    decision_ledger_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / decision_ledger_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        ocr_run_id = args.ocr_followup_prep_run_id or latest_value(con, "llm_ocr_followup_prep_run", "ocr_followup_prep_run_id")
        semantic_run_id = args.semantic_followup_prep_run_id or latest_value(con, "llm_semantic_followup_prep_run", "semantic_followup_prep_run_id")
        action_queue_run_id = args.action_queue_run_id or latest_value(con, "llm_conveyor_action_queue_run", "action_queue_run_id")
        quality_run_id = args.quality_review_prep_run_id or latest_value(con, "llm_quality_review_prep_run", "quality_review_prep_run_id")
        promotion_run_id = args.promotion_review_prep_run_id or latest_value(con, "llm_promotion_review_prep_run", "promotion_review_prep_run_id")
        generated_run_id = args.generated_atom_decision_run_id or latest_value(con, "generated_atom_decision_run", "generated_atom_decision_run_id")
        ocr_rows = load_rows(con, "llm_ocr_followup_prep_action", "ocr_followup_prep_run_id", ocr_run_id)
        semantic_rows = load_rows(con, "llm_semantic_followup_prep_action", "semantic_followup_prep_run_id", semantic_run_id)
        conflict_rows = load_conflict_actions(con, action_queue_run_id)
        quality_rows = load_rows(con, "llm_quality_review_prep_package", "quality_review_prep_run_id", quality_run_id)
        promotion_rows = load_rows(con, "llm_promotion_review_prep_package", "promotion_review_prep_run_id", promotion_run_id)
        generated_rows = load_rows(con, "generated_atom_decision", "generated_atom_decision_run_id", generated_run_id)
    finally:
        con.close()

    overrides = load_decision_overrides(args.decision_input_csv)
    decision_rows = build_decisions(
        decision_ledger_run_id,
        ocr_rows,
        semantic_rows,
        conflict_rows,
        quality_rows,
        promotion_rows,
        generated_rows,
        overrides,
    )
    open_rows = [row for row in decision_rows if is_open_decision(row)]
    summary = {
        "created_at_utc": created_at,
        "decision_ledger_run_id": decision_ledger_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "source_run_ids": {
            "ocr_followup_prep_run_id": ocr_run_id,
            "semantic_followup_prep_run_id": semantic_run_id,
            "action_queue_run_id": action_queue_run_id,
            "quality_review_prep_run_id": quality_run_id,
            "promotion_review_prep_run_id": promotion_run_id,
            "generated_atom_decision_run_id": generated_run_id,
        },
        "decision_count": len(decision_rows),
        "open_decision_count": len(open_rows),
        "lane_counts": dict(Counter(row["lane"] for row in decision_rows)),
        "decision_status_counts": dict(Counter(row["decision_status"] for row in decision_rows)),
        "route_to_lane_counts": dict(Counter(row["route_to_lane"] for row in decision_rows)),
        "target_table_counts": dict(Counter(row["target_table"] for row in decision_rows if row["target_table"])),
        "decision_input_csv": str(args.decision_input_csv) if args.decision_input_csv else "",
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "review_decision_ledger.csv", decision_rows, DECISION_FIELDS)
    write_csv(out_dir / "open_decision_queue.csv", open_rows, DECISION_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "review_decision_work_order.md", summary, decision_rows)
    persist(args.db_path, decision_ledger_run_id, out_dir, decision_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
