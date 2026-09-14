#!/usr/bin/env python
"""Close completed OCR follow-up tasks into local source-document notes.

OCR follow-up rows are action items, not facts. Once the sidecar OCR is done
and the packet review has materialized claims, this station converts the
original OCR task into an auditable `source_document_note` so the conveyor does
not keep asking for a crop/re-OCR pass that already happened.
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "ocr_completion_resolution_preps"

PROMOTION_VALUE = "approved_for_local_promotion"

DECISION_INPUT_FIELDS = [
    "decision_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_boxscore_id",
    "resolved_target_table",
    "resolved_target_entity_key",
    "proposed_fields_json",
    "notes",
]

ITEM_FIELDS = [
    "ocr_completion_resolution_run_id",
    "promotion_apply_run_id",
    "decision_id",
    "source_document_id",
    "boxscore_id",
    "recommended_next_action",
    "sidecar_done",
    "recommended_next_pass",
    "ocr_text_path",
    "ocr_json_path",
    "ocr_text_chars",
    "review_claim_count",
    "review_output_path",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "reason",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "ocr_completion_resolution_run_id",
    "promotion_apply_run_id",
    "output_dir",
    "ocr_route_count",
    "approved_count",
    "held_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def truthy(value: Any) -> bool:
    return clean(value).strip().lower() in {"1", "true", "yes", "y"}


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_apply_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT promotion_apply_run_id
        FROM newspaper_review.review_decision_apply_run
        ORDER BY created_at_utc DESC, promotion_apply_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def latest_decision_input(root: Path) -> Path | None:
    patterns = [
        root / "ocr_completion_resolution_preps" / "*" / "decision_input.csv",
        root / "semantic_claim_resolution_preps" / "*" / "decision_input.csv",
        root / "team_game_stat_resolution_preps" / "*" / "decision_input.csv",
        root / "schema_gap_resolution_preps" / "*" / "decision_input.csv",
        root / "semantic_followup_resolution_preps" / "*" / "decision_input.csv",
        root / "quality_lane_resolution_preps" / "*" / "decision_input.csv",
        root / "semantic_game_key_resolution_preps" / "*" / "decision_input.csv",
        root / "player_box_score_resolution_preps" / "*" / "decision_input.csv",
        root / "event_detail_resolution_preps" / "*" / "decision_input.csv",
        root / "lineup_identity_resolution_preps" / "*" / "decision_input.csv",
        root / "review_decision_inputs" / "*" / "decision_input.csv",
    ]
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path(path) for path in glob.glob(str(pattern)))
    paths = [path for path in paths if path.exists()]
    if not paths:
        return None
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0]


def read_base_decisions(path: Path | None) -> list[dict[str, str]]:
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def combine_decision_inputs(base_rows: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_id: dict[str, dict[str, str]] = {}
    for row in base_rows:
        decision_id = clean(row.get("decision_id"))
        if decision_id:
            by_id[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    for row in new_rows:
        decision_id = clean(row.get("decision_id"))
        if decision_id:
            by_id[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    return [by_id[key] for key in sorted(by_id)]


def latest_ocr_row_status(root: Path) -> Path | None:
    paths = [Path(path) for path in glob.glob(str(root / "ocr_followup_chunk_runs" / "*" / "ocr_followup_row_status.csv"))]
    paths = [path for path in paths if path.exists()]
    if not paths:
        return None
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0]


def read_ocr_status(path: Path | None) -> dict[str, dict[str, str]]:
    if not path or not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {clean(row.get("candidate_id")): row for row in rows if clean(row.get("candidate_id"))}


def load_ocr_route_rows(con: duckdb.DuckDBPyConnection, promotion_apply_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND route_to_lane = 'ocr_visual_followup'
        ORDER BY source_document_id, decision_id
        """,
        [promotion_apply_run_id],
    )


def load_claim_counts(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> dict[str, int]:
    if not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT source_document_id, COUNT(*) AS claim_count
        FROM newspaper_review.llm_review_claim
        WHERE source_document_id IN ({placeholders})
        GROUP BY source_document_id
        """,
        source_document_ids,
    )
    return {clean(row.get("source_document_id")): int(row.get("claim_count") or 0) for row in rows}


def source_note_payload(row: dict[str, Any], status: dict[str, str], claim_count: int) -> dict[str, Any]:
    source_document_id = clean(row.get("source_document_id"))
    boxscore_id = clean(row.get("boxscore_id"))
    text_path = clean(status.get("ocr_text_path"))
    json_path = clean(status.get("ocr_json_path"))
    chars = clean(status.get("ocr_text_chars"))
    note_text = (
        f"OCR follow-up completed for {source_document_id}; "
        f"pass={clean(status.get('recommended_next_pass'))}; "
        f"text_chars={chars or '0'}; materialized_claims={claim_count}. "
        f"Original reason: {clean(row.get('reason'))}"
    )
    return {
        "source_document_note_id": stable_id("ocr_completion_note", source_document_id, boxscore_id, text_path, json_path),
        "source_document_id": source_document_id,
        "boxscore_id": boxscore_id,
        "note_type": "ocr_followup_completed",
        "note_category": "ocr_completion",
        "note_text": note_text,
        "related_target_table": clean(row.get("target_table")),
        "related_entity_key": clean(row.get("target_entity_key")),
        "reconciliation_status": "ocr_completed_claims_materialized" if claim_count else "ocr_completed_no_materialized_claims",
        "evidence_text": clean(row.get("reason")),
        "confidence_score": "0.70",
        "review_status": "local_atom_ocr_completion_accepted",
        "promotion_status": "local_atom_only",
        "source_documents_json": json.dumps([source_document_id] if source_document_id else []),
        "artifact_path": text_path or clean(row.get("artifact_path")),
        "created_by_station": "build_newspaper_ocr_completion_resolution_prep.py",
    }


def build_outputs(
    run_id: str,
    promotion_apply_run_id: str,
    route_rows: list[dict[str, Any]],
    ocr_status: dict[str, dict[str, str]],
    claim_counts: dict[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    created_at = iso_now()
    item_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, str]] = []
    for row in route_rows:
        source_document_id = clean(row.get("source_document_id"))
        status = ocr_status.get(source_document_id, {})
        done = truthy(status.get("sidecar_done"))
        claim_count = claim_counts.get(source_document_id, 0)
        if not done:
            item_rows.append({
                "ocr_completion_resolution_run_id": run_id,
                "promotion_apply_run_id": promotion_apply_run_id,
                "decision_id": clean(row.get("decision_id")),
                "source_document_id": source_document_id,
                "boxscore_id": clean(row.get("boxscore_id")),
                "recommended_next_action": clean(row.get("recommended_next_action")),
                "sidecar_done": "false",
                "review_claim_count": claim_count,
                "decision_status": "pending_followup",
                "decision_value": "pending",
                "route_to_lane": "ocr_visual_followup",
                "reason": "OCR sidecar not complete; keeping route open",
                "proposed_fields_json": clean(row.get("proposed_fields_json")),
                "created_at_utc": created_at,
            })
            continue
        payload = source_note_payload(row, status, claim_count)
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        decision_rows.append({
            "decision_id": clean(row.get("decision_id")),
            "decision_status": "approved_for_local_promotion",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "",
            "resolved_boxscore_id": clean(row.get("boxscore_id")),
            "resolved_target_table": "source_document_note",
            "resolved_target_entity_key": f"source_document_note|ocr_completion|{clean(row.get('boxscore_id'))}|{source_document_id}",
            "proposed_fields_json": payload_json,
            "notes": "completed OCR follow-up preserved as source_document_note",
        })
        item_rows.append({
            "ocr_completion_resolution_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "decision_id": clean(row.get("decision_id")),
            "source_document_id": source_document_id,
            "boxscore_id": clean(row.get("boxscore_id")),
            "recommended_next_action": clean(row.get("recommended_next_action")),
            "sidecar_done": "true",
            "recommended_next_pass": clean(status.get("recommended_next_pass")),
            "ocr_text_path": clean(status.get("ocr_text_path")),
            "ocr_json_path": clean(status.get("ocr_json_path")),
            "ocr_text_chars": clean(status.get("ocr_text_chars")),
            "review_claim_count": claim_count,
            "review_output_path": clean(row.get("artifact_path")),
            "decision_status": "approved_for_local_promotion",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "",
            "reason": "OCR sidecar completed; close OCR action into source_document_note",
            "proposed_fields_json": payload_json,
            "created_at_utc": created_at,
        })
    return item_rows, decision_rows


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(fields))
    field_list = ", ".join(fields)
    values = [[clean(row.get(field)) for field in fields] for row in rows]
    con.executemany(f"INSERT INTO newspaper_review.{table} ({field_list}) VALUES ({placeholders})", values)


def persist(
    db_path: Path,
    run_id: str,
    promotion_apply_run_id: str,
    out_dir: Path,
    item_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.ocr_completion_resolution_item ("
            + ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.ocr_completion_resolution_run ("
            + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
            + ")"
        )
        for field in ITEM_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.ocr_completion_resolution_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in RUN_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.ocr_completion_resolution_run ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        con.execute("DELETE FROM newspaper_review.ocr_completion_resolution_item WHERE ocr_completion_resolution_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.ocr_completion_resolution_run WHERE ocr_completion_resolution_run_id = ?", [run_id])
        insert_rows(con, "ocr_completion_resolution_item", item_rows, ITEM_FIELDS)
        approved = sum(1 for row in item_rows if clean(row.get("decision_value")) == PROMOTION_VALUE)
        insert_rows(con, "ocr_completion_resolution_run", [{
            "ocr_completion_resolution_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "output_dir": str(out_dir),
            "ocr_route_count": len(item_rows),
            "approved_count": approved,
            "held_count": len(item_rows) - approved,
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper OCR Completion Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['ocr_completion_resolution_run_id']}`",
        f"Apply source: `{summary['promotion_apply_run_id']}`",
        "",
        "## Counts",
        "",
        f"- OCR route rows: `{summary['ocr_route_count']}`",
        f"- Approved completion notes: `{summary['approved_count']}`",
        f"- Held rows: `{summary['held_count']}`",
        "",
        "## Items",
        "",
        "| source document | boxscore | done | text chars | claims | status |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for row in item_rows:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("source_document_id")),
                clean(row.get("boxscore_id")),
                clean(row.get("sidecar_done")),
                clean(row.get("ocr_text_chars")),
                clean(row.get("review_claim_count")),
                clean(row.get("decision_status")),
            ])
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_ocr_completion_resolution")
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--ocr-row-status-csv", type=Path, default=None)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        promotion_apply_run_id = args.promotion_apply_run_id or latest_apply_run(con)
        route_rows = load_ocr_route_rows(con, promotion_apply_run_id)
        source_document_ids = [clean(row.get("source_document_id")) for row in route_rows if clean(row.get("source_document_id"))]
        claim_counts = load_claim_counts(con, source_document_ids)
    finally:
        con.close()

    status_path = args.ocr_row_status_csv or latest_ocr_row_status(args.root)
    ocr_status = read_ocr_status(status_path)
    item_rows, decision_rows = build_outputs(run_id, promotion_apply_run_id, route_rows, ocr_status, claim_counts)

    base_input = args.base_decision_input_csv or latest_decision_input(args.root)
    base_rows = read_base_decisions(base_input)
    combined_rows = combine_decision_inputs(base_rows, decision_rows)

    decision_input_path = out_dir / "decision_input.csv"
    approved = len(decision_rows)
    summary = {
        "created_at_utc": created_at,
        "ocr_completion_resolution_run_id": run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "ocr_row_status_csv": str(status_path) if status_path else "",
        "base_decision_input_csv": str(base_input) if base_input else "",
        "decision_input_csv": str(decision_input_path),
        "ocr_route_count": len(route_rows),
        "approved_count": approved,
        "held_count": len(route_rows) - approved,
        "sidecar_status_counts": dict(Counter(clean(row.get("sidecar_done")) for row in item_rows)),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "ocr_completion_resolution_items.csv", item_rows, ITEM_FIELDS)
    write_csv(out_dir / "decision_input_delta.csv", decision_rows, DECISION_INPUT_FIELDS)
    write_csv(decision_input_path, combined_rows, DECISION_INPUT_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "ocr_completion_resolution_report.md", summary, item_rows)
    if not args.no_db:
        persist(args.db_path, run_id, promotion_apply_run_id, out_dir, item_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
