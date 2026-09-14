#!/usr/bin/env python
"""Prepare semantic follow-up handoffs from the newspaper action queue.

Semantic follow-ups are cases where OCR text is present but the conveyor needs
human/LLM reconciliation before a row can safely move toward promotion. Examples
include target-game routing, team-label normalization, duplicate-source routing,
and boxscore reconciliation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "semantic_followup_preps"

ACTION_FIELDS = [
    "semantic_followup_prep_run_id",
    "action_queue_run_id",
    "ingest_run_id",
    "action_id",
    "priority",
    "followup_type",
    "recommended_resolution_pass",
    "source_document_id",
    "packet_id",
    "boxscore_id",
    "document_boxscore_id",
    "target_table",
    "target_entity_key",
    "reason",
    "publication",
    "issue_date",
    "page",
    "source_url",
    "asset_pdf_path",
    "asset_image_path",
    "doc_disposition",
    "document_next_action",
    "claim_count",
    "high_claim_count",
    "medium_claim_count",
    "low_claim_count",
    "source_documents_json",
    "claims_json",
    "region_count",
    "region_ids_json",
    "crop_image_paths_json",
    "region_text_paths_json",
    "region_excerpt_json",
    "review_output_path",
    "resolution_status",
    "resolved_boxscore_id",
    "resolved_target_table",
    "resolved_target_entity_key",
    "resolution_notes",
    "created_at_utc",
]

DOCUMENT_FIELDS = [
    "semantic_followup_prep_run_id",
    "action_queue_run_id",
    "ingest_run_id",
    "source_document_id",
    "priority",
    "followup_types_json",
    "recommended_resolution_passes_json",
    "action_count",
    "packet_ids_json",
    "boxscore_ids_json",
    "reasons_json",
    "publication",
    "issue_date",
    "page",
    "source_url",
    "asset_pdf_path",
    "asset_image_path",
    "doc_disposition",
    "document_next_action",
    "claim_count",
    "claim_tables_json",
    "claims_json",
    "region_count",
    "region_ids_json",
    "crop_image_paths_json",
    "region_text_paths_json",
    "region_excerpt_json",
    "resolution_status",
    "resolution_notes",
    "created_at_utc",
]

RUN_FIELDS = [
    "semantic_followup_prep_run_id",
    "action_queue_run_id",
    "ingest_run_id",
    "output_dir",
    "action_count",
    "document_count",
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


def parse_int(value: Any) -> int:
    try:
        return int(float(clean(value)))
    except (TypeError, ValueError):
        return 0


def truncate(value: Any, max_chars: int) -> str:
    text = clean(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + f"\n[TRUNCATED at {max_chars} of {len(text)} chars]"


def read_text_if_exists(path_text: str, max_chars: int) -> str:
    if not path_text:
        return ""
    path = Path(path_text)
    if not path.exists():
        return ""
    try:
        return truncate(path.read_text(encoding="utf-8", errors="replace"), max_chars)
    except OSError:
        return ""


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


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in rows:
        values = [clean(row.get(field)).replace("\n", " ") for field in fields]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def latest_action_queue_run(con: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    row = con.execute(
        """
        SELECT action_queue_run_id, ingest_run_id
        FROM newspaper_review.llm_conveyor_action_queue_run
        ORDER BY created_at_utc DESC, action_queue_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return "", ""
    return clean(row[0]), clean(row[1])


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def load_actions(con: duckdb.DuckDBPyConnection, action_queue_run_id: str, limit: int) -> list[dict[str, Any]]:
    if not action_queue_run_id:
        return []
    sql = """
        SELECT
          a.*,
          run.ingest_run_id,
          sd.boxscore_id AS source_boxscore_id,
          sd.publication AS source_publication,
          CAST(sd.issue_date AS VARCHAR) AS source_issue_date,
          sd.page AS source_page,
          sd.source_url,
          sd.asset_pdf_path,
          sd.asset_image_path,
          d.boxscore_id AS document_boxscore_id,
          d.doc_disposition,
          d.document_next_action,
          d.claim_count,
          d.high_claim_count,
          d.medium_claim_count,
          d.low_claim_count,
          d.review_output_path AS document_review_output_path
        FROM newspaper_review.llm_conveyor_action_queue a
        LEFT JOIN newspaper_review.llm_conveyor_action_queue_run run
          ON a.action_queue_run_id = run.action_queue_run_id
        LEFT JOIN newspaper_review.source_document sd
          ON a.source_document_id = sd.source_document_id
        LEFT JOIN newspaper_review.llm_document_review d
          ON run.ingest_run_id = d.ingest_run_id
         AND a.source_document_id = d.source_document_id
        WHERE a.action_queue_run_id = ?
          AND a.action_type = 'semantic_followup'
        ORDER BY a.priority, a.followup_type, a.source_document_id
    """
    params: list[Any] = [action_queue_run_id]
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    return query_dicts(con, sql, params)


def load_regions(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT
          source_document_id,
          region_id,
          region_label,
          region_type,
          crop_image_path,
          region_text_path,
          bbox_json,
          region_ocr_confidence_mean,
          region_quality_score,
          next_action,
          reason_code
        FROM newspaper_review.source_region
        WHERE source_document_id IN ({placeholders})
        ORDER BY source_document_id, region_id
        """,
        source_document_ids,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[clean(row.get("source_document_id"))].append(row)
    return grouped


def load_claims(
    con: duckdb.DuckDBPyConnection,
    ingest_run_id: str,
    source_document_ids: list[str],
    max_claims_per_doc: int,
) -> dict[str, list[dict[str, Any]]]:
    if not ingest_run_id or not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT
          source_document_id,
          claim_index,
          target_table,
          target_field,
          entity_text,
          raw_value,
          normalized_value,
          numeric_value,
          unit,
          evidence_region_id,
          evidence_quote,
          confidence_score,
          confidence_lane,
          promotion_recommendation,
          row_group_key,
          reason
        FROM newspaper_review.llm_review_claim
        WHERE ingest_run_id = ?
          AND source_document_id IN ({placeholders})
        ORDER BY source_document_id, claim_index
        """,
        [ingest_run_id, *source_document_ids],
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        source_document_id = clean(row.get("source_document_id"))
        if len(grouped[source_document_id]) < max_claims_per_doc:
            grouped[source_document_id].append(row)
    return grouped


def recommended_resolution_pass(followup_type: str) -> str:
    kind = followup_type.lower().strip()
    if kind == "boxscore_reconciliation":
        return "verify_target_boxscore_and_game_key"
    if kind == "team_label_resolution":
        return "resolve_team_label_to_franchise_key"
    if kind == "duplicate_source_route":
        return "preserve_duplicate_evidence_and_route_once"
    if kind == "semantic_route_review":
        return "assign_correct_atom_lane_or_new_game"
    if kind == "cross_game_result_reconciliation":
        return "split_or_route_cross_game_results"
    return "semantic_resolution_review"


def region_payload(regions: list[dict[str, Any]], max_regions: int, max_chars: int) -> dict[str, Any]:
    selected = regions[:max_regions]
    excerpts = []
    for region in selected:
        text_path = clean(region.get("region_text_path"))
        excerpts.append({
            "region_id": clean(region.get("region_id")),
            "region_label": clean(region.get("region_label")),
            "region_type": clean(region.get("region_type")),
            "crop_image_path": clean(region.get("crop_image_path")),
            "region_text_path": text_path,
            "text_excerpt": read_text_if_exists(text_path, max_chars),
        })
    return {
        "region_count": len(regions),
        "region_ids": [clean(region.get("region_id")) for region in selected],
        "crop_image_paths": [clean(region.get("crop_image_path")) for region in selected if clean(region.get("crop_image_path"))],
        "region_text_paths": [clean(region.get("region_text_path")) for region in selected if clean(region.get("region_text_path"))],
        "region_excerpts": excerpts,
    }


def claims_payload(claims: list[dict[str, Any]], max_quote_chars: int) -> list[dict[str, Any]]:
    payload = []
    for claim in claims:
        payload.append({
            "claim_index": clean(claim.get("claim_index")),
            "target_table": clean(claim.get("target_table")),
            "target_field": clean(claim.get("target_field")),
            "entity_text": clean(claim.get("entity_text")),
            "raw_value": clean(claim.get("raw_value")),
            "normalized_value": clean(claim.get("normalized_value")),
            "numeric_value": clean(claim.get("numeric_value")),
            "unit": clean(claim.get("unit")),
            "evidence_region_id": clean(claim.get("evidence_region_id")),
            "evidence_quote": truncate(claim.get("evidence_quote"), max_quote_chars),
            "confidence_score": clean(claim.get("confidence_score")),
            "confidence_lane": clean(claim.get("confidence_lane")),
            "promotion_recommendation": clean(claim.get("promotion_recommendation")),
            "row_group_key": clean(claim.get("row_group_key")),
            "reason": clean(claim.get("reason")),
        })
    return payload


def build_rows(
    prep_run_id: str,
    action_queue_run_id: str,
    actions: list[dict[str, Any]],
    regions_by_doc: dict[str, list[dict[str, Any]]],
    claims_by_doc: dict[str, list[dict[str, Any]]],
    max_regions: int,
    max_region_chars: int,
    max_claim_quote_chars: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    created_at = iso_now()
    action_rows: list[dict[str, Any]] = []
    for action in actions:
        source_document_id = clean(action.get("source_document_id"))
        region_info = region_payload(regions_by_doc.get(source_document_id, []), max_regions, max_region_chars)
        claim_info = claims_payload(claims_by_doc.get(source_document_id, []), max_claim_quote_chars)
        followup_type = clean(action.get("followup_type"))
        action_rows.append({
            "semantic_followup_prep_run_id": prep_run_id,
            "action_queue_run_id": action_queue_run_id,
            "ingest_run_id": clean(action.get("ingest_run_id")),
            "action_id": clean(action.get("action_id")),
            "priority": parse_int(action.get("priority")),
            "followup_type": followup_type,
            "recommended_resolution_pass": recommended_resolution_pass(followup_type),
            "source_document_id": source_document_id,
            "packet_id": clean(action.get("packet_id")),
            "boxscore_id": clean(action.get("boxscore_id")) or clean(action.get("source_boxscore_id")),
            "document_boxscore_id": clean(action.get("document_boxscore_id")),
            "target_table": clean(action.get("target_table")),
            "target_entity_key": clean(action.get("target_entity_key")),
            "reason": clean(action.get("reason")),
            "publication": clean(action.get("source_publication")) or clean(action.get("publication")),
            "issue_date": clean(action.get("source_issue_date")) or clean(action.get("issue_date")),
            "page": clean(action.get("source_page")) or clean(action.get("page")),
            "source_url": clean(action.get("source_url")),
            "asset_pdf_path": clean(action.get("asset_pdf_path")),
            "asset_image_path": clean(action.get("asset_image_path")),
            "doc_disposition": clean(action.get("doc_disposition")),
            "document_next_action": clean(action.get("document_next_action")),
            "claim_count": clean(action.get("claim_count")),
            "high_claim_count": clean(action.get("high_claim_count")),
            "medium_claim_count": clean(action.get("medium_claim_count")),
            "low_claim_count": clean(action.get("low_claim_count")),
            "source_documents_json": clean(action.get("source_documents_json")) or json.dumps([source_document_id], ensure_ascii=False),
            "claims_json": json.dumps(claim_info, sort_keys=True, ensure_ascii=False),
            "region_count": region_info["region_count"],
            "region_ids_json": json.dumps(region_info["region_ids"], sort_keys=True, ensure_ascii=False),
            "crop_image_paths_json": json.dumps(region_info["crop_image_paths"], sort_keys=True, ensure_ascii=False),
            "region_text_paths_json": json.dumps(region_info["region_text_paths"], sort_keys=True, ensure_ascii=False),
            "region_excerpt_json": json.dumps(region_info["region_excerpts"], sort_keys=True, ensure_ascii=False),
            "review_output_path": clean(action.get("artifact_path")) or clean(action.get("document_review_output_path")),
            "resolution_status": "pending",
            "resolved_boxscore_id": "",
            "resolved_target_table": "",
            "resolved_target_entity_key": "",
            "resolution_notes": "",
            "created_at_utc": created_at,
        })

    by_doc: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in action_rows:
        by_doc[clean(row.get("source_document_id"))].append(row)

    document_rows: list[dict[str, Any]] = []
    for source_document_id, rows in sorted(by_doc.items()):
        first = rows[0]
        followup_types = sorted({clean(row.get("followup_type")) for row in rows if clean(row.get("followup_type"))})
        resolution_passes = sorted({
            clean(row.get("recommended_resolution_pass"))
            for row in rows
            if clean(row.get("recommended_resolution_pass"))
        })
        packet_ids = sorted({clean(row.get("packet_id")) for row in rows if clean(row.get("packet_id"))})
        boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in rows if clean(row.get("boxscore_id"))})
        reasons = [clean(row.get("reason")) for row in rows if clean(row.get("reason"))]
        claims = claims_by_doc.get(source_document_id, [])
        claim_tables = sorted({clean(claim.get("target_table")) for claim in claims if clean(claim.get("target_table"))})
        document_rows.append({
            "semantic_followup_prep_run_id": prep_run_id,
            "action_queue_run_id": action_queue_run_id,
            "ingest_run_id": clean(first.get("ingest_run_id")),
            "source_document_id": source_document_id,
            "priority": min(parse_int(row.get("priority")) for row in rows),
            "followup_types_json": json.dumps(followup_types, sort_keys=True, ensure_ascii=False),
            "recommended_resolution_passes_json": json.dumps(resolution_passes, sort_keys=True, ensure_ascii=False),
            "action_count": len(rows),
            "packet_ids_json": json.dumps(packet_ids, sort_keys=True, ensure_ascii=False),
            "boxscore_ids_json": json.dumps(boxscore_ids, sort_keys=True, ensure_ascii=False),
            "reasons_json": json.dumps(reasons, sort_keys=True, ensure_ascii=False),
            "publication": clean(first.get("publication")),
            "issue_date": clean(first.get("issue_date")),
            "page": clean(first.get("page")),
            "source_url": clean(first.get("source_url")),
            "asset_pdf_path": clean(first.get("asset_pdf_path")),
            "asset_image_path": clean(first.get("asset_image_path")),
            "doc_disposition": clean(first.get("doc_disposition")),
            "document_next_action": clean(first.get("document_next_action")),
            "claim_count": clean(first.get("claim_count")),
            "claim_tables_json": json.dumps(claim_tables, sort_keys=True, ensure_ascii=False),
            "claims_json": clean(first.get("claims_json")),
            "region_count": clean(first.get("region_count")),
            "region_ids_json": clean(first.get("region_ids_json")),
            "crop_image_paths_json": clean(first.get("crop_image_paths_json")),
            "region_text_paths_json": clean(first.get("region_text_paths_json")),
            "region_excerpt_json": clean(first.get("region_excerpt_json")),
            "resolution_status": "pending",
            "resolution_notes": "",
            "created_at_utc": created_at,
        })
    return action_rows, document_rows


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table_name, fields in [
        ("llm_semantic_followup_prep_action", ACTION_FIELDS),
        ("llm_semantic_followup_prep_document", DOCUMENT_FIELDS),
    ]:
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.{table_name} ({defs})")
        existing = {
            row[0] for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review' AND table_name=?
                """,
                [table_name],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table_name} ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_semantic_followup_prep_run (
          semantic_followup_prep_run_id VARCHAR,
          action_queue_run_id VARCHAR,
          ingest_run_id VARCHAR,
          output_dir VARCHAR,
          action_count INTEGER,
          document_count INTEGER,
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
    prep_run_id: str,
    action_queue_run_id: str,
    ingest_run_id: str,
    out_dir: Path,
    action_rows: list[dict[str, Any]],
    document_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        for table in [
            "llm_semantic_followup_prep_action",
            "llm_semantic_followup_prep_document",
            "llm_semantic_followup_prep_run",
        ]:
            con.execute(
                f"DELETE FROM newspaper_review.{table} WHERE semantic_followup_prep_run_id = ?",
                [prep_run_id],
            )
        insert_rows(con, "llm_semantic_followup_prep_action", action_rows, ACTION_FIELDS)
        insert_rows(con, "llm_semantic_followup_prep_document", document_rows, DOCUMENT_FIELDS)
        insert_rows(con, "llm_semantic_followup_prep_run", [{
            "semantic_followup_prep_run_id": prep_run_id,
            "action_queue_run_id": action_queue_run_id,
            "ingest_run_id": ingest_run_id,
            "output_dir": str(out_dir),
            "action_count": len(action_rows),
            "document_count": len(document_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], document_rows: list[dict[str, Any]], max_docs: int) -> None:
    lines = [
        "# Newspaper Semantic Follow-Up Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Prep run: `{summary['semantic_followup_prep_run_id']}`",
        f"Action queue run: `{summary['action_queue_run_id']}`",
        f"Ingest run: `{summary['ingest_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Semantic actions: `{summary['action_count']}`",
        f"- Source documents: `{summary['document_count']}`",
        f"- Follow-up types: `{summary['followup_type_counts']}`",
        f"- Recommended resolution passes: `{summary['recommended_resolution_pass_counts']}`",
        "",
        "## Resolution Contract",
        "",
        "For each row, resolve the intended target and keep the source evidence attached. Use `resolved_boxscore_id`, `resolved_target_table`, `resolved_target_entity_key`, and `resolution_notes` when this becomes a decision artifact.",
        "",
        "## Document Queue",
        "",
    ]
    lines.extend(markdown_table(document_rows[:max_docs], [
        "priority",
        "source_document_id",
        "boxscore_ids_json",
        "followup_types_json",
        "recommended_resolution_passes_json",
        "claim_tables_json",
        "publication",
        "page",
    ]))
    lines.extend(["", "## Source Evidence", ""])
    for row in document_rows[:max_docs]:
        lines.extend([
            f"### {row['source_document_id']}",
            "",
            f"- PDF: `{row['asset_pdf_path']}`",
            f"- Reasons: `{row['reasons_json']}`",
            f"- Claims: `{row['claims_json']}`",
            f"- Crops: `{row['crop_image_paths_json']}`",
            "",
        ])
        try:
            excerpts = json.loads(clean(row.get("region_excerpt_json")))
        except json.JSONDecodeError:
            excerpts = []
        for excerpt in excerpts:
            lines.extend([
                f"#### {excerpt.get('region_id', '')}",
                "",
                f"- Crop: `{excerpt.get('crop_image_path', '')}`",
                f"- Text path: `{excerpt.get('region_text_path', '')}`",
                "",
                "```text",
                clean(excerpt.get("text_excerpt")),
                "```",
                "",
            ])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--action-queue-run-id", default="")
    parser.add_argument("--label", default="newspaper_semantic_followup_prep")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--max-regions-per-doc", type=int, default=4)
    parser.add_argument("--max-region-chars", type=int, default=1800)
    parser.add_argument("--max-claims-per-doc", type=int, default=12)
    parser.add_argument("--max-claim-quote-chars", type=int, default=420)
    parser.add_argument("--markdown-doc-limit", type=int, default=40)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prep_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / prep_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        latest_action_queue_run_id, latest_ingest_run_id = latest_action_queue_run(con)
        action_queue_run_id = args.action_queue_run_id or latest_action_queue_run_id
        actions = load_actions(con, action_queue_run_id, args.limit)
        ingest_run_ids = sorted({clean(row.get("ingest_run_id")) for row in actions if clean(row.get("ingest_run_id"))})
        ingest_run_id = ingest_run_ids[0] if len(ingest_run_ids) == 1 else latest_ingest_run_id
        source_document_ids = sorted({clean(row.get("source_document_id")) for row in actions if clean(row.get("source_document_id"))})
        regions_by_doc = load_regions(con, source_document_ids)
        claims_by_doc = load_claims(con, ingest_run_id, source_document_ids, args.max_claims_per_doc)
    finally:
        con.close()

    action_rows, document_rows = build_rows(
        prep_run_id,
        action_queue_run_id,
        actions,
        regions_by_doc,
        claims_by_doc,
        args.max_regions_per_doc,
        args.max_region_chars,
        args.max_claim_quote_chars,
    )
    summary = {
        "created_at_utc": created_at,
        "semantic_followup_prep_run_id": prep_run_id,
        "action_queue_run_id": action_queue_run_id,
        "ingest_run_id": ingest_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "action_count": len(action_rows),
        "document_count": len(document_rows),
        "followup_type_counts": dict(Counter(row["followup_type"] for row in action_rows)),
        "recommended_resolution_pass_counts": dict(Counter(row["recommended_resolution_pass"] for row in action_rows)),
        "source_documents_without_regions": [
            row["source_document_id"] for row in document_rows if parse_int(row.get("region_count")) == 0
        ],
        "source_documents_without_claims": [
            row["source_document_id"] for row in document_rows if not parse_json_list(row.get("claims_json"))
        ],
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "semantic_followup_action_manifest.csv", action_rows, ACTION_FIELDS)
    write_csv(out_dir / "semantic_followup_document_manifest.csv", document_rows, DOCUMENT_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "semantic_followup_work_order.md", summary, document_rows, args.markdown_doc_limit)
    persist(args.db_path, prep_run_id, action_queue_run_id, ingest_run_id, out_dir, action_rows, document_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
