#!/usr/bin/env python
"""Prepare promotion-review handoffs from ready newspaper packages.

This station packages the local `ready_for_promotion_review` rows into an
auditable review bundle with proposed fields and source evidence. It does not
promote anything into live tables.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "promotion_review_preps"

PACKAGE_FIELDS = [
    "promotion_review_prep_run_id",
    "action_queue_run_id",
    "package_run_id",
    "promotion_package_id",
    "action_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "confidence_bar",
    "max_confidence_score",
    "avg_confidence_score",
    "evidence_document_count",
    "item_count",
    "promote_item_count",
    "review_item_count",
    "proposed_fields_json",
    "source_documents_json",
    "materialized_row_ids_json",
    "review_decision",
    "review_notes",
    "created_at_utc",
]

ITEM_FIELDS = [
    "promotion_review_prep_run_id",
    "promotion_package_id",
    "promotion_package_item_id",
    "target_table",
    "target_entity_key",
    "llm_materialized_row_id",
    "llm_review_claim_id",
    "boxscore_id",
    "source_document_id",
    "publication",
    "source_url",
    "asset_pdf_path",
    "region_id",
    "evidence_text",
    "confidence_score",
    "confidence_lane",
    "promotion_recommendation",
    "row_group_key",
    "row_fields_json",
    "llm_reason",
    "created_at_utc",
]

RUN_FIELDS = [
    "promotion_review_prep_run_id",
    "action_queue_run_id",
    "package_run_id",
    "output_dir",
    "package_count",
    "item_count",
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


def latest_value(con: duckdb.DuckDBPyConnection, table: str, id_field: str) -> str:
    row = con.execute(
        f"""
        SELECT {id_field}
        FROM newspaper_review.{table}
        ORDER BY created_at_utc DESC, {id_field} DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def load_ready_actions(con: duckdb.DuckDBPyConnection, action_queue_run_id: str, limit: int) -> list[dict[str, Any]]:
    sql = """
        SELECT *
        FROM newspaper_review.llm_conveyor_action_queue
        WHERE action_queue_run_id = ?
          AND action_type = 'promotion_review'
        ORDER BY priority, target_table, boxscore_id, target_entity_key
    """
    params: list[Any] = [action_queue_run_id]
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    return query_dicts(con, sql, params)


def load_packages(con: duckdb.DuckDBPyConnection, package_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not package_ids:
        return {}
    placeholders = ",".join(["?"] * len(package_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_promotion_package
        WHERE promotion_package_id IN ({placeholders})
        """,
        package_ids,
    )
    return {clean(row.get("promotion_package_id")): row for row in rows}


def load_items(con: duckdb.DuckDBPyConnection, package_ids: list[str]) -> list[dict[str, Any]]:
    if not package_ids:
        return []
    placeholders = ",".join(["?"] * len(package_ids))
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_promotion_package_item
        WHERE promotion_package_id IN ({placeholders})
        ORDER BY promotion_package_id, confidence_score DESC, source_document_id
        """,
        package_ids,
    )


def build_rows(
    prep_run_id: str,
    action_queue_run_id: str,
    package_run_id: str,
    actions: list[dict[str, Any]],
    packages_by_id: dict[str, dict[str, Any]],
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    created_at = iso_now()
    package_rows: list[dict[str, Any]] = []
    for action in actions:
        package_id = clean(action.get("promotion_package_id"))
        package = packages_by_id.get(package_id, {})
        package_rows.append({
            "promotion_review_prep_run_id": prep_run_id,
            "action_queue_run_id": action_queue_run_id,
            "package_run_id": package_run_id,
            "promotion_package_id": package_id,
            "action_id": clean(action.get("action_id")),
            "target_table": clean(action.get("target_table")) or clean(package.get("target_table")),
            "target_entity_key": clean(action.get("target_entity_key")) or clean(package.get("target_entity_key")),
            "boxscore_id": clean(action.get("boxscore_id")) or clean(package.get("boxscore_id")),
            "confidence_bar": clean(action.get("confidence_bar")) or clean(package.get("confidence_bar")),
            "max_confidence_score": clean(action.get("max_confidence_score")) or clean(package.get("max_confidence_score")),
            "avg_confidence_score": clean(package.get("avg_confidence_score")),
            "evidence_document_count": clean(action.get("evidence_document_count")) or clean(package.get("evidence_document_count")),
            "item_count": clean(action.get("item_count")) or clean(package.get("item_count")),
            "promote_item_count": clean(package.get("promote_item_count")),
            "review_item_count": clean(package.get("review_item_count")),
            "proposed_fields_json": clean(action.get("proposed_fields_json")) or clean(package.get("proposed_fields_json")),
            "source_documents_json": clean(action.get("source_documents_json")) or clean(package.get("source_documents_json")),
            "materialized_row_ids_json": clean(package.get("materialized_row_ids_json")),
            "review_decision": "pending",
            "review_notes": "",
            "created_at_utc": created_at,
        })

    item_rows: list[dict[str, Any]] = []
    for item in items:
        item_rows.append({
            "promotion_review_prep_run_id": prep_run_id,
            "promotion_package_id": clean(item.get("promotion_package_id")),
            "promotion_package_item_id": clean(item.get("promotion_package_item_id")),
            "target_table": clean(item.get("target_table")),
            "target_entity_key": clean(item.get("target_entity_key")),
            "llm_materialized_row_id": clean(item.get("llm_materialized_row_id")),
            "llm_review_claim_id": clean(item.get("llm_review_claim_id")),
            "boxscore_id": clean(item.get("boxscore_id")),
            "source_document_id": clean(item.get("source_document_id")),
            "publication": clean(item.get("publication")),
            "source_url": clean(item.get("source_url")),
            "asset_pdf_path": clean(item.get("asset_pdf_path")),
            "region_id": clean(item.get("region_id")),
            "evidence_text": clean(item.get("evidence_text")),
            "confidence_score": clean(item.get("confidence_score")),
            "confidence_lane": clean(item.get("confidence_lane")),
            "promotion_recommendation": clean(item.get("promotion_recommendation")),
            "row_group_key": clean(item.get("row_group_key")),
            "row_fields_json": clean(item.get("row_fields_json")),
            "llm_reason": clean(item.get("llm_reason")),
            "created_at_utc": created_at,
        })
    return package_rows, item_rows


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table_name, fields in [
        ("llm_promotion_review_prep_package", PACKAGE_FIELDS),
        ("llm_promotion_review_prep_item", ITEM_FIELDS),
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
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_promotion_review_prep_run (
          promotion_review_prep_run_id VARCHAR,
          action_queue_run_id VARCHAR,
          package_run_id VARCHAR,
          output_dir VARCHAR,
          package_count INTEGER,
          item_count INTEGER,
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
    package_run_id: str,
    out_dir: Path,
    package_rows: list[dict[str, Any]],
    item_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        for table in [
            "llm_promotion_review_prep_package",
            "llm_promotion_review_prep_item",
            "llm_promotion_review_prep_run",
        ]:
            con.execute(
                f"DELETE FROM newspaper_review.{table} WHERE promotion_review_prep_run_id = ?",
                [prep_run_id],
            )
        insert_rows(con, "llm_promotion_review_prep_package", package_rows, PACKAGE_FIELDS)
        insert_rows(con, "llm_promotion_review_prep_item", item_rows, ITEM_FIELDS)
        insert_rows(con, "llm_promotion_review_prep_run", [{
            "promotion_review_prep_run_id": prep_run_id,
            "action_queue_run_id": action_queue_run_id,
            "package_run_id": package_run_id,
            "output_dir": str(out_dir),
            "package_count": len(package_rows),
            "item_count": len(item_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], package_rows: list[dict[str, Any]], item_rows: list[dict[str, Any]]) -> None:
    items_by_package: dict[str, list[dict[str, Any]]] = {}
    for item in item_rows:
        items_by_package.setdefault(clean(item.get("promotion_package_id")), []).append(item)

    lines = [
        "# Newspaper Promotion Review Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Prep run: `{summary['promotion_review_prep_run_id']}`",
        f"Action queue run: `{summary['action_queue_run_id']}`",
        f"Package run: `{summary['package_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Promotion-ready packages: `{summary['package_count']}`",
        f"- Evidence items: `{summary['item_count']}`",
        f"- Target tables: `{summary['target_table_counts']}`",
        "",
        "## Package Queue",
        "",
    ]
    lines.extend(markdown_table(package_rows, [
        "target_table",
        "boxscore_id",
        "confidence_bar",
        "max_confidence_score",
        "evidence_document_count",
        "proposed_fields_json",
    ]))
    lines.extend(["", "## Evidence", ""])
    for package in package_rows:
        package_id = clean(package.get("promotion_package_id"))
        lines.extend([
            f"### {clean(package.get('target_table'))} / {clean(package.get('boxscore_id'))}",
            "",
            f"- Package: `{package_id}`",
            f"- Proposed: `{clean(package.get('proposed_fields_json'))}`",
            f"- Sources: `{clean(package.get('source_documents_json'))}`",
            "",
        ])
        evidence_rows = items_by_package.get(package_id, [])
        lines.extend(markdown_table(evidence_rows, [
            "source_document_id",
            "confidence_score",
            "confidence_lane",
            "promotion_recommendation",
            "evidence_text",
        ]))
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--action-queue-run-id", default="")
    parser.add_argument("--package-run-id", default="")
    parser.add_argument("--label", default="newspaper_promotion_review_prep")
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prep_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / prep_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        action_queue_run_id = args.action_queue_run_id or latest_value(con, "llm_conveyor_action_queue_run", "action_queue_run_id")
        package_run_id = args.package_run_id or latest_value(con, "llm_promotion_package_run", "package_run_id")
        actions = load_ready_actions(con, action_queue_run_id, args.limit)
        package_ids = [clean(row.get("promotion_package_id")) for row in actions if clean(row.get("promotion_package_id"))]
        packages = load_packages(con, package_ids)
        items = load_items(con, package_ids)
    finally:
        con.close()

    package_rows, item_rows = build_rows(prep_run_id, action_queue_run_id, package_run_id, actions, packages, items)
    summary = {
        "created_at_utc": created_at,
        "promotion_review_prep_run_id": prep_run_id,
        "action_queue_run_id": action_queue_run_id,
        "package_run_id": package_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "package_count": len(package_rows),
        "item_count": len(item_rows),
        "target_table_counts": dict(Counter(row["target_table"] for row in package_rows)),
        "boxscore_count": len({row["boxscore_id"] for row in package_rows if row["boxscore_id"]}),
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "promotion_review_package_manifest.csv", package_rows, PACKAGE_FIELDS)
    write_csv(out_dir / "promotion_review_item_manifest.csv", item_rows, ITEM_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "promotion_review_work_order.md", summary, package_rows, item_rows)
    persist(args.db_path, prep_run_id, action_queue_run_id, package_run_id, out_dir, package_rows, item_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
