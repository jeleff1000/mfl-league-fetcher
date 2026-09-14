#!/usr/bin/env python
"""
Apply approved resolved newspaper package decisions into local final tables.

This writes only D-drive artifacts and the local newspaper DuckDB:

- newspaper_final.<target_table> for approved rows
- newspaper_review.resolved_package_decision_apply_* audit tables

It never writes to Fly, v26, the live supertable, or production league tables.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_newspaper_review_decision_apply import ID_FIELDS, TARGET_FIELDS  # noqa: E402


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_package_decision_apply_runs"

APPROVE_VALUES = {
    "approve",
    "approved",
    "approved_for_local_promotion",
    "approved_for_local_newspaper_final",
}

CLEAR_FIELD_SENTINEL = "__CLEAR_FIELD__"

FINAL_PREFIX_FIELDS = [
    "resolved_package_apply_run_id",
    "resolved_package_decision_run_id",
    "resolved_promotion_package_run_id",
    "review_packet_run_id",
    "package_item_id",
    "target_entity_key",
]

FINAL_SUFFIX_FIELDS = [
    "source_documents_json",
    "evidence_text",
    "confidence_bar",
    "decision_value",
    "decision_status",
    "route_to_lane",
    "decision_source",
    "unmapped_fields_json",
    "created_at_utc",
]

APPLIED_FIELDS = [
    "resolved_package_apply_run_id",
    "resolved_package_decision_run_id",
    "resolved_promotion_package_run_id",
    "review_packet_run_id",
    "package_item_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "decision_value",
    "decision_status",
    "local_final_table",
    "local_final_row_id",
    "created_at_utc",
]

RUN_FIELDS = [
    "resolved_package_apply_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "decision_count",
    "applied_row_count",
    "target_table_counts_json",
    "applied_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def parse_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_list(value: Any) -> list[str]:
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
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def markdown_escape(value: Any) -> str:
    return clean(value).replace("|", "\\|").replace("\n", " ")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    if not rows:
        return ["_(none)_"]
    lines = [
        "| " + " | ".join(fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(markdown_escape(row.get(field, "")) for field in fields) + " |")
    return lines


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_decision_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT resolved_package_decision_run_id
        FROM newspaper_review.resolved_package_decision_ledger_run
        ORDER BY created_at_utc DESC, resolved_package_decision_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_approved_decisions(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in APPROVE_VALUES)
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.resolved_package_decision_ledger
        WHERE resolved_package_decision_run_id = ?
          AND LOWER(TRIM(decision_value)) IN ({placeholders})
        ORDER BY target_table, boxscore_id, target_entity_key, package_item_id
        """,
        [decision_run_id, *sorted(APPROVE_VALUES)],
    )


def final_fields(target_table: str) -> list[str]:
    fields = list(FINAL_PREFIX_FIELDS)
    for field in TARGET_FIELDS[target_table]:
        if field not in fields:
            fields.append(field)
    for field in FINAL_SUFFIX_FIELDS:
        if field not in fields:
            fields.append(field)
    return fields


def merge_payload(decision: dict[str, Any]) -> dict[str, Any]:
    payload = parse_obj(decision.get("full_row_json"))
    proposed = parse_obj(decision.get("proposed_fields_json"))
    for key, value in proposed.items():
        if clean(value) == CLEAR_FIELD_SENTINEL:
            payload[key] = ""
        elif clean(value):
            payload[key] = value
    return payload


def first_source_doc(source_documents_json: str) -> str:
    docs = parse_list(source_documents_json)
    return docs[0] if docs else ""


def final_row_from_decision(apply_run_id: str, decision: dict[str, Any], created_at: str) -> tuple[dict[str, Any], dict[str, Any]]:
    target_table = clean(decision.get("target_table"))
    if target_table not in TARGET_FIELDS:
        raise ValueError(f"Unsupported target table: {target_table}")
    fields = final_fields(target_table)
    payload = merge_payload(decision)
    target_fields = set(TARGET_FIELDS[target_table])
    mapped = {key: clean(value) for key, value in payload.items() if key in target_fields and clean(value)}
    unmapped = {key: value for key, value in payload.items() if key not in target_fields and clean(value)}

    row = {field: "" for field in fields}
    row.update({
        "resolved_package_apply_run_id": apply_run_id,
        "resolved_package_decision_run_id": clean(decision.get("resolved_package_decision_run_id")),
        "resolved_promotion_package_run_id": clean(decision.get("resolved_promotion_package_run_id")),
        "review_packet_run_id": clean(decision.get("review_packet_run_id")),
        "package_item_id": clean(decision.get("package_item_id")),
        "target_entity_key": clean(decision.get("target_entity_key")),
        "source_documents_json": clean(decision.get("source_documents_json")),
        "evidence_text": clean(decision.get("evidence_text")),
        "confidence_bar": clean(decision.get("confidence_bar")),
        "decision_value": clean(decision.get("decision_value")),
        "decision_status": clean(decision.get("decision_status")),
        "route_to_lane": clean(decision.get("route_to_lane")),
        "decision_source": clean(decision.get("decision_source")),
        "unmapped_fields_json": json.dumps(unmapped, sort_keys=True, ensure_ascii=True) if unmapped else "",
        "created_at_utc": created_at,
    })
    for field, value in mapped.items():
        row[field] = value

    if "boxscore_id" in row and not clean(row.get("boxscore_id")):
        row["boxscore_id"] = clean(decision.get("boxscore_id"))
    if "player_week" in row and not clean(row.get("player_week")):
        row["player_week"] = clean(decision.get("player_week"))
    if "NFL_player_id" in row and not clean(row.get("NFL_player_id")):
        row["NFL_player_id"] = clean(decision.get("NFL_player_id"))
    if "source_document_id" in row and not clean(row.get("source_document_id")):
        row["source_document_id"] = first_source_doc(clean(decision.get("source_documents_json")))
    if "confidence_score" in row and not clean(row.get("confidence_score")):
        row["confidence_score"] = clean(payload.get("confidence_score") or payload.get("max_confidence_score"))

    id_field = ID_FIELDS[target_table]
    if id_field in row and not clean(row.get(id_field)):
        row[id_field] = stable_id("newspaper_final", target_table, decision.get("package_item_id"))

    applied = {
        "resolved_package_apply_run_id": apply_run_id,
        "resolved_package_decision_run_id": clean(decision.get("resolved_package_decision_run_id")),
        "resolved_promotion_package_run_id": clean(decision.get("resolved_promotion_package_run_id")),
        "review_packet_run_id": clean(decision.get("review_packet_run_id")),
        "package_item_id": clean(decision.get("package_item_id")),
        "target_table": target_table,
        "target_entity_key": clean(row.get("target_entity_key")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "player_week": clean(row.get("player_week")),
        "NFL_player_id": clean(row.get("NFL_player_id")),
        "decision_value": clean(decision.get("decision_value")),
        "decision_status": clean(decision.get("decision_status")),
        "local_final_table": f"newspaper_final.{target_table}",
        "local_final_row_id": clean(row.get(id_field)),
        "created_at_utc": created_at,
    }
    return row, applied


def build_outputs(apply_run_id: str, decisions: list[dict[str, Any]], created_at: str) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    final_by_table: dict[str, list[dict[str, Any]]] = {table: [] for table in TARGET_FIELDS}
    applied: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for decision in decisions:
        target_table = clean(decision.get("target_table"))
        if target_table not in TARGET_FIELDS:
            continue
        row, applied_row = final_row_from_decision(apply_run_id, decision, created_at)
        id_field = ID_FIELDS[target_table]
        dedupe_key = (target_table, clean(row.get(id_field)) or clean(row.get("target_entity_key")))
        if dedupe_key[1] and dedupe_key in seen:
            continue
        if dedupe_key[1]:
            seen.add(dedupe_key)
        final_by_table[target_table].append(row)
        applied.append(applied_row)
    return final_by_table, applied


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_final")
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table in TARGET_FIELDS:
        fields = final_fields(table)
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_final.{table} ({defs})")
        existing = {
            row[0]
            for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_final' AND table_name=?
                """,
                [table],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_final.{table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")

    for table, fields in [
        ("resolved_package_decision_apply_run", RUN_FIELDS),
        ("resolved_package_applied_final_row", APPLIED_FIELDS),
    ]:
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.{table} ({defs})")
        existing = {
            row[0]
            for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review' AND table_name=?
                """,
                [table],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def delete_package_items(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]]) -> None:
    package_ids = sorted({clean(row.get("package_item_id")) for row in rows if clean(row.get("package_item_id"))})
    if not package_ids:
        return
    placeholders = ", ".join("?" for _ in package_ids)
    con.execute(f"DELETE FROM newspaper_final.{table} WHERE package_item_id IN ({placeholders})", package_ids)


def persist(
    db_path: Path,
    run_row: dict[str, Any],
    final_by_table: dict[str, list[dict[str, Any]]],
    applied_rows: list[dict[str, Any]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        apply_run_id = clean(run_row.get("resolved_package_apply_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_package_decision_apply_run WHERE resolved_package_apply_run_id = ?",
            [apply_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_package_applied_final_row WHERE resolved_package_apply_run_id = ?",
            [apply_run_id],
        )
        for table, rows in final_by_table.items():
            con.execute(f"DELETE FROM newspaper_final.{table} WHERE resolved_package_apply_run_id = ?", [apply_run_id])
            delete_package_items(con, table, rows)
            insert_rows(con, f"newspaper_final.{table}", rows, final_fields(table))
        insert_rows(con, "newspaper_review.resolved_package_applied_final_row", applied_rows, APPLIED_FIELDS)
        insert_rows(con, "newspaper_review.resolved_package_decision_apply_run", [run_row], RUN_FIELDS)
    finally:
        con.close()


def write_report(path: Path, summary: dict[str, Any], applied_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Resolved Package Decision Apply",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Apply run: `{summary['resolved_package_apply_run_id']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Applied local final rows: `{summary['applied_row_count']}`",
        f"- Target table counts: `{summary['target_table_counts']}`",
        "",
        "## Applied Rows",
        "",
    ]
    lines.extend(markdown_table(applied_rows[:120], [
        "target_table",
        "boxscore_id",
        "player_week",
        "NFL_player_id",
        "local_final_row_id",
        "decision_value",
    ]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_package_decision_apply")
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    apply_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / apply_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = args.resolved_package_decision_run_id or latest_decision_run(con)
        decisions = load_approved_decisions(con, decision_run_id)
    finally:
        con.close()
    if not decision_run_id:
        raise SystemExit("No resolved package decision run found")

    final_by_table, applied_rows = build_outputs(apply_run_id, decisions, created_at)

    for table, rows in final_by_table.items():
        if rows:
            write_csv(out_dir / f"local_final_{table}.csv", rows, final_fields(table))
    applied_csv = out_dir / "applied_final_rows.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "resolved_package_decision_apply_report.md"
    write_csv(applied_csv, applied_rows, APPLIED_FIELDS)

    table_counts = {table: len(rows) for table, rows in final_by_table.items() if rows}
    summary = {
        "resolved_package_apply_run_id": apply_run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "decision_count": len(decisions),
        "applied_row_count": len(applied_rows),
        "target_table_counts": table_counts,
        "applied_csv": str(applied_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }
    write_json(summary_json, summary)
    write_report(report_md, summary, applied_rows)

    run_row = {
        "resolved_package_apply_run_id": apply_run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "decision_count": str(len(decisions)),
        "applied_row_count": str(len(applied_rows)),
        "target_table_counts_json": json.dumps(table_counts, sort_keys=True),
        "applied_csv": str(applied_csv),
        "summary_json": str(summary_json),
        "persisted_to_duckdb": "false" if args.dry_run else "true",
        "created_at_utc": created_at,
    }
    if not args.dry_run:
        persist(args.db_path, run_row, final_by_table, applied_rows)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
