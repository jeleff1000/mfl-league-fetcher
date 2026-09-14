#!/usr/bin/env python
"""Approve ready-review scoring rows resolved by source/article context.

This station is for rows where the event atom is structurally resolved, but
the evidence snippet uses period shorthand such as "the Indian star" or
"the big Packer tackle." It only approves package IDs with documented context
support and keeps all output local to the D-drive newspaper conveyor.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_source_context_decisions"

APPROVED_FIELDS = [
    "package_item_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "proposed_fields_json",
    "notes",
]

AUDIT_FIELDS = [
    "source_context_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "route_to_lane",
    "decision_status",
    "decision_value",
    "approval_class",
    "reason",
    "support_details_json",
    "evidence_text",
    "source_documents_json",
    "source_text_paths_json",
    "materialized_support_json",
    "original_proposed_fields_json",
    "patched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "source_context_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "closed_count",
    "held_count",
    "approved_target_counts_json",
    "closed_target_counts_json",
    "approved_class_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

# These package IDs are intentionally explicit. This lane is for source-context
# decisions after a human/LLM review has identified the resolving context.
CONTEXT_APPROVALS = {
    "d9a9395c7f23f1fb170efedae219a074": {
        "approval_class": "cross_source_named_player_context",
        "review_status": "source_context_cross_source_supported",
        "reason": "same-game materialized source spells out Thorpe's eighty-yard run",
        "materialized_required_any": [
            ["thorpe", "eighty"],
            ["thorpe", "80"],
        ],
    },
    "3ce98121c1c97913ccd1a4c40c4345b4": {
        "approval_class": "article_context_previous_sentence_supported",
        "review_status": "source_context_previous_sentence_supported",
        "reason": "article context names Strong immediately before the extra-point shorthand",
        "source_required_all": ["strong", "extra", "point"],
    },
    "867c1764b5ba2b9e790f140138694f96": {
        "approval_class": "visual_article_context_alias_supported",
        "review_status": "source_context_visual_alias_supported",
        "reason": "article/crop context ties the big Packer tackle alias to Schwammel's place-kick sequence",
        "source_required_all": ["big", "packer", "tackle", "40", "place", "kick"],
        "manual_visual_context_confirmed": True,
    },
    "08d08dce03d98975b2e5973d985a6e39": {
        "approval_class": "article_context_pass_play_continuity",
        "review_status": "source_context_pass_play_supported",
        "reason": "article context links Herber-to-Hutson pass text to the 44-yard scoring completion",
        "source_required_all": ["herber", "hutson", "gain", "44"],
    },
    "88a480722c78c44c60c8501b16cc879a": {
        "approval_class": "article_context_goal_line_stand_pbp",
        "review_status": "source_context_goal_line_stand_supported",
        "reason": "article context corroborates an already-final Tigers turnover-on-downs stop inches from the Staley goal",
        "decision_status": "closed",
        "decision_value": "corroboration_archive",
        "route_to_lane": "archive",
        "source_required_all": ["tigers", "held", "downs", "six", "inches", "staley", "one", "yard"],
        "field_patches": {
            "possession_team_raw": "Chicago Tigers",
            "possession_team": "CHT",
            "distance_raw": "six inches",
            "yardline_raw": "Staley one yard line",
            "play_type": "turnover_on_downs",
        },
    },
}


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(value).lower()).strip()


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
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")


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


def load_rows(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.resolved_package_decision_route_queue
        WHERE resolved_package_decision_run_id = ?
          AND (
            (route_to_lane = 'ready_resolved_atom_review' AND target_table = 'scoring_event')
            OR (route_to_lane = 'context_review' AND target_table = 'play_by_play_event')
          )
        ORDER BY boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def load_materialized_scoring(con: duckdb.DuckDBPyConnection, ids: list[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    placeholders = ", ".join("?" for _ in ids)
    return query_dicts(
        con,
        f"""
        SELECT llm_materialized_row_id, source_document_id, boxscore_id,
               scoring_player_raw, scoring_NFL_player_id, event_type,
               distance_yards, play_text
        FROM newspaper_review.llm_scoring_event
        WHERE llm_materialized_row_id IN ({placeholders})
        ORDER BY source_document_id, llm_materialized_row_id
        """,
        ids,
    )


def load_source_text_paths(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> list[str]:
    if not source_document_ids:
        return []
    placeholders = ", ".join("?" for _ in source_document_ids)
    paths = []
    for row in query_dicts(
        con,
        f"""
        SELECT region_text_path AS path
        FROM newspaper_review.source_region
        WHERE source_document_id IN ({placeholders})
          AND region_text_path IS NOT NULL
        UNION
        SELECT ocr_text_path AS path
        FROM newspaper_review.source_text_pass
        WHERE source_document_id IN ({placeholders})
          AND ocr_text_path IS NOT NULL
        """,
        [*source_document_ids, *source_document_ids],
    ):
        path = clean(row.get("path"))
        if path and path not in paths:
            paths.append(path)
    return paths


def read_text_paths(paths: list[str]) -> str:
    chunks = []
    for path_text in paths:
        path = Path(path_text)
        if not path.exists() or not path.is_file():
            continue
        try:
            chunks.append(path.read_text(encoding="utf-8", errors="ignore"))
        except OSError:
            continue
    return "\n".join(chunks)


def any_term_group_present(blob: str, term_groups: list[list[str]]) -> bool:
    blob_norm = norm(blob)
    for terms in term_groups:
        if all(norm(term) in blob_norm for term in terms):
            return True
    return False


def all_terms_present(blob: str, terms: list[str]) -> bool:
    blob_norm = norm(blob)
    return all(norm(term) in blob_norm for term in terms)


def build_decision(
    row: dict[str, Any],
    proposed: dict[str, Any],
    spec: dict[str, Any],
    materialized_rows: list[dict[str, Any]],
    source_text_paths: list[str],
    source_blob: str,
) -> tuple[bool, str, dict[str, Any]]:
    details = {
        "configured_reason": spec["reason"],
        "manual_visual_context_confirmed": bool(spec.get("manual_visual_context_confirmed")),
        "materialized_row_count": len(materialized_rows),
        "source_text_path_count": len(source_text_paths),
    }
    materialized_blob = " ".join(clean(item.get("play_text")) for item in materialized_rows)
    if spec.get("materialized_required_any"):
        ok = any_term_group_present(materialized_blob, spec["materialized_required_any"])
        details["materialized_required_any"] = spec["materialized_required_any"]
        details["materialized_check_passed"] = ok
        if not ok:
            return False, "required materialized source context not found", details
    if spec.get("source_required_all"):
        ok = all_terms_present(source_blob, spec["source_required_all"])
        details["source_required_all"] = spec["source_required_all"]
        details["source_check_passed"] = ok
        if not ok and not spec.get("manual_visual_context_confirmed"):
            return False, "required source text context not found", details
    patched = dict(proposed)
    patched.update(spec.get("field_patches", {}))
    patched["review_status"] = spec["review_status"]
    details["patched"] = patched
    return True, spec["reason"], details


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    materialized_by_id: dict[str, dict[str, Any]],
    source_text_by_docs: dict[tuple[str, ...], tuple[list[str], str]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    override_rows = []
    held = []
    audit_rows = []
    for row in rows:
        package_item_id = clean(row.get("package_item_id"))
        proposed = parse_obj(row.get("proposed_fields_json"))
        original_json = json.dumps(proposed, sort_keys=True, ensure_ascii=True)
        source_ids = parse_list(proposed.get("source_materialized_row_ids_json"))
        materialized_rows = [materialized_by_id[item] for item in source_ids if item in materialized_by_id]
        source_docs = parse_list(row.get("source_documents_json")) or parse_list(proposed.get("source_documents_json"))
        doc_key = tuple(sorted(source_docs))
        source_text_paths, source_blob = source_text_by_docs.get(doc_key, ([], ""))
        spec = CONTEXT_APPROVALS.get(package_item_id)

        if spec:
            ok, reason, details = build_decision(row, proposed, spec, materialized_rows, source_text_paths, source_blob)
            approval_class = clean(spec.get("approval_class"))
        else:
            ok = False
            reason = "no source-context approval rule for routed row"
            details = {}
            approval_class = "held_source_context"

        patched = details.get("patched") if ok else proposed
        patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
        audit = {
            "source_context_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "decision_status": clean(spec.get("decision_status")) if ok and spec else "held",
            "decision_value": clean(spec.get("decision_value")) if ok and spec else "needs_review",
            "approval_class": approval_class if ok else "held_source_context",
            "reason": reason,
            "support_details_json": json.dumps(details, sort_keys=True, ensure_ascii=True),
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "source_text_paths_json": json.dumps(source_text_paths, sort_keys=True, ensure_ascii=True),
            "materialized_support_json": json.dumps(materialized_rows, sort_keys=True, ensure_ascii=True),
            "original_proposed_fields_json": original_json,
            "patched_proposed_fields_json": patched_json,
            "created_at_utc": created_at,
        }
        if ok and not audit["decision_status"]:
            audit["decision_status"] = "approved"
        if ok and not audit["decision_value"]:
            audit["decision_value"] = "approved_for_local_newspaper_final"
        audit_rows.append(audit)
        if ok:
            override_rows.append(
                {
                    "package_item_id": package_item_id,
                    "decision_status": audit["decision_status"],
                    "decision_value": audit["decision_value"],
                    "route_to_lane": clean(spec.get("route_to_lane")) or ("local_promotion" if audit["decision_status"] == "approved" else "archive"),
                    "target_table": clean(row.get("target_table")),
                    "target_entity_key": clean(row.get("target_entity_key")),
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "proposed_fields_json": patched_json,
                    "notes": f"{approval_class}: {reason}",
                }
            )
        else:
            held.append(audit)
    return override_rows, held, audit_rows


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        "CREATE TABLE IF NOT EXISTS newspaper_review.resolved_source_context_decision_run ("
        + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
        + ")"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS newspaper_review.resolved_source_context_decision_item ("
        + ", ".join(f"{field} VARCHAR" for field in AUDIT_FIELDS)
        + ")"
    )
    for table, fields in [
        ("resolved_source_context_decision_run", RUN_FIELDS),
        ("resolved_source_context_decision_item", AUDIT_FIELDS),
    ]:
        existing = {
            row[1]
            for row in con.execute(f"PRAGMA table_info('newspaper_review.{table}')").fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(db_path: Path, run_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.resolved_source_context_decision_run WHERE source_context_decision_run_id = ?",
            [run_row["source_context_decision_run_id"]],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_source_context_decision_item WHERE source_context_decision_run_id = ?",
            [run_row["source_context_decision_run_id"]],
        )
        insert_rows(con, "resolved_source_context_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_source_context_decision_item", item_rows, AUDIT_FIELDS)
    finally:
        con.close()


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


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Source-Context Decisions",
        "",
        f"- Run: `{summary['source_context_decision_run_id']}`",
        f"- Decision ledger: `{summary['resolved_package_decision_run_id']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Persisted to DuckDB: `{summary['persisted_to_duckdb']}`",
        "",
        "## Decisions",
        "",
    ]
    lines.extend(
        markdown_table(
            audit_rows,
            [
                "decision_status",
                "approval_class",
                "package_item_id",
                "boxscore_id",
                "reason",
                "evidence_text",
            ],
        )
    )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="1920_1939_source_context_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
        all_source_ids = sorted(
            {
                item
                for row in rows
                for item in parse_list(parse_obj(row.get("proposed_fields_json")).get("source_materialized_row_ids_json"))
            }
        )
        materialized = load_materialized_scoring(read_con, all_source_ids)
        materialized_by_id = {clean(row.get("llm_materialized_row_id")): row for row in materialized}
        doc_keys = {
            tuple(sorted(parse_list(row.get("source_documents_json")) or parse_list(parse_obj(row.get("proposed_fields_json")).get("source_documents_json"))))
            for row in rows
        }
        source_text_by_docs = {}
        for doc_key in doc_keys:
            paths = load_source_text_paths(read_con, list(doc_key))
            source_text_by_docs[doc_key] = (paths, read_text_paths(paths))
    finally:
        read_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    override_rows, held, audit_rows = build_rows(
        rows,
        run_id,
        decision_run_id,
        materialized_by_id,
        source_text_by_docs,
        created_at,
    )
    approved_target_counts = Counter(row["target_table"] for row in audit_rows if row["decision_status"] == "approved")
    closed_target_counts = Counter(row["target_table"] for row in audit_rows if row["decision_status"] == "closed")
    approved_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_source_context_rows.csv"
    audit_csv = out_dir / "source_context_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "source_context_decision_report.md"

    summary = {
        "source_context_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": sum(1 for row in audit_rows if row["decision_status"] == "approved"),
        "closed_count": sum(1 for row in audit_rows if row["decision_status"] == "closed"),
        "held_count": len(held),
        "approved_target_counts": dict(approved_target_counts),
        "closed_target_counts": dict(closed_target_counts),
        "approved_class_counts": dict(approved_class_counts),
        "hold_reason_counts": dict(hold_reason_counts),
        "approved_csv": str(approved_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }

    write_csv(approved_csv, override_rows, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit_rows, AUDIT_FIELDS)
    write_json(summary_json, summary)
    report_md.write_text(render_markdown(summary, audit_rows), encoding="utf-8")

    if not args.dry_run:
        persist(
            args.db_path,
            {
                "source_context_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": sum(1 for row in audit_rows if row["decision_status"] == "approved"),
                "closed_count": sum(1 for row in audit_rows if row["decision_status"] == "closed"),
                "held_count": len(held),
                "approved_target_counts_json": json.dumps(dict(approved_target_counts), sort_keys=True),
                "closed_target_counts_json": json.dumps(dict(closed_target_counts), sort_keys=True),
                "approved_class_counts_json": json.dumps(dict(approved_class_counts), sort_keys=True),
                "hold_reason_counts_json": json.dumps(dict(hold_reason_counts), sort_keys=True),
                "approved_csv": str(approved_csv),
                "held_csv": str(held_csv),
                "audit_csv": str(audit_csv),
                "summary_json": str(summary_json),
                "persisted_to_duckdb": True,
                "created_at_utc": created_at,
            },
            audit_rows,
        )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
