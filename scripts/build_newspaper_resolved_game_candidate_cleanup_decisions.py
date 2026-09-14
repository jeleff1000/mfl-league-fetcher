#!/usr/bin/env python
"""Close duplicate or false-positive resolved newspaper game candidates.

This station handles the game/score mapping lane after the safe PFR-match
station has already approved exact local matches. It does not promote new stats.
It only closes rows when local evidence proves the row is either:

- a duplicate/corroborating extraction of another still-active game candidate,
- an inverted duplicate of an already-staged local final game candidate, or
- an OCR/source-context false positive from a non-NFL/local/college/high-school
  snippet.

Everything that still looks like a possible non-PFR game, schedule discrepancy,
or real score conflict remains routed.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_game_candidate_cleanup_decisions"

OVERRIDE_FIELDS = [
    "package_item_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "notes",
]

AUDIT_FIELDS = [
    "game_candidate_cleanup_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "decision_status",
    "decision_value",
    "cleanup_class",
    "reason",
    "canonical_package_item_id",
    "candidate_key_json",
    "evidence_summary_json",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "game_candidate_cleanup_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "closed_count",
    "held_count",
    "cleanup_class_counts_json",
    "hold_reason_counts_json",
    "overrides_csv",
    "closed_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

# These rows were inspected against their source materialized evidence. The
# reasons intentionally name the unrelated source context so the closure is
# auditable and reversible if better OCR later contradicts it.
FALSE_POSITIVE_PACKAGE_REASONS = {
    "237c2cf2d385924326dda2fd7aab4cc3": "source evidence is college/opening-season roundup text, not Akron-Frankford 13-0",
    "b047e1074e48094a281e395893cc4bfc": "source evidence is baseball/college roundup text, not Akron-Frankford 3-1",
    "d0bb33e11c6aebec11d427e900e7c692": "source evidence supports Penn/F&M college 41-0 text, not Akron-Frankford",
    "4552b119203bd99f34db8e2daaa5b02e": "source evidence names Harrisburg/local-game 20-18 context, not Frankford-Cleveland",
    "0ec174c116097f7e43c44cd8d9dc18e3": "source score 25-7 comes from Pottsville High/Mahanoy local-school text, not the Frankford-Pottsville NFL game",
    "ebb29e80439f67010e5dfdb94b8b4ed8": "source evidence names Kulpmont/local text, not Rochester-Pottsville",
    "1991c16541428595a036581fffcc2197": "source evidence says Poly Prep/Peddie 6-0, not Brooklyn-Frankford",
    "36f1ff277d7378a2a5dc7bb819cc923a": "source evidence says Evander Childs/Flushing/Richmond Hill 12-7, not Brooklyn-Frankford",
    "03dce1ce6416f0e68d243f88e192ae17": "source evidence says Kewaunee High/Algoma 36-6, not Chicago-Portsmouth",
    "2eb6c2d0f84f3cc57f8b659173852f9a": "source evidence says Shawano/Antigo basketball/cagers 23-20, not Cardinals-Bears",
}

INVERTED_FINAL_DUPLICATES = {
    "1a968a8fe6aa56867d8cf07745e1ad12": {
        "canonical_package_item_id": "3563911ce7a4779c381ed7ce51e2dd46",
        "reason": "source evidence supports Giants over Frankford 31-0; canonical NYG-FRN row is already staged locally",
    }
}


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def parse_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_json_list(value: Any) -> list[str]:
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
          AND target_table = 'game_candidate'
        ORDER BY route_to_lane, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def source_materialized_ids(proposed: dict[str, Any]) -> list[str]:
    return parse_json_list(proposed.get("source_materialized_row_ids_json"))


def load_game_evidence(
    con: duckdb.DuckDBPyConnection,
    materialized_ids: list[str],
) -> dict[str, dict[str, Any]]:
    ids = sorted({clean(item) for item in materialized_ids if clean(item)})
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = query_dicts(
        con,
        f"""
        SELECT
          llm_materialized_row_id,
          source_document_id,
          publication,
          page,
          confidence_score,
          evidence_text,
          field_evidence_json
        FROM newspaper_review.llm_game_candidate
        WHERE llm_materialized_row_id IN ({placeholders})
        """,
        ids,
    )
    return {clean(row.get("llm_materialized_row_id")): row for row in rows}


def candidate_key(row: dict[str, Any]) -> dict[str, str]:
    proposed = parse_obj(row.get("proposed_fields_json"))
    return {
        "boxscore_id": clean(row.get("boxscore_id")) or clean(proposed.get("boxscore_id")),
        "team_1": clean(proposed.get("team_1_resolved")) or clean(proposed.get("team_1_raw")),
        "team_1_score": clean(proposed.get("team_1_score")),
        "team_2": clean(proposed.get("team_2_resolved")) or clean(proposed.get("team_2_raw")),
        "team_2_score": clean(proposed.get("team_2_score")),
        "source_materialized_ids": "|".join(source_materialized_ids(proposed)),
    }


def duplicate_group_key(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    key = candidate_key(row)
    return (
        key["boxscore_id"],
        key["team_1"],
        key["team_1_score"],
        key["team_2"],
        key["team_2_score"],
        key["source_materialized_ids"],
    )


def canonical_score(row: dict[str, Any]) -> tuple[int, int, str]:
    target_key = clean(row.get("target_entity_key"))
    score = 10 if "_game_score_" in target_key else 0
    score += 5 if "game_score" in target_key else 0
    return (score, len(target_key), clean(row.get("package_item_id")))


def duplicate_closure_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    groups: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = duplicate_group_key(row)
        if all(key):
            groups.setdefault(key, []).append(row)
    output: dict[str, str] = {}
    for group_rows in groups.values():
        if len(group_rows) < 2:
            continue
        canonical = sorted(group_rows, key=canonical_score, reverse=True)[0]
        canonical_id = clean(canonical.get("package_item_id"))
        for row in group_rows:
            package_item_id = clean(row.get("package_item_id"))
            if package_item_id and package_item_id != canonical_id:
                output[package_item_id] = canonical_id
    return output


def evidence_summary(proposed: dict[str, Any], evidence_by_id: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for materialized_id in source_materialized_ids(proposed):
        row = evidence_by_id.get(materialized_id, {})
        out.append({
            "llm_materialized_row_id": materialized_id,
            "source_document_id": clean(row.get("source_document_id")),
            "publication": clean(row.get("publication")),
            "page": clean(row.get("page")),
            "confidence_score": clean(row.get("confidence_score")),
            "evidence_text": clean(row.get("evidence_text"))[:500],
        })
    return out


def classify(row: dict[str, Any], duplicate_map: dict[str, str]) -> tuple[str, str, str, str]:
    package_item_id = clean(row.get("package_item_id"))
    if package_item_id in INVERTED_FINAL_DUPLICATES:
        info = INVERTED_FINAL_DUPLICATES[package_item_id]
        return "closed", "corroboration_archive", "inverted_duplicate_of_final", clean(info["reason"])
    if package_item_id in duplicate_map:
        return (
            "closed",
            "corroboration_archive",
            "duplicate_game_candidate_source",
            f"duplicate candidate/source extraction; canonical package {duplicate_map[package_item_id]} remains active",
        )
    if package_item_id in FALSE_POSITIVE_PACKAGE_REASONS:
        return "closed", "reject", "source_context_false_positive", FALSE_POSITIVE_PACKAGE_REASONS[package_item_id]
    return "held", clean(row.get("decision_value")) or "pending_review", "still_requires_game_mapping_review", clean(row.get("reason"))


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    evidence_by_id: dict[str, dict[str, Any]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    duplicate_map = duplicate_closure_map(rows)
    closed: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []

    for row in rows:
        proposed = parse_obj(row.get("proposed_fields_json"))
        package_item_id = clean(row.get("package_item_id"))
        status, value, cleanup_class, reason = classify(row, duplicate_map)
        canonical_id = ""
        if cleanup_class == "duplicate_game_candidate_source":
            canonical_id = duplicate_map.get(package_item_id, "")
        elif cleanup_class == "inverted_duplicate_of_final":
            canonical_id = clean(INVERTED_FINAL_DUPLICATES[package_item_id]["canonical_package_item_id"])

        audit_row = {
            "game_candidate_cleanup_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "decision_status": status,
            "decision_value": value,
            "cleanup_class": cleanup_class,
            "reason": reason,
            "canonical_package_item_id": canonical_id,
            "candidate_key_json": json.dumps(candidate_key(row), sort_keys=True, ensure_ascii=True),
            "evidence_summary_json": json.dumps(evidence_summary(proposed, evidence_by_id), sort_keys=True, ensure_ascii=True),
            "source_documents_json": clean(row.get("source_documents_json")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if status == "closed":
            closed.append({
                "package_item_id": package_item_id,
                "decision_status": "closed",
                "decision_value": value,
                "route_to_lane": "archive_game_candidate_cleanup",
                "target_table": clean(row.get("target_table")),
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "notes": f"{cleanup_class}: {reason}",
            })
        else:
            held.append(audit_row)
    return closed, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_game_candidate_cleanup_decision_run (
          game_candidate_cleanup_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          closed_count INTEGER,
          held_count INTEGER,
          cleanup_class_counts_json VARCHAR,
          hold_reason_counts_json VARCHAR,
          overrides_csv VARCHAR,
          closed_csv VARCHAR,
          held_csv VARCHAR,
          audit_csv VARCHAR,
          summary_json VARCHAR,
          persisted_to_duckdb BOOLEAN,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_game_candidate_cleanup_decision_item (
          game_candidate_cleanup_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          package_item_id VARCHAR,
          route_to_lane VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          boxscore_id VARCHAR,
          decision_status VARCHAR,
          decision_value VARCHAR,
          cleanup_class VARCHAR,
          reason VARCHAR,
          canonical_package_item_id VARCHAR,
          candidate_key_json VARCHAR,
          evidence_summary_json VARCHAR,
          source_documents_json VARCHAR,
          proposed_fields_json VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[row.get(field) for field in fields] for row in rows],
    )


def persist(db_path: Path, run_row: dict[str, Any], audit_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("game_candidate_cleanup_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_game_candidate_cleanup_decision_run WHERE game_candidate_cleanup_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_game_candidate_cleanup_decision_item WHERE game_candidate_cleanup_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_game_candidate_cleanup_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_game_candidate_cleanup_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Game Candidate Cleanup Decisions",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Closed: `{summary['closed_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Cleanup classes: `{summary['cleanup_class_counts']}`",
        "",
        "## Rows",
        "",
        "| status | value | boxscore | class | package | reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in audit_rows:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("decision_status")),
                clean(row.get("decision_value")),
                clean(row.get("boxscore_id")),
                clean(row.get("cleanup_class")),
                clean(row.get("package_item_id")),
                clean(row.get("reason")).replace("|", "\\|")[:180],
            ])
            + " |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Ledger overrides: `{summary['overrides_csv']}`",
        f"- Closed rows: `{summary['closed_csv']}`",
        f"- Held rows: `{summary['held_csv']}`",
        f"- Audit CSV: `{summary['audit_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_game_candidate_cleanup_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
        ids: list[str] = []
        for row in rows:
            ids.extend(source_materialized_ids(parse_obj(row.get("proposed_fields_json"))))
        evidence_by_id = load_game_evidence(read_con, ids)
    finally:
        read_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    closed, held, audit_rows = build_rows(rows, run_id, decision_run_id, evidence_by_id, created_at)

    cleanup_class_counts = Counter(row["cleanup_class"] for row in audit_rows if row["decision_status"] == "closed")
    hold_reason_counts = Counter(row["reason"] for row in held)

    overrides_csv = out_dir / "approved_decision_overrides.csv"
    closed_csv = out_dir / "closed_game_candidate_rows.csv"
    held_csv = out_dir / "held_game_candidate_cleanup_rows.csv"
    audit_csv = out_dir / "game_candidate_cleanup_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "game_candidate_cleanup_decision_report.md"

    summary = {
        "game_candidate_cleanup_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "closed_count": len(closed),
        "held_count": len(held),
        "cleanup_class_counts": dict(cleanup_class_counts),
        "hold_reason_counts": dict(hold_reason_counts),
        "overrides_csv": str(overrides_csv),
        "closed_csv": str(closed_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }

    write_csv(overrides_csv, closed, OVERRIDE_FIELDS)
    write_csv(closed_csv, closed, OVERRIDE_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit_rows, AUDIT_FIELDS)
    write_json(summary_json, summary)
    report_md.write_text(render_markdown(summary, audit_rows), encoding="utf-8")

    if not args.dry_run:
        persist(
            args.db_path,
            {
                "game_candidate_cleanup_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "closed_count": len(closed),
                "held_count": len(held),
                "cleanup_class_counts_json": json.dumps(dict(cleanup_class_counts), sort_keys=True),
                "hold_reason_counts_json": json.dumps(dict(hold_reason_counts), sort_keys=True),
                "overrides_csv": str(overrides_csv),
                "closed_csv": str(closed_csv),
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
