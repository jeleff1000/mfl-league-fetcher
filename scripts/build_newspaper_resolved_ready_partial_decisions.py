#!/usr/bin/env python
"""Approve safe ready-review rows by partial stat or cross-atom support.

This station drains conservative parts of `ready_resolved_atom_review` that the
direct-source station intentionally holds:

- player box rows where the source supports a safer subset of fields,
- player box rows corroborated by an already-staged final scoring event, and
- same-date source rows whose text directly names a pass touchdown.

It writes ordinary decision overrides and keeps all work local to D-drive.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_ready_partial_decisions"
CLEAR_FIELD_SENTINEL = "__CLEAR_FIELD__"

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
    "ready_partial_decision_run_id",
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
    "original_proposed_fields_json",
    "patched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "ready_partial_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_target_counts_json",
    "approved_class_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

PASS_TD_TYPES = {"receiving_touchdown", "touchdown_pass", "touchdown_reception", "pass_touchdown"}


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


def norm_words(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(value).lower()).strip()


def last_name(value: Any) -> str:
    parts = [part for part in norm_words(value).split() if part]
    return parts[-1] if parts else ""


def evidence_blob(row: dict[str, Any], proposed: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            clean(row.get("evidence_text")),
            clean(proposed.get("play_text")),
            clean(proposed.get("source_row_text")),
        ]
        if part
    )


def source_date_matches(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    if len(boxscore_id) < 8:
        return False
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    for doc in docs:
        match = re.match(r"([^#:]+)", doc)
        if match and match.group(1)[:8] == boxscore_id[:8]:
            return True
    return False


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
          AND route_to_lane = 'ready_resolved_atom_review'
          AND target_table IN ('player_game_box_score', 'scoring_event')
        ORDER BY target_table, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def load_final_scoring(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    try:
        return query_dicts(
            con,
            """
            SELECT boxscore_id, scoring_event_id, event_type, scoring_player_raw, scoring_NFL_player_id,
                   receiver_raw, receiver_NFL_player_id, passer_raw, passer_NFL_player_id,
                   distance_yards, play_text, package_item_id
            FROM newspaper_final.scoring_event
            """
        )
    except Exception:
        return []


def matching_final_passing_events(proposed: dict[str, Any], final_scoring: list[dict[str, Any]]) -> list[dict[str, Any]]:
    boxscore_id = clean(proposed.get("boxscore_id"))
    player_id = clean(proposed.get("NFL_player_id"))
    if not boxscore_id or not player_id:
        return []
    matches = []
    for row in final_scoring:
        if clean(row.get("boxscore_id")) != boxscore_id:
            continue
        if clean(row.get("passer_NFL_player_id")) != player_id:
            continue
        if clean(row.get("event_type")).lower() in PASS_TD_TYPES:
            matches.append(row)
    return matches


def approve_partial_thorpe_box(row: dict[str, Any], proposed: dict[str, Any]) -> tuple[bool, str, dict[str, Any], dict[str, Any]]:
    evidence = norm_words(evidence_blob(row, proposed))
    if clean(proposed.get("player_raw")) != "Jim Thorpe":
        return False, "", {}, {}
    if clean(proposed.get("boxscore_id")) != "192211260bff":
        return False, "", {}, {}
    if not ("scored twice" in evidence and "passed to" in evidence):
        return False, "", {}, {}
    patched = dict(proposed)
    removed = {}
    if clean(patched.get("rushing_tds")):
        removed["rushing_tds"] = patched.get("rushing_tds")
    patched["rushing_tds"] = CLEAR_FIELD_SENTINEL
    patched["touchdowns"] = clean(patched.get("touchdowns")) or "2"
    patched["passing_tds"] = clean(patched.get("passing_tds")) or "1"
    patched["review_status"] = "partial_player_box_score_supported"
    return True, "source supports total touchdowns and passing TD; rushing split removed", patched, {"removed_fields": removed}


def approve_box_from_final_scoring(
    proposed: dict[str, Any],
    final_scoring: list[dict[str, Any]],
) -> tuple[bool, str, dict[str, Any]]:
    if clean(proposed.get("passing_tds")) != "1":
        return False, "", {}
    matches = matching_final_passing_events(proposed, final_scoring)
    if not matches:
        return False, "", {}
    patched = dict(proposed)
    patched["review_status"] = "cross_atom_final_scoring_event_supported"
    return True, "final local scoring event corroborates one passing touchdown", {
        "matching_scoring_events": [
            {
                "package_item_id": clean(row.get("package_item_id")),
                "scoring_event_id": clean(row.get("scoring_event_id")),
                "event_type": clean(row.get("event_type")),
                "play_text": clean(row.get("play_text")),
            }
            for row in matches
        ],
        "patched": patched,
    }


def approve_same_date_pass_td(row: dict[str, Any], proposed: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    event_type = clean(proposed.get("event_type")).lower()
    if event_type not in PASS_TD_TYPES:
        return False, "", {}
    if not source_date_matches(row, proposed):
        return False, "", {}
    evidence = norm_words(evidence_blob(row, proposed))
    passer_last = last_name(proposed.get("passer_raw"))
    receiver_last = last_name(proposed.get("receiver_raw") or proposed.get("scoring_player_raw"))
    if "pass" not in evidence:
        return False, "", {}
    if passer_last and passer_last not in evidence:
        return False, "", {}
    if receiver_last and receiver_last not in evidence:
        return False, "", {}
    patched = dict(proposed)
    patched["review_status"] = "same_date_direct_pass_touchdown_text"
    return True, "same-date source text directly names pass touchdown participants", {"patched": patched}


def classify(
    row: dict[str, Any],
    final_scoring: list[dict[str, Any]],
) -> tuple[bool, str, str, dict[str, Any], dict[str, Any]]:
    target = clean(row.get("target_table"))
    proposed = parse_obj(row.get("proposed_fields_json"))
    if target == "player_game_box_score":
        ok, reason, patched, details = approve_partial_thorpe_box(row, proposed)
        if ok:
            return True, "partial_player_box_score", reason, patched, details
        ok, reason, details = approve_box_from_final_scoring(proposed, final_scoring)
        if ok:
            return True, "cross_atom_player_box_score", reason, details["patched"], details
        return False, "held_ready_partial", "player box row needs more source context or safer field split", proposed, {}
    if target == "scoring_event":
        ok, reason, details = approve_same_date_pass_td(row, proposed)
        if ok:
            return True, "same_date_direct_pass_touchdown", reason, details["patched"], details
        return False, "held_ready_partial", "scoring row still needs direct source/visual context", proposed, {}
    return False, "held_ready_partial", "target table not handled", proposed, {}


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    final_scoring: list[dict[str, Any]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in rows:
        ok, approval_class, reason, patched, details = classify(row, final_scoring)
        patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
        target = clean(row.get("target_table"))
        audit_row = {
            "ready_partial_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "target_table": target,
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else clean(row.get("decision_value")),
            "approval_class": approval_class,
            "reason": reason,
            "support_details_json": json.dumps(details, sort_keys=True, ensure_ascii=True) if details else "",
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "original_proposed_fields_json": clean(row.get("proposed_fields_json")),
            "patched_proposed_fields_json": patched_json,
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok:
            approved.append({
                "package_item_id": clean(row.get("package_item_id")),
                "decision_status": "approved",
                "decision_value": "approved_for_local_newspaper_final",
                "route_to_lane": f"local_final_{target}",
                "target_table": target,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "proposed_fields_json": patched_json,
                "notes": f"{approval_class}: {reason}",
            })
        else:
            held.append(audit_row)
    return approved, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_ready_partial_decision_run (
          ready_partial_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          approved_count INTEGER,
          held_count INTEGER,
          approved_target_counts_json VARCHAR,
          approved_class_counts_json VARCHAR,
          hold_reason_counts_json VARCHAR,
          approved_csv VARCHAR,
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
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_ready_partial_decision_item (
          ready_partial_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          package_item_id VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          boxscore_id VARCHAR,
          route_to_lane VARCHAR,
          decision_status VARCHAR,
          decision_value VARCHAR,
          approval_class VARCHAR,
          reason VARCHAR,
          support_details_json VARCHAR,
          evidence_text VARCHAR,
          source_documents_json VARCHAR,
          original_proposed_fields_json VARCHAR,
          patched_proposed_fields_json VARCHAR,
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
        run_id = clean(run_row.get("ready_partial_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_ready_partial_decision_run WHERE ready_partial_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_ready_partial_decision_item WHERE ready_partial_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_ready_partial_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_ready_partial_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Ready Partial Decisions",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Approval classes: `{summary['approved_class_counts']}`",
        "",
        "## Rows",
        "",
        "| status | table | boxscore | class | package | reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in audit_rows:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("decision_status")),
                clean(row.get("target_table")),
                clean(row.get("boxscore_id")),
                clean(row.get("approval_class")),
                clean(row.get("package_item_id")),
                clean(row.get("reason")).replace("|", "\\|")[:180],
            ])
            + " |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Approved overrides: `{summary['approved_csv']}`",
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
    parser.add_argument("--label", default="resolved_ready_partial_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
        final_scoring = load_final_scoring(read_con)
    finally:
        read_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    approved, held, audit_rows = build_rows(rows, run_id, decision_run_id, final_scoring, created_at)
    approved_target_counts = Counter(row["target_table"] for row in approved)
    approved_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_ready_partial_rows.csv"
    audit_csv = out_dir / "ready_partial_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "ready_partial_decision_report.md"

    summary = {
        "ready_partial_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_target_counts": dict(approved_target_counts),
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

    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit_rows, AUDIT_FIELDS)
    write_json(summary_json, summary)
    report_md.write_text(render_markdown(summary, audit_rows), encoding="utf-8")

    if not args.dry_run:
        persist(
            args.db_path,
            {
                "ready_partial_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": len(approved),
                "held_count": len(held),
                "approved_target_counts_json": json.dumps(dict(approved_target_counts), sort_keys=True),
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
