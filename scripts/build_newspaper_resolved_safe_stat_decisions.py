#!/usr/bin/env python
"""
Generate conservative decision overrides for safe resolved newspaper stat rows.

This station consumes the latest resolved package decision route queue and emits
an override CSV for rows whose proposed player_game_box_score fields are directly
supported by the evidence text. It intentionally skips touchdown-type inference
rows for a later review pass.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_safe_stat_decisions"

APPROVED_FIELDS = [
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
    "safe_stat_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "confidence_bar",
    "stat_fields_json",
    "decision_status",
    "decision_value",
    "reason",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "safe_stat_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_fields_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

STAT_FIELDS = [
    "carries",
    "rushing_yards",
    "attempts",
    "completions",
    "passing_yards",
    "passing_interceptions",
    "receptions",
    "receiving_yards",
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "fg_long",
    "def_interceptions",
    "def_sacks",
]

TD_FIELDS = {
    "rushing_tds",
    "passing_tds",
    "receiving_tds",
    "def_tds",
    "special_teams_tds",
    "touchdowns",
}

NUMBER_WORDS = {
    "0": {"0", "zero", "none", "no"},
    "1": {"1", "one", "a", "an"},
    "2": {"2", "two", "twice", "both"},
    "3": {"3", "three", "thrice"},
    "4": {"4", "four"},
    "5": {"5", "five"},
    "6": {"6", "six"},
    "7": {"7", "seven"},
    "8": {"8", "eight"},
    "9": {"9", "nine"},
    "10": {"10", "ten"},
    "11": {"11", "eleven"},
    "12": {"12", "twelve"},
    "20": {"20", "twenty"},
    "25": {"25", "twenty-five", "twenty five"},
    "30": {"30", "thirty"},
    "35": {"35", "thirty-five", "thirty five"},
    "39": {"39", "thirty-nine", "thirty nine"},
    "40": {"40", "forty"},
}


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
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


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def load_candidate_rows(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.resolved_package_decision_route_queue
        WHERE resolved_package_decision_run_id = ?
          AND target_table = 'player_game_box_score'
        ORDER BY route_to_lane, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def value_tokens(value: str) -> set[str]:
    value = clean(value)
    tokens = set(NUMBER_WORDS.get(value, set()))
    if value:
        tokens.add(value)
    return tokens


def text_has_value(text: str, value: str) -> bool:
    lowered = clean(text).lower()
    for token in value_tokens(value):
        if token.isdigit():
            if re.search(rf"(?<!\d){re.escape(token)}(?!\d)", lowered):
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", lowered):
            return True
    return False


def evidence_blob(row: dict[str, Any], proposed: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            clean(row.get("evidence_text")),
            clean(proposed.get("evidence_text")),
            clean(proposed.get("source_row_text")),
            clean(proposed.get("play_text")),
        ]
        if part
    )


def stat_fields(proposed: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for field in STAT_FIELDS + sorted(TD_FIELDS):
        value = clean(proposed.get(field))
        if value:
            fields[field] = value
    return fields


def supports_kicking_field(field: str, value: str, evidence: str) -> bool:
    lowered = clean(evidence).lower()
    field_goal_terms = r"\b(field goal|goal from field|dropkicks?|drop kicks?|placekicks?|place kicks?|placekicked)\b"
    pat_terms = r"\b(goal after touchdown|goal from touchdown|extra point|point after)\b"
    if field == "fg_long":
        return text_has_value(lowered, value)
    if field == "fg_att":
        if value.isdigit() and int(value) > 1:
            attempt_mentions = len(re.findall(field_goal_terms + r"|\bkick from\b", lowered))
            if attempt_mentions >= int(value):
                return True
        if text_has_value(lowered, value):
            return True
        if value == "1" and re.search(field_goal_terms + r"|\bkick from\b", lowered):
            return True
    if field == "fg_made":
        if value == "0":
            return bool(re.search(r"\b(unsuccessful|missed|failed|no good)\b", lowered))
        if value.isdigit() and int(value) > 1:
            made_mentions = len(re.findall(field_goal_terms, lowered))
            if made_mentions >= int(value):
                return True
        if text_has_value(lowered, value):
            return True
        if value == "1" and re.search(r"\b(made|kicked|booted)\b|" + field_goal_terms, lowered):
            return True
    if field == "pat_att":
        if text_has_value(lowered, value):
            return True
        if value == "1" and re.search(r"\b(missed|failed|no good)\b.{0,30}\bgoal\b|\bgoal\b.{0,30}\b(missed|failed|no good)\b", lowered):
            return True
        if value == "1" and re.search(pat_terms, lowered):
            return True
    if field == "pat_made":
        if value == "0":
            return bool(re.search(r"\b(missed|failed|no good)\b", lowered))
        if text_has_value(lowered, value):
            return True
        if value == "1" and re.search(r"\b(kicked|made).{0,30}\b(goal|extra point|point after)\b|" + pat_terms, lowered):
            return True
    return False


def row_is_safe(row: dict[str, Any]) -> tuple[bool, str, dict[str, str]]:
    if clean(row.get("route_to_lane")) != "ready_resolved_atom_review":
        return False, "not in ready_resolved_atom_review lane", {}

    proposed = parse_obj(row.get("proposed_fields_json"))
    fields = stat_fields(proposed)
    if not fields:
        return False, "no stat fields found", {}

    td_present = sorted(field for field in fields if field in TD_FIELDS)
    if td_present:
        return False, f"touchdown-type fields require separate review: {', '.join(td_present)}", fields

    for required in ["boxscore_id", "player_week", "NFL_player_id"]:
        if not clean(proposed.get(required)) and not clean(row.get(required)):
            return False, f"missing required {required}", fields
    if not clean(row.get("source_documents_json")):
        return False, "missing source document ids", fields

    evidence = evidence_blob(row, proposed)
    if not evidence:
        return False, "missing evidence text", fields

    for field, value in fields.items():
        if field in {"fg_made", "fg_att", "fg_long", "pat_made", "pat_att"}:
            if not supports_kicking_field(field, value, evidence):
                return False, f"evidence does not support {field}={value}", fields
            continue
        if not text_has_value(evidence, value):
            return False, f"evidence does not contain value for {field}={value}", fields

    return True, "all non-touchdown stat fields directly supported by evidence text", fields


def build_rows(source_rows: list[dict[str, Any]], run_id: str, decision_run_id: str, created_at: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in source_rows:
        ok, reason, fields = row_is_safe(row)
        audit_row = {
            "safe_stat_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "player_week": clean(row.get("player_week")),
            "NFL_player_id": clean(row.get("NFL_player_id")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "stat_fields_json": json.dumps(fields, ensure_ascii=True, sort_keys=True),
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else "hold_for_review",
            "reason": reason,
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok:
            approved.append({
                "package_item_id": clean(row.get("package_item_id")),
                "decision_status": "approved",
                "decision_value": "approved_for_local_newspaper_final",
                "route_to_lane": "local_final_player_game_box_score",
                "target_table": clean(row.get("target_table")),
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "notes": reason,
            })
        else:
            held.append(audit_row)
    return approved, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    item_defs = ", ".join(f"{field} VARCHAR" for field in AUDIT_FIELDS)
    run_defs = ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_stat_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_stat_decision_run ({run_defs})")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(db_path: Path, run_row: dict[str, Any], audit_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("safe_stat_decision_run_id"))
        con.execute("DELETE FROM newspaper_review.resolved_safe_stat_decision_run WHERE safe_stat_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.resolved_safe_stat_decision_item WHERE safe_stat_decision_run_id = ?", [run_id])
        insert_rows(con, "resolved_safe_stat_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_safe_stat_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_safe_stat_decisions")
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = args.resolved_package_decision_run_id or latest_decision_run(con)
        source_rows = load_candidate_rows(con, decision_run_id)
    finally:
        con.close()
    if not decision_run_id:
        raise SystemExit("No resolved package decision run found")

    approved, held, audit = build_rows(source_rows, run_id, decision_run_id, created_at)
    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_safe_stat_candidates.csv"
    audit_csv = out_dir / "safe_stat_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_fields = Counter()
    hold_reasons = Counter(row["reason"] for row in held)
    for row in audit:
        if row["decision_status"] == "approved":
            for field in parse_obj(row["stat_fields_json"]):
                approved_fields[field] += 1

    summary = {
        "safe_stat_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_fields": dict(sorted(approved_fields.items())),
        "hold_reason_counts": dict(sorted(hold_reasons.items())),
        "approved_csv": str(approved_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }
    write_json(summary_json, summary)

    run_row = {
        "safe_stat_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_fields_json": json.dumps(summary["approved_fields"], sort_keys=True),
        "hold_reason_counts_json": json.dumps(summary["hold_reason_counts"], sort_keys=True),
        "approved_csv": str(approved_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "persisted_to_duckdb": "false" if args.dry_run else "true",
        "created_at_utc": created_at,
    }
    if not args.dry_run:
        persist(args.db_path, run_row, audit)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
