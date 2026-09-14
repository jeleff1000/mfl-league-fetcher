#!/usr/bin/env python
"""Approve source-supported total touchdown rows without inferring TD type.

This station drains the narrow `touchdown_total_type_review` lane. It promotes
only the total `touchdowns` field plus directly stated kicking fields already
present in the row. It deliberately refuses to fill rushing/receiving/passing,
defensive, or special-teams touchdown splits unless a different lane has source
support for those splits.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_touchdown_total_decisions"

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
    "touchdown_total_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "player_raw",
    "confidence_bar",
    "touchdowns",
    "kicking_fields_json",
    "decision_status",
    "decision_value",
    "reason",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "touchdown_total_decision_run_id",
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

TD_SPLIT_FIELDS = {
    "rushing_tds",
    "passing_tds",
    "receiving_tds",
    "def_tds",
    "special_teams_tds",
}

KICKING_FIELDS = [
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "fg_long",
]

NUMBER_WORDS = {
    "0": {"0", "zero", "none", "no"},
    "1": {"1", "one", "a", "an"},
    "2": {"2", "two", "twice", "both"},
    "3": {"3", "three"},
    "4": {"4", "four"},
    "5": {"5", "five"},
    "6": {"6", "six"},
    "7": {"7", "seven"},
    "8": {"8", "eight"},
    "9": {"9", "nine"},
    "10": {"10", "ten"},
    "25": {"25", "twenty five", "twenty-five"},
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


def load_rows(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
    rows = query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.resolved_package_decision_route_queue
        WHERE resolved_package_decision_run_id = ?
          AND route_to_lane = 'touchdown_total_type_review'
          AND target_table = 'player_game_box_score'
        ORDER BY boxscore_id, package_item_id
        """,
        [decision_run_id],
    )
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        deduped[clean(row.get("package_item_id"))] = row
    return list(deduped.values())


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


def supports_kicking_field(field: str, value: str, evidence: str) -> bool:
    lowered = clean(evidence).lower()
    field_goal_terms = r"\b(field goal|goal from field|dropkicks?|drop kicks?|placekicks?|place kicks?|placekicked)\b"
    pat_terms = r"\b(goal after touchdown|goal from touchdown|extra point|point after)\b"
    if field == "fg_long":
        return text_has_value(lowered, value)
    if field in {"fg_made", "fg_att"}:
        if text_has_value(lowered, value):
            return True
        return value == "1" and bool(re.search(field_goal_terms, lowered))
    if field in {"pat_made", "pat_att"}:
        if text_has_value(lowered, value):
            return True
        return value == "1" and bool(re.search(pat_terms, lowered))
    return text_has_value(lowered, value)


def player_named_in_evidence(proposed: dict[str, Any], evidence: str) -> bool:
    player = clean(proposed.get("player_raw"))
    if not player:
        return False
    lowered = clean(evidence).lower()
    name_parts = [part.lower() for part in re.findall(r"[A-Za-z]+", player) if len(part) > 1]
    if not name_parts:
        return False
    return any(re.search(rf"(?<![a-z]){re.escape(part)}(?![a-z])", lowered) for part in name_parts)


def player_name_parts(proposed: dict[str, Any]) -> list[str]:
    return [part.lower() for part in re.findall(r"[A-Za-z]+", clean(proposed.get("player_raw"))) if len(part) > 1]


def supports_touchdown_total_value(proposed: dict[str, Any], evidence: str, touchdowns: str) -> bool:
    if text_has_value(evidence, touchdowns):
        return True
    if touchdowns != "1":
        return False
    lowered = clean(evidence).lower()
    parts = player_name_parts(proposed)
    if not parts:
        return False

    for match in re.finditer(r"\btouchdowns?\s*:\s*([^.;\n]+)", lowered):
        listed = match.group(1)
        if any(re.search(rf"(?<![a-z]){re.escape(part)}(?![a-z])", listed) for part in parts):
            return True

    for part in parts:
        if re.search(rf"(?<![a-z]){re.escape(part)}(?![a-z]).{{0,60}}\b(scored|made)\b.{{0,30}}\btouchdown\b", lowered):
            return True
        if re.search(rf"\b(scored|made)\b.{{0,30}}\btouchdown\b.{{0,60}}(?<![a-z]){re.escape(part)}(?![a-z])", lowered):
            return True
    return False


def source_matches_boxscore(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(row.get("boxscore_id")) or clean(proposed.get("boxscore_id"))
    docs = clean(row.get("source_documents_json")) or clean(proposed.get("source_documents_json"))
    if not boxscore_id or not docs:
        return False
    return boxscore_id in docs


def kicking_fields(proposed: dict[str, Any]) -> dict[str, str]:
    return {field: clean(proposed.get(field)) for field in KICKING_FIELDS if clean(proposed.get(field))}


def supports_touchdown_total(row: dict[str, Any]) -> tuple[bool, str, dict[str, Any], str]:
    proposed = parse_obj(row.get("proposed_fields_json"))
    evidence = evidence_blob(row, proposed)
    if clean(row.get("target_table")) != "player_game_box_score":
        return False, "not a player_game_box_score row", proposed, evidence
    if not source_matches_boxscore(row, proposed):
        return False, "source document does not match boxscore id", proposed, evidence
    if not player_named_in_evidence(proposed, evidence):
        return False, "evidence does not name the player", proposed, evidence
    if not clean(proposed.get("NFL_player_id")) or not clean(proposed.get("player_week")):
        return False, "missing resolved player identity", proposed, evidence

    split_fields = {field: clean(proposed.get(field)) for field in TD_SPLIT_FIELDS if clean(proposed.get(field))}
    if split_fields:
        return False, f"contains touchdown split fields: {json.dumps(split_fields, sort_keys=True)}", proposed, evidence

    touchdowns = clean(proposed.get("touchdowns"))
    if not touchdowns:
        return False, "missing total touchdowns field", proposed, evidence
    if not re.fullmatch(r"\d+", touchdowns):
        return False, f"touchdowns is not an integer: {touchdowns}", proposed, evidence

    lowered = evidence.lower()
    has_td_word = bool(re.search(r"\b(touchdown|touchdowns|td|tds)\b", lowered))
    has_scored_phrase = bool(re.search(r"\bscored\b", lowered))
    if not (has_td_word or has_scored_phrase):
        return False, "evidence lacks touchdown/scored cue", proposed, evidence
    if not supports_touchdown_total_value(proposed, evidence, touchdowns):
        return False, f"evidence does not support touchdowns={touchdowns}", proposed, evidence

    for field, value in kicking_fields(proposed).items():
        if not supports_kicking_field(field, value, evidence):
            return False, f"evidence does not support {field}={value}", proposed, evidence

    return True, "total touchdowns and stated kicking fields are directly supported; touchdown type split left blank", proposed, evidence


def build_rows(
    source_rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in source_rows:
        ok, reason, proposed, evidence = supports_touchdown_total(row)
        kick_fields = kicking_fields(proposed)
        audit_row = {
            "touchdown_total_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "player_week": clean(proposed.get("player_week")) or clean(row.get("player_week")),
            "NFL_player_id": clean(proposed.get("NFL_player_id")) or clean(row.get("NFL_player_id")),
            "player_raw": clean(proposed.get("player_raw")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "touchdowns": clean(proposed.get("touchdowns")),
            "kicking_fields_json": json.dumps(kick_fields, ensure_ascii=True, sort_keys=True),
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else "hold_for_review",
            "reason": reason,
            "evidence_text": evidence or clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")) or clean(proposed.get("source_documents_json")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok:
            approved.append(
                {
                    "package_item_id": clean(row.get("package_item_id")),
                    "decision_status": "approved",
                    "decision_value": "approved_for_local_newspaper_final",
                    "route_to_lane": "local_final_player_game_box_score_touchdown_total",
                    "target_table": clean(row.get("target_table")),
                    "target_entity_key": clean(row.get("target_entity_key")),
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "notes": reason,
                }
            )
        else:
            held.append(audit_row)
    return approved, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    item_defs = ", ".join(f"{field} VARCHAR" for field in AUDIT_FIELDS)
    run_defs = ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_touchdown_total_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_touchdown_total_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("touchdown_total_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_touchdown_total_decision_run WHERE touchdown_total_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_touchdown_total_decision_item WHERE touchdown_total_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "resolved_touchdown_total_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_touchdown_total_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_touchdown_total_decisions")
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
        source_rows = load_rows(con, decision_run_id) if decision_run_id else []
    finally:
        con.close()
    if not decision_run_id:
        raise SystemExit("No resolved package decision run found")

    approved, held, audit = build_rows(source_rows, run_id, decision_run_id, created_at)
    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_touchdown_total_candidates.csv"
    audit_csv = out_dir / "touchdown_total_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_fields = Counter()
    hold_reasons = Counter(row["reason"] for row in held)
    for row in audit:
        if row["decision_status"] == "approved":
            if clean(row.get("touchdowns")):
                approved_fields["touchdowns"] += 1
            for field in parse_obj(row.get("kicking_fields_json")):
                approved_fields[field] += 1

    summary = {
        "touchdown_total_decision_run_id": run_id,
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
        "touchdown_total_decision_run_id": run_id,
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
