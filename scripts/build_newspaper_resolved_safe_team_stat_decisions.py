#!/usr/bin/env python
"""
Generate conservative decision overrides for safe resolved newspaper team stats.

This station promotes only team-game stat claims where the source text names the
team(s), shows the claimed value(s), supports the stat family, and the source
document prefix matches the resolved boxscore_id.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_safe_team_stat_decisions"

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
    "safe_team_stat_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "stat_name",
    "team_1_raw",
    "team_1_nfl_team",
    "team_1_value",
    "team_2_raw",
    "team_2_nfl_team",
    "team_2_value",
    "confidence_bar",
    "risk_level",
    "decision_status",
    "decision_value",
    "reason",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "safe_team_stat_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_stat_name_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

STAT_TERMS = {
    "first_downs": {"first down", "first downs"},
    "pass_attempts": {"passes", "pass attempts", "attempted"},
    "pass_completions": {"completions", "completed"},
    "passes_intercepted": {"intercepted", "interceptions"},
    "passes_grounded": {"grounded"},
    "third_quarter_touchdowns": {"touchdown", "touchdowns"},
    "total_yards": {"yards", "yards gained", "gained"},
}

NUMBER_WORDS = {
    "0": {"0", "zero", "none"},
    "1": {"1", "one", "lone", "single"},
    "2": {"2", "two", "both"},
    "3": {"3", "three"},
    "4": {"4", "four"},
    "5": {"5", "five"},
    "6": {"6", "six"},
    "7": {"7", "seven"},
    "10": {"10", "ten"},
    "11": {"11", "eleven"},
    "21": {"21", "twenty one", "twenty-one"},
    "217": {"217"},
    "289": {"289"},
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


def parse_list(value: Any) -> list[str]:
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
          AND target_table = 'team_game_stat_claim'
        ORDER BY boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def norm_words(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(text).lower()).strip()


def evidence_blob(row: dict[str, Any], proposed: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            clean(row.get("evidence_text")),
            clean(proposed.get("evidence_text")),
            clean(proposed.get("source_row_text")),
        ]
        if part
    )


def team_tokens(team_raw: str, team_code: str) -> set[str]:
    tokens = set()
    for token in norm_words(team_raw).split():
        if len(token) >= 3 and token not in {"the", "club", "team"}:
            tokens.add(token)
            if token.endswith("s") and len(token) > 4:
                tokens.add(token[:-1])
    code = norm_words(team_code)
    if len(code) >= 3:
        tokens.add(code)
    return tokens


def evidence_has_team(evidence: str, team_raw: str, team_code: str) -> bool:
    words = norm_words(evidence)
    return any(re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words) for token in team_tokens(team_raw, team_code))


def evidence_has_value(evidence: str, value: str) -> bool:
    words = norm_words(evidence)
    for token in NUMBER_WORDS.get(clean(value), {clean(value)}):
        if not token:
            continue
        if token.isdigit():
            if re.search(rf"(?<!\d){re.escape(token)}(?!\d)", words):
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words):
            return True
    return False


def evidence_has_stat(evidence: str, stat_name: str) -> bool:
    words = norm_words(evidence)
    return any(term in words for term in STAT_TERMS.get(stat_name, {stat_name.replace("_", " ")}))


def source_matches_boxscore(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    prefixes = []
    for doc in docs:
        match = re.match(r"([^#:]+)", doc)
        if match:
            prefixes.append(match.group(1))
    return bool(boxscore_id and boxscore_id in prefixes)


def row_is_safe(row: dict[str, Any]) -> tuple[bool, str, dict[str, str]]:
    if clean(row.get("route_to_lane")) != "ready_resolved_atom_review":
        return False, "not in ready_resolved_atom_review lane", {}

    proposed = parse_obj(row.get("proposed_fields_json"))
    fields = {
        "boxscore_id": clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id")),
        "stat_name": clean(proposed.get("stat_name")),
        "team_1_raw": clean(proposed.get("team_1_raw")),
        "team_1_nfl_team": clean(proposed.get("team_1_nfl_team")),
        "team_1_value": clean(proposed.get("team_1_value")),
        "team_2_raw": clean(proposed.get("team_2_raw")),
        "team_2_nfl_team": clean(proposed.get("team_2_nfl_team")),
        "team_2_value": clean(proposed.get("team_2_value")),
    }
    for required in ["boxscore_id", "stat_name", "team_1_raw", "team_1_nfl_team", "team_1_value"]:
        if not fields[required]:
            return False, f"missing required {required}", fields
    if not parse_list(proposed.get("source_documents_json")) and not parse_list(row.get("source_documents_json")):
        return False, "missing source document ids", fields
    if not source_matches_boxscore(row, proposed):
        return False, "source document prefix does not match boxscore_id", fields

    evidence = evidence_blob(row, proposed)
    if not evidence:
        return False, "missing evidence text", fields
    if not evidence_has_stat(evidence, fields["stat_name"]):
        return False, f"evidence does not show stat family {fields['stat_name']}", fields
    if not evidence_has_team(evidence, fields["team_1_raw"], fields["team_1_nfl_team"]):
        return False, "evidence does not name team_1", fields
    if not evidence_has_value(evidence, fields["team_1_value"]):
        return False, "evidence does not show team_1_value", fields
    if fields["team_2_value"]:
        if not fields["team_2_raw"] or not fields["team_2_nfl_team"]:
            return False, "team_2_value present without team_2 identity", fields
        if not evidence_has_team(evidence, fields["team_2_raw"], fields["team_2_nfl_team"]):
            return False, "evidence does not name team_2", fields
        if not evidence_has_value(evidence, fields["team_2_value"]):
            return False, "evidence does not show team_2_value", fields
    return True, "direct team stat evidence names teams and values", fields


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
        ok, reason, fields = row_is_safe(row)
        audit_row = {
            "safe_team_stat_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "stat_name": clean(fields.get("stat_name")),
            "team_1_raw": clean(fields.get("team_1_raw")),
            "team_1_nfl_team": clean(fields.get("team_1_nfl_team")),
            "team_1_value": clean(fields.get("team_1_value")),
            "team_2_raw": clean(fields.get("team_2_raw")),
            "team_2_nfl_team": clean(fields.get("team_2_nfl_team")),
            "team_2_value": clean(fields.get("team_2_value")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "risk_level": clean(row.get("risk_level")),
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
            approved.append(
                {
                    "package_item_id": clean(row.get("package_item_id")),
                    "decision_status": "approved",
                    "decision_value": "approved_for_local_newspaper_final",
                    "route_to_lane": "local_final_team_game_stat_claim",
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
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_team_stat_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_team_stat_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("safe_team_stat_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_safe_team_stat_decision_run WHERE safe_team_stat_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_safe_team_stat_decision_item WHERE safe_team_stat_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "resolved_safe_team_stat_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_safe_team_stat_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_safe_team_stat_decisions")
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
    held_csv = out_dir / "held_team_stat_candidates.csv"
    audit_csv = out_dir / "safe_team_stat_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_stat_counts = Counter(row["stat_name"] for row in audit if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)
    summary = {
        "safe_team_stat_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_stat_name_counts": dict(sorted(approved_stat_counts.items())),
        "hold_reason_counts": dict(sorted(hold_reason_counts.items())),
        "approved_csv": str(approved_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }
    write_json(summary_json, summary)

    run_row = {
        "safe_team_stat_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_stat_name_counts_json": json.dumps(summary["approved_stat_name_counts"], sort_keys=True),
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
