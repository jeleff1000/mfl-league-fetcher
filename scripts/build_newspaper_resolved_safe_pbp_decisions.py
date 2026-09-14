#!/usr/bin/env python
"""
Generate conservative decision overrides for safe resolved newspaper PBP rows.

This station promotes direct notable-play/PBP evidence when the source game
matches, the play text supports the play type, and any asserted player actors
are named in the evidence with resolved NFL ids.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_safe_pbp_decisions"

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
    "safe_pbp_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "play_type",
    "possession_team_raw",
    "possession_team",
    "primary_player_raw",
    "primary_NFL_player_id",
    "secondary_player_raw",
    "secondary_NFL_player_id",
    "yards",
    "points",
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
    "safe_pbp_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_play_type_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

PLAYER_REQUIRED = {
    "called_back_kickoff_return_touchdown",
    "completed_forward_pass",
    "criss_cross_run",
    "forward_pass",
    "goal_line_rush",
    "injury_exit",
    "injury_substitution",
    "kickoff_return",
    "punt_return",
    "rushing_gain",
    "rushing_play",
    "touchdown_pass",
    "touchdown_recovery",
}

PLAY_TERMS = {
    "called_back_kickoff_return_touchdown": {"kickoff", "touchdown", "brought back", "whistle"},
    "completed_forward_pass": {"tossed", "throw", "pass", "arms"},
    "criss_cross_run": {"criss cross", "criss-cross", "raced"},
    "forward_pass": {"forward pass", "forward", "pass"},
    "fumbled_punt_recovery": {"fumbled punt", "recovered"},
    "goal_line_rush": {"carried", "failed", "go over"},
    "goal_line_stand": {"held", "downs", "goal", "line"},
    "injury_exit": {"left", "carried off", "injury", "smashed"},
    "injury_substitution": {"injury", "took his place", "suffered"},
    "intercepted_forward_pass": {"forward pass", "intercepted"},
    "interception": {"intercepted", "pass"},
    "kickoff_return": {"kickoff", "travels", "returned"},
    "punt_return": {"punt", "returned"},
    "rushing_gain": {"gain", "through"},
    "rushing_play": {"carried", "run"},
    "team_scoring_summary": {"touchdowns", "forward passes"},
    "touchdown_pass": {"pass", "touchdown", "scored"},
    "touchdown_recovery": {"covered", "ball", "touchdown"},
}

PLAY_PATTERNS = {
    "called_back_kickoff_return_touchdown": [
        r"\bkickoff\b.*\btouchdown\b.*\bbrought back\b",
        r"\bkickoff\b.*\bwhistle\b",
    ],
    "completed_forward_pass": [r"\b(tossed|throw|pass)\b.*\b(arms|caught|out of bounds)\b"],
    "criss_cross_run": [r"\bcriss cross\b", r"\bcriss-cross\b"],
    "forward_pass": [r"\bforward pass\b", r"\bforward\b.*\b(pass|caught)\b"],
    "fumbled_punt_recovery": [r"\brecovered\b.*\bfumbled punt\b", r"\bfumbled punt\b"],
    "goal_line_rush": [r"\bcarried\b.*\b(failed|go over)\b"],
    "goal_line_stand": [r"\bheld\b.*\b(downs|goal|line|yard|foot)\b", r"\bcould not gain\b"],
    "injury_exit": [r"\bleft\b.*\bcarried off\b", r"\bcarried off\b.*\bsmashed\b", r"\bsmashed ribs\b"],
    "injury_substitution": [r"\binjury\b.*\btook .* place\b", r"\bsuffered\b.*\btook .* place\b"],
    "intercepted_forward_pass": [r"\bforward pass\b.*\bintercepted\b"],
    "interception": [r"\bintercepted\b.*\bpass\b"],
    "kickoff_return": [r"\bkickoff\b.*\b(travels|returned|raced)\b", r"\btravels\b.*\bkickoff\b"],
    "punt_return": [r"\breturned\b.*\bpunt\b", r"\bpunt\b.*\breturn"],
    "rushing_gain": [r"\bgain\b.*\bthrough\b"],
    "rushing_play": [r"\bcarried\b.*\brun\b"],
    "team_scoring_summary": [r"\btouchdowns\b.*\bforward passes\b"],
    "touchdown_pass": [r"\bpass\b.*\btouchdown\b", r"\bpass\b.*\bscored\b"],
    "touchdown_recovery": [r"\bcovered\b.*\bball\b.*\btouchdown\b"],
}

NUMBER_WORDS = {
    "3": {"3", "three"},
    "18": {"18", "eighteen"},
    "21": {"21", "twenty one", "twenty-one"},
    "26": {"26", "twenty six", "twenty-six"},
    "30": {"30", "thirty"},
    "50": {"50", "fifty"},
    "55": {"55", "fifty five", "fifty-five"},
    "90": {"90", "ninety"},
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
          AND target_table = 'play_by_play_event'
        ORDER BY route_to_lane, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def norm_words(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(text).lower()).strip()


def compact(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean(text).lower())


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


def name_tokens(raw_name: str) -> set[str]:
    words = [token for token in norm_words(raw_name).split() if len(token) >= 2]
    tokens = set(words)
    if words:
        tokens.add(" ".join(words))
    return tokens


def evidence_has_name(evidence: str, raw_name: str) -> bool:
    words = norm_words(evidence)
    compact_evidence = compact(evidence)
    for token in name_tokens(raw_name):
        if " " in token:
            if compact(token) in compact_evidence:
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words):
            return True
    return False


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
    if not clean(value):
        return True
    words = norm_words(evidence)
    for token in NUMBER_WORDS.get(clean(value), {clean(value)}):
        if token.isdigit():
            if re.search(rf"(?<!\d){re.escape(token)}(?!\d)", words):
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words):
            return True
    return False


def evidence_has_play_type(evidence: str, play_type: str) -> bool:
    words = norm_words(evidence)
    for pattern in PLAY_PATTERNS.get(play_type, []):
        if re.search(pattern, words):
            return True
    terms = PLAY_TERMS.get(play_type, {play_type.replace("_", " ")})
    return all(term in words for term in terms)


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
    if clean(row.get("confidence_bar")).lower() == "conflict":
        return False, "confidence_bar conflict requires review", {}

    proposed = parse_obj(row.get("proposed_fields_json"))
    fields = {
        "boxscore_id": clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id")),
        "play_type": clean(proposed.get("play_type")),
        "possession_team_raw": clean(proposed.get("possession_team_raw")),
        "possession_team": clean(proposed.get("possession_team")),
        "primary_player_raw": clean(proposed.get("primary_player_raw")),
        "primary_NFL_player_id": clean(proposed.get("primary_NFL_player_id")),
        "secondary_player_raw": clean(proposed.get("secondary_player_raw")),
        "secondary_NFL_player_id": clean(proposed.get("secondary_NFL_player_id")),
        "yards": clean(proposed.get("yards")),
        "points": clean(proposed.get("points")),
    }

    for required in ["boxscore_id", "play_type"]:
        if not fields[required]:
            return False, f"missing required {required}", fields
    if not parse_list(proposed.get("source_documents_json")) and not parse_list(row.get("source_documents_json")):
        return False, "missing source document ids", fields
    if not source_matches_boxscore(row, proposed):
        return False, "source document prefix does not match boxscore_id", fields

    evidence = evidence_blob(row, proposed)
    if not evidence:
        return False, "missing evidence text", fields
    if not evidence_has_play_type(evidence, fields["play_type"]):
        return False, f"evidence does not directly support {fields['play_type']}", fields
    if fields["yards"] and not evidence_has_value(evidence, fields["yards"]):
        return False, "evidence does not show yards value", fields

    if fields["primary_player_raw"]:
        if not fields["primary_NFL_player_id"]:
            return False, "primary player identity unresolved", fields
        if not evidence_has_name(evidence, fields["primary_player_raw"]):
            return False, f"evidence does not name primary player {fields['primary_player_raw']}", fields
    elif fields["play_type"] in PLAYER_REQUIRED:
        return False, f"play_type requires player identity: {fields['play_type']}", fields
    elif not evidence_has_team(evidence, fields["possession_team_raw"], fields["possession_team"]):
        return False, "playerless play does not name possession team", fields

    if fields["secondary_player_raw"]:
        if not fields["secondary_NFL_player_id"]:
            return False, "secondary player identity unresolved", fields
        if not evidence_has_name(evidence, fields["secondary_player_raw"]):
            return False, f"evidence does not name secondary player {fields['secondary_player_raw']}", fields

    return True, "direct PBP evidence supports play type and actors", fields


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
            "safe_pbp_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "play_type": clean(fields.get("play_type")),
            "possession_team_raw": clean(fields.get("possession_team_raw")),
            "possession_team": clean(fields.get("possession_team")),
            "primary_player_raw": clean(fields.get("primary_player_raw")),
            "primary_NFL_player_id": clean(fields.get("primary_NFL_player_id")),
            "secondary_player_raw": clean(fields.get("secondary_player_raw")),
            "secondary_NFL_player_id": clean(fields.get("secondary_NFL_player_id")),
            "yards": clean(fields.get("yards")),
            "points": clean(fields.get("points")),
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
                    "route_to_lane": "local_final_play_by_play_event",
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
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_pbp_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_pbp_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("safe_pbp_decision_run_id"))
        con.execute("DELETE FROM newspaper_review.resolved_safe_pbp_decision_run WHERE safe_pbp_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.resolved_safe_pbp_decision_item WHERE safe_pbp_decision_run_id = ?", [run_id])
        insert_rows(con, "resolved_safe_pbp_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_safe_pbp_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_safe_pbp_decisions")
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
    held_csv = out_dir / "held_pbp_candidates.csv"
    audit_csv = out_dir / "safe_pbp_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_play_counts = Counter(row["play_type"] for row in audit if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)
    summary = {
        "safe_pbp_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_play_type_counts": dict(sorted(approved_play_counts.items())),
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
        "safe_pbp_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_play_type_counts_json": json.dumps(summary["approved_play_type_counts"], sort_keys=True),
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
