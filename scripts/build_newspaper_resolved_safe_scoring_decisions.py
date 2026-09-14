#!/usr/bin/env python
"""
Generate conservative decision overrides for safe resolved newspaper scoring rows.

This station promotes only direct scoring evidence into the local newspaper final
tables. It intentionally holds rows with unresolved identities, source/game key
mismatches, unsupported event text, or playerless events that are not anchored by
the scoring team.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_safe_scoring_decisions"

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
    "safe_scoring_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "scoring_team",
    "scoring_player_raw",
    "scoring_NFL_player_id",
    "event_type",
    "points",
    "distance_yards",
    "passer_raw",
    "passer_NFL_player_id",
    "receiver_raw",
    "receiver_NFL_player_id",
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
    "safe_scoring_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_event_type_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

TOUCHDOWN_EVENTS = {
    "touchdown",
    "touchdown_run",
    "rushing_touchdown",
    "receiving_touchdown",
    "touchdown_reception",
    "touchdown_pass",
    "interception_return_touchdown",
    "blocked_punt_return_touchdown",
    "fake_forward_pass_touchdown",
}

FIELD_GOAL_EVENTS = {
    "field_goal",
    "field_goal_place_kick",
    "field_goal_drop_kick",
    "drop_kick_field_goal",
}

PAT_MADE_EVENTS = {
    "extra_point",
    "extra_point_pass",
    "goal_after_touchdown",
    "goal_after_touchdown_free_kick",
    "pat_kick",
    "point_after_touchdown",
    "penalty_awarded_try",
}

PAT_MISSED_EVENTS = {
    "missed_extra_point",
    "missed_goal_after_touchdown",
    "pat_failed",
}

TEAM_LEVEL_ALLOWED = {
    "touchdown",
    "field_goal",
    "drop_kick_field_goal",
    "goal_after_touchdown_free_kick",
    "missed_extra_point",
    "missed_goal_after_touchdown",
    "pat_failed",
    "penalty_awarded_try",
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
          AND target_table = 'scoring_event'
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
    if not clean(raw_name):
        return False
    words = norm_words(evidence)
    compact_evidence = compact(evidence)
    for token in name_tokens(raw_name):
        if " " in token:
            if compact(token) and compact(token) in compact_evidence:
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
    for token in team_tokens(team_raw, team_code):
        if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words):
            return True
    return False


def source_prefixes(row: dict[str, Any], proposed: dict[str, Any]) -> list[str]:
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    prefixes = []
    for doc in docs:
        match = re.match(r"([^#:]+)", doc)
        if match:
            prefixes.append(match.group(1))
    return prefixes


def source_matches_boxscore(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    return bool(boxscore_id and boxscore_id in source_prefixes(row, proposed))


def expected_points(event_type: str) -> set[str]:
    if event_type in TOUCHDOWN_EVENTS:
        return {"6"}
    if event_type in FIELD_GOAL_EVENTS:
        return {"3"}
    if event_type in PAT_MADE_EVENTS:
        return {"1"}
    if event_type in PAT_MISSED_EVENTS:
        return {"0"}
    return set()


def points_are_supported(event_type: str, points: str) -> bool:
    return clean(points) in expected_points(event_type)


def supports_field_goal(evidence: str) -> bool:
    lowered = norm_words(evidence)
    return bool(
        re.search(
            r"\b(field goal|field goals|drop kick|drop kicked|dropkick|place kick|place kicked|placekicked|"
            r"placement|placement kick|between the uprights|between the goal posts|between goal posts|"
            r"kick from|kicked from|booted a place kick|boots goal)\b",
            lowered,
        )
    )


def supports_pat_made(event_type: str, evidence: str) -> bool:
    lowered = norm_words(evidence)
    if event_type == "penalty_awarded_try":
        return bool(re.search(r"\b(point was added|point added|offside|automatically)\b", lowered))
    return bool(
        re.search(
            r"\b(extra point|extra points|points after touchdowns|point after|goal after|free kick|"
            r"kicked goal|kicked the goal|kicked a goal|kick for point|kicked for the extra point|"
            r"placekicked the point|placement was good|added .* extra points|behind .* goal line|winning point)\b",
            lowered,
        )
    )


def supports_pat_missed(evidence: str) -> bool:
    lowered = norm_words(evidence)
    return bool(
        re.search(
            r"\b(missed|failed|failure|blocked|no good)\b.*\b(goal|extra point|point|drop kick)\b|"
            r"\b(goal|extra point|point|drop kick)\b.*\b(missed|failed|blocked|no good)\b",
            lowered,
        )
    )


def supports_touchdown(evidence: str, player_raw: str) -> bool:
    lowered = norm_words(evidence)
    strong = re.search(
        r"\b(touchdown|touchdowns|touch down|goal line|goal|behind .* goal|crossed .* goal|"
        r"over .* line|across .* line|behind .* line|six pointer|six pointers|carried .* over|lone tally)\b",
        lowered,
    )
    if strong:
        return True
    if player_raw and re.search(r"\b(score|scores|scored|raced|ran)\b", lowered):
        return True
    return False


def event_text_supported(event_type: str, evidence: str, player_raw: str) -> bool:
    if event_type in TOUCHDOWN_EVENTS:
        return supports_touchdown(evidence, player_raw)
    if event_type in FIELD_GOAL_EVENTS:
        return supports_field_goal(evidence)
    if event_type in PAT_MADE_EVENTS:
        return supports_pat_made(event_type, evidence)
    if event_type in PAT_MISSED_EVENTS:
        return supports_pat_missed(evidence)
    return False


def row_is_safe(row: dict[str, Any]) -> tuple[bool, str, dict[str, str]]:
    if clean(row.get("route_to_lane")) != "ready_resolved_atom_review":
        return False, "not in ready_resolved_atom_review lane", {}

    proposed = parse_obj(row.get("proposed_fields_json"))
    fields = {
        "boxscore_id": clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id")),
        "scoring_team": clean(proposed.get("scoring_team")) or clean(row.get("scoring_team")),
        "scoring_team_raw": clean(proposed.get("scoring_team_raw")),
        "scoring_player_raw": clean(proposed.get("scoring_player_raw")),
        "scoring_NFL_player_id": clean(proposed.get("scoring_NFL_player_id")),
        "event_type": clean(proposed.get("event_type")),
        "points": clean(proposed.get("points")),
        "distance_yards": clean(proposed.get("distance_yards")),
        "passer_raw": clean(proposed.get("passer_raw")),
        "passer_NFL_player_id": clean(proposed.get("passer_NFL_player_id")),
        "receiver_raw": clean(proposed.get("receiver_raw")),
        "receiver_NFL_player_id": clean(proposed.get("receiver_NFL_player_id")),
    }

    for required in ["boxscore_id", "scoring_team", "event_type", "points"]:
        if not fields[required]:
            return False, f"missing required {required}", fields
    if not parse_list(proposed.get("source_documents_json")) and not parse_list(row.get("source_documents_json")):
        return False, "missing source document ids", fields
    if not source_matches_boxscore(row, proposed):
        return False, "source document prefix does not match boxscore_id", fields

    evidence = evidence_blob(row, proposed)
    if not evidence:
        return False, "missing evidence text", fields
    if not points_are_supported(fields["event_type"], fields["points"]):
        return False, f"event_type/points mismatch: {fields['event_type']}={fields['points']}", fields
    if not event_text_supported(fields["event_type"], evidence, fields["scoring_player_raw"]):
        return False, f"evidence does not directly support {fields['event_type']}", fields

    if fields["scoring_player_raw"]:
        if not fields["scoring_NFL_player_id"]:
            return False, "scoring player identity unresolved", fields
        if not evidence_has_name(evidence, fields["scoring_player_raw"]):
            return False, f"evidence does not name scoring player {fields['scoring_player_raw']}", fields
    elif fields["event_type"] not in TEAM_LEVEL_ALLOWED:
        return False, f"playerless event_type requires manual review: {fields['event_type']}", fields
    elif not evidence_has_team(evidence, fields["scoring_team_raw"], fields["scoring_team"]):
        return False, "playerless event does not name scoring team", fields

    if fields["passer_raw"]:
        if not fields["passer_NFL_player_id"]:
            return False, "passer identity unresolved", fields
        if not evidence_has_name(evidence, fields["passer_raw"]):
            return False, f"evidence does not name passer {fields['passer_raw']}", fields
    if fields["receiver_raw"]:
        if not fields["receiver_NFL_player_id"]:
            return False, "receiver identity unresolved", fields
        if not evidence_has_name(evidence, fields["receiver_raw"]):
            return False, f"evidence does not name receiver {fields['receiver_raw']}", fields

    if fields["scoring_player_raw"]:
        return True, "direct scoring evidence names resolved scoring player", fields
    return True, "direct team-level scoring evidence with no player asserted", fields


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
            "safe_scoring_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "scoring_team": clean(fields.get("scoring_team")),
            "scoring_player_raw": clean(fields.get("scoring_player_raw")),
            "scoring_NFL_player_id": clean(fields.get("scoring_NFL_player_id")),
            "event_type": clean(fields.get("event_type")),
            "points": clean(fields.get("points")),
            "distance_yards": clean(fields.get("distance_yards")),
            "passer_raw": clean(fields.get("passer_raw")),
            "passer_NFL_player_id": clean(fields.get("passer_NFL_player_id")),
            "receiver_raw": clean(fields.get("receiver_raw")),
            "receiver_NFL_player_id": clean(fields.get("receiver_NFL_player_id")),
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
                    "route_to_lane": "local_final_scoring_event",
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
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_scoring_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_scoring_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("safe_scoring_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_safe_scoring_decision_run WHERE safe_scoring_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_safe_scoring_decision_item WHERE safe_scoring_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "resolved_safe_scoring_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_safe_scoring_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_safe_scoring_decisions")
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
    held_csv = out_dir / "held_scoring_candidates.csv"
    audit_csv = out_dir / "safe_scoring_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_event_counts = Counter(row["event_type"] for row in audit if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)
    summary = {
        "safe_scoring_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_event_type_counts": dict(sorted(approved_event_counts.items())),
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
        "safe_scoring_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_event_type_counts_json": json.dumps(summary["approved_event_type_counts"], sort_keys=True),
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
