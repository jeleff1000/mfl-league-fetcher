#!/usr/bin/env python
"""
Generate conservative second-pass decisions for remaining resolved newspaper rows.

This station handles rows that were held by first-pass automations for reasons
that can be checked mechanically:

- same-date source document prefixes where the evidence directly names teams or
  players and the resolved game key differs only by suffix
- lineup rows where the evidence names the player in a direct lineup/article row
- direct touchdown/kicking/team/PBP facts whose whole proposed row is supported

It still holds aliases ("Indian star"), context-only rows ("visitors"), source
date mismatches, unsupported scorer/passers, and rows whose proposed stat fields
are only partially supported.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_second_pass_decisions"

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
    "second_pass_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "decision_status",
    "decision_value",
    "reason",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "second_pass_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_target_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

ROUTE_BY_TARGET = {
    "lineup_participation": "local_final_lineup_participation",
    "play_by_play_event": "local_final_play_by_play_event",
    "player_game_box_score": "local_final_player_game_box_score",
    "scoring_event": "local_final_scoring_event",
    "team_game_stat_claim": "local_final_team_game_stat_claim",
}

NUMBER_WORDS = {
    "0": {"0", "zero", "none"},
    "1": {"1", "one", "a", "an", "single"},
    "2": {"2", "two", "twice", "both"},
    "3": {"3", "three"},
    "4": {"4", "four"},
    "5": {"5", "five"},
    "6": {"6", "six"},
    "7": {"7", "seven"},
    "10": {"10", "ten"},
    "21": {"21", "twenty one", "twenty-one"},
    "26": {"26", "twenty six", "twenty-six"},
    "40": {"40", "forty"},
    "43": {"43", "forty three", "forty-three"},
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
          AND route_to_lane = 'ready_resolved_atom_review'
        ORDER BY target_table, boxscore_id, package_item_id
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


def source_prefixes(row: dict[str, Any], proposed: dict[str, Any]) -> list[str]:
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    prefixes = []
    for doc in docs:
        match = re.match(r"([^#:]+)", doc)
        if match:
            prefixes.append(match.group(1))
    return prefixes


def same_source_date(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    if len(boxscore_id) < 8:
        return False
    return any(prefix[:8] == boxscore_id[:8] for prefix in source_prefixes(row, proposed))


def exact_source_game(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    return bool(boxscore_id and boxscore_id in source_prefixes(row, proposed))


def source_is_usable(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    return exact_source_game(row, proposed) or same_source_date(row, proposed)


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
    value = clean(value)
    if not value:
        return True
    words = norm_words(evidence)
    for token in NUMBER_WORDS.get(value, {value}):
        if token.isdigit():
            if re.search(rf"(?<!\d){re.escape(token)}(?!\d)", words):
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words):
            return True
    return False


def supports_scoring_event(proposed: dict[str, Any], evidence: str) -> bool:
    event_type = clean(proposed.get("event_type"))
    points = clean(proposed.get("points"))
    player = clean(proposed.get("scoring_player_raw"))
    team = clean(proposed.get("scoring_team"))
    team_raw = clean(proposed.get("scoring_team_raw"))
    words = norm_words(evidence)

    expected_points = {
        "blocked_punt_return_touchdown": "6",
        "field_goal": "3",
        "field_goal_place_kick": "3",
        "drop_kick_field_goal": "3",
        "pat_kick": "1",
        "rushing_touchdown": "6",
        "touchdown": "6",
        "touchdown_run": "6",
    }
    if event_type in expected_points and points != expected_points[event_type]:
        return False
    if event_type == "field_goal" and not re.search(r"\b(field goal|drop kick|placekick|placekicked|place kick|successful place kick)\b", words):
        return False
    if event_type in {"field_goal_place_kick", "drop_kick_field_goal"} and not re.search(r"\b(place kick|placekick|drop kick|booted)\b", words):
        return False
    if event_type == "pat_kick" and not re.search(r"\bkicked .*goal|goal from placement\b", words):
        return False
    if event_type in {"rushing_touchdown", "touchdown", "touchdown_run", "blocked_punt_return_touchdown"}:
        if not re.search(r"\b(touchdown|score|scored|ran for a touchdown|through for the score)\b", words):
            return False
    if event_type in {"receiving_touchdown", "touchdown_pass"}:
        if points != "6":
            return False
        if not re.search(r"\b(touchdown|scored|crossed .* goal|goal line|behind .* goal)\b", words):
            return False
    if player:
        if not clean(proposed.get("scoring_NFL_player_id")):
            return False
        if not evidence_has_name(evidence, player):
            return False
    elif not evidence_has_team(evidence, team_raw, team):
        return False
    for raw_field, id_field in [
        ("passer_raw", "passer_NFL_player_id"),
        ("receiver_raw", "receiver_NFL_player_id"),
    ]:
        raw = clean(proposed.get(raw_field))
        if raw and (not clean(proposed.get(id_field)) or not evidence_has_name(evidence, raw)):
            return False
    return True


def supports_box_score(proposed: dict[str, Any], evidence: str) -> bool:
    player = clean(proposed.get("player_raw"))
    if not player or not clean(proposed.get("NFL_player_id")) or not evidence_has_name(evidence, player):
        return False
    words = norm_words(evidence)
    if clean(proposed.get("passing_tds")) == "1":
        if not re.search(r"\b(to|pass|toss).*\b(touchdown|goal line|score)\b|\b(touchdown|goal line|score).*\b(to|pass|toss)\b", words):
            return False
    if clean(proposed.get("touchdowns")) == "1":
        if not re.search(r"\btouchdown\b", words):
            return False
    if clean(proposed.get("fg_made")) == "1" or clean(proposed.get("fg_att")) == "1":
        if not re.search(r"\bgoal from field|field goal|place kick|placekick\b", words):
            return False
    if clean(proposed.get("pat_made")) == "1" or clean(proposed.get("pat_att")) == "1":
        if not re.search(r"\bgoal from touchdown|extra point|point after\b", words):
            return False
    if clean(proposed.get("def_tds")):
        if not re.search(r"\bintercepted\b.*\btouchdown\b|\btouchdown\b.*\bintercepted\b", words):
            return False
    if clean(proposed.get("special_teams_tds")):
        if not re.search(r"\b(blocked punt|punt|kickoff)\b.*\btouchdown\b|\btouchdown\b.*\b(blocked punt|punt|kickoff)\b", words):
            return False
    unsupported_td_fields = [field for field in ["rushing_tds", "receiving_tds"] if clean(proposed.get(field))]
    return not unsupported_td_fields


def supports_lineup(proposed: dict[str, Any], evidence: str) -> bool:
    player = clean(proposed.get("player_raw"))
    if not player or not clean(proposed.get("NFL_player_id")) or not evidence_has_name(evidence, player):
        return False
    participation_type = clean(proposed.get("participation_type"))
    if participation_type == "article_named_lineman":
        return True
    return bool(re.search(r"\b(HB|LHB|RHB|FB|QB|LG|RG|LT|RT|LE|RE|C)\b", clean(proposed.get("source_row_text"))))


def supports_pbp(proposed: dict[str, Any], evidence: str) -> bool:
    if clean(proposed.get("confidence_bar")).lower() == "conflict":
        return False
    play_type = clean(proposed.get("play_type"))
    words = norm_words(evidence)
    patterns = {
        "criss_cross_run": r"\bcriss cross\b|\bcriss-cross\b",
        "punt_return": r"\breturned\b.*\bpunt\b",
    }
    if play_type not in patterns or not re.search(patterns[play_type], words):
        return False
    primary = clean(proposed.get("primary_player_raw"))
    if primary and (not clean(proposed.get("primary_NFL_player_id")) or not evidence_has_name(evidence, primary)):
        return False
    if clean(proposed.get("yards")) and not evidence_has_value(evidence, proposed.get("yards")):
        return False
    return True


def supports_team_stat(proposed: dict[str, Any], evidence: str) -> bool:
    if clean(proposed.get("stat_name")) != "first_downs":
        return False
    if not evidence_has_team(evidence, clean(proposed.get("team_1_raw")), clean(proposed.get("team_1_nfl_team"))):
        return False
    return evidence_has_value(evidence, clean(proposed.get("team_1_value")))


def row_decision(row: dict[str, Any]) -> tuple[bool, str]:
    target = clean(row.get("target_table"))
    if target not in ROUTE_BY_TARGET:
        return False, f"target table not handled by second pass: {target}"
    proposed = parse_obj(row.get("proposed_fields_json"))
    evidence = evidence_blob(row, proposed)
    if not evidence:
        return False, "missing evidence text"
    if not source_is_usable(row, proposed):
        return False, "source document date does not match boxscore_id"

    if target == "lineup_participation" and supports_lineup(proposed, evidence):
        return True, "second pass direct lineup/participation evidence"
    if target == "scoring_event" and supports_scoring_event(proposed, evidence):
        return True, "second pass direct same-date scoring evidence"
    if target == "player_game_box_score" and supports_box_score(proposed, evidence):
        return True, "second pass direct whole-row box score evidence"
    if target == "play_by_play_event" and supports_pbp(proposed, evidence):
        return True, "second pass direct same-date PBP evidence"
    if target == "team_game_stat_claim" and supports_team_stat(proposed, evidence):
        return True, "second pass direct same-date team stat evidence"
    return False, "second pass support rules not met"


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
        ok, reason = row_decision(row)
        target = clean(row.get("target_table"))
        audit_row = {
            "second_pass_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": target,
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
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
                    "route_to_lane": ROUTE_BY_TARGET[target],
                    "target_table": target,
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
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_second_pass_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_second_pass_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("second_pass_decision_run_id"))
        con.execute("DELETE FROM newspaper_review.resolved_second_pass_decision_run WHERE second_pass_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.resolved_second_pass_decision_item WHERE second_pass_decision_run_id = ?", [run_id])
        insert_rows(con, "resolved_second_pass_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_second_pass_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_second_pass_decisions")
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
    held_csv = out_dir / "held_second_pass_candidates.csv"
    audit_csv = out_dir / "second_pass_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_target_counts = Counter(row["target_table"] for row in audit if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)
    summary = {
        "second_pass_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_target_counts": dict(sorted(approved_target_counts.items())),
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
        "second_pass_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_target_counts_json": json.dumps(summary["approved_target_counts"], sort_keys=True),
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
