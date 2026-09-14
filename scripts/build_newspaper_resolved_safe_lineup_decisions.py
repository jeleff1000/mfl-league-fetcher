#!/usr/bin/env python
"""
Generate conservative decision overrides for safe resolved newspaper lineup rows.

This station approves only direct lineup evidence:

- starter rows where the source/evidence line names the player and position
- substitution rows where the source/evidence line names the player and uses a
  substitution marker such as "for"

Generic summary-context rows and article-name-only participation rows are held
for a later review pass.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_safe_lineup_decisions"

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
    "safe_lineup_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
    "confidence_bar",
    "risk_level",
    "participation_type",
    "starter_position",
    "listed_position_raw",
    "is_starter",
    "decision_status",
    "decision_value",
    "reason",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "safe_lineup_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_type_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

POSITION_ALIASES = {
    "LE": {"le", "l e", "left end"},
    "L.E.": {"le", "l e", "left end"},
    "RE": {"re", "r e", "right end"},
    "R.E.": {"re", "r e", "right end"},
    "LT": {"lt", "l t", "left tackle"},
    "L.T.": {"lt", "l t", "left tackle"},
    "RT": {"rt", "r t", "right tackle"},
    "R.T.": {"rt", "r t", "right tackle"},
    "LG": {"lg", "l g", "left guard"},
    "L.G.": {"lg", "l g", "left guard"},
    "RG": {"rg", "r g", "right guard"},
    "R.G.": {"rg", "r g", "right guard"},
    "C": {"c", "center", "centre"},
    "Q": {"q", "qb", "quarterback"},
    "Q.": {"q", "qb", "quarterback"},
    "QB": {"q", "qb", "quarterback"},
    "Q.B.": {"q", "qb", "quarterback"},
    "LH": {"lh", "lhb", "left half", "left halfback"},
    "L.H.": {"lh", "lhb", "left half", "left halfback"},
    "LHB": {"lh", "lhb", "left half", "left halfback"},
    "L.H.B.": {"lh", "lhb", "left half", "left halfback"},
    "RH": {"rh", "rhb", "right half", "right halfback"},
    "R.H.": {"rh", "rhb", "right half", "right halfback"},
    "RHB": {"rh", "rhb", "right half", "right halfback"},
    "R.H.B.": {"rh", "rhb", "right half", "right halfback"},
    "F": {"f", "fb", "fullback"},
    "F.": {"f", "fb", "fullback"},
    "FB": {"f", "fb", "fullback"},
    "F.B.": {"f", "fb", "fullback"},
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
          AND target_table = 'lineup_participation'
        ORDER BY boxscore_id, package_item_id
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
            clean(proposed.get("source_row_text")),
            clean(proposed.get("evidence_text")),
        ]
        if part
    )


def name_tokens(player_raw: str) -> set[str]:
    raw = norm_words(player_raw)
    tokens = {token for token in raw.split() if len(token) >= 2}
    if tokens:
        tokens.add(raw)
    return tokens


def evidence_has_name(evidence: str, player_raw: str) -> bool:
    words = norm_words(evidence)
    compact_evidence = compact(evidence)
    for token in name_tokens(player_raw):
        if " " in token:
            if compact(token) and compact(token) in compact_evidence:
                return True
        elif re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", words):
            return True
    return False


def position_aliases(position: str) -> set[str]:
    position = clean(position)
    aliases = {position, norm_words(position), compact(position)}
    aliases.update(POSITION_ALIASES.get(position, set()))
    aliases.update(POSITION_ALIASES.get(position.upper(), set()))
    return {alias for alias in aliases if alias}


def evidence_has_position(evidence: str, position: str) -> bool:
    words = norm_words(evidence)
    compact_evidence = compact(evidence)
    for alias in position_aliases(position):
        alias_words = norm_words(alias)
        alias_compact = compact(alias)
        if not alias_words and not alias_compact:
            continue
        if len(alias_compact) <= 2:
            if re.search(rf"(?<![a-z0-9]){re.escape(alias_words or alias_compact)}(?![a-z0-9])", words):
                return True
        elif alias_compact in compact_evidence:
            return True
    return False


def row_is_safe(row: dict[str, Any]) -> tuple[bool, str, dict[str, str]]:
    if clean(row.get("route_to_lane")) != "ready_resolved_atom_review":
        return False, "not in ready_resolved_atom_review lane", {}

    proposed = parse_obj(row.get("proposed_fields_json"))
    fields = {
        "participation_type": clean(proposed.get("participation_type")),
        "player_raw": clean(proposed.get("player_raw")),
        "starter_position": clean(proposed.get("starter_position")),
        "listed_position_raw": clean(proposed.get("listed_position_raw")),
        "is_starter": clean(proposed.get("is_starter")),
    }
    for required in ["boxscore_id", "player_week", "NFL_player_id", "nfl_team", "opponent_nfl_team"]:
        if not clean(proposed.get(required)) and not clean(row.get(required)):
            return False, f"missing required {required}", fields
    if not clean(row.get("source_documents_json")):
        return False, "missing source document ids", fields

    evidence = evidence_blob(row, proposed)
    if not evidence:
        return False, "missing evidence text", fields

    player_raw = fields["player_raw"]
    if not player_raw:
        return False, "missing player_raw", fields
    if not evidence_has_name(evidence, player_raw):
        return False, f"evidence does not name player {player_raw}", fields

    participation_type = fields["participation_type"]
    if participation_type == "starter":
        position = fields["starter_position"] or fields["listed_position_raw"]
        if fields["is_starter"] != "1":
            return False, "starter row does not have is_starter=1", fields
        if not position:
            return False, "starter row missing position", fields
        if not evidence_has_position(evidence, position):
            return False, f"evidence does not show position {position}", fields
        return True, "direct starter line names player and position", fields

    if participation_type == "substitution":
        if not re.search(r"(?<![a-z0-9])for(?![a-z0-9])", norm_words(evidence)):
            return False, "substitution row lacks 'for' marker", fields
        return True, "direct substitution line names player and substitution marker", fields

    return False, f"participation_type requires manual review: {participation_type or '(blank)'}", fields


def build_rows(source_rows: list[dict[str, Any]], run_id: str, decision_run_id: str, created_at: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in source_rows:
        ok, reason, fields = row_is_safe(row)
        audit_row = {
            "safe_lineup_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "player_week": clean(row.get("player_week")),
            "NFL_player_id": clean(row.get("NFL_player_id")),
            "nfl_team": clean(row.get("nfl_team")),
            "opponent_nfl_team": clean(row.get("opponent_nfl_team")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "risk_level": clean(row.get("risk_level")),
            "participation_type": clean(fields.get("participation_type")),
            "starter_position": clean(fields.get("starter_position")),
            "listed_position_raw": clean(fields.get("listed_position_raw")),
            "is_starter": clean(fields.get("is_starter")),
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
                "route_to_lane": "local_final_lineup_participation",
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
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_lineup_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_lineup_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("safe_lineup_decision_run_id"))
        con.execute("DELETE FROM newspaper_review.resolved_safe_lineup_decision_run WHERE safe_lineup_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.resolved_safe_lineup_decision_item WHERE safe_lineup_decision_run_id = ?", [run_id])
        insert_rows(con, "resolved_safe_lineup_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_safe_lineup_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_safe_lineup_decisions")
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
    held_csv = out_dir / "held_lineup_candidates.csv"
    audit_csv = out_dir / "safe_lineup_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_type_counts = Counter(row["participation_type"] for row in audit if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)
    summary = {
        "safe_lineup_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_type_counts": dict(sorted(approved_type_counts.items())),
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
        "safe_lineup_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_type_counts_json": json.dumps(summary["approved_type_counts"], sort_keys=True),
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
