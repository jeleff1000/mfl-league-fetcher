#!/usr/bin/env python
"""Approve safe resolved newspaper game/score candidate rows.

This local-only station drains `game_candidate` rows from the resolved package
route queue when the newspaper candidate matches the local PFR team-game index
on date, team pair, and final score. Conflicts and wrong-game mappings remain
routed for review.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_newspaper_identity_resolution_from_readiness import (  # noqa: E402
    DEFAULT_TEAM_GAMES,
    clean,
    parse_int,
    query_dicts,
    resolve_team,
)


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_safe_game_candidate_decisions"

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
    "safe_game_candidate_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "game_date",
    "team_1_raw",
    "team_1_resolved",
    "team_1_score",
    "team_2_raw",
    "team_2_resolved",
    "team_2_score",
    "pfr_match_json",
    "decision_status",
    "decision_value",
    "reason",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "patched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "safe_game_candidate_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def load_team_games_with_scores(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    boxscore_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    if not boxscore_ids:
        return {}
    rel = str(path).replace("\\", "/").replace("'", "''")
    placeholders = ",".join(["?"] * len(boxscore_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT
          boxscore_id,
          CAST(game_date AS VARCHAR) AS game_date,
          TRY_CAST(year AS INTEGER) AS year,
          TRY_CAST(week AS INTEGER) AS week,
          season_type,
          team_code,
          opponent_code,
          TRY_CAST(team_points AS INTEGER) AS team_points,
          TRY_CAST(opponent_points AS INTEGER) AS opponent_points
        FROM read_parquet('{rel}')
        WHERE boxscore_id IN ({placeholders})
        ORDER BY boxscore_id, team_code
        """,
        boxscore_ids,
    )
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        output.setdefault(clean(row.get("boxscore_id")), []).append(row)
    return output


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


def score_int(value: Any) -> int | None:
    parsed = parse_int(value)
    return parsed if parsed is not None else None


def find_pfr_score_match(
    boxscore_id: str,
    t1: str,
    s1: int | None,
    t2: str,
    s2: int | None,
    team_games: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not boxscore_id or not t1 or not t2 or s1 is None or s2 is None:
        return {}
    for game in team_games.get(boxscore_id, []):
        team = clean(game.get("team_code"))
        opp = clean(game.get("opponent_code"))
        team_points = score_int(game.get("team_points"))
        opp_points = score_int(game.get("opponent_points"))
        if team == t1 and opp == t2 and team_points == s1 and opp_points == s2:
            return dict(game)
        if team == t2 and opp == t1 and team_points == s2 and opp_points == s1:
            return dict(game)
    return {}


def team_exists_for_boxscore(
    boxscore_id: str,
    team: str,
    team_games: dict[str, list[dict[str, Any]]],
) -> bool:
    return any(clean(game.get("team_code")) == team for game in team_games.get(boxscore_id, []))


def resolve_candidate_team(
    raw_team: str,
    resolved_team: str,
    boxscore_id: str,
    team_games: dict[str, list[dict[str, Any]]],
) -> str:
    team, _opp, _row, _method = resolve_team(raw_team, boxscore_id, team_games)
    if team:
        return team
    resolved = clean(resolved_team).upper()
    if re.fullmatch(r"[A-Z]{2,4}", resolved) and team_exists_for_boxscore(boxscore_id, resolved, team_games):
        return resolved
    return ""


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    team_games: dict[str, list[dict[str, Any]]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in rows:
        proposed = parse_obj(row.get("proposed_fields_json"))
        boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
        game_date = clean(proposed.get("game_date"))
        t1_raw = clean(proposed.get("team_1_raw"))
        t2_raw = clean(proposed.get("team_2_raw"))
        s1 = score_int(proposed.get("team_1_score"))
        s2 = score_int(proposed.get("team_2_score"))
        t1 = resolve_candidate_team(t1_raw, clean(proposed.get("team_1_resolved")), boxscore_id, team_games)
        t2 = resolve_candidate_team(t2_raw, clean(proposed.get("team_2_resolved")), boxscore_id, team_games)
        pfr_match = find_pfr_score_match(boxscore_id, t1, s1, t2, s2, team_games)
        pfr_date = clean(pfr_match.get("game_date"))

        if not source_date_matches(row, proposed):
            ok = False
            reason = "source document date does not match boxscore_id"
        elif not team_games.get(boxscore_id):
            ok = False
            reason = "boxscore_id not present in local PFR team games"
        elif not t1 or not t2:
            ok = False
            reason = "team label did not resolve to local PFR team code"
        elif not pfr_match:
            ok = False
            reason = "newspaper score/team pair does not match local PFR team-game score"
        elif game_date and pfr_date and game_date != pfr_date:
            ok = False
            reason = "candidate game_date differs from local PFR game_date"
        else:
            ok = True
            reason = "newspaper candidate matches local PFR date/team pair/final score"

        patched = dict(proposed)
        if ok:
            patched["team_1_resolved"] = t1
            patched["team_2_resolved"] = t2
            patched["reconciliation_status"] = "pfr_score_confirmed_local_newspaper_evidence"
            if pfr_date and not clean(patched.get("game_date")):
                patched["game_date"] = pfr_date

        patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
        audit_row = {
            "safe_game_candidate_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": boxscore_id,
            "game_date": game_date,
            "team_1_raw": t1_raw,
            "team_1_resolved": t1,
            "team_1_score": clean(proposed.get("team_1_score")),
            "team_2_raw": t2_raw,
            "team_2_resolved": t2,
            "team_2_score": clean(proposed.get("team_2_score")),
            "pfr_match_json": json.dumps(pfr_match, sort_keys=True, ensure_ascii=True) if pfr_match else "",
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else clean(row.get("decision_value")),
            "reason": reason,
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "patched_proposed_fields_json": patched_json,
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok:
            approved.append({
                "package_item_id": clean(row.get("package_item_id")),
                "decision_status": "approved",
                "decision_value": "approved_for_local_newspaper_final",
                "route_to_lane": "local_final_game_candidate",
                "target_table": "game_candidate",
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": boxscore_id,
                "proposed_fields_json": patched_json,
                "notes": reason,
            })
        else:
            held.append(audit_row)
    return approved, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_game_candidate_decision_run (
          safe_game_candidate_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          approved_count INTEGER,
          held_count INTEGER,
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
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_safe_game_candidate_decision_item (
          safe_game_candidate_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          package_item_id VARCHAR,
          route_to_lane VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          boxscore_id VARCHAR,
          game_date VARCHAR,
          team_1_raw VARCHAR,
          team_1_resolved VARCHAR,
          team_1_score VARCHAR,
          team_2_raw VARCHAR,
          team_2_resolved VARCHAR,
          team_2_score VARCHAR,
          pfr_match_json VARCHAR,
          decision_status VARCHAR,
          decision_value VARCHAR,
          reason VARCHAR,
          evidence_text VARCHAR,
          source_documents_json VARCHAR,
          proposed_fields_json VARCHAR,
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
        run_id = clean(run_row.get("safe_game_candidate_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_safe_game_candidate_decision_run WHERE safe_game_candidate_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_safe_game_candidate_decision_item WHERE safe_game_candidate_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_safe_game_candidate_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_safe_game_candidate_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Safe Game Candidate Decisions",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Hold reasons: `{summary['hold_reason_counts']}`",
        "",
        "## Rows",
        "",
        "| status | boxscore | teams | score | reason | evidence |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in audit_rows:
        teams = f"{row['team_1_raw']}=>{row['team_1_resolved']} / {row['team_2_raw']}=>{row['team_2_resolved']}"
        score = f"{row['team_1_score']}-{row['team_2_score']}"
        evidence = clean(row.get("evidence_text")).replace("|", "\\|")[:140]
        reason = clean(row.get("reason")).replace("|", "\\|")
        lines.append(
            f"| `{row['decision_status']}` | `{row['boxscore_id']}` | {teams} | {score} | "
            f"{reason} | {evidence} |"
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
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="safe_game_candidate_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
    finally:
        read_con.close()

    boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in rows if clean(row.get("boxscore_id"))})
    ext_con = duckdb.connect()
    try:
        team_games = load_team_games_with_scores(ext_con, args.team_games_path, boxscore_ids)
    finally:
        ext_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    approved, held, audit_rows = build_rows(rows, run_id, decision_run_id, team_games, created_at)
    hold_reason_counts = Counter(row["reason"] for row in held)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_game_candidate_rows.csv"
    audit_csv = out_dir / "safe_game_candidate_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "safe_game_candidate_decision_report.md"

    summary = {
        "safe_game_candidate_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "hold_reason_counts": dict(hold_reason_counts),
        "approved_csv": str(approved_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": args.dry_run,
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
                "safe_game_candidate_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": len(approved),
                "held_count": len(held),
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

    print(json.dumps({
        "safe_game_candidate_decision_run_id": run_id,
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "hold_reason_counts": dict(hold_reason_counts),
        "approved_csv": str(approved_csv),
        "report_md": str(report_md),
        "dry_run": args.dry_run,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
