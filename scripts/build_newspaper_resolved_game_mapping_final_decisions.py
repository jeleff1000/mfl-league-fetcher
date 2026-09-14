#!/usr/bin/env python
"""Resolve the last game-candidate mapping route rows.

This station handles newspaper game candidates that do not map cleanly onto a
PFR boxscore or that conflict with the PFR team-games file. It keeps the useful
newspaper-only candidates in `newspaper_final.game_candidate`, closes duplicate
evidence as corroboration, and rejects OCR/inversion conflicts.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_game_mapping_final_decisions"
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")

OVERRIDE_FIELDS = [
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
    "game_mapping_final_decision_run_id",
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
    "source_documents_json",
    "original_proposed_fields_json",
    "patched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "game_mapping_final_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "closed_count",
    "held_count",
    "approved_target_counts_json",
    "closed_target_counts_json",
    "approved_class_counts_json",
    "closed_class_counts_json",
    "hold_reason_counts_json",
    "overrides_csv",
    "approved_csv",
    "closed_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

GAME_DECISIONS = {
    "3918504451db5cdcd5ebd385cce65dcb": {
        "decision_status": "approved",
        "decision_value": "approved_for_local_newspaper_final",
        "route_to_lane": "local_promotion",
        "approval_class": "newspaper_only_multi_source_game_candidate",
        "reason": "three independent newspaper rows corroborate a Buffalo-Dayton 7-0 game claim not present as a PFR exact game",
        "canonical_target_entity_key": "game_candidate|newspaper_only|1922-10-22|BUF|DAY|7-0",
        "canonical_boxscore_id": "newspaper_19221022_buf_day",
        "patch_fields": {
            "game_candidate_id": "newspaper_19221022_buf_day_7_0",
            "boxscore_id": "newspaper_19221022_buf_day",
            "game_date": "1922-10-22",
            "year": "1922",
            "week": "4",
            "team_1_raw": "Buffalo All-Americans",
            "team_1_resolved": "BUF",
            "team_1_score": "7",
            "team_2_raw": "Dayton Triangles",
            "team_2_resolved": "DAY",
            "team_2_score": "0",
            "reconciliation_status": "newspaper_only_multi_source_non_pfr_candidate",
            "confidence_score": "92",
            "source_document_id": "192210220day#3:442545013",
            "evidence_text": "Three newspaper extracts claim Buffalo All-Americans defeated Dayton Triangles 7-0 on 1922-10-22.",
            "source_documents_json": "[\"192210220day#1:87562644\", \"192210220day#2:441654854\", \"192210220day#3:442545013\"]",
            "source_materialized_row_ids_json": "[\"1ec90269f07f8fb1c638be4a02d96faa\", \"f31ab600db82d6bf72590d9b4fc0dd9f\", \"d348ff22e802655392e3a2d8a1d247a3\"]",
        },
        "source_package_ids": [
            "3918504451db5cdcd5ebd385cce65dcb",
            "44fbc9a20b6a7d223b279185cca13004",
            "ceb5a35f21fb1678dbdbbbfa07569329",
        ],
    },
    "44fbc9a20b6a7d223b279185cca13004": {
        "decision_status": "closed",
        "decision_value": "corroboration_archive",
        "route_to_lane": "archive",
        "approval_class": "duplicate_game_candidate_corrob_source",
        "reason": "source is retained as corroboration for canonical Buffalo-Dayton newspaper-only game candidate",
        "canonical_package_item_id": "3918504451db5cdcd5ebd385cce65dcb",
    },
    "ceb5a35f21fb1678dbdbbbfa07569329": {
        "decision_status": "closed",
        "decision_value": "corroboration_archive",
        "route_to_lane": "archive",
        "approval_class": "duplicate_game_candidate_corrob_source",
        "reason": "source is retained as corroboration for canonical Buffalo-Dayton newspaper-only game candidate",
        "canonical_package_item_id": "3918504451db5cdcd5ebd385cce65dcb",
    },
    "fed7c9f62cce6678b6c6d386f3cc73f4": {
        "decision_status": "approved",
        "decision_value": "approved_for_local_newspaper_final",
        "route_to_lane": "local_promotion",
        "approval_class": "newspaper_only_single_source_game_candidate",
        "reason": "newspaper row claims Milwaukee-Hammond 0-0; retain as a candidate because it does not map to the PFR exact game list",
        "canonical_target_entity_key": "game_candidate|newspaper_only|1922-10-22|MIL|HAM|0-0",
        "canonical_boxscore_id": "newspaper_19221022_mil_ham",
        "patch_fields": {
            "game_candidate_id": "newspaper_19221022_mil_ham_0_0",
            "boxscore_id": "newspaper_19221022_mil_ham",
            "game_date": "1922-10-22",
            "year": "1922",
            "week": "4",
            "team_1_raw": "Milwaukee Badgers",
            "team_1_resolved": "MIL",
            "team_1_score": "0",
            "team_2_raw": "Hammond Pros",
            "team_2_resolved": "HAM",
            "team_2_score": "0",
            "reconciliation_status": "newspaper_only_single_source_non_pfr_candidate",
            "confidence_score": "88",
            "source_document_id": "192210220mil#2:520386283",
            "evidence_text": "Newspaper extract claims Milwaukee Badgers and Hammond Pros fought to a 0-0 standstill on 1922-10-22.",
            "source_documents_json": "[\"192210220mil#2:520386283\"]",
            "source_materialized_row_ids_json": "[\"f85d94972e0135f9338f8b3d865e43cf\"]",
        },
    },
    "78372e3645a526d6e3542473d714ea1d": {
        "decision_status": "closed",
        "decision_value": "archive",
        "route_to_lane": "archive",
        "approval_class": "non_nfl_or_alt_league_game_candidate",
        "reason": "Cleveland Panthers-New York Yankees 10-0 appears to be an AFL/alternate-league score, not an NFL/PFR target game",
    },
    "9d9dbb1574c436a7014665c4a2833de9": {
        "decision_status": "closed",
        "decision_value": "reject",
        "route_to_lane": "archive",
        "approval_class": "pfr_score_conflict_rejected",
        "reason": "source/OCR score claim CHI 9 PRV 5 conflicts with local PFR team-games row PRV 9 CHI 6",
    },
    "5c692d5aebb86ba3feab76dc591ad823": {
        "decision_status": "closed",
        "decision_value": "reject",
        "route_to_lane": "archive",
        "approval_class": "pfr_score_inversion_rejected",
        "reason": "source/OCR score claim PRT 9 CHI 6 inverts the local PFR team-games row CHI 9 PRT 6",
    },
}


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
          AND route_to_lane IN ('manual_game_mapping_review', 'score_conflict_review')
        ORDER BY route_to_lane, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def load_pfr_date_rows(path: Path) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}
    con = duckdb.connect()
    try:
        path_text = str(path).replace("\\", "/")
        rows = query_dicts(
            con,
            f"""
            SELECT boxscore_id, game_date, year, week, team_code, opponent_code,
                   result, team_points, opponent_points, coverage_status, boxscore_url
            FROM read_parquet('{path_text}')
            WHERE game_date IN ('1922-10-22', '1925-12-09', '1926-09-26', '1931-11-08')
            ORDER BY game_date, boxscore_id, team_code
            """,
        )
    finally:
        con.close()
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        output.setdefault(clean(row.get("game_date")), []).append(row)
    return output


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    pfr_rows_by_date: dict[str, list[dict[str, Any]]],
    created_at: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    overrides = []
    approved = []
    closed = []
    held = []
    audit_rows = []
    for row in rows:
        package_item_id = clean(row.get("package_item_id"))
        proposed = parse_obj(row.get("proposed_fields_json"))
        spec = GAME_DECISIONS.get(package_item_id)
        if not spec:
            spec = {
                "decision_status": "held",
                "decision_value": "needs_review",
                "route_to_lane": clean(row.get("route_to_lane")),
                "approval_class": "held_game_mapping",
                "reason": "no final game-mapping rule for routed row",
            }

        patched = dict(proposed)
        patched.update(spec.get("patch_fields", {}))
        patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
        target_entity_key = clean(spec.get("canonical_target_entity_key")) or clean(row.get("target_entity_key"))
        boxscore_id = clean(spec.get("canonical_boxscore_id")) or clean(row.get("boxscore_id"))
        support_details = {
            "configured_reason": spec["reason"],
            "source_package_ids": spec.get("source_package_ids", [package_item_id]),
            "canonical_package_item_id": spec.get("canonical_package_item_id", ""),
            "local_pfr_team_games_same_date": pfr_rows_by_date.get(clean(proposed.get("game_date")), []),
        }
        audit = {
            "game_mapping_final_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "target_table": clean(row.get("target_table")),
            "target_entity_key": target_entity_key,
            "boxscore_id": boxscore_id,
            "route_to_lane": clean(row.get("route_to_lane")),
            "decision_status": spec["decision_status"],
            "decision_value": spec["decision_value"],
            "approval_class": spec["approval_class"],
            "reason": spec["reason"],
            "support_details_json": json.dumps(support_details, sort_keys=True, ensure_ascii=True),
            "source_documents_json": clean(row.get("source_documents_json")),
            "original_proposed_fields_json": json.dumps(proposed, sort_keys=True, ensure_ascii=True),
            "patched_proposed_fields_json": patched_json,
            "created_at_utc": created_at,
        }
        audit_rows.append(audit)
        if spec["decision_status"] == "held":
            held.append(audit)
            continue
        override = {
            "package_item_id": package_item_id,
            "decision_status": spec["decision_status"],
            "decision_value": spec["decision_value"],
            "route_to_lane": spec["route_to_lane"],
            "target_table": clean(row.get("target_table")),
            "target_entity_key": target_entity_key,
            "boxscore_id": boxscore_id,
            "proposed_fields_json": patched_json,
            "notes": f"{spec['approval_class']}: {spec['reason']}",
        }
        overrides.append(override)
        if spec["decision_status"] == "approved":
            approved.append(override)
        elif spec["decision_status"] == "closed":
            closed.append(override)
    return overrides, approved, closed, held, audit_rows


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        "CREATE TABLE IF NOT EXISTS newspaper_review.resolved_game_mapping_final_decision_run ("
        + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
        + ")"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS newspaper_review.resolved_game_mapping_final_decision_item ("
        + ", ".join(f"{field} VARCHAR" for field in AUDIT_FIELDS)
        + ")"
    )
    for table, fields in [
        ("resolved_game_mapping_final_decision_run", RUN_FIELDS),
        ("resolved_game_mapping_final_decision_item", AUDIT_FIELDS),
    ]:
        existing = {
            row[1]
            for row in con.execute(f"PRAGMA table_info('newspaper_review.{table}')").fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(db_path: Path, run_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.resolved_game_mapping_final_decision_run WHERE game_mapping_final_decision_run_id = ?",
            [run_row["game_mapping_final_decision_run_id"]],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_game_mapping_final_decision_item WHERE game_mapping_final_decision_run_id = ?",
            [run_row["game_mapping_final_decision_run_id"]],
        )
        insert_rows(con, "resolved_game_mapping_final_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_game_mapping_final_decision_item", item_rows, AUDIT_FIELDS)
    finally:
        con.close()


def markdown_escape(value: Any) -> str:
    return clean(value).replace("|", "\\|").replace("\n", " ")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    if not rows:
        return ["_(none)_"]
    lines = [
        "| " + " | ".join(fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(markdown_escape(row.get(field, "")) for field in fields) + " |")
    return lines


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Game-Mapping Final Decisions",
        "",
        f"- Run: `{summary['game_mapping_final_decision_run_id']}`",
        f"- Decision ledger: `{summary['resolved_package_decision_run_id']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Closed: `{summary['closed_count']}`",
        f"- Held: `{summary['held_count']}`",
        "",
        "## Decisions",
        "",
    ]
    lines.extend(
        markdown_table(
            audit_rows,
            ["decision_status", "approval_class", "package_item_id", "boxscore_id", "reason"],
        )
    )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="1920_1939_game_mapping_final_v1")
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

    pfr_rows_by_date = load_pfr_date_rows(args.team_games_path)
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    overrides, approved, closed, held, audit_rows = build_rows(rows, run_id, decision_run_id, pfr_rows_by_date, created_at)

    approved_target_counts = Counter(row["target_table"] for row in approved)
    closed_target_counts = Counter(row["target_table"] for row in closed)
    approved_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "approved")
    closed_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "closed")
    hold_reason_counts = Counter(row["reason"] for row in held)

    overrides_csv = out_dir / "approved_decision_overrides.csv"
    approved_csv = out_dir / "approved_game_mapping_overrides.csv"
    closed_csv = out_dir / "closed_game_mapping_overrides.csv"
    held_csv = out_dir / "held_game_mapping_rows.csv"
    audit_csv = out_dir / "game_mapping_final_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "game_mapping_final_decision_report.md"

    summary = {
        "game_mapping_final_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "team_games_path": str(args.team_games_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "closed_count": len(closed),
        "held_count": len(held),
        "approved_target_counts": dict(approved_target_counts),
        "closed_target_counts": dict(closed_target_counts),
        "approved_class_counts": dict(approved_class_counts),
        "closed_class_counts": dict(closed_class_counts),
        "hold_reason_counts": dict(hold_reason_counts),
        "overrides_csv": str(overrides_csv),
        "approved_csv": str(approved_csv),
        "closed_csv": str(closed_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }

    write_csv(overrides_csv, overrides, OVERRIDE_FIELDS)
    write_csv(approved_csv, approved, OVERRIDE_FIELDS)
    write_csv(closed_csv, closed, OVERRIDE_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit_rows, AUDIT_FIELDS)
    write_json(summary_json, summary)
    report_md.write_text(render_markdown(summary, audit_rows), encoding="utf-8")

    if not args.dry_run:
        persist(
            args.db_path,
            {
                "game_mapping_final_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": len(approved),
                "closed_count": len(closed),
                "held_count": len(held),
                "approved_target_counts_json": json.dumps(dict(approved_target_counts), sort_keys=True),
                "closed_target_counts_json": json.dumps(dict(closed_target_counts), sort_keys=True),
                "approved_class_counts_json": json.dumps(dict(approved_class_counts), sort_keys=True),
                "closed_class_counts_json": json.dumps(dict(closed_class_counts), sort_keys=True),
                "hold_reason_counts_json": json.dumps(dict(hold_reason_counts), sort_keys=True),
                "overrides_csv": str(overrides_csv),
                "approved_csv": str(approved_csv),
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
