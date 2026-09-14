#!/usr/bin/env python
"""Approve rows where a source document is visually verified for another game.

Some article crops were acquired under a seed boxscore but visibly describe a
different PFR game. This station handles those cases only when a visual-support
map proves the article headline/score/teams match the target game. It emits
ordinary decision overrides and records the visual proof path in audit tables.
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
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_source_game_remap_decisions"

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
    "source_game_remap_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "source_document_id",
    "decision_status",
    "decision_value",
    "approval_class",
    "reason",
    "visual_support_json",
    "source_metadata_json",
    "game_context_json",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "source_game_remap_decision_run_id",
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

VISUAL_REMAPS = {
    ("192110300gnb", "192110230dti#1:302660628"): {
        "crop_path": r"D:\league-history-data\nfl\derived\newspaper_atoms\article_atom_rounds\20260625T140645Z_1920_1939_batch0001_article_lane_chunk04_v1\article_crops\192110230dti\192110230dti_1_302660628_a01_r01_headline_wide_story_band.png",
        "headline": "Independents Defeat Green Bay, 13 to 3",
        "issue_date_from_path": "1921-10-31",
        "target_game_date": "1921-10-30",
        "teams": {"RII": 13, "GNB": 3},
        "note": "Visual crop shows the Green Bay, Wis. Oct. 31 article and summary for Rock Island 13, Green Bay 3.",
    }
}

APPROVED_PACKAGE_ITEMS = {
    # Visual lineup/summary identity bridge rows.
    "12d74d6a5c26074c1e9a80f0882cf662": "visual_lineup_identity",
    "4412cb4f573bcd917f6a04f292943823": "visual_lineup_identity",
    "5150d620367128687e44920304632e35": "visual_substitution_identity",
    "5b4f7de9042a28495f2a0a0753f1b68c": "visual_lineup_identity",
    "916bd1c01a06d79fdd4ee1293a64f43d": "visual_lineup_identity",
    "c15841344374d341740c0d417008ad8e": "visual_substitution_identity",
    "f52e138927dea9b9d5ac1cb0fe514290": "visual_lineup_identity",
    # Canonical event/stat rows from the visually verified article.
    "6ec5feeead03669076d705930d9592c0": "visual_article_pbp",
    "3c74bd2d8ee12b7d547f6aa9bc9659c0": "visual_article_box_score",
    "03b8c51ece786bf6b5cd08ac481b945e": "visual_article_scoring",
    "2e34cdae95c7d9d3ece61260e17e939b": "visual_article_scoring",
    "3a82422d38c0362041a02b8b65f28dd5": "visual_article_scoring",
    "3e989d92ec172079cf57f201e0f2ba35": "visual_article_scoring",
}

DUPLICATE_PACKAGE_ITEMS = {
    "19bd572c68a59027c942a9bd5f402c7f": "duplicate Lambeau 25-yard field goal; canonical package 3e989d92ec172079cf57f201e0f2ba35 approved",
    "75827c01fd799e053bdedac381e74cf6": "duplicate Novak 1-yard rushing touchdown; canonical package 2e34cdae95c7d9d3ece61260e17e939b approved",
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
    boxscore_ids = sorted({key[0] for key in VISUAL_REMAPS})
    placeholders = ",".join("?" for _ in boxscore_ids)
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.resolved_package_decision_route_queue
        WHERE resolved_package_decision_run_id = ?
          AND boxscore_id IN ({placeholders})
        ORDER BY boxscore_id, target_table, package_item_id
        """,
        [decision_run_id, *boxscore_ids],
    )


def source_doc_for_row(row: dict[str, Any]) -> str:
    docs = parse_list(row.get("source_documents_json"))
    for doc in docs:
        if (clean(row.get("boxscore_id")), doc) in VISUAL_REMAPS:
            return doc
    return docs[0] if docs else ""


def load_source_metadata(con: duckdb.DuckDBPyConnection, source_docs: list[str]) -> dict[str, dict[str, Any]]:
    if not source_docs:
        return {}
    placeholders = ",".join("?" for _ in source_docs)
    rows = query_dicts(
        con,
        f"""
        SELECT source_document_id, boxscore_id, publication, issue_date, page, source_url, asset_pdf_path, capture_type
        FROM newspaper_review.source_document
        WHERE source_document_id IN ({placeholders})
        """,
        source_docs,
    )
    return {clean(row.get("source_document_id")): row for row in rows}


def load_game_context(path: Path, boxscore_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not boxscore_ids:
        return {}
    con = duckdb.connect(":memory:")
    try:
        placeholders = ",".join("?" for _ in boxscore_ids)
        rows = query_dicts(
            con,
            f"""
            SELECT boxscore_id, game_date, week, team_code, opponent_code, team_points, opponent_points, is_home, is_away
            FROM read_parquet(?)
            WHERE boxscore_id IN ({placeholders})
            ORDER BY boxscore_id, team_code
            """,
            [str(path), *boxscore_ids],
        )
    finally:
        con.close()
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        output.setdefault(clean(row.get("boxscore_id")), []).append(row)
    return output


def visual_support_ok(boxscore_id: str, source_doc: str, source_meta: dict[str, Any], game_rows: list[dict[str, Any]]) -> tuple[bool, str, dict[str, Any]]:
    support = VISUAL_REMAPS.get((boxscore_id, source_doc), {})
    if not support:
        return False, "no visual remap support for source document and boxscore", {}
    crop = Path(clean(support.get("crop_path")))
    if not crop.exists():
        return False, f"visual crop missing: {crop}", support

    teams = support.get("teams", {})
    if not isinstance(teams, dict) or not teams:
        return False, "visual support missing expected teams/scores", support
    game_scores = {clean(row.get("team_code")): clean(row.get("team_points")) for row in game_rows}
    for team, points in teams.items():
        if game_scores.get(team) != str(points):
            return False, f"PFR score mismatch for {team}: expected {points}, got {game_scores.get(team)}", support

    target_date = clean(support.get("target_game_date"))
    dates = {clean(row.get("game_date")) for row in game_rows if clean(row.get("game_date"))}
    if target_date and target_date not in dates:
        return False, f"PFR game date mismatch: expected {target_date}, got {sorted(dates)}", support

    asset_path = clean(source_meta.get("asset_pdf_path"))
    issue_token = clean(support.get("issue_date_from_path"))
    if issue_token and issue_token not in asset_path:
        return False, f"source asset path does not include visual issue date {issue_token}", support

    return True, "visual crop verifies source document describes target boxscore", support


def classify_row(row: dict[str, Any], source_meta: dict[str, Any], game_context: dict[str, list[dict[str, Any]]]) -> tuple[bool, str, str, dict[str, Any]]:
    package_item_id = clean(row.get("package_item_id"))
    boxscore_id = clean(row.get("boxscore_id"))
    source_doc = source_doc_for_row(row)
    ok, support_reason, support = visual_support_ok(boxscore_id, source_doc, source_meta, game_context.get(boxscore_id, []))
    if not ok:
        return False, "held_source_game_remap", support_reason, support
    if package_item_id in APPROVED_PACKAGE_ITEMS:
        return True, APPROVED_PACKAGE_ITEMS[package_item_id], support_reason, support
    if package_item_id in DUPLICATE_PACKAGE_ITEMS:
        return False, "held_duplicate_visual_remap", DUPLICATE_PACKAGE_ITEMS[package_item_id], support
    return False, "held_unmapped_visual_remap", "visual remap verified, but row is not in the canonical approval map", support


def build_rows(
    source_rows: list[dict[str, Any]],
    source_metadata: dict[str, dict[str, Any]],
    game_context: dict[str, list[dict[str, Any]]],
    run_id: str,
    decision_run_id: str,
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in source_rows:
        source_doc = source_doc_for_row(row)
        meta = source_metadata.get(source_doc, {})
        game_rows = game_context.get(clean(row.get("boxscore_id")), [])
        ok, approval_class, reason, support = classify_row(row, meta, game_context)
        close_duplicate = approval_class == "held_duplicate_visual_remap"
        decision_status = "approved" if ok else "closed" if close_duplicate else "held"
        decision_value = "approved_for_local_newspaper_final" if ok else "archive" if close_duplicate else "hold_for_review"
        audit_row = {
            "source_game_remap_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": clean(row.get("package_item_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "source_document_id": source_doc,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "approval_class": approval_class,
            "reason": reason,
            "visual_support_json": json.dumps(support, ensure_ascii=True, sort_keys=True),
            "source_metadata_json": json.dumps(meta, default=str, ensure_ascii=True, sort_keys=True),
            "game_context_json": json.dumps(game_rows, default=str, ensure_ascii=True, sort_keys=True),
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok or close_duplicate:
            approved.append(
                {
                    "package_item_id": clean(row.get("package_item_id")),
                    "decision_status": decision_status,
                    "decision_value": decision_value,
                    "route_to_lane": f"local_final_source_game_remap_{approval_class}" if ok else "closed_source_game_remap_duplicate",
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
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_source_game_remap_decision_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_source_game_remap_decision_run ({run_defs})")


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
        run_id = clean(run_row.get("source_game_remap_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_source_game_remap_decision_run WHERE source_game_remap_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_source_game_remap_decision_item WHERE source_game_remap_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "resolved_source_game_remap_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_source_game_remap_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_source_game_remap_decisions")
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
        source_docs = sorted({source_doc_for_row(row) for row in source_rows if source_doc_for_row(row)})
        source_metadata = load_source_metadata(con, source_docs)
    finally:
        con.close()
    if not decision_run_id:
        raise SystemExit("No resolved package decision run found")

    boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in source_rows if clean(row.get("boxscore_id"))})
    game_context = load_game_context(args.team_games_path, boxscore_ids)
    approved, held, audit = build_rows(source_rows, source_metadata, game_context, run_id, decision_run_id, created_at)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_source_game_remap_candidates.csv"
    audit_csv = out_dir / "source_game_remap_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit, AUDIT_FIELDS)

    approved_count = sum(1 for row in audit if row["decision_status"] == "approved")
    closed_count = sum(1 for row in audit if row["decision_status"] == "closed")
    approved_target_counts = Counter(row["target_table"] for row in audit if row["decision_status"] == "approved")
    approved_class_counts = Counter(row["approval_class"] for row in audit if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)

    summary = {
        "source_game_remap_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "input_row_count": len(source_rows),
        "approved_count": approved_count,
        "closed_count": closed_count,
        "held_count": len(held),
        "approved_target_counts": dict(sorted(approved_target_counts.items())),
        "approved_class_counts": dict(sorted(approved_class_counts.items())),
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
        "source_game_remap_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "output_dir": str(out_dir),
        "input_row_count": str(len(source_rows)),
        "approved_count": str(len(approved)),
        "held_count": str(len(held)),
        "approved_target_counts_json": json.dumps(summary["approved_target_counts"], sort_keys=True),
        "approved_class_counts_json": json.dumps(summary["approved_class_counts"], sort_keys=True),
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
