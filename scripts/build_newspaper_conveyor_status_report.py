#!/usr/bin/env python
"""Build a status report for the D-drive newspaper atom conveyor.

This is the "where are we and what moves next?" station. It summarizes the
latest coverage queues, document queues, LLM packet/read-state/ingest/materialize
runs, and local DuckDB counts without writing to live supertable/Fly tables.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "conveyor_status_reports"

DB_TABLES = [
    "source_document",
    "source_text_pass",
    "source_region",
    "atom_claim",
    "game_candidate",
    "scoring_event",
    "play_by_play_event",
    "player_game_box_score",
    "lineup_participation",
    "player_identity_candidate",
    "promotion_candidate",
    "conveyor_document_state",
    "llm_document_review",
    "llm_review_claim",
    "llm_review_followup",
    "llm_game_candidate",
    "llm_scoring_event",
    "llm_play_by_play_event",
    "llm_player_game_box_score",
    "llm_lineup_participation",
    "llm_player_identity_candidate",
    "llm_promotion_candidate",
    "llm_promotion_package",
    "llm_promotion_package_item",
    "llm_promotion_conflict",
    "llm_promotion_package_run",
    "llm_conveyor_action_queue",
    "llm_conveyor_action_queue_run",
    "llm_ocr_followup_prep_action",
    "llm_ocr_followup_prep_document",
    "llm_ocr_followup_prep_run",
    "llm_ocr_followup_round_manifest",
    "llm_ocr_followup_round_manifest_run",
    "llm_semantic_followup_prep_action",
    "llm_semantic_followup_prep_document",
    "llm_semantic_followup_prep_run",
    "llm_quality_review_prep_package",
    "llm_quality_review_prep_item",
    "llm_quality_review_prep_run",
    "llm_promotion_review_prep_package",
    "llm_promotion_review_prep_item",
    "llm_promotion_review_prep_run",
    "llm_review_decision_ledger",
    "llm_review_decision_ledger_run",
    "review_decision_apply_run",
    "review_decision_applied_promotion",
    "review_decision_route_queue",
    "schema_gap_resolution_item",
    "schema_gap_resolution_run",
    "team_game_stat_resolution_item",
    "team_game_stat_resolution_run",
    "identity_resolution_readiness_run",
    "identity_resolution_readiness_task",
    "identity_resolution_readiness_candidate",
    "readiness_promotion_lane_run",
    "readiness_promotion_lane_item",
    "resolved_atom_materialize_run",
    "resolved_schema_gap_patch_run",
    "resolved_schema_gap_patch_item",
    "resolved_game_mapping_triage_run",
    "resolved_game_mapping_triage_item",
    "resolved_promotion_package_run",
    "resolved_promotion_package_item",
    "resolved_package_review_packet_run",
    "resolved_package_review_packet",
    "resolved_package_review_packet_item",
    "resolved_package_decision_ledger_run",
    "resolved_package_decision_ledger",
    "resolved_package_decision_route_queue",
    "resolved_package_decision_closed",
    "resolved_safe_stat_decision_run",
    "resolved_safe_stat_decision_item",
    "resolved_safe_lineup_decision_run",
    "resolved_safe_lineup_decision_item",
    "resolved_safe_scoring_decision_run",
    "resolved_safe_scoring_decision_item",
    "resolved_safe_team_stat_decision_run",
    "resolved_safe_team_stat_decision_item",
    "resolved_safe_pbp_decision_run",
    "resolved_safe_pbp_decision_item",
    "resolved_safe_identity_candidate_decision_run",
    "resolved_safe_identity_candidate_decision_item",
    "resolved_identity_event_decision_run",
    "resolved_identity_event_decision_item",
    "resolved_safe_game_candidate_decision_run",
    "resolved_safe_game_candidate_decision_item",
    "resolved_game_candidate_cleanup_decision_run",
    "resolved_game_candidate_cleanup_decision_item",
    "resolved_ready_partial_decision_run",
    "resolved_ready_partial_decision_item",
    "resolved_source_context_decision_run",
    "resolved_source_context_decision_item",
    "resolved_roster_identity_decision_run",
    "resolved_roster_identity_decision_item",
    "resolved_game_mapping_final_decision_run",
    "resolved_game_mapping_final_decision_item",
    "resolved_direct_source_decision_run",
    "resolved_direct_source_decision_item",
    "resolved_touchdown_total_decision_run",
    "resolved_touchdown_total_decision_item",
    "resolved_source_game_remap_decision_run",
    "resolved_source_game_remap_decision_item",
    "resolved_second_pass_decision_run",
    "resolved_second_pass_decision_item",
    "resolved_package_decision_apply_run",
    "resolved_package_applied_final_row",
    "generated_atom_decision",
    "generated_atom_decision_run",
    "batch_closeout_audit_run",
    "batch_closeout_audit_item",
    "llm_review_ingest_run",
    "llm_materialize_run",
    "page_coverage_audit_run",
    "page_coverage_audit_item",
    "page_visual_review_packet_run",
    "page_visual_review_packet",
    "page_visual_review_packet_item",
    "page_visual_review_ingest_run",
    "page_visual_review_decision",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_read_error": f"{type(exc).__name__}: {exc}"}


def csv_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(0, sum(1 for _ in csv.DictReader(handle)))


def read_csv_rows(path: Path, max_rows: int = 5) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = []
        for row in csv.DictReader(handle):
            rows.append(row)
            if len(rows) >= max_rows:
                break
        return rows


def read_csv_all(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def latest_dir(root: Path) -> Path | None:
    if not root.exists():
        return None
    dirs = sorted(path for path in root.iterdir() if path.is_dir())
    return dirs[-1] if dirs else None


def latest_dirs(root: Path, limit: int = 5) -> list[Path]:
    if not root.exists():
        return []
    return list(reversed(sorted(path for path in root.iterdir() if path.is_dir())[-limit:]))


def summarize_run(root: Path, name: str) -> dict[str, Any]:
    path = latest_dir(root / name)
    if not path:
        return {"status": "missing", "path": ""}
    summary = read_json(path / "summary.json")
    return {
        "status": "found",
        "path": str(path),
        "summary": summary,
    }


def load_db_counts(db_path: Path) -> tuple[dict[str, int | str], list[str]]:
    if not db_path.exists():
        return {}, [f"db_missing: {db_path}"]
    warnings: list[str] = []
    counts: dict[str, int | str] = {}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, [f"db_unavailable: {type(exc).__name__}: {exc}"]
    try:
        for table in DB_TABLES:
            try:
                counts[table] = con.execute(f"SELECT COUNT(*) FROM newspaper_review.{table}").fetchone()[0]
            except Exception as exc:
                counts[table] = f"missing_or_error:{type(exc).__name__}"
        return counts, warnings
    finally:
        con.close()


def db_table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and row[0])


def query_dicts(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | tuple[Any, ...] | None = None,
) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    columns = [col[0] for col in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def load_page_visual_progress(db_path: Path, packet_run_id: str, packet_item_count: int) -> dict[str, Any]:
    if not db_path.exists() or not packet_run_id:
        return {
            "status": "missing",
            "page_visual_review_packet_run_id": packet_run_id,
            "packet_item_count": packet_item_count,
        }
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {
            "status": "db_unavailable",
            "error": f"{type(exc).__name__}: {exc}",
            "page_visual_review_packet_run_id": packet_run_id,
            "packet_item_count": packet_item_count,
        }
    try:
        if not db_table_exists(con, "newspaper_review", "page_visual_review_decision"):
            return {
                "status": "missing_table",
                "page_visual_review_packet_run_id": packet_run_id,
                "packet_item_count": packet_item_count,
                "reviewed_item_count": 0,
                "remaining_item_count": packet_item_count,
            }
        packet_item_scope = ""
        if db_table_exists(con, "newspaper_review", "page_visual_review_packet_item"):
            packet_item_scope = """
              AND EXISTS (
                SELECT 1
                FROM newspaper_review.page_visual_review_packet_item AS packet_item
                WHERE packet_item.page_visual_review_packet_run_id = decision.page_visual_review_packet_run_id
                  AND packet_item.visual_review_item_id = decision.visual_review_item_id
              )
            """
        rows = query_dicts(
            con,
            """
            SELECT
              COUNT(DISTINCT visual_review_item_id) AS reviewed_item_count,
              COUNT(*) AS decision_row_count,
              SUM(CASE WHEN decision = 'extract_atoms' THEN 1 ELSE 0 END) AS extract_decision_count,
              SUM(COALESCE(CAST(NULLIF(generated_decision_count, '') AS INTEGER), 0)) AS generated_decision_count
            FROM newspaper_review.page_visual_review_decision AS decision
            WHERE decision.page_visual_review_packet_run_id = ?
            """ + packet_item_scope,
            [packet_run_id],
        )
        rollup = rows[0] if rows else {}
        decision_counts = {
            row["decision"]: row["n"]
            for row in query_dicts(
                con,
                """
                SELECT decision, COUNT(*) AS n
                FROM newspaper_review.page_visual_review_decision AS decision
                WHERE decision.page_visual_review_packet_run_id = ?
                """ + packet_item_scope + """
                GROUP BY decision
                ORDER BY decision
                """,
                [packet_run_id],
            )
        }
        target_counts = {
            row["target_table"]: row["n"]
            for row in query_dicts(
                con,
                """
                SELECT target_table, COUNT(*) AS n
                FROM newspaper_review.generated_atom_decision
                WHERE source_prep_run_id = ?
                GROUP BY target_table
                ORDER BY target_table
                """,
                [packet_run_id],
            )
        } if db_table_exists(con, "newspaper_review", "generated_atom_decision") else {}
        reviewed = as_int(rollup.get("reviewed_item_count"))
        return {
            "status": "found",
            "page_visual_review_packet_run_id": packet_run_id,
            "packet_item_count": packet_item_count,
            "reviewed_item_count": reviewed,
            "remaining_item_count": max(0, packet_item_count - reviewed),
            "decision_row_count": as_int(rollup.get("decision_row_count")),
            "extract_decision_count": as_int(rollup.get("extract_decision_count")),
            "generated_decision_count": as_int(rollup.get("generated_decision_count")),
            "decision_counts": decision_counts,
            "generated_target_table_counts": target_counts,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "page_visual_review_packet_run_id": packet_run_id,
            "packet_item_count": packet_item_count,
        }
    finally:
        con.close()


def load_resolved_counts(db_path: Path) -> tuple[dict[str, int | str], list[str]]:
    if not db_path.exists():
        return {}, [f"db_missing: {db_path}"]
    warnings: list[str] = []
    counts: dict[str, int | str] = {}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, [f"db_unavailable: {type(exc).__name__}: {exc}"]
    try:
        tables = [
            row[0]
            for row in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='newspaper_resolved'
                ORDER BY table_name
                """
            ).fetchall()
        ]
        for table in tables:
            try:
                counts[table] = con.execute(f"SELECT COUNT(*) FROM newspaper_resolved.{table}").fetchone()[0]
            except Exception as exc:
                counts[table] = f"missing_or_error:{type(exc).__name__}"
        return counts, warnings
    finally:
        con.close()


def load_resolved_current_counts(db_path: Path) -> tuple[dict[str, Any], list[str]]:
    if not db_path.exists():
        return {}, [f"db_missing: {db_path}"]
    warnings: list[str] = []
    payload: dict[str, Any] = {"resolved_atom_materialize_run_id": "", "table_counts": {}}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, [f"db_unavailable: {type(exc).__name__}: {exc}"]
    try:
        row = con.execute(
            """
            SELECT resolved_atom_materialize_run_id
            FROM newspaper_review.resolved_atom_materialize_run
            ORDER BY created_at_utc DESC, resolved_atom_materialize_run_id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_run_id = str(row[0]).strip() if row and row[0] is not None else ""
        payload["resolved_atom_materialize_run_id"] = latest_run_id
        if not latest_run_id:
            warnings.append("resolved_current_missing_materialize_run")
            return payload, warnings

        tables = [
            table_row[0]
            for table_row in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='newspaper_resolved'
                ORDER BY table_name
                """
            ).fetchall()
        ]
        counts: dict[str, int | str] = {}
        for table in tables:
            has_run_col = con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema='newspaper_resolved'
                  AND table_name=?
                  AND column_name='resolved_atom_materialize_run_id'
                """,
                [table],
            ).fetchone()[0]
            if not has_run_col:
                counts[table] = "missing_run_column"
                continue
            try:
                counts[table] = con.execute(
                    f"SELECT COUNT(*) FROM newspaper_resolved.{table} WHERE resolved_atom_materialize_run_id = ?",
                    [latest_run_id],
                ).fetchone()[0]
            except Exception as exc:
                counts[table] = f"missing_or_error:{type(exc).__name__}"
        payload["table_counts"] = counts
        return payload, warnings
    finally:
        con.close()


def load_final_counts(db_path: Path) -> tuple[dict[str, int | str], list[str]]:
    if not db_path.exists():
        return {}, [f"db_missing: {db_path}"]
    warnings: list[str] = []
    counts: dict[str, int | str] = {}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, [f"db_unavailable: {type(exc).__name__}: {exc}"]
    try:
        tables = [
            row[0]
            for row in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='newspaper_final'
                ORDER BY table_name
                """
            ).fetchall()
        ]
        for table in tables:
            try:
                counts[table] = con.execute(f"SELECT COUNT(*) FROM newspaper_final.{table}").fetchone()[0]
            except Exception as exc:
                counts[table] = f"missing_or_error:{type(exc).__name__}"
        return counts, warnings
    finally:
        con.close()


def document_queue_summaries(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in latest_dirs(root / "document_extraction_queues", limit=6):
        summary = read_json(path / "summary.json")
        next_action_csv = path / "document_next_action_queue.csv"
        rows.append({
            "path": str(path),
            "label": path.name,
            "summary": summary,
            "next_action_queue_count": csv_count(next_action_csv),
        })
    return rows


def first_unread_packet(read_state_dir: Path | None) -> dict[str, str]:
    if not read_state_dir:
        return {}
    rows = read_csv_rows(read_state_dir / "next_unread_packet_queue.csv", max_rows=1)
    return rows[0] if rows else {}


def as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def extract_batch_token(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        match = re.search(r"batch\d{4}", str(value))
        if match:
            return match.group(0)
    return ""


def current_summary_path(summary: dict[str, Any], current_batch_token: str, *keys: str) -> str:
    if not isinstance(summary, dict):
        return ""
    for key in keys:
        value = summary.get(key, "")
        if not value:
            continue
        value_batch = extract_batch_token(value)
        if not current_batch_token or not value_batch or value_batch == current_batch_token:
            return str(value)
    return ""


def ocr_sidecar_status(root: Path) -> dict[str, Any]:
    manifest_dir = latest_dir(root / "ocr_followup_round_manifests")
    if not manifest_dir:
        return {"status": "missing", "manifest_path": ""}
    manifest_summary = read_json(manifest_dir / "summary.json")
    manifest_path = Path(manifest_summary.get("manifest_path") or (manifest_dir / "ocr_round_manifest.csv"))
    rows = read_csv_all(manifest_path)
    done_rows: list[dict[str, str]] = []
    pending_rows: list[dict[str, str]] = []
    text_bytes_total = 0
    json_bytes_total = 0
    for row in rows:
        text_path = Path(row.get("suggested_ocr_text_path") or "")
        json_path = Path(row.get("suggested_ocr_json_path") or "")
        text_done = text_path.exists() and text_path.stat().st_size > 0
        json_done = json_path.exists() and json_path.stat().st_size > 0
        if text_done:
            text_bytes_total += text_path.stat().st_size
        if json_done:
            json_bytes_total += json_path.stat().st_size
        if text_done and json_done:
            done_rows.append(row)
        else:
            pending_rows.append(row)
    latest_chunk = summarize_run(root, "ocr_followup_chunk_runs")
    return {
        "status": "found",
        "manifest_dir": str(manifest_dir),
        "manifest_path": str(manifest_path),
        "document_count": len(rows),
        "done_count": len(done_rows),
        "pending_count": len(pending_rows),
        "done_by_pass": dict(Counter(row.get("recommended_next_pass") for row in done_rows)),
        "pending_by_pass": dict(Counter(row.get("recommended_next_pass") for row in pending_rows)),
        "total_ocr_text_bytes": text_bytes_total,
        "total_ocr_json_bytes": json_bytes_total,
        "latest_chunk_run": latest_chunk,
    }


def derive_next_actions(report: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    page_coverage_summary = report.get("page_coverage_audits", {}).get("summary", {})
    page_visual_summary = report.get("page_visual_review_packets", {}).get("summary", {})
    page_visual_progress = report.get("page_visual_progress", {})
    read_state = report.get("llm_read_state", {}).get("summary", {})
    packet_counts = read_state.get("packet_state_counts", {})
    doc_counts = read_state.get("document_read_state_counts", {})
    ingest_summary = report.get("llm_ingest", {}).get("summary", {})
    materialized = report.get("llm_materialized_rows", {}).get("summary", {})
    materialized_counts = materialized.get("materialized_row_counts", {})
    action_summary = report.get("conveyor_action_queues", {}).get("summary", {})
    ocr_prep_summary = report.get("ocr_followup_preps", {}).get("summary", {})
    ocr_manifest_summary = report.get("ocr_followup_round_manifests", {}).get("summary", {})
    ocr_sidecars = report.get("ocr_sidecar_status", {})
    semantic_prep_summary = report.get("semantic_followup_preps", {}).get("summary", {})
    quality_prep_summary = report.get("quality_review_preps", {}).get("summary", {})
    promotion_prep_summary = report.get("promotion_review_preps", {}).get("summary", {})
    decision_ledger_summary = report.get("review_decision_ledgers", {}).get("summary", {})
    decision_apply_summary = report.get("review_decision_apply_runs", {}).get("summary", {})
    closeout_summary = report.get("batch_closeout_audits", {}).get("summary", {})
    article_chunk_summary = report.get("article_atom_chunk_runs", {}).get("summary", {})
    v26_readiness_summary = report.get("v26_readiness_reports", {}).get("summary", {})
    identity_summary = report.get("identity_resolution_from_readiness", {}).get("summary", {})
    lane_summary = report.get("readiness_promotion_lanes", {}).get("summary", {})
    resolved_summary = report.get("resolved_atom_materializations", {}).get("summary", {})
    game_triage_summary = report.get("resolved_game_mapping_triage", {}).get("summary", {})
    resolved_package_summary = report.get("resolved_promotion_packages", {}).get("summary", {})
    review_packet_summary = report.get("resolved_package_review_packets", {}).get("summary", {})
    resolved_decision_summary = report.get("resolved_package_decision_ledgers", {}).get("summary", {})
    safe_stat_summary = report.get("resolved_safe_stat_decisions", {}).get("summary", {})
    safe_lineup_summary = report.get("resolved_safe_lineup_decisions", {}).get("summary", {})
    safe_scoring_summary = report.get("resolved_safe_scoring_decisions", {}).get("summary", {})
    safe_team_stat_summary = report.get("resolved_safe_team_stat_decisions", {}).get("summary", {})
    safe_pbp_summary = report.get("resolved_safe_pbp_decisions", {}).get("summary", {})
    safe_identity_candidate_summary = report.get("resolved_safe_identity_candidate_decisions", {}).get("summary", {})
    identity_event_summary = report.get("resolved_identity_event_decisions", {}).get("summary", {})
    safe_game_candidate_summary = report.get("resolved_safe_game_candidate_decisions", {}).get("summary", {})
    direct_source_summary = report.get("resolved_direct_source_decisions", {}).get("summary", {})
    touchdown_total_summary = report.get("resolved_touchdown_total_decisions", {}).get("summary", {})
    source_game_remap_summary = report.get("resolved_source_game_remap_decisions", {}).get("summary", {})
    second_pass_summary = report.get("resolved_second_pass_decisions", {}).get("summary", {})
    route_dossier_summary = report.get("resolved_route_followup_dossiers", {}).get("summary", {})
    final_apply_summary = report.get("resolved_package_decision_apply_runs", {}).get("summary", {})
    db_counts = report.get("db_counts", {})
    resolved_closeout_ready = False
    if isinstance(closeout_summary, dict) and isinstance(final_apply_summary, dict):
        resolved_closeout_ready = (
            closeout_summary.get("apply_run_kind") == "resolved_package"
            and closeout_summary.get("closeout_status") == "ready_to_advance"
            and closeout_summary.get("promotion_apply_run_id") == final_apply_summary.get("resolved_package_apply_run_id")
        )
    current_batch_token = extract_batch_token(
        report.get("llm_packets", {}).get("path", ""),
        report.get("llm_read_state", {}).get("path", ""),
        action_summary.get("action_queue_run_id", "") if isinstance(action_summary, dict) else "",
        action_summary.get("output_dir", "") if isinstance(action_summary, dict) else "",
        ingest_summary.get("ingest_run_id", "") if isinstance(ingest_summary, dict) else "",
        ingest_summary.get("output_dir", "") if isinstance(ingest_summary, dict) else "",
        materialized.get("materialize_run_id", "") if isinstance(materialized, dict) else "",
        materialized.get("output_dir", "") if isinstance(materialized, dict) else "",
    )
    decision_apply_batch_token = extract_batch_token(
        decision_apply_summary.get("promotion_apply_run_id", "") if isinstance(decision_apply_summary, dict) else "",
        decision_apply_summary.get("output_dir", "") if isinstance(decision_apply_summary, dict) else "",
    )
    decision_apply_is_current = bool(decision_apply_summary) and (
        not current_batch_token
        or not decision_apply_batch_token
        or decision_apply_batch_token == current_batch_token
    )
    latest_route_counts = (
        decision_apply_summary.get("route_queue_counts", {})
        if decision_apply_is_current and isinstance(decision_apply_summary, dict)
        else {}
    )
    if not isinstance(latest_route_counts, dict):
        latest_route_counts = {}
    latest_apply_controls_routes = decision_apply_is_current

    if isinstance(page_coverage_summary, dict) and page_coverage_summary:
        visual_count = as_int(page_coverage_summary.get("documents_needing_visual_read"))
        article_count = as_int(page_coverage_summary.get("documents_needing_article_pass"))
        semantic_count = as_int(page_coverage_summary.get("documents_needing_semantic_reparse"))
        ocr_count = as_int(page_coverage_summary.get("documents_needing_ocr"))
        atom_docs = as_int(page_coverage_summary.get("documents_with_atoms"))
        high_value_docs = as_int(page_coverage_summary.get("documents_with_high_value_atoms"))
        report_path = page_coverage_summary.get("report_md_path", "")
        if visual_count:
            packet_items = as_int(page_visual_summary.get("item_count")) if isinstance(page_visual_summary, dict) else 0
            packet_audit_run_id = (
                page_visual_summary.get("page_coverage_audit_run_id", "")
                if isinstance(page_visual_summary, dict)
                else ""
            )
            coverage_run_id = page_coverage_summary.get("page_coverage_audit_run_id", "")
            if packet_items >= visual_count and packet_audit_run_id == coverage_run_id:
                reviewed_items = as_int(page_visual_progress.get("reviewed_item_count")) if isinstance(page_visual_progress, dict) else 0
                remaining_items = as_int(page_visual_progress.get("remaining_item_count")) if isinstance(page_visual_progress, dict) else visual_count
                actions.append(
                    "Review page visual packets from the latest coverage audit: "
                    f"`{reviewed_items}` / `{packet_items}` docs reviewed, `{remaining_items}` remaining, "
                    f"`{page_visual_summary.get('packet_count', 0)}` packets and `{page_visual_summary.get('rendered_image_count', 0)}` rendered images. "
                    f"Decision template: {page_visual_summary.get('decision_template_csv', '')}"
                )
            else:
                actions.append(
                    "Run page visual-read/re-OCR packets from the latest coverage audit: "
                    f"`{visual_count}` docs still have football/stat markers without extracted atoms; "
                    f"`{atom_docs}` docs already have atoms and `{high_value_docs}` have high-value atoms. "
                    f"Coverage report: {report_path}"
                )
        elif ocr_count or article_count or semantic_count:
            actions.append(
                "Finish page coverage conveyor gaps: "
                f"OCR `{ocr_count}`, article pass `{article_count}`, semantic reparse `{semantic_count}`. "
                f"Coverage report: {report_path}"
            )
        elif page_coverage_summary.get("certified_no_word_left_behind"):
            actions.append(f"Page coverage audit is clean for the current plan. Coverage report: {report_path}")

    resolved_status_counts = (
        resolved_summary.get("materialization_status_counts", {})
        if isinstance(resolved_summary, dict)
        else {}
    )
    if isinstance(resolved_status_counts, dict) and resolved_status_counts:
        package_count = as_int(resolved_package_summary.get("package_item_count")) if isinstance(resolved_package_summary, dict) else 0
        if package_count:
            ready = as_int(resolved_package_summary.get("ready_review_count"))
            archive = as_int(resolved_package_summary.get("archive_count"))
            conflicts = as_int(resolved_package_summary.get("conflict_count"))
            manual = as_int(resolved_package_summary.get("manual_followup_count"))
            package_path = resolved_package_summary.get("output_dir", "")
            packet_count = as_int(review_packet_summary.get("packet_count")) if isinstance(review_packet_summary, dict) else 0
            packet_items = as_int(review_packet_summary.get("item_count")) if isinstance(review_packet_summary, dict) else 0
            packet_package_run_id = (
                review_packet_summary.get("resolved_promotion_package_run_id", "")
                if isinstance(review_packet_summary, dict)
                else ""
            )
            package_run_id = (
                resolved_package_summary.get("resolved_promotion_package_run_id", "")
                if isinstance(resolved_package_summary, dict)
                else ""
            )
            if packet_count and packet_items == package_count and packet_package_run_id == package_run_id:
                decision_count = as_int(resolved_decision_summary.get("decision_count")) if isinstance(resolved_decision_summary, dict) else 0
                decision_package_run_id = (
                    resolved_decision_summary.get("resolved_promotion_package_run_id", "")
                    if isinstance(resolved_decision_summary, dict)
                    else ""
                )
                if decision_count == package_count and decision_package_run_id == package_run_id:
                    route_count = as_int(resolved_decision_summary.get("route_queue_count"))
                    closed_count = as_int(resolved_decision_summary.get("closed_decision_count"))
                    approved_count = as_int(resolved_decision_summary.get("approved_decision_count"))
                    decision_report = resolved_decision_summary.get("report_md", "")
                    applied_count = as_int(final_apply_summary.get("applied_row_count")) if isinstance(final_apply_summary, dict) else 0
                    apply_decision_run_id = (
                        final_apply_summary.get("resolved_package_decision_run_id", "")
                        if isinstance(final_apply_summary, dict)
                        else ""
                    )
                    decision_run_id = (
                        resolved_decision_summary.get("resolved_package_decision_run_id", "")
                        if isinstance(resolved_decision_summary, dict)
                        else ""
                    )
                    if approved_count and applied_count and apply_decision_run_id == decision_run_id:
                        if route_count == 0:
                            actions.append(f"Resolved decision routes are drained: `{applied_count}` local-final rows are in `newspaper_final` from `{approved_count}` approved decisions, `{closed_count}` archive/context rows closed. Ledger report: {decision_report}")
                        else:
                            actions.append(f"Continue draining resolved decision routes: `{applied_count}` local-final rows are in `newspaper_final` from `{approved_count}` approved decisions, `{route_count}` rows remain routed, `{closed_count}` archive/context rows closed. Ledger report: {decision_report}")
                    else:
                        actions.append(f"Work resolved package decision ledger: `{decision_count}` decisions, `{route_count}` routed, `{closed_count}` closed archive/context, `{approved_count}` approved overrides. Ledger report: {decision_report}")
                else:
                    packet_report = review_packet_summary.get("report_md", "")
                    actions.append(f"Review resolved package packets: `{packet_count}` packets / `{packet_items}` items covering `{ready}` pending review, `{archive}` archive-only, `{conflicts}` conflicts, `{manual}` manual/identity follow-ups. Packet report: {packet_report}")
            else:
                actions.append(f"Use latest resolved promotion package: `{ready}` pending review, `{archive}` archive-only, `{conflicts}` conflicts, `{manual}` manual/identity follow-ups. Package: {package_path}")
        ready_count = as_int(resolved_status_counts.get("ready_for_resolved_review"))
        touchdown_total_count = as_int(resolved_status_counts.get("ready_with_touchdown_total_review"))
        schema_count = as_int(resolved_status_counts.get("schema_review_required"))
        mapping_count = as_int(resolved_status_counts.get("hold_needs_game_mapping"))
        held_identity_count = as_int(resolved_status_counts.get("hold_needs_identity"))
        bridge_count = as_int(resolved_status_counts.get("identity_bridge_review_required"))
        context_count = as_int(resolved_status_counts.get("context_review_required"))
        resolved_path = resolved_summary.get("output_dir", "")
        if ready_count and not package_count:
            actions.append(f"Review/apply `{ready_count}` resolved atom rows from `newspaper_resolved.*`. Materialization: {resolved_path}")
        if touchdown_total_count and not package_count:
            actions.append(f"Review `{touchdown_total_count}` resolved rows with total touchdowns preserved but touchdown type still flagged for split review.")
        if schema_count:
            actions.append(f"Resolve `{schema_count}` schema-gap stat rows so they can map cleanly to target atom columns. Materialization: {resolved_path}")
        if mapping_count and not package_count:
            triage_input_count = as_int(game_triage_summary.get("input_row_count")) if isinstance(game_triage_summary, dict) else 0
            if triage_input_count == mapping_count:
                triage_actions = game_triage_summary.get("action_status_counts", {}) if isinstance(game_triage_summary, dict) else {}
                backfill = as_int(triage_actions.get("ready_score_backfill_review")) if isinstance(triage_actions, dict) else 0
                manual = as_int(triage_actions.get("manual_game_mapping_review")) if isinstance(triage_actions, dict) else 0
                conflict = as_int(triage_actions.get("conflict_review")) if isinstance(triage_actions, dict) else 0
                context = as_int(triage_actions.get("archive_context_only")) if isinstance(triage_actions, dict) else 0
                corroboration = as_int(triage_actions.get("archive_corroboration_only")) if isinstance(triage_actions, dict) else 0
                triage_path = game_triage_summary.get("output_dir", "")
                actions.append(f"Game mapping triage split `{mapping_count}` holds: `{backfill}` score backfills, `{conflict}` conflicts, `{manual}` manual mappings, `{context}` context-only, `{corroboration}` corroborations. Triage: {triage_path}")
            else:
                actions.append(f"Resolve `{mapping_count}` game-mapping holds before promoting score candidates. Materialization: {resolved_path}")
        if (bridge_count or held_identity_count) and not package_count:
            actions.append(f"Work identity leftovers: `{bridge_count}` bridge candidates and `{held_identity_count}` held event rows still need identity review.")
        if context_count and not package_count:
            actions.append(f"Review `{context_count}` context-only notable-play row for whether it should stay context or become structured PBP.")

    readiness_lane_counts = (
        v26_readiness_summary.get("readiness_lane_latest_entity_counts", {})
        if isinstance(v26_readiness_summary, dict)
        else {}
    )
    if isinstance(readiness_lane_counts, dict) and readiness_lane_counts:
        identity_remaining = as_int(readiness_lane_counts.get("identity_resolution_needed"))
        conflict_remaining = as_int(readiness_lane_counts.get("conflict_review"))
        semantic_remaining = as_int(readiness_lane_counts.get("semantic_resolution_needed"))
        readiness_path = v26_readiness_summary.get("output_dir", "")
        if identity_remaining:
            identity_path = identity_summary.get("output_dir", "") if isinstance(identity_summary, dict) else ""
            suffix = f" Latest identity pass: {identity_path}" if identity_path else ""
            actions.append(f"Drain `{identity_remaining}` remaining v26-readiness identity rows with another identity/manual pass. Readiness: {readiness_path}.{suffix}")
        if conflict_remaining:
            actions.append(f"Review `{conflict_remaining}` v26 conflict rows before any v26 promotion package is considered. Readiness: {readiness_path}")
        if semantic_remaining:
            actions.append(f"Resolve `{semantic_remaining}` semantic rows that need team/game/stat meaning before promotion. Readiness: {readiness_path}")

    lane_counts = lane_summary.get("lane_counts", {}) if isinstance(lane_summary, dict) else {}
    if isinstance(lane_counts, dict) and lane_counts:
        lane_path = lane_summary.get("output_dir", "")
        lane_total = as_int(lane_summary.get("promotion_review_count"))
        actions.append(f"Use latest readiness lane split for `{lane_total}` promotion-review items: `{lane_counts}`. Lane report: {lane_path}")
    if isinstance(lane_counts, dict) and lane_counts and not resolved_status_counts:
        lane_path = lane_summary.get("output_dir", "")
        actions.append(f"Materialize the latest readiness promotion lanes into `newspaper_resolved.*`. Lane run: {lane_path}")

    unread_packets = int(packet_counts.get("unread") or 0)
    unread_docs = int(doc_counts.get("unread") or 0)
    if unread_packets:
        packet = report.get("first_unread_packet", {})
        packet_path = packet.get("packet_md_path") or "(packet path missing)"
        actions.append(f"Review next LLM packet: {packet_path} ({unread_packets} unread packets, {unread_docs} unread docs).")

    ingest_run_id = ingest_summary.get("ingest_run_id", "") if isinstance(ingest_summary, dict) else ""
    materialized_ingest_run_id = materialized.get("ingest_run_id", "") if isinstance(materialized, dict) else ""
    latest_ingest_claims = int(ingest_summary.get("claim_count") or 0) if isinstance(ingest_summary, dict) else 0
    materialize_current = bool(ingest_run_id and materialized_ingest_run_id == ingest_run_id)
    if latest_ingest_claims > 0 and not materialize_current:
        actions.append("Run `newspaper_llm_materialize_rows.py` because the latest reviewed claims do not have a materialize receipt yet.")

    if any(int(value or 0) > 0 for value in materialized_counts.values()) and not int(action_summary.get("action_count") or 0):
        actions.append("Review materialized `llm_*` rows for promotion packaging and follow-up routing.")

    if int(decision_ledger_summary.get("open_decision_count") or 0) > 0 and not decision_apply_is_current:
        ledger_path = decision_ledger_summary.get("output_dir", "")
        suffix = f" Ledger: {ledger_path}" if ledger_path else ""
        actions.append(f"Work the consolidated newspaper decision ledger with `{decision_ledger_summary.get('open_decision_count')}` open decisions.{suffix}")

    if resolved_closeout_ready:
        closeout_dir = closeout_summary.get("output_dir", "")
        actions.append(f"Resolved-package closeout is `ready_to_advance`; use the v26-readiness lanes or next batch plan as the next conveyor input. Closeout: {closeout_dir}")

    if decision_apply_is_current and not resolved_closeout_ready:
        promoted = int(decision_apply_summary.get("promoted_row_count") or 0)
        routed = int(decision_apply_summary.get("route_queue_count") or 0)
        apply_path = decision_apply_summary.get("output_dir", "")
        actions.append(f"Latest local decision apply run staged `{promoted}` promoted rows and routed `{routed}` decisions. Apply: {apply_path}")
        if routed == 0:
            apply_run_id = decision_apply_summary.get("promotion_apply_run_id", "")
            closeout_apply_run_id = closeout_summary.get("promotion_apply_run_id", "") if isinstance(closeout_summary, dict) else ""
            closeout_status = closeout_summary.get("closeout_status", "") if isinstance(closeout_summary, dict) else ""
            if closeout_apply_run_id == apply_run_id and closeout_status == "ready_to_advance":
                closeout_dir = closeout_summary.get("output_dir", "")
                actions.append(f"Latest batch closeout is `ready_to_advance`; advance to the next batch. Closeout: {closeout_dir}")
            else:
                actions.append("Latest local route queue is empty for this batch; run `build_newspaper_batch_closeout_audit.py` before advancing.")

    if article_chunk_summary:
        manifest_rows = int(article_chunk_summary.get("manifest_rows") or 0)
        pending_rows = int(article_chunk_summary.get("final_pending_rows") or 0)
        output_dir = article_chunk_summary.get("output_dir", "")
        if manifest_rows and pending_rows == 0:
            actions.append(f"Latest article chunk run completed `{manifest_rows}` documents with no pending rows. Chunk run: {output_dir}")
        elif pending_rows:
            remaining = article_chunk_summary.get("remaining_manifest_path", "")
            actions.append(f"Resume latest article chunk run; `{pending_rows}` documents remain. Remaining manifest: {remaining}")

    current_conflicts = 0 if latest_apply_controls_routes else int(action_summary.get("promotion_conflict_count") or 0)
    if current_conflicts > 0:
        actions.append(f"Resolve `{current_conflicts}` promotion conflicts from the latest action queue.")

    current_ocr_followups = (
        as_int(latest_route_counts.get("ocr_visual_followup"))
        if "ocr_visual_followup" in latest_route_counts
        else 0 if latest_apply_controls_routes else as_int(action_summary.get("ocr_visual_followup_count"))
    )
    if current_ocr_followups > 0:
        prep_path = ocr_prep_summary.get("output_dir", "")
        manifest_path = ocr_manifest_summary.get("manifest_path", "")
        pending_ocr = as_int(ocr_sidecars.get("pending_count"))
        done_ocr = as_int(ocr_sidecars.get("done_count"))
        total_ocr = as_int(ocr_sidecars.get("document_count"))
        suffix_parts = []
        if prep_path:
            suffix_parts.append(f"Prep: {prep_path}")
        if manifest_path:
            suffix_parts.append(f"Manifest: {manifest_path}")
        suffix = " " + " ".join(suffix_parts) if suffix_parts else ""
        if pending_ocr:
            actions.append(f"Run OCR/visual follow-up on `{pending_ocr}` remaining source documents (`{done_ocr}`/`{total_ocr}` complete).{suffix}")
        elif total_ocr:
            packet_path = report.get("latest_paths", {}).get("llm_packets", "")
            packet_suffix = f" Latest OCR packet run: {packet_path}" if packet_path else ""
            actions.append(f"OCR/visual follow-up sidecars are complete (`{done_ocr}`/`{total_ocr}`). Review the OCR follow-up LLM packets next.{packet_suffix}")
        else:
            actions.append(f"Run OCR/visual follow-up on `{current_ocr_followups}` queued source documents.{suffix}")

    current_semantic_followups = (
        as_int(latest_route_counts.get("semantic_followup"))
        if "semantic_followup" in latest_route_counts
        else 0 if latest_apply_controls_routes else as_int(action_summary.get("semantic_followup_count"))
    )
    if current_semantic_followups > 0:
        prep_path = current_summary_path(semantic_prep_summary, current_batch_token, "output_dir")
        queue_path = current_summary_path(action_summary, current_batch_token, "output_dir")
        suffix = f" Prep: {prep_path}" if prep_path else f" Queue: {queue_path}" if queue_path else ""
        actions.append(f"Resolve `{current_semantic_followups}` semantic follow-ups before promotion.{suffix}")

    current_quality_reviews = (
        as_int(latest_route_counts.get("quality_review"))
        if "quality_review" in latest_route_counts
        else 0 if latest_apply_controls_routes else as_int(action_summary.get("quality_review_count"))
    )
    if current_quality_reviews > 0:
        prep_path = current_summary_path(quality_prep_summary, current_batch_token, "output_dir")
        queue_path = current_summary_path(action_summary, current_batch_token, "output_dir")
        suffix = f" Prep: {prep_path}" if prep_path else f" Queue: {queue_path}" if queue_path else ""
        actions.append(f"Review `{current_quality_reviews}` quality-review local packages for promote/follow-up/hold decisions.{suffix}")

    current_promotion_ready = 0 if latest_apply_controls_routes else int(action_summary.get("promotion_ready_count") or 0)
    if current_promotion_ready > 0:
        prep_path = current_summary_path(promotion_prep_summary, current_batch_token, "output_dir")
        queue_path = current_summary_path(action_summary, current_batch_token, "output_dir")
        suffix = f" Prep: {prep_path}" if prep_path else f" Queue: {queue_path}" if queue_path else ""
        actions.append(f"Review `{current_promotion_ready}` promotion-ready local packages.{suffix}")

    if not actions:
        actions.append("No reviewed LLM output is present yet; send the first unread packet to ChatGPT and save its JSON response.")
    return actions


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in rows:
        values = [str(row.get(field, "")).replace("\n", " ") for field in fields]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    db_counts = report.get("db_counts", {})
    latest_paths = report.get("latest_paths", {})
    page_coverage_summary = report.get("page_coverage_audits", {}).get("summary", {})
    page_visual_summary = report.get("page_visual_review_packets", {}).get("summary", {})
    page_visual_progress = report.get("page_visual_progress", {})
    packet_summary = report.get("llm_packets", {}).get("summary", {})
    read_summary = report.get("llm_read_state", {}).get("summary", {})
    ingest_summary = report.get("llm_ingest", {}).get("summary", {})
    materialized_summary = report.get("llm_materialized_rows", {}).get("summary", {})
    package_summary = report.get("llm_promotion_packages", {}).get("summary", {})
    action_summary = report.get("conveyor_action_queues", {}).get("summary", {})
    ocr_prep_summary = report.get("ocr_followup_preps", {}).get("summary", {})
    ocr_manifest_summary = report.get("ocr_followup_round_manifests", {}).get("summary", {})
    semantic_prep_summary = report.get("semantic_followup_preps", {}).get("summary", {})
    quality_prep_summary = report.get("quality_review_preps", {}).get("summary", {})
    promotion_prep_summary = report.get("promotion_review_preps", {}).get("summary", {})
    decision_ledger_summary = report.get("review_decision_ledgers", {}).get("summary", {})
    decision_apply_summary = report.get("review_decision_apply_runs", {}).get("summary", {})
    closeout_summary = report.get("batch_closeout_audits", {}).get("summary", {})
    article_chunk_summary = report.get("article_atom_chunk_runs", {}).get("summary", {})
    v26_readiness_summary = report.get("v26_readiness_reports", {}).get("summary", {})
    identity_summary = report.get("identity_resolution_from_readiness", {}).get("summary", {})
    lane_summary = report.get("readiness_promotion_lanes", {}).get("summary", {})
    resolved_summary = report.get("resolved_atom_materializations", {}).get("summary", {})
    schema_patch_summary = report.get("resolved_schema_gap_patches", {}).get("summary", {})
    game_triage_summary = report.get("resolved_game_mapping_triage", {}).get("summary", {})
    resolved_package_summary = report.get("resolved_promotion_packages", {}).get("summary", {})
    review_packet_summary = report.get("resolved_package_review_packets", {}).get("summary", {})
    resolved_decision_summary = report.get("resolved_package_decision_ledgers", {}).get("summary", {})
    safe_stat_summary = report.get("resolved_safe_stat_decisions", {}).get("summary", {})
    safe_lineup_summary = report.get("resolved_safe_lineup_decisions", {}).get("summary", {})
    safe_scoring_summary = report.get("resolved_safe_scoring_decisions", {}).get("summary", {})
    safe_team_stat_summary = report.get("resolved_safe_team_stat_decisions", {}).get("summary", {})
    safe_pbp_summary = report.get("resolved_safe_pbp_decisions", {}).get("summary", {})
    safe_identity_candidate_summary = report.get("resolved_safe_identity_candidate_decisions", {}).get("summary", {})
    identity_event_summary = report.get("resolved_identity_event_decisions", {}).get("summary", {})
    safe_game_candidate_summary = report.get("resolved_safe_game_candidate_decisions", {}).get("summary", {})
    direct_source_summary = report.get("resolved_direct_source_decisions", {}).get("summary", {})
    touchdown_total_summary = report.get("resolved_touchdown_total_decisions", {}).get("summary", {})
    source_game_remap_summary = report.get("resolved_source_game_remap_decisions", {}).get("summary", {})
    second_pass_summary = report.get("resolved_second_pass_decisions", {}).get("summary", {})
    route_dossier_summary = report.get("resolved_route_followup_dossiers", {}).get("summary", {})
    final_apply_summary = report.get("resolved_package_decision_apply_runs", {}).get("summary", {})
    ocr_sidecars = report.get("ocr_sidecar_status", {})
    first_packet = report.get("first_unread_packet", {})

    lines = [
        "# Newspaper Atom Conveyor Status",
        "",
        f"Created: `{report.get('created_at_utc')}`",
        "",
        "## Next Actions",
        "",
    ]
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    lines.extend([
        "",
        "## Latest Runs",
        "",
    ])
    latest_rows = [{"station": key, "path": value} for key, value in latest_paths.items()]
    lines.extend(markdown_table(latest_rows, ["station", "path"]))
    lines.extend([
        "",
        "## Current LLM Review",
        "",
        f"- Packet run documents: `{packet_summary.get('documents_selected', 0)}`",
        f"- Packet count: `{packet_summary.get('packets_created', 0)}`",
        f"- Read-state packets: `{read_summary.get('packet_state_counts', {})}`",
        f"- Read-state documents: `{read_summary.get('document_read_state_counts', {})}`",
        f"- Review output dir: `{read_summary.get('review_output_dir', '')}`",
        f"- First unread packet: `{first_packet.get('packet_md_path', '')}`",
        "",
        "## LLM Ingest / Materialization",
        "",
        f"- Review output files found: `{ingest_summary.get('review_output_files_found', 0)}`",
        f"- LLM document reviews: `{ingest_summary.get('document_review_count', 0)}`",
        f"- LLM claims: `{ingest_summary.get('claim_count', 0)}`",
        f"- LLM followups: `{ingest_summary.get('followup_count', 0)}`",
        f"- Materialized row counts: `{materialized_summary.get('materialized_row_counts', {})}`",
        f"- Promotion package count: `{package_summary.get('package_count', 0)}`",
        f"- Promotion package statuses: `{package_summary.get('package_status_counts', {})}`",
        f"- Promotion conflicts: `{package_summary.get('conflict_count', 0)}`",
        f"- Action queue counts: `{action_summary.get('action_type_counts', {})}`",
        f"- Action queue followups: `{action_summary.get('followup_type_counts', {})}`",
        f"- Action queue output: `{action_summary.get('output_dir', '')}`",
        f"- OCR follow-up prep: `{ocr_prep_summary.get('output_dir', '')}`",
        f"- OCR follow-up next passes: `{ocr_prep_summary.get('recommended_next_pass_counts', {})}`",
        f"- OCR follow-up manifest: `{ocr_manifest_summary.get('manifest_path', '')}`",
        f"- OCR follow-up manifest docs: `{ocr_manifest_summary.get('document_count', 0)}`",
        f"- OCR follow-up sidecars done/pending: `{ocr_sidecars.get('done_count', 0)}` / `{ocr_sidecars.get('pending_count', 0)}`",
        f"- OCR follow-up sidecar passes done: `{ocr_sidecars.get('done_by_pass', {})}`",
        f"- Page coverage audit: `{page_coverage_summary.get('output_dir', '')}`",
        f"- Page coverage docs/atoms/high-value: `{page_coverage_summary.get('planned_document_count', 0)}` / `{page_coverage_summary.get('documents_with_atoms', 0)}` / `{page_coverage_summary.get('documents_with_high_value_atoms', 0)}`",
        f"- Page coverage next actions: `{page_coverage_summary.get('recommended_next_action_counts', {})}`",
        f"- Page coverage no-word-left-behind certified: `{page_coverage_summary.get('certified_no_word_left_behind', '')}`",
        f"- Page visual packets: `{page_visual_summary.get('output_dir', '')}`",
        f"- Page visual packets/items/images: `{page_visual_summary.get('packet_count', 0)}` / `{page_visual_summary.get('item_count', 0)}` / `{page_visual_summary.get('rendered_image_count', 0)}`",
        f"- Page visual reviewed/remaining/generated: `{page_visual_progress.get('reviewed_item_count', 0)}` / `{page_visual_progress.get('remaining_item_count', 0)}` / `{page_visual_progress.get('generated_decision_count', 0)}`",
        f"- Page visual generated target tables: `{page_visual_progress.get('generated_target_table_counts', {})}`",
        f"- Page visual decision template: `{page_visual_summary.get('decision_template_csv', '')}`",
        f"- Article chunk run: `{article_chunk_summary.get('output_dir', '')}`",
        f"- Article chunk done/pending: `{article_chunk_summary.get('final_done_rows', 0)}` / `{article_chunk_summary.get('final_pending_rows', 0)}`",
        f"- Semantic follow-up prep: `{semantic_prep_summary.get('output_dir', '')}`",
        f"- Semantic follow-up resolution passes: `{semantic_prep_summary.get('recommended_resolution_pass_counts', {})}`",
        f"- Quality review prep: `{quality_prep_summary.get('output_dir', '')}`",
        f"- Quality review next actions: `{quality_prep_summary.get('recommended_next_action_counts', {})}`",
        f"- Promotion review prep: `{promotion_prep_summary.get('output_dir', '')}`",
        f"- Promotion review target tables: `{promotion_prep_summary.get('target_table_counts', {})}`",
        f"- Review decision ledger: `{decision_ledger_summary.get('output_dir', '')}`",
        f"- Review decision ledger open decisions: `{decision_ledger_summary.get('open_decision_count', 0)}`",
        f"- Review decision apply run: `{decision_apply_summary.get('output_dir', '')}`",
        f"- Review decision apply promoted rows: `{decision_apply_summary.get('promoted_row_count', 0)}`",
        f"- Review decision apply route queues: `{decision_apply_summary.get('route_queue_counts', {})}`",
        f"- Batch closeout audit: `{closeout_summary.get('output_dir', '')}`",
        f"- Batch closeout status: `{closeout_summary.get('closeout_status', '')}`",
        f"- Batch closeout unresolved notes: `{closeout_summary.get('unresolved_note_count', 0)}`",
        "",
        "## Current Newspaper Atom Readiness",
        "",
        f"- v26 readiness report: `{v26_readiness_summary.get('output_dir', '')}`",
        f"- v26 readiness lanes: `{v26_readiness_summary.get('readiness_lane_latest_entity_counts', {})}`",
        f"- v26 value classes: `{v26_readiness_summary.get('value_class_latest_entity_counts', {})}`",
        f"- Identity resolution pass: `{identity_summary.get('output_dir', '')}`",
        f"- Identity resolution counts: auto `{identity_summary.get('auto_resolved_count', 0)}`, review `{identity_summary.get('review_count', 0)}`, unresolved `{identity_summary.get('unresolved_count', 0)}`",
        f"- Promotion lane run: `{lane_summary.get('output_dir', '')}`",
        f"- Promotion lane counts: `{lane_summary.get('lane_counts', {})}`",
        f"- Promotion lane statuses: `{lane_summary.get('action_status_counts', {})}`",
        f"- Resolved atom materialization: `{resolved_summary.get('output_dir', '')}`",
        f"- Resolved atom table counts: `{resolved_summary.get('target_table_counts', {})}`",
        f"- Resolved atom statuses: `{resolved_summary.get('materialization_status_counts', {})}`",
        f"- Schema gap patch run: `{schema_patch_summary.get('output_dir', '')}`",
        f"- Schema gap patch counts: ready `{schema_patch_summary.get('ready_count', 0)}`, patched `{schema_patch_summary.get('patched_row_count', 0)}`, touchdown-type review `{schema_patch_summary.get('touchdown_type_review_count', 0)}`, holds `{schema_patch_summary.get('hold_count', 0)}`",
        f"- Game mapping triage: `{game_triage_summary.get('output_dir', '')}`",
        f"- Game mapping triage counts: `{game_triage_summary.get('triage_status_counts', {})}`",
        f"- Game mapping actions: `{game_triage_summary.get('action_status_counts', {})}`",
        f"- Resolved promotion package: `{resolved_package_summary.get('output_dir', '')}`",
        f"- Resolved package lanes: `{resolved_package_summary.get('package_lane_counts', {})}`",
        f"- Resolved package statuses: `{resolved_package_summary.get('review_status_counts', {})}`",
        f"- Resolved package ready/archive/conflict/manual: `{resolved_package_summary.get('ready_review_count', 0)}` / `{resolved_package_summary.get('archive_count', 0)}` / `{resolved_package_summary.get('conflict_count', 0)}` / `{resolved_package_summary.get('manual_followup_count', 0)}`",
        f"- Resolved package decision input: `{resolved_package_summary.get('decision_input_csv', '')}`",
        f"- Resolved review packet run: `{review_packet_summary.get('output_dir', '')}`",
        f"- Resolved review packets/items: `{review_packet_summary.get('packet_count', 0)}` / `{review_packet_summary.get('item_count', 0)}`",
        f"- Resolved review packet report: `{review_packet_summary.get('report_md', '')}`",
        f"- Resolved decision ledger: `{resolved_decision_summary.get('output_dir', '')}`",
        f"- Resolved decisions routed/closed/approved: `{resolved_decision_summary.get('route_queue_count', 0)}` / `{resolved_decision_summary.get('closed_decision_count', 0)}` / `{resolved_decision_summary.get('approved_decision_count', 0)}`",
        f"- Resolved decision route lanes: `{resolved_decision_summary.get('route_queue_counts', {})}`",
        f"- Safe stat decision pass: `{safe_stat_summary.get('output_dir', '')}`",
        f"- Safe stat approved/held: `{safe_stat_summary.get('approved_count', 0)}` / `{safe_stat_summary.get('held_count', 0)}`",
        f"- Safe lineup decision pass: `{safe_lineup_summary.get('output_dir', '')}`",
        f"- Safe lineup approved/held: `{safe_lineup_summary.get('approved_count', 0)}` / `{safe_lineup_summary.get('held_count', 0)}`",
        f"- Safe scoring decision pass: `{safe_scoring_summary.get('output_dir', '')}`",
        f"- Safe scoring approved/held: `{safe_scoring_summary.get('approved_count', 0)}` / `{safe_scoring_summary.get('held_count', 0)}`",
        f"- Safe team-stat decision pass: `{safe_team_stat_summary.get('output_dir', '')}`",
        f"- Safe team-stat approved/held: `{safe_team_stat_summary.get('approved_count', 0)}` / `{safe_team_stat_summary.get('held_count', 0)}`",
        f"- Safe PBP decision pass: `{safe_pbp_summary.get('output_dir', '')}`",
        f"- Safe PBP approved/held: `{safe_pbp_summary.get('approved_count', 0)}` / `{safe_pbp_summary.get('held_count', 0)}`",
        f"- Safe identity-candidate pass: `{safe_identity_candidate_summary.get('output_dir', '')}`",
        f"- Safe identity-candidate approved/held: `{safe_identity_candidate_summary.get('approved_count', 0)}` / `{safe_identity_candidate_summary.get('held_count', 0)}`",
        f"- Identity-event decision pass: `{identity_event_summary.get('output_dir', '')}`",
        f"- Identity-event approved/held: `{identity_event_summary.get('approved_count', 0)}` / `{identity_event_summary.get('held_count', 0)}`",
        f"- Safe game-candidate pass: `{safe_game_candidate_summary.get('output_dir', '')}`",
        f"- Safe game-candidate approved/held: `{safe_game_candidate_summary.get('approved_count', 0)}` / `{safe_game_candidate_summary.get('held_count', 0)}`",
        f"- Direct-source decision pass: `{direct_source_summary.get('output_dir', '')}`",
        f"- Direct-source approved/held: `{direct_source_summary.get('approved_count', 0)}` / `{direct_source_summary.get('held_count', 0)}`",
        f"- Touchdown-total decision pass: `{touchdown_total_summary.get('output_dir', '')}`",
        f"- Touchdown-total approved/held: `{touchdown_total_summary.get('approved_count', 0)}` / `{touchdown_total_summary.get('held_count', 0)}`",
        f"- Source-game remap pass: `{source_game_remap_summary.get('output_dir', '')}`",
        f"- Source-game remap approved/held: `{source_game_remap_summary.get('approved_count', 0)}` / `{source_game_remap_summary.get('held_count', 0)}`",
        f"- Second-pass direct decision run: `{second_pass_summary.get('output_dir', '')}`",
        f"- Second-pass approved/held: `{second_pass_summary.get('approved_count', 0)}` / `{second_pass_summary.get('held_count', 0)}`",
        f"- Remaining route dossier: `{route_dossier_summary.get('output_dir', '')}`",
        f"- Remaining route recommended actions: `{route_dossier_summary.get('recommended_action_counts', {})}`",
        f"- Resolved final apply run: `{final_apply_summary.get('output_dir', '')}`",
        f"- Resolved final applied rows: `{final_apply_summary.get('applied_row_count', 0)}`",
        f"- Resolved final target tables: `{final_apply_summary.get('target_table_counts', {})}`",
        "",
        "## Local DuckDB Counts",
        "",
    ])
    db_rows = [{"table": key, "count": value} for key, value in db_counts.items()]
    lines.extend(markdown_table(db_rows, ["table", "count"]))
    resolved_counts = report.get("resolved_db_counts", {})
    if resolved_counts:
        lines.extend([
            "",
            "## Local Resolved Table Counts",
            "",
        ])
        resolved_rows = [{"table": key, "count": value} for key, value in resolved_counts.items()]
        lines.extend(markdown_table(resolved_rows, ["table", "count"]))
    current_resolved = report.get("resolved_current_db_counts", {})
    current_counts = current_resolved.get("table_counts", {}) if isinstance(current_resolved, dict) else {}
    if current_counts:
        lines.extend([
            "",
            "## Current Resolved Table Counts",
            "",
            f"Latest materialize run: `{current_resolved.get('resolved_atom_materialize_run_id', '')}`",
            "",
        ])
        current_rows = [{"table": key, "count": value} for key, value in current_counts.items()]
        lines.extend(markdown_table(current_rows, ["table", "count"]))
    final_counts = report.get("final_db_counts", {})
    if final_counts:
        lines.extend([
            "",
            "## Local Final Table Counts",
            "",
        ])
        final_rows = [{"table": key, "count": value} for key, value in final_counts.items()]
        lines.extend(markdown_table(final_rows, ["table", "count"]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_conveyor_status")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = args.out_root / f"{stamp()}_{args.label}"
    out_dir.mkdir(parents=True, exist_ok=True)

    runs = {
        "extraction_queue": summarize_run(args.root, "extraction_queues"),
        "article_batch_plan": summarize_run(args.root, "article_batch_plans"),
        "llm_packets": summarize_run(args.root, "llm_review_packets"),
        "llm_read_state": summarize_run(args.root, "llm_review_read_states"),
        "llm_ingest": summarize_run(args.root, "llm_review_ingests"),
        "llm_materialized_rows": summarize_run(args.root, "llm_materialized_rows"),
        "article_atom_chunk_runs": summarize_run(args.root, "article_atom_chunk_runs"),
        "llm_promotion_packages": summarize_run(args.root, "llm_promotion_packages"),
        "conveyor_action_queues": summarize_run(args.root, "conveyor_action_queues"),
        "ocr_followup_preps": summarize_run(args.root, "ocr_followup_preps"),
        "ocr_followup_round_manifests": summarize_run(args.root, "ocr_followup_round_manifests"),
        "page_coverage_audits": summarize_run(args.root, "page_coverage_audits"),
        "page_visual_review_packets": summarize_run(args.root, "page_visual_review_packets"),
        "page_visual_review_ingests": summarize_run(args.root, "page_visual_review_ingests"),
        "semantic_followup_preps": summarize_run(args.root, "semantic_followup_preps"),
        "quality_review_preps": summarize_run(args.root, "quality_review_preps"),
        "promotion_review_preps": summarize_run(args.root, "promotion_review_preps"),
        "review_decision_ledgers": summarize_run(args.root, "review_decision_ledgers"),
        "review_decision_apply_runs": summarize_run(args.root, "review_decision_apply_runs"),
        "batch_closeout_audits": summarize_run(args.root, "batch_closeout_audits"),
        "ocr_followup_chunk_runs": summarize_run(args.root, "ocr_followup_chunk_runs"),
        "v26_readiness_reports": summarize_run(args.root, "v26_readiness_reports"),
        "identity_resolution_from_readiness": summarize_run(args.root, "identity_resolution_from_readiness"),
        "readiness_promotion_lanes": summarize_run(args.root, "readiness_promotion_lanes"),
        "resolved_atom_materializations": summarize_run(args.root, "resolved_atom_materializations"),
        "resolved_schema_gap_patches": summarize_run(args.root, "resolved_schema_gap_patches"),
        "resolved_game_mapping_triage": summarize_run(args.root, "resolved_game_mapping_triage"),
        "resolved_promotion_packages": summarize_run(args.root, "resolved_promotion_packages"),
        "resolved_package_review_packets": summarize_run(args.root, "resolved_package_review_packets"),
        "resolved_package_decision_ledgers": summarize_run(args.root, "resolved_package_decision_ledgers"),
        "resolved_safe_stat_decisions": summarize_run(args.root, "resolved_safe_stat_decisions"),
        "resolved_safe_lineup_decisions": summarize_run(args.root, "resolved_safe_lineup_decisions"),
        "resolved_safe_scoring_decisions": summarize_run(args.root, "resolved_safe_scoring_decisions"),
        "resolved_safe_team_stat_decisions": summarize_run(args.root, "resolved_safe_team_stat_decisions"),
        "resolved_safe_pbp_decisions": summarize_run(args.root, "resolved_safe_pbp_decisions"),
        "resolved_safe_identity_candidate_decisions": summarize_run(args.root, "resolved_safe_identity_candidate_decisions"),
        "resolved_identity_event_decisions": summarize_run(args.root, "resolved_identity_event_decisions"),
        "resolved_safe_game_candidate_decisions": summarize_run(args.root, "resolved_safe_game_candidate_decisions"),
        "resolved_direct_source_decisions": summarize_run(args.root, "resolved_direct_source_decisions"),
        "resolved_touchdown_total_decisions": summarize_run(args.root, "resolved_touchdown_total_decisions"),
        "resolved_source_game_remap_decisions": summarize_run(args.root, "resolved_source_game_remap_decisions"),
        "resolved_second_pass_decisions": summarize_run(args.root, "resolved_second_pass_decisions"),
        "resolved_route_followup_dossiers": summarize_run(args.root, "resolved_route_followup_dossiers"),
        "resolved_package_decision_apply_runs": summarize_run(args.root, "resolved_package_decision_apply_runs"),
        "plan_closeout_reports": summarize_run(args.root, "plan_closeout_reports"),
    }
    read_state_path = Path(runs["llm_read_state"]["path"]) if runs["llm_read_state"].get("path") else None
    db_counts, db_warnings = load_db_counts(args.db_path)
    resolved_db_counts, resolved_db_warnings = load_resolved_counts(args.db_path)
    resolved_current_db_counts, resolved_current_db_warnings = load_resolved_current_counts(args.db_path)
    final_db_counts, final_db_warnings = load_final_counts(args.db_path)
    latest_paths = {key: value.get("path", "") for key, value in runs.items()}
    page_visual_summary = runs["page_visual_review_packets"].get("summary", {})
    page_visual_progress = load_page_visual_progress(
        args.db_path,
        str(page_visual_summary.get("page_visual_review_packet_run_id") or ""),
        as_int(page_visual_summary.get("item_count")) if isinstance(page_visual_summary, dict) else 0,
    )

    report: dict[str, Any] = {
        "created_at_utc": iso_now(),
        "root": str(args.root),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "latest_paths": latest_paths,
        "db_counts": db_counts,
        "db_warnings": db_warnings,
        "resolved_db_counts": resolved_db_counts,
        "resolved_db_warnings": resolved_db_warnings,
        "resolved_current_db_counts": resolved_current_db_counts,
        "resolved_current_db_warnings": resolved_current_db_warnings,
        "final_db_counts": final_db_counts,
        "final_db_warnings": final_db_warnings,
        "document_queues": document_queue_summaries(args.root),
        "first_unread_packet": first_unread_packet(read_state_path),
        "ocr_sidecar_status": ocr_sidecar_status(args.root),
        "page_visual_progress": page_visual_progress,
        **runs,
    }
    report["next_actions"] = derive_next_actions(report)

    write_json(out_dir / "conveyor_status_report.json", report)
    write_markdown(out_dir / "conveyor_status_report.md", report)
    print(json.dumps({
        "created_at_utc": report["created_at_utc"],
        "output_dir": str(out_dir),
        "next_actions": report["next_actions"],
        "latest_packet_run": latest_paths.get("llm_packets", ""),
        "latest_read_state": latest_paths.get("llm_read_state", ""),
        "ocr_sidecar_status": report["ocr_sidecar_status"],
        "db_counts_subset": {
            "atom_claim": db_counts.get("atom_claim"),
            "promotion_candidate": db_counts.get("promotion_candidate"),
            "llm_review_claim": db_counts.get("llm_review_claim"),
            "llm_player_game_box_score": db_counts.get("llm_player_game_box_score"),
            "readiness_promotion_lane_item": db_counts.get("readiness_promotion_lane_item"),
            "resolved_atom_materialize_run": db_counts.get("resolved_atom_materialize_run"),
            "resolved_schema_gap_patch_item": db_counts.get("resolved_schema_gap_patch_item"),
            "resolved_game_mapping_triage_item": db_counts.get("resolved_game_mapping_triage_item"),
            "resolved_promotion_package_item": db_counts.get("resolved_promotion_package_item"),
            "resolved_package_review_packet_item": db_counts.get("resolved_package_review_packet_item"),
            "resolved_package_decision_ledger": db_counts.get("resolved_package_decision_ledger"),
            "resolved_package_decision_route_queue": db_counts.get("resolved_package_decision_route_queue"),
            "resolved_package_applied_final_row": db_counts.get("resolved_package_applied_final_row"),
            "resolved_safe_lineup_decision_item": db_counts.get("resolved_safe_lineup_decision_item"),
            "resolved_safe_scoring_decision_item": db_counts.get("resolved_safe_scoring_decision_item"),
            "resolved_safe_team_stat_decision_item": db_counts.get("resolved_safe_team_stat_decision_item"),
            "resolved_safe_pbp_decision_item": db_counts.get("resolved_safe_pbp_decision_item"),
            "resolved_safe_identity_candidate_decision_item": db_counts.get("resolved_safe_identity_candidate_decision_item"),
            "resolved_identity_event_decision_item": db_counts.get("resolved_identity_event_decision_item"),
            "resolved_safe_game_candidate_decision_item": db_counts.get("resolved_safe_game_candidate_decision_item"),
            "resolved_direct_source_decision_item": db_counts.get("resolved_direct_source_decision_item"),
            "resolved_touchdown_total_decision_item": db_counts.get("resolved_touchdown_total_decision_item"),
            "resolved_source_game_remap_decision_item": db_counts.get("resolved_source_game_remap_decision_item"),
            "resolved_second_pass_decision_item": db_counts.get("resolved_second_pass_decision_item"),
        },
        "resolved_db_counts": resolved_db_counts,
        "resolved_current_db_counts": resolved_current_db_counts,
        "final_db_counts": final_db_counts,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
