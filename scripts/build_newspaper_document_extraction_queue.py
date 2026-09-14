#!/usr/bin/env python
"""Build a document-level newspaper extraction queue.

The game-level queue is useful for coverage, but it is too coarse for the
actual conveyor: one game can have many PDF pulls, and one successful atom
should not stop the remaining source documents from being OCRed and parsed.

This script classifies each PDF document independently using OCR sidecars,
local newspaper_review document state, and local atom/domain rows. It writes
auditable inventories plus OCR/article/review handoff manifests.
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


DEFAULT_PLAN_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\article_batch_plans")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\document_extraction_queues")
DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def latest_document_inventory(root: Path) -> Path:
    candidates = sorted(root.glob("*/document_inventory.csv"))
    if not candidates:
        raise FileNotFoundError(f"No document_inventory.csv found under {root}")
    return candidates[-1]


def load_review_rollups(db_path: Path) -> tuple[dict[str, dict[str, Any]], str, str]:
    if not db_path.exists():
        return {}, "missing", "db_path_not_found"
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, "unavailable", f"{type(exc).__name__}: {exc}"
    try:
        states = {
            row[0]: {
                "latest_run_id": row[1] or "",
                "current_station": row[2] or "",
                "current_status": row[3] or "",
                "state_next_action": row[4] or "",
                "terminal_status": row[5] or "",
                "terminal_reason": row[6] or "",
            }
            for row in con.execute(
                """
                SELECT
                  source_document_id,
                  latest_run_id,
                  current_station,
                  current_status,
                  next_action,
                  terminal_status,
                  terminal_reason
                FROM newspaper_review.conveyor_document_state
                """
            ).fetchall()
        }
        atoms = {
            row[0]: {
                "doc_atom_count": row[1] or 0,
                "doc_game_candidate_atoms": row[2] or 0,
                "doc_player_stat_atoms": row[3] or 0,
                "doc_scoring_event_atoms": row[4] or 0,
                "doc_pbp_event_atoms": row[5] or 0,
                "doc_lineup_atoms": row[6] or 0,
            }
            for row in con.execute(
                """
                SELECT
                  source_document_id,
                  COUNT(*) AS atom_count,
                  SUM(CASE WHEN semantic_target_table = 'game_candidate' THEN 1 ELSE 0 END) AS game_candidate_atoms,
                  SUM(CASE WHEN semantic_target_table = 'player_game_box_score' THEN 1 ELSE 0 END) AS player_stat_atoms,
                  SUM(CASE WHEN semantic_target_table = 'scoring_event' THEN 1 ELSE 0 END) AS scoring_event_atoms,
                  SUM(CASE WHEN semantic_target_table = 'play_by_play_event' THEN 1 ELSE 0 END) AS pbp_event_atoms,
                  SUM(CASE WHEN semantic_target_table = 'lineup_participation' THEN 1 ELSE 0 END) AS lineup_atoms
                FROM newspaper_review.atom_claim
                GROUP BY source_document_id
                """
            ).fetchall()
        }
        promotions = {
            row[0]: row[1] or 0
            for row in con.execute(
                """
                SELECT a.source_document_id, COUNT(*) AS promotion_candidates
                FROM newspaper_review.promotion_candidate p
                JOIN newspaper_review.atom_claim a ON a.atom_claim_id = p.atom_claim_id
                GROUP BY a.source_document_id
                """
            ).fetchall()
        }
        rollups: dict[str, dict[str, Any]] = {}
        for source_document_id, state in states.items():
            rollups.setdefault(source_document_id, {}).update(state)
        for source_document_id, atom in atoms.items():
            rollups.setdefault(source_document_id, {}).update(atom)
        for source_document_id, count in promotions.items():
            rollups.setdefault(source_document_id, {})["doc_promotion_candidates"] = count
        return rollups, "loaded", ""
    except Exception as exc:
        return {}, "unavailable", f"{type(exc).__name__}: {exc}"
    finally:
        con.close()


def sidecar_ready(row: dict[str, str]) -> bool:
    return (
        bool(row.get("suggested_ocr_text_path"))
        and bool(row.get("suggested_ocr_json_path"))
        and Path(row["suggested_ocr_text_path"]).exists()
        and Path(row["suggested_ocr_json_path"]).exists()
    )


def classify_document(row: dict[str, str], rollup: dict[str, Any]) -> tuple[str, str, str]:
    atom_count = int(rollup.get("doc_atom_count") or 0)
    promotions = int(rollup.get("doc_promotion_candidates") or 0)
    event_atoms = (
        int(rollup.get("doc_player_stat_atoms") or 0)
        + int(rollup.get("doc_scoring_event_atoms") or 0)
        + int(rollup.get("doc_pbp_event_atoms") or 0)
    )
    current_station = str(rollup.get("current_station") or "")
    current_status = str(rollup.get("current_status") or "")

    if atom_count:
        if promotions:
            return "review_promotion_candidates", "review", "document_has_promotion_candidates"
        if event_atoms:
            return "review_event_atoms", "review", "document_has_event_or_stat_atoms"
        if current_station == "article_semantic_reparse" or current_status == "semantic_atoms_extracted":
            return "review_score_lineup_marker_atoms", "review", "semantic_reparse_done_score_lineup_or_marker_atoms"
        return "queue_semantic_reparse", "medium", "document_has_only_score_lineup_or_marker_atoms"

    if current_status == "semantic_reparse_no_atoms":
        return "terminal_no_current_atoms", "terminal", "semantic_reparse_done_no_atoms"
    if current_status == "semantic_reparse_no_regions":
        return "terminal_no_article_regions", "terminal", "semantic_reparse_done_no_regions"
    if current_status == "exhausted_article_text_no_atoms":
        return "queue_semantic_reparse", "medium", "article_text_exists_no_current_atoms"
    if current_status in {"exhausted_low_signal_article_crops", "low_signal_or_weak_ocr"}:
        return "terminal_low_signal", "terminal", "article_crop_attempt_low_signal"
    if current_status == "asset_missing":
        return "asset_missing", "asset", "article_conveyor_missing_pdf"

    if sidecar_ready(row):
        return "queue_article_atom_conveyor", "high", "ocr_sidecar_ready_no_document_atoms"
    return "queue_full_page_ocr", "high", "no_ocr_sidecar_for_pdf_document"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--document-inventory", type=Path, default=None)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="document_extraction_queue")
    parser.add_argument("--batch-filter", default="", help="Optional round_name/batch label to limit handoff manifests.")
    parser.add_argument(
        "--filter-manifest",
        type=Path,
        action="append",
        default=[],
        help="Optional manifest(s) whose candidate_id values define the handoff document set.",
    )
    parser.add_argument("--max-ocr-docs", type=int, default=None)
    parser.add_argument("--max-article-docs", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    document_inventory = args.document_inventory or latest_document_inventory(DEFAULT_PLAN_ROOT)
    rows = read_csv(document_inventory)
    rollups, rollup_status, rollup_error = load_review_rollups(args.db_path)

    out_dir = args.out_root / f"{stamp()}_{args.label}"
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory_rows: list[dict[str, Any]] = []
    for row in rows:
        source_document_id = row.get("candidate_id") or ""
        rollup = rollups.get(source_document_id, {})
        next_action, lane, reason = classify_document(row, rollup)
        record = {
            **row,
            "source_document_id": source_document_id,
            "document_ocr_sidecar_ready": sidecar_ready(row),
            "latest_run_id": rollup.get("latest_run_id", ""),
            "current_station": rollup.get("current_station", ""),
            "current_status": rollup.get("current_status", ""),
            "terminal_status": rollup.get("terminal_status", ""),
            "doc_atom_count": rollup.get("doc_atom_count", 0),
            "doc_game_candidate_atoms": rollup.get("doc_game_candidate_atoms", 0),
            "doc_player_stat_atoms": rollup.get("doc_player_stat_atoms", 0),
            "doc_scoring_event_atoms": rollup.get("doc_scoring_event_atoms", 0),
            "doc_pbp_event_atoms": rollup.get("doc_pbp_event_atoms", 0),
            "doc_lineup_atoms": rollup.get("doc_lineup_atoms", 0),
            "doc_promotion_candidates": rollup.get("doc_promotion_candidates", 0),
            "document_next_action": next_action,
            "document_work_lane": lane,
            "document_reason_code": reason,
        }
        inventory_rows.append(record)

    base_fields = list(rows[0].keys()) if rows else []
    extra_fields = [
        "source_document_id",
        "document_ocr_sidecar_ready",
        "latest_run_id",
        "current_station",
        "current_status",
        "terminal_status",
        "doc_atom_count",
        "doc_game_candidate_atoms",
        "doc_player_stat_atoms",
        "doc_scoring_event_atoms",
        "doc_pbp_event_atoms",
        "doc_lineup_atoms",
        "doc_promotion_candidates",
        "document_next_action",
        "document_work_lane",
        "document_reason_code",
    ]
    fields = base_fields + [field for field in extra_fields if field not in base_fields]
    write_csv(out_dir / "document_pdf_atom_inventory.csv", inventory_rows, fields)

    actionable_rows = [
        row for row in inventory_rows
        if str(row["document_next_action"]).startswith(("queue_", "review_"))
    ]
    write_csv(out_dir / "document_next_action_queue.csv", actionable_rows, fields)

    handoff_source = inventory_rows
    filter_candidate_ids: set[str] = set()
    for manifest_path in args.filter_manifest:
        for manifest_row in read_csv(manifest_path):
            candidate_id = str(manifest_row.get("candidate_id") or "").strip()
            if candidate_id:
                filter_candidate_ids.add(candidate_id)
    if filter_candidate_ids:
        handoff_source = [
            row for row in handoff_source
            if str(row.get("candidate_id") or "").strip() in filter_candidate_ids
        ]
    elif args.batch_filter:
        handoff_source = [
            row for row in handoff_source
            if args.batch_filter in str(row.get("round_name") or "")
            or args.batch_filter in str(row.get("candidate_checkpoint") or "")
        ]

    filtered_actionable_rows = [
        row for row in handoff_source
        if str(row["document_next_action"]).startswith(("queue_", "review_"))
    ]
    write_csv(out_dir / "filtered_document_next_action_queue.csv", filtered_actionable_rows, fields)

    ocr_rows = [row for row in handoff_source if row["document_next_action"] == "queue_full_page_ocr"]
    article_rows = [row for row in handoff_source if row["document_next_action"] == "queue_article_atom_conveyor"]
    semantic_rows = [row for row in handoff_source if row["document_next_action"] == "queue_semantic_reparse"]
    review_event_rows = [row for row in handoff_source if row["document_next_action"] == "review_event_atoms"]
    review_score_lineup_rows = [
        row for row in handoff_source if row["document_next_action"] == "review_score_lineup_marker_atoms"
    ]
    review_promotion_rows = [
        row for row in handoff_source if row["document_next_action"] == "review_promotion_candidates"
    ]
    if args.max_ocr_docs is not None:
        ocr_rows = ocr_rows[: args.max_ocr_docs]
    if args.max_article_docs is not None:
        article_rows = article_rows[: args.max_article_docs]
    write_csv(out_dir / "next_ocr_round_manifest.csv", ocr_rows, fields)
    write_csv(out_dir / "next_article_candidate_manifest.csv", article_rows, fields)
    write_csv(out_dir / "next_semantic_reparse_manifest.csv", semantic_rows, fields)
    write_csv(out_dir / "next_review_event_manifest.csv", review_event_rows, fields)
    write_csv(out_dir / "next_review_score_lineup_marker_manifest.csv", review_score_lineup_rows, fields)
    write_csv(out_dir / "next_review_promotion_candidate_manifest.csv", review_promotion_rows, fields)

    next_actions = Counter(row["document_next_action"] for row in inventory_rows)
    filtered_next_actions = Counter(row["document_next_action"] for row in handoff_source)
    by_batch = Counter(str(row.get("round_name") or "") for row in inventory_rows)
    summary = {
        "created_at_utc": iso_now(),
        "label": args.label,
        "document_inventory": str(document_inventory),
        "db_path": str(args.db_path),
        "out_dir": str(out_dir),
        "documents": len(inventory_rows),
        "actionable_documents": len(actionable_rows),
        "next_action_counts": dict(sorted(next_actions.items())),
        "batch_filter": args.batch_filter,
        "filter_manifests": [str(path) for path in args.filter_manifest],
        "filter_candidate_ids": len(filter_candidate_ids),
        "filtered_documents": len(handoff_source),
        "filtered_actionable_documents": len(filtered_actionable_rows),
        "filtered_next_action_counts": dict(sorted(filtered_next_actions.items())),
        "handoff_ocr_documents": len(ocr_rows),
        "handoff_article_documents": len(article_rows),
        "handoff_semantic_reparse_documents": len(semantic_rows),
        "handoff_review_event_documents": len(review_event_rows),
        "handoff_review_score_lineup_marker_documents": len(review_score_lineup_rows),
        "handoff_review_promotion_candidate_documents": len(review_promotion_rows),
        "rollup_status": rollup_status,
        "rollup_error": rollup_error,
        "batches_seen": len(by_batch),
        "live_tables_touched": False,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out_dir / "README.md").write_text(
        "\n".join(
            [
                "# Newspaper Document Extraction Queue",
                "",
                f"Created: `{summary['created_at_utc']}`",
                f"Document inventory: `{document_inventory}`",
                "",
                "This queue is document-level. One game-level atom does not stop the other PDFs for that game.",
                "",
                "## Outputs",
                "",
                "- `document_pdf_atom_inventory.csv`: every PDF document and current station.",
                "- `document_next_action_queue.csv`: documents still requiring queue/review action.",
                "- `filtered_document_next_action_queue.csv`: batch-filtered queue/review action rows.",
                "- `next_ocr_round_manifest.csv`: bounded OCR handoff.",
                "- `next_article_candidate_manifest.csv`: bounded article atom handoff.",
                "- `next_semantic_reparse_manifest.csv`: batch-filtered semantic reparse handoff.",
                "- `next_review_event_manifest.csv`: batch-filtered event atom review handoff.",
                "- `next_review_score_lineup_marker_manifest.csv`: batch-filtered score/lineup marker review handoff.",
                "- `next_review_promotion_candidate_manifest.csv`: batch-filtered promotion-candidate review handoff.",
                "",
                "Live Fly/DuckDB/supertable writes: no.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
