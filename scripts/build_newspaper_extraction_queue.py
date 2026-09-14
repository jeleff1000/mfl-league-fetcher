#!/usr/bin/env python
"""Build a newspaper PDF-to-atom extraction queue.

This is a control-plane audit for the newspaper conveyor. It compares the
pre-PBP target game queue with PDFs already extracted on disk and with local
newspaper_review atom rows, then emits the next station each game should enter.

It does not write to live Fly, ___ops, ___leagues, or the supertable.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_QUEUE = Path(
    r"D:\league-history-data\nfl\derived\validation\newspaper_acquisition_queue"
    r"\20260620T_v26_pre_pbp_newspaper_acquisition_queue"
    r"\pre_pbp_newspaper_acquisition_queue_1920_1978.csv"
)
DEFAULT_RAW_ROOT = Path(
    r"D:\league-history-data\nfl\raw\newspaper_archives"
    r"\source_horizon_game_candidates\game_completeness_download_pilots"
)
DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\extraction_queues")
DEFAULT_ROUNDS_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\article_atom_rounds")

GAME_ID_RE = re.compile(r"\b(19[0-9]{2}[0-9]{4}[a-z]{3})\b", re.I)


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


def scan_pdfs(raw_root: Path, target_game_ids: set[str]) -> dict[str, list[str]]:
    pdfs_by_game: dict[str, list[str]] = defaultdict(list)
    if not raw_root.exists():
        return pdfs_by_game
    for game_id in sorted(target_game_ids):
        game_dir = raw_root / game_id
        if not game_dir.exists():
            continue
        for pdf in game_dir.rglob("*.pdf"):
            pdfs_by_game[game_id].append(str(pdf))
    return {game_id: sorted(paths) for game_id, paths in pdfs_by_game.items()}


def load_atom_rollup(db_path: Path) -> tuple[dict[str, dict[str, Any]], str, str]:
    if not db_path.exists():
        return {}, "missing", "db_path_not_found"
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, "unavailable", f"{type(exc).__name__}: {exc}"
    try:
        atom_rows = con.execute(
            """
            SELECT
              boxscore_id,
              COUNT(*) AS atom_count,
              COUNT(DISTINCT source_document_id) AS atom_documents,
              SUM(CASE WHEN semantic_target_table = 'game_candidate' THEN 1 ELSE 0 END) AS game_candidate_atoms,
              SUM(CASE WHEN semantic_target_table = 'player_game_box_score' THEN 1 ELSE 0 END) AS player_stat_atoms,
              SUM(CASE WHEN semantic_target_table = 'scoring_event' THEN 1 ELSE 0 END) AS scoring_event_atoms,
              SUM(CASE WHEN semantic_target_table = 'play_by_play_event' THEN 1 ELSE 0 END) AS pbp_event_atoms,
              SUM(CASE WHEN semantic_target_table = 'lineup_participation' THEN 1 ELSE 0 END) AS lineup_atoms,
              MAX(run_id) AS latest_atom_run_id
            FROM newspaper_review.atom_claim
            WHERE boxscore_id IS NOT NULL AND boxscore_id <> ''
            GROUP BY boxscore_id
            """
        ).fetchall()
        rollup = {
            row[0]: {
                "atom_count": row[1] or 0,
                "atom_documents": row[2] or 0,
                "game_candidate_atoms": row[3] or 0,
                "player_stat_atoms": row[4] or 0,
                "scoring_event_atoms": row[5] or 0,
                "pbp_event_atoms": row[6] or 0,
                "lineup_atoms": row[7] or 0,
                "latest_atom_run_id": row[8] or "",
            }
            for row in atom_rows
        }
        promo_rows = con.execute(
            """
            SELECT a.boxscore_id, COUNT(*) AS promotion_candidates
            FROM newspaper_review.promotion_candidate p
            JOIN newspaper_review.atom_claim a ON a.atom_claim_id = p.atom_claim_id
            WHERE a.boxscore_id IS NOT NULL AND a.boxscore_id <> ''
            GROUP BY a.boxscore_id
            """
        ).fetchall()
        for boxscore_id, count in promo_rows:
            rollup.setdefault(boxscore_id, {})["promotion_candidates"] = count or 0
        return rollup, "loaded", ""
    except Exception as exc:
        return {}, "unavailable", f"{type(exc).__name__}: {exc}"
    finally:
        con.close()


def new_rollup_row(run_id: str) -> dict[str, Any]:
    return {
        "atom_count": 0,
        "atom_documents": 0,
        "game_candidate_atoms": 0,
        "player_stat_atoms": 0,
        "scoring_event_atoms": 0,
        "pbp_event_atoms": 0,
        "lineup_atoms": 0,
        "promotion_candidates": 0,
        "latest_atom_run_id": run_id,
        "_documents": set(),
        "_atom_ids": set(),
    }


def finalize_file_rollup(rollup: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    for item in rollup.values():
        item["atom_documents"] = len(item.pop("_documents", set()))
        item.pop("_atom_ids", None)
    return rollup


def load_atom_rollup_from_round_files(rounds_root: Path) -> tuple[dict[str, dict[str, Any]], str, str]:
    if not rounds_root.exists():
        return {}, "missing", "rounds_root_not_found"

    claim_files = sorted(rounds_root.glob("*/article_atom_claims.csv"))
    if not claim_files:
        return {}, "missing", "no_article_atom_claims_csv_files"

    latest_run_by_document: dict[str, str] = {}
    file_by_run: dict[str, Path] = {}
    try:
        for path in claim_files:
            run_id = path.parent.name
            file_by_run[run_id] = path
            for row in read_csv(path):
                document_id = row.get("source_document_id") or ""
                if document_id and run_id > latest_run_by_document.get(document_id, ""):
                    latest_run_by_document[document_id] = run_id

        rollup: dict[str, dict[str, Any]] = {}
        for run_id, path in sorted(file_by_run.items()):
            for row in read_csv(path):
                document_id = row.get("source_document_id") or ""
                boxscore_id = (row.get("boxscore_id") or "").lower()
                if not document_id or not boxscore_id:
                    continue
                if latest_run_by_document.get(document_id) != run_id:
                    continue

                item = rollup.setdefault(boxscore_id, new_rollup_row(run_id))
                item["latest_atom_run_id"] = max(str(item["latest_atom_run_id"]), run_id)
                item["_documents"].add(document_id)

                atom_id = row.get("atom_claim_id") or f"{run_id}:{document_id}:{row.get('region_id', '')}:{row.get('atom_type', '')}"
                if atom_id in item["_atom_ids"]:
                    continue
                item["_atom_ids"].add(atom_id)
                item["atom_count"] += 1

                target_table = row.get("semantic_target_table") or ""
                if target_table == "game_candidate":
                    item["game_candidate_atoms"] += 1
                elif target_table == "player_game_box_score":
                    item["player_stat_atoms"] += 1
                elif target_table == "scoring_event":
                    item["scoring_event_atoms"] += 1
                elif target_table == "play_by_play_event":
                    item["pbp_event_atoms"] += 1
                elif target_table == "lineup_participation":
                    item["lineup_atoms"] += 1

                promotion_status = row.get("promotion_status") or ""
                if promotion_status and promotion_status != "not_promoted":
                    item["promotion_candidates"] += 1

        return finalize_file_rollup(rollup), "loaded_from_round_files", ""
    except Exception as exc:
        return {}, "unavailable", f"{type(exc).__name__}: {exc}"


def classify_next_action(pdf_count: int, atom: dict[str, Any]) -> tuple[str, str, str]:
    atom_count = int(atom.get("atom_count") or 0)
    player_stats = int(atom.get("player_stat_atoms") or 0)
    scoring_events = int(atom.get("scoring_event_atoms") or 0)
    pbp_events = int(atom.get("pbp_event_atoms") or 0)
    promotions = int(atom.get("promotion_candidates") or 0)
    latest_run_id = str(atom.get("latest_atom_run_id") or "")
    if pdf_count == 0:
        return ("missing_pdf_acquisition", "none", "no_pdf_for_target_game")
    if atom_count == 0:
        return ("queue_ocr_article_semantic", "high", "pdf_exists_no_atom_rows")
    if player_stats or scoring_events or pbp_events:
        if promotions:
            return ("review_promotion_candidates", "review", "stat_or_event_candidates_exist")
        return ("review_event_atoms", "review", "event_atoms_exist_no_promotions")
    if "semantic_reparse" in latest_run_id:
        return ("review_score_lineup_marker_atoms", "review", "semantic_reparse_done_score_lineup_or_marker_atoms")
    return ("queue_semantic_reparse", "medium", "only_score_lineup_or_marker_atoms")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--rounds-root", type=Path, default=DEFAULT_ROUNDS_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="")
    parser.add_argument("--year-min", type=int, default=1920)
    parser.add_argument("--year-max", type=int, default=1939)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = []
    for row in read_csv(args.queue):
        year_raw = row.get("year") or ""
        if not year_raw.isdigit():
            continue
        year = int(year_raw)
        if args.year_min <= year <= args.year_max and row.get("boxscore_id"):
            rows.append(row)

    target_game_ids = {row["boxscore_id"].lower() for row in rows}
    pdfs_by_game = scan_pdfs(args.raw_root, target_game_ids)
    atom_rollup, atom_rollup_status, atom_rollup_error = load_atom_rollup(args.db_path)
    if atom_rollup_status != "loaded":
        atom_rollup, file_status, file_error = load_atom_rollup_from_round_files(args.rounds_root)
        atom_rollup_status = f"{atom_rollup_status}; {file_status}"
        atom_rollup_error = "; ".join(part for part in [atom_rollup_error, file_error] if part)
    run_label = args.label or f"{args.year_min}_{args.year_max}_pdf_to_atom_queue"
    out_dir = args.out_root / f"{stamp()}_{run_label}"
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory_rows: list[dict[str, Any]] = []
    queue_rows: list[dict[str, Any]] = []
    for row in rows:
        boxscore_id = row["boxscore_id"].lower()
        pdfs = pdfs_by_game.get(boxscore_id, [])
        atom = atom_rollup.get(boxscore_id, {})
        next_action, lane, reason = classify_next_action(len(pdfs), atom)
        record = {
            "boxscore_id": boxscore_id,
            "year": int(row["year"]),
            "week": row.get("week", ""),
            "season_type": row.get("season_type", ""),
            "game_date": row.get("game_date", ""),
            "away_team": row.get("away_team", ""),
            "home_team": row.get("home_team", ""),
            "acquisition_priority_band": row.get("acquisition_priority_band", ""),
            "acquisition_score": row.get("acquisition_score", ""),
            "pdf_count": len(pdfs),
            "sample_pdf": pdfs[0] if pdfs else "",
            "atom_count": atom.get("atom_count", 0),
            "atom_documents": atom.get("atom_documents", 0),
            "game_candidate_atoms": atom.get("game_candidate_atoms", 0),
            "player_stat_atoms": atom.get("player_stat_atoms", 0),
            "scoring_event_atoms": atom.get("scoring_event_atoms", 0),
            "pbp_event_atoms": atom.get("pbp_event_atoms", 0),
            "lineup_atoms": atom.get("lineup_atoms", 0),
            "promotion_candidates": atom.get("promotion_candidates", 0),
            "latest_atom_run_id": atom.get("latest_atom_run_id", ""),
            "next_action": next_action,
            "work_lane": lane,
            "reason_code": reason,
        }
        inventory_rows.append(record)
        queue_rows.append(record)

    fields = [
        "boxscore_id",
        "year",
        "week",
        "season_type",
        "game_date",
        "away_team",
        "home_team",
        "acquisition_priority_band",
        "acquisition_score",
        "pdf_count",
        "sample_pdf",
        "atom_count",
        "atom_documents",
        "game_candidate_atoms",
        "player_stat_atoms",
        "scoring_event_atoms",
        "pbp_event_atoms",
        "lineup_atoms",
        "promotion_candidates",
        "latest_atom_run_id",
        "next_action",
        "work_lane",
        "reason_code",
    ]
    write_csv(out_dir / "game_pdf_atom_inventory.csv", inventory_rows, fields)
    write_csv(out_dir / "game_pdf_atom_next_action_queue.csv", queue_rows, fields)

    next_action_counts = Counter(row["next_action"] for row in inventory_rows)
    pdf_depth = Counter("0" if row["pdf_count"] == 0 else "1" if row["pdf_count"] == 1 else "2" if row["pdf_count"] == 2 else "3+" for row in inventory_rows)
    by_year = Counter(row["year"] for row in inventory_rows)
    covered_by_year = Counter(row["year"] for row in inventory_rows if row["pdf_count"] > 0)
    atom_by_year = Counter(row["year"] for row in inventory_rows if row["atom_count"] > 0)
    summary = {
        "created_at_utc": iso_now(),
        "queue": str(args.queue),
        "raw_root": str(args.raw_root),
        "db_path": str(args.db_path),
        "rounds_root": str(args.rounds_root),
        "out_dir": str(out_dir),
        "year_min": args.year_min,
        "year_max": args.year_max,
        "target_games": len(inventory_rows),
        "games_with_pdf": sum(1 for row in inventory_rows if row["pdf_count"] > 0),
        "games_without_pdf": sum(1 for row in inventory_rows if row["pdf_count"] == 0),
        "games_with_atoms": sum(1 for row in inventory_rows if row["atom_count"] > 0),
        "games_with_promotion_candidates": sum(1 for row in inventory_rows if row["promotion_candidates"] > 0),
        "atom_rollup_status": atom_rollup_status,
        "atom_rollup_error": atom_rollup_error,
        "next_action_counts": dict(sorted(next_action_counts.items())),
        "pdf_depth_counts": dict(sorted(pdf_depth.items())),
        "targets_by_year": dict(sorted(by_year.items())),
        "pdf_covered_by_year": dict(sorted(covered_by_year.items())),
        "atom_covered_by_year": dict(sorted(atom_by_year.items())),
        "live_tables_touched": False,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out_dir / "README.md").write_text(
        "\n".join(
            [
                "# Newspaper PDF-to-Atom Extraction Queue",
                "",
                f"Created: `{summary['created_at_utc']}`",
                "",
                f"Target years: `{args.year_min}-{args.year_max}`",
                "",
                f"Target games: `{summary['target_games']}`",
                f"Games with PDF: `{summary['games_with_pdf']}`",
                f"Games without PDF: `{summary['games_without_pdf']}`",
                f"Games with local atom rows: `{summary['games_with_atoms']}`",
                f"Games with promotion candidates: `{summary['games_with_promotion_candidates']}`",
                f"Atom rollup status: `{summary['atom_rollup_status']}`",
                "",
                "Live Fly/DuckDB tables touched: no.",
                "",
                "## Outputs",
                "",
                "- `game_pdf_atom_inventory.csv`: every target game and its current PDF/atom state.",
                "- `game_pdf_atom_next_action_queue.csv`: games that still need acquisition, OCR, semantic reparse, or review.",
                "- `summary.json`: aggregate counts for gating and audit.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
