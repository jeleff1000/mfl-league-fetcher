#!/usr/bin/env python
"""Plan bounded newspaper OCR/article batches from the extraction queue.

This expands game-level PDF coverage into document-level OCR and article
manifests. It is a control-plane planner only: it writes local CSV/JSON/runbook
artifacts and never writes to Fly, ___ops, ___leagues, or the live supertable.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXTRACTION_QUEUE_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\extraction_queues")
DEFAULT_RAW_ROOT = Path(
    r"D:\league-history-data\nfl\raw\newspaper_archives"
    r"\source_horizon_game_candidates\game_completeness_download_pilots"
)
DEFAULT_OCR_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\ocr_sidecars")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\article_batch_plans")
ARTICLE_CONVEYOR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor\newspapers_article_atom_conveyor.py")
OCR_CONVEYOR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor\newspapers_ocr_round.py")

RESULT_IMAGE_RE = re.compile(r"result_(?:\d{3}_)?(\d{5,})", re.I)
PAGE_RE = re.compile(r"(?:^|[_-])p(?:age)?[_-]?(\d+)(?:[_-]|$)", re.I)

MANIFEST_FIELDS = [
    "processed_at_utc",
    "round_name",
    "candidate_id",
    "boxscore_id",
    "candidate_rank",
    "image_id",
    "year",
    "game_date",
    "away_team",
    "home_team",
    "publication",
    "result_date",
    "page",
    "candidate_status",
    "document_status",
    "next_action",
    "ocr_sidecar_dir",
    "suggested_ocr_text_path",
    "suggested_ocr_json_path",
    "asset_signature",
    "pdf_path",
    "pdf_exists",
    "pdf_bytes",
    "pdf_pages",
    "pdf_valid",
    "pdf_text_chars",
    "pdf_viewer_only",
    "text_source_type",
    "extractable_text_chars",
    "stat_keyword_hits",
    "stat_keyword_terms",
    "atom_claim_count",
    "sidecar_text_files",
    "result_root",
    "candidate_checkpoint",
    "elapsed_ms",
    "notes",
    "article_score",
    "strong_hits",
    "weak_hits",
    "noise_hits",
    "ocr_chars",
    "article_decision",
    "article_next_action",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stable_id(*parts: Any, length: int = 20) -> str:
    text = "|".join(str(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:length]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        return {}


def latest_extraction_queue(root: Path) -> Path:
    candidates = sorted(root.glob("*/game_pdf_atom_next_action_queue.csv"))
    if not candidates:
        raise FileNotFoundError(f"No game_pdf_atom_next_action_queue.csv files under {root}")
    return candidates[-1]


def image_id_from_result(result_root: Path, meta: dict[str, Any], pdf: Path) -> str:
    for key in ["imageId", "image_id", "image"]:
        value = meta.get(key)
        if value:
            return str(value)
    match = RESULT_IMAGE_RE.search(result_root.name)
    if match:
        return match.group(1)
    match = re.search(r"(\d{5,})", pdf.name)
    if match:
        return match.group(1)
    return stable_id(result_root, pdf, length=12)


def page_from_pdf(pdf: Path, meta: dict[str, Any]) -> str:
    for key in ["page", "pageNumber", "page_number"]:
        value = meta.get(key)
        if value not in (None, ""):
            return str(value)
    match = PAGE_RE.search(pdf.stem)
    return match.group(1) if match else ""


def asset_signature(pdf: Path) -> str:
    try:
        stat = pdf.stat()
        return stable_id(pdf, stat.st_size, int(stat.st_mtime), length=24)
    except OSError:
        return stable_id(pdf, length=24)


def sidecar_paths(ocr_root: Path, boxscore_id: str, rank: int, image_id: str) -> tuple[Path, Path, Path]:
    safe_image_id = re.sub(r"[^A-Za-z0-9_-]+", "_", image_id) or f"img_{rank}"
    sidecar_dir = ocr_root / boxscore_id / f"{boxscore_id}_{rank}_{safe_image_id}"
    return sidecar_dir, sidecar_dir / "ocr_text.txt", sidecar_dir / "ocr_layout.json"


def result_root_for_pdf(pdf: Path) -> Path:
    if pdf.parent.name.lower() == "downloads":
        return pdf.parent.parent
    return pdf.parent


def collect_game_documents(row: dict[str, str], raw_root: Path, ocr_root: Path) -> list[dict[str, Any]]:
    boxscore_id = row["boxscore_id"].lower()
    game_dir = raw_root / boxscore_id
    if not game_dir.exists():
        return []

    raw_docs: list[dict[str, Any]] = []
    for pdf in sorted(game_dir.rglob("*.pdf")):
        result_root = result_root_for_pdf(pdf)
        meta = read_json(result_root / "source_meta.json")
        image_id = image_id_from_result(result_root, meta, pdf)
        raw_docs.append(
            {
                "pdf": pdf,
                "result_root": result_root,
                "meta": meta,
                "image_id": image_id,
                "source_score": float(meta.get("score") or 0),
            }
        )

    raw_docs.sort(key=lambda item: (-item["source_score"], str(item["result_root"]), str(item["pdf"])))
    docs: list[dict[str, Any]] = []
    seen_image_ids: set[str] = set()
    for rank, item in enumerate(raw_docs, start=1):
        pdf = item["pdf"]
        meta = item["meta"]
        image_id = item["image_id"]
        if image_id in seen_image_ids:
            continue
        seen_image_ids.add(image_id)
        sidecar_dir, text_path, json_path = sidecar_paths(ocr_root, boxscore_id, rank, image_id)
        text_exists = text_path.exists()
        json_exists = json_path.exists()
        ocr_chars = len(text_path.read_text(encoding="utf-8", errors="ignore")) if text_exists else 0
        candidate_id = f"{boxscore_id}#{rank}:{image_id}"
        docs.append(
            {
                "processed_at_utc": iso_now(),
                "round_name": "",
                "candidate_id": candidate_id,
                "boxscore_id": boxscore_id,
                "candidate_rank": rank,
                "image_id": image_id,
                "year": row.get("year", ""),
                "game_date": row.get("game_date", ""),
                "away_team": row.get("away_team", ""),
                "home_team": row.get("home_team", ""),
                "publication": meta.get("publication") or "",
                "result_date": meta.get("publicationDate") or "",
                "page": page_from_pdf(pdf, meta),
                "candidate_status": "extracted_pdf_available",
                "document_status": "ocr_sidecar_ready" if text_exists and json_exists else "needs_full_page_ocr",
                "next_action": "queue_article_atom_conveyor" if text_exists and json_exists else "queue_full_page_ocr",
                "ocr_sidecar_dir": str(sidecar_dir),
                "suggested_ocr_text_path": str(text_path),
                "suggested_ocr_json_path": str(json_path),
                "asset_signature": asset_signature(pdf),
                "pdf_path": str(pdf),
                "pdf_exists": pdf.exists(),
                "pdf_bytes": pdf.stat().st_size if pdf.exists() else 0,
                "pdf_pages": "",
                "pdf_valid": "",
                "pdf_text_chars": "",
                "pdf_viewer_only": "",
                "text_source_type": "ocr_sidecar" if text_exists else "none",
                "extractable_text_chars": ocr_chars,
                "stat_keyword_hits": "",
                "stat_keyword_terms": "",
                "atom_claim_count": row.get("atom_count", 0),
                "sidecar_text_files": str(text_path) if text_exists else "",
                "result_root": str(item["result_root"]),
                "candidate_checkpoint": "",
                "elapsed_ms": "",
                "notes": "planned_from_pdf_to_atom_queue",
                "article_score": "",
                "strong_hits": "",
                "weak_hits": "",
                "noise_hits": "",
                "ocr_chars": ocr_chars,
                "article_decision": "article_full_batch_attempt" if text_exists and json_exists else "article_waiting_for_full_page_ocr",
                "article_next_action": "queue_article_atom_conveyor" if text_exists and json_exists else "queue_full_page_ocr",
                "source_url": meta.get("canonicalUrl") or "",
            }
        )
    return docs


def chunked(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extraction-queue", type=Path, default=None)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--ocr-root", type=Path, default=DEFAULT_OCR_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_article_batch_plan")
    parser.add_argument("--action", action="append", default=["queue_ocr_article_semantic"])
    parser.add_argument("--year-min", type=int, default=1920)
    parser.add_argument("--year-max", type=int, default=1939)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--include-article-without-ocr", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    extraction_queue = args.extraction_queue or latest_extraction_queue(EXTRACTION_QUEUE_ROOT)
    queue_rows = []
    for row in read_csv(extraction_queue):
        year_raw = row.get("year") or ""
        if not year_raw.isdigit():
            continue
        year = int(year_raw)
        if args.year_min <= year <= args.year_max and row.get("next_action") in set(args.action):
            queue_rows.append(row)

    docs: list[dict[str, Any]] = []
    missing_pdf_games = 0
    for row in queue_rows:
        game_docs = collect_game_documents(row, args.raw_root, args.ocr_root)
        if not game_docs:
            missing_pdf_games += 1
            continue
        docs.extend(game_docs)

    docs.sort(
        key=lambda item: (
            int(item.get("year") or 0),
            item.get("game_date") or "",
            item.get("boxscore_id") or "",
            int(item.get("candidate_rank") or 0),
        )
    )
    if args.max_docs:
        docs = docs[: args.max_docs]

    run_dir = args.out_root / f"{stamp()}_{args.label}"
    run_dir.mkdir(parents=True, exist_ok=True)
    write_csv(run_dir / "document_inventory.csv", docs, MANIFEST_FIELDS + ["source_url"])

    batches = chunked(docs, args.batch_size)
    batch_rows: list[dict[str, Any]] = []
    command_lines: list[str] = []
    for batch_index, batch_docs in enumerate(batches, start=1):
        batch_id = f"batch_{batch_index:04d}"
        batch_dir = run_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        ocr_docs = [doc for doc in batch_docs if doc["next_action"] == "queue_full_page_ocr"]
        article_docs = [
            doc
            for doc in batch_docs
            if doc["next_action"] == "queue_article_atom_conveyor"
            or (args.include_article_without_ocr and doc["next_action"] == "queue_full_page_ocr")
        ]
        for doc in batch_docs:
            doc["round_name"] = f"{args.label}_{batch_id}"

        ocr_manifest = batch_dir / "ocr_round_manifest.csv"
        article_manifest = batch_dir / "article_candidate_manifest.csv"
        write_csv(ocr_manifest, ocr_docs, MANIFEST_FIELDS + ["source_url"])
        write_csv(article_manifest, article_docs, MANIFEST_FIELDS + ["source_url"])

        games = sorted({doc["boxscore_id"] for doc in batch_docs})
        batch_rows.append(
            {
                "batch_id": batch_id,
                "documents": len(batch_docs),
                "games": len(games),
                "ocr_documents": len(ocr_docs),
                "article_documents": len(article_docs),
                "first_boxscore_id": games[0] if games else "",
                "last_boxscore_id": games[-1] if games else "",
                "ocr_manifest": str(ocr_manifest),
                "article_manifest": str(article_manifest),
                "ocr_command_label": f"{args.label}_{batch_id}_ocr",
                "article_command_label": f"{args.label}_{batch_id}_article",
            }
        )
        if ocr_docs:
            command_lines.extend(
                [
                    f"# {batch_id}: OCR {len(ocr_docs)} documents",
                    f"py -3.10 {OCR_CONVEYOR} --manifest {ocr_manifest} --label {args.label}_{batch_id}_ocr --dpi 150 --psm 6 --timeout-seconds 90",
                    "",
                ]
            )
        if article_docs:
            command_lines.extend(
                [
                    f"# {batch_id}: article atoms {len(article_docs)} documents",
                    f"py -3.10 {ARTICLE_CONVEYOR} --manifest {article_manifest} --label {args.label}_{batch_id}_article --dpi 200 --psm 4 --max-anchors 1 --max-proposals 2 --upscale 1.2 --contrast 1.7 --timeout-seconds 45",
                    "",
                ]
            )

    write_csv(run_dir / "batch_index.csv", batch_rows, [
        "batch_id",
        "documents",
        "games",
        "ocr_documents",
        "article_documents",
        "first_boxscore_id",
        "last_boxscore_id",
        "ocr_manifest",
        "article_manifest",
        "ocr_command_label",
        "article_command_label",
    ])
    (run_dir / "run_article_batches.ps1").write_text("\n".join(command_lines), encoding="utf-8")

    next_actions = Counter(doc["next_action"] for doc in docs)
    years = Counter(str(doc["year"]) for doc in docs)
    summary = {
        "created_at_utc": iso_now(),
        "label": args.label,
        "run_dir": str(run_dir),
        "extraction_queue": str(extraction_queue),
        "raw_root": str(args.raw_root),
        "ocr_root": str(args.ocr_root),
        "selected_game_rows": len(queue_rows),
        "missing_pdf_games_after_scan": missing_pdf_games,
        "documents": len(docs),
        "batches": len(batches),
        "batch_size": args.batch_size,
        "next_action_counts": dict(sorted(next_actions.items())),
        "documents_by_year": dict(sorted(years.items())),
        "live_tables_touched": False,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (run_dir / "README.md").write_text(
        "\n".join(
            [
                "# Newspaper Article Batch Plan",
                "",
                f"Created: `{summary['created_at_utc']}`",
                "",
                f"Extraction queue: `{extraction_queue}`",
                f"Documents planned: `{summary['documents']}`",
                f"Batches: `{summary['batches']}`",
                "",
                "## Files",
                "",
                "- `document_inventory.csv`: every planned PDF document with OCR/article sidecar paths.",
                "- `batch_index.csv`: bounded batch handoff inventory.",
                "- `batch_*/ocr_round_manifest.csv`: run these first for full-page OCR.",
                "- `batch_*/article_candidate_manifest.csv`: run after OCR sidecars exist.",
                "- `run_article_batches.ps1`: generated command ledger; review/run batch by batch.",
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
