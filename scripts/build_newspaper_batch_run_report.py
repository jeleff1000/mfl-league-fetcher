#!/usr/bin/env python
"""Build a compact audit report for a newspaper conveyor batch slice."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ocr-summary", type=Path, action="append", required=True)
    parser.add_argument("--article-summary", type=Path, required=True)
    parser.add_argument("--queue-summary", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def db_counts(db_path: Path) -> dict[str, int]:
    tables = [
        "atom_claim",
        "game_candidate",
        "scoring_event",
        "play_by_play_event",
        "player_game_box_score",
        "lineup_participation",
        "promotion_candidate",
        "conveyor_document_state",
    ]
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return {
            table: con.execute(f"SELECT COUNT(*) FROM newspaper_review.{table}").fetchone()[0]
            for table in tables
        }
    finally:
        con.close()


def first_value(payload: dict, *keys: str, default=""):
    for key in keys:
        if key in payload:
            return payload[key]
    return default


def truthy(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def aggregate_ocr_summaries(paths: list[Path]) -> dict:
    payloads = [read_json(path) for path in paths]
    if len(payloads) == 1:
        return payloads[0]

    row_status_by_candidate: dict[str, dict[str, str]] = {}
    for payload in payloads:
        row_status_path = Path(str(payload.get("row_status_path") or ""))
        for row in read_csv_rows(row_status_path):
            candidate_id = str(row.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            current = row_status_by_candidate.get(candidate_id)
            if current is None or truthy(row.get("sidecar_done")) or not truthy(current.get("sidecar_done")):
                row_status_by_candidate[candidate_id] = row

    done_rows = [
        row for row in row_status_by_candidate.values()
        if truthy(row.get("sidecar_done"))
    ]
    pending_rows = [
        row for row in row_status_by_candidate.values()
        if not truthy(row.get("sidecar_done"))
    ]
    chunk_counts = Counter()
    for payload in payloads:
        for key, value in dict(payload.get("chunk_exit_counts") or {}).items():
            chunk_counts[str(key)] += int(value or 0)

    total_chars = 0
    for row in done_rows:
        try:
            total_chars += int(float(row.get("ocr_text_chars") or 0))
        except ValueError:
            pass

    return {
        "output_dir": "; ".join(str(first_value(payload, "run_dir", "output_dir")) for payload in payloads),
        "manifest_rows": len(row_status_by_candidate) or max(int(payload.get("manifest_rows") or 0) for payload in payloads),
        "final_done_rows": len(done_rows),
        "final_pending_rows": len(pending_rows),
        "chunk_exit_counts": dict(chunk_counts),
        "total_followup_ocr_text_chars": total_chars or sum(
            int(payload.get("total_followup_ocr_text_chars") or payload.get("total_ocr_text_chars") or 0)
            for payload in payloads
        ),
        "docs_with_keyword_hits": "",
        "status": "complete" if not pending_rows else "partial",
    }


def main() -> int:
    args = parse_args()
    ocr = aggregate_ocr_summaries(args.ocr_summary)
    article = read_json(args.article_summary)
    queue = read_json(args.queue_summary)
    counts = db_counts(args.db_path)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Newspaper Conveyor Batch Slice Report",
        "",
        f"OCR run: `{first_value(ocr, 'run_dir', 'output_dir')}`",
        f"Article run: `{first_value(article, 'run_dir', 'output_dir')}`",
        f"Queue refresh: `{first_value(queue, 'out_dir', 'output_dir')}`",
        "",
        "## OCR",
        "",
        f"- Documents selected: `{first_value(ocr, 'documents_selected', 'manifest_rows')}`",
        f"- Documents done: `{first_value(ocr, 'final_done_rows', default='')}`",
        f"- Pending rows: `{first_value(ocr, 'final_pending_rows', default='')}`",
        f"- Status counts: `{json.dumps(first_value(ocr, 'status_counts', 'chunk_exit_counts', default={}), sort_keys=True)}`",
        f"- Total OCR text chars: `{first_value(ocr, 'total_ocr_text_chars', 'total_followup_ocr_text_chars')}`",
        f"- Docs with keyword hits: `{first_value(ocr, 'docs_with_keyword_hits', default='')}`",
        "",
        "## Article Atoms",
        "",
        f"- Documents: `{first_value(article, 'documents', 'final_done_rows')}`",
        f"- Regions: `{first_value(article, 'regions', default='')}`",
        f"- Atoms: `{first_value(article, 'atoms', default='')}`",
        f"- Status counts: `{json.dumps(first_value(article, 'status_counts', 'chunk_exit_counts', default={}), sort_keys=True)}`",
        f"- Atom counts: `{json.dumps(first_value(article, 'atom_counts', 'domain_counts', default={}), sort_keys=True)}`",
        f"- Open extraction states: `{first_value(article, 'open_extraction_state_count', default='')}`",
        "",
        "## Queue After Run",
        "",
        f"- Filtered documents: `{first_value(queue, 'filtered_documents', default='')}`",
        f"- Filtered actionable documents: `{first_value(queue, 'filtered_actionable_documents', default='')}`",
        f"- Games with atoms: `{first_value(queue, 'games_with_atoms', default='')}`",
        f"- Games with promotion candidates: `{first_value(queue, 'games_with_promotion_candidates', default='')}`",
        f"- Next actions: `{json.dumps(first_value(queue, 'filtered_next_action_counts', 'next_action_counts', default={}), sort_keys=True)}`",
        f"- Atom rollup status: `{first_value(queue, 'atom_rollup_status', 'rollup_status', default='')}`",
        "",
        "## Local Review Table Counts",
        "",
    ]
    for table, count in counts.items():
        lines.append(f"- `{table}`: `{count}`")
    lines.extend(
        [
            "",
            "Live Fly/DuckDB/supertable writes: no.",
            "",
            "Interpretation: this was a bounded proof that extracted PDFs can move through full-page OCR,",
            "article crop OCR, semantic atom extraction, local review-table staging, and queue advancement.",
            "The noisy player strings in low-confidence notable-play atoms should remain in review/promotion",
            "lanes until identity and semantic confidence are high enough to promote.",
            "",
        ]
    )
    args.out.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
