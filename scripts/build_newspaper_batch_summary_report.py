#!/usr/bin/env python
"""Build a consolidated newspaper conveyor batch report."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def add_counts(total: Counter, counts: dict) -> None:
    for key, value in counts.items():
        total[str(key)] += int(value)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ocr-summary", type=Path, action="append", default=[])
    parser.add_argument("--article-summary", type=Path, action="append", default=[])
    parser.add_argument("--document-queue-summary", type=Path, action="append", default=[])
    parser.add_argument("--game-queue-summary", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--title", default="Newspaper Conveyor Batch Report")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ocr_summaries = [read_json(path) for path in args.ocr_summary]
    article_summaries = [read_json(path) for path in args.article_summary]
    document_queue_summaries = [read_json(path) for path in args.document_queue_summary]
    game_queue = read_json(args.game_queue_summary)

    ocr_status = Counter()
    ocr_docs = 0
    ocr_chars = 0
    ocr_keyword_docs = 0
    for summary in ocr_summaries:
        ocr_docs += int(summary.get("documents_selected") or 0)
        ocr_chars += int(summary.get("total_ocr_text_chars") or 0)
        ocr_keyword_docs += int(summary.get("docs_with_keyword_hits") or 0)
        add_counts(ocr_status, summary.get("status_counts") or {})

    article_status = Counter()
    atom_counts = Counter()
    article_docs = 0
    article_regions = 0
    article_atoms = 0
    open_states = 0
    for summary in article_summaries:
        article_docs += int(summary.get("documents") or 0)
        article_regions += int(summary.get("regions") or 0)
        article_atoms += int(summary.get("atoms") or 0)
        open_states += int(summary.get("open_extraction_state_count") or 0)
        add_counts(article_status, summary.get("status_counts") or {})
        add_counts(atom_counts, summary.get("atom_counts") or {})

    doc_queue_counts = []
    for summary in document_queue_summaries:
        doc_queue_counts.append(
            {
                "label": summary.get("label", ""),
                "documents": summary.get("documents", 0),
                "next_action_counts": summary.get("next_action_counts", {}),
            }
        )

    counts = db_counts(args.db_path)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# {args.title}",
        "",
        "## OCR",
        "",
        f"- OCR runs: `{len(ocr_summaries)}`",
        f"- Documents selected: `{ocr_docs}`",
        f"- Status counts: `{json.dumps(dict(sorted(ocr_status.items())), sort_keys=True)}`",
        f"- Total OCR chars represented: `{ocr_chars}`",
        f"- Docs with OCR keyword hits: `{ocr_keyword_docs}`",
        "",
        "## Article Extraction",
        "",
        f"- Article runs: `{len(article_summaries)}`",
        f"- Documents processed: `{article_docs}`",
        f"- Regions OCRed: `{article_regions}`",
        f"- Atom claims emitted: `{article_atoms}`",
        f"- Status counts: `{json.dumps(dict(sorted(article_status.items())), sort_keys=True)}`",
        f"- Atom counts: `{json.dumps(dict(sorted(atom_counts.items())), sort_keys=True)}`",
        f"- Open extraction states: `{open_states}`",
        "",
        "## Document Queues",
        "",
    ]
    for item in doc_queue_counts:
        lines.append(
            f"- `{item['label']}`: `{item['documents']}` docs, actions "
            f"`{json.dumps(item['next_action_counts'], sort_keys=True)}`"
        )
    lines.extend(
        [
            "",
            "## Game Queue",
            "",
            f"- Queue: `{game_queue['out_dir']}`",
            f"- Games with PDFs: `{game_queue['games_with_pdf']}`",
            f"- Games without PDFs: `{game_queue['games_without_pdf']}`",
            f"- Games with atoms: `{game_queue['games_with_atoms']}`",
            f"- Games with promotion candidates: `{game_queue['games_with_promotion_candidates']}`",
            f"- Next actions: `{json.dumps(game_queue['next_action_counts'], sort_keys=True)}`",
            "",
            "## Local Review Table Counts",
            "",
        ]
    )
    for table, count in counts.items():
        lines.append(f"- `{table}`: `{count}`")
    lines.extend(
        [
            "",
            "Live Fly/DuckDB/supertable writes: no.",
            "",
            "The batch is now off the OCR/article conveyor and into reparse/review/terminal lanes.",
            "Document-level queueing prevents one successful source from suppressing the rest of a game's PDFs.",
            "",
        ]
    )
    args.out.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
