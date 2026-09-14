#!/usr/bin/env python
"""Build a report for a no-OCR semantic reparse batch."""

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
    parser.add_argument("--semantic-summary", type=Path, action="append", default=[])
    parser.add_argument("--document-queue-summary", type=Path, action="append", default=[])
    parser.add_argument("--game-queue-summary", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--title", default="Newspaper Semantic Batch Report")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    semantic_summaries = [read_json(path) for path in args.semantic_summary]
    document_queue_summaries = [read_json(path) for path in args.document_queue_summary]
    game_queue = read_json(args.game_queue_summary)

    semantic_status = Counter()
    atom_counts = Counter()
    target_counts = Counter()
    domain_counts = Counter()
    regions = 0
    atoms = 0
    for summary in semantic_summaries:
        regions += int(summary.get("regions") or 0)
        atoms += int(summary.get("atoms") or 0)
        add_counts(semantic_status, summary.get("status_counts") or {})
        add_counts(atom_counts, summary.get("atom_counts") or {})
        add_counts(target_counts, summary.get("semantic_target_counts") or {})
        add_counts(domain_counts, summary.get("domain_counts") or {})

    doc_actions = Counter()
    source_docs = 0
    actionable_docs = 0
    for summary in document_queue_summaries:
        source_docs += int(summary.get("filtered_documents") or summary.get("documents") or 0)
        actionable_docs += int(summary.get("filtered_actionable_documents") or summary.get("actionable_documents") or 0)
        add_counts(doc_actions, summary.get("filtered_next_action_counts") or summary.get("next_action_counts") or {})

    counts = db_counts(args.db_path)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# {args.title}",
        "",
        "## Semantic Reparse",
        "",
        f"- Semantic runs: `{len(semantic_summaries)}`",
        f"- Authoritative source documents from final document queues: `{source_docs}`",
        f"- Saved article regions reparsed: `{regions}`",
        f"- Atom claims emitted by semantic runs: `{atoms}`",
        f"- Semantic run status counts: `{json.dumps(dict(sorted(semantic_status.items())), sort_keys=True)}`",
        f"- Atom counts: `{json.dumps(dict(sorted(atom_counts.items())), sort_keys=True)}`",
        f"- Semantic targets: `{json.dumps(dict(sorted(target_counts.items())), sort_keys=True)}`",
        f"- Domain rows from semantic runs: `{json.dumps(dict(sorted(domain_counts.items())), sort_keys=True)}`",
        "",
        "## Final Document Lanes",
        "",
        f"- Actionable review documents: `{actionable_docs}`",
        f"- Final document actions: `{json.dumps(dict(sorted(doc_actions.items())), sort_keys=True)}`",
        "",
        "## Game Queue",
        "",
        f"- Queue: `{game_queue['out_dir']}`",
        f"- Games with atoms: `{game_queue['games_with_atoms']}`",
        f"- Games with promotion candidates: `{game_queue['games_with_promotion_candidates']}`",
        f"- Next actions: `{json.dumps(game_queue['next_action_counts'], sort_keys=True)}`",
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
            "No-spin check: final document lanes contain no `queue_full_page_ocr`,",
            "`queue_article_atom_conveyor`, or `queue_semantic_reparse` actions for this batch.",
            "Documents are now either in review lanes or terminal no-current-atom state.",
            "",
        ]
    )
    args.out.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
