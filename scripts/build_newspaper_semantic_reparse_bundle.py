#!/usr/bin/env python
"""Build a filtered article-run bundle for semantic reparse.

`newspapers_article_semantic_reparse.py` operates on one article-run directory.
Batch handoff manifests can reference documents spread across several article
runs/chunks. This station gathers exactly the source documents named by a
manifest into a small article-run-shaped folder:

- article_regions.csv
- conveyor_document_state.csv
- summary.json

The output is local D-drive control-plane data only; it does not write to Fly,
v26, or production supertable tables.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_ROUNDS_ROOT = DEFAULT_ROOT / "article_atom_rounds"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "semantic_reparse_bundles"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    return "" if value is None else str(value)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True, default=str) + "\n", encoding="utf-8")


def csv_fields(rows: list[dict[str, Any]], fallback: list[str] | None = None) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for field in row:
            if field not in seen:
                seen.add(field)
                fields.append(field)
    return fields or list(fallback or [])


def source_document_id(row: dict[str, str]) -> str:
    return clean(row.get("source_document_id") or row.get("candidate_id")).strip()


def load_manifest_docs(manifest: Path) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for row in read_csv(manifest):
        doc_id = source_document_id(row)
        if doc_id:
            docs[doc_id] = row
    return docs


def indexed_rows_by_doc(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        doc_id = source_document_id(row)
        if doc_id:
            out[doc_id].append(row)
    return dict(out)


def run_dir_for_doc(rounds_root: Path, doc_id: str, manifest_row: dict[str, str]) -> Path | None:
    latest_run_id = clean(manifest_row.get("latest_run_id")).strip()
    if latest_run_id:
        candidate = rounds_root / latest_run_id
        if (candidate / "article_regions.csv").exists():
            return candidate
    matches = []
    for path in rounds_root.glob("*/conveyor_document_state.csv"):
        try:
            for row in read_csv(path):
                if source_document_id(row) == doc_id:
                    matches.append(path.parent)
                    break
        except OSError:
            continue
    if not matches:
        return None
    return sorted(matches, key=lambda p: p.name)[-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--rounds-root", type=Path, default=DEFAULT_ROUNDS_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_semantic_reparse_bundle")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_docs = load_manifest_docs(args.manifest)
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    run_dirs_by_doc: dict[str, Path] = {}
    missing_docs: list[dict[str, Any]] = []
    for doc_id, manifest_row in manifest_docs.items():
        run_dir = run_dir_for_doc(args.rounds_root, doc_id, manifest_row)
        if run_dir is None:
            missing_docs.append({
                "source_document_id": doc_id,
                "latest_run_id": manifest_row.get("latest_run_id", ""),
                "reason": "article_run_dir_not_found",
            })
            continue
        run_dirs_by_doc[doc_id] = run_dir

    region_rows: list[dict[str, Any]] = []
    state_rows: list[dict[str, Any]] = []
    source_run_counts = Counter(str(path) for path in run_dirs_by_doc.values())
    loaded_run_dirs = sorted(set(run_dirs_by_doc.values()), key=lambda p: p.name)
    docs_by_run = defaultdict(set)
    for doc_id, run_dir in run_dirs_by_doc.items():
        docs_by_run[run_dir].add(doc_id)

    for run_dir in loaded_run_dirs:
        wanted = docs_by_run[run_dir]
        region_index = indexed_rows_by_doc(read_csv(run_dir / "article_regions.csv"))
        state_index = indexed_rows_by_doc(read_csv(run_dir / "conveyor_document_state.csv"))
        for doc_id in sorted(wanted):
            regions = region_index.get(doc_id, [])
            states = state_index.get(doc_id, [])
            if not regions:
                missing_docs.append({
                    "source_document_id": doc_id,
                    "latest_run_id": run_dir.name,
                    "reason": "article_regions_missing_for_doc",
                })
            region_rows.extend(regions)
            state_rows.extend(states)

    region_fields = csv_fields(region_rows, [
        "run_id",
        "source_document_id",
        "candidate_id",
        "boxscore_id",
        "publication",
        "page",
        "region_id",
        "region_type",
        "anchor_text",
        "bbox_json",
        "crop_image_path",
        "region_text_path",
        "region_text_chars",
        "football_score",
        "lineup_score",
        "status",
        "reason_code",
        "elapsed_ms",
    ])
    state_fields = csv_fields(state_rows, [
        "source_document_id",
        "latest_run_id",
        "current_station",
        "current_status",
        "confidence_lane",
        "value_lane",
        "attempt_count_total",
        "attempt_count_current_station",
        "next_action",
        "reason_code",
        "last_advanced_at_utc",
        "terminal_status",
        "terminal_reason",
    ])

    write_csv(out_dir / "article_regions.csv", region_rows, region_fields)
    write_csv(out_dir / "conveyor_document_state.csv", state_rows, state_fields)
    write_csv(out_dir / "missing_documents.csv", missing_docs, ["source_document_id", "latest_run_id", "reason"])

    summary = {
        "created_at_utc": iso_now(),
        "run_id": run_id,
        "label": args.label,
        "manifest": str(args.manifest),
        "rounds_root": str(args.rounds_root),
        "run_dir": str(out_dir),
        "documents_requested": len(manifest_docs),
        "documents_with_article_run": len(run_dirs_by_doc),
        "documents_missing": len(missing_docs),
        "article_regions": len(region_rows),
        "state_rows": len(state_rows),
        "source_run_count": len(loaded_run_dirs),
        "source_run_counts": dict(source_run_counts),
        "live_tables_touched": False,
    }
    write_json(out_dir / "summary.json", summary)
    (out_dir / "README.md").write_text(
        "\n".join([
            "# Newspaper Semantic Reparse Bundle",
            "",
            f"Created: `{summary['created_at_utc']}`",
            f"Manifest: `{args.manifest}`",
            "",
            "Use this folder as `--article-run-dir` for `newspapers_article_semantic_reparse.py`.",
            "",
            "Live Fly/DuckDB/supertable writes: no.",
            "",
        ]),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=True, sort_keys=True))
    return 0 if not missing_docs else 1


if __name__ == "__main__":
    raise SystemExit(main())
