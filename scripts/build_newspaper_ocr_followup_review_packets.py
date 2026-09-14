#!/usr/bin/env python
"""Build second-pass LLM review packets from completed OCR follow-up sidecars.

The first LLM pass can decide that a pull needs a better OCR/visual pass. Once
those sidecars exist, this station puts the new text back onto the same review
contract so extraction keeps moving through the normal ingest/materialize/
promotion conveyor instead of becoming a pile of orphan OCR files.
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

try:
    from build_newspaper_llm_review_packets import (
        clean,
        enrich_atom,
        load_db_context,
        packet_markdown,
        packetize,
        review_contract,
        truncate_text,
        write_csv,
        write_json,
        write_jsonl,
    )
except ImportError:  # D-drive tool mirror uses the shorter newspaper_*.py name.
    from newspaper_llm_review_packets import (  # type: ignore
        clean,
        enrich_atom,
        load_db_context,
        packet_markdown,
        packetize,
        review_contract,
        truncate_text,
        write_csv,
        write_json,
        write_jsonl,
    )


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_MANIFEST_ROOT = DEFAULT_ROOT / "ocr_followup_round_manifests"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "llm_review_packets"
DEFAULT_REVIEW_OUTPUT_ROOT = DEFAULT_ROOT / "llm_review_outputs"

PACKET_FIELDS = [
    "packet_id",
    "packet_json_path",
    "packet_jsonl_path",
    "packet_md_path",
    "document_count",
    "document_actions_json",
]

DOCUMENT_FIELDS = [
    "packet_id",
    "source_document_id",
    "document_next_action",
    "boxscore_id",
    "publication",
    "issue_date",
    "page",
    "region_count",
    "atom_claim_count",
    "promotion_candidate_count",
    "ocr_text_chars",
    "recommended_next_pass",
    "ocr_text_path",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_id(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9_.:-]+", "_", clean(value).strip())
    return text.strip("._") or "unknown"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def latest_manifest(root: Path) -> Path:
    dirs = sorted(path for path in root.iterdir() if path.is_dir()) if root.exists() else []
    for directory in reversed(dirs):
        manifest = directory / "ocr_round_manifest.csv"
        if manifest.exists():
            return manifest
    raise FileNotFoundError(f"No ocr_round_manifest.csv found under {root}")


def parse_json_list(raw: str) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return [raw]
    if isinstance(value, list):
        return [clean(item) for item in value if clean(item)]
    if value:
        return [clean(value)]
    return []


def completed_ocr_rows(
    manifest_rows: list[dict[str, str]],
    min_ocr_chars: int,
    source_document_ids: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in manifest_rows:
        source_document_id = clean(row.get("source_document_id") or row.get("candidate_id"))
        if not source_document_id or source_document_id in seen:
            continue
        if source_document_ids and source_document_id not in source_document_ids:
            continue
        text_path = Path(clean(row.get("suggested_ocr_text_path")))
        json_path = Path(clean(row.get("suggested_ocr_json_path")))
        if not text_path.exists():
            continue
        text = text_path.read_text(encoding="utf-8", errors="replace")
        if len(text) < min_ocr_chars:
            continue
        rows.append({
            **row,
            "source_document_id": source_document_id,
            "ocr_text": text,
            "ocr_text_chars": len(text),
            "ocr_text_path": str(text_path),
            "ocr_json_path": str(json_path),
            "ocr_json_exists": json_path.exists(),
        })
        seen.add(source_document_id)
        if limit > 0 and len(rows) >= limit:
            break
    return rows


def prior_regions(db_doc: dict[str, Any], max_region_chars: int, include_prior_regions: bool) -> list[dict[str, Any]]:
    if not include_prior_regions:
        return []
    regions: list[dict[str, Any]] = []
    for region in db_doc.get("source_regions", []):
        text_path = Path(clean(region.get("region_text_path")))
        text = ""
        total_chars = 0
        status = "missing_path"
        if text_path.exists():
            text = text_path.read_text(encoding="utf-8", errors="replace")
            total_chars = len(text)
            status = "loaded"
        regions.append({
            "region_id": region.get("region_id"),
            "run_id": region.get("run_id"),
            "page_number": region.get("page_number"),
            "region_type": region.get("region_type"),
            "region_label": region.get("region_label"),
            "anchor_text": region.get("anchor_text"),
            "crop_image_path": region.get("crop_image_path"),
            "region_text_path": region.get("region_text_path"),
            "region_text_chars": total_chars,
            "region_text_status": status,
            "region_quality_score": region.get("region_quality_score"),
            "status": region.get("status"),
            "next_action": region.get("next_action"),
            "reason_code": region.get("reason_code"),
            "text_excerpt": truncate_text(text, max_region_chars),
        })
    return regions


def build_doc_packet(
    manifest_row: dict[str, Any],
    db_doc: dict[str, Any],
    max_ocr_chars: int,
    max_region_chars: int,
    max_evidence_chars: int,
    include_prior_regions: bool,
) -> dict[str, Any]:
    source_document_id = clean(manifest_row.get("source_document_id"))
    source_document = db_doc.get("source_document") or {}
    next_pass = clean(manifest_row.get("recommended_next_pass"))
    ocr_region_id = f"ocr_followup:{safe_id(source_document_id)}:{safe_id(next_pass)}"
    prior_text_paths = parse_json_list(clean(manifest_row.get("prior_region_text_paths_json")))
    prior_crop_paths = parse_json_list(clean(manifest_row.get("prior_crop_image_paths_json")))
    ocr_region = {
        "region_id": ocr_region_id,
        "run_id": clean(manifest_row.get("ocr_followup_prep_run_id")),
        "page_number": clean(manifest_row.get("page")),
        "region_type": "ocr_followup_sidecar",
        "region_label": next_pass,
        "anchor_text": "Completed OCR follow-up sidecar",
        "crop_image_path": "; ".join(prior_crop_paths),
        "region_text_path": clean(manifest_row.get("ocr_text_path")),
        "region_text_chars": manifest_row.get("ocr_text_chars"),
        "region_text_status": "loaded",
        "region_quality_score": "",
        "status": "ocr_followup_complete",
        "next_action": "extract_atoms_from_ocr_followup",
        "reason_code": clean(manifest_row.get("reason")),
        "text_excerpt": truncate_text(clean(manifest_row.get("ocr_text")), max_ocr_chars),
    }
    regions = [ocr_region] + prior_regions(db_doc, max_region_chars, include_prior_regions)
    existing_atoms = [enrich_atom(atom, max_evidence_chars) for atom in db_doc.get("atom_claims", [])]
    return {
        "source_document_id": source_document_id,
        "document_next_action": "review_ocr_followup_atoms",
        "document_reason_code": clean(manifest_row.get("reason")),
        "document_work_lane": "ocr_followup_review",
        "queue_context": {
            "source_queue_csv": clean(manifest_row.get("_source_manifest")),
            "ocr_followup_prep_run_id": clean(manifest_row.get("ocr_followup_prep_run_id")),
            "action_queue_run_id": clean(manifest_row.get("action_queue_run_id")),
            "recommended_next_pass": next_pass,
            "followup_types_json": clean(manifest_row.get("followup_types_json")),
            "prior_region_text_paths": prior_text_paths,
            "prior_crop_image_paths": prior_crop_paths,
            "ocr_text_path": clean(manifest_row.get("ocr_text_path")),
            "ocr_json_path": clean(manifest_row.get("ocr_json_path")),
            "ocr_json_exists": bool(manifest_row.get("ocr_json_exists")),
            "ocr_text_chars": manifest_row.get("ocr_text_chars"),
        },
        "source_document": source_document,
        "candidate_metadata": {
            "boxscore_id": clean(manifest_row.get("boxscore_id") or source_document.get("boxscore_id")),
            "game_date": "",
            "year": "",
            "week": "",
            "team_1": "",
            "team_2": "",
            "publication": clean(manifest_row.get("publication") or source_document.get("publication")),
            "issue_date": clean(manifest_row.get("result_date") or source_document.get("issue_date")),
            "page": clean(manifest_row.get("page") or source_document.get("page")),
            "source_url": clean(source_document.get("source_url")),
            "asset_pdf_path": clean(manifest_row.get("pdf_path") or source_document.get("asset_pdf_path")),
        },
        "source_regions": regions,
        "existing_atom_claims": existing_atoms,
        "domain_rows": {
            "promotion_candidates": db_doc.get("promotion_candidates", []),
            "scoring_events": db_doc.get("scoring_events", []),
            "play_by_play_events": db_doc.get("play_by_play_events", []),
            "player_game_box_scores": db_doc.get("player_game_box_scores", []),
            "lineup_participation": db_doc.get("lineup_participation", []),
        },
    }


def packet_prompt(out_dir: Path, review_output_dir: Path) -> str:
    return "\n".join([
        "# ChatGPT Newspaper OCR Follow-up Review Prompt",
        "",
        "Read one packet at a time from this second-pass OCR follow-up packet run and return JSON only.",
        "",
        f"Packet run: `{out_dir}`",
        f"Review-output folder: `{review_output_dir}`",
        f"Output template: `{out_dir / 'empty_review_output_template.json'}`",
        f"Packet manifest: `{out_dir / 'llm_review_packet_manifest.csv'}`",
        "",
        "For each document:",
        "- Treat `ocr_followup_sidecar` as the primary new evidence.",
        "- Extract all football atoms directly supported by that OCR text: scoring events, notable play-by-play, lineups, player stat rows, identities, and game facts.",
        "- Keep duplicate/corroborating facts when this OCR text independently supports them; do not discard a fact just because an existing atom already appears.",
        "- Use high confidence only for explicit evidence with clear table/field mapping.",
        "- Use medium confidence when OCR, identity, team mapping, or stat category needs another lane.",
        "- Use `evidence_region_id` from the packet region and keep evidence quotes short.",
        "- Never invent missing names, teams, values, dates, or stat categories.",
        "",
        "Return a JSON object with `packet_id`, `reviewer`, `reviewed_at_utc`, and `document_reviews`.",
        "",
    ])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--manifest-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_ocr_followup_review_packets")
    parser.add_argument("--source-document-id", action="append", default=[])
    parser.add_argument("--max-docs", type=int, default=0)
    parser.add_argument("--packet-docs", type=int, default=2)
    parser.add_argument("--min-ocr-chars", type=int, default=1)
    parser.add_argument("--max-ocr-chars", type=int, default=50000)
    parser.add_argument("--max-region-chars", type=int, default=1500)
    parser.add_argument("--max-evidence-chars", type=int, default=900)
    parser.add_argument("--no-prior-regions", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = args.manifest or latest_manifest(args.manifest_root)
    manifest_rows = read_csv(manifest)
    for row in manifest_rows:
        row["_source_manifest"] = str(manifest)
    selected_rows = completed_ocr_rows(
        manifest_rows,
        args.min_ocr_chars,
        set(args.source_document_id),
        args.max_docs,
    )

    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    source_ids = [clean(row.get("source_document_id")) for row in selected_rows]
    db_context = load_db_context(args.db_path, source_ids) if source_ids else {}
    contract = review_contract()
    contract["station"] = "llm_ocr_followup_review"
    contract["goal"] = (
        "Extract every newspaper-supported football atom from completed OCR follow-up sidecars "
        "without promoting to live tables."
    )
    contract["rules"] = [
        "Use the OCR follow-up sidecar as primary evidence for this pass.",
        "Emit duplicates/corroboration when the sidecar independently supports a fact.",
    ] + contract["rules"]

    doc_packets = [
        build_doc_packet(
            row,
            db_context.get(clean(row.get("source_document_id")), {}),
            max_ocr_chars=args.max_ocr_chars,
            max_region_chars=args.max_region_chars,
            max_evidence_chars=args.max_evidence_chars,
            include_prior_regions=not args.no_prior_regions,
        )
        for row in selected_rows
    ]
    packets = packetize(doc_packets, max(1, args.packet_docs))
    packet_rows: list[dict[str, Any]] = []
    document_rows: list[dict[str, Any]] = []
    for index, docs in enumerate(packets, start=1):
        packet_id = f"packet_{index:04d}"
        packet_payload = {
            "packet_id": packet_id,
            "created_at_utc": iso_now(),
            "review_contract": contract,
            "documents": docs,
        }
        json_path = out_dir / "packets_json" / f"{packet_id}.json"
        jsonl_path = out_dir / "packets_jsonl" / f"{packet_id}.jsonl"
        md_path = out_dir / "packets_md" / f"{packet_id}.md"
        write_json(json_path, packet_payload)
        write_jsonl(jsonl_path, docs)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(packet_markdown(packet_id, docs, contract), encoding="utf-8")
        action_counts = Counter(doc.get("document_next_action") for doc in docs)
        packet_rows.append({
            "packet_id": packet_id,
            "packet_json_path": str(json_path),
            "packet_jsonl_path": str(jsonl_path),
            "packet_md_path": str(md_path),
            "document_count": len(docs),
            "document_actions_json": json.dumps(dict(action_counts), sort_keys=True),
        })
        for doc in docs:
            meta = doc["candidate_metadata"]
            qc = doc["queue_context"]
            document_rows.append({
                "packet_id": packet_id,
                "source_document_id": doc["source_document_id"],
                "document_next_action": doc.get("document_next_action"),
                "boxscore_id": meta.get("boxscore_id"),
                "publication": meta.get("publication"),
                "issue_date": meta.get("issue_date"),
                "page": meta.get("page"),
                "region_count": len(doc.get("source_regions", [])),
                "atom_claim_count": len(doc.get("existing_atom_claims", [])),
                "promotion_candidate_count": len(doc.get("domain_rows", {}).get("promotion_candidates", [])),
                "ocr_text_chars": qc.get("ocr_text_chars"),
                "recommended_next_pass": qc.get("recommended_next_pass"),
                "ocr_text_path": qc.get("ocr_text_path"),
            })

    write_csv(out_dir / "llm_review_packet_manifest.csv", packet_rows, PACKET_FIELDS)
    write_csv(out_dir / "document_packet_manifest.csv", document_rows, DOCUMENT_FIELDS)
    write_json(out_dir / "empty_review_output_template.json", {
        "packet_id": "",
        "reviewer": "llm",
        "reviewed_at_utc": iso_now(),
        "document_reviews": [contract["expected_output_shape"]],
    })

    review_output_dir = DEFAULT_REVIEW_OUTPUT_ROOT / run_id
    review_output_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "chatgpt_packet_review_prompt.md").write_text(packet_prompt(out_dir, review_output_dir), encoding="utf-8")
    summary = {
        "created_at_utc": iso_now(),
        "run_id": run_id,
        "label": args.label,
        "db_path": str(args.db_path),
        "manifest": str(manifest),
        "documents_selected": len(selected_rows),
        "packets_created": len(packet_rows),
        "packet_docs": args.packet_docs,
        "min_ocr_chars": args.min_ocr_chars,
        "max_ocr_chars": args.max_ocr_chars,
        "max_region_chars": args.max_region_chars,
        "include_prior_regions": not args.no_prior_regions,
        "recommended_next_pass_counts": dict(Counter(row.get("recommended_next_pass") for row in selected_rows)),
        "ocr_text_chars_total": sum(int(row.get("ocr_text_chars") or 0) for row in selected_rows),
        "output_dir": str(out_dir),
        "review_output_dir": str(review_output_dir),
    }
    write_json(out_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
