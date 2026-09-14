#!/usr/bin/env python
"""Build visual-review packets from the page coverage audit lane.

The page coverage audit flags documents where OCR exists but the conveyor still
sees football/stat markers without extracted atoms. This station turns that
lane into finite review packets: source PDF/image path, OCR excerpt, target atom
contract, and a CSV/JSON decision template.

It is local-only. It writes D-drive packet artifacts and optional local
`newspaper_review.page_visual_review_packet_*` receipt tables. It does not write
to Fly, v26, the production supertable, `newspaper_promoted`, or
`newspaper_final`.
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

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_AUDIT_ROOT = DEFAULT_ROOT / "page_coverage_audits"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "page_visual_review_packets"
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_REVIEW_OUTPUT_ROOT = DEFAULT_ROOT / "page_visual_review_outputs"

TARGET_TABLES = [
    "game_candidate",
    "lineup_participation",
    "scoring_event",
    "play_by_play_event",
    "player_game_box_score",
    "player_game_stat_claim",
    "team_game_stat_claim",
    "player_identity_candidate",
    "source_document_note",
]

RUN_FIELDS = [
    "page_visual_review_packet_run_id",
    "page_coverage_audit_run_id",
    "created_at_utc",
    "audit_dir",
    "audit_candidates_csv",
    "output_dir",
    "review_output_dir",
    "packet_count",
    "item_count",
    "max_items_per_packet",
    "render_images",
    "rendered_image_count",
    "render_error_count",
    "decision_template_csv",
    "packet_manifest_csv",
    "packet_items_csv",
    "summary_json",
    "report_md",
    "persisted_to_duckdb",
]

PACKET_FIELDS = [
    "page_visual_review_packet_run_id",
    "page_coverage_audit_run_id",
    "packet_id",
    "packet_index",
    "packet_md_path",
    "packet_json_path",
    "packet_jsonl_path",
    "packet_item_csv_path",
    "item_count",
    "priority_score_max",
    "reason_code_counts_json",
    "created_at_utc",
]

ITEM_FIELDS = [
    "page_visual_review_packet_run_id",
    "page_coverage_audit_run_id",
    "packet_id",
    "packet_index",
    "packet_item_index",
    "visual_review_item_id",
    "source_document_id",
    "boxscore_id",
    "year",
    "game_date",
    "away_team",
    "home_team",
    "publication",
    "result_date",
    "page",
    "candidate_rank",
    "image_id",
    "pdf_path",
    "rendered_image_path",
    "render_status",
    "render_error",
    "source_url",
    "ocr_text_path",
    "ocr_text_chars",
    "sidecar_quality_lane",
    "football_keyword_score",
    "keyword_terms",
    "keyword_excerpt",
    "coverage_status",
    "reason_code",
    "current_station",
    "current_status",
    "db_atom_count",
    "review_contract_json",
    "created_at_utc",
]

DECISION_FIELDS = [
    "visual_review_item_id",
    "source_document_id",
    "boxscore_id",
    "packet_id",
    "decision",
    "confidence_bar",
    "needs_followup_type",
    "atoms_json",
    "notes",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def as_int(value: Any, default: int = 0) -> int:
    try:
        text = clean(value)
        if not text:
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)
    return str(value)


def safe_slug(value: Any, max_len: int = 90) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", clean(value)).strip("._-")
    if not slug:
        slug = hashlib.sha1(clean(value).encode("utf-8", errors="ignore")).hexdigest()[:12]
    return slug[:max_len]


def stable_id(*parts: Any, length: int = 20) -> str:
    raw = "|".join(clean(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:length]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: safe_cell(row.get(field)) for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True, default=str), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True, default=str) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def latest_audit_dir(root: Path) -> Path:
    candidates = [
        path for path in sorted(root.iterdir()) if path.is_dir() and (path / "visual_read_packet_candidates.csv").exists()
    ] if root.exists() else []
    if not candidates:
        raise FileNotFoundError(f"No visual_read_packet_candidates.csv found under {root}")
    return candidates[-1]


def chunked(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def truncate_text(text: str, max_chars: int) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + f"\n\n[TRUNCATED at {max_chars} of {len(text)} chars]"


def read_ocr_excerpt(row: dict[str, str], max_chars: int) -> tuple[str, int, str]:
    path_text = clean(row.get("sidecar_text_path") or row.get("ocr_text_path"))
    if not path_text:
        return clean(row.get("keyword_excerpt")), 0, "missing_path"
    text_path = Path(path_text)
    if not text_path.exists():
        return clean(row.get("keyword_excerpt")), 0, "file_not_found"
    if not text_path.is_file():
        return clean(row.get("keyword_excerpt")), 0, "not_a_file"
    try:
        text = text_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return clean(row.get("keyword_excerpt")), 0, f"{type(exc).__name__}: {exc}"
    keyword_excerpt = clean(row.get("keyword_excerpt"))
    if keyword_excerpt:
        return keyword_excerpt, len(text), "keyword_excerpt"
    return truncate_text(text, max_chars), len(text), "loaded_head"


def review_contract(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "task": "Read the page image/PDF and OCR excerpt, then extract only NFL game facts that are visibly supported.",
        "allowed_decisions": [
            "extract_atoms",
            "no_useful_stat",
            "needs_better_ocr_or_crop",
            "wrong_game_or_non_nfl",
            "asset_problem",
        ],
        "target_tables": TARGET_TABLES,
        "atom_rules": [
            "Every atom must include source_document_id, target_table, evidence_text, and confidence_bar.",
            "Use source_document_note for useful context that is not structured enough for a stat/event/table row.",
            "Do not invent player identities; use raw names when the page is clear but identity is unresolved.",
            "Prefer small atoms over summaries: one scoring play, one lineup row, one box-score row, one team stat claim.",
            "If the image contradicts OCR, trust the image and note the OCR problem.",
        ],
        "confidence_bar": {
            "high": "directly legible in image/OCR and maps to an allowed target table",
            "medium": "legible but identity/team/stat mapping needs later resolution",
            "low": "interesting but too ambiguous for promotion",
        },
        "source_document_id": row.get("source_document_id", ""),
        "boxscore_id": row.get("boxscore_id", ""),
    }


def render_pdf_image(pdf_path: Path, image_path: Path, scale: float) -> tuple[str, str]:
    if not pdf_path.exists() or not pdf_path.is_file():
        return "missing_pdf", "pdf_path_not_found"
    try:
        import pypdfium2 as pdfium  # type: ignore
    except Exception as exc:
        return "render_unavailable", f"{type(exc).__name__}: {exc}"
    try:
        image_path.parent.mkdir(parents=True, exist_ok=True)
        pdf = pdfium.PdfDocument(str(pdf_path))
        try:
            if len(pdf) == 0:
                return "render_error", "pdf_has_no_pages"
            page = pdf[0]
            try:
                bitmap = page.render(scale=scale)
                pil_image = bitmap.to_pil()
                pil_image.save(image_path)
            finally:
                page.close()
        finally:
            pdf.close()
    except Exception as exc:
        return "render_error", f"{type(exc).__name__}: {exc}"
    return "rendered", ""


def build_items(
    rows: list[dict[str, str]],
    run_id: str,
    coverage_run_id: str,
    out_dir: Path,
    created_at: str,
    max_ocr_chars: int,
    render_images: bool,
    render_scale: float,
    max_render_docs: int,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    rendered_count = 0
    for row in rows:
        source_document_id = clean(row.get("source_document_id") or row.get("candidate_id"))
        item_id = f"{run_id}_{stable_id(source_document_id, row.get('pdf_path'), row.get('reason_code'))}"
        ocr_excerpt, ocr_chars, _ = read_ocr_excerpt(row, max_ocr_chars)
        render_status = "not_requested"
        render_error = ""
        rendered_image_path = ""
        if render_images and (max_render_docs <= 0 or rendered_count < max_render_docs):
            pdf_path = Path(clean(row.get("pdf_path")) or "__missing_pdf_path__")
            image_name = f"{safe_slug(source_document_id)}.png"
            image_path = out_dir / "images" / image_name
            render_status, render_error = render_pdf_image(pdf_path, image_path, render_scale)
            if render_status == "rendered":
                rendered_image_path = str(image_path)
                rendered_count += 1
        item_row: dict[str, Any] = {
            "page_visual_review_packet_run_id": run_id,
            "page_coverage_audit_run_id": coverage_run_id,
            "packet_id": "",
            "packet_index": "",
            "packet_item_index": "",
            "visual_review_item_id": item_id,
            "source_document_id": source_document_id,
            "boxscore_id": clean(row.get("boxscore_id")),
            "year": clean(row.get("year")),
            "game_date": clean(row.get("game_date")),
            "away_team": clean(row.get("away_team")),
            "home_team": clean(row.get("home_team")),
            "publication": clean(row.get("publication")),
            "result_date": clean(row.get("result_date")),
            "page": clean(row.get("page")),
            "candidate_rank": clean(row.get("candidate_rank")),
            "image_id": clean(row.get("image_id")),
            "pdf_path": clean(row.get("pdf_path")),
            "rendered_image_path": rendered_image_path,
            "render_status": render_status,
            "render_error": render_error,
            "source_url": clean(row.get("source_url")),
            "ocr_text_path": clean(row.get("sidecar_text_path")),
            "ocr_text_chars": ocr_chars or as_int(row.get("sidecar_text_chars_actual")),
            "sidecar_quality_lane": clean(row.get("sidecar_quality_lane")),
            "football_keyword_score": as_int(row.get("football_keyword_score")),
            "keyword_terms": clean(row.get("keyword_terms")),
            "keyword_excerpt": ocr_excerpt,
            "coverage_status": clean(row.get("coverage_status")),
            "reason_code": clean(row.get("reason_code")),
            "current_station": clean(row.get("current_station")),
            "current_status": clean(row.get("current_status")),
            "db_atom_count": as_int(row.get("db_atom_count")),
            "review_contract_json": review_contract({"source_document_id": source_document_id, "boxscore_id": row.get("boxscore_id")}),
            "created_at_utc": created_at,
        }
        items.append(item_row)
    return items


def packet_payload(packet_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "packet": packet_row,
        "review_contract": {
            "output_format": {
                "visual_review_item_id": "string",
                "source_document_id": "string",
                "decision": "extract_atoms | no_useful_stat | needs_better_ocr_or_crop | wrong_game_or_non_nfl | asset_problem",
                "confidence_bar": "high | medium | low",
                "needs_followup_type": "optional string",
                "atoms": [
                    {
                        "target_table": "one of target_tables",
                        "atom_type": "string",
                        "raw_entity": "string",
                        "normalized_value": "string",
                        "numeric_value": "optional number",
                        "unit": "optional string",
                        "evidence_text": "short quote or image-visible transcription",
                        "notes": "optional string",
                    }
                ],
                "notes": "optional string",
            },
            "target_tables": TARGET_TABLES,
        },
        "items": item_rows,
    }


def packet_markdown(packet_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> str:
    lines = [
        f"# Newspaper Page Visual Review Packet {packet_row['packet_index']}",
        "",
        "Read the page image/PDF and OCR excerpt. Extract only facts that are visible in the source.",
        "",
        "Allowed decisions: `extract_atoms`, `no_useful_stat`, `needs_better_ocr_or_crop`, `wrong_game_or_non_nfl`, `asset_problem`.",
        "",
        "Target tables: `" + "`, `".join(TARGET_TABLES) + "`.",
        "",
        "Return decisions in the companion decision template CSV or equivalent JSON using the packet item IDs.",
        "",
    ]
    for item in item_rows:
        lines.extend([
            f"## Item {item['packet_item_index']}: {item['source_document_id']}",
            "",
            f"- Game: `{item['boxscore_id']}` `{item['away_team']}` at `{item['home_team']}` on `{item['game_date']}`",
            f"- Source: `{item['publication']}` `{item['result_date']}` page `{item['page']}`",
            f"- Source URL: `{item['source_url']}`",
            f"- PDF: `{item['pdf_path']}`",
            f"- Rendered image: `{item['rendered_image_path']}`",
            f"- OCR text: `{item['ocr_text_path']}`",
            f"- Coverage status: `{item['coverage_status']}` reason `{item['reason_code']}`",
            f"- Keyword score: `{item['football_keyword_score']}` terms `{item['keyword_terms']}`",
            "",
            "OCR excerpt:",
            "",
            "```text",
            clean(item.get("keyword_excerpt")),
            "```",
            "",
        ])
    return "\n".join(lines) + "\n"


def persist(
    db_path: Path,
    run_row: dict[str, Any],
    packet_rows: list[dict[str, Any]],
    item_rows: list[dict[str, Any]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS newspaper_review.page_visual_review_packet_run (
              page_visual_review_packet_run_id VARCHAR,
              page_coverage_audit_run_id VARCHAR,
              created_at_utc TIMESTAMP,
              audit_dir VARCHAR,
              audit_candidates_csv VARCHAR,
              output_dir VARCHAR,
              review_output_dir VARCHAR,
              packet_count INTEGER,
              item_count INTEGER,
              max_items_per_packet INTEGER,
              render_images BOOLEAN,
              rendered_image_count INTEGER,
              render_error_count INTEGER,
              decision_template_csv VARCHAR,
              packet_manifest_csv VARCHAR,
              packet_items_csv VARCHAR,
              summary_json VARCHAR,
              report_md VARCHAR,
              persisted_to_duckdb BOOLEAN
            )
            """
        )
        packet_defs = ",\n              ".join(f"{field} VARCHAR" for field in PACKET_FIELDS)
        item_defs = ",\n              ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.page_visual_review_packet ({packet_defs})")
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.page_visual_review_packet_item ({item_defs})")
        run_id = clean(run_row.get("page_visual_review_packet_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.page_visual_review_packet_run WHERE page_visual_review_packet_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.page_visual_review_packet WHERE page_visual_review_packet_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.page_visual_review_packet_item WHERE page_visual_review_packet_run_id = ?",
            [run_id],
        )
        run_placeholders = ",".join(["?"] * len(RUN_FIELDS))
        con.execute(
            f"INSERT INTO newspaper_review.page_visual_review_packet_run ({','.join(RUN_FIELDS)}) VALUES ({run_placeholders})",
            [run_row.get(field) for field in RUN_FIELDS],
        )
        if packet_rows:
            packet_placeholders = ",".join(["?"] * len(PACKET_FIELDS))
            con.executemany(
                f"INSERT INTO newspaper_review.page_visual_review_packet ({','.join(PACKET_FIELDS)}) VALUES ({packet_placeholders})",
                [[safe_cell(row.get(field)) for field in PACKET_FIELDS] for row in packet_rows],
            )
        if item_rows:
            item_placeholders = ",".join(["?"] * len(ITEM_FIELDS))
            con.executemany(
                f"INSERT INTO newspaper_review.page_visual_review_packet_item ({','.join(ITEM_FIELDS)}) VALUES ({item_placeholders})",
                [[safe_cell(row.get(field)) for field in ITEM_FIELDS] for row in item_rows],
            )
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=None)
    parser.add_argument("--audit-root", type=Path, default=DEFAULT_AUDIT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--review-output-root", type=Path, default=DEFAULT_REVIEW_OUTPUT_ROOT)
    parser.add_argument("--label", default="1920_1939_page_visual_review_packets_v1")
    parser.add_argument("--max-items-per-packet", type=int, default=12)
    parser.add_argument("--max-docs", type=int, default=0, help="0 means all candidates.")
    parser.add_argument("--max-ocr-chars", type=int, default=1600)
    parser.add_argument("--render-images", action="store_true")
    parser.add_argument("--render-scale", type=float, default=1.8)
    parser.add_argument("--max-render-docs", type=int, default=0, help="0 means render every selected item.")
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    audit_dir = args.audit_dir or latest_audit_dir(args.audit_root)
    audit_summary = read_json(audit_dir / "summary.json")
    coverage_run_id = clean(audit_summary.get("page_coverage_audit_run_id")) or audit_dir.name
    candidates_csv = audit_dir / "visual_read_packet_candidates.csv"
    candidate_rows = read_csv(candidates_csv)
    candidate_rows.sort(
        key=lambda row: (
            -as_int(row.get("football_keyword_score")),
            clean(row.get("boxscore_id")),
            as_int(row.get("candidate_rank")),
        )
    )
    if args.max_docs > 0:
        candidate_rows = candidate_rows[: args.max_docs]

    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    review_output_dir = args.review_output_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    review_output_dir.mkdir(parents=True, exist_ok=True)

    item_rows = build_items(
        candidate_rows,
        run_id,
        coverage_run_id,
        out_dir,
        created_at,
        args.max_ocr_chars,
        args.render_images,
        args.render_scale,
        args.max_render_docs,
    )

    packet_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, Any]] = []
    all_packet_items: list[dict[str, Any]] = []
    for packet_index, packet_items in enumerate(chunked(item_rows, args.max_items_per_packet), start=1):
        packet_id = f"{run_id}_packet{packet_index:04d}"
        packet_dir = out_dir / f"packet_{packet_index:04d}"
        packet_dir.mkdir(parents=True, exist_ok=True)
        for item_index, item in enumerate(packet_items, start=1):
            item["packet_id"] = packet_id
            item["packet_index"] = packet_index
            item["packet_item_index"] = item_index
            decision_rows.append({
                "visual_review_item_id": item["visual_review_item_id"],
                "source_document_id": item["source_document_id"],
                "boxscore_id": item["boxscore_id"],
                "packet_id": packet_id,
                "decision": "",
                "confidence_bar": "",
                "needs_followup_type": "",
                "atoms_json": "[]",
                "notes": "",
            })
        packet_md = packet_dir / f"{packet_id}.md"
        packet_json = packet_dir / f"{packet_id}.json"
        packet_jsonl = packet_dir / f"{packet_id}.jsonl"
        packet_csv = packet_dir / f"{packet_id}_items.csv"
        packet_row = {
            "page_visual_review_packet_run_id": run_id,
            "page_coverage_audit_run_id": coverage_run_id,
            "packet_id": packet_id,
            "packet_index": packet_index,
            "packet_md_path": str(packet_md),
            "packet_json_path": str(packet_json),
            "packet_jsonl_path": str(packet_jsonl),
            "packet_item_csv_path": str(packet_csv),
            "item_count": len(packet_items),
            "priority_score_max": max(as_int(item.get("football_keyword_score")) for item in packet_items),
            "reason_code_counts_json": dict(Counter(clean(item.get("reason_code")) for item in packet_items)),
            "created_at_utc": created_at,
        }
        packet_rows.append(packet_row)
        write_csv(packet_csv, packet_items, ITEM_FIELDS)
        write_json(packet_json, packet_payload(packet_row, packet_items))
        write_jsonl(packet_jsonl, packet_items)
        packet_md.write_text(packet_markdown(packet_row, packet_items), encoding="utf-8")
        all_packet_items.extend(packet_items)

    decision_template_csv = review_output_dir / "visual_review_decision_template.csv"
    packet_manifest_csv = out_dir / "packet_manifest.csv"
    packet_items_csv = out_dir / "packet_items.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "page_visual_review_packet_report.md"
    write_csv(decision_template_csv, decision_rows, DECISION_FIELDS)
    write_csv(packet_manifest_csv, packet_rows, PACKET_FIELDS)
    write_csv(packet_items_csv, all_packet_items, ITEM_FIELDS)

    render_counts = Counter(clean(row.get("render_status")) for row in all_packet_items)
    reason_counts = Counter(clean(row.get("reason_code")) for row in all_packet_items)
    coverage_counts = Counter(clean(row.get("coverage_status")) for row in all_packet_items)
    run_row = {
        "page_visual_review_packet_run_id": run_id,
        "page_coverage_audit_run_id": coverage_run_id,
        "created_at_utc": created_at,
        "audit_dir": str(audit_dir),
        "audit_candidates_csv": str(candidates_csv),
        "output_dir": str(out_dir),
        "review_output_dir": str(review_output_dir),
        "packet_count": len(packet_rows),
        "item_count": len(all_packet_items),
        "max_items_per_packet": args.max_items_per_packet,
        "render_images": args.render_images,
        "rendered_image_count": render_counts["rendered"],
        "render_error_count": sum(count for status, count in render_counts.items() if status.startswith("render_error") or status in {"missing_pdf", "render_unavailable"}),
        "decision_template_csv": str(decision_template_csv),
        "packet_manifest_csv": str(packet_manifest_csv),
        "packet_items_csv": str(packet_items_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.no_persist,
    }
    summary = {
        **run_row,
        "reason_code_counts": dict(sorted(reason_counts.items())),
        "coverage_status_counts": dict(sorted(coverage_counts.items())),
        "render_status_counts": dict(sorted(render_counts.items())),
        "target_tables": TARGET_TABLES,
        "live_tables_touched": False,
    }
    write_json(summary_json, summary)
    report_lines = [
        "# Newspaper Page Visual Review Packets",
        "",
        f"Created: `{created_at}`",
        f"Run: `{run_id}`",
        f"Audit: `{audit_dir}`",
        f"Packets/items: `{len(packet_rows)}` / `{len(all_packet_items)}`",
        f"Decision template: `{decision_template_csv}`",
        f"Rendered images: `{render_counts['rendered']}`",
        "",
        "## Reason Counts",
        "",
    ]
    for key, value in sorted(reason_counts.items()):
        report_lines.append(f"- `{key}`: `{value}`")
    report_lines.extend([
        "",
        "## Outputs",
        "",
        f"- Packet manifest: `{packet_manifest_csv}`",
        f"- Packet items: `{packet_items_csv}`",
        f"- Review output dir: `{review_output_dir}`",
        "",
        "Live Fly/v26/supertable writes: no.",
        "",
    ])
    report_md.write_text("\n".join(report_lines), encoding="utf-8")

    if not args.no_persist:
        persist(args.db_path, run_row, packet_rows, all_packet_items)

    print(json.dumps({
        "page_visual_review_packet_run_id": run_id,
        "output_dir": str(out_dir),
        "review_output_dir": str(review_output_dir),
        "packet_count": len(packet_rows),
        "item_count": len(all_packet_items),
        "render_status_counts": dict(sorted(render_counts.items())),
        "decision_template_csv": str(decision_template_csv),
        "report_md": str(report_md),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
