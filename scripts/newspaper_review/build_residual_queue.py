from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
PACKET_RUN_ID = "20260701T013631Z_1920_1939_full_plan_page_visual_review_packets_v2"
PACKET_ITEMS = ROOT / "page_visual_review_packets" / PACKET_RUN_ID / "packet_items.csv"
OUT_DIR = ROOT / "conveyor_quality_gates" / "20260710T194743Z_residual_evidence_reread_v1"

DIRECT_FLAGS = {
    "non_extract_with_strong_ocr_hints",
    "extract_missing_game_candidate",
    "extract_missing_source_note",
    "ocr_line_score_terms_no_line_score_atom",
}

DEFERRED_PATTERNS = [
    r"not extracted",
    r"not promoted",
    r"not structured",
    r"left as context",
    r"too degraded",
    r"too small",
    r"not legible",
    r"not safely legible",
    r"too soft",
    r"partially noisy",
    r"partial/noisy",
    r"present but",
    r"too poor",
    r"not cleanly legible",
    r"not fully normalized",
    r"intentionally left blank",
    r"not safe to promote",
    r"not safe to extract",
]
DEFERRED_RE = re.compile("|".join(f"(?:{pattern})" for pattern in DEFERRED_PATTERNS), re.I)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def latest_file(root: Path, pattern: str) -> Path:
    matches = sorted(root.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matched {root / pattern}")
    return matches[-1]


def split_flags(value: object) -> set[str]:
    return {part.strip() for part in str(value or "").split(";") if part.strip()}


def is_explicit_deferred(notes: str) -> bool:
    return bool(DEFERRED_RE.search(notes or ""))


def classify_source(
    *,
    decision: str,
    notes: str,
    sanity_flags: set[str],
    consistency_flags: set[str],
    closure_status: str,
) -> dict[str, object]:
    reasons: list[str] = []
    if decision == "asset_problem":
        return {"excluded": True, "tier": 0, "reasons": ["asset_problem"]}

    if is_explicit_deferred(notes):
        reasons.append("explicit_deferred_evidence")
        tier = 1
    elif sanity_flags & DIRECT_FLAGS:
        reasons.append("direct_structural_gap")
        tier = 2
    elif any(flag.startswith("ocr_") for flag in sanity_flags):
        reasons.append("raw_page_ocr_atom_gap")
        tier = 3
    elif consistency_flags:
        reasons.append("publication_year_consistency_outlier")
        tier = 4
    else:
        return {"excluded": False, "tier": 5, "reasons": []}

    if sanity_flags:
        reasons.append("sanity_flags=" + ";".join(sorted(sanity_flags)))
    if consistency_flags:
        reasons.append("consistency_flags=" + ";".join(sorted(consistency_flags)))
    if closure_status == "closed_with_source_note":
        reasons.append("generic_closure_not_exhaustion_proof")
    return {"excluded": False, "tier": tier, "reasons": reasons}


def merge_signal_rows(
    *,
    sanity_rows: list[dict[str, str]],
    consistency_rows: list[dict[str, str]],
    reviewed_rows: list[dict[str, str]],
    closure_rows: list[dict[str, str]],
) -> dict[str, dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}

    def ensure(source_id: str) -> dict[str, object]:
        return merged.setdefault(
            source_id,
            {
                "source_document_id": source_id,
                "sanity_flags": set(),
                "consistency_flags": set(),
                "decision": "",
                "notes": "",
                "closure_status": "",
            },
        )

    for row in reviewed_rows:
        source_id = row.get("source_document_id", "")
        if not source_id:
            continue
        has_signal = row.get("decision") == "asset_problem" or is_explicit_deferred(row.get("notes_excerpt", ""))
        if not has_signal:
            continue
        target = ensure(source_id)
        target.update({key: value for key, value in row.items() if value not in (None, "")})
        target["decision"] = row.get("decision", "")
        target["notes"] = row.get("notes_excerpt", "")

    for row in sanity_rows:
        source_id = row.get("source_document_id", "")
        if not source_id:
            continue
        target = ensure(source_id)
        target.update({key: value for key, value in row.items() if value not in (None, "") and key not in {"flag", "flags"}})
        target["sanity_flags"].update(split_flags(row.get("flag")))
        target["sanity_flags"].update(split_flags(row.get("flags")))
        target["decision"] = row.get("decision", target.get("decision", ""))
        target["notes"] = row.get("notes_excerpt", target.get("notes", ""))

    for row in consistency_rows:
        source_id = row.get("source_document_id", "")
        if not source_id:
            continue
        target = ensure(source_id)
        target.update({key: value for key, value in row.items() if value not in (None, "") and key != "flags"})
        target["consistency_flags"].update(split_flags(row.get("flags")))
        target["decision"] = row.get("decision", target.get("decision", ""))
        target["notes"] = row.get("notes_excerpt", target.get("notes", ""))

    for row in closure_rows:
        source_id = row.get("source_document_id", "")
        if source_id in merged:
            merged[source_id]["closure_status"] = row.get("closure_status", "")

    return merged


def main() -> None:
    sanity_rows_path = latest_file(
        ROOT / "page_visual_sanity_audits",
        "*_visual_packet_recall_sanity_v1/visual_packet_reviewed_page_sanity_rows.csv",
    )
    sanity_candidates_path = sanity_rows_path.parent / "visual_packet_second_pass_candidates.csv"
    consistency_path = latest_file(
        ROOT / "publication_year_consistency_audits",
        "*_publication_year_consistency_v1/publication_year_consistency_candidates.csv",
    )
    closure_path = latest_file(
        ROOT / "conveyor_quality_gates",
        "*_publication_year_consistency_revisit_closure_*/closure_ledger.csv",
    )

    reviewed_rows = read_csv(sanity_rows_path)
    sanity_rows = read_csv(sanity_candidates_path)
    consistency_rows = read_csv(consistency_path)
    closure_rows = read_csv(closure_path)
    packet_rows = read_csv(PACKET_ITEMS)
    packet_by_source = {row["source_document_id"]: row for row in packet_rows}

    merged = merge_signal_rows(
        sanity_rows=sanity_rows,
        consistency_rows=consistency_rows,
        reviewed_rows=reviewed_rows,
        closure_rows=closure_rows,
    )

    queue: list[dict[str, object]] = []
    excluded: list[dict[str, object]] = []
    for source_id, row in merged.items():
        classification = classify_source(
            decision=str(row.get("decision", "")),
            notes=str(row.get("notes", "")),
            sanity_flags=set(row["sanity_flags"]),
            consistency_flags=set(row["consistency_flags"]),
            closure_status=str(row.get("closure_status", "")),
        )
        packet = packet_by_source.get(source_id, {})
        output = dict(row)
        output.update(packet)
        output.update(
            {
                "tier": classification["tier"],
                "review_status": "excluded_unavailable_asset" if classification["excluded"] else "pending",
                "review_reasons": " | ".join(classification["reasons"]),
                "sanity_flags": ";".join(sorted(row["sanity_flags"])),
                "consistency_flags": ";".join(sorted(row["consistency_flags"])),
                "prior_notes": row.get("notes", ""),
                "closure_status": row.get("closure_status", ""),
            }
        )
        if classification["excluded"]:
            excluded.append(output)
        elif int(classification["tier"]) < 5:
            queue.append(output)

    queue.sort(key=lambda row: (int(row["tier"]), int(row.get("generated_total_distinct") or 0), row["source_document_id"]))
    excluded.sort(key=lambda row: row["source_document_id"])
    for index, row in enumerate(queue, start=1):
        row["queue_rank"] = index

    fields = [
        "queue_rank", "tier", "review_status", "review_reasons", "source_document_id", "boxscore_id",
        "year", "game_date", "away_team", "home_team", "publication", "result_date", "page",
        "decision", "generated_total_distinct", "sanity_flags", "consistency_flags", "closure_status",
        "rendered_image_path", "pdf_path", "ocr_text_path", "visual_review_item_id", "packet_id",
        "packet_item_index", "prior_notes",
    ]
    write_csv(OUT_DIR / "residual_reread_queue.csv", queue, fields)
    write_csv(OUT_DIR / "excluded_assets.csv", excluded, fields)

    tier_counts = Counter(int(row["tier"]) for row in queue)
    summary = {
        "created_at_utc": stamp(),
        "sanity_rows_input": str(sanity_rows_path),
        "sanity_candidates_input": str(sanity_candidates_path),
        "consistency_candidates_input": str(consistency_path),
        "closure_ledger_input": str(closure_path),
        "queue_count": len(queue),
        "excluded_asset_count": len(excluded),
        "tier_counts": dict(sorted(tier_counts.items())),
        "queue_source_ids_unique": len({row["source_document_id"] for row in queue}),
    }
    (OUT_DIR / "queue_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Residual Evidence Reread Queue",
        "",
        f"- Created: {summary['created_at_utc']}",
        f"- Readable pages queued: {summary['queue_count']}",
        f"- Unavailable assets excluded: {summary['excluded_asset_count']}",
        f"- Tier counts: {summary['tier_counts']}",
        "- Tier 1: original notes explicitly identify visible but deferred evidence",
        "- Tier 2: direct structural or decision gap",
        "- Tier 3: raw page OCR/atom mismatch requiring target-region validation",
        "- Tier 4: publication/year consistency outlier",
        "",
        f"- Queue: `{OUT_DIR / 'residual_reread_queue.csv'}`",
        f"- Exclusions: `{OUT_DIR / 'excluded_assets.csv'}`",
    ]
    (OUT_DIR / "queue_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
