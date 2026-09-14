#!/usr/bin/env python
"""Build a read-state handoff for newspaper LLM review packets.

This lets another LLM session review packet files independently while the
conveyor keeps an auditable queue of what is unread, reviewed, promotable, or
needs another OCR/review round.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_PACKET_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_packets")
DEFAULT_OUTPUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_outputs")
DEFAULT_STATE_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_read_states")


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def latest_packet_run(root: Path) -> Path:
    candidates = sorted(path for path in root.iterdir() if path.is_dir())
    if not candidates:
        raise FileNotFoundError(f"No packet run folders found under {root}")
    return candidates[-1]


def load_review_outputs(output_dir: Path) -> dict[str, dict[str, Any]]:
    by_doc: dict[str, dict[str, Any]] = {}
    if not output_dir.exists():
        return by_doc
    for path in sorted(output_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        reviews = payload.get("document_reviews")
        if isinstance(reviews, dict):
            reviews = [reviews]
        if not isinstance(reviews, list):
            continue
        for review in reviews:
            if not isinstance(review, dict):
                continue
            source_document_id = str(review.get("source_document_id") or "").strip()
            if not source_document_id:
                continue
            reviewed_claims = review.get("reviewed_claims") or []
            followups = review.get("needs_followup") or []
            claim_count = len(reviewed_claims) if isinstance(reviewed_claims, list) else 0
            followup_count = len(followups) if isinstance(followups, list) else 0
            promotion_count = 0
            high_count = 0
            medium_count = 0
            low_count = 0
            if isinstance(reviewed_claims, list):
                for claim in reviewed_claims:
                    if not isinstance(claim, dict):
                        continue
                    if claim.get("promotion_recommendation") == "promote":
                        promotion_count += 1
                    lane = claim.get("confidence_lane")
                    if lane == "high":
                        high_count += 1
                    elif lane == "medium":
                        medium_count += 1
                    elif lane == "low":
                        low_count += 1
            by_doc[source_document_id] = {
                "review_output_path": str(path),
                "reviewed_at_utc": payload.get("reviewed_at_utc") or review.get("reviewed_at_utc") or "",
                "reviewer": payload.get("reviewer") or review.get("reviewer") or "",
                "doc_disposition": review.get("doc_disposition") or "",
                "reviewed_claim_count": claim_count,
                "review_high_count": high_count,
                "review_medium_count": medium_count,
                "review_low_count": low_count,
                "review_promotion_recommendation_count": promotion_count,
                "review_followup_count": followup_count,
            }
    return by_doc


def classify_state(packet_doc: dict[str, str], review: dict[str, Any] | None) -> tuple[str, str]:
    if not review:
        return "unread", "needs_llm_review"
    disposition = str(review.get("doc_disposition") or "")
    if int(review.get("review_promotion_recommendation_count") or 0) > 0:
        return "reviewed_promotable", "llm_recommended_promotion"
    if int(review.get("review_followup_count") or 0) > 0:
        return "reviewed_needs_followup", "llm_requested_followup"
    if disposition == "no_useful_atoms":
        return "reviewed_no_useful_atoms", "llm_found_no_useful_atoms"
    if disposition == "low_signal":
        return "reviewed_low_signal", "llm_marked_low_signal"
    if disposition:
        return "reviewed", f"llm_disposition_{disposition}"
    return "reviewed", "llm_output_present"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet-run-dir", type=Path, default=None)
    parser.add_argument("--review-output-dir", type=Path, default=None)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--label", default="llm_read_state")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    packet_run_dir = args.packet_run_dir or latest_packet_run(DEFAULT_PACKET_ROOT)
    review_output_dir = args.review_output_dir or (DEFAULT_OUTPUT_ROOT / packet_run_dir.name)
    out_dir = args.state_root / f"{stamp()}_{args.label}"
    out_dir.mkdir(parents=True, exist_ok=True)

    packet_manifest = read_csv(packet_run_dir / "llm_review_packet_manifest.csv")
    document_manifest = read_csv(packet_run_dir / "document_packet_manifest.csv")
    reviews_by_doc = load_review_outputs(review_output_dir)

    packet_paths = {row["packet_id"]: row for row in packet_manifest}
    docs_by_packet: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in document_manifest:
        docs_by_packet[row["packet_id"]].append(row)

    document_rows: list[dict[str, Any]] = []
    for row in document_manifest:
        review = reviews_by_doc.get(row["source_document_id"])
        read_state, reason_code = classify_state(row, review)
        document_rows.append({
            **row,
            "read_state": read_state,
            "read_state_reason_code": reason_code,
            "review_output_path": (review or {}).get("review_output_path", ""),
            "reviewed_at_utc": (review or {}).get("reviewed_at_utc", ""),
            "reviewer": (review or {}).get("reviewer", ""),
            "doc_disposition": (review or {}).get("doc_disposition", ""),
            "reviewed_claim_count": (review or {}).get("reviewed_claim_count", 0),
            "review_high_count": (review or {}).get("review_high_count", 0),
            "review_medium_count": (review or {}).get("review_medium_count", 0),
            "review_low_count": (review or {}).get("review_low_count", 0),
            "review_promotion_recommendation_count": (review or {}).get("review_promotion_recommendation_count", 0),
            "review_followup_count": (review or {}).get("review_followup_count", 0),
        })

    state_by_doc = {row["source_document_id"]: row for row in document_rows}
    packet_rows: list[dict[str, Any]] = []
    for packet_id, docs in docs_by_packet.items():
        states = Counter(state_by_doc[row["source_document_id"]]["read_state"] for row in docs)
        packet_state = "complete" if states and not states.get("unread") else "unread"
        if states.get("reviewed_promotable"):
            packet_state = "has_promotable_reviews" if states.get("unread") else "complete_has_promotable_reviews"
        elif states.get("reviewed_needs_followup"):
            packet_state = "has_followup_reviews" if states.get("unread") else "complete_has_followup_reviews"
        packet_info = packet_paths.get(packet_id, {})
        packet_rows.append({
            "packet_id": packet_id,
            "packet_state": packet_state,
            "document_count": len(docs),
            "unread_document_count": states.get("unread", 0),
            "reviewed_document_count": len(docs) - states.get("unread", 0),
            "promotable_document_count": states.get("reviewed_promotable", 0),
            "followup_document_count": states.get("reviewed_needs_followup", 0),
            "packet_md_path": packet_info.get("packet_md_path", ""),
            "packet_json_path": packet_info.get("packet_json_path", ""),
            "packet_jsonl_path": packet_info.get("packet_jsonl_path", ""),
            "document_states_json": json.dumps(dict(states), sort_keys=True),
        })

    document_fields = [
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
        "read_state",
        "read_state_reason_code",
        "review_output_path",
        "reviewed_at_utc",
        "reviewer",
        "doc_disposition",
        "reviewed_claim_count",
        "review_high_count",
        "review_medium_count",
        "review_low_count",
        "review_promotion_recommendation_count",
        "review_followup_count",
    ]
    packet_fields = [
        "packet_id",
        "packet_state",
        "document_count",
        "unread_document_count",
        "reviewed_document_count",
        "promotable_document_count",
        "followup_document_count",
        "packet_md_path",
        "packet_json_path",
        "packet_jsonl_path",
        "document_states_json",
    ]
    write_csv(out_dir / "llm_document_read_state.csv", document_rows, document_fields)
    write_csv(out_dir / "llm_packet_read_state.csv", packet_rows, packet_fields)
    write_csv(
        out_dir / "next_unread_packet_queue.csv",
        [row for row in packet_rows if int(row["unread_document_count"]) > 0],
        packet_fields,
    )
    write_csv(
        out_dir / "next_promotable_review_queue.csv",
        [row for row in document_rows if row["read_state"] == "reviewed_promotable"],
        document_fields,
    )
    write_csv(
        out_dir / "next_followup_review_queue.csv",
        [row for row in document_rows if row["read_state"] == "reviewed_needs_followup"],
        document_fields,
    )

    summary = {
        "created_at_utc": iso_now(),
        "label": args.label,
        "packet_run_dir": str(packet_run_dir),
        "review_output_dir": str(review_output_dir),
        "output_dir": str(out_dir),
        "packet_count": len(packet_rows),
        "document_count": len(document_rows),
        "document_read_state_counts": dict(Counter(row["read_state"] for row in document_rows)),
        "packet_state_counts": dict(Counter(row["packet_state"] for row in packet_rows)),
    }
    write_json(out_dir / "summary.json", summary)
    (out_dir / "README.md").write_text(
        "\n".join([
            "# Newspaper LLM Read State",
            "",
            "This folder tracks which review packets still need to be read and which reviewed documents are promotable or need follow-up.",
            "",
            "Workflow:",
            "1. Give ChatGPT one packet from `next_unread_packet_queue.csv`, preferably the Markdown path.",
            "2. Save ChatGPT's structured JSON response under the configured `review_output_dir`.",
            "3. Re-run this script to update the unread, promotable, and follow-up queues.",
            "",
        ]),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
