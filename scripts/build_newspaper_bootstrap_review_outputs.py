#!/usr/bin/env python
"""Bootstrap conservative review JSON from extracted newspaper atom packets.

This is a first-pass conveyor station. It converts existing mechanical atom
claims in LLM review packets into the same JSON shape a human/LLM review would
write, but only for claims that already have an obvious table mapping. Generic
markers stay as follow-up notes so the conveyor can keep moving without
pretending a marker is a fully extracted stat.

All outputs stay under the local D-drive newspaper atom workspace.
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


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_PACKET_ROOT = DEFAULT_ROOT / "llm_review_packets"
DEFAULT_REVIEW_OUTPUT_ROOT = DEFAULT_ROOT / "llm_review_outputs"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "bootstrap_review_outputs"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def latest_dir(root: Path) -> Path:
    candidates = sorted(path for path in root.iterdir() if path.is_dir())
    if not candidates:
        raise FileNotFoundError(f"No packet run directories under {root}")
    return candidates[-1]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_score(value: Any) -> tuple[str, str]:
    text = clean(value)
    match = re.search(r"(\d{1,2})\s*(?:-|to|–|—)\s*(\d{1,2})", text, flags=re.IGNORECASE)
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def evidence_score_pairs(value: Any) -> list[tuple[str, str]]:
    text = clean(value)
    return [
        (match.group(1), match.group(2))
        for match in re.finditer(
            r"(\d{1,2})\s*(?:-|to|[\u2013\u2014]|â€“|â€”)\s*(\d{1,2})",
            text,
            flags=re.IGNORECASE,
        )
    ]


def score_pair_in_evidence(evidence: Any, score_1: str, score_2: str) -> bool:
    return (score_1, score_2) in evidence_score_pairs(evidence)


def parse_team_pair(value: Any) -> tuple[str, str]:
    text = clean(value).strip()
    match = re.search(r"\b([A-Z]{2,4})/([A-Z]{2,4})\b", text)
    if not match:
        return "", ""
    return match.group(1), match.group(2)


def evidence_is_explicit_final(evidence: Any) -> bool:
    text = clean(evidence).lower()
    return bool(
        re.search(
            r"\b(final\s+(score|count)|score\s+ended|ended\s+in|final\s+session|defeat(?:ed|s)?|won|victory|score\s+of)\b",
            text,
        )
    )


def confidence_from_atom(atom: dict[str, Any], *, explicit_final: bool = False) -> tuple[int, str, str]:
    raw_score = atom.get("confidence_score")
    try:
        score = int(round(float(raw_score)))
    except (TypeError, ValueError):
        score = 50
    if explicit_final:
        score = max(score, 82)
    if score >= 80:
        return score, "high", "promote"
    if score >= 55:
        return score, "medium", "review"
    return score, "low", "review"


def field_evidence(fields: dict[str, Any], atom: dict[str, Any], score: int) -> dict[str, dict[str, Any]]:
    region_id = clean(atom.get("region_id"))
    quote = clean(atom.get("evidence_text"))
    return {
        field: {
            "evidence_region_id": region_id,
            "evidence_quote": quote,
            "confidence_score": score,
        }
        for field, value in fields.items()
        if clean(value)
    }


def game_candidate_claim(doc: dict[str, Any], atom: dict[str, Any]) -> dict[str, Any] | None:
    team_1, team_2 = parse_team_pair(atom.get("entity_text"))
    score_1, score_2 = parse_score(atom.get("normalized_value") or atom.get("raw_value"))
    if not (team_1 and team_2 and score_1 and score_2):
        return None
    meta = doc.get("candidate_metadata", {}) or {}
    explicit = evidence_is_explicit_final(atom.get("evidence_text"))
    evidence_pairs = evidence_score_pairs(atom.get("evidence_text"))
    used_evidence_pair = False
    if explicit and len(evidence_pairs) == 1:
        score_1, score_2 = evidence_pairs[0]
        used_evidence_pair = True
    elif not score_pair_in_evidence(atom.get("evidence_text"), score_1, score_2):
        explicit = False
    confidence, lane, recommendation = confidence_from_atom(atom, explicit_final=explicit)
    if not used_evidence_pair and not score_pair_in_evidence(atom.get("evidence_text"), score_1, score_2):
        recommendation = "review"
        lane = "medium" if confidence >= 55 else "low"
    fields = {
        "boxscore_id": clean(atom.get("boxscore_id") or meta.get("boxscore_id")),
        "game_date": clean(meta.get("game_date")),
        "year": clean(meta.get("year")),
        "week": clean(meta.get("week")),
        "team_1_raw": team_1,
        "team_2_raw": team_2,
        "team_1_resolved": team_1,
        "team_2_resolved": team_2,
        "team_1_score": score_1,
        "team_2_score": score_2,
        "reconciliation_status": "bootstrap_review_from_newspaper_score_atom",
        "confidence_score": str(confidence),
        "evidence_text": clean(atom.get("evidence_text")),
        "source_document_id": clean(doc.get("source_document_id")),
        "region_id": clean(atom.get("region_id")),
    }
    return {
        "target_table": "game_candidate",
        "target_field": "",
        "target_fields": fields,
        "row_group_key": f"{fields['boxscore_id']}|{team_1}|{team_2}|{score_1}-{score_2}",
        "entity_text": clean(atom.get("entity_text")),
        "raw_value": clean(atom.get("raw_value")),
        "normalized_value": f"{score_1}-{score_2}",
        "numeric_value": None,
        "unit": "score",
        "evidence_region_id": clean(atom.get("region_id")),
        "evidence_quote": clean(atom.get("evidence_text")),
        "field_evidence": field_evidence(fields, atom, confidence),
        "confidence_score": confidence,
        "confidence_lane": lane,
        "promotion_recommendation": recommendation,
        "reason": (
            "Bootstrapped from newspaper score atom; when possible, the score is taken from "
            "the evidence quote itself instead of nearby OCR-normalized score text."
        ),
    }


def domain_row_by_atom(doc: dict[str, Any], domain_key: str, atom_claim_id: str) -> dict[str, Any]:
    for row in (doc.get("domain_rows", {}) or {}).get(domain_key, []) or []:
        if clean(row.get("atom_claim_id")) == atom_claim_id:
            return row
    return {}


def mapped_domain_claim(
    doc: dict[str, Any],
    atom: dict[str, Any],
    *,
    target_table: str,
    domain_key: str,
    row_group_fields: list[str],
    min_promote_score: int = 80,
) -> dict[str, Any] | None:
    row = domain_row_by_atom(doc, domain_key, clean(atom.get("atom_claim_id")))
    if not row:
        return None
    fields = {key: value for key, value in row.items() if clean(value)}
    if not fields:
        return None
    confidence, lane, recommendation = confidence_from_atom(atom)
    if confidence < min_promote_score:
        recommendation = "review"
        lane = "medium" if confidence >= 55 else "low"
    row_group = "|".join(clean(fields.get(field)) for field in row_group_fields if clean(fields.get(field)))
    if not row_group:
        row_group = clean(atom.get("atom_claim_id"))
    return {
        "target_table": target_table,
        "target_field": "",
        "target_fields": fields,
        "row_group_key": row_group,
        "entity_text": clean(atom.get("entity_text")),
        "raw_value": clean(atom.get("raw_value")),
        "normalized_value": clean(atom.get("normalized_value")),
        "numeric_value": atom.get("numeric_value"),
        "unit": clean(atom.get("unit")),
        "evidence_region_id": clean(atom.get("region_id")),
        "evidence_quote": clean(atom.get("evidence_text")),
        "field_evidence": field_evidence(fields, atom, confidence),
        "confidence_score": confidence,
        "confidence_lane": lane,
        "promotion_recommendation": recommendation,
        "reason": "Bootstrapped from table-shaped domain row emitted by article atom conveyor.",
    }


def followup_for_atom(atom: dict[str, Any]) -> dict[str, str] | None:
    atom_type = clean(atom.get("atom_type"))
    evidence = clean(atom.get("evidence_text"))
    if atom_type == "source_lineup_box":
        return {
            "followup_type": "identity_resolution",
            "reason": f"Lineup/substitution marker needs player/team extraction from article text: {evidence[:240]}",
        }
    if atom_type == "source_play_by_play_marker":
        return {
            "followup_type": "larger_crop",
            "reason": f"Play-by-play/period marker present but no structured event was safe to auto-extract: {evidence[:240]}",
        }
    return None


def reviewed_claims_for_doc(doc: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, str]], Counter]:
    claims: list[dict[str, Any]] = []
    followups: list[dict[str, str]] = []
    counts: Counter = Counter()
    seen_claim_keys: set[str] = set()
    seen_followups: set[tuple[str, str]] = set()

    for atom in doc.get("existing_atom_claims", []) or []:
        atom_type = clean(atom.get("atom_type"))
        claim: dict[str, Any] | None = None
        if atom_type == "source_game_score":
            claim = game_candidate_claim(doc, atom)
        elif atom_type == "source_scoring_event_touchdown":
            claim = mapped_domain_claim(
                doc,
                atom,
                target_table="scoring_event",
                domain_key="scoring_events",
                row_group_fields=["boxscore_id", "scoring_team_raw", "scoring_player_raw", "event_type", "distance_yards"],
            )
        elif atom_type == "source_notable_play":
            claim = mapped_domain_claim(
                doc,
                atom,
                target_table="play_by_play_event",
                domain_key="play_by_play_events",
                row_group_fields=["boxscore_id", "play_type", "primary_player_raw", "yards", "play_text"],
                min_promote_score=90,
            )
        elif atom_type == "source_player_game_box_score":
            claim = mapped_domain_claim(
                doc,
                atom,
                target_table="player_game_box_score",
                domain_key="player_game_box_scores",
                row_group_fields=["boxscore_id", "player_raw", "nfl_team", "source_row_text"],
            )

        if claim:
            key = json.dumps(
                [claim["target_table"], claim.get("row_group_key"), claim.get("target_fields", {})],
                sort_keys=True,
                ensure_ascii=False,
            )
            if key not in seen_claim_keys:
                claims.append(claim)
                seen_claim_keys.add(key)
                counts[f"claim_{claim['target_table']}"] += 1
                counts[f"recommend_{claim['promotion_recommendation']}"] += 1
            continue

        followup = followup_for_atom(atom)
        if followup:
            key = (followup["followup_type"], followup["reason"])
            if key not in seen_followups:
                followups.append(followup)
                seen_followups.add(key)
                counts[f"followup_{followup['followup_type']}"] += 1

    return claims, followups, counts


def doc_disposition(claims: list[dict[str, Any]], followups: list[dict[str, str]], doc: dict[str, Any]) -> str:
    if any(claim.get("promotion_recommendation") == "promote" for claim in claims):
        return "promotable"
    if claims or followups:
        return "review_needed"
    if doc.get("existing_atom_claims"):
        return "review_needed"
    return "no_useful_atoms"


def build_packet_review(packet_path: Path) -> tuple[dict[str, Any], Counter]:
    packet = read_json(packet_path)
    packet_id = clean(packet.get("packet_id")) or packet_path.stem
    counts: Counter = Counter()
    document_reviews: list[dict[str, Any]] = []
    for doc in packet.get("documents", []) or []:
        claims, followups, doc_counts = reviewed_claims_for_doc(doc)
        counts.update(doc_counts)
        counts["documents"] += 1
        if claims:
            counts["documents_with_claims"] += 1
        if followups:
            counts["documents_with_followup"] += 1
        document_reviews.append({
            "source_document_id": clean(doc.get("source_document_id")),
            "doc_disposition": doc_disposition(claims, followups, doc),
            "reviewed_claims": claims,
            "needs_followup": followups,
        })
    return {
        "packet_id": packet_id,
        "reviewer": "bootstrap_existing_atom_claims",
        "reviewed_at_utc": iso_now(),
        "document_reviews": document_reviews,
    }, counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet-run-dir", type=Path, default=None)
    parser.add_argument("--packet-root", type=Path, default=DEFAULT_PACKET_ROOT)
    parser.add_argument("--review-output-root", type=Path, default=DEFAULT_REVIEW_OUTPUT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="bootstrap_review_outputs")
    parser.add_argument("--packet-id", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    packet_run_dir = args.packet_run_dir or latest_dir(args.packet_root)
    packet_json_dir = packet_run_dir / "packets_json"
    packet_ids = set(args.packet_id or [])
    packet_paths = sorted(packet_json_dir.glob("packet_*.json"))
    if packet_ids:
        packet_paths = [path for path in packet_paths if path.stem in packet_ids]
    if not packet_paths:
        raise SystemExit("No packet JSON files selected.")

    review_output_dir = args.review_output_root / packet_run_dir.name
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    manifest_rows: list[dict[str, Any]] = []
    total_counts: Counter = Counter()
    for packet_path in packet_paths:
        review, counts = build_packet_review(packet_path)
        total_counts.update(counts)
        packet_id = clean(review.get("packet_id"))
        output_path = review_output_dir / f"{packet_id}_review.json"
        write_json(output_path, review)
        manifest_rows.append({
            "packet_id": packet_id,
            "packet_json_path": str(packet_path),
            "review_output_path": str(output_path),
            "document_reviews": len(review.get("document_reviews", [])),
            "reviewed_claims": sum(len(doc.get("reviewed_claims", [])) for doc in review.get("document_reviews", [])),
            "followups": sum(len(doc.get("needs_followup", [])) for doc in review.get("document_reviews", [])),
        })

    write_csv(out_dir / "bootstrap_review_output_manifest.csv", manifest_rows, [
        "packet_id",
        "packet_json_path",
        "review_output_path",
        "document_reviews",
        "reviewed_claims",
        "followups",
    ])
    summary = {
        "created_at_utc": iso_now(),
        "run_id": run_id,
        "packet_run_dir": str(packet_run_dir),
        "review_output_dir": str(review_output_dir),
        "output_dir": str(out_dir),
        "packets_reviewed": len(packet_paths),
        "counts": dict(total_counts),
        "live_tables_touched": False,
    }
    write_json(out_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
