#!/usr/bin/env python
"""Ingest ChatGPT/LLM newspaper review JSON into local D-drive review tables.

This station closes the handoff loop:

1. `build_newspaper_llm_review_packets.py` creates packet files to read.
2. ChatGPT returns structured JSON review output for those packets.
3. This script flattens that JSON into separate review tables and queues.

It does not write to live supertable/Fly tables. It writes only to the local
newspaper atom DuckDB database and D-drive artifacts by default.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_PACKET_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_packets")
DEFAULT_OUTPUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_outputs")
DEFAULT_INGEST_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_ingests")

TARGET_TABLES = {
    "game_candidate",
    "scoring_event",
    "play_by_play_event",
    "player_game_box_score",
    "lineup_participation",
    "player_identity_candidate",
    "promotion_candidate",
}
CONFIDENCE_LANES = {"high", "medium", "low"}
PROMOTION_RECOMMENDATIONS = {"promote", "review", "reject"}

DOCUMENT_FIELDS = [
    "ingest_run_id",
    "packet_id",
    "source_document_id",
    "source_packet_path",
    "review_output_path",
    "reviewer",
    "reviewed_at_utc",
    "doc_disposition",
    "document_next_action",
    "boxscore_id",
    "publication",
    "issue_date",
    "page",
    "claim_count",
    "high_claim_count",
    "medium_claim_count",
    "low_claim_count",
    "promotion_recommendation_count",
    "followup_count",
    "validation_status",
    "validation_notes",
    "created_at_utc",
]

CLAIM_FIELDS = [
    "llm_review_claim_id",
    "ingest_run_id",
    "packet_id",
    "source_document_id",
    "claim_index",
    "target_table",
    "target_field",
    "target_fields_json",
    "row_group_key",
    "entity_text",
    "raw_value",
    "normalized_value",
    "numeric_value",
    "unit",
    "evidence_region_id",
    "evidence_quote",
    "field_evidence_json",
    "confidence_score",
    "confidence_lane",
    "promotion_recommendation",
    "reason",
    "review_status",
    "validation_status",
    "validation_notes",
    "review_output_path",
    "created_at_utc",
]

FOLLOWUP_FIELDS = [
    "llm_review_followup_id",
    "ingest_run_id",
    "packet_id",
    "source_document_id",
    "followup_index",
    "followup_type",
    "reason",
    "status",
    "review_output_path",
    "created_at_utc",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def clean_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_intish(value: Any) -> int | None:
    number = parse_float(value)
    if number is None:
        return None
    return int(round(number))


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


def load_packet_manifest(packet_run_dir: Path) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    packet_rows = read_csv(packet_run_dir / "llm_review_packet_manifest.csv")
    doc_rows = read_csv(packet_run_dir / "document_packet_manifest.csv")
    packet_by_id = {row["packet_id"]: row for row in packet_rows}
    doc_by_id = {row["source_document_id"]: row for row in doc_rows}
    return packet_by_id, doc_by_id


def review_output_paths(output_dir: Path) -> list[Path]:
    if not output_dir.exists():
        return []
    return sorted(path for path in output_dir.rglob("*.json") if path.is_file())


def normalize_review_payload(payload: Any, path: Path) -> list[dict[str, Any]]:
    """Return document review payloads with top-level packet metadata attached."""
    if isinstance(payload, list):
        packet_id = ""
        reviewer = ""
        reviewed_at_utc = ""
        reviews = payload
    elif isinstance(payload, dict):
        if "source_document_id" in payload:
            reviews = [payload]
        else:
            reviews = payload.get("document_reviews") or payload.get("documents") or []
        packet_id = clean(payload.get("packet_id"))
        reviewer = clean(payload.get("reviewer"))
        reviewed_at_utc = clean(payload.get("reviewed_at_utc"))
    else:
        return []

    if isinstance(reviews, dict):
        reviews = [reviews]
    if not isinstance(reviews, list):
        return []

    normalized: list[dict[str, Any]] = []
    for review in reviews:
        if not isinstance(review, dict):
            continue
        row = dict(review)
        row.setdefault("_packet_id", packet_id)
        row.setdefault("_reviewer", reviewer)
        row.setdefault("_reviewed_at_utc", reviewed_at_utc)
        row.setdefault("_review_output_path", str(path))
        normalized.append(row)
    return normalized


def load_reviews(output_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reviews: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for path in review_output_paths(output_dir):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            errors.append({
                "review_output_path": str(path),
                "error_type": type(exc).__name__,
                "error": str(exc),
            })
            continue
        reviews.extend(normalize_review_payload(payload, path))
    return reviews, errors


def validate_claim(claim: dict[str, Any]) -> tuple[str, str]:
    notes: list[str] = []
    target_table = clean(claim.get("target_table"))
    target_field = clean(claim.get("target_field"))
    target_fields = claim.get("target_fields")
    confidence_lane = clean(claim.get("confidence_lane"))
    promotion = clean(claim.get("promotion_recommendation"))
    if target_table and target_table not in TARGET_TABLES:
        notes.append(f"unknown_target_table={target_table}")
    if target_fields not in (None, "") and not isinstance(target_fields, dict):
        notes.append("target_fields_not_object")
    if confidence_lane and confidence_lane not in CONFIDENCE_LANES:
        notes.append(f"unknown_confidence_lane={confidence_lane}")
    if promotion and promotion not in PROMOTION_RECOMMENDATIONS:
        notes.append(f"unknown_promotion_recommendation={promotion}")
    if not clean(claim.get("evidence_quote")):
        notes.append("missing_evidence_quote")
    if not target_table:
        notes.append("missing_target_table")
    if not target_field and not target_fields:
        notes.append("missing_target_field_or_target_fields")
    return ("valid" if not notes else "needs_review", ";".join(notes))


def flatten_reviews(
    reviews: list[dict[str, Any]],
    packet_by_id: dict[str, dict[str, str]],
    doc_by_id: dict[str, dict[str, str]],
    ingest_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    created_at = iso_now()
    document_rows: list[dict[str, Any]] = []
    claim_rows: list[dict[str, Any]] = []
    followup_rows: list[dict[str, Any]] = []

    for review in reviews:
        source_document_id = clean(review.get("source_document_id"))
        packet_id = clean(review.get("_packet_id") or review.get("packet_id"))
        doc_manifest = doc_by_id.get(source_document_id, {})
        if not packet_id:
            packet_id = clean(doc_manifest.get("packet_id"))
        packet_manifest = packet_by_id.get(packet_id, {})
        claims = review.get("reviewed_claims") or []
        followups = review.get("needs_followup") or []
        if isinstance(claims, dict):
            claims = [claims]
        if not isinstance(claims, list):
            claims = []
        if isinstance(followups, dict):
            followups = [followups]
        if not isinstance(followups, list):
            followups = []

        lane_counts = Counter(clean(claim.get("confidence_lane")) for claim in claims if isinstance(claim, dict))
        promotion_count = sum(
            1 for claim in claims
            if isinstance(claim, dict) and clean(claim.get("promotion_recommendation")) == "promote"
        )
        validation_notes: list[str] = []
        if not source_document_id:
            validation_notes.append("missing_source_document_id")
        elif source_document_id not in doc_by_id:
            validation_notes.append("source_document_not_in_packet_manifest")
        if not packet_id:
            validation_notes.append("missing_packet_id")

        document_rows.append({
            "ingest_run_id": ingest_run_id,
            "packet_id": packet_id,
            "source_document_id": source_document_id,
            "source_packet_path": packet_manifest.get("packet_md_path") or packet_manifest.get("packet_json_path") or "",
            "review_output_path": clean(review.get("_review_output_path")),
            "reviewer": clean(review.get("_reviewer") or review.get("reviewer")),
            "reviewed_at_utc": clean(review.get("_reviewed_at_utc") or review.get("reviewed_at_utc")),
            "doc_disposition": clean(review.get("doc_disposition")),
            "document_next_action": clean(doc_manifest.get("document_next_action")),
            "boxscore_id": clean(doc_manifest.get("boxscore_id")),
            "publication": clean(doc_manifest.get("publication")),
            "issue_date": clean(doc_manifest.get("issue_date")),
            "page": clean(doc_manifest.get("page")),
            "claim_count": len(claims),
            "high_claim_count": lane_counts.get("high", 0),
            "medium_claim_count": lane_counts.get("medium", 0),
            "low_claim_count": lane_counts.get("low", 0),
            "promotion_recommendation_count": promotion_count,
            "followup_count": len(followups),
            "validation_status": "valid" if not validation_notes else "needs_review",
            "validation_notes": ";".join(validation_notes),
            "created_at_utc": created_at,
        })

        for index, claim in enumerate(claims, start=1):
            if not isinstance(claim, dict):
                continue
            validation_status, validation_note = validate_claim(claim)
            confidence_score = parse_intish(claim.get("confidence_score"))
            claim_rows.append({
                "llm_review_claim_id": stable_id(
                    ingest_run_id,
                    packet_id,
                    source_document_id,
                    index,
                    claim.get("target_table"),
                    claim.get("target_field"),
                    clean_json(claim.get("target_fields")),
                    claim.get("row_group_key"),
                    claim.get("entity_text"),
                    claim.get("raw_value"),
                    claim.get("evidence_region_id"),
                    claim.get("evidence_quote"),
                ),
                "ingest_run_id": ingest_run_id,
                "packet_id": packet_id,
                "source_document_id": source_document_id,
                "claim_index": index,
                "target_table": clean(claim.get("target_table")),
                "target_field": clean(claim.get("target_field")),
                "target_fields_json": clean_json(claim.get("target_fields")),
                "row_group_key": clean(claim.get("row_group_key")),
                "entity_text": clean(claim.get("entity_text")),
                "raw_value": clean(claim.get("raw_value")),
                "normalized_value": clean(claim.get("normalized_value")),
                "numeric_value": parse_float(claim.get("numeric_value")),
                "unit": clean(claim.get("unit")),
                "evidence_region_id": clean(claim.get("evidence_region_id")),
                "evidence_quote": clean(claim.get("evidence_quote")),
                "field_evidence_json": clean_json(claim.get("field_evidence")),
                "confidence_score": confidence_score,
                "confidence_lane": clean(claim.get("confidence_lane")),
                "promotion_recommendation": clean(claim.get("promotion_recommendation")),
                "reason": clean(claim.get("reason")),
                "review_status": "llm_reviewed",
                "validation_status": validation_status,
                "validation_notes": validation_note,
                "review_output_path": clean(review.get("_review_output_path")),
                "created_at_utc": created_at,
            })

        for index, followup in enumerate(followups, start=1):
            if not isinstance(followup, dict):
                continue
            followup_rows.append({
                "llm_review_followup_id": stable_id(
                    ingest_run_id,
                    packet_id,
                    source_document_id,
                    index,
                    followup.get("followup_type"),
                    followup.get("reason"),
                ),
                "ingest_run_id": ingest_run_id,
                "packet_id": packet_id,
                "source_document_id": source_document_id,
                "followup_index": index,
                "followup_type": clean(followup.get("followup_type")),
                "reason": clean(followup.get("reason")),
                "status": "open",
                "review_output_path": clean(review.get("_review_output_path")),
                "created_at_utc": created_at,
            })

    return document_rows, claim_rows, followup_rows


def create_llm_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_document_review (
          ingest_run_id VARCHAR,
          packet_id VARCHAR,
          source_document_id VARCHAR,
          source_packet_path VARCHAR,
          review_output_path VARCHAR,
          reviewer VARCHAR,
          reviewed_at_utc VARCHAR,
          doc_disposition VARCHAR,
          document_next_action VARCHAR,
          boxscore_id VARCHAR,
          publication VARCHAR,
          issue_date VARCHAR,
          page VARCHAR,
          claim_count INTEGER,
          high_claim_count INTEGER,
          medium_claim_count INTEGER,
          low_claim_count INTEGER,
          promotion_recommendation_count INTEGER,
          followup_count INTEGER,
          validation_status VARCHAR,
          validation_notes VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_review_claim (
          llm_review_claim_id VARCHAR,
          ingest_run_id VARCHAR,
          packet_id VARCHAR,
          source_document_id VARCHAR,
          claim_index INTEGER,
          target_table VARCHAR,
          target_field VARCHAR,
          target_fields_json VARCHAR,
          row_group_key VARCHAR,
          entity_text VARCHAR,
          raw_value VARCHAR,
          normalized_value VARCHAR,
          numeric_value DOUBLE,
          unit VARCHAR,
          evidence_region_id VARCHAR,
          evidence_quote VARCHAR,
          field_evidence_json VARCHAR,
          confidence_score INTEGER,
          confidence_lane VARCHAR,
          promotion_recommendation VARCHAR,
          reason VARCHAR,
          review_status VARCHAR,
          validation_status VARCHAR,
          validation_notes VARCHAR,
          review_output_path VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    for column_name, column_type in [
        ("target_fields_json", "VARCHAR"),
        ("row_group_key", "VARCHAR"),
        ("field_evidence_json", "VARCHAR"),
    ]:
        con.execute(
            f"ALTER TABLE newspaper_review.llm_review_claim "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_review_followup (
          llm_review_followup_id VARCHAR,
          ingest_run_id VARCHAR,
          packet_id VARCHAR,
          source_document_id VARCHAR,
          followup_index INTEGER,
          followup_type VARCHAR,
          reason VARCHAR,
          status VARCHAR,
          review_output_path VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_review_ingest_run (
          ingest_run_id VARCHAR,
          packet_run_dir VARCHAR,
          review_output_dir VARCHAR,
          output_dir VARCHAR,
          document_review_count INTEGER,
          claim_count INTEGER,
          followup_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ",".join(["?"] * len(fields))
    con.executemany(
        f"INSERT INTO {table} ({','.join(fields)}) VALUES ({placeholders})",
        [[row.get(field) for field in fields] for row in rows],
    )


def persist_to_duckdb(
    db_path: Path,
    ingest_run_id: str,
    packet_run_dir: Path,
    review_output_dir: Path,
    out_dir: Path,
    document_rows: list[dict[str, Any]],
    claim_rows: list[dict[str, Any]],
    followup_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_llm_tables(con)
        for table in [
            "newspaper_review.llm_document_review",
            "newspaper_review.llm_review_claim",
            "newspaper_review.llm_review_followup",
            "newspaper_review.llm_review_ingest_run",
        ]:
            con.execute(f"DELETE FROM {table} WHERE ingest_run_id = ?", [ingest_run_id])
        insert_rows(con, "newspaper_review.llm_document_review", document_rows, DOCUMENT_FIELDS)
        insert_rows(con, "newspaper_review.llm_review_claim", claim_rows, CLAIM_FIELDS)
        insert_rows(con, "newspaper_review.llm_review_followup", followup_rows, FOLLOWUP_FIELDS)
        insert_rows(con, "newspaper_review.llm_review_ingest_run", [{
            "ingest_run_id": ingest_run_id,
            "packet_run_dir": str(packet_run_dir),
            "review_output_dir": str(review_output_dir),
            "output_dir": str(out_dir),
            "document_review_count": len(document_rows),
            "claim_count": len(claim_rows),
            "followup_count": len(followup_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], [
            "ingest_run_id",
            "packet_run_dir",
            "review_output_dir",
            "output_dir",
            "document_review_count",
            "claim_count",
            "followup_count",
            "status",
            "created_at_utc",
            "summary_json_path",
        ])
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet-run-dir", type=Path, default=None)
    parser.add_argument("--review-output-dir", type=Path, default=None)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_INGEST_ROOT)
    parser.add_argument("--label", default="llm_review_ingest")
    parser.add_argument("--no-db", action="store_true", help="Write artifacts only; do not persist local DuckDB tables.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    packet_run_dir = args.packet_run_dir or latest_packet_run(DEFAULT_PACKET_ROOT)
    review_output_dir = args.review_output_dir or (DEFAULT_OUTPUT_ROOT / packet_run_dir.name)
    ingest_run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / ingest_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    packet_by_id, doc_by_id = load_packet_manifest(packet_run_dir)
    reviews, parse_errors = load_reviews(review_output_dir)
    document_rows, claim_rows, followup_rows = flatten_reviews(reviews, packet_by_id, doc_by_id, ingest_run_id)

    promotable_claims = [
        row for row in claim_rows
        if row["promotion_recommendation"] == "promote" and row["validation_status"] == "valid"
    ]
    review_claims = [
        row for row in claim_rows
        if row["promotion_recommendation"] == "review" or row["validation_status"] != "valid"
    ]

    write_csv(out_dir / "llm_document_reviews.csv", document_rows, DOCUMENT_FIELDS)
    write_csv(out_dir / "llm_review_claims.csv", claim_rows, CLAIM_FIELDS)
    write_csv(out_dir / "llm_review_followups.csv", followup_rows, FOLLOWUP_FIELDS)
    write_csv(out_dir / "llm_promotable_claim_queue.csv", promotable_claims, CLAIM_FIELDS)
    write_csv(out_dir / "llm_claim_review_queue.csv", review_claims, CLAIM_FIELDS)
    write_csv(out_dir / "llm_followup_queue.csv", followup_rows, FOLLOWUP_FIELDS)

    target_rollups = []
    by_target = Counter(row["target_table"] or "(missing)" for row in claim_rows)
    for target_table, count in sorted(by_target.items()):
        target_rows = [row for row in claim_rows if (row["target_table"] or "(missing)") == target_table]
        target_rollups.append({
            "target_table": target_table,
            "claim_count": count,
            "promotable_claim_count": sum(1 for row in target_rows if row["promotion_recommendation"] == "promote"),
            "high_claim_count": sum(1 for row in target_rows if row["confidence_lane"] == "high"),
            "medium_claim_count": sum(1 for row in target_rows if row["confidence_lane"] == "medium"),
            "low_claim_count": sum(1 for row in target_rows if row["confidence_lane"] == "low"),
        })
    write_csv(out_dir / "llm_target_table_rollup.csv", target_rollups, [
        "target_table",
        "claim_count",
        "promotable_claim_count",
        "high_claim_count",
        "medium_claim_count",
        "low_claim_count",
    ])

    if parse_errors:
        write_csv(out_dir / "review_output_parse_errors.csv", parse_errors, [
            "review_output_path",
            "error_type",
            "error",
        ])
    else:
        write_csv(out_dir / "review_output_parse_errors.csv", [], [
            "review_output_path",
            "error_type",
            "error",
        ])

    summary = {
        "created_at_utc": iso_now(),
        "ingest_run_id": ingest_run_id,
        "packet_run_dir": str(packet_run_dir),
        "review_output_dir": str(review_output_dir),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "review_output_files_found": len(review_output_paths(review_output_dir)),
        "parse_error_count": len(parse_errors),
        "document_review_count": len(document_rows),
        "claim_count": len(claim_rows),
        "followup_count": len(followup_rows),
        "promotable_claim_count": len(promotable_claims),
        "claim_review_queue_count": len(review_claims),
        "claim_target_table_counts": dict(Counter(row["target_table"] or "(missing)" for row in claim_rows)),
        "document_disposition_counts": dict(Counter(row["doc_disposition"] or "(missing)" for row in document_rows)),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_json(summary_path, summary)
    (out_dir / "README.md").write_text(
        "\n".join([
            "# Newspaper LLM Review Ingest",
            "",
            "This folder contains flattened ChatGPT/LLM review outputs.",
            "",
            "- `llm_document_reviews.csv`: one row per reviewed source document.",
            "- `llm_review_claims.csv`: one row per LLM-reviewed claim.",
            "- `llm_promotable_claim_queue.csv`: valid claims recommended for promotion review.",
            "- `llm_claim_review_queue.csv`: claims that still need human/system review.",
            "- `llm_followup_queue.csv`: documents needing better OCR, larger crops, identity resolution, or mapping.",
            "",
            "These are separate from raw OCR/parser atoms and from live supertable tables.",
            "",
        ]),
        encoding="utf-8",
    )

    if not args.no_db:
        persist_to_duckdb(
            args.db_path,
            ingest_run_id,
            packet_run_dir,
            review_output_dir,
            out_dir,
            document_rows,
            claim_rows,
            followup_rows,
            summary_path,
        )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
