#!/usr/bin/env python
"""Build LLM review packets from newspaper document review queues.

The OCR/article conveyor is intentionally mechanical: render, OCR, crop, make
candidate atoms, and keep moving. This station prepares the material that needs
language-model judgment without losing conveyor discipline. It does not write to
live tables; it writes auditable packet artifacts on the D drive by default.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_packets")
DEFAULT_REVIEW_OUTPUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_review_outputs")
DEFAULT_ACTIONS = (
    "review_event_atoms",
    "review_promotion_candidates",
    "review_score_lineup_marker_atoms",
)


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


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_actions(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def truncate_text(text: str, max_chars: int) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + f"\n\n[TRUNCATED at {max_chars} of {len(text)} chars]"


def read_text_excerpt(path_value: str, max_chars: int) -> tuple[str, int, str]:
    if not path_value:
        return "", 0, "missing_path"
    path = Path(path_value)
    if not path.exists():
        return "", 0, "file_not_found"
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:  # pragma: no cover - defensive for odd OCR files
        return "", 0, f"{type(exc).__name__}: {exc}"
    return truncate_text(text, max_chars), len(text), "loaded"


def load_review_documents(queue_csvs: list[Path], actions: set[str], max_docs: int | None) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    seen: set[str] = set()
    for queue_csv in queue_csvs:
        for row in read_csv(queue_csv):
            source_document_id = row.get("source_document_id") or row.get("candidate_id") or ""
            if not source_document_id or source_document_id in seen:
                continue
            if row.get("document_next_action") not in actions:
                continue
            selected.append({**row, "source_document_id": source_document_id, "source_queue_csv": str(queue_csv)})
            seen.add(source_document_id)
            if max_docs is not None and len(selected) >= max_docs:
                return selected
    return selected


def query_rows(con: duckdb.DuckDBPyConnection, sql: str, source_ids: list[str]) -> list[dict[str, Any]]:
    if not source_ids:
        return []
    placeholders = ",".join(["?"] * len(source_ids))
    result = con.execute(sql.format(source_ids=placeholders), source_ids)
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def load_db_context(db_path: Path, source_ids: list[str]) -> dict[str, Any]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        documents = query_rows(
            con,
            """
            SELECT
              source_document_id,
              source_candidate_id,
              boxscore_id,
              source_archive,
              image_id,
              publication,
              publication_city,
              issue_date,
              page,
              source_url,
              capture_type,
              asset_pdf_path,
              asset_image_path,
              visual_qa_status
            FROM newspaper_review.source_document
            WHERE source_document_id IN ({source_ids})
            """,
            source_ids,
        )
        regions = query_rows(
            con,
            """
            SELECT
              region_id,
              source_document_id,
              run_id,
              parent_text_pass_id,
              page_number,
              region_type,
              region_label,
              anchor_text,
              crop_image_path,
              region_text_path,
              region_quality_score,
              status,
              next_action,
              reason_code
            FROM newspaper_review.source_region
            WHERE source_document_id IN ({source_ids})
            ORDER BY source_document_id, page_number, region_label, region_id
            """,
            source_ids,
        )
        atoms = query_rows(
            con,
            """
            SELECT
              atom_claim_id,
              source_document_id,
              region_id,
              run_id,
              boxscore_id,
              atom_type,
              atom_family,
              semantic_target_table,
              semantic_target_field,
              entity_scope,
              entity_text,
              raw_value,
              normalized_value,
              numeric_value,
              unit,
              evidence_text,
              evidence_text_path,
              evidence_image_path,
              extraction_method,
              extractor_version,
              confidence_score,
              confidence_lane,
              value_lane,
              review_status,
              promotion_status,
              reason_code,
              notes
            FROM newspaper_review.atom_claim
            WHERE source_document_id IN ({source_ids})
            ORDER BY source_document_id, confidence_score DESC, semantic_target_table, atom_type, atom_claim_id
            """,
            source_ids,
        )
        promotion_candidates = query_rows(
            con,
            """
            SELECT
              a.source_document_id,
              p.promotion_candidate_id,
              p.promotion_package_id,
              p.atom_claim_id,
              p.source_domain_table,
              p.target_table,
              p.target_row_key,
              p.target_field,
              p.existing_value,
              p.proposed_value,
              p.confidence_score,
              p.review_status,
              p.promotion_status,
              p.promotion_notes
            FROM newspaper_review.promotion_candidate p
            JOIN newspaper_review.atom_claim a ON a.atom_claim_id = p.atom_claim_id
            WHERE a.source_document_id IN ({source_ids})
            ORDER BY a.source_document_id, p.confidence_score DESC, p.promotion_candidate_id
            """,
            source_ids,
        )
        scoring_events = query_rows(
            con,
            """
            SELECT
              source_document_id,
              atom_claim_id,
              boxscore_id,
              event_order,
              period_raw,
              clock_raw,
              scoring_team_raw,
              scoring_player_raw,
              passer_raw,
              receiver_raw,
              event_type,
              points,
              distance_yards,
              play_text,
              confidence_score,
              review_status,
              promotion_status,
              region_id
            FROM newspaper_review.scoring_event
            WHERE source_document_id IN ({source_ids})
            ORDER BY source_document_id, event_order, atom_claim_id
            """,
            source_ids,
        )
        pbp_events = query_rows(
            con,
            """
            SELECT
              source_document_id,
              atom_claim_id,
              boxscore_id,
              event_order,
              period_raw,
              clock_raw,
              possession_team_raw,
              down_raw,
              distance_raw,
              yardline_raw,
              play_type,
              primary_player_raw,
              secondary_player_raw,
              yards,
              points,
              play_text,
              confidence_score,
              review_status,
              promotion_status,
              region_id
            FROM newspaper_review.play_by_play_event
            WHERE source_document_id IN ({source_ids})
            ORDER BY source_document_id, event_order, atom_claim_id
            """,
            source_ids,
        )
        box_scores = query_rows(
            con,
            """
            SELECT
              source_document_id,
              atom_claim_id,
              boxscore_id,
              player_week,
              player_raw,
              nfl_team,
              opponent_nfl_team,
              position,
              starter_position,
              is_starter,
              carries,
              rushing_yards,
              rushing_tds,
              attempts,
              completions,
              passing_yards,
              passing_tds,
              passing_interceptions,
              receptions,
              receiving_yards,
              receiving_tds,
              pat_made,
              pat_att,
              fg_made,
              fg_att,
              fg_long,
              def_interceptions,
              def_sacks,
              def_tds,
              special_teams_tds,
              source_row_text,
              confidence_score,
              review_status,
              promotion_status,
              region_id
            FROM newspaper_review.player_game_box_score
            WHERE source_document_id IN ({source_ids})
            ORDER BY source_document_id, player_raw, atom_claim_id
            """,
            source_ids,
        )
        lineups = query_rows(
            con,
            """
            SELECT
              source_document_id,
              atom_claim_id,
              boxscore_id,
              player_week,
              player_raw,
              team_raw,
              nfl_team,
              opponent_nfl_team,
              listed_position_raw,
              starter_position,
              is_starter,
              participation_type,
              lineup_side_raw,
              source_row_text,
              confidence_score,
              review_status,
              promotion_status,
              region_id
            FROM newspaper_review.lineup_participation
            WHERE source_document_id IN ({source_ids})
            ORDER BY source_document_id, team_raw, player_raw, atom_claim_id
            """,
            source_ids,
        )
    finally:
        con.close()

    by_doc: dict[str, dict[str, Any]] = {}
    for doc in documents:
        by_doc[clean(doc["source_document_id"])] = {"source_document": doc}
    for table_name, rows in [
        ("source_regions", regions),
        ("atom_claims", atoms),
        ("promotion_candidates", promotion_candidates),
        ("scoring_events", scoring_events),
        ("play_by_play_events", pbp_events),
        ("player_game_box_scores", box_scores),
        ("lineup_participation", lineups),
    ]:
        for row in rows:
            source_document_id = clean(row["source_document_id"])
            by_doc.setdefault(source_document_id, {}).setdefault(table_name, []).append(row)
    return by_doc


def review_contract() -> dict[str, Any]:
    return {
        "station": "llm_review",
        "goal": "Extract newspaper-supported football atoms into separate review tables without promoting to the live supertable.",
        "rules": [
            "Only emit facts directly supported by the supplied OCR/region text.",
            "Keep duplicates as corroborating evidence when they support the same fact from different documents or regions.",
            "Prefer exact raw text plus a normalized value; leave normalized_value empty when unsure.",
            "For row-shaped facts such as player box scores, scoring events, or lineup rows, put the row identity in row_group_key and put every extracted column in target_fields.",
            "Use field_evidence when different columns in the same row come from different phrases or OCR regions.",
            "Use high confidence only when the evidence is explicit and the target field/table is clear.",
            "Use medium confidence when the fact is likely but OCR, team mapping, player identity, or stat category needs review.",
            "Use low confidence for weak evidence; low-confidence items should not become promotion candidates.",
            "Never write to live tables from this station.",
        ],
        "target_tables": [
            "game_candidate",
            "scoring_event",
            "play_by_play_event",
            "player_game_box_score",
            "lineup_participation",
            "player_identity_candidate",
            "promotion_candidate",
        ],
        "expected_output_shape": {
            "source_document_id": "string",
            "doc_disposition": "promotable|review_needed|needs_more_ocr|low_signal|no_useful_atoms",
            "reviewed_claims": [
                {
                    "target_table": "string",
                    "target_field": "string for single-field claims; empty when target_fields is used",
                    "target_fields": {
                        "column_name": "normalized value for row-shaped extraction"
                    },
                    "row_group_key": "stable row identity such as player name + team + stat line or scoring event order",
                    "entity_text": "string",
                    "raw_value": "string",
                    "normalized_value": "string",
                    "numeric_value": "number|null",
                    "unit": "string",
                    "evidence_region_id": "string",
                    "evidence_quote": "short direct quote from packet text",
                    "field_evidence": {
                        "column_name": {
                            "evidence_region_id": "string",
                            "evidence_quote": "short direct quote",
                            "confidence_score": "integer 0-100"
                        }
                    },
                    "confidence_score": "integer 0-100",
                    "confidence_lane": "high|medium|low",
                    "promotion_recommendation": "promote|review|reject",
                    "reason": "string",
                }
            ],
            "needs_followup": [
                {
                    "followup_type": "better_ocr|larger_crop|identity_resolution|team_mapping|boxscore_reconciliation",
                    "reason": "string",
                }
            ],
        },
    }


def enrich_atom(atom: dict[str, Any], max_evidence_chars: int) -> dict[str, Any]:
    row = {key: atom.get(key) for key in [
        "atom_claim_id",
        "region_id",
        "run_id",
        "boxscore_id",
        "atom_type",
        "atom_family",
        "semantic_target_table",
        "semantic_target_field",
        "entity_scope",
        "entity_text",
        "raw_value",
        "normalized_value",
        "numeric_value",
        "unit",
        "extraction_method",
        "extractor_version",
        "confidence_score",
        "confidence_lane",
        "value_lane",
        "review_status",
        "promotion_status",
        "reason_code",
        "notes",
    ]}
    row["evidence_text"] = truncate_text(clean(atom.get("evidence_text")), max_evidence_chars)
    row["evidence_text_path"] = clean(atom.get("evidence_text_path"))
    row["evidence_image_path"] = clean(atom.get("evidence_image_path"))
    return row


def build_doc_packet(
    queue_row: dict[str, str],
    db_doc: dict[str, Any],
    max_region_chars: int,
    max_evidence_chars: int,
) -> dict[str, Any]:
    regions = []
    for region in db_doc.get("source_regions", []):
        text, total_chars, text_status = read_text_excerpt(clean(region.get("region_text_path")), max_region_chars)
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
            "region_text_status": text_status,
            "region_quality_score": region.get("region_quality_score"),
            "status": region.get("status"),
            "next_action": region.get("next_action"),
            "reason_code": region.get("reason_code"),
            "text_excerpt": text,
        })

    source_document = db_doc.get("source_document") or {}
    existing_atoms = [enrich_atom(atom, max_evidence_chars) for atom in db_doc.get("atom_claims", [])]
    return {
        "source_document_id": queue_row["source_document_id"],
        "document_next_action": queue_row.get("document_next_action"),
        "document_reason_code": queue_row.get("document_reason_code"),
        "document_work_lane": queue_row.get("document_work_lane"),
        "queue_context": {
            "source_queue_csv": queue_row.get("source_queue_csv"),
            "round_name": queue_row.get("round_name"),
            "candidate_checkpoint": queue_row.get("candidate_checkpoint"),
            "doc_atom_count": queue_row.get("doc_atom_count"),
            "doc_player_stat_atoms": queue_row.get("doc_player_stat_atoms"),
            "doc_scoring_event_atoms": queue_row.get("doc_scoring_event_atoms"),
            "doc_pbp_event_atoms": queue_row.get("doc_pbp_event_atoms"),
            "doc_lineup_atoms": queue_row.get("doc_lineup_atoms"),
            "doc_promotion_candidates": queue_row.get("doc_promotion_candidates"),
        },
        "source_document": source_document,
        "candidate_metadata": {
            "boxscore_id": queue_row.get("boxscore_id") or source_document.get("boxscore_id"),
            "game_date": queue_row.get("game_date"),
            "year": queue_row.get("year"),
            "week": queue_row.get("week"),
            "team_1": queue_row.get("team_1"),
            "team_2": queue_row.get("team_2"),
            "publication": queue_row.get("publication") or source_document.get("publication"),
            "issue_date": queue_row.get("issue_date") or source_document.get("issue_date"),
            "page": queue_row.get("page") or source_document.get("page"),
            "source_url": queue_row.get("source_url") or source_document.get("source_url"),
            "asset_pdf_path": queue_row.get("pdf_path") or source_document.get("asset_pdf_path"),
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


def packetize(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[index:index + size] for index in range(0, len(rows), size)]


def packet_markdown(packet_id: str, docs: list[dict[str, Any]], contract: dict[str, Any]) -> str:
    lines = [
        f"# Newspaper LLM Review Packet {packet_id}",
        "",
        "## Contract",
        "",
        f"- Station: `{contract['station']}`",
        f"- Goal: {contract['goal']}",
        "- Rules:",
    ]
    lines.extend(f"  - {rule}" for rule in contract["rules"])
    lines.extend([
        "",
        "## Documents",
        "",
    ])
    for doc in docs:
        meta = doc["candidate_metadata"]
        lines.extend([
            f"### {doc['source_document_id']}",
            "",
            f"- Action: `{doc.get('document_next_action')}`",
            f"- Reason: `{doc.get('document_reason_code')}`",
            f"- Game: `{meta.get('boxscore_id')}` {meta.get('team_1') or ''} vs {meta.get('team_2') or ''} ({meta.get('game_date') or meta.get('issue_date') or ''})",
            f"- Source: {meta.get('publication') or ''}, page {meta.get('page') or ''}",
            f"- PDF: `{meta.get('asset_pdf_path') or ''}`",
            "",
            "#### Existing Atoms",
            "",
        ])
        atoms = doc.get("existing_atom_claims", [])
        if not atoms:
            lines.append("_No existing atoms._")
        else:
            for atom in atoms[:30]:
                lines.append(
                    f"- `{atom.get('semantic_target_table')}` / `{atom.get('atom_type')}` "
                    f"confidence `{atom.get('confidence_score')}`: "
                    f"{atom.get('entity_text') or ''} {atom.get('raw_value') or ''}"
                )
                evidence = atom.get("evidence_text") or ""
                if evidence:
                    lines.append(f"  - Evidence: {evidence}")
        lines.extend(["", "#### Region Text", ""])
        for region in doc.get("source_regions", []):
            lines.extend([
                f"##### {region.get('region_id')}",
                "",
                f"- Label: `{region.get('region_label')}`",
                f"- Text status: `{region.get('region_text_status')}` chars `{region.get('region_text_chars')}`",
                f"- Crop: `{region.get('crop_image_path') or ''}`",
                "",
                "```text",
                region.get("text_excerpt") or "",
                "```",
                "",
            ])
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-csv", type=Path, action="append", required=True, help="document_next_action_queue.csv path. Repeatable.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="llm_review_packets")
    parser.add_argument("--actions", default=",".join(DEFAULT_ACTIONS))
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--packet-docs", type=int, default=5)
    parser.add_argument("--max-region-chars", type=int, default=6000)
    parser.add_argument("--max-evidence-chars", type=int, default=900)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    actions = parse_actions(args.actions)
    queue_rows = load_review_documents(args.queue_csv, actions, args.max_docs)
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    source_ids = [row["source_document_id"] for row in queue_rows]
    db_context = load_db_context(args.db_path, source_ids) if source_ids else {}
    contract = review_contract()

    doc_packets = [
        build_doc_packet(
            row,
            db_context.get(row["source_document_id"], {}),
            max_region_chars=args.max_region_chars,
            max_evidence_chars=args.max_evidence_chars,
        )
        for row in queue_rows
    ]

    packets = packetize(doc_packets, args.packet_docs)
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
            document_rows.append({
                "packet_id": packet_id,
                "source_document_id": doc["source_document_id"],
                "document_next_action": doc.get("document_next_action"),
                "boxscore_id": doc["candidate_metadata"].get("boxscore_id"),
                "publication": doc["candidate_metadata"].get("publication"),
                "issue_date": doc["candidate_metadata"].get("issue_date"),
                "page": doc["candidate_metadata"].get("page"),
                "region_count": len(doc.get("source_regions", [])),
                "atom_claim_count": len(doc.get("existing_atom_claims", [])),
                "promotion_candidate_count": len(doc.get("domain_rows", {}).get("promotion_candidates", [])),
            })

    write_csv(out_dir / "llm_review_packet_manifest.csv", packet_rows, [
        "packet_id",
        "packet_json_path",
        "packet_jsonl_path",
        "packet_md_path",
        "document_count",
        "document_actions_json",
    ])
    write_csv(out_dir / "document_packet_manifest.csv", document_rows, [
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
    ])
    write_json(out_dir / "empty_review_output_template.json", {
        "packet_id": "",
        "reviewer": "llm",
        "reviewed_at_utc": iso_now(),
        "document_reviews": [contract["expected_output_shape"]],
    })
    summary = {
        "created_at_utc": iso_now(),
        "run_id": run_id,
        "label": args.label,
        "db_path": str(args.db_path),
        "queue_csvs": [str(path) for path in args.queue_csv],
        "actions": sorted(actions),
        "documents_selected": len(queue_rows),
        "packets_created": len(packet_rows),
        "document_action_counts": dict(Counter(row.get("document_next_action") for row in queue_rows)),
        "packet_docs": args.packet_docs,
        "max_region_chars": args.max_region_chars,
        "max_evidence_chars": args.max_evidence_chars,
        "output_dir": str(out_dir),
    }
    write_json(out_dir / "summary.json", summary)
    review_output_dir = DEFAULT_REVIEW_OUTPUT_ROOT / run_id
    (out_dir / "chatgpt_packet_review_prompt.md").write_text(
        "\n".join([
            "# ChatGPT Newspaper Packet Review Prompt",
            "",
            "Read one packet at a time from this packet run and return JSON only.",
            "",
            f"Packet run: `{out_dir}`",
            f"Review-output folder: `{review_output_dir}`",
            f"Output template: `{out_dir / 'empty_review_output_template.json'}`",
            f"Packet manifest: `{out_dir / 'llm_review_packet_manifest.csv'}`",
            "",
            "For each document in the packet:",
            "- Extract only facts directly supported by the supplied OCR/region text.",
            "- Use `target_fields` for row-shaped records such as player box scores, scoring events, play-by-play events, and lineup rows.",
            "- Use `target_field` only for a single-field claim.",
            "- Fill `row_group_key` with a stable identity for the row, such as player/team/stat-line text or scoring-event order.",
            "- Fill `field_evidence` when individual fields in a row need their own evidence quote or confidence.",
            "- Mark high confidence only when the evidence and table mapping are explicit.",
            "- Mark medium confidence when OCR, player identity, team mapping, or stat category needs review.",
            "- Mark low confidence for weak evidence and set `promotion_recommendation` to `reject` or `review`.",
            "- Do not invent missing names, teams, values, or dates.",
            "",
            "Save the response as one JSON file per packet in the review-output folder, for example:",
            f"`{review_output_dir / 'packet_0001_review.json'}`",
            "",
            "After saving review JSON, rerun the read-state and ingest stations for this packet run.",
            "",
        ]),
        encoding="utf-8",
    )
    (out_dir / "README.md").write_text(
        "\n".join([
            "# Newspaper LLM Review Packets",
            "",
            "This folder contains review packets built from OCR/article conveyor outputs.",
            "",
            "Use `packets_md/` for human-readable LLM review and `packets_json/` or `packets_jsonl/` for structured ingestion.",
            "The reviewer should return JSON matching `empty_review_output_template.json`.",
            "Use `chatgpt_packet_review_prompt.md` as the handoff prompt for ChatGPT packet review.",
            "",
            "No live supertable writes happen here. Reviewed claims remain separate until a promotion step accepts them.",
            "",
        ]),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
