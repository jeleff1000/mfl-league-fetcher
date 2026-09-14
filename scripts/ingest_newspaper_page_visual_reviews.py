#!/usr/bin/env python
"""Ingest page visual-review decisions into generated newspaper atom decisions.

This closes the page-coverage visual lane:

1. A reviewer fills a visual review decision CSV/JSON for packet items.
2. This station records the decisions in local `newspaper_review` audit tables.
3. Extracted atoms become `newspaper_review.generated_atom_decision` rows.
4. Existing review decision ledger/apply stations can promote them locally.

It is local-only. It writes D-drive artifacts and local newspaper_review tables;
it does not write to Fly, v26, the production supertable, `newspaper_promoted`,
or `newspaper_final`.
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


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_PACKET_ROOT = DEFAULT_ROOT / "page_visual_review_packets"
DEFAULT_REVIEW_OUTPUT_ROOT = DEFAULT_ROOT / "page_visual_review_outputs"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "page_visual_review_ingests"
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"

PROMOTION_VALUE = "approved_for_local_promotion"

DECISION_FIELDS = [
    "decision_ledger_run_id",
    "decision_id",
    "lane",
    "source_prep_run_id",
    "action_queue_run_id",
    "action_id",
    "promotion_package_id",
    "source_document_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "confidence_bar",
    "confidence_score",
    "evidence_document_count",
    "item_count",
    "recommended_next_action",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_boxscore_id",
    "resolved_target_table",
    "resolved_target_entity_key",
    "reason",
    "proposed_fields_json",
    "source_documents_json",
    "artifact_path",
    "notes",
    "created_at_utc",
]

GENERATED_DECISION_FIELDS = ["generated_atom_decision_run_id", *DECISION_FIELDS]

VISUAL_DECISION_FIELDS = [
    "page_visual_review_ingest_run_id",
    "page_visual_review_packet_run_id",
    "visual_review_item_id",
    "source_document_id",
    "boxscore_id",
    "packet_id",
    "decision",
    "confidence_bar",
    "needs_followup_type",
    "atoms_json",
    "notes",
    "atom_count",
    "generated_decision_count",
    "ingest_status",
    "ingest_error",
    "created_at_utc",
]

RUN_FIELDS = [
    "page_visual_review_ingest_run_id",
    "page_visual_review_packet_run_id",
    "generated_atom_decision_run_id",
    "created_at_utc",
    "packet_dir",
    "decision_csv",
    "output_dir",
    "review_decision_count",
    "extract_decision_count",
    "non_extract_decision_count",
    "generated_decision_count",
    "target_table_counts_json",
    "decision_counts_json",
    "summary_json_path",
    "persisted_to_duckdb",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)
    return str(value)


def stable_id(*parts: Any, length: int = 32) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:length]


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def confidence_score(bar: str) -> str:
    value = clean(bar).lower()
    if value == "high":
        return "0.9"
    if value == "medium":
        return "0.7"
    if value == "low":
        return "0.45"
    return "0.7"


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
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True, default=str), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def latest_packet_dir(root: Path) -> Path:
    dirs = [path for path in sorted(root.iterdir()) if path.is_dir() and (path / "summary.json").exists()] if root.exists() else []
    if not dirs:
        raise FileNotFoundError(f"No page visual packet runs found under {root}")
    return dirs[-1]


def latest_decision_csv(review_root: Path, packet_run_id: str) -> Path:
    preferred = review_root / packet_run_id / "visual_review_decision_template.csv"
    if preferred.exists():
        return preferred
    candidates = sorted(review_root.glob("*/visual_review_decision_template.csv"))
    if not candidates:
        raise FileNotFoundError(f"No visual_review_decision_template.csv found under {review_root}")
    return candidates[-1]


def load_packet_items(packet_dir: Path) -> dict[str, dict[str, str]]:
    path = packet_dir / "packet_items.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing packet_items.csv: {path}")
    return {clean(row.get("visual_review_item_id")): row for row in read_csv(path) if clean(row.get("visual_review_item_id"))}


def normalize_atom_fields(
    atom: dict[str, Any],
    decision_row: dict[str, Any],
    item: dict[str, Any],
    atom_index: int,
) -> tuple[str, str, dict[str, Any], str, str]:
    target_table = clean(atom.get("target_table"))
    fields = parse_json_obj(atom.get("fields"))
    for key, value in atom.items():
        if key in {"target_table", "target_entity_key", "fields", "confidence_bar", "confidence_score", "notes"}:
            continue
        fields.setdefault(key, value)
    source_document_id = clean(decision_row.get("source_document_id")) or clean(item.get("source_document_id"))
    boxscore_id = clean(fields.get("boxscore_id")) or clean(decision_row.get("boxscore_id")) or clean(item.get("boxscore_id"))
    fields["boxscore_id"] = boxscore_id
    fields.setdefault("source_document_id", source_document_id)
    fields.setdefault("region_id", f"visual_review:{clean(decision_row.get('visual_review_item_id'))}")
    fields.setdefault("review_status", "visual_review_extracted")
    fields.setdefault("promotion_status", "not_promoted")
    fields.setdefault("confidence_score", clean(atom.get("confidence_score")) or confidence_score(clean(atom.get("confidence_bar")) or clean(decision_row.get("confidence_bar"))))

    evidence_text = (
        clean(fields.get("evidence_text"))
        or clean(fields.get("play_text"))
        or clean(fields.get("source_row_text"))
        or clean(atom.get("evidence_text"))
    )
    if target_table == "source_document_note":
        fields.setdefault("note_category", "visual_review")
        fields.setdefault("created_by_station", "page_visual_review_ingest")
        fields.setdefault("source_documents_json", json.dumps([source_document_id], ensure_ascii=False))
        fields.setdefault("evidence_text", evidence_text)
    elif target_table == "game_candidate":
        fields.setdefault("evidence_text", evidence_text)
    elif target_table == "scoring_event":
        fields.setdefault("play_text", evidence_text)
    elif target_table == "play_by_play_event":
        fields.setdefault("play_text", evidence_text)
    elif target_table in {"player_game_box_score", "lineup_participation", "player_game_stat_claim", "team_game_stat_claim"}:
        fields.setdefault("source_row_text", evidence_text)
        fields.setdefault("evidence_text", evidence_text)

    target_key = clean(atom.get("target_entity_key"))
    if not target_key:
        parts = [
            target_table,
            boxscore_id,
            clean(fields.get("player_week")) or clean(fields.get("NFL_player_id")) or clean(fields.get("player_raw")) or clean(fields.get("scoring_player_raw")) or clean(fields.get("primary_player_raw")),
            clean(fields.get("stat_name")) or clean(fields.get("event_type")) or clean(fields.get("play_type")) or clean(fields.get("note_type")) or str(atom_index),
            source_document_id,
        ]
        target_key = "|".join(part for part in parts if part)
    bar = clean(atom.get("confidence_bar")) or clean(decision_row.get("confidence_bar")) or "medium"
    score = clean(atom.get("confidence_score")) or confidence_score(bar)
    return target_table, target_key, fields, bar, score


def build_generated_rows(
    run_id: str,
    packet_run_id: str,
    decision_rows: list[dict[str, str]],
    packet_items: dict[str, dict[str, str]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    for decision in decision_rows:
        visual_item_id = clean(decision.get("visual_review_item_id"))
        if not visual_item_id:
            continue
        item = packet_items.get(visual_item_id, {})
        source_document_id = clean(decision.get("source_document_id")) or clean(item.get("source_document_id"))
        boxscore_id = clean(decision.get("boxscore_id")) or clean(item.get("boxscore_id"))
        decision_value = clean(decision.get("decision")).lower()
        atoms = parse_json_list(decision.get("atoms_json"))
        ingest_status = "recorded"
        ingest_error = ""
        if decision_value == "extract_atoms" and not atoms:
            ingest_status = "error"
            ingest_error = "extract_atoms_without_atoms_json"
        generated_for_decision = 0
        if decision_value == "extract_atoms" and atoms:
            for atom_index, atom in enumerate(atoms, start=1):
                if not isinstance(atom, dict):
                    ingest_status = "error"
                    ingest_error = "atoms_json_contains_non_object"
                    continue
                target_table, target_key, fields, bar, score = normalize_atom_fields(atom, decision, item, atom_index)
                if not target_table:
                    ingest_status = "error"
                    ingest_error = "atom_missing_target_table"
                    continue
                generated_id = stable_id("page_visual_generated", run_id, visual_item_id, target_table, target_key, atom_index)
                source_docs = json.dumps([source_document_id], ensure_ascii=False)
                generated_rows.append({
                    "generated_atom_decision_run_id": run_id,
                    "decision_ledger_run_id": "",
                    "decision_id": generated_id,
                    "lane": "page_visual_review",
                    "source_prep_run_id": packet_run_id,
                    "action_queue_run_id": "",
                    "action_id": visual_item_id,
                    "promotion_package_id": "",
                    "source_document_id": source_document_id,
                    "target_table": target_table,
                    "target_entity_key": target_key,
                    "boxscore_id": boxscore_id,
                    "confidence_bar": bar,
                    "confidence_score": score,
                    "evidence_document_count": "1",
                    "item_count": "1",
                    "recommended_next_action": "review_generated_visual_atom",
                    "decision_status": PROMOTION_VALUE,
                    "decision_value": PROMOTION_VALUE,
                    "route_to_lane": "",
                    "resolved_boxscore_id": boxscore_id,
                    "resolved_target_table": target_table,
                    "resolved_target_entity_key": target_key,
                    "reason": clean(atom.get("notes")) or clean(decision.get("notes")) or "page visual review extracted atom",
                    "proposed_fields_json": json.dumps(fields, ensure_ascii=False, sort_keys=True),
                    "source_documents_json": source_docs,
                    "artifact_path": clean(item.get("rendered_image_path")) or clean(item.get("pdf_path")),
                    "notes": clean(decision.get("notes")),
                    "created_at_utc": created_at,
                })
                generated_for_decision += 1
        audit_rows.append({
            "page_visual_review_ingest_run_id": run_id,
            "page_visual_review_packet_run_id": packet_run_id,
            "visual_review_item_id": visual_item_id,
            "source_document_id": source_document_id,
            "boxscore_id": boxscore_id,
            "packet_id": clean(decision.get("packet_id")) or clean(item.get("packet_id")),
            "decision": decision_value,
            "confidence_bar": clean(decision.get("confidence_bar")),
            "needs_followup_type": clean(decision.get("needs_followup_type")),
            "atoms_json": clean(decision.get("atoms_json")),
            "notes": clean(decision.get("notes")),
            "atom_count": len(atoms),
            "generated_decision_count": generated_for_decision,
            "ingest_status": ingest_status,
            "ingest_error": ingest_error,
            "created_at_utc": created_at,
        })
    return generated_rows, audit_rows


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ",".join(["?"] * len(fields))
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({','.join(fields)}) VALUES ({placeholders})",
        [[safe_cell(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    run_row: dict[str, Any],
    generated_rows: list[dict[str, Any]],
    audit_rows: list[dict[str, Any]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.generated_atom_decision ("
            + ", ".join(f"{field} VARCHAR" for field in GENERATED_DECISION_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.generated_atom_decision_run ("
            + ", ".join(f"{field} VARCHAR" for field in [
                "generated_atom_decision_run_id",
                "promotion_apply_run_id",
                "output_dir",
                "source_document_count",
                "generated_decision_count",
                "semantic_note_override_count",
                "status",
                "created_at_utc",
                "summary_json_path",
            ])
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.page_visual_review_ingest_run ("
            + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.page_visual_review_decision ("
            + ", ".join(f"{field} VARCHAR" for field in VISUAL_DECISION_FIELDS)
            + ")"
        )
        for field in GENERATED_DECISION_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.generated_atom_decision ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in RUN_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.page_visual_review_ingest_run ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in VISUAL_DECISION_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.page_visual_review_decision ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        run_id = clean(run_row.get("page_visual_review_ingest_run_id"))
        con.execute("DELETE FROM newspaper_review.generated_atom_decision WHERE generated_atom_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.generated_atom_decision_run WHERE generated_atom_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.page_visual_review_ingest_run WHERE page_visual_review_ingest_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.page_visual_review_decision WHERE page_visual_review_ingest_run_id = ?", [run_id])
        insert_rows(con, "generated_atom_decision", generated_rows, GENERATED_DECISION_FIELDS)
        insert_rows(con, "generated_atom_decision_run", [{
            "generated_atom_decision_run_id": run_id,
            "promotion_apply_run_id": "",
            "output_dir": clean(run_row.get("output_dir")),
            "source_document_count": len({clean(row.get("source_document_id")) for row in generated_rows if clean(row.get("source_document_id"))}),
            "generated_decision_count": len(generated_rows),
            "semantic_note_override_count": 0,
            "status": "complete",
            "created_at_utc": clean(run_row.get("created_at_utc")),
            "summary_json_path": clean(run_row.get("summary_json_path")),
        }], [
            "generated_atom_decision_run_id",
            "promotion_apply_run_id",
            "output_dir",
            "source_document_count",
            "generated_decision_count",
            "semantic_note_override_count",
            "status",
            "created_at_utc",
            "summary_json_path",
        ])
        insert_rows(con, "page_visual_review_ingest_run", [run_row], RUN_FIELDS)
        insert_rows(con, "page_visual_review_decision", audit_rows, VISUAL_DECISION_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet-dir", type=Path, default=None)
    parser.add_argument("--packet-root", type=Path, default=DEFAULT_PACKET_ROOT)
    parser.add_argument("--decision-csv", type=Path, default=None)
    parser.add_argument("--review-output-root", type=Path, default=DEFAULT_REVIEW_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="page_visual_review_ingest")
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    packet_dir = args.packet_dir or latest_packet_dir(args.packet_root)
    packet_summary = read_json(packet_dir / "summary.json")
    packet_run_id = clean(packet_summary.get("page_visual_review_packet_run_id")) or packet_dir.name
    decision_csv = args.decision_csv or latest_decision_csv(args.review_output_root, packet_run_id)
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    packet_items = load_packet_items(packet_dir)
    all_decision_rows = read_csv(decision_csv)
    decision_rows = [row for row in all_decision_rows if clean(row.get("decision"))]
    generated_rows, audit_rows = build_generated_rows(run_id, packet_run_id, decision_rows, packet_items, created_at)

    target_counts = Counter(clean(row.get("target_table")) for row in generated_rows)
    decision_counts = Counter(clean(row.get("decision")) for row in audit_rows)
    summary_path = out_dir / "summary.json"
    run_row = {
        "page_visual_review_ingest_run_id": run_id,
        "page_visual_review_packet_run_id": packet_run_id,
        "generated_atom_decision_run_id": run_id,
        "created_at_utc": created_at,
        "packet_dir": str(packet_dir),
        "decision_csv": str(decision_csv),
        "output_dir": str(out_dir),
        "review_decision_count": len(audit_rows),
        "extract_decision_count": decision_counts["extract_atoms"],
        "non_extract_decision_count": len(audit_rows) - decision_counts["extract_atoms"],
        "generated_decision_count": len(generated_rows),
        "target_table_counts_json": json.dumps(dict(sorted(target_counts.items())), sort_keys=True),
        "decision_counts_json": json.dumps(dict(sorted(decision_counts.items())), sort_keys=True),
        "summary_json_path": str(summary_path),
        "persisted_to_duckdb": not args.no_persist,
    }
    summary = {
        **run_row,
        "target_table_counts": dict(sorted(target_counts.items())),
        "decision_counts": dict(sorted(decision_counts.items())),
        "ingest_status_counts": dict(sorted(Counter(clean(row.get("ingest_status")) for row in audit_rows).items())),
        "live_tables_touched": False,
    }
    write_csv(out_dir / "visual_review_decisions_ingested.csv", audit_rows, VISUAL_DECISION_FIELDS)
    write_csv(out_dir / "generated_atom_decisions.csv", generated_rows, GENERATED_DECISION_FIELDS)
    write_json(summary_path, summary)
    report = [
        "# Newspaper Page Visual Review Ingest",
        "",
        f"Created: `{created_at}`",
        f"Run: `{run_id}`",
        f"Packet run: `{packet_run_id}`",
        f"Decision CSV: `{decision_csv}`",
        "",
        "## Counts",
        "",
        f"- Review decisions read: `{len(audit_rows)}`",
        f"- Extract decisions: `{decision_counts['extract_atoms']}`",
        f"- Generated atom decisions: `{len(generated_rows)}`",
        f"- Target tables: `{dict(sorted(target_counts.items()))}`",
        "",
        "Live Fly/v26/supertable writes: no.",
        "",
    ]
    (out_dir / "page_visual_review_ingest_report.md").write_text("\n".join(report), encoding="utf-8")
    if not args.no_persist:
        persist(args.db_path, run_row, generated_rows, audit_rows)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
