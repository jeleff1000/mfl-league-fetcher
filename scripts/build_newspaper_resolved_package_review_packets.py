#!/usr/bin/env python
"""
Build human-readable review packets from resolved newspaper promotion packages.

This is a local conveyor station only. It does not promote to v26 or write
production tables; it chunks the latest resolved package into auditable review
packets and records packet metadata in the local newspaper DuckDB.
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
DEFAULT_PACKAGE_ROOT = DEFAULT_ROOT / "resolved_promotion_packages"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_package_review_packets"
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"

PACKET_FIELDS = [
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "packet_id",
    "packet_index",
    "packet_md_path",
    "packet_csv_path",
    "package_lane",
    "target_table",
    "review_status",
    "risk_level",
    "item_count",
    "recommended_action",
    "created_at_utc",
]

ITEM_FIELDS = [
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "packet_id",
    "packet_index",
    "packet_item_index",
    "package_item_id",
    "package_lane",
    "target_table",
    "review_status",
    "risk_level",
    "target_entity_key",
    "boxscore_id",
    "game_date",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
    "confidence_bar",
    "source_documents_json",
    "evidence_text",
    "proposed_summary",
    "reason",
    "decision_prompt",
    "packet_md_path",
    "created_at_utc",
]

RUN_FIELDS = [
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "created_at_utc",
    "package_dir",
    "output_dir",
    "packet_count",
    "item_count",
    "max_items_per_packet",
    "lane_counts_json",
    "status_counts_json",
    "target_table_counts_json",
    "manifest_csv",
    "items_csv",
    "summary_json",
    "persisted_to_duckdb",
]

FIELD_PRIORITY = {
    "game_candidate": [
        "year",
        "week",
        "game_date",
        "boxscore_id",
        "season_type",
        "team_1_resolved",
        "team_1_raw",
        "team_1_score",
        "team_2_resolved",
        "team_2_raw",
        "team_2_score",
        "reconciliation_status",
        "v26_score_status",
    ],
    "lineup_participation": [
        "player_raw",
        "resolved_player",
        "NFL_player_id",
        "player_week",
        "nfl_team",
        "opponent_nfl_team",
        "participation_type",
        "starter_position",
        "listed_position_raw",
        "is_starter",
        "source_row_text",
    ],
    "scoring_event": [
        "scoring_player_raw",
        "scoring_NFL_player_id",
        "event_type",
        "points",
        "scoring_team",
        "passer_raw",
        "passer_NFL_player_id",
        "receiver_raw",
        "receiver_NFL_player_id",
        "distance_yards",
        "play_text",
    ],
    "play_by_play_event": [
        "primary_player_raw",
        "primary_NFL_player_id",
        "secondary_player_raw",
        "secondary_NFL_player_id",
        "play_type",
        "possession_team",
        "period_raw",
        "clock_raw",
        "down_raw",
        "distance_raw",
        "yardline_raw",
        "yards",
        "points",
        "play_text",
    ],
    "player_game_box_score": [
        "player_raw",
        "NFL_player_id",
        "player_week",
        "nfl_team",
        "opponent_nfl_team",
        "passing_yards",
        "passing_tds",
        "rushing_yards",
        "rushing_tds",
        "receiving_yards",
        "receiving_tds",
        "receptions",
        "touchdowns",
        "pat_made",
        "pat_att",
        "fg_made",
        "fg_att",
        "fg_long",
        "schema_resolution_status",
        "schema_patch_reason",
        "source_row_text",
    ],
    "team_game_stat_claim": [
        "stat_name",
        "stat_unit",
        "team_1_nfl_team",
        "team_1_raw",
        "team_1_value",
        "team_2_nfl_team",
        "team_2_raw",
        "team_2_value",
        "claimed_score_text",
        "source_row_text",
        "stat_context",
    ],
    "player_identity_candidate": [
        "raw_player_name",
        "resolved_player",
        "NFL_player_id",
        "player_week",
        "raw_team",
        "nfl_team",
        "opponent_nfl_team",
        "match_method",
        "evidence_text",
    ],
}

PROMPT_BY_LANE = {
    "ready_resolved_atom_review": "Approve only if the extracted fields match the evidence and target entity; otherwise reject or route to follow-up.",
    "score_backfill_review": "Review as a local score backfill candidate where v26 has the game but a blank score.",
    "touchdown_total_type_review": "Confirm the total touchdown is supported; leave touchdown type split flagged if the source does not specify type.",
    "manual_game_mapping_review": "Resolve the game mapping before any promotion decision.",
    "score_conflict_review": "Compare against v26/source context; do not approve until the score conflict is explained.",
    "identity_bridge_review": "Confirm the raw player-to-NFL_player_id bridge before using it for downstream rows.",
    "identity_review": "Resolve the missing player identity before event promotion.",
    "context_review": "Decide whether this remains context-only or should become structured play-by-play.",
    "context_archive": "Archive as useful context but not a promotable NFL stat row.",
    "corroboration_archive": "Archive as corroborating evidence for an already represented fact.",
}

LANE_PRIORITY = {
    "ready_resolved_atom_review": 10,
    "score_backfill_review": 20,
    "touchdown_total_type_review": 30,
    "identity_bridge_review": 40,
    "identity_review": 50,
    "manual_game_mapping_review": 60,
    "score_conflict_review": 70,
    "context_review": 80,
    "corroboration_archive": 90,
    "context_archive": 100,
}

TABLE_PRIORITY = {
    "player_game_box_score": 10,
    "scoring_event": 20,
    "play_by_play_event": 30,
    "lineup_participation": 40,
    "team_game_stat_claim": 50,
    "game_candidate": 60,
    "player_identity_candidate": 70,
}

RISK_PRIORITY = {"low": 10, "medium": 20, "high": 30}


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def stable_id(*parts: Any) -> str:
    raw = "|".join(clean(part) for part in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def safe_slug(value: str, max_len: int = 80) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return slug[:max_len].strip("_") or "packet"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_object(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_list_json(value: Any) -> list[str]:
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if isinstance(parsed, list):
        return [clean(item) for item in parsed if clean(item)]
    return [clean(parsed)] if clean(parsed) else []


def latest_package_summary(package_root: Path, package_run_id: str = "") -> tuple[Path, dict[str, Any]]:
    if package_run_id:
        package_dir = package_root / package_run_id
        summary = package_dir / "summary.json"
        if not summary.exists():
            raise FileNotFoundError(f"summary.json not found for package run: {package_run_id}")
        return package_dir, load_json(summary)

    summaries = sorted(package_root.glob("*/summary.json"), key=lambda path: path.parent.name, reverse=True)
    if not summaries:
        raise FileNotFoundError(f"No resolved promotion package summaries found under {package_root}")
    summary_path = summaries[0]
    return summary_path.parent, load_json(summary_path)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def merged_payload(row: dict[str, Any]) -> dict[str, Any]:
    full = parse_object(row.get("full_row_json"))
    proposed = parse_object(row.get("proposed_fields_json"))
    merged = dict(full)
    merged.update({key: value for key, value in proposed.items() if clean(value)})
    return merged


def first_value(*values: Any) -> str:
    for value in values:
        text = clean(value)
        if text:
            return text
    return ""


def evidence_for(row: dict[str, Any], payload: dict[str, Any]) -> str:
    return first_value(
        row.get("evidence_text"),
        payload.get("evidence_text"),
        payload.get("source_row_text"),
        payload.get("play_text"),
        payload.get("stat_context"),
    )


def sources_for(row: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    for value in [row.get("source_documents_json"), payload.get("source_documents_json")]:
        parsed = parse_list_json(value)
        if parsed:
            return parsed
    source_id = first_value(payload.get("source_document_id"), row.get("source_document_id"))
    return [source_id] if source_id else []


def proposed_summary(target_table: str, payload: dict[str, Any]) -> str:
    keys = FIELD_PRIORITY.get(target_table, [])
    pairs: list[str] = []
    for key in keys:
        value = clean(payload.get(key))
        if value:
            pairs.append(f"{key}={value}")
    if pairs:
        return "; ".join(pairs)

    fallback_pairs = []
    for key, value in payload.items():
        if key.endswith("_json") or key.endswith("_id") or key in {"target_entity_key", "atom_claim_id"}:
            continue
        text = clean(value)
        if text:
            fallback_pairs.append(f"{key}={text}")
        if len(fallback_pairs) >= 10:
            break
    return "; ".join(fallback_pairs)


def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        LANE_PRIORITY.get(clean(row.get("package_lane")), 999),
        TABLE_PRIORITY.get(clean(row.get("target_table")), 999),
        RISK_PRIORITY.get(clean(row.get("risk_level")), 999),
        clean(row.get("boxscore_id")),
        clean(row.get("target_entity_key")),
    )


def packet_group_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        clean(row.get("package_lane")),
        clean(row.get("target_table")),
        clean(row.get("review_status")),
        clean(row.get("risk_level")),
    )


def build_review_items(rows: list[dict[str, str]], run_id: str, package_run_id: str, created_at: str) -> list[dict[str, Any]]:
    review_items: list[dict[str, Any]] = []
    for row in rows:
        payload = merged_payload(row)
        target_table = first_value(row.get("target_table"), payload.get("_target_table"), payload.get("lane_target_table"))
        source_docs = sources_for(row, payload)
        item = {
            "review_packet_run_id": run_id,
            "resolved_promotion_package_run_id": package_run_id,
            "packet_id": "",
            "packet_index": "",
            "packet_item_index": "",
            "package_item_id": clean(row.get("package_item_id")),
            "package_lane": clean(row.get("package_lane")),
            "target_table": target_table,
            "review_status": clean(row.get("review_status")),
            "risk_level": clean(row.get("risk_level")),
            "target_entity_key": first_value(row.get("target_entity_key"), payload.get("target_entity_key")),
            "boxscore_id": first_value(row.get("boxscore_id"), payload.get("boxscore_id")),
            "game_date": first_value(row.get("game_date"), payload.get("game_date")),
            "player_week": first_value(row.get("player_week"), payload.get("player_week")),
            "NFL_player_id": first_value(row.get("NFL_player_id"), payload.get("NFL_player_id")),
            "nfl_team": first_value(row.get("nfl_team"), payload.get("nfl_team"), payload.get("team_1_resolved")),
            "opponent_nfl_team": first_value(row.get("opponent_nfl_team"), payload.get("opponent_nfl_team"), payload.get("team_2_resolved")),
            "confidence_bar": first_value(row.get("confidence_bar"), payload.get("confidence_bar")),
            "source_documents_json": json.dumps(source_docs, ensure_ascii=True),
            "evidence_text": evidence_for(row, payload),
            "proposed_summary": proposed_summary(target_table, payload),
            "reason": first_value(row.get("reason"), payload.get("schema_patch_reason"), payload.get("reconciliation_status")),
            "decision_prompt": PROMPT_BY_LANE.get(clean(row.get("package_lane")), "Review the extracted atom and route it to approve, reject, or follow-up."),
            "packet_md_path": "",
            "created_at_utc": created_at,
        }
        review_items.append(item)
    return sorted(review_items, key=sort_key)


def chunk_items(items: list[dict[str, Any]], max_items: int) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    for start in range(0, len(items), max_items):
        chunks.append(items[start : start + max_items])
    return chunks


def markdown_escape(text: Any) -> str:
    return clean(text).replace("|", "\\|").replace("\n", " ")


def write_packet_markdown(path: Path, packet: dict[str, Any], items: list[dict[str, Any]]) -> None:
    lines = [
        f"# Resolved Package Review Packet {packet['packet_index']}",
        "",
        f"- Packet ID: `{packet['packet_id']}`",
        f"- Package run: `{packet['resolved_promotion_package_run_id']}`",
        f"- Lane/table/status/risk: `{packet['package_lane']}` / `{packet['target_table']}` / `{packet['review_status']}` / `{packet['risk_level']}`",
        f"- Items: `{packet['item_count']}`",
        f"- Recommended action: {packet['recommended_action']}",
        "",
        "## Review Rule",
        "",
        "Approve only when the proposed fields are directly supported by the evidence and source document IDs. Otherwise mark the item for reject, identity review, game mapping review, conflict review, or OCR/source follow-up.",
        "",
        "## Items",
        "",
    ]

    for item_index, item in enumerate(items, start=1):
        sources = ", ".join(parse_list_json(item.get("source_documents_json")))
        lines.extend(
            [
                f"### {item_index}. `{item['package_item_id']}`",
                "",
                f"- Target: `{item['target_table']}` / `{item['target_entity_key']}`",
                f"- Game/player: boxscore `{item['boxscore_id']}`, date `{item['game_date']}`, player_week `{item['player_week']}`, NFL_player_id `{item['NFL_player_id']}`, team `{item['nfl_team']}` vs `{item['opponent_nfl_team']}`",
                f"- Confidence/risk: `{item['confidence_bar']}` / `{item['risk_level']}`",
                f"- Proposed: {item['proposed_summary']}",
                f"- Evidence: {item['evidence_text']}",
                f"- Sources: `{sources}`",
                f"- Reason: {item['reason']}",
                f"- Decision prompt: {item['decision_prompt']}",
                "",
            ]
        )

    path.write_text("\n".join(lines), encoding="utf-8")


def write_packet_csv(path: Path, items: list[dict[str, Any]]) -> None:
    fields = [
        "packet_item_index",
        "package_item_id",
        "package_lane",
        "target_table",
        "review_status",
        "risk_level",
        "target_entity_key",
        "boxscore_id",
        "game_date",
        "player_week",
        "NFL_player_id",
        "nfl_team",
        "opponent_nfl_team",
        "confidence_bar",
        "source_documents_json",
        "evidence_text",
        "proposed_summary",
        "reason",
        "decision_prompt",
    ]
    write_csv(path, items, fields)


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    if not rows:
        return ["_(none)_"]
    lines = [
        "| " + " | ".join(fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(markdown_escape(row.get(field, "")) for field in fields) + " |")
    return lines


def write_run_report(path: Path, summary: dict[str, Any], packet_rows: list[dict[str, Any]]) -> None:
    rollup_fields = ["package_lane", "target_table", "review_status", "risk_level", "item_count", "packet_count"]
    rollup_counter: Counter[tuple[str, str, str, str]] = Counter()
    packet_counter: Counter[tuple[str, str, str, str]] = Counter()
    for packet in packet_rows:
        key = (
            clean(packet.get("package_lane")),
            clean(packet.get("target_table")),
            clean(packet.get("review_status")),
            clean(packet.get("risk_level")),
        )
        rollup_counter[key] += int(packet.get("item_count") or 0)
        packet_counter[key] += 1
    rollup_rows = [
        {
            "package_lane": key[0],
            "target_table": key[1],
            "review_status": key[2],
            "risk_level": key[3],
            "item_count": count,
            "packet_count": packet_counter[key],
        }
        for key, count in sorted(
            rollup_counter.items(),
            key=lambda item: (
                LANE_PRIORITY.get(item[0][0], 999),
                TABLE_PRIORITY.get(item[0][1], 999),
                RISK_PRIORITY.get(item[0][3], 999),
            ),
        )
    ]

    lines = [
        "# Newspaper Resolved Package Review Packets",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Package run: `{summary['resolved_promotion_package_run_id']}`",
        f"- Package dir: `{summary['package_dir']}`",
        f"- Output dir: `{summary['output_dir']}`",
        f"- Packets: `{summary['packet_count']}`",
        f"- Items: `{summary['item_count']}`",
        f"- Max items per packet: `{summary['max_items_per_packet']}`",
        "",
        "## Rollup",
        "",
    ]
    lines.extend(markdown_table(rollup_rows, rollup_fields))
    lines.extend(
        [
            "",
            "## Files",
            "",
            f"- Manifest CSV: `{summary['manifest_csv']}`",
            f"- Packet items CSV: `{summary['items_csv']}`",
            f"- Summary JSON: `{summary['summary_json']}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    packet_defs = ", ".join(f"{field} VARCHAR" for field in PACKET_FIELDS)
    item_defs = ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
    run_defs = ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_package_review_packet ({packet_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_package_review_packet_item ({item_defs})")
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_package_review_packet_run ({run_defs})")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    run_row: dict[str, Any],
    packet_rows: list[dict[str, Any]],
    item_rows: list[dict[str, Any]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("review_packet_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_package_review_packet_run WHERE review_packet_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_package_review_packet WHERE review_packet_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_package_review_packet_item WHERE review_packet_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_package_review_packet_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_package_review_packet", packet_rows, PACKET_FIELDS)
        insert_rows(con, "newspaper_review.resolved_package_review_packet_item", item_rows, ITEM_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-root", type=Path, default=DEFAULT_PACKAGE_ROOT)
    parser.add_argument("--package-run-id", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--label", default="resolved_package_review_packets")
    parser.add_argument("--max-items-per-packet", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_items_per_packet < 1:
        raise ValueError("--max-items-per-packet must be >= 1")

    package_dir, package_summary = latest_package_summary(args.package_root, args.package_run_id)
    package_run_id = clean(package_summary.get("resolved_promotion_package_run_id")) or package_dir.name
    created_at = iso_now()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    packets_dir = out_dir / "packets"
    packets_dir.mkdir(parents=True, exist_ok=True)

    items_path = Path(package_summary.get("items_csv") or package_dir / "resolved_promotion_package_items.csv")
    source_rows = read_csv(items_path)
    review_items = build_review_items(source_rows, run_id, package_run_id, created_at)

    packet_rows: list[dict[str, Any]] = []
    packet_item_rows: list[dict[str, Any]] = []
    packet_index = 0

    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for item in review_items:
        grouped.setdefault(packet_group_key(item), []).append(item)

    for key in sorted(
        grouped,
        key=lambda group: (
            LANE_PRIORITY.get(group[0], 999),
            TABLE_PRIORITY.get(group[1], 999),
            RISK_PRIORITY.get(group[3], 999),
            group,
        ),
    ):
        group_items = grouped[key]
        for chunk in chunk_items(group_items, args.max_items_per_packet):
            packet_index += 1
            lane, target_table, review_status, risk_level = key
            packet_id = stable_id(run_id, packet_index, lane, target_table, review_status, risk_level)
            slug = safe_slug(f"{packet_index:04d}_{lane}_{target_table}_{review_status}_{risk_level}")
            packet_md = packets_dir / f"{slug}.md"
            packet_csv = packets_dir / f"{slug}.csv"
            recommended_action = PROMPT_BY_LANE.get(lane, "Review extracted atoms and choose approve, reject, or follow-up.")

            for item_index, item in enumerate(chunk, start=1):
                item["packet_id"] = packet_id
                item["packet_index"] = str(packet_index)
                item["packet_item_index"] = str(item_index)
                item["packet_md_path"] = str(packet_md)
                packet_item_rows.append(dict(item))

            packet = {
                "review_packet_run_id": run_id,
                "resolved_promotion_package_run_id": package_run_id,
                "packet_id": packet_id,
                "packet_index": str(packet_index),
                "packet_md_path": str(packet_md),
                "packet_csv_path": str(packet_csv),
                "package_lane": lane,
                "target_table": target_table,
                "review_status": review_status,
                "risk_level": risk_level,
                "item_count": str(len(chunk)),
                "recommended_action": recommended_action,
                "created_at_utc": created_at,
            }
            packet_rows.append(packet)
            write_packet_markdown(packet_md, packet, chunk)
            write_packet_csv(packet_csv, chunk)

    manifest_csv = out_dir / "review_packet_manifest.csv"
    items_csv = out_dir / "review_packet_items.csv"
    report_md = out_dir / "review_packet_report.md"
    summary_json = out_dir / "summary.json"

    write_csv(manifest_csv, packet_rows, PACKET_FIELDS)
    write_csv(items_csv, packet_item_rows, ITEM_FIELDS)

    lane_counts = Counter(item["package_lane"] for item in packet_item_rows)
    status_counts = Counter(item["review_status"] for item in packet_item_rows)
    target_counts = Counter(item["target_table"] for item in packet_item_rows)

    run_row = {
        "review_packet_run_id": run_id,
        "resolved_promotion_package_run_id": package_run_id,
        "created_at_utc": created_at,
        "package_dir": str(package_dir),
        "output_dir": str(out_dir),
        "packet_count": str(len(packet_rows)),
        "item_count": str(len(packet_item_rows)),
        "max_items_per_packet": str(args.max_items_per_packet),
        "lane_counts_json": json.dumps(dict(sorted(lane_counts.items())), ensure_ascii=True, sort_keys=True),
        "status_counts_json": json.dumps(dict(sorted(status_counts.items())), ensure_ascii=True, sort_keys=True),
        "target_table_counts_json": json.dumps(dict(sorted(target_counts.items())), ensure_ascii=True, sort_keys=True),
        "manifest_csv": str(manifest_csv),
        "items_csv": str(items_csv),
        "summary_json": str(summary_json),
        "persisted_to_duckdb": "false" if args.dry_run else "true",
    }

    summary = dict(run_row)
    summary.update(
        {
            "packet_count": len(packet_rows),
            "item_count": len(packet_item_rows),
            "max_items_per_packet": args.max_items_per_packet,
            "lane_counts": dict(sorted(lane_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
            "target_table_counts": dict(sorted(target_counts.items())),
            "report_md": str(report_md),
            "dry_run": bool(args.dry_run),
        }
    )

    write_run_report(report_md, summary, packet_rows)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if not args.dry_run:
        persist(args.db_path, run_row, packet_rows, packet_item_rows)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
