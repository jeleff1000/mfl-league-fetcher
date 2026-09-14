#!/usr/bin/env python
"""Split after-readiness promotion candidates into auditable conveyor lanes.

This station consumes a v26 readiness report's `queue_promotion_review.csv`,
rejoins each row to its full local promoted atom, optionally reapplies an
identity-resolution overlay in memory, and writes lane-specific D-drive
queues plus local DuckDB review tables. It does not write to Fly, v26, or the
production supertable.
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from build_newspaper_v26_readiness_report import (
    PROMOTED_TABLES,
    apply_identity_overlay,
    fetch_promoted_rows,
    load_identity_auto_patches,
)


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_READINESS_ROOT = DEFAULT_ROOT / "v26_readiness_reports"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "readiness_promotion_lanes"

RUN_FIELDS = [
    "readiness_promotion_lane_run_id",
    "readiness_run_id",
    "identity_resolution_run_id",
    "readiness_dir",
    "output_dir",
    "promotion_review_count",
    "lane_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

ITEM_FIELDS = [
    "readiness_promotion_lane_run_id",
    "readiness_run_id",
    "identity_resolution_run_id",
    "source_readiness_row_id",
    "promotion_lane",
    "action_status",
    "risk_level",
    "target_table",
    "proposed_target_table",
    "target_entity_key",
    "promotion_apply_run_id",
    "decision_id",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
    "stat_name",
    "stat_value",
    "value_class",
    "v26_status",
    "identity_status",
    "confidence_bar",
    "confidence_score",
    "source_document_count",
    "stat_fields_json",
    "proposed_fields_json",
    "full_atom_json",
    "evidence_text",
    "source_documents_json",
    "lane_queue_csv",
    "created_at_utc",
]

ROLLUP_FIELDS = [
    "promotion_lane",
    "target_table",
    "value_class",
    "action_status",
    "risk_level",
    "item_count",
    "high_count",
    "medium_count",
    "low_count",
    "unknown_count",
]

DECISION_INPUT_FIELDS = [
    "decision_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_boxscore_id",
    "resolved_target_table",
    "resolved_target_entity_key",
    "proposed_fields_json",
    "notes",
]

TARGET_EXPORT_META_FIELDS = [
    "readiness_promotion_lane_run_id",
    "source_readiness_row_id",
    "promotion_lane",
    "action_status",
    "risk_level",
    "value_class",
    "v26_status",
    "identity_status",
]

IDENTITY_FIELDS = [
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
]

STAT_FIELDS = [
    "carries",
    "rushing_yards",
    "rushing_tds",
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "fg_long",
    "def_interceptions",
    "def_sacks",
    "def_tds",
    "special_teams_tds",
    "touchdowns",
]

PROPOSED_FIELDS_BY_TABLE = {
    "game_candidate": [
        "boxscore_id",
        "game_date",
        "year",
        "week",
        "season_type",
        "team_1_raw",
        "team_2_raw",
        "team_1_resolved",
        "team_2_resolved",
        "team_1_score",
        "team_2_score",
        "reconciliation_status",
        "evidence_text",
    ],
    "lineup_participation": [
        *IDENTITY_FIELDS,
        "player_raw",
        "team_raw",
        "listed_position_raw",
        "starter_position",
        "is_starter",
        "participation_type",
        "lineup_side_raw",
        "source_row_text",
    ],
    "scoring_event": [
        "boxscore_id",
        "event_order",
        "period_raw",
        "clock_raw",
        "scoring_team_raw",
        "scoring_team",
        "scoring_player_raw",
        "scoring_NFL_player_id",
        "passer_raw",
        "passer_NFL_player_id",
        "receiver_raw",
        "receiver_NFL_player_id",
        "event_type",
        "points",
        "distance_yards",
        "play_text",
    ],
    "play_by_play_event": [
        "boxscore_id",
        "event_order",
        "period_raw",
        "clock_raw",
        "possession_team_raw",
        "possession_team",
        "down_raw",
        "distance_raw",
        "yardline_raw",
        "play_type",
        "primary_player_raw",
        "primary_NFL_player_id",
        "secondary_player_raw",
        "secondary_NFL_player_id",
        "yards",
        "points",
        "play_text",
    ],
    "player_game_box_score": [
        *IDENTITY_FIELDS,
        "player_raw",
        "position",
        "starter_position",
        "is_starter",
        *STAT_FIELDS,
        "source_row_text",
    ],
    "team_game_stat_claim": [
        "boxscore_id",
        "game_date",
        "year",
        "week",
        "stat_name",
        "stat_unit",
        "team_1_raw",
        "team_1_nfl_team",
        "team_1_value",
        "team_1_opponent_nfl_team",
        "team_2_raw",
        "team_2_nfl_team",
        "team_2_value",
        "team_2_opponent_nfl_team",
        "claimed_score_text",
        "reconciliation_status",
        "stat_context",
        "source_row_text",
        "evidence_text",
    ],
    "player_identity_candidate": [
        *IDENTITY_FIELDS,
        "raw_player_name",
        "raw_team",
        "resolved_player",
        "match_method",
        "evidence_text",
    ],
}

LANE_BY_VALUE_CLASS = {
    "score_v26_missing_or_unmatched": ("game_score_review", "needs_game_mapping_review", "medium"),
    "score_adds_missing_game_date": ("game_date_backfill_review", "ready_for_apply_review", "low"),
    "additive_scoring_event_detail": ("scoring_event_detail", "ready_for_apply_review", "medium"),
    "lineup_missing_v26_player_week": ("lineup_player_week_review", "ready_for_apply_review", "medium"),
    "lineup_fills_blank_v26_starter": ("lineup_starter_fill_review", "ready_for_apply_review", "low"),
    "stat_additive_or_schema_gap": ("player_stat_schema_gap", "ready_for_schema_review", "medium"),
    "additive_play_by_play_event": ("play_by_play_event_detail", "ready_for_apply_review", "medium"),
    "additive_notable_play_context": ("play_by_play_event_detail", "ready_for_context_review", "medium"),
    "additive_team_game_stat": ("team_game_stat", "ready_for_apply_review", "medium"),
    "identity_bridge_candidate": ("identity_bridge", "ready_for_identity_bridge_review", "low"),
}


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
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return str(value)


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: clean(row.get(field)) for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True) + "\n", encoding="utf-8")


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "unknown"


def latest_readiness_dir(root: Path) -> Path:
    paths = sorted(
        (Path(path) for path in glob.glob(str(root / "*" / "queue_promotion_review.csv"))),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not paths:
        raise FileNotFoundError(f"No queue_promotion_review.csv found under {root}")
    return paths[0].parent


def parse_summary(readiness_dir: Path) -> dict[str, Any]:
    path = readiness_dir / "summary.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def nonempty_subset(row: dict[str, Any], fields: list[str]) -> dict[str, str]:
    output: dict[str, str] = {}
    for field in fields:
        value = clean(row.get(field)).strip()
        if value:
            output[field] = value
    return output


def stat_fields(row: dict[str, Any]) -> dict[str, str]:
    return nonempty_subset(row, STAT_FIELDS)


def proposed_fields(table: str, row: dict[str, Any]) -> dict[str, str]:
    fields = PROPOSED_FIELDS_BY_TABLE.get(table, [])
    proposed = nonempty_subset(row, fields)
    for field in [
        "confidence_score",
        "confidence_bar",
        "max_confidence_score",
        "source_documents_json",
        "evidence_item_count",
    ]:
        value = clean(row.get(field)).strip()
        if value:
            proposed[field] = value
    return proposed


def lane_for(row: dict[str, Any]) -> tuple[str, str, str]:
    value_class = clean(row.get("value_class"))
    lane, status, risk = LANE_BY_VALUE_CLASS.get(value_class, ("manual_promotion_review", "needs_manual_triage", "high"))
    if clean(row.get("target_table")) == "scoring_event":
        required_pairs = [
            ("scoring_player_raw", "scoring_NFL_player_id"),
            ("passer_raw", "passer_NFL_player_id"),
            ("receiver_raw", "receiver_NFL_player_id"),
        ]
        for raw_field, id_field in required_pairs:
            if clean(row.get(raw_field)) and not clean(row.get(id_field)):
                return lane, "needs_identity_review", "high"
    if clean(row.get("target_table")) == "play_by_play_event":
        required_pairs = [
            ("primary_player_raw", "primary_NFL_player_id"),
            ("secondary_player_raw", "secondary_NFL_player_id"),
        ]
        for raw_field, id_field in required_pairs:
            if clean(row.get(raw_field)) and not clean(row.get(id_field)):
                return lane, "needs_identity_review", "high"
    if clean(row.get("target_table")) == "lineup_participation" and not clean(row.get("player_week")):
        return lane, "needs_identity_review", "high"
    if clean(row.get("target_table")) == "player_game_box_score" and not clean(row.get("NFL_player_id")):
        return lane, "needs_identity_review", "high"
    return lane, status, risk


def build_items(
    run_id: str,
    readiness_run_id: str,
    identity_resolution_run_id: str,
    readiness_rows: list[dict[str, str]],
    promoted_by_key: dict[tuple[str, str], dict[str, Any]],
    lane_file_by_lane: dict[str, Path],
    created_at: str,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for index, readiness in enumerate(readiness_rows, start=1):
        table = clean(readiness.get("target_table"))
        target_key = clean(readiness.get("target_entity_key"))
        full_row = promoted_by_key.get((table, target_key), {})
        merged = dict(full_row)
        merged.update({key: value for key, value in readiness.items() if clean(value)})
        lane, action_status, risk_level = lane_for(merged)
        proposal = proposed_fields(table, merged)
        stats = stat_fields(merged)
        item = {
            "readiness_promotion_lane_run_id": run_id,
            "readiness_run_id": readiness_run_id,
            "identity_resolution_run_id": identity_resolution_run_id,
            "source_readiness_row_id": str(index),
            "promotion_lane": lane,
            "action_status": "source_row_not_found" if not full_row else action_status,
            "risk_level": "high" if not full_row else risk_level,
            "target_table": table,
            "proposed_target_table": table,
            "target_entity_key": target_key,
            "promotion_apply_run_id": clean(merged.get("promotion_apply_run_id")),
            "decision_id": clean(merged.get("decision_id")),
            "boxscore_id": clean(merged.get("boxscore_id")),
            "player_week": clean(merged.get("player_week")),
            "NFL_player_id": clean(merged.get("NFL_player_id")),
            "nfl_team": clean(merged.get("nfl_team")),
            "opponent_nfl_team": clean(merged.get("opponent_nfl_team")),
            "stat_name": clean(merged.get("stat_name")),
            "stat_value": clean(merged.get("stat_value")),
            "value_class": clean(merged.get("value_class")),
            "v26_status": clean(merged.get("v26_status")),
            "identity_status": clean(merged.get("identity_status")),
            "confidence_bar": clean(merged.get("confidence_bar")),
            "confidence_score": clean(merged.get("confidence_score")),
            "source_document_count": clean(merged.get("source_document_count")) or clean(merged.get("evidence_item_count")),
            "stat_fields_json": compact_json(stats),
            "proposed_fields_json": compact_json(proposal),
            "full_atom_json": compact_json(full_row),
            "evidence_text": clean(merged.get("evidence_text")) or clean(merged.get("source_row_text")) or clean(merged.get("play_text")),
            "source_documents_json": clean(merged.get("source_documents_json")),
            "lane_queue_csv": str(lane_file_by_lane[lane]),
            "created_at_utc": created_at,
        }
        items.append(item)
    return items


def build_rollups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str], Counter] = {}
    for item in items:
        key = (
            clean(item.get("promotion_lane")),
            clean(item.get("target_table")),
            clean(item.get("value_class")),
            clean(item.get("action_status")),
            clean(item.get("risk_level")),
        )
        if key not in grouped:
            grouped[key] = Counter()
        grouped[key]["item_count"] += 1
        confidence = clean(item.get("confidence_bar")).lower() or "unknown"
        if confidence not in {"high", "medium", "low"}:
            confidence = "unknown"
        grouped[key][f"{confidence}_count"] += 1
    rows: list[dict[str, Any]] = []
    for key, counts in sorted(grouped.items(), key=lambda item: (-item[1]["item_count"], item[0])):
        lane, table, value_class, action_status, risk = key
        rows.append({
            "promotion_lane": lane,
            "target_table": table,
            "value_class": value_class,
            "action_status": action_status,
            "risk_level": risk,
            "item_count": counts["item_count"],
            "high_count": counts["high_count"],
            "medium_count": counts["medium_count"],
            "low_count": counts["low_count"],
            "unknown_count": counts["unknown_count"],
        })
    return rows


def build_decision_inputs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        status = clean(item.get("action_status"))
        if status == "source_row_not_found":
            decision_status = "hold"
            decision_value = "source_row_not_found"
        elif status.startswith("ready_for"):
            decision_status = "pending_review"
            decision_value = "review_for_local_promotion"
        else:
            decision_status = "hold"
            decision_value = status
        rows.append({
            "decision_id": stable_id(
                item.get("readiness_promotion_lane_run_id"),
                item.get("target_table"),
                item.get("target_entity_key"),
                item.get("promotion_lane"),
            ),
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": clean(item.get("promotion_lane")),
            "resolved_boxscore_id": clean(item.get("boxscore_id")),
            "resolved_target_table": clean(item.get("proposed_target_table")),
            "resolved_target_entity_key": clean(item.get("target_entity_key")),
            "proposed_fields_json": clean(item.get("proposed_fields_json")),
            "notes": f"{clean(item.get('value_class'))}; {clean(item.get('v26_status'))}; {clean(item.get('identity_status'))}",
        })
    return rows


def parse_json_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def write_target_exports(out_dir: Path, items: list[dict[str, Any]]) -> dict[str, str]:
    export_dir = out_dir / "target_table_exports"
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        table = clean(item.get("target_table"))
        full_atom = parse_json_obj(item.get("full_atom_json"))
        if not table or not full_atom:
            continue
        row = {field: clean(item.get(field)) for field in TARGET_EXPORT_META_FIELDS}
        row.update(full_atom)
        grouped.setdefault(table, []).append(row)

    output_paths: dict[str, str] = {}
    for table, rows in sorted(grouped.items()):
        atom_fields = sorted({
            field
            for row in rows
            for field in row
            if field not in TARGET_EXPORT_META_FIELDS
        })
        fields = [*TARGET_EXPORT_META_FIELDS, *atom_fields]
        path = export_dir / f"{slug(table)}.csv"
        write_csv(path, rows, fields)
        output_paths[table] = str(path)
    return output_paths


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    defs = ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.readiness_promotion_lane_item ({defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='readiness_promotion_lane_item'
            """
        ).fetchall()
    }
    for field in ITEM_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.readiness_promotion_lane_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.readiness_promotion_lane_run (
          readiness_promotion_lane_run_id VARCHAR,
          readiness_run_id VARCHAR,
          identity_resolution_run_id VARCHAR,
          readiness_dir VARCHAR,
          output_dir VARCHAR,
          promotion_review_count INTEGER,
          lane_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[row.get(field) for field in fields] for row in rows],
    )


def persist(db_path: Path, run_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("readiness_promotion_lane_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.readiness_promotion_lane_run WHERE readiness_promotion_lane_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.readiness_promotion_lane_item WHERE readiness_promotion_lane_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.readiness_promotion_lane_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.readiness_promotion_lane_item", item_rows, ITEM_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], rollups: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Readiness Promotion Lanes",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Readiness run: `{summary['readiness_run_id']}`",
        f"- Identity overlay run: `{summary['identity_resolution_run_id'] or 'none'}`",
        f"- Promotion-review items: `{summary['promotion_review_count']}`",
        f"- Lanes: `{summary['lane_count']}`",
        "",
        "## Lane Counts",
        "",
    ]
    for lane, count in summary["lane_counts"].items():
        lines.append(f"- `{lane}`: {count}")
    lines.extend([
        "",
        "## Rollup",
        "",
        "| Lane | Table | Class | Status | Risk | Items | Confidence |",
        "|---|---|---|---|---|---:|---|",
    ])
    for row in rollups:
        confidence = (
            f"H {row['high_count']}, M {row['medium_count']}, "
            f"L {row['low_count']}, U {row['unknown_count']}"
        )
        lines.append(
            f"| `{row['promotion_lane']}` | `{row['target_table']}` | `{row['value_class']}` | "
            f"`{row['action_status']}` | `{row['risk_level']}` | {row['item_count']} | {confidence} |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- All lane items: `{summary['items_csv']}`",
        f"- Rollup CSV: `{summary['rollup_csv']}`",
        f"- Decision-input candidates: `{summary['decision_input_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
        "",
        "## Target Exports",
        "",
    ])
    for table, path in summary.get("target_table_export_csvs", {}).items():
        lines.append(f"- `{table}`: `{path}`")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--readiness-dir", type=Path, default=None)
    parser.add_argument("--readiness-root", type=Path, default=DEFAULT_READINESS_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--identity-resolution-run-id", default="")
    parser.add_argument("--label", default="readiness_promotion_lanes")
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    readiness_dir = args.readiness_dir or latest_readiness_dir(args.readiness_root)
    readiness_summary = parse_summary(readiness_dir)
    readiness_run_id = clean(readiness_summary.get("run_id")) or readiness_dir.name
    identity_run_id = args.identity_resolution_run_id or clean(readiness_summary.get("identity_resolution_run_id"))
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    readiness_csv = readiness_dir / "queue_promotion_review.csv"
    readiness_rows = read_csv(readiness_csv)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        rows_by_table = {table: fetch_promoted_rows(con, table) for table in PROMOTED_TABLES}
        identity_patches = load_identity_auto_patches(con, identity_run_id)
    finally:
        con.close()
    overlay_receipts = apply_identity_overlay(rows_by_table, identity_patches, created_at)

    promoted_by_key = {
        (table, clean(row.get("target_entity_key"))): row
        for table, rows in rows_by_table.items()
        for row in rows
        if clean(row.get("target_entity_key"))
    }

    lanes = sorted({lane_for(row)[0] for row in readiness_rows})
    lane_file_by_lane = {lane: out_dir / f"queue_{slug(lane)}.csv" for lane in lanes}
    item_rows = build_items(
        run_id,
        readiness_run_id,
        identity_run_id,
        readiness_rows,
        promoted_by_key,
        lane_file_by_lane,
        created_at,
    )
    rollups = build_rollups(item_rows)
    decision_inputs = build_decision_inputs(item_rows)

    lane_counts = Counter(clean(row.get("promotion_lane")) for row in item_rows)
    status_counts = Counter(clean(row.get("action_status")) for row in item_rows)
    risk_counts = Counter(clean(row.get("risk_level")) for row in item_rows)
    overlay_counts = Counter(clean(row.get("applied_status")) for row in overlay_receipts)

    items_csv = out_dir / "promotion_lane_items.csv"
    rollup_csv = out_dir / "promotion_lane_rollup.csv"
    decision_input_csv = out_dir / "decision_input_candidates.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "promotion_lane_report.md"
    target_export_paths = write_target_exports(out_dir, item_rows)

    write_csv(items_csv, item_rows, ITEM_FIELDS)
    write_csv(rollup_csv, rollups, ROLLUP_FIELDS)
    write_csv(decision_input_csv, decision_inputs, DECISION_INPUT_FIELDS)
    for lane, path in lane_file_by_lane.items():
        write_csv(path, [row for row in item_rows if row["promotion_lane"] == lane], ITEM_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "readiness_promotion_lane_run_id": run_id,
        "readiness_run_id": readiness_run_id,
        "identity_resolution_run_id": identity_run_id,
        "readiness_dir": str(readiness_dir),
        "readiness_queue_csv": str(readiness_csv),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "promotion_review_count": len(item_rows),
        "lane_count": len(lane_counts),
        "lane_counts": dict(sorted(lane_counts.items())),
        "action_status_counts": dict(sorted(status_counts.items())),
        "risk_counts": dict(sorted(risk_counts.items())),
        "identity_overlay_receipt_counts": dict(sorted(overlay_counts.items())),
        "items_csv": str(items_csv),
        "rollup_csv": str(rollup_csv),
        "decision_input_csv": str(decision_input_csv),
        "lane_queue_csvs": {lane: str(path) for lane, path in sorted(lane_file_by_lane.items())},
        "target_table_export_csvs": target_export_paths,
        "summary_json": str(summary_json),
        "markdown": str(markdown),
        "persisted_to_duckdb": not args.no_persist,
    }
    write_json(summary_json, summary)
    markdown.write_text(render_markdown(summary, rollups), encoding="utf-8")

    if not args.no_persist:
        persist(
            args.db_path,
            {
                "readiness_promotion_lane_run_id": run_id,
                "readiness_run_id": readiness_run_id,
                "identity_resolution_run_id": identity_run_id,
                "readiness_dir": str(readiness_dir),
                "output_dir": str(out_dir),
                "promotion_review_count": len(item_rows),
                "lane_count": len(lane_counts),
                "status": "complete",
                "created_at_utc": created_at,
                "summary_json_path": str(summary_json),
            },
            item_rows,
        )

    print(json.dumps({
        "readiness_promotion_lane_run_id": run_id,
        "output_dir": str(out_dir),
        "promotion_review_count": len(item_rows),
        "lane_counts": dict(sorted(lane_counts.items())),
        "action_status_counts": dict(sorted(status_counts.items())),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
