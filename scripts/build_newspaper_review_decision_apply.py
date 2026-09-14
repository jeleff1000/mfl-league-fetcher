#!/usr/bin/env python
"""Apply newspaper review decisions into local promoted tables and route queues.

This station consumes the consolidated decision ledger. It writes only local
D-drive artifacts and local DuckDB tables:

- newspaper_promoted.<target_table> for explicitly approved local promotions
- newspaper_review.review_decision_route_queue for pending/follow-up/hold/reject
- CSV manifests under review_decision_apply_runs/

It never writes to Fly, the supertable, or production league tables.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "review_decision_apply_runs"

TARGET_FIELDS = {
    "game_candidate": [
        "game_candidate_id", "atom_claim_id", "boxscore_id", "game_date", "year", "week", "season_type",
        "team_1_raw", "team_2_raw", "team_1_resolved", "team_2_resolved", "team_1_score", "team_2_score",
        "reconciliation_status", "confidence_score", "evidence_text", "source_document_id", "region_id",
    ],
    "scoring_event": [
        "scoring_event_id", "atom_claim_id", "boxscore_id", "event_order", "period_raw", "clock_raw",
        "scoring_team_raw", "scoring_team", "scoring_player_raw", "scoring_NFL_player_id", "passer_raw",
        "passer_NFL_player_id", "receiver_raw", "receiver_NFL_player_id", "event_type", "points",
        "distance_yards", "play_text", "confidence_score", "review_status", "promotion_status",
        "source_document_id", "region_id",
    ],
    "play_by_play_event": [
        "pbp_event_id", "atom_claim_id", "boxscore_id", "event_order", "period_raw", "clock_raw",
        "possession_team_raw", "possession_team", "down_raw", "distance_raw", "yardline_raw", "play_type",
        "primary_player_raw", "primary_NFL_player_id", "secondary_player_raw", "secondary_NFL_player_id",
        "yards", "points", "play_text", "confidence_score", "review_status", "promotion_status",
        "source_document_id", "region_id",
    ],
    "player_game_box_score": [
        "player_game_box_score_id", "atom_claim_id", "boxscore_id", "player_week", "player_raw",
        "NFL_player_id", "nfl_team", "opponent_nfl_team", "position", "starter_position", "is_starter",
        "carries", "rushing_yards", "rushing_tds", "attempts", "completions", "passing_yards",
        "passing_tds", "passing_interceptions", "receptions", "receiving_yards", "receiving_tds",
        "pat_made", "pat_att", "fg_made", "fg_att", "fg_long", "def_interceptions", "def_sacks",
        "def_tds", "special_teams_tds", "touchdowns", "source_row_text", "confidence_score", "review_status",
        "promotion_status", "source_document_id", "region_id",
    ],
    "player_game_stat_claim": [
        "player_game_stat_claim_id", "atom_claim_id", "boxscore_id", "game_date", "year", "week",
        "player_week", "player_raw", "resolved_player", "NFL_player_id", "team_raw", "nfl_team",
        "opponent_nfl_team", "stat_name", "stat_value", "stat_unit", "stat_fields_json",
        "stat_context", "source_row_text", "confidence_score", "review_status", "promotion_status",
        "source_document_id", "region_id", "match_method", "identity_status",
    ],
    "player_game_note": [
        "player_game_note_id", "atom_claim_id", "boxscore_id", "game_date", "year", "week",
        "player_week", "player_raw", "resolved_player", "NFL_player_id", "team_raw", "nfl_team",
        "opponent_nfl_team", "note_type", "note_text", "note_fields_json", "source_row_text",
        "confidence_score", "review_status", "promotion_status", "source_document_id", "region_id",
        "match_method", "identity_status",
    ],
    "team_game_stat_claim": [
        "team_game_stat_claim_id", "atom_claim_id", "boxscore_id", "game_date", "year", "week",
        "stat_name", "stat_unit", "team_1_raw", "team_1_nfl_team", "team_1_value",
        "team_1_opponent_nfl_team", "team_2_raw", "team_2_nfl_team", "team_2_value",
        "team_2_opponent_nfl_team", "claimed_score_text", "reconciliation_status",
        "stat_context", "source_row_text", "evidence_text", "confidence_score", "review_status",
        "promotion_status", "source_document_id", "region_id", "match_method",
    ],
    "source_document_note": [
        "source_document_note_id", "atom_claim_id", "source_document_id", "boxscore_id",
        "note_type", "note_category", "note_text", "related_target_table", "related_entity_key",
        "reconciliation_status", "evidence_text", "confidence_score", "review_status",
        "promotion_status", "source_documents_json", "artifact_path", "created_by_station",
    ],
    "lineup_participation": [
        "lineup_participation_id", "atom_claim_id", "boxscore_id", "player_week", "player_raw",
        "NFL_player_id", "team_raw", "nfl_team", "opponent_nfl_team", "listed_position_raw",
        "starter_position", "is_starter", "participation_type", "lineup_side_raw", "source_row_text",
        "confidence_score", "review_status", "promotion_status", "source_document_id", "region_id",
    ],
    "player_identity_candidate": [
        "identity_candidate_id", "atom_claim_id", "raw_player_name", "raw_team", "resolved_player",
        "NFL_player_id", "player_week", "nfl_team", "opponent_nfl_team", "boxscore_id", "match_method",
        "confidence_score", "review_status", "evidence_text", "source_document_id", "region_id",
    ],
    "promotion_candidate": [
        "promotion_candidate_id", "promotion_package_id", "atom_claim_id", "source_domain_table",
        "target_table", "target_row_key", "target_field", "existing_value", "proposed_value",
        "confidence_score", "review_status", "reviewer", "reviewed_at_utc", "promotion_status",
        "promotion_notes",
    ],
}

ID_FIELDS = {
    "game_candidate": "game_candidate_id",
    "scoring_event": "scoring_event_id",
    "play_by_play_event": "pbp_event_id",
    "player_game_box_score": "player_game_box_score_id",
    "player_game_stat_claim": "player_game_stat_claim_id",
    "player_game_note": "player_game_note_id",
    "team_game_stat_claim": "team_game_stat_claim_id",
    "source_document_note": "source_document_note_id",
    "lineup_participation": "lineup_participation_id",
    "player_identity_candidate": "identity_candidate_id",
    "promotion_candidate": "promotion_candidate_id",
}

PROMOTED_PREFIX_FIELDS = [
    "promotion_apply_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "promotion_package_id",
    "target_entity_key",
]

PROMOTED_SUFFIX_FIELDS = [
    "source_materialized_row_ids_json",
    "source_documents_json",
    "evidence_item_count",
    "confidence_bar",
    "max_confidence_score",
    "decision_value",
    "decision_status",
    "route_to_lane",
    "promotion_source",
    "unmapped_fields_json",
    "created_at_utc",
]

ROUTE_FIELDS = [
    "promotion_apply_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "lane",
    "route_to_lane",
    "decision_status",
    "decision_value",
    "source_prep_run_id",
    "action_queue_run_id",
    "action_id",
    "promotion_package_id",
    "source_document_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "recommended_next_action",
    "reason",
    "proposed_fields_json",
    "source_documents_json",
    "artifact_path",
    "notes",
    "created_at_utc",
]

CLOSED_FIELDS = [
    "promotion_apply_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "lane",
    "decision_status",
    "decision_value",
    "closure_bucket",
    "source_prep_run_id",
    "action_queue_run_id",
    "action_id",
    "promotion_package_id",
    "source_document_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "recommended_next_action",
    "reason",
    "proposed_fields_json",
    "source_documents_json",
    "artifact_path",
    "notes",
    "created_at_utc",
]

APPLIED_PACKAGE_FIELDS = [
    "promotion_apply_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "promotion_package_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "decision_value",
    "decision_status",
    "local_promoted_table",
    "local_promoted_row_id",
    "created_at_utc",
]

RUN_FIELDS = [
    "promotion_apply_run_id",
    "decision_ledger_run_id",
    "output_dir",
    "decision_count",
    "promoted_row_count",
    "route_queue_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

PROMOTE_VALUES = {
    "promote",
    "approve",
    "approved",
    "approve_promote",
    "approved_for_local_promotion",
    "local_promote",
}

REJECT_VALUES = {"reject", "rejected", "not_useful", "do_not_promote"}
HOLD_VALUES = {"hold", "hold_context", "context_only", "wait_for_more_evidence"}
CLOSED_STATUSES = {"closed", "closed_not_promoted", "rejected", "terminal"}
CLOSED_ROUTES = {"closed", "closed_rejected", "terminal_reject", "terminal_closed"}


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


def parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [clean(item) for item in value if clean(item)]
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


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    lines = ["| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in rows:
        values = [clean(row.get(field)).replace("\n", " ") for field in fields]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_decision_ledger_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT decision_ledger_run_id
        FROM newspaper_review.llm_review_decision_ledger_run
        ORDER BY created_at_utc DESC, decision_ledger_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_decisions(con: duckdb.DuckDBPyConnection, decision_ledger_run_id: str) -> list[dict[str, Any]]:
    if not decision_ledger_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
        ORDER BY lane, route_to_lane, target_table, boxscore_id, source_document_id
        """,
        [decision_ledger_run_id],
    )


def load_packages(con: duckdb.DuckDBPyConnection, package_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not package_ids:
        return {}
    placeholders = ",".join(["?"] * len(package_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_promotion_package
        WHERE promotion_package_id IN ({placeholders})
        """,
        package_ids,
    )
    return {clean(row.get("promotion_package_id")): row for row in rows}


def load_package_items(con: duckdb.DuckDBPyConnection, package_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not package_ids:
        return {}
    placeholders = ",".join(["?"] * len(package_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_promotion_package_item
        WHERE promotion_package_id IN ({placeholders})
        ORDER BY promotion_package_id, confidence_score DESC, source_document_id
        """,
        package_ids,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[clean(row.get("promotion_package_id"))].append(row)
    return grouped


def load_decision_overrides(path: Path | None) -> dict[str, dict[str, str]]:
    if not path or not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {clean(row.get("decision_id")): row for row in rows if clean(row.get("decision_id"))}


def overlay_decisions(decisions: list[dict[str, Any]], overrides: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    if not overrides:
        return decisions
    output = []
    for row in decisions:
        merged = dict(row)
        override = overrides.get(clean(row.get("decision_id")))
        if override:
            for field in [
                "decision_status",
                "decision_value",
                "route_to_lane",
                "resolved_boxscore_id",
                "resolved_target_table",
                "resolved_target_entity_key",
                "proposed_fields_json",
                "notes",
            ]:
                value = clean(override.get(field))
                if value or field == "route_to_lane":
                    merged[field] = value
        output.append(merged)
    return output


def target_output_fields(target_table: str) -> list[str]:
    fields = list(PROMOTED_PREFIX_FIELDS)
    for field in TARGET_FIELDS[target_table]:
        if field not in fields:
            fields.append(field)
    for field in PROMOTED_SUFFIX_FIELDS:
        if field not in fields:
            fields.append(field)
    return fields


def should_promote(row: dict[str, Any], auto_accept_promotion_review_ready: bool) -> tuple[bool, str]:
    value = clean(row.get("decision_value")).lower().strip()
    lane = clean(row.get("lane"))
    if value in PROMOTE_VALUES:
        return True, "explicit_decision"
    if auto_accept_promotion_review_ready and lane == "promotion_review" and clean(row.get("target_table")):
        return True, "auto_accept_promotion_review_ready"
    return False, ""


def should_close(row: dict[str, Any]) -> tuple[bool, str]:
    value = clean(row.get("decision_value")).lower().strip()
    status = clean(row.get("decision_status")).lower().strip()
    route = clean(row.get("route_to_lane")).lower().strip()
    if value in REJECT_VALUES:
        return True, value or "rejected"
    if status in CLOSED_STATUSES:
        return True, status or "closed"
    if route in CLOSED_ROUTES:
        return True, route or "closed"
    return False, ""


def route_bucket(row: dict[str, Any]) -> str:
    value = clean(row.get("decision_value")).lower().strip()
    route = clean(row.get("route_to_lane")).lower().strip()
    lane = clean(row.get("lane")).lower().strip()
    if value in REJECT_VALUES:
        return "reject"
    if value in HOLD_VALUES or "hold" in route:
        return "hold"
    if lane == "ocr_visual_followup" or "ocr" in route or "crop" in route or "visual" in route:
        return "ocr_visual_followup"
    if lane == "semantic_followup" or "semantic" in route or "boxscore" in route or "team_label" in route or "route" in route:
        return "semantic_followup"
    if lane == "quality_review" or "quality" in route or "identity" in route or "second_pass" in route or "evidence_check" in route:
        return "quality_review"
    return "pending_decision"


def promoted_row_from_decision(
    promotion_apply_run_id: str,
    decision_ledger_run_id: str,
    decision: dict[str, Any],
    package: dict[str, Any],
    package_items: list[dict[str, Any]],
    promotion_source: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    created_at = iso_now()
    target_table = clean(decision.get("resolved_target_table")) or clean(decision.get("target_table")) or clean(package.get("target_table"))
    target_fields = set(TARGET_FIELDS[target_table])
    proposed = parse_json_obj(decision.get("proposed_fields_json")) or parse_json_obj(package.get("proposed_fields_json"))
    mapped = {key: value for key, value in proposed.items() if key in target_fields}
    unmapped = {key: value for key, value in proposed.items() if key not in target_fields}

    fields = target_output_fields(target_table)
    row = {field: "" for field in fields}
    row.update({
        "promotion_apply_run_id": promotion_apply_run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "decision_id": clean(decision.get("decision_id")),
        "promotion_package_id": clean(decision.get("promotion_package_id")),
        "target_entity_key": clean(decision.get("resolved_target_entity_key")) or clean(decision.get("target_entity_key")) or clean(package.get("target_entity_key")),
        "source_materialized_row_ids_json": clean(package.get("materialized_row_ids_json")),
        "source_documents_json": clean(decision.get("source_documents_json")) or clean(package.get("source_documents_json")),
        "evidence_item_count": str(len(package_items)),
        "confidence_bar": clean(decision.get("confidence_bar")) or clean(package.get("confidence_bar")),
        "max_confidence_score": clean(decision.get("confidence_score")) or clean(package.get("max_confidence_score")),
        "decision_value": clean(decision.get("decision_value")),
        "decision_status": clean(decision.get("decision_status")),
        "route_to_lane": clean(decision.get("route_to_lane")),
        "promotion_source": promotion_source,
        "unmapped_fields_json": json.dumps(unmapped, sort_keys=True, ensure_ascii=False) if unmapped else "",
        "created_at_utc": created_at,
    })
    for field, value in mapped.items():
        row[field] = clean(value)
    if not clean(row.get("boxscore_id")):
        row["boxscore_id"] = clean(decision.get("resolved_boxscore_id")) or clean(decision.get("boxscore_id")) or clean(package.get("boxscore_id"))
    if "confidence_score" in row and not clean(row.get("confidence_score")):
        row["confidence_score"] = clean(decision.get("confidence_score")) or clean(package.get("max_confidence_score"))
    id_field = ID_FIELDS[target_table]
    if id_field in row and not clean(row.get(id_field)):
        row[id_field] = stable_id("local_promoted", target_table, decision.get("decision_id"), package.get("promotion_package_id"))
    if "atom_claim_id" in row and not clean(row.get("atom_claim_id")):
        materialized_ids = parse_json_list(package.get("materialized_row_ids_json"))
        row["atom_claim_id"] = materialized_ids[0] if materialized_ids else clean(decision.get("decision_id"))

    applied = {
        "promotion_apply_run_id": promotion_apply_run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "decision_id": clean(decision.get("decision_id")),
        "promotion_package_id": clean(decision.get("promotion_package_id")),
        "target_table": target_table,
        "target_entity_key": clean(row.get("target_entity_key")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "decision_value": clean(decision.get("decision_value")),
        "decision_status": clean(decision.get("decision_status")),
        "local_promoted_table": f"newspaper_promoted.{target_table}",
        "local_promoted_row_id": clean(row.get(id_field)),
        "created_at_utc": created_at,
    }
    return row, applied


def route_row_from_decision(promotion_apply_run_id: str, decision_ledger_run_id: str, decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "promotion_apply_run_id": promotion_apply_run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "decision_id": clean(decision.get("decision_id")),
        "lane": clean(decision.get("lane")),
        "route_to_lane": route_bucket(decision),
        "decision_status": clean(decision.get("decision_status")),
        "decision_value": clean(decision.get("decision_value")),
        "source_prep_run_id": clean(decision.get("source_prep_run_id")),
        "action_queue_run_id": clean(decision.get("action_queue_run_id")),
        "action_id": clean(decision.get("action_id")),
        "promotion_package_id": clean(decision.get("promotion_package_id")),
        "source_document_id": clean(decision.get("source_document_id")),
        "target_table": clean(decision.get("target_table")),
        "target_entity_key": clean(decision.get("target_entity_key")),
        "boxscore_id": clean(decision.get("boxscore_id")),
        "recommended_next_action": clean(decision.get("recommended_next_action")),
        "reason": clean(decision.get("reason")),
        "proposed_fields_json": clean(decision.get("proposed_fields_json")),
        "source_documents_json": clean(decision.get("source_documents_json")),
        "artifact_path": clean(decision.get("artifact_path")),
        "notes": clean(decision.get("notes")),
        "created_at_utc": iso_now(),
    }


def closed_row_from_decision(
    promotion_apply_run_id: str,
    decision_ledger_run_id: str,
    decision: dict[str, Any],
    closure_bucket: str,
) -> dict[str, Any]:
    return {
        "promotion_apply_run_id": promotion_apply_run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "decision_id": clean(decision.get("decision_id")),
        "lane": clean(decision.get("lane")),
        "decision_status": clean(decision.get("decision_status")),
        "decision_value": clean(decision.get("decision_value")),
        "closure_bucket": closure_bucket,
        "source_prep_run_id": clean(decision.get("source_prep_run_id")),
        "action_queue_run_id": clean(decision.get("action_queue_run_id")),
        "action_id": clean(decision.get("action_id")),
        "promotion_package_id": clean(decision.get("promotion_package_id")),
        "source_document_id": clean(decision.get("source_document_id")),
        "target_table": clean(decision.get("target_table")),
        "target_entity_key": clean(decision.get("target_entity_key")),
        "boxscore_id": clean(decision.get("boxscore_id")),
        "recommended_next_action": clean(decision.get("recommended_next_action")),
        "reason": clean(decision.get("reason")),
        "proposed_fields_json": clean(decision.get("proposed_fields_json")),
        "source_documents_json": clean(decision.get("source_documents_json")),
        "artifact_path": clean(decision.get("artifact_path")),
        "notes": clean(decision.get("notes")),
        "created_at_utc": iso_now(),
    }


def build_apply_outputs(
    promotion_apply_run_id: str,
    decision_ledger_run_id: str,
    decisions: list[dict[str, Any]],
    packages: dict[str, dict[str, Any]],
    package_items: dict[str, list[dict[str, Any]]],
    auto_accept_promotion_review_ready: bool,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    promoted_by_table: dict[str, list[dict[str, Any]]] = {target: [] for target in TARGET_FIELDS}
    applied_rows: list[dict[str, Any]] = []
    route_rows: list[dict[str, Any]] = []
    closed_rows: list[dict[str, Any]] = []

    for decision in decisions:
        target_table = clean(decision.get("resolved_target_table")) or clean(decision.get("target_table"))
        package_id = clean(decision.get("promotion_package_id"))
        should_apply, promotion_source = should_promote(decision, auto_accept_promotion_review_ready)
        if should_apply and target_table in TARGET_FIELDS and (package_id in packages or not package_id):
            promoted_row, applied_row = promoted_row_from_decision(
                promotion_apply_run_id,
                decision_ledger_run_id,
                decision,
                packages.get(package_id, {}),
                package_items.get(package_id, []),
                promotion_source,
            )
            promoted_by_table[target_table].append(promoted_row)
            applied_rows.append(applied_row)
            continue
        close_decision, closure_bucket = should_close(decision)
        if close_decision:
            closed_rows.append(closed_row_from_decision(
                promotion_apply_run_id,
                decision_ledger_run_id,
                decision,
                closure_bucket,
            ))
            continue
        route_rows.append(route_row_from_decision(promotion_apply_run_id, decision_ledger_run_id, decision))

    return dedupe_promoted_rows(promoted_by_table), applied_rows, route_rows, closed_rows


def dedupe_promoted_rows(promoted_by_table: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    deduped: dict[str, list[dict[str, Any]]] = {}
    for target_table, rows in promoted_by_table.items():
        id_field = ID_FIELDS[target_table]
        seen: set[str] = set()
        unique_rows: list[dict[str, Any]] = []
        for row in rows:
            row_id = clean(row.get(id_field))
            key = row_id or clean(row.get("target_entity_key"))
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            unique_rows.append(row)
        deduped[target_table] = unique_rows
    return deduped


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_promoted")
    for target_table, fields in TARGET_FIELDS.items():
        all_fields = target_output_fields(target_table)
        defs = ", ".join(f"{field} VARCHAR" for field in all_fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_promoted.{target_table} ({defs})")
        existing = {
            row[0] for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_promoted' AND table_name=?
                """,
                [target_table],
            ).fetchall()
        }
        for field in all_fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_promoted.{target_table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")

    for table_name, fields in [
        ("review_decision_applied_promotion", APPLIED_PACKAGE_FIELDS),
        ("review_decision_route_queue", ROUTE_FIELDS),
        ("review_decision_closed", CLOSED_FIELDS),
    ]:
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.{table_name} ({defs})")
        existing = {
            row[0] for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review' AND table_name=?
                """,
                [table_name],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table_name} ADD COLUMN IF NOT EXISTS {field} VARCHAR")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.review_decision_apply_run (
          promotion_apply_run_id VARCHAR,
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          decision_count INTEGER,
          promoted_row_count INTEGER,
          route_queue_count INTEGER,
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
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    promotion_apply_run_id: str,
    decision_ledger_run_id: str,
    out_dir: Path,
    promoted_by_table: dict[str, list[dict[str, Any]]],
    applied_rows: list[dict[str, Any]],
    route_rows: list[dict[str, Any]],
    closed_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.review_decision_apply_run WHERE promotion_apply_run_id = ?",
            [promotion_apply_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.review_decision_applied_promotion WHERE promotion_apply_run_id = ?",
            [promotion_apply_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.review_decision_route_queue WHERE promotion_apply_run_id = ?",
            [promotion_apply_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.review_decision_closed WHERE promotion_apply_run_id = ?",
            [promotion_apply_run_id],
        )
        for target_table, rows in promoted_by_table.items():
            con.execute(
                f"DELETE FROM newspaper_promoted.{target_table} WHERE promotion_apply_run_id = ?",
                [promotion_apply_run_id],
            )
            insert_rows(con, f"newspaper_promoted.{target_table}", rows, target_output_fields(target_table))
        insert_rows(con, "newspaper_review.review_decision_applied_promotion", applied_rows, APPLIED_PACKAGE_FIELDS)
        insert_rows(con, "newspaper_review.review_decision_route_queue", route_rows, ROUTE_FIELDS)
        insert_rows(con, "newspaper_review.review_decision_closed", closed_rows, CLOSED_FIELDS)
        insert_rows(con, "newspaper_review.review_decision_apply_run", [{
            "promotion_apply_run_id": promotion_apply_run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "decision_count": len(applied_rows) + len(route_rows),
            "promoted_row_count": sum(len(rows) for rows in promoted_by_table.values()),
            "route_queue_count": len(route_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(
    path: Path,
    summary: dict[str, Any],
    applied_rows: list[dict[str, Any]],
    route_rows: list[dict[str, Any]],
    closed_rows: list[dict[str, Any]],
) -> None:
    lines = [
        "# Newspaper Review Decision Apply Run",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Apply run: `{summary['promotion_apply_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Promoted local rows: `{summary['promoted_row_count']}`",
        f"- Route queue rows: `{summary['route_queue_count']}`",
        f"- Terminal closed rows: `{summary['closed_decision_count']}`",
        f"- Promoted tables: `{summary['promoted_target_table_counts']}`",
        f"- Route buckets: `{summary['route_queue_counts']}`",
        f"- Closure buckets: `{summary['closed_decision_counts']}`",
        "",
        "## Applied Promotions",
        "",
    ]
    lines.extend(markdown_table(applied_rows[:80], [
        "target_table",
        "boxscore_id",
        "local_promoted_table",
        "local_promoted_row_id",
        "decision_value",
    ]))
    lines.extend(["", "## Route Queue", ""])
    lines.extend(markdown_table(route_rows[:120], [
        "route_to_lane",
        "lane",
        "target_table",
        "boxscore_id",
        "source_document_id",
        "recommended_next_action",
        "reason",
    ]))
    lines.extend(["", "## Terminal Closed Decisions", ""])
    lines.extend(markdown_table(closed_rows[:120], [
        "closure_bucket",
        "lane",
        "target_table",
        "boxscore_id",
        "source_document_id",
        "reason",
        "notes",
    ]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_review_decision_apply")
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument("--decision-input-csv", type=Path, default=None)
    parser.add_argument("--auto-accept-promotion-review-ready", action="store_true")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    promotion_apply_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / promotion_apply_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_ledger_run_id = args.decision_ledger_run_id or latest_decision_ledger_run(con)
        decisions = load_decisions(con, decision_ledger_run_id)
        overrides = load_decision_overrides(args.decision_input_csv)
        decisions = overlay_decisions(decisions, overrides)
        package_ids = sorted({clean(row.get("promotion_package_id")) for row in decisions if clean(row.get("promotion_package_id"))})
        packages = load_packages(con, package_ids)
        package_items = load_package_items(con, package_ids)
    finally:
        con.close()

    promoted_by_table, applied_rows, route_rows, closed_rows = build_apply_outputs(
        promotion_apply_run_id,
        decision_ledger_run_id,
        decisions,
        packages,
        package_items,
        args.auto_accept_promotion_review_ready,
    )

    for target_table, rows in promoted_by_table.items():
        write_csv(out_dir / f"local_promoted_{target_table}.csv", rows, target_output_fields(target_table))
    write_csv(out_dir / "applied_promotions.csv", applied_rows, APPLIED_PACKAGE_FIELDS)
    write_csv(out_dir / "route_queue.csv", route_rows, ROUTE_FIELDS)
    write_csv(out_dir / "closed_decisions.csv", closed_rows, CLOSED_FIELDS)
    rows_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in route_rows:
        rows_by_route[clean(row.get("route_to_lane")) or "pending_decision"].append(row)
    for route, rows in rows_by_route.items():
        safe_route = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in route)
        write_csv(out_dir / f"route_{safe_route}.csv", rows, ROUTE_FIELDS)

    promoted_counts = {target: len(rows) for target, rows in promoted_by_table.items() if rows}
    summary = {
        "created_at_utc": created_at,
        "promotion_apply_run_id": promotion_apply_run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "decision_input_csv": str(args.decision_input_csv) if args.decision_input_csv else "",
        "auto_accept_promotion_review_ready": bool(args.auto_accept_promotion_review_ready),
        "decision_count": len(decisions),
        "promoted_row_count": sum(promoted_counts.values()),
        "route_queue_count": len(route_rows),
        "closed_decision_count": len(closed_rows),
        "promoted_target_table_counts": promoted_counts,
        "route_queue_counts": dict(Counter(row["route_to_lane"] for row in route_rows)),
        "closed_decision_counts": dict(Counter(row["closure_bucket"] for row in closed_rows)),
        "decision_value_counts": dict(Counter(clean(row.get("decision_value")) for row in decisions)),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_json(summary_path, summary)
    write_markdown(out_dir / "review_decision_apply_report.md", summary, applied_rows, route_rows, closed_rows)
    if not args.no_db:
        persist(args.db_path, promotion_apply_run_id, decision_ledger_run_id, out_dir, promoted_by_table, applied_rows, route_rows, closed_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
