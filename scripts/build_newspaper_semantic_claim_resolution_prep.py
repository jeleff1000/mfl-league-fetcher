#!/usr/bin/env python
"""Generate atom-level decisions from remaining semantic follow-up claims.

The earlier semantic queue is document-oriented: one held decision can contain
many useful claims. This station turns selected structured LLM claims into
separate generated decisions, then routes the original document note into a
`source_document_note` atom so the conveyor can keep moving without losing why
the source was held.
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


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "semantic_claim_resolution_preps"
DEFAULT_PLAYER_INDEX = Path(r"D:\league-history-data\nfl\raw\pfr\players\player_index.parquet")
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")

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

RUN_FIELDS = [
    "generated_atom_decision_run_id",
    "promotion_apply_run_id",
    "output_dir",
    "source_document_count",
    "generated_decision_count",
    "semantic_note_override_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

PLAYER_STAT_UNITS = {
    "touchdowns",
    "goals_from_touchdown",
    "player_game_summary",
    "scoring_summary",
}

PLAYER_NOTE_UNITS = {"playing_time", "injury"}


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
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def norm_words(value: Any) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", clean(value).lower()) if token]


def norm_text(value: Any) -> str:
    return " ".join(norm_words(value))


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


def parse_num(value: Any) -> float | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    number = parse_num(value)
    if number is None:
        return None
    return int(number)


def normalize_confidence(value: Any, fallback: str = "0.70") -> str:
    number = parse_num(value)
    if number is None:
        return fallback
    if number > 1 and number <= 100:
        number = number / 100.0
    if number < 0 or number > 1:
        return fallback
    return f"{number:.3f}".rstrip("0").rstrip(".")


def normalize_stat_value(value: Any) -> str:
    number = parse_num(value)
    if number is None:
        return clean(value)
    if abs(number - int(number)) < 0.000001:
        return str(int(number))
    return f"{number:.3f}".rstrip("0").rstrip(".")


def normalize_note_type(value: Any) -> str:
    text = norm_text(value).replace(" ", "_")
    if text.endswith("_note"):
        text = text[:-5]
    return text


def confidence_bar(value: str) -> str:
    number = parse_num(value)
    if number is None:
        return "medium"
    if number >= 0.8:
        return "high"
    if number >= 0.55:
        return "medium"
    return "low"


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_apply_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT promotion_apply_run_id
        FROM newspaper_review.review_decision_apply_run
        ORDER BY created_at_utc DESC, promotion_apply_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def latest_decision_input(root: Path) -> Path | None:
    patterns = [
        root / "semantic_claim_resolution_preps" / "*" / "decision_input.csv",
        root / "team_game_stat_resolution_preps" / "*" / "decision_input.csv",
        root / "schema_gap_resolution_preps" / "*" / "decision_input.csv",
        root / "semantic_followup_resolution_preps" / "*" / "decision_input.csv",
        root / "quality_lane_resolution_preps" / "*" / "decision_input.csv",
        root / "semantic_game_key_resolution_preps" / "*" / "decision_input.csv",
        root / "player_box_score_resolution_preps" / "*" / "decision_input.csv",
        root / "event_detail_resolution_preps" / "*" / "decision_input.csv",
        root / "lineup_identity_resolution_preps" / "*" / "decision_input.csv",
        root / "review_decision_inputs" / "*" / "decision_input.csv",
    ]
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path(path) for path in glob.glob(str(pattern)))
    paths = [path for path in paths if path.exists()]
    if not paths:
        return None
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0]


def read_base_decisions(path: Path | None) -> list[dict[str, str]]:
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def combine_decision_inputs(base_rows: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_id: dict[str, dict[str, str]] = {}
    for row in base_rows:
        decision_id = clean(row.get("decision_id"))
        if decision_id:
            by_id[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    for row in new_rows:
        decision_id = clean(row.get("decision_id"))
        if decision_id:
            by_id[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    return [by_id[key] for key in sorted(by_id)]


def load_semantic_route_rows(con: duckdb.DuckDBPyConnection, promotion_apply_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND route_to_lane = 'semantic_followup'
        ORDER BY source_document_id, decision_id
        """,
        [promotion_apply_run_id],
    )


def load_claims(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> list[dict[str, Any]]:
    if not source_document_ids:
        return []
    placeholders = ",".join(["?"] * len(source_document_ids))
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_review_claim
        WHERE source_document_id IN ({placeholders})
        ORDER BY source_document_id, target_table, entity_text, raw_value, confidence_score DESC
        """,
        source_document_ids,
    )


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and row[0])


def load_existing_atom_keys(con: duckdb.DuckDBPyConnection, promotion_apply_run_id: str) -> tuple[set[tuple[str, str, str, str]], set[tuple[str, str, str]]]:
    stat_keys: set[tuple[str, str, str, str]] = set()
    note_keys: set[tuple[str, str, str]] = set()
    if table_exists(con, "newspaper_promoted", "player_game_stat_claim"):
        rows = query_dicts(
            con,
            """
            SELECT boxscore_id, COALESCE(NULLIF(NFL_player_id, ''), player_raw) AS player_key,
                   stat_name, stat_value
            FROM newspaper_promoted.player_game_stat_claim
            WHERE promotion_apply_run_id = ?
            """,
            [promotion_apply_run_id],
        )
        for row in rows:
            stat_keys.add((
                clean(row.get("boxscore_id")),
                clean(row.get("player_key")),
                clean(row.get("stat_name")),
                normalize_stat_value(row.get("stat_value")),
            ))
    if table_exists(con, "newspaper_promoted", "player_game_note"):
        rows = query_dicts(
            con,
            """
            SELECT boxscore_id, COALESCE(NULLIF(NFL_player_id, ''), player_raw) AS player_key,
                   note_type
            FROM newspaper_promoted.player_game_note
            WHERE promotion_apply_run_id = ?
            """,
            [promotion_apply_run_id],
        )
        for row in rows:
            note_keys.add((
                clean(row.get("boxscore_id")),
                clean(row.get("player_key")),
                normalize_note_type(row.get("note_type")),
            ))
    return stat_keys, note_keys


def load_team_games(team_games_path: Path) -> dict[str, dict[str, Any]]:
    con = duckdb.connect()
    try:
        rows = query_dicts(
            con,
            """
            SELECT boxscore_id, CAST(game_date AS VARCHAR) AS game_date, year, week,
                   team_code, opponent_code
            FROM read_parquet(?)
            """,
            [str(team_games_path)],
        )
    finally:
        con.close()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        out[f"{clean(row.get('boxscore_id'))}|{clean(row.get('team_code'))}"] = row
    return out


def load_player_index(player_index_path: Path) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        return query_dicts(
            con,
            """
            SELECT player, pfr_id, first_year, last_year, index_position
            FROM read_parquet(?)
            WHERE player IS NOT NULL
              AND pfr_id IS NOT NULL
            """,
            [str(player_index_path)],
        )
    finally:
        con.close()


def boxscore_context(team_games: dict[str, dict[str, Any]], boxscore_id: str) -> dict[str, str]:
    candidates = [row for key, row in team_games.items() if key.startswith(f"{boxscore_id}|")]
    if not candidates:
        return {"game_date": "", "year": "", "week": ""}
    row = candidates[0]
    return {
        "game_date": clean(row.get("game_date")),
        "year": clean(parse_int(row.get("year")) or ""),
        "week": clean(parse_int(row.get("week")) or ""),
    }


def resolve_player(raw_player: Any, year: int | None, player_index: list[dict[str, Any]]) -> dict[str, str]:
    raw_norm = norm_text(raw_player)
    if not raw_norm:
        return {"resolved_player": "", "NFL_player_id": "", "match_method": "missing_player_name", "identity_status": "unresolved_player_name_missing"}
    raw_tokens = raw_norm.split()
    candidates = []
    for row in player_index:
        first_year = parse_int(row.get("first_year"))
        last_year = parse_int(row.get("last_year"))
        if year is not None and first_year is not None and last_year is not None and not (first_year <= year <= last_year):
            continue
        player_norm = norm_text(row.get("player"))
        tokens = player_norm.split()
        if not tokens:
            continue
        if player_norm == raw_norm:
            rank = 0
        elif len(raw_tokens) == 1 and tokens[-1] == raw_tokens[0]:
            rank = 1
        elif len(raw_tokens) > 1 and tokens[-1] == raw_tokens[-1]:
            rank = 2
        else:
            continue
        candidates.append({**row, "match_rank": rank})
    if not candidates:
        return {"resolved_player": "", "NFL_player_id": "", "match_method": "player_index_no_match", "identity_status": "unresolved_player_identity"}
    candidates.sort(key=lambda row: (int(row.get("match_rank") or 9), clean(row.get("player"))))
    best_rank = int(candidates[0].get("match_rank") or 0)
    best = [row for row in candidates if int(row.get("match_rank") or 0) == best_rank]
    if len(best) == 1:
        methods = {
            0: "player_index_exact_name",
            1: "player_index_unique_last_name_by_year",
            2: "player_index_unique_last_name_with_first_initial_by_year",
        }
        return {
            "resolved_player": clean(best[0].get("player")),
            "NFL_player_id": clean(best[0].get("pfr_id")),
            "match_method": methods.get(best_rank, "player_index_unique_by_year"),
            "identity_status": "resolved_player_index",
        }
    return {"resolved_player": "", "NFL_player_id": "", "match_method": "player_index_ambiguous", "identity_status": "ambiguous_player_identity"}


def claim_key(row: dict[str, Any]) -> str:
    parts = [
        row.get("source_document_id"),
        row.get("target_table"),
        row.get("entity_text"),
        row.get("raw_value"),
        row.get("normalized_value"),
        row.get("unit"),
        row.get("evidence_quote"),
    ]
    return stable_id(*parts)


def claim_player(row: dict[str, Any]) -> str:
    text = clean(row.get("entity_text"))
    if "|" in text:
        text = text.split("|")[-1]
    if "/" in text:
        text = text.split("/")[0]
    return text.strip()


def stat_fields_from_claim(row: dict[str, Any]) -> tuple[str, str, dict[str, str]]:
    unit = norm_text(row.get("unit")).replace(" ", "_")
    value = clean(row.get("numeric_value"))
    normalized = clean(row.get("normalized_value"))
    raw = clean(row.get("raw_value"))
    fields: dict[str, str] = {}
    if unit == "touchdowns":
        fields["touchdowns"] = value or "1"
        return "touchdowns", fields["touchdowns"], fields
    if unit == "goals_from_touchdown":
        fields["goals_from_touchdown"] = value or ""
        return "goals_from_touchdown", fields["goals_from_touchdown"], fields
    if unit == "player_game_summary":
        if value:
            fields["touchdowns"] = value
        return "touchdowns", value, fields
    if unit == "scoring_summary":
        td = re.search(r"\b(\d+)\s+TD\b", normalized, flags=re.IGNORECASE)
        pat = re.search(r"\b(\d+)\s+PAT/goals\b", normalized, flags=re.IGNORECASE)
        drop = re.search(r"\b(\d+)\s+(\d+)-yard drop kick\b", normalized, flags=re.IGNORECASE)
        if td:
            fields["touchdowns"] = td.group(1)
        if pat:
            fields["goals_from_touchdown"] = pat.group(1)
        if drop:
            fields["drop_kick_field_goals"] = drop.group(1)
            fields["drop_kick_long_yards"] = drop.group(2)
        return "scoring_summary", "", fields
    return unit or "stat_claim", value or raw, fields


def player_week(nfl_player_id: str, context: dict[str, str]) -> str:
    if not nfl_player_id or not context.get("year") or not context.get("week"):
        return ""
    return f"{nfl_player_id}_{context['year']}_{context['week']}"


def player_stat_payload(row: dict[str, Any], route_by_doc: dict[str, dict[str, Any]], team_games: dict[str, dict[str, Any]], player_index: list[dict[str, Any]]) -> dict[str, Any]:
    source_document_id = clean(row.get("source_document_id"))
    route = route_by_doc.get(source_document_id, {})
    boxscore_id = clean(route.get("boxscore_id"))
    context = boxscore_context(team_games, boxscore_id)
    raw_player = claim_player(row)
    player = resolve_player(raw_player, parse_int(context.get("year")), player_index)
    stat_name, stat_value, stat_fields = stat_fields_from_claim(row)
    return {
        "player_game_stat_claim_id": stable_id("semantic_claim_stat", claim_key(row), boxscore_id, raw_player, stat_name),
        "boxscore_id": boxscore_id,
        "game_date": context.get("game_date", ""),
        "year": context.get("year", ""),
        "week": context.get("week", ""),
        "player_week": player_week(player.get("NFL_player_id", ""), context),
        "player_raw": raw_player,
        "resolved_player": player.get("resolved_player", ""),
        "NFL_player_id": player.get("NFL_player_id", ""),
        "team_raw": "",
        "nfl_team": "",
        "opponent_nfl_team": "",
        "stat_name": stat_name,
        "stat_value": stat_value,
        "stat_unit": "count",
        "stat_fields_json": json.dumps(stat_fields, sort_keys=True, ensure_ascii=False),
        "stat_context": clean(row.get("reason")),
        "source_row_text": clean(row.get("evidence_quote")) or clean(row.get("raw_value")),
        "confidence_score": normalize_confidence(row.get("confidence_score")),
        "review_status": "local_atom_semantic_claim_accepted",
        "promotion_status": "local_atom_only",
        "source_document_id": source_document_id,
        "region_id": clean(row.get("evidence_region_id")),
        "match_method": player.get("match_method", ""),
        "identity_status": player.get("identity_status", ""),
    }


def player_note_payload(row: dict[str, Any], route_by_doc: dict[str, dict[str, Any]], team_games: dict[str, dict[str, Any]], player_index: list[dict[str, Any]]) -> dict[str, Any]:
    source_document_id = clean(row.get("source_document_id"))
    route = route_by_doc.get(source_document_id, {})
    boxscore_id = clean(route.get("boxscore_id"))
    context = boxscore_context(team_games, boxscore_id)
    raw_player = claim_player(row)
    player = resolve_player(raw_player, parse_int(context.get("year")), player_index)
    unit = norm_text(row.get("unit")).replace(" ", "_") or "semantic_note"
    return {
        "player_game_note_id": stable_id("semantic_claim_note", claim_key(row), boxscore_id, raw_player, unit),
        "boxscore_id": boxscore_id,
        "game_date": context.get("game_date", ""),
        "year": context.get("year", ""),
        "week": context.get("week", ""),
        "player_week": player_week(player.get("NFL_player_id", ""), context),
        "player_raw": raw_player,
        "resolved_player": player.get("resolved_player", ""),
        "NFL_player_id": player.get("NFL_player_id", ""),
        "team_raw": "",
        "nfl_team": "",
        "opponent_nfl_team": "",
        "note_type": unit,
        "note_text": clean(row.get("normalized_value")) or clean(row.get("raw_value")) or clean(row.get("evidence_quote")),
        "note_fields_json": json.dumps({unit: clean(row.get("raw_value"))}, sort_keys=True, ensure_ascii=False),
        "source_row_text": clean(row.get("evidence_quote")) or clean(row.get("raw_value")),
        "confidence_score": normalize_confidence(row.get("confidence_score")),
        "review_status": "local_atom_semantic_claim_accepted",
        "promotion_status": "local_atom_only",
        "source_document_id": source_document_id,
        "region_id": clean(row.get("evidence_region_id")),
        "match_method": player.get("match_method", ""),
        "identity_status": player.get("identity_status", ""),
    }


def source_note_payload(route: dict[str, Any]) -> dict[str, Any]:
    source_document_id = clean(route.get("source_document_id"))
    boxscore_id = clean(route.get("boxscore_id"))
    reason = clean(route.get("reason"))
    note_category = "semantic_followup_note"
    if "polluted" in reason.lower() or "false" in reason.lower() or "non-football" in reason.lower():
        note_category = "contamination_guard"
    elif "corroboration" in reason.lower() or "supports" in reason.lower():
        note_category = "source_corroboration"
    elif "conflict" in reason.lower() or "suggested" in reason.lower():
        note_category = "source_conflict"
    return {
        "source_document_note_id": stable_id("source_document_note", source_document_id, boxscore_id, reason),
        "source_document_id": source_document_id,
        "boxscore_id": boxscore_id,
        "note_type": "semantic_followup_resolution_note",
        "note_category": note_category,
        "note_text": reason,
        "related_target_table": clean(route.get("target_table")),
        "related_entity_key": clean(route.get("target_entity_key")),
        "reconciliation_status": note_category,
        "evidence_text": reason,
        "confidence_score": "0.70",
        "review_status": "local_atom_semantic_note_accepted",
        "promotion_status": "local_atom_only",
        "source_documents_json": clean(route.get("source_documents_json")) or json.dumps([source_document_id] if source_document_id else []),
        "artifact_path": clean(route.get("artifact_path")),
        "created_by_station": "build_newspaper_semantic_claim_resolution_prep.py",
    }


def generated_decision(
    run_id: str,
    target_table: str,
    boxscore_id: str,
    source_document_id: str,
    target_entity_key: str,
    payload: dict[str, Any],
    reason: str,
    created_at: str,
) -> dict[str, Any]:
    conf = normalize_confidence(payload.get("confidence_score"))
    decision_id = stable_id("generated_atom_decision", run_id, target_table, source_document_id, target_entity_key)
    row = {field: "" for field in GENERATED_DECISION_FIELDS}
    row.update({
        "generated_atom_decision_run_id": run_id,
        "decision_ledger_run_id": "",
        "decision_id": decision_id,
        "lane": "semantic_claim_resolution",
        "source_prep_run_id": run_id,
        "source_document_id": source_document_id,
        "target_table": target_table,
        "target_entity_key": target_entity_key,
        "boxscore_id": boxscore_id,
        "confidence_bar": confidence_bar(conf),
        "confidence_score": conf,
        "evidence_document_count": "1",
        "item_count": "1",
        "recommended_next_action": "local_atom_promotion",
        "decision_status": "approved_for_local_promotion",
        "decision_value": PROMOTION_VALUE,
        "route_to_lane": "",
        "resolved_boxscore_id": boxscore_id,
        "resolved_target_table": target_table,
        "resolved_target_entity_key": target_entity_key,
        "reason": reason,
        "proposed_fields_json": json.dumps(payload, sort_keys=True, ensure_ascii=False),
        "source_documents_json": json.dumps([source_document_id] if source_document_id else []),
        "notes": "generated from structured semantic follow-up claim",
        "created_at_utc": created_at,
    })
    return row


def build_generated_decisions(
    run_id: str,
    route_rows: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    team_games: dict[str, dict[str, Any]],
    player_index: list[dict[str, Any]],
    existing_stat_keys: set[tuple[str, str, str, str]],
    existing_note_keys: set[tuple[str, str, str]],
) -> list[dict[str, Any]]:
    created_at = iso_now()
    route_by_doc = {clean(row.get("source_document_id")): row for row in route_rows if clean(row.get("source_document_id"))}
    seen_claims: set[str] = set()
    out: list[dict[str, Any]] = []
    for claim in claims:
        source_document_id = clean(claim.get("source_document_id"))
        if source_document_id not in route_by_doc:
            continue
        key = claim_key(claim)
        if key in seen_claims:
            continue
        seen_claims.add(key)
        target_table = clean(claim.get("target_table"))
        unit = norm_text(claim.get("unit")).replace(" ", "_")
        if target_table == "player_game_box_score" and unit in PLAYER_STAT_UNITS:
            payload = player_stat_payload(claim, route_by_doc, team_games, player_index)
            if not clean(payload.get("stat_fields_json")) or clean(payload.get("stat_fields_json")) == "{}":
                continue
            boxscore_id = clean(payload.get("boxscore_id"))
            stat_key = (
                boxscore_id,
                clean(payload.get("NFL_player_id")) or clean(payload.get("player_raw")),
                clean(payload.get("stat_name")),
                normalize_stat_value(payload.get("stat_value")),
            )
            if stat_key in existing_stat_keys:
                continue
            target_key = f"player_game_stat_claim|{boxscore_id}|{payload.get('NFL_player_id') or payload.get('player_raw')}|{payload.get('stat_name')}"
            out.append(generated_decision(
                run_id,
                "player_game_stat_claim",
                boxscore_id,
                source_document_id,
                target_key,
                payload,
                clean(claim.get("reason")) or "semantic player stat claim",
                created_at,
            ))
        elif target_table in {"player_game_box_score", "play_by_play_event", "lineup_participation"} and unit in PLAYER_NOTE_UNITS:
            payload = player_note_payload(claim, route_by_doc, team_games, player_index)
            boxscore_id = clean(payload.get("boxscore_id"))
            note_key = (
                boxscore_id,
                clean(payload.get("NFL_player_id")) or clean(payload.get("player_raw")),
                normalize_note_type(payload.get("note_type")),
            )
            if note_key in existing_note_keys:
                continue
            target_key = f"player_game_note|{boxscore_id}|{payload.get('NFL_player_id') or payload.get('player_raw')}|{payload.get('note_type')}"
            out.append(generated_decision(
                run_id,
                "player_game_note",
                boxscore_id,
                source_document_id,
                target_key,
                payload,
                clean(claim.get("reason")) or "semantic player note claim",
                created_at,
            ))
    return out


def build_source_note_overrides(route_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for route in route_rows:
        payload = source_note_payload(route)
        boxscore_id = clean(route.get("boxscore_id"))
        rows.append({
            "decision_id": clean(route.get("decision_id")),
            "decision_status": "approved_for_local_promotion",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "",
            "resolved_boxscore_id": boxscore_id,
            "resolved_target_table": "source_document_note",
            "resolved_target_entity_key": f"source_document_note|{boxscore_id}|{clean(route.get('source_document_id')) or clean(route.get('decision_id'))}",
            "proposed_fields_json": json.dumps(payload, sort_keys=True, ensure_ascii=False),
            "notes": "document-level semantic follow-up preserved as source_document_note",
        })
    return rows


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(fields))
    field_list = ", ".join(fields)
    values = [[clean(row.get(field)) for field in fields] for row in rows]
    con.executemany(f"INSERT INTO newspaper_review.{table} ({field_list}) VALUES ({placeholders})", values)


def persist(
    db_path: Path,
    run_id: str,
    promotion_apply_run_id: str,
    out_dir: Path,
    generated_rows: list[dict[str, Any]],
    semantic_note_overrides: list[dict[str, str]],
    summary_path: Path,
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
            + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
            + ")"
        )
        for field in GENERATED_DECISION_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.generated_atom_decision ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in RUN_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.generated_atom_decision_run ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        con.execute("DELETE FROM newspaper_review.generated_atom_decision WHERE generated_atom_decision_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.generated_atom_decision_run WHERE generated_atom_decision_run_id = ?", [run_id])
        insert_rows(con, "generated_atom_decision", generated_rows, GENERATED_DECISION_FIELDS)
        insert_rows(con, "generated_atom_decision_run", [{
            "generated_atom_decision_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "output_dir": str(out_dir),
            "source_document_count": len({clean(row.get("source_document_id")) for row in generated_rows if clean(row.get("source_document_id"))}),
            "generated_decision_count": len(generated_rows),
            "semantic_note_override_count": len(semantic_note_overrides),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], generated_rows: list[dict[str, Any]], note_rows: list[dict[str, str]]) -> None:
    lines = [
        "# Newspaper Semantic Claim Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['generated_atom_decision_run_id']}`",
        f"Apply source: `{summary['promotion_apply_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Generated atom decisions: `{summary['generated_decision_count']}`",
        f"- Semantic note overrides: `{summary['semantic_note_override_count']}`",
        f"- Generated target tables: `{summary['generated_target_table_counts']}`",
        "",
        "## Generated Decisions",
        "",
        "| target | boxscore | source document | confidence | key |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for row in generated_rows[:80]:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("target_table")),
                clean(row.get("boxscore_id")),
                clean(row.get("source_document_id")),
                clean(row.get("confidence_score")),
                clean(row.get("target_entity_key")).replace("|", "/"),
            ])
            + " |"
        )
    lines.extend([
        "",
        "## Semantic Notes",
        "",
        f"- Original semantic notes converted to `source_document_note`: `{len(note_rows)}`",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_semantic_claim_resolution")
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--player-index", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        promotion_apply_run_id = args.promotion_apply_run_id or latest_apply_run(con)
        route_rows = load_semantic_route_rows(con, promotion_apply_run_id)
        source_document_ids = sorted({clean(row.get("source_document_id")) for row in route_rows if clean(row.get("source_document_id"))})
        claims = load_claims(con, source_document_ids)
        existing_stat_keys, existing_note_keys = load_existing_atom_keys(con, promotion_apply_run_id)
    finally:
        con.close()

    team_games = load_team_games(args.team_games)
    player_index = load_player_index(args.player_index)
    generated_rows = build_generated_decisions(
        run_id,
        route_rows,
        claims,
        team_games,
        player_index,
        existing_stat_keys,
        existing_note_keys,
    )
    semantic_note_overrides = build_source_note_overrides(route_rows)

    base_input = args.base_decision_input_csv or latest_decision_input(args.root)
    base_rows = read_base_decisions(base_input)
    combined_rows = combine_decision_inputs(base_rows, semantic_note_overrides)

    decision_input_path = out_dir / "decision_input.csv"
    summary = {
        "created_at_utc": created_at,
        "generated_atom_decision_run_id": run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "base_decision_input_csv": str(base_input) if base_input else "",
        "decision_input_csv": str(decision_input_path),
        "semantic_route_rows_read": len(route_rows),
        "source_document_count": len(source_document_ids),
        "claims_read": len(claims),
        "generated_decision_count": len(generated_rows),
        "semantic_note_override_count": len(semantic_note_overrides),
        "generated_target_table_counts": dict(Counter(row["target_table"] for row in generated_rows)),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "generated_atom_decisions.csv", generated_rows, GENERATED_DECISION_FIELDS)
    write_csv(out_dir / "decision_input_delta.csv", semantic_note_overrides, DECISION_INPUT_FIELDS)
    write_csv(decision_input_path, combined_rows, DECISION_INPUT_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "semantic_claim_resolution_report.md", summary, generated_rows, semantic_note_overrides)
    if not args.no_db:
        persist(args.db_path, run_id, promotion_apply_run_id, out_dir, generated_rows, semantic_note_overrides, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
