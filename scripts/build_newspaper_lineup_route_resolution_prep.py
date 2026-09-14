#!/usr/bin/env python
"""Resolve current lineup quality-route rows or preserve them as notes.

This station consumes the latest local route queue, not the older package-only
prep tables. It promotes lineup rows only when player/team identity is clear.
Ambiguous or OCR-miss rows become `source_document_note` atoms carrying the raw
lineup evidence, so the conveyor can close without inventing identities.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "lineup_route_resolution_preps"
DEFAULT_PLAYER_INDEX = Path(r"D:\league-history-data\nfl\raw\pfr\players\player_index.parquet")
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")
DEFAULT_V26 = Path(
    r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet"
)

PROMOTION_VALUE = "approved_for_local_promotion"

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

ITEM_FIELDS = [
    "lineup_route_resolution_run_id",
    "promotion_apply_run_id",
    "decision_id",
    "boxscore_id",
    "game_date",
    "year",
    "week",
    "raw_team",
    "resolved_team",
    "opponent_team",
    "raw_player",
    "listed_position_raw",
    "participation_type",
    "resolved_target_table",
    "match_status",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_NFL_player_id",
    "resolved_player",
    "resolved_player_week",
    "candidate_count",
    "best_score",
    "second_score",
    "score_margin",
    "reason",
    "proposed_fields_json",
    "enriched_proposed_fields_json",
    "source_documents_json",
    "created_at_utc",
]

CANDIDATE_FIELDS = [
    "lineup_route_resolution_run_id",
    "decision_id",
    "boxscore_id",
    "raw_player",
    "raw_team",
    "listed_position_raw",
    "candidate_rank",
    "candidate_score",
    "candidate_pfr_id",
    "candidate_player",
    "candidate_position",
    "first_year",
    "last_year",
    "last_name_match",
    "initial_match",
    "position_match",
    "team_year_seen_in_v26",
    "match_notes",
    "created_at_utc",
]

RUN_FIELDS = [
    "lineup_route_resolution_run_id",
    "promotion_apply_run_id",
    "output_dir",
    "lineup_route_count",
    "approved_count",
    "note_count",
    "candidate_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

TEAM_ALIASES = {
    "akron": ["AKR"],
    "akron pros": ["AKR"],
    "akron indians": ["AKR"],
    "indians": ["AKR"],
    "buffalo": ["BUF", "BFF"],
    "buffalo all americans": ["BUF", "BFF"],
    "buffalo all-americans": ["BUF", "BFF"],
    "canton": ["CAN"],
    "canton bulldogs": ["CAN"],
    "bears": ["CHI"],
    "chicago bears": ["CHI"],
    "chicago cardinals": ["CRD"],
    "cardinals": ["CRD", "RAC"],
    "chicago staleys": ["CHI"],
    "decatur staleys": ["CHI"],
    "staleys": ["CHI"],
    "chicago tigers": ["CHT"],
    "cleveland": ["CLE"],
    "cleveland tigers": ["CLE"],
    "columbus": ["COL"],
    "columbus panhandle": ["COL"],
    "columbus panhandles": ["COL"],
    "panhandle": ["COL"],
    "panhandles": ["COL"],
    "dayton": ["DAY"],
    "dayton triangles": ["DAY"],
    "triangles": ["DAY"],
    "evansville": ["EVN"],
    "evansville crimson giants": ["EVN"],
    "green bay": ["GNB"],
    "green bay packers": ["GNB"],
    "hammond": ["HAM"],
    "hammond pros": ["HAM"],
    "independents": ["RII"],
    "marines": ["MIN"],
    "minneapolis": ["MIN"],
    "minneapolis marines": ["MIN"],
    "muncie": ["MUN"],
    "muncie flyers": ["MUN"],
    "oorang": ["OOR"],
    "oorang indians": ["OOR"],
    "racine cardinals": ["RAC"],
    "rochester": ["RCH"],
    "rochester jeffersons": ["RCH"],
    "rock island": ["RII"],
    "rock island independents": ["RII"],
}

POSITION_GROUPS = {
    "LT": "T",
    "RT": "T",
    "T": "T",
    "LG": "G",
    "RG": "G",
    "G": "G",
    "C": "C",
    "LE": "E",
    "RE": "E",
    "E": "E",
    "QB": "B",
    "LHB": "B",
    "RHB": "B",
    "HB": "B",
    "FB": "B",
    "BB": "B",
    "TB": "B",
    "WB": "B",
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
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def norm_words(value: Any) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", clean(value).lower()) if token]


def norm_text(value: Any) -> str:
    return " ".join(norm_words(value))


def norm_team_label(value: Any) -> str:
    tokens = norm_words(value)
    while tokens and tokens[-1].isdigit():
        tokens.pop()
    return " ".join(tokens)


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


def parse_int(value: Any) -> int | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def player_name_parts(raw_player: Any) -> tuple[str, str, str]:
    tokens = norm_words(raw_player)
    if tokens == ["bo", "mcmillan"]:
        tokens = ["bo", "mcmillin"]
    if not tokens:
        return "", "", ""
    first_initial = tokens[0][0] if len(tokens) > 1 and len(tokens[0]) == 1 else ""
    return " ".join(tokens), tokens[-1], first_initial


def position_group(value: Any) -> str:
    raw = clean(value).upper().replace("(", "").replace(")", "")
    return POSITION_GROUPS.get(raw, "")


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
        root / "lineup_route_resolution_preps" / "*" / "decision_input.csv",
        root / "ocr_completion_resolution_preps" / "*" / "decision_input.csv",
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


def load_route_rows(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
    include_visual_verify_holds: bool,
) -> list[dict[str, Any]]:
    if include_visual_verify_holds:
        route_filter = """
          (
            route_to_lane = 'quality_review'
            OR recommended_next_action = 'visual_verify_then_promote_or_hold'
          )
        """
    else:
        route_filter = "route_to_lane = 'quality_review'"
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND {route_filter}
          AND target_table = 'lineup_participation'
        ORDER BY boxscore_id, target_entity_key, decision_id
        """,
        [promotion_apply_run_id],
    )


def load_team_games(team_games_path: Path, boxscore_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not boxscore_ids:
        return {}
    con = duckdb.connect()
    try:
        placeholders = ",".join(["?"] * len(boxscore_ids))
        rows = query_dicts(
            con,
            f"""
            SELECT boxscore_id, CAST(game_date AS VARCHAR) AS game_date, year, week,
                   team_code, opponent_code
            FROM read_parquet(?)
            WHERE boxscore_id IN ({placeholders})
            """,
            [str(team_games_path), *boxscore_ids],
        )
    finally:
        con.close()
    return {f"{clean(row.get('boxscore_id'))}|{clean(row.get('team_code'))}": row for row in rows}


def load_player_index(player_index_path: Path, min_year: int, max_year: int) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        return query_dicts(
            con,
            """
            SELECT player, pfr_id, first_year, last_year, index_position
            FROM read_parquet(?)
            WHERE pfr_id IS NOT NULL
              AND player IS NOT NULL
              AND first_year <= ?
              AND last_year >= ?
            """,
            [str(player_index_path), max_year, min_year],
        )
    finally:
        con.close()


def load_v26_team_year_ids(v26_path: Path, min_year: int, max_year: int) -> set[tuple[str, str, int]]:
    con = duckdb.connect()
    try:
        rows = query_dicts(
            con,
            """
            SELECT DISTINCT NFL_player_id, nfl_team, CAST(year AS INTEGER) AS year
            FROM read_parquet(?)
            WHERE year BETWEEN ? AND ?
              AND NFL_player_id IS NOT NULL
              AND nfl_team IS NOT NULL
            """,
            [str(v26_path), min_year, max_year],
        )
    finally:
        con.close()
    return {
        (clean(row.get("NFL_player_id")), clean(row.get("nfl_team")), int(row.get("year")))
        for row in rows
        if clean(row.get("NFL_player_id")) and clean(row.get("nfl_team")) and row.get("year") is not None
    }


def resolve_team(raw_team: Any, boxscore_id: str, team_games: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    raw_code = clean(raw_team).upper()
    if raw_code:
        game = team_games.get(f"{boxscore_id}|{raw_code}", {})
        if game:
            return raw_code, game
    codes = TEAM_ALIASES.get(norm_team_label(raw_team), [])
    for code in codes:
        game = team_games.get(f"{boxscore_id}|{code}", {})
        if game:
            return code, game
    if not codes:
        return "", {}
    return "", {}


def score_candidate(
    candidate: dict[str, Any],
    raw_player: str,
    listed_position: str,
    resolved_team: str,
    year: int | None,
    v26_team_year_ids: set[tuple[str, str, int]],
) -> tuple[int, list[str]]:
    raw_norm, last_name, first_initial = player_name_parts(raw_player)
    player_norm, cand_last, _ = player_name_parts(candidate.get("player"))
    if not last_name or cand_last != last_name:
        return -999, ["last_name_mismatch"]
    score = 70
    notes = ["last_name_match"]
    if player_norm == raw_norm:
        score += 35
        notes.append("exact_name_match")
    if first_initial:
        first = norm_words(candidate.get("player"))[0] if norm_words(candidate.get("player")) else ""
        if first.startswith(first_initial):
            score += 20
            notes.append("initial_match")
        else:
            score -= 25
            notes.append("initial_mismatch")
    pos_group = position_group(listed_position)
    cand_positions = {token for token in re.split(r"[^A-Z]+", clean(candidate.get("index_position")).upper()) if token}
    cand_groups = {POSITION_GROUPS.get(token, token if token in {"E", "T", "G", "C", "B"} else "") for token in cand_positions}
    cand_groups.discard("")
    if pos_group:
        if pos_group in cand_groups:
            score += 15
            notes.append("position_group_match")
        else:
            score -= 10
            notes.append("position_group_mismatch")
    if year is not None and (clean(candidate.get("pfr_id")), resolved_team, year) in v26_team_year_ids:
        score += 35
        notes.append("team_year_seen_in_v26")
    return score, notes


def resolve_player(
    raw_player: str,
    listed_position: str,
    resolved_team: str,
    year: int | None,
    player_index: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, str, int]],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str, str]:
    candidates: list[dict[str, Any]] = []
    for candidate in player_index:
        first_year = parse_int(candidate.get("first_year"))
        last_year = parse_int(candidate.get("last_year"))
        if year is not None and first_year is not None and last_year is not None and not (first_year <= year <= last_year):
            continue
        score, notes = score_candidate(candidate, raw_player, listed_position, resolved_team, year, v26_team_year_ids)
        if score < 0:
            continue
        row = dict(candidate)
        row["candidate_score"] = score
        row["match_notes"] = ";".join(notes)
        candidates.append(row)
    candidates.sort(key=lambda row: (-int(row.get("candidate_score") or 0), clean(row.get("player"))))
    if not candidates:
        return None, [], "unresolved_no_player_index_candidate", "no player-index candidate matched last name/year"
    best = candidates[0]
    second_score = int(candidates[1].get("candidate_score") or 0) if len(candidates) > 1 else -1
    margin = int(best.get("candidate_score") or 0) - second_score
    best_score = int(best.get("candidate_score") or 0)
    if best_score >= 115 and margin >= 20:
        return best, candidates, "resolved_unique_high_confidence", "unique player-index/v26 match"
    if best_score >= 105 and len(candidates) == 1:
        return best, candidates, "resolved_single_candidate", "single player-index candidate"
    if best_score >= 85 and len(candidates) == 1 and "position_group_match" in clean(best.get("match_notes")):
        return best, candidates, "resolved_single_candidate_position_match", "single era candidate with lineup position match"
    return None, candidates, "held_ambiguous_player_identity", "candidate match is ambiguous or low confidence"


def lineup_payload(
    row: dict[str, Any],
    proposed: dict[str, Any],
    team_code: str,
    game: dict[str, Any],
    player: dict[str, Any],
) -> dict[str, Any]:
    year = parse_int(game.get("year"))
    week = parse_int(game.get("week"))
    nfl_player_id = clean(player.get("pfr_id"))
    player_week = f"{nfl_player_id}_{year}_{week}" if nfl_player_id and year is not None and week is not None else ""
    listed_position = clean(proposed.get("listed_position_raw")) or clean(proposed.get("position_raw"))
    is_starter = clean(proposed.get("is_starter")) or ("1" if clean(proposed.get("lineup_role")).lower() == "starter" else "")
    participation_type = clean(proposed.get("participation_type")) or ("starter" if is_starter == "1" else clean(proposed.get("lineup_role")))
    return {
        "lineup_participation_id": stable_id("lineup_route", row.get("decision_id"), row.get("boxscore_id"), nfl_player_id, listed_position, participation_type),
        "boxscore_id": clean(row.get("boxscore_id")),
        "player_week": player_week,
        "player_raw": clean(proposed.get("player_raw")),
        "NFL_player_id": nfl_player_id,
        "team_raw": clean(proposed.get("team_raw")),
        "nfl_team": team_code,
        "opponent_nfl_team": clean(game.get("opponent_code")),
        "listed_position_raw": listed_position,
        "starter_position": listed_position if is_starter == "1" else "",
        "is_starter": is_starter,
        "participation_type": participation_type,
        "lineup_side_raw": clean(proposed.get("lineup_side_raw")),
        "source_row_text": clean(proposed.get("source_row_text")),
        "confidence_score": clean(proposed.get("confidence_score")) or "0.70",
        "review_status": "lineup_route_identity_resolved",
        "promotion_status": "local_atom_only",
        "source_document_id": clean(row.get("source_document_id")),
        "region_id": clean(proposed.get("region_id")),
    }


def source_note_payload(row: dict[str, Any], proposed: dict[str, Any], match_status: str, reason: str) -> dict[str, Any]:
    boxscore_id = clean(row.get("boxscore_id"))
    raw_player = clean(proposed.get("player_raw"))
    raw_team = clean(proposed.get("team_raw")) or clean(proposed.get("nfl_team"))
    listed_position = clean(proposed.get("listed_position_raw")) or clean(proposed.get("position_raw"))
    note_text = (
        f"Unpromoted lineup participation evidence for {raw_player} / {raw_team} "
        f"({listed_position or 'position unknown'}): {clean(proposed.get('source_row_text'))}. "
        f"Match status: {match_status}. Reason: {reason}."
    )
    return {
        "source_document_note_id": stable_id("lineup_identity_note", row.get("decision_id"), boxscore_id, raw_player, raw_team, listed_position),
        "source_document_id": clean(row.get("source_document_id")),
        "boxscore_id": boxscore_id,
        "note_type": "lineup_identity_unresolved",
        "note_category": "lineup_identity_review",
        "note_text": note_text,
        "related_target_table": "lineup_participation",
        "related_entity_key": clean(row.get("target_entity_key")),
        "reconciliation_status": match_status,
        "evidence_text": clean(proposed.get("source_row_text")),
        "confidence_score": "0.60",
        "review_status": "local_atom_lineup_identity_note_accepted",
        "promotion_status": "local_atom_only",
        "source_documents_json": clean(row.get("source_documents_json")),
        "artifact_path": clean(row.get("artifact_path")),
        "created_by_station": "build_newspaper_lineup_route_resolution_prep.py",
    }


def build_rows(
    run_id: str,
    promotion_apply_run_id: str,
    route_rows: list[dict[str, Any]],
    team_games: dict[str, dict[str, Any]],
    player_index: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, str, int]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    created_at = iso_now()
    resolution_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, str]] = []
    for row in route_rows:
        proposed = parse_json_obj(row.get("proposed_fields_json"))
        boxscore_id = clean(row.get("boxscore_id"))
        raw_team = clean(proposed.get("team_raw")) or clean(proposed.get("nfl_team"))
        team_code, game = resolve_team(raw_team, boxscore_id, team_games)
        year = parse_int(game.get("year"))
        listed_position = clean(proposed.get("listed_position_raw")) or clean(proposed.get("position_raw"))
        raw_player = clean(proposed.get("player_raw"))
        player, candidates, match_status, reason = resolve_player(raw_player, listed_position, team_code, year, player_index, v26_team_year_ids)
        if not team_code:
            player = None
            match_status = "held_unresolved_team"
            reason = "team alias could not be resolved"
        for idx, candidate in enumerate(candidates[:8], start=1):
            notes = clean(candidate.get("match_notes"))
            candidate_rows.append({
                "lineup_route_resolution_run_id": run_id,
                "decision_id": clean(row.get("decision_id")),
                "boxscore_id": boxscore_id,
                "raw_player": raw_player,
                "raw_team": raw_team,
                "listed_position_raw": listed_position,
                "candidate_rank": idx,
                "candidate_score": clean(candidate.get("candidate_score")),
                "candidate_pfr_id": clean(candidate.get("pfr_id")),
                "candidate_player": clean(candidate.get("player")),
                "candidate_position": clean(candidate.get("index_position")),
                "first_year": clean(candidate.get("first_year")),
                "last_year": clean(candidate.get("last_year")),
                "last_name_match": "1" if "last_name_match" in notes else "",
                "initial_match": "1" if "initial_match" in notes else "",
                "position_match": "1" if "position_group_match" in notes else "",
                "team_year_seen_in_v26": "1" if "team_year_seen_in_v26" in notes else "",
                "match_notes": notes,
                "created_at_utc": created_at,
            })
        best_score = clean(candidates[0].get("candidate_score")) if candidates else ""
        second_score = clean(candidates[1].get("candidate_score")) if len(candidates) > 1 else ""
        score_margin = ""
        if candidates:
            score_margin = str(int(candidates[0].get("candidate_score") or 0) - (int(candidates[1].get("candidate_score") or 0) if len(candidates) > 1 else -1))
        if player:
            payload = lineup_payload(row, proposed, team_code, game, player)
            target_table = "lineup_participation"
            target_key = f"lineup_participation|{boxscore_id}|{payload.get('NFL_player_id')}|{payload.get('starter_position') or payload.get('participation_type')}"
            status = "approved_for_local_promotion"
            value = PROMOTION_VALUE
            resolved_player = clean(player.get("player"))
            resolved_id = clean(player.get("pfr_id"))
            resolved_week = clean(payload.get("player_week"))
        else:
            payload = source_note_payload(row, proposed, match_status, reason)
            target_table = "source_document_note"
            target_key = f"source_document_note|lineup_identity|{boxscore_id}|{raw_team}|{raw_player}|{listed_position}"
            status = "approved_for_local_promotion"
            value = PROMOTION_VALUE
            resolved_player = ""
            resolved_id = ""
            resolved_week = ""
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        decision_rows.append({
            "decision_id": clean(row.get("decision_id")),
            "decision_status": status,
            "decision_value": value,
            "route_to_lane": "",
            "resolved_boxscore_id": boxscore_id,
            "resolved_target_table": target_table,
            "resolved_target_entity_key": target_key,
            "proposed_fields_json": payload_json,
            "notes": reason,
        })
        resolution_rows.append({
            "lineup_route_resolution_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "decision_id": clean(row.get("decision_id")),
            "boxscore_id": boxscore_id,
            "game_date": clean(game.get("game_date")),
            "year": clean(parse_int(game.get("year")) or ""),
            "week": clean(parse_int(game.get("week")) or ""),
            "raw_team": raw_team,
            "resolved_team": team_code,
            "opponent_team": clean(game.get("opponent_code")),
            "raw_player": raw_player,
            "listed_position_raw": listed_position,
            "participation_type": clean(proposed.get("participation_type")) or clean(proposed.get("lineup_role")),
            "resolved_target_table": target_table,
            "match_status": match_status,
            "decision_status": status,
            "decision_value": value,
            "route_to_lane": "",
            "resolved_NFL_player_id": resolved_id,
            "resolved_player": resolved_player,
            "resolved_player_week": resolved_week,
            "candidate_count": len(candidates),
            "best_score": best_score,
            "second_score": second_score,
            "score_margin": score_margin,
            "reason": reason,
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "enriched_proposed_fields_json": payload_json,
            "source_documents_json": clean(row.get("source_documents_json")),
            "created_at_utc": created_at,
        })
    return resolution_rows, candidate_rows, decision_rows


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
    resolution_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.lineup_route_resolution_item ("
            + ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.lineup_route_resolution_candidate ("
            + ", ".join(f"{field} VARCHAR" for field in CANDIDATE_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.lineup_route_resolution_run ("
            + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
            + ")"
        )
        for field in ITEM_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.lineup_route_resolution_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in CANDIDATE_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.lineup_route_resolution_candidate ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in RUN_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.lineup_route_resolution_run ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        con.execute("DELETE FROM newspaper_review.lineup_route_resolution_item WHERE lineup_route_resolution_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.lineup_route_resolution_candidate WHERE lineup_route_resolution_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.lineup_route_resolution_run WHERE lineup_route_resolution_run_id = ?", [run_id])
        insert_rows(con, "lineup_route_resolution_item", resolution_rows, ITEM_FIELDS)
        insert_rows(con, "lineup_route_resolution_candidate", candidate_rows, CANDIDATE_FIELDS)
        lineup_count = sum(1 for row in resolution_rows if clean(row.get("resolved_target_table")) == "lineup_participation")
        note_count = sum(1 for row in resolution_rows if clean(row.get("resolved_target_table")) == "source_document_note")
        insert_rows(con, "lineup_route_resolution_run", [{
            "lineup_route_resolution_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "output_dir": str(out_dir),
            "lineup_route_count": len(resolution_rows),
            "approved_count": lineup_count,
            "note_count": note_count,
            "candidate_count": len(candidate_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_report(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Lineup Route Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['lineup_route_resolution_run_id']}`",
        f"Apply source: `{summary['promotion_apply_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Route rows: `{summary['lineup_route_count']}`",
        f"- Promoted lineup rows: `{summary['approved_count']}`",
        f"- Preserved notes: `{summary['note_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Items",
        "",
        "| target | boxscore | team | player | pos | status | reason |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("resolved_target_table")),
                clean(row.get("boxscore_id")),
                clean(row.get("resolved_team")) or clean(row.get("raw_team")),
                clean(row.get("raw_player")),
                clean(row.get("listed_position_raw")),
                clean(row.get("match_status")),
                clean(row.get("reason")),
            ])
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_lineup_route_resolution")
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument(
        "--include-visual-verify-holds",
        action="store_true",
        help="Also resolve current visual-verify lineup hold rows from the route queue.",
    )
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
        route_rows = load_route_rows(con, promotion_apply_run_id, args.include_visual_verify_holds)
    finally:
        con.close()

    boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in route_rows if clean(row.get("boxscore_id"))})
    team_games = load_team_games(args.team_games_path, boxscore_ids)
    years = [parse_int(row.get("year")) for row in team_games.values() if parse_int(row.get("year")) is not None]
    min_year = min(years) if years else 1920
    max_year = max(years) if years else 1939
    player_index = load_player_index(args.player_index_path, min_year, max_year)
    v26_team_year_ids = load_v26_team_year_ids(args.v26_path, min_year, max_year)
    resolution_rows, candidate_rows, decision_rows = build_rows(
        run_id,
        promotion_apply_run_id,
        route_rows,
        team_games,
        player_index,
        v26_team_year_ids,
    )

    base_input = args.base_decision_input_csv or latest_decision_input(args.root)
    base_rows = read_base_decisions(base_input)
    combined_rows = combine_decision_inputs(base_rows, decision_rows)

    decision_input_path = out_dir / "decision_input.csv"
    available_teams_by_boxscore: dict[str, list[str]] = {}
    for key in team_games:
        boxscore_id, _, team_code = key.partition("|")
        if boxscore_id and team_code:
            available_teams_by_boxscore.setdefault(boxscore_id, []).append(team_code)
    unresolved_team_examples = []
    for row in resolution_rows:
        if clean(row.get("match_status")) != "held_unresolved_team":
            continue
        unresolved_team_examples.append({
            "boxscore_id": clean(row.get("boxscore_id")),
            "raw_team": clean(row.get("raw_team")),
            "available_team_codes": sorted(available_teams_by_boxscore.get(clean(row.get("boxscore_id")), [])),
        })
        if len(unresolved_team_examples) >= 20:
            break

    summary = {
        "created_at_utc": created_at,
        "lineup_route_resolution_run_id": run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "base_decision_input_csv": str(base_input) if base_input else "",
        "decision_input_csv": str(decision_input_path),
        "lineup_route_count": len(route_rows),
        "approved_count": sum(1 for row in resolution_rows if clean(row.get("resolved_target_table")) == "lineup_participation"),
        "note_count": sum(1 for row in resolution_rows if clean(row.get("resolved_target_table")) == "source_document_note"),
        "candidate_count": len(candidate_rows),
        "match_status_counts": dict(Counter(clean(row.get("match_status")) for row in resolution_rows)),
        "include_visual_verify_holds": bool(args.include_visual_verify_holds),
        "raw_team_counts": dict(Counter(clean(row.get("raw_team")) for row in resolution_rows).most_common(25)),
        "unresolved_team_counts": dict(
            Counter(
                clean(row.get("raw_team"))
                for row in resolution_rows
                if clean(row.get("match_status")) == "held_unresolved_team"
            ).most_common(25)
        ),
        "unresolved_team_examples": unresolved_team_examples,
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "lineup_route_resolution_items.csv", resolution_rows, ITEM_FIELDS)
    write_csv(out_dir / "lineup_route_resolution_candidates.csv", candidate_rows, CANDIDATE_FIELDS)
    write_csv(out_dir / "decision_input_delta.csv", decision_rows, DECISION_INPUT_FIELDS)
    write_csv(decision_input_path, combined_rows, DECISION_INPUT_FIELDS)
    write_json(summary_path, summary)
    write_report(out_dir / "lineup_route_resolution_report.md", summary, resolution_rows)
    if not args.no_db:
        persist(args.db_path, run_id, promotion_apply_run_id, out_dir, resolution_rows, candidate_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
