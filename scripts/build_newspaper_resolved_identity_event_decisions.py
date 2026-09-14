#!/usr/bin/env python
"""Patch and approve safe identity-blocked resolved newspaper event rows.

This station reads the current resolved package route queue and handles only
`identity_review` rows. It writes decision-override CSVs that can be fed back
into `build_newspaper_resolved_package_decision_ledger.py`.

Safety posture:
- local-only: D-drive artifacts plus the local newspaper DuckDB review schema
- no writes to Fly, v26, or the live supertable
- auto-patches only identities anchored by existing local identity candidates,
  event-detail resolver candidates, exact/full-name matches, a unique last-name
  candidate observed for the same team/year in v26, or a unique active-year
  last-name candidate when no first-name conflict is present
- permits a scoring event whose scorer/receiver is already identified even if
  an auxiliary raw passer remains unresolved, so the conveyor keeps moving
- closes duplicate package items as corroborating archives instead of promoting
  the same atom twice
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_newspaper_identity_resolution_from_readiness import (  # noqa: E402
    DEFAULT_PLAYER_INDEX,
    DEFAULT_TEAM_GAMES,
    DEFAULT_V26,
    clean,
    load_player_index,
    load_team_games,
    load_v26_team_year_ids,
    norm,
    norm_key,
    parse_int,
    player_index_candidates,
    query_dicts,
    resolve_team,
    year_from_boxscore,
)


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_identity_event_decisions"

APPROVED_FIELDS = [
    "package_item_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "proposed_fields_json",
    "notes",
]

CLOSED_OVERRIDE_FIELDS = APPROVED_FIELDS

AUDIT_FIELDS = [
    "identity_event_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "route_to_lane",
    "decision_status",
    "decision_value",
    "approval_class",
    "reason",
    "missing_before_json",
    "resolved_roles_json",
    "remaining_missing_json",
    "candidate_summary_json",
    "evidence_text",
    "source_documents_json",
    "original_proposed_fields_json",
    "patched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "identity_event_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_class_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

RECEIVING_EVENT_TYPES = {
    "receiving_touchdown",
    "touchdown_pass",
    "touchdown_reception",
    "td_reception",
    "pass_touchdown",
}

EVENT_DETAIL_ROLE_ALIASES = {
    "scoring": ["scoring", "primary"],
    "receiver": ["receiver", "primary"],
    "passer": ["passer"],
    "primary": ["primary"],
    "secondary": ["secondary"],
}


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_list(value: Any) -> list[str]:
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


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")


def latest_decision_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT resolved_package_decision_run_id
        FROM newspaper_review.resolved_package_decision_ledger_run
        ORDER BY created_at_utc DESC, resolved_package_decision_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_rows(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.resolved_package_decision_route_queue
        WHERE resolved_package_decision_run_id = ?
          AND route_to_lane = 'identity_review'
          AND target_table IN ('scoring_event', 'play_by_play_event')
        ORDER BY target_table, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def source_date_matches(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    if len(boxscore_id) < 8:
        return False
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    for doc in docs:
        match = re.match(r"([^#:]+)", doc)
        if match and match.group(1)[:8] == boxscore_id[:8]:
            return True
    return False


def event_type(proposed: dict[str, Any]) -> str:
    return clean(proposed.get("event_type")).lower()


def is_receiving_event(proposed: dict[str, Any]) -> bool:
    kind = event_type(proposed)
    return kind in RECEIVING_EVENT_TYPES or ("receiv" in kind and "touchdown" in kind)


def role_specs(target_table: str, proposed: dict[str, Any]) -> list[dict[str, str]]:
    roles: list[dict[str, str]] = []
    if target_table == "scoring_event":
        raw_team = clean(proposed.get("scoring_team_raw")) or clean(proposed.get("scoring_team"))
        specs = [
            ("scoring", "scoring_player_raw", "scoring_NFL_player_id"),
            ("passer", "passer_raw", "passer_NFL_player_id"),
            ("receiver", "receiver_raw", "receiver_NFL_player_id"),
        ]
        for role, raw_field, id_field in specs:
            raw_player = clean(proposed.get(raw_field))
            if raw_player and not clean(proposed.get(id_field)):
                roles.append({
                    "role": role,
                    "raw_player": raw_player,
                    "raw_team": raw_team,
                    "target_id_field": id_field,
                })
        if is_receiving_event(proposed) and clean(proposed.get("scoring_player_raw")):
            if not clean(proposed.get("receiver_raw")) and not clean(proposed.get("receiver_NFL_player_id")):
                roles.append({
                    "role": "receiver",
                    "raw_player": clean(proposed.get("scoring_player_raw")),
                    "raw_team": raw_team,
                    "target_id_field": "receiver_NFL_player_id",
                })
    elif target_table == "play_by_play_event":
        raw_team = clean(proposed.get("possession_team_raw")) or clean(proposed.get("possession_team"))
        specs = [
            ("primary", "primary_player_raw", "primary_NFL_player_id"),
            ("secondary", "secondary_player_raw", "secondary_NFL_player_id"),
        ]
        for role, raw_field, id_field in specs:
            raw_player = clean(proposed.get(raw_field))
            if raw_player and not clean(proposed.get(id_field)):
                roles.append({
                    "role": role,
                    "raw_player": raw_player,
                    "raw_team": raw_team,
                    "target_id_field": id_field,
                })
    return roles


def load_final_identity_bridges(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    rows = query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_final.player_identity_candidate
        WHERE COALESCE(NFL_player_id, '') <> ''
        ORDER BY created_at_utc, package_item_id
        """,
    )
    output: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        boxscore_id = clean(row.get("boxscore_id"))
        raw_player = norm(row.get("raw_player_name"))
        raw_team = norm_key(row.get("raw_team")) or clean(row.get("nfl_team")).lower()
        if not boxscore_id or not raw_player:
            continue
        output.setdefault((boxscore_id, raw_player, raw_team), []).append(row)
    return output


def load_event_detail_candidates(
    con: duckdb.DuckDBPyConnection,
    decision_run_id: str,
) -> dict[tuple[str, str, str, str], list[dict[str, Any]]]:
    try:
        rows = query_dicts(
            con,
            """
            WITH routes AS (
              SELECT
                package_item_id,
                target_table,
                boxscore_id,
                json_extract_string(proposed_fields_json, '$.decision_id') AS decision_id
              FROM newspaper_review.resolved_package_decision_route_queue
              WHERE resolved_package_decision_run_id = ?
                AND route_to_lane = 'identity_review'
            )
            SELECT
              r.decision_id,
              r.boxscore_id,
              r.target_table,
              c.candidate_role,
              c.raw_player,
              c.raw_team,
              c.candidate_rank,
              c.candidate_score,
              c.candidate_pfr_id,
              c.candidate_player,
              c.candidate_position,
              c.first_year,
              c.last_year,
              c.match_notes
            FROM routes r
            JOIN newspaper_review.event_detail_resolution_candidate c
              ON r.decision_id = c.decision_id
             AND r.boxscore_id = c.boxscore_id
             AND r.target_table = c.target_table
            WHERE COALESCE(c.candidate_pfr_id, '') <> ''
            ORDER BY
              r.decision_id,
              r.boxscore_id,
              r.target_table,
              c.candidate_role,
              TRY_CAST(c.candidate_rank AS INTEGER),
              TRY_CAST(c.candidate_score AS INTEGER) DESC
            """,
            [decision_run_id],
        )
    except Exception:
        return {}

    output: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        notes = parse_obj(row.get("match_notes"))
        notes.update({
            "candidate_rank": clean(row.get("candidate_rank")),
            "raw_player": clean(row.get("raw_player")),
            "raw_team": clean(row.get("raw_team")),
        })
        candidate = {
            "candidate_score": parse_int(row.get("candidate_score")) or 0,
            "candidate_NFL_player_id": clean(row.get("candidate_pfr_id")),
            "candidate_player": clean(row.get("candidate_player")),
            "candidate_position": clean(row.get("candidate_position")),
            "first_year": clean(row.get("first_year")),
            "last_year": clean(row.get("last_year")),
            "candidate_source": "event_detail_resolution_candidate",
            "candidate_role": clean(row.get("candidate_role")).lower(),
            "match_notes": json.dumps(notes, sort_keys=True, ensure_ascii=True),
        }
        key = (
            clean(row.get("decision_id")),
            clean(row.get("boxscore_id")),
            clean(row.get("target_table")),
            clean(row.get("candidate_role")).lower(),
        )
        output.setdefault(key, []).append(candidate)
    return output


def event_detail_candidates(
    proposed: dict[str, Any],
    row: dict[str, Any],
    target_table: str,
    role: dict[str, str],
    event_candidates: dict[tuple[str, str, str, str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    decision_id = clean(proposed.get("decision_id"))
    boxscore_id = clean(row.get("boxscore_id")) or clean(proposed.get("boxscore_id"))
    raw_player = norm(role.get("raw_player"))
    aliases = EVENT_DETAIL_ROLE_ALIASES.get(clean(role.get("role")).lower(), [clean(role.get("role")).lower()])
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for alias in aliases:
        key = (decision_id, boxscore_id, target_table, alias)
        for candidate in event_candidates.get(key, []):
            notes = candidate_notes(candidate)
            candidate_raw = norm(notes.get("raw_player"))
            if candidate_raw and raw_player and candidate_raw != raw_player:
                continue
            nfl_id = clean(candidate.get("candidate_NFL_player_id"))
            if nfl_id and nfl_id not in seen:
                output.append(candidate)
                seen.add(nfl_id)
    return output


def bridge_candidates(
    raw_player: str,
    raw_team: str,
    boxscore_id: str,
    bridges: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    keys = [
        (boxscore_id, norm(raw_player), norm_key(raw_team)),
        (boxscore_id, norm(raw_player), clean(raw_team).lower()),
    ]
    rows: list[dict[str, Any]] = []
    for key in keys:
        rows = bridges.get(key, [])
        if rows:
            break
    candidates: list[dict[str, Any]] = []
    for row in rows:
        candidates.append({
            "candidate_score": 95,
            "candidate_NFL_player_id": clean(row.get("NFL_player_id")),
            "candidate_player": clean(row.get("resolved_player")),
            "candidate_position": "",
            "first_year": "",
            "last_year": "",
            "candidate_source": "existing_final_player_identity_candidate",
            "match_notes": json.dumps({
                "bridge_match_method": clean(row.get("match_method")),
                "bridge_confidence_score": clean(row.get("confidence_score")),
            }, sort_keys=True),
        })
    return candidates


def candidate_notes(candidate: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = json.loads(clean(candidate.get("match_notes")))
    except json.JSONDecodeError:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


def choose_role_resolution(candidates: list[dict[str, Any]]) -> tuple[bool, str]:
    if not candidates:
        return False, "no candidate matched player name/year"
    best = candidates[0]
    best_score = int(best.get("candidate_score") or 0)
    second_score = int(candidates[1].get("candidate_score") or 0) if len(candidates) > 1 else 0
    margin = best_score - second_score
    source = clean(best.get("candidate_source"))
    notes = candidate_notes(best)
    last_name_exact = clean(notes.get("last_name_match")) == "exact"
    first_match = clean(notes.get("first_match"))
    if source == "existing_final_player_identity_candidate" and best_score >= 90:
        return True, "matched existing local final identity candidate"
    if best_score >= 100 and (len(candidates) == 1 or margin >= 20):
        return True, "high-confidence full-name player-index match"
    if best_score >= 80 and len(candidates) == 1 and clean(notes.get("team_year_seen_in_v26")) == "1":
        return True, "unique last-name candidate observed for same team/year in v26"
    if (
        best_score >= 60
        and len(candidates) == 1
        and source in {"pfr_player_index", "event_detail_resolution_candidate"}
        and last_name_exact
        and first_match in {"", "not_supplied", "full_name", "initial"}
    ):
        return True, "unique active-year last-name candidate without first-name conflict"
    return False, "candidate exists but is not auto-safe"


def patch_role(proposed: dict[str, Any], target_table: str, role: dict[str, str], candidate: dict[str, Any], team_code: str) -> None:
    nfl_id = clean(candidate.get("candidate_NFL_player_id"))
    player = clean(candidate.get("candidate_player"))
    id_field = clean(role.get("target_id_field"))
    role_name = clean(role.get("role"))
    if id_field:
        proposed[id_field] = nfl_id
    if target_table == "scoring_event":
        if team_code and not clean(proposed.get("scoring_team")):
            proposed["scoring_team"] = team_code
        if role_name == "scoring" and is_receiving_event(proposed):
            if not clean(proposed.get("receiver_raw")):
                proposed["receiver_raw"] = clean(proposed.get("scoring_player_raw"))
            if not clean(proposed.get("receiver_NFL_player_id")):
                proposed["receiver_NFL_player_id"] = nfl_id
        if role_name == "receiver" and not clean(proposed.get("scoring_NFL_player_id")):
            scoring_raw = norm(proposed.get("scoring_player_raw"))
            receiver_raw = norm(role.get("raw_player"))
            if scoring_raw and scoring_raw == receiver_raw:
                proposed["scoring_NFL_player_id"] = nfl_id
    elif target_table == "play_by_play_event":
        if team_code and not clean(proposed.get("possession_team")):
            proposed["possession_team"] = team_code
    proposed[f"{role_name}_resolved_player"] = player


def unresolved_roles(target_table: str, proposed: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "role": role["role"],
            "raw_player": role["raw_player"],
            "target_id_field": role["target_id_field"],
        }
        for role in role_specs(target_table, proposed)
    ]


def core_scoring_identified(proposed: dict[str, Any]) -> bool:
    if clean(proposed.get("scoring_player_raw")) and not clean(proposed.get("scoring_NFL_player_id")):
        return False
    if is_receiving_event(proposed):
        if clean(proposed.get("receiver_raw")) and not clean(proposed.get("receiver_NFL_player_id")):
            return False
        if not clean(proposed.get("receiver_NFL_player_id")) and not clean(proposed.get("scoring_NFL_player_id")):
            return False
    return bool(clean(proposed.get("points")) or "touchdown" in event_type(proposed))


def classify_approval(target_table: str, proposed: dict[str, Any], remaining: list[dict[str, str]]) -> tuple[bool, str, str]:
    if not remaining:
        return True, "fully_identity_patched", "all raw player roles have local IDs"
    if target_table == "scoring_event" and core_scoring_identified(proposed):
        only_aux_passer = all(role["role"] == "passer" for role in remaining)
        if only_aux_passer:
            return True, "core_scoring_identity_approved", "core scoring/receiver identity is present; unresolved raw passer retained"
    return False, "held_identity_review", "one or more required player identities remain unresolved"


def duplicate_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    proposed = parse_obj(row.get("proposed_fields_json"))
    target_table = clean(row.get("target_table"))
    boxscore_id = clean(row.get("boxscore_id"))
    event_id = clean(proposed.get("pbp_event_id")) or clean(proposed.get("scoring_event_id"))
    decision_id = clean(proposed.get("decision_id"))
    play_text = norm(proposed.get("play_text"))
    unique_id = event_id or decision_id
    if not target_table or not boxscore_id or not unique_id:
        return ("", "", "", "")
    return (target_table, boxscore_id, unique_id, play_text)


def canonical_score(row: dict[str, Any]) -> tuple[int, int, str]:
    proposed = parse_obj(row.get("proposed_fields_json"))
    score = 0
    if clean(proposed.get("review_status")) == "event_detail_resolved":
        score += 100
    target_key = clean(row.get("target_entity_key"))
    team = clean(proposed.get("possession_team")) or clean(proposed.get("scoring_team"))
    if team and f"|{team}|" in target_key:
        score += 20
    if clean(proposed.get("distance_yards")) or clean(proposed.get("yardline_raw")):
        score += 10
    if clean(proposed.get("decision_ledger_run_id")) > "20260626":
        score += 5
    return (score, len(target_key), clean(row.get("package_item_id")))


def duplicate_closure_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = duplicate_key(row)
        if key[0]:
            groups.setdefault(key, []).append(row)

    output: dict[str, str] = {}
    for group_rows in groups.values():
        if len(group_rows) < 2:
            continue
        canonical = sorted(group_rows, key=canonical_score, reverse=True)[0]
        canonical_id = clean(canonical.get("package_item_id"))
        for row in group_rows:
            package_item_id = clean(row.get("package_item_id"))
            if package_item_id and package_item_id != canonical_id:
                output[package_item_id] = canonical_id
    return output


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    team_games: dict[str, list[dict[str, Any]]],
    player_index: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, int, str]],
    bridges: dict[tuple[str, str, str], list[dict[str, Any]]],
    event_candidates: dict[tuple[str, str, str, str], list[dict[str, Any]]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    duplicate_map = duplicate_closure_map(rows)
    for row in rows:
        target_table = clean(row.get("target_table"))
        proposed = parse_obj(row.get("proposed_fields_json"))
        patched = dict(proposed)
        initial_missing = unresolved_roles(target_table, patched)
        resolved_roles: list[dict[str, Any]] = []
        candidate_summary: list[dict[str, Any]] = []
        source_ok = source_date_matches(row, patched)
        package_item_id = clean(row.get("package_item_id"))

        if package_item_id in duplicate_map:
            canonical_id = duplicate_map[package_item_id]
            patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
            reason = f"duplicate package item closed; canonical package {canonical_id} remains active"
            audit_row = {
                "identity_event_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "package_item_id": package_item_id,
                "target_table": target_table,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "route_to_lane": clean(row.get("route_to_lane")),
                "decision_status": "closed",
                "decision_value": "corroboration_archive",
                "approval_class": "duplicate_package_item_closed",
                "reason": reason,
                "missing_before_json": json.dumps(initial_missing, sort_keys=True, ensure_ascii=True),
                "resolved_roles_json": "[]",
                "remaining_missing_json": json.dumps(initial_missing, sort_keys=True, ensure_ascii=True),
                "candidate_summary_json": "[]",
                "evidence_text": clean(row.get("evidence_text")),
                "source_documents_json": clean(row.get("source_documents_json")),
                "original_proposed_fields_json": clean(row.get("proposed_fields_json")),
                "patched_proposed_fields_json": patched_json,
                "created_at_utc": created_at,
            }
            audit.append(audit_row)
            closed.append({
                "package_item_id": package_item_id,
                "decision_status": "closed",
                "decision_value": "corroboration_archive",
                "route_to_lane": "archive_duplicate",
                "target_table": target_table,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "proposed_fields_json": patched_json,
                "notes": reason,
            })
            continue

        for role in initial_missing:
            raw_player = clean(role.get("raw_player"))
            raw_team = clean(proposed.get("scoring_team_raw") if target_table == "scoring_event" else proposed.get("possession_team_raw"))
            raw_team = raw_team or clean(role.get("raw_team"))
            boxscore_id = clean(row.get("boxscore_id")) or clean(patched.get("boxscore_id"))
            team_code, opponent, game_row, _team_method = resolve_team(raw_team, boxscore_id, team_games)
            year = parse_int(game_row.get("year")) or year_from_boxscore(boxscore_id) or 0
            candidates = bridge_candidates(raw_player, raw_team, boxscore_id, bridges)
            if not candidates:
                candidates = event_detail_candidates(patched, row, target_table, role, event_candidates)
            if not candidates:
                candidates = player_index_candidates(raw_player, year, team_code, player_index, v26_team_year_ids)
            safe, reason = choose_role_resolution(candidates)
            best = candidates[0] if candidates else {}
            candidate_summary.append({
                "role": clean(role.get("role")),
                "raw_player": raw_player,
                "raw_team": raw_team,
                "team_code": team_code,
                "safe": safe,
                "reason": reason,
                "candidate_count": len(candidates),
                "best": {
                    "NFL_player_id": clean(best.get("candidate_NFL_player_id")),
                    "player": clean(best.get("candidate_player")),
                    "score": clean(best.get("candidate_score")),
                    "source": clean(best.get("candidate_source")),
                    "match_notes": clean(best.get("match_notes")),
                },
            })
            if safe and source_ok and best:
                role_patch = dict(role)
                role_patch["raw_team"] = raw_team
                patch_role(patched, target_table, role_patch, best, team_code)
                resolved_roles.append({
                    "role": clean(role.get("role")),
                    "raw_player": raw_player,
                    "NFL_player_id": clean(best.get("candidate_NFL_player_id")),
                    "player": clean(best.get("candidate_player")),
                    "reason": reason,
                })

        remaining = unresolved_roles(target_table, patched)
        ok, approval_class, reason = classify_approval(target_table, patched, remaining)
        if ok and not source_ok:
            ok = False
            approval_class = "held_source_date_review"
            reason = "source document date does not match boxscore_id"

        patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
        audit_row = {
            "identity_event_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "target_table": target_table,
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else "needs_identity",
            "approval_class": approval_class,
            "reason": reason,
            "missing_before_json": json.dumps(initial_missing, sort_keys=True, ensure_ascii=True),
            "resolved_roles_json": json.dumps(resolved_roles, sort_keys=True, ensure_ascii=True),
            "remaining_missing_json": json.dumps(remaining, sort_keys=True, ensure_ascii=True),
            "candidate_summary_json": json.dumps(candidate_summary, sort_keys=True, ensure_ascii=True),
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "original_proposed_fields_json": clean(row.get("proposed_fields_json")),
            "patched_proposed_fields_json": patched_json,
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok:
            approved.append({
                "package_item_id": package_item_id,
                "decision_status": "approved",
                "decision_value": "approved_for_local_newspaper_final",
                "route_to_lane": f"local_final_{target_table}",
                "target_table": target_table,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "proposed_fields_json": patched_json,
                "notes": f"{approval_class}: {reason}",
            })
        else:
            held.append(audit_row)
    return approved, closed, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_identity_event_decision_run (
          identity_event_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          approved_count INTEGER,
          held_count INTEGER,
          approved_class_counts_json VARCHAR,
          hold_reason_counts_json VARCHAR,
          approved_csv VARCHAR,
          held_csv VARCHAR,
          audit_csv VARCHAR,
          summary_json VARCHAR,
          persisted_to_duckdb BOOLEAN,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_identity_event_decision_item (
          identity_event_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          package_item_id VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          boxscore_id VARCHAR,
          route_to_lane VARCHAR,
          decision_status VARCHAR,
          decision_value VARCHAR,
          approval_class VARCHAR,
          reason VARCHAR,
          missing_before_json VARCHAR,
          resolved_roles_json VARCHAR,
          remaining_missing_json VARCHAR,
          candidate_summary_json VARCHAR,
          evidence_text VARCHAR,
          source_documents_json VARCHAR,
          original_proposed_fields_json VARCHAR,
          patched_proposed_fields_json VARCHAR,
          created_at_utc VARCHAR
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


def persist(db_path: Path, run_row: dict[str, Any], audit_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("identity_event_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_identity_event_decision_run WHERE identity_event_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_identity_event_decision_item WHERE identity_event_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_identity_event_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_identity_event_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Identity Event Decisions",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Closed duplicates: `{summary['closed_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Approval classes: `{summary['approved_class_counts']}`",
        f"- Hold reasons: `{summary['hold_reason_counts']}`",
        "",
        "## Rows",
        "",
        "| status | table | boxscore | class | reason | resolved | remaining | evidence |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in audit_rows:
        evidence = clean(row.get("evidence_text")).replace("|", "\\|")[:140]
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("decision_status")),
                clean(row.get("target_table")),
                clean(row.get("boxscore_id")),
                clean(row.get("approval_class")),
                clean(row.get("reason")).replace("|", "\\|"),
                clean(row.get("resolved_roles_json")).replace("|", "\\|")[:140],
                clean(row.get("remaining_missing_json")).replace("|", "\\|")[:140],
                evidence,
            ])
            + " |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Decision overrides: `{summary['approved_csv']}`",
        f"- Closed overrides: `{summary['closed_csv']}`",
        f"- Held rows: `{summary['held_csv']}`",
        f"- Audit CSV: `{summary['audit_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_identity_event_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
        bridges = load_final_identity_bridges(read_con)
        event_candidates = load_event_detail_candidates(read_con, decision_run_id)
    finally:
        read_con.close()

    boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in rows if clean(row.get("boxscore_id"))})
    years = [year_from_boxscore(boxscore_id) for boxscore_id in boxscore_ids]
    min_year = min(year for year in years if year) if any(years) else 1920
    max_year = max(year for year in years if year) if any(years) else 1939

    ext_con = duckdb.connect()
    try:
        team_games = load_team_games(ext_con, args.team_games_path, boxscore_ids)
        player_index = load_player_index(ext_con, args.player_index_path, min_year, max_year)
        v26_team_year_ids = load_v26_team_year_ids(ext_con, args.v26_path, min_year, max_year)
    finally:
        ext_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    approved, closed, held, audit_rows = build_rows(
        rows,
        run_id,
        decision_run_id,
        team_games,
        player_index,
        v26_team_year_ids,
        bridges,
        event_candidates,
        created_at,
    )

    approved_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "approved")
    closed_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "closed")
    hold_reason_counts = Counter(row["reason"] for row in held)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    closed_csv = out_dir / "closed_decision_overrides.csv"
    held_csv = out_dir / "held_identity_event_rows.csv"
    audit_csv = out_dir / "identity_event_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "identity_event_decision_report.md"

    summary = {
        "identity_event_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "closed_count": len(closed),
        "held_count": len(held),
        "approved_class_counts": dict(approved_class_counts),
        "closed_class_counts": dict(closed_class_counts),
        "hold_reason_counts": dict(hold_reason_counts),
        "approved_csv": str(approved_csv),
        "closed_csv": str(closed_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": args.dry_run,
    }

    write_csv(approved_csv, approved + closed, APPROVED_FIELDS)
    write_csv(closed_csv, closed, CLOSED_OVERRIDE_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit_rows, AUDIT_FIELDS)
    write_json(summary_json, summary)
    report_md.write_text(render_markdown(summary, audit_rows), encoding="utf-8")

    if not args.dry_run:
        persist(
            args.db_path,
            {
                "identity_event_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": len(approved),
                "held_count": len(held),
                "approved_class_counts_json": json.dumps(dict(approved_class_counts), sort_keys=True),
                "hold_reason_counts_json": json.dumps(dict(hold_reason_counts), sort_keys=True),
                "approved_csv": str(approved_csv),
                "held_csv": str(held_csv),
                "audit_csv": str(audit_csv),
                "summary_json": str(summary_json),
                "persisted_to_duckdb": True,
                "created_at_utc": created_at,
            },
            audit_rows,
        )

    print(json.dumps({
        "identity_event_decision_run_id": run_id,
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "closed_count": len(closed),
        "held_count": len(held),
        "approved_class_counts": dict(approved_class_counts),
        "closed_class_counts": dict(closed_class_counts),
        "hold_reason_counts": dict(hold_reason_counts),
        "approved_csv": str(approved_csv),
        "closed_csv": str(closed_csv),
        "report_md": str(report_md),
        "dry_run": args.dry_run,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
