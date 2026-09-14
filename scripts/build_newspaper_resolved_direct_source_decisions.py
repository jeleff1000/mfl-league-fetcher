#!/usr/bin/env python
"""Approve directly supported resolved newspaper rows from source text/visual review.

This station drains conservative portions of the `ready_resolved_atom_review`
lane. It is local-only and writes D-drive artifacts plus `newspaper_review`
audit tables.

Rules are deliberately narrow:
- exact source-game document required
- scoring/PBP/team-stat facts must be directly supported by evidence text
- raw named players must be named in the evidence, with tiny spelling tolerance
- aliases such as "Indian star" or "former N.Y.U. star" remain held
- duplicate package rows are closed as corroborating archives when the same
  event is already in `newspaper_final` or a better canonical routed row exists
- one inspected lineup summary crop is supported by an explicit visual-support
  map, so the audit says exactly why those rows moved
"""
from __future__ import annotations

import argparse
import csv
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
    DEFAULT_TEAM_GAMES,
    TEAM_ALIASES,
    clean,
    norm_key,
    parse_int,
    query_dicts,
)


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_direct_source_decisions"

VISUAL_LINEUP_SUPPORT = {
    "192410120chi#3:333935461": {
        "crop_path": r"D:\league-history-data\nfl\derived\newspaper_atoms\article_atom_rounds\20260628T230020Z_1920_1939_batch0005_article_chunks_wave01_chunk_0001\article_crops\192410120chi\192410120chi_3_333935461_a01_r01_headline_wide_story_band.png",
        "note": "Visual crop bottom-right 'The Summary' box lists Bears starters by position.",
        "team": "CHI",
        "players": {
            "j sternaman": "QB",
            "trafton": "C",
            "blacklock": "RT",
            "hanny": "LE",
            "healey": "LT",
            "knop": "FB",
            "walquist": "RH",
            "mcmillen": "RG",
        },
    }
}

LOCAL_TEAM_ALIASES = {
    "racine tornadoes": "RAC",
}

APPROVED_FIELDS = [
    "package_item_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "notes",
]

CLOSED_OVERRIDE_FIELDS = APPROVED_FIELDS

AUDIT_FIELDS = [
    "direct_source_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "decision_status",
    "decision_value",
    "approval_class",
    "reason",
    "support_details_json",
    "evidence_text",
    "source_documents_json",
    "proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "direct_source_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_target_counts_json",
    "approved_class_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

NUMBER_WORDS = {
    "0": {"0", "zero"},
    "1": {"1", "one", "a", "an"},
    "2": {"2", "two", "twice", "both"},
    "3": {"3", "three"},
    "4": {"4", "four"},
    "6": {"6", "six"},
    "7": {"7", "seven"},
    "12": {"12", "twelve"},
    "38": {"38", "thirty eight", "thirty-eight"},
    "40": {"40", "forty"},
    "44": {"44", "forty four", "forty-four"},
    "80": {"80", "eighty"},
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
          AND route_to_lane = 'ready_resolved_atom_review'
          AND target_table IN (
            'lineup_participation',
            'play_by_play_event',
            'player_game_box_score',
            'scoring_event',
            'team_game_stat_claim'
          )
        ORDER BY target_table, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def load_team_games_context(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    boxscore_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    if not boxscore_ids:
        return {}
    rel = str(path).replace("\\", "/").replace("'", "''")
    placeholders = ",".join(["?"] * len(boxscore_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT
          boxscore_id,
          CAST(game_date AS VARCHAR) AS game_date,
          TRY_CAST(year AS INTEGER) AS year,
          TRY_CAST(week AS INTEGER) AS week,
          team_code,
          opponent_code,
          TRY_CAST(team_points AS INTEGER) AS team_points,
          TRY_CAST(opponent_points AS INTEGER) AS opponent_points,
          TRY_CAST(is_away AS BOOLEAN) AS is_away,
          TRY_CAST(is_home AS BOOLEAN) AS is_home
        FROM read_parquet('{rel}')
        WHERE boxscore_id IN ({placeholders})
        ORDER BY boxscore_id, team_code
        """,
        boxscore_ids,
    )
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        output.setdefault(clean(row.get("boxscore_id")), []).append(row)
    return output


def source_prefixes(row: dict[str, Any], proposed: dict[str, Any]) -> list[str]:
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    prefixes = []
    for doc in docs:
        match = re.match(r"([^#:]+)(?:#(\d+))?(?::(\d+))?", doc)
        if match:
            base = match.group(1)
            if match.group(2):
                base = f"{base}#{match.group(2)}"
            if match.group(3):
                base = f"{base}:{match.group(3)}"
            prefixes.append(base)
    return prefixes


def source_doc_ids(row: dict[str, Any], proposed: dict[str, Any]) -> list[str]:
    docs = parse_list(proposed.get("source_documents_json")) or parse_list(row.get("source_documents_json"))
    return docs


def exact_source_game(row: dict[str, Any], proposed: dict[str, Any]) -> bool:
    boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
    for doc in source_doc_ids(row, proposed):
        match = re.match(r"([^#:]+)", doc)
        if match and match.group(1) == boxscore_id:
            return True
    return False


def norm_words(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(text).lower()).strip()


def compact(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean(text).lower())


def edit_distance_one(a: str, b: str) -> bool:
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        return sum(c1 != c2 for c1, c2 in zip(a, b)) <= 1
    if len(a) > len(b):
        a, b = b, a
    for i in range(len(b)):
        if a == b[:i] + b[i + 1:]:
            return True
    return False


def evidence_has_name(evidence: str, raw_name: str) -> bool:
    words = norm_words(evidence).split()
    if not raw_name:
        return True
    tokens = [token for token in norm_words(raw_name).split() if len(token) >= 2]
    if not tokens:
        return True
    if compact(raw_name) in compact(evidence):
        return True
    last = tokens[-1]
    if any(token == last for token in words):
        return True
    if len(last) >= 5 and any(edit_distance_one(token, last) for token in words):
        return True
    return False


def evidence_has_number(evidence: str, value: Any) -> bool:
    text = norm_words(evidence)
    value_text = clean(value)
    if not value_text:
        return True
    words = NUMBER_WORDS.get(value_text, {value_text})
    return any(re.search(rf"(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])", text) for word in words)


def evidence_blob(row: dict[str, Any], proposed: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            clean(row.get("evidence_text")),
            clean(proposed.get("play_text")),
            clean(proposed.get("source_row_text")),
        ]
        if part
    )


def resolve_team_code(raw_team: str) -> str:
    key = norm_key(raw_team)
    if key in LOCAL_TEAM_ALIASES:
        return LOCAL_TEAM_ALIASES[key]
    if key in TEAM_ALIASES:
        return TEAM_ALIASES[key]
    raw = clean(raw_team).upper()
    return raw if re.fullmatch(r"[A-Z]{2,4}", raw) else ""


def team_row(team_games: dict[str, list[dict[str, Any]]], boxscore_id: str, team: str) -> dict[str, Any]:
    for row in team_games.get(boxscore_id, []):
        if clean(row.get("team_code")) == team:
            return row
    return {}


def approve_lineup(
    row: dict[str, Any],
    proposed: dict[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
    docs = source_doc_ids(row, proposed)
    player_key = norm_words(proposed.get("player_raw"))
    position = clean(proposed.get("starter_position") or proposed.get("listed_position_raw")).upper()
    team = clean(proposed.get("nfl_team"))
    for doc in docs:
        support = VISUAL_LINEUP_SUPPORT.get(doc)
        if not support:
            continue
        expected_position = support["players"].get(player_key)
        if expected_position and expected_position == position and team == support["team"]:
            crop_path = clean(support.get("crop_path"))
            if crop_path and not Path(crop_path).exists():
                return False, "visual support crop missing on D drive", {"doc": doc, "crop_path": crop_path}
            return True, "visual lineup summary confirms starter row", {
                "doc": doc,
                "crop_path": crop_path,
                "support_note": support["note"],
                "player": clean(proposed.get("player_raw")),
                "position": position,
                "team": team,
            }
    return False, "no visual support map matched lineup player/position", {"docs": docs}


def approve_pbp(
    row: dict[str, Any],
    proposed: dict[str, Any],
    team_games: dict[str, list[dict[str, Any]]],
) -> tuple[bool, str, dict[str, Any]]:
    evidence = evidence_blob(row, proposed)
    play_text = clean(proposed.get("play_text"))
    play_type = clean(proposed.get("play_type"))
    if not exact_source_game(row, proposed):
        return False, "source document prefix is not the exact boxscore_id", {}
    if play_text and compact(play_text) not in compact(evidence):
        return False, "play_text is not directly present in evidence", {"play_text": play_text}
    if play_type == "team_scoring_summary":
        if "two" in norm_words(evidence) and "touchdowns" in norm_words(evidence) and "forward passes" in norm_words(evidence):
            team = clean(proposed.get("possession_team"))
            if team:
                away = team_row(team_games, clean(proposed.get("boxscore_id")), team).get("is_away")
                return True, "source directly states two visitor touchdowns from forward passes", {
                    "possession_team": team,
                    "team_is_away": away,
                    "points": clean(proposed.get("points")),
                }
        return False, "team scoring summary phrase not strong enough", {}
    if play_type in {"intercepted_forward_pass", "completed_forward_pass"}:
        return True, "play text directly states forward-pass event", {"play_type": play_type}
    return False, "PBP play_type not handled by direct-source station", {"play_type": play_type}


def scoring_raw_players(proposed: dict[str, Any]) -> list[str]:
    return [
        clean(proposed.get(field))
        for field in ["scoring_player_raw", "passer_raw", "receiver_raw"]
        if clean(proposed.get(field))
    ]


def approve_scoring(row: dict[str, Any], proposed: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    evidence = evidence_blob(row, proposed)
    text = norm_words(evidence)
    event_type = clean(proposed.get("event_type")).lower()
    if not exact_source_game(row, proposed):
        return False, "source document prefix is not the exact boxscore_id", {}
    for raw_player in scoring_raw_players(proposed):
        if not evidence_has_name(evidence, raw_player):
            return False, f"evidence does not directly name player {raw_player}", {"raw_player": raw_player}
    if clean(proposed.get("distance_yards")) and not evidence_has_number(evidence, proposed.get("distance_yards")):
        return False, "evidence does not directly support distance_yards", {"distance_yards": clean(proposed.get("distance_yards"))}
    if event_type in {"receiving_touchdown", "touchdown_pass", "touchdown_reception"}:
        if "pass" in text or "forward" in text:
            return True, "direct pass touchdown phrase names passer/receiver", {"event_type": event_type}
        return False, "receiving touchdown text lacks pass/forward cue", {}
    if event_type in {"rushing_touchdown", "touchdown_run"}:
        if "run" in text or "races" in text or "went" in text:
            return True, "direct rushing touchdown phrase names scorer", {"event_type": event_type}
        return False, "rushing touchdown text lacks run cue", {}
    if event_type in {"pat_failed", "missed_extra_point"}:
        if ("failed" in text or "failure" in text) and ("goal" in text or "extra point" in text or "kick" in text):
            return True, "direct failed goal/extra point phrase", {"event_type": event_type}
        return False, "failed PAT text lacks failed kick cue", {}
    if event_type == "fake_forward_pass_touchdown":
        if "fake forward pass" in text and "score" in text:
            return True, "direct fake-forward-pass scoring phrase", {"event_type": event_type}
        return False, "fake forward pass scoring phrase not present", {}
    if event_type in {"field_goal", "drop_kick_field_goal", "extra_point"}:
        if scoring_raw_players(proposed) and all(evidence_has_name(evidence, raw) for raw in scoring_raw_players(proposed)):
            if event_type == "field_goal" and "kick" in text:
                return True, "direct field-goal phrase names scorer", {"event_type": event_type}
            if event_type == "extra_point" and ("extra point" in text or "point" in text):
                return True, "direct extra-point phrase names scorer", {"event_type": event_type}
        return False, "kicking score does not directly name scorer", {"event_type": event_type}
    return False, "scoring event_type not handled by direct-source station", {"event_type": event_type}


def approve_player_box(proposed: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    source = clean(proposed.get("source_row_text"))
    player = clean(proposed.get("player_raw"))
    if not evidence_has_name(source, player):
        return False, f"source row does not name player {player}", {}
    if clean(proposed.get("rushing_tds")):
        run_cues = ["run", "rushed", "plunged", "through the line", "around end", "went over"]
        if not any(cue in norm_words(source) for cue in run_cues):
            return False, "source row does not directly support rushing_tds", {"rushing_tds": clean(proposed.get("rushing_tds"))}
    if clean(proposed.get("passing_tds")) and "passed to" not in norm_words(source):
        return False, "source row does not directly support passing_tds", {"passing_tds": clean(proposed.get("passing_tds"))}
    return True, "player box score row directly supported", {}


def approve_team_stat(
    row: dict[str, Any],
    proposed: dict[str, Any],
    team_games: dict[str, list[dict[str, Any]]],
) -> tuple[bool, str, dict[str, Any]]:
    evidence = evidence_blob(row, proposed)
    text = norm_words(evidence)
    if not exact_source_game(row, proposed):
        return False, "source document prefix is not the exact boxscore_id", {}
    if clean(proposed.get("stat_name")) != "first_downs":
        return False, "team stat_name not handled by direct-source station", {"stat_name": clean(proposed.get("stat_name"))}
    if not evidence_has_number(evidence, proposed.get("team_1_value")) or not evidence_has_number(evidence, proposed.get("team_2_value")):
        return False, "evidence does not support both team stat values", {}
    team_1 = resolve_team_code(clean(proposed.get("team_1_raw")))
    team_2 = resolve_team_code(clean(proposed.get("team_2_raw")))
    if not team_1 or not team_2:
        return False, "team labels did not resolve for team stat claim", {"team_1": team_1, "team_2": team_2}
    boxscore_id = clean(proposed.get("boxscore_id"))
    row_1 = team_row(team_games, boxscore_id, team_1)
    row_2 = team_row(team_games, boxscore_id, team_2)
    if "winners" in text and row_1 and not (parse_int(row_1.get("team_points")) or 0) > (parse_int(row_1.get("opponent_points")) or 0):
        return False, "team_1 is not the local PFR winner despite source saying winners", {"team_1": team_1}
    if "visiting" in text and row_2 and row_2.get("is_away") is not True:
        return False, "team_2 is not the local PFR visitor despite source saying visiting aggregation", {"team_2": team_2}
    return True, "source directly states first downs and local game context maps winner/visitor", {
        "team_1_resolved": team_1,
        "team_2_resolved": team_2,
    }


def classify_row(
    row: dict[str, Any],
    team_games: dict[str, list[dict[str, Any]]],
) -> tuple[bool, str, str, dict[str, Any]]:
    target = clean(row.get("target_table"))
    proposed = parse_obj(row.get("proposed_fields_json"))
    if target == "lineup_participation":
        ok, reason, details = approve_lineup(row, proposed)
        return ok, "visual_lineup_summary" if ok else "held_direct_source", reason, details
    if target == "play_by_play_event":
        ok, reason, details = approve_pbp(row, proposed, team_games)
        return ok, "direct_pbp_text" if ok else "held_direct_source", reason, details
    if target == "scoring_event":
        ok, reason, details = approve_scoring(row, proposed)
        return ok, "direct_scoring_text" if ok else "held_direct_source", reason, details
    if target == "player_game_box_score":
        if not exact_source_game(row, proposed):
            return False, "held_direct_source", "source document prefix is not the exact boxscore_id", {}
        ok, reason, details = approve_player_box(proposed)
        return ok, "direct_player_box_text" if ok else "held_direct_source", reason, details
    if target == "team_game_stat_claim":
        ok, reason, details = approve_team_stat(row, proposed, team_games)
        return ok, "direct_team_stat_text" if ok else "held_direct_source", reason, details
    return False, "held_direct_source", "target table not handled", {"target_table": target}


def event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    proposed = parse_obj(row.get("proposed_fields_json"))
    target = clean(row.get("target_table"))
    boxscore_id = clean(row.get("boxscore_id"))
    if target == "play_by_play_event":
        event_id = clean(proposed.get("pbp_event_id"))
    elif target == "scoring_event":
        event_id = clean(proposed.get("scoring_event_id"))
    elif target == "player_game_box_score":
        event_id = clean(proposed.get("player_game_box_score_id"))
    else:
        event_id = ""
    if not target or not boxscore_id or not event_id:
        return ("", "", "")
    return (target, boxscore_id, event_id)


def load_existing_final_event_keys(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str, str], str]:
    output: dict[tuple[str, str, str], str] = {}
    queries = [
        ("play_by_play_event", "pbp_event_id"),
        ("scoring_event", "scoring_event_id"),
        ("player_game_box_score", "player_game_box_score_id"),
    ]
    for table, event_col in queries:
        try:
            rows = query_dicts(
                con,
                f"""
                SELECT package_item_id, boxscore_id, {event_col} AS event_id
                FROM newspaper_final.{table}
                WHERE COALESCE({event_col}, '') <> ''
                """,
            )
        except Exception:
            continue
        for row in rows:
            key = (table, clean(row.get("boxscore_id")), clean(row.get("event_id")))
            output[key] = clean(row.get("package_item_id"))
    return output


def canonical_score(row: dict[str, Any]) -> tuple[int, int, str]:
    proposed = parse_obj(row.get("proposed_fields_json"))
    score = 0
    if clean(proposed.get("review_status")) in {
        "event_detail_resolved",
        "box_score_resolved",
        "source_article_direct",
        "codex_final_ocr_followup_resolved",
    }:
        score += 100
    target_key = clean(row.get("target_entity_key"))
    team = clean(proposed.get("possession_team")) or clean(proposed.get("scoring_team")) or clean(proposed.get("nfl_team"))
    if team and f"|{team}|" in target_key:
        score += 20
    if clean(proposed.get("distance_yards")):
        score += 10
    return (score, len(target_key), clean(row.get("package_item_id")))


def duplicate_closure_map(
    rows: list[dict[str, Any]],
    existing_final_keys: dict[tuple[str, str, str], str],
) -> dict[str, tuple[str, str]]:
    output: dict[str, tuple[str, str]] = {}
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = event_key(row)
        if not key[0]:
            continue
        package_item_id = clean(row.get("package_item_id"))
        existing_package_id = existing_final_keys.get(key, "")
        if existing_package_id and existing_package_id != package_item_id:
            output[package_item_id] = (
                existing_package_id,
                f"duplicate package item closed; event already staged in local final package {existing_package_id}",
            )
            continue
        groups.setdefault(key, []).append(row)

    for group_rows in groups.values():
        if len(group_rows) < 2:
            continue
        canonical = sorted(group_rows, key=canonical_score, reverse=True)[0]
        canonical_id = clean(canonical.get("package_item_id"))
        for row in group_rows:
            package_item_id = clean(row.get("package_item_id"))
            if package_item_id and package_item_id != canonical_id:
                output[package_item_id] = (
                    canonical_id,
                    f"duplicate package item closed; canonical package {canonical_id} remains active",
                )
    return output


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    team_games: dict[str, list[dict[str, Any]]],
    existing_final_keys: dict[tuple[str, str, str], str],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    duplicate_map = duplicate_closure_map(rows, existing_final_keys)
    for row in rows:
        target = clean(row.get("target_table"))
        package_item_id = clean(row.get("package_item_id"))
        if package_item_id in duplicate_map:
            canonical_id, reason = duplicate_map[package_item_id]
            details = {"canonical_package_item_id": canonical_id}
            audit_row = {
                "direct_source_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "package_item_id": package_item_id,
                "route_to_lane": clean(row.get("route_to_lane")),
                "target_table": target,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "decision_status": "closed",
                "decision_value": "corroboration_archive",
                "approval_class": "duplicate_package_item_closed",
                "reason": reason,
                "support_details_json": json.dumps(details, sort_keys=True, ensure_ascii=True),
                "evidence_text": clean(row.get("evidence_text")),
                "source_documents_json": clean(row.get("source_documents_json")),
                "proposed_fields_json": clean(row.get("proposed_fields_json")),
                "created_at_utc": created_at,
            }
            audit.append(audit_row)
            closed.append({
                "package_item_id": package_item_id,
                "decision_status": "closed",
                "decision_value": "corroboration_archive",
                "route_to_lane": "archive_duplicate",
                "target_table": target,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "notes": reason,
            })
            continue

        ok, approval_class, reason, details = classify_row(row, team_games)
        audit_row = {
            "direct_source_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "route_to_lane": clean(row.get("route_to_lane")),
            "target_table": target,
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else clean(row.get("decision_value")),
            "approval_class": approval_class,
            "reason": reason,
            "support_details_json": json.dumps(details, sort_keys=True, ensure_ascii=True) if details else "",
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "created_at_utc": created_at,
        }
        audit.append(audit_row)
        if ok:
            approved.append({
                "package_item_id": package_item_id,
                "decision_status": "approved",
                "decision_value": "approved_for_local_newspaper_final",
                "route_to_lane": f"local_final_{target}",
                "target_table": target,
                "target_entity_key": clean(row.get("target_entity_key")),
                "boxscore_id": clean(row.get("boxscore_id")),
                "notes": f"{approval_class}: {reason}",
            })
        else:
            held.append(audit_row)
    return approved, closed, held, audit


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_direct_source_decision_run (
          direct_source_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          approved_count INTEGER,
          held_count INTEGER,
          approved_target_counts_json VARCHAR,
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
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_direct_source_decision_item (
          direct_source_decision_run_id VARCHAR,
          resolved_package_decision_run_id VARCHAR,
          package_item_id VARCHAR,
          route_to_lane VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          boxscore_id VARCHAR,
          decision_status VARCHAR,
          decision_value VARCHAR,
          approval_class VARCHAR,
          reason VARCHAR,
          support_details_json VARCHAR,
          evidence_text VARCHAR,
          source_documents_json VARCHAR,
          proposed_fields_json VARCHAR,
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
        run_id = clean(run_row.get("direct_source_decision_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_direct_source_decision_run WHERE direct_source_decision_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_direct_source_decision_item WHERE direct_source_decision_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_direct_source_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_direct_source_decision_item", audit_rows, AUDIT_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Direct Source Decisions",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Closed duplicates: `{summary['closed_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Approved target counts: `{summary['approved_target_counts']}`",
        f"- Approved classes: `{summary['approved_class_counts']}`",
        f"- Hold reasons: `{summary['hold_reason_counts']}`",
        "",
        "## Rows",
        "",
        "| status | table | boxscore | class | reason | evidence |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in audit_rows:
        reason = clean(row.get("reason")).replace("|", "\\|")
        evidence = clean(row.get("evidence_text")).replace("|", "\\|")[:140]
        lines.append(
            f"| `{row['decision_status']}` | `{row['target_table']}` | `{row['boxscore_id']}` | "
            f"`{row['approval_class']}` | {reason} | {evidence} |"
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
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_direct_source_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
        existing_final_keys = load_existing_final_event_keys(read_con)
    finally:
        read_con.close()

    boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in rows if clean(row.get("boxscore_id"))})
    ext_con = duckdb.connect()
    try:
        team_games = load_team_games_context(ext_con, args.team_games_path, boxscore_ids)
    finally:
        ext_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    approved, closed, held, audit_rows = build_rows(rows, run_id, decision_run_id, team_games, existing_final_keys, created_at)

    approved_target_counts = Counter(row["target_table"] for row in approved)
    approved_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "approved")
    closed_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "closed")
    hold_reason_counts = Counter(row["reason"] for row in held)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    closed_csv = out_dir / "closed_decision_overrides.csv"
    held_csv = out_dir / "held_direct_source_rows.csv"
    audit_csv = out_dir / "direct_source_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "direct_source_decision_report.md"

    summary = {
        "direct_source_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "closed_count": len(closed),
        "held_count": len(held),
        "approved_target_counts": dict(approved_target_counts),
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
                "direct_source_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": len(approved),
                "held_count": len(held),
                "approved_target_counts_json": json.dumps(dict(approved_target_counts), sort_keys=True),
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
        "direct_source_decision_run_id": run_id,
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "closed_count": len(closed),
        "held_count": len(held),
        "approved_target_counts": dict(approved_target_counts),
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
