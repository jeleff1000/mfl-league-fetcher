#!/usr/bin/env python
"""Resolve remaining semantic follow-ups with target-game mapping evidence.

This station focuses on atoms that already contain useful event/game text but
were blocked because the target game was not mapped. It can also promote
same-source game candidates when PFR has a known APFA team vs NON_NFL row.
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "semantic_followup_resolution_preps"
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")

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

RESOLUTION_FIELDS = [
    "semantic_followup_resolution_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "target_table",
    "resolved_target_table",
    "boxscore_id",
    "resolved_boxscore_id",
    "source_documents_json",
    "source_asset_dates_json",
    "raw_team",
    "resolved_team",
    "opponent_team",
    "raw_player",
    "match_status",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "reason",
    "proposed_fields_json",
    "enriched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "semantic_followup_resolution_run_id",
    "decision_ledger_run_id",
    "output_dir",
    "semantic_decision_count",
    "approved_count",
    "held_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

TEAM_ALIASES = {
    "akron": "AKR",
    "akron pros": "AKR",
    "akron indians": "AKR",
    "buffalo all americans": "BUF",
    "buffalo all-americans": "BUF",
    "canton": "CAN",
    "canton bulldogs": "CAN",
    "chicago cardinals": "CRD",
    "cardinals": "CRD",
    "chicago tigers": "CHT",
    "cincinnati celts": "",
    "cincy celts": "",
    "cleveland tigers": "CLE",
    "columbus panhandles": "COL",
    "panhandles": "COL",
    "dayton triangles": "DAY",
    "decatur staleys": "CHI",
    "staleys": "CHI",
    "detroit heralds": "DET",
    "detroit tigers": "DET",
    "evansville crimson giants": "EVN",
    "green bay packers": "GNB",
    "green bay": "GNB",
    "hammond": "HAM",
    "hammond pros": "HAM",
    "minneapolis marines": "MIN",
    "moline tractors": "",
    "muncie flyers": "MUN",
    "racine cardinals": "RAC",
    "rock island independents": "RII",
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


def norm_words(value: Any) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", clean(value).lower()) if token]


def norm_team(value: Any) -> str:
    return " ".join(norm_words(value))


def parse_int(value: Any) -> int | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


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


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'newspaper_review'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def latest_decision_ledger_run(con: duckdb.DuckDBPyConnection) -> str:
    if not table_exists(con, "llm_review_decision_ledger_run"):
        return ""
    row = con.execute(
        """
        SELECT decision_ledger_run_id
        FROM newspaper_review.llm_review_decision_ledger_run
        ORDER BY created_at_utc DESC, decision_ledger_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def latest_decision_input(root: Path) -> Path | None:
    patterns = [
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
    order: list[str] = []
    for row in base_rows + new_rows:
        decision_id = clean(row.get("decision_id"))
        if not decision_id:
            continue
        if decision_id not in by_id:
            order.append(decision_id)
        by_id[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    return [by_id[decision_id] for decision_id in order]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_dates_from_text(value: Any) -> list[str]:
    text = clean(value)
    return [
        f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        for match in re.finditer(r"(19[0-9]{2})[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12][0-9]|3[01])", text)
    ]


def parse_date(value: Any) -> date | None:
    text = clean(value)
    if not text or text.lower() == "nat":
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def source_asset_dates(source_docs: list[dict[str, Any]]) -> list[str]:
    dates: list[str] = []
    for doc in source_docs:
        for field in ["issue_date", "asset_pdf_path", "asset_image_path", "source_url"]:
            dates.extend(parse_dates_from_text(doc.get(field)))
    return sorted(set(dates))


def asset_date_supports(game_date: str, asset_dates: list[str]) -> bool:
    target = parse_date(game_date)
    if not target:
        return False
    for item in asset_dates:
        item_date = parse_date(item)
        if not item_date:
            continue
        delta = (item_date - target).days
        if 0 <= delta <= 3:
            return True
    return False


def team_code(raw: Any) -> str:
    return TEAM_ALIASES.get(norm_team(raw), "")


def source_doc_ids(row: dict[str, Any]) -> list[str]:
    return parse_json_list(row.get("source_documents_json"))


def load_semantic_decisions(con: duckdb.DuckDBPyConnection, decision_ledger_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
          AND route_to_lane = 'semantic_followup'
          AND COALESCE(decision_status, '') IN ('', 'pending', 'pending_followup')
          AND COALESCE(decision_value, '') IN ('', 'pending')
        ORDER BY target_table, boxscore_id, target_entity_key, decision_id
        """,
        [decision_ledger_run_id],
    )


def load_quality_game_candidates(con: duckdb.DuckDBPyConnection, decision_ledger_run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(query_dicts(
        con,
        """
        SELECT decision_id, target_table, boxscore_id, proposed_fields_json, source_documents_json,
               decision_status, decision_value
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
          AND target_table = 'game_candidate'
        """,
        [decision_ledger_run_id],
    ))
    if table_exists(con, "quality_lane_resolution_decision"):
        rows.extend(query_dicts(
            con,
            """
            SELECT decision_id, target_table, resolved_boxscore_id AS boxscore_id,
                   enriched_proposed_fields_json AS proposed_fields_json, source_documents_json,
                   decision_status, decision_value
            FROM newspaper_review.quality_lane_resolution_decision
            WHERE target_table = 'game_candidate'
            """,
        ))
    return rows


def load_promoted_game_candidates(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    if not table_exists(con, "noop"):
        pass
    table_exists_row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'newspaper_promoted'
          AND table_name = 'game_candidate'
        """
    ).fetchone()
    if not table_exists_row or not table_exists_row[0]:
        return []
    return query_dicts(
        con,
        """
        SELECT decision_id, 'game_candidate' AS target_table, boxscore_id,
               source_documents_json,
               to_json(struct_pack(
                 boxscore_id := boxscore_id,
                 team_1_raw := team_1_raw,
                 team_2_raw := team_2_raw,
                 team_1_score := team_1_score,
                 team_2_score := team_2_score,
                 team_1_resolved := team_1_resolved,
                 team_2_resolved := team_2_resolved,
                 game_date := game_date,
                 year := year,
                 week := week,
                 season_type := season_type,
                 evidence_text := evidence_text
               )) AS proposed_fields_json,
               'approved' AS decision_status,
               'approved_for_local_promotion' AS decision_value
        FROM newspaper_promoted.game_candidate
        """
    )


def load_team_games(con: duckdb.DuckDBPyConnection, team_games_path: Path) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT
          boxscore_id,
          CAST(game_date AS VARCHAR) AS game_date,
          TRY_CAST(year AS INTEGER) AS year,
          TRY_CAST(week AS INTEGER) AS week,
          season_type,
          team_code,
          opponent_code,
          TRY_CAST(team_points AS INTEGER) AS team_points,
          TRY_CAST(opponent_points AS INTEGER) AS opponent_points
        FROM read_parquet(?)
        WHERE TRY_CAST(year AS INTEGER) BETWEEN 1920 AND 1939
        ORDER BY boxscore_id, team_code
        """,
        [str(team_games_path)],
    )


def load_source_documents(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    rows = query_dicts(
        con,
        f"SELECT * FROM newspaper_review.source_document WHERE source_document_id IN ({placeholders})",
        source_document_ids,
    )
    return {clean(row.get("source_document_id")): row for row in rows}


def find_game_match(
    proposed: dict[str, Any],
    source_docs: list[dict[str, Any]],
    team_games: list[dict[str, Any]],
) -> tuple[str, dict[str, Any], str, str]:
    team_1 = clean(proposed.get("team_1_raw"))
    team_2 = clean(proposed.get("team_2_raw"))
    code_1 = clean(proposed.get("team_1_resolved")) or team_code(team_1)
    code_2 = clean(proposed.get("team_2_resolved")) or team_code(team_2)
    score_1 = parse_int(proposed.get("team_1_score"))
    score_2 = parse_int(proposed.get("team_2_score"))
    boxscore_id = clean(proposed.get("boxscore_id"))
    if boxscore_id:
        for row in team_games:
            if clean(row.get("boxscore_id")) == boxscore_id and clean(row.get("team_code")) in {code_1, code_2}:
                return "already_mapped_game_candidate", row, code_1, code_2
    if score_1 is None or score_2 is None:
        return "missing_score_for_game_mapping", {}, code_1, code_2

    candidates: list[tuple[dict[str, Any], str, str, bool]] = []
    for row in team_games:
        team = clean(row.get("team_code"))
        opp = clean(row.get("opponent_code"))
        if code_1 and code_2:
            if team == code_1 and opp == code_2 and row.get("team_points") == score_1 and row.get("opponent_points") == score_2:
                candidates.append((row, code_1, code_2, False))
            elif team == code_2 and opp == code_1 and row.get("team_points") == score_2 and row.get("opponent_points") == score_1:
                candidates.append((row, code_1, code_2, False))
        elif code_1 and not code_2:
            if team == code_1 and opp == "NON_NFL" and row.get("team_points") == score_1 and row.get("opponent_points") == score_2:
                candidates.append((row, code_1, "NON_NFL", True))
        elif code_2 and not code_1:
            if team == code_2 and opp == "NON_NFL" and row.get("team_points") == score_2 and row.get("opponent_points") == score_1:
                candidates.append((row, "NON_NFL", code_2, True))
    by_boxscore = {clean(item[0].get("boxscore_id")): item for item in candidates}
    candidates = list(by_boxscore.values())
    if not candidates:
        return "no_pfr_game_match_for_teams_scores", {}, code_1, code_2
    asset_dates = source_asset_dates(source_docs)
    if len(candidates) == 1:
        row, resolved_1, resolved_2, used_non_nfl = candidates[0]
        if used_non_nfl and asset_dates and not asset_date_supports(clean(row.get("game_date")), asset_dates):
            return "non_nfl_game_requires_source_date_confirmation", {}, resolved_1, resolved_2
        return "unique_teams_scores_match", row, resolved_1, resolved_2
    dated = [item for item in candidates if asset_date_supports(clean(item[0].get("game_date")), asset_dates)]
    if len(dated) == 1:
        row, resolved_1, resolved_2, _ = dated[0]
        return "unique_teams_scores_source_date_match", row, resolved_1, resolved_2
    return "multiple_pfr_game_matches", {}, code_1, code_2


def game_target_key(boxscore_id: str, proposed: dict[str, Any]) -> str:
    return "|".join([
        "game_candidate",
        boxscore_id,
        clean(proposed.get("team_1_raw")),
        clean(proposed.get("team_2_raw")),
    ])


def event_target_key(target_table: str, boxscore_id: str, enriched: dict[str, Any]) -> str:
    if target_table == "scoring_event":
        return "|".join([
            "scoring_event",
            boxscore_id,
            clean(enriched.get("scoring_team")),
            clean(enriched.get("scoring_player_raw")),
            clean(enriched.get("event_type")),
            clean(enriched.get("distance_yards")),
            clean(enriched.get("play_text"))[:80],
        ])
    return "|".join([
        "play_by_play_event",
        boxscore_id,
        clean(enriched.get("possession_team")),
        clean(enriched.get("primary_player_raw")),
        clean(enriched.get("play_type")),
        clean(enriched.get("yards")),
        clean(enriched.get("play_text"))[:80],
    ])


def build_source_game_map(
    game_candidate_rows: list[dict[str, Any]],
    source_docs_by_id: dict[str, dict[str, Any]],
    team_games: list[dict[str, Any]],
    hold_boxscore_ids: set[str],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    source_game_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    game_decisions: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in game_candidate_rows:
        decision_id = clean(row.get("decision_id"))
        if not decision_id or decision_id in seen:
            continue
        seen.add(decision_id)
        proposed = parse_json_obj(row.get("proposed_fields_json"))
        docs = [source_docs_by_id[doc_id] for doc_id in source_doc_ids(row) if doc_id in source_docs_by_id]
        status, game_row, code_1, code_2 = find_game_match(proposed, docs, team_games)
        if status in {"already_mapped_game_candidate", "unique_teams_scores_match", "unique_teams_scores_source_date_match"} and game_row:
            boxscore_id = clean(game_row.get("boxscore_id"))
            if boxscore_id in hold_boxscore_ids:
                continue
            team_codes = sorted({clean(game_row.get("team_code")), clean(game_row.get("opponent_code")), code_1, code_2} - {""})
            entry = {
                "decision_id": decision_id,
                "boxscore_id": boxscore_id,
                "team_codes": team_codes,
                "game_row": game_row,
                "proposed": proposed,
                "match_status": status,
            }
            for doc_id in source_doc_ids(row):
                source_game_map[doc_id].append(entry)
            if clean(row.get("decision_status")) != "approved" or clean(row.get("decision_value")) != PROMOTION_VALUE:
                enriched = dict(proposed)
                enriched.update({
                    "boxscore_id": boxscore_id,
                    "game_date": clean(game_row.get("game_date")),
                    "year": clean(game_row.get("year")),
                    "week": clean(game_row.get("week")),
                    "season_type": clean(game_row.get("season_type")),
                    "team_1_resolved": code_1 or clean(game_row.get("team_code")),
                    "team_2_resolved": code_2 or clean(game_row.get("opponent_code")),
                    "reconciliation_status": "semantic_followup_game_mapped",
                })
                game_decisions.append({
                    "decision_id": decision_id,
                    "target_table": "game_candidate",
                    "resolved_boxscore_id": boxscore_id,
                    "resolved_target_table": "game_candidate",
                    "resolved_target_entity_key": game_target_key(boxscore_id, enriched),
                    "proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
                    "reason": f"{status}; PFR game matched from same-source quality/semantic candidate",
                })
    return source_game_map, game_decisions


def event_team_raw(target_table: str, proposed: dict[str, Any]) -> str:
    if target_table == "scoring_event":
        return clean(proposed.get("scoring_team_raw"))
    return clean(proposed.get("possession_team_raw") or proposed.get("event_team_raw"))


def event_player_raw(target_table: str, proposed: dict[str, Any]) -> str:
    if target_table == "scoring_event":
        return clean(proposed.get("scoring_player_raw"))
    return clean(proposed.get("primary_player_raw") or proposed.get("player_raw"))


def enrich_event(target_table: str, proposed: dict[str, Any], boxscore_id: str, team: str) -> dict[str, Any]:
    enriched = dict(proposed)
    enriched["boxscore_id"] = boxscore_id
    if target_table == "scoring_event":
        enriched["scoring_team"] = team
        enriched.setdefault("review_status", "semantic_followup_game_mapped")
        enriched.setdefault("promotion_status", "local_promotion_candidate")
    else:
        if not clean(enriched.get("play_type")) and clean(enriched.get("event_type")):
            enriched["play_type"] = clean(enriched.get("event_type"))
        if not clean(enriched.get("possession_team_raw")) and clean(enriched.get("event_team_raw")):
            enriched["possession_team_raw"] = clean(enriched.get("event_team_raw"))
        if not clean(enriched.get("primary_player_raw")) and clean(enriched.get("player_raw")):
            enriched["primary_player_raw"] = clean(enriched.get("player_raw"))
        enriched["possession_team"] = team
        enriched.setdefault("review_status", "semantic_followup_game_mapped")
        enriched.setdefault("promotion_status", "local_promotion_candidate")
    return enriched


def build_resolution_rows(
    run_id: str,
    decision_ledger_run_id: str,
    semantic_rows: list[dict[str, Any]],
    source_docs_by_id: dict[str, dict[str, Any]],
    source_game_map: dict[str, list[dict[str, Any]]],
    game_decision_inputs: list[dict[str, Any]],
    emit_hold_overrides: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], Counter]:
    created_at = iso_now()
    resolution_rows: list[dict[str, Any]] = []
    decision_inputs: list[dict[str, str]] = []
    counts: Counter = Counter()

    for item in game_decision_inputs:
        decision_inputs.append({
            "decision_id": item["decision_id"],
            "decision_status": "approved",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "local_promotion",
            "resolved_boxscore_id": item["resolved_boxscore_id"],
            "resolved_target_table": item["resolved_target_table"],
            "resolved_target_entity_key": item["resolved_target_entity_key"],
            "proposed_fields_json": item["proposed_fields_json"],
            "notes": item["reason"],
        })

    for row in semantic_rows:
        decision_id = clean(row.get("decision_id"))
        target_table = clean(row.get("target_table"))
        proposed = parse_json_obj(row.get("proposed_fields_json"))
        docs = [source_docs_by_id[doc_id] for doc_id in source_doc_ids(row) if doc_id in source_docs_by_id]
        asset_dates = source_asset_dates(docs)
        raw_team = event_team_raw(target_table, proposed)
        raw_player = event_player_raw(target_table, proposed)
        resolved_team = team_code(raw_team)
        opponent_team = ""
        resolved_boxscore_id = clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id"))
        enriched = dict(proposed)
        resolved_target_table = target_table
        resolved_key = clean(row.get("target_entity_key"))

        if target_table not in {"scoring_event", "play_by_play_event"}:
            match_status = "document_level_semantic_followup"
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "semantic_followup"
            reason = "document-level semantic note has no target atom table"
        elif resolved_boxscore_id:
            match_status = "already_has_target_boxscore"
            decision_status = "approved"
            decision_value = PROMOTION_VALUE
            route = "local_promotion"
            reason = "semantic event already has target boxscore"
        else:
            candidates = [
                entry
                for doc_id in source_doc_ids(row)
                for entry in source_game_map.get(doc_id, [])
            ]
            if resolved_team:
                candidates = [entry for entry in candidates if resolved_team in entry["team_codes"]]
            unique = {entry["boxscore_id"]: entry for entry in candidates}
            if len(unique) == 1 and resolved_team:
                entry = next(iter(unique.values()))
                resolved_boxscore_id = entry["boxscore_id"]
                game_row = entry["game_row"]
                if clean(game_row.get("team_code")) == resolved_team:
                    opponent_team = clean(game_row.get("opponent_code"))
                elif clean(game_row.get("opponent_code")) == resolved_team:
                    opponent_team = clean(game_row.get("team_code"))
                enriched = enrich_event(target_table, proposed, resolved_boxscore_id, resolved_team)
                resolved_key = event_target_key(target_table, resolved_boxscore_id, enriched)
                match_status = "inherited_unique_same_source_game_mapping"
                decision_status = "approved"
                decision_value = PROMOTION_VALUE
                route = "local_promotion"
                reason = "event inherited unique same-source APFA/PFR game mapping"
            elif not resolved_team:
                match_status = "unresolved_event_team"
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "semantic_followup"
                reason = "event team could not be normalized to APFA/PFR team code"
            else:
                match_status = "no_unique_same_source_game_mapping"
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "semantic_followup"
                reason = "event source has no unique mapped game for the event team"

        if decision_status == "approved" or emit_hold_overrides:
            decision_inputs.append({
                "decision_id": decision_id,
                "decision_status": decision_status,
                "decision_value": decision_value,
                "route_to_lane": route,
                "resolved_boxscore_id": resolved_boxscore_id,
                "resolved_target_table": resolved_target_table,
                "resolved_target_entity_key": resolved_key,
                "proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
                "notes": reason,
            })

        resolution_rows.append({
            "semantic_followup_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "decision_id": decision_id,
            "target_table": target_table,
            "resolved_target_table": resolved_target_table,
            "boxscore_id": clean(proposed.get("boxscore_id")) or clean(row.get("boxscore_id")),
            "resolved_boxscore_id": resolved_boxscore_id,
            "source_documents_json": clean(row.get("source_documents_json")),
            "source_asset_dates_json": json.dumps(asset_dates, sort_keys=True),
            "raw_team": raw_team,
            "resolved_team": resolved_team,
            "opponent_team": opponent_team,
            "raw_player": raw_player,
            "match_status": match_status,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": route,
            "reason": reason,
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "enriched_proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
            "created_at_utc": created_at,
        })
        counts[match_status] += 1
        counts[f"decision_{decision_status}"] += 1
        counts[f"target_{target_table or 'none'}"] += 1

    return resolution_rows, decision_inputs, counts


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    defs = ", ".join(f"{field} VARCHAR" for field in RESOLUTION_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.semantic_followup_resolution_decision ({defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'newspaper_review'
              AND table_name = 'semantic_followup_resolution_decision'
            """
        ).fetchall()
    }
    for field in RESOLUTION_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.semantic_followup_resolution_decision ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.semantic_followup_resolution_run (
          semantic_followup_resolution_run_id VARCHAR,
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          semantic_decision_count INTEGER,
          approved_count INTEGER,
          held_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(fields))
    columns = ", ".join(fields)
    values = [[clean(row.get(field)) for field in fields] for row in rows]
    con.executemany(f"INSERT INTO newspaper_review.{table} ({columns}) VALUES ({placeholders})", values)


def persist(
    db_path: Path,
    run_id: str,
    decision_ledger_run_id: str,
    out_dir: Path,
    rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.semantic_followup_resolution_decision WHERE semantic_followup_resolution_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.semantic_followup_resolution_run WHERE semantic_followup_resolution_run_id = ?",
            [run_id],
        )
        insert_rows(con, "semantic_followup_resolution_decision", rows, RESOLUTION_FIELDS)
        insert_rows(con, "semantic_followup_resolution_run", [{
            "semantic_followup_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "semantic_decision_count": len(rows),
            "approved_count": sum(1 for row in rows if row.get("decision_status") == "approved"),
            "held_count": sum(1 for row in rows if row.get("decision_status") != "approved"),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_report(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Semantic Follow-Up Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['semantic_followup_resolution_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Semantic rows reviewed: `{summary['semantic_decision_count']}`",
        f"- Approved/local-promoted decisions: `{summary['approved_count']}`",
        f"- Held/routed decisions: `{summary['held_count']}`",
        f"- Extra game decisions emitted: `{summary['extra_game_decision_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Approved Samples",
        "",
        "| table | boxscore | team | player | status | reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in [item for item in rows if item.get("decision_status") == "approved"][:60]:
        lines.append(
            f"| {row['target_table']} | {row['resolved_boxscore_id']} | {row['resolved_team']} | "
            f"{row['raw_player']} | {row['match_status']} | {row['reason']} |"
        )
    lines.extend(["", "## Held Samples", "", "| table | boxscore | status | reason |", "| --- | --- | --- | --- |"])
    for row in [item for item in rows if item.get("decision_status") != "approved"][:80]:
        lines.append(f"| {row['target_table']} | {row['boxscore_id']} | {row['match_status']} | {row['reason']} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="semantic_followup_resolution_prep")
    parser.add_argument("--emit-hold-overrides", action="store_true")
    parser.add_argument(
        "--hold-boxscore-id",
        action="append",
        default=["192011070rii"],
        help="Boxscore id to keep out of automatic semantic follow-up promotion. Can be repeated.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    created_at = iso_now()

    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_ledger_run_id = args.decision_ledger_run_id or latest_decision_ledger_run(read_con)
        if not decision_ledger_run_id:
            raise SystemExit("No decision ledger run found.")
        semantic_rows = load_semantic_decisions(read_con, decision_ledger_run_id)
        game_candidate_rows = load_quality_game_candidates(read_con, decision_ledger_run_id)
        game_candidate_rows.extend(load_promoted_game_candidates(read_con))
        source_document_ids = sorted({
            doc_id
            for row in semantic_rows + game_candidate_rows
            for doc_id in source_doc_ids(row)
        })
        source_docs_by_id = load_source_documents(read_con, source_document_ids)
        team_games = load_team_games(read_con, args.team_games_path)
    finally:
        read_con.close()

    source_game_map, extra_game_decisions = build_source_game_map(
        game_candidate_rows,
        source_docs_by_id,
        team_games,
        set(args.hold_boxscore_id or []),
    )
    resolution_rows, semantic_input_rows, counts = build_resolution_rows(
        run_id,
        decision_ledger_run_id,
        semantic_rows,
        source_docs_by_id,
        source_game_map,
        extra_game_decisions,
        args.emit_hold_overrides,
    )

    base_input_path = args.base_decision_input_csv or latest_decision_input(DEFAULT_ROOT)
    base_rows = read_base_decisions(base_input_path)
    combined_rows = combine_decision_inputs(base_rows, semantic_input_rows)

    resolution_csv = out_dir / "semantic_followup_resolution_decisions.csv"
    semantic_input_csv = out_dir / "semantic_followup_decision_input.csv"
    combined_input_csv = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    report_path = out_dir / "semantic_followup_resolution_report.md"

    write_csv(resolution_csv, resolution_rows, RESOLUTION_FIELDS)
    write_csv(semantic_input_csv, semantic_input_rows, DECISION_INPUT_FIELDS)
    write_csv(combined_input_csv, combined_rows, DECISION_INPUT_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "semantic_followup_resolution_run_id": run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "output_dir": str(out_dir),
        "semantic_decision_count": len(resolution_rows),
        "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
        "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
        "extra_game_decision_count": len(extra_game_decisions),
        "match_status_counts": dict(Counter(row.get("match_status") for row in resolution_rows)),
        "decision_status_counts": dict(Counter(row.get("decision_status") for row in resolution_rows)),
        "semantic_decision_input_csv": str(semantic_input_csv),
        "combined_decision_input_csv": str(combined_input_csv),
        "base_decision_input_csv": str(base_input_path) if base_input_path else "",
        "resolution_csv": str(resolution_csv),
        "report_path": str(report_path),
        "emit_hold_overrides": bool(args.emit_hold_overrides),
        "hold_boxscore_ids": sorted(set(args.hold_boxscore_id or [])),
    }
    write_json(summary_path, summary)
    write_report(report_path, summary, resolution_rows)
    persist(args.db_path, run_id, decision_ledger_run_id, out_dir, resolution_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
