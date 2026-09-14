#!/usr/bin/env python
"""Resolve newspaper semantic/game-key holds into auditable decision inputs.

This station handles the `verify_target_boxscore_and_game_key` lane. It does
not write promoted atoms directly. Instead, it emits decision overrides that
the existing review ledger/apply stations consume.

Conservative approval gates:
- exact source prefix/source document boxscore match is allowed when the atom is
  structurally usable;
- source-prefix mismatch is allowed only when the source article text/date
  independently supports the target game key;
- missing target games remain queued with a concrete reason.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "semantic_game_key_resolution_preps"
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
    "semantic_game_key_resolution_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "target_table",
    "target_entity_key",
    "target_boxscore_id",
    "source_prefixes_json",
    "source_document_boxscore_ids_json",
    "source_asset_dates_json",
    "target_game_date",
    "target_team_codes_json",
    "text_team_hits_json",
    "text_anchor_hits_json",
    "source_text_paths_json",
    "match_status",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "reason",
    "proposed_fields_json",
    "enriched_proposed_fields_json",
    "source_documents_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "semantic_game_key_resolution_run_id",
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
    "akron pros": "AKR",
    "akron indians": "AKR",
    "buffalo all americans": "BUF",
    "canton bulldogs": "CAN",
    "chicago cardinals": "CRD",
    "chicago tigers": "CHT",
    "cincinnati celts": "CIN",
    "cleveland tigers": "CLE",
    "columbus panhandles": "COL",
    "dayton triangles": "DAY",
    "decatur staleys": "CHI",
    "staleys": "CHI",
    "detroit heralds": "DET",
    "detroit tigers": "DET",
    "evansville crimson giants": "EVN",
    "green bay packers": "GNB",
    "green bay": "GNB",
    "hammond pros": "HAM",
    "hammond": "HAM",
    "minneapolis marines": "MIN",
    "muncie flyers": "MUN",
    "racine cardinals": "RAC",
    "rock island independents": "RII",
}

TEAM_TEXT_ALIASES = {
    "AKR": ["akron", "pros", "indians"],
    "BUF": ["buffalo", "all americans"],
    "CAN": ["canton", "bulldogs"],
    "CHT": ["chicago tigers", "tigers"],
    "CHI": ["decatur", "staleys", "stalevs"],
    "CIN": ["cincinnati", "celts"],
    "CLE": ["cleveland", "tigers"],
    "COL": ["columbus", "panhandles"],
    "CRD": ["chicago cardinals", "cardinals"],
    "DAY": ["dayton", "triangles"],
    "DET": ["detroit", "heralds", "tigers"],
    "EVN": ["evansville", "crimson giants"],
    "GNB": ["green bay", "packers", "bay"],
    "HAM": ["hammond", "pros"],
    "MIN": ["minneapolis", "marines"],
    "MUN": ["muncie", "flyers"],
    "RAC": ["racine", "cardinals"],
    "RII": ["rock island", "independents"],
}

PLAYER_BOX_SCORE_STAT_FIELDS = [
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
]

STOPWORDS = {
    "after",
    "from",
    "game",
    "goal",
    "line",
    "made",
    "over",
    "play",
    "scored",
    "through",
    "touchdown",
    "yards",
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


def latest_apply_run(con: duckdb.DuckDBPyConnection) -> str:
    if not table_exists(con, "review_decision_apply_run"):
        return ""
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
        root / "ocr_completion_resolution_preps" / "*" / "decision_input.csv",
        root / "lineup_route_resolution_preps" / "*" / "decision_input.csv",
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


def source_prefixes(source_documents_json: Any) -> list[str]:
    prefixes = []
    for doc in parse_json_list(source_documents_json):
        prefix = doc.split("#", 1)[0].strip()
        if prefix:
            prefixes.append(prefix)
    return sorted(set(prefixes))


def load_semantic_decisions(con: duckdb.DuckDBPyConnection, decision_ledger_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
          AND route_to_lane = 'verify_target_boxscore_and_game_key'
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY target_table, boxscore_id, target_entity_key, decision_id
        """,
        [decision_ledger_run_id],
    )


def load_current_semantic_route_rows(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND (
            recommended_next_action = 'verify_target_boxscore_and_game_key'
            OR route_to_lane = 'semantic_followup'
          )
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY target_table, boxscore_id, target_entity_key, decision_id
        """,
        [promotion_apply_run_id],
    )


def load_team_games(
    con: duckdb.DuckDBPyConnection, team_games_path: Path, boxscore_ids: list[str]
) -> dict[str, list[dict[str, Any]]]:
    if not boxscore_ids:
        return {}
    placeholders = ",".join(["?"] * len(boxscore_ids))
    rows = query_dicts(
        con,
        f"""
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
        WHERE boxscore_id IN ({placeholders})
        ORDER BY boxscore_id, team_code
        """,
        [str(team_games_path), *boxscore_ids],
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[clean(row.get("boxscore_id"))].append(row)
    return grouped


def load_source_documents(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.source_document
        WHERE source_document_id IN ({placeholders})
        """,
        source_document_ids,
    )
    return {clean(row.get("source_document_id")): row for row in rows}


def load_source_text_paths(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> dict[str, list[str]]:
    if not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    out: dict[str, list[str]] = defaultdict(list)
    for row in query_dicts(
        con,
        f"""
        SELECT source_document_id, region_text_path AS text_path
        FROM newspaper_review.source_region
        WHERE source_document_id IN ({placeholders})
          AND region_text_path IS NOT NULL
        """,
        source_document_ids,
    ):
        if clean(row.get("text_path")):
            out[clean(row.get("source_document_id"))].append(clean(row.get("text_path")))
    for row in query_dicts(
        con,
        f"""
        SELECT source_document_id, ocr_text_path AS text_path
        FROM newspaper_review.source_text_pass
        WHERE source_document_id IN ({placeholders})
          AND ocr_text_path IS NOT NULL
        """,
        source_document_ids,
    ):
        if clean(row.get("text_path")):
            out[clean(row.get("source_document_id"))].append(clean(row.get("text_path")))
    return {doc_id: sorted(set(paths)) for doc_id, paths in out.items()}


def latest_enriched_rows(con: duckdb.DuckDBPyConnection, table: str, run_field: str) -> dict[str, dict[str, Any]]:
    if not table_exists(con, table):
        return {}
    rows = query_dicts(
        con,
        f"""
        SELECT decision_id, enriched_proposed_fields_json, match_status, reason, {run_field} AS source_run_id
        FROM newspaper_review.{table}
        WHERE decision_id IS NOT NULL
        ORDER BY {run_field}
        """,
    )
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        out[clean(row.get("decision_id"))] = row
    return out


def parse_dates_from_text(value: Any) -> list[str]:
    text = clean(value)
    dates: list[str] = []
    for match in re.finditer(r"(19[0-9]{2})[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12][0-9]|3[01])", text):
        dates.append(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")
    return dates


def parse_date(value: Any) -> date | None:
    text = clean(value)
    if not text or text.lower() == "nat":
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def read_text(paths: list[str]) -> str:
    chunks: list[str] = []
    for path_text in paths:
        path = Path(path_text)
        if not path.exists():
            continue
        try:
            chunks.append(path.read_text(encoding="utf-8", errors="ignore"))
        except OSError:
            continue
    return "\n".join(chunks).lower()


def team_code_from_raw(raw_team: Any) -> str:
    key = " ".join(norm_words(raw_team))
    return TEAM_ALIASES.get(key, "")


def resolve_team(
    boxscore_id: str,
    raw_team: str,
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
) -> tuple[str, str]:
    rows = team_games_by_boxscore.get(boxscore_id, [])
    alias = team_code_from_raw(raw_team)
    if alias:
        for row in rows:
            if clean(row.get("team_code")) == alias:
                return alias, clean(row.get("opponent_code"))
    if len(rows) == 1:
        row = rows[0]
        return clean(row.get("team_code")), clean(row.get("opponent_code"))
    return "", ""


def normalize_event_fields(target_table: str, proposed: dict[str, Any]) -> dict[str, Any]:
    out = dict(proposed)
    if target_table == "play_by_play_event":
        if not clean(out.get("play_type")) and clean(out.get("event_type")):
            out["play_type"] = clean(out.get("event_type"))
        if not clean(out.get("possession_team_raw")) and clean(out.get("event_team_raw")):
            out["possession_team_raw"] = clean(out.get("event_team_raw"))
        if not clean(out.get("primary_player_raw")) and clean(out.get("player_raw")):
            out["primary_player_raw"] = clean(out.get("player_raw"))
        out.pop("event_type", None)
        out.pop("event_team_raw", None)
        out.pop("player_raw", None)
    elif target_table == "scoring_event":
        if clean(out.get("assisting_player_raw")) and not clean(out.get("passer_raw")):
            out["passer_raw"] = clean(out.get("assisting_player_raw"))
        out.pop("assisting_player_raw", None)
    return out


def normalize_player_box_fields(proposed: dict[str, Any]) -> dict[str, Any]:
    out = dict(proposed)
    if clean(out.get("field_goals_made")) and not clean(out.get("fg_made")):
        out["fg_made"] = clean(out.get("field_goals_made"))
        out["fg_att"] = clean(out.get("field_goal_attempts")) or clean(out.get("field_goals_made"))
    if clean(out.get("field_goal_distance_yards")) and not clean(out.get("fg_long")):
        out["fg_long"] = clean(out.get("field_goal_distance_yards"))
    if clean(out.get("goals_from_touchdown")) and not clean(out.get("pat_made")):
        out["pat_made"] = clean(out.get("goals_from_touchdown"))
    if clean(out.get("goals_from_touchdown_attempts")) and not clean(out.get("pat_att")):
        out["pat_att"] = clean(out.get("goals_from_touchdown_attempts"))
    return out


def player_box_id_bridge(
    player_box_enriched: dict[str, dict[str, Any]]
) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    for row in player_box_enriched.values():
        fields = parse_json_obj(row.get("enriched_proposed_fields_json"))
        boxscore_id = clean(fields.get("boxscore_id"))
        raw = " ".join(norm_words(fields.get("player_raw")))
        player_id = clean(fields.get("NFL_player_id"))
        if boxscore_id and raw and player_id:
            out[(boxscore_id, raw)] = player_id
    return out


def enrich_proposed(
    decision: dict[str, Any],
    event_enriched: dict[str, dict[str, Any]],
    player_box_enriched: dict[str, dict[str, Any]],
    id_bridge: dict[tuple[str, str], str],
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    target_table = clean(decision.get("target_table"))
    proposed = parse_json_obj(decision.get("proposed_fields_json"))
    enriched_source = {}
    if target_table in {"scoring_event", "play_by_play_event"}:
        enriched_source = parse_json_obj(
            event_enriched.get(clean(decision.get("decision_id")), {}).get("enriched_proposed_fields_json")
        )
        enriched = normalize_event_fields(target_table, enriched_source or proposed)
    elif target_table == "player_game_box_score":
        enriched_source = parse_json_obj(
            player_box_enriched.get(clean(decision.get("decision_id")), {}).get("enriched_proposed_fields_json")
        )
        enriched = normalize_player_box_fields(enriched_source or proposed)
    else:
        enriched = dict(proposed)

    boxscore_id = clean(enriched.get("boxscore_id")) or clean(decision.get("boxscore_id"))
    if boxscore_id and not clean(enriched.get("boxscore_id")):
        enriched["boxscore_id"] = boxscore_id

    if target_table == "scoring_event":
        raw_team = clean(enriched.get("scoring_team_raw"))
        team_code, opponent_code = resolve_team(boxscore_id, raw_team, team_games_by_boxscore)
        if team_code and not clean(enriched.get("scoring_team")):
            enriched["scoring_team"] = team_code
        raw_player = " ".join(norm_words(enriched.get("scoring_player_raw")))
        bridged = id_bridge.get((boxscore_id, raw_player))
        if bridged and not clean(enriched.get("scoring_NFL_player_id")):
            enriched["scoring_NFL_player_id"] = bridged
    elif target_table == "play_by_play_event":
        raw_team = clean(enriched.get("possession_team_raw"))
        team_code, opponent_code = resolve_team(boxscore_id, raw_team, team_games_by_boxscore)
        if team_code and not clean(enriched.get("possession_team")):
            enriched["possession_team"] = team_code
    elif target_table == "player_game_box_score":
        raw_team = clean(enriched.get("team_raw"))
        team_code, opponent_code = resolve_team(boxscore_id, raw_team, team_games_by_boxscore)
        if team_code and not clean(enriched.get("nfl_team")):
            enriched["nfl_team"] = team_code
        if opponent_code and not clean(enriched.get("opponent_nfl_team")):
            enriched["opponent_nfl_team"] = opponent_code

    if target_table in {"scoring_event", "play_by_play_event", "player_game_box_score"}:
        if not clean(enriched.get("review_status")):
            enriched["review_status"] = "semantic_game_key_resolved"
        if not clean(enriched.get("promotion_status")):
            enriched["promotion_status"] = "local_promotion_candidate"
    return enriched


def target_key(target_table: str, boxscore_id: str, enriched: dict[str, Any], fallback: str) -> str:
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
    if target_table == "play_by_play_event":
        return "|".join([
            "play_by_play_event",
            boxscore_id,
            clean(enriched.get("possession_team")),
            clean(enriched.get("primary_player_raw")),
            clean(enriched.get("play_type")),
            clean(enriched.get("yards")),
            clean(enriched.get("play_text"))[:80],
        ])
    if target_table == "player_game_box_score":
        stats = []
        for field in PLAYER_BOX_SCORE_STAT_FIELDS:
            if clean(enriched.get(field)):
                stats.append(f"{field}={clean(enriched.get(field))}")
        return "|".join([
            "player_game_box_score",
            boxscore_id,
            clean(enriched.get("NFL_player_id")) or clean(enriched.get("player_raw")),
            ",".join(stats),
        ])
    return fallback


def structural_status(target_table: str, enriched: dict[str, Any]) -> tuple[bool, str]:
    if target_table == "scoring_event":
        if not clean(enriched.get("boxscore_id")):
            return False, "missing target boxscore_id"
        if not clean(enriched.get("event_type")):
            return False, "missing scoring event_type"
        if not clean(enriched.get("scoring_team")):
            return False, "missing resolved scoring team"
        if clean(enriched.get("points")) == "":
            return False, "missing scoring points"
        if not (clean(enriched.get("play_text")) or clean(enriched.get("scoring_player_raw"))):
            return False, "missing scoring evidence text/player"
        return True, "structurally complete scoring event"
    if target_table == "play_by_play_event":
        if not clean(enriched.get("boxscore_id")):
            return False, "missing target boxscore_id"
        if not clean(enriched.get("play_type")):
            return False, "missing play_type"
        if not clean(enriched.get("play_text")):
            return False, "missing play_text"
        if not clean(enriched.get("possession_team")) and not clean(enriched.get("primary_player_raw")):
            return False, "missing resolved team/player anchor"
        return True, "structurally complete play event"
    if target_table == "player_game_box_score":
        if not clean(enriched.get("boxscore_id")):
            return False, "missing target boxscore_id"
        if not clean(enriched.get("NFL_player_id")):
            return False, "missing NFL_player_id"
        if not clean(enriched.get("nfl_team")):
            return False, "missing resolved nfl_team"
        if not any(clean(enriched.get(field)) for field in PLAYER_BOX_SCORE_STAT_FIELDS):
            return False, "missing mapped stat field"
        return True, "structurally complete player game box score"
    return False, "target table is not promotable by semantic game-key station"


def source_asset_dates(source_docs: list[dict[str, Any]]) -> list[str]:
    dates: list[str] = []
    for doc in source_docs:
        for field in ["issue_date", "asset_pdf_path", "asset_image_path", "source_url"]:
            dates.extend(parse_dates_from_text(doc.get(field)))
    return sorted(set(dates))


def date_supports_target(target_game_date: str, asset_dates: list[str]) -> bool:
    target = parse_date(target_game_date)
    if not target:
        return False
    for asset in asset_dates:
        asset_date = parse_date(asset)
        if not asset_date:
            continue
        delta = (asset_date - target).days
        if 0 <= delta <= 3:
            return True
    return False


def text_team_hits(text: str, team_codes: list[str]) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for code in team_codes:
        found = []
        for alias in TEAM_TEXT_ALIASES.get(code, [code.lower()]):
            if alias.lower() in text:
                found.append(alias)
        if found:
            hits[code] = sorted(set(found))
    return hits


def anchor_terms(target_table: str, enriched: dict[str, Any]) -> list[str]:
    raw_terms: list[str] = []
    for field in [
        "scoring_player_raw",
        "primary_player_raw",
        "secondary_player_raw",
        "passer_raw",
        "receiver_raw",
        "player_raw",
    ]:
        text = clean(enriched.get(field))
        if text:
            words = norm_words(text)
            if words:
                raw_terms.append(words[-1])
    for field in ["play_text", "source_row_text"]:
        for word in norm_words(enriched.get(field)):
            if len(word) >= 6 and word not in STOPWORDS:
                raw_terms.append(word)
    return sorted(set(raw_terms))


def text_anchor_hits(text: str, terms: list[str]) -> list[str]:
    return sorted({term for term in terms if term and term in text})


def source_support_status(
    target_boxscore_id: str,
    source_prefixes_list: list[str],
    source_doc_boxscores: list[str],
    target_game_rows: list[dict[str, Any]],
    source_docs: list[dict[str, Any]],
    source_text: str,
    enriched: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    team_codes = sorted({clean(row.get("team_code")) for row in target_game_rows if clean(row.get("team_code"))})
    target_game_date = clean(target_game_rows[0].get("game_date")) if target_game_rows else ""
    asset_dates = source_asset_dates(source_docs)
    team_hits = text_team_hits(source_text, team_codes)
    anchors = text_anchor_hits(source_text, anchor_terms("", enriched))
    date_ok = date_supports_target(target_game_date, asset_dates)
    evidence = {
        "target_game_date": target_game_date,
        "asset_dates": asset_dates,
        "team_hits": team_hits,
        "anchor_hits": anchors,
        "date_within_0_3_days_after_game": date_ok,
    }

    if target_boxscore_id in source_prefixes_list or target_boxscore_id in source_doc_boxscores:
        return "exact_source_game_key_match", "source prefix/source document boxscore matches target", evidence

    if len(team_codes) >= 2 and all(code in team_hits for code in team_codes[:2]) and anchors and date_ok:
        return (
            "article_text_date_supports_target_game",
            "source prefix differs, but article text contains target teams/player anchors and source asset date follows target game",
            evidence,
        )
    if len(team_codes) >= 2 and all(code in team_hits for code in team_codes[:2]) and len(anchors) >= 2:
        return (
            "article_text_supports_target_game",
            "source prefix differs, but article text contains target teams and multiple player/play anchors",
            evidence,
        )
    return "source_game_key_not_proven", "source text/date evidence is not strong enough to override target game-key hold", evidence


def build_resolution_rows(
    run_id: str,
    decision_ledger_run_id: str,
    decisions: list[dict[str, Any]],
    source_docs_by_id: dict[str, dict[str, Any]],
    source_text_paths_by_doc: dict[str, list[str]],
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
    event_enriched: dict[str, dict[str, Any]],
    player_box_enriched: dict[str, dict[str, Any]],
    hold_boxscore_ids: set[str],
    emit_hold_overrides: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], Counter]:
    created_at = iso_now()
    id_bridge = player_box_id_bridge(player_box_enriched)
    rows: list[dict[str, Any]] = []
    decision_inputs: list[dict[str, str]] = []
    counts: Counter = Counter()

    for decision in decisions:
        decision_id = clean(decision.get("decision_id"))
        target_table = clean(decision.get("target_table"))
        enriched = enrich_proposed(decision, event_enriched, player_box_enriched, id_bridge, team_games_by_boxscore)
        target_boxscore_id = clean(enriched.get("boxscore_id")) or clean(decision.get("boxscore_id"))
        source_docs_json = clean(decision.get("source_documents_json"))
        doc_ids = parse_json_list(source_docs_json)
        source_docs = [source_docs_by_id[doc_id] for doc_id in doc_ids if doc_id in source_docs_by_id]
        source_doc_boxscores = sorted({
            clean(doc.get("boxscore_id")) for doc in source_docs if clean(doc.get("boxscore_id"))
        })
        prefixes = source_prefixes(source_docs_json)
        text_paths = sorted({
            path for doc_id in doc_ids for path in source_text_paths_by_doc.get(doc_id, [])
        })
        source_text = read_text(text_paths)
        target_game_rows = team_games_by_boxscore.get(target_boxscore_id, [])
        target_team_codes = sorted({
            clean(row.get("team_code")) for row in target_game_rows if clean(row.get("team_code"))
        })
        structurally_complete, structural_reason = structural_status(target_table, enriched)

        if not target_table:
            match_status = "document_level_semantic_followup"
            reason = "document has no target atom table yet; keep in semantic followup"
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "semantic_followup"
        elif target_boxscore_id in hold_boxscore_ids:
            match_status = "explicit_hold_boxscore"
            reason = f"boxscore_id {target_boxscore_id} is on the explicit hold list"
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "semantic_followup"
        elif not target_boxscore_id:
            match_status = "missing_target_boxscore"
            reason = "atom still needs a target boxscore_id before it can promote"
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "semantic_followup"
        elif not target_game_rows:
            match_status = "target_boxscore_not_in_team_games"
            reason = "target boxscore_id was not found in PFR team-games parquet"
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "semantic_followup"
        elif not structurally_complete:
            match_status = "structural_hold"
            reason = structural_reason
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "semantic_followup"
        else:
            match_status, support_reason, evidence = source_support_status(
                target_boxscore_id,
                prefixes,
                source_doc_boxscores,
                target_game_rows,
                source_docs,
                source_text,
                enriched,
            )
            if match_status in {
                "exact_source_game_key_match",
                "article_text_date_supports_target_game",
                "article_text_supports_target_game",
            }:
                decision_status = "approved"
                decision_value = PROMOTION_VALUE
                route = "local_promotion"
                reason = f"{support_reason}; {structural_reason}"
            else:
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "semantic_followup"
                reason = support_reason
        if "evidence" not in locals():
            evidence = {
                "target_game_date": clean(target_game_rows[0].get("game_date")) if target_game_rows else "",
                "asset_dates": source_asset_dates(source_docs),
                "team_hits": text_team_hits(source_text, target_team_codes),
                "anchor_hits": text_anchor_hits(source_text, anchor_terms(target_table, enriched)),
                "date_within_0_3_days_after_game": date_supports_target(
                    clean(target_game_rows[0].get("game_date")) if target_game_rows else "",
                    source_asset_dates(source_docs),
                ),
            }

        resolved_key = target_key(target_table, target_boxscore_id, enriched, clean(decision.get("target_entity_key")))
        if decision_status == "approved" or emit_hold_overrides:
            decision_inputs.append({
                "decision_id": decision_id,
                "decision_status": decision_status,
                "decision_value": decision_value,
                "route_to_lane": route,
                "resolved_boxscore_id": target_boxscore_id,
                "resolved_target_table": target_table,
                "resolved_target_entity_key": resolved_key,
                "proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
                "notes": reason,
            })

        row = {
            "semantic_game_key_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "decision_id": decision_id,
            "target_table": target_table,
            "target_entity_key": clean(decision.get("target_entity_key")),
            "target_boxscore_id": target_boxscore_id,
            "source_prefixes_json": json.dumps(prefixes, sort_keys=True),
            "source_document_boxscore_ids_json": json.dumps(source_doc_boxscores, sort_keys=True),
            "source_asset_dates_json": json.dumps(evidence.get("asset_dates", []), sort_keys=True),
            "target_game_date": clean(evidence.get("target_game_date")),
            "target_team_codes_json": json.dumps(target_team_codes, sort_keys=True),
            "text_team_hits_json": json.dumps(evidence.get("team_hits", {}), sort_keys=True),
            "text_anchor_hits_json": json.dumps(evidence.get("anchor_hits", []), sort_keys=True),
            "source_text_paths_json": json.dumps(text_paths, sort_keys=True),
            "match_status": match_status,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": route,
            "reason": reason,
            "proposed_fields_json": clean(decision.get("proposed_fields_json")),
            "enriched_proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
            "source_documents_json": source_docs_json,
            "created_at_utc": created_at,
        }
        rows.append(row)
        counts[match_status] += 1
        counts[f"decision_{decision_status}"] += 1
        del evidence

    return rows, decision_inputs, counts


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    defs = ", ".join(f"{field} VARCHAR" for field in RESOLUTION_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.semantic_game_key_resolution_decision ({defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'newspaper_review'
              AND table_name = 'semantic_game_key_resolution_decision'
            """
        ).fetchall()
    }
    for field in RESOLUTION_FIELDS:
        if field not in existing:
            con.execute(
                f"ALTER TABLE newspaper_review.semantic_game_key_resolution_decision ADD COLUMN IF NOT EXISTS {field} VARCHAR"
            )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.semantic_game_key_resolution_run (
          semantic_game_key_resolution_run_id VARCHAR,
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
            """
            DELETE FROM newspaper_review.semantic_game_key_resolution_decision
            WHERE semantic_game_key_resolution_run_id = ?
            """,
            [run_id],
        )
        con.execute(
            """
            DELETE FROM newspaper_review.semantic_game_key_resolution_run
            WHERE semantic_game_key_resolution_run_id = ?
            """,
            [run_id],
        )
        insert_rows(con, "semantic_game_key_resolution_decision", rows, RESOLUTION_FIELDS)
        insert_rows(con, "semantic_game_key_resolution_run", [{
            "semantic_game_key_resolution_run_id": run_id,
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
        "# Newspaper Semantic Game-Key Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['semantic_game_key_resolution_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Semantic rows reviewed: `{summary['semantic_decision_count']}`",
        f"- Approved for local promotion: `{summary['approved_count']}`",
        f"- Held/routed: `{summary['held_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Approved Samples",
        "",
        "| table | boxscore | status | reason | anchors |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in [item for item in rows if item.get("decision_status") == "approved"][:40]:
        lines.append(
            f"| {row['target_table']} | {row['target_boxscore_id']} | {row['match_status']} | "
            f"{row['reason']} | {row['text_anchor_hits_json']} |"
        )
    lines.extend(["", "## Held Samples", "", "| table | boxscore | status | reason |", "| --- | --- | --- | --- |"])
    for row in [item for item in rows if item.get("decision_status") != "approved"][:60]:
        lines.append(
            f"| {row['target_table']} | {row['target_boxscore_id']} | {row['match_status']} | {row['reason']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument(
        "--promotion-apply-run-id",
        default="",
        help="Read semantic/game-key rows from this local apply run's route queue.",
    )
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="semantic_game_key_resolution_prep")
    parser.add_argument(
        "--emit-hold-overrides",
        action="store_true",
        help="Write pending/hold overrides for rows that fail semantic game-key gates.",
    )
    parser.add_argument(
        "--hold-boxscore-id",
        action="append",
        default=["192011070rii"],
        help="Boxscore id to keep out of automatic local promotion. Can be repeated.",
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
        promotion_apply_run_id = clean(args.promotion_apply_run_id)
        if promotion_apply_run_id:
            decisions = load_current_semantic_route_rows(read_con, promotion_apply_run_id)
        else:
            decisions = load_semantic_decisions(read_con, decision_ledger_run_id)
        target_boxscore_ids = sorted({
            clean(parse_json_obj(row.get("proposed_fields_json")).get("boxscore_id")) or clean(row.get("boxscore_id"))
            for row in decisions
            if clean(parse_json_obj(row.get("proposed_fields_json")).get("boxscore_id")) or clean(row.get("boxscore_id"))
        })
        source_document_ids = sorted({
            doc_id for row in decisions for doc_id in parse_json_list(row.get("source_documents_json"))
        })
        team_games_by_boxscore = load_team_games(read_con, args.team_games_path, target_boxscore_ids)
        source_docs_by_id = load_source_documents(read_con, source_document_ids)
        source_text_paths_by_doc = load_source_text_paths(read_con, source_document_ids)
        event_enriched = latest_enriched_rows(
            read_con, "event_detail_resolution_decision", "event_detail_resolution_run_id"
        )
        player_box_enriched = latest_enriched_rows(
            read_con, "player_box_score_resolution_decision", "player_box_score_resolution_run_id"
        )
    finally:
        read_con.close()

    resolution_rows, semantic_input_rows, counts = build_resolution_rows(
        run_id,
        decision_ledger_run_id,
        decisions,
        source_docs_by_id,
        source_text_paths_by_doc,
        team_games_by_boxscore,
        event_enriched,
        player_box_enriched,
        set(args.hold_boxscore_id or []),
        args.emit_hold_overrides,
    )

    base_input_path = args.base_decision_input_csv or latest_decision_input(DEFAULT_ROOT)
    base_rows = read_base_decisions(base_input_path)
    combined_rows = combine_decision_inputs(base_rows, semantic_input_rows)

    resolution_csv = out_dir / "semantic_game_key_resolution_decisions.csv"
    semantic_input_csv = out_dir / "semantic_game_key_decision_input.csv"
    combined_input_csv = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    report_path = out_dir / "semantic_game_key_resolution_report.md"

    write_csv(resolution_csv, resolution_rows, RESOLUTION_FIELDS)
    write_csv(semantic_input_csv, semantic_input_rows, DECISION_INPUT_FIELDS)
    write_csv(combined_input_csv, combined_rows, DECISION_INPUT_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "semantic_game_key_resolution_run_id": run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "semantic_decision_count": len(resolution_rows),
        "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
        "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
        "match_status_counts": dict(Counter(row.get("match_status") for row in resolution_rows)),
        "decision_status_counts": dict(Counter(row.get("decision_status") for row in resolution_rows)),
        "semantic_decision_input_csv": str(semantic_input_csv),
        "combined_decision_input_csv": str(combined_input_csv),
        "base_decision_input_csv": str(base_input_path) if base_input_path else "",
        "resolution_csv": str(resolution_csv),
        "report_path": str(report_path),
        "hold_boxscore_ids": sorted(set(args.hold_boxscore_id or [])),
        "emit_hold_overrides": bool(args.emit_hold_overrides),
    }
    write_json(summary_path, summary)
    write_report(report_path, summary, resolution_rows)
    persist(args.db_path, run_id, decision_ledger_run_id, out_dir, resolution_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
