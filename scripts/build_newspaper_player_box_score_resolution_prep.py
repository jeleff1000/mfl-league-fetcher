#!/usr/bin/env python
"""Resolve newspaper player box-score stat rows into local promotion inputs.

This station consumes held `player_game_box_score` decisions and promotes only
numeric stat atoms that can be mapped into the local newspaper box-score model
with resolved game/team/player identity. It writes only D-drive artifacts and
local DuckDB review tables.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import glob
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "player_box_score_resolution_preps"
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

RESOLUTION_FIELDS = [
    "player_box_score_resolution_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "promotion_package_id",
    "boxscore_id",
    "source_boxscore_prefixes_json",
    "game_date",
    "year",
    "week",
    "raw_team",
    "resolved_team",
    "opponent_team",
    "raw_player",
    "resolved_NFL_player_id",
    "resolved_player",
    "resolved_player_week",
    "match_status",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "stat_fields_json",
    "reason",
    "proposed_fields_json",
    "enriched_proposed_fields_json",
    "source_documents_json",
    "created_at_utc",
]

CANDIDATE_FIELDS = [
    "player_box_score_resolution_run_id",
    "decision_id",
    "boxscore_id",
    "raw_player",
    "raw_team",
    "candidate_rank",
    "candidate_score",
    "candidate_pfr_id",
    "candidate_player",
    "candidate_position",
    "first_year",
    "last_year",
    "match_notes",
    "created_at_utc",
]

RUN_FIELDS = [
    "player_box_score_resolution_run_id",
    "decision_ledger_run_id",
    "output_dir",
    "box_score_decision_count",
    "approved_count",
    "held_count",
    "candidate_count",
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
    "chicago bears": "CHI",
    "chicago tigers": "CHT",
    "cincinnati celts": "CIN",
    "cleveland tigers": "CLE",
    "columbus panhandles": "COL",
    "dayton triangles": "DAY",
    "decatur staleys": "CHI",
    "staleys": "CHI",
    "detroit heralds": "DET",
    "evansville crimson giants": "EVN",
    "green bay packers": "GNB",
    "hammond pros": "HAM",
    "hammond": "HAM",
    "louisville": "LOU",
    "louisville brecks": "LOU",
    "minneapolis marines": "MIN",
    "muncie flyers": "MUN",
    "racine": "RAC",
    "racine cardinals": "RAC",
    "racine legion": "RAC",
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


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean(value).lower())


def norm_words(value: Any) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", clean(value).lower()) if token]


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
        return [clean(item) for item in value]
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [clean(item) for item in parsed]


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
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


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
    paths: list[Path] = []
    for folder in [
        "review_decision_inputs",
        "lineup_identity_resolution_preps",
        "event_detail_resolution_preps",
        "player_box_score_resolution_preps",
    ]:
        paths.extend(Path(path) for path in glob.glob(str(root / folder / "*" / "decision_input.csv")))
    if not paths:
        return None
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0]


def load_box_score_decisions(
    con: duckdb.DuckDBPyConnection,
    decision_ledger_run_id: str,
    include_local_promotion_box_scores: bool,
) -> list[dict[str, Any]]:
    route_filter = "route_to_lane = 'quality_review_decide_promote_followup_or_reject'"
    if include_local_promotion_box_scores:
        route_filter = """
        (
          route_to_lane = 'quality_review_decide_promote_followup_or_reject'
          OR route_to_lane = 'local_promotion'
          OR decision_value = 'approved_for_local_promotion'
        )
        """
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
          AND target_table = 'player_game_box_score'
          AND {route_filter}
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY boxscore_id, target_entity_key
        """,
        [decision_ledger_run_id],
    )


def load_current_box_score_route_rows(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND target_table = 'player_game_box_score'
          AND recommended_next_action IN (
            'quality_review_decide_promote_followup_or_reject',
            'evidence_check_then_promote',
            'verify_target_boxscore_and_game_key',
            'resolve_player_or_team_identity_then_promote'
          )
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY boxscore_id, target_entity_key, decision_id
        """,
        [promotion_apply_run_id],
    )


def load_team_games(con: duckdb.DuckDBPyConnection, team_games_path: Path, boxscore_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not boxscore_ids:
        return {}
    rel = str(team_games_path).replace("\\", "/").replace("'", "''")
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
        FROM read_parquet('{rel}')
        WHERE boxscore_id IN ({placeholders})
        ORDER BY boxscore_id, team_code
        """,
        boxscore_ids,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[clean(row.get("boxscore_id"))].append(row)
    return grouped


def load_player_index(con: duckdb.DuckDBPyConnection, player_index_path: Path, min_year: int, max_year: int) -> list[dict[str, Any]]:
    rel = str(player_index_path).replace("\\", "/").replace("'", "''")
    rows = query_dicts(
        con,
        f"""
        SELECT
          pfr_id,
          player,
          index_position,
          TRY_CAST(first_year AS INTEGER) AS first_year,
          TRY_CAST(last_year AS INTEGER) AS last_year
        FROM read_parquet('{rel}')
        WHERE TRY_CAST(first_year AS INTEGER) <= ?
          AND TRY_CAST(last_year AS INTEGER) >= ?
          AND pfr_id IS NOT NULL
          AND player IS NOT NULL
        """,
        [max_year, min_year],
    )
    for row in rows:
        tokens = norm_words(row.get("player"))
        row["_tokens"] = tokens
        row["_first"] = tokens[0] if tokens else ""
        row["_last"] = tokens[-1] if tokens else ""
        row["_first_initial"] = row["_first"][:1]
        row["_full_norm"] = norm(row.get("player"))
    return rows


def load_v26_team_year_ids(con: duckdb.DuckDBPyConnection, v26_path: Path, min_year: int, max_year: int) -> set[tuple[str, int, str]]:
    if not v26_path.exists():
        return set()
    rel = str(v26_path).replace("\\", "/").replace("'", "''")
    rows = query_dicts(
        con,
        f"""
        SELECT DISTINCT NFL_player_id, TRY_CAST(year AS INTEGER) AS year, nfl_team
        FROM read_parquet('{rel}')
        WHERE TRY_CAST(year AS INTEGER) BETWEEN ? AND ?
          AND NFL_player_id IS NOT NULL
          AND nfl_team IS NOT NULL
        """,
        [min_year, max_year],
    )
    return {(clean(row.get("NFL_player_id")), int(row.get("year")), clean(row.get("nfl_team"))) for row in rows if row.get("year") is not None}


def load_promoted_scoring_events(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    if not promotion_apply_run_id:
        return {}
    rows = query_dicts(
        con,
        """
        SELECT boxscore_id, scoring_team, scoring_team_raw, scoring_player_raw,
               scoring_NFL_player_id, event_type, points, play_text
        FROM newspaper_promoted.scoring_event
        WHERE promotion_apply_run_id = ?
        """,
        [promotion_apply_run_id],
    )
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (clean(row.get("boxscore_id")), norm(row.get("scoring_player_raw")))
        grouped[key].append(row)
        if clean(row.get("scoring_NFL_player_id")):
            grouped[(clean(row.get("boxscore_id")), norm(row.get("scoring_NFL_player_id")))].append(row)
    return grouped


def source_prefixes(source_documents_json: Any, *fallback_docs: Any) -> list[str]:
    prefixes = []
    for doc in parse_json_list(source_documents_json):
        prefix = clean(doc).split("#", 1)[0].strip()
        if prefix:
            prefixes.append(prefix)
    for doc in fallback_docs:
        prefix = clean(doc).split("#", 1)[0].strip()
        if prefix:
            prefixes.append(prefix)
    return sorted(set(prefixes))


def resolve_team(boxscore_id: str, raw_team: str, team_games_by_boxscore: dict[str, list[dict[str, Any]]]) -> tuple[str, str, dict[str, Any]]:
    raw_key = " ".join(norm_words(raw_team))
    alias = TEAM_ALIASES.get(raw_key, "")
    rows = team_games_by_boxscore.get(boxscore_id, [])
    raw_code = clean(raw_team).upper()
    if raw_code:
        for row in rows:
            if clean(row.get("team_code")) == raw_code:
                return raw_code, clean(row.get("opponent_code")), row
    if alias:
        for row in rows:
            if clean(row.get("team_code")) == alias:
                return alias, clean(row.get("opponent_code")), row
    if len(rows) == 1:
        row = rows[0]
        return clean(row.get("team_code")), clean(row.get("opponent_code")), row
    return "", "", rows[0] if rows else {}


def player_candidate_score(
    raw_player: str,
    candidate: dict[str, Any],
    year: int,
    team_code: str,
    v26_team_year_ids: set[tuple[str, int, str]],
) -> tuple[int, dict[str, str]]:
    tokens = norm_words(raw_player)
    if not tokens:
        return 0, {}
    raw_last = tokens[-1]
    raw_first = tokens[0] if len(tokens) > 1 else ""
    full_norm = norm(raw_player)
    cand_last = clean(candidate.get("_last"))
    last_match = "exact"
    if raw_last != cand_last:
        ratio = difflib.SequenceMatcher(None, raw_last, cand_last).ratio()
        if ratio >= 0.86:
            last_match = "fuzzy"
        else:
            return 0, {}
    if last_match == "fuzzy":
        score = 48
    else:
        score = 60
    notes = {"last_name_match": last_match, "first_match": "not_supplied", "team_year_seen_in_v26": "0"}
    if full_norm == clean(candidate.get("_full_norm")):
        score += 50
        notes["first_match"] = "full_name"
    elif raw_first:
        cand_first = clean(candidate.get("_first"))
        if len(raw_first) == 1 and raw_first == clean(candidate.get("_first_initial")):
            score += 20
            notes["first_match"] = "initial"
        elif len(raw_first) > 1 and (cand_first.startswith(raw_first) or raw_first.startswith(cand_first)):
            score += 18
            notes["first_match"] = "prefix"
        else:
            score -= 10
            notes["first_match"] = "no"
    pfr_id = clean(candidate.get("pfr_id"))
    if (pfr_id, year, team_code) in v26_team_year_ids:
        score += 20
        notes["team_year_seen_in_v26"] = "1"
    return score, notes


def resolve_player(
    raw_player: str,
    candidates: list[dict[str, Any]],
    year: int,
    team_code: str,
    v26_team_year_ids: set[tuple[str, int, str]],
) -> tuple[str, str, list[dict[str, Any]], str]:
    scored: list[dict[str, Any]] = []
    for candidate in candidates:
        first_year = candidate.get("first_year")
        last_year = candidate.get("last_year")
        if first_year is None or last_year is None or int(first_year) > year or int(last_year) < year:
            continue
        score, notes = player_candidate_score(raw_player, candidate, year, team_code, v26_team_year_ids)
        if score <= 0:
            continue
        scored.append({
            "candidate_score": score,
            "candidate_pfr_id": clean(candidate.get("pfr_id")),
            "candidate_player": clean(candidate.get("player")),
            "candidate_position": clean(candidate.get("index_position")),
            "first_year": clean(candidate.get("first_year")),
            "last_year": clean(candidate.get("last_year")),
            "match_notes": json.dumps(notes, sort_keys=True),
        })
    scored.sort(key=lambda row: (-int(row["candidate_score"]), row["candidate_player"], row["candidate_pfr_id"]))
    if not scored:
        return "", "", [], "unmatched_player"
    best = scored[0]
    second_score = int(scored[1]["candidate_score"]) if len(scored) > 1 else 0
    margin = int(best["candidate_score"]) - second_score
    if int(best["candidate_score"]) >= 80 and (len(scored) == 1 or margin >= 15):
        return clean(best["candidate_pfr_id"]), clean(best["candidate_player"]), scored, "resolved_player"
    if int(best["candidate_score"]) >= 75 and len(scored) == 1:
        return clean(best["candidate_pfr_id"]), clean(best["candidate_player"]), scored, "resolved_player"
    if int(best["candidate_score"]) >= 60 and len(scored) == 1 and '"last_name_match": "exact"' in clean(best.get("match_notes")):
        return clean(best["candidate_pfr_id"]), clean(best["candidate_player"]), scored, "resolved_player"
    if int(best["candidate_score"]) >= 48 and len(scored) == 1 and '"last_name_match": "fuzzy"' in clean(best.get("match_notes")):
        return clean(best["candidate_pfr_id"]), clean(best["candidate_player"]), scored, "resolved_player"
    return "", "", scored, "ambiguous_player"


def infer_td_stats_from_scoring_events(
    boxscore_id: str,
    player_raw: str,
    nfl_player_id: str,
    scoring_events: dict[tuple[str, str], list[dict[str, Any]]],
) -> dict[str, int]:
    matched = list(scoring_events.get((boxscore_id, norm(player_raw)), []))
    if nfl_player_id:
        matched.extend(scoring_events.get((boxscore_id, norm(nfl_player_id)), []))
    seen: set[tuple[str, str]] = set()
    stats = {"rushing_tds": 0, "receiving_tds": 0, "passing_tds": 0}
    for event in matched:
        key = (clean(event.get("event_type")), clean(event.get("play_text")))
        if key in seen:
            continue
        seen.add(key)
        event_type = clean(event.get("event_type")).lower()
        play_text = clean(event.get("play_text")).lower()
        if "touchdown" not in event_type:
            continue
        if "reception" in event_type or "pass" in play_text or "forward" in play_text:
            stats["receiving_tds"] += 1
        elif "pass" in event_type and "touchdown" in event_type:
            stats["passing_tds"] += 1
        elif "run" in event_type or "rush" in event_type or "plung" in play_text or "buck" in play_text:
            stats["rushing_tds"] += 1
        else:
            stats["rushing_tds"] += 1
    return {key: value for key, value in stats.items() if value}


def build_stat_fields(
    proposed: dict[str, Any],
    boxscore_id: str,
    player_raw: str,
    nfl_player_id: str,
    scoring_events: dict[tuple[str, str], list[dict[str, Any]]],
) -> tuple[dict[str, int], list[str]]:
    stats: dict[str, int] = {}
    reasons: list[str] = []

    passthrough_fields = [
        "rushing_yards",
        "rushing_tds",
        "receiving_tds",
        "passing_tds",
        "passing_interceptions",
        "pat_made",
        "pat_att",
        "fg_made",
        "fg_att",
        "fg_long",
    ]
    for field in passthrough_fields:
        parsed = parse_int(proposed.get(field))
        if parsed is not None:
            stats[field] = parsed
            reasons.append(f"kept existing mapped {field}")

    rushing_yards = parse_int(proposed.get("reported_rushing_yards"))
    if rushing_yards is not None:
        stats["rushing_yards"] = rushing_yards
        reasons.append("mapped reported_rushing_yards to rushing_yards")

    goals = parse_int(proposed.get("goals_from_touchdown"))
    if goals is not None:
        stats["pat_made"] = goals
        attempts = parse_int(proposed.get("goals_from_touchdown_attempts"))
        misses = parse_int(proposed.get("goals_from_touchdown_missed"))
        if attempts is not None:
            stats["pat_att"] = attempts
        elif misses is not None:
            stats["pat_att"] = goals + misses
        else:
            stats["pat_att"] = goals
        reasons.append("mapped goals_from_touchdown to pat_made/pat_att")

    missed_goals = parse_int(proposed.get("goals_from_touchdown_missed"))
    if missed_goals is not None and goals is None:
        stats["pat_made"] = 0
        stats["pat_att"] = missed_goals
        reasons.append("mapped goals_from_touchdown_missed to pat_made/pat_att")

    field_goals = parse_int(proposed.get("field_goals_made"))
    if field_goals is None:
        field_goals = parse_int(proposed.get("drop_kick_made"))
    if field_goals is not None:
        stats["fg_made"] = field_goals
        stats["fg_att"] = max(field_goals, parse_int(proposed.get("field_goal_attempts")) or field_goals)
        distance = parse_int(proposed.get("field_goal_distance_yards"))
        if distance is None:
            distance = parse_int(proposed.get("drop_kick_distance_yards"))
        if distance is not None:
            stats["fg_long"] = distance
        reasons.append("mapped field goal/drop kick fields to fg_made/fg_att/fg_long")

    touchdowns = parse_int(proposed.get("touchdowns"))
    if touchdowns is not None:
        td_stats = infer_td_stats_from_scoring_events(boxscore_id, player_raw, nfl_player_id, scoring_events)
        already_mapped_tds = sum(stats.get(field, 0) for field in ["rushing_tds", "receiving_tds", "passing_tds"])
        if already_mapped_tds:
            reasons.append("touchdowns already represented by mapped TD fields")
        elif td_stats and sum(td_stats.values()) == touchdowns:
            stats.update(td_stats)
            reasons.append("mapped touchdowns using promoted scoring-event types")
        elif td_stats and sum(td_stats.values()) < touchdowns:
            stats.update(td_stats)
            stats["touchdowns"] = touchdowns
            reasons.append("partially mapped touchdowns using promoted scoring-event types; kept generic total touchdowns")
        else:
            stats["touchdowns"] = touchdowns
            reasons.append("kept generic total touchdowns; no unambiguous scoring-event type mapping")

    return stats, reasons


def target_key(boxscore_id: str, nfl_player_id: str, stat_fields: dict[str, int]) -> str:
    stat_part = ",".join(f"{key}={value}" for key, value in sorted(stat_fields.items()))
    return f"player_game_box_score|{boxscore_id}|{nfl_player_id}|{stat_part}"


def read_base_decisions(path: Path | None) -> list[dict[str, str]]:
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def combine_decision_inputs(base_rows: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for row in base_rows:
        decision_id = clean(row.get("decision_id"))
        if decision_id:
            merged[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    for row in new_rows:
        decision_id = clean(row.get("decision_id"))
        if decision_id:
            merged[decision_id] = {field: clean(row.get(field)) for field in DECISION_INPUT_FIELDS}
    return list(merged.values())


def build_resolution_rows(
    run_id: str,
    decision_ledger_run_id: str,
    decisions: list[dict[str, Any]],
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
    player_candidates: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, int, str]],
    scoring_events: dict[tuple[str, str], list[dict[str, Any]]],
    hold_boxscore_ids: set[str],
    emit_hold_overrides: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]], Counter]:
    created_at = iso_now()
    resolution_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    decision_inputs: list[dict[str, str]] = []
    counts: Counter = Counter()

    for decision in decisions:
        proposed = parse_json_obj(decision.get("proposed_fields_json"))
        boxscore_id = clean(proposed.get("boxscore_id")) or clean(decision.get("boxscore_id"))
        prefixes = source_prefixes(
            decision.get("source_documents_json"),
            decision.get("source_document_id"),
            proposed.get("source_document_id"),
        )
        raw_team = (
            clean(proposed.get("team_raw"))
            or clean(proposed.get("nfl_team"))
            or clean(proposed.get("scoring_team_raw"))
            or clean(proposed.get("scoring_team"))
        )
        raw_player = clean(proposed.get("player_raw"))
        team_code, opponent_code, game_row = resolve_team(boxscore_id, raw_team, team_games_by_boxscore)
        year = int(game_row.get("year") or 0) if game_row else 0
        week = int(game_row.get("week") or 0) if game_row else 0
        player_id, player_name, candidates, player_status = resolve_player(
            raw_player,
            player_candidates,
            year,
            team_code,
            v26_team_year_ids,
        ) if year and team_code else ("", "", [], "unresolved_game_or_team")

        for rank, cand in enumerate(candidates[:8], start=1):
            candidate_rows.append({
                "player_box_score_resolution_run_id": run_id,
                "decision_id": clean(decision.get("decision_id")),
                "boxscore_id": boxscore_id,
                "raw_player": raw_player,
                "raw_team": raw_team,
                "candidate_rank": str(rank),
                "created_at_utc": created_at,
                **cand,
            })

        stat_fields, stat_reasons = build_stat_fields(proposed, boxscore_id, raw_player, player_id, scoring_events)
        enriched = dict(proposed)
        enriched.update({
            "boxscore_id": boxscore_id,
            "player_raw": raw_player,
            "NFL_player_id": player_id,
            "player_week": f"{player_id}_{year}_{week}" if player_id and year and week else "",
            "nfl_team": team_code,
            "opponent_nfl_team": opponent_code,
            "review_status": "box_score_resolved",
            "promotion_status": "local_promotion_candidate",
        })
        enriched.update(stat_fields)

        match_status = "ready_for_local_promotion"
        reason_parts = list(stat_reasons)
        if not boxscore_id:
            match_status = "needs_game_mapping"
            reason_parts.append("boxscore_id is missing")
        elif boxscore_id in hold_boxscore_ids:
            match_status = "held_boxscore_conflict"
            reason_parts.append(f"boxscore_id {boxscore_id} is in explicit hold list")
        elif prefixes and boxscore_id not in prefixes:
            match_status = "source_boxscore_mismatch"
            reason_parts.append(f"source document prefixes {prefixes} do not include {boxscore_id}")
        elif not team_code:
            match_status = "unresolved_team"
            reason_parts.append("team could not be resolved against PFR team-game rows")
        elif not player_id:
            match_status = player_status
            reason_parts.append("player identity is not safely resolved")
        elif not stat_fields:
            match_status = "no_mapped_numeric_stat"
            reason_parts.append("no numeric stat field could be mapped into player_game_box_score")

        approved = match_status == "ready_for_local_promotion"
        decision_status = "approved" if approved else "pending"
        decision_value = PROMOTION_VALUE if approved else "pending"
        route_to_lane = "local_promotion" if approved else "quality_review_decide_promote_followup_or_reject"
        if match_status in {"needs_game_mapping", "source_boxscore_mismatch", "unresolved_team"}:
            route_to_lane = "verify_target_boxscore_and_game_key"
        elif match_status in {"unmatched_player", "ambiguous_player", "unresolved_game_or_team"}:
            route_to_lane = "resolve_player_or_team_identity_then_promote"

        resolution_rows.append({
            "player_box_score_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "decision_id": clean(decision.get("decision_id")),
            "promotion_package_id": clean(decision.get("promotion_package_id")),
            "boxscore_id": boxscore_id,
            "source_boxscore_prefixes_json": json.dumps(prefixes, sort_keys=True),
            "game_date": clean(game_row.get("game_date")) if game_row else "",
            "year": str(year) if year else "",
            "week": str(week) if week else "",
            "raw_team": raw_team,
            "resolved_team": team_code,
            "opponent_team": opponent_code,
            "raw_player": raw_player,
            "resolved_NFL_player_id": player_id,
            "resolved_player": player_name,
            "resolved_player_week": clean(enriched.get("player_week")),
            "match_status": match_status,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": route_to_lane,
            "stat_fields_json": json.dumps(stat_fields, sort_keys=True),
            "reason": "; ".join(reason_parts),
            "proposed_fields_json": clean(decision.get("proposed_fields_json")),
            "enriched_proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
            "source_documents_json": clean(decision.get("source_documents_json")),
            "created_at_utc": created_at,
        })
        counts[match_status] += 1
        counts[decision_status] += 1

        if approved or emit_hold_overrides:
            decision_inputs.append({
                "decision_id": clean(decision.get("decision_id")),
                "decision_status": decision_status,
                "decision_value": decision_value,
                "route_to_lane": route_to_lane,
                "resolved_boxscore_id": boxscore_id if approved else "",
                "resolved_target_table": "player_game_box_score" if approved else "",
                "resolved_target_entity_key": target_key(boxscore_id, player_id, stat_fields) if approved else "",
                "proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False) if approved else "",
                "notes": "; ".join(reason_parts),
            })
    return resolution_rows, candidate_rows, decision_inputs, counts


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table, fields in [
        ("player_box_score_resolution_decision", RESOLUTION_FIELDS),
        ("player_box_score_resolution_candidate", CANDIDATE_FIELDS),
    ]:
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.{table} ({defs})")
        existing = {
            row[0] for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review'
                  AND table_name=?
                """,
                [table],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.player_box_score_resolution_run (
          player_box_score_resolution_run_id VARCHAR,
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          box_score_decision_count INTEGER,
          approved_count INTEGER,
          held_count INTEGER,
          candidate_count INTEGER,
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
        f"INSERT INTO newspaper_review.{table} ({','.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    run_id: str,
    decision_ledger_run_id: str,
    out_dir: Path,
    resolution_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        for table, id_col in [
            ("player_box_score_resolution_decision", "player_box_score_resolution_run_id"),
            ("player_box_score_resolution_candidate", "player_box_score_resolution_run_id"),
            ("player_box_score_resolution_run", "player_box_score_resolution_run_id"),
        ]:
            con.execute(f"DELETE FROM newspaper_review.{table} WHERE {id_col} = ?", [run_id])
        insert_rows(con, "player_box_score_resolution_decision", resolution_rows, RESOLUTION_FIELDS)
        insert_rows(con, "player_box_score_resolution_candidate", candidate_rows, CANDIDATE_FIELDS)
        insert_rows(con, "player_box_score_resolution_run", [{
            "player_box_score_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "box_score_decision_count": len(resolution_rows),
            "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
            "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
            "candidate_count": len(candidate_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_report(path: Path, summary: dict[str, Any], resolution_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Player Box Score Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['player_box_score_resolution_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Rows reviewed: `{summary['box_score_decision_count']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Held/routed: `{summary['held_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Held Samples",
        "",
        "| boxscore | team | player | status | stats | reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in [row for row in resolution_rows if row.get("decision_status") != "approved"][:60]:
        lines.append(
            f"| {row['boxscore_id']} | {row['raw_team']} | {row['raw_player']} | "
            f"{row['match_status']} | {row['stat_fields_json']} | {row['reason']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="player_box_score_resolution_prep")
    parser.add_argument(
        "--include-local-promotion-box-scores",
        action="store_true",
        help="Also re-normalize already approved local player box-score decisions.",
    )
    parser.add_argument(
        "--emit-hold-overrides",
        action="store_true",
        help="Write pending/hold overrides for processed rows that fail promotion gates.",
    )
    parser.add_argument(
        "--hold-boxscore-id",
        action="append",
        default=["192011070rii"],
        help="Boxscore id to keep out of automatic box-score promotion. Can be repeated.",
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
        apply_run_id = args.promotion_apply_run_id or latest_apply_run(read_con)
        if args.promotion_apply_run_id:
            decisions = load_current_box_score_route_rows(read_con, apply_run_id)
        else:
            decisions = load_box_score_decisions(
                read_con,
                decision_ledger_run_id,
                args.include_local_promotion_box_scores,
            )
        boxscore_ids = sorted({
            clean(parse_json_obj(row.get("proposed_fields_json")).get("boxscore_id")) or clean(row.get("boxscore_id"))
            for row in decisions
            if clean(parse_json_obj(row.get("proposed_fields_json")).get("boxscore_id")) or clean(row.get("boxscore_id"))
        })
        team_games_by_boxscore = load_team_games(read_con, args.team_games_path, boxscore_ids)
        years = [
            int(row.get("year"))
            for rows in team_games_by_boxscore.values()
            for row in rows
            if row.get("year") is not None
        ]
        min_year = min(years) if years else 1920
        max_year = max(years) if years else 1939
        player_candidates = load_player_index(read_con, args.player_index_path, min_year, max_year)
        v26_team_year_ids = load_v26_team_year_ids(read_con, args.v26_path, min_year, max_year)
        scoring_events = load_promoted_scoring_events(read_con, apply_run_id)
    finally:
        read_con.close()

    resolution_rows, candidate_rows, box_score_input_rows, counts = build_resolution_rows(
        run_id,
        decision_ledger_run_id,
        decisions,
        team_games_by_boxscore,
        player_candidates,
        v26_team_year_ids,
        scoring_events,
        set(args.hold_boxscore_id or []),
        args.emit_hold_overrides,
    )

    base_input_path = args.base_decision_input_csv or latest_decision_input(DEFAULT_ROOT)
    base_rows = read_base_decisions(base_input_path)
    combined_rows = combine_decision_inputs(base_rows, box_score_input_rows)

    resolution_csv = out_dir / "player_box_score_resolution_decisions.csv"
    candidate_csv = out_dir / "player_box_score_resolution_candidates.csv"
    box_score_input_csv = out_dir / "player_box_score_decision_input.csv"
    combined_input_csv = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    report_path = out_dir / "player_box_score_resolution_report.md"

    write_csv(resolution_csv, resolution_rows, RESOLUTION_FIELDS)
    write_csv(candidate_csv, candidate_rows, CANDIDATE_FIELDS)
    write_csv(box_score_input_csv, box_score_input_rows, DECISION_INPUT_FIELDS)
    write_csv(combined_input_csv, combined_rows, DECISION_INPUT_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "player_box_score_resolution_run_id": run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "promotion_apply_run_id_for_scoring_events": apply_run_id,
        "output_dir": str(out_dir),
        "box_score_decision_count": len(resolution_rows),
        "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
        "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
        "candidate_count": len(candidate_rows),
        "box_score_decision_input_csv": str(box_score_input_csv),
        "combined_decision_input_csv": str(combined_input_csv),
        "base_decision_input_csv": str(base_input_path) if base_input_path else "",
        "match_status_counts": dict(Counter(row.get("match_status") for row in resolution_rows)),
        "decision_status_counts": dict(Counter(row.get("decision_status") for row in resolution_rows)),
        "hold_boxscore_ids": sorted(set(args.hold_boxscore_id or [])),
        "include_local_promotion_box_scores": bool(args.include_local_promotion_box_scores),
        "emit_hold_overrides": bool(args.emit_hold_overrides),
        "resolution_csv": str(resolution_csv),
        "candidate_csv": str(candidate_csv),
        "report_path": str(report_path),
    }
    write_json(summary_path, summary)
    write_report(report_path, summary, resolution_rows)
    persist(args.db_path, run_id, decision_ledger_run_id, out_dir, resolution_rows, candidate_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
