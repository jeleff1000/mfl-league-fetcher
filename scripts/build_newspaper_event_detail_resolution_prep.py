#!/usr/bin/env python
"""Resolve medium-confidence newspaper scoring/PBP event details.

This station consumes the consolidated newspaper decision ledger and turns
`second_pass_article_review_for_event_detail` rows into either local promotion
decisions or explicit follow-up holds. It writes only local D-drive artifacts
and local DuckDB review tables.
"""

from __future__ import annotations

import argparse
import csv
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "event_detail_resolution_preps"
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
    "event_detail_resolution_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "promotion_package_id",
    "target_table",
    "boxscore_id",
    "source_boxscore_prefixes_json",
    "game_date",
    "year",
    "week",
    "raw_team",
    "resolved_team",
    "opponent_team",
    "event_type",
    "primary_player_raw",
    "primary_NFL_player_id",
    "secondary_player_raw",
    "secondary_NFL_player_id",
    "distance_yards",
    "yards",
    "points",
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

CANDIDATE_FIELDS = [
    "event_detail_resolution_run_id",
    "decision_id",
    "boxscore_id",
    "target_table",
    "raw_player",
    "raw_team",
    "candidate_role",
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
    "event_detail_resolution_run_id",
    "decision_ledger_run_id",
    "output_dir",
    "event_decision_count",
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
    "chicago bears": "CHI",
    "bears": "CHI",
    "chicago cardinals": "CRD",
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


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


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
    patterns = [
        root / "ocr_completion_resolution_preps" / "*" / "decision_input.csv",
        root / "lineup_route_resolution_preps" / "*" / "decision_input.csv",
        root / "lineup_identity_resolution_preps" / "*" / "decision_input.csv",
        root / "event_detail_resolution_preps" / "*" / "decision_input.csv",
        root / "review_decision_inputs" / "*" / "decision_input.csv",
    ]
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path(path) for path in glob.glob(str(pattern)))
    if not paths:
        return None
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0]


def load_event_decisions(
    con: duckdb.DuckDBPyConnection,
    decision_ledger_run_id: str,
    include_local_promotion_events: bool,
) -> list[dict[str, Any]]:
    route_filter = "route_to_lane = 'second_pass_article_review_for_event_detail'"
    if include_local_promotion_events:
        route_filter = """
        (
          route_to_lane = 'second_pass_article_review_for_event_detail'
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
          AND {route_filter}
          AND target_table IN ('scoring_event', 'play_by_play_event')
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY target_table, boxscore_id, target_entity_key
        """,
        [decision_ledger_run_id],
    )


def load_current_event_route_rows(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND target_table IN ('scoring_event', 'play_by_play_event')
          AND recommended_next_action IN (
            'second_pass_article_review_for_event_detail',
            'evidence_check_then_promote'
          )
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY target_table, boxscore_id, target_entity_key
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


def source_prefixes(source_documents_json: Any) -> list[str]:
    docs = parse_json_list(source_documents_json)
    prefixes = []
    for doc in docs:
        prefix = clean(doc).split("#", 1)[0].strip()
        if prefix:
            prefixes.append(prefix)
    return sorted(set(prefixes))


def resolve_team(
    boxscore_id: str,
    raw_team: str,
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
) -> tuple[str, str, dict[str, Any]]:
    raw_key = " ".join(norm_words(raw_team))
    alias = TEAM_ALIASES.get(raw_key, "")
    rows = team_games_by_boxscore.get(boxscore_id, [])
    if alias:
        for row in rows:
            if clean(row.get("team_code")) == alias:
                return alias, clean(row.get("opponent_code")), row
    if len(rows) == 1:
        row = rows[0]
        return clean(row.get("team_code")), clean(row.get("opponent_code")), row
    return "", "", rows[0] if rows else {}


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
    if raw_last != cand_last:
        return 0, {}
    score = 60
    notes = {"last_name_match": "exact", "first_match": "not_supplied", "team_year_seen_in_v26": "0"}
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
) -> tuple[str, str, str, list[dict[str, Any]]]:
    raw_player = clean(raw_player).strip()
    if not raw_player or raw_player.lower() in {"blocked kick", "staleys"} or " and " in raw_player.lower():
        return "", "", "not_attempted", []
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
        return "", "", "unmatched", []
    best = scored[0]
    second_score = int(scored[1]["candidate_score"]) if len(scored) > 1 else 0
    margin = int(best["candidate_score"]) - second_score
    if int(best["candidate_score"]) >= 80 and (len(scored) == 1 or margin >= 15):
        return clean(best["candidate_pfr_id"]), clean(best["candidate_player"]), "resolved", scored
    if int(best["candidate_score"]) >= 75 and len(scored) == 1:
        return clean(best["candidate_pfr_id"]), clean(best["candidate_player"]), "resolved", scored
    return "", "", "ambiguous", scored


def target_key(target_table: str, boxscore_id: str, enriched: dict[str, Any]) -> str:
    if target_table == "scoring_event":
        parts = [
            "scoring_event",
            boxscore_id,
            clean(enriched.get("scoring_team")),
            clean(enriched.get("scoring_player_raw")),
            clean(enriched.get("event_type")),
            clean(enriched.get("distance_yards")),
            clean(enriched.get("play_text"))[:80],
        ]
    else:
        parts = [
            "play_by_play_event",
            boxscore_id,
            clean(enriched.get("possession_team")),
            clean(enriched.get("primary_player_raw")),
            clean(enriched.get("play_type")),
            clean(enriched.get("yards")),
            clean(enriched.get("play_text"))[:80],
        ]
    return "|".join(parts)


def is_structurally_complete(target_table: str, enriched: dict[str, Any]) -> tuple[bool, str]:
    if target_table == "scoring_event":
        if not clean(enriched.get("event_type")):
            return False, "missing scoring event_type"
        if not clean(enriched.get("scoring_team_raw")):
            return False, "missing scoring team"
        if clean(enriched.get("points")) == "":
            return False, "missing scoring points"
        if not (clean(enriched.get("play_text")) or clean(enriched.get("scoring_player_raw"))):
            return False, "missing scoring evidence text/player"
        return True, "complete scoring event"
    if not clean(enriched.get("play_type")):
        return False, "missing play_type"
    if not clean(enriched.get("play_text")):
        return False, "missing play_text"
    if not (clean(enriched.get("possession_team_raw")) or clean(enriched.get("primary_player_raw"))):
        return False, "missing team/player anchor"
    return True, "complete play event"


def build_resolution_rows(
    run_id: str,
    decision_ledger_run_id: str,
    event_decisions: list[dict[str, Any]],
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
    player_candidates: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, int, str]],
    hold_boxscore_ids: set[str],
    emit_hold_overrides: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]], Counter]:
    created_at = iso_now()
    resolution_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    decision_inputs: list[dict[str, str]] = []
    counts: Counter = Counter()

    for decision in event_decisions:
        target_table = clean(decision.get("target_table"))
        proposed = normalize_event_fields(target_table, parse_json_obj(decision.get("proposed_fields_json")))
        boxscore_id = clean(proposed.get("boxscore_id")) or clean(decision.get("boxscore_id"))
        prefixes = source_prefixes(decision.get("source_documents_json"))
        raw_team = (
            clean(proposed.get("scoring_team_raw"))
            if target_table == "scoring_event"
            else clean(proposed.get("possession_team_raw"))
        )
        team_code, opponent_code, game_row = resolve_team(boxscore_id, raw_team, team_games_by_boxscore)
        year = int(game_row.get("year") or 0) if game_row else 0
        week = int(game_row.get("week") or 0) if game_row else 0

        match_status = "ready_for_local_promotion"
        reason_parts: list[str] = []
        if not boxscore_id:
            match_status = "needs_game_mapping"
            reason_parts.append("boxscore_id is missing")
        elif boxscore_id in hold_boxscore_ids:
            match_status = "held_boxscore_conflict"
            reason_parts.append(f"boxscore_id {boxscore_id} is in the explicit hold list")
        elif prefixes and boxscore_id not in prefixes:
            match_status = "source_boxscore_mismatch"
            reason_parts.append(f"source document prefixes {prefixes} do not include {boxscore_id}")
        elif not team_code:
            match_status = "unresolved_team"
            reason_parts.append("team could not be resolved against PFR team-game rows")

        enriched = dict(proposed)
        if target_table == "scoring_event":
            enriched["scoring_team"] = team_code
            primary_raw = clean(enriched.get("scoring_player_raw"))
            secondary_raws = [
                ("passer", clean(enriched.get("passer_raw"))),
                ("receiver", clean(enriched.get("receiver_raw"))),
            ]
        else:
            enriched["possession_team"] = team_code
            primary_raw = clean(enriched.get("primary_player_raw"))
            secondary_raws = [("secondary", clean(enriched.get("secondary_player_raw")))]
        enriched["boxscore_id"] = boxscore_id
        enriched["review_status"] = "event_detail_resolved"
        enriched["promotion_status"] = "local_promotion_candidate"

        primary_id, primary_player, primary_status, primary_candidates = resolve_player(
            primary_raw,
            player_candidates,
            year,
            team_code,
            v26_team_year_ids,
        )
        if target_table == "scoring_event":
            enriched["scoring_NFL_player_id"] = primary_id
        else:
            enriched["primary_NFL_player_id"] = primary_id
        for rank, cand in enumerate(primary_candidates[:6], start=1):
            candidate_rows.append({
                "event_detail_resolution_run_id": run_id,
                "decision_id": clean(decision.get("decision_id")),
                "boxscore_id": boxscore_id,
                "target_table": target_table,
                "raw_player": primary_raw,
                "raw_team": raw_team,
                "candidate_role": "primary",
                "candidate_rank": str(rank),
                "created_at_utc": created_at,
                **cand,
            })

        secondary_ids: dict[str, str] = {}
        secondary_players: dict[str, str] = {}
        for role, raw_player in secondary_raws:
            resolved_id, resolved_player, _, scored = resolve_player(
                raw_player,
                player_candidates,
                year,
                team_code,
                v26_team_year_ids,
            )
            secondary_ids[role] = resolved_id
            secondary_players[role] = resolved_player
            for rank, cand in enumerate(scored[:6], start=1):
                candidate_rows.append({
                    "event_detail_resolution_run_id": run_id,
                    "decision_id": clean(decision.get("decision_id")),
                    "boxscore_id": boxscore_id,
                    "target_table": target_table,
                    "raw_player": raw_player,
                    "raw_team": raw_team,
                    "candidate_role": role,
                    "candidate_rank": str(rank),
                    "created_at_utc": created_at,
                    **cand,
                })
        if target_table == "scoring_event":
            enriched["passer_NFL_player_id"] = secondary_ids.get("passer", "")
            enriched["receiver_NFL_player_id"] = secondary_ids.get("receiver", "")
        else:
            enriched["secondary_NFL_player_id"] = secondary_ids.get("secondary", "")

        complete, complete_reason = is_structurally_complete(target_table, enriched)
        if match_status == "ready_for_local_promotion" and not complete:
            match_status = "incomplete_event_detail"
            reason_parts.append(complete_reason)
        elif complete:
            reason_parts.append(complete_reason)

        approved = match_status == "ready_for_local_promotion"
        decision_status = "approved" if approved else "pending"
        decision_value = PROMOTION_VALUE if approved else "pending"
        route_to_lane = "local_promotion" if approved else "second_pass_article_review_for_event_detail"
        if match_status in {"needs_game_mapping", "source_boxscore_mismatch", "unresolved_team"}:
            route_to_lane = "verify_target_boxscore_and_game_key"

        resolution_rows.append({
            "event_detail_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "decision_id": clean(decision.get("decision_id")),
            "promotion_package_id": clean(decision.get("promotion_package_id")),
            "target_table": target_table,
            "boxscore_id": boxscore_id,
            "source_boxscore_prefixes_json": json.dumps(prefixes, sort_keys=True),
            "game_date": clean(game_row.get("game_date")) if game_row else "",
            "year": str(year) if year else "",
            "week": str(week) if week else "",
            "raw_team": raw_team,
            "resolved_team": team_code,
            "opponent_team": opponent_code,
            "event_type": clean(enriched.get("event_type") or enriched.get("play_type")),
            "primary_player_raw": primary_raw,
            "primary_NFL_player_id": primary_id,
            "secondary_player_raw": clean(enriched.get("secondary_player_raw") or enriched.get("passer_raw") or enriched.get("receiver_raw")),
            "secondary_NFL_player_id": clean(enriched.get("secondary_NFL_player_id") or enriched.get("passer_NFL_player_id") or enriched.get("receiver_NFL_player_id")),
            "distance_yards": clean(enriched.get("distance_yards")),
            "yards": clean(enriched.get("yards")),
            "points": clean(enriched.get("points")),
            "match_status": match_status,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": route_to_lane,
            "reason": "; ".join(reason_parts),
            "proposed_fields_json": clean(decision.get("proposed_fields_json")),
            "enriched_proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
            "source_documents_json": clean(decision.get("source_documents_json")),
            "created_at_utc": created_at,
        })
        counts[match_status] += 1
        counts[decision_status] += 1
        counts[f"{target_table}_{decision_status}"] += 1

        if approved or emit_hold_overrides:
            decision_inputs.append({
                "decision_id": clean(decision.get("decision_id")),
                "decision_status": decision_status,
                "decision_value": decision_value,
                "route_to_lane": route_to_lane,
                "resolved_boxscore_id": boxscore_id if approved else "",
                "resolved_target_table": target_table if approved else "",
                "resolved_target_entity_key": target_key(target_table, boxscore_id, enriched) if approved else "",
                "proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False) if approved else "",
                "notes": "; ".join(reason_parts),
            })
    return resolution_rows, candidate_rows, decision_inputs, counts


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


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table, fields in [
        ("event_detail_resolution_decision", RESOLUTION_FIELDS),
        ("event_detail_resolution_candidate", CANDIDATE_FIELDS),
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
        CREATE TABLE IF NOT EXISTS newspaper_review.event_detail_resolution_run (
          event_detail_resolution_run_id VARCHAR,
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          event_decision_count INTEGER,
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
            ("event_detail_resolution_decision", "event_detail_resolution_run_id"),
            ("event_detail_resolution_candidate", "event_detail_resolution_run_id"),
            ("event_detail_resolution_run", "event_detail_resolution_run_id"),
        ]:
            con.execute(f"DELETE FROM newspaper_review.{table} WHERE {id_col} = ?", [run_id])
        insert_rows(con, "event_detail_resolution_decision", resolution_rows, RESOLUTION_FIELDS)
        insert_rows(con, "event_detail_resolution_candidate", candidate_rows, CANDIDATE_FIELDS)
        insert_rows(con, "event_detail_resolution_run", [{
            "event_detail_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "event_decision_count": len(resolution_rows),
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
        "# Newspaper Event Detail Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['event_detail_resolution_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Event rows reviewed: `{summary['event_decision_count']}`",
        f"- Approved for local promotion: `{summary['approved_count']}`",
        f"- Held/routed: `{summary['held_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Held Samples",
        "",
        "| table | boxscore | team | event | player | status | reason |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    held = [row for row in resolution_rows if row.get("decision_status") != "approved"]
    for row in held[:60]:
        lines.append(
            f"| {row['target_table']} | {row['boxscore_id']} | {row['raw_team']} | "
            f"{row['event_type']} | {row['primary_player_raw']} | {row['match_status']} | {row['reason']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument(
        "--promotion-apply-run-id",
        default="",
        help="Read only current event-detail rows from this local apply run's route queue.",
    )
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="event_detail_resolution_prep")
    parser.add_argument(
        "--include-local-promotion-events",
        action="store_true",
        help="Also re-normalize already approved local scoring/PBP decisions.",
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
        help="Boxscore id to keep out of automatic event promotion. Can be repeated.",
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
            event_decisions = load_current_event_route_rows(read_con, promotion_apply_run_id)
        else:
            event_decisions = load_event_decisions(read_con, decision_ledger_run_id, args.include_local_promotion_events)
        boxscore_ids = sorted({
            clean(parse_json_obj(row.get("proposed_fields_json")).get("boxscore_id")) or clean(row.get("boxscore_id"))
            for row in event_decisions
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
    finally:
        read_con.close()

    resolution_rows, candidate_rows, event_input_rows, counts = build_resolution_rows(
        run_id,
        decision_ledger_run_id,
        event_decisions,
        team_games_by_boxscore,
        player_candidates,
        v26_team_year_ids,
        set(args.hold_boxscore_id or []),
        args.emit_hold_overrides,
    )

    base_input_path = args.base_decision_input_csv or latest_decision_input(DEFAULT_ROOT)
    base_rows = read_base_decisions(base_input_path)
    combined_input_rows = combine_decision_inputs(base_rows, event_input_rows)

    resolution_csv = out_dir / "event_detail_resolution_decisions.csv"
    candidate_csv = out_dir / "event_detail_resolution_candidates.csv"
    event_input_csv = out_dir / "event_detail_decision_input.csv"
    combined_input_csv = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    report_path = out_dir / "event_detail_resolution_report.md"

    write_csv(resolution_csv, resolution_rows, RESOLUTION_FIELDS)
    write_csv(candidate_csv, candidate_rows, CANDIDATE_FIELDS)
    write_csv(event_input_csv, event_input_rows, DECISION_INPUT_FIELDS)
    write_csv(combined_input_csv, combined_input_rows, DECISION_INPUT_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "event_detail_resolution_run_id": run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "event_decision_count": len(resolution_rows),
        "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
        "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
        "candidate_count": len(candidate_rows),
        "event_decision_input_csv": str(event_input_csv),
        "combined_decision_input_csv": str(combined_input_csv),
        "base_decision_input_csv": str(base_input_path) if base_input_path else "",
        "match_status_counts": dict(Counter(row.get("match_status") for row in resolution_rows)),
        "decision_status_counts": dict(Counter(row.get("decision_status") for row in resolution_rows)),
        "hold_boxscore_ids": sorted(set(args.hold_boxscore_id or [])),
        "include_local_promotion_events": bool(args.include_local_promotion_events),
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
