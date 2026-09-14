#!/usr/bin/env python
"""Resolve held newspaper lineup rows into auditable local promotion inputs.

This station consumes a newspaper review decision ledger and builds an identity
resolution lane for `lineup_participation` packages. It writes only local
D-drive artifacts and local DuckDB review tables. It does not write to Fly or
the canonical supertable.
"""

from __future__ import annotations

import argparse
import csv
import difflib
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "lineup_identity_resolution_preps"
DEFAULT_PLAYER_INDEX = Path(r"D:\league-history-data\nfl\raw\pfr\players\player_index.parquet")
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")
DEFAULT_V26 = Path(
    r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet"
)
DEFAULT_BASE_INPUT = Path(
    r"D:\league-history-data\nfl\derived\newspaper_atoms\review_decision_inputs\20260625T230231Z_1920_1939_batch0001_packets0001_0011_safe_local_promotions_v1\decision_input.csv"
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
    "lineup_identity_resolution_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "promotion_package_id",
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
    "lineup_identity_resolution_run_id",
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
    "lineup_identity_resolution_run_id",
    "decision_ledger_run_id",
    "output_dir",
    "lineup_decision_count",
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
    "canton bulldogs": "CBD",
    "chicago cardinals": "CRD",
    "chicago tigers": "CHT",
    "cleveland tigers": "CLE",
    "columbus panhandles": "COL",
    "dayton triangles": "DAY",
    "decatur staleys": "CHI",
    "staleys": "CHI",
    "detroit heralds": "DET",
    "evansville crimson giants": "ECG",
    "green bay packers": "GNB",
    "hammond pros": "HAM",
    "hammond": "HAM",
    "minneapolis marines": "MIN",
    "muncie flyers": "MUN",
    "racine cardinals": "RAC",
    "rock island independents": "RII",
}

BACKFIELD_TOKENS = {"B", "BB", "FB", "HB", "LHB", "RHB", "QB", "TB", "WB", "LH", "RH"}
LINEUP_POSITION_TO_GROUP = {
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


def latest_package_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT package_run_id
        FROM newspaper_review.llm_promotion_package_run
        ORDER BY created_at_utc DESC, package_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_lineup_decisions(con: duckdb.DuckDBPyConnection, decision_ledger_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
          AND target_table = 'lineup_participation'
          AND decision_status IN ('', 'pending')
        ORDER BY boxscore_id, target_entity_key
        """,
        [decision_ledger_run_id],
    )


def load_game_packages(con: duckdb.DuckDBPyConnection, package_run_id: str) -> list[dict[str, Any]]:
    if not package_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT boxscore_id, proposed_fields_json
        FROM newspaper_review.llm_promotion_package
        WHERE package_run_id = ?
          AND target_table = 'game_candidate'
        """,
        [package_run_id],
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
        row["_position_tokens"] = {token.upper() for token in re.split(r"[^A-Za-z]+", clean(row.get("index_position"))) if token}
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


def team_code_from_scores(rows: list[dict[str, Any]], team_score: Any, opp_score: Any) -> tuple[str, str]:
    try:
        score = int(float(clean(team_score)))
        opp = int(float(clean(opp_score)))
    except ValueError:
        return "", ""
    matches = [
        row for row in rows
        if row.get("team_points") == score and row.get("opponent_points") == opp
    ]
    if len(matches) == 1:
        return clean(matches[0].get("team_code")), clean(matches[0].get("opponent_code"))
    return "", ""


def build_team_name_map(
    game_packages: list[dict[str, Any]],
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
) -> dict[tuple[str, str], tuple[str, str]]:
    mapping: dict[tuple[str, str], tuple[str, str]] = {}
    for package in game_packages:
        boxscore_id = clean(package.get("boxscore_id"))
        rows = team_games_by_boxscore.get(boxscore_id, [])
        proposed = parse_json_obj(package.get("proposed_fields_json"))
        entries = [
            (
                clean(proposed.get("team_1_raw")),
                proposed.get("team_1_score"),
                proposed.get("team_2_score"),
            ),
            (
                clean(proposed.get("team_2_raw")),
                proposed.get("team_2_score"),
                proposed.get("team_1_score"),
            ),
        ]
        for raw_team, score, opp_score in entries:
            if not raw_team:
                continue
            team_code, opponent_code = team_code_from_scores(rows, score, opp_score)
            if team_code:
                mapping[(boxscore_id, norm(raw_team))] = (team_code, opponent_code)
    return mapping


def resolve_team(
    boxscore_id: str,
    raw_team: str,
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
    team_name_map: dict[tuple[str, str], tuple[str, str]],
) -> tuple[str, str, dict[str, Any]]:
    raw_norm = norm(raw_team)
    if (boxscore_id, raw_norm) in team_name_map:
        team_code, opponent_code = team_name_map[(boxscore_id, raw_norm)]
    else:
        team_code = TEAM_ALIASES.get(" ".join(norm_words(raw_team)), "")
        opponent_code = ""
        for row in team_games_by_boxscore.get(boxscore_id, []):
            if clean(row.get("team_code")) == team_code:
                opponent_code = clean(row.get("opponent_code"))
                break
    game_row = {}
    for row in team_games_by_boxscore.get(boxscore_id, []):
        if clean(row.get("team_code")) == team_code:
            game_row = row
            break
    return team_code, opponent_code, game_row


def position_match(listed_position: str, candidate_tokens: set[str]) -> str:
    raw = clean(listed_position).upper().strip()
    if not raw or not candidate_tokens:
        return "neutral"
    group = LINEUP_POSITION_TO_GROUP.get(raw, raw)
    if group == "B":
        return "yes" if candidate_tokens & BACKFIELD_TOKENS else "no"
    return "yes" if group in candidate_tokens else "no"


def last_name_similarity(raw_last: str, candidate_last: str) -> tuple[bool, str]:
    if not raw_last or not candidate_last:
        return False, "none"
    if raw_last == candidate_last:
        return True, "exact"
    ratio = difflib.SequenceMatcher(None, raw_last, candidate_last).ratio()
    if ratio >= 0.86:
        return True, "fuzzy"
    return False, "none"


def candidate_score(
    decision: dict[str, Any],
    proposed: dict[str, Any],
    candidate: dict[str, Any],
    year: int,
    team_code: str,
    v26_team_year_ids: set[tuple[str, int, str]],
) -> tuple[int, dict[str, str]]:
    raw_player = clean(proposed.get("player_raw"))
    raw_tokens = norm_words(raw_player)
    if not raw_tokens:
        return 0, {"last_name_match": "none", "initial_match": "none", "position_match": "neutral", "team_year_seen_in_v26": "0"}

    raw_last = raw_tokens[-1]
    raw_first = raw_tokens[0] if len(raw_tokens) > 1 else ""
    raw_initial = raw_first[:1] if raw_first and len(raw_first) == 1 else ""
    full_norm = norm(raw_player)

    last_ok, last_kind = last_name_similarity(raw_last, clean(candidate.get("_last")))
    if not last_ok:
        return 0, {"last_name_match": "none", "initial_match": "none", "position_match": "neutral", "team_year_seen_in_v26": "0"}

    score = 0
    notes = {
        "last_name_match": last_kind,
        "initial_match": "not_supplied",
        "position_match": position_match(clean(proposed.get("listed_position_raw")), candidate.get("_position_tokens", set())),
        "team_year_seen_in_v26": "0",
    }
    if full_norm == clean(candidate.get("_full_norm")):
        score += 100
    elif last_kind == "exact":
        score += 60
    else:
        score += 48

    if raw_initial:
        if raw_initial == clean(candidate.get("_first_initial")):
            score += 25
            notes["initial_match"] = "yes"
        else:
            score -= 15
            notes["initial_match"] = "no"
    elif raw_first and len(raw_first) > 1:
        cand_first = clean(candidate.get("_first"))
        if cand_first.startswith(raw_first) or raw_first.startswith(cand_first):
            score += 15
            notes["initial_match"] = "first_name_prefix"
        else:
            notes["initial_match"] = "first_name_not_matched"

    if notes["position_match"] == "yes":
        score += 15
    elif notes["position_match"] == "no":
        score -= 10

    pfr_id = clean(candidate.get("pfr_id"))
    if (pfr_id, year, team_code) in v26_team_year_ids:
        score += 20
        notes["team_year_seen_in_v26"] = "1"

    if clean(decision.get("confidence_bar")).lower() == "medium":
        score -= 2
    return score, notes


def resolve_player(
    decision: dict[str, Any],
    proposed: dict[str, Any],
    candidates: list[dict[str, Any]],
    year: int,
    team_code: str,
    v26_team_year_ids: set[tuple[str, int, str]],
) -> tuple[str, list[dict[str, Any]], str]:
    scored: list[dict[str, Any]] = []
    for candidate in candidates:
        first_year = candidate.get("first_year")
        last_year = candidate.get("last_year")
        if first_year is None or last_year is None or int(first_year) > year or int(last_year) < year:
            continue
        score, notes = candidate_score(decision, proposed, candidate, year, team_code, v26_team_year_ids)
        if score <= 0:
            continue
        scored.append({
            "candidate_score": score,
            "candidate_pfr_id": clean(candidate.get("pfr_id")),
            "candidate_player": clean(candidate.get("player")),
            "candidate_position": clean(candidate.get("index_position")),
            "first_year": clean(candidate.get("first_year")),
            "last_year": clean(candidate.get("last_year")),
            **notes,
            "match_notes": json.dumps(notes, sort_keys=True),
        })
    scored.sort(key=lambda row: (-int(row["candidate_score"]), row["candidate_player"], row["candidate_pfr_id"]))
    if not scored:
        return "unmatched_player", [], "no active-year PFR index candidate matched the raw lineup name"
    best = scored[0]
    second_score = int(scored[1]["candidate_score"]) if len(scored) > 1 else 0
    margin = int(best["candidate_score"]) - second_score
    if int(best["candidate_score"]) >= 80 and (len(scored) == 1 or margin >= 15):
        return "resolved_high_confidence", scored, "unique high-confidence active-year/name/position match"
    if (
        int(best["candidate_score"]) >= 70
        and clean(best.get("last_name_match")) == "exact"
        and clean(best.get("position_match")) == "yes"
        and clean(best.get("initial_match")) != "no"
        and (len(scored) == 1 or margin >= 20)
    ):
        return "resolved_high_confidence", scored, "exact active-year surname with compatible lineup position"
    if int(best["candidate_score"]) >= 75 and len(scored) == 1:
        return "resolved_high_confidence", scored, "single active-year candidate above threshold"
    if len(scored) == 1:
        return "ambiguous_player_identity", scored, "single active-year candidate below auto-approval threshold"
    return "ambiguous_player_identity", scored, "multiple or low-margin active-year candidates remain"


def enriched_target_key(boxscore_id: str, nfl_player_id: str, team_code: str, listed_position: str, participation_type: str) -> str:
    return "|".join([
        "lineup_participation",
        boxscore_id,
        nfl_player_id,
        team_code,
        clean(listed_position),
        clean(participation_type),
    ])


def build_resolution_rows(
    run_id: str,
    decision_ledger_run_id: str,
    lineup_decisions: list[dict[str, Any]],
    team_games_by_boxscore: dict[str, list[dict[str, Any]]],
    team_name_map: dict[tuple[str, str], tuple[str, str]],
    player_candidates: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, int, str]],
    hold_boxscore_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]], Counter]:
    created_at = iso_now()
    resolution_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    decision_inputs: list[dict[str, str]] = []
    counts: Counter = Counter()

    for decision in lineup_decisions:
        proposed = parse_json_obj(decision.get("proposed_fields_json"))
        boxscore_id = clean(proposed.get("boxscore_id")) or clean(decision.get("boxscore_id"))
        raw_team = clean(proposed.get("team_raw"))
        raw_player = clean(proposed.get("player_raw"))
        listed_position = clean(proposed.get("listed_position_raw"))
        participation_type = clean(proposed.get("participation_type")) or ("starter" if clean(proposed.get("is_starter")) == "1" else "")
        team_code, opponent_code, game_row = resolve_team(boxscore_id, raw_team, team_games_by_boxscore, team_name_map)
        year = int(game_row.get("year") or 0) if game_row else 0
        week = int(game_row.get("week") or 0) if game_row else 0

        if boxscore_id in hold_boxscore_ids:
            match_status = "held_boxscore_conflict"
            scored = []
            reason = f"boxscore_id {boxscore_id} is in the explicit identity-resolution hold list"
        elif not team_code or not year or not week:
            match_status = "unresolved_team_or_game"
            scored: list[dict[str, Any]] = []
            reason = "could not resolve newspaper team/game to a PFR team-game row"
        else:
            match_status, scored, reason = resolve_player(
                decision,
                proposed,
                player_candidates,
                year,
                team_code,
                v26_team_year_ids,
            )

        best = scored[0] if scored else {}
        second_score = int(scored[1]["candidate_score"]) if len(scored) > 1 else 0
        best_score = int(best.get("candidate_score") or 0)
        score_margin = best_score - second_score
        approved = match_status == "resolved_high_confidence" and bool(team_code)
        decision_status = "approved" if approved else "pending"
        decision_value = PROMOTION_VALUE if approved else "pending"
        route_to_lane = "local_promotion" if approved else "resolve_player_or_team_identity_then_promote"
        resolved_nfl_id = clean(best.get("candidate_pfr_id")) if approved else ""
        resolved_player = clean(best.get("candidate_player")) if approved else ""
        resolved_player_week = f"{resolved_nfl_id}_{year}_{week}" if approved else ""

        enriched = dict(proposed)
        if approved:
            enriched.update({
                "boxscore_id": boxscore_id,
                "player_raw": raw_player,
                "NFL_player_id": resolved_nfl_id,
                "player_week": resolved_player_week,
                "team_raw": raw_team,
                "nfl_team": team_code,
                "opponent_nfl_team": opponent_code,
                "listed_position_raw": listed_position,
                "starter_position": listed_position,
                "participation_type": participation_type,
                "review_status": "identity_resolved",
                "promotion_status": "local_promotion_candidate",
            })

        resolution_row = {
            "lineup_identity_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "decision_id": clean(decision.get("decision_id")),
            "promotion_package_id": clean(decision.get("promotion_package_id")),
            "boxscore_id": boxscore_id,
            "game_date": clean(game_row.get("game_date")) if game_row else "",
            "year": str(year) if year else "",
            "week": str(week) if week else "",
            "raw_team": raw_team,
            "resolved_team": team_code,
            "opponent_team": opponent_code,
            "raw_player": raw_player,
            "listed_position_raw": listed_position,
            "participation_type": participation_type,
            "match_status": match_status,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": route_to_lane,
            "resolved_NFL_player_id": resolved_nfl_id,
            "resolved_player": resolved_player,
            "resolved_player_week": resolved_player_week,
            "candidate_count": str(len(scored)),
            "best_score": str(best_score) if best_score else "",
            "second_score": str(second_score) if second_score else "",
            "score_margin": str(score_margin) if best_score else "",
            "reason": reason,
            "proposed_fields_json": clean(decision.get("proposed_fields_json")),
            "enriched_proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
            "source_documents_json": clean(decision.get("source_documents_json")),
            "created_at_utc": created_at,
        }
        resolution_rows.append(resolution_row)
        counts[match_status] += 1
        counts[decision_status] += 1
        counts[f"team_{'resolved' if team_code else 'unresolved'}"] += 1

        for rank, cand in enumerate(scored[:8], start=1):
            candidate_rows.append({
                "lineup_identity_resolution_run_id": run_id,
                "decision_id": clean(decision.get("decision_id")),
                "boxscore_id": boxscore_id,
                "raw_player": raw_player,
                "raw_team": raw_team,
                "listed_position_raw": listed_position,
                "candidate_rank": str(rank),
                "created_at_utc": created_at,
                **cand,
            })

        if approved:
            decision_inputs.append({
                "decision_id": clean(decision.get("decision_id")),
                "decision_status": "approved",
                "decision_value": PROMOTION_VALUE,
                "route_to_lane": "local_promotion",
                "resolved_boxscore_id": boxscore_id,
                "resolved_target_table": "lineup_participation",
                "resolved_target_entity_key": enriched_target_key(
                    boxscore_id,
                    resolved_nfl_id,
                    team_code,
                    listed_position,
                    participation_type,
                ),
                "proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
                "notes": reason,
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
        ("lineup_identity_resolution_decision", RESOLUTION_FIELDS),
        ("lineup_identity_resolution_candidate", CANDIDATE_FIELDS),
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
        CREATE TABLE IF NOT EXISTS newspaper_review.lineup_identity_resolution_run (
          lineup_identity_resolution_run_id VARCHAR,
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          lineup_decision_count INTEGER,
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
            ("lineup_identity_resolution_decision", "lineup_identity_resolution_run_id"),
            ("lineup_identity_resolution_candidate", "lineup_identity_resolution_run_id"),
            ("lineup_identity_resolution_run", "lineup_identity_resolution_run_id"),
        ]:
            con.execute(f"DELETE FROM newspaper_review.{table} WHERE {id_col} = ?", [run_id])
        insert_rows(con, "lineup_identity_resolution_decision", resolution_rows, RESOLUTION_FIELDS)
        insert_rows(con, "lineup_identity_resolution_candidate", candidate_rows, CANDIDATE_FIELDS)
        insert_rows(con, "lineup_identity_resolution_run", [{
            "lineup_identity_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "lineup_decision_count": len(resolution_rows),
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
        "# Newspaper Lineup Identity Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['lineup_identity_resolution_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Lineup rows reviewed: `{summary['lineup_decision_count']}`",
        f"- Approved for local lineup promotion: `{summary['approved_count']}`",
        f"- Held for identity/team review: `{summary['held_count']}`",
        f"- Candidate rows written: `{summary['candidate_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Held Samples",
        "",
        "| boxscore | team | player | pos | status | reason | candidates |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    held = [row for row in resolution_rows if row.get("decision_status") != "approved"]
    for row in held[:40]:
        lines.append(
            f"| {row['boxscore_id']} | {row['raw_team']} | {row['raw_player']} | "
            f"{row['listed_position_raw']} | {row['match_status']} | {row['reason']} | {row['candidate_count']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument("--package-run-id", default="")
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--base-decision-input-csv", type=Path, default=DEFAULT_BASE_INPUT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="lineup_identity_resolution_prep")
    parser.add_argument(
        "--hold-boxscore-id",
        action="append",
        default=["192011070rii"],
        help="Boxscore id to keep out of automatic lineup identity promotion. Can be repeated.",
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
        package_run_id = args.package_run_id or latest_package_run(read_con)
        if not decision_ledger_run_id:
            raise SystemExit("No decision ledger run found.")
        lineup_decisions = load_lineup_decisions(read_con, decision_ledger_run_id)
        boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in lineup_decisions if clean(row.get("boxscore_id"))})
        team_games_by_boxscore = load_team_games(read_con, args.team_games_path, boxscore_ids)
        game_packages = load_game_packages(read_con, package_run_id)
        team_name_map = build_team_name_map(game_packages, team_games_by_boxscore)
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

    resolution_rows, candidate_rows, lineup_input_rows, counts = build_resolution_rows(
        run_id,
        decision_ledger_run_id,
        lineup_decisions,
        team_games_by_boxscore,
        team_name_map,
        player_candidates,
        v26_team_year_ids,
        set(args.hold_boxscore_id or []),
    )
    base_rows = read_base_decisions(args.base_decision_input_csv)
    combined_input_rows = combine_decision_inputs(base_rows, lineup_input_rows)

    resolution_csv = out_dir / "lineup_identity_resolution_decisions.csv"
    candidate_csv = out_dir / "lineup_identity_resolution_candidates.csv"
    lineup_input_csv = out_dir / "lineup_decision_input.csv"
    combined_input_csv = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    report_path = out_dir / "lineup_identity_resolution_report.md"

    write_csv(resolution_csv, resolution_rows, RESOLUTION_FIELDS)
    write_csv(candidate_csv, candidate_rows, CANDIDATE_FIELDS)
    write_csv(lineup_input_csv, lineup_input_rows, DECISION_INPUT_FIELDS)
    write_csv(combined_input_csv, combined_input_rows, DECISION_INPUT_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "lineup_identity_resolution_run_id": run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "package_run_id": package_run_id,
        "output_dir": str(out_dir),
        "lineup_decision_count": len(resolution_rows),
        "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
        "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
        "candidate_count": len(candidate_rows),
        "lineup_decision_input_csv": str(lineup_input_csv),
        "combined_decision_input_csv": str(combined_input_csv),
        "base_decision_input_csv": str(args.base_decision_input_csv) if args.base_decision_input_csv else "",
        "match_status_counts": dict(Counter(row.get("match_status") for row in resolution_rows)),
        "decision_status_counts": dict(Counter(row.get("decision_status") for row in resolution_rows)),
        "team_resolution_counts": {key: counts[key] for key in sorted(counts) if key.startswith("team_")},
        "hold_boxscore_ids": sorted(set(args.hold_boxscore_id or [])),
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
