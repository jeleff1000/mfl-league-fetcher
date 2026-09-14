#!/usr/bin/env python
"""Resolve the v26-readiness identity queue into local review lanes.

This station is local-only. It reads a readiness identity queue, local
newspaper-promoted atom rows, local player/team indexes, and writes D-drive
artifacts plus local `newspaper_review` tables. It does not write to Fly, v26,
or production supertable tables.
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
DEFAULT_READINESS_ROOT = DEFAULT_ROOT / "v26_readiness_reports"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "identity_resolution_from_readiness"
DEFAULT_PLAYER_INDEX = Path(r"D:\league-history-data\nfl\raw\pfr\players\player_index.parquet")
DEFAULT_TEAM_GAMES = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet")
DEFAULT_V26 = Path(
    r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet"
)

TEAM_ALIASES = {
    "akr": "AKR",
    "indians": "AKR",
    "akron indians": "AKR",
    "akron pros": "AKR",
    "all americans": "BUF",
    "all-americans": "BUF",
    "bkn": "BKN",
    "brooklyn dodgers": "BKN",
    "buf": "BUF",
    "buffalo all americans": "BUF",
    "buffalo all-americans": "BUF",
    "can": "CAN",
    "canton bulldogs": "CAN",
    "chi": "CHI",
    "chicago bears": "CHI",
    "chicago staleys": "CHI",
    "decatur staleys": "CHI",
    "staleys": "CHI",
    "cht": "CHT",
    "chicago tigers": "CHT",
    "cin": "CIN",
    "cincinnati celts": "CIN",
    "cle": "CLE",
    "cleveland tigers": "CLE",
    "col": "COL",
    "panhandle": "COL",
    "panhandles": "COL",
    "columbus panhandles": "COL",
    "crd": "CRD",
    "cardinals": "CRD",
    "chicago cardinals": "CRD",
    "day": "DAY",
    "dayton triangles": "DAY",
    "det": "DET",
    "detroit heralds": "DET",
    "detroit panthers": "DET",
    "evn": "EVN",
    "evansville crimson giants": "EVN",
    "frn": "FRN",
    "frankford": "FRN",
    "frankford yellow jackets": "FRN",
    "yellow jackets": "FRN",
    "gnb": "GNB",
    "green bay": "GNB",
    "green bay packers": "GNB",
    "ham": "HAM",
    "hammond": "HAM",
    "hammond pros": "HAM",
    "lou": "LOU",
    "louisville": "LOU",
    "louisville brecks": "LOU",
    "brecks": "LOU",
    "mil": "MIL",
    "milwaukee": "MIL",
    "milwaukee badgers": "MIL",
    "badgers": "MIL",
    "min": "MIN",
    "minneapolis marines": "MIN",
    "mun": "MUN",
    "muncie flyers": "MUN",
    "nyg": "NYG",
    "new york giants": "NYG",
    "giants": "NYG",
    "pit": "PIT",
    "pittsburgh": "PIT",
    "pittsburgh pirates": "PIT",
    "prv": "PRV",
    "providence": "PRV",
    "providence steam roller": "PRV",
    "steam roller": "PRV",
    "rii": "RII",
    "independents": "RII",
    "rock island independents": "RII",
    "rac": "RAC",
    "racine": "RAC",
    "racine cardinals": "RAC",
    "racine legion": "RAC",
    "was": "WAS",
    "washington redskins": "WAS",
}

PROMOTED_TABLES = [
    "scoring_event",
    "play_by_play_event",
    "player_game_box_score",
    "player_game_stat_claim",
    "source_document_note",
]

TASK_FIELDS = [
    "identity_resolution_run_id",
    "task_id",
    "readiness_row_id",
    "target_table",
    "target_entity_key",
    "role",
    "target_id_field",
    "boxscore_id",
    "year",
    "week",
    "raw_player",
    "raw_team",
    "resolved_team",
    "opponent_team",
    "resolution_status",
    "resolution_lane",
    "resolution_method",
    "resolved_NFL_player_id",
    "resolved_player",
    "resolved_player_week",
    "candidate_count",
    "best_score",
    "second_score",
    "score_margin",
    "reason",
    "proposed_patch_json",
    "evidence_text",
    "source_documents_json",
    "created_at_utc",
]

SKIPPED_FIELDS = [
    "identity_resolution_run_id",
    "readiness_row_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "skip_reason",
    "evidence_text",
    "source_documents_json",
    "created_at_utc",
]

CANDIDATE_FIELDS = [
    "identity_resolution_run_id",
    "task_id",
    "target_table",
    "target_entity_key",
    "role",
    "boxscore_id",
    "raw_player",
    "raw_team",
    "candidate_rank",
    "candidate_score",
    "candidate_NFL_player_id",
    "candidate_player",
    "candidate_position",
    "first_year",
    "last_year",
    "candidate_source",
    "match_notes",
    "created_at_utc",
]

RUN_FIELDS = [
    "identity_resolution_run_id",
    "readiness_dir",
    "output_dir",
    "task_count",
    "auto_resolved_count",
    "review_count",
    "unresolved_count",
    "candidate_count",
    "status",
    "summary_json_path",
    "created_at_utc",
]


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
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return str(value)


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean(value).lower())


def norm_key(value: Any) -> str:
    return " ".join(norm_words(value))


def norm_words(value: Any) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", clean(value).lower()) if token]


def team_label_key(value: Any) -> str:
    """Normalize newspaper team labels that often carry scores in parentheses."""
    text = re.sub(r"\([^)]*\)", " ", clean(value))
    return norm_key(text)


def parse_int(value: Any) -> int | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def parse_json_list(value: Any) -> list[str]:
    text = clean(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [clean(item) for item in parsed]


def parse_json_obj(value: Any) -> dict[str, Any]:
    text = clean(value).strip()
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
        for row in rows:
            writer.writerow({field: safe_cell(row.get(field)) for field in fields})


def query_dicts(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | tuple[Any, ...] | None = None,
) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_readiness_dir(root: Path) -> Path:
    dirs = sorted([path for path in root.iterdir() if path.is_dir()], key=lambda path: path.stat().st_mtime, reverse=True)
    if not dirs:
        raise FileNotFoundError(f"No readiness report dirs found under {root}")
    return dirs[0]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return bool(row)


def load_promoted_rows(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for table in PROMOTED_TABLES:
        if not table_exists(con, "newspaper_promoted", table):
            continue
        rows = query_dicts(
            con,
            f"""
            SELECT *
            FROM newspaper_promoted."{table}"
            ORDER BY created_at_utc, promotion_apply_run_id, decision_id
            """,
        )
        for row in rows:
            key = clean(row.get("target_entity_key"))
            if key:
                by_key[(table, key)] = row
    return by_key


def load_team_games(con: duckdb.DuckDBPyConnection, path: Path, boxscore_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
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
          season_type,
          team_code,
          opponent_code
        FROM read_parquet('{rel}')
        WHERE boxscore_id IN ({placeholders})
        ORDER BY boxscore_id, team_code
        """,
        boxscore_ids,
    )
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[clean(row.get("boxscore_id"))].append(row)
    return out


def load_player_index(con: duckdb.DuckDBPyConnection, path: Path, min_year: int, max_year: int) -> list[dict[str, Any]]:
    rel = str(path).replace("\\", "/").replace("'", "''")
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


def load_v26_team_year_ids(con: duckdb.DuckDBPyConnection, path: Path, min_year: int, max_year: int) -> set[tuple[str, int, str]]:
    if not path.exists():
        return set()
    rows = query_dicts(
        con,
        """
        SELECT DISTINCT NFL_player_id, TRY_CAST(year AS INTEGER) AS year, nfl_team
        FROM read_parquet(?)
        WHERE TRY_CAST(year AS INTEGER) BETWEEN ? AND ?
          AND NFL_player_id IS NOT NULL
          AND nfl_team IS NOT NULL
        """,
        [str(path), min_year, max_year],
    )
    return {
        (clean(row.get("NFL_player_id")), int(row["year"]), clean(row.get("nfl_team")))
        for row in rows
        if row.get("year") is not None
    }


def load_identity_bridges(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    out: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    if table_exists(con, "newspaper_promoted", "player_identity_candidate"):
        rows = query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_promoted.player_identity_candidate
            WHERE COALESCE(NFL_player_id, '') <> ''
            ORDER BY created_at_utc, promotion_apply_run_id, decision_id
            """,
        )
        for row in rows:
            key = (
                clean(row.get("boxscore_id")),
                norm(row.get("raw_player_name")),
                norm_key(row.get("raw_team")) or clean(row.get("nfl_team")).lower(),
            )
            if key[1]:
                out[key].append(row)
    return out


def resolve_team(raw_team: str, boxscore_id: str, team_games: dict[str, list[dict[str, Any]]]) -> tuple[str, str, dict[str, Any], str]:
    rows = team_games.get(boxscore_id, [])
    raw = clean(raw_team).strip()
    raw_key = norm_key(raw)
    cleaned_key = team_label_key(raw)
    alias = TEAM_ALIASES.get(raw_key) or TEAM_ALIASES.get(cleaned_key) or ""
    if not alias and raw.upper() in {clean(row.get("team_code")) for row in rows}:
        alias = raw.upper()
    if alias:
        for row in rows:
            if clean(row.get("team_code")) == alias:
                return alias, clean(row.get("opponent_code")), row, "team_alias"
        return alias, "", rows[0] if rows else {}, "team_alias_no_team_game_row"
    if len(rows) == 1:
        row = rows[0]
        return clean(row.get("team_code")), clean(row.get("opponent_code")), row, "single_team_game_row"
    return "", "", rows[0] if rows else {}, "unresolved_team"


def year_from_boxscore(boxscore_id: str) -> int | None:
    if len(boxscore_id) >= 4 and boxscore_id[:4].isdigit():
        return int(boxscore_id[:4])
    return None


def raw_roles_for_row(table: str, row: dict[str, Any]) -> list[dict[str, str]]:
    roles: list[dict[str, str]] = []
    if table == "scoring_event":
        raw_team = clean(row.get("scoring_team_raw")) or clean(row.get("scoring_team"))
        for role, raw_field, id_field in [
            ("scoring", "scoring_player_raw", "scoring_NFL_player_id"),
            ("passer", "passer_raw", "passer_NFL_player_id"),
            ("receiver", "receiver_raw", "receiver_NFL_player_id"),
        ]:
            raw_player = clean(row.get(raw_field)).strip()
            if raw_player and not clean(row.get(id_field)):
                roles.append({"role": role, "raw_player": raw_player, "raw_team": raw_team, "target_id_field": id_field})
    elif table == "play_by_play_event":
        raw_team = clean(row.get("possession_team_raw")) or clean(row.get("possession_team"))
        for role, raw_field, id_field in [
            ("primary", "primary_player_raw", "primary_NFL_player_id"),
            ("secondary", "secondary_player_raw", "secondary_NFL_player_id"),
        ]:
            raw_player = clean(row.get(raw_field)).strip()
            if raw_player and not clean(row.get(id_field)):
                roles.append({"role": role, "raw_player": raw_player, "raw_team": raw_team, "target_id_field": id_field})
    elif table in {"player_game_box_score", "player_game_stat_claim"}:
        raw_player = clean(row.get("player_raw")).strip()
        if raw_player and not clean(row.get("NFL_player_id")):
            roles.append({
                "role": "player",
                "raw_player": raw_player,
                "raw_team": clean(row.get("team_raw")) or clean(row.get("nfl_team")),
                "target_id_field": "NFL_player_id",
            })
    elif table == "source_document_note":
        raw_player = ""
        raw_team = ""
        for key in [clean(row.get("related_entity_key")), clean(row.get("target_entity_key"))]:
            parts = key.split("|")
            if key.startswith("lineup_participation|") and len(parts) >= 5:
                candidate_player = parts[2]
                candidate_team = parts[3]
            elif key.startswith("source_document_note|lineup_identity|") and len(parts) >= 6:
                candidate_team = parts[3]
                candidate_player = parts[4]
            else:
                continue
            if candidate_player and not candidate_player.startswith(clean(row.get("boxscore_id"))):
                raw_player = candidate_player
                raw_team = candidate_team
                break
        if raw_player:
            roles.append({
                "role": "lineup_note",
                "raw_player": raw_player,
                "raw_team": raw_team,
                "target_id_field": "NFL_player_id",
            })
    return roles


def candidate_score(
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
    cand_last = clean(candidate.get("_last"))
    if raw_last != cand_last:
        return 0, {}
    score = 60
    notes = {"last_name_match": "exact", "first_match": "not_supplied", "team_year_seen_in_v26": "0"}
    if raw_first and norm(raw_player) == clean(candidate.get("_full_norm")):
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
            score -= 12
            notes["first_match"] = "no"
    if team_code and (clean(candidate.get("pfr_id")), year, team_code) in v26_team_year_ids:
        score += 20
        notes["team_year_seen_in_v26"] = "1"
    return score, notes


def bridge_candidates(
    raw_player: str,
    raw_team: str,
    boxscore_id: str,
    bridges: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    exact_key = (boxscore_id, norm(raw_player), norm_key(raw_team))
    rows = bridges.get(exact_key, [])
    if not rows and raw_team:
        rows = bridges.get((boxscore_id, norm(raw_player), raw_team.lower()), [])
    output: list[dict[str, Any]] = []
    for row in rows:
        output.append({
            "candidate_score": 95,
            "candidate_NFL_player_id": clean(row.get("NFL_player_id")),
            "candidate_player": clean(row.get("resolved_player")),
            "candidate_position": "",
            "first_year": "",
            "last_year": "",
            "candidate_source": "existing_player_identity_candidate",
            "match_notes": json.dumps({
                "bridge_match_method": clean(row.get("match_method")),
                "bridge_confidence_score": clean(row.get("confidence_score")),
            }, sort_keys=True),
        })
    return output


def player_index_candidates(
    raw_player: str,
    year: int,
    team_code: str,
    player_index: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, int, str]],
) -> list[dict[str, Any]]:
    if not raw_player or not year:
        return []
    scored: list[dict[str, Any]] = []
    for candidate in player_index:
        first_year = candidate.get("first_year")
        last_year = candidate.get("last_year")
        if first_year is None or last_year is None or int(first_year) > year or int(last_year) < year:
            continue
        score, notes = candidate_score(raw_player, candidate, year, team_code, v26_team_year_ids)
        if score <= 0:
            continue
        scored.append({
            "candidate_score": score,
            "candidate_NFL_player_id": clean(candidate.get("pfr_id")),
            "candidate_player": clean(candidate.get("player")),
            "candidate_position": clean(candidate.get("index_position")),
            "first_year": clean(candidate.get("first_year")),
            "last_year": clean(candidate.get("last_year")),
            "candidate_source": "pfr_player_index",
            "match_notes": json.dumps(notes, sort_keys=True),
        })
    scored.sort(key=lambda row: (-int(row["candidate_score"]), row["candidate_player"], row["candidate_NFL_player_id"]))
    return scored


def choose_resolution(
    candidates: list[dict[str, Any]],
    raw_team: str,
    team_code: str,
    target_table: str,
    auto_unique_team_year_lastname: bool,
) -> tuple[str, str, str]:
    if not candidates:
        return "unresolved_no_candidate", "identity_resolution_needed", "no candidate matched player name/year"
    best = candidates[0]
    best_score = int(best["candidate_score"])
    second_score = int(candidates[1]["candidate_score"]) if len(candidates) > 1 else 0
    margin = best_score - second_score
    if clean(best.get("candidate_source")) == "existing_player_identity_candidate" and best_score >= 90:
        return "auto_resolved", "auto_resolved", "matched existing local player_identity_candidate"
    if best_score >= 100 and (len(candidates) == 1 or margin >= 20):
        return "auto_resolved", "auto_resolved", "high-confidence full-name/team-year player-index match"
    if best_score >= 85 and team_code and (len(candidates) == 1 or margin >= 20):
        return "auto_resolved", "auto_resolved", "high-confidence team-year player-index match"
    match_notes = parse_json_obj(best.get("match_notes"))
    if (
        auto_unique_team_year_lastname
        and target_table in {"scoring_event", "play_by_play_event", "player_game_box_score", "player_game_stat_claim"}
        and team_code
        and len(candidates) == 1
        and clean(best.get("candidate_source")) == "pfr_player_index"
        and best_score >= 80
        and clean(match_notes.get("last_name_match")) == "exact"
        and clean(match_notes.get("team_year_seen_in_v26")) == "1"
    ):
        return "auto_resolved", "auto_resolved", "unique exact-last-name team/year player-index match"
    if best_score >= 70:
        return "needs_review", "identity_candidate_review", "candidate exists but margin/score is not auto-safe"
    if raw_team and not team_code:
        return "unresolved_team", "identity_resolution_needed", "team unresolved, cannot safely score player"
    return "unresolved_low_confidence", "identity_resolution_needed", "candidate score below review threshold"


def build_patch(table: str, role: dict[str, str], nfl_id: str, player: str, year: int, week: int, team_code: str, opponent: str) -> dict[str, str]:
    patch = {role["target_id_field"]: nfl_id}
    if role["target_id_field"] == "NFL_player_id":
        patch["NFL_player_id"] = nfl_id
        patch["player_week"] = f"{nfl_id}_{year}_{week}" if nfl_id and year and week else ""
        if team_code:
            patch["nfl_team"] = team_code
        if opponent:
            patch["opponent_nfl_team"] = opponent
    else:
        if table == "scoring_event" and role["role"] == "scoring":
            patch["scoring_NFL_player_id"] = nfl_id
        if table == "play_by_play_event" and role["role"] == "primary":
            patch["primary_NFL_player_id"] = nfl_id
        if table in {"scoring_event", "play_by_play_event"} and team_code:
            team_field = "scoring_team" if table == "scoring_event" else "possession_team"
            patch[team_field] = team_code
    patch["resolved_player"] = player
    return {key: value for key, value in patch.items() if value}


def build_tasks(
    run_id: str,
    readiness_rows: list[dict[str, str]],
    promoted_by_key: dict[tuple[str, str], dict[str, Any]],
    team_games: dict[str, list[dict[str, Any]]],
    player_index: list[dict[str, Any]],
    v26_team_year_ids: set[tuple[str, int, str]],
    bridges: dict[tuple[str, str, str], list[dict[str, Any]]],
    auto_unique_team_year_lastname: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    created_at = iso_now()
    task_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []

    for readiness_index, readiness in enumerate(readiness_rows, start=1):
        table = clean(readiness.get("target_table"))
        target_key = clean(readiness.get("target_entity_key"))
        row = promoted_by_key.get((table, target_key))
        if not row:
            skipped_rows.append({
                "identity_resolution_run_id": run_id,
                "readiness_row_id": str(readiness_index),
                "target_table": table,
                "target_entity_key": target_key,
                "boxscore_id": clean(readiness.get("boxscore_id")),
                "skip_reason": "promoted_row_not_found_for_target_entity_key",
                "evidence_text": clean(readiness.get("evidence_text")),
                "source_documents_json": clean(readiness.get("source_documents_json")),
                "created_at_utc": created_at,
            })
            continue
        boxscore_id = clean(row.get("boxscore_id")) or clean(readiness.get("boxscore_id"))
        roles = raw_roles_for_row(table, row)
        if not roles:
            skipped_rows.append({
                "identity_resolution_run_id": run_id,
                "readiness_row_id": str(readiness_index),
                "target_table": table,
                "target_entity_key": target_key,
                "boxscore_id": boxscore_id,
                "skip_reason": "no_raw_player_role_extracted",
                "evidence_text": clean(readiness.get("evidence_text")) or clean(row.get("note_text")),
                "source_documents_json": clean(row.get("source_documents_json")),
                "created_at_utc": created_at,
            })
            continue
        for role in roles:
            raw_player = clean(role.get("raw_player"))
            raw_team = clean(role.get("raw_team"))
            team_code, opponent, game_row, team_method = resolve_team(raw_team, boxscore_id, team_games)
            year = parse_int(game_row.get("year")) or year_from_boxscore(boxscore_id) or 0
            week = parse_int(game_row.get("week")) or 0
            candidates = bridge_candidates(raw_player, raw_team, boxscore_id, bridges)
            if not candidates:
                candidates = player_index_candidates(raw_player, year, team_code, player_index, v26_team_year_ids)
            status, lane, reason = choose_resolution(
                candidates,
                raw_team,
                team_code,
                table,
                auto_unique_team_year_lastname,
            )
            best = candidates[0] if candidates else {}
            second = candidates[1] if len(candidates) > 1 else {}
            task_id = stable_id(run_id, table, target_key, role.get("role"), raw_player, raw_team)
            for rank, candidate in enumerate(candidates[:8], start=1):
                candidate_rows.append({
                    "identity_resolution_run_id": run_id,
                    "task_id": task_id,
                    "target_table": table,
                    "target_entity_key": target_key,
                    "role": clean(role.get("role")),
                    "boxscore_id": boxscore_id,
                    "raw_player": raw_player,
                    "raw_team": raw_team,
                    "candidate_rank": rank,
                    "created_at_utc": created_at,
                    **candidate,
                })
            nfl_id = clean(best.get("candidate_NFL_player_id")) if status == "auto_resolved" else ""
            player = clean(best.get("candidate_player")) if status == "auto_resolved" else ""
            patch = build_patch(table, role, nfl_id, player, year, week, team_code, opponent) if nfl_id else {}
            task_rows.append({
                "identity_resolution_run_id": run_id,
                "task_id": task_id,
                "readiness_row_id": str(readiness_index),
                "target_table": table,
                "target_entity_key": target_key,
                "role": clean(role.get("role")),
                "target_id_field": clean(role.get("target_id_field")),
                "boxscore_id": boxscore_id,
                "year": str(year) if year else "",
                "week": str(week) if week else "",
                "raw_player": raw_player,
                "raw_team": raw_team,
                "resolved_team": team_code,
                "opponent_team": opponent,
                "resolution_status": status,
                "resolution_lane": lane,
                "resolution_method": clean(best.get("candidate_source")) or team_method,
                "resolved_NFL_player_id": nfl_id,
                "resolved_player": player,
                "resolved_player_week": f"{nfl_id}_{year}_{week}" if nfl_id and year and week else "",
                "candidate_count": len(candidates),
                "best_score": clean(best.get("candidate_score")),
                "second_score": clean(second.get("candidate_score")),
                "score_margin": str(int(best.get("candidate_score") or 0) - int(second.get("candidate_score") or 0)) if best else "",
                "reason": reason,
                "proposed_patch_json": json.dumps(patch, sort_keys=True),
                "evidence_text": clean(readiness.get("evidence_text")) or clean(row.get("play_text")) or clean(row.get("source_row_text")) or clean(row.get("note_text")),
                "source_documents_json": clean(row.get("source_documents_json")),
                "created_at_utc": created_at,
            })
    return task_rows, candidate_rows, skipped_rows


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.identity_resolution_readiness_run (
          identity_resolution_run_id VARCHAR,
          readiness_dir VARCHAR,
          output_dir VARCHAR,
          task_count INTEGER,
          auto_resolved_count INTEGER,
          review_count INTEGER,
          unresolved_count INTEGER,
          candidate_count INTEGER,
          status VARCHAR,
          summary_json_path VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.identity_resolution_readiness_task (
          identity_resolution_run_id VARCHAR,
          task_id VARCHAR,
          readiness_row_id VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          role VARCHAR,
          target_id_field VARCHAR,
          boxscore_id VARCHAR,
          year VARCHAR,
          week VARCHAR,
          raw_player VARCHAR,
          raw_team VARCHAR,
          resolved_team VARCHAR,
          opponent_team VARCHAR,
          resolution_status VARCHAR,
          resolution_lane VARCHAR,
          resolution_method VARCHAR,
          resolved_NFL_player_id VARCHAR,
          resolved_player VARCHAR,
          resolved_player_week VARCHAR,
          candidate_count INTEGER,
          best_score VARCHAR,
          second_score VARCHAR,
          score_margin VARCHAR,
          reason VARCHAR,
          proposed_patch_json VARCHAR,
          evidence_text VARCHAR,
          source_documents_json VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.identity_resolution_readiness_candidate (
          identity_resolution_run_id VARCHAR,
          task_id VARCHAR,
          target_table VARCHAR,
          target_entity_key VARCHAR,
          role VARCHAR,
          boxscore_id VARCHAR,
          raw_player VARCHAR,
          raw_team VARCHAR,
          candidate_rank INTEGER,
          candidate_score INTEGER,
          candidate_NFL_player_id VARCHAR,
          candidate_player VARCHAR,
          candidate_position VARCHAR,
          first_year VARCHAR,
          last_year VARCHAR,
          candidate_source VARCHAR,
          match_notes VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )


def insert_rows(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rows: list[dict[str, Any]],
    fields: list[str],
) -> None:
    if not rows:
        return
    field_sql = ", ".join(fields)
    placeholders = ", ".join("?" for _ in fields)
    values = [[row.get(field) for field in fields] for row in rows]
    con.executemany(f"INSERT INTO {table_name} ({field_sql}) VALUES ({placeholders})", values)


def persist(db_path: Path, run_row: dict[str, Any], task_rows: list[dict[str, Any]], candidate_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("identity_resolution_run_id"))
        con.execute("DELETE FROM newspaper_review.identity_resolution_readiness_run WHERE identity_resolution_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.identity_resolution_readiness_task WHERE identity_resolution_run_id = ?", [run_id])
        con.execute("DELETE FROM newspaper_review.identity_resolution_readiness_candidate WHERE identity_resolution_run_id = ?", [run_id])
        insert_rows(con, "newspaper_review.identity_resolution_readiness_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.identity_resolution_readiness_task", task_rows, TASK_FIELDS)
        insert_rows(con, "newspaper_review.identity_resolution_readiness_candidate", candidate_rows, CANDIDATE_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], status_counts: Counter, lane_counts: Counter, samples: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Identity Resolution From Readiness",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Readiness dir: `{summary['readiness_dir']}`",
        f"- Task count: `{summary['task_count']}`",
        f"- Auto resolved: `{summary['auto_resolved_count']}`",
        f"- Review queue: `{summary['review_count']}`",
        f"- Unresolved: `{summary['unresolved_count']}`",
        f"- Skipped input rows: `{summary['skipped_count']}`",
        f"- Candidate rows: `{summary['candidate_count']}`",
        "",
        "## Status Counts",
        "",
    ]
    for key, count in sorted(status_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- `{key}`: {count}")
    lines.extend(["", "## Lane Counts", ""])
    for key, count in sorted(lane_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- `{key}`: {count}")
    lines.extend([
        "",
        "## Samples",
        "",
        "| Status | Table | Role | Raw | Team | Candidate | Score | Evidence |",
        "|---|---|---|---|---|---|---:|---|",
    ])
    for row in samples[:50]:
        evidence = clean(row.get("evidence_text")).replace("|", "/")[:120]
        candidate = clean(row.get("resolved_player")) or clean(row.get("reason"))
        lines.append(
            f"| `{row['resolution_status']}` | `{row['target_table']}` | `{row['role']}` | "
            f"{row['raw_player']} | {row['raw_team']} | {candidate} | "
            f"{row['best_score'] or ''} | {evidence} |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- All tasks: `{summary['tasks_csv']}`",
        f"- Auto patches: `{summary['auto_patches_csv']}`",
        f"- Review queue: `{summary['review_queue_csv']}`",
        f"- Unresolved queue: `{summary['unresolved_queue_csv']}`",
        f"- Skipped inputs: `{summary['skipped_csv']}`",
        f"- Candidates: `{summary['candidates_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readiness-dir", type=Path, default=None)
    parser.add_argument("--readiness-root", type=Path, default=DEFAULT_READINESS_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="identity_resolution_from_readiness")
    parser.add_argument("--auto-unique-team-year-lastname", action="store_true")
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    readiness_dir = args.readiness_dir or latest_readiness_dir(args.readiness_root)
    queue_csv = readiness_dir / "queue_identity_resolution_needed.csv"
    if not queue_csv.exists():
        raise FileNotFoundError(f"Identity queue not found: {queue_csv}")
    readiness_rows = read_csv(queue_csv)
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    boxscore_ids = sorted({clean(row.get("boxscore_id")) for row in readiness_rows if clean(row.get("boxscore_id"))})
    years = [year_from_boxscore(boxscore_id) for boxscore_id in boxscore_ids]
    min_year = min(year for year in years if year) if any(years) else 1920
    max_year = max(year for year in years if year) if any(years) else 1939

    read_con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        promoted_by_key = load_promoted_rows(read_con)
        bridges = load_identity_bridges(read_con)
    finally:
        read_con.close()

    ext_con = duckdb.connect()
    try:
        team_games = load_team_games(ext_con, args.team_games_path, boxscore_ids)
        player_index = load_player_index(ext_con, args.player_index_path, min_year, max_year)
        v26_team_year_ids = load_v26_team_year_ids(ext_con, args.v26_path, min_year, max_year)
    finally:
        ext_con.close()

    task_rows, candidate_rows, skipped_rows = build_tasks(
        run_id,
        readiness_rows,
        promoted_by_key,
        team_games,
        player_index,
        v26_team_year_ids,
        bridges,
        args.auto_unique_team_year_lastname,
    )
    status_counts = Counter(clean(row.get("resolution_status")) for row in task_rows)
    lane_counts = Counter(clean(row.get("resolution_lane")) for row in task_rows)
    auto_rows = [row for row in task_rows if row["resolution_status"] == "auto_resolved"]
    review_rows = [row for row in task_rows if row["resolution_lane"] == "identity_candidate_review"]
    unresolved_rows = [
        row for row in task_rows
        if row["resolution_lane"] == "identity_resolution_needed"
    ]

    tasks_csv = out_dir / "identity_resolution_tasks.csv"
    auto_csv = out_dir / "auto_resolved_identity_patches.csv"
    review_csv = out_dir / "identity_candidate_review_queue.csv"
    unresolved_csv = out_dir / "unresolved_identity_queue.csv"
    skipped_csv = out_dir / "skipped_identity_queue.csv"
    candidates_csv = out_dir / "identity_resolution_candidates.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "identity_resolution_report.md"

    summary = {
        "identity_resolution_run_id": run_id,
        "created_at_utc": created_at,
        "readiness_dir": str(readiness_dir),
        "identity_queue_csv": str(queue_csv),
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "task_count": len(task_rows),
        "auto_resolved_count": len(auto_rows),
        "review_count": len(review_rows),
        "unresolved_count": len(unresolved_rows),
        "skipped_count": len(skipped_rows),
        "candidate_count": len(candidate_rows),
        "resolution_status_counts": dict(status_counts),
        "resolution_lane_counts": dict(lane_counts),
        "tasks_csv": str(tasks_csv),
        "auto_patches_csv": str(auto_csv),
        "review_queue_csv": str(review_csv),
        "unresolved_queue_csv": str(unresolved_csv),
        "skipped_csv": str(skipped_csv),
        "candidates_csv": str(candidates_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown),
        "persisted_to_duckdb": not args.no_persist,
        "auto_unique_team_year_lastname": bool(args.auto_unique_team_year_lastname),
    }

    write_csv(tasks_csv, task_rows, TASK_FIELDS)
    write_csv(auto_csv, auto_rows, TASK_FIELDS)
    write_csv(review_csv, review_rows, TASK_FIELDS)
    write_csv(unresolved_csv, unresolved_rows, TASK_FIELDS)
    write_csv(skipped_csv, skipped_rows, SKIPPED_FIELDS)
    write_csv(candidates_csv, candidate_rows, CANDIDATE_FIELDS)
    summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=True, sort_keys=True) + "\n", encoding="utf-8")
    markdown.write_text(render_markdown(summary, status_counts, lane_counts, task_rows), encoding="utf-8")

    if not args.no_persist:
        persist(
            args.db_path,
            {
                "identity_resolution_run_id": run_id,
                "readiness_dir": str(readiness_dir),
                "output_dir": str(out_dir),
                "task_count": len(task_rows),
                "auto_resolved_count": len(auto_rows),
                "review_count": len(review_rows),
                "unresolved_count": len(unresolved_rows),
                "candidate_count": len(candidate_rows),
                "status": "complete",
                "summary_json_path": str(summary_json),
                "created_at_utc": created_at,
            },
            task_rows,
            candidate_rows,
        )

    print(json.dumps({
        "identity_resolution_run_id": run_id,
        "output_dir": str(out_dir),
        "task_count": len(task_rows),
        "auto_resolved_count": len(auto_rows),
        "review_count": len(review_rows),
        "unresolved_count": len(unresolved_rows),
        "skipped_count": len(skipped_rows),
        "candidate_count": len(candidate_rows),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
