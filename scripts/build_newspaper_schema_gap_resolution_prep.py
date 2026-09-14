#!/usr/bin/env python
"""Resolve schema-gap newspaper holds into local atom-table decisions.

This station handles evidence that is useful but does not fit the narrower
`player_game_box_score` columns yet, such as unsplit touchdown totals, points
from touchdowns, playing-time notes, and injury notes. It writes only local
D-drive artifacts and local DuckDB review tables; it never writes to Fly, the
supertable, or production league tables.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "schema_gap_resolution_preps"
DEFAULT_PLAYER_INDEX = Path(r"D:\league-history-data\nfl\raw\pfr\players\player_index.parquet")
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

ITEM_FIELDS = [
    "schema_gap_resolution_run_id",
    "promotion_apply_run_id",
    "decision_id",
    "promotion_package_id",
    "source_document_id",
    "source_target_table",
    "resolved_target_table",
    "target_entity_key",
    "boxscore_id",
    "game_date",
    "year",
    "week",
    "raw_player",
    "resolved_player",
    "resolved_NFL_player_id",
    "resolved_player_week",
    "raw_team",
    "resolved_team",
    "opponent_team",
    "match_method",
    "identity_status",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "reason",
    "schema_gap_fields_json",
    "proposed_fields_json",
    "enriched_proposed_fields_json",
    "source_documents_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "schema_gap_resolution_run_id",
    "promotion_apply_run_id",
    "output_dir",
    "schema_gap_decision_count",
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
    "hammond": "HAM",
    "hammond pros": "HAM",
    "minneapolis marines": "MIN",
    "muncie flyers": "MUN",
    "racine cardinals": "RAC",
    "rock island independents": "RII",
}

STAT_GAP_FIELDS = ["touchdowns", "points_from_touchdowns"]
NOTE_GAP_FIELDS = ["playing_time_note", "injury_note"]


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


def parse_int(value: Any) -> int | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


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


def load_route_rows(con: duckdb.DuckDBPyConnection, promotion_apply_run_id: str) -> list[dict[str, Any]]:
    if not promotion_apply_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND target_table = 'player_game_box_score'
        ORDER BY boxscore_id, decision_id
        """,
        [promotion_apply_run_id],
    )


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
        boxscore_id = clean(row.get("boxscore_id"))
        team_code = clean(row.get("team_code"))
        if boxscore_id and team_code:
            out[f"{boxscore_id}|{team_code}"] = row
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


def resolve_team(raw_team: Any, boxscore_id: str, team_games: dict[str, dict[str, Any]]) -> dict[str, Any]:
    team = TEAM_ALIASES.get(norm_text(raw_team), "")
    if not team:
        return {"nfl_team": "", "opponent_nfl_team": "", "game_date": "", "year": "", "week": ""}
    game = team_games.get(f"{boxscore_id}|{team}", {})
    return {
        "nfl_team": team,
        "opponent_nfl_team": clean(game.get("opponent_code")),
        "game_date": clean(game.get("game_date")),
        "year": clean(parse_int(game.get("year")) or ""),
        "week": clean(parse_int(game.get("week")) or ""),
    }


def resolve_player(raw_player: Any, year: int | None, player_index: list[dict[str, Any]]) -> dict[str, str]:
    raw_norm = norm_text(raw_player)
    if not raw_norm:
        return {
            "resolved_player": "",
            "NFL_player_id": "",
            "match_method": "missing_player_name",
            "identity_status": "unresolved_player_name_missing",
        }
    candidates: list[dict[str, Any]] = []
    raw_tokens = raw_norm.split()
    for row in player_index:
        first_year = parse_int(row.get("first_year"))
        last_year = parse_int(row.get("last_year"))
        if year is not None and first_year is not None and last_year is not None:
            if year < first_year or year > last_year:
                continue
        player_norm = norm_text(row.get("player"))
        player_tokens = player_norm.split()
        if not player_tokens:
            continue
        if player_norm == raw_norm:
            rank = 0
        elif len(raw_tokens) == 1 and player_tokens[-1] == raw_tokens[0]:
            rank = 1
        else:
            continue
        candidates.append({**row, "match_rank": rank})
    if not candidates:
        return {
            "resolved_player": "",
            "NFL_player_id": "",
            "match_method": "player_index_no_match",
            "identity_status": "unresolved_player_identity",
        }
    candidates.sort(key=lambda row: (int(row.get("match_rank") or 9), clean(row.get("player"))))
    best_rank = int(candidates[0].get("match_rank") or 0)
    best = [row for row in candidates if int(row.get("match_rank") or 0) == best_rank]
    if len(best) == 1:
        method = "player_index_exact_name" if best_rank == 0 else "player_index_unique_last_name_by_year"
        return {
            "resolved_player": clean(best[0].get("player")),
            "NFL_player_id": clean(best[0].get("pfr_id")),
            "match_method": method,
            "identity_status": "resolved_player_index",
        }
    return {
        "resolved_player": "",
        "NFL_player_id": "",
        "match_method": "player_index_ambiguous",
        "identity_status": "ambiguous_player_identity",
    }


def stat_payload(
    proposed: dict[str, Any],
    route_row: dict[str, Any],
    team_info: dict[str, Any],
    player_info: dict[str, str],
) -> dict[str, Any]:
    stat_fields = {field: clean(proposed.get(field)) for field in STAT_GAP_FIELDS if clean(proposed.get(field))}
    primary = "touchdowns" if "touchdowns" in stat_fields else next(iter(stat_fields), "")
    player_week = ""
    year = parse_int(team_info.get("year"))
    week = parse_int(team_info.get("week"))
    if clean(player_info.get("NFL_player_id")) and year is not None and week is not None:
        player_week = f"{player_info['NFL_player_id']}_{year}_{week}"
    return {
        "player_game_stat_claim_id": stable_id(
            "player_game_stat_claim",
            route_row.get("decision_id"),
            route_row.get("boxscore_id"),
            proposed.get("player_raw"),
            primary,
        ),
        "boxscore_id": clean(route_row.get("boxscore_id")),
        "game_date": clean(team_info.get("game_date")),
        "year": clean(team_info.get("year")),
        "week": clean(team_info.get("week")),
        "player_week": player_week,
        "player_raw": clean(proposed.get("player_raw")),
        "resolved_player": clean(player_info.get("resolved_player")),
        "NFL_player_id": clean(player_info.get("NFL_player_id")),
        "team_raw": clean(proposed.get("team_raw")),
        "nfl_team": clean(team_info.get("nfl_team")),
        "opponent_nfl_team": clean(team_info.get("opponent_nfl_team")),
        "stat_name": primary,
        "stat_value": clean(stat_fields.get(primary)),
        "stat_unit": "count" if primary == "touchdowns" else "points",
        "stat_fields_json": json.dumps(stat_fields, sort_keys=True, ensure_ascii=False),
        "stat_context": "newspaper schema-gap stat claim; touchdown type not split unless separately supported by scoring/PBP evidence",
        "source_row_text": clean(proposed.get("source_row_text")),
        "confidence_score": clean(proposed.get("confidence_score")) or "0.70",
        "review_status": "local_atom_schema_gap_accepted",
        "promotion_status": "local_atom_only",
        "source_document_id": clean(route_row.get("source_document_id")),
        "region_id": clean(proposed.get("region_id")),
        "match_method": clean(player_info.get("match_method")),
        "identity_status": clean(player_info.get("identity_status")),
    }


def note_payload(
    proposed: dict[str, Any],
    route_row: dict[str, Any],
    team_info: dict[str, Any],
    player_info: dict[str, str],
) -> dict[str, Any]:
    note_fields = {field: clean(proposed.get(field)) for field in NOTE_GAP_FIELDS if clean(proposed.get(field))}
    note_type = "injury_note" if "injury_note" in note_fields else next(iter(note_fields), "")
    note_text = clean(note_fields.get(note_type))
    if len(note_fields) > 1:
        note_text = "; ".join(f"{field}: {value}" for field, value in note_fields.items())
        note_type = "multi_note"
    player_week = ""
    year = parse_int(team_info.get("year"))
    week = parse_int(team_info.get("week"))
    if clean(player_info.get("NFL_player_id")) and year is not None and week is not None:
        player_week = f"{player_info['NFL_player_id']}_{year}_{week}"
    return {
        "player_game_note_id": stable_id(
            "player_game_note",
            route_row.get("decision_id"),
            route_row.get("boxscore_id"),
            proposed.get("player_raw"),
            note_type,
        ),
        "boxscore_id": clean(route_row.get("boxscore_id")),
        "game_date": clean(team_info.get("game_date")),
        "year": clean(team_info.get("year")),
        "week": clean(team_info.get("week")),
        "player_week": player_week,
        "player_raw": clean(proposed.get("player_raw")),
        "resolved_player": clean(player_info.get("resolved_player")),
        "NFL_player_id": clean(player_info.get("NFL_player_id")),
        "team_raw": clean(proposed.get("team_raw")),
        "nfl_team": clean(team_info.get("nfl_team")),
        "opponent_nfl_team": clean(team_info.get("opponent_nfl_team")),
        "note_type": note_type,
        "note_text": note_text,
        "note_fields_json": json.dumps(note_fields, sort_keys=True, ensure_ascii=False),
        "source_row_text": clean(proposed.get("source_row_text")),
        "confidence_score": clean(proposed.get("confidence_score")) or "0.70",
        "review_status": "local_atom_schema_gap_accepted",
        "promotion_status": "local_atom_only",
        "source_document_id": clean(route_row.get("source_document_id")),
        "region_id": clean(proposed.get("region_id")),
        "match_method": clean(player_info.get("match_method")),
        "identity_status": clean(player_info.get("identity_status")),
    }


def build_resolution_rows(
    schema_gap_resolution_run_id: str,
    promotion_apply_run_id: str,
    route_rows: list[dict[str, Any]],
    team_games: dict[str, dict[str, Any]],
    player_index: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    created_at = iso_now()
    item_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, str]] = []

    for row in route_rows:
        proposed = parse_json_obj(row.get("proposed_fields_json"))
        stat_fields = {field: clean(proposed.get(field)) for field in STAT_GAP_FIELDS if clean(proposed.get(field))}
        note_fields = {field: clean(proposed.get(field)) for field in NOTE_GAP_FIELDS if clean(proposed.get(field))}
        if not stat_fields and not note_fields:
            continue

        boxscore_id = clean(row.get("boxscore_id"))
        team_info = resolve_team(proposed.get("team_raw"), boxscore_id, team_games)
        year_int = parse_int(team_info.get("year"))
        player_info = resolve_player(proposed.get("player_raw"), year_int, player_index)
        if stat_fields:
            resolved_table = "player_game_stat_claim"
            enriched = stat_payload(proposed, row, team_info, player_info)
            reason = "schema-gap stat preserved as local player_game_stat_claim"
        else:
            resolved_table = "player_game_note"
            enriched = note_payload(proposed, row, team_info, player_info)
            reason = "schema-gap note preserved as local player_game_note"

        target_key = f"{resolved_table}|{boxscore_id}|{enriched.get('NFL_player_id') or proposed.get('player_raw')}|{enriched.get('stat_name') or enriched.get('note_type')}"
        enriched_json = json.dumps(enriched, sort_keys=True, ensure_ascii=False)
        decision_rows.append({
            "decision_id": clean(row.get("decision_id")),
            "decision_status": "approved_for_local_promotion",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "",
            "resolved_boxscore_id": boxscore_id,
            "resolved_target_table": resolved_table,
            "resolved_target_entity_key": target_key,
            "proposed_fields_json": enriched_json,
            "notes": reason,
        })
        item_rows.append({
            "schema_gap_resolution_run_id": schema_gap_resolution_run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "decision_id": clean(row.get("decision_id")),
            "promotion_package_id": clean(row.get("promotion_package_id")),
            "source_document_id": clean(row.get("source_document_id")),
            "source_target_table": clean(row.get("target_table")),
            "resolved_target_table": resolved_table,
            "target_entity_key": target_key,
            "boxscore_id": boxscore_id,
            "game_date": clean(team_info.get("game_date")),
            "year": clean(team_info.get("year")),
            "week": clean(team_info.get("week")),
            "raw_player": clean(proposed.get("player_raw")),
            "resolved_player": clean(player_info.get("resolved_player")),
            "resolved_NFL_player_id": clean(player_info.get("NFL_player_id")),
            "resolved_player_week": clean(enriched.get("player_week")),
            "raw_team": clean(proposed.get("team_raw")),
            "resolved_team": clean(team_info.get("nfl_team")),
            "opponent_team": clean(team_info.get("opponent_nfl_team")),
            "match_method": clean(player_info.get("match_method")),
            "identity_status": clean(player_info.get("identity_status")),
            "decision_status": "approved_for_local_promotion",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "",
            "reason": reason,
            "schema_gap_fields_json": json.dumps({**stat_fields, **note_fields}, sort_keys=True, ensure_ascii=False),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "enriched_proposed_fields_json": enriched_json,
            "source_documents_json": clean(row.get("source_documents_json")),
            "created_at_utc": created_at,
        })
    return item_rows, decision_rows


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(fields))
    field_list = ", ".join(fields)
    values = [[clean(row.get(field)) for field in fields] for row in rows]
    con.executemany(f"INSERT INTO newspaper_review.{table} ({field_list}) VALUES ({placeholders})", values)


def persist(
    db_path: Path,
    schema_gap_resolution_run_id: str,
    promotion_apply_run_id: str,
    out_dir: Path,
    item_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.schema_gap_resolution_item ("
            + ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.schema_gap_resolution_run ("
            + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
            + ")"
        )
        for field in ITEM_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.schema_gap_resolution_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in RUN_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.schema_gap_resolution_run ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        con.execute(
            "DELETE FROM newspaper_review.schema_gap_resolution_item WHERE schema_gap_resolution_run_id = ?",
            [schema_gap_resolution_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.schema_gap_resolution_run WHERE schema_gap_resolution_run_id = ?",
            [schema_gap_resolution_run_id],
        )
        insert_rows(con, "schema_gap_resolution_item", item_rows, ITEM_FIELDS)
        insert_rows(con, "schema_gap_resolution_run", [{
            "schema_gap_resolution_run_id": schema_gap_resolution_run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "output_dir": str(out_dir),
            "schema_gap_decision_count": len(item_rows),
            "approved_count": len(item_rows),
            "held_count": 0,
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Schema-Gap Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['schema_gap_resolution_run_id']}`",
        f"Apply source: `{summary['promotion_apply_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Reviewed schema-gap rows: `{summary['schema_gap_decision_count']}`",
        f"- Approved local atom rows: `{summary['approved_count']}`",
        f"- Target tables: `{summary['resolved_target_table_counts']}`",
        f"- Identity statuses: `{summary['identity_status_counts']}`",
        "",
        "## Items",
        "",
        "| target | boxscore | player | team | identity | fields | reason |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in item_rows:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("resolved_target_table")),
                clean(row.get("boxscore_id")),
                clean(row.get("raw_player")),
                clean(row.get("resolved_team")),
                clean(row.get("identity_status")),
                clean(row.get("schema_gap_fields_json")).replace("|", "/"),
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
    parser.add_argument("--label", default="newspaper_schema_gap_resolution")
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--player-index", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--team-games", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    schema_gap_resolution_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / schema_gap_resolution_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        promotion_apply_run_id = args.promotion_apply_run_id or latest_apply_run(con)
        route_rows = load_route_rows(con, promotion_apply_run_id)
    finally:
        con.close()

    team_games = load_team_games(args.team_games)
    player_index = load_player_index(args.player_index)
    item_rows, new_decision_rows = build_resolution_rows(
        schema_gap_resolution_run_id,
        promotion_apply_run_id,
        route_rows,
        team_games,
        player_index,
    )
    base_input = args.base_decision_input_csv or latest_decision_input(args.root)
    base_rows = read_base_decisions(base_input)
    combined_rows = combine_decision_inputs(base_rows, new_decision_rows)

    decision_input_path = out_dir / "decision_input.csv"
    summary = {
        "created_at_utc": created_at,
        "schema_gap_resolution_run_id": schema_gap_resolution_run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "base_decision_input_csv": str(base_input) if base_input else "",
        "decision_input_csv": str(decision_input_path),
        "route_rows_read": len(route_rows),
        "schema_gap_decision_count": len(item_rows),
        "approved_count": len(new_decision_rows),
        "held_count": 0,
        "resolved_target_table_counts": dict(Counter(row["resolved_target_table"] for row in item_rows)),
        "identity_status_counts": dict(Counter(row["identity_status"] for row in item_rows)),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "schema_gap_resolution_items.csv", item_rows, ITEM_FIELDS)
    write_csv(out_dir / "decision_input_delta.csv", new_decision_rows, DECISION_INPUT_FIELDS)
    write_csv(decision_input_path, combined_rows, DECISION_INPUT_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "schema_gap_resolution_report.md", summary, item_rows)
    if not args.no_db:
        persist(args.db_path, schema_gap_resolution_run_id, promotion_apply_run_id, out_dir, item_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
