#!/usr/bin/env python
"""Classify local newspaper-promoted atoms against the v26 supertable.

This station is deliberately read-only. It reads the local D-drive newspaper
atom DuckDB and local v26 parquet, then writes D-drive CSV/JSON/Markdown
artifacts. It does not write to Fly, v26, or production supertable tables.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_RELEASE_ROOT = Path(r"D:\league-history-data\nfl\releases")
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "v26_readiness_reports"

PROMOTED_TABLES = [
    "game_candidate",
    "lineup_participation",
    "play_by_play_event",
    "player_game_box_score",
    "player_game_stat_claim",
    "player_game_note",
    "team_game_stat_claim",
    "source_document_note",
    "player_identity_candidate",
    "scoring_event",
]

PLAYER_STAT_MAPPINGS = {
    "carries": ["carries", "rush_att", "rushing_att", "rush_attempts", "rushing_attempts"],
    "rushing_yards": ["rushing_yards", "rush_yds", "rushing_yds"],
    "rushing_tds": ["rushing_tds", "rush_td", "rush_tds", "rushing_td"],
    "attempts": ["attempts", "pass_att", "passing_att", "passing_attempts"],
    "completions": ["completions", "pass_cmp", "passing_cmp", "passing_completions"],
    "passing_yards": ["passing_yards", "pass_yds", "passing_yds"],
    "passing_tds": ["passing_tds", "pass_td", "pass_tds", "passing_td"],
    "passing_interceptions": ["passing_interceptions", "pass_int", "passing_int"],
    "receptions": ["receptions", "rec", "receiving_rec"],
    "receiving_yards": ["receiving_yards", "rec_yds", "receiving_yds"],
    "receiving_tds": ["receiving_tds", "rec_td", "rec_tds", "receiving_td"],
    "pat_made": ["pat_made", "xpm", "extra_points_made"],
    "pat_att": ["pat_att", "xpa", "extra_points_attempted"],
    "fg_made": ["fg_made", "fgm", "field_goals_made"],
    "fg_att": ["fg_att", "fga", "field_goals_attempted"],
    "fg_long": ["fg_long", "long_fg"],
    "def_interceptions": ["def_interceptions", "def_int", "defensive_interceptions"],
    "def_sacks": ["def_sacks", "sacks"],
    "def_tds": ["def_tds", "def_td", "defensive_tds"],
    "special_teams_tds": ["special_teams_tds", "return_tds", "ret_td"],
}

STAT_CLAIM_FIELD_ALIASES = {
    "touchdowns": [],
    "td": [],
    "tds": [],
    "points": [],
    "goals_from_touchdown": ["pat_made"],
    "extra_points": ["pat_made"],
    "field_goals": ["fg_made"],
}

ROW_FIELDS = [
    "target_table",
    "target_entity_key",
    "promotion_apply_run_id",
    "decision_id",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
    "stat_name",
    "stat_value",
    "event_type",
    "play_type",
    "confidence_bar",
    "confidence_score",
    "source_document_count",
    "readiness_lane",
    "value_class",
    "v26_status",
    "identity_status",
    "latest_entity_row",
    "evidence_text",
    "source_documents_json",
]

STAT_DETAIL_FIELDS = [
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "promoted_field",
    "promoted_value",
    "v26_field",
    "v26_value",
    "v26_status",
    "readiness_lane",
    "source_text",
]

ROLLUP_FIELDS = [
    "target_table",
    "readiness_lane",
    "value_class",
    "raw_row_count",
    "latest_entity_count",
    "distinct_entity_count",
    "distinct_boxscore_count",
    "high_count",
    "medium_count",
    "low_count",
    "unknown_count",
]

IDENTITY_PATCH_RECEIPT_FIELDS = [
    "identity_resolution_run_id",
    "task_id",
    "target_table",
    "target_entity_key",
    "field_name",
    "old_value",
    "new_value",
    "applied_status",
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
    return str(value)


def parse_num(value: Any) -> float | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_int(value: Any) -> str:
    num = parse_num(value)
    if num is None:
        return clean(value).strip()
    return str(int(num))


def parse_json_obj(value: Any) -> dict[str, Any]:
    text = clean(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_json_list(value: Any) -> list[Any]:
    text = clean(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return compact_json(value)
    return str(value)


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


def latest_v26(release_root: Path) -> Path:
    files = sorted(
        glob.glob(str(release_root / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
        key=lambda path: Path(path).stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(f"No v26 parquet found under {release_root}")
    return Path(files[0])


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


def table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    if not table_exists(con, schema, table):
        return []
    return [
        clean(row[0])
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
    ]


def fetch_promoted_rows(con: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    cols = table_columns(con, "newspaper_promoted", table)
    if not cols:
        return []
    result = con.execute(
        f"""
        SELECT *
        FROM newspaper_promoted."{table}"
        ORDER BY created_at_utc, promotion_apply_run_id, decision_id
        """
    )
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def load_identity_auto_patches(
    con: duckdb.DuckDBPyConnection,
    identity_resolution_run_id: str,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    if not identity_resolution_run_id:
        return {}
    if not table_exists(con, "newspaper_review", "identity_resolution_readiness_task"):
        return {}
    rows = query_dicts(
        con,
        """
        SELECT
          identity_resolution_run_id,
          task_id,
          target_table,
          target_entity_key,
          proposed_patch_json
        FROM newspaper_review.identity_resolution_readiness_task
        WHERE identity_resolution_run_id = ?
          AND resolution_status = 'auto_resolved'
        ORDER BY task_id
        """,
        [identity_resolution_run_id],
    )
    patches: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        patch = parse_json_obj(row.get("proposed_patch_json"))
        if not patch:
            continue
        row = dict(row)
        row["_patch"] = patch
        patches[clean(row.get("target_table"))][clean(row.get("target_entity_key"))].append(row)
    return patches


def apply_identity_overlay(
    rows_by_table: dict[str, list[dict[str, Any]]],
    patches_by_table: dict[str, dict[str, list[dict[str, Any]]]],
    created_at_utc: str,
) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    if not patches_by_table:
        return receipts
    matched_targets: set[tuple[str, str]] = set()
    for table, rows in rows_by_table.items():
        table_patches = patches_by_table.get(table, {})
        if not table_patches:
            continue
        for row in rows:
            target_key = clean(row.get("target_entity_key"))
            patch_tasks = table_patches.get(target_key, [])
            if not patch_tasks:
                continue
            matched_targets.add((table, target_key))
            for patch_task in patch_tasks:
                patch = patch_task.get("_patch") or {}
                for field_name, value in patch.items():
                    if field_name not in row:
                        receipts.append({
                            "identity_resolution_run_id": clean(patch_task.get("identity_resolution_run_id")),
                            "task_id": clean(patch_task.get("task_id")),
                            "target_table": table,
                            "target_entity_key": target_key,
                            "field_name": field_name,
                            "old_value": "",
                            "new_value": value,
                            "applied_status": "field_not_in_target_table",
                            "created_at_utc": created_at_utc,
                        })
                        continue
                    old_value = clean(row.get(field_name))
                    new_value = clean(value)
                    if not new_value or old_value == new_value:
                        status = "already_present" if old_value == new_value and new_value else "blank_patch_value"
                    else:
                        row[field_name] = new_value
                        status = "applied"
                    receipts.append({
                        "identity_resolution_run_id": clean(patch_task.get("identity_resolution_run_id")),
                        "task_id": clean(patch_task.get("task_id")),
                        "target_table": table,
                        "target_entity_key": target_key,
                        "field_name": field_name,
                        "old_value": old_value,
                        "new_value": new_value,
                        "applied_status": status,
                        "created_at_utc": created_at_utc,
                    })
    for table, patches_by_key in patches_by_table.items():
        for target_key, patch_tasks in patches_by_key.items():
            if (table, target_key) in matched_targets:
                continue
            for patch_task in patch_tasks:
                receipts.append({
                    "identity_resolution_run_id": clean(patch_task.get("identity_resolution_run_id")),
                    "task_id": clean(patch_task.get("task_id")),
                    "target_table": table,
                    "target_entity_key": target_key,
                    "field_name": "",
                    "old_value": "",
                    "new_value": "",
                    "applied_status": "target_row_not_found",
                    "created_at_utc": created_at_utc,
                })
    return receipts


def v26_columns(con: duckdb.DuckDBPyConnection, v26_path: Path) -> list[str]:
    result = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(v26_path)])
    return [item[0] for item in result.description]


def choose_stat_mappings(v26_cols: set[str]) -> dict[str, str]:
    lower_to_actual = {col.lower(): col for col in v26_cols}
    mapping: dict[str, str] = {}
    for promoted_col, candidates in PLAYER_STAT_MAPPINGS.items():
        for candidate in candidates:
            actual = lower_to_actual.get(candidate.lower())
            if actual:
                mapping[promoted_col] = actual
                break
    return mapping


def load_v26_player_rows(
    con: duckdb.DuckDBPyConnection,
    v26_path: Path,
    v26_cols: set[str],
    player_weeks: list[str],
    stat_mapping: dict[str, str],
) -> dict[str, dict[str, Any]]:
    unique_weeks = sorted({week for week in player_weeks if week})
    if not unique_weeks or "player_week" not in v26_cols:
        return {}
    selected = ["player_week"]
    for optional in [
        "player",
        "NFL_player_id",
        "nfl_team",
        "opponent_nfl_team",
        "position",
        "starter_position",
        "is_starter",
    ]:
        if optional in v26_cols and optional not in selected:
            selected.append(optional)
    for mapped_col in stat_mapping.values():
        if mapped_col in v26_cols and mapped_col not in selected:
            selected.append(mapped_col)
    quoted = ", ".join(f'"{col}"' for col in selected)
    out: dict[str, dict[str, Any]] = {}
    for index in range(0, len(unique_weeks), 750):
        chunk = unique_weeks[index:index + 750]
        placeholders = ",".join(["?"] * len(chunk))
        rows = query_dicts(
            con,
            f"""
            SELECT {quoted}
            FROM read_parquet(?)
            WHERE player_week IN ({placeholders})
            """,
            [str(v26_path), *chunk],
        )
        for row in rows:
            player_week = clean(row.get("player_week"))
            if player_week and player_week not in out:
                out[player_week] = row
    return out


def load_v26_game_rows(con: duckdb.DuckDBPyConnection, v26_path: Path, v26_cols: set[str]) -> list[dict[str, Any]]:
    needed = {"year", "game_date", "nfl_team", "opponent_nfl_team", "points_allowed"}
    if not needed.issubset(v26_cols):
        return []
    position_filter = "AND position = 'DEF'" if "position" in v26_cols else ""
    return query_dicts(
        con,
        f"""
        SELECT
          CAST(CAST(year AS INTEGER) AS VARCHAR) AS year,
          CAST(CAST(game_date AS DATE) AS VARCHAR) AS game_date,
          nfl_team AS team,
          opponent_nfl_team AS opponent,
          points_allowed,
          COUNT(1) AS row_count
        FROM read_parquet(?)
        WHERE CAST(year AS INTEGER) BETWEEN 1920 AND 1939
          AND nfl_team IS NOT NULL
          AND opponent_nfl_team IS NOT NULL
          {position_filter}
        GROUP BY 1,2,3,4,5
        ORDER BY year, game_date, team, opponent
        """,
        [str(v26_path)],
    )


def score_rows_from_points_allowed(
    rows: list[dict[str, Any]],
    team_1: str,
    team_2: str,
    source: str,
) -> list[dict[str, Any]]:
    by_pair = {(clean(row.get("team")), clean(row.get("opponent"))): row for row in rows}
    row_1 = by_pair.get((team_1, team_2))
    row_2 = by_pair.get((team_2, team_1))
    if not row_1 and not row_2:
        return []
    out: list[dict[str, Any]] = []
    if row_1:
        out.append({
            **row_1,
            "team_score": normalize_int(row_2.get("points_allowed")) if row_2 else "",
            "opponent_score": normalize_int(row_1.get("points_allowed")),
            "score_source": source,
        })
    if row_2:
        out.append({
            **row_2,
            "team_score": normalize_int(row_1.get("points_allowed")) if row_1 else "",
            "opponent_score": normalize_int(row_2.get("points_allowed")),
            "score_source": source,
        })
    return out


def build_game_score_index(v26_game_rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, str, str, str], list[dict[str, Any]]], dict[tuple[str, str, str], list[dict[str, Any]]]]:
    dated_raw: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    undated_raw: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in v26_game_rows:
        year = clean(row.get("year"))
        game_date = clean(row.get("game_date"))
        team = clean(row.get("team"))
        opponent = clean(row.get("opponent"))
        if game_date:
            dated_raw[(game_date, team, opponent)].append(row)
        else:
            undated_raw[(year, team, opponent)].append(row)

    dated: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    undated: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    pair_keys = {
        (date, team, opponent)
        for (date, team, opponent) in dated_raw
        if team and opponent
    }
    for date, team, opponent in pair_keys:
        if (date, opponent, team) in dated_raw:
            pair_rows = dated_raw[(date, team, opponent)] + dated_raw[(date, opponent, team)]
            key = (date, *sorted([team, opponent]))
            dated[key] = score_rows_from_points_allowed(pair_rows, team, opponent, "dated_opponent_points_allowed_pair")

    year_pair_keys = {
        (year, team, opponent)
        for (year, team, opponent) in undated_raw
        if team and opponent
    }
    for year, team, opponent in year_pair_keys:
        if (year, opponent, team) in undated_raw:
            pair_rows = undated_raw[(year, team, opponent)] + undated_raw[(year, opponent, team)]
            key = (year, *sorted([team, opponent]))
            undated[key] = score_rows_from_points_allowed(pair_rows, team, opponent, "undated_opponent_points_allowed_pair")
    return dated, undated


def date_from_boxscore_id(boxscore_id: str) -> str:
    if len(boxscore_id) < 8 or not boxscore_id[:8].isdigit():
        return ""
    raw = boxscore_id[:8]
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def year_from_boxscore_id(boxscore_id: str) -> str:
    return boxscore_id[:4] if len(boxscore_id) >= 4 and boxscore_id[:4].isdigit() else ""


def entity_key(table: str, row: dict[str, Any]) -> str:
    key = clean(row.get("target_entity_key"))
    if key:
        return key
    for fallback in [
        f"{table}_id",
        "game_candidate_id",
        "lineup_participation_id",
        "pbp_event_id",
        "player_game_box_score_id",
        "player_game_stat_claim_id",
        "team_game_stat_claim_id",
        "source_document_note_id",
        "identity_candidate_id",
        "scoring_event_id",
        "decision_id",
    ]:
        value = clean(row.get(fallback))
        if value:
            return value
    return compact_json(row)


def source_document_count(row: dict[str, Any]) -> int:
    docs = parse_json_list(row.get("source_documents_json"))
    return len({clean(doc) for doc in docs if clean(doc)})


def confidence_score(row: dict[str, Any]) -> str:
    return clean(row.get("max_confidence_score")) or clean(row.get("confidence_score"))


def evidence_text(row: dict[str, Any]) -> str:
    for field in ["evidence_text", "source_row_text", "play_text", "note_text", "stat_context"]:
        text = clean(row.get(field)).strip()
        if text:
            return text[:280]
    return ""


def base_readiness_row(table: str, row: dict[str, Any], latest_entity_row: bool) -> dict[str, Any]:
    return {
        "target_table": table,
        "target_entity_key": entity_key(table, row),
        "promotion_apply_run_id": clean(row.get("promotion_apply_run_id")),
        "decision_id": clean(row.get("decision_id")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "player_week": clean(row.get("player_week")),
        "NFL_player_id": clean(row.get("NFL_player_id")),
        "nfl_team": clean(row.get("nfl_team")),
        "opponent_nfl_team": clean(row.get("opponent_nfl_team")),
        "stat_name": clean(row.get("stat_name")),
        "stat_value": clean(row.get("stat_value")),
        "event_type": clean(row.get("event_type")),
        "play_type": clean(row.get("play_type")),
        "confidence_bar": clean(row.get("confidence_bar")).lower() or "unknown",
        "confidence_score": confidence_score(row),
        "source_document_count": source_document_count(row),
        "latest_entity_row": "1" if latest_entity_row else "0",
        "evidence_text": evidence_text(row),
        "source_documents_json": clean(row.get("source_documents_json")),
    }


def classify_game_candidate(
    row: dict[str, Any],
    dated_scores: dict[tuple[str, str, str, str], list[dict[str, Any]]],
    undated_scores: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> tuple[str, str, str, str]:
    boxscore_id = clean(row.get("boxscore_id"))
    game_date = clean(row.get("game_date")) or date_from_boxscore_id(boxscore_id)
    year = normalize_int(row.get("year")) or year_from_boxscore_id(boxscore_id)
    team_1 = clean(row.get("team_1_resolved")) or clean(row.get("team_1_nfl_team"))
    team_2 = clean(row.get("team_2_resolved")) or clean(row.get("team_2_nfl_team"))
    score_1 = normalize_int(row.get("team_1_score"))
    score_2 = normalize_int(row.get("team_2_score"))
    if not score_1 or not score_2:
        return "followup_needed", "score_missing_values", "newspaper_score_missing", "score_missing"
    if not team_1 or not team_2:
        return "semantic_resolution_needed", "score_needs_team_resolution", "not_compared", "team_resolution_needed"
    key = (game_date, *sorted([team_1, team_2]))
    v26_rows = dated_scores.get(key, [])
    v26_status = "found_dated" if v26_rows else ""
    if not v26_rows and year:
        v26_rows = undated_scores.get((year, *sorted([team_1, team_2])), [])
        v26_status = "found_undated" if v26_rows else ""
    if not v26_rows:
        return "promotion_review", "score_v26_missing_or_unmatched", "missing_game", "game_score_missing_in_v26_or_mapping"

    newspaper_pair = tuple(sorted([score_1, score_2]))
    v26_pairs = {
        tuple(sorted([normalize_int(item.get("team_score")), normalize_int(item.get("opponent_score"))]))
        for item in v26_rows
    }
    if newspaper_pair in v26_pairs:
        if v26_status == "found_undated":
            return "promotion_review", "score_adds_missing_game_date", "undated_exact_score_match", "date_additive_score_corrob"
        return "corroboration_only", "score_corroborates_v26", "exact_score_match", "score_corrob"
    return "conflict_review", "score_conflict_or_mapping_issue", "score_conflict_or_team_mapping_needed", "score_conflict"


def compare_stat_value(
    row: dict[str, Any],
    field: str,
    v26_field: str,
    v26_row: dict[str, Any] | None,
) -> tuple[str, str]:
    promoted_num = parse_num(row.get(field))
    if promoted_num is None:
        return "", ""
    if not v26_field:
        return "no_comparable_v26_column", ""
    if not v26_row:
        return "no_v26_player_week_row", ""
    v26_value = clean(v26_row.get(v26_field))
    v26_num = parse_num(v26_value)
    if v26_num is None:
        return "v26_blank_or_non_numeric", v26_value
    if abs(v26_num - promoted_num) < 0.00001:
        return "same_as_v26", v26_value
    return "differs_from_v26", v26_value


def classify_stat_status(statuses: list[str]) -> tuple[str, str, str]:
    if not statuses:
        return "context_only", "no_structured_stat_values", "not_compared"
    if any(status == "differs_from_v26" for status in statuses):
        return "conflict_review", "stat_differs_from_v26", "differs_from_v26"
    if any(status in {"v26_blank_or_non_numeric", "no_v26_player_week_row", "no_comparable_v26_column"} for status in statuses):
        return "promotion_review", "stat_additive_or_schema_gap", ",".join(sorted(set(statuses)))
    if all(status == "same_as_v26" for status in statuses):
        return "corroboration_only", "stat_corroborates_v26", "same_as_v26"
    return "quality_review", "stat_review_needed", ",".join(sorted(set(statuses)))


def classify_player_box_score(
    table: str,
    row: dict[str, Any],
    v26_by_week: dict[str, dict[str, Any]],
    stat_mapping: dict[str, str],
) -> tuple[str, str, str, str, list[dict[str, Any]]]:
    player_week = clean(row.get("player_week"))
    nfl_id = clean(row.get("NFL_player_id"))
    stat_details: list[dict[str, Any]] = []
    statuses: list[str] = []
    if not player_week or not nfl_id:
        identity = "missing_player_week_or_nfl_id"
    else:
        identity = "resolved"
    v26_row = v26_by_week.get(player_week) if player_week else None
    for field in PLAYER_STAT_MAPPINGS:
        if parse_num(row.get(field)) is None:
            continue
        v26_field = stat_mapping.get(field, "")
        status, v26_value = compare_stat_value(row, field, v26_field, v26_row)
        statuses.append(status)
        stat_details.append({
            "target_table": table,
            "target_entity_key": entity_key(table, row),
            "boxscore_id": clean(row.get("boxscore_id")),
            "player_week": player_week,
            "NFL_player_id": nfl_id,
            "promoted_field": field,
            "promoted_value": clean(row.get(field)),
            "v26_field": v26_field,
            "v26_value": v26_value,
            "v26_status": status,
            "source_text": evidence_text(row),
        })
    if identity != "resolved" and statuses:
        return "identity_resolution_needed", "stat_has_values_but_identity_unresolved", ",".join(sorted(set(statuses))), identity, stat_details
    lane, value_class, v26_status = classify_stat_status(statuses)
    return lane, value_class, v26_status, identity, stat_details


def stat_claim_candidate_fields(row: dict[str, Any]) -> dict[str, str]:
    fields = {clean(key): clean(value) for key, value in parse_json_obj(row.get("stat_fields_json")).items()}
    stat_name = clean(row.get("stat_name")).strip().lower()
    stat_value = clean(row.get("stat_value"))
    if stat_name:
        fields.setdefault(stat_name, stat_value)
    return fields


def mapped_claim_fields(row: dict[str, Any], stat_mapping: dict[str, str]) -> dict[str, tuple[str, str]]:
    out: dict[str, tuple[str, str]] = {}
    for raw_name, raw_value in stat_claim_candidate_fields(row).items():
        key = raw_name.lower().strip()
        if key in stat_mapping:
            out[key] = (key, raw_value)
            continue
        for alias in STAT_CLAIM_FIELD_ALIASES.get(key, []):
            if alias in stat_mapping:
                out[alias] = (key, raw_value)
    return out


def classify_player_stat_claim(
    row: dict[str, Any],
    v26_by_week: dict[str, dict[str, Any]],
    stat_mapping: dict[str, str],
) -> tuple[str, str, str, str, list[dict[str, Any]]]:
    player_week = clean(row.get("player_week"))
    nfl_id = clean(row.get("NFL_player_id"))
    identity = "resolved" if player_week and nfl_id else clean(row.get("identity_status")) or "missing_player_week_or_nfl_id"
    mapped = mapped_claim_fields(row, stat_mapping)
    stat_details: list[dict[str, Any]] = []
    statuses: list[str] = []
    v26_row = v26_by_week.get(player_week) if player_week else None
    for promoted_field, (source_field, source_value) in mapped.items():
        v26_field = stat_mapping.get(promoted_field, "")
        status, v26_value = compare_stat_value({promoted_field: source_value}, promoted_field, v26_field, v26_row)
        statuses.append(status)
        stat_details.append({
            "target_table": "player_game_stat_claim",
            "target_entity_key": entity_key("player_game_stat_claim", row),
            "boxscore_id": clean(row.get("boxscore_id")),
            "player_week": player_week,
            "NFL_player_id": nfl_id,
            "promoted_field": source_field,
            "promoted_value": source_value,
            "v26_field": v26_field,
            "v26_value": v26_value,
            "v26_status": status,
            "source_text": evidence_text(row),
        })
    if identity != "resolved" and (mapped or clean(row.get("stat_value"))):
        return "identity_resolution_needed", "player_stat_claim_identity_unresolved", ",".join(sorted(set(statuses))) or "not_compared", identity, stat_details
    if statuses:
        lane, value_class, v26_status = classify_stat_status(statuses)
        return lane, value_class, v26_status, identity, stat_details
    stat_name = clean(row.get("stat_name")).lower()
    if "touchdown" in stat_name or stat_name in {"td", "tds"}:
        return "semantic_resolution_needed", "touchdown_type_split_needed", "no_direct_v26_column", identity, stat_details
    return "promotion_review", "schema_gap_stat_claim", "no_comparable_v26_column", identity, stat_details


def classify_lineup(
    row: dict[str, Any],
    v26_by_week: dict[str, dict[str, Any]],
) -> tuple[str, str, str, str]:
    player_week = clean(row.get("player_week"))
    nfl_id = clean(row.get("NFL_player_id"))
    if not player_week or not nfl_id:
        return "identity_resolution_needed", "lineup_identity_unresolved", "not_compared", "missing_player_week_or_nfl_id"
    v26_row = v26_by_week.get(player_week)
    if not v26_row:
        return "promotion_review", "lineup_missing_v26_player_week", "no_v26_player_week_row", "resolved"
    starter_position = clean(row.get("starter_position")).upper()
    is_starter = clean(row.get("is_starter"))
    v26_position = clean(v26_row.get("starter_position")).upper()
    v26_starter = clean(v26_row.get("is_starter"))
    if not starter_position and not is_starter:
        return "context_only", "lineup_no_starter_claim", "not_compared", "resolved"
    if not v26_position and not v26_starter:
        return "promotion_review", "lineup_fills_blank_v26_starter", "v26_starter_blank", "resolved"
    if (starter_position and starter_position == v26_position) or (is_starter and is_starter == v26_starter):
        return "corroboration_only", "lineup_corroborates_v26", "same_as_v26", "resolved"
    return "conflict_review", "lineup_differs_from_v26", "differs_from_v26", "resolved"


def classify_scoring_event(row: dict[str, Any]) -> tuple[str, str, str, str]:
    event_type = clean(row.get("event_type"))
    points = clean(row.get("points"))
    raw_players = [
        clean(row.get("scoring_player_raw")),
        clean(row.get("passer_raw")),
        clean(row.get("receiver_raw")),
    ]
    resolved_players = [
        clean(row.get("scoring_NFL_player_id")),
        clean(row.get("passer_NFL_player_id")),
        clean(row.get("receiver_NFL_player_id")),
    ]
    if not event_type:
        return "semantic_resolution_needed", "scoring_event_missing_event_type", "not_compared", "semantic_missing"
    if any(raw_players) and not any(resolved_players):
        return "identity_resolution_needed", "scoring_event_player_identity_unresolved", "not_compared", "missing_player_ids"
    if not clean(row.get("scoring_team")) and clean(row.get("scoring_team_raw")):
        return "semantic_resolution_needed", "scoring_event_team_unresolved", "not_compared", "team_resolution_needed"
    if points:
        return "promotion_review", "additive_scoring_event_detail", "not_in_v26_event_model", "resolved_or_team_only"
    return "quality_review", "scoring_event_missing_points", "not_compared", "resolved_or_team_only"


def classify_pbp_event(row: dict[str, Any]) -> tuple[str, str, str, str]:
    play_text = clean(row.get("play_text"))
    play_type = clean(row.get("play_type"))
    primary_raw = clean(row.get("primary_player_raw"))
    primary_id = clean(row.get("primary_NFL_player_id"))
    if not play_text:
        return "semantic_resolution_needed", "pbp_missing_play_text", "not_compared", "semantic_missing"
    if primary_raw and not primary_id:
        return "identity_resolution_needed", "pbp_primary_identity_unresolved", "not_compared", "missing_player_ids"
    if not play_type:
        return "promotion_review", "additive_notable_play_context", "not_in_v26_pbp_model", "partial_structure"
    return "promotion_review", "additive_play_by_play_event", "not_in_v26_pbp_model", "resolved_or_team_only"


def classify_team_stat(row: dict[str, Any]) -> tuple[str, str, str, str]:
    status = clean(row.get("reconciliation_status")).lower()
    if "conflict" in status:
        return "conflict_review", "team_stat_score_or_mapping_conflict", "not_compared", "team_stat_conflict"
    if not clean(row.get("team_1_nfl_team")) or not clean(row.get("team_2_nfl_team")):
        return "semantic_resolution_needed", "team_stat_team_resolution_needed", "not_compared", "team_resolution_needed"
    if not clean(row.get("stat_name")):
        return "semantic_resolution_needed", "team_stat_missing_stat_name", "not_compared", "semantic_missing"
    return "promotion_review", "additive_team_game_stat", "not_in_v26_team_game_stat_model", "resolved"


def classify_identity_candidate(row: dict[str, Any]) -> tuple[str, str, str, str]:
    if clean(row.get("NFL_player_id")) and clean(row.get("player_week")):
        return "promotion_review", "identity_bridge_candidate", "not_compared", "resolved"
    return "identity_resolution_needed", "identity_candidate_unresolved", "not_compared", "identity_unresolved"


def classify_note(table: str, row: dict[str, Any]) -> tuple[str, str, str, str]:
    status = clean(row.get("reconciliation_status")).lower()
    note_type = clean(row.get("note_type")).lower()
    if any(token in status for token in ["ambiguous", "unresolved", "held"]):
        return "identity_resolution_needed", "held_context_identity_or_team_unresolved", "not_compared", status or note_type
    if "conflict" in status or "conflict" in note_type:
        return "conflict_review", "context_conflict_evidence", "not_compared", status or note_type
    if table == "player_game_note":
        return "context_only", "player_game_note_context", "not_compared", status or note_type
    return "context_only", "source_document_note_context", "not_compared", status or note_type


def mark_latest_entity_rows(rows_by_table: dict[str, list[dict[str, Any]]]) -> set[tuple[str, str, int]]:
    latest: dict[tuple[str, str], tuple[tuple[str, str, str], int]] = {}
    for table, rows in rows_by_table.items():
        for index, row in enumerate(rows):
            key = entity_key(table, row)
            sort_key = (
                clean(row.get("created_at_utc")),
                clean(row.get("promotion_apply_run_id")),
                clean(row.get("decision_id")) or f"{index:08d}",
            )
            item_key = (table, key)
            if item_key not in latest or sort_key > latest[item_key][0]:
                latest[item_key] = (sort_key, index)
    return {(table, key, index) for (table, key), (_, index) in latest.items()}


def classify_all(
    rows_by_table: dict[str, list[dict[str, Any]]],
    v26_by_week: dict[str, dict[str, Any]],
    stat_mapping: dict[str, str],
    dated_scores: dict[tuple[str, str, str, str], list[dict[str, Any]]],
    undated_scores: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    latest_keys = mark_latest_entity_rows(rows_by_table)
    readiness_rows: list[dict[str, Any]] = []
    stat_detail_rows: list[dict[str, Any]] = []
    for table, rows in rows_by_table.items():
        for index, row in enumerate(rows):
            key = entity_key(table, row)
            latest = (table, key, index) in latest_keys
            out = base_readiness_row(table, row, latest)
            details: list[dict[str, Any]] = []
            if table == "game_candidate":
                lane, value_class, v26_status, identity_status = classify_game_candidate(row, dated_scores, undated_scores)
            elif table == "player_game_box_score":
                lane, value_class, v26_status, identity_status, details = classify_player_box_score(table, row, v26_by_week, stat_mapping)
            elif table == "player_game_stat_claim":
                lane, value_class, v26_status, identity_status, details = classify_player_stat_claim(row, v26_by_week, stat_mapping)
            elif table == "lineup_participation":
                lane, value_class, v26_status, identity_status = classify_lineup(row, v26_by_week)
            elif table == "scoring_event":
                lane, value_class, v26_status, identity_status = classify_scoring_event(row)
            elif table == "play_by_play_event":
                lane, value_class, v26_status, identity_status = classify_pbp_event(row)
            elif table == "team_game_stat_claim":
                lane, value_class, v26_status, identity_status = classify_team_stat(row)
            elif table == "player_identity_candidate":
                lane, value_class, v26_status, identity_status = classify_identity_candidate(row)
            elif table in {"source_document_note", "player_game_note"}:
                lane, value_class, v26_status, identity_status = classify_note(table, row)
            else:
                lane, value_class, v26_status, identity_status = "quality_review", "unclassified_table", "not_compared", "unknown"
            out.update({
                "readiness_lane": lane,
                "value_class": value_class,
                "v26_status": v26_status,
                "identity_status": identity_status,
            })
            for detail in details:
                detail["readiness_lane"] = lane
            readiness_rows.append(out)
            stat_detail_rows.extend(details)
    return readiness_rows, stat_detail_rows


def rollup_rows(readiness_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in readiness_rows:
        grouped[(row["target_table"], row["readiness_lane"], row["value_class"])].append(row)
    out: list[dict[str, Any]] = []
    for (table, lane, value_class), group in sorted(grouped.items()):
        bars = Counter(clean(row.get("confidence_bar")).lower() or "unknown" for row in group)
        out.append({
            "target_table": table,
            "readiness_lane": lane,
            "value_class": value_class,
            "raw_row_count": len(group),
            "latest_entity_count": sum(1 for row in group if clean(row.get("latest_entity_row")) == "1"),
            "distinct_entity_count": len({row["target_entity_key"] for row in group}),
            "distinct_boxscore_count": len({row["boxscore_id"] for row in group if row["boxscore_id"]}),
            "high_count": bars["high"],
            "medium_count": bars["medium"],
            "low_count": bars["low"],
            "unknown_count": bars["unknown"] + bars[""],
        })
    return out


def sample_rows(readiness_rows: list[dict[str, Any]], limit_per_lane: int = 12) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    counts: Counter[tuple[str, str]] = Counter()
    for row in readiness_rows:
        if clean(row.get("latest_entity_row")) != "1":
            continue
        key = (row["readiness_lane"], row["target_table"])
        if counts[key] >= limit_per_lane:
            continue
        if row["readiness_lane"] in {"promotion_review", "conflict_review", "identity_resolution_needed", "semantic_resolution_needed"}:
            samples.append(row)
            counts[key] += 1
    return samples


def render_markdown(summary: dict[str, Any], rollups: list[dict[str, Any]], samples: list[dict[str, Any]]) -> str:
    lane_counts = summary["readiness_lane_latest_entity_counts"]
    lines = [
        "# Newspaper v26 Readiness Report",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Newspaper DB: `{summary['newspaper_db']}`",
        f"- v26 parquet: `{summary['v26_path']}`",
        f"- Identity overlay run: `{summary['identity_resolution_run_id'] or 'none'}`",
        f"- Identity overlay applied fields: `{summary['identity_overlay_applied_field_count']}`",
        f"- Raw promoted rows classified: `{summary['raw_promoted_rows']}`",
        f"- Distinct promoted entities: `{summary['distinct_promoted_entities']}`",
        f"- Latest entity rows classified: `{summary['latest_entity_rows']}`",
        "",
        "## Readiness Lanes",
        "",
    ]
    for lane, count in sorted(lane_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- `{lane}`: {count}")
    lines.extend([
        "",
        "## Table Rollup",
        "",
        "| Table | Lane | Class | Raw | Latest | Games | Confidence |",
        "|---|---|---|---:|---:|---:|---|",
    ])
    for row in rollups:
        confidence = (
            f"H {row['high_count']}, M {row['medium_count']}, "
            f"L {row['low_count']}, U {row['unknown_count']}"
        )
        lines.append(
            f"| `{row['target_table']}` | `{row['readiness_lane']}` | `{row['value_class']}` | "
            f"{row['raw_row_count']} | {row['latest_entity_count']} | "
            f"{row['distinct_boxscore_count']} | {confidence} |"
        )
    lines.extend([
        "",
        "## Action Samples",
        "",
        "| Lane | Table | Boxscore | Entity | Class | Evidence |",
        "|---|---|---|---|---|---|",
    ])
    for row in samples[:60]:
        evidence = clean(row.get("evidence_text")).replace("|", "/")[:140]
        entity = clean(row.get("target_entity_key")).replace("|", "/")[:90]
        lines.append(
            f"| `{row['readiness_lane']}` | `{row['target_table']}` | `{row['boxscore_id']}` | "
            f"`{entity}` | `{row['value_class']}` | {evidence} |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Readiness rows: `{summary['readiness_csv']}`",
        f"- Latest readiness rows: `{summary['latest_readiness_csv']}`",
        f"- Promotion-review queue: `{summary['promotion_review_queue_csv']}`",
        f"- Identity-resolution queue: `{summary['identity_resolution_queue_csv']}`",
        f"- Conflict-review queue: `{summary['conflict_review_queue_csv']}`",
        f"- Semantic-resolution queue: `{summary['semantic_resolution_queue_csv']}`",
        f"- Stat details: `{summary['stat_detail_csv']}`",
        f"- Rollup CSV: `{summary['rollup_csv']}`",
        f"- Samples CSV: `{summary['samples_csv']}`",
        f"- Identity overlay receipts: `{summary['identity_overlay_receipts_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--newspaper-db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--release-root", type=Path, default=DEFAULT_RELEASE_ROOT)
    parser.add_argument("--v26-path", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_v26_readiness")
    parser.add_argument("--identity-resolution-run-id", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    v26_path = args.v26_path or latest_v26(args.release_root)

    newspaper_con = duckdb.connect(str(args.newspaper_db), read_only=True)
    try:
        rows_by_table = {table: fetch_promoted_rows(newspaper_con, table) for table in PROMOTED_TABLES}
        identity_patches = load_identity_auto_patches(newspaper_con, args.identity_resolution_run_id)
    finally:
        newspaper_con.close()
    identity_overlay_receipts = apply_identity_overlay(rows_by_table, identity_patches, created_at)

    player_weeks = [
        clean(row.get("player_week"))
        for rows in rows_by_table.values()
        for row in rows
        if clean(row.get("player_week"))
    ]

    v26_con = duckdb.connect()
    try:
        cols = set(v26_columns(v26_con, v26_path))
        stat_mapping = choose_stat_mappings(cols)
        v26_by_week = load_v26_player_rows(v26_con, v26_path, cols, player_weeks, stat_mapping)
        v26_game_rows = load_v26_game_rows(v26_con, v26_path, cols)
    finally:
        v26_con.close()

    dated_scores, undated_scores = build_game_score_index(v26_game_rows)
    readiness, stat_details = classify_all(rows_by_table, v26_by_week, stat_mapping, dated_scores, undated_scores)
    rollups = rollup_rows(readiness)
    samples = sample_rows(readiness)

    readiness_csv = out_dir / "atom_readiness_rows.csv"
    latest_readiness_csv = out_dir / "atom_readiness_latest_entities.csv"
    stat_detail_csv = out_dir / "stat_detail_comparison.csv"
    rollup_csv = out_dir / "table_readiness_rollup.csv"
    samples_csv = out_dir / "action_samples.csv"
    identity_overlay_receipts_csv = out_dir / "identity_overlay_receipts.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "v26_readiness_report.md"

    latest_rows = [row for row in readiness if clean(row.get("latest_entity_row")) == "1"]
    lane_files = {
        "promotion_review": out_dir / "queue_promotion_review.csv",
        "identity_resolution_needed": out_dir / "queue_identity_resolution_needed.csv",
        "conflict_review": out_dir / "queue_conflict_review.csv",
        "semantic_resolution_needed": out_dir / "queue_semantic_resolution_needed.csv",
        "corroboration_only": out_dir / "archive_corroboration_only.csv",
        "context_only": out_dir / "archive_context_only.csv",
    }
    lane_counts = Counter(row["readiness_lane"] for row in latest_rows)
    table_counts = Counter(row["target_table"] for row in latest_rows)
    value_counts = Counter(row["value_class"] for row in latest_rows)
    table_raw_counts = {table: len(rows) for table, rows in rows_by_table.items()}
    identity_receipt_counts = Counter(row["applied_status"] for row in identity_overlay_receipts)

    summary = {
        "created_at_utc": created_at,
        "run_id": run_id,
        "output_dir": str(out_dir),
        "newspaper_db": str(args.newspaper_db),
        "v26_path": str(v26_path),
        "identity_resolution_run_id": args.identity_resolution_run_id,
        "identity_overlay_receipt_counts": dict(identity_receipt_counts),
        "identity_overlay_applied_field_count": identity_receipt_counts.get("applied", 0),
        "identity_overlay_task_count": len({
            clean(row.get("task_id"))
            for row in identity_overlay_receipts
            if clean(row.get("task_id"))
        }),
        "identity_overlay_target_count": len({
            (clean(row.get("target_table")), clean(row.get("target_entity_key")))
            for row in identity_overlay_receipts
            if clean(row.get("target_entity_key"))
        }),
        "v26_stat_mapping": stat_mapping,
        "raw_promoted_rows": len(readiness),
        "distinct_promoted_entities": len({(row["target_table"], row["target_entity_key"]) for row in readiness}),
        "latest_entity_rows": len(latest_rows),
        "raw_table_counts": table_raw_counts,
        "latest_table_counts": dict(table_counts),
        "readiness_lane_latest_entity_counts": dict(lane_counts),
        "value_class_latest_entity_counts": dict(value_counts),
        "stat_detail_rows": len(stat_details),
        "latest_readiness_csv": str(latest_readiness_csv),
        "promotion_review_queue_csv": str(lane_files["promotion_review"]),
        "identity_resolution_queue_csv": str(lane_files["identity_resolution_needed"]),
        "conflict_review_queue_csv": str(lane_files["conflict_review"]),
        "semantic_resolution_queue_csv": str(lane_files["semantic_resolution_needed"]),
        "corroboration_archive_csv": str(lane_files["corroboration_only"]),
        "context_archive_csv": str(lane_files["context_only"]),
        "readiness_csv": str(readiness_csv),
        "stat_detail_csv": str(stat_detail_csv),
        "rollup_csv": str(rollup_csv),
        "samples_csv": str(samples_csv),
        "identity_overlay_receipts_csv": str(identity_overlay_receipts_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown),
    }

    write_csv(readiness_csv, readiness, ROW_FIELDS)
    write_csv(latest_readiness_csv, latest_rows, ROW_FIELDS)
    for lane, path in lane_files.items():
        write_csv(path, [row for row in latest_rows if row["readiness_lane"] == lane], ROW_FIELDS)
    write_csv(stat_detail_csv, stat_details, STAT_DETAIL_FIELDS)
    write_csv(rollup_csv, rollups, ROLLUP_FIELDS)
    write_csv(samples_csv, samples, ROW_FIELDS)
    write_csv(identity_overlay_receipts_csv, identity_overlay_receipts, IDENTITY_PATCH_RECEIPT_FIELDS)
    summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=True, sort_keys=True) + "\n", encoding="utf-8")
    markdown.write_text(render_markdown(summary, rollups, samples), encoding="utf-8")

    print(json.dumps({
        "run_id": run_id,
        "output_dir": str(out_dir),
        "raw_promoted_rows": len(readiness),
        "latest_entity_rows": len(latest_rows),
        "readiness_lane_latest_entity_counts": dict(lane_counts),
        "stat_detail_rows": len(stat_details),
        "identity_overlay_receipt_counts": dict(identity_receipt_counts),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
