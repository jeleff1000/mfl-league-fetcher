#!/usr/bin/env python
"""Resolve team-level newspaper stat claims into local atom decisions.

This station captures paired team-game statistics, such as "Rock Island
outgained Decatur 381 to 244 yards", that do not belong in player box-score
tables. It writes only D-drive artifacts and local DuckDB review tables.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "team_game_stat_resolution_preps"
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
    "team_game_stat_resolution_run_id",
    "promotion_apply_run_id",
    "decision_id",
    "lane",
    "route_to_lane",
    "source_document_id",
    "promotion_package_id",
    "boxscore_id",
    "game_date",
    "year",
    "week",
    "stat_name",
    "stat_unit",
    "team_1_raw",
    "team_1_nfl_team",
    "team_1_value",
    "team_1_opponent_nfl_team",
    "team_2_raw",
    "team_2_nfl_team",
    "team_2_value",
    "team_2_opponent_nfl_team",
    "claimed_score_text",
    "reconciliation_status",
    "match_method",
    "reason",
    "source_row_text",
    "evidence_text",
    "proposed_fields_json",
    "enriched_proposed_fields_json",
    "source_documents_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "team_game_stat_resolution_run_id",
    "promotion_apply_run_id",
    "output_dir",
    "candidate_decision_count",
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
    "canton": "CAN",
    "canton bulldogs": "CAN",
    "chicago cardinals": "CRD",
    "chicago tigers": "CHT",
    "decatur": "CHI",
    "decatur staleys": "CHI",
    "staleys": "CHI",
    "rock island": "RII",
    "rock island independents": "RII",
    "independents": "RII",
}

TEAM_NAME_PATTERNS = [
    ("Rock Island Independents", "RII", r"\brock island(?: independents)?\b"),
    ("Decatur Staleys", "CHI", r"\b(?:decatur(?: staleys)?|staleys)\b"),
    ("Akron Pros", "AKR", r"\bakron(?: pros| indians)?\b"),
    ("Canton Bulldogs", "CAN", r"\bcanton(?: bulldogs)?\b"),
    ("Chicago Cardinals", "CRD", r"\bchicago cardinals\b"),
    ("Chicago Tigers", "CHT", r"\bchicago tigers\b"),
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


def normalize_confidence(value: Any, fallback: str = "0.70") -> str:
    text = clean(value).strip()
    if not text:
        return fallback
    try:
        number = float(text)
    except ValueError:
        return fallback
    if number > 1 and number <= 100:
        number = number / 100.0
    if number < 0 or number > 1:
        return fallback
    return f"{number:.3f}".rstrip("0").rstrip(".")


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
        root / "team_game_stat_resolution_preps" / "*" / "decision_input.csv",
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
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND (
            route_to_lane = 'semantic_followup'
            OR (route_to_lane = 'pending_decision' AND target_table = 'game_candidate')
          )
        ORDER BY route_to_lane, source_document_id, decision_id
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
                   team_code, opponent_code, team_points, opponent_points
            FROM read_parquet(?)
            """,
            [str(team_games_path)],
        )
    finally:
        con.close()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        out[f"{clean(row.get('boxscore_id'))}|{clean(row.get('team_code'))}"] = row
    return out


def load_team_stat_claims(con: duckdb.DuckDBPyConnection, source_document_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not source_document_ids:
        return {}
    placeholders = ",".join(["?"] * len(source_document_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_review_claim
        WHERE source_document_id IN ({placeholders})
          AND target_table = 'game_candidate'
          AND LOWER(COALESCE(unit, '')) IN ('team_total_yards', 'team_yards')
        ORDER BY source_document_id, confidence_score DESC, claim_index
        """,
        source_document_ids,
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(clean(row.get("source_document_id")), []).append(row)
    return grouped


def team_code(raw_team: Any) -> str:
    return TEAM_ALIASES.get(norm_text(raw_team), "")


def team_game_info(team_games: dict[str, dict[str, Any]], boxscore_id: str, code: str) -> dict[str, Any]:
    return team_games.get(f"{boxscore_id}|{code}", {})


def parse_team_yards_from_text(text: Any) -> dict[str, dict[str, str]]:
    value = clean(text)
    if not value:
        return {}
    out: dict[str, dict[str, str]] = {}
    for raw_name, code, pattern in TEAM_NAME_PATTERNS:
        match = re.search(pattern + r"\s+(\d+)\s+(?:total\s+)?yards?", value, flags=re.IGNORECASE)
        if match:
            out[code] = {"team_raw": raw_name, "nfl_team": code, "value": match.group(1)}
    return out


def parse_team_yards_claim(claim: dict[str, Any]) -> dict[str, dict[str, str]]:
    parsed = parse_team_yards_from_text(claim.get("normalized_value"))
    if len(parsed) >= 2:
        return parsed
    parsed = parse_team_yards_from_text(claim.get("raw_value"))
    if len(parsed) >= 2:
        return parsed
    return {}


def parse_pending_candidate(proposed: dict[str, Any]) -> dict[str, dict[str, str]]:
    pairs: dict[str, dict[str, str]] = {}
    for side in ["1", "2"]:
        raw_team = clean(proposed.get(f"team_{side}_raw"))
        value = clean(proposed.get(f"team_{side}_total_yards"))
        code = team_code(raw_team)
        if code and value:
            pairs[code] = {"team_raw": raw_team, "nfl_team": code, "value": value}
    return pairs


def score_conflict_status(proposed: dict[str, Any], team_games: dict[str, dict[str, Any]], boxscore_id: str) -> str:
    statuses = []
    for side in ["1", "2"]:
        code = team_code(proposed.get(f"team_{side}_raw"))
        claimed = parse_int(proposed.get(f"team_{side}_score"))
        game = team_game_info(team_games, boxscore_id, code) if code else {}
        pfr_points = parse_int(game.get("team_points"))
        if code and claimed is not None and pfr_points is not None and claimed != pfr_points:
            statuses.append(f"{code}:{claimed}!={pfr_points}")
    if statuses:
        return "score_conflicts_with_pfr_team_games:" + ",".join(statuses)
    if proposed:
        return "source_claim_reconciles_to_boxscore_or_score_unchecked"
    return "source_claim_from_semantic_followup"


def build_claim_payload(
    route_row: dict[str, Any],
    pairs: dict[str, dict[str, str]],
    team_games: dict[str, dict[str, Any]],
    claim: dict[str, Any] | None = None,
) -> dict[str, Any]:
    boxscore_id = clean(route_row.get("boxscore_id"))
    proposed = parse_json_obj(route_row.get("proposed_fields_json"))
    ordered = sorted(pairs.values(), key=lambda row: clean(row.get("nfl_team")))
    if len(ordered) < 2:
        return {}
    first, second = ordered[0], ordered[1]
    game1 = team_game_info(team_games, boxscore_id, clean(first.get("nfl_team")))
    game2 = team_game_info(team_games, boxscore_id, clean(second.get("nfl_team")))
    year = clean(parse_int(game1.get("year")) or parse_int(game2.get("year")) or "")
    week = clean(parse_int(game1.get("week")) or parse_int(game2.get("week")) or "")
    game_date = clean(game1.get("game_date")) or clean(game2.get("game_date"))
    evidence_text = clean((claim or {}).get("evidence_quote")) or clean(proposed.get("evidence_text"))
    source_row_text = clean((claim or {}).get("raw_value")) or clean(proposed.get("evidence_text"))
    claimed_score = ""
    if clean(proposed.get("team_1_score")) or clean(proposed.get("team_2_score")):
        claimed_score = (
            f"{clean(proposed.get('team_1_raw'))} {clean(proposed.get('team_1_score'))}, "
            f"{clean(proposed.get('team_2_raw'))} {clean(proposed.get('team_2_score'))}"
        )
    reconciliation_status = score_conflict_status(proposed, team_games, boxscore_id)
    return {
        "team_game_stat_claim_id": stable_id(
            "team_game_stat_claim",
            route_row.get("decision_id"),
            boxscore_id,
            first.get("nfl_team"),
            first.get("value"),
            second.get("nfl_team"),
            second.get("value"),
        ),
        "boxscore_id": boxscore_id,
        "game_date": game_date,
        "year": year,
        "week": week,
        "stat_name": "total_yards",
        "stat_unit": "yards",
        "team_1_raw": clean(first.get("team_raw")),
        "team_1_nfl_team": clean(first.get("nfl_team")),
        "team_1_value": clean(first.get("value")),
        "team_1_opponent_nfl_team": clean(game1.get("opponent_code")),
        "team_2_raw": clean(second.get("team_raw")),
        "team_2_nfl_team": clean(second.get("nfl_team")),
        "team_2_value": clean(second.get("value")),
        "team_2_opponent_nfl_team": clean(game2.get("opponent_code")),
        "claimed_score_text": claimed_score,
        "reconciliation_status": reconciliation_status,
        "stat_context": clean((claim or {}).get("reason")) or "team-game stat preserved from semantic follow-up",
        "source_row_text": source_row_text,
        "evidence_text": evidence_text,
        "confidence_score": normalize_confidence((claim or {}).get("confidence_score")),
        "review_status": "local_atom_team_game_stat_accepted",
        "promotion_status": "local_atom_only",
        "source_document_id": clean(route_row.get("source_document_id")),
        "region_id": clean((claim or {}).get("evidence_region_id")),
        "match_method": "paired_team_yards_semantic_claim",
    }


def build_resolution_rows(
    run_id: str,
    promotion_apply_run_id: str,
    route_rows: list[dict[str, Any]],
    claims_by_doc: dict[str, list[dict[str, Any]]],
    team_games: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    created_at = iso_now()
    item_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    for row in route_rows:
        decision_id = clean(row.get("decision_id"))
        proposed = parse_json_obj(row.get("proposed_fields_json"))
        candidate_pairs = parse_pending_candidate(proposed)
        candidate_claim: dict[str, Any] | None = None
        if not candidate_pairs:
            for claim in claims_by_doc.get(clean(row.get("source_document_id")), []):
                candidate_pairs = parse_team_yards_claim(claim)
                if candidate_pairs:
                    candidate_claim = claim
                    break
        if len(candidate_pairs) < 2:
            continue
        payload = build_claim_payload(row, candidate_pairs, team_games, candidate_claim)
        if not payload:
            continue
        key = (
            clean(payload.get("boxscore_id")),
            clean(payload.get("team_1_nfl_team")) + ":" + clean(payload.get("team_1_value")),
            clean(payload.get("team_2_nfl_team")) + ":" + clean(payload.get("team_2_value")),
        )
        if key in seen:
            continue
        seen.add(key)
        target_key = (
            f"team_game_stat_claim|{payload['boxscore_id']}|{payload['stat_name']}|"
            f"{payload['team_1_nfl_team']}:{payload['team_1_value']}|{payload['team_2_nfl_team']}:{payload['team_2_value']}"
        )
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        reason = "team-game stat claim preserved in dedicated local atom table"
        decision_rows.append({
            "decision_id": decision_id,
            "decision_status": "approved_for_local_promotion",
            "decision_value": PROMOTION_VALUE,
            "route_to_lane": "",
            "resolved_boxscore_id": clean(payload.get("boxscore_id")),
            "resolved_target_table": "team_game_stat_claim",
            "resolved_target_entity_key": target_key,
            "proposed_fields_json": payload_json,
            "notes": reason,
        })
        item_rows.append({
            "team_game_stat_resolution_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "decision_id": decision_id,
            "lane": clean(row.get("lane")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "source_document_id": clean(row.get("source_document_id")),
            "promotion_package_id": clean(row.get("promotion_package_id")),
            "boxscore_id": clean(payload.get("boxscore_id")),
            "game_date": clean(payload.get("game_date")),
            "year": clean(payload.get("year")),
            "week": clean(payload.get("week")),
            "stat_name": clean(payload.get("stat_name")),
            "stat_unit": clean(payload.get("stat_unit")),
            "team_1_raw": clean(payload.get("team_1_raw")),
            "team_1_nfl_team": clean(payload.get("team_1_nfl_team")),
            "team_1_value": clean(payload.get("team_1_value")),
            "team_1_opponent_nfl_team": clean(payload.get("team_1_opponent_nfl_team")),
            "team_2_raw": clean(payload.get("team_2_raw")),
            "team_2_nfl_team": clean(payload.get("team_2_nfl_team")),
            "team_2_value": clean(payload.get("team_2_value")),
            "team_2_opponent_nfl_team": clean(payload.get("team_2_opponent_nfl_team")),
            "claimed_score_text": clean(payload.get("claimed_score_text")),
            "reconciliation_status": clean(payload.get("reconciliation_status")),
            "match_method": clean(payload.get("match_method")),
            "reason": reason,
            "source_row_text": clean(payload.get("source_row_text")),
            "evidence_text": clean(payload.get("evidence_text")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "enriched_proposed_fields_json": payload_json,
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
    run_id: str,
    promotion_apply_run_id: str,
    out_dir: Path,
    item_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.team_game_stat_resolution_item ("
            + ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
            + ")"
        )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.team_game_stat_resolution_run ("
            + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
            + ")"
        )
        for field in ITEM_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.team_game_stat_resolution_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        for field in RUN_FIELDS:
            con.execute(f"ALTER TABLE newspaper_review.team_game_stat_resolution_run ADD COLUMN IF NOT EXISTS {field} VARCHAR")
        con.execute(
            "DELETE FROM newspaper_review.team_game_stat_resolution_item WHERE team_game_stat_resolution_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.team_game_stat_resolution_run WHERE team_game_stat_resolution_run_id = ?",
            [run_id],
        )
        insert_rows(con, "team_game_stat_resolution_item", item_rows, ITEM_FIELDS)
        insert_rows(con, "team_game_stat_resolution_run", [{
            "team_game_stat_resolution_run_id": run_id,
            "promotion_apply_run_id": promotion_apply_run_id,
            "output_dir": str(out_dir),
            "candidate_decision_count": len(item_rows),
            "approved_count": len(item_rows),
            "held_count": 0,
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Team-Game Stat Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['team_game_stat_resolution_run_id']}`",
        f"Apply source: `{summary['promotion_apply_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Approved local atom rows: `{summary['approved_count']}`",
        f"- Reconciliation statuses: `{summary['reconciliation_status_counts']}`",
        "",
        "## Items",
        "",
        "| boxscore | stat | team 1 | value 1 | team 2 | value 2 | status |",
        "| --- | --- | --- | ---: | --- | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join([
                clean(row.get("boxscore_id")),
                clean(row.get("stat_name")),
                clean(row.get("team_1_nfl_team")),
                clean(row.get("team_1_value")),
                clean(row.get("team_2_nfl_team")),
                clean(row.get("team_2_value")),
                clean(row.get("reconciliation_status")),
            ])
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_team_game_stat_resolution")
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--team-games", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        promotion_apply_run_id = args.promotion_apply_run_id or latest_apply_run(con)
        route_rows = load_route_rows(con, promotion_apply_run_id)
        source_document_ids = sorted({clean(row.get("source_document_id")) for row in route_rows if clean(row.get("source_document_id"))})
        claims_by_doc = load_team_stat_claims(con, source_document_ids)
    finally:
        con.close()

    team_games = load_team_games(args.team_games)
    item_rows, new_decision_rows = build_resolution_rows(
        run_id,
        promotion_apply_run_id,
        route_rows,
        claims_by_doc,
        team_games,
    )
    base_input = args.base_decision_input_csv or latest_decision_input(args.root)
    base_rows = read_base_decisions(base_input)
    combined_rows = combine_decision_inputs(base_rows, new_decision_rows)

    decision_input_path = out_dir / "decision_input.csv"
    summary = {
        "created_at_utc": created_at,
        "team_game_stat_resolution_run_id": run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "base_decision_input_csv": str(base_input) if base_input else "",
        "decision_input_csv": str(decision_input_path),
        "route_rows_read": len(route_rows),
        "candidate_decision_count": len(item_rows),
        "approved_count": len(new_decision_rows),
        "held_count": 0,
        "reconciliation_status_counts": dict(Counter(row["reconciliation_status"] for row in item_rows)),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_csv(out_dir / "team_game_stat_resolution_items.csv", item_rows, ITEM_FIELDS)
    write_csv(out_dir / "decision_input_delta.csv", new_decision_rows, DECISION_INPUT_FIELDS)
    write_csv(decision_input_path, combined_rows, DECISION_INPUT_FIELDS)
    write_json(summary_path, summary)
    write_markdown(out_dir / "team_game_stat_resolution_report.md", summary, item_rows)
    if not args.no_db:
        persist(args.db_path, run_id, promotion_apply_run_id, out_dir, item_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
