#!/usr/bin/env python
"""Resolve quality-review newspaper holds into safer downstream decisions.

This station works the quality bucket without pretending low-confidence atoms
are final facts:

- uniquely mapped game candidates can become local game_candidate promotions;
- events missing a game can inherit a uniquely mapped same-source game;
- single-candidate lineup identity holds can be promoted only as
  player_identity_candidate rows, not as final lineup_participation rows;
- schema gaps and explicit conflict holds stay queued with concrete reasons.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "quality_lane_resolution_preps"
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
    "quality_lane_resolution_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "target_table",
    "resolved_target_table",
    "boxscore_id",
    "resolved_boxscore_id",
    "source_documents_json",
    "source_asset_dates_json",
    "raw_team_1",
    "raw_team_2",
    "resolved_team_1",
    "resolved_team_2",
    "raw_player",
    "resolved_NFL_player_id",
    "resolved_player",
    "candidate_count",
    "best_score",
    "second_score",
    "score_margin",
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
    "quality_lane_resolution_run_id",
    "decision_ledger_run_id",
    "output_dir",
    "quality_decision_count",
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
    "chicago bears": "CHI",
    "bears": "CHI",
    "chicago cardinals": "CRD",
    "cardinals": "CRD",
    "chicago tigers": "CHT",
    "cincinnati celts": "CIN",
    "cincy celts": "CIN",
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
    "muncie flyers": "MUN",
    "racine": "RAC",
    "racine cardinals": "RAC",
    "racine legion": "RAC",
    "rock island independents": "RII",
}

KNOWN_TEAM_CODES = {
    "AKR", "BKN", "BOS", "BRL", "BUF", "CAN", "CHI", "CHT", "CIN", "CLE", "CLI", "COL",
    "CRD", "DAY", "DET", "DUL", "EVN", "FRN", "GNB", "HAM", "HRT", "KAN", "KEN", "LAB",
    "LOU", "MIL", "MIN", "MUN", "NYG", "NYY", "OOR", "PHI", "PIT", "POT", "PRT", "PRV",
    "RAC", "RAM", "RCH", "RII", "SIS", "STL", "TOL", "TON", "TOR", "WAS",
}

TEAM_EVIDENCE_TERMS = {
    "AKR": ["akron", "pros", "indians"],
    "BKN": ["brooklyn", "dodgers"],
    "BOS": ["boston", "bulldogs"],
    "BRL": ["brooklyn", "lions"],
    "BUF": ["buffalo", "all americans", "all-americans", "bisons"],
    "CAN": ["canton", "bulldogs"],
    "CHI": ["chicago", "bears", "staleys"],
    "CHT": ["chicago", "tigers"],
    "CIN": ["cincinnati", "celts"],
    "CLE": ["cleveland", "bulldogs", "tigers"],
    "CLI": ["cleveland", "indians"],
    "COL": ["columbus", "panhandles"],
    "CRD": ["cardinals", "racine cardinals", "chicago cardinals"],
    "DAY": ["dayton", "triangles"],
    "DET": ["detroit", "heralds", "tigers"],
    "DUL": ["duluth", "kelleys", "eskimos"],
    "EVN": ["evansville", "crimson giants"],
    "FRN": ["frankford", "yellow jackets", "yellowjackets"],
    "GNB": ["green bay", "packers"],
    "HAM": ["hammond", "pros"],
    "KEN": ["kenosha", "maroons"],
    "LOU": ["louisville", "brecks"],
    "MIL": ["milwaukee", "badgers"],
    "MIN": ["minneapolis", "marines"],
    "MUN": ["muncie", "flyers"],
    "NYG": ["new york giants", "giants"],
    "OOR": ["oorang", "indians"],
    "RAC": ["racine", "legion"],
    "RCH": ["rochester", "jeffersons"],
    "RII": ["rock island", "independents"],
    "STL": ["st louis", "st. louis", "all stars", "all-stars"],
    "TOL": ["toledo", "maroons"],
}

QUALITY_ROUTES = {
    "evidence_check_then_promote",
    "quality_review_decide_promote_followup_or_reject",
    "resolve_player_or_team_identity_then_promote",
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


def score_pair_in_evidence(score_1: int | None, score_2: int | None, evidence: Any) -> bool:
    if score_1 is None or score_2 is None:
        return False
    text = clean(evidence).lower()
    if not text:
        return False
    left = re.escape(str(score_1))
    right = re.escape(str(score_2))
    return bool(re.search(rf"(?<!\d){left}\s*(?:-|to|–|—)\s*{right}(?!\d)", text))


def evidence_mentions_team(evidence: Any, code: str, raw: Any) -> bool:
    text = f" {norm_team(evidence)} "
    terms = list(TEAM_EVIDENCE_TERMS.get(code, []))
    raw_term = norm_team(raw)
    if raw_term and len(raw_term) > 3:
        terms.append(raw_term)
    for term in terms:
        normed = norm_team(term)
        if normed and f" {normed} " in text:
            return True
    return False


def game_evidence_supports(proposed: dict[str, Any], code_1: str, code_2: str) -> bool:
    evidence = proposed.get("evidence_text")
    score_1 = parse_int(proposed.get("team_1_score"))
    score_2 = parse_int(proposed.get("team_2_score"))
    if not score_pair_in_evidence(score_1, score_2, evidence):
        return False
    return (
        evidence_mentions_team(evidence, code_1, proposed.get("team_1_raw"))
        and evidence_mentions_team(evidence, code_2, proposed.get("team_2_raw"))
    )


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
        root / "lineup_route_resolution_preps" / "*" / "decision_input.csv",
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
    text = clean(raw).strip().upper()
    if text in KNOWN_TEAM_CODES:
        return text
    return TEAM_ALIASES.get(norm_team(raw), "")


def load_quality_decisions(con: duckdb.DuckDBPyConnection, decision_ledger_run_id: str) -> list[dict[str, Any]]:
    placeholders = ",".join(["?"] * len(QUALITY_ROUTES))
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
          AND route_to_lane IN ({placeholders})
          AND COALESCE(decision_status, '') IN ('', 'pending', 'pending_followup')
          AND COALESCE(decision_value, '') IN ('', 'pending')
        ORDER BY target_table, boxscore_id, target_entity_key, decision_id
        """,
        [decision_ledger_run_id, *sorted(QUALITY_ROUTES)],
    )


def load_current_quality_route_rows(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE promotion_apply_run_id = ?
          AND target_table = 'game_candidate'
          AND recommended_next_action IN (
            'quality_review_decide_promote_followup_or_reject',
            'evidence_check_then_promote'
          )
          AND COALESCE(decision_status, '') NOT IN ('rejected', 'reject')
        ORDER BY boxscore_id, target_entity_key, decision_id
        """,
        [promotion_apply_run_id],
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


def latest_lineup_decisions(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, Any]]:
    if not table_exists(con, "lineup_identity_resolution_decision"):
        return {}
    rows = query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.lineup_identity_resolution_decision
        ORDER BY lineup_identity_resolution_run_id
        """,
    )
    return {clean(row.get("decision_id")): row for row in rows}


def latest_lineup_candidates(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict[str, Any]]]:
    if not table_exists(con, "lineup_identity_resolution_candidate"):
        return {}
    rows = query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.lineup_identity_resolution_candidate
        ORDER BY lineup_identity_resolution_run_id, decision_id, TRY_CAST(candidate_rank AS INTEGER)
        """,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_run: dict[str, str] = {}
    for row in rows:
        decision_id = clean(row.get("decision_id"))
        run_id = clean(row.get("lineup_identity_resolution_run_id"))
        if decision_id in seen_run and seen_run[decision_id] != run_id:
            grouped[decision_id] = []
        seen_run[decision_id] = run_id
        grouped[decision_id].append(row)
    return grouped


def latest_player_box_decisions(con: duckdb.DuckDBPyConnection) -> dict[str, dict[str, Any]]:
    if not table_exists(con, "player_box_score_resolution_decision"):
        return {}
    rows = query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.player_box_score_resolution_decision
        ORDER BY player_box_score_resolution_run_id
        """,
    )
    return {clean(row.get("decision_id")): row for row in rows}


def find_game_match(
    proposed: dict[str, Any],
    source_docs: list[dict[str, Any]],
    team_games: list[dict[str, Any]],
) -> tuple[str, dict[str, Any], str, str]:
    team_1 = clean(proposed.get("team_1_raw"))
    team_2 = clean(proposed.get("team_2_raw"))
    code_1 = team_code(team_1) or team_code(proposed.get("team_1_resolved"))
    code_2 = team_code(team_2) or team_code(proposed.get("team_2_resolved"))
    score_1 = parse_int(proposed.get("team_1_score"))
    score_2 = parse_int(proposed.get("team_2_score"))
    if not code_1 or not code_2:
        return "unmapped_or_non_nfl_team", {}, code_1, code_2
    if score_1 is None or score_2 is None:
        return "missing_score_for_game_mapping", {}, code_1, code_2

    candidates = []
    for row in team_games:
        if (
            clean(row.get("team_code")) == code_1
            and clean(row.get("opponent_code")) == code_2
            and row.get("team_points") == score_1
            and row.get("opponent_points") == score_2
        ):
            candidates.append(row)
        elif (
            clean(row.get("team_code")) == code_2
            and clean(row.get("opponent_code")) == code_1
            and row.get("team_points") == score_2
            and row.get("opponent_points") == score_1
        ):
            candidates.append(row)
    by_boxscore: dict[str, dict[str, Any]] = {clean(row.get("boxscore_id")): row for row in candidates}
    candidates = list(by_boxscore.values())
    if not candidates:
        return "no_pfr_game_match_for_teams_scores", {}, code_1, code_2
    proposed_boxscore_id = clean(proposed.get("boxscore_id"))
    if proposed_boxscore_id:
        same_boxscore = [row for row in candidates if clean(row.get("boxscore_id")) == proposed_boxscore_id]
        if len(same_boxscore) == 1:
            return "unique_teams_scores_boxscore_match", same_boxscore[0], code_1, code_2
    if len(candidates) == 1:
        asset_dates = source_asset_dates(source_docs)
        if asset_date_supports(clean(candidates[0].get("game_date")), asset_dates):
            return "unique_teams_scores_source_date_match", candidates[0], code_1, code_2
        return "teams_scores_match_but_source_date_or_boxscore_mismatch", {}, code_1, code_2
    asset_dates = source_asset_dates(source_docs)
    dated = [row for row in candidates if asset_date_supports(clean(row.get("game_date")), asset_dates)]
    if len(dated) == 1:
        return "unique_teams_scores_source_date_match", dated[0], code_1, code_2
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


def identity_target_key(boxscore_id: str, nfl_player_id: str, raw_player: str, raw_team: str) -> str:
    return "|".join(["player_identity_candidate", boxscore_id, nfl_player_id, raw_player, raw_team])


def source_doc_ids(row: dict[str, Any]) -> list[str]:
    return parse_json_list(row.get("source_documents_json"))


def build_resolution_rows(
    run_id: str,
    decision_ledger_run_id: str,
    decisions: list[dict[str, Any]],
    source_docs_by_id: dict[str, dict[str, Any]],
    team_games: list[dict[str, Any]],
    lineup_decisions: dict[str, dict[str, Any]],
    lineup_candidates: dict[str, list[dict[str, Any]]],
    player_box_decisions: dict[str, dict[str, Any]],
    emit_hold_overrides: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], Counter]:
    created_at = iso_now()
    rows: list[dict[str, Any]] = []
    decision_inputs: list[dict[str, str]] = []
    counts: Counter = Counter()
    source_game_map: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for decision in decisions:
        if clean(decision.get("target_table")) != "game_candidate":
            continue
        proposed = parse_json_obj(decision.get("proposed_fields_json"))
        docs = [source_docs_by_id[doc_id] for doc_id in source_doc_ids(decision) if doc_id in source_docs_by_id]
        status, game_row, code_1, code_2 = find_game_match(proposed, docs, team_games)
        if status.startswith("unique_"):
            source_entry = {
                "decision_id": clean(decision.get("decision_id")),
                "boxscore_id": clean(game_row.get("boxscore_id")),
                "team_codes": sorted([code_1, code_2]),
                "game_row": game_row,
                "proposed": proposed,
            }
            for doc_id in source_doc_ids(decision):
                source_game_map[doc_id].append(source_entry)

    for decision in decisions:
        decision_id = clean(decision.get("decision_id"))
        target_table = clean(decision.get("target_table"))
        proposed = parse_json_obj(decision.get("proposed_fields_json"))
        docs = [source_docs_by_id[doc_id] for doc_id in source_doc_ids(decision) if doc_id in source_docs_by_id]
        asset_dates = source_asset_dates(docs)
        enriched = dict(proposed)
        resolved_target_table = target_table
        resolved_boxscore_id = clean(proposed.get("boxscore_id")) or clean(decision.get("boxscore_id"))
        resolved_key = clean(decision.get("target_entity_key"))
        raw_player = clean(proposed.get("player_raw") or proposed.get("scoring_player_raw"))
        raw_team_1 = clean(proposed.get("team_1_raw") or proposed.get("team_raw") or proposed.get("scoring_team_raw"))
        raw_team_2 = clean(proposed.get("team_2_raw"))
        resolved_team_1 = ""
        resolved_team_2 = ""
        resolved_nfl_id = ""
        resolved_player = ""
        candidate_count = ""
        best_score = ""
        second_score = ""
        score_margin = ""

        if target_table == "game_candidate":
            status, game_row, code_1, code_2 = find_game_match(proposed, docs, team_games)
            resolved_team_1, resolved_team_2 = code_1, code_2
            if status.startswith("unique_") and game_evidence_supports(proposed, code_1, code_2):
                resolved_boxscore_id = clean(game_row.get("boxscore_id"))
                enriched.update({
                    "boxscore_id": resolved_boxscore_id,
                    "game_date": clean(game_row.get("game_date")),
                    "year": clean(game_row.get("year")),
                    "week": clean(game_row.get("week")),
                    "season_type": clean(game_row.get("season_type")),
                    "team_1_resolved": code_1,
                    "team_2_resolved": code_2,
                    "reconciliation_status": "quality_lane_game_mapped",
                })
                match_status = status
                decision_status = "approved"
                decision_value = PROMOTION_VALUE
                route = "local_promotion"
                reason = "unique PFR game matched newspaper teams/scores"
                resolved_key = game_target_key(resolved_boxscore_id, enriched)
            else:
                match_status = status if not status.startswith("unique_") else "unique_match_but_weak_evidence"
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "quality_review"
                reason = (
                    "game candidate mapped but evidence text did not include both teams and exact score"
                    if status.startswith("unique_")
                    else "game candidate could not be mapped uniquely to an NFL/PFR game"
                )
                if (
                    status == "unmapped_or_non_nfl_team"
                    and not any(clean(proposed.get(field)) for field in ["team_1_raw", "team_2_raw", "team_1_score", "team_2_score"])
                ):
                    doc_ids = source_doc_ids(decision)
                    source_doc_id = doc_ids[0] if doc_ids else ""
                    resolved_target_table = "source_document_note"
                    resolved_key = "|".join([
                        "source_document_note",
                        "unmappable_game_candidate",
                        clean(decision.get("boxscore_id")),
                        clean(decision.get("decision_id")),
                    ])
                    enriched = {
                        "source_document_note_id": stable_id("unmappable_game_candidate", decision_id, decision.get("boxscore_id")),
                        "source_document_id": source_doc_id,
                        "boxscore_id": clean(decision.get("boxscore_id")),
                        "note_type": "unmappable_game_candidate",
                        "note_category": "quality_review_closed",
                        "note_text": (
                            "Game-candidate hold had no extractable team/score fields and could not be mapped "
                            "to an NFL/PFR game; preserved as source context only."
                        ),
                        "related_target_table": "game_candidate",
                        "related_entity_key": clean(decision.get("target_entity_key")),
                        "reconciliation_status": status,
                        "evidence_text": clean(decision.get("reason")),
                        "confidence_score": "0.50",
                        "review_status": "local_atom_quality_note_accepted",
                        "promotion_status": "local_atom_only",
                        "source_documents_json": clean(decision.get("source_documents_json")),
                        "artifact_path": clean(decision.get("artifact_path")),
                        "created_by_station": "build_newspaper_quality_lane_resolution_prep.py",
                    }
                    match_status = "closed_unmappable_empty_game_candidate_note"
                    decision_status = "approved"
                    decision_value = PROMOTION_VALUE
                    route = "local_promotion"
                    reason = "empty/unmappable game candidate preserved as source-document note"

        elif target_table in {"scoring_event", "play_by_play_event"} and not resolved_boxscore_id:
            candidates = [
                entry
                for doc_id in source_doc_ids(decision)
                for entry in source_game_map.get(doc_id, [])
            ]
            raw_team = clean(proposed.get("scoring_team_raw") or proposed.get("possession_team_raw") or proposed.get("event_team_raw"))
            event_team_code = team_code(raw_team)
            if event_team_code:
                candidates = [entry for entry in candidates if event_team_code in entry["team_codes"]]
            unique_by_boxscore = {entry["boxscore_id"]: entry for entry in candidates}
            if len(unique_by_boxscore) == 1:
                entry = next(iter(unique_by_boxscore.values()))
                resolved_boxscore_id = entry["boxscore_id"]
                resolved_team_1 = event_team_code
                enriched["boxscore_id"] = resolved_boxscore_id
                if target_table == "scoring_event":
                    enriched["scoring_team"] = event_team_code
                    enriched.setdefault("review_status", "quality_lane_game_mapped")
                    enriched.setdefault("promotion_status", "local_promotion_candidate")
                else:
                    if not clean(enriched.get("play_type")) and clean(enriched.get("event_type")):
                        enriched["play_type"] = clean(enriched.get("event_type"))
                    if not clean(enriched.get("possession_team_raw")) and clean(enriched.get("event_team_raw")):
                        enriched["possession_team_raw"] = clean(enriched.get("event_team_raw"))
                    if not clean(enriched.get("primary_player_raw")) and clean(enriched.get("player_raw")):
                        enriched["primary_player_raw"] = clean(enriched.get("player_raw"))
                    enriched["possession_team"] = event_team_code
                    enriched.setdefault("review_status", "quality_lane_game_mapped")
                    enriched.setdefault("promotion_status", "local_promotion_candidate")
                match_status = "inherited_unique_same_source_game_candidate"
                decision_status = "approved"
                decision_value = PROMOTION_VALUE
                route = "local_promotion"
                reason = "event inherited unique same-source mapped game candidate"
                resolved_key = event_target_key(target_table, resolved_boxscore_id, enriched)
            else:
                match_status = "missing_game_mapping_no_unique_same_source_candidate"
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "semantic_followup"
                reason = "event still needs a unique target game before promotion"

        elif target_table == "lineup_participation":
            lineup_row = lineup_decisions.get(decision_id, {})
            candidates = lineup_candidates.get(decision_id, [])
            candidate_count = str(len(candidates))
            if candidates:
                best = candidates[0]
                second = candidates[1] if len(candidates) > 1 else {}
                best_score = clean(best.get("candidate_score"))
                second_score = clean(second.get("candidate_score"))
                try:
                    score_margin = str(int(best_score or 0) - int(second_score or 0))
                except ValueError:
                    score_margin = ""
            match = clean(lineup_row.get("match_status"))
            if match == "held_boxscore_conflict":
                match_status = "held_boxscore_conflict"
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "quality_review"
                reason = clean(lineup_row.get("reason")) or "explicit lineup hold"
            elif len(candidates) == 1:
                best = candidates[0]
                resolved_nfl_id = clean(best.get("candidate_pfr_id"))
                resolved_player = clean(best.get("candidate_player"))
                resolved_boxscore_id = clean(proposed.get("boxscore_id")) or clean(decision.get("boxscore_id"))
                raw_team = clean(proposed.get("team_raw"))
                resolved_team_1 = clean(lineup_row.get("resolved_team"))
                raw_player_name = clean(proposed.get("player_raw"))
                year = clean(lineup_row.get("year"))
                week = clean(lineup_row.get("week"))
                player_week = f"{resolved_nfl_id}_{year}_{week}" if resolved_nfl_id and year and week else ""
                enriched = {
                    "raw_player_name": raw_player_name,
                    "raw_team": raw_team,
                    "resolved_player": resolved_player,
                    "NFL_player_id": resolved_nfl_id,
                    "player_week": player_week,
                    "nfl_team": resolved_team_1,
                    "opponent_nfl_team": clean(lineup_row.get("opponent_team")),
                    "boxscore_id": resolved_boxscore_id,
                    "match_method": "single_active_year_candidate_below_lineup_threshold",
                    "confidence_score": best_score,
                    "review_status": "identity_candidate_only",
                    "evidence_text": clean(proposed.get("source_row_text")),
                }
                resolved_target_table = "player_identity_candidate"
                resolved_key = identity_target_key(resolved_boxscore_id, resolved_nfl_id, raw_player_name, raw_team)
                match_status = "promote_single_candidate_as_identity_candidate"
                decision_status = "approved"
                decision_value = PROMOTION_VALUE
                route = "local_promotion"
                reason = "single active-year lineup candidate promoted as player_identity_candidate only, not final lineup"
            else:
                match_status = match or "lineup_identity_followup"
                decision_status = "pending_followup"
                decision_value = "pending"
                route = "quality_review"
                reason = clean(lineup_row.get("reason")) or "lineup identity needs additional evidence"

        elif target_table == "player_game_box_score":
            box_row = player_box_decisions.get(decision_id, {})
            match_status = clean(box_row.get("match_status")) or "player_box_quality_followup"
            decision_status = "pending_followup"
            decision_value = "pending"
            if clean(proposed.get("touchdowns")) and not any(clean(proposed.get(field)) for field in ["rushing_tds", "receiving_tds", "passing_tds"]):
                route = "schema_followup"
                reason = "generic touchdowns need a newspaper total_touchdowns/points_scored atom field before promotion"
            elif clean(proposed.get("injury_note")) or clean(proposed.get("playing_time_note")):
                route = "event_or_participation_followup"
                reason = "playing-time/injury note belongs in event or participation lane, not player box score"
            else:
                route = "quality_review"
                reason = clean(box_row.get("reason")) or "player box score quality followup"

        else:
            match_status = "quality_followup_unhandled_target"
            decision_status = "pending_followup"
            decision_value = "pending"
            route = "quality_review"
            reason = "quality lane target needs manual or future station handling"

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

        rows.append({
            "quality_lane_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "decision_id": decision_id,
            "target_table": target_table,
            "resolved_target_table": resolved_target_table,
            "boxscore_id": clean(proposed.get("boxscore_id")) or clean(decision.get("boxscore_id")),
            "resolved_boxscore_id": resolved_boxscore_id,
            "source_documents_json": clean(decision.get("source_documents_json")),
            "source_asset_dates_json": json.dumps(asset_dates, sort_keys=True),
            "raw_team_1": raw_team_1,
            "raw_team_2": raw_team_2,
            "resolved_team_1": resolved_team_1,
            "resolved_team_2": resolved_team_2,
            "raw_player": raw_player,
            "resolved_NFL_player_id": resolved_nfl_id,
            "resolved_player": resolved_player,
            "candidate_count": candidate_count,
            "best_score": best_score,
            "second_score": second_score,
            "score_margin": score_margin,
            "match_status": match_status,
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": route,
            "reason": reason,
            "proposed_fields_json": clean(decision.get("proposed_fields_json")),
            "enriched_proposed_fields_json": json.dumps(enriched, sort_keys=True, ensure_ascii=False),
            "created_at_utc": created_at,
        })
        counts[match_status] += 1
        counts[f"decision_{decision_status}"] += 1
        counts[f"target_{target_table or 'none'}"] += 1

    return rows, decision_inputs, counts


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    defs = ", ".join(f"{field} VARCHAR" for field in RESOLUTION_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.quality_lane_resolution_decision ({defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'newspaper_review'
              AND table_name = 'quality_lane_resolution_decision'
            """
        ).fetchall()
    }
    for field in RESOLUTION_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.quality_lane_resolution_decision ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.quality_lane_resolution_run (
          quality_lane_resolution_run_id VARCHAR,
          decision_ledger_run_id VARCHAR,
          output_dir VARCHAR,
          quality_decision_count INTEGER,
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
            "DELETE FROM newspaper_review.quality_lane_resolution_decision WHERE quality_lane_resolution_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.quality_lane_resolution_run WHERE quality_lane_resolution_run_id = ?",
            [run_id],
        )
        insert_rows(con, "quality_lane_resolution_decision", rows, RESOLUTION_FIELDS)
        insert_rows(con, "quality_lane_resolution_run", [{
            "quality_lane_resolution_run_id": run_id,
            "decision_ledger_run_id": decision_ledger_run_id,
            "output_dir": str(out_dir),
            "quality_decision_count": len(rows),
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
        "# Newspaper Quality Lane Resolution Prep",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['quality_lane_resolution_run_id']}`",
        f"Decision ledger: `{summary['decision_ledger_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Quality rows reviewed: `{summary['quality_decision_count']}`",
        f"- Approved/local-promoted decisions: `{summary['approved_count']}`",
        f"- Held/routed decisions: `{summary['held_count']}`",
        f"- Match statuses: `{summary['match_status_counts']}`",
        "",
        "## Approved Samples",
        "",
        "| original table | resolved table | boxscore | player/team | status | reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in [item for item in rows if item.get("decision_status") == "approved"][:60]:
        lines.append(
            f"| {row['target_table']} | {row['resolved_target_table']} | {row['resolved_boxscore_id']} | "
            f"{row['raw_player'] or row['raw_team_1']} | {row['match_status']} | {row['reason']} |"
        )
    lines.extend(["", "## Held Samples", "", "| table | boxscore | player/team | status | reason |", "| --- | --- | --- | --- | --- |"])
    for row in [item for item in rows if item.get("decision_status") != "approved"][:80]:
        lines.append(
            f"| {row['target_table']} | {row['boxscore_id']} | {row['raw_player'] or row['raw_team_1']} | "
            f"{row['match_status']} | {row['reason']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument(
        "--promotion-apply-run-id",
        default="",
        help="Read quality rows from this local apply run's route queue.",
    )
    parser.add_argument("--team-games-path", type=Path, default=DEFAULT_TEAM_GAMES)
    parser.add_argument("--base-decision-input-csv", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="quality_lane_resolution_prep")
    parser.add_argument(
        "--emit-hold-overrides",
        action="store_true",
        help="Write pending/follow-up overrides for rows that fail quality gates.",
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
            decisions = load_current_quality_route_rows(read_con, promotion_apply_run_id)
        else:
            decisions = load_quality_decisions(read_con, decision_ledger_run_id)
        source_document_ids = sorted({doc_id for row in decisions for doc_id in source_doc_ids(row)})
        source_docs_by_id = load_source_documents(read_con, source_document_ids)
        team_games = load_team_games(read_con, args.team_games_path)
        lineup_rows = latest_lineup_decisions(read_con)
        lineup_candidate_rows = latest_lineup_candidates(read_con)
        player_box_rows = latest_player_box_decisions(read_con)
    finally:
        read_con.close()

    resolution_rows, quality_input_rows, counts = build_resolution_rows(
        run_id,
        decision_ledger_run_id,
        decisions,
        source_docs_by_id,
        team_games,
        lineup_rows,
        lineup_candidate_rows,
        player_box_rows,
        args.emit_hold_overrides,
    )

    base_input_path = args.base_decision_input_csv or latest_decision_input(DEFAULT_ROOT)
    base_rows = read_base_decisions(base_input_path)
    combined_rows = combine_decision_inputs(base_rows, quality_input_rows)

    resolution_csv = out_dir / "quality_lane_resolution_decisions.csv"
    quality_input_csv = out_dir / "quality_lane_decision_input.csv"
    combined_input_csv = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    report_path = out_dir / "quality_lane_resolution_report.md"

    write_csv(resolution_csv, resolution_rows, RESOLUTION_FIELDS)
    write_csv(quality_input_csv, quality_input_rows, DECISION_INPUT_FIELDS)
    write_csv(combined_input_csv, combined_rows, DECISION_INPUT_FIELDS)

    summary = {
        "created_at_utc": created_at,
        "quality_lane_resolution_run_id": run_id,
        "decision_ledger_run_id": decision_ledger_run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "output_dir": str(out_dir),
        "quality_decision_count": len(resolution_rows),
        "approved_count": sum(1 for row in resolution_rows if row.get("decision_status") == "approved"),
        "held_count": sum(1 for row in resolution_rows if row.get("decision_status") != "approved"),
        "match_status_counts": dict(Counter(row.get("match_status") for row in resolution_rows)),
        "decision_status_counts": dict(Counter(row.get("decision_status") for row in resolution_rows)),
        "quality_decision_input_csv": str(quality_input_csv),
        "combined_decision_input_csv": str(combined_input_csv),
        "base_decision_input_csv": str(base_input_path) if base_input_path else "",
        "resolution_csv": str(resolution_csv),
        "report_path": str(report_path),
        "emit_hold_overrides": bool(args.emit_hold_overrides),
    }
    write_json(summary_path, summary)
    write_report(report_path, summary, resolution_rows)
    persist(args.db_path, run_id, decision_ledger_run_id, out_dir, resolution_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
