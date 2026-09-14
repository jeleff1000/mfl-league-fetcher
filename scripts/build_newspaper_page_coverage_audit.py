#!/usr/bin/env python
"""Audit page/text coverage for the local newspaper atom conveyor.

This station answers a narrower question than the acquisition coverage audit:
for each planned PDF pull, do we have a real asset, OCR text/layout sidecars,
article-region passes, atom claims, and a finite next action?

It is deliberately local-only. It reads the D-drive newspaper atom DuckDB and
filesystem artifacts, writes D-drive CSV/JSON/Markdown outputs, and optionally
persists receipt rows in `newspaper_review.page_coverage_audit_*`. It does not
write to Fly, v26, production supertable tables, `newspaper_promoted`, or
`newspaper_final`.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_PLAN_ROOT = DEFAULT_ROOT / "article_batch_plans"
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "page_coverage_audits"

HIGH_VALUE_TARGETS = {
    "lineup_participation",
    "play_by_play_event",
    "player_game_box_score",
    "player_game_stat_claim",
    "scoring_event",
    "team_game_stat_claim",
}

TERMINAL_NO_ATOM_STATUSES = {
    "semantic_reparse_no_atoms",
    "semantic_reparse_no_regions",
    "exhausted_low_signal_article_crops",
    "low_signal_or_weak_ocr",
    "terminal_no_current_atoms",
}

KEYWORD_PATTERNS: list[tuple[str, str, int, str]] = [
    ("touchdown", r"\btouchdowns?\b", 6, "scoring"),
    ("field goal", r"\bfield\s+goals?\b", 6, "scoring"),
    ("extra point", r"\bextra\s+points?\b", 5, "scoring"),
    ("drop kick", r"\bdrop[-\s]?kicks?\b", 5, "scoring"),
    ("goal from touchdown", r"\bgoals?\s+from\s+touchdowns?\b", 5, "scoring"),
    ("safety", r"\bsafet(?:y|ies)\b", 4, "scoring"),
    ("forward pass", r"\bforward\s+passes?\b", 4, "play"),
    ("interception", r"\bintercept(?:ed|ion|ions)?\b", 4, "play"),
    ("fumble", r"\bfumbl(?:e|ed|es)\b", 3, "play"),
    ("kickoff", r"\bkick[-\s]?offs?\b", 3, "play"),
    ("punt", r"\bpunts?\b", 2, "play"),
    ("first down", r"\bfirst\s+downs?\b", 4, "stat"),
    ("yards", r"\byards?\b", 3, "stat"),
    ("lineup", r"\bline[-\s]?ups?\b", 6, "lineup"),
    ("line-up", r"\bline[-\s]?up\b", 6, "lineup"),
    ("box score", r"\bbox\s+scores?\b", 6, "stat"),
    ("statistics", r"\bstatistics?\b", 5, "stat"),
    ("summary", r"\bsummary\b", 3, "stat"),
    ("individual", r"\bindividual\b", 3, "stat"),
    ("passes completed", r"\bpasses?\s+completed\b", 5, "stat"),
    ("attempts", r"\battempts?\b", 2, "stat"),
    ("rush", r"\brush(?:ed|ing|es)?\b", 3, "stat"),
    ("substitutes", r"\bsubstitutes?\b", 3, "lineup"),
    ("referee", r"\breferee\b", 2, "context"),
    ("quarter", r"\bquarters?\b", 2, "context"),
    ("defeated", r"\bdefeat(?:ed|s)?\b", 2, "context"),
    ("beat", r"\bbeats?\b", 2, "context"),
]

RUN_FIELDS = [
    "page_coverage_audit_run_id",
    "label",
    "document_inventory",
    "db_path",
    "output_dir",
    "planned_document_count",
    "coverage_status_counts_json",
    "recommended_next_action_counts_json",
    "coverage_lane_counts_json",
    "sidecar_quality_lane_counts_json",
    "documents_with_atoms",
    "documents_with_high_value_atoms",
    "documents_needing_ocr",
    "documents_needing_article_pass",
    "documents_needing_semantic_reparse",
    "documents_needing_visual_read",
    "documents_terminal_no_current_atoms",
    "certified_no_word_left_behind",
    "summary_json_path",
    "report_md_path",
    "created_at_utc",
]

ITEM_FIELDS = [
    "page_coverage_audit_run_id",
    "source_document_id",
    "candidate_id",
    "boxscore_id",
    "year",
    "game_date",
    "away_team",
    "home_team",
    "publication",
    "result_date",
    "page",
    "candidate_rank",
    "image_id",
    "round_name",
    "pdf_path",
    "pdf_exists",
    "pdf_bytes_plan",
    "pdf_bytes_actual",
    "pdf_file_status",
    "source_url",
    "sidecar_text_path",
    "sidecar_json_path",
    "sidecar_text_exists",
    "sidecar_json_exists",
    "sidecar_text_chars_plan",
    "sidecar_text_chars_actual",
    "sidecar_text_bytes",
    "sidecar_json_bytes",
    "sidecar_mean_confidence",
    "sidecar_page_count",
    "sidecar_quality_lane",
    "football_keyword_score",
    "football_keyword_hits",
    "stat_keyword_hits",
    "strong_keyword_hits",
    "keyword_terms",
    "keyword_excerpt",
    "db_source_document_exists",
    "db_text_pass_count",
    "db_full_page_text_pass_count",
    "db_article_text_pass_count",
    "db_text_chars_total",
    "db_text_chars_max",
    "db_region_count",
    "db_region_text_count",
    "db_region_low_signal_count",
    "db_region_quality_score_avg",
    "db_atom_count",
    "db_high_value_atom_count",
    "db_game_candidate_atoms",
    "db_lineup_atoms",
    "db_scoring_event_atoms",
    "db_pbp_event_atoms",
    "db_player_stat_atoms",
    "db_team_stat_atoms",
    "db_promotion_candidate_count",
    "current_station",
    "current_status",
    "state_next_action",
    "terminal_status",
    "terminal_reason",
    "coverage_status",
    "coverage_lane",
    "recommended_next_action",
    "reason_code",
    "evidence_summary_json",
    "created_at_utc",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)
    return str(value)


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_int(value: Any, default: int = 0) -> int:
    try:
        text = clean(value)
        if not text:
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def as_float(value: Any) -> float | None:
    try:
        text = clean(value)
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def path_from_cell(value: Any) -> Path | None:
    text = clean(value)
    if not text:
        return None
    return Path(text)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: safe_cell(row.get(field)) for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def default_document_inventory(plan_root: Path) -> Path:
    full_plan_candidates: list[Path] = []
    all_candidates: list[Path] = []
    for inventory in sorted(plan_root.glob("*/document_inventory.csv")):
        all_candidates.append(inventory)
        summary = read_json(inventory.parent / "summary.json")
        label = clean(summary.get("label"))
        if "full_pdf_article_batches" in label or "full_pdf_article_batches" in inventory.parent.name:
            full_plan_candidates.append(inventory)
    candidates = full_plan_candidates or all_candidates
    if not candidates:
        raise FileNotFoundError(f"No document_inventory.csv found under {plan_root}")
    return sorted(candidates)[-1]


def query_dicts(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | tuple[Any, ...] | None = None,
) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    columns = [col[0] for col in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(1)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and row[0])


def merge_rollup(target: dict[str, dict[str, Any]], key: str, values: dict[str, Any]) -> None:
    target.setdefault(key, {}).update(values)


def load_review_rollups(db_path: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    warnings: list[str] = []
    rollups: dict[str, dict[str, Any]] = defaultdict(dict)
    if not db_path.exists():
        return {}, [f"db_missing:{db_path}"]
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        return {}, [f"db_unavailable:{type(exc).__name__}:{exc}"]
    try:
        if table_exists(con, "newspaper_review", "source_document"):
            for row in query_dicts(
                con,
                """
                SELECT
                  source_document_id,
                  source_url AS db_source_url,
                  asset_pdf_path AS db_asset_pdf_path,
                  acquisition_status,
                  visual_qa_status
                FROM newspaper_review.source_document
                """,
            ):
                merge_rollup(rollups, clean(row.get("source_document_id")), {
                    "db_source_document_exists": 1,
                    **row,
                })
        else:
            warnings.append("missing_table:newspaper_review.source_document")

        if table_exists(con, "newspaper_review", "source_text_pass"):
            for row in query_dicts(
                con,
                """
                SELECT
                  source_document_id,
                  COUNT(*) AS db_text_pass_count,
                  SUM(CASE WHEN pass_type LIKE '%full_page%' THEN 1 ELSE 0 END) AS db_full_page_text_pass_count,
                  SUM(CASE WHEN pass_type LIKE '%article%' THEN 1 ELSE 0 END) AS db_article_text_pass_count,
                  SUM(COALESCE(text_chars, 0)) AS db_text_chars_total,
                  MAX(COALESCE(text_chars, 0)) AS db_text_chars_max
                FROM newspaper_review.source_text_pass
                GROUP BY source_document_id
                """,
            ):
                merge_rollup(rollups, clean(row.get("source_document_id")), row)
        else:
            warnings.append("missing_table:newspaper_review.source_text_pass")

        if table_exists(con, "newspaper_review", "source_region"):
            for row in query_dicts(
                con,
                """
                SELECT
                  source_document_id,
                  COUNT(*) AS db_region_count,
                  SUM(CASE WHEN COALESCE(region_text_path, '') <> '' THEN 1 ELSE 0 END) AS db_region_text_count,
                  SUM(CASE WHEN status = 'low_signal_or_weak_ocr' THEN 1 ELSE 0 END) AS db_region_low_signal_count,
                  AVG(region_quality_score) AS db_region_quality_score_avg
                FROM newspaper_review.source_region
                GROUP BY source_document_id
                """,
            ):
                merge_rollup(rollups, clean(row.get("source_document_id")), row)
        else:
            warnings.append("missing_table:newspaper_review.source_region")

        if table_exists(con, "newspaper_review", "atom_claim"):
            for row in query_dicts(
                con,
                """
                SELECT
                  source_document_id,
                  COUNT(*) AS db_atom_count,
                  SUM(CASE WHEN semantic_target_table IN (
                    'lineup_participation',
                    'play_by_play_event',
                    'player_game_box_score',
                    'player_game_stat_claim',
                    'scoring_event',
                    'team_game_stat_claim'
                  ) THEN 1 ELSE 0 END) AS db_high_value_atom_count,
                  SUM(CASE WHEN semantic_target_table = 'game_candidate' THEN 1 ELSE 0 END) AS db_game_candidate_atoms,
                  SUM(CASE WHEN semantic_target_table = 'lineup_participation' THEN 1 ELSE 0 END) AS db_lineup_atoms,
                  SUM(CASE WHEN semantic_target_table = 'scoring_event' THEN 1 ELSE 0 END) AS db_scoring_event_atoms,
                  SUM(CASE WHEN semantic_target_table = 'play_by_play_event' THEN 1 ELSE 0 END) AS db_pbp_event_atoms,
                  SUM(CASE WHEN semantic_target_table IN ('player_game_box_score', 'player_game_stat_claim') THEN 1 ELSE 0 END) AS db_player_stat_atoms,
                  SUM(CASE WHEN semantic_target_table = 'team_game_stat_claim' THEN 1 ELSE 0 END) AS db_team_stat_atoms
                FROM newspaper_review.atom_claim
                GROUP BY source_document_id
                """,
            ):
                merge_rollup(rollups, clean(row.get("source_document_id")), row)
        else:
            warnings.append("missing_table:newspaper_review.atom_claim")

        if (
            table_exists(con, "newspaper_review", "promotion_candidate")
            and table_exists(con, "newspaper_review", "atom_claim")
        ):
            for row in query_dicts(
                con,
                """
                SELECT
                  a.source_document_id,
                  COUNT(*) AS db_promotion_candidate_count
                FROM newspaper_review.promotion_candidate p
                JOIN newspaper_review.atom_claim a ON a.atom_claim_id = p.atom_claim_id
                GROUP BY a.source_document_id
                """,
            ):
                merge_rollup(rollups, clean(row.get("source_document_id")), row)

        if table_exists(con, "newspaper_review", "conveyor_document_state"):
            for row in query_dicts(
                con,
                """
                SELECT
                  source_document_id,
                  current_station,
                  current_status,
                  next_action AS state_next_action,
                  terminal_status,
                  terminal_reason
                FROM newspaper_review.conveyor_document_state
                """,
            ):
                merge_rollup(rollups, clean(row.get("source_document_id")), row)
        else:
            warnings.append("missing_table:newspaper_review.conveyor_document_state")
    except Exception as exc:
        warnings.append(f"db_rollup_error:{type(exc).__name__}:{exc}")
    finally:
        con.close()
    return dict(rollups), warnings


def parse_layout_info(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {"sidecar_json_exists": False}
    info: dict[str, Any] = {
        "sidecar_json_exists": True,
        "sidecar_json_bytes": path.stat().st_size,
        "sidecar_page_count": "",
    }
    try:
        with path.open("rb") as handle:
            prefix = handle.read(65536).decode("utf-8", errors="ignore")
    except Exception as exc:
        return {
            "sidecar_json_exists": True,
            "sidecar_json_bytes": info["sidecar_json_bytes"],
            "sidecar_json_error": f"{type(exc).__name__}:{exc}",
        }
    mean_match = re.search(r'"mean_confidence"\s*:\s*(-?\d+(?:\.\d+)?)', prefix)
    info["sidecar_mean_confidence"] = mean_match.group(1) if mean_match else ""
    page_match = re.search(r'"page_count"\s*:\s*(\d+)', prefix)
    if page_match:
        info["sidecar_page_count"] = page_match.group(1)
    return info


def read_text_file(path: Path | None) -> tuple[str, int, int, bool]:
    if not path or not path.exists() or not path.is_file():
        return "", 0, 0, False
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return "", 0, path.stat().st_size, True
    return text, len(text), path.stat().st_size, True


def quality_lane(text_exists: bool, json_exists: bool, chars: int, mean_confidence: float | None) -> str:
    if not text_exists:
        return "missing_text"
    if chars <= 0:
        return "empty_text"
    if chars < 300:
        return "tiny_text"
    if chars < 1200:
        return "weak_text"
    if mean_confidence is not None and mean_confidence < 20:
        return "weak_confidence"
    if not json_exists:
        return "text_without_layout"
    if chars >= 6000 and (mean_confidence is None or mean_confidence >= 30):
        return "strong_text"
    return "usable_text"


def keyword_profile(text: str) -> dict[str, Any]:
    lower = text.lower()
    score = 0
    terms: list[str] = []
    category_counts: Counter[str] = Counter()
    strong_hits = 0
    first_match: re.Match[str] | None = None
    first_pattern = ""
    for term, pattern, weight, category in KEYWORD_PATTERNS:
        matches = list(re.finditer(pattern, lower))
        if not matches:
            continue
        capped_count = min(len(matches), 5)
        score += weight * capped_count
        category_counts[category] += len(matches)
        terms.append(term)
        if weight >= 5:
            strong_hits += len(matches)
        if first_match is None:
            first_match = matches[0]
            first_pattern = term
    excerpt = ""
    if first_match:
        start = max(0, first_match.start() - 180)
        end = min(len(text), first_match.end() + 220)
        excerpt = re.sub(r"\s+", " ", text[start:end]).strip()
        if len(excerpt) > 500:
            excerpt = excerpt[:497] + "..."
    return {
        "football_keyword_score": score,
        "football_keyword_hits": sum(category_counts.values()),
        "stat_keyword_hits": category_counts["stat"] + category_counts["lineup"] + category_counts["scoring"],
        "strong_keyword_hits": strong_hits,
        "keyword_terms": ";".join(terms[:40]),
        "keyword_excerpt": excerpt,
        "first_keyword": first_pattern,
    }


def has_relevant_markers(profile: dict[str, Any]) -> bool:
    return (
        as_int(profile.get("football_keyword_score")) >= 8
        or as_int(profile.get("stat_keyword_hits")) >= 2
        or as_int(profile.get("strong_keyword_hits")) >= 1
    )


def classify_document(
    pdf_exists: bool,
    text_exists: bool,
    json_exists: bool,
    quality: str,
    profile: dict[str, Any],
    rollup: dict[str, Any],
) -> tuple[str, str, str, str]:
    atom_count = as_int(rollup.get("db_atom_count"))
    high_value_atoms = as_int(rollup.get("db_high_value_atom_count"))
    region_count = as_int(rollup.get("db_region_count"))
    text_pass_count = as_int(rollup.get("db_text_pass_count"))
    current_status = clean(rollup.get("current_status"))
    relevant_markers = has_relevant_markers(profile)

    if not pdf_exists:
        return "asset_missing", "asset", "asset_missing_followup", "pdf_missing_on_disk"
    if not text_exists:
        return "ocr_missing", "coverage_gap", "queue_full_page_ocr", "missing_ocr_text_sidecar"
    if quality in {"empty_text", "tiny_text"}:
        if atom_count:
            return "atoms_from_tiny_ocr", "covered_with_warning", "review_existing_atoms", "atom_exists_but_ocr_text_tiny"
        return "ocr_tiny_recheck", "visual_review", "build_visual_read_packet", "tiny_ocr_text_requires_visual_check"
    if not json_exists:
        return "ocr_text_without_layout", "coverage_gap", "queue_full_page_ocr", "missing_ocr_layout_json"

    if atom_count:
        if high_value_atoms:
            return "covered_high_value_atoms", "covered", "review_existing_atoms", "high_value_atoms_extracted"
        return "covered_context_atoms", "covered", "review_existing_atoms", "context_atoms_extracted"

    if quality in {"weak_text", "weak_confidence"}:
        if relevant_markers:
            return "weak_ocr_relevant_no_atoms", "visual_review", "build_visual_read_packet", "weak_ocr_with_football_or_stat_markers"
        return "weak_ocr_no_markers", "terminal", "terminal_low_signal_no_marker", "weak_ocr_without_football_or_stat_markers"

    if current_status in TERMINAL_NO_ATOM_STATUSES:
        if relevant_markers:
            return "text_has_markers_no_atoms", "visual_review", "build_visual_read_packet", "completed_no_atom_pass_but_text_has_markers"
        return "covered_no_atoms_terminal", "terminal", "terminal_no_current_atoms", "completed_no_atom_pass_without_relevant_markers"

    if region_count:
        if relevant_markers:
            return "article_regions_need_semantic_pass", "semantic_processing", "queue_semantic_reparse", "article_regions_exist_without_atoms"
        return "article_regions_no_markers", "terminal", "terminal_no_current_atoms", "article_regions_exist_without_relevant_markers"

    if text_pass_count:
        return "text_pass_without_regions_or_atoms", "article_processing", "queue_article_atom_conveyor", "text_pass_exists_without_article_regions"

    return "ocr_ready_unprocessed", "article_processing", "queue_article_atom_conveyor", "ocr_ready_no_article_pass"


def build_audit_rows(
    run_id: str,
    created_at: str,
    inventory_rows: list[dict[str, str]],
    rollups: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    audit_rows: list[dict[str, Any]] = []
    for row in inventory_rows:
        candidate_id = clean(row.get("candidate_id"))
        source_document_id = candidate_id
        rollup = rollups.get(source_document_id, {})

        pdf_path = path_from_cell(row.get("pdf_path"))
        pdf_exists = bool(pdf_path and pdf_path.exists() and pdf_path.is_file())
        pdf_bytes_actual = pdf_path.stat().st_size if pdf_exists and pdf_path else 0
        pdf_file_status = "exists" if pdf_exists else "missing"

        text_path = path_from_cell(row.get("suggested_ocr_text_path"))
        json_path = path_from_cell(row.get("suggested_ocr_json_path"))
        sidecar_text, sidecar_chars, sidecar_bytes, sidecar_text_exists = read_text_file(text_path)
        layout_info = parse_layout_info(json_path)
        sidecar_json_exists = bool(layout_info.get("sidecar_json_exists"))
        mean_confidence = as_float(layout_info.get("sidecar_mean_confidence"))
        quality = quality_lane(sidecar_text_exists, sidecar_json_exists, sidecar_chars, mean_confidence)
        profile = keyword_profile(sidecar_text)
        coverage_status, coverage_lane, next_action, reason_code = classify_document(
            pdf_exists,
            sidecar_text_exists,
            sidecar_json_exists,
            quality,
            profile,
            rollup,
        )

        evidence_summary = {
            "pdf_file_status": pdf_file_status,
            "sidecar_quality_lane": quality,
            "db_source_document_exists": bool(as_int(rollup.get("db_source_document_exists"))),
            "db_text_pass_count": as_int(rollup.get("db_text_pass_count")),
            "db_region_count": as_int(rollup.get("db_region_count")),
            "db_atom_count": as_int(rollup.get("db_atom_count")),
            "current_status": clean(rollup.get("current_status")),
            "keyword_terms": profile.get("keyword_terms", ""),
        }

        audit_rows.append({
            "page_coverage_audit_run_id": run_id,
            "source_document_id": source_document_id,
            "candidate_id": candidate_id,
            "boxscore_id": clean(row.get("boxscore_id")),
            "year": clean(row.get("year")),
            "game_date": clean(row.get("game_date")),
            "away_team": clean(row.get("away_team")),
            "home_team": clean(row.get("home_team")),
            "publication": clean(row.get("publication")),
            "result_date": clean(row.get("result_date")),
            "page": clean(row.get("page")),
            "candidate_rank": clean(row.get("candidate_rank")),
            "image_id": clean(row.get("image_id")),
            "round_name": clean(row.get("round_name")),
            "pdf_path": clean(row.get("pdf_path")),
            "pdf_exists": pdf_exists,
            "pdf_bytes_plan": clean(row.get("pdf_bytes")),
            "pdf_bytes_actual": pdf_bytes_actual,
            "pdf_file_status": pdf_file_status,
            "source_url": clean(row.get("source_url")) or clean(rollup.get("db_source_url")),
            "sidecar_text_path": clean(row.get("suggested_ocr_text_path")),
            "sidecar_json_path": clean(row.get("suggested_ocr_json_path")),
            "sidecar_text_exists": sidecar_text_exists,
            "sidecar_json_exists": sidecar_json_exists,
            "sidecar_text_chars_plan": clean(row.get("extractable_text_chars")) or clean(row.get("ocr_chars")),
            "sidecar_text_chars_actual": sidecar_chars,
            "sidecar_text_bytes": sidecar_bytes,
            "sidecar_json_bytes": layout_info.get("sidecar_json_bytes", 0),
            "sidecar_mean_confidence": mean_confidence if mean_confidence is not None else "",
            "sidecar_page_count": layout_info.get("sidecar_page_count", ""),
            "sidecar_quality_lane": quality,
            "football_keyword_score": profile.get("football_keyword_score", 0),
            "football_keyword_hits": profile.get("football_keyword_hits", 0),
            "stat_keyword_hits": profile.get("stat_keyword_hits", 0),
            "strong_keyword_hits": profile.get("strong_keyword_hits", 0),
            "keyword_terms": profile.get("keyword_terms", ""),
            "keyword_excerpt": profile.get("keyword_excerpt", ""),
            "db_source_document_exists": bool(as_int(rollup.get("db_source_document_exists"))),
            "db_text_pass_count": as_int(rollup.get("db_text_pass_count")),
            "db_full_page_text_pass_count": as_int(rollup.get("db_full_page_text_pass_count")),
            "db_article_text_pass_count": as_int(rollup.get("db_article_text_pass_count")),
            "db_text_chars_total": as_int(rollup.get("db_text_chars_total")),
            "db_text_chars_max": as_int(rollup.get("db_text_chars_max")),
            "db_region_count": as_int(rollup.get("db_region_count")),
            "db_region_text_count": as_int(rollup.get("db_region_text_count")),
            "db_region_low_signal_count": as_int(rollup.get("db_region_low_signal_count")),
            "db_region_quality_score_avg": rollup.get("db_region_quality_score_avg", ""),
            "db_atom_count": as_int(rollup.get("db_atom_count")),
            "db_high_value_atom_count": as_int(rollup.get("db_high_value_atom_count")),
            "db_game_candidate_atoms": as_int(rollup.get("db_game_candidate_atoms")),
            "db_lineup_atoms": as_int(rollup.get("db_lineup_atoms")),
            "db_scoring_event_atoms": as_int(rollup.get("db_scoring_event_atoms")),
            "db_pbp_event_atoms": as_int(rollup.get("db_pbp_event_atoms")),
            "db_player_stat_atoms": as_int(rollup.get("db_player_stat_atoms")),
            "db_team_stat_atoms": as_int(rollup.get("db_team_stat_atoms")),
            "db_promotion_candidate_count": as_int(rollup.get("db_promotion_candidate_count")),
            "current_station": clean(rollup.get("current_station")),
            "current_status": clean(rollup.get("current_status")),
            "state_next_action": clean(rollup.get("state_next_action")),
            "terminal_status": clean(rollup.get("terminal_status")),
            "terminal_reason": clean(rollup.get("terminal_reason")),
            "coverage_status": coverage_status,
            "coverage_lane": coverage_lane,
            "recommended_next_action": next_action,
            "reason_code": reason_code,
            "evidence_summary_json": evidence_summary,
            "created_at_utc": created_at,
        })
    return audit_rows


def build_rollup(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    counts = Counter(clean(row.get(field)) for row in rows)
    return [{field: key, "document_count": value} for key, value in sorted(counts.items())]


def build_year_rollup(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        year = clean(row.get("year")) or "unknown"
        grouped[year]["documents"] += 1
        grouped[year][f"action::{clean(row.get('recommended_next_action'))}"] += 1
        grouped[year][f"lane::{clean(row.get('coverage_lane'))}"] += 1
    output: list[dict[str, Any]] = []
    action_names = sorted({key.split("::", 1)[1] for counter in grouped.values() for key in counter if key.startswith("action::")})
    lane_names = sorted({key.split("::", 1)[1] for counter in grouped.values() for key in counter if key.startswith("lane::")})
    for year, counter in sorted(grouped.items()):
        record: dict[str, Any] = {"year": year, "documents": counter["documents"]}
        for action in action_names:
            record[f"action_{action}"] = counter[f"action::{action}"]
        for lane in lane_names:
            record[f"lane_{lane}"] = counter[f"lane::{lane}"]
        output.append(record)
    return output


def top_visual_candidates(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    candidates = [
        row for row in rows
        if clean(row.get("recommended_next_action")) == "build_visual_read_packet"
    ]
    candidates.sort(
        key=lambda row: (
            -as_int(row.get("football_keyword_score")),
            -as_int(row.get("sidecar_text_chars_actual")),
            clean(row.get("boxscore_id")),
            as_int(row.get("candidate_rank")),
        )
    )
    if limit <= 0:
        return candidates
    return candidates[:limit]


def render_markdown(summary: dict[str, Any], samples: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Page Coverage Audit",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Run: `{summary['page_coverage_audit_run_id']}`",
        f"Document inventory: `{summary['document_inventory']}`",
        f"Output: `{summary['output_dir']}`",
        "",
        "## Counts",
        "",
        f"- Planned documents: `{summary['planned_document_count']}`",
        f"- Documents with atoms: `{summary['documents_with_atoms']}`",
        f"- Documents with high-value atoms: `{summary['documents_with_high_value_atoms']}`",
        f"- Need OCR: `{summary['documents_needing_ocr']}`",
        f"- Need article pass: `{summary['documents_needing_article_pass']}`",
        f"- Need semantic reparse: `{summary['documents_needing_semantic_reparse']}`",
        f"- Need visual read/re-OCR packet: `{summary['documents_needing_visual_read']}`",
        f"- Terminal no-current-atom docs: `{summary['documents_terminal_no_current_atoms']}`",
        f"- Certified no word left behind: `{summary['certified_no_word_left_behind']}`",
        "",
        "## Recommended Actions",
        "",
    ]
    for key, value in summary["recommended_next_action_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Coverage Statuses", ""])
    for key, value in summary["coverage_status_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    if samples:
        lines.extend(["", "## Highest-Priority Visual Candidates", ""])
        for row in samples[:10]:
            lines.append(
                "- "
                f"`{row.get('source_document_id')}` "
                f"{row.get('publication')} p{row.get('page')} "
                f"score `{row.get('football_keyword_score')}` "
                f"reason `{row.get('reason_code')}`"
            )
    lines.extend([
        "",
        "## Outputs",
        "",
        "- `page_coverage_audit.csv`: one row per planned document.",
        "- `queue_full_page_ocr_manifest.csv`: missing/tiny OCR handoff.",
        "- `queue_article_atom_conveyor_manifest.csv`: OCR-ready docs not yet article-processed.",
        "- `queue_semantic_reparse_manifest.csv`: article text/regions requiring semantic pass.",
        "- `visual_read_packet_candidates.csv`: weak/no-atom OCR with football/stat markers.",
        "- `covered_with_atoms.csv`: documents that have raw atom claims.",
        "- `terminal_no_current_atoms.csv`: completed no-atom/low-signal docs without markers.",
        "",
        "Live Fly/v26/supertable writes: no.",
        "",
    ])
    return "\n".join(lines)


def persist(db_path: Path, run_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS newspaper_review.page_coverage_audit_run (
              page_coverage_audit_run_id VARCHAR,
              label VARCHAR,
              document_inventory VARCHAR,
              db_path VARCHAR,
              output_dir VARCHAR,
              planned_document_count INTEGER,
              coverage_status_counts_json VARCHAR,
              recommended_next_action_counts_json VARCHAR,
              coverage_lane_counts_json VARCHAR,
              sidecar_quality_lane_counts_json VARCHAR,
              documents_with_atoms INTEGER,
              documents_with_high_value_atoms INTEGER,
              documents_needing_ocr INTEGER,
              documents_needing_article_pass INTEGER,
              documents_needing_semantic_reparse INTEGER,
              documents_needing_visual_read INTEGER,
              documents_terminal_no_current_atoms INTEGER,
              certified_no_word_left_behind BOOLEAN,
              summary_json_path VARCHAR,
              report_md_path VARCHAR,
              created_at_utc TIMESTAMP
            )
            """
        )
        item_defs = ",\n              ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS newspaper_review.page_coverage_audit_item (
              {item_defs}
            )
            """
        )
        run_id = clean(run_row.get("page_coverage_audit_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.page_coverage_audit_run WHERE page_coverage_audit_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.page_coverage_audit_item WHERE page_coverage_audit_run_id = ?",
            [run_id],
        )
        run_placeholders = ",".join(["?"] * len(RUN_FIELDS))
        con.execute(
            f"INSERT INTO newspaper_review.page_coverage_audit_run ({','.join(RUN_FIELDS)}) VALUES ({run_placeholders})",
            [run_row.get(field) for field in RUN_FIELDS],
        )
        if item_rows:
            item_placeholders = ",".join(["?"] * len(ITEM_FIELDS))
            con.executemany(
                f"INSERT INTO newspaper_review.page_coverage_audit_item ({','.join(ITEM_FIELDS)}) VALUES ({item_placeholders})",
                [[safe_cell(row.get(field)) for field in ITEM_FIELDS] for row in item_rows],
            )
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--document-inventory", type=Path, default=None)
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="1920_1939_page_coverage_audit_v1")
    parser.add_argument("--visual-sample-limit", type=int, default=0, help="0 means write every visual candidate.")
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    document_inventory = args.document_inventory or default_document_inventory(args.plan_root)
    inventory_rows = read_csv(document_inventory)
    rollups, db_warnings = load_review_rollups(args.db_path)
    audit_rows = build_audit_rows(run_id, created_at, inventory_rows, rollups)

    coverage_status_counts = Counter(clean(row.get("coverage_status")) for row in audit_rows)
    action_counts = Counter(clean(row.get("recommended_next_action")) for row in audit_rows)
    lane_counts = Counter(clean(row.get("coverage_lane")) for row in audit_rows)
    quality_counts = Counter(clean(row.get("sidecar_quality_lane")) for row in audit_rows)

    documents_needing_ocr = (
        action_counts["queue_full_page_ocr"]
        + action_counts["queue_full_page_ocr_layout_json"]
    )
    documents_needing_article_pass = action_counts["queue_article_atom_conveyor"]
    documents_needing_semantic_reparse = action_counts["queue_semantic_reparse"]
    documents_needing_visual_read = action_counts["build_visual_read_packet"]
    documents_with_atoms = sum(1 for row in audit_rows if as_int(row.get("db_atom_count")) > 0)
    documents_with_high_value_atoms = sum(1 for row in audit_rows if as_int(row.get("db_high_value_atom_count")) > 0)
    documents_terminal_no_current_atoms = action_counts["terminal_no_current_atoms"] + action_counts["terminal_low_signal_no_marker"]
    certified_no_word_left_behind = (
        documents_needing_ocr == 0
        and documents_needing_article_pass == 0
        and documents_needing_semantic_reparse == 0
        and documents_needing_visual_read == 0
    )

    audit_csv = out_dir / "page_coverage_audit.csv"
    write_csv(audit_csv, audit_rows, ITEM_FIELDS)
    write_csv(
        out_dir / "queue_full_page_ocr_manifest.csv",
        [row for row in audit_rows if clean(row.get("recommended_next_action")) == "queue_full_page_ocr"],
        ITEM_FIELDS,
    )
    write_csv(
        out_dir / "queue_article_atom_conveyor_manifest.csv",
        [row for row in audit_rows if clean(row.get("recommended_next_action")) == "queue_article_atom_conveyor"],
        ITEM_FIELDS,
    )
    write_csv(
        out_dir / "queue_semantic_reparse_manifest.csv",
        [row for row in audit_rows if clean(row.get("recommended_next_action")) == "queue_semantic_reparse"],
        ITEM_FIELDS,
    )
    visual_candidates = top_visual_candidates(audit_rows, args.visual_sample_limit)
    write_csv(out_dir / "visual_read_packet_candidates.csv", visual_candidates, ITEM_FIELDS)
    write_csv(
        out_dir / "covered_with_atoms.csv",
        [row for row in audit_rows if as_int(row.get("db_atom_count")) > 0],
        ITEM_FIELDS,
    )
    write_csv(
        out_dir / "terminal_no_current_atoms.csv",
        [
            row for row in audit_rows
            if clean(row.get("recommended_next_action")) in {"terminal_no_current_atoms", "terminal_low_signal_no_marker"}
        ],
        ITEM_FIELDS,
    )
    write_csv(out_dir / "coverage_status_rollup.csv", build_rollup(audit_rows, "coverage_status"), ["coverage_status", "document_count"])
    write_csv(out_dir / "recommended_next_action_rollup.csv", build_rollup(audit_rows, "recommended_next_action"), ["recommended_next_action", "document_count"])
    year_rollup = build_year_rollup(audit_rows)
    year_fields = list(year_rollup[0].keys()) if year_rollup else ["year", "documents"]
    write_csv(out_dir / "coverage_by_year.csv", year_rollup, year_fields)

    summary = {
        "created_at_utc": created_at,
        "page_coverage_audit_run_id": run_id,
        "label": args.label,
        "document_inventory": str(document_inventory),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "planned_document_count": len(audit_rows),
        "coverage_status_counts": dict(sorted(coverage_status_counts.items())),
        "recommended_next_action_counts": dict(sorted(action_counts.items())),
        "coverage_lane_counts": dict(sorted(lane_counts.items())),
        "sidecar_quality_lane_counts": dict(sorted(quality_counts.items())),
        "documents_with_atoms": documents_with_atoms,
        "documents_with_high_value_atoms": documents_with_high_value_atoms,
        "documents_needing_ocr": documents_needing_ocr,
        "documents_needing_article_pass": documents_needing_article_pass,
        "documents_needing_semantic_reparse": documents_needing_semantic_reparse,
        "documents_needing_visual_read": documents_needing_visual_read,
        "documents_terminal_no_current_atoms": documents_terminal_no_current_atoms,
        "certified_no_word_left_behind": certified_no_word_left_behind,
        "db_warnings": db_warnings,
        "audit_csv": str(audit_csv),
        "summary_json_path": str(out_dir / "summary.json"),
        "report_md_path": str(out_dir / "page_coverage_audit_report.md"),
        "persisted_to_duckdb": not args.no_persist,
        "live_tables_touched": False,
    }
    write_json(out_dir / "summary.json", summary)
    (out_dir / "page_coverage_audit_report.md").write_text(
        render_markdown(summary, visual_candidates),
        encoding="utf-8",
    )

    run_row = {
        "page_coverage_audit_run_id": run_id,
        "label": args.label,
        "document_inventory": str(document_inventory),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "planned_document_count": len(audit_rows),
        "coverage_status_counts_json": json.dumps(summary["coverage_status_counts"], sort_keys=True),
        "recommended_next_action_counts_json": json.dumps(summary["recommended_next_action_counts"], sort_keys=True),
        "coverage_lane_counts_json": json.dumps(summary["coverage_lane_counts"], sort_keys=True),
        "sidecar_quality_lane_counts_json": json.dumps(summary["sidecar_quality_lane_counts"], sort_keys=True),
        "documents_with_atoms": documents_with_atoms,
        "documents_with_high_value_atoms": documents_with_high_value_atoms,
        "documents_needing_ocr": documents_needing_ocr,
        "documents_needing_article_pass": documents_needing_article_pass,
        "documents_needing_semantic_reparse": documents_needing_semantic_reparse,
        "documents_needing_visual_read": documents_needing_visual_read,
        "documents_terminal_no_current_atoms": documents_terminal_no_current_atoms,
        "certified_no_word_left_behind": certified_no_word_left_behind,
        "summary_json_path": str(out_dir / "summary.json"),
        "report_md_path": str(out_dir / "page_coverage_audit_report.md"),
        "created_at_utc": created_at,
    }
    if not args.no_persist:
        persist(args.db_path, run_row, audit_rows)

    print(json.dumps({
        "page_coverage_audit_run_id": run_id,
        "output_dir": str(out_dir),
        "planned_document_count": len(audit_rows),
        "recommended_next_action_counts": summary["recommended_next_action_counts"],
        "coverage_status_counts": summary["coverage_status_counts"],
        "documents_with_atoms": documents_with_atoms,
        "documents_with_high_value_atoms": documents_with_high_value_atoms,
        "documents_needing_ocr": documents_needing_ocr,
        "documents_needing_article_pass": documents_needing_article_pass,
        "documents_needing_semantic_reparse": documents_needing_semantic_reparse,
        "documents_needing_visual_read": documents_needing_visual_read,
        "certified_no_word_left_behind": certified_no_word_left_behind,
        "report_md": str(out_dir / "page_coverage_audit_report.md"),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
