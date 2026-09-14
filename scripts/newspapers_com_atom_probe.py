#!/usr/bin/env python
"""Probe Newspapers.com captures for extractable v26-style stat atoms.

This is intentionally a shallow, rotation-friendly pass. It measures how much
usable evidence we have, attempts cheap text/regex extraction when OCR-like text
exists, and writes D-drive artifacts that match the v26 supertable shape closely
enough for later promotion/review work.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import duckdb
except Exception:  # noqa: BLE001 - report as a pipeline capability gap.
    duckdb = None

try:
    from pypdf import PdfReader
except Exception:  # noqa: BLE001 - report as a pipeline capability gap.
    PdfReader = None


RELEASES_ROOT = Path(r"D:\league-history-data\nfl\releases")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")

CHECKPOINT_CANDIDATE_SUFFIX = "_candidate_checkpoint.csv"
CHECKPOINT_GAME_SUFFIX = "_game_checkpoint.csv"

IDENTITY_COLUMNS = {
    "player",
    "NFL_player_id",
    "player_week",
    "year",
    "week",
    "game_date",
    "season_type",
    "position",
    "nfl_position",
    "fantasy_position",
    "nfl_team",
    "opponent_nfl_team",
    "nfl_franchise_number",
    "opponent_nfl_franchise_number",
    "headshot_url",
    "data_source",
    "home_away",
    "age",
    "is_starter",
    "starter_position",
    "recon_correction_log",
}

DERIVED_PREFIXES = (
    "pts_",
    "fpts_",
    "rank_",
    "rank_season_",
    "rank_alltime_",
    "ppg_",
    "rolling_",
    "consistency_",
    "weighted_",
    "avg_pts_",
    "lamar_",
    "bonus_",
)

DERIVED_NAME_PATTERNS = (
    re.compile(r".*_recomputed_at_\d+$"),
    re.compile(r".*_repaired_at_\d+$"),
    re.compile(r".*_populated_at_\d+$"),
    re.compile(r".*_merged_at_\d+$"),
    re.compile(r".*_canonical$"),
    re.compile(r".*_pct$"),
    re.compile(r".*_share$"),
    re.compile(r".*_per_.*"),
)

TEXT_ATOM_COLUMNS = {
    "pick6",
    "sfty",
    "fgm",
    "2pm",
    "three_out",
    "fourth_down_stop",
    "rate",
    "pts",
    "timeouts",
    "fg%",
    "xp%",
}

GAME_ID_COLUMNS = [
    "player",
    "NFL_player_id",
    "player_week",
    "year",
    "week",
    "game_date",
    "season_type",
    "position",
    "nfl_team",
    "opponent_nfl_team",
    "nfl_franchise_number",
    "opponent_nfl_franchise_number",
    "data_source",
]

STAT_KEYWORDS = {
    "box score": r"\bbox\s*score\b",
    "nfl summaries": r"\bnfl\s+summar(?:y|ies)\b",
    "team statistics": r"\bteam\s+statistics\b",
    "statistics": r"\bstatistics\b",
    "passing": r"\bpassing\b",
    "rushing": r"\brushing\b",
    "receiving": r"\breceiving\b",
    "field goal": r"\bfield goals?\b",
    "first downs": r"\bfirst downs?\b",
    "fumbles": r"\bfumbles?\b",
    "interceptions": r"\binterceptions?\b",
    "penalties": r"\bpenalties\b",
}

ATOM_REGEXES = [
    (
        "passing_yards",
        re.compile(r"\b(?:passing yards|yards passing|net yards passing|pass(?:ing)? yds?)\b\D{0,30}(?P<value>-?\d+(?:\.\d+)?)", re.I),
    ),
    (
        "rushing_yards",
        re.compile(r"\b(?:rushing yards|yards rushing|rush(?:ing)? yds?)\b\D{0,30}(?P<value>-?\d+(?:\.\d+)?)", re.I),
    ),
    (
        "receiving_yards",
        re.compile(r"\b(?:receiving yards|yards receiving|rec(?:eiving)? yds?)\b\D{0,30}(?P<value>-?\d+(?:\.\d+)?)", re.I),
    ),
    ("passing_tds", re.compile(r"\b(?:passing tds?|td passes|touchdown passes)\b\D{0,30}(?P<value>\d+)", re.I)),
    ("rushing_tds", re.compile(r"\b(?:rushing tds?|td runs|touchdown runs)\b\D{0,30}(?P<value>\d+)", re.I)),
    ("receiving_tds", re.compile(r"\b(?:receiving tds?|td catches|touchdown catches)\b\D{0,30}(?P<value>\d+)", re.I)),
    ("receptions", re.compile(r"\b(?:receptions|catches)\b\D{0,30}(?P<value>\d+)", re.I)),
    ("carries", re.compile(r"\b(?:carries|rushes)\b\D{0,30}(?P<value>\d+)", re.I)),
    ("fumbles", re.compile(r"\bfumbles?\b\D{0,30}(?P<value>\d+)", re.I)),
    ("fumbles_lost", re.compile(r"\bfumbles?\s*[-/]\s*lost\b\D{0,30}(?P<value>\d+)", re.I)),
    ("penalties", re.compile(r"\bpenalties\b\D{0,30}(?P<value>\d+)", re.I)),
    ("penalty_yards", re.compile(r"\bpenalt(?:y|ies)\s*(?:yards|yds?)\b\D{0,30}(?P<value>\d+)", re.I)),
    ("def_sacks", re.compile(r"\bsacks?\b\D{0,30}(?P<value>\d+)", re.I)),
    ("def_interceptions", re.compile(r"\binterceptions?\b\D{0,30}(?P<value>\d+)", re.I)),
    ("points_allowed", re.compile(r"\bpoints allowed\b\D{0,30}(?P<value>\d+)", re.I)),
    ("total_yds_allowed", re.compile(r"\b(?:total yards allowed|yards allowed)\b\D{0,30}(?P<value>\d+)", re.I)),
]

FG_MADE_ATT_RE = re.compile(
    r"\b(?:field goals?|fg)\b\D{0,20}(?P<made>\d+)\s*[-/]\s*(?P<att>\d+)",
    re.I,
)
PAT_MADE_ATT_RE = re.compile(
    r"\b(?:extra points?|pat|xp)\b\D{0,20}(?P<made>\d+)\s*[-/]\s*(?P<att>\d+)",
    re.I,
)
COMP_ATT_INT_RE = re.compile(
    r"\b(?:comp(?:letions)?[-/ ]att(?:empts)?[-/ ]int|passing)\b\D{0,20}(?P<comp>\d+)\s*[-/]\s*(?P<att>\d+)\s*[-/]\s*(?P<int>\d+)",
    re.I,
)


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def latest_v26_table(releases_root: Path = RELEASES_ROOT) -> Path:
    matches = sorted(releases_root.glob("*_v26/tables/nfl_player_stats_all.parquet"))
    if not matches:
        raise FileNotFoundError(f"no v26 nfl_player_stats_all.parquet found under {releases_root}")
    return matches[-1]


def is_numeric_type(dtype: str) -> bool:
    upper = dtype.upper()
    return any(token in upper for token in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "INTEGER", "BIGINT", "UBIGINT", "SMALLINT", "TINYINT"))


def column_family(name: str) -> str:
    lower = name.lower()
    if lower.startswith("passing_") or lower in {"attempts", "completions", "sacks_suffered", "sack_yards_lost", "passer_rating", "completion_pct"}:
        return "passing"
    if lower.startswith("rushing_") or lower == "carries":
        return "rushing"
    if lower.startswith("receiving_") or lower in {"receptions", "targets"}:
        return "receiving"
    if "fumble" in lower or lower.startswith("fum_"):
        return "fumbles"
    if lower.startswith("fg_") or lower.startswith("pat_") or lower.startswith("gwfg_"):
        return "kicking"
    if lower.startswith("def_"):
        return "defense"
    if lower.startswith("pts_allow") or lower in {"points_allowed", "dst_points_allowed", "rushing_yds_allowed", "passing_yds_allowed", "total_yds_allowed"}:
        return "team_defense"
    if "return" in lower or lower.startswith("ret_") or lower.startswith("kickoff_") or lower.startswith("punt_return"):
        return "returns"
    if lower.startswith("punt") or lower == "punts":
        return "punting"
    if "snap" in lower or lower == "is_starter":
        return "usage"
    if lower in {"penalties", "penalty_yards", "misc_yards", "special_teams_tds"}:
        return "misc"
    return "other"


def classify_column(name: str, dtype: str) -> str:
    lower = name.lower()
    if name in IDENTITY_COLUMNS:
        return "identity"
    if name in TEXT_ATOM_COLUMNS:
        return "raw_atom_text"
    if lower.startswith(DERIVED_PREFIXES):
        return "derived"
    if any(pattern.fullmatch(name) or pattern.match(name) for pattern in DERIVED_NAME_PATTERNS):
        return "derived"
    if not is_numeric_type(dtype):
        return "provenance_or_text"
    return "raw_atom_numeric"


def describe_v26(v26_table: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if duckdb is None:
        raise RuntimeError("duckdb is not importable; cannot inspect v26 parquet schema")
    con = duckdb.connect()
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_string(v26_table.as_posix())})").fetchall()
    catalog: list[dict[str, Any]] = []
    raw_atoms: list[str] = []
    for ordinal, row in enumerate(rows, start=1):
        name = row[0]
        dtype = row[1]
        classification = classify_column(name, dtype)
        family = column_family(name)
        is_raw = classification in {"raw_atom_numeric", "raw_atom_text"}
        if is_raw:
            raw_atoms.append(name)
        catalog.append(
            {
                "ordinal": ordinal,
                "column_name": name,
                "duckdb_type": dtype,
                "classification": classification,
                "family": family,
                "included_in_atom_probe": is_raw,
            }
        )
    return catalog, raw_atoms


def find_candidate_checkpoint(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    matches = sorted(path.glob(f"*{CHECKPOINT_CANDIDATE_SUFFIX}"))
    if matches:
        return matches
    return sorted(path.rglob(f"*{CHECKPOINT_CANDIDATE_SUFFIX}"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fallback_fields: list[str]) -> None:
    fields: list[str]
    if rows:
        seen: dict[str, None] = {}
        for row in rows:
            for key in row:
                seen.setdefault(key, None)
        fields = list(seen)
    else:
        fields = fallback_fields
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_candidates(inputs: list[Path], max_candidates: int | None = None) -> tuple[list[dict[str, str]], list[Path]]:
    checkpoint_files: list[Path] = []
    for item in inputs:
        checkpoint_files.extend(find_candidate_checkpoint(item))
    checkpoint_files = sorted(dict.fromkeys(checkpoint_files))
    if not checkpoint_files:
        raise FileNotFoundError("no candidate checkpoint CSVs found")

    candidates: list[dict[str, str]] = []
    for checkpoint in checkpoint_files:
        for row in read_csv(checkpoint):
            row = dict(row)
            row["candidate_checkpoint"] = str(checkpoint)
            candidates.append(row)
            if max_candidates and len(candidates) >= max_candidates:
                return candidates, checkpoint_files
    return candidates, checkpoint_files


def short_context(text: str, start: int, end: int, radius: int = 90) -> str:
    return re.sub(r"\s+", " ", text[max(0, start - radius) : min(len(text), end + radius)]).strip()


def read_json_text(path: Path) -> str:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return ""
    snippets: list[str] = []
    if isinstance(data, dict):
        for key in ("resultText", "bodyLead", "title", "canonicalUrl", "publication", "sourcePageTitle"):
            value = data.get(key)
            if isinstance(value, str):
                snippets.append(value)
    elif isinstance(data, list):
        snippets.append(json.dumps(data[:3], ensure_ascii=False))
    return "\n".join(snippets)


def collect_sidecar_text(result_root: Path, char_limit: int) -> tuple[str, list[str]]:
    snippets: list[str] = []
    sources: list[str] = []
    if not result_root.exists():
        return "", sources

    for text_file in sorted(result_root.rglob("*")):
        if not text_file.is_file():
            continue
        lower_name = text_file.name.lower()
        if text_file.suffix.lower() == ".txt" or "ocr" in lower_name or "transcript" in lower_name:
            try:
                value = text_file.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if value.strip():
                snippets.append(value[:char_limit])
                sources.append(str(text_file))

    for json_name in ("source_meta.json", "viewer_meta.json"):
        json_path = result_root / json_name
        if json_path.exists():
            value = read_json_text(json_path)
            if value.strip():
                snippets.append(value)
                sources.append(str(json_path))

    combined = "\n".join(snippets)
    return combined[:char_limit], sources


def extract_pdf_text(pdf_path: Path, char_limit: int) -> tuple[str, dict[str, Any]]:
    meta = {"pdf_exists": pdf_path.exists(), "pdf_pages": "", "pdf_text_error": ""}
    if not pdf_path.exists():
        meta["pdf_text_error"] = "missing_pdf"
        return "", meta
    if PdfReader is None:
        meta["pdf_text_error"] = "pypdf_not_importable"
        return "", meta
    try:
        reader = PdfReader(str(pdf_path))
        meta["pdf_pages"] = len(reader.pages)
        chunks = []
        for page in reader.pages:
            chunks.append(page.extract_text() or "")
            if sum(len(chunk) for chunk in chunks) >= char_limit:
                break
        return "\n".join(chunks)[:char_limit], meta
    except Exception as exc:  # noqa: BLE001 - candidate coverage should record failures.
        meta["pdf_text_error"] = str(exc)
        return "", meta


def count_keywords(text: str) -> tuple[int, str]:
    hits = [name for name, pattern in STAT_KEYWORDS.items() if re.search(pattern, text, re.I)]
    return len(hits), "|".join(hits)


def is_viewer_only_pdf_text(text: str) -> bool:
    if not text.strip():
        return True
    normalized = re.sub(r"\s+", " ", text).lower()
    viewer_markers = [
        "find text on this page",
        "clip print/download",
        "save to ancestry",
        "newspapers.com",
        "page:",
    ]
    marker_count = sum(1 for marker in viewer_markers if marker in normalized)
    keyword_count, _ = count_keywords(text)
    return len(text) < 600 and marker_count >= 2 and keyword_count == 0


def extract_atoms_from_text(
    candidate: dict[str, str],
    text: str,
    raw_atom_set: set[str],
    max_hits_per_atom: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = {
        "boxscore_id": candidate.get("boxscore_id", ""),
        "candidate_rank": candidate.get("candidate_rank", ""),
        "image_id": candidate.get("image_id", ""),
        "publication": candidate.get("publication", ""),
        "result_date": candidate.get("result_date", ""),
        "page": candidate.get("page", ""),
        "candidate_checkpoint": candidate.get("candidate_checkpoint", ""),
    }
    per_atom_counts: Counter[str] = Counter()

    for atom, pattern in ATOM_REGEXES:
        if atom not in raw_atom_set:
            continue
        for match in pattern.finditer(text):
            if per_atom_counts[atom] >= max_hits_per_atom:
                break
            value = match.group("value")
            rows.append(
                {
                    **base,
                    "atom": atom,
                    "value": value,
                    "entity_scope": "unknown_text_context",
                    "confidence": "low_regex",
                    "extractor": "label_value_regex",
                    "context": short_context(text, match.start(), match.end()),
                }
            )
            per_atom_counts[atom] += 1

    for match in FG_MADE_ATT_RE.finditer(text):
        for atom, group in (("fg_made", "made"), ("fg_att", "att")):
            if atom in raw_atom_set and per_atom_counts[atom] < max_hits_per_atom:
                rows.append(
                    {
                        **base,
                        "atom": atom,
                        "value": match.group(group),
                        "entity_scope": "unknown_text_context",
                        "confidence": "low_regex",
                        "extractor": "made_attempt_regex",
                        "context": short_context(text, match.start(), match.end()),
                    }
                )
                per_atom_counts[atom] += 1

    for match in PAT_MADE_ATT_RE.finditer(text):
        for atom, group in (("pat_made", "made"), ("pat_att", "att")):
            if atom in raw_atom_set and per_atom_counts[atom] < max_hits_per_atom:
                rows.append(
                    {
                        **base,
                        "atom": atom,
                        "value": match.group(group),
                        "entity_scope": "unknown_text_context",
                        "confidence": "low_regex",
                        "extractor": "made_attempt_regex",
                        "context": short_context(text, match.start(), match.end()),
                    }
                )
                per_atom_counts[atom] += 1

    for match in COMP_ATT_INT_RE.finditer(text):
        for atom, group in (("completions", "comp"), ("attempts", "att"), ("passing_interceptions", "int")):
            if atom in raw_atom_set and per_atom_counts[atom] < max_hits_per_atom:
                rows.append(
                    {
                        **base,
                        "atom": atom,
                        "value": match.group(group),
                        "entity_scope": "unknown_text_context",
                        "confidence": "low_regex",
                        "extractor": "comp_att_int_regex",
                        "context": short_context(text, match.start(), match.end()),
                    }
                )
                per_atom_counts[atom] += 1

    return rows


def analyze_candidates(
    candidates: list[dict[str, str]],
    raw_atom_columns: list[str],
    text_char_limit: int,
    max_hits_per_atom: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    raw_atom_set = set(raw_atom_columns)
    coverage_rows: list[dict[str, Any]] = []
    atom_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []

    for idx, candidate in enumerate(candidates, start=1):
        start = time.monotonic()
        pdf_path = Path(candidate.get("pdf_path") or "")
        result_root = Path(candidate.get("result_root") or "")
        pdf_text, pdf_meta = extract_pdf_text(pdf_path, text_char_limit)
        sidecar_text, sidecar_sources = collect_sidecar_text(result_root, text_char_limit)

        ocr_like_text = ""
        if result_root.exists():
            for text_file in sorted(result_root.rglob("*")):
                lower_name = text_file.name.lower()
                if text_file.is_file() and ("ocr" in lower_name or "transcript" in lower_name or text_file.suffix.lower() == ".txt"):
                    try:
                        value = text_file.read_text(encoding="utf-8", errors="ignore")
                    except OSError:
                        continue
                    if len(value.strip()) > len(ocr_like_text):
                        ocr_like_text = value[:text_char_limit]

        pdf_viewer_only = is_viewer_only_pdf_text(pdf_text)
        if ocr_like_text.strip():
            extractable_text = ocr_like_text
            best_text_source = "ocr_or_text_sidecar"
            text_quality = "ocr_available"
        elif pdf_text.strip() and not pdf_viewer_only:
            extractable_text = pdf_text
            best_text_source = "pdf_embedded_text"
            text_quality = "pdf_extractable"
        else:
            extractable_text = ""
            best_text_source = "metadata_only" if sidecar_text.strip() else "none"
            text_quality = "viewer_only_or_no_ocr" if pdf_text.strip() or sidecar_text.strip() else "no_text"

        combined_for_keywords = "\n".join(part for part in (extractable_text, sidecar_text, pdf_text) if part)
        keyword_count, keyword_terms = count_keywords(combined_for_keywords)
        extracted = extract_atoms_from_text(candidate, extractable_text, raw_atom_set, max_hits_per_atom) if extractable_text else []
        atom_rows.extend(extracted)

        extracted_by_atom: dict[str, Any] = {}
        for row in extracted:
            extracted_by_atom.setdefault(row["atom"], row["value"])

        elapsed_ms = round((time.monotonic() - start) * 1000)
        candidate_id = f"{candidate.get('boxscore_id', '')}#{candidate.get('candidate_rank', '')}:{candidate.get('image_id', '')}"
        coverage = {
            "candidate_index": idx,
            "candidate_id": candidate_id,
            "boxscore_id": candidate.get("boxscore_id", ""),
            "candidate_rank": candidate.get("candidate_rank", ""),
            "year": candidate.get("year", ""),
            "game_date": candidate.get("game_date", ""),
            "away_team": candidate.get("away_team", ""),
            "home_team": candidate.get("home_team", ""),
            "status": candidate.get("status", ""),
            "image_id": candidate.get("image_id", ""),
            "publication": candidate.get("publication", ""),
            "result_date": candidate.get("result_date", ""),
            "page": candidate.get("page", ""),
            "pdf_path": str(pdf_path) if str(pdf_path) != "." else "",
            "pdf_exists": pdf_meta["pdf_exists"],
            "pdf_pages": pdf_meta["pdf_pages"],
            "pdf_text_chars": len(pdf_text),
            "pdf_viewer_only": pdf_viewer_only,
            "pdf_text_error": pdf_meta["pdf_text_error"],
            "sidecar_text_chars": len(sidecar_text),
            "sidecar_sources_count": len(sidecar_sources),
            "best_text_source": best_text_source,
            "text_quality": text_quality,
            "extractable_text_chars": len(extractable_text),
            "stat_keyword_hits": keyword_count,
            "stat_keyword_terms": keyword_terms,
            "extracted_atom_count": len(extracted),
            "extracted_atom_names": "|".join(sorted(extracted_by_atom)),
            "elapsed_ms": elapsed_ms,
            "result_root": str(result_root) if str(result_root) != "." else "",
            "candidate_checkpoint": candidate.get("candidate_checkpoint", ""),
        }
        coverage_rows.append(coverage)

        wide = {
            "candidate_id": candidate_id,
            "boxscore_id": coverage["boxscore_id"],
            "candidate_rank": coverage["candidate_rank"],
            "image_id": coverage["image_id"],
            "publication": coverage["publication"],
            "result_date": coverage["result_date"],
            "page": coverage["page"],
            "text_quality": text_quality,
            "extracted_atom_count": len(extracted),
        }
        for atom in raw_atom_columns:
            wide[atom] = extracted_by_atom.get(atom, "")
        wide_rows.append(wide)

    return coverage_rows, atom_rows, wide_rows


def truthy_non_null(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def truthy_non_zero(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.strip() not in {"0", "0.0"}
    try:
        return float(value) != 0.0
    except (TypeError, ValueError):
        return False


def game_filter_from_def_rows(def_rows: list[dict[str, Any]], fallback: dict[str, str]) -> tuple[str, str, str]:
    if def_rows:
        first = def_rows[0]
        year = int(first["year"])
        week = int(first["week"])
        game_date = str(first["game_date"])[:10]
        pair_clauses = []
        for row in def_rows:
            f1 = row.get("nfl_franchise_number")
            f2 = row.get("opponent_nfl_franchise_number")
            if f1 is None or f2 is None:
                continue
            pair_clauses.append(
                f"(nfl_franchise_number = {int(f1)} AND opponent_nfl_franchise_number = {int(f2)})"
            )
        if pair_clauses:
            where = (
                f"CAST(year AS INTEGER) = {year} AND CAST(week AS INTEGER) = {week} "
                f"AND CAST(game_date AS DATE) = DATE {sql_string(game_date)} "
                f"AND ({' OR '.join(sorted(set(pair_clauses)))})"
            )
            teams = " | ".join(
                sorted({f"{row.get('nfl_team')} vs {row.get('opponent_nfl_team')}" for row in def_rows})
            )
            return where, "def_player_week_franchise_pair", teams

    game_date = (fallback.get("game_date") or "")[:10]
    teams = [fallback.get("away_team", ""), fallback.get("home_team", "")]
    team_list = ", ".join(sql_string(team) for team in teams if team)
    if game_date and team_list:
        where = (
            f"CAST(game_date AS DATE) = DATE {sql_string(game_date)} "
            f"AND (nfl_team IN ({team_list}) OR opponent_nfl_team IN ({team_list}))"
        )
        return where, "checkpoint_date_team_fallback", " vs ".join(teams)
    return "1 = 0", "unmatched", ""


def compute_v26_game_coverage(
    candidates: list[dict[str, str]],
    v26_table: Path,
    catalog: list[dict[str, Any]],
    raw_atom_columns: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if duckdb is None:
        return [], {"v26_coverage_error": "duckdb_not_importable"}

    years = sorted({int(float(row["year"])) for row in candidates if row.get("year")})
    if not years:
        return [], {"v26_coverage_error": "no_candidate_years"}

    column_names = {row["column_name"] for row in catalog}
    selected_cols = [col for col in GAME_ID_COLUMNS + raw_atom_columns if col in column_names]
    select_sql = ", ".join(qident(col) for col in dict.fromkeys(selected_cols))
    year_sql = ", ".join(str(year) for year in years)

    con = duckdb.connect()
    con.execute("SET progress_bar_time=99999")
    con.execute(
        f"""
        CREATE TEMP TABLE st AS
        SELECT {select_sql}
        FROM read_parquet({sql_string(v26_table.as_posix())})
        WHERE CAST(year AS INTEGER) IN ({year_sql})
        """
    )

    by_boxscore: dict[str, dict[str, str]] = {}
    for candidate in candidates:
        by_boxscore.setdefault(candidate.get("boxscore_id", ""), candidate)

    boxscore_ids = sorted(boxscore for boxscore in by_boxscore if boxscore)
    if boxscore_ids:
        like_clause = " OR ".join(
            f"player_week LIKE {sql_string('%' + boxscore_id + '%')}" for boxscore_id in boxscore_ids
        )
        def_query = f"""
            SELECT player_week, year, week, game_date, nfl_team, opponent_nfl_team,
                   nfl_franchise_number, opponent_nfl_franchise_number
            FROM st
            WHERE position = 'DEF' AND ({like_clause})
        """
        def_cols = [desc[0] for desc in con.execute(def_query).description]
        def_rows_raw = [dict(zip(def_cols, row)) for row in con.execute(def_query).fetchall()]
    else:
        def_rows_raw = []

    def_by_boxscore: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in def_rows_raw:
        player_week = str(row.get("player_week") or "")
        for boxscore_id in boxscore_ids:
            if boxscore_id in player_week:
                def_by_boxscore[boxscore_id].append(row)

    family_by_col = {row["column_name"]: row["family"] for row in catalog}
    numeric_by_col = {row["column_name"]: is_numeric_type(row["duckdb_type"]) for row in catalog}
    output: list[dict[str, Any]] = []

    for boxscore_id in boxscore_ids:
        where, method, matched_teams = game_filter_from_def_rows(def_by_boxscore.get(boxscore_id, []), by_boxscore[boxscore_id])
        query = f"SELECT {select_sql} FROM st WHERE {where}"
        result = con.execute(query)
        cols = [desc[0] for desc in result.description]
        rows = [dict(zip(cols, row)) for row in result.fetchall()]

        non_null_cols: set[str] = set()
        non_zero_cols: set[str] = set()
        non_zero_cell_by_family: Counter[str] = Counter()
        non_null_cell_by_family: Counter[str] = Counter()
        for row in rows:
            for atom in raw_atom_columns:
                if atom not in row:
                    continue
                value = row.get(atom)
                if truthy_non_null(value):
                    non_null_cols.add(atom)
                    non_null_cell_by_family[family_by_col.get(atom, "other")] += 1
                if numeric_by_col.get(atom, False) and truthy_non_zero(value):
                    non_zero_cols.add(atom)
                    non_zero_cell_by_family[family_by_col.get(atom, "other")] += 1
                elif not numeric_by_col.get(atom, False) and truthy_non_zero(value):
                    non_zero_cols.add(atom)
                    non_zero_cell_by_family[family_by_col.get(atom, "other")] += 1

        non_null_col_by_family = Counter(family_by_col.get(atom, "other") for atom in non_null_cols)
        non_zero_col_by_family = Counter(family_by_col.get(atom, "other") for atom in non_zero_cols)

        candidate_group = [row for row in candidates if row.get("boxscore_id") == boxscore_id]
        output.append(
            {
                "boxscore_id": boxscore_id,
                "game_date": by_boxscore[boxscore_id].get("game_date", ""),
                "away_team": by_boxscore[boxscore_id].get("away_team", ""),
                "home_team": by_boxscore[boxscore_id].get("home_team", ""),
                "candidate_rows": len(candidate_group),
                "candidate_pdf_rows": sum(1 for row in candidate_group if row.get("pdf_path")),
                "v26_match_method": method,
                "v26_matched_teams": matched_teams,
                "v26_rows": len(rows),
                "v26_def_rows": sum(1 for row in rows if row.get("position") == "DEF"),
                "v26_raw_atom_columns_any_non_null": len(non_null_cols),
                "v26_raw_atom_columns_any_non_zero": len(non_zero_cols),
                "v26_non_null_family_column_counts_json": json.dumps(dict(sorted(non_null_col_by_family.items())), sort_keys=True),
                "v26_non_zero_family_column_counts_json": json.dumps(dict(sorted(non_zero_col_by_family.items())), sort_keys=True),
                "v26_non_null_family_cell_counts_json": json.dumps(dict(sorted(non_null_cell_by_family.items())), sort_keys=True),
                "v26_non_zero_family_cell_counts_json": json.dumps(dict(sorted(non_zero_cell_by_family.items())), sort_keys=True),
                "v26_non_zero_atom_names": "|".join(sorted(non_zero_cols)),
            }
        )

    return output, {"v26_years_loaded": years, "v26_rows_loaded": con.execute("SELECT COUNT(*) FROM st").fetchone()[0]}


def build_game_summary(
    candidates: list[dict[str, str]],
    candidate_coverage: list[dict[str, Any]],
    v26_game_coverage: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    coverage_by_boxscore: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_coverage:
        coverage_by_boxscore[row["boxscore_id"]].append(row)

    candidate_by_boxscore: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidates:
        candidate_by_boxscore[row.get("boxscore_id", "")].append(row)

    v26_by_boxscore = {row["boxscore_id"]: row for row in v26_game_coverage}
    rows: list[dict[str, Any]] = []
    for boxscore_id in sorted(candidate_by_boxscore):
        group = candidate_by_boxscore[boxscore_id]
        coverage = coverage_by_boxscore.get(boxscore_id, [])
        first = group[0]
        row = {
            "boxscore_id": boxscore_id,
            "game_date": first.get("game_date", ""),
            "year": first.get("year", ""),
            "away_team": first.get("away_team", ""),
            "home_team": first.get("home_team", ""),
            "candidate_rows": len(group),
            "pdf_rows": sum(1 for item in group if item.get("pdf_path")),
            "pdf_exists_rows": sum(1 for item in coverage if item.get("pdf_exists")),
            "ocr_available_rows": sum(1 for item in coverage if item.get("text_quality") == "ocr_available"),
            "pdf_extractable_rows": sum(1 for item in coverage if item.get("text_quality") == "pdf_extractable"),
            "viewer_only_or_no_ocr_rows": sum(1 for item in coverage if item.get("text_quality") == "viewer_only_or_no_ocr"),
            "metadata_only_rows": sum(1 for item in coverage if item.get("best_text_source") == "metadata_only"),
            "extractable_text_chars_total": sum(int(item.get("extractable_text_chars") or 0) for item in coverage),
            "extracted_atom_count_total": sum(int(item.get("extracted_atom_count") or 0) for item in coverage),
            "candidate_publications": " | ".join(dict.fromkeys(item.get("publication", "") for item in group if item.get("publication"))),
        }
        row.update(v26_by_boxscore.get(boxscore_id, {}))
        rows.append(row)
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--checkpoint",
        action="append",
        type=Path,
        required=True,
        help="Candidate checkpoint CSV or checkpoint directory. Repeat for chunks/full-year rollups.",
    )
    parser.add_argument("--label", required=True, help="Run label used in output directory names.")
    parser.add_argument("--v26-table", type=Path, default=None, help="Path to v26 nfl_player_stats_all.parquet.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT, help="D-drive output root.")
    parser.add_argument("--max-candidates", type=int, default=None, help="Optional candidate cap for smoke/pilot runs.")
    parser.add_argument("--text-char-limit", type=int, default=250_000, help="Maximum text chars read per candidate.")
    parser.add_argument("--max-hits-per-atom", type=int, default=5, help="Cap regex hits per atom per candidate.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    v26_table = args.v26_table or latest_v26_table()
    out_dir = args.out_root / f"{now_stamp()}_{args.label}"
    out_dir.mkdir(parents=True, exist_ok=True)

    candidates, checkpoint_files = load_candidates(args.checkpoint, args.max_candidates)
    catalog, raw_atom_columns = describe_v26(v26_table)
    candidate_coverage, atom_rows, wide_rows = analyze_candidates(
        candidates,
        raw_atom_columns,
        args.text_char_limit,
        args.max_hits_per_atom,
    )
    v26_game_coverage, v26_meta = compute_v26_game_coverage(candidates, v26_table, catalog, raw_atom_columns)
    game_summary = build_game_summary(candidates, candidate_coverage, v26_game_coverage)

    write_csv(out_dir / "v26_atom_catalog.csv", catalog, ["ordinal", "column_name", "duckdb_type", "classification", "family", "included_in_atom_probe"])
    write_csv(
        out_dir / "candidate_text_coverage.csv",
        candidate_coverage,
        [
            "candidate_id",
            "boxscore_id",
            "candidate_rank",
            "image_id",
            "publication",
            "text_quality",
            "extractable_text_chars",
            "extracted_atom_count",
        ],
    )
    write_csv(
        out_dir / "candidate_atoms_long.csv",
        atom_rows,
        [
            "boxscore_id",
            "candidate_rank",
            "image_id",
            "publication",
            "result_date",
            "page",
            "atom",
            "value",
            "entity_scope",
            "confidence",
            "extractor",
            "context",
        ],
    )
    write_csv(
        out_dir / "candidate_atoms_wide_v26_shape.csv",
        wide_rows,
        ["candidate_id", "boxscore_id", "candidate_rank", "image_id", "publication", "text_quality", "extracted_atom_count", *raw_atom_columns],
    )
    write_csv(
        out_dir / "game_coverage_summary.csv",
        game_summary,
        [
            "boxscore_id",
            "game_date",
            "year",
            "away_team",
            "home_team",
            "candidate_rows",
            "pdf_rows",
            "ocr_available_rows",
            "pdf_extractable_rows",
            "viewer_only_or_no_ocr_rows",
            "extracted_atom_count_total",
            "v26_rows",
            "v26_raw_atom_columns_any_non_null",
            "v26_raw_atom_columns_any_non_zero",
        ],
    )

    quality_counts = Counter(row["text_quality"] for row in candidate_coverage)
    summary = {
        "label": args.label,
        "created_at_utc": now_stamp(),
        "out_dir": str(out_dir),
        "v26_table": str(v26_table),
        "checkpoint_files": [str(path) for path in checkpoint_files],
        "candidate_rows": len(candidates),
        "games": len({row.get("boxscore_id") for row in candidates}),
        "raw_atom_columns_in_v26_probe": len(raw_atom_columns),
        "candidate_text_quality_counts": dict(sorted(quality_counts.items())),
        "candidates_with_extractable_text": sum(
            1 for row in candidate_coverage if row["text_quality"] in {"ocr_available", "pdf_extractable"}
        ),
        "candidate_atoms_extracted": len(atom_rows),
        "games_with_candidate_atoms": len({row["boxscore_id"] for row in atom_rows}),
        "v26_meta": v26_meta,
        "notes": [
            "Browser-rendered Newspapers.com PDFs commonly contain viewer chrome text, not newspaper OCR.",
            "Atom rows are low-confidence regex candidates only; promote nothing without human/source review.",
            "Coverage rows are the authoritative first-pass answer for how much usable text/evidence exists.",
        ],
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out_dir / "README.md").write_text(
        "\n".join(
            [
                f"# Newspapers.com atom probe: {args.label}",
                "",
                "Generated artifacts:",
                "",
                "- `v26_atom_catalog.csv`: v26 schema classification and atom-family catalog.",
                "- `candidate_text_coverage.csv`: per-candidate PDF/text/OCR/extraction coverage.",
                "- `candidate_atoms_long.csv`: low-confidence regex atom candidates, if any.",
                "- `candidate_atoms_wide_v26_shape.csv`: candidate rows with v26 atom columns.",
                "- `game_coverage_summary.csv`: per-game acquisition coverage plus current v26 atom presence.",
                "- `summary.json`: run totals and caveats.",
                "",
                "Do not write these atoms into the supertable directly. Use them as review packages for pipeline fixes.",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
