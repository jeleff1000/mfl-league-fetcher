"""Normalize ChatGPT newspaper-return CSVs into non-applying source candidates.

This module is intentionally conservative. It preserves every reviewer proposal,
splits multi-document citations, and assigns only a source-table family plus a
gate status. It never writes DuckDB and never creates a promoted atom.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def normalized_atom_type(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", clean(value).lower()).strip("_")


def parse_json_object(value: str) -> dict[str, Any] | None:
    if not value.lstrip().startswith("{"):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def parse_text_final_score(value: str) -> dict[str, str] | None:
    match = re.fullmatch(
        r"\s*([A-Za-z0-9 .'-]+?)\s+(\d+)\s*[,;]\s*([A-Za-z0-9 .'-]+?)\s+(\d+)\s*",
        value,
    )
    if not match:
        return None
    return {
        "team_1_raw": match.group(1).strip(),
        "team_1_score": match.group(2),
        "team_2_raw": match.group(3).strip(),
        "team_2_score": match.group(4),
    }


def split_source_document_ids(value: Any) -> list[str]:
    parts = [part.strip() for part in clean(value).split(";") if part.strip()]
    return parts or [""]


def route_target_table(atom_type: str) -> str:
    """Map known reviewer vocabularies to source-table families only."""
    if atom_type in {
        "game_final_score", "final_score", "game_score", "game_result",
        "game_outcome", "game_final", "game_final_score", "game_score_final",
        "away_score", "home_score",
    } or atom_type.startswith("game_final_score"):
        return "game_candidate"
    if atom_type.startswith(("lineup", "starting_lineup", "substitutions", "player_substitution")):
        return "lineup_participation"
    if atom_type.startswith("team_") or atom_type.startswith("team_") or atom_type.startswith("team"):
        return "team_game_stat_claim"
    if atom_type in {
        "score_by_period", "points_by_period", "quarter_score", "quarter_scores",
        "halftime_score", "score_after_third_quarter", "second_half_scoring_summary",
    }:
        return "team_game_stat_claim"
    if atom_type.startswith(("player_injury", "player_ejection", "player_game_note", "player_game_status", "player_game_exit")):
        return "player_game_note"
    if atom_type.startswith("player_") or atom_type.startswith("player_") or atom_type in {"punter", "field_goal_scorer", "extra_point_scorer"}:
        return "player_game_stat_claim"
    if any(token in atom_type for token in ("touchdown", "field_goal", "extra_point", "safety", "scoring_play", "scoring_event", "scoring_drive")):
        return "scoring_event"
    if any(token in atom_type for token in ("turnover", "interception", "fumble", "punt", "kickoff", "pass_play", "pass_completion", "blocked", "drive_setup", "play_by_play", "rush")):
        return "play_by_play_event"
    if any(token in atom_type for token in (
        "attendance", "venue", "weather", "temperature", "field_condition", "official",
        "record", "streak", "standing", "championship", "title", "milestone", "coach",
        "daypart", "time_of_day", "location", "series", "defending_champion", "period_length",
    )):
        return "source_document_note"
    return ""


def is_conflict_finding(value: Any) -> bool:
    return clean(value).lower() in {"contradictory", "incorrect", "uncertain", "too_uncertain"}


def proposal_id(row: Mapping[str, Any], source_document_id: str) -> str:
    raw = "\x1f".join([
        clean(row.get("return_file")),
        clean(row.get("return_row_number")),
        source_document_id,
        clean(row.get("proposed_atom_type")),
        clean(row.get("proposed_value")),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def normalized_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(value).lower()).strip()


def score_signature(fields: Mapping[str, Any]) -> tuple[tuple[str, str], ...] | None:
    away_team = fields.get("away_team") or fields.get("team_1_raw") or fields.get("team_1_resolved")
    away_score = fields.get("away_score") or fields.get("team_1_score")
    home_team = fields.get("home_team") or fields.get("team_2_raw") or fields.get("team_2_resolved")
    home_score = fields.get("home_score") or fields.get("team_2_score")
    if not all(clean(value) for value in (away_team, away_score, home_team, home_score)):
        return None
    return tuple(sorted(((normalized_text(away_team), clean(away_score)), (normalized_text(home_team), clean(home_score)))))


def event_class(value: Any) -> str:
    text = normalized_atom_type(value)
    for event in ("touchdown", "field_goal", "extra_point", "safety"):
        if event in text:
            return event
    return text


def period_number(value: Any) -> str:
    text = normalized_text(value)
    if not text:
        return ""
    if text[0].isdigit():
        return text[0]
    words = {"first": "1", "second": "2", "third": "3", "fourth": "4"}
    return next((number for word, number in words.items() if word in text), "")


def scoring_identity(fields: Mapping[str, Any]) -> tuple[set[str], str, str, str] | None:
    count = clean(fields.get("count"))
    if count and count not in {"1", "1.0"}:
        return None
    team_values = {
        normalized_text(fields.get(key))
        for key in ("team", "scoring_team", "scoring_team_raw")
        if normalized_text(fields.get(key))
    }
    player = normalized_text(
        fields.get("player")
        or fields.get("scorer")
        or fields.get("kicker")
        or fields.get("scoring_player")
        or fields.get("scoring_player_raw")
    )
    event = event_class(fields.get("event") or fields.get("event_type") or fields.get("result"))
    if not team_values or not player or not event:
        return None
    return team_values, player, event, period_number(fields.get("quarter") or fields.get("period") or fields.get("period_raw"))


def integer_value(value: Any) -> int | None:
    text = clean(value)
    try:
        return int(text)
    except ValueError:
        return None


def player_scoring_summary_identity(candidate: Mapping[str, str], fields: Mapping[str, Any]) -> tuple[set[str], str, str, int] | None:
    atom_type = clean(candidate.get("normalized_atom_type"))
    if "touchdown" in atom_type:
        event, count = "touchdown", integer_value(fields.get("touchdowns") or fields.get("value"))
    elif "extra_point" in atom_type and "made" in atom_type:
        event, count = "extra_point", integer_value(fields.get("made") or fields.get("value"))
    elif "field_goal" in atom_type and "made" in atom_type:
        event, count = "field_goal", integer_value(fields.get("made") or fields.get("value"))
    else:
        return None
    teams = {normalized_text(fields.get("team"))} - {""}
    player = normalized_text(fields.get("player"))
    if not teams or not player or count is None:
        return None
    return teams, player, event, count


def team_stat_identity(candidate: Mapping[str, str], fields: Mapping[str, Any]) -> tuple[set[str], set[str], str] | None:
    atom_type = clean(candidate.get("normalized_atom_type"))
    stat_aliases = {
        "team_first_downs": {"first_downs"},
        "team_total_yards": {"total_yards", "total_yards_gained"},
    }.get(atom_type)
    if not stat_aliases:
        return None
    team = normalized_text(fields.get("team"))
    value = clean(fields.get("value"))
    if not team or not value:
        return None
    return {team}, stat_aliases, value


def source_dedupe_disposition(candidate: Mapping[str, str], existing_rows: Iterable[Mapping[str, Any]]) -> str:
    """Return an exact-only duplicate disposition for a normalized candidate.

    This deliberately recognizes only full final-score equality. All other
    candidate shapes remain in the visual-semantic lane until a source reader
    confirms whether they are a duplicate, a variant, or a novel source atom.
    """
    if clean(candidate.get("normalization_status")).startswith("hold_"):
        return "held_by_normalization"
    try:
        candidate_fields = json.loads(clean(candidate.get("candidate_fields_json")) or "{}")
    except json.JSONDecodeError:
        return "visual_semantic_dedupe_required"
    if not isinstance(candidate_fields, dict):
        return "visual_semantic_dedupe_required"
    target_table = clean(candidate.get("target_table"))
    source_document_id = clean(candidate.get("source_document_id"))
    if target_table == "game_candidate":
        candidate_score = score_signature(candidate_fields)
        if candidate_score is None:
            return "visual_semantic_dedupe_required"
        for existing in existing_rows:
            if (
                clean(existing.get("target_table")) == "game_candidate"
                and clean(existing.get("source_document_id")) == source_document_id
                and score_signature(existing) == candidate_score
            ):
                return "exact_source_duplicate"
    if target_table == "scoring_event":
        candidate_event = scoring_identity(candidate_fields)
        if candidate_event is None:
            return "visual_semantic_dedupe_required"
        candidate_teams, candidate_player, candidate_event_class, candidate_period = candidate_event
        for existing in existing_rows:
            if clean(existing.get("target_table")) != "scoring_event" or clean(existing.get("source_document_id")) != source_document_id:
                continue
            existing_event = scoring_identity(existing)
            if existing_event is None:
                continue
            existing_teams, existing_player, existing_event_class, existing_period = existing_event
            if (
                candidate_teams.intersection(existing_teams)
                and candidate_player == existing_player
                and candidate_event_class == existing_event_class
                and (not candidate_period or candidate_period == existing_period)
            ):
                return "exact_source_duplicate"
    if target_table == "player_game_stat_claim":
        summary_identity = player_scoring_summary_identity(candidate, candidate_fields)
        if summary_identity is None:
            return "visual_semantic_dedupe_required"
        candidate_teams, candidate_player, candidate_event_class, candidate_count = summary_identity
        matching_events = 0
        for existing in existing_rows:
            if clean(existing.get("target_table")) != "scoring_event" or clean(existing.get("source_document_id")) != source_document_id:
                continue
            existing_event = scoring_identity(existing)
            if existing_event is None:
                continue
            existing_teams, existing_player, existing_event_class, _ = existing_event
            if candidate_teams.intersection(existing_teams) and candidate_player == existing_player and candidate_event_class == existing_event_class:
                matching_events += 1
        if matching_events == candidate_count:
            return "exact_source_duplicate"
    if target_table == "team_game_stat_claim":
        candidate_stat = team_stat_identity(candidate, candidate_fields)
        if candidate_stat is None:
            return "visual_semantic_dedupe_required"
        candidate_teams, stat_aliases, candidate_value = candidate_stat
        for existing in existing_rows:
            if clean(existing.get("target_table")) != "team_game_stat_claim" or clean(existing.get("source_document_id")) != source_document_id:
                continue
            if clean(existing.get("stat_name")) not in stat_aliases:
                continue
            existing_pairs = (
                (normalized_text(existing.get("team_1_nfl_team")), clean(existing.get("team_1_value"))),
                (normalized_text(existing.get("team_2_nfl_team")), clean(existing.get("team_2_value"))),
            )
            if any(team in candidate_teams and value == candidate_value for team, value in existing_pairs):
                return "exact_source_duplicate"
    return "visual_semantic_dedupe_required"


def normalize_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Preserve every raw proposal in an explicitly non-applying normalized ledger."""
    normalized: list[dict[str, str]] = []
    for raw_row in rows:
        raw = {key: clean(value) for key, value in raw_row.items()}
        atom_type = normalized_atom_type(raw.get("proposed_atom_type"))
        target_table = route_target_table(atom_type)
        parsed_value = parse_json_object(raw.get("proposed_value", ""))
        candidate_fields = parsed_value
        if candidate_fields is None and target_table == "game_candidate":
            candidate_fields = parse_text_final_score(raw.get("proposed_value", ""))
        for source_document_id in split_source_document_ids(raw.get("source_document_id")):
            if not source_document_id:
                status = "hold_missing_source_document"
            elif not target_table:
                status = "hold_unrecognized_type"
            elif is_conflict_finding(raw.get("finding_type")):
                status = "hold_source_conflict"
            elif candidate_fields is not None:
                status = "routed_structured"
            else:
                status = "routed_textual"
            normalized.append({
                **raw,
                "proposal_id": proposal_id(raw, source_document_id),
                "source_document_id": source_document_id,
                "source_document_id_raw": raw.get("source_document_id", ""),
                "normalized_atom_type": atom_type,
                "target_table": target_table,
                "normalization_status": status,
                "candidate_fields_json": json.dumps(candidate_fields or {}, sort_keys=True, ensure_ascii=True),
                "value_format": "json_object" if parsed_value is not None else "text",
                "apply_eligibility": "never_direct_apply" if status.startswith("hold_") else "visual_and_source_dedupe_required",
                "normalization_policy": "secondary_witness_only_no_direct_database_apply",
            })
    return normalized


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    result = normalize_rows(read_csv(args.input_csv))
    args.out_dir.mkdir(parents=True, exist_ok=False)
    write_csv(args.out_dir / "normalized_secondary_witness_ledger.csv", result)
    summary = {
        "input_row_count": len(read_csv(args.input_csv)),
        "normalized_row_count": len(result),
        "target_table_counts": dict(sorted(Counter(row["target_table"] for row in result).items())),
        "normalization_status_counts": dict(sorted(Counter(row["normalization_status"] for row in result).items())),
        "write_guarantee": "No DuckDB, newspaper_promoted, canonical weekly/PBP, v26, live, Fly, or supertable write occurred.",
    }
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
