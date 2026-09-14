from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Iterable, Mapping, Sequence

import duckdb


DATA_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DATABASE_PATH = DATA_ROOT / "databases" / "newspaper_atoms.duckdb"
OUTPUT_ROOT = DATA_ROOT / "witness_conflict_audits"

BOX_SCORE_STAT_FIELDS = (
    "carries",
    "rushing_yards",
    "rushing_tds",
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "fg_long",
    "def_interceptions",
    "def_sacks",
    "def_tds",
    "special_teams_tds",
    "touchdowns",
)

NON_SEMANTIC_PROMOTION_FIELDS = {
    "reconciliation_status",
    "source_row_text",
    "evidence_text",
    "source_document_id",
    "source_documents_json",
    "source_materialized_row_ids_json",
    "region_id",
    "atom_claim_id",
    "confidence_score",
    "confidence_bar",
    "max_confidence_score",
    "review_status",
    "promotion_status",
    "decision_value",
    "decision_status",
    "route_to_lane",
    "promotion_source",
    "created_at_utc",
}

ENTITY_ORDER_OR_NAME_FIELDS = {"team_1_raw", "team_2_raw"}


def normalize_scalar(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    if re.fullmatch(r"[-+]?\d[\d,]*(?:\.\d+)?", text):
        return text.replace(",", "")
    return text.lower()


def canonical_team_values(
    team_1: object,
    value_1: object,
    team_2: object,
    value_2: object,
) -> str:
    pairs = [
        (normalize_scalar(team_1), normalize_scalar(value_1)),
        (normalize_scalar(team_2), normalize_scalar(value_2)),
    ]
    pairs = [(team, value) for team, value in pairs if team and value]
    return "|".join(f"{team}={value}" for team, value in sorted(pairs))


def find_scalar_conflicts(
    rows: Iterable[Mapping[str, object]],
    *,
    key_fields: Sequence[str],
    value_field: str,
    source_field: str = "source_document_id",
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, ...], dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for row in rows:
        key = tuple(normalize_scalar(row.get(field)) for field in key_fields)
        value = normalize_scalar(row.get(value_field))
        source = str(row.get(source_field) or "").strip()
        if not all(key) or not value or not source:
            continue
        grouped[key][value].add(source)

    conflicts = []
    for key, value_sources in grouped.items():
        source_documents = {source for sources in value_sources.values() for source in sources}
        if len(value_sources) < 2 or len(source_documents) < 2:
            continue
        conflicts.append(
            {
                "key": dict(zip(key_fields, key)),
                "values": sorted(value_sources),
                "source_documents": sorted(source_documents),
                "value_sources": {value: sorted(sources) for value, sources in sorted(value_sources.items())},
            }
        )
    return conflicts


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def query_rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def write_csv(path: Path, rows: list[dict[str, object]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def safe_json(value: object, fallback: object) -> object:
    try:
        return json.loads(str(value or ""))
    except (TypeError, json.JSONDecodeError):
        return fallback


def preferred_team(row: Mapping[str, object], side: int) -> str:
    for field in (f"team_{side}_nfl_team", f"team_{side}_resolved", f"team_{side}_raw"):
        value = normalize_scalar(row.get(field))
        if value:
            return value
    return ""


def game_score_value(row: Mapping[str, object]) -> str:
    return canonical_team_values(
        row.get("team_1_resolved") or row.get("team_1_raw"),
        row.get("team_1_score"),
        row.get("team_2_resolved") or row.get("team_2_raw"),
        row.get("team_2_score"),
    )


def team_stat_value(row: Mapping[str, object]) -> str:
    first_value = normalize_scalar(row.get("team_1_value"))
    second_value = normalize_scalar(row.get("team_2_value"))
    stat_name = normalize_scalar(row.get("stat_name"))
    if stat_name.startswith("attendance"):
        values = sorted({value for value in (first_value, second_value) if value})
        return "|".join(values)
    return canonical_team_values(
        preferred_team(row, 1),
        first_value,
        preferred_team(row, 2),
        second_value,
    )


def normalize_line_score(value: object) -> str:
    text = str(value or "").lower()
    numbers = [int(number) for number in re.findall(r"\d+", text)]
    if len(numbers) == 1 and re.fullmatch(r"\d{4}", text.strip()):
        numbers = [int(digit) for digit in text.strip()]
    if len(numbers) == 4:
        numbers.append(sum(numbers))
    if len(numbers) != 5:
        return ""
    return "|".join(str(number) for number in numbers)


def team_stat_comparison_rows(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    comparisons: list[dict[str, object]] = []
    for row in rows:
        stat_name = normalize_scalar(row.get("stat_name"))
        boxscore_id = normalize_scalar(row.get("boxscore_id"))
        if not stat_name or not boxscore_id:
            continue
        if stat_name.startswith("attendance"):
            value = team_stat_value(row)
            if value:
                comparison = dict(row)
                comparison["_comparison_key"] = f"{boxscore_id}|{stat_name}"
                comparison["_comparison_value"] = value
                comparison["_comparison_priority"] = "P1"
                comparisons.append(comparison)
            continue
        line_score_stat = "score_by_period" in stat_name or "line_score" in stat_name or stat_name == "points_by_quarter"
        for side in (1, 2):
            team = preferred_team(row, side)
            value = normalize_scalar(row.get(f"team_{side}_value"))
            if line_score_stat:
                value = normalize_line_score(row.get(f"team_{side}_value"))
            if not team or not value:
                continue
            comparison = dict(row)
            comparison["_comparison_key"] = f"{boxscore_id}|{stat_name}|{team}"
            comparison["_comparison_value"] = value
            comparison["_comparison_priority"] = "P1" if stat_name == "final_score" else "P2"
            comparisons.append(comparison)
    return comparisons


def player_identity(row: Mapping[str, object]) -> str:
    for field in ("NFL_player_id", "scoring_NFL_player_id", "primary_NFL_player_id", "player_week", "resolved_player", "player_raw", "scoring_player_raw", "primary_player_raw"):
        value = normalize_scalar(row.get(field))
        if value:
            return value
    return ""


def generated_proposed_fields(row: Mapping[str, object]) -> dict[str, object]:
    fields = safe_json(row.get("generated_proposed_fields_json"), {})
    return fields if isinstance(fields, dict) else {}


def witness_source_document(row: Mapping[str, object]) -> str:
    fields = generated_proposed_fields(row)
    return str(
        row.get("source_document_id")
        or row.get("generated_source_document_id")
        or fields.get("source_document_id")
        or ""
    ).strip()


def evidence_payload(row: Mapping[str, object]) -> dict[str, str]:
    fields = generated_proposed_fields(row)
    text = (
        fields.get("evidence_text")
        or row.get("claim_evidence_text")
        or row.get("evidence_text")
        or row.get("source_row_text")
        or row.get("play_text")
        or ""
    )
    return {
        "atom_claim_id": str(row.get("atom_claim_id") or ""),
        "source_document_id": witness_source_document(row),
        "region_id": str(row.get("region_id") or fields.get("region_id") or ""),
        "evidence_text": str(text)[:700],
        "evidence_text_path": str(row.get("claim_evidence_text_path") or fields.get("evidence_text_path") or ""),
        "evidence_image_path": str(
            fields.get("artifact_path")
            or row.get("generated_artifact_path")
            or row.get("claim_evidence_image_path")
            or ""
        ),
    }


def conflict_records(
    rows: Iterable[Mapping[str, object]],
    *,
    surface: str,
    key_builder,
    value_builder,
    field_builder,
    comparison_basis: str,
    priority,
) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for row in rows:
        key = str(key_builder(row) or "").strip()
        value = str(value_builder(row) or "").strip()
        source = witness_source_document(row)
        field_name = str(field_builder(row) or "").strip()
        if not key or not value or not source or not field_name:
            continue
        prepared_row = dict(row)
        prepared_row["_conflict_key"] = key
        prepared_row["_conflict_value"] = value
        prepared_row["_field_name"] = field_name
        prepared.append(prepared_row)

    compact_conflicts = find_scalar_conflicts(
        prepared,
        key_fields=("_conflict_key",),
        value_field="_conflict_value",
    )
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in prepared:
        grouped[normalize_scalar(row["_conflict_key"])].append(row)

    records: list[dict[str, object]] = []
    for compact in compact_conflicts:
        key = str(compact["key"]["_conflict_key"])
        witness_rows = [
            row
            for row in grouped[key]
            if normalize_scalar(row["_conflict_value"]) in compact["values"]
        ]
        value_sources: dict[str, list[str]] = defaultdict(list)
        witnesses: dict[str, list[dict[str, str]]] = defaultdict(list)
        boxscore_ids = set()
        for row in witness_rows:
            value = normalize_scalar(row["_conflict_value"])
            source = str(row.get("source_document_id") or "")
            boxscore_ids.add(str(row.get("boxscore_id") or ""))
            if source not in value_sources[value]:
                value_sources[value].append(source)
            payload = evidence_payload(row)
            if payload not in witnesses[source]:
                payload["value"] = value
                witnesses[source].append(payload)
        field_names = sorted({str(row["_field_name"]) for row in witness_rows})
        record_priority = priority(witness_rows[0]) if callable(priority) else priority
        record_seed = "|".join([surface, key, "|".join(compact["values"])])
        records.append(
            {
                "audit_conflict_id": hashlib.sha256(record_seed.encode("utf-8")).hexdigest()[:24],
                "conflict_class": "cross_source_scalar_disagreement",
                "review_status": "open_visual_reconciliation",
                "priority": record_priority,
                "surface": surface,
                "field_name": ";".join(field_names),
                "boxscore_id": ";".join(sorted(value for value in boxscore_ids if value)),
                "comparison_key": key,
                "comparison_basis": comparison_basis,
                "values_json": json.dumps(compact["values"], sort_keys=True),
                "value_sources_json": json.dumps(
                    {value: sorted(sources) for value, sources in sorted(value_sources.items())},
                    sort_keys=True,
                ),
                "source_documents_json": json.dumps(compact["source_documents"], sort_keys=True),
                "witness_evidence_json": json.dumps(witnesses, sort_keys=True),
                "source_document_count": len(compact["source_documents"]),
                "distinct_value_count": len(compact["values"]),
            }
        )
    return records


def source_rows_with_claim_evidence(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> list[dict[str, object]]:
    return query_rows(
        connection,
        f"""
        select
            promoted.*,
            claims.evidence_text as claim_evidence_text,
            claims.evidence_text_path as claim_evidence_text_path,
            claims.evidence_image_path as claim_evidence_image_path,
            generated.source_document_id as generated_source_document_id,
            generated.proposed_fields_json as generated_proposed_fields_json,
            generated.artifact_path as generated_artifact_path
        from newspaper_promoted.{table_name} as promoted
        left join newspaper_review.atom_claim as claims
          on promoted.atom_claim_id = claims.atom_claim_id
        left join newspaper_review.generated_atom_decision as generated
          on promoted.atom_claim_id = generated.decision_id
        """,
    )


def run_detected_conflicts(connection: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    detected: list[dict[str, object]] = []

    games = source_rows_with_claim_evidence(connection, "game_candidate")
    detected.extend(
        conflict_records(
            games,
            surface="game_candidate",
            key_builder=lambda row: f"{row.get('boxscore_id')}|final_score",
            value_builder=game_score_value,
            field_builder=lambda row: "final_score",
            comparison_basis="same boxscore; team/value pairs canonicalized so reversed listings corroborate",
            priority="P1",
        )
    )

    team_stats = source_rows_with_claim_evidence(connection, "team_game_stat_claim")
    team_stat_facts = team_stat_comparison_rows(team_stats)
    detected.extend(
        conflict_records(
            team_stat_facts,
            surface="team_game_stat_claim",
            key_builder=lambda row: str(row.get("_comparison_key") or ""),
            value_builder=lambda row: str(row.get("_comparison_value") or ""),
            field_builder=lambda row: str(row.get("stat_name") or ""),
            comparison_basis="same boxscore, normalized stat label, and team; attendance compares one game-level reported scalar",
            priority=lambda row: str(row.get("_comparison_priority") or "P2"),
        )
    )

    player_stats = source_rows_with_claim_evidence(connection, "player_game_stat_claim")
    detected.extend(
        conflict_records(
            player_stats,
            surface="player_game_stat_claim",
            key_builder=lambda row: "|".join(
                [
                    str(row.get("boxscore_id") or ""),
                    player_identity(row),
                    normalize_scalar(row.get("nfl_team") or row.get("team_raw")),
                    normalize_scalar(row.get("stat_name")),
                ]
            ),
            value_builder=lambda row: str(row.get("stat_value") or ""),
            field_builder=lambda row: str(row.get("stat_name") or ""),
            comparison_basis="same boxscore, player identity, team, and normalized player-stat label",
            priority="P2",
        )
    )

    player_boxes = source_rows_with_claim_evidence(connection, "player_game_box_score")
    for stat_field in BOX_SCORE_STAT_FIELDS:
        detected.extend(
            conflict_records(
                player_boxes,
                surface="player_game_box_score",
                key_builder=lambda row, field=stat_field: "|".join(
                    [
                        str(row.get("boxscore_id") or ""),
                        player_identity(row),
                        normalize_scalar(row.get("nfl_team")),
                        field,
                    ]
                ),
                value_builder=lambda row, field=stat_field: str(row.get(field) or ""),
                field_builder=lambda row, field=stat_field: field,
                comparison_basis="same boxscore, player identity, team, and named box-score column",
                priority="P1",
            )
        )

    scoring = source_rows_with_claim_evidence(connection, "scoring_event")
    scoring_rows = [
        row
        for row in scoring
        if normalize_scalar(row.get("period_raw"))
        and normalize_scalar(row.get("scoring_team") or row.get("scoring_team_raw"))
        and player_identity(row)
        and normalize_scalar(row.get("event_type"))
        and normalize_scalar(row.get("points"))
        and normalize_scalar(row.get("distance_yards"))
    ]
    detected.extend(
        conflict_records(
            scoring_rows,
            surface="scoring_event",
            key_builder=lambda row: "|".join(
                [
                    str(row.get("boxscore_id") or ""),
                    normalize_scalar(row.get("period_raw")),
                    normalize_scalar(row.get("scoring_team") or row.get("scoring_team_raw")),
                    player_identity(row),
                    normalize_scalar(row.get("event_type")),
                    normalize_scalar(row.get("points")),
                ]
            ),
            value_builder=lambda row: str(row.get("distance_yards") or ""),
            field_builder=lambda row: "distance_yards",
            comparison_basis="same scoring-event anchors; repeated same-player scores remain candidate-level until visual review",
            priority="P2",
        )
    )

    pbp = source_rows_with_claim_evidence(connection, "play_by_play_event")
    pbp_rows = [
        row
        for row in pbp
        if normalize_scalar(row.get("period_raw"))
        and normalize_scalar(row.get("clock_raw"))
        and normalize_scalar(row.get("possession_team") or row.get("possession_team_raw"))
        and player_identity(row)
        and normalize_scalar(row.get("play_type"))
        and (normalize_scalar(row.get("yards")) or normalize_scalar(row.get("points")))
    ]
    detected.extend(
        conflict_records(
            pbp_rows,
            surface="play_by_play_event",
            key_builder=lambda row: "|".join(
                [
                    str(row.get("boxscore_id") or ""),
                    normalize_scalar(row.get("period_raw")),
                    normalize_scalar(row.get("clock_raw")),
                    normalize_scalar(row.get("possession_team") or row.get("possession_team_raw")),
                    player_identity(row),
                    normalize_scalar(row.get("play_type")),
                ]
            ),
            value_builder=lambda row: "|".join(
                [normalize_scalar(row.get("yards")), normalize_scalar(row.get("points"))]
            ),
            field_builder=lambda row: "yards_and_points",
            comparison_basis="same clock, period, possession, player, and play type; candidate-level only",
            priority="P3",
        )
    )
    return sorted(detected, key=lambda row: (str(row["priority"]), str(row["surface"]), str(row["comparison_key"])))


def run_existing_conflict_inventory(connection: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    explicit = query_rows(connection, "select * from newspaper_review.conflict_claim order by conflict_claim_id")
    promotion_rows = query_rows(connection, "select * from newspaper_review.llm_promotion_conflict order by promotion_conflict_id")
    deduplicated: dict[str, dict[str, object]] = {}
    for row in promotion_rows:
        values = safe_json(row.get("values_json"), row.get("values_json") or "")
        documents = safe_json(row.get("source_documents_json"), row.get("source_documents_json") or "")
        field_name = normalize_scalar(row.get("field_name"))
        dedupe_key = json.dumps(
            [
                normalize_scalar(row.get("target_table")),
                normalize_scalar(row.get("target_entity_key")),
                field_name,
                values,
                documents,
            ],
            sort_keys=True,
        )
        entry = deduplicated.setdefault(
            dedupe_key,
            {
                "existing_conflict_key": hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()[:24],
                "target_table": row.get("target_table") or "",
                "target_entity_key": row.get("target_entity_key") or "",
                "field_name": row.get("field_name") or "",
                "classification": (
                    "metadata_or_provenance_divergence"
                    if field_name in NON_SEMANTIC_PROMOTION_FIELDS
                    else (
                        "entity_order_or_name_variant"
                        if field_name in ENTITY_ORDER_OR_NAME_FIELDS
                        else "prior_pipeline_semantic_conflict_candidate"
                    )
                ),
                "values_json": json.dumps(values, sort_keys=True),
                "source_documents_json": json.dumps(documents, sort_keys=True),
                "statuses_json": [],
                "promotion_conflict_ids_json": [],
                "raw_occurrence_count": 0,
                "first_created_at_utc": row.get("created_at_utc") or "",
                "last_created_at_utc": row.get("created_at_utc") or "",
            },
        )
        entry["raw_occurrence_count"] = int(entry["raw_occurrence_count"]) + 1
        entry["statuses_json"].append(row.get("status") or "")
        entry["promotion_conflict_ids_json"].append(row.get("promotion_conflict_id") or "")
        entry["last_created_at_utc"] = row.get("created_at_utc") or entry["last_created_at_utc"]

    normalized = []
    for entry in deduplicated.values():
        entry["statuses_json"] = json.dumps(sorted(set(entry["statuses_json"])))
        entry["promotion_conflict_ids_json"] = json.dumps(sorted(set(entry["promotion_conflict_ids_json"])))
        normalized.append(entry)
    return explicit, sorted(normalized, key=lambda row: (str(row["classification"]), str(row["target_table"]), str(row["target_entity_key"])))


def evidence_coverage_rows(connection: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    surfaces = (
        "game_candidate",
        "team_game_stat_claim",
        "player_game_stat_claim",
        "player_game_box_score",
        "scoring_event",
        "play_by_play_event",
        "lineup_participation",
        "source_document_note",
    )
    coverage = []
    for table_name in surfaces:
        rows = source_rows_with_claim_evidence(connection, table_name)
        coverage.append(
            {
                "surface": table_name,
                "promoted_rows": len(rows),
                "distinct_atom_claims": len({str(row.get("atom_claim_id") or "") for row in rows if row.get("atom_claim_id")}),
                "rows_with_source_document": sum(1 for row in rows if witness_source_document(row)),
                "rows_with_evidence_text": sum(1 for row in rows if evidence_payload(row)["evidence_text"]),
                "rows_with_evidence_image": sum(1 for row in rows if evidence_payload(row)["evidence_image_path"]),
            }
        )
    return coverage


def main() -> None:
    created_at = iso_now()
    output_dir = OUTPUT_ROOT / f"{stamp()}_newspaper_witness_conflict_audit_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DATABASE_PATH), read_only=True)
    try:
        detected = run_detected_conflicts(connection)
        explicit, promotion = run_existing_conflict_inventory(connection)
        evidence_coverage = evidence_coverage_rows(connection)
    finally:
        connection.close()

    detected_fields = [
        "audit_conflict_id",
        "conflict_class",
        "review_status",
        "priority",
        "surface",
        "field_name",
        "boxscore_id",
        "comparison_key",
        "comparison_basis",
        "values_json",
        "value_sources_json",
        "source_documents_json",
        "witness_evidence_json",
        "source_document_count",
        "distinct_value_count",
    ]
    explicit_fields = [
        "conflict_claim_id",
        "atom_claim_id",
        "conflict_type",
        "target_table",
        "target_field",
        "target_row_key",
        "existing_value",
        "newspaper_value",
        "conflict_summary",
        "confidence_score",
        "review_status",
        "source_document_id",
        "region_id",
    ]
    promotion_fields = [
        "existing_conflict_key",
        "classification",
        "target_table",
        "target_entity_key",
        "field_name",
        "values_json",
        "source_documents_json",
        "statuses_json",
        "promotion_conflict_ids_json",
        "raw_occurrence_count",
        "first_created_at_utc",
        "last_created_at_utc",
    ]
    coverage_fields = [
        "surface",
        "promoted_rows",
        "distinct_atom_claims",
        "rows_with_source_document",
        "rows_with_evidence_text",
        "rows_with_evidence_image",
    ]
    by_boxscore: dict[str, dict[str, object]] = {}
    for row in detected:
        for boxscore_id in str(row["boxscore_id"]).split(";"):
            if not boxscore_id:
                continue
            aggregate = by_boxscore.setdefault(
                boxscore_id,
                {
                    "boxscore_id": boxscore_id,
                    "detected_conflict_count": 0,
                    "p1_count": 0,
                    "p2_count": 0,
                    "p3_count": 0,
                    "surfaces": set(),
                    "fields": set(),
                    "source_documents": set(),
                },
            )
            aggregate["detected_conflict_count"] = int(aggregate["detected_conflict_count"]) + 1
            aggregate[f"{str(row['priority']).lower()}_count"] = int(aggregate[f"{str(row['priority']).lower()}_count"]) + 1
            aggregate["surfaces"].add(str(row["surface"]))
            aggregate["fields"].add(str(row["field_name"]))
            aggregate["source_documents"].update(safe_json(row["source_documents_json"], []))
    boxscore_rows = []
    for aggregate in by_boxscore.values():
        boxscore_rows.append(
            {
                "boxscore_id": aggregate["boxscore_id"],
                "detected_conflict_count": aggregate["detected_conflict_count"],
                "p1_count": aggregate["p1_count"],
                "p2_count": aggregate["p2_count"],
                "p3_count": aggregate["p3_count"],
                "surfaces": ";".join(sorted(aggregate["surfaces"])),
                "fields": ";".join(sorted(aggregate["fields"])),
                "source_documents_json": json.dumps(sorted(aggregate["source_documents"])),
            }
        )
    boxscore_rows.sort(key=lambda row: (-int(row["p1_count"]), -int(row["detected_conflict_count"]), str(row["boxscore_id"])))

    write_csv(output_dir / "detected_scalar_witness_conflicts.csv", detected, detected_fields)
    write_csv(output_dir / "explicit_conflict_claims.csv", explicit, explicit_fields)
    write_csv(output_dir / "prior_promotion_package_conflicts.csv", promotion, promotion_fields)
    write_csv(output_dir / "evidence_coverage_summary.csv", evidence_coverage, coverage_fields)
    write_csv(
        output_dir / "conflicts_by_boxscore.csv",
        boxscore_rows,
        ["boxscore_id", "detected_conflict_count", "p1_count", "p2_count", "p3_count", "surfaces", "fields", "source_documents_json"],
    )

    priority_counts: dict[str, int] = defaultdict(int)
    surface_counts: dict[str, int] = defaultdict(int)
    for row in detected:
        priority_counts[str(row["priority"])] += 1
        surface_counts[str(row["surface"])] += 1
    promotion_class_counts: dict[str, int] = defaultdict(int)
    raw_promotion_occurrences = 0
    for row in promotion:
        promotion_class_counts[str(row["classification"])] += 1
        raw_promotion_occurrences += int(row["raw_occurrence_count"])
    summary = {
        "created_at_utc": created_at,
        "database_path": str(DATABASE_PATH),
        "database_open_mode": "read_only",
        "output_dir": str(output_dir),
        "scope": "local newspaper_promoted atoms and local acquired newspaper witnesses only",
        "detected_cross_source_scalar_conflicts": len(detected),
        "detected_by_priority": dict(sorted(priority_counts.items())),
        "detected_by_surface": dict(sorted(surface_counts.items())),
        "boxscores_with_detected_conflicts": len(boxscore_rows),
        "explicit_conflict_claim_rows": len(explicit),
        "prior_promotion_conflict_raw_rows": raw_promotion_occurrences,
        "prior_promotion_conflict_deduplicated_rows": len(promotion),
        "prior_promotion_conflict_classification_counts": dict(sorted(promotion_class_counts.items())),
        "artifacts": {
            "detected_scalar_witness_conflicts_csv": str(output_dir / "detected_scalar_witness_conflicts.csv"),
            "explicit_conflict_claims_csv": str(output_dir / "explicit_conflict_claims.csv"),
            "prior_promotion_package_conflicts_csv": str(output_dir / "prior_promotion_package_conflicts.csv"),
            "evidence_coverage_summary_csv": str(output_dir / "evidence_coverage_summary.csv"),
            "conflicts_by_boxscore_csv": str(output_dir / "conflicts_by_boxscore.csv"),
        },
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    methodology = """# Witness Conflict Audit Methodology

This is a read-only audit of the separate local newspaper atom database. It does not write to the database, v26, live/Fly, or the supertable.

## What Counts As A Detected Conflict

- Two or more distinct normalized scalar values must occur for the same semantic comparison key.
- At least two source-document IDs must participate. A contradictory pair of values found only inside one article is not labeled a witness conflict.
- Final-score and pair-stat values canonicalize team/value pairs, so reversed newspaper team order is treated as corroboration.
- Each detected row keeps source document IDs, atom claim IDs, text evidence, text paths, and image-crop paths when present.

## Comparison Surfaces

- Game candidate final scores.
- Team game-stat claims, including attendance and structured team-pair values.
- Player stat claims and named player box-score columns.
- Strongly anchored scoring-event and play-by-play rows. These remain candidates because repeated similar plays can be difficult to align across independent newspaper accounts.

## Existing Prior-Pipeline Records

Prior promotion-package rows are kept in a separate artifact and deduplicated. Differences in source prose, reconciliation labels, provenance, and confidence are classified as metadata/provenance divergence, not factual witness conflicts.

## Limits

- The audit covers acquired local newspaper witnesses only, not pages that were never downloaded.
- A detected row is a review queue entry, not an automatic resolution or deletion.
- No direct comparison to v26, weekly tables, live/Fly, or the supertable occurs in this pass.
"""
    (output_dir / "methodology.md").write_text(methodology, encoding="utf-8")
    report = [
        "# Newspaper Witness Conflict Audit",
        "",
        f"- Created: {created_at}",
        f"- Database: `{DATABASE_PATH}` (read-only)",
        "- Scope: local promoted newspaper atoms and acquired local newspaper witnesses only.",
        f"- Detected cross-source scalar conflicts: {len(detected)} across {len(boxscore_rows)} boxscores.",
        f"- Explicit conflict_claim rows: {len(explicit)}.",
        f"- Prior promotion-package rows: {raw_promotion_occurrences} raw, {len(promotion)} deduplicated.",
        "",
        "## Detected By Priority",
        "",
    ]
    for priority, count in sorted(priority_counts.items()):
        report.append(f"- {priority}: {count}")
    report.extend(["", "## Detected By Surface", ""])
    for surface, count in sorted(surface_counts.items()):
        report.append(f"- {surface}: {count}")
    report.extend(["", "## First Review Queue", ""])
    for row in boxscore_rows[:25]:
        report.append(
            f"- `{row['boxscore_id']}`: {row['detected_conflict_count']} detected "
            f"(P1 {row['p1_count']}, P2 {row['p2_count']}, P3 {row['p3_count']}); {row['fields']}"
        )
    report.extend(
        [
            "",
            "## Artifacts",
            "",
            "- `detected_scalar_witness_conflicts.csv`: auditable candidate disagreements with retained witness evidence.",
            "- `conflicts_by_boxscore.csv`: review queue ordered by priority and conflict count.",
            "- `prior_promotion_package_conflicts.csv`: deduplicated older package-level differences, classified by meaning.",
            "- `evidence_coverage_summary.csv`: text/crop coverage by promoted atom surface.",
            "- `methodology.md`: match criteria and scope limits.",
        ]
    )
    (output_dir / "summary.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
