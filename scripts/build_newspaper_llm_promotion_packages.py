#!/usr/bin/env python
"""Build local promotion packages from materialized LLM newspaper rows.

This station groups table-shaped LLM review rows into auditable packages before
any live promotion. Duplicate sources become corroborating evidence; conflicting
field values become conflict rows that must be reviewed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_promotion_packages")

TARGET_TABLES = [
    "game_candidate",
    "scoring_event",
    "play_by_play_event",
    "player_game_box_score",
    "lineup_participation",
    "team_game_stat_claim",
    "player_identity_candidate",
]

PROVENANCE_TARGET_FIELDS = [
    "evidence_text",
    "source_document_id",
    "region_id",
    "confidence_score",
    "review_status",
    "promotion_status",
]

PACKAGE_FIELDS = [
    "promotion_package_id",
    "package_run_id",
    "ingest_run_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "package_status",
    "confidence_bar",
    "max_confidence_score",
    "avg_confidence_score",
    "evidence_document_count",
    "item_count",
    "promote_item_count",
    "review_item_count",
    "conflict_count",
    "proposed_fields_json",
    "source_documents_json",
    "materialized_row_ids_json",
    "notes",
    "created_at_utc",
]

ITEM_FIELDS = [
    "promotion_package_item_id",
    "promotion_package_id",
    "package_run_id",
    "ingest_run_id",
    "target_table",
    "target_entity_key",
    "llm_materialized_row_id",
    "llm_review_claim_id",
    "boxscore_id",
    "source_document_id",
    "publication",
    "source_url",
    "asset_pdf_path",
    "region_id",
    "evidence_text",
    "confidence_score",
    "confidence_lane",
    "promotion_recommendation",
    "row_group_key",
    "row_fields_json",
    "llm_reason",
    "created_at_utc",
]

CONFLICT_FIELDS = [
    "promotion_conflict_id",
    "promotion_package_id",
    "package_run_id",
    "ingest_run_id",
    "target_table",
    "target_entity_key",
    "field_name",
    "value_count",
    "values_json",
    "source_documents_json",
    "status",
    "created_at_utc",
]

NON_BLOCKING_CONFLICT_FIELDS = {
    "evidence_text",
    "match_method",
    "play_text",
    "possession_team_raw",
    "reconciliation_status",
    "review_status",
    "scoring_team_raw",
    "source_row_text",
    "team_1_raw",
    "team_2_raw",
}


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_conflict_value(field: str, value: Any) -> str:
    text = re.sub(r"\s+", " ", clean(value).strip().lower())
    if field == "period_raw":
        period_aliases = {
            "opening quarter": "first quarter",
            "opening period": "first quarter",
            "first period": "first quarter",
            "second period": "second quarter",
            "third period": "third quarter",
            "fourth period": "fourth quarter",
            "final period": "fourth quarter",
            "final quarter": "fourth quarter",
        }
        return period_aliases.get(text, text)
    return text


def compatible_normalized_values(field: str, values: set[str]) -> set[str]:
    if field == "event_type" and "touchdown" in values:
        specific_touchdowns = {value for value in values if value.endswith("_touchdown")}
        if len(specific_touchdowns) == 1 and values <= specific_touchdowns | {"touchdown"}:
            return specific_touchdowns
    return values


def proposed_value(field: str, counter: Counter[str]) -> str:
    if field == "event_type" and "touchdown" in counter:
        specific_touchdowns = [value for value in counter if value.endswith("_touchdown")]
        if len(specific_touchdowns) == 1:
            return specific_touchdowns[0]
    return counter.most_common(1)[0][0]


def parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
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
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def latest_materialized_ingest_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT ingest_run_id
        FROM newspaper_review.llm_materialize_run
        ORDER BY created_at_utc DESC, materialize_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else ""


def load_rows(db_path: Path, ingest_run_id: str | None) -> tuple[str, dict[str, list[dict[str, Any]]]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        selected_ingest_run = ingest_run_id or latest_materialized_ingest_run(con)
        rows_by_table: dict[str, list[dict[str, Any]]] = {table: [] for table in TARGET_TABLES}
        if not selected_ingest_run:
            return "", rows_by_table
        for table in TARGET_TABLES:
            result = con.execute(
                f"SELECT * FROM newspaper_review.llm_{table} WHERE ingest_run_id = ?",
                [selected_ingest_run],
            )
            fields = [item[0] for item in result.description]
            rows_by_table[table] = [dict(zip(fields, row)) for row in result.fetchall()]
        return selected_ingest_run, rows_by_table
    finally:
        con.close()


def row_fields(row: dict[str, Any]) -> dict[str, Any]:
    parsed = parse_json_object(row.get("target_fields_json"))
    if parsed:
        for field in PROVENANCE_TARGET_FIELDS:
            parsed.pop(field, None)
        return parsed
    skip = {
        "llm_materialized_row_id",
        "llm_review_claim_id",
        "atom_claim_id",
        "ingest_run_id",
        "packet_id",
        "source_archive",
        "publication",
        "issue_date",
        "page",
        "source_url",
        "asset_pdf_path",
        "row_group_key",
        "target_fields_json",
        "field_evidence_json",
        "confidence_lane",
        "promotion_recommendation",
        "llm_reason",
        "llm_validation_status",
        "llm_validation_notes",
        "unmapped_fields_json",
        "review_output_path",
        "created_at_utc",
    }
    return {key: value for key, value in row.items() if key not in skip and clean(value)}


def game_entity_key(row: dict[str, Any]) -> str:
    teams = sorted([clean(row.get("team_1_raw")), clean(row.get("team_2_raw"))])
    return "|".join(["game_candidate", clean(row.get("boxscore_id")), *teams])


def entity_key(target_table: str, row: dict[str, Any]) -> str:
    boxscore_id = clean(row.get("boxscore_id"))
    row_group_key = clean(row.get("row_group_key"))
    if row_group_key:
        return "|".join([target_table, boxscore_id, row_group_key])
    if target_table == "game_candidate":
        return game_entity_key(row)
    if target_table == "scoring_event":
        return "|".join([
            target_table,
            boxscore_id,
            clean(row.get("event_order")),
            clean(row.get("period_raw")),
            clean(row.get("scoring_team_raw")),
            clean(row.get("scoring_player_raw")),
            clean(row.get("event_type")),
            clean(row.get("row_group_key")),
        ])
    if target_table == "play_by_play_event":
        return "|".join([
            target_table,
            boxscore_id,
            clean(row.get("event_order")),
            clean(row.get("primary_player_raw")),
            clean(row.get("play_type")),
            clean(row.get("row_group_key")),
        ])
    if target_table == "player_game_box_score":
        return "|".join([
            target_table,
            boxscore_id,
            clean(row.get("player_raw")),
            clean(row.get("nfl_team")),
            clean(row.get("position")),
        ])
    if target_table == "lineup_participation":
        return "|".join([
            target_table,
            boxscore_id,
            clean(row.get("player_raw")),
            clean(row.get("team_raw")),
            clean(row.get("listed_position_raw")),
            clean(row.get("participation_type")),
        ])
    if target_table == "team_game_stat_claim":
        return "|".join([
            target_table,
            boxscore_id,
            clean(row.get("stat_name")),
            clean(row.get("team_1_nfl_team")),
            clean(row.get("team_2_nfl_team")),
        ])
    if target_table == "player_identity_candidate":
        return "|".join([
            target_table,
            boxscore_id,
            clean(row.get("raw_player_name")),
            clean(row.get("raw_team")),
        ])
    return "|".join([target_table, boxscore_id, clean(row.get("row_group_key")), clean(row.get("llm_materialized_row_id"))])


def consensus_fields(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    values_by_field: dict[str, Counter[str]] = defaultdict(Counter)
    sources_by_field_value: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        fields = row_fields(row)
        for field, value in fields.items():
            value_text = clean(value)
            if not value_text:
                continue
            values_by_field[field][value_text] += 1
            sources_by_field_value[(field, value_text)].add(clean(row.get("source_document_id")))

    proposed: dict[str, Any] = {}
    conflicts: list[dict[str, Any]] = []
    for field, counter in sorted(values_by_field.items()):
        if not counter:
            continue
        proposed[field] = proposed_value(field, counter)
        normalized_values = compatible_normalized_values(
            field,
            {normalize_conflict_value(field, value) for value in counter},
        )
        if len(counter) > 1 and len(normalized_values) > 1 and field not in NON_BLOCKING_CONFLICT_FIELDS:
            values_payload = [
                {
                    "value": value,
                    "count": count,
                    "source_documents": sorted(sources_by_field_value[(field, value)]),
                }
                for value, count in counter.most_common()
            ]
            conflicts.append({
                "field_name": field,
                "value_count": len(counter),
                "values": values_payload,
                "source_documents": sorted({doc for item in values_payload for doc in item["source_documents"]}),
            })
    return proposed, conflicts


def confidence_bar(scores: list[float], lanes: list[str], conflict_count: int) -> str:
    if conflict_count:
        return "conflict"
    max_score = max(scores) if scores else 0
    if "high" in lanes or max_score >= 80:
        return "high"
    if "medium" in lanes or max_score >= 55:
        return "medium"
    return "low"


def package_status(rows: list[dict[str, Any]], conflict_count: int, bar: str) -> str:
    if conflict_count:
        return "needs_conflict_review"
    recommendations = {clean(row.get("promotion_recommendation")) for row in rows}
    if "promote" in recommendations and bar == "high":
        return "ready_for_promotion_review"
    if "promote" in recommendations:
        return "needs_quality_review"
    return "needs_review"


def evidence_text(row: dict[str, Any]) -> str:
    for field in ["evidence_text", "play_text", "source_row_text"]:
        value = clean(row.get(field))
        if value:
            return value
    return clean(row.get("target_fields_json"))


def build_packages(
    package_run_id: str,
    ingest_run_id: str,
    rows_by_table: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    created_at = iso_now()
    package_rows: list[dict[str, Any]] = []
    item_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []

    for target_table, rows in rows_by_table.items():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[entity_key(target_table, row)].append(row)

        for target_entity_key, group_rows in sorted(groups.items()):
            proposed, conflicts = consensus_fields(group_rows)
            scores = [score for score in (parse_float(row.get("confidence_score")) for row in group_rows) if score is not None]
            lanes = [clean(row.get("confidence_lane")) for row in group_rows if clean(row.get("confidence_lane"))]
            bar = confidence_bar(scores, lanes, len(conflicts))
            package_id = stable_id(package_run_id, ingest_run_id, target_table, target_entity_key)
            source_documents = sorted({clean(row.get("source_document_id")) for row in group_rows if clean(row.get("source_document_id"))})
            package_rows.append({
                "promotion_package_id": package_id,
                "package_run_id": package_run_id,
                "ingest_run_id": ingest_run_id,
                "target_table": target_table,
                "target_entity_key": target_entity_key,
                "boxscore_id": clean(group_rows[0].get("boxscore_id")),
                "package_status": package_status(group_rows, len(conflicts), bar),
                "confidence_bar": bar,
                "max_confidence_score": f"{max(scores):.0f}" if scores else "",
                "avg_confidence_score": f"{(sum(scores) / len(scores)):.1f}" if scores else "",
                "evidence_document_count": len(source_documents),
                "item_count": len(group_rows),
                "promote_item_count": sum(1 for row in group_rows if clean(row.get("promotion_recommendation")) == "promote"),
                "review_item_count": sum(1 for row in group_rows if clean(row.get("promotion_recommendation")) == "review"),
                "conflict_count": len(conflicts),
                "proposed_fields_json": json.dumps(proposed, sort_keys=True, ensure_ascii=False),
                "source_documents_json": json.dumps(source_documents, sort_keys=True, ensure_ascii=False),
                "materialized_row_ids_json": json.dumps(
                    [clean(row.get("llm_materialized_row_id")) for row in group_rows],
                    sort_keys=True,
                    ensure_ascii=False,
                ),
                "notes": "",
                "created_at_utc": created_at,
            })

            for row in group_rows:
                item_rows.append({
                    "promotion_package_item_id": stable_id(package_id, row.get("llm_materialized_row_id")),
                    "promotion_package_id": package_id,
                    "package_run_id": package_run_id,
                    "ingest_run_id": ingest_run_id,
                    "target_table": target_table,
                    "target_entity_key": target_entity_key,
                    "llm_materialized_row_id": clean(row.get("llm_materialized_row_id")),
                    "llm_review_claim_id": clean(row.get("llm_review_claim_id")),
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "source_document_id": clean(row.get("source_document_id")),
                    "publication": clean(row.get("publication")),
                    "source_url": clean(row.get("source_url")),
                    "asset_pdf_path": clean(row.get("asset_pdf_path")),
                    "region_id": clean(row.get("region_id")),
                    "evidence_text": evidence_text(row),
                    "confidence_score": clean(row.get("confidence_score")),
                    "confidence_lane": clean(row.get("confidence_lane")),
                    "promotion_recommendation": clean(row.get("promotion_recommendation")),
                    "row_group_key": clean(row.get("row_group_key")),
                    "row_fields_json": json.dumps(row_fields(row), sort_keys=True, ensure_ascii=False),
                    "llm_reason": clean(row.get("llm_reason")),
                    "created_at_utc": created_at,
                })

            for conflict in conflicts:
                conflict_rows.append({
                    "promotion_conflict_id": stable_id(package_id, conflict["field_name"]),
                    "promotion_package_id": package_id,
                    "package_run_id": package_run_id,
                    "ingest_run_id": ingest_run_id,
                    "target_table": target_table,
                    "target_entity_key": target_entity_key,
                    "field_name": conflict["field_name"],
                    "value_count": conflict["value_count"],
                    "values_json": json.dumps(conflict["values"], sort_keys=True, ensure_ascii=False),
                    "source_documents_json": json.dumps(conflict["source_documents"], sort_keys=True, ensure_ascii=False),
                    "status": "open",
                    "created_at_utc": created_at,
                })
    return package_rows, item_rows, conflict_rows


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table_name, fields in [
        ("llm_promotion_package", PACKAGE_FIELDS),
        ("llm_promotion_package_item", ITEM_FIELDS),
        ("llm_promotion_conflict", CONFLICT_FIELDS),
    ]:
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.{table_name} ({defs})")
        existing = {
            row[0] for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review' AND table_name=?
                """,
                [table_name],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table_name} ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_promotion_package_run (
          package_run_id VARCHAR,
          ingest_run_id VARCHAR,
          output_dir VARCHAR,
          package_count VARCHAR,
          item_count VARCHAR,
          conflict_count VARCHAR,
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
    package_run_id: str,
    ingest_run_id: str,
    out_dir: Path,
    package_rows: list[dict[str, Any]],
    item_rows: list[dict[str, Any]],
    conflict_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        for table in [
            "llm_promotion_package",
            "llm_promotion_package_item",
            "llm_promotion_conflict",
            "llm_promotion_package_run",
        ]:
            con.execute(f"DELETE FROM newspaper_review.{table} WHERE package_run_id = ?", [package_run_id])
        insert_rows(con, "llm_promotion_package", package_rows, PACKAGE_FIELDS)
        insert_rows(con, "llm_promotion_package_item", item_rows, ITEM_FIELDS)
        insert_rows(con, "llm_promotion_conflict", conflict_rows, CONFLICT_FIELDS)
        con.execute(
            """
            INSERT INTO newspaper_review.llm_promotion_package_run
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                package_run_id,
                ingest_run_id,
                str(out_dir),
                str(len(package_rows)),
                str(len(item_rows)),
                str(len(conflict_rows)),
                "complete",
                iso_now(),
                str(summary_path),
            ],
        )
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--ingest-run-id", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="llm_promotion_packages")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    package_run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / package_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    ingest_run_id, rows_by_table = load_rows(args.db_path, args.ingest_run_id or None)
    package_rows, item_rows, conflict_rows = build_packages(package_run_id, ingest_run_id, rows_by_table)

    write_csv(out_dir / "llm_promotion_packages.csv", package_rows, PACKAGE_FIELDS)
    write_csv(out_dir / "llm_promotion_package_items.csv", item_rows, ITEM_FIELDS)
    write_csv(out_dir / "llm_promotion_conflicts.csv", conflict_rows, CONFLICT_FIELDS)
    rollup_rows = []
    package_counts = Counter(row["target_table"] for row in package_rows)
    item_counts = Counter(row["target_table"] for row in item_rows)
    conflict_counts = Counter(row["target_table"] for row in conflict_rows)
    for target_table in TARGET_TABLES:
        rollup_rows.append({
            "target_table": target_table,
            "package_count": package_counts.get(target_table, 0),
            "item_count": item_counts.get(target_table, 0),
            "conflict_count": conflict_counts.get(target_table, 0),
            "ready_for_promotion_review_count": sum(
                1 for row in package_rows
                if row["target_table"] == target_table and row["package_status"] == "ready_for_promotion_review"
            ),
        })
    write_csv(out_dir / "llm_promotion_package_rollup.csv", rollup_rows, [
        "target_table",
        "package_count",
        "item_count",
        "conflict_count",
        "ready_for_promotion_review_count",
    ])

    summary = {
        "created_at_utc": iso_now(),
        "package_run_id": package_run_id,
        "ingest_run_id": ingest_run_id,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "package_count": len(package_rows),
        "item_count": len(item_rows),
        "conflict_count": len(conflict_rows),
        "package_status_counts": dict(Counter(row["package_status"] for row in package_rows)),
        "target_table_package_counts": dict(package_counts),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_json(summary_path, summary)
    (out_dir / "README.md").write_text(
        "\n".join([
            "# Newspaper LLM Promotion Packages",
            "",
            "These are local review packages built from materialized LLM newspaper rows.",
            "They group corroborating evidence and flag field-level conflicts before any live promotion.",
            "",
        ]),
        encoding="utf-8",
    )
    if not args.no_db:
        persist(args.db_path, package_run_id, ingest_run_id, out_dir, package_rows, item_rows, conflict_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
