#!/usr/bin/env python
"""Materialize LLM-reviewed newspaper claims into table-specific local rows.

The LLM ingest table is intentionally generic so it can preserve whatever a
reviewer saw. This station turns those reviewed claims into the separate,
football-shaped staging tables we need before any promotion step:

- llm_game_candidate
- llm_scoring_event
- llm_play_by_play_event
- llm_player_game_box_score
- llm_lineup_participation
- llm_player_identity_candidate
- llm_promotion_candidate

All outputs remain local D-drive review artifacts. No live supertable/Fly writes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\llm_materialized_rows")

META_PREFIX_FIELDS = [
    "llm_materialized_row_id",
    "llm_review_claim_id",
]

META_SUFFIX_FIELDS = [
    "ingest_run_id",
    "packet_id",
    "source_document_id",
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
]

TABLE_FIELDS = {
    "game_candidate": [
        "game_candidate_id", "atom_claim_id", "boxscore_id", "game_date", "year", "week", "season_type",
        "team_1_raw", "team_2_raw", "team_1_resolved", "team_2_resolved", "team_1_score", "team_2_score",
        "reconciliation_status", "confidence_score", "evidence_text", "source_document_id", "region_id",
    ],
    "scoring_event": [
        "scoring_event_id", "atom_claim_id", "boxscore_id", "event_order", "period_raw", "clock_raw",
        "scoring_team_raw", "scoring_team", "scoring_player_raw", "scoring_NFL_player_id", "passer_raw",
        "passer_NFL_player_id", "receiver_raw", "receiver_NFL_player_id", "event_type", "points",
        "distance_yards", "play_text", "confidence_score", "review_status", "promotion_status",
        "source_document_id", "region_id",
    ],
    "play_by_play_event": [
        "pbp_event_id", "atom_claim_id", "boxscore_id", "event_order", "period_raw", "clock_raw",
        "possession_team_raw", "possession_team", "down_raw", "distance_raw", "yardline_raw", "play_type",
        "primary_player_raw", "primary_NFL_player_id", "secondary_player_raw", "secondary_NFL_player_id",
        "yards", "points", "play_text", "confidence_score", "review_status", "promotion_status",
        "source_document_id", "region_id",
    ],
    "player_game_box_score": [
        "player_game_box_score_id", "atom_claim_id", "boxscore_id", "player_week", "player_raw",
        "NFL_player_id", "nfl_team", "opponent_nfl_team", "position", "starter_position", "is_starter",
        "carries", "rushing_yards", "rushing_tds", "attempts", "completions", "passing_yards",
        "passing_tds", "passing_interceptions", "receptions", "receiving_yards", "receiving_tds",
        "pat_made", "pat_att", "fg_made", "fg_att", "fg_long", "def_interceptions", "def_sacks",
        "def_tds", "special_teams_tds", "touchdowns", "source_row_text", "confidence_score", "review_status",
        "promotion_status", "source_document_id", "region_id",
    ],
    "lineup_participation": [
        "lineup_participation_id", "atom_claim_id", "boxscore_id", "player_week", "player_raw",
        "NFL_player_id", "team_raw", "nfl_team", "opponent_nfl_team", "listed_position_raw",
        "starter_position", "is_starter", "participation_type", "lineup_side_raw", "source_row_text",
        "confidence_score", "review_status", "promotion_status", "source_document_id", "region_id",
    ],
    "team_game_stat_claim": [
        "team_game_stat_claim_id", "atom_claim_id", "boxscore_id", "game_date", "year", "week",
        "stat_name", "stat_unit", "team_1_raw", "team_1_nfl_team", "team_1_value",
        "team_1_opponent_nfl_team", "team_2_raw", "team_2_nfl_team", "team_2_value",
        "team_2_opponent_nfl_team", "claimed_score_text", "reconciliation_status",
        "stat_context", "source_row_text", "evidence_text", "confidence_score", "review_status",
        "promotion_status", "source_document_id", "region_id", "match_method",
    ],
    "player_identity_candidate": [
        "identity_candidate_id", "atom_claim_id", "raw_player_name", "raw_team", "resolved_player",
        "NFL_player_id", "player_week", "nfl_team", "opponent_nfl_team", "boxscore_id", "match_method",
        "confidence_score", "review_status", "evidence_text", "source_document_id", "region_id",
    ],
    "promotion_candidate": [
        "promotion_candidate_id", "promotion_package_id", "atom_claim_id", "source_domain_table",
        "target_table", "target_row_key", "target_field", "existing_value", "proposed_value",
        "confidence_score", "review_status", "reviewer", "reviewed_at_utc", "promotion_status",
        "promotion_notes",
    ],
}

ID_FIELDS = {
    "game_candidate": "game_candidate_id",
    "scoring_event": "scoring_event_id",
    "play_by_play_event": "pbp_event_id",
    "player_game_box_score": "player_game_box_score_id",
    "lineup_participation": "lineup_participation_id",
    "team_game_stat_claim": "team_game_stat_claim_id",
    "player_identity_candidate": "identity_candidate_id",
    "promotion_candidate": "promotion_candidate_id",
}

FIELD_ALIASES = {
    "game_candidate": {
        "team1": "team_1_raw",
        "team_1": "team_1_raw",
        "team2": "team_2_raw",
        "team_2": "team_2_raw",
        "team1_score": "team_1_score",
        "team2_score": "team_2_score",
        "score_1": "team_1_score",
        "score_2": "team_2_score",
        "evidence_quote": "evidence_text",
    },
    "scoring_event": {
        "order": "event_order",
        "quarter": "period_raw",
        "period": "period_raw",
        "time": "clock_raw",
        "clock": "clock_raw",
        "team": "scoring_team_raw",
        "player": "scoring_player_raw",
        "scorer": "scoring_player_raw",
        "passer": "passer_raw",
        "receiver": "receiver_raw",
        "type": "event_type",
        "yards": "distance_yards",
        "distance": "distance_yards",
        "text": "play_text",
        "evidence_quote": "play_text",
    },
    "play_by_play_event": {
        "order": "event_order",
        "quarter": "period_raw",
        "period": "period_raw",
        "time": "clock_raw",
        "clock": "clock_raw",
        "team": "possession_team_raw",
        "possession_team": "possession_team_raw",
        "down": "down_raw",
        "distance": "distance_raw",
        "yardline": "yardline_raw",
        "type": "play_type",
        "player": "primary_player_raw",
        "primary_player": "primary_player_raw",
        "secondary_player": "secondary_player_raw",
        "text": "play_text",
        "evidence_quote": "play_text",
    },
    "player_game_box_score": {
        "player": "player_raw",
        "team": "nfl_team",
        "opponent": "opponent_nfl_team",
        "pos": "position",
        "starter": "is_starter",
        "rush_att": "carries",
        "rushes": "carries",
        "rush_yds": "rushing_yards",
        "rush_yards": "rushing_yards",
        "rush_td": "rushing_tds",
        "rush_tds": "rushing_tds",
        "pass_att": "attempts",
        "pass_cmp": "completions",
        "pass_comp": "completions",
        "pass_yds": "passing_yards",
        "pass_yards": "passing_yards",
        "pass_td": "passing_tds",
        "pass_tds": "passing_tds",
        "pass_int": "passing_interceptions",
        "pass_ints": "passing_interceptions",
        "rec": "receptions",
        "recv": "receptions",
        "receiving_receptions": "receptions",
        "rec_yds": "receiving_yards",
        "recv_yds": "receiving_yards",
        "rec_yards": "receiving_yards",
        "rec_td": "receiving_tds",
        "rec_tds": "receiving_tds",
        "td": "touchdowns",
        "tds": "touchdowns",
        "fgm": "fg_made",
        "fga": "fg_att",
        "xpm": "pat_made",
        "xpa": "pat_att",
        "text": "source_row_text",
        "evidence_quote": "source_row_text",
    },
    "lineup_participation": {
        "player": "player_raw",
        "team": "team_raw",
        "position": "listed_position_raw",
        "pos": "listed_position_raw",
        "starter": "is_starter",
        "side": "lineup_side_raw",
        "text": "source_row_text",
        "evidence_quote": "source_row_text",
    },
    "team_game_stat_claim": {
        "game": "boxscore_id",
        "date": "game_date",
        "stat": "stat_name",
        "unit": "stat_unit",
        "team1": "team_1_raw",
        "team_1": "team_1_raw",
        "team1_code": "team_1_nfl_team",
        "team_1_code": "team_1_nfl_team",
        "team1_value": "team_1_value",
        "team_1_stat": "team_1_value",
        "team2": "team_2_raw",
        "team_2": "team_2_raw",
        "team2_code": "team_2_nfl_team",
        "team_2_code": "team_2_nfl_team",
        "team2_value": "team_2_value",
        "team_2_stat": "team_2_value",
        "context": "stat_context",
        "text": "source_row_text",
        "evidence_quote": "evidence_text",
    },
    "player_identity_candidate": {
        "player": "raw_player_name",
        "team": "raw_team",
        "resolved": "resolved_player",
        "evidence_quote": "evidence_text",
    },
    "promotion_candidate": {
        "domain_table": "source_domain_table",
        "row_key": "target_row_key",
        "field": "target_field",
        "value": "proposed_value",
        "notes": "promotion_notes",
    },
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


def normalize_key(key: str) -> str:
    return key.strip().lower().replace(" ", "_").replace("-", "_")


def all_output_fields(target_table: str) -> list[str]:
    base_fields = TABLE_FIELDS[target_table]
    fields = list(META_PREFIX_FIELDS)
    for field in base_fields:
        if field not in fields:
            fields.append(field)
    for field in META_SUFFIX_FIELDS:
        if field not in fields:
            fields.append(field)
    return fields


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def latest_ingest_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT ingest_run_id
        FROM newspaper_review.llm_review_ingest_run
        ORDER BY created_at_utc DESC, ingest_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else ""


def load_claims(db_path: Path, ingest_run_id: str | None, include_non_promotable: bool) -> tuple[str, list[dict[str, Any]]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        selected_ingest_run = ingest_run_id or latest_ingest_run(con)
        if not selected_ingest_run:
            return "", []
        params: list[Any] = [selected_ingest_run]
        promotion_filter = ""
        if not include_non_promotable:
            promotion_filter = "AND c.promotion_recommendation IN ('promote', 'review')"
        result = con.execute(
            f"""
            SELECT
              c.*,
              d.source_archive,
              d.publication,
              d.issue_date,
              d.page,
              d.source_url,
              d.asset_pdf_path,
              d.boxscore_id AS source_document_boxscore_id
            FROM newspaper_review.llm_review_claim c
            LEFT JOIN newspaper_review.source_document d
              ON d.source_document_id = c.source_document_id
            WHERE c.ingest_run_id = ?
              AND c.target_table IN ({','.join(['?'] * len(TABLE_FIELDS))})
              {promotion_filter}
            ORDER BY c.target_table, c.source_document_id, c.claim_index
            """,
            params + list(TABLE_FIELDS.keys()),
        )
        fields = [item[0] for item in result.description]
        return selected_ingest_run, [dict(zip(fields, row)) for row in result.fetchall()]
    finally:
        con.close()


def canonicalize_fields(target_table: str, fields: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    canonical_fields = set(TABLE_FIELDS[target_table])
    aliases = FIELD_ALIASES.get(target_table, {})
    mapped: dict[str, Any] = {}
    unmapped: dict[str, Any] = {}
    for key, value in fields.items():
        normalized = normalize_key(str(key))
        canonical = aliases.get(normalized, normalized)
        if canonical in canonical_fields:
            mapped[canonical] = value
        else:
            unmapped[key] = value
    return mapped, unmapped


def fields_from_claim(claim: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    target_table = claim["target_table"]
    target_fields = parse_json_object(claim.get("target_fields_json"))
    if not target_fields:
        target_field = clean(claim.get("target_field"))
        if target_field:
            target_fields[target_field] = (
                claim.get("normalized_value")
                or claim.get("numeric_value")
                or claim.get("raw_value")
            )
    mapped, unmapped = canonicalize_fields(target_table, target_fields)
    return mapped, unmapped


def materialize_claim(claim: dict[str, Any], materialize_run_id: str) -> dict[str, Any]:
    target_table = claim["target_table"]
    mapped_fields, unmapped_fields = fields_from_claim(claim)
    domain_fields = TABLE_FIELDS[target_table]
    output_fields = all_output_fields(target_table)
    row = {field: "" for field in output_fields}

    llm_review_claim_id = clean(claim.get("llm_review_claim_id"))
    row_id = stable_id(
        materialize_run_id,
        llm_review_claim_id,
        target_table,
        claim.get("row_group_key"),
        claim.get("target_fields_json"),
        claim.get("target_field"),
        claim.get("normalized_value"),
        claim.get("raw_value"),
    )
    id_field = ID_FIELDS[target_table]
    domain_row_id = stable_id("llm_domain", target_table, row_id)

    if "boxscore_id" in mapped_fields:
        boxscore_id = clean(mapped_fields.get("boxscore_id"))
    else:
        boxscore_id = clean(claim.get("source_document_boxscore_id"))

    row.update({
        "llm_materialized_row_id": row_id,
        "llm_review_claim_id": llm_review_claim_id,
        id_field: domain_row_id,
        "atom_claim_id": llm_review_claim_id,
        "boxscore_id": boxscore_id,
        "source_document_id": clean(claim.get("source_document_id")),
        "region_id": clean(claim.get("evidence_region_id")),
        "confidence_score": clean(claim.get("confidence_score")),
        "review_status": "llm_reviewed",
        "promotion_status": clean(claim.get("promotion_recommendation")),
        "evidence_text": clean(claim.get("evidence_quote")),
        "play_text": clean(claim.get("evidence_quote")),
        "source_row_text": clean(claim.get("evidence_quote")),
        "ingest_run_id": clean(claim.get("ingest_run_id")),
        "packet_id": clean(claim.get("packet_id")),
        "source_archive": clean(claim.get("source_archive")),
        "publication": clean(claim.get("publication")),
        "issue_date": clean(claim.get("issue_date")),
        "page": clean(claim.get("page")),
        "source_url": clean(claim.get("source_url")),
        "asset_pdf_path": clean(claim.get("asset_pdf_path")),
        "row_group_key": clean(claim.get("row_group_key")),
        "target_fields_json": clean(claim.get("target_fields_json")),
        "field_evidence_json": clean(claim.get("field_evidence_json")),
        "confidence_lane": clean(claim.get("confidence_lane")),
        "promotion_recommendation": clean(claim.get("promotion_recommendation")),
        "llm_reason": clean(claim.get("reason")),
        "llm_validation_status": clean(claim.get("validation_status")),
        "llm_validation_notes": clean(claim.get("validation_notes")),
        "unmapped_fields_json": json.dumps(unmapped_fields, sort_keys=True, ensure_ascii=False) if unmapped_fields else "",
        "review_output_path": clean(claim.get("review_output_path")),
        "created_at_utc": iso_now(),
    })

    for field, value in mapped_fields.items():
        if field in domain_fields:
            row[field] = clean(value)

    if target_table == "promotion_candidate":
        row["promotion_package_id"] = materialize_run_id
        row["source_domain_table"] = row["source_domain_table"] or clean(claim.get("target_table"))
        row["target_table"] = row["target_table"] or clean(claim.get("target_table"))
        row["target_field"] = row["target_field"] or clean(claim.get("target_field"))
        row["target_row_key"] = row["target_row_key"] or clean(claim.get("row_group_key"))
        row["proposed_value"] = row["proposed_value"] or clean(claim.get("normalized_value") or claim.get("raw_value"))
        row["reviewer"] = "llm"
        row["reviewed_at_utc"] = row["created_at_utc"]
        row["promotion_notes"] = row["promotion_notes"] or clean(claim.get("reason"))
    return row


def create_materialized_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for target_table in TABLE_FIELDS:
        table_name = f"newspaper_review.llm_{target_table}"
        fields = all_output_fields(target_table)
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({defs})")
        existing = {
            row[0] for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review' AND table_name=?
                """,
                [f"llm_{target_table}"],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_materialize_run (
          materialize_run_id VARCHAR,
          ingest_run_id VARCHAR,
          output_dir VARCHAR,
          status VARCHAR,
          claim_count VARCHAR,
          materialized_row_count VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def persist_materialized_rows(
    db_path: Path,
    materialize_run_id: str,
    ingest_run_id: str,
    out_dir: Path,
    rows_by_target: dict[str, list[dict[str, Any]]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_materialized_tables(con)
        for target_table, rows in rows_by_target.items():
            table_name = f"newspaper_review.llm_{target_table}"
            con.execute(f"DELETE FROM {table_name} WHERE llm_materialized_row_id IN (SELECT llm_materialized_row_id FROM {table_name} WHERE ingest_run_id = ?)", [ingest_run_id])
            if rows:
                fields = all_output_fields(target_table)
                placeholders = ",".join(["?"] * len(fields))
                con.executemany(
                    f"INSERT INTO {table_name} ({','.join(fields)}) VALUES ({placeholders})",
                    [[row.get(field, "") for field in fields] for row in rows],
                )
        con.execute(
            "CREATE TABLE IF NOT EXISTS newspaper_review.llm_materialize_run (materialize_run_id VARCHAR, ingest_run_id VARCHAR, output_dir VARCHAR, status VARCHAR, claim_count VARCHAR, materialized_row_count VARCHAR, created_at_utc VARCHAR, summary_json_path VARCHAR)"
        )
        con.execute("DELETE FROM newspaper_review.llm_materialize_run WHERE materialize_run_id = ?", [materialize_run_id])
        con.execute(
            "INSERT INTO newspaper_review.llm_materialize_run VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                materialize_run_id,
                ingest_run_id,
                str(out_dir),
                "complete",
                str(sum(len(rows) for rows in rows_by_target.values())),
                str(sum(len(rows) for rows in rows_by_target.values())),
                iso_now(),
                str(summary_path),
            ],
        )
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--ingest-run-id", default="", help="Defaults to latest newspaper_review.llm_review_ingest_run.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="llm_materialized_rows")
    parser.add_argument("--include-non-promotable", action="store_true")
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    materialize_run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / materialize_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    ingest_run_id, claims = load_claims(
        args.db_path,
        args.ingest_run_id or None,
        include_non_promotable=args.include_non_promotable,
    )
    rows_by_target: dict[str, list[dict[str, Any]]] = {target: [] for target in TABLE_FIELDS}
    for claim in claims:
        target_table = claim.get("target_table")
        if target_table not in TABLE_FIELDS:
            continue
        rows_by_target[target_table].append(materialize_claim(claim, materialize_run_id))

    for target_table, rows in rows_by_target.items():
        write_csv(out_dir / f"llm_{target_table}.csv", rows, all_output_fields(target_table))

    rollup_rows = []
    for target_table, rows in rows_by_target.items():
        rollup_rows.append({
            "target_table": target_table,
            "materialized_row_count": len(rows),
            "promote_count": sum(1 for row in rows if row.get("promotion_recommendation") == "promote"),
            "review_count": sum(1 for row in rows if row.get("promotion_recommendation") == "review"),
            "unmapped_field_row_count": sum(1 for row in rows if row.get("unmapped_fields_json")),
        })
    write_csv(out_dir / "llm_materialized_table_rollup.csv", rollup_rows, [
        "target_table",
        "materialized_row_count",
        "promote_count",
        "review_count",
        "unmapped_field_row_count",
    ])

    summary = {
        "created_at_utc": iso_now(),
        "materialize_run_id": materialize_run_id,
        "ingest_run_id": ingest_run_id,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "claims_loaded": len(claims),
        "include_non_promotable": args.include_non_promotable,
        "persisted_to_duckdb": not args.no_db,
        "materialized_row_counts": {target: len(rows) for target, rows in rows_by_target.items()},
    }
    summary_path = out_dir / "summary.json"
    write_json(summary_path, summary)
    (out_dir / "README.md").write_text(
        "\n".join([
            "# Newspaper LLM Materialized Rows",
            "",
            "This folder contains table-specific rows materialized from LLM-reviewed newspaper claims.",
            "The corresponding DuckDB tables are named `newspaper_review.llm_<target_table>`.",
            "",
            "These rows remain local review staging. They are not live supertable writes.",
            "",
        ]),
        encoding="utf-8",
    )

    if not args.no_db:
        persist_materialized_rows(args.db_path, materialize_run_id, ingest_run_id, out_dir, rows_by_target, summary_path)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
