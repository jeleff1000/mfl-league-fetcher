#!/usr/bin/env python
"""Package resolved newspaper atoms into final local review lanes.

This station reads the latest `newspaper_resolved.*` materialization and the
latest game-mapping triage, then writes auditable local package queues for
ready promotion review, score backfills, archives, conflicts, and manual
follow-up. It does not write to Fly, v26, or production supertable tables.
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_promotion_packages"

RESOLVED_TABLES = [
    "game_candidate",
    "lineup_participation",
    "play_by_play_event",
    "player_game_box_score",
    "player_identity_candidate",
    "scoring_event",
    "team_game_stat_claim",
]

RUN_FIELDS = [
    "resolved_promotion_package_run_id",
    "resolved_atom_materialize_run_id",
    "resolved_game_mapping_triage_run_id",
    "output_dir",
    "input_row_count",
    "package_item_count",
    "ready_review_count",
    "score_backfill_review_count",
    "archive_count",
    "conflict_count",
    "manual_followup_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

ITEM_FIELDS = [
    "resolved_promotion_package_run_id",
    "resolved_atom_materialize_run_id",
    "resolved_game_mapping_triage_run_id",
    "package_item_id",
    "package_lane",
    "review_status",
    "risk_level",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "game_date",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
    "materialization_status",
    "triage_status",
    "action_status",
    "value_class",
    "v26_status",
    "identity_status",
    "confidence_bar",
    "source_document_count",
    "proposed_fields_json",
    "full_row_json",
    "reason",
    "source_documents_json",
    "evidence_text",
    "created_at_utc",
]

ROLLUP_FIELDS = [
    "package_lane",
    "target_table",
    "review_status",
    "risk_level",
    "item_count",
]

DECISION_FIELDS = [
    "decision_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_target_table",
    "resolved_target_entity_key",
    "resolved_boxscore_id",
    "proposed_fields_json",
    "notes",
]

META_FIELDS = {
    "resolved_atom_materialize_run_id",
    "readiness_promotion_lane_run_id",
    "readiness_run_id",
    "identity_resolution_run_id",
    "source_readiness_row_id",
    "promotion_lane",
    "action_status",
    "materialization_status",
    "schema_gap_patch_run_id",
    "schema_resolution_status",
    "schema_patch_fields_json",
    "schema_patch_reason",
    "risk_level",
    "value_class",
    "v26_status",
    "identity_status",
    "lane_target_table",
    "created_at_utc",
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
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return str(value)


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def parse_json_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: clean(row.get(field)) for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True) + "\n", encoding="utf-8")


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_run_id(con: duckdb.DuckDBPyConnection, table: str, id_field: str) -> str:
    try:
        row = con.execute(
            f"""
            SELECT {id_field}
            FROM newspaper_review.{table}
            ORDER BY created_at_utc DESC, {id_field} DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        return ""
    return clean(row[0]) if row else ""


def load_resolved_rows(con: duckdb.DuckDBPyConnection, resolved_run_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in RESOLVED_TABLES:
        try:
            table_rows = query_dicts(
                con,
                f"""
                SELECT *, '{table}' AS _target_table
                FROM newspaper_resolved.{table}
                WHERE resolved_atom_materialize_run_id = ?
                """,
                [resolved_run_id],
            )
        except Exception:
            table_rows = []
        rows.extend(table_rows)
    return rows


def load_game_triage(con: duckdb.DuckDBPyConnection, triage_run_id: str) -> dict[str, dict[str, Any]]:
    if not triage_run_id:
        return {}
    try:
        rows = query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.resolved_game_mapping_triage_item
            WHERE resolved_game_mapping_triage_run_id = ?
            """,
            [triage_run_id],
        )
    except Exception:
        return {}
    return {
        clean(row.get("target_entity_key")): row
        for row in rows
        if clean(row.get("target_entity_key"))
    }


def proposed_fields(row: dict[str, Any]) -> dict[str, str]:
    return {
        field: clean(value)
        for field, value in row.items()
        if field not in META_FIELDS
        and not field.startswith("_")
        and clean(value).strip()
    }


def evidence_text(row: dict[str, Any]) -> str:
    for field in ["evidence_text", "source_row_text", "play_text", "note_text"]:
        value = clean(row.get(field)).strip()
        if value:
            return value
    return ""


def package_lane(row: dict[str, Any], triage: dict[str, Any]) -> tuple[str, str, str, str]:
    table = clean(row.get("_target_table"))
    status = clean(row.get("materialization_status"))
    if table == "game_candidate" and triage:
        action = clean(triage.get("action_status"))
        if action == "ready_score_backfill_review":
            return "score_backfill_review", "pending_review", "medium", clean(triage.get("reason"))
        if action == "archive_corroboration_only":
            return "corroboration_archive", "archive", "low", clean(triage.get("reason"))
        if action == "archive_context_only":
            return "context_archive", "archive", "low", clean(triage.get("reason"))
        if action == "conflict_review":
            return "score_conflict_review", "needs_review", "high", clean(triage.get("reason"))
        if action == "manual_game_mapping_review":
            return "manual_game_mapping_review", "needs_review", "medium", clean(triage.get("reason"))
    if status == "ready_for_resolved_review":
        return "ready_resolved_atom_review", "pending_review", clean(row.get("risk_level")) or "low", "resolved atom is ready for local review"
    if status == "ready_with_touchdown_total_review":
        return "touchdown_total_type_review", "pending_review", "medium", "total touchdowns are preserved; type split still needs review"
    if status == "hold_needs_identity":
        return "identity_review", "needs_identity", "high", "required player identity is unresolved"
    if status == "identity_bridge_review_required":
        return "identity_bridge_review", "needs_review", "low", "identity bridge candidate needs acceptance"
    if status == "context_review_required":
        return "context_review", "needs_review", "medium", "context row needs structured-vs-context decision"
    if status == "hold_needs_game_mapping":
        return "manual_game_mapping_review", "needs_review", "medium", "game mapping triage missing"
    return "manual_review", "needs_review", "medium", status or "unknown materialization status"


def build_items(
    run_id: str,
    resolved_run_id: str,
    triage_run_id: str,
    resolved_rows: list[dict[str, Any]],
    triage_by_key: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    created_at = iso_now()
    items: list[dict[str, Any]] = []
    for row in sorted(resolved_rows, key=lambda r: (clean(r.get("_target_table")), clean(r.get("boxscore_id")), clean(r.get("target_entity_key")))):
        table = clean(row.get("_target_table"))
        target_key = clean(row.get("target_entity_key"))
        triage = triage_by_key.get(target_key, {}) if table == "game_candidate" else {}
        lane, review_status, risk, reason = package_lane(row, triage)
        action_status = clean(triage.get("action_status")) or clean(row.get("action_status"))
        fields = proposed_fields(row)
        items.append({
            "resolved_promotion_package_run_id": run_id,
            "resolved_atom_materialize_run_id": resolved_run_id,
            "resolved_game_mapping_triage_run_id": triage_run_id,
            "package_item_id": stable_id(run_id, table, target_key, lane),
            "package_lane": lane,
            "review_status": review_status,
            "risk_level": risk,
            "target_table": table,
            "target_entity_key": target_key,
            "boxscore_id": clean(row.get("boxscore_id")),
            "game_date": clean(row.get("game_date")),
            "player_week": clean(row.get("player_week")),
            "NFL_player_id": clean(row.get("NFL_player_id")),
            "nfl_team": clean(row.get("nfl_team")),
            "opponent_nfl_team": clean(row.get("opponent_nfl_team")),
            "materialization_status": clean(row.get("materialization_status")),
            "triage_status": clean(triage.get("triage_status")),
            "action_status": action_status,
            "value_class": clean(row.get("value_class")),
            "v26_status": clean(row.get("v26_status")),
            "identity_status": clean(row.get("identity_status")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "source_document_count": clean(row.get("source_document_count")) or clean(row.get("evidence_item_count")),
            "proposed_fields_json": compact_json(fields),
            "full_row_json": compact_json(row),
            "reason": reason,
            "source_documents_json": clean(row.get("source_documents_json")),
            "evidence_text": evidence_text(row),
            "created_at_utc": created_at,
        })
    return items


def build_rollups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str, str]] = Counter()
    for row in items:
        counts[(
            clean(row.get("package_lane")),
            clean(row.get("target_table")),
            clean(row.get("review_status")),
            clean(row.get("risk_level")),
        )] += 1
    output: list[dict[str, Any]] = []
    for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lane, table, review_status, risk = key
        output.append({
            "package_lane": lane,
            "target_table": table,
            "review_status": review_status,
            "risk_level": risk,
            "item_count": count,
        })
    return output


def build_decisions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for item in items:
        review_status = clean(item.get("review_status"))
        if review_status == "pending_review":
            decision_status = "pending_review"
            decision_value = "review_for_local_promotion"
        elif review_status == "archive":
            decision_status = "archive"
            decision_value = clean(item.get("package_lane"))
        else:
            decision_status = "hold"
            decision_value = review_status
        decisions.append({
            "decision_id": stable_id(item.get("package_item_id"), decision_status, decision_value),
            "decision_status": decision_status,
            "decision_value": decision_value,
            "route_to_lane": clean(item.get("package_lane")),
            "resolved_target_table": clean(item.get("target_table")),
            "resolved_target_entity_key": clean(item.get("target_entity_key")),
            "resolved_boxscore_id": clean(item.get("boxscore_id")),
            "proposed_fields_json": clean(item.get("proposed_fields_json")),
            "notes": clean(item.get("reason")),
        })
    return decisions


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    item_defs = ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_promotion_package_item ({item_defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='resolved_promotion_package_item'
            """
        ).fetchall()
    }
    for field in ITEM_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.resolved_promotion_package_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_promotion_package_run (
          resolved_promotion_package_run_id VARCHAR,
          resolved_atom_materialize_run_id VARCHAR,
          resolved_game_mapping_triage_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          package_item_count INTEGER,
          ready_review_count INTEGER,
          score_backfill_review_count INTEGER,
          archive_count INTEGER,
          conflict_count INTEGER,
          manual_followup_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[row.get(field) for field in fields] for row in rows],
    )


def persist(db_path: Path, run_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("resolved_promotion_package_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_promotion_package_run WHERE resolved_promotion_package_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_promotion_package_item WHERE resolved_promotion_package_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_promotion_package_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_promotion_package_item", item_rows, ITEM_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], rollups: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Resolved Promotion Packages",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Resolved atom run: `{summary['resolved_atom_materialize_run_id']}`",
        f"- Game triage run: `{summary['resolved_game_mapping_triage_run_id'] or 'none'}`",
        f"- Package items: `{summary['package_item_count']}`",
        f"- Lane counts: `{summary['package_lane_counts']}`",
        "",
        "## Rollup",
        "",
        "| Lane | Table | Review Status | Risk | Items |",
        "|---|---|---|---|---:|",
    ]
    for row in rollups:
        lines.append(
            f"| `{row['package_lane']}` | `{row['target_table']}` | `{row['review_status']}` | "
            f"`{row['risk_level']}` | {row['item_count']} |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Items CSV: `{summary['items_csv']}`",
        f"- Decision input: `{summary['decision_input_csv']}`",
        f"- Ready review CSV: `{summary['ready_review_csv']}`",
        f"- Archive CSV: `{summary['archive_csv']}`",
        f"- Conflict CSV: `{summary['conflict_csv']}`",
        f"- Manual follow-up CSV: `{summary['manual_followup_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--resolved-atom-materialize-run-id", default="")
    parser.add_argument("--resolved-game-mapping-triage-run-id", default="")
    parser.add_argument("--label", default="resolved_promotion_packages")
    parser.add_argument("--no-persist", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        resolved_run_id = args.resolved_atom_materialize_run_id or latest_run_id(
            con,
            "resolved_atom_materialize_run",
            "resolved_atom_materialize_run_id",
        )
        if not resolved_run_id:
            raise RuntimeError("No resolved atom materialization run found")
        triage_run_id = args.resolved_game_mapping_triage_run_id or latest_run_id(
            con,
            "resolved_game_mapping_triage_run",
            "resolved_game_mapping_triage_run_id",
        )
        resolved_rows = load_resolved_rows(con, resolved_run_id)
        triage_by_key = load_game_triage(con, triage_run_id)
    finally:
        con.close()

    item_rows = build_items(run_id, resolved_run_id, triage_run_id, resolved_rows, triage_by_key)
    rollups = build_rollups(item_rows)
    decision_rows = build_decisions(item_rows)

    lane_counts = Counter(row["package_lane"] for row in item_rows)
    status_counts = Counter(row["review_status"] for row in item_rows)
    target_counts = Counter(row["target_table"] for row in item_rows)

    ready_rows = [row for row in item_rows if row["review_status"] == "pending_review"]
    archive_rows = [row for row in item_rows if row["review_status"] == "archive"]
    conflict_rows = [row for row in item_rows if "conflict" in row["package_lane"]]
    manual_rows = [row for row in item_rows if row["review_status"] not in {"pending_review", "archive"}]
    score_backfill_rows = [row for row in item_rows if row["package_lane"] == "score_backfill_review"]

    items_csv = out_dir / "resolved_promotion_package_items.csv"
    rollup_csv = out_dir / "resolved_promotion_package_rollup.csv"
    decision_input_csv = out_dir / "decision_input_candidates.csv"
    ready_review_csv = out_dir / "ready_resolved_review_queue.csv"
    archive_csv = out_dir / "archive_queue.csv"
    conflict_csv = out_dir / "conflict_review_queue.csv"
    manual_csv = out_dir / "manual_followup_queue.csv"
    score_backfill_csv = out_dir / "score_backfill_review_queue.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "resolved_promotion_package_report.md"

    summary = {
        "created_at_utc": created_at,
        "resolved_promotion_package_run_id": run_id,
        "resolved_atom_materialize_run_id": resolved_run_id,
        "resolved_game_mapping_triage_run_id": triage_run_id,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(resolved_rows),
        "package_item_count": len(item_rows),
        "ready_review_count": len(ready_rows),
        "score_backfill_review_count": len(score_backfill_rows),
        "archive_count": len(archive_rows),
        "conflict_count": len(conflict_rows),
        "manual_followup_count": len(manual_rows),
        "package_lane_counts": dict(sorted(lane_counts.items())),
        "review_status_counts": dict(sorted(status_counts.items())),
        "target_table_counts": dict(sorted(target_counts.items())),
        "items_csv": str(items_csv),
        "rollup_csv": str(rollup_csv),
        "decision_input_csv": str(decision_input_csv),
        "ready_review_csv": str(ready_review_csv),
        "archive_csv": str(archive_csv),
        "conflict_csv": str(conflict_csv),
        "manual_followup_csv": str(manual_csv),
        "score_backfill_csv": str(score_backfill_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown),
        "persisted_to_duckdb": not args.no_persist,
    }

    write_csv(items_csv, item_rows, ITEM_FIELDS)
    write_csv(rollup_csv, rollups, ROLLUP_FIELDS)
    write_csv(decision_input_csv, decision_rows, DECISION_FIELDS)
    write_csv(ready_review_csv, ready_rows, ITEM_FIELDS)
    write_csv(archive_csv, archive_rows, ITEM_FIELDS)
    write_csv(conflict_csv, conflict_rows, ITEM_FIELDS)
    write_csv(manual_csv, manual_rows, ITEM_FIELDS)
    write_csv(score_backfill_csv, score_backfill_rows, ITEM_FIELDS)
    for lane in sorted(lane_counts):
        write_csv(out_dir / f"queue_{lane}.csv", [row for row in item_rows if row["package_lane"] == lane], ITEM_FIELDS)
    write_json(summary_json, summary)
    markdown.write_text(render_markdown(summary, rollups), encoding="utf-8")

    if not args.no_persist:
        persist(
            args.db_path,
            {
                "resolved_promotion_package_run_id": run_id,
                "resolved_atom_materialize_run_id": resolved_run_id,
                "resolved_game_mapping_triage_run_id": triage_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(resolved_rows),
                "package_item_count": len(item_rows),
                "ready_review_count": len(ready_rows),
                "score_backfill_review_count": len(score_backfill_rows),
                "archive_count": len(archive_rows),
                "conflict_count": len(conflict_rows),
                "manual_followup_count": len(manual_rows),
                "status": "complete",
                "created_at_utc": created_at,
                "summary_json_path": str(summary_json),
            },
            item_rows,
        )

    print(json.dumps({
        "resolved_promotion_package_run_id": run_id,
        "resolved_atom_materialize_run_id": resolved_run_id,
        "resolved_game_mapping_triage_run_id": triage_run_id,
        "output_dir": str(out_dir),
        "package_item_count": len(item_rows),
        "package_lane_counts": dict(sorted(lane_counts.items())),
        "review_status_counts": dict(sorted(status_counts.items())),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
