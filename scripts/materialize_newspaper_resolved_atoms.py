#!/usr/bin/env python
"""Materialize readiness promotion lanes into local resolved atom tables.

This is a local staging layer. It reads `newspaper_review.readiness_promotion_lane_item`
rows, parses their full atom payloads, and writes target-shaped tables under
`newspaper_resolved.*` plus D-drive CSV exports. It does not write to Fly, v26,
or production supertable tables.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_atom_materializations"

META_FIELDS = [
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
]

RUN_FIELDS = [
    "resolved_atom_materialize_run_id",
    "readiness_promotion_lane_run_id",
    "schema_gap_patch_run_id",
    "output_dir",
    "input_item_count",
    "materialized_row_count",
    "target_table_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]

ROLLUP_FIELDS = [
    "target_table",
    "promotion_lane",
    "action_status",
    "materialization_status",
    "risk_level",
    "row_count",
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
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return str(value)


def parse_json_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def safe_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value


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


def latest_lane_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT readiness_promotion_lane_run_id
        FROM newspaper_review.readiness_promotion_lane_run
        ORDER BY created_at_utc DESC, readiness_promotion_lane_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def latest_schema_gap_patch_run(con: duckdb.DuckDBPyConnection) -> str:
    try:
        row = con.execute(
            """
            SELECT resolved_schema_gap_patch_run_id
            FROM newspaper_review.resolved_schema_gap_patch_run
            ORDER BY created_at_utc DESC, resolved_schema_gap_patch_run_id DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        return ""
    return clean(row[0]) if row else ""


def load_lane_items(con: duckdb.DuckDBPyConnection, lane_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.readiness_promotion_lane_item
        WHERE readiness_promotion_lane_run_id = ?
        ORDER BY target_table, promotion_lane, boxscore_id, target_entity_key
        """,
        [lane_run_id],
    )


def load_schema_gap_patches(
    con: duckdb.DuckDBPyConnection,
    schema_gap_patch_run_id: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    if not schema_gap_patch_run_id:
        return {}
    try:
        rows = query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.resolved_schema_gap_patch_item
            WHERE resolved_schema_gap_patch_run_id = ?
            ORDER BY boxscore_id, player_raw, target_entity_key
            """,
            [schema_gap_patch_run_id],
        )
    except Exception:
        return {}
    return {
        (clean(row.get("target_table")), clean(row.get("target_entity_key"))): row
        for row in rows
        if clean(row.get("target_table")) and clean(row.get("target_entity_key"))
    }


def materialization_status(action_status: str, schema_resolution_status: str = "") -> str:
    if action_status == "ready_for_schema_review" and schema_resolution_status:
        if schema_resolution_status == "ready_existing_or_inferred_stat_fields":
            return "ready_for_resolved_review"
        if schema_resolution_status == "ready_with_touchdown_total_needs_type_split":
            return "ready_with_touchdown_total_review"
        if schema_resolution_status.startswith("hold_"):
            return schema_resolution_status
        return schema_resolution_status
    if action_status == "ready_for_apply_review":
        return "ready_for_resolved_review"
    if action_status == "ready_for_schema_review":
        return "schema_review_required"
    if action_status == "ready_for_identity_bridge_review":
        return "identity_bridge_review_required"
    if action_status == "ready_for_context_review":
        return "context_review_required"
    if action_status == "needs_game_mapping_review":
        return "hold_needs_game_mapping"
    if action_status == "source_row_not_found":
        return "hold_source_row_not_found"
    if action_status == "needs_identity_review":
        return "hold_needs_identity"
    return f"hold_{action_status}" if action_status else "hold_unknown"


def build_target_rows(
    run_id: str,
    items: list[dict[str, Any]],
    schema_patches: dict[tuple[str, str], dict[str, Any]],
    created_at: str,
) -> dict[str, list[dict[str, Any]]]:
    rows_by_table: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        target_table = clean(item.get("target_table"))
        atom = parse_json_obj(item.get("full_atom_json"))
        if not target_table or not atom:
            continue
        action_status = clean(item.get("action_status"))
        target_key = clean(item.get("target_entity_key"))
        schema_patch = schema_patches.get((target_table, target_key), {})
        schema_patch_fields = parse_json_obj(schema_patch.get("patch_fields_json"))
        atom.update({key: clean(value) for key, value in schema_patch_fields.items() if clean(value)})
        schema_resolution_status = clean(schema_patch.get("schema_resolution_status"))
        row = {
            "resolved_atom_materialize_run_id": run_id,
            "readiness_promotion_lane_run_id": clean(item.get("readiness_promotion_lane_run_id")),
            "readiness_run_id": clean(item.get("readiness_run_id")),
            "identity_resolution_run_id": clean(item.get("identity_resolution_run_id")),
            "source_readiness_row_id": clean(item.get("source_readiness_row_id")),
            "promotion_lane": clean(item.get("promotion_lane")),
            "action_status": action_status,
            "materialization_status": materialization_status(action_status, schema_resolution_status),
            "schema_gap_patch_run_id": clean(schema_patch.get("resolved_schema_gap_patch_run_id")),
            "schema_resolution_status": schema_resolution_status,
            "schema_patch_fields_json": clean(schema_patch.get("patch_fields_json")),
            "schema_patch_reason": clean(schema_patch.get("reason")),
            "risk_level": clean(item.get("risk_level")),
            "value_class": clean(item.get("value_class")),
            "v26_status": clean(item.get("v26_status")),
            "identity_status": clean(item.get("identity_status")),
            "lane_target_table": target_table,
            "created_at_utc": created_at,
        }
        row.update(atom)
        rows_by_table.setdefault(target_table, []).append(row)
    return rows_by_table


def fields_for_rows(rows: list[dict[str, Any]]) -> list[str]:
    atom_fields = sorted({
        field
        for row in rows
        for field in row
        if field not in META_FIELDS
    })
    return [*META_FIELDS, *atom_fields]


def build_rollups(rows_by_table: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str, str, str]] = Counter()
    for table, rows in rows_by_table.items():
        for row in rows:
            counts[(
                table,
                clean(row.get("promotion_lane")),
                clean(row.get("action_status")),
                clean(row.get("materialization_status")),
                clean(row.get("risk_level")),
            )] += 1
    output: list[dict[str, Any]] = []
    for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        table, lane, action_status, status, risk = key
        output.append({
            "target_table": table,
            "promotion_lane": lane,
            "action_status": action_status,
            "materialization_status": status,
            "risk_level": risk,
            "row_count": count,
        })
    return output


def ensure_tables(con: duckdb.DuckDBPyConnection, rows_by_table: dict[str, list[dict[str, Any]]]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_resolved")
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_atom_materialize_run (
          resolved_atom_materialize_run_id VARCHAR,
          readiness_promotion_lane_run_id VARCHAR,
          schema_gap_patch_run_id VARCHAR,
          output_dir VARCHAR,
          input_item_count INTEGER,
          materialized_row_count INTEGER,
          target_table_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )
    existing_run_cols = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='resolved_atom_materialize_run'
            """
        ).fetchall()
    }
    for field in RUN_FIELDS:
        if field not in existing_run_cols:
            field_type = "INTEGER" if field in {"input_item_count", "materialized_row_count", "target_table_count"} else "VARCHAR"
            con.execute(f"ALTER TABLE newspaper_review.resolved_atom_materialize_run ADD COLUMN IF NOT EXISTS {safe_identifier(field)} {field_type}")
    for table, rows in rows_by_table.items():
        safe_table = safe_identifier(table)
        fields = fields_for_rows(rows)
        defs = ", ".join(f"{safe_identifier(field)} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_resolved.{safe_table} ({defs})")
        existing = {
            row[0]
            for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_resolved'
                  AND table_name=?
                """,
                [table],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_resolved.{safe_table} ADD COLUMN IF NOT EXISTS {safe_identifier(field)} VARCHAR")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    field_sql = ", ".join(safe_identifier(field) for field in fields)
    con.executemany(
        f"INSERT INTO {table} ({field_sql}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    run_row: dict[str, Any],
    rows_by_table: dict[str, list[dict[str, Any]]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con, rows_by_table)
        run_id = clean(run_row.get("resolved_atom_materialize_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_atom_materialize_run WHERE resolved_atom_materialize_run_id = ?",
            [run_id],
        )
        for table in rows_by_table:
            safe_table = safe_identifier(table)
            con.execute(
                f"DELETE FROM newspaper_resolved.{safe_table} WHERE resolved_atom_materialize_run_id = ?",
                [run_id],
            )
        insert_rows(con, "newspaper_review.resolved_atom_materialize_run", [run_row], RUN_FIELDS)
        for table, rows in rows_by_table.items():
            fields = fields_for_rows(rows)
            insert_rows(con, f"newspaper_resolved.{safe_identifier(table)}", rows, fields)
    finally:
        con.close()


def latest_lane_run_dir(root: Path) -> Path:
    paths = sorted(
        (Path(path).parent for path in glob.glob(str(root / "readiness_promotion_lanes" / "*" / "summary.json"))),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not paths:
        raise FileNotFoundError("No readiness promotion lane summaries found")
    return paths[0]


def render_markdown(summary: dict[str, Any], rollups: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Resolved Atom Materialization",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Lane run: `{summary['readiness_promotion_lane_run_id']}`",
        f"- Schema patch run: `{summary.get('schema_gap_patch_run_id', '') or 'none'}`",
        f"- Schema patch items: `{summary.get('schema_gap_patch_item_count', 0)}`",
        f"- Input lane items: `{summary['input_item_count']}`",
        f"- Materialized rows: `{summary['materialized_row_count']}`",
        f"- Target tables: `{summary['target_table_count']}`",
        "",
        "## Status Counts",
        "",
    ]
    for status, count in summary["materialization_status_counts"].items():
        lines.append(f"- `{status}`: {count}")
    lines.extend([
        "",
        "## Rollup",
        "",
        "| Table | Lane | Action | Status | Risk | Rows |",
        "|---|---|---|---|---|---:|",
    ])
    for row in rollups:
        lines.append(
            f"| `{row['target_table']}` | `{row['promotion_lane']}` | `{row['action_status']}` | "
            f"`{row['materialization_status']}` | `{row['risk_level']}` | {row['row_count']} |"
        )
    lines.extend([
        "",
        "## Exports",
        "",
    ])
    for table, path in summary["target_exports_csv"].items():
        lines.append(f"- `{table}`: `{path}`")
    lines.extend([
        "",
        f"- Summary JSON: `{summary['summary_json']}`",
        f"- Rollup CSV: `{summary['rollup_csv']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--readiness-promotion-lane-run-id", default="")
    parser.add_argument("--schema-gap-patch-run-id", default="")
    parser.add_argument("--label", default="resolved_atom_materialization")
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
        lane_run_id = args.readiness_promotion_lane_run_id or latest_lane_run(con)
        if not lane_run_id:
            raise RuntimeError("No readiness promotion lane run found")
        schema_gap_patch_run_id = args.schema_gap_patch_run_id or latest_schema_gap_patch_run(con)
        lane_items = load_lane_items(con, lane_run_id)
        schema_patches = load_schema_gap_patches(con, schema_gap_patch_run_id)
    finally:
        con.close()

    rows_by_table = build_target_rows(run_id, lane_items, schema_patches, created_at)
    rollups = build_rollups(rows_by_table)

    exports_dir = out_dir / "tables"
    export_paths: dict[str, str] = {}
    for table, rows in sorted(rows_by_table.items()):
        fields = fields_for_rows(rows)
        path = exports_dir / f"{table}.csv"
        write_csv(path, rows, fields)
        export_paths[table] = str(path)

    status_counts = Counter(
        clean(row.get("materialization_status"))
        for rows in rows_by_table.values()
        for row in rows
    )
    target_counts = {table: len(rows) for table, rows in sorted(rows_by_table.items())}

    rollup_csv = out_dir / "resolved_atom_rollup.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "resolved_atom_materialization_report.md"
    summary = {
        "created_at_utc": created_at,
        "resolved_atom_materialize_run_id": run_id,
        "readiness_promotion_lane_run_id": lane_run_id,
        "schema_gap_patch_run_id": schema_gap_patch_run_id,
        "schema_gap_patch_item_count": len(schema_patches),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_item_count": len(lane_items),
        "materialized_row_count": sum(len(rows) for rows in rows_by_table.values()),
        "target_table_count": len(rows_by_table),
        "target_table_counts": target_counts,
        "materialization_status_counts": dict(sorted(status_counts.items())),
        "target_exports_csv": export_paths,
        "rollup_csv": str(rollup_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown),
        "persisted_to_duckdb": not args.no_persist,
    }
    write_csv(rollup_csv, rollups, ROLLUP_FIELDS)
    write_json(summary_json, summary)
    markdown.write_text(render_markdown(summary, rollups), encoding="utf-8")

    if not args.no_persist:
        persist(
            args.db_path,
            {
                "resolved_atom_materialize_run_id": run_id,
                "readiness_promotion_lane_run_id": lane_run_id,
                "schema_gap_patch_run_id": schema_gap_patch_run_id,
                "output_dir": str(out_dir),
                "input_item_count": len(lane_items),
                "materialized_row_count": sum(len(rows) for rows in rows_by_table.values()),
                "target_table_count": len(rows_by_table),
                "status": "complete",
                "created_at_utc": created_at,
                "summary_json_path": str(summary_json),
            },
            rows_by_table,
        )

    print(json.dumps({
        "resolved_atom_materialize_run_id": run_id,
        "readiness_promotion_lane_run_id": lane_run_id,
        "schema_gap_patch_run_id": schema_gap_patch_run_id,
        "output_dir": str(out_dir),
        "materialized_row_count": summary["materialized_row_count"],
        "target_table_counts": target_counts,
        "materialization_status_counts": dict(sorted(status_counts.items())),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
