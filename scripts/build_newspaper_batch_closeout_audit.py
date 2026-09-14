#!/usr/bin/env python
"""Close out a local newspaper promotion batch.

This station is deliberately local-only. It reads the D-drive newspaper atom
DuckDB, writes D-drive audit artifacts, and optionally records the audit in the
local `newspaper_review` schema. It does not write to Fly, v26, or production
supertable tables.

The closeout is the conveyor belt's "can we move on?" receipt:

- promoted row counts by target table
- remaining route queue rows for the apply run
- source-document notes that explain held/ambiguous evidence
- latest v26 quality-overlap summary, when available
- readiness gates and blockers
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "batch_closeout_audits"

PROMOTED_TABLES = [
    "game_candidate",
    "lineup_participation",
    "play_by_play_event",
    "player_game_box_score",
    "player_game_stat_claim",
    "player_game_note",
    "team_game_stat_claim",
    "source_document_note",
    "player_identity_candidate",
    "scoring_event",
]

PROMOTED_SAMPLE_COLUMNS = [
    "boxscore_id",
    "target_entity_key",
    "player_raw",
    "player_week",
    "raw_player_name",
    "resolved_player",
    "nfl_team",
    "stat_name",
    "stat_value",
    "event_type",
    "play_type",
    "source_row_text",
    "play_text",
    "evidence_text",
    "note_text",
    "confidence_bar",
    "max_confidence_score",
    "source_documents_json",
]

RUN_FIELDS = [
    "batch_closeout_audit_run_id",
    "promotion_apply_run_id",
    "apply_run_kind",
    "output_dir",
    "closeout_status",
    "blocker_count",
    "warning_count",
    "promoted_row_count",
    "route_queue_count",
    "unresolved_note_count",
    "quality_report_dir",
    "summary_json_path",
    "created_at_utc",
]

ITEM_FIELDS = [
    "batch_closeout_audit_run_id",
    "promotion_apply_run_id",
    "item_type",
    "item_key",
    "severity",
    "status",
    "detail_json",
    "created_at_utc",
]

TERMINAL_NOTE_STATUSES = {
    "accepted",
    "accepted_as_note",
    "complete",
    "contamination_guard",
    "corroborated",
    "corroborates_existing_source",
    "documented",
    "ocr_completed_claims_materialized",
    "resolved",
    "semantic_followup_note",
    "sidecar_done",
    "source_corroboration",
    "verified",
}

UNRESOLVED_STATUS_TOKENS = {
    "ambiguous",
    "conflict",
    "held",
    "needs",
    "pending",
    "review",
    "unresolved",
}

TERMINAL_ROUTE_VALUES = {
    "closed",
    "closed_not_promoted",
    "closed_rejected",
    "do_not_promote",
    "not_useful",
    "reject",
    "rejected",
    "terminal",
    "terminal_closed",
    "terminal_reject",
}

APPLY_KIND_CONFIG = {
    "review": {
        "schema": "newspaper_promoted",
        "apply_id_column": "promotion_apply_run_id",
        "apply_table": "review_decision_apply_run",
    },
    "resolved_package": {
        "schema": "newspaper_final",
        "apply_id_column": "resolved_package_apply_run_id",
        "apply_table": "resolved_package_decision_apply_run",
    },
}


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: safe_cell(row.get(field)) for field in fields})


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


def table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> set[str]:
    rows = query_dicts(
        con,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    )
    return {str(row["column_name"]) for row in rows}


def latest_apply_run(con: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    candidates: list[dict[str, str]] = []
    if table_exists(con, "newspaper_review", "review_decision_apply_run"):
        candidates.extend(
            {
                "apply_run_id": safe_cell(row.get("promotion_apply_run_id")),
                "apply_run_kind": "review",
                "created_at_utc": safe_cell(row.get("created_at_utc")),
            }
            for row in query_dicts(
                con,
                """
                SELECT promotion_apply_run_id, created_at_utc
                FROM newspaper_review.review_decision_apply_run
                """,
            )
        )
    if table_exists(con, "newspaper_review", "resolved_package_decision_apply_run"):
        candidates.extend(
            {
                "apply_run_id": safe_cell(row.get("resolved_package_apply_run_id")),
                "apply_run_kind": "resolved_package",
                "created_at_utc": safe_cell(row.get("created_at_utc")),
            }
            for row in query_dicts(
                con,
                """
                SELECT resolved_package_apply_run_id, created_at_utc
                FROM newspaper_review.resolved_package_decision_apply_run
                """,
            )
        )
    candidates = [row for row in candidates if row["apply_run_id"]]
    if not candidates:
        return "", ""
    candidates.sort(key=lambda row: (row["created_at_utc"], row["apply_run_id"]))
    latest = candidates[-1]
    return latest["apply_run_id"], latest["apply_run_kind"]


def detect_apply_run_kind(con: duckdb.DuckDBPyConnection, apply_run_id: str) -> str:
    if table_exists(con, "newspaper_review", "resolved_package_decision_apply_run"):
        found = query_dicts(
            con,
            """
            SELECT 1 AS found
            FROM newspaper_review.resolved_package_decision_apply_run
            WHERE resolved_package_apply_run_id = ?
            LIMIT 1
            """,
            [apply_run_id],
        )
        if found:
            return "resolved_package"
    if table_exists(con, "newspaper_review", "review_decision_apply_run"):
        found = query_dicts(
            con,
            """
            SELECT 1 AS found
            FROM newspaper_review.review_decision_apply_run
            WHERE promotion_apply_run_id = ?
            LIMIT 1
            """,
            [apply_run_id],
        )
        if found:
            return "review"
    return ""


def load_apply_run(
    con: duckdb.DuckDBPyConnection,
    apply_run_id: str,
    apply_run_kind: str,
) -> dict[str, Any]:
    if apply_run_kind == "review":
        if not table_exists(con, "newspaper_review", "review_decision_apply_run"):
            return {}
        rows = query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.review_decision_apply_run
            WHERE promotion_apply_run_id = ?
            """,
            [apply_run_id],
        )
        return rows[0] if rows else {}

    if apply_run_kind == "resolved_package":
        if not table_exists(con, "newspaper_review", "resolved_package_decision_apply_run"):
            return {}
        rows = query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.resolved_package_decision_apply_run
            WHERE resolved_package_apply_run_id = ?
            """,
            [apply_run_id],
        )
        if not rows:
            return {}
        row = rows[0]
        decision_run_id = safe_cell(row.get("resolved_package_decision_run_id"))
        ledger_rows = []
        if decision_run_id and table_exists(con, "newspaper_review", "resolved_package_decision_ledger_run"):
            ledger_rows = query_dicts(
                con,
                """
                SELECT route_queue_count
                FROM newspaper_review.resolved_package_decision_ledger_run
                WHERE resolved_package_decision_run_id = ?
                """,
                [decision_run_id],
            )
        route_count = int(ledger_rows[0].get("route_queue_count") or 0) if ledger_rows else 0
        return {
            **row,
            "promotion_apply_run_id": safe_cell(row.get("resolved_package_apply_run_id")),
            "decision_ledger_run_id": decision_run_id,
            "promoted_row_count": int(row.get("applied_row_count") or 0),
            "route_queue_count": route_count,
            "status": "complete" if safe_cell(row.get("persisted_to_duckdb")).lower() == "true" else "incomplete",
            "summary_json_path": safe_cell(row.get("summary_json")),
        }
    return {}


def load_route_rows(
    con: duckdb.DuckDBPyConnection,
    apply_run_id: str,
    apply_run_kind: str,
    apply_run: dict[str, Any],
) -> list[dict[str, Any]]:
    if apply_run_kind == "review":
        if not table_exists(con, "newspaper_review", "review_decision_route_queue"):
            return []
        return query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.review_decision_route_queue
            WHERE promotion_apply_run_id = ?
            ORDER BY route_to_lane, target_table, boxscore_id, decision_id
            """,
            [apply_run_id],
        )

    if apply_run_kind == "resolved_package":
        decision_run_id = safe_cell(apply_run.get("resolved_package_decision_run_id")) or safe_cell(
            apply_run.get("decision_ledger_run_id")
        )
        if not decision_run_id or not table_exists(con, "newspaper_review", "resolved_package_decision_route_queue"):
            return []
        return query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.resolved_package_decision_route_queue
            WHERE resolved_package_decision_run_id = ?
            ORDER BY route_to_lane, target_table, boxscore_id, package_item_id
            """,
            [decision_run_id],
        )
    return []


def is_terminal_route_row(row: dict[str, Any]) -> bool:
    values = {
        safe_cell(row.get("decision_status")).strip().lower(),
        safe_cell(row.get("decision_value")).strip().lower(),
        safe_cell(row.get("route_to_lane")).strip().lower(),
    }
    return bool(values & TERMINAL_ROUTE_VALUES)


def promoted_table_summary(
    con: duckdb.DuckDBPyConnection,
    table: str,
    apply_run_id: str,
    apply_run_kind: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = APPLY_KIND_CONFIG[apply_run_kind]
    schema = config["schema"]
    apply_id_column = config["apply_id_column"]
    if not table_exists(con, schema, table):
        return {
            "target_table": table,
            "promoted_rows": 0,
            "distinct_boxscore_ids": 0,
            "high_confidence_rows": 0,
            "medium_confidence_rows": 0,
            "low_confidence_rows": 0,
            "unknown_confidence_rows": 0,
            "missing_table": 1,
        }, []

    cols = table_columns(con, schema, table)
    if apply_id_column not in cols:
        return {
            "target_table": table,
            "promoted_rows": 0,
            "distinct_boxscore_ids": 0,
            "high_confidence_rows": 0,
            "medium_confidence_rows": 0,
            "low_confidence_rows": 0,
            "unknown_confidence_rows": 0,
            "missing_table": 0,
        }, []

    box_expr = "COUNT(DISTINCT boxscore_id)" if "boxscore_id" in cols else "0"
    totals = query_dicts(
        con,
        f"""
        SELECT COUNT(1) AS promoted_rows, {box_expr} AS distinct_boxscore_ids
        FROM {schema}.{table}
        WHERE {apply_id_column} = ?
        """,
        [apply_run_id],
    )[0]

    confidence_counts = Counter()
    if "confidence_bar" in cols:
        for row in query_dicts(
            con,
            f"""
            SELECT COALESCE(NULLIF(TRIM(confidence_bar), ''), 'unknown') AS confidence_bar,
                   COUNT(1) AS row_count
            FROM {schema}.{table}
            WHERE {apply_id_column} = ?
            GROUP BY 1
            """,
            [apply_run_id],
        ):
            confidence_counts[str(row["confidence_bar"]).lower()] += int(row["row_count"] or 0)
    else:
        confidence_counts["unknown"] = int(totals["promoted_rows"] or 0)

    sample_cols = [col for col in PROMOTED_SAMPLE_COLUMNS if col in cols]
    sample_rows: list[dict[str, Any]] = []
    if sample_cols and int(totals["promoted_rows"] or 0) > 0:
        selected = ", ".join(sample_cols)
        for row in query_dicts(
            con,
            f"""
            SELECT {selected}
            FROM {schema}.{table}
            WHERE {apply_id_column} = ?
            ORDER BY boxscore_id NULLS LAST, target_entity_key NULLS LAST
            LIMIT 5
            """,
            [apply_run_id],
        ):
            sample_rows.append({"target_table": table, **row})

    summary = {
        "target_table": table,
        "promoted_rows": int(totals["promoted_rows"] or 0),
        "distinct_boxscore_ids": int(totals["distinct_boxscore_ids"] or 0),
        "high_confidence_rows": int(confidence_counts["high"]),
        "medium_confidence_rows": int(confidence_counts["medium"]),
        "low_confidence_rows": int(confidence_counts["low"]),
        "unknown_confidence_rows": int(confidence_counts["unknown"]),
        "missing_table": 0,
    }
    return summary, sample_rows


def load_source_note_rows(
    con: duckdb.DuckDBPyConnection,
    apply_run_id: str,
    apply_run_kind: str,
) -> list[dict[str, Any]]:
    config = APPLY_KIND_CONFIG[apply_run_kind]
    schema = config["schema"]
    apply_id_column = config["apply_id_column"]
    if not table_exists(con, schema, "source_document_note"):
        return []
    cols = table_columns(con, schema, "source_document_note")
    select_cols = [
        col for col in [
            "source_document_note_id",
            "source_document_id",
            "boxscore_id",
            "note_type",
            "note_category",
            "note_text",
            "related_target_table",
            "related_entity_key",
            "reconciliation_status",
            "evidence_text",
            "confidence_bar",
            "artifact_path",
            "source_documents_json",
        ] if col in cols
    ]
    if not select_cols:
        return []
    return query_dicts(
        con,
        f"""
        SELECT {", ".join(select_cols)}
        FROM {schema}.source_document_note
        WHERE {apply_id_column} = ?
        ORDER BY note_type, note_category, reconciliation_status, boxscore_id, source_document_id
        """,
        [apply_run_id],
    )


def summarize_source_notes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str, str]] = Counter()
    for row in rows:
        key = (
            safe_cell(row.get("note_type")),
            safe_cell(row.get("note_category")),
            safe_cell(row.get("reconciliation_status")),
            safe_cell(row.get("related_target_table")),
        )
        counts[key] += 1
    return [
        {
            "note_type": note_type,
            "note_category": note_category,
            "reconciliation_status": reconciliation_status,
            "related_target_table": related_target_table,
            "row_count": count,
        }
        for (note_type, note_category, reconciliation_status, related_target_table), count
        in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def is_unresolved_note(row: dict[str, Any]) -> bool:
    status = safe_cell(row.get("reconciliation_status")).strip().lower()
    note_type = safe_cell(row.get("note_type")).strip().lower()
    note_category = safe_cell(row.get("note_category")).strip().lower()
    haystack = " ".join([status, note_type, note_category])
    if status and status not in TERMINAL_NOTE_STATUSES:
        return True
    return any(token in haystack for token in UNRESOLVED_STATUS_TOKENS)


def find_quality_report(root: Path, apply_run_id: str) -> tuple[Path | None, dict[str, Any]]:
    report_root = root / "promotion_quality_reports"
    if not report_root.exists():
        return None, {}
    matches: list[tuple[float, Path, dict[str, Any]]] = []
    for path in report_root.iterdir():
        if not path.is_dir():
            continue
        summary_path = path / "summary.json"
        if not summary_path.exists():
            continue
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if summary.get("promotion_apply_run_id") == apply_run_id:
            matches.append((path.stat().st_mtime, path, summary))
    if not matches:
        return None, {}
    _, path, summary = sorted(matches)[-1]
    return path, summary


def quality_overlap_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scope in ["player_stat_v26_overlap", "lineup_v26_overlap"]:
        block = summary.get(scope) or {}
        for status, count in (block.get("status_counts") or {}).items():
            rows.append({
                "scope": scope,
                "status": status,
                "row_count": count,
            })
    return rows


def detail_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def build_markdown(summary: dict[str, Any], output_files: dict[str, str]) -> str:
    gates = summary["gates"]
    promoted_counts = summary["promoted_counts"]
    source_note_summary = summary["source_note_summary"]
    quality_rows = summary["quality_overlap_rows"]

    lines = [
        "# Newspaper Batch Closeout Audit",
        "",
        f"- Created: {summary['created_at_utc']}",
        f"- Apply run: `{summary['promotion_apply_run_id']}`",
        f"- Apply run kind: `{summary.get('apply_run_kind', 'review')}`",
        f"- Newspaper DB: `{summary['db_path']}`",
        f"- Closeout status: `{summary['closeout_status']}`",
        f"- Route queue rows remaining: `{summary['route_queue_count']}`",
        f"- Terminal route receipts: `{summary.get('terminal_route_queue_count', 0)}`",
        f"- Promoted rows: `{summary['promoted_row_count']}`",
        f"- Output directory: `{summary['output_dir']}`",
        "",
        "## Gates",
        "",
        "| Gate | Status | Detail |",
        "|---|---|---|",
    ]
    for gate in gates:
        lines.append(f"| {gate['gate']} | {gate['status']} | {gate['detail']} |")

    lines.extend([
        "",
        "## Promoted Rows",
        "",
        "| Target table | Rows | Games | Confidence |",
        "|---|---:|---:|---|",
    ])
    for row in promoted_counts:
        confidence = (
            f"high {row['high_confidence_rows']}, "
            f"medium {row['medium_confidence_rows']}, "
            f"low {row['low_confidence_rows']}, "
            f"unknown {row['unknown_confidence_rows']}"
        )
        lines.append(
            f"| `{row['target_table']}` | {row['promoted_rows']} | "
            f"{row['distinct_boxscore_ids']} | {confidence} |"
        )

    lines.extend([
        "",
        "## v26 Overlap Signals",
        "",
    ])
    if quality_rows:
        lines.extend([
            "| Scope | Status | Rows |",
            "|---|---|---:|",
        ])
        for row in quality_rows:
            lines.append(f"| `{row['scope']}` | `{row['status']}` | {row['row_count']} |")
    else:
        lines.append("- No matching promotion quality report found for this apply run.")

    lines.extend([
        "",
        "## Source Notes",
        "",
        "| Note type | Category | Reconciliation | Target | Rows |",
        "|---|---|---|---|---:|",
    ])
    for row in source_note_summary:
        lines.append(
            f"| `{row['note_type']}` | `{row['note_category']}` | "
            f"`{row['reconciliation_status']}` | `{row['related_target_table']}` | "
            f"{row['row_count']} |"
        )

    lines.extend([
        "",
        "## Files",
        "",
    ])
    for label, path in output_files.items():
        lines.append(f"- `{label}`: `{path}`")

    if summary["blockers"]:
        lines.extend(["", "## Blockers", ""])
        for blocker in summary["blockers"]:
            lines.append(f"- {blocker['detail']}")

    if summary["warnings"]:
        lines.extend(["", "## Warnings", ""])
        for warning in summary["warnings"]:
            lines.append(f"- {warning['detail']}")

    return "\n".join(lines) + "\n"


def ensure_persist_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.batch_closeout_audit_run (
          batch_closeout_audit_run_id VARCHAR,
          promotion_apply_run_id VARCHAR,
          apply_run_kind VARCHAR,
          output_dir VARCHAR,
          closeout_status VARCHAR,
          blocker_count INTEGER,
          warning_count INTEGER,
          promoted_row_count INTEGER,
          route_queue_count INTEGER,
          unresolved_note_count INTEGER,
          quality_report_dir VARCHAR,
          summary_json_path VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.batch_closeout_audit_item (
          batch_closeout_audit_run_id VARCHAR,
          promotion_apply_run_id VARCHAR,
          item_type VARCHAR,
          item_key VARCHAR,
          severity VARCHAR,
          status VARCHAR,
          detail_json VARCHAR,
          created_at_utc VARCHAR
        )
        """
    )
    run_cols = table_columns(con, "newspaper_review", "batch_closeout_audit_run")
    if "apply_run_kind" not in run_cols:
        con.execute("ALTER TABLE newspaper_review.batch_closeout_audit_run ADD COLUMN apply_run_kind VARCHAR")


def insert_rows(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: list[dict[str, Any]],
    fields: list[str],
) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(fields))
    columns = ", ".join(fields)
    values = [[row.get(field) for field in fields] for row in rows]
    con.executemany(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", values)


def persist_audit(
    db_path: Path,
    run_row: dict[str, Any],
    item_rows: list[dict[str, Any]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_persist_tables(con)
        insert_rows(con, "newspaper_review.batch_closeout_audit_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.batch_closeout_audit_item", item_rows, ITEM_FIELDS)
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--apply-run-kind", choices=["auto", "review", "resolved_package"], default="auto")
    parser.add_argument("--label", default="newspaper_batch_closeout")
    parser.add_argument("--no-persist", action="store_true")
    args = parser.parse_args()

    created_at = iso_now()
    closeout_run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / closeout_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.db_path.exists():
        raise SystemExit(f"Newspaper DB not found: {args.db_path}")

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        if args.promotion_apply_run_id:
            apply_run_id = args.promotion_apply_run_id
            apply_run_kind = (
                detect_apply_run_kind(con, apply_run_id)
                if args.apply_run_kind == "auto"
                else args.apply_run_kind
            )
        else:
            apply_run_id, apply_run_kind = latest_apply_run(con)
        if not apply_run_id:
            raise SystemExit("No newspaper decision apply run found.")
        if not apply_run_kind:
            raise SystemExit(f"Could not detect apply run kind for {apply_run_id}.")

        apply_run = load_apply_run(con, apply_run_id, apply_run_kind)
        route_rows = load_route_rows(con, apply_run_id, apply_run_kind, apply_run)

        promoted_counts: list[dict[str, Any]] = []
        promoted_samples: list[dict[str, Any]] = []
        for table in PROMOTED_TABLES:
            count_row, sample_rows = promoted_table_summary(con, table, apply_run_id, apply_run_kind)
            promoted_counts.append(count_row)
            promoted_samples.extend(sample_rows)

        source_note_rows = load_source_note_rows(con, apply_run_id, apply_run_kind)
        source_note_summary = summarize_source_notes(source_note_rows)
        unresolved_source_notes = [row for row in source_note_rows if is_unresolved_note(row)]
        quality_dir, quality_summary = find_quality_report(args.root, apply_run_id)
        quality_rows = quality_overlap_rows(quality_summary)
    finally:
        con.close()

    promoted_row_count = sum(int(row["promoted_rows"]) for row in promoted_counts)
    active_route_rows = [row for row in route_rows if not is_terminal_route_row(row)]
    terminal_route_rows = [row for row in route_rows if is_terminal_route_row(row)]
    raw_route_queue_count = len(route_rows)
    route_queue_count = len(active_route_rows)
    terminal_route_queue_count = len(terminal_route_rows)
    recorded_route_count = int(apply_run.get("route_queue_count") or 0) if apply_run else 0
    recorded_promoted_count = int(apply_run.get("promoted_row_count") or 0) if apply_run else 0

    gates: list[dict[str, str]] = []
    blockers: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []

    def add_gate(gate: str, status: str, detail: str, severity: str = "info") -> None:
        row = {"gate": gate, "status": status, "detail": detail, "severity": severity}
        gates.append(row)
        if status == "fail":
            blockers.append(row)
        elif status == "warn":
            warnings.append(row)

    add_gate(
        "apply_run_exists",
        "pass" if apply_run else "fail",
        f"{apply_run_kind} apply run row found" if apply_run else "apply run row missing",
    )
    add_gate(
        "apply_run_status",
        "pass" if str(apply_run.get("status", "")).lower() == "complete" else "warn",
        f"status={apply_run.get('status', '')}",
    )
    add_gate(
        "route_queue_drained",
        "pass" if route_queue_count == 0 else "fail",
        (
            f"active_route_queue_rows={route_queue_count}; "
            f"terminal_route_rows={terminal_route_queue_count}; "
            f"raw_route_queue_rows={raw_route_queue_count}"
        ),
    )
    add_gate(
        "recorded_route_count_zero",
        "pass" if route_queue_count == 0 else "fail",
        (
            f"recorded_route_queue_count={recorded_route_count}; "
            f"active_route_queue_rows={route_queue_count}; "
            f"terminal_route_rows={terminal_route_queue_count}"
        ),
    )
    add_gate(
        "promoted_rows_present",
        "pass" if promoted_row_count > 0 else "warn",
        f"promoted_rows={promoted_row_count}",
    )
    add_gate(
        "recorded_promoted_count_matches",
        "pass" if recorded_promoted_count == promoted_row_count else "warn",
        f"recorded={recorded_promoted_count}; table_count={promoted_row_count}",
    )
    add_gate(
        "quality_report_found",
        "pass" if quality_dir else "warn",
        str(quality_dir) if quality_dir else "no matching promotion quality report found",
    )

    closeout_status = "ready_to_advance" if not blockers else "needs_followup"

    output_files = {
        "promoted_counts": str(out_dir / "promoted_counts.csv"),
        "remaining_route_queue": str(out_dir / "remaining_route_queue.csv"),
        "terminal_route_receipts": str(out_dir / "terminal_route_receipts.csv"),
        "source_note_summary": str(out_dir / "source_note_summary.csv"),
        "unresolved_source_notes": str(out_dir / "unresolved_source_notes.csv"),
        "quality_overlap_summary": str(out_dir / "quality_overlap_summary.csv"),
        "promoted_evidence_samples": str(out_dir / "promoted_evidence_samples.csv"),
        "summary": str(out_dir / "summary.json"),
        "markdown": str(out_dir / "batch_closeout_audit.md"),
    }

    write_csv(
        out_dir / "promoted_counts.csv",
        promoted_counts,
        [
            "target_table",
            "promoted_rows",
            "distinct_boxscore_ids",
            "high_confidence_rows",
            "medium_confidence_rows",
            "low_confidence_rows",
            "unknown_confidence_rows",
            "missing_table",
        ],
    )
    route_fields = list(route_rows[0].keys()) if route_rows else [
        "promotion_apply_run_id",
        "resolved_package_decision_run_id",
        "decision_id",
        "package_item_id",
        "route_to_lane",
        "target_table",
        "target_entity_key",
        "boxscore_id",
        "reason",
        "recommended_next_action",
    ]
    write_csv(out_dir / "remaining_route_queue.csv", active_route_rows, route_fields)
    write_csv(out_dir / "terminal_route_receipts.csv", terminal_route_rows, route_fields)
    write_csv(
        out_dir / "source_note_summary.csv",
        source_note_summary,
        [
            "note_type",
            "note_category",
            "reconciliation_status",
            "related_target_table",
            "row_count",
        ],
    )
    note_fields = list(source_note_rows[0].keys()) if source_note_rows else [
        "source_document_note_id",
        "source_document_id",
        "boxscore_id",
        "note_type",
        "note_category",
        "note_text",
        "related_target_table",
        "related_entity_key",
        "reconciliation_status",
        "evidence_text",
        "confidence_bar",
        "artifact_path",
        "source_documents_json",
    ]
    write_csv(out_dir / "unresolved_source_notes.csv", unresolved_source_notes, note_fields)
    write_csv(out_dir / "quality_overlap_summary.csv", quality_rows, ["scope", "status", "row_count"])
    sample_fields = ["target_table"] + PROMOTED_SAMPLE_COLUMNS
    write_csv(out_dir / "promoted_evidence_samples.csv", promoted_samples, sample_fields)

    summary = {
        "batch_closeout_audit_run_id": closeout_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "promotion_apply_run_id": apply_run_id,
        "apply_run_kind": apply_run_kind,
        "closeout_status": closeout_status,
        "apply_run": apply_run,
        "promoted_row_count": promoted_row_count,
        "recorded_promoted_row_count": recorded_promoted_count,
        "route_queue_count": route_queue_count,
        "raw_route_queue_count": raw_route_queue_count,
        "terminal_route_queue_count": terminal_route_queue_count,
        "recorded_route_queue_count": recorded_route_count,
        "unresolved_note_count": len(unresolved_source_notes),
        "quality_report_dir": str(quality_dir) if quality_dir else "",
        "gates": gates,
        "blockers": blockers,
        "warnings": warnings,
        "promoted_counts": promoted_counts,
        "source_note_summary": source_note_summary,
        "quality_overlap_rows": quality_rows,
        "output_files": output_files,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=True, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (out_dir / "batch_closeout_audit.md").write_text(
        build_markdown(summary, output_files),
        encoding="utf-8",
    )

    if not args.no_persist:
        item_rows: list[dict[str, Any]] = []
        for gate in gates:
            item_rows.append({
                "batch_closeout_audit_run_id": closeout_run_id,
                "promotion_apply_run_id": apply_run_id,
                "item_type": "gate",
                "item_key": gate["gate"],
                "severity": gate["severity"],
                "status": gate["status"],
                "detail_json": detail_json(gate),
                "created_at_utc": created_at,
            })
        for row in promoted_counts:
            item_rows.append({
                "batch_closeout_audit_run_id": closeout_run_id,
                "promotion_apply_run_id": apply_run_id,
                "item_type": "promoted_count",
                "item_key": row["target_table"],
                "severity": "info",
                "status": "observed",
                "detail_json": detail_json(row),
                "created_at_utc": created_at,
            })
        persist_audit(
            args.db_path,
            {
                "batch_closeout_audit_run_id": closeout_run_id,
                "promotion_apply_run_id": apply_run_id,
                "apply_run_kind": apply_run_kind,
                "output_dir": str(out_dir),
                "closeout_status": closeout_status,
                "blocker_count": len(blockers),
                "warning_count": len(warnings),
                "promoted_row_count": promoted_row_count,
                "route_queue_count": route_queue_count,
                "unresolved_note_count": len(unresolved_source_notes),
                "quality_report_dir": str(quality_dir) if quality_dir else "",
                "summary_json_path": str(out_dir / "summary.json"),
                "created_at_utc": created_at,
            },
            item_rows,
        )

    print(json.dumps({
        "batch_closeout_audit_run_id": closeout_run_id,
        "promotion_apply_run_id": apply_run_id,
        "apply_run_kind": apply_run_kind,
        "closeout_status": closeout_status,
        "promoted_row_count": promoted_row_count,
        "route_queue_count": route_queue_count,
        "terminal_route_queue_count": terminal_route_queue_count,
        "unresolved_note_count": len(unresolved_source_notes),
        "output_dir": str(out_dir),
    }, indent=2, sort_keys=True))
    return 0 if closeout_status == "ready_to_advance" else 2


if __name__ == "__main__":
    raise SystemExit(main())
