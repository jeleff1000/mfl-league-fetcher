#!/usr/bin/env python
"""
Build an auditable decision ledger for resolved newspaper package review packets.

This station consumes the resolved package review packets and turns every packet
item into an explicit local decision state. By default it does not promote rows;
it closes archive-only items and routes everything else for review/follow-up.
Optional CSV overrides can approve or reroute individual package items.
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


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_package_decision_ledgers"

DECISION_FIELDS = [
    "resolved_package_decision_run_id",
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "packet_id",
    "packet_index",
    "packet_item_index",
    "package_item_id",
    "package_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "game_date",
    "player_week",
    "NFL_player_id",
    "nfl_team",
    "opponent_nfl_team",
    "review_status",
    "risk_level",
    "confidence_bar",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "suggested_decision_value",
    "decision_source",
    "source_documents_json",
    "evidence_text",
    "proposed_summary",
    "proposed_fields_json",
    "full_row_json",
    "reason",
    "decision_prompt",
    "packet_md_path",
    "notes",
    "created_at_utc",
]

ROUTE_FIELDS = [
    "resolved_package_decision_run_id",
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "package_item_id",
    "package_lane",
    "route_to_lane",
    "decision_status",
    "decision_value",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "risk_level",
    "confidence_bar",
    "recommended_next_action",
    "reason",
    "packet_md_path",
    "source_documents_json",
    "evidence_text",
    "proposed_fields_json",
    "notes",
    "created_at_utc",
]

CLOSED_FIELDS = [
    "resolved_package_decision_run_id",
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "package_item_id",
    "package_lane",
    "closure_bucket",
    "decision_status",
    "decision_value",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "risk_level",
    "confidence_bar",
    "reason",
    "packet_md_path",
    "source_documents_json",
    "evidence_text",
    "proposed_fields_json",
    "notes",
    "created_at_utc",
]

RUN_FIELDS = [
    "resolved_package_decision_run_id",
    "review_packet_run_id",
    "resolved_promotion_package_run_id",
    "output_dir",
    "decision_count",
    "route_queue_count",
    "closed_decision_count",
    "approved_decision_count",
    "decision_status_counts_json",
    "decision_value_counts_json",
    "route_queue_counts_json",
    "closed_decision_counts_json",
    "target_table_counts_json",
    "decision_ledger_csv",
    "route_queue_csv",
    "closed_decisions_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

APPROVE_VALUES = {"approve", "approved", "approved_for_local_promotion", "approved_for_local_newspaper_final"}
CLOSE_VALUES = {"archive", "context_archive", "corroboration_archive", "reject", "rejected", "closed"}


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join(clean(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_packet_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT review_packet_run_id
        FROM newspaper_review.resolved_package_review_packet_run
        ORDER BY created_at_utc DESC, review_packet_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_packet_items(con: duckdb.DuckDBPyConnection, review_packet_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT
          p.review_packet_run_id,
          p.resolved_promotion_package_run_id,
          p.packet_id,
          p.packet_index,
          p.packet_item_index,
          p.package_item_id,
          p.package_lane,
          p.target_table,
          p.review_status,
          p.risk_level,
          p.target_entity_key,
          p.boxscore_id,
          p.game_date,
          p.player_week,
          p.NFL_player_id,
          p.nfl_team,
          p.opponent_nfl_team,
          p.confidence_bar,
          p.source_documents_json,
          p.evidence_text,
          p.proposed_summary,
          p.reason,
          p.decision_prompt,
          p.packet_md_path,
          i.proposed_fields_json,
          i.full_row_json
        FROM newspaper_review.resolved_package_review_packet_item p
        LEFT JOIN newspaper_review.resolved_promotion_package_item i
          ON p.package_item_id = i.package_item_id
         AND p.resolved_promotion_package_run_id = i.resolved_promotion_package_run_id
        WHERE p.review_packet_run_id = ?
        ORDER BY TRY_CAST(p.packet_index AS INTEGER), TRY_CAST(p.packet_item_index AS INTEGER), p.package_item_id
        """,
        [review_packet_run_id],
    )


def load_overrides(paths: list[Path] | None) -> dict[str, dict[str, str]]:
    if not paths:
        return {}
    output: dict[str, dict[str, str]] = {}
    for path in paths:
        rows = read_csv(path)
        for row in rows:
            key = clean(row.get("package_item_id")) or clean(row.get("decision_id"))
            if key:
                output[key] = row
    return output


def default_decision(row: dict[str, Any], auto_close_archives: bool) -> tuple[str, str, str, str, str]:
    review_status = clean(row.get("review_status"))
    lane = clean(row.get("package_lane"))
    prompt = clean(row.get("decision_prompt"))

    if review_status == "archive" and auto_close_archives:
        return "closed", "archive", lane, "archive", "closed archive-only item; retained as evidence, not a promotable stat row"
    if review_status == "needs_identity":
        return "needs_identity", "needs_identity", lane or "identity_review", "needs_identity", prompt
    if review_status == "needs_review":
        return "needs_review", "needs_review", lane or "manual_review", "needs_review", prompt
    if review_status == "pending_review":
        return "pending_review", "pending_review", lane or "packet_review", "review_then_decide", prompt
    return "pending_review", "pending_review", lane or "packet_review", "review_then_decide", prompt


def apply_override(decision: dict[str, Any], override: dict[str, str]) -> dict[str, Any]:
    if not override:
        return decision
    output = dict(decision)
    for field in [
        "decision_status",
        "decision_value",
        "route_to_lane",
        "target_table",
        "target_entity_key",
        "boxscore_id",
        "proposed_fields_json",
        "notes",
    ]:
        value = clean(override.get(field))
        if value:
            output[field] = value
    output["decision_source"] = "override_csv"
    return output


def build_decisions(
    rows: list[dict[str, Any]],
    run_id: str,
    overrides: dict[str, dict[str, str]],
    auto_close_archives: bool,
    created_at: str,
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for row in rows:
        status, value, route, suggested, default_note = default_decision(row, auto_close_archives)
        decision = {
            "resolved_package_decision_run_id": run_id,
            "review_packet_run_id": clean(row.get("review_packet_run_id")),
            "resolved_promotion_package_run_id": clean(row.get("resolved_promotion_package_run_id")),
            "packet_id": clean(row.get("packet_id")),
            "packet_index": clean(row.get("packet_index")),
            "packet_item_index": clean(row.get("packet_item_index")),
            "package_item_id": clean(row.get("package_item_id")),
            "package_lane": clean(row.get("package_lane")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "game_date": clean(row.get("game_date")),
            "player_week": clean(row.get("player_week")),
            "NFL_player_id": clean(row.get("NFL_player_id")),
            "nfl_team": clean(row.get("nfl_team")),
            "opponent_nfl_team": clean(row.get("opponent_nfl_team")),
            "review_status": clean(row.get("review_status")),
            "risk_level": clean(row.get("risk_level")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "decision_status": status,
            "decision_value": value,
            "route_to_lane": route,
            "suggested_decision_value": suggested,
            "decision_source": "default_scaffold",
            "source_documents_json": clean(row.get("source_documents_json")),
            "evidence_text": clean(row.get("evidence_text")),
            "proposed_summary": clean(row.get("proposed_summary")),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "full_row_json": clean(row.get("full_row_json")),
            "reason": clean(row.get("reason")),
            "decision_prompt": clean(row.get("decision_prompt")),
            "packet_md_path": clean(row.get("packet_md_path")),
            "notes": default_note,
            "created_at_utc": created_at,
        }
        decision = apply_override(decision, overrides.get(decision["package_item_id"], {}))
        decisions.append(decision)
    return decisions


def should_close(decision: dict[str, Any]) -> tuple[bool, str]:
    value = clean(decision.get("decision_value")).lower()
    status = clean(decision.get("decision_status")).lower()
    lane = clean(decision.get("package_lane"))
    if value in CLOSE_VALUES or status == "closed":
        return True, value or lane or "closed"
    return False, ""


def build_route_and_closed(decisions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []

    for decision in decisions:
        value = clean(decision.get("decision_value")).lower()
        if value in APPROVE_VALUES:
            approved.append(decision)
            continue
        close, bucket = should_close(decision)
        if close:
            row = {field: clean(decision.get(field)) for field in CLOSED_FIELDS}
            row["closure_bucket"] = bucket
            closed.append(row)
            continue
        row = {field: clean(decision.get(field)) for field in ROUTE_FIELDS}
        row["recommended_next_action"] = clean(decision.get("decision_prompt")) or clean(decision.get("notes"))
        routes.append(row)
    return approved, routes, closed


def markdown_escape(value: Any) -> str:
    return clean(value).replace("|", "\\|").replace("\n", " ")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    if not rows:
        return ["_(none)_"]
    lines = [
        "| " + " | ".join(fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(markdown_escape(row.get(field, "")) for field in fields) + " |")
    return lines


def write_report(
    path: Path,
    summary: dict[str, Any],
    decisions: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    closed: list[dict[str, Any]],
    approved: list[dict[str, Any]],
) -> None:
    lines = [
        "# Newspaper Resolved Package Decision Ledger",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"- Review packet run: `{summary['review_packet_run_id']}`",
        f"- Resolved package run: `{summary['resolved_promotion_package_run_id']}`",
        f"- Output: `{summary['output_dir']}`",
        "",
        "## Counts",
        "",
        f"- Decisions: `{summary['decision_count']}`",
        f"- Route queue: `{summary['route_queue_count']}`",
        f"- Closed: `{summary['closed_decision_count']}`",
        f"- Approved by override: `{summary['approved_decision_count']}`",
        f"- Decision statuses: `{summary['decision_status_counts']}`",
        f"- Route lanes: `{summary['route_queue_counts']}`",
        "",
        "## Route Queue Sample",
        "",
    ]
    lines.extend(markdown_table(routes[:80], [
        "route_to_lane",
        "target_table",
        "boxscore_id",
        "package_item_id",
        "risk_level",
        "recommended_next_action",
    ]))
    lines.extend(["", "## Closed Sample", ""])
    lines.extend(markdown_table(closed[:40], [
        "closure_bucket",
        "target_table",
        "boxscore_id",
        "package_item_id",
        "reason",
    ]))
    lines.extend(["", "## Approved Override Sample", ""])
    lines.extend(markdown_table(approved[:40], [
        "decision_value",
        "target_table",
        "boxscore_id",
        "package_item_id",
        "risk_level",
    ]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    for table, fields in [
        ("resolved_package_decision_ledger", DECISION_FIELDS),
        ("resolved_package_decision_route_queue", ROUTE_FIELDS),
        ("resolved_package_decision_closed", CLOSED_FIELDS),
        ("resolved_package_decision_ledger_run", RUN_FIELDS),
    ]:
        defs = ", ".join(f"{field} VARCHAR" for field in fields)
        con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.{table} ({defs})")
        existing = {
            row[0]
            for row in con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='newspaper_review' AND table_name=?
                """,
                [table],
            ).fetchall()
        }
        for field in fields:
            if field not in existing:
                con.execute(f"ALTER TABLE newspaper_review.{table} ADD COLUMN IF NOT EXISTS {field} VARCHAR")


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in fields)
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({', '.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    run_row: dict[str, Any],
    decisions: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    closed: list[dict[str, Any]],
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        run_id = clean(run_row.get("resolved_package_decision_run_id"))
        for table in [
            "resolved_package_decision_ledger_run",
            "resolved_package_decision_ledger",
            "resolved_package_decision_route_queue",
            "resolved_package_decision_closed",
        ]:
            con.execute(f"DELETE FROM newspaper_review.{table} WHERE resolved_package_decision_run_id = ?", [run_id])
        insert_rows(con, "resolved_package_decision_ledger_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_package_decision_ledger", decisions, DECISION_FIELDS)
        insert_rows(con, "resolved_package_decision_route_queue", routes, ROUTE_FIELDS)
        insert_rows(con, "resolved_package_decision_closed", closed, CLOSED_FIELDS)
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="resolved_package_decision_ledger")
    parser.add_argument("--review-packet-run-id", default="")
    parser.add_argument("--decision-input-csv", type=Path, action="append", default=[])
    parser.add_argument("--no-auto-close-archives", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        review_packet_run_id = args.review_packet_run_id or latest_packet_run(con)
        rows = load_packet_items(con, review_packet_run_id)
    finally:
        con.close()
    if not review_packet_run_id:
        raise SystemExit("No resolved package review packet run found")
    if not rows:
        raise SystemExit(f"No packet items found for review packet run {review_packet_run_id}")

    overrides = load_overrides(args.decision_input_csv)
    decisions = build_decisions(
        rows,
        run_id,
        overrides,
        auto_close_archives=not args.no_auto_close_archives,
        created_at=created_at,
    )
    approved, routes, closed = build_route_and_closed(decisions)
    package_run_id = clean(decisions[0].get("resolved_promotion_package_run_id")) if decisions else ""

    decision_csv = out_dir / "resolved_package_decision_ledger.csv"
    route_csv = out_dir / "resolved_package_route_queue.csv"
    closed_csv = out_dir / "resolved_package_closed_decisions.csv"
    approved_csv = out_dir / "resolved_package_approved_overrides.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "resolved_package_decision_ledger_report.md"

    write_csv(decision_csv, decisions, DECISION_FIELDS)
    write_csv(route_csv, routes, ROUTE_FIELDS)
    write_csv(closed_csv, closed, CLOSED_FIELDS)
    write_csv(approved_csv, approved, DECISION_FIELDS)

    rows_by_route: dict[str, list[dict[str, Any]]] = {}
    for row in routes:
        rows_by_route.setdefault(clean(row.get("route_to_lane")) or "pending_review", []).append(row)
    for route, route_rows in rows_by_route.items():
        safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in route)
        write_csv(out_dir / f"route_{safe}.csv", route_rows, ROUTE_FIELDS)

    status_counts = Counter(clean(row.get("decision_status")) for row in decisions)
    value_counts = Counter(clean(row.get("decision_value")) for row in decisions)
    route_counts = Counter(clean(row.get("route_to_lane")) for row in routes)
    closed_counts = Counter(clean(row.get("closure_bucket")) for row in closed)
    target_counts = Counter(clean(row.get("target_table")) for row in decisions)

    summary = {
        "resolved_package_decision_run_id": run_id,
        "review_packet_run_id": review_packet_run_id,
        "resolved_promotion_package_run_id": package_run_id,
        "created_at_utc": created_at,
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "decision_input_csv": ";".join(str(path) for path in args.decision_input_csv),
        "decision_input_csvs": [str(path) for path in args.decision_input_csv],
        "decision_count": len(decisions),
        "route_queue_count": len(routes),
        "closed_decision_count": len(closed),
        "approved_decision_count": len(approved),
        "decision_status_counts": dict(sorted(status_counts.items())),
        "decision_value_counts": dict(sorted(value_counts.items())),
        "route_queue_counts": dict(sorted(route_counts.items())),
        "closed_decision_counts": dict(sorted(closed_counts.items())),
        "target_table_counts": dict(sorted(target_counts.items())),
        "decision_ledger_csv": str(decision_csv),
        "route_queue_csv": str(route_csv),
        "closed_decisions_csv": str(closed_csv),
        "approved_overrides_csv": str(approved_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }
    run_row = {
        "resolved_package_decision_run_id": run_id,
        "review_packet_run_id": review_packet_run_id,
        "resolved_promotion_package_run_id": package_run_id,
        "output_dir": str(out_dir),
        "decision_count": str(len(decisions)),
        "route_queue_count": str(len(routes)),
        "closed_decision_count": str(len(closed)),
        "approved_decision_count": str(len(approved)),
        "decision_status_counts_json": json.dumps(summary["decision_status_counts"], sort_keys=True),
        "decision_value_counts_json": json.dumps(summary["decision_value_counts"], sort_keys=True),
        "route_queue_counts_json": json.dumps(summary["route_queue_counts"], sort_keys=True),
        "closed_decision_counts_json": json.dumps(summary["closed_decision_counts"], sort_keys=True),
        "target_table_counts_json": json.dumps(summary["target_table_counts"], sort_keys=True),
        "decision_ledger_csv": str(decision_csv),
        "route_queue_csv": str(route_csv),
        "closed_decisions_csv": str(closed_csv),
        "summary_json": str(summary_json),
        "persisted_to_duckdb": "false" if args.dry_run else "true",
        "created_at_utc": created_at,
    }

    write_json(summary_json, summary)
    write_report(report_md, summary, decisions, routes, closed, approved)
    if not args.dry_run:
        persist(args.db_path, run_row, decisions, routes, closed)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
