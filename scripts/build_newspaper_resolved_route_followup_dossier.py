#!/usr/bin/env python
"""Build a follow-up dossier for the current resolved package route queue.

This is read-only against local DuckDB. It summarizes the latest
`resolved_package_decision_route_queue`, attaches the latest second-pass hold
reason when available, and writes CSV/Markdown/JSON artifacts to D drive.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_route_followup_dossiers"
DEFAULT_TOOL_MIRROR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor")

DOSSIER_FIELDS = [
    "resolved_package_decision_run_id",
    "package_item_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "confidence_bar",
    "risk_level",
    "followup_reason",
    "recommended_action",
    "source_documents_json",
    "evidence_text",
    "proposed_summary",
    "proposed_fields_json",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    text = str(value).strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def parse_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_decision_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT resolved_package_decision_run_id
        FROM newspaper_review.resolved_package_decision_ledger_run
        ORDER BY created_at_utc DESC, resolved_package_decision_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def latest_second_pass_run(con: duckdb.DuckDBPyConnection) -> str:
    try:
        row = con.execute(
            """
            SELECT second_pass_decision_run_id
            FROM newspaper_review.resolved_second_pass_decision_run
            ORDER BY created_at_utc DESC, second_pass_decision_run_id DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        row = None
    return clean(row[0]) if row else ""


def load_routes(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
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


def load_second_pass_holds(con: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, dict[str, Any]]:
    if not run_id:
        return {}
    try:
        rows = query_dicts(
            con,
            """
            SELECT *
            FROM newspaper_review.resolved_second_pass_decision_item
            WHERE second_pass_decision_run_id = ?
            """,
            [run_id],
        )
    except Exception:
        return {}
    return {clean(row.get("package_item_id")): row for row in rows}


def proposed_summary(target_table: str, proposed: dict[str, Any]) -> str:
    keys_by_target = {
        "game_candidate": ["game_date", "team_1_raw", "team_1_score", "team_2_raw", "team_2_score", "claimed_score_text"],
        "lineup_participation": ["participation_type", "player_raw", "NFL_player_id", "nfl_team", "starter_position", "listed_position_raw", "source_row_text"],
        "play_by_play_event": ["play_type", "possession_team_raw", "primary_player_raw", "secondary_player_raw", "yards", "points", "play_text"],
        "player_game_box_score": ["player_raw", "NFL_player_id", "nfl_team", "rushing_tds", "passing_tds", "receiving_tds", "def_tds", "special_teams_tds", "touchdowns", "fg_made", "pat_made", "source_row_text"],
        "player_identity_candidate": ["player_raw", "candidate_NFL_player_id", "candidate_player_name", "source_context"],
        "scoring_event": ["event_type", "scoring_team_raw", "scoring_player_raw", "passer_raw", "receiver_raw", "points", "distance_yards", "play_text"],
        "team_game_stat_claim": ["stat_name", "team_1_raw", "team_1_value", "team_2_raw", "team_2_value", "source_row_text"],
    }
    parts = []
    for key in keys_by_target.get(target_table, []):
        value = clean(proposed.get(key))
        if value:
            parts.append(f"{key}={value}")
    return " | ".join(parts)


def recommended_action(row: dict[str, Any], hold: dict[str, Any]) -> str:
    lane = clean(row.get("route_to_lane"))
    target = clean(row.get("target_table"))
    reason = clean(hold.get("reason"))
    if lane == "ready_resolved_atom_review" and reason:
        if "source document date" in reason:
            return "verify game mapping/source date before approval"
        if "support rules not met" in reason:
            return "manual source-text/visual review"
        if "game_candidate" in reason:
            return "game candidate score/source review"
    if "identity" in lane:
        return "resolve player identity, then rerun resolved decision ledger"
    if lane in {"manual_game_mapping_review", "score_conflict_review", "score_backfill_review"}:
        return "resolve game/score mapping before atom promotion"
    if lane == "touchdown_total_type_review":
        return "split touchdown total into rush/rec/pass/def/st only with source support"
    if lane == "context_review":
        return "read surrounding article context"
    return f"review lane: {lane or '(blank)'} / target: {target}"


def build_dossier_rows(
    decision_run_id: str,
    route_rows: list[dict[str, Any]],
    second_pass_holds: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    rows = []
    for row in route_rows:
        proposed = parse_obj(row.get("proposed_fields_json"))
        package_item_id = clean(row.get("package_item_id"))
        hold = second_pass_holds.get(package_item_id, {})
        target = clean(row.get("target_table"))
        rows.append({
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "target_table": target,
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "decision_status": clean(row.get("decision_status")),
            "decision_value": clean(row.get("decision_value")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "confidence_bar": clean(row.get("confidence_bar")),
            "risk_level": clean(row.get("risk_level")),
            "followup_reason": clean(hold.get("reason")) or clean(row.get("decision_value")) or clean(row.get("route_to_lane")),
            "recommended_action": recommended_action(row, hold),
            "source_documents_json": clean(row.get("source_documents_json")),
            "evidence_text": clean(row.get("evidence_text")),
            "proposed_summary": proposed_summary(target, proposed),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
        })
    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DOSSIER_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def markdown_table(rows: list[dict[str, Any]], fields: list[str]) -> list[str]:
    if not rows:
        return ["_No rows._"]
    out = ["| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in rows:
        values = []
        for field in fields:
            value = clean(row.get(field)).replace("|", "\\|").replace("\n", " ")
            if len(value) > 220:
                value = value[:217] + "..."
            values.append(value)
        out.append("| " + " | ".join(values) + " |")
    return out


def write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, str]]) -> None:
    lines = [
        "# Resolved Route Follow-up Dossier",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Decision run: `{summary['resolved_package_decision_run_id']}`",
        f"Second-pass run: `{summary['second_pass_decision_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Route rows: `{summary['route_row_count']}`",
        f"- By lane: `{summary['route_lane_counts']}`",
        f"- By target: `{summary['target_table_counts']}`",
        f"- Recommended actions: `{summary['recommended_action_counts']}`",
        "",
        "## Rows",
        "",
    ]
    lines.extend(markdown_table(rows, [
        "target_table",
        "boxscore_id",
        "route_to_lane",
        "followup_reason",
        "recommended_action",
        "proposed_summary",
        "evidence_text",
    ]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--tool-mirror-root", type=Path, default=DEFAULT_TOOL_MIRROR)
    parser.add_argument("--label", default="resolved_route_followup_dossier")
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--no-mirror", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        decision_run_id = args.resolved_package_decision_run_id or latest_decision_run(con)
        second_pass_run_id = latest_second_pass_run(con)
        route_rows = load_routes(con, decision_run_id)
        holds = load_second_pass_holds(con, second_pass_run_id)
    finally:
        con.close()
    if not decision_run_id:
        raise SystemExit("No resolved package decision run found")

    rows = build_dossier_rows(decision_run_id, route_rows, holds)
    csv_path = out_dir / "resolved_route_followup_dossier.csv"
    md_path = out_dir / "resolved_route_followup_dossier.md"
    summary_path = out_dir / "summary.json"
    write_csv(csv_path, rows)

    route_lane_counts = Counter(row["route_to_lane"] for row in rows)
    target_counts = Counter(row["target_table"] for row in rows)
    action_counts = Counter(row["recommended_action"] for row in rows)
    summary = {
        "resolved_route_followup_dossier_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "second_pass_decision_run_id": second_pass_run_id,
        "created_at_utc": iso_now(),
        "output_dir": str(out_dir),
        "db_path": str(args.db_path),
        "route_row_count": len(rows),
        "route_lane_counts": dict(sorted(route_lane_counts.items())),
        "target_table_counts": dict(sorted(target_counts.items())),
        "recommended_action_counts": dict(sorted(action_counts.items())),
        "dossier_csv": str(csv_path),
        "dossier_md": str(md_path),
        "summary_json": str(summary_path),
    }
    write_json(summary_path, summary)
    write_markdown(md_path, summary, rows)

    if not args.no_mirror:
        mirror_dir = args.tool_mirror_root / "resolved_route_followup_dossiers" / run_id
        mirror_dir.mkdir(parents=True, exist_ok=True)
        write_csv(mirror_dir / "resolved_route_followup_dossier.csv", rows)
        write_json(mirror_dir / "summary.json", summary)
        write_markdown(mirror_dir / "resolved_route_followup_dossier.md", summary, rows)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
