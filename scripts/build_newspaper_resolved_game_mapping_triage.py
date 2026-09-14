#!/usr/bin/env python
"""Triage resolved newspaper game-score mapping holds against v26.

This station consumes `newspaper_resolved.game_candidate` rows with
`hold_needs_game_mapping`, compares them to local v26 by date/team/score, and
splits them into auditable queues. It writes only local D-drive artifacts and
local DuckDB review tables.
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
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_game_mapping_triage"
DEFAULT_V26 = Path(
    r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet"
)

ITEM_FIELDS = [
    "resolved_game_mapping_triage_run_id",
    "resolved_atom_materialize_run_id",
    "target_entity_key",
    "boxscore_id",
    "game_date",
    "year",
    "week",
    "team_1_raw",
    "team_1_resolved",
    "team_1_score",
    "team_2_raw",
    "team_2_resolved",
    "team_2_score",
    "triage_status",
    "action_status",
    "risk_level",
    "v26_match_status",
    "v26_game_date",
    "v26_week",
    "v26_team",
    "v26_opponent",
    "v26_team_score",
    "v26_opponent_score",
    "duplicate_key",
    "duplicate_key_count",
    "reason",
    "reconciliation_status",
    "evidence_text",
    "source_documents_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "resolved_game_mapping_triage_run_id",
    "resolved_atom_materialize_run_id",
    "v26_path",
    "output_dir",
    "input_row_count",
    "context_count",
    "corroboration_count",
    "conflict_count",
    "missing_or_manual_count",
    "status",
    "created_at_utc",
    "summary_json_path",
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


def parse_int(value: Any) -> int | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


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


def latest_resolved_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT resolved_atom_materialize_run_id
        FROM newspaper_review.resolved_atom_materialize_run
        ORDER BY created_at_utc DESC, resolved_atom_materialize_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def latest_run_dir(root: Path, run_id: str) -> Path | None:
    for path in sorted(Path(p) for p in glob.glob(str(root / "resolved_atom_materializations" / "*" / "summary.json"))):
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if clean(summary.get("resolved_atom_materialize_run_id")) == run_id:
            return path.parent
    return None


def load_game_holds(con: duckdb.DuckDBPyConnection, resolved_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_resolved.game_candidate
        WHERE resolved_atom_materialize_run_id = ?
          AND materialization_status = 'hold_needs_game_mapping'
        ORDER BY game_date, boxscore_id, team_1_resolved, team_2_resolved, target_entity_key
        """,
        [resolved_run_id],
    )


def load_v26_games(v26_path: Path) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        return query_dicts(
            con,
            """
            SELECT
              CAST(game_date AS VARCHAR) AS game_date,
              CAST(year AS INTEGER) AS year,
              CAST(week AS INTEGER) AS week,
              nfl_team,
              opponent_nfl_team,
              CAST(MAX(pts_def_team_pts) AS INTEGER) AS team_score,
              CAST(MAX(points_allowed) AS INTEGER) AS opponent_score
            FROM read_parquet(?)
            WHERE game_date IS NOT NULL
              AND nfl_team IS NOT NULL
              AND opponent_nfl_team IS NOT NULL
              AND year BETWEEN 1920 AND 1939
            GROUP BY game_date, year, week, nfl_team, opponent_nfl_team
            """,
            [str(v26_path)],
        )
    finally:
        con.close()


def date_key(value: Any) -> str:
    text = clean(value)
    return text[:10]


def duplicate_key(row: dict[str, Any]) -> str:
    return "|".join([
        date_key(row.get("game_date")),
        clean(row.get("team_1_resolved")),
        clean(row.get("team_2_resolved")),
        clean(row.get("team_1_score")),
        clean(row.get("team_2_score")),
    ])


def index_v26(v26_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in v26_rows:
        out.setdefault(date_key(row.get("game_date")), []).append(row)
    return out


def score_matches(row: dict[str, Any], v26: dict[str, Any], directed: bool) -> bool:
    s1 = parse_int(row.get("team_1_score"))
    s2 = parse_int(row.get("team_2_score"))
    vt = parse_int(v26.get("team_score"))
    vo = parse_int(v26.get("opponent_score"))
    if None in {s1, s2, vt, vo}:
        return False
    return (s1 == vt and s2 == vo) if directed else (s1 == vo and s2 == vt)


def find_match(row: dict[str, Any], v26_by_date: dict[str, list[dict[str, Any]]]) -> tuple[str, dict[str, Any], str]:
    team1 = clean(row.get("team_1_resolved"))
    team2 = clean(row.get("team_2_resolved"))
    date = date_key(row.get("game_date"))
    candidates = v26_by_date.get(date, [])
    same_pair: list[tuple[dict[str, Any], bool]] = []
    for candidate in candidates:
        vt = clean(candidate.get("nfl_team"))
        vo = clean(candidate.get("opponent_nfl_team"))
        if vt == team1 and vo == team2:
            same_pair.append((candidate, True))
        elif vt == team2 and vo == team1:
            same_pair.append((candidate, False))
    for candidate, directed in same_pair:
        if score_matches(row, candidate, directed):
            return "exact_score_match", candidate, "date_team_score_match"
    if same_pair:
        candidate, _ = same_pair[0]
        if parse_int(candidate.get("team_score")) is None or parse_int(candidate.get("opponent_score")) is None:
            return "v26_score_missing", candidate, "date_team_match_v26_score_blank"
        return "score_conflict", candidate, "date_team_match_score_differs"
    if candidates:
        return "date_no_team_match", candidates[0], "date_exists_but_team_pair_missing"
    return "missing_date", {}, "no_v26_game_on_date"


def classify_row(row: dict[str, Any], v26_by_date: dict[str, list[dict[str, Any]]], duplicate_counts: Counter[str]) -> dict[str, str]:
    team1 = clean(row.get("team_1_resolved"))
    team2 = clean(row.get("team_2_resolved"))
    dup_key = duplicate_key(row)
    if team1 == "NON_NFL" or team2 == "NON_NFL":
        triage_status = "context_non_nfl_opponent"
        action_status = "archive_context_only"
        risk = "low"
        match_status = "not_v26_candidate_non_nfl_opponent"
        v26 = {}
        reason = "one side resolved to NON_NFL"
    else:
        match_status, v26, reason = find_match(row, v26_by_date)
        if match_status == "exact_score_match":
            triage_status = "corroborates_v26_score"
            action_status = "archive_corroboration_only"
            risk = "low"
        elif match_status == "v26_score_missing":
            triage_status = "score_fills_blank_v26_game"
            action_status = "ready_score_backfill_review"
            risk = "medium"
        elif match_status == "score_conflict":
            triage_status = "score_conflict_vs_v26"
            action_status = "conflict_review"
            risk = "high"
        elif match_status == "date_no_team_match":
            triage_status = "team_mapping_or_boxscore_mismatch"
            action_status = "manual_game_mapping_review"
            risk = "medium"
        else:
            triage_status = "potential_missing_game_or_bad_date"
            action_status = "manual_game_mapping_review"
            risk = "medium"
    return {
        "triage_status": triage_status,
        "action_status": action_status,
        "risk_level": risk,
        "v26_match_status": match_status,
        "v26_game_date": date_key(v26.get("game_date")),
        "v26_week": clean(v26.get("week")),
        "v26_team": clean(v26.get("nfl_team")),
        "v26_opponent": clean(v26.get("opponent_nfl_team")),
        "v26_team_score": clean(v26.get("team_score")),
        "v26_opponent_score": clean(v26.get("opponent_score")),
        "duplicate_key": dup_key,
        "duplicate_key_count": str(duplicate_counts[dup_key]),
        "reason": reason,
    }


def build_items(run_id: str, resolved_run_id: str, rows: list[dict[str, Any]], v26_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = iso_now()
    duplicate_counts = Counter(duplicate_key(row) for row in rows)
    v26_by_date = index_v26(v26_rows)
    items: list[dict[str, Any]] = []
    for row in rows:
        classification = classify_row(row, v26_by_date, duplicate_counts)
        items.append({
            "resolved_game_mapping_triage_run_id": run_id,
            "resolved_atom_materialize_run_id": resolved_run_id,
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "game_date": date_key(row.get("game_date")),
            "year": clean(row.get("year")),
            "week": clean(row.get("week")),
            "team_1_raw": clean(row.get("team_1_raw")),
            "team_1_resolved": clean(row.get("team_1_resolved")),
            "team_1_score": clean(row.get("team_1_score")),
            "team_2_raw": clean(row.get("team_2_raw")),
            "team_2_resolved": clean(row.get("team_2_resolved")),
            "team_2_score": clean(row.get("team_2_score")),
            **classification,
            "reconciliation_status": clean(row.get("reconciliation_status")),
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "created_at_utc": created_at,
        })
    return items


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    item_defs = ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_game_mapping_triage_item ({item_defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='resolved_game_mapping_triage_item'
            """
        ).fetchall()
    }
    for field in ITEM_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.resolved_game_mapping_triage_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_game_mapping_triage_run (
          resolved_game_mapping_triage_run_id VARCHAR,
          resolved_atom_materialize_run_id VARCHAR,
          v26_path VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          context_count INTEGER,
          corroboration_count INTEGER,
          conflict_count INTEGER,
          missing_or_manual_count INTEGER,
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
        run_id = clean(run_row.get("resolved_game_mapping_triage_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_game_mapping_triage_run WHERE resolved_game_mapping_triage_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_game_mapping_triage_item WHERE resolved_game_mapping_triage_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_game_mapping_triage_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_game_mapping_triage_item", item_rows, ITEM_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], item_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Resolved Game Mapping Triage",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Resolved atom run: `{summary['resolved_atom_materialize_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Triage counts: `{summary['triage_status_counts']}`",
        "",
        "## Samples",
        "",
        "| Status | Date | Teams | Newspaper Score | v26 Score | Reason |",
        "|---|---|---|---|---|---|",
    ]
    for row in item_rows[:25]:
        teams = f"{row['team_1_resolved']} vs {row['team_2_resolved']}"
        score = f"{row['team_1_score']}-{row['team_2_score']}"
        v26_score = f"{row['v26_team']} {row['v26_team_score']}, {row['v26_opponent']} {row['v26_opponent_score']}".strip()
        lines.append(
            f"| `{row['triage_status']}` | `{row['game_date']}` | `{teams}` | `{score}` | `{v26_score}` | `{row['reason']}` |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Items CSV: `{summary['items_csv']}`",
        f"- Context CSV: `{summary['context_csv']}`",
        f"- Corroboration CSV: `{summary['corroboration_csv']}`",
        f"- Conflict CSV: `{summary['conflict_csv']}`",
        f"- Manual CSV: `{summary['manual_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--resolved-atom-materialize-run-id", default="")
    parser.add_argument("--label", default="resolved_game_mapping_triage")
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
        resolved_run_id = args.resolved_atom_materialize_run_id or latest_resolved_run(con)
        if not resolved_run_id:
            raise RuntimeError("No resolved atom materialization run found")
        rows = load_game_holds(con, resolved_run_id)
    finally:
        con.close()

    v26_rows = load_v26_games(args.v26_path)
    item_rows = build_items(run_id, resolved_run_id, rows, v26_rows)
    status_counts = Counter(row["triage_status"] for row in item_rows)
    action_counts = Counter(row["action_status"] for row in item_rows)

    context_rows = [row for row in item_rows if row["action_status"] == "archive_context_only"]
    corroboration_rows = [row for row in item_rows if row["action_status"] == "archive_corroboration_only"]
    conflict_rows = [row for row in item_rows if row["action_status"] == "conflict_review"]
    manual_rows = [row for row in item_rows if row["action_status"] == "manual_game_mapping_review"]

    items_csv = out_dir / "resolved_game_mapping_triage_items.csv"
    context_csv = out_dir / "context_non_nfl_or_nonpromotable_scores.csv"
    corroboration_csv = out_dir / "corroborates_v26_scores.csv"
    conflict_csv = out_dir / "score_conflict_review.csv"
    manual_csv = out_dir / "manual_game_mapping_review.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "resolved_game_mapping_triage_report.md"

    summary = {
        "created_at_utc": created_at,
        "resolved_game_mapping_triage_run_id": run_id,
        "resolved_atom_materialize_run_id": resolved_run_id,
        "resolved_atom_run_dir": str(latest_run_dir(args.root, resolved_run_id) or ""),
        "v26_path": str(args.v26_path),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(item_rows),
        "context_count": len(context_rows),
        "corroboration_count": len(corroboration_rows),
        "conflict_count": len(conflict_rows),
        "missing_or_manual_count": len(manual_rows),
        "triage_status_counts": dict(sorted(status_counts.items())),
        "action_status_counts": dict(sorted(action_counts.items())),
        "items_csv": str(items_csv),
        "context_csv": str(context_csv),
        "corroboration_csv": str(corroboration_csv),
        "conflict_csv": str(conflict_csv),
        "manual_csv": str(manual_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown),
        "persisted_to_duckdb": not args.no_persist,
    }

    write_csv(items_csv, item_rows, ITEM_FIELDS)
    write_csv(context_csv, context_rows, ITEM_FIELDS)
    write_csv(corroboration_csv, corroboration_rows, ITEM_FIELDS)
    write_csv(conflict_csv, conflict_rows, ITEM_FIELDS)
    write_csv(manual_csv, manual_rows, ITEM_FIELDS)
    write_json(summary_json, summary)
    markdown.write_text(render_markdown(summary, item_rows), encoding="utf-8")

    if not args.no_persist:
        persist(
            args.db_path,
            {
                "resolved_game_mapping_triage_run_id": run_id,
                "resolved_atom_materialize_run_id": resolved_run_id,
                "v26_path": str(args.v26_path),
                "output_dir": str(out_dir),
                "input_row_count": len(item_rows),
                "context_count": len(context_rows),
                "corroboration_count": len(corroboration_rows),
                "conflict_count": len(conflict_rows),
                "missing_or_manual_count": len(manual_rows),
                "status": "complete",
                "created_at_utc": created_at,
                "summary_json_path": str(summary_json),
            },
            item_rows,
        )

    print(json.dumps({
        "resolved_game_mapping_triage_run_id": run_id,
        "resolved_atom_materialize_run_id": resolved_run_id,
        "output_dir": str(out_dir),
        "input_row_count": len(item_rows),
        "triage_status_counts": dict(sorted(status_counts.items())),
        "action_status_counts": dict(sorted(action_counts.items())),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
