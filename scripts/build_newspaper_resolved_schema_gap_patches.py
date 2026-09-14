#!/usr/bin/env python
"""Build auditable patches for resolved newspaper schema-gap stat rows.

This station consumes `newspaper_resolved.player_game_box_score` rows whose
materialization status is `schema_review_required`, fills safe derived fields
such as PAT/FG attempts and total touchdowns, and records any remaining
touchdown-type ambiguity. It writes only local D-drive artifacts and local
DuckDB review tables.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_schema_gap_patches"

STAT_FIELDS = [
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
]

OWN_TD_FIELDS = ["rushing_tds", "receiving_tds", "def_tds", "special_teams_tds"]

ITEM_FIELDS = [
    "resolved_schema_gap_patch_run_id",
    "resolved_atom_materialize_run_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "player_week",
    "NFL_player_id",
    "player_raw",
    "nfl_team",
    "opponent_nfl_team",
    "schema_resolution_status",
    "action_status",
    "risk_level",
    "original_stat_fields_json",
    "patch_fields_json",
    "resolved_stat_fields_json",
    "reason",
    "source_row_text",
    "source_documents_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "resolved_schema_gap_patch_run_id",
    "resolved_atom_materialize_run_id",
    "output_dir",
    "input_row_count",
    "patched_row_count",
    "ready_count",
    "touchdown_type_review_count",
    "hold_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]


NUMBER_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
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


def parse_int(value: Any) -> int | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


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


def load_schema_gap_rows(con: duckdb.DuckDBPyConnection, resolved_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_resolved.player_game_box_score
        WHERE resolved_atom_materialize_run_id = ?
          AND materialization_status = 'schema_review_required'
        ORDER BY boxscore_id, player_raw, target_entity_key
        """,
        [resolved_run_id],
    )


def stat_dict(row: dict[str, Any]) -> dict[str, str]:
    return {
        field: clean(row.get(field))
        for field in STAT_FIELDS
        if clean(row.get(field)).strip()
    }


def number_from_words(text: str) -> int | None:
    text = text.lower().replace("-", " ")
    if text.isdigit():
        return int(text)
    total = 0
    found = False
    for token in text.split():
        if token not in NUMBER_WORDS:
            return None
        total += NUMBER_WORDS[token]
        found = True
    return total if found else None


def extract_fg_long(text: str) -> int | None:
    lower = text.lower()
    patterns = [
        r"from (?:the )?([a-z]+(?:-[a-z]+)?|\d+)[ -]?yard",
        r"([a-z]+(?:-[a-z]+)?|\d+)[ -]?yard (?:line|mark)",
    ]
    values: list[int] = []
    for pattern in patterns:
        for match in re.finditer(pattern, lower):
            value = number_from_words(match.group(1))
            if value is not None and 0 < value < 100:
                values.append(value)
    return max(values) if values else None


def touchdown_named_in_text(row: dict[str, Any]) -> bool:
    text = clean(row.get("source_row_text")).lower()
    player = clean(row.get("player_raw")).lower()
    if not text or not player:
        return False
    last = player.split()[-1]
    if re.search(rf"touchdowns?\s*:\s*{re.escape(last)}\b", text):
        return True
    if re.search(rf"\b{re.escape(last)}\b[^.]*\bscored\b[^.]*\btouchdowns?\b", text):
        return True
    return False


def resolve_row(row: dict[str, Any]) -> tuple[str, dict[str, str], dict[str, str], str]:
    original = stat_dict(row)
    resolved = dict(original)
    patch: dict[str, str] = {}
    reasons: list[str] = []

    pat_made = parse_int(resolved.get("pat_made"))
    pat_att = parse_int(resolved.get("pat_att"))
    if pat_made is not None and pat_made >= 0 and pat_att is None:
        patch["pat_att"] = str(pat_made)
        resolved["pat_att"] = str(pat_made)
        reasons.append("filled_pat_att_from_pat_made")

    fg_made = parse_int(resolved.get("fg_made"))
    fg_att = parse_int(resolved.get("fg_att"))
    if fg_made is not None and fg_made >= 0 and fg_att is None:
        patch["fg_att"] = str(fg_made)
        resolved["fg_att"] = str(fg_made)
        reasons.append("filled_fg_att_from_fg_made")

    fg_long = parse_int(resolved.get("fg_long"))
    inferred_long = extract_fg_long(clean(row.get("source_row_text")))
    if fg_long is None and fg_made and inferred_long:
        patch["fg_long"] = str(inferred_long)
        resolved["fg_long"] = str(inferred_long)
        reasons.append("filled_fg_long_from_text")

    own_td_total = sum(parse_int(resolved.get(field)) or 0 for field in OWN_TD_FIELDS)
    touchdowns = parse_int(resolved.get("touchdowns"))
    if touchdowns is None and own_td_total > 0:
        patch["touchdowns"] = str(own_td_total)
        resolved["touchdowns"] = str(own_td_total)
        touchdowns = own_td_total
        reasons.append("filled_total_touchdowns_from_split_td_fields")
    elif touchdowns is None and touchdown_named_in_text(row):
        patch["touchdowns"] = "1"
        resolved["touchdowns"] = "1"
        touchdowns = 1
        reasons.append("filled_total_touchdowns_from_touchdowns_scoring_text")

    untyped_touchdowns = bool(touchdowns and own_td_total == 0)
    if untyped_touchdowns:
        reasons.append("touchdown_total_preserved_but_type_split_needed")

    if not clean(row.get("NFL_player_id")) or not clean(row.get("player_week")):
        status = "hold_needs_identity"
    elif untyped_touchdowns:
        status = "ready_with_touchdown_total_needs_type_split"
    elif resolved:
        status = "ready_existing_or_inferred_stat_fields"
    else:
        status = "hold_no_structured_stat_fields"
    return status, patch, resolved, ",".join(reasons) if reasons else "existing_structured_fields_are_usable"


def build_items(run_id: str, resolved_run_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = iso_now()
    items: list[dict[str, Any]] = []
    for row in rows:
        status, patch, resolved, reason = resolve_row(row)
        items.append({
            "resolved_schema_gap_patch_run_id": run_id,
            "resolved_atom_materialize_run_id": resolved_run_id,
            "target_table": "player_game_box_score",
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "player_week": clean(row.get("player_week")),
            "NFL_player_id": clean(row.get("NFL_player_id")),
            "player_raw": clean(row.get("player_raw")),
            "nfl_team": clean(row.get("nfl_team")),
            "opponent_nfl_team": clean(row.get("opponent_nfl_team")),
            "schema_resolution_status": status,
            "action_status": "ready_for_resolved_review" if status.startswith("ready_") else "hold",
            "risk_level": "medium" if "touchdown_total" in status else "low" if status.startswith("ready_") else "high",
            "original_stat_fields_json": compact_json(stat_dict(row)),
            "patch_fields_json": compact_json(patch),
            "resolved_stat_fields_json": compact_json(resolved),
            "reason": reason,
            "source_row_text": clean(row.get("source_row_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "created_at_utc": created_at,
        })
    return items


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    item_defs = ", ".join(f"{field} VARCHAR" for field in ITEM_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.resolved_schema_gap_patch_item ({item_defs})")
    existing = {
        row[0]
        for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='resolved_schema_gap_patch_item'
            """
        ).fetchall()
    }
    for field in ITEM_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.resolved_schema_gap_patch_item ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.resolved_schema_gap_patch_run (
          resolved_schema_gap_patch_run_id VARCHAR,
          resolved_atom_materialize_run_id VARCHAR,
          output_dir VARCHAR,
          input_row_count INTEGER,
          patched_row_count INTEGER,
          ready_count INTEGER,
          touchdown_type_review_count INTEGER,
          hold_count INTEGER,
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
        run_id = clean(run_row.get("resolved_schema_gap_patch_run_id"))
        con.execute(
            "DELETE FROM newspaper_review.resolved_schema_gap_patch_run WHERE resolved_schema_gap_patch_run_id = ?",
            [run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_schema_gap_patch_item WHERE resolved_schema_gap_patch_run_id = ?",
            [run_id],
        )
        insert_rows(con, "newspaper_review.resolved_schema_gap_patch_run", [run_row], RUN_FIELDS)
        insert_rows(con, "newspaper_review.resolved_schema_gap_patch_item", item_rows, ITEM_FIELDS)
    finally:
        con.close()


def render_markdown(summary: dict[str, Any], item_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Resolved Schema Gap Patches",
        "",
        f"- Created: `{summary['created_at_utc']}`",
        f"- Resolved atom run: `{summary['resolved_atom_materialize_run_id']}`",
        f"- Input rows: `{summary['input_row_count']}`",
        f"- Patched rows: `{summary['patched_row_count']}`",
        f"- Status counts: `{summary['schema_resolution_status_counts']}`",
        "",
        "## Samples",
        "",
        "| Status | Boxscore | Player | Patch | Reason | Text |",
        "|---|---|---|---|---|---|",
    ]
    for row in item_rows[:20]:
        text = clean(row.get("source_row_text")).replace("\n", " ")
        if len(text) > 120:
            text = text[:117] + "..."
        lines.append(
            f"| `{row['schema_resolution_status']}` | `{row['boxscore_id']}` | `{row['player_raw']}` | "
            f"`{row['patch_fields_json']}` | `{row['reason']}` | {text} |"
        )
    lines.extend([
        "",
        "## Files",
        "",
        f"- Items CSV: `{summary['items_csv']}`",
        f"- Ready CSV: `{summary['ready_csv']}`",
        f"- Holds CSV: `{summary['holds_csv']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--resolved-atom-materialize-run-id", default="")
    parser.add_argument("--label", default="resolved_schema_gap_patches")
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
        rows = load_schema_gap_rows(con, resolved_run_id)
    finally:
        con.close()

    item_rows = build_items(run_id, resolved_run_id, rows)
    status_counts = Counter(row["schema_resolution_status"] for row in item_rows)
    patched_count = sum(1 for row in item_rows if clean(row.get("patch_fields_json")) != "{}")
    ready_count = sum(1 for row in item_rows if clean(row.get("action_status")) == "ready_for_resolved_review")
    touchdown_type_review_count = sum(1 for row in item_rows if "touchdown_total" in clean(row.get("schema_resolution_status")))
    hold_count = sum(1 for row in item_rows if clean(row.get("action_status")) == "hold")

    items_csv = out_dir / "resolved_schema_gap_patch_items.csv"
    ready_csv = out_dir / "ready_schema_gap_patch_items.csv"
    holds_csv = out_dir / "held_schema_gap_patch_items.csv"
    summary_json = out_dir / "summary.json"
    markdown = out_dir / "resolved_schema_gap_patch_report.md"
    summary = {
        "created_at_utc": created_at,
        "resolved_schema_gap_patch_run_id": run_id,
        "resolved_atom_materialize_run_id": resolved_run_id,
        "resolved_atom_run_dir": str(latest_run_dir(args.root, resolved_run_id) or ""),
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "patched_row_count": patched_count,
        "ready_count": ready_count,
        "touchdown_type_review_count": touchdown_type_review_count,
        "hold_count": hold_count,
        "schema_resolution_status_counts": dict(sorted(status_counts.items())),
        "items_csv": str(items_csv),
        "ready_csv": str(ready_csv),
        "holds_csv": str(holds_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown),
        "persisted_to_duckdb": not args.no_persist,
    }
    write_csv(items_csv, item_rows, ITEM_FIELDS)
    write_csv(ready_csv, [row for row in item_rows if row["action_status"] == "ready_for_resolved_review"], ITEM_FIELDS)
    write_csv(holds_csv, [row for row in item_rows if row["action_status"] == "hold"], ITEM_FIELDS)
    write_json(summary_json, summary)
    markdown.write_text(render_markdown(summary, item_rows), encoding="utf-8")

    if not args.no_persist:
        persist(
            args.db_path,
            {
                "resolved_schema_gap_patch_run_id": run_id,
                "resolved_atom_materialize_run_id": resolved_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "patched_row_count": patched_count,
                "ready_count": ready_count,
                "touchdown_type_review_count": touchdown_type_review_count,
                "hold_count": hold_count,
                "status": "complete",
                "created_at_utc": created_at,
                "summary_json_path": str(summary_json),
            },
            item_rows,
        )

    print(json.dumps({
        "resolved_schema_gap_patch_run_id": run_id,
        "resolved_atom_materialize_run_id": resolved_run_id,
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "patched_row_count": patched_count,
        "ready_count": ready_count,
        "touchdown_type_review_count": touchdown_type_review_count,
        "hold_count": hold_count,
        "schema_resolution_status_counts": dict(sorted(status_counts.items())),
        "markdown": str(markdown),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
