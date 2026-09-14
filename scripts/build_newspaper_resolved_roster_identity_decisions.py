#!/usr/bin/env python
"""Approve identity-review rows backed by roster/player-index evidence.

This station handles the rare early-NFL rows where the newspaper atom has a
surname or OCR/nickname variant, while the stable player ID lives in the local
PFR player index. It is deliberately explicit: only configured package IDs are
patched, and every patch validates against the D-drive PFR index before it can
produce a decision override.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "resolved_roster_identity_decisions"
DEFAULT_PLAYER_INDEX = Path(r"D:\league-history-data\nfl\raw\pfr\players\player_index.parquet")
DEFAULT_V26 = Path(
    r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26"
    r"\tables\nfl_player_stats_all.parquet"
)

APPROVED_FIELDS = [
    "package_item_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "proposed_fields_json",
    "notes",
]

AUDIT_FIELDS = [
    "roster_identity_decision_run_id",
    "resolved_package_decision_run_id",
    "package_item_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "route_to_lane",
    "decision_status",
    "decision_value",
    "approval_class",
    "reason",
    "canonical_player",
    "NFL_player_id",
    "pfr_url",
    "support_details_json",
    "evidence_text",
    "source_documents_json",
    "original_proposed_fields_json",
    "patched_proposed_fields_json",
    "created_at_utc",
]

RUN_FIELDS = [
    "roster_identity_decision_run_id",
    "resolved_package_decision_run_id",
    "output_dir",
    "input_row_count",
    "approved_count",
    "held_count",
    "approved_target_counts_json",
    "approved_class_counts_json",
    "hold_reason_counts_json",
    "approved_csv",
    "held_csv",
    "audit_csv",
    "summary_json",
    "persisted_to_duckdb",
    "created_at_utc",
]

IDENTITY_PATCHES = {
    "7c088bbc6479e257bcd06a7c32c53542": {
        "approval_class": "ocr_variant_roster_identity",
        "NFL_player_id": "WagnBu20",
        "canonical_player": "Buff Wagner",
        "raw_variants": ["hutt wagner", "buff wagner", "wagner"],
        "reason": "source Hutt/Buff Wagner variant maps to Buff Wagner in the local PFR index",
        "roster_source_url": "https://www.nfl.com/teams/green-bay-packers/roster/1921/REG",
        "field_patches": {
            "secondary_player_raw": "Buff Wagner",
            "secondary_NFL_player_id": "WagnBu20",
            "review_status": "roster_identity_supported",
        },
    },
    "27f546297600f34269aacf085f9dbcf8": {
        "approval_class": "roster_spelling_variant_identity",
        "NFL_player_id": "GardFr20",
        "canonical_player": "Frank Garden",
        "raw_variants": ["gardner", "garden"],
        "reason": "Rock Island scorer Gardner maps to canonical Frank Garden in the local PFR index",
        "roster_source_url": "https://www.nfl.com/teams/rock-island-independents/roster/1920/REG",
        "field_patches": {
            "scoring_NFL_player_id": "GardFr20",
            "receiver_NFL_player_id": "GardFr20",
            "review_status": "roster_identity_supported",
        },
    },
    "b2132d4b850743e13e0ab0785d53f510": {
        "approval_class": "v26_stat_identity_match",
        "NFL_player_id": "RoesFr20",
        "canonical_player": "Fritz Roeseler",
        "raw_variants": ["roessler", "roeseler"],
        "reason": "Racine Roessler maps to Fritz Roeseler, with a matching 1922 Racine receiving TD in v26",
        "roster_source_url": "https://www.nfl.com/teams/racine-legion/roster/1922/REG",
        "require_v26_team_year": True,
        "field_patches": {
            "scoring_NFL_player_id": "RoesFr20",
            "receiver_NFL_player_id": "RoesFr20",
            "review_status": "roster_identity_supported_v26_stat_match",
        },
    },
    "3244340fbe75c3663ba1bdeeaebe791f": {
        "approval_class": "roster_spelling_variant_identity",
        "NFL_player_id": "MathBa20",
        "canonical_player": "Barney Mathews",
        "raw_variants": ["matthews", "mathews", "barney mathews"],
        "reason": "Racine Matthews maps to Barney Mathews in the local PFR index",
        "roster_source_url": "https://www.nfl.com/teams/racine-tornadoes/roster/1926/REG",
        "field_patches": {
            "scoring_NFL_player_id": "MathBa20",
            "receiver_NFL_player_id": "MathBa20",
            "review_status": "roster_identity_supported",
        },
    },
    "8702c60c60829ed8df3791e001ec5fe1": {
        "approval_class": "v26_stat_identity_match",
        "NFL_player_id": "MacDMi20",
        "canonical_player": "Mickey MacDonnell",
        "raw_variants": ["mcdonald", "macdonald", "macdonnell", "mickey macdonnell"],
        "reason": "Cardinals McDonald/MacDonnell variant maps to Mickey MacDonnell, with a matching 1926 Cardinals receiving TD in v26",
        "roster_source_url": "https://www.nfl.com/teams/chicago-cardinals/roster/1926/REG",
        "require_v26_team_year": True,
        "field_patches": {
            "scoring_NFL_player_id": "MacDMi20",
            "receiver_NFL_player_id": "MacDMi20",
            "review_status": "roster_identity_supported_v26_stat_match",
        },
    },
}


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def norm_words(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean(value).lower()).strip()


def parse_obj(value: Any) -> dict[str, Any]:
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
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
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")


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


def load_rows(con: duckdb.DuckDBPyConnection, decision_run_id: str) -> list[dict[str, Any]]:
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.resolved_package_decision_route_queue
        WHERE resolved_package_decision_run_id = ?
          AND route_to_lane = 'identity_review'
          AND target_table IN ('scoring_event', 'play_by_play_event')
        ORDER BY target_table, boxscore_id, package_item_id
        """,
        [decision_run_id],
    )


def load_player_index(con: duckdb.DuckDBPyConnection, path: Path, ids: list[str]) -> dict[str, dict[str, Any]]:
    if not ids:
        return {}
    path_text = str(path).replace("\\", "/")
    placeholders = ", ".join("?" for _ in ids)
    rows = query_dicts(
        con,
        f"""
        SELECT pfr_id, player, url, index_position, first_year, last_year, raw_text
        FROM read_parquet('{path_text}')
        WHERE pfr_id IN ({placeholders})
        """,
        ids,
    )
    return {clean(row.get("pfr_id")): row for row in rows}


def load_v26_rows(con: duckdb.DuckDBPyConnection, path: Path, ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not ids or not path.exists():
        return {}
    path_text = str(path).replace("\\", "/")
    placeholders = ", ".join("?" for _ in ids)
    rows = query_dicts(
        con,
        f"""
        SELECT year, week, NFL_player_id, player, nfl_team, opponent_nfl_team, position,
               receiving_tds, passing_tds, rushing_tds, player_week
        FROM read_parquet('{path_text}')
        WHERE NFL_player_id IN ({placeholders})
          AND year BETWEEN 1920 AND 1926
        ORDER BY NFL_player_id, year, week
        """,
        ids,
    )
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        output.setdefault(clean(row.get("NFL_player_id")), []).append(row)
    return output


def year_from_boxscore(boxscore_id: str) -> int | None:
    text = clean(boxscore_id)
    if len(text) < 4 or not text[:4].isdigit():
        return None
    return int(text[:4])


def source_blob(row: dict[str, Any], proposed: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            clean(row.get("target_entity_key")),
            clean(row.get("evidence_text")),
            clean(proposed.get("play_text")),
            clean(proposed.get("scoring_player_raw")),
            clean(proposed.get("receiver_raw")),
            clean(proposed.get("secondary_player_raw")),
        ]
        if part
    )


def matching_v26_team_year(v26_rows: list[dict[str, Any]], year: int | None, team: str) -> list[dict[str, Any]]:
    if year is None:
        return []
    team = clean(team)
    return [
        row
        for row in v26_rows
        if int(float(clean(row.get("year")) or 0)) == year
        and (not team or clean(row.get("nfl_team")) == team)
    ]


def build_patch(
    row: dict[str, Any],
    spec: dict[str, Any],
    proposed: dict[str, Any],
    pfr_row: dict[str, Any] | None,
    v26_rows: list[dict[str, Any]],
) -> tuple[bool, str, dict[str, Any], dict[str, Any]]:
    if not pfr_row:
        return False, "configured player ID missing from local PFR index", {}, {}

    year = year_from_boxscore(clean(row.get("boxscore_id")))
    first_year = int(float(clean(pfr_row.get("first_year")) or 0))
    last_year = int(float(clean(pfr_row.get("last_year")) or 0))
    if year is None or not (first_year <= year <= last_year):
        return False, "configured player active-year window does not cover event year", {}, {}

    blob = norm_words(source_blob(row, proposed))
    variants = [norm_words(item) for item in spec.get("raw_variants", [])]
    if variants and not any(variant and variant in blob for variant in variants):
        return False, "source text does not contain configured raw-name variant", {}, {}

    team = clean(proposed.get("scoring_team")) or clean(proposed.get("possession_team"))
    v26_matches = matching_v26_team_year(v26_rows, year, team)
    if spec.get("require_v26_team_year") and not v26_matches:
        return False, "required v26 team-year corroboration missing", {}, {}

    patched = dict(proposed)
    patched.update(spec.get("field_patches", {}))
    patched["identity_resolution_source"] = "roster_identity_decision"
    patched["identity_resolution_player"] = spec["canonical_player"]
    patched["identity_resolution_pfr_id"] = spec["NFL_player_id"]
    details = {
        "pfr_index_row": pfr_row,
        "v26_team_year_matches": v26_matches,
        "roster_source_url": spec.get("roster_source_url", ""),
        "raw_variants": spec.get("raw_variants", []),
        "event_year": year,
        "event_team": team,
    }
    return True, spec["reason"], patched, details


def build_rows(
    rows: list[dict[str, Any]],
    run_id: str,
    decision_run_id: str,
    pfr_by_id: dict[str, dict[str, Any]],
    v26_by_id: dict[str, list[dict[str, Any]]],
    created_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    approved = []
    held = []
    audit_rows = []
    for row in rows:
        package_item_id = clean(row.get("package_item_id"))
        proposed = parse_obj(row.get("proposed_fields_json"))
        spec = IDENTITY_PATCHES.get(package_item_id)
        original_json = json.dumps(proposed, sort_keys=True, ensure_ascii=True)
        if spec:
            pfr_id = clean(spec.get("NFL_player_id"))
            ok, reason, patched, details = build_patch(
                row,
                spec,
                proposed,
                pfr_by_id.get(pfr_id),
                v26_by_id.get(pfr_id, []),
            )
            approval_class = clean(spec.get("approval_class"))
        else:
            pfr_id = ""
            ok = False
            reason = "no roster identity rule for routed row"
            patched = proposed
            details = {}
            approval_class = "held_roster_identity"

        patched_json = json.dumps(patched, sort_keys=True, ensure_ascii=True)
        audit = {
            "roster_identity_decision_run_id": run_id,
            "resolved_package_decision_run_id": decision_run_id,
            "package_item_id": package_item_id,
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "decision_status": "approved" if ok else "held",
            "decision_value": "approved_for_local_newspaper_final" if ok else "needs_identity",
            "approval_class": approval_class if ok else "held_roster_identity",
            "reason": reason,
            "canonical_player": clean(spec.get("canonical_player")) if spec else "",
            "NFL_player_id": pfr_id,
            "pfr_url": clean((pfr_by_id.get(pfr_id) or {}).get("url")),
            "support_details_json": json.dumps(details, sort_keys=True, ensure_ascii=True),
            "evidence_text": clean(row.get("evidence_text")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "original_proposed_fields_json": original_json,
            "patched_proposed_fields_json": patched_json,
            "created_at_utc": created_at,
        }
        audit_rows.append(audit)
        if ok:
            approved.append(
                {
                    "package_item_id": package_item_id,
                    "decision_status": "approved",
                    "decision_value": "approved_for_local_newspaper_final",
                    "route_to_lane": "local_promotion",
                    "target_table": clean(row.get("target_table")),
                    "target_entity_key": clean(row.get("target_entity_key")),
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "proposed_fields_json": patched_json,
                    "notes": f"{approval_class}: {reason}",
                }
            )
        else:
            held.append(audit)
    return approved, held, audit_rows


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    con.execute(
        "CREATE TABLE IF NOT EXISTS newspaper_review.resolved_roster_identity_decision_run ("
        + ", ".join(f"{field} VARCHAR" for field in RUN_FIELDS)
        + ")"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS newspaper_review.resolved_roster_identity_decision_item ("
        + ", ".join(f"{field} VARCHAR" for field in AUDIT_FIELDS)
        + ")"
    )
    for table, fields in [
        ("resolved_roster_identity_decision_run", RUN_FIELDS),
        ("resolved_roster_identity_decision_item", AUDIT_FIELDS),
    ]:
        existing = {
            row[1]
            for row in con.execute(f"PRAGMA table_info('newspaper_review.{table}')").fetchall()
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


def persist(db_path: Path, run_row: dict[str, Any], item_rows: list[dict[str, Any]]) -> None:
    con = duckdb.connect(str(db_path))
    try:
        ensure_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.resolved_roster_identity_decision_run WHERE roster_identity_decision_run_id = ?",
            [run_row["roster_identity_decision_run_id"]],
        )
        con.execute(
            "DELETE FROM newspaper_review.resolved_roster_identity_decision_item WHERE roster_identity_decision_run_id = ?",
            [run_row["roster_identity_decision_run_id"]],
        )
        insert_rows(con, "resolved_roster_identity_decision_run", [run_row], RUN_FIELDS)
        insert_rows(con, "resolved_roster_identity_decision_item", item_rows, AUDIT_FIELDS)
    finally:
        con.close()


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


def render_markdown(summary: dict[str, Any], audit_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Resolved Roster Identity Decisions",
        "",
        f"- Run: `{summary['roster_identity_decision_run_id']}`",
        f"- Decision ledger: `{summary['resolved_package_decision_run_id']}`",
        f"- Approved: `{summary['approved_count']}`",
        f"- Held: `{summary['held_count']}`",
        f"- Persisted to DuckDB: `{summary['persisted_to_duckdb']}`",
        "",
        "## Decisions",
        "",
    ]
    lines.extend(
        markdown_table(
            audit_rows,
            [
                "decision_status",
                "approval_class",
                "package_item_id",
                "boxscore_id",
                "canonical_player",
                "NFL_player_id",
                "reason",
            ],
        )
    )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolved-package-decision-run-id", default="")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--player-index-path", type=Path, default=DEFAULT_PLAYER_INDEX)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="1920_1939_roster_identity_decisions_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configured_ids = sorted({clean(spec["NFL_player_id"]) for spec in IDENTITY_PATCHES.values()})
    read_con = duckdb.connect(str(args.db_path), read_only=True)
    aux_con = duckdb.connect()
    try:
        decision_run_id = clean(args.resolved_package_decision_run_id) or latest_decision_run(read_con)
        rows = load_rows(read_con, decision_run_id)
        pfr_by_id = load_player_index(aux_con, args.player_index_path, configured_ids)
        v26_by_id = load_v26_rows(aux_con, args.v26_path, configured_ids)
    finally:
        read_con.close()
        aux_con.close()

    run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    approved, held, audit_rows = build_rows(rows, run_id, decision_run_id, pfr_by_id, v26_by_id, created_at)
    approved_target_counts = Counter(row["target_table"] for row in approved)
    approved_class_counts = Counter(row["approval_class"] for row in audit_rows if row["decision_status"] == "approved")
    hold_reason_counts = Counter(row["reason"] for row in held)

    approved_csv = out_dir / "approved_decision_overrides.csv"
    held_csv = out_dir / "held_roster_identity_rows.csv"
    audit_csv = out_dir / "roster_identity_decision_audit.csv"
    summary_json = out_dir / "summary.json"
    report_md = out_dir / "roster_identity_decision_report.md"

    summary = {
        "roster_identity_decision_run_id": run_id,
        "resolved_package_decision_run_id": decision_run_id,
        "created_at_utc": created_at,
        "db_path": str(args.db_path),
        "player_index_path": str(args.player_index_path),
        "v26_path": str(args.v26_path),
        "output_dir": str(out_dir),
        "input_row_count": len(rows),
        "approved_count": len(approved),
        "held_count": len(held),
        "approved_target_counts": dict(approved_target_counts),
        "approved_class_counts": dict(approved_class_counts),
        "hold_reason_counts": dict(hold_reason_counts),
        "approved_csv": str(approved_csv),
        "held_csv": str(held_csv),
        "audit_csv": str(audit_csv),
        "summary_json": str(summary_json),
        "report_md": str(report_md),
        "persisted_to_duckdb": not args.dry_run,
        "dry_run": bool(args.dry_run),
    }

    write_csv(approved_csv, approved, APPROVED_FIELDS)
    write_csv(held_csv, held, AUDIT_FIELDS)
    write_csv(audit_csv, audit_rows, AUDIT_FIELDS)
    write_json(summary_json, summary)
    report_md.write_text(render_markdown(summary, audit_rows), encoding="utf-8")

    if not args.dry_run:
        persist(
            args.db_path,
            {
                "roster_identity_decision_run_id": run_id,
                "resolved_package_decision_run_id": decision_run_id,
                "output_dir": str(out_dir),
                "input_row_count": len(rows),
                "approved_count": len(approved),
                "held_count": len(held),
                "approved_target_counts_json": json.dumps(dict(approved_target_counts), sort_keys=True),
                "approved_class_counts_json": json.dumps(dict(approved_class_counts), sort_keys=True),
                "hold_reason_counts_json": json.dumps(dict(hold_reason_counts), sort_keys=True),
                "approved_csv": str(approved_csv),
                "held_csv": str(held_csv),
                "audit_csv": str(audit_csv),
                "summary_json": str(summary_json),
                "persisted_to_duckdb": True,
                "created_at_utc": created_at,
            },
            audit_rows,
        )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
