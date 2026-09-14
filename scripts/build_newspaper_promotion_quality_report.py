#!/usr/bin/env python
"""Build a quality/novelty report for locally promoted newspaper atoms.

The report is intentionally read-only against the local newspaper DuckDB and
the local v26 supertable parquet. It answers two recurring questions:

- Are the promoted atoms structurally complete enough to trust?
- Which promoted facts add information beyond the v26 player stat table?
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "promotion_quality_reports"
DEFAULT_V26 = Path(
    r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet"
)

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

KEY_FIELDS = {
    "game_candidate": [
        "boxscore_id",
        "game_date",
        "team_1_resolved",
        "team_2_resolved",
        "team_1_score",
        "team_2_score",
        "source_documents_json",
    ],
    "lineup_participation": [
        "boxscore_id",
        "player_week",
        "NFL_player_id",
        "nfl_team",
        "opponent_nfl_team",
        "is_starter",
        "source_documents_json",
    ],
    "play_by_play_event": [
        "boxscore_id",
        "play_type",
        "play_text",
        "source_documents_json",
    ],
    "player_game_box_score": [
        "boxscore_id",
        "player_week",
        "NFL_player_id",
        "nfl_team",
        "opponent_nfl_team",
        "source_documents_json",
    ],
    "player_game_stat_claim": [
        "boxscore_id",
        "player_raw",
        "stat_name",
        "stat_value",
        "source_row_text",
        "source_documents_json",
    ],
    "player_game_note": [
        "boxscore_id",
        "player_raw",
        "note_type",
        "note_text",
        "source_row_text",
        "source_documents_json",
    ],
    "team_game_stat_claim": [
        "boxscore_id",
        "stat_name",
        "team_1_nfl_team",
        "team_1_value",
        "team_2_nfl_team",
        "team_2_value",
        "source_documents_json",
    ],
    "source_document_note": [
        "source_document_id",
        "boxscore_id",
        "note_type",
        "note_text",
        "reconciliation_status",
        "source_documents_json",
    ],
    "player_identity_candidate": [
        "boxscore_id",
        "player_week",
        "NFL_player_id",
        "nfl_team",
        "raw_player_name",
        "resolved_player",
        "confidence_score",
        "source_documents_json",
    ],
    "scoring_event": [
        "boxscore_id",
        "scoring_team",
        "event_type",
        "points",
        "play_text",
        "source_documents_json",
    ],
}

STRUCTURAL_FIELDS = {
    "game_candidate": [
        "atom_claim_id",
        "reconciliation_status",
        "evidence_text",
        "source_documents_json",
    ],
    "lineup_participation": [
        "is_starter",
        "starter_position",
        "participation_type",
        "source_row_text",
        "source_documents_json",
    ],
    "play_by_play_event": [
        "event_order",
        "period_raw",
        "clock_raw",
        "possession_team",
        "down_raw",
        "distance_raw",
        "yardline_raw",
        "play_type",
        "play_text",
        "source_documents_json",
    ],
    "player_game_box_score": [
        "source_row_text",
        "source_documents_json",
    ],
    "player_game_stat_claim": [
        "stat_name",
        "stat_value",
        "stat_unit",
        "stat_fields_json",
        "stat_context",
        "match_method",
        "identity_status",
        "source_row_text",
        "source_documents_json",
    ],
    "player_game_note": [
        "note_type",
        "note_text",
        "note_fields_json",
        "match_method",
        "identity_status",
        "source_row_text",
        "source_documents_json",
    ],
    "team_game_stat_claim": [
        "stat_name",
        "stat_unit",
        "team_1_nfl_team",
        "team_1_value",
        "team_2_nfl_team",
        "team_2_value",
        "reconciliation_status",
        "stat_context",
        "source_row_text",
        "evidence_text",
        "source_documents_json",
    ],
    "source_document_note": [
        "note_type",
        "note_category",
        "note_text",
        "related_target_table",
        "reconciliation_status",
        "evidence_text",
        "source_documents_json",
    ],
    "player_identity_candidate": [
        "raw_player_name",
        "raw_team",
        "resolved_player",
        "match_method",
        "review_status",
        "evidence_text",
        "source_documents_json",
    ],
    "scoring_event": [
        "event_order",
        "period_raw",
        "clock_raw",
        "scoring_team",
        "scoring_player_raw",
        "event_type",
        "distance_yards",
        "play_text",
        "source_documents_json",
    ],
}

IDENTITY_FIELDS = {
    "lineup_participation": ["NFL_player_id", "starter_position", "is_starter"],
    "play_by_play_event": ["primary_NFL_player_id", "secondary_NFL_player_id", "yards", "points"],
    "player_game_box_score": ["NFL_player_id"],
    "player_game_stat_claim": ["NFL_player_id", "player_week", "nfl_team", "identity_status"],
    "player_game_note": ["NFL_player_id", "player_week", "nfl_team", "identity_status"],
    "team_game_stat_claim": ["team_1_nfl_team", "team_1_value", "team_2_nfl_team", "team_2_value", "reconciliation_status"],
    "source_document_note": ["source_document_id", "note_type", "reconciliation_status"],
    "player_identity_candidate": ["NFL_player_id", "player_week", "nfl_team", "confidence_score"],
    "scoring_event": [
        "scoring_NFL_player_id",
        "passer_NFL_player_id",
        "receiver_NFL_player_id",
        "distance_yards",
    ],
}

PLAYER_STAT_MAPPINGS = {
    "carries": ["carries", "rush_att", "rushing_att", "rush_attempts", "rushing_attempts"],
    "rushing_yards": ["rushing_yards", "rush_yds", "rushing_yds"],
    "rushing_tds": ["rushing_tds", "rush_td", "rush_tds", "rushing_td"],
    "attempts": ["attempts", "pass_att", "passing_att", "passing_attempts"],
    "completions": ["completions", "pass_cmp", "passing_cmp", "passing_completions"],
    "passing_yards": ["passing_yards", "pass_yds", "passing_yds"],
    "passing_tds": ["passing_tds", "pass_td", "pass_tds", "passing_td"],
    "passing_interceptions": ["passing_interceptions", "pass_int", "passing_int"],
    "receptions": ["receptions", "rec", "receiving_rec"],
    "receiving_yards": ["receiving_yards", "rec_yds", "receiving_yds"],
    "receiving_tds": ["receiving_tds", "rec_td", "rec_tds", "receiving_td"],
    "pat_made": ["pat_made", "xpm", "extra_points_made"],
    "pat_att": ["pat_att", "xpa", "extra_points_attempted"],
    "fg_made": ["fg_made", "fgm", "field_goals_made"],
    "fg_att": ["fg_att", "fga", "field_goals_attempted"],
    "fg_long": ["fg_long", "long_fg"],
    "def_interceptions": ["def_interceptions", "def_int", "defensive_interceptions"],
    "def_sacks": ["def_sacks", "sacks"],
    "def_tds": ["def_tds", "def_td", "defensive_tds"],
    "special_teams_tds": ["special_teams_tds", "return_tds", "ret_td"],
}


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return str(value)


def parse_num(value: Any) -> float | None:
    text = clean(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return bool(row)


def table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    if not table_exists(con, schema, table):
        return []
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [clean(row[0]) for row in rows]


def latest_apply_run(con: duckdb.DuckDBPyConnection) -> str:
    if not table_exists(con, "newspaper_review", "review_decision_apply_run"):
        return ""
    row = con.execute(
        """
        SELECT promotion_apply_run_id
        FROM newspaper_review.review_decision_apply_run
        ORDER BY created_at_utc DESC, promotion_apply_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def fetch_promoted_rows(
    con: duckdb.DuckDBPyConnection, table: str, apply_run_id: str
) -> list[dict[str, Any]]:
    cols = table_columns(con, "newspaper_promoted", table)
    if not cols:
        return []
    where = ""
    params: list[Any] = []
    if apply_run_id and "promotion_apply_run_id" in cols:
        where = "WHERE promotion_apply_run_id = ?"
        params.append(apply_run_id)
    result = con.execute(
        f"SELECT * FROM newspaper_promoted.{table} {where} ORDER BY decision_id", params
    )
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def v26_columns(con: duckdb.DuckDBPyConnection, v26_path: Path) -> list[str]:
    if not v26_path.exists():
        return []
    result = con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(v26_path)])
    return [item[0] for item in result.description]


def choose_v26_mappings(v26_cols: set[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    lower_to_actual = {col.lower(): col for col in v26_cols}
    for promoted_col, candidates in PLAYER_STAT_MAPPINGS.items():
        for candidate in candidates:
            actual = lower_to_actual.get(candidate.lower())
            if actual:
                mapping[promoted_col] = actual
                break
    return mapping


def fetch_v26_player_rows(
    con: duckdb.DuckDBPyConnection,
    v26_path: Path,
    player_weeks: list[str],
    selected_cols: list[str],
) -> dict[str, dict[str, Any]]:
    if not player_weeks or not selected_cols or not v26_path.exists():
        return {}
    unique_weeks = sorted({week for week in player_weeks if week})
    if not unique_weeks:
        return {}
    placeholders = ",".join(["?"] * len(unique_weeks))
    quoted_cols = ", ".join([f'"{col}"' for col in selected_cols])
    query = (
        f"SELECT {quoted_cols} "
        "FROM read_parquet(?) "
        f"WHERE player_week IN ({placeholders})"
    )
    result = con.execute(query, [str(v26_path), *unique_weeks])
    fields = [item[0] for item in result.description]
    return {clean(row[fields.index("player_week")]): dict(zip(fields, row)) for row in result.fetchall()}


def summarize_fill(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, Any]:
    total = len(rows)
    out = {}
    for field in fields:
        filled = sum(1 for row in rows if clean(row.get(field)).strip())
        out[field] = {
            "filled": filled,
            "total": total,
            "pct": round((filled / total * 100.0), 1) if total else 0.0,
        }
    return out


def summarize_confidence(rows: list[dict[str, Any]]) -> dict[str, Any]:
    bars = Counter(clean(row.get("confidence_bar")).lower() or "unknown" for row in rows)
    scores: list[float] = []
    evidence_counts: list[float] = []
    for row in rows:
        score = parse_num(row.get("max_confidence_score")) or parse_num(row.get("confidence_score"))
        if score is not None:
            scores.append(score)
        evidence = parse_num(row.get("evidence_item_count"))
        if evidence is not None:
            evidence_counts.append(evidence)
    return {
        "confidence_bars": dict(sorted(bars.items())),
        "score_min": min(scores) if scores else None,
        "score_avg": round(sum(scores) / len(scores), 3) if scores else None,
        "score_max": max(scores) if scores else None,
        "evidence_item_count_min": min(evidence_counts) if evidence_counts else None,
        "evidence_item_count_avg": round(sum(evidence_counts) / len(evidence_counts), 2)
        if evidence_counts
        else None,
        "evidence_item_count_max": max(evidence_counts) if evidence_counts else None,
    }


def summarize_player_stat_overlap(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    v26_path: Path,
    v26_cols: set[str],
) -> dict[str, Any]:
    if not rows:
        return {
            "available": bool(v26_cols),
            "status_counts": {},
            "field_counts": {},
            "examples": [],
            "v26_stat_mapping": {},
        }
    mapping = choose_v26_mappings(v26_cols)
    selected_cols = ["player_week"]
    for optional in ["player", "NFL_player_id", "boxscore_id", "team"]:
        if optional in v26_cols:
            selected_cols.append(optional)
    for col in mapping.values():
        if col not in selected_cols:
            selected_cols.append(col)
    v26_by_week = fetch_v26_player_rows(
        con, v26_path, [clean(row.get("player_week")) for row in rows], selected_cols
    )

    status_counts: Counter = Counter()
    field_counts: dict[str, Counter] = defaultdict(Counter)
    examples: list[dict[str, Any]] = []

    for row in rows:
        player_week = clean(row.get("player_week"))
        v26_row = v26_by_week.get(player_week)
        row_stats: list[str] = []
        for promoted_col in PLAYER_STAT_MAPPINGS:
            promoted_num = parse_num(row.get(promoted_col))
            if promoted_num is None:
                continue
            v26_col = mapping.get(promoted_col)
            if not v26_col:
                status = "no_comparable_v26_column"
                v26_value = ""
            elif not v26_row:
                status = "no_v26_player_week_row"
                v26_value = ""
            else:
                v26_num = parse_num(v26_row.get(v26_col))
                v26_value = clean(v26_row.get(v26_col))
                if v26_num is None:
                    status = "v26_blank_or_non_numeric"
                elif abs(v26_num - promoted_num) < 0.00001:
                    status = "same_as_v26"
                else:
                    status = "differs_from_v26"
            status_counts[status] += 1
            field_counts[promoted_col][status] += 1
            row_stats.append(f"{promoted_col}={clean(row.get(promoted_col))}")
            if len(examples) < 12 and status in {
                "differs_from_v26",
                "v26_blank_or_non_numeric",
                "no_v26_player_week_row",
                "no_comparable_v26_column",
            }:
                examples.append(
                    {
                        "player_raw": clean(row.get("player_raw")),
                        "player_week": player_week,
                        "boxscore_id": clean(row.get("boxscore_id")),
                        "promoted_field": promoted_col,
                        "promoted_value": clean(row.get(promoted_col)),
                        "v26_field": v26_col or "",
                        "v26_value": v26_value,
                        "status": status,
                        "source_row_text": clean(row.get("source_row_text"))[:220],
                    }
                )
        if len(examples) < 12 and row_stats:
            examples.append(
                {
                    "player_raw": clean(row.get("player_raw")),
                    "player_week": player_week,
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "promoted_stats": "; ".join(row_stats),
                    "status": "sample_promoted_row",
                    "source_row_text": clean(row.get("source_row_text"))[:220],
                }
            )

    return {
        "available": bool(v26_cols),
        "status_counts": dict(status_counts),
        "field_counts": {field: dict(counts) for field, counts in sorted(field_counts.items())},
        "examples": examples,
        "v26_stat_mapping": mapping,
    }


def summarize_structural_novelty(v26_cols: set[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for table, fields in STRUCTURAL_FIELDS.items():
        missing = [field for field in fields if field not in v26_cols]
        present = [field for field in fields if field in v26_cols]
        out[table] = {
            "fields_checked": fields,
            "not_in_v26": missing,
            "present_in_v26": present,
        }
    return out


def summarize_identity_fill(rows_by_table: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for table, fields in IDENTITY_FIELDS.items():
        rows = rows_by_table.get(table, [])
        out[table] = summarize_fill(rows, fields)
    return out


def summarize_lineup_overlap(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    v26_path: Path,
    v26_cols: set[str],
) -> dict[str, Any]:
    if not rows or not v26_path.exists() or "player_week" not in v26_cols:
        return {"available": bool(v26_cols), "status_counts": {}, "examples": []}
    selected_cols = ["player_week"]
    for optional in ["player", "nfl_team", "starter_position", "is_starter"]:
        if optional in v26_cols:
            selected_cols.append(optional)
    player_weeks = sorted({clean(row.get("player_week")) for row in rows if clean(row.get("player_week"))})
    if not player_weeks:
        return {"available": True, "status_counts": {}, "examples": []}
    placeholders = ",".join(["?"] * len(player_weeks))
    quoted_cols = ", ".join([f'"{col}"' for col in selected_cols])
    result = con.execute(
        f"SELECT {quoted_cols} FROM read_parquet(?) WHERE player_week IN ({placeholders})",
        [str(v26_path), *player_weeks],
    )
    fields = [item[0] for item in result.description]
    v26_by_week = {
        clean(row[fields.index("player_week")]): dict(zip(fields, row)) for row in result.fetchall()
    }

    counts: Counter = Counter()
    examples: list[dict[str, Any]] = []
    for row in rows:
        player_week = clean(row.get("player_week"))
        v26_row = v26_by_week.get(player_week)
        starter_position = clean(row.get("starter_position")).strip()
        is_starter = clean(row.get("is_starter")).strip()
        if not v26_row:
            status = "no_v26_player_week_row"
        elif not starter_position and not is_starter:
            status = "no_starter_claim"
        else:
            v26_position = clean(v26_row.get("starter_position")).strip()
            v26_starter = clean(v26_row.get("is_starter")).strip()
            if not v26_position and not v26_starter:
                status = "v26_starter_blank"
            elif v26_position.upper() == starter_position.upper() or v26_starter == is_starter:
                status = "same_as_v26"
            else:
                status = "differs_from_v26"
        counts[status] += 1
        if len(examples) < 12 and status != "same_as_v26":
            examples.append(
                {
                    "player_raw": clean(row.get("player_raw")),
                    "player_week": player_week,
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "starter_position": starter_position,
                    "is_starter": is_starter,
                    "status": status,
                    "source_row_text": clean(row.get("source_row_text"))[:220],
                }
            )
    return {"available": True, "status_counts": dict(counts), "examples": examples}


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({field for row in rows for field in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Newspaper Promotion Quality Report",
        "",
        f"- Created: {summary['created_at_utc']}",
        f"- Apply run: `{summary['promotion_apply_run_id']}`",
        f"- Newspaper DB: `{summary['db_path']}`",
        f"- v26 parquet: `{summary['v26_path']}`",
        "",
        "## Promoted Rows",
        "",
        "| Target table | Rows | Confidence | Key field notes |",
        "|---|---:|---|---|",
    ]
    for table, item in summary["tables"].items():
        confidence = ", ".join(
            f"{key}: {value}" for key, value in item["confidence"]["confidence_bars"].items()
        )
        fill_bits = []
        for field, fill in item["key_field_fill"].items():
            if fill["pct"] < 100:
                fill_bits.append(f"{field} {fill['filled']}/{fill['total']}")
        key_notes = "; ".join(fill_bits) if fill_bits else "all checked key fields filled"
        lines.append(f"| `{table}` | {item['row_count']} | {confidence or 'n/a'} | {key_notes} |")

    player_overlap = summary["player_stat_v26_overlap"]
    lines.extend(
        [
            "",
            "## Player Stat Comparison To v26",
            "",
            f"- Comparable v26 stat mappings found: {len(player_overlap.get('v26_stat_mapping', {}))}",
            "- Stat instance outcomes: "
            + (
                ", ".join(
                    f"{key}: {value}"
                    for key, value in player_overlap.get("status_counts", {}).items()
                )
                or "none"
            ),
            "",
            "## Structural Novelty",
            "",
        ]
    )
    for table, item in summary["structural_novelty"].items():
        missing = ", ".join(f"`{field}`" for field in item["not_in_v26"])
        present = ", ".join(f"`{field}`" for field in item["present_in_v26"])
        lines.append(f"- `{table}` fields not in v26: {missing or 'none'}")
        if present:
            lines.append(f"  Present in v26: {present}")

    lineup_overlap = summary["lineup_v26_overlap"]
    lines.extend(
        [
            "",
            "## Lineup Comparison To v26",
            "",
            "- Lineup outcomes: "
            + (
                ", ".join(
                    f"{key}: {value}" for key, value in lineup_overlap.get("status_counts", {}).items()
                )
                or "none"
            ),
        ]
    )

    lines.extend(["", "## Identity Fill", ""])
    for table, fields in summary["identity_fill"].items():
        bits = [
            f"{field} {item['filled']}/{item['total']}"
            for field, item in fields.items()
        ]
        lines.append(f"- `{table}`: {', '.join(bits)}")

    if player_overlap.get("examples"):
        lines.extend(["", "## Examples", ""])
        for example in player_overlap["examples"][:8]:
            label = example.get("status", "example")
            player = example.get("player_raw", "")
            boxscore = example.get("boxscore_id", "")
            lines.append(f"- `{label}`: {player} / {boxscore} / {example}")

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--v26-path", type=Path, default=DEFAULT_V26)
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="latest_promotion_quality")
    args = parser.parse_args()

    con = duckdb.connect(str(args.db_path), read_only=True)
    apply_run_id = args.promotion_apply_run_id or latest_apply_run(con)
    if not apply_run_id:
        raise SystemExit("No promotion apply run found.")

    v26_cols_list = v26_columns(con, args.v26_path)
    v26_cols = set(v26_cols_list)

    rows_by_table: dict[str, list[dict[str, Any]]] = {}
    table_summary: dict[str, Any] = {}
    for table in PROMOTED_TABLES:
        rows = fetch_promoted_rows(con, table, apply_run_id)
        rows_by_table[table] = rows
        table_summary[table] = {
            "row_count": len(rows),
            "key_field_fill": summarize_fill(rows, KEY_FIELDS.get(table, [])),
            "confidence": summarize_confidence(rows),
        }

    player_overlap = summarize_player_stat_overlap(
        con, rows_by_table.get("player_game_box_score", []), args.v26_path, v26_cols
    )
    lineup_overlap = summarize_lineup_overlap(
        con, rows_by_table.get("lineup_participation", []), args.v26_path, v26_cols
    )

    out_dir = args.out_root / f"{stamp()}_{args.label}"
    out_dir.mkdir(parents=True, exist_ok=True)
    samples_path = out_dir / "promoted_samples.csv"
    sample_rows: list[dict[str, Any]] = []
    for table, rows in rows_by_table.items():
        for row in rows[:10]:
            sample_rows.append(
                {
                    "target_table": table,
                    "boxscore_id": clean(row.get("boxscore_id")),
                    "target_entity_key": clean(row.get("target_entity_key")),
                    "player_raw": clean(row.get("player_raw")) or clean(row.get("raw_player_name")),
                    "event_type": clean(row.get("event_type")),
                    "play_type": clean(row.get("play_type")),
                    "play_text": clean(row.get("play_text"))[:240],
                    "source_row_text": clean(row.get("source_row_text"))[:240],
                    "confidence_bar": clean(row.get("confidence_bar")),
                    "confidence_score": clean(row.get("max_confidence_score"))
                    or clean(row.get("confidence_score")),
                }
            )
    write_csv(samples_path, sample_rows)

    summary = {
        "created_at_utc": iso_now(),
        "promotion_apply_run_id": apply_run_id,
        "db_path": str(args.db_path),
        "v26_path": str(args.v26_path),
        "v26_available": bool(v26_cols),
        "v26_column_count": len(v26_cols),
        "tables": table_summary,
        "player_stat_v26_overlap": player_overlap,
        "lineup_v26_overlap": lineup_overlap,
        "identity_fill": summarize_identity_fill(rows_by_table),
        "structural_novelty": summarize_structural_novelty(v26_cols),
        "sample_csv": str(samples_path),
    }

    summary_path = out_dir / "summary.json"
    markdown_path = out_dir / "promotion_quality_report.md"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(render_markdown(summary), encoding="utf-8")

    print(
        json.dumps(
            {
                "promotion_apply_run_id": apply_run_id,
                "output_dir": str(out_dir),
                "row_counts": {
                    table: item["row_count"] for table, item in table_summary.items()
                },
                "player_stat_v26_overlap": player_overlap["status_counts"],
                "lineup_v26_overlap": lineup_overlap["status_counts"],
                "v26_available": bool(v26_cols),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
