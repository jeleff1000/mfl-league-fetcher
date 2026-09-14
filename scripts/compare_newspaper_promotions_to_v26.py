#!/usr/bin/env python
"""Compare local newspaper promotion packages against latest v26 release.

This is a read-only audit. It answers whether the newspaper conveyor is finding
facts that are already represented in the local v26 table, versus facts that are
new corroboration or genuinely additive atoms still awaiting review.
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


DEFAULT_NEWSPAPER_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_RELEASE_ROOT = Path(r"D:\league-history-data\nfl\releases")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\v26_comparison_reports")

PACKAGE_FIELDS = [
    "comparison_run_id",
    "package_run_id",
    "promotion_package_id",
    "package_status",
    "target_table",
    "boxscore_id",
    "confidence_bar",
    "max_confidence_score",
    "evidence_document_count",
    "item_count",
    "newspaper_team_1_raw",
    "newspaper_team_1_score",
    "newspaper_team_2_raw",
    "newspaper_team_2_score",
    "v26_match_status",
    "v26_score_status",
    "v26_rows",
    "v26_distinct_team_games",
    "v26_score_pairs_json",
    "v26_columns_used_json",
    "value_class",
    "proposed_fields_json",
    "source_documents_json",
]

TABLE_FIELDS = [
    "comparison_run_id",
    "package_run_id",
    "package_status",
    "target_table",
    "package_count",
    "ready_count",
    "needs_review_count",
    "high_count",
    "medium_count",
    "v26_exact_score_count",
    "v26_missing_game_count",
    "v26_conflict_count",
    "not_v26_score_comparable_count",
    "value_class_counts_json",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
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
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def latest_v26(release_root: Path) -> Path:
    files = sorted(
        glob.glob(str(release_root / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
        key=lambda p: Path(p).stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(f"No v26 parquet found under {release_root}")
    return Path(files[0])


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_package_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT package_run_id
        FROM newspaper_review.llm_promotion_package_run
        ORDER BY created_at_utc DESC, package_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_packages(con: duckdb.DuckDBPyConnection, package_run_id: str) -> list[dict[str, Any]]:
    if not package_run_id:
        return []
    return query_dicts(
        con,
        """
        SELECT *
        FROM newspaper_review.llm_promotion_package
        WHERE package_run_id = ?
        ORDER BY package_status, target_table, boxscore_id, target_entity_key
        """,
        [package_run_id],
    )


def first_existing(cols: set[str], candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return ""


def v26_score_columns(con: duckdb.DuckDBPyConnection, v26_path: Path) -> dict[str, str]:
    rel = str(v26_path).replace("\\", "/").replace("'", "''")
    con.execute(f"CREATE OR REPLACE VIEW v26_cmp AS SELECT * FROM read_parquet('{rel}')")
    cols = {row[1] for row in con.execute("PRAGMA table_info(v26_cmp)").fetchall()}
    return {
        "boxscore_id": first_existing(cols, ["boxscore_id", "game_id", "pfr_game_id"]),
        "team": first_existing(cols, ["nfl_team", "team", "team_code", "recent_team"]),
        "opponent": first_existing(cols, ["opponent_nfl_team", "opponent", "opp", "opponent_team"]),
        "team_score": first_existing(cols, ["team_score", "points_for", "pts_for", "score_for", "team_points"]),
        "opponent_score": first_existing(cols, ["opponent_score", "points_against", "pts_against", "score_against", "opponent_points"]),
        "points_allowed": first_existing(cols, ["points_allowed", "dst_points_allowed", "pts_allow"]),
        "game_date": first_existing(cols, ["game_date"]),
        "year": first_existing(cols, ["year", "season"]),
        "week": first_existing(cols, ["week"]),
        "position": first_existing(cols, ["position"]),
    }


def normalize_int(value: Any) -> str:
    text = clean(value).strip()
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except ValueError:
        return text


def score_pair_from_package(package: dict[str, Any]) -> tuple[str, str, str, str]:
    proposed = parse_json_obj(package.get("proposed_fields_json"))
    return (
        clean(proposed.get("team_1_raw")),
        normalize_int(proposed.get("team_1_score")),
        clean(proposed.get("team_2_raw")),
        normalize_int(proposed.get("team_2_score")),
    )


def resolved_game_key(package: dict[str, Any]) -> tuple[str, str, str, str]:
    proposed = parse_json_obj(package.get("proposed_fields_json"))
    year = normalize_int(proposed.get("year"))
    game_date = clean(proposed.get("game_date"))
    team_1 = clean(proposed.get("team_1_resolved")) or clean(proposed.get("team_1_raw"))
    team_2 = clean(proposed.get("team_2_resolved")) or clean(proposed.get("team_2_raw"))
    return year, game_date, team_1, team_2


def load_v26_game_scores_by_boxscore(
    con: duckdb.DuckDBPyConnection,
    columns: dict[str, str],
    boxscore_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    box_col = columns.get("boxscore_id", "")
    team_col = columns.get("team", "")
    opp_col = columns.get("opponent", "")
    team_score_col = columns.get("team_score", "")
    opp_score_col = columns.get("opponent_score", "")
    if not all([box_col, team_col, opp_col, team_score_col, opp_score_col]) or not boxscore_ids:
        return {}
    placeholders = ",".join(["?"] * len(boxscore_ids))
    pos_filter = ""
    params: list[Any] = list(boxscore_ids)
    if columns.get("position"):
        pos_filter = f" AND {columns['position']} = 'DEF'"
    rows = query_dicts(
        con,
        f"""
        SELECT
          {box_col} AS boxscore_id,
          {team_col} AS team,
          {opp_col} AS opponent,
          {team_score_col} AS team_score,
          {opp_score_col} AS opponent_score,
          COUNT(*) AS row_count
        FROM v26_cmp
        WHERE {box_col} IN ({placeholders})
          {pos_filter}
        GROUP BY 1,2,3,4,5
        ORDER BY boxscore_id, team
        """,
        params,
    )
    by_game: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_game.setdefault(clean(row.get("boxscore_id")), []).append(row)
    return by_game


def infer_score_rows_from_points_allowed(
    package: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    score_source: str = "opponent_points_allowed_pair",
) -> list[dict[str, Any]]:
    _, _, team_1, team_2 = resolved_game_key(package)
    by_pair = {
        (clean(row.get("team")), clean(row.get("opponent"))): row
        for row in raw_rows
    }
    row_1 = by_pair.get((team_1, team_2))
    row_2 = by_pair.get((team_2, team_1))
    if not row_1 and not row_2:
        return []
    outputs: list[dict[str, Any]] = []
    if row_1:
        outputs.append({
            **row_1,
            "team_score": normalize_int(row_2.get("points_allowed")) if row_2 else "",
            "opponent_score": normalize_int(row_1.get("points_allowed")),
            "score_source": score_source,
        })
    if row_2:
        outputs.append({
            **row_2,
            "team_score": normalize_int(row_1.get("points_allowed")) if row_1 else "",
            "opponent_score": normalize_int(row_2.get("points_allowed")),
            "score_source": score_source,
        })
    return outputs


def load_v26_game_scores_by_schedule(
    con: duckdb.DuckDBPyConnection,
    columns: dict[str, str],
    packages: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    year_col = columns.get("year", "")
    date_col = columns.get("game_date", "")
    team_col = columns.get("team", "")
    opp_col = columns.get("opponent", "")
    points_allowed_col = columns.get("points_allowed", "")
    if not all([year_col, date_col, team_col, opp_col, points_allowed_col]):
        return {}
    pos_filter = f" AND {columns['position']} = 'DEF'" if columns.get("position") else ""
    by_game: dict[str, list[dict[str, Any]]] = {}
    for package in packages:
        if clean(package.get("target_table")) != "game_candidate":
            continue
        boxscore_id = clean(package.get("boxscore_id"))
        year, game_date, team_1, team_2 = resolved_game_key(package)
        if not all([boxscore_id, year, game_date, team_1, team_2]):
            continue
        rows = query_dicts(
            con,
            f"""
            SELECT
              {year_col} AS year,
              CAST({date_col} AS VARCHAR) AS game_date,
              {team_col} AS team,
              {opp_col} AS opponent,
              {points_allowed_col} AS points_allowed,
              COUNT(*) AS row_count
            FROM v26_cmp
            WHERE CAST({year_col} AS INTEGER) = ?
              AND CAST({date_col} AS DATE) = CAST(? AS DATE)
              AND (
                ({team_col} = ? AND {opp_col} = ?)
                OR ({team_col} = ? AND {opp_col} = ?)
              )
              {pos_filter}
            GROUP BY 1,2,3,4,5
            ORDER BY team
            """,
            [year, game_date, team_1, team_2, team_2, team_1],
        )
        inferred_rows = infer_score_rows_from_points_allowed(package, rows)
        if inferred_rows:
            by_game[boxscore_id] = inferred_rows
    return by_game


def load_v26_game_scores_by_undated_matchup(
    con: duckdb.DuckDBPyConnection,
    columns: dict[str, str],
    packages: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    year_col = columns.get("year", "")
    date_col = columns.get("game_date", "")
    team_col = columns.get("team", "")
    opp_col = columns.get("opponent", "")
    points_allowed_col = columns.get("points_allowed", "")
    if not all([year_col, date_col, team_col, opp_col, points_allowed_col]):
        return {}
    pos_filter = f" AND {columns['position']} = 'DEF'" if columns.get("position") else ""
    by_game: dict[str, list[dict[str, Any]]] = {}
    for package in packages:
        if clean(package.get("target_table")) != "game_candidate":
            continue
        boxscore_id = clean(package.get("boxscore_id"))
        year, _, team_1, team_2 = resolved_game_key(package)
        if not all([boxscore_id, year, team_1, team_2]):
            continue
        rows = query_dicts(
            con,
            f"""
            SELECT
              {year_col} AS year,
              CAST({date_col} AS VARCHAR) AS game_date,
              {team_col} AS team,
              {opp_col} AS opponent,
              {points_allowed_col} AS points_allowed,
              COUNT(*) AS row_count
            FROM v26_cmp
            WHERE CAST({year_col} AS INTEGER) = ?
              AND {date_col} IS NULL
              AND (
                ({team_col} = ? AND {opp_col} = ?)
                OR ({team_col} = ? AND {opp_col} = ?)
              )
              {pos_filter}
            GROUP BY 1,2,3,4,5
            ORDER BY team
            """,
            [year, team_1, team_2, team_2, team_1],
        )
        inferred_rows = infer_score_rows_from_points_allowed(
            package,
            rows,
            score_source="undated_opponent_points_allowed_pair",
        )
        if inferred_rows:
            by_game[boxscore_id] = inferred_rows
    return by_game


def load_v26_game_scores(
    con: duckdb.DuckDBPyConnection,
    columns: dict[str, str],
    packages: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    boxscore_ids = sorted({clean(package.get("boxscore_id")) for package in packages if clean(package.get("boxscore_id"))})
    by_game: dict[str, list[dict[str, Any]]] = {}
    for source in (
        load_v26_game_scores_by_boxscore(con, columns, boxscore_ids),
        load_v26_game_scores_by_schedule(con, columns, packages),
        load_v26_game_scores_by_undated_matchup(con, columns, packages),
    ):
        for boxscore_id, rows in source.items():
            by_game.setdefault(boxscore_id, rows)
    return by_game


def score_status(package: dict[str, Any], v26_rows: list[dict[str, Any]], comparable: bool) -> tuple[str, str]:
    if not comparable:
        return "not_comparable", "not_v26_score_comparable"
    _, score_1, _, score_2 = score_pair_from_package(package)
    if not score_1 and not score_2:
        return "newspaper_score_missing", "score_followup_needed_no_newspaper_score"
    if not v26_rows:
        return "missing_game", "score_candidate_v26_missing_game"
    undated_match = any(clean(row.get("score_source")).startswith("undated_") for row in v26_rows)
    newspaper_pair = sorted([score_1, score_2])
    v26_pairs = {
        tuple(sorted([normalize_int(row.get("team_score")), normalize_int(row.get("opponent_score"))]))
        for row in v26_rows
    }
    if tuple(newspaper_pair) in v26_pairs:
        if undated_match:
            return "undated_exact_score_match", "fills_v26_missing_game_date"
        return "exact_score_match", "corroborates_existing_v26_score"
    newspaper_scores = {score for score in newspaper_pair if score != ""}
    for row in v26_rows:
        row_scores = [normalize_int(row.get("team_score")), normalize_int(row.get("opponent_score"))]
        present_scores = {score for score in row_scores if score != ""}
        if "" in row_scores and present_scores and present_scores.issubset(newspaper_scores):
            if undated_match:
                return "undated_game_match_score_incomplete", "fills_v26_missing_date_or_score"
            return "partial_score_match_missing_side", "fills_v26_missing_score_side"
    if undated_match:
        return "undated_game_match_score_incomplete", "fills_v26_missing_date_or_score"
    return "score_conflict_or_team_mapping_needed", "possible_v26_score_delta_or_mapping_issue"


def classify_non_score_package(package: dict[str, Any]) -> str:
    target_table = clean(package.get("target_table"))
    status = clean(package.get("package_status"))
    confidence = clean(package.get("confidence_bar"))
    if target_table in {"scoring_event", "play_by_play_event", "lineup_participation", "player_identity_candidate"}:
        return f"potentially_additive_{target_table}_{status}_{confidence}"
    return f"non_score_{target_table}_{status}_{confidence}"


def build_rows(
    comparison_run_id: str,
    package_run_id: str,
    packages: list[dict[str, Any]],
    v26_rows_by_game: dict[str, list[dict[str, Any]]],
    columns: dict[str, str],
) -> list[dict[str, Any]]:
    comparable = (
        all(columns.get(key) for key in ["boxscore_id", "team", "opponent", "team_score", "opponent_score"])
        or all(columns.get(key) for key in ["year", "game_date", "team", "opponent", "points_allowed"])
    )
    rows = []
    for package in packages:
        target_table = clean(package.get("target_table"))
        boxscore_id = clean(package.get("boxscore_id"))
        team_1, score_1, team_2, score_2 = score_pair_from_package(package)
        v26_rows = v26_rows_by_game.get(boxscore_id, [])
        if target_table == "game_candidate":
            v26_score_status, value_class = score_status(package, v26_rows, comparable)
            v26_match_status = (
                "found_undated_match"
                if any(clean(row.get("score_source")).startswith("undated_") for row in v26_rows)
                else ("found" if v26_rows else "missing")
            )
        else:
            v26_score_status = "not_score_package"
            value_class = classify_non_score_package(package)
            v26_match_status = "not_checked_for_non_score_atom"
        rows.append({
            "comparison_run_id": comparison_run_id,
            "package_run_id": package_run_id,
            "promotion_package_id": clean(package.get("promotion_package_id")),
            "package_status": clean(package.get("package_status")),
            "target_table": target_table,
            "boxscore_id": boxscore_id,
            "confidence_bar": clean(package.get("confidence_bar")),
            "max_confidence_score": clean(package.get("max_confidence_score")),
            "evidence_document_count": clean(package.get("evidence_document_count")),
            "item_count": clean(package.get("item_count")),
            "newspaper_team_1_raw": team_1,
            "newspaper_team_1_score": score_1,
            "newspaper_team_2_raw": team_2,
            "newspaper_team_2_score": score_2,
            "v26_match_status": v26_match_status,
            "v26_score_status": v26_score_status,
            "v26_rows": sum(int(row.get("row_count") or 0) for row in v26_rows),
            "v26_distinct_team_games": len(v26_rows),
            "v26_score_pairs_json": json.dumps(v26_rows, sort_keys=True, ensure_ascii=False),
            "v26_columns_used_json": json.dumps(columns, sort_keys=True, ensure_ascii=False),
            "value_class": value_class,
            "proposed_fields_json": clean(package.get("proposed_fields_json")),
            "source_documents_json": clean(package.get("source_documents_json")),
        })
    return rows


def table_rollup(comparison_run_id: str, package_run_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["package_status"], row["target_table"]), []).append(row)
    output = []
    for (package_status, target_table), group in sorted(grouped.items()):
        value_counts = Counter(row["value_class"] for row in group)
        output.append({
            "comparison_run_id": comparison_run_id,
            "package_run_id": package_run_id,
            "package_status": package_status,
            "target_table": target_table,
            "package_count": len(group),
            "ready_count": sum(1 for row in group if row["package_status"] == "ready_for_promotion_review"),
            "needs_review_count": sum(1 for row in group if row["package_status"] == "needs_review"),
            "high_count": sum(1 for row in group if row["confidence_bar"] == "high"),
            "medium_count": sum(1 for row in group if row["confidence_bar"] == "medium"),
            "v26_exact_score_count": sum(1 for row in group if row["v26_score_status"] == "exact_score_match"),
            "v26_missing_game_count": sum(1 for row in group if row["v26_score_status"] == "missing_game"),
            "v26_conflict_count": sum(1 for row in group if row["v26_score_status"] == "score_conflict_or_team_mapping_needed"),
            "not_v26_score_comparable_count": sum(1 for row in group if row["v26_score_status"] in {"not_comparable", "not_score_package"}),
            "value_class_counts_json": json.dumps(dict(value_counts), sort_keys=True, ensure_ascii=False),
        })
    return output


def write_markdown(path: Path, summary: dict[str, Any], rollup_rows: list[dict[str, Any]], package_rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Newspaper Promotion vs v26 Comparison",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"v26: `{summary['v26_path']}`",
        f"Package run: `{summary['package_run_id']}`",
        "",
        "## Summary",
        "",
        f"- Packages compared: `{summary['package_count']}`",
        f"- Ready packages: `{summary['package_status_counts'].get('ready_for_promotion_review', 0)}`",
        f"- Needs-review packages: `{summary['package_status_counts'].get('needs_review', 0)}`",
        f"- Ready game candidates exact in v26: `{summary['ready_game_candidate_exact_v26_count']}`",
        f"- Ready game candidates missing/conflicting in v26: `{summary['ready_game_candidate_not_exact_v26_count']}`",
        f"- Potential additive non-score packages: `{summary['potential_additive_non_score_count']}`",
        "",
        "## Rollup",
        "",
        "| status | table | packages | high | medium | exact v26 scores | missing game | conflict | value classes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rollup_rows:
        lines.append(
            f"| {row['package_status']} | {row['target_table']} | {row['package_count']} | "
            f"{row['high_count']} | {row['medium_count']} | {row['v26_exact_score_count']} | "
            f"{row['v26_missing_game_count']} | {row['v26_conflict_count']} | {row['value_class_counts_json']} |"
        )
    lines.extend(["", "## Ready Game Candidates", ""])
    ready_rows = [
        row for row in package_rows
        if row["package_status"] == "ready_for_promotion_review" and row["target_table"] == "game_candidate"
    ]
    lines.append("| boxscore | newspaper score | confidence | docs | v26 status | value |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in ready_rows:
        score = (
            f"{row['newspaper_team_1_raw']} {row['newspaper_team_1_score']} - "
            f"{row['newspaper_team_2_raw']} {row['newspaper_team_2_score']}"
        )
        lines.append(
            f"| {row['boxscore_id']} | {score} | {row['max_confidence_score']} | "
            f"{row['evidence_document_count']} | {row['v26_score_status']} | {row['value_class']} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--newspaper-db", type=Path, default=DEFAULT_NEWSPAPER_DB)
    parser.add_argument("--release-root", type=Path, default=DEFAULT_RELEASE_ROOT)
    parser.add_argument("--v26-path", type=Path, default=None)
    parser.add_argument("--package-run-id", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_vs_v26_comparison")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    comparison_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / comparison_run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    v26_path = args.v26_path or latest_v26(args.release_root)

    newspaper_con = duckdb.connect(str(args.newspaper_db), read_only=True)
    try:
        package_run_id = args.package_run_id or latest_package_run(newspaper_con)
        packages = load_packages(newspaper_con, package_run_id)
    finally:
        newspaper_con.close()

    v26_con = duckdb.connect()
    try:
        columns = v26_score_columns(v26_con, v26_path)
        v26_rows_by_game = load_v26_game_scores(v26_con, columns, packages)
    finally:
        v26_con.close()

    package_rows = build_rows(comparison_run_id, package_run_id, packages, v26_rows_by_game, columns)
    rollup_rows = table_rollup(comparison_run_id, package_run_id, package_rows)
    package_status_counts = Counter(row["package_status"] for row in package_rows)
    target_table_counts = Counter(row["target_table"] for row in package_rows)
    value_class_counts = Counter(row["value_class"] for row in package_rows)
    ready_game_rows = [
        row for row in package_rows
        if row["package_status"] == "ready_for_promotion_review" and row["target_table"] == "game_candidate"
    ]
    summary = {
        "created_at_utc": created_at,
        "comparison_run_id": comparison_run_id,
        "output_dir": str(out_dir),
        "newspaper_db": str(args.newspaper_db),
        "v26_path": str(v26_path),
        "v26_columns_used": columns,
        "package_run_id": package_run_id,
        "package_count": len(package_rows),
        "package_status_counts": dict(package_status_counts),
        "target_table_counts": dict(target_table_counts),
        "value_class_counts": dict(value_class_counts),
        "ready_game_candidate_count": len(ready_game_rows),
        "ready_game_candidate_exact_v26_count": sum(1 for row in ready_game_rows if row["v26_score_status"] == "exact_score_match"),
        "ready_game_candidate_not_exact_v26_count": sum(1 for row in ready_game_rows if row["v26_score_status"] != "exact_score_match"),
        "potential_additive_non_score_count": sum(1 for row in package_rows if row["value_class"].startswith("potentially_additive_")),
    }
    write_csv(out_dir / "package_comparison.csv", package_rows, PACKAGE_FIELDS)
    write_csv(out_dir / "target_table_rollup.csv", rollup_rows, TABLE_FIELDS)
    write_json(out_dir / "summary.json", summary)
    write_markdown(out_dir / "comparison_report.md", summary, rollup_rows, package_rows)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
