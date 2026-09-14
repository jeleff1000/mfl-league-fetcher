#!/usr/bin/env python
"""Query local v26 release game context for newspaper conveyor resolution."""

from __future__ import annotations

import argparse
import csv
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_RELEASE_ROOT = Path(r"D:\league-history-data\nfl\releases")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\v26_game_context_queries")
DEFAULT_TOOL_MIRROR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor")

FIELDS = [
    "boxscore_id",
    "year",
    "game_date",
    "week",
    "team",
    "opponent",
    "team_score",
    "opponent_score",
    "points_allowed",
    "position",
    "player_rows",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def latest_v26(release_root: Path) -> Path:
    files = sorted(
        glob.glob(str(release_root / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
        key=lambda item: Path(item).stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(f"No v26 parquet found under {release_root}")
    return Path(files[0])


def first_existing(cols: set[str], names: list[str]) -> str:
    for name in names:
        if name in cols:
            return name
    return ""


def column_map(con: duckdb.DuckDBPyConnection, v26_path: Path) -> dict[str, str]:
    rel = str(v26_path).replace("\\", "/").replace("'", "''")
    con.execute(f"CREATE OR REPLACE VIEW v26_ctx AS SELECT * FROM read_parquet('{rel}')")
    cols = {row[1] for row in con.execute("PRAGMA table_info(v26_ctx)").fetchall()}
    return {
        "boxscore_id": first_existing(cols, ["boxscore_id", "game_id", "pfr_game_id"]),
        "year": first_existing(cols, ["year", "season"]),
        "game_date": first_existing(cols, ["game_date"]),
        "week": first_existing(cols, ["week"]),
        "team": first_existing(cols, ["nfl_team", "team", "team_code", "recent_team"]),
        "opponent": first_existing(cols, ["opponent_nfl_team", "opponent", "opp", "opponent_team"]),
        "team_score": first_existing(cols, ["team_score", "points_for", "pts_for", "score_for", "team_points"]),
        "opponent_score": first_existing(cols, ["opponent_score", "points_against", "pts_against", "score_against", "opponent_points"]),
        "points_allowed": first_existing(cols, ["points_allowed", "dst_points_allowed", "pts_allow"]),
        "position": first_existing(cols, ["position"]),
    }


def select_expr(columns: dict[str, str], key: str, alias: str | None = None) -> str:
    column = columns.get(key, "")
    out_alias = alias or key
    if column:
        return f"CAST({column} AS VARCHAR) AS {out_alias}"
    return f"'' AS {out_alias}"


def query_context(
    con: duckdb.DuckDBPyConnection,
    columns: dict[str, str],
    boxscore_ids: list[str],
    years: list[str],
    teams: list[str],
) -> list[dict[str, Any]]:
    where_parts: list[str] = []
    params: list[Any] = []

    if boxscore_ids and columns.get("boxscore_id"):
        placeholders = ",".join(["?"] * len(boxscore_ids))
        where_parts.append(f"{columns['boxscore_id']} IN ({placeholders})")
        params.extend(boxscore_ids)

    if years and columns.get("year"):
        placeholders = ",".join(["?"] * len(years))
        where_parts.append(f"CAST(TRY_CAST({columns['year']} AS DOUBLE) AS INTEGER) IN ({placeholders})")
        params.extend([int(float(year)) for year in years])

    if teams and columns.get("team"):
        team_checks = [columns["team"]]
        if columns.get("opponent"):
            team_checks.append(columns["opponent"])
        placeholders = ",".join(["?"] * len(teams))
        where_parts.append("(" + " OR ".join(f"{col} IN ({placeholders})" for col in team_checks) + ")")
        for _ in team_checks:
            params.extend(teams)

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"
    group_fields = [
        select_expr(columns, "boxscore_id"),
        select_expr(columns, "year"),
        select_expr(columns, "game_date"),
        select_expr(columns, "week"),
        select_expr(columns, "team"),
        select_expr(columns, "opponent"),
        select_expr(columns, "team_score"),
        select_expr(columns, "opponent_score"),
        select_expr(columns, "points_allowed"),
        select_expr(columns, "position"),
    ]
    sql = f"""
        SELECT
          {", ".join(group_fields)},
          COUNT(*) AS player_rows
        FROM v26_ctx
        WHERE {where_sql}
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        ORDER BY year, game_date, boxscore_id, team, opponent, position
    """
    result = con.execute(sql, params)
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def diagnostics(con: duckdb.DuckDBPyConnection, columns: dict[str, str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    payload["total_rows"] = con.execute("SELECT COUNT(*) FROM v26_ctx").fetchone()[0]
    if columns.get("year"):
        row = con.execute(
            f"SELECT MIN(CAST({columns['year']} AS VARCHAR)), MAX(CAST({columns['year']} AS VARCHAR)) FROM v26_ctx"
        ).fetchone()
        payload["year_min"] = clean(row[0]) if row else ""
        payload["year_max"] = clean(row[1]) if row else ""
        payload["year_samples"] = [
            clean(row[0])
            for row in con.execute(
                f"""
                SELECT DISTINCT CAST({columns['year']} AS VARCHAR) AS year_value
                FROM v26_ctx
                ORDER BY year_value
                LIMIT 20
                """
            ).fetchall()
        ]
    if columns.get("team"):
        payload["team_samples"] = [
            clean(row[0])
            for row in con.execute(
                f"""
                SELECT DISTINCT CAST({columns['team']} AS VARCHAR) AS team_value
                FROM v26_ctx
                WHERE {columns['team']} IS NOT NULL
                ORDER BY team_value
                LIMIT 80
                """
            ).fetchall()
        ]
    return payload


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path, default=DEFAULT_RELEASE_ROOT)
    parser.add_argument("--v26-path", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="v26_game_context")
    parser.add_argument("--boxscore-id", action="append", default=[])
    parser.add_argument("--year", action="append", default=[])
    parser.add_argument("--team", action="append", default=[])
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--mirror-tool", action="store_true")
    parser.add_argument("--tool-mirror-root", type=Path, default=DEFAULT_TOOL_MIRROR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.mirror_tool:
        args.tool_mirror_root.mkdir(parents=True, exist_ok=True)
        target = args.tool_mirror_root / Path(__file__).name
        if Path(__file__).resolve() != target.resolve():
            target.write_text(Path(__file__).read_text(encoding="utf-8"), encoding="utf-8")

    v26_path = args.v26_path or latest_v26(args.release_root)
    con = duckdb.connect()
    try:
        columns = column_map(con, v26_path)
        diagnostic_payload = diagnostics(con, columns) if args.diagnostics else {}
        rows = query_context(
            con,
            columns,
            [clean(item) for item in args.boxscore_id if clean(item)],
            [clean(item) for item in args.year if clean(item)],
            [clean(item) for item in args.team if clean(item)],
        )
    finally:
        con.close()

    out_dir = args.out_root / f"{stamp()}_{args.label}"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "v26_game_context.csv"
    summary_path = out_dir / "summary.json"
    write_csv(csv_path, rows)
    summary = {
        "created_at_utc": iso_now(),
        "output_dir": str(out_dir),
        "csv_path": str(csv_path),
        "v26_path": str(v26_path),
        "columns": columns,
        "diagnostics": diagnostic_payload,
        "row_count": len(rows),
        "boxscore_ids": args.boxscore_id,
        "years": args.year,
        "teams": args.team,
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
