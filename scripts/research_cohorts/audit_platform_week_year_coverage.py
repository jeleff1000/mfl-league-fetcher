"""Audit populated player-week coverage by platform and week-year."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def audit(snapshot: Path) -> dict:
    con = duckdb.connect(str(snapshot), read_only=True)
    rows = con.execute("""
        SELECT
          LOWER(TRIM(CAST(platform AS VARCHAR))) AS platform,
          CAST(year AS INTEGER) AS year,
          CAST(week AS INTEGER) AS week,
          COUNT(DISTINCT db_name) AS league_count,
          COUNT(*) AS player_rows,
          COUNT(*) FILTER (WHERE is_started IS NOT NULL) AS is_started_populated,
          COUNT(DISTINCT db_name) FILTER (WHERE CAST(is_started AS INTEGER)=1) AS is_started_leagues,
          COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1) AS started_rows,
          COUNT(DISTINCT db_name) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND win IS NOT NULL) AS started_win_leagues,
          COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND win IS NOT NULL) AS started_win_populated,
          COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND win IS NULL) AS started_win_missing
        FROM public.player_fantasy
        GROUP BY 1,2,3
        ORDER BY 1,2,3
    """).fetchall()
    names = [d[0] for d in con.description]
    data = [dict(zip(names, row)) for row in rows]
    for row in data:
        row["is_started_low_vs_leagues"] = row["is_started_leagues"] < row["league_count"]
        row["started_win_low_vs_leagues"] = row["started_win_leagues"] < row["league_count"]

    by_platform = {}
    for platform in sorted({r["platform"] for r in data}):
        group = [r for r in data if r["platform"] == platform]
        by_platform[platform] = {
            "week_year_groups": len(group),
            "low_is_started_groups": sum(r["is_started_low_vs_leagues"] for r in group),
            "low_started_win_groups": sum(r["started_win_low_vs_leagues"] for r in group),
            "platform_null_groups": sum(r["platform"] is None for r in group),
            "total_leagues_across_groups": sum(r["league_count"] for r in group),
            "started_rows": sum(r["started_rows"] for r in group),
            "is_started_leagues": sum(r["is_started_leagues"] for r in group),
            "started_win_leagues": sum(r["started_win_leagues"] for r in group),
            "started_win_populated": sum(r["started_win_populated"] for r in group),
            "started_win_missing": sum(r["started_win_missing"] for r in group),
        }

    result = {
        "definition": {
            "unit": "platform/year/week",
            "league_count": "distinct db_name in the group",
            "is_started_low": "distinct leagues with a starter < league_count",
            "started_win_low": "distinct leagues with a started row and non-null win < league_count",
        },
        "player_rows": int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]),
        "week_year_rows": data,
        "by_platform": by_platform,
    }
    con.close()
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = audit(args.snapshot)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result["by_platform"], indent=2))


if __name__ == "__main__":
    main()
