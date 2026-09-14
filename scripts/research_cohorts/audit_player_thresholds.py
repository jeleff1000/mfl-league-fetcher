"""Read-only threshold audit of the canonical player_fantasy table.

The audit deliberately uses only columns that exist in the canonical player
schema.  It does not read matchup tables and it does not write to the lake.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def audit(snapshot: Path) -> dict[str, object]:
    con = duckdb.connect(str(snapshot), read_only=True)
    available = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    required = {"platform", "db_name", "year", "week", "win", "is_started", "is_rostered", "is_playoffs", "champion"}
    missing = sorted(required - available)
    if missing:
        raise SystemExit(f"canonical player_fantasy is missing required columns: {missing}")

    settings_available = {
        r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()
    }
    settings_platform = ""
    if "platform" in settings_available:
        settings_platform = "NULLIF(LOWER(TRIM(CAST(ls.platform AS VARCHAR))), '')"
    else:
        settings_platform = "NULL"
    platform_expr = f"COALESCE({settings_platform}, CASE WHEN LOWER(CAST(p.db_name AS VARCHAR)) LIKE 'smpl_mfl_%' THEN 'mfl' WHEN LOWER(CAST(p.db_name AS VARCHAR)) LIKE 'smpl_ffl_%' THEN 'fleaflicker' WHEN LOWER(CAST(p.db_name AS VARCHAR)) LIKE 'smpl_sleeper_%' THEN 'sleeper' ELSE 'unknown' END)"
    settings_join = "LEFT JOIN (SELECT db_name, CAST(year AS INTEGER) AS year, ANY_VALUE(platform) AS platform FROM public.league_settings GROUP BY 1, 2) ls ON ls.db_name = p.db_name AND ls.year = CAST(p.year AS INTEGER)" if "platform" in settings_available else ""

    # The requested units are player rows grouped by the league database,
    # season, and week; platform is only a reporting split.
    con.execute("DROP TABLE IF EXISTS _threshold_week")
    con.execute(
        f"""
        CREATE TEMP TABLE _threshold_week AS
        SELECT
          {platform_expr} AS platform,
          p.db_name,
          CAST(p.year AS INTEGER) AS year,
          CAST(p.week AS INTEGER) AS week,
          COUNT(*) FILTER (WHERE TRY_CAST(p.win AS INTEGER) = 1) AS win_players,
          COUNT(*) FILTER (WHERE TRY_CAST(p.is_started AS INTEGER) = 1) AS started_players,
          COUNT(*) FILTER (WHERE TRY_CAST(p.is_rostered AS INTEGER) = 1) AS rostered_players
        FROM public.player_fantasy p
        {settings_join}
        GROUP BY 1, 2, 3, 4
        """
    )
    con.execute("DROP TABLE IF EXISTS _threshold_season")
    con.execute(
        f"""
        CREATE TEMP TABLE _threshold_season AS
        SELECT
          {platform_expr} AS platform,
          p.db_name,
          CAST(p.year AS INTEGER) AS year,
          COUNT(*) FILTER (
            WHERE TRY_CAST(p.is_playoffs AS INTEGER) = 1
              AND TRY_CAST(p.is_started AS INTEGER) = 1
          ) AS playoff_start_players,
          COUNT(*) FILTER (
            WHERE TRY_CAST(p.is_playoffs AS INTEGER) = 1
              AND TRY_CAST(p.win AS INTEGER) = 1
          ) AS playoff_win_players,
          COUNT(*) FILTER (WHERE TRY_CAST(p.champion AS INTEGER) = 1) AS championship_players
        FROM public.player_fantasy p
        {settings_join}
        GROUP BY 1, 2, 3
        """
    )

    week = con.execute(
        """
        SELECT platform,
          COUNT(*) AS league_week_years,
          COUNT(*) FILTER (WHERE win_players < 10) AS lt10_win_players,
          COUNT(*) FILTER (WHERE started_players < 20) AS lt20_started_players,
          COUNT(*) FILTER (WHERE rostered_players < 30) AS lt30_rostered_players
        FROM _threshold_week
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    week_cols = [d[0] for d in con.description]

    season = con.execute(
        """
        SELECT platform,
          COUNT(*) AS league_years,
          COUNT(*) FILTER (WHERE playoff_start_players < 10) AS lt10_playoff_start_players,
          COUNT(*) FILTER (WHERE playoff_win_players < 5) AS lt5_playoff_win_players,
          COUNT(*) FILTER (WHERE championship_players < 5) AS lt5_championship_players
        FROM _threshold_season
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    season_cols = [d[0] for d in con.description]
    unknown = [
        {"db_name": row[0], "year": int(row[1])}
        for row in con.execute(
            "SELECT db_name, year FROM _threshold_season WHERE platform = 'unknown' ORDER BY db_name, year"
        ).fetchall()
    ]

    result = {
        "population_source": "public.player_fantasy only",
        "read_only": True,
        "schema_columns_used": sorted(required),
        "thresholds": {
            "week": {"win_players_lt": 10, "started_players_lt": 20, "rostered_players_lt": 30},
            "season": {"playoff_start_players_lt": 10, "playoff_win_players_lt": 5, "championship_players_lt": 5},
        },
        "by_platform_week": [dict(zip(week_cols, row)) for row in week],
        "by_platform_season": [dict(zip(season_cols, row)) for row in season],
        "unknown_platform_league_years": unknown,
    }
    con.close()
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = audit(args.snapshot)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"by_platform_week": result["by_platform_week"], "by_platform_season": result["by_platform_season"]}, indent=2))


if __name__ == "__main__":
    main()
