"""Audit championship-signal gaps without mutating the research lake."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    con = duckdb.connect()
    con.execute(f"ATTACH '{args.base.resolve().as_posix().replace(chr(39), chr(39) * 2)}' AS lake (READ_ONLY)")
    cols = {r[0] for r in con.execute("DESCRIBE lake.public.matchup").fetchall()}
    required = {"db_name", "year", "week", "is_playoffs", "is_championship"}
    missing = required - cols
    if missing:
        raise SystemExit(f"matchup missing columns: {sorted(missing)}")

    # Keep the audit's denominator scope explicit.  A matchup signal audit is
    # conditional on a row existing in matchup; it is not a census of the
    # research population.  These three key sets make that distinction
    # measurable and prevent a 9k matchup subset from being reported as the
    # full ~65k league-year lake.
    for table in ("league_settings", "player_fantasy"):
        try:
            con.execute(f"DESCRIBE lake.public.{table}").fetchall()
        except duckdb.CatalogException:
            raise SystemExit(
                f"population audit requires lake.public.{table}; "
                "refusing to report matchup-only coverage"
            )
    settings_cols = {
        row[0] for row in con.execute("DESCRIBE lake.public.league_settings").fetchall()
    }
    settings_platform_expr = (
        "LOWER(NULLIF(TRIM(CAST(platform AS VARCHAR)), ''))"
        if "platform" in settings_cols else "NULL"
    )
    con.execute("""
      CREATE OR REPLACE TEMP TABLE population_detail AS
      WITH settings AS (
        SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name,
                        CAST(year AS INTEGER) AS year,
                        {settings_platform_expr} AS source_platform,
                        1 AS present
        FROM lake.public.league_settings
      ), players AS (
        SELECT DISTINCT CAST(p.db_name AS VARCHAR) AS db_name,
                        CAST(p.year AS INTEGER) AS year,
                        1 AS present
        FROM lake.public.player_fantasy p
        JOIN settings s USING (db_name, year)
      ), matchups AS (
        SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name,
                        CAST(year AS INTEGER) AS year,
                        1 AS present
        FROM lake.public.matchup
      )
      SELECT k.db_name, k.year,
             CASE
               WHEN s.source_platform IS NOT NULL THEN s.source_platform
               WHEN lower(k.db_name) LIKE 'smpl_mfl_%' THEN 'mfl'
               WHEN lower(k.db_name) LIKE 'smpl_ffl_%' THEN 'fleaflicker'
               WHEN lower(k.db_name) LIKE 'smpl_slpr_%'
                 OR lower(k.db_name) LIKE 'smpl_sleeper_%' THEN 'sleeper'
               ELSE 'other'
             END AS platform,
             (s.present IS NOT NULL) AS has_settings,
             (p.present IS NOT NULL) AS has_player_rows,
             (m.present IS NOT NULL) AS has_matchup_rows
      FROM (
        SELECT db_name, year FROM settings
        UNION SELECT db_name, year FROM players
        UNION SELECT db_name, year FROM matchups
      ) k
      LEFT JOIN settings s USING (db_name, year)
      LEFT JOIN players p USING (db_name, year)
      LEFT JOIN matchups m USING (db_name, year)
    """.format(settings_platform_expr=settings_platform_expr))
    con.execute("""
      CREATE OR REPLACE TEMP TABLE population_sets AS
      WITH settings AS (
        SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name,
                        CAST(year AS INTEGER) AS year
        FROM lake.public.league_settings
      ), players AS (
        SELECT DISTINCT CAST(p.db_name AS VARCHAR) AS db_name,
                        CAST(p.year AS INTEGER) AS year
        FROM lake.public.player_fantasy p
        JOIN settings s USING (db_name, year)
      ), matchups AS (
        SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name,
                        CAST(year AS INTEGER) AS year
        FROM lake.public.matchup
      )
      SELECT
        (SELECT COUNT(*) FROM settings) AS settings_league_years,
        (SELECT COUNT(*) FROM players) AS player_league_years,
        (SELECT COUNT(*) FROM matchups) AS matchup_league_years,
        (SELECT COUNT(*) FROM players p
          WHERE NOT EXISTS (SELECT 1 FROM matchups m
                            WHERE m.db_name=p.db_name AND m.year=p.year))
          AS player_without_matchup,
        (SELECT COUNT(*) FROM settings s
          WHERE NOT EXISTS (SELECT 1 FROM players p
                            WHERE p.db_name=s.db_name AND p.year=s.year))
          AS settings_without_player_rows,
        (SELECT COUNT(*) FROM matchups m
          WHERE NOT EXISTS (SELECT 1 FROM settings s
                            WHERE s.db_name=m.db_name AND s.year=m.year))
          AS matchup_without_settings
    """)
    population_by_platform_year = con.execute("""
      SELECT platform, year,
             COUNT(*) FILTER (WHERE has_settings) AS settings_league_years,
             COUNT(*) FILTER (WHERE has_player_rows AND has_settings) AS player_league_years,
             COUNT(*) FILTER (WHERE has_matchup_rows) AS matchup_league_years,
             COUNT(*) FILTER (WHERE has_player_rows AND has_settings AND NOT has_matchup_rows)
               AS player_without_matchup,
             COUNT(*) FILTER (WHERE has_settings AND NOT has_player_rows)
               AS settings_without_player_rows,
             COUNT(*) FILTER (WHERE has_matchup_rows AND NOT has_settings)
               AS matchup_without_settings
      FROM population_detail
      GROUP BY platform, year
      ORDER BY platform, year
    """).fetchall()
    population_by_platform_year_cols = [d[0] for d in con.description]
    missing_matchup = con.execute("""
      SELECT db_name, year, platform
      FROM population_detail
      WHERE has_settings AND has_player_rows AND NOT has_matchup_rows
      ORDER BY platform, year, db_name
    """).fetchall()
    missing_matchup_cols = [d[0] for d in con.description]
    settings_only = con.execute("""
      SELECT db_name, year, platform
      FROM population_detail
      WHERE has_settings AND NOT has_player_rows
      ORDER BY platform, year, db_name
    """).fetchall()
    settings_only_cols = [d[0] for d in con.description]

    # Keep this report at league-season grain.  It intentionally does not infer
    # a championship from the season champion marker.
    con.execute("""
      CREATE OR REPLACE TEMP TABLE ly AS
      SELECT CAST(db_name AS VARCHAR) AS db_name,
             CAST(year AS INTEGER) AS year,
             CASE
               WHEN lower(CAST(db_name AS VARCHAR)) LIKE 'smpl_mfl_%' THEN 'mfl'
               WHEN lower(CAST(db_name AS VARCHAR)) LIKE 'smpl_ffl_%' THEN 'fleaflicker'
               WHEN lower(CAST(db_name AS VARCHAR)) LIKE 'smpl_slpr_%'
                 OR lower(CAST(db_name AS VARCHAR)) LIKE 'smpl_sleeper_%' THEN 'sleeper'
               ELSE 'other'
             END AS platform,
             COUNT(*) AS matchup_rows,
             COUNT(*) FILTER (WHERE CAST(is_playoffs AS INTEGER)=1) AS playoff_rows,
             COUNT(*) FILTER (WHERE CAST(is_championship AS INTEGER)=1) AS championship_rows,
             COUNT(*) FILTER (WHERE CAST(champion AS INTEGER)=1) AS season_champion_rows,
             MIN(CAST(week AS INTEGER)) FILTER (WHERE CAST(is_playoffs AS INTEGER)=1) AS first_playoff_week,
             MAX(CAST(week AS INTEGER)) FILTER (WHERE CAST(is_playoffs AS INTEGER)=1) AS last_playoff_week,
             COUNT(DISTINCT CAST(week AS INTEGER)) FILTER (WHERE CAST(is_playoffs AS INTEGER)=1) AS playoff_weeks
      FROM lake.public.matchup
      GROUP BY ALL
    """)
    summary = con.execute("""
      SELECT platform, year,
             COUNT(*) AS league_years,
             COUNT(*) FILTER (WHERE playoff_rows > 0) AS playoff_league_years,
             COUNT(*) FILTER (WHERE championship_rows > 0) AS championship_league_years,
             COUNT(*) FILTER (WHERE playoff_rows > 0 AND championship_rows = 0) AS playoff_without_championship,
             COUNT(*) FILTER (WHERE championship_rows > 0 AND playoff_rows = 0) AS championship_without_playoff,
             COUNT(*) FILTER (WHERE playoff_rows > 0 AND championship_rows = 0 AND season_champion_rows > 0) AS gap_with_season_champion_marker,
             COUNT(*) FILTER (WHERE playoff_rows > 0 AND championship_rows = 0 AND season_champion_rows = 0) AS gap_without_any_champion_marker
      FROM ly
      GROUP BY ALL
      ORDER BY platform, year
    """).fetchall()
    summary_cols = [
        "platform", "year", "league_years", "playoff_league_years",
        "championship_league_years", "playoff_without_championship",
        "championship_without_playoff", "gap_with_season_champion_marker",
        "gap_without_any_champion_marker",
    ]
    gap_examples = con.execute("""
      SELECT * FROM ly
      WHERE playoff_rows > 0 AND championship_rows = 0
      ORDER BY year, platform, db_name
      LIMIT 250
    """).fetchall()
    gap_cols = [d[0] for d in con.description]

    # Explicit championship rows should be at the latest playoff week for the
    # same team. This identifies inference/identity errors without using manager
    # names as a primary key.
    team = "CAST(franchise_id AS VARCHAR)" if "franchise_id" in cols else "CAST(team_key AS VARCHAR)" if "team_key" in cols else "CAST(manager AS VARCHAR)"
    premature = con.execute(f"""
      WITH ch AS (
        SELECT db_name, CAST(year AS INTEGER) AS season_year, {team} team_id,
               MAX(CAST(week AS INTEGER)) AS championship_week
        FROM lake.public.matchup
        WHERE CAST(is_championship AS INTEGER)=1
        GROUP BY ALL
      ), lp AS (
        SELECT db_name, CAST(year AS INTEGER) AS season_year, {team} team_id,
               MAX(CAST(week AS INTEGER)) FILTER (WHERE CAST(is_playoffs AS INTEGER)=1) AS last_playoff_week
        FROM lake.public.matchup
        GROUP BY ALL
      )
      SELECT ch.db_name, ch.season_year, ch.team_id, ch.championship_week, lp.last_playoff_week
      FROM ch JOIN lp USING (db_name, season_year, team_id)
      WHERE lp.last_playoff_week > ch.championship_week
      ORDER BY ch.season_year, ch.db_name, ch.team_id
    """).fetchall()
    premature_cols = [d[0] for d in con.description]
    population_row = con.execute("SELECT * FROM population_sets").fetchone()
    population_cols = [d[0] for d in con.description]
    result = {
        "population_coverage": dict(zip(population_cols, population_row)),
        "population_coverage_by_platform_year": [
            dict(zip(population_by_platform_year_cols, row))
            for row in population_by_platform_year
        ],
        "player_league_years_missing_matchup": [
            dict(zip(missing_matchup_cols, row)) for row in missing_matchup
        ],
        "settings_only_league_years": [
            dict(zip(settings_only_cols, row)) for row in settings_only
        ],
        "total_league_years": int(con.execute("SELECT COUNT(*) FROM ly").fetchone()[0]),
        "playoff_league_years": int(con.execute("SELECT COUNT(*) FROM ly WHERE playoff_rows > 0").fetchone()[0]),
        "championship_league_years": int(con.execute("SELECT COUNT(*) FROM ly WHERE championship_rows > 0").fetchone()[0]),
        "playoff_without_championship": int(con.execute("SELECT COUNT(*) FROM ly WHERE playoff_rows > 0 AND championship_rows = 0").fetchone()[0]),
        "championship_without_playoff": int(con.execute("SELECT COUNT(*) FROM ly WHERE championship_rows > 0 AND playoff_rows = 0").fetchone()[0]),
        "summary_by_platform_year": [dict(zip(summary_cols, row)) for row in summary],
        "gap_examples": [dict(zip(gap_cols, row)) for row in gap_examples],
        "premature_team_anchors": [dict(zip(premature_cols, row)) for row in premature],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if not isinstance(v, list)}, indent=2))
    con.close()


if __name__ == "__main__":
    main()
