"""Audit whether championship flags occur before a champion team's last playoff week."""
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
    con.execute(f"ATTACH '{args.base.resolve().as_posix().replace(chr(39), chr(39) * 2)}' AS base (READ_ONLY)")

    mcols = {r[0] for r in con.execute("DESCRIBE base.public.matchup").fetchall()}
    pcols = {r[0] for r in con.execute("DESCRIBE base.public.player_fantasy").fetchall()}
    required = {"db_name", "year", "week", "manager", "champion", "is_playoffs"}
    if not required <= mcols:
        raise SystemExit(f"matchup missing {sorted(required - mcols)}")

    team_m = "CAST(team_key AS VARCHAR)" if "team_key" in mcols else "NULL::VARCHAR"
    franchise_m = "CAST(franchise_id AS VARCHAR)" if "franchise_id" in mcols else "NULL::VARCHAR"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE flagged AS
      SELECT DISTINCT CAST(db_name AS VARCHAR) db_name, CAST(year AS INTEGER) season_year,
             LOWER(TRIM(CAST(manager AS VARCHAR))) manager_key,
             {team_m} team_key, {franchise_m} franchise_id,
             CAST(week AS INTEGER) flagged_week
      FROM base.public.matchup
      WHERE CAST(champion AS INTEGER)=1 AND CAST(is_playoffs AS INTEGER)=1
    """)
    matchup_flagged = con.execute("SELECT COUNT(*) FROM flagged").fetchone()[0]

    # Older folds can carry champion only on player rows. Join those rows to the
    # matchup graph, preserving the matchup-side team identity.
    player_sources = 0
    if "champion" in pcols and "is_rostered" in pcols:
        pteam = "CAST(p.team_key AS VARCHAR)" if "team_key" in pcols else "NULL::VARCHAR"
        join = ("(p.team_key IS NOT NULL AND m.team_key IS NOT DISTINCT FROM p.team_key) "
                "OR LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(CAST(p.manager AS VARCHAR)))") \
            if "team_key" in pcols and "team_key" in mcols else \
            "LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(CAST(p.manager AS VARCHAR)))"
        mteam_join = "CAST(m.team_key AS VARCHAR)" if "team_key" in mcols else "NULL::VARCHAR"
        mfranchise_join = "CAST(m.franchise_id AS VARCHAR)" if "franchise_id" in mcols else "NULL::VARCHAR"
        con.execute(f"""
          INSERT INTO flagged
          SELECT DISTINCT CAST(m.db_name AS VARCHAR), CAST(m.year AS INTEGER),
                 LOWER(TRIM(CAST(m.manager AS VARCHAR))),
                 {mteam_join}, {mfranchise_join},
                 CAST(m.week AS INTEGER)
          FROM base.public.player_fantasy p
          JOIN base.public.matchup m
            ON m.db_name=p.db_name AND CAST(m.year AS INTEGER)=CAST(p.year AS INTEGER)
           AND CAST(m.week AS INTEGER)=CAST(p.week AS INTEGER) AND ({join})
          WHERE CAST(p.champion AS INTEGER)=1 AND CAST(p.is_rostered AS INTEGER)=1
            AND CAST(m.is_playoffs AS INTEGER)=1
        """)
        player_sources = con.execute("SELECT COUNT(*) FROM flagged").fetchone()[0] - matchup_flagged

    # Use the same identity precedence as the repair logic: franchise/team key,
    # then manager. The audit intentionally reports both strict and fallback views.
    con.execute("""
      CREATE OR REPLACE TEMP TABLE grouped AS
      SELECT db_name, season_year, manager_key,
             MAX(flagged_week) flagged_week,
             MAX(team_key) team_key,
             MAX(franchise_id) franchise_id
      FROM flagged
      GROUP BY ALL
    """)
    team_expr = "CAST(team_key AS VARCHAR)" if "team_key" in mcols else "NULL::VARCHAR"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE last_playoff AS
      SELECT CAST(db_name AS VARCHAR) db_name, CAST(year AS INTEGER) season_year,
             LOWER(TRIM(CAST(manager AS VARCHAR))) manager_key,
             {team_expr} team_key, MAX(CAST(week AS INTEGER)) last_playoff_week
      FROM base.public.matchup
      WHERE CAST(is_playoffs AS INTEGER)=1
      GROUP BY ALL
    """)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE last_playoff_manager AS
      SELECT CAST(db_name AS VARCHAR) db_name, CAST(year AS INTEGER) season_year,
             LOWER(TRIM(CAST(manager AS VARCHAR))) manager_key,
             MAX(CAST(week AS INTEGER)) last_playoff_week
      FROM base.public.matchup
      WHERE CAST(is_playoffs AS INTEGER)=1
      GROUP BY ALL
    """)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE affected AS
      SELECT g.*, p.last_playoff_week
      FROM grouped g
      JOIN last_playoff p USING (db_name, season_year, manager_key, team_key)
      WHERE p.last_playoff_week > g.flagged_week
    """)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE affected_manager AS
      SELECT g.*, p.last_playoff_week
      FROM grouped g
      JOIN last_playoff_manager p USING (db_name, season_year, manager_key)
      WHERE p.last_playoff_week > g.flagged_week
    """)
    counts = con.execute("""
      SELECT
        (SELECT COUNT(*) FROM grouped) flagged_team_keys,
        (SELECT COUNT(*) FROM affected) prematurely_anchored_team_keys,
        (SELECT COUNT(DISTINCT db_name || ':' || season_year) FROM grouped) flagged_league_seasons,
        (SELECT COUNT(DISTINCT db_name || ':' || season_year) FROM affected) affected_league_seasons,
        (SELECT COUNT(*) FROM affected_manager) prematurely_anchored_manager_keys,
        (SELECT COUNT(DISTINCT db_name || ':' || season_year) FROM affected_manager) affected_manager_league_seasons,
        (SELECT COUNT(*) FROM last_playoff) playoff_team_keys
    """).fetchone()
    examples = con.execute("""
      SELECT db_name, season_year, manager_key, flagged_week, last_playoff_week
      FROM affected ORDER BY season_year, db_name, manager_key LIMIT 100
    """).fetchall()
    manager_examples = con.execute("""
      SELECT db_name, season_year, manager_key, flagged_week, last_playoff_week
      FROM affected_manager ORDER BY season_year, db_name, manager_key LIMIT 100
    """).fetchall()
    result = {
        **dict(zip(["flagged_team_keys", "prematurely_anchored_team_keys",
                    "flagged_league_seasons", "affected_league_seasons",
                    "prematurely_anchored_manager_keys", "affected_manager_league_seasons",
                    "playoff_team_keys"], counts)),
        "matchup_flagged_rows": matchup_flagged,
        "player_only_sources_added": player_sources,
        "examples": [dict(zip(["db_name", "year", "manager", "flagged_week", "last_playoff_week"], row)) for row in examples],
        "manager_examples": [dict(zip(["db_name", "year", "manager", "flagged_week", "last_playoff_week"], row)) for row in manager_examples],
    }
    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))
    con.close()


if __name__ == "__main__":
    main()
