"""Scan GH source rows for matchup outcomes lost by the strict franchise join."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--years", nargs="+", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute(
        f"ATTACH '{(args.root / 'corpus_snapshot.duckdb').resolve().as_posix()}' AS lake (READ_ONLY)"
    )
    pf = columns(con, "lake.public.player_fantasy")
    mu = columns(con, "lake.public.matchup")
    ls = columns(con, "lake.public.league_settings")
    required_pf = {"db_name", "year", "week", "NFL_player_id", "is_started", "manager"}
    if not required_pf <= pf:
        raise SystemExit("player_fantasy lacks required identity/start columns")
    if not {"db_name", "year", "week", "manager"} <= mu:
        raise SystemExit("matchup lacks required identity columns")

    years = ",".join(str(y) for y in sorted(set(args.years)))
    pf_outcome = [f'f."{c}" IS NOT NULL' for c in ("win", "loss", "tie") if c in pf]
    if {"team_points", "opponent_points"} <= pf:
        pf_outcome.append('f."team_points" IS NOT NULL AND f."opponent_points" IS NOT NULL')
    mu_outcome = [f'm."{c}" IS NOT NULL' for c in ("win", "loss", "tie") if c in mu]
    if {"opponent", "opponent_points"} <= mu:
        mu_outcome.append('m."opponent" IS NOT NULL AND m."opponent_points" IS NOT NULL')
    if not mu_outcome:
        raise SystemExit("matchup has no recognized outcome fields")
    mu_outcome_sql = " OR ".join(mu_outcome)
    has_franchise = "franchise_id" in pf and "franchise_id" in mu
    has_rostered = "is_rostered" in pf
    points_col = next((c for c in ("fantasy_points", "points") if c in pf), None)
    points_null_expr = f"f.\"{points_col}\" IS NULL" if points_col else "FALSE"
    points_nonzero_expr = (
        f"NOT ({points_null_expr}) AND TRY_CAST(f.\"{points_col}\" AS DOUBLE) <> 0"
        if points_col else "FALSE"
    )
    team_setting = next((c for c in ("teams", "num_teams", "team_count") if c in ls), None)
    if team_setting:
        settings_cte = f"""
      settings AS (
        SELECT CAST(db_name AS VARCHAR) AS db_name, CAST(year AS INTEGER) AS year,
               MAX(TRY_CAST(\"{team_setting}\" AS INTEGER)) AS expected_teams
        FROM lake.public.league_settings
        WHERE CAST(year AS INTEGER) IN ({years})
        GROUP BY 1,2
      ),
"""
        team_check_join = "LEFT JOIN team_check tc USING (db_name, year)"
    else:
        settings_cte = """
      settings AS (
        SELECT CAST(NULL AS VARCHAR) AS db_name, CAST(NULL AS INTEGER) AS year,
               CAST(NULL AS INTEGER) AS expected_teams
        WHERE FALSE
      ),
"""
        team_check_join = "LEFT JOIN team_check tc USING (db_name, year)"
    rostered_expr = (
        "CAST(f.is_rostered AS INTEGER)=1" if has_rostered
        else "f.manager IS NOT NULL AND LOWER(TRIM(CAST(f.manager AS VARCHAR))) NOT IN ('unrostered','fa','free agent','waivers','')"
    )
    unrostered_expr = f"NOT ({rostered_expr})"
    manager_key_f = "LOWER(TRIM(CAST(f.manager AS VARCHAR)))"
    manager_key_m = "LOWER(TRIM(CAST(m.manager AS VARCHAR)))"
    franchise_f = 'CAST(f.franchise_id AS VARCHAR)' if "franchise_id" in pf else "NULL"

    strict_cte = (
        ", strict_outcomes AS ("
        "SELECT DISTINCT CAST(m.db_name AS VARCHAR) AS db_name, CAST(m.year AS INTEGER) AS year, "
        "CAST(m.week AS INTEGER) AS week, "
        f"{manager_key_m} AS manager_key, CAST(m.franchise_id AS VARCHAR) AS franchise_id "
        "FROM lake.public.matchup m "
        f"WHERE CAST(m.year AS INTEGER) IN ({years}) AND ({mu_outcome_sql})"
        ")"
        if has_franchise else ""
    )
    strict_join = (
        "LEFT JOIN strict_outcomes so ON so.db_name=p.db_name AND so.year=p.year "
        "AND so.week=p.week AND so.manager_key=p.manager_key "
        "AND so.franchise_id IS NOT DISTINCT FROM p.franchise_id"
        if has_franchise else ""
    )
    strict_select = "so.db_name" if has_franchise else "mo.db_name"
    query = f"""
      WITH player_rows AS (
        SELECT CAST(f.db_name AS VARCHAR) AS db_name, CAST(f.year AS INTEGER) AS year,
               CAST(f.week AS INTEGER) AS week, CAST(f.NFL_player_id AS VARCHAR) AS NFL_player_id,
               {manager_key_f} AS manager_key, {franchise_f} AS franchise_id,
               ({' OR '.join(pf_outcome) or 'FALSE'}) AS direct_outcome,
               ROW_NUMBER() OVER (
                 PARTITION BY f.db_name, f.year, f.week, f.NFL_player_id
                 ORDER BY CAST(f.is_started AS INTEGER) DESC,
                          {('f.fantasy_points DESC' if 'fantasy_points' in pf else '1')}
               ) AS rn
        FROM lake.public.player_fantasy f
        WHERE CAST(f.year AS INTEGER) IN ({years})
          AND CAST(f.is_started AS INTEGER)=1
          AND f.NFL_player_id IS NOT NULL
          AND f.manager IS NOT NULL
      ),
      players AS (SELECT * FROM player_rows WHERE rn=1),
      capture AS (
        SELECT CAST(f.db_name AS VARCHAR) AS db_name,
               CAST(f.year AS INTEGER) AS year,
               COUNT(*) AS all_player_rows,
               COUNT(DISTINCT CAST(f.week AS INTEGER) || ':' || CAST(f.NFL_player_id AS VARCHAR))
                 AS distinct_player_week_rows,
               COUNT(*) - COUNT(DISTINCT CAST(f.week AS INTEGER) || ':' || CAST(f.NFL_player_id AS VARCHAR))
                 AS duplicate_player_week_rows,
               COUNT(DISTINCT CAST(f.week AS INTEGER) || ':' || CAST(f.NFL_player_id AS VARCHAR) || ':' ||
                     COALESCE(LOWER(TRIM(CAST(f.manager AS VARCHAR))), ''))
                 AS distinct_player_team_week_rows,
               COUNT(*) - COUNT(DISTINCT CAST(f.week AS INTEGER) || ':' || CAST(f.NFL_player_id AS VARCHAR) || ':' ||
                     COALESCE(LOWER(TRIM(CAST(f.manager AS VARCHAR))), ''))
                 AS duplicate_player_team_week_rows,
               COUNT(*) FILTER (WHERE {rostered_expr}) AS rostered_player_rows,
               COUNT(*) FILTER (WHERE {unrostered_expr}) AS unrostered_player_rows,
               COUNT(*) FILTER (WHERE {rostered_expr} AND CAST(f.is_started AS INTEGER)=1)
                 AS rostered_started_rows,
               COUNT(*) FILTER (WHERE {rostered_expr} AND CAST(f.is_started AS INTEGER)=0)
                 AS rostered_bench_rows,
               COUNT(*) FILTER (WHERE f.manager IS NULL OR TRIM(CAST(f.manager AS VARCHAR))='')
                 AS null_manager_rows,
               COUNT(*) FILTER (WHERE f.NFL_player_id IS NULL) AS null_nfl_player_id_rows,
               COUNT(*) FILTER (WHERE f.NFL_player_id IS NULL
                                      AND CAST(f.is_started AS INTEGER)=1)
                 AS null_nfl_started_rows,
               COUNT(*) FILTER (WHERE f.NFL_player_id IS NULL
                                      AND ({points_nonzero_expr}))
                 AS null_nfl_nonzero_points_rows,
               COUNT(*) FILTER (WHERE {points_null_expr}) AS null_points_rows
        FROM lake.public.player_fantasy f
        WHERE CAST(f.year AS INTEGER) IN ({years})
        GROUP BY 1,2
      ),
      {settings_cte}
      team_counts AS (
        SELECT CAST(f.db_name AS VARCHAR) AS db_name,
               CAST(f.year AS INTEGER) AS year,
               CAST(f.week AS INTEGER) AS week,
               COUNT(DISTINCT LOWER(TRIM(CAST(f.manager AS VARCHAR)))) AS observed_teams
        FROM lake.public.player_fantasy f
        WHERE CAST(f.year AS INTEGER) IN ({years})
          AND f.NFL_player_id IS NOT NULL
        GROUP BY 1,2,3
      ),
      team_check AS (
        SELECT tc.db_name, tc.year,
               COUNT(*) AS team_count_checked_weeks,
               COUNT(*) FILTER (WHERE s.expected_teams IS NOT NULL) AS team_count_known_weeks,
               COUNT(*) FILTER (WHERE s.expected_teams IS NOT NULL
                                      AND tc.observed_teams <> s.expected_teams)
                 AS team_count_mismatch_weeks
        FROM team_counts tc
        LEFT JOIN settings s USING (db_name, year)
        GROUP BY 1,2
      ),
      matchup_capture AS (
        SELECT CAST(m.db_name AS VARCHAR) AS db_name,
               CAST(m.year AS INTEGER) AS year,
               COUNT(*) AS matchup_rows,
               COUNT(*) FILTER (WHERE {mu_outcome_sql}) AS matchup_outcome_rows
        FROM lake.public.matchup m
        WHERE CAST(m.year AS INTEGER) IN ({years})
        GROUP BY 1,2
      ),
      manager_outcomes AS (
        SELECT DISTINCT CAST(m.db_name AS VARCHAR) AS db_name, CAST(m.year AS INTEGER) AS year,
               CAST(m.week AS INTEGER) AS week, {manager_key_m} AS manager_key
        FROM lake.public.matchup m
        WHERE CAST(m.year AS INTEGER) IN ({years}) AND ({mu_outcome_sql})
      ){strict_cte},
      joined AS (
        SELECT p.*, mo.db_name AS manager_outcome_db, {strict_select} AS strict_outcome_db
        FROM players p
        LEFT JOIN manager_outcomes mo
          ON mo.db_name=p.db_name AND mo.year=p.year AND mo.week=p.week
         AND mo.manager_key=p.manager_key
        {strict_join}
      )
      SELECT db_name, year,
             c.all_player_rows, c.distinct_player_week_rows,
             c.duplicate_player_week_rows, c.rostered_player_rows,
             c.distinct_player_team_week_rows, c.duplicate_player_team_week_rows,
             c.unrostered_player_rows, c.rostered_started_rows,
             c.rostered_bench_rows, c.null_manager_rows,
             c.null_nfl_player_id_rows, c.null_nfl_started_rows,
             c.null_nfl_nonzero_points_rows, c.null_points_rows,
             COALESCE(tc.team_count_checked_weeks, 0) AS team_count_checked_weeks,
             COALESCE(tc.team_count_known_weeks, 0) AS team_count_known_weeks,
             COALESCE(tc.team_count_mismatch_weeks, 0) AS team_count_mismatch_weeks,
             COALESCE(mc.matchup_rows, 0) AS matchup_rows,
             COALESCE(mc.matchup_outcome_rows, 0) AS matchup_outcome_rows,
             COUNT(*) AS started_rows,
             COUNT(*) FILTER (WHERE direct_outcome) AS direct_outcome_rows,
             COUNT(*) FILTER (WHERE manager_outcome_db IS NOT NULL) AS manager_joined_rows,
             COUNT(*) FILTER (WHERE strict_outcome_db IS NOT NULL) AS strict_joined_rows,
             COUNT(*) FILTER (WHERE direct_outcome OR manager_outcome_db IS NOT NULL)
               AS combined_outcome_rows,
             COUNT(*) FILTER (WHERE manager_outcome_db IS NOT NULL AND strict_outcome_db IS NULL)
               AS recovered_by_manager_fallback_rows,
             COUNT(*) FILTER (WHERE NOT direct_outcome AND manager_outcome_db IS NULL)
               AS no_combined_outcome_rows
      FROM joined
      JOIN capture c USING (db_name, year)
      {team_check_join}
      LEFT JOIN matchup_capture mc USING (db_name, year)
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
      ORDER BY 2,1
    """
    rows = con.execute(query).fetchall()
    names = [d[0] for d in con.description]
    detail = [dict(zip(names, row)) for row in rows]
    identity_rescue = [
        r for r in detail
        if r["null_nfl_started_rows"] or r["null_nfl_nonzero_points_rows"]
    ]
    summary = {
        "years": sorted(set(args.years)),
        "league_years": len(detail),
        "all_player_rows": sum(r["all_player_rows"] for r in detail),
        "distinct_player_week_rows": sum(r["distinct_player_week_rows"] for r in detail),
        "duplicate_player_week_rows": sum(r["duplicate_player_week_rows"] for r in detail),
        "distinct_player_team_week_rows": sum(r["distinct_player_team_week_rows"] for r in detail),
        "duplicate_player_team_week_rows": sum(r["duplicate_player_team_week_rows"] for r in detail),
        "rostered_player_rows": sum(r["rostered_player_rows"] for r in detail),
        "unrostered_player_rows": sum(r["unrostered_player_rows"] for r in detail),
        "rostered_started_rows": sum(r["rostered_started_rows"] for r in detail),
        "rostered_bench_rows": sum(r["rostered_bench_rows"] for r in detail),
        "null_manager_rows": sum(r["null_manager_rows"] for r in detail),
        "null_nfl_player_id_rows": sum(r["null_nfl_player_id_rows"] for r in detail),
        "null_nfl_started_rows": sum(r["null_nfl_started_rows"] for r in detail),
        "null_nfl_nonzero_points_rows": sum(r["null_nfl_nonzero_points_rows"] for r in detail),
        "null_points_rows": sum(r["null_points_rows"] for r in detail),
        "team_count_checked_weeks": sum(r["team_count_checked_weeks"] for r in detail),
        "team_count_known_weeks": sum(r["team_count_known_weeks"] for r in detail),
        "team_count_mismatch_weeks": sum(r["team_count_mismatch_weeks"] for r in detail),
        "matchup_rows": sum(r["matchup_rows"] for r in detail),
        "matchup_outcome_rows": sum(r["matchup_outcome_rows"] for r in detail),
        "matchup_rows_without_outcomes": sum(
            r["matchup_rows"] - r["matchup_outcome_rows"] for r in detail
        ),
        "started_rows": sum(r["started_rows"] for r in detail),
        "direct_outcome_rows": sum(r["direct_outcome_rows"] for r in detail),
        "manager_joined_rows": sum(r["manager_joined_rows"] for r in detail),
        "strict_joined_rows": sum(r["strict_joined_rows"] for r in detail),
        "combined_outcome_rows": sum(r["combined_outcome_rows"] for r in detail),
        "recovered_by_manager_fallback_rows": sum(r["recovered_by_manager_fallback_rows"] for r in detail),
        "no_combined_outcome_rows": sum(r["no_combined_outcome_rows"] for r in detail),
        "franchise_strict_lane_available": has_franchise,
        "rows": detail,
        "identity_rescue_league_years": len(identity_rescue),
        "identity_rescue_started_rows": sum(r["null_nfl_started_rows"] for r in identity_rescue),
        "identity_rescue_nonzero_points_rows": sum(
            r["null_nfl_nonzero_points_rows"] for r in identity_rescue
        ),
        "identity_rescue": identity_rescue,
    }
    args.out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k not in {"rows", "identity_rescue"}}, sort_keys=True))


if __name__ == "__main__":
    main()
