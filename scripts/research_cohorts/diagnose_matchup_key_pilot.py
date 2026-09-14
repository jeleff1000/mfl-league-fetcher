"""Inspect one league-year's player/matchup join keys in the GH public lake."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def cols(con, table: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--db-name", required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    con = duckdb.connect()
    con.execute(f"ATTACH '{(args.root / 'corpus_snapshot.duckdb').resolve().as_posix()}' AS lake (READ_ONLY)")
    pf = cols(con, "lake.public.player_fantasy")
    mu = cols(con, "lake.public.matchup")
    # Keep the public-cache schema visible in the pilot artifact so an unresolved
    # player row can be classified without downloading or inspecting the lake locally.
    started = "CAST(f.is_started AS INTEGER)=1" if "is_started" in pf else "FALSE"
    rostered = "CAST(f.is_rostered AS INTEGER)=1" if "is_rostered" in pf else "FALSE"
    unrostered = "CAST(f.is_rostered AS INTEGER)<>1" if "is_rostered" in pf else "FALSE"
    clutch = "f.clutch_equity IS NOT NULL" if "clutch_equity" in pf else "FALSE"
    direct_outcome = " OR ".join(f"{c} IS NOT NULL" for c in ("win", "loss", "tie") if c in pf) or "FALSE"
    outcome = " OR ".join(f"m.{c} IS NOT NULL" for c in ("win", "loss", "tie") if c in mu)
    if {"opponent", "opponent_points"} <= mu:
        outcome += (" OR " if outcome else "") + "(m.opponent IS NOT NULL AND m.opponent_points IS NOT NULL)"
    outcome = outcome or "FALSE"
    base = "f.db_name=? AND CAST(f.year AS INTEGER)=?"
    mbase = "m.db_name=? AND CAST(m.year AS INTEGER)=?"
    join = ["CAST(m.week AS INTEGER)=CAST(f.week AS INTEGER)"]
    if "manager" in pf and "manager" in mu:
        join.append("LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(CAST(f.manager AS VARCHAR)))")
    manager_join_sql = " AND ".join(join)
    strict_join = list(join)
    if "franchise_id" in pf and "franchise_id" in mu:
        strict_join.append("m.franchise_id IS NOT DISTINCT FROM f.franchise_id")
    strict_join_sql = " AND ".join(strict_join)
    q = f"""
      SELECT
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base}) AS player_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {rostered}) AS rostered_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {unrostered}) AS unrostered_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {rostered} AND {started}) AS rostered_started_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {rostered} AND NOT ({started})) AS rostered_bench_rows,
        (SELECT COALESCE(SUM(n-1),0) FROM (
          SELECT CAST(f.week AS INTEGER) AS week, CAST(f.NFL_player_id AS VARCHAR) AS player_id,
                 COUNT(*) AS n
          FROM lake.public.player_fantasy f WHERE {base}
          GROUP BY 1,2 HAVING COUNT(*)>1
        )) AS duplicate_player_week_rows,
        (SELECT COALESCE(SUM(n-1),0) FROM (
          SELECT CAST(f.week AS INTEGER) AS week, CAST(f.NFL_player_id AS VARCHAR) AS player_id,
                 LOWER(TRIM(CAST(f.manager AS VARCHAR))) AS manager_key, COUNT(*) AS n
          FROM lake.public.player_fantasy f WHERE {base}
          GROUP BY 1,2,3 HAVING COUNT(*)>1
        )) AS duplicate_player_team_week_rows,
        (SELECT COUNT(*) FROM (
          SELECT CAST(f.week AS INTEGER) AS week, CAST(f.NFL_player_id AS VARCHAR) AS player_id
          FROM lake.public.player_fantasy f WHERE {base}
          GROUP BY 1,2 HAVING COUNT(DISTINCT LOWER(TRIM(CAST(f.manager AS VARCHAR))))>1
        )) AS multi_manager_player_week_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {started}) AS started_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {started} AND {clutch}) AS clutch_started_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f WHERE {base} AND {started} AND ({direct_outcome})) AS direct_outcome_started_rows,
        (SELECT COUNT(*) FROM lake.public.matchup m WHERE {mbase}) AS matchup_rows,
        (SELECT COUNT(*) FROM lake.public.matchup m WHERE {mbase} AND ({outcome})) AS matchup_outcome_rows,
        (SELECT COUNT(*) FROM lake.public.player_fantasy f JOIN lake.public.matchup m
          ON m.db_name=f.db_name AND CAST(m.year AS INTEGER)=CAST(f.year AS INTEGER) AND {strict_join_sql}
          WHERE {base} AND {started} AND ({outcome})) AS started_with_joined_outcome
        ,(SELECT COUNT(*) FROM lake.public.player_fantasy f JOIN lake.public.matchup m
          ON m.db_name=f.db_name AND CAST(m.year AS INTEGER)=CAST(f.year AS INTEGER) AND {manager_join_sql}
          WHERE {base} AND {started} AND ({outcome})) AS started_with_manager_joined_outcome
    """
    row = con.execute(q, [args.db_name, args.year] * 15).fetchone()
    result = dict(zip([d[0] for d in con.description], row))
    week_q = f"""
      SELECT CAST(f.week AS INTEGER) AS week,
             COUNT(*) FILTER (WHERE {started}) AS started_rows,
             COUNT(*) FILTER (WHERE {started} AND ({outcome})) AS joined_outcome_rows,
             COUNT(*) FILTER (WHERE {started} AND NOT ({outcome})) AS no_outcome_rows
      FROM lake.public.player_fantasy f
      LEFT JOIN lake.public.matchup m
        ON m.db_name=f.db_name AND CAST(m.year AS INTEGER)=CAST(f.year AS INTEGER) AND {manager_join_sql}
      WHERE {base}
      GROUP BY 1 ORDER BY 1
    """
    result["weeks"] = [dict(zip([d[0] for d in con.description], r)) for r in con.execute(week_q, [args.db_name, args.year]).fetchall()]
    points = "fantasy_points" if "fantasy_points" in pf else ("points" if "points" in pf else None)
    points_expr = f"COUNT(DISTINCT CAST(f.{points} AS VARCHAR)) AS distinct_points," if points else "0 AS distinct_points,"
    dup_q = f"""
      SELECT CAST(f.week AS INTEGER) AS week,
             CAST(f.NFL_player_id AS VARCHAR) AS player_id,
             LOWER(TRIM(CAST(f.manager AS VARCHAR))) AS manager_key,
             COUNT(*) AS row_count,
             COUNT(DISTINCT CAST(f.is_started AS INTEGER)) AS distinct_started,
             {points_expr}
             COUNT(DISTINCT CAST(f.is_rostered AS INTEGER)) AS distinct_rostered
      FROM lake.public.player_fantasy f
      WHERE {base}
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
      ORDER BY row_count DESC, week, player_id
      LIMIT 20
    """
    result["duplicate_groups"] = [
        dict(zip([d[0] for d in con.description], r))
        for r in con.execute(dup_q, [args.db_name, args.year]).fetchall()
    ]
    settings_cols = cols(con, "lake.public.league_settings")
    team_col = next((c for c in ("teams", "num_teams", "team_count") if c in settings_cols), None)
    if team_col:
        team_q = f"""
          WITH expected AS (
            SELECT MAX(TRY_CAST(\"{team_col}\" AS INTEGER)) AS expected_teams
            FROM lake.public.league_settings
            WHERE db_name=? AND CAST(year AS INTEGER)=?
          )
          SELECT CAST(f.week AS INTEGER) AS week,
                 COUNT(DISTINCT LOWER(TRIM(CAST(f.manager AS VARCHAR)))) AS observed_teams,
                 e.expected_teams
          FROM lake.public.player_fantasy f CROSS JOIN expected e
          WHERE {base}
          GROUP BY 1,3 ORDER BY 1
        """
        result["team_week_counts"] = [
            dict(zip([d[0] for d in con.description], r))
            for r in con.execute(team_q, [args.db_name, args.year] * 2).fetchall()
        ]
    preferred_identity_cols = (
        "manager", "week", "is_rostered", "is_started", "fantasy_points",
        "team_points", "win", "loss", "tie", "clutch_equity", "is_playoffs",
        "champion", "final_playoff_seed"
    )
    identity_cols = [c for c in preferred_identity_cols if c in pf]
    identity_cols = [c for c in identity_cols if c not in {"NFL_player_id", "db_name", "year", "week"}][:20]
    if identity_cols:
        select_cols = ", ".join(f'"{c}"' for c in identity_cols)
        null_q = f"""
          SELECT {select_cols}
          FROM lake.public.player_fantasy f
          WHERE {base} AND NFL_player_id IS NULL
          LIMIT 10
        """
        names = [d[0] for d in con.description] if False else identity_cols
        result["null_identity_columns"] = identity_cols
        result["null_identity_samples"] = [
            dict(zip(identity_cols, r))
            for r in con.execute(null_q, [args.db_name, args.year]).fetchall()
        ]
    result.update({
        "db_name": args.db_name,
        "year": args.year,
        "player_columns": sorted(pf),
        "matchup_columns": sorted(mu),
    })
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
