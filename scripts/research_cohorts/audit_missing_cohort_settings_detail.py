"""Detailed, read-only audit of missing cohort-setting evidence.

The output is an evidence ledger, not a promotion.  It works at league-year
grain and never treats a platform-wide pattern as proof for an individual
league-year.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def esc(p: Path) -> str:
    return str(p.resolve()).replace("'", "''")


def cols(con, ref):
    return {r[0] for r in con.execute(f"DESCRIBE {ref}").fetchall()}


def q(con, sql):
    return con.execute(sql).fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    con = duckdb.connect()
    con.execute(f"ATTACH '{esc(args.base)}' AS lake (READ_ONLY)")
    try:
        s = cols(con, "lake.public.league_settings")
        p = cols(con, "lake.public.player_fantasy")
        required = {"db_name", "year", "platform"}
        if not required <= s or not required <= p:
            raise SystemExit(f"missing required columns: settings={sorted(required-s)} player={sorted(required-p)}")
        # Every target is restricted to populated league-years.
        con.execute("""
          CREATE OR REPLACE TEMP TABLE targets AS
          SELECT s.db_name, CAST(s.year AS INTEGER) AS "year", s.platform,
                 s.scoring_pass_td, s.playoff_teams, s.sleeper_best_ball,
                 s.roster_IDP, s.roster_DL, s.roster_LB, s.roster_DB,
                 s.roster_DB_LB, s.roster_DL_LB, s.roster_SUPER_FLEX, s.roster_FLX
          FROM lake.public.league_settings s
          WHERE EXISTS (SELECT 1 FROM lake.public.player_fantasy p
                        WHERE p.db_name=s.db_name AND p.year=s.year)
        """)
        base = {r[0]: int(r[1]) for r in q(con, "SELECT 'league_years',COUNT(*) FROM targets")}
        # Missing-setting inventory and platform/year rollup.
        miss = {}
        for name, pred in {
            "pass_td": "scoring_pass_td IS NULL",
            "playoff_teams": "playoff_teams IS NULL",
            "roster_config": "roster_IDP IS NULL AND roster_DL IS NULL AND roster_LB IS NULL AND roster_DB IS NULL AND roster_DB_LB IS NULL AND roster_DL_LB IS NULL AND roster_SUPER_FLEX IS NULL AND roster_FLX IS NULL",
            "best_ball": "sleeper_best_ball IS NULL",
        }.items():
            miss[name] = {
                "total": int(q(con, f"SELECT COUNT(*) FROM targets WHERE {pred}")[0][0]),
                "by_platform": {str(r[0] or "NULL"): int(r[1]) for r in q(con, f"SELECT platform,COUNT(*) FROM targets WHERE {pred} GROUP BY 1 ORDER BY 1")},
                "by_year": {str(r[0]): int(r[1]) for r in q(con, f"SELECT year,COUNT(*) FROM targets WHERE {pred} GROUP BY 1 ORDER BY 1")},
            }

        detail = {}
        # Started-slot evidence for roster shape and lineup signatures.
        if {"week", "team_key", "fantasy_position", "is_started"} <= p:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE slot_evidence AS
              SELECT t.db_name,t.year,t.platform,
                COUNT(DISTINCT p.week) week_count,
                COUNT(DISTINCT CASE WHEN p.is_started=1 THEN p.week END) started_week_count,
                MAX(CASE WHEN p.is_started=1 AND UPPER(TRIM(CAST(p.fantasy_position AS VARCHAR))) IN ('DL','LB','DB','IDP') THEN 1 ELSE 0 END) idp_slot,
                MAX(CASE WHEN p.is_started=1 AND UPPER(TRIM(CAST(p.fantasy_position AS VARCHAR))) IN ('SFLEX','SFLX','SUPERFLEX','SUPER_FLEX','SUPER FLEX','SUPER-FLEX') THEN 1 ELSE 0 END) sflex_slot,
                MAX(CASE WHEN p.is_started=1 AND UPPER(TRIM(CAST(p.fantasy_position AS VARCHAR))) IN ('FLEX','FLX','REC_FLEX','W/R','WR/RB','RB/WR','WRRB') THEN 1 ELSE 0 END) flex_slot,
                MAX(CASE WHEN p.is_started=1 AND UPPER(TRIM(CAST(p.fantasy_position AS VARCHAR)))='QB' THEN 1 ELSE 0 END) qb_slot,
                MAX(qb_count) max_qbs_started,
                COUNT(DISTINCT CASE WHEN p.is_started=1 THEN UPPER(TRIM(CAST(p.fantasy_position AS VARCHAR))) END) started_slot_types,
                COUNT(DISTINCT CASE WHEN p.is_started=1 THEN CONCAT(CAST(p.week AS VARCHAR),'|',CAST(p.team_key AS VARCHAR),'|',CAST(p.NFL_player_id AS VARCHAR)) END) started_cells
              FROM targets t JOIN lake.public.player_fantasy p USING (db_name,year)
              LEFT JOIN (
                SELECT db_name,year,week,COALESCE(NULLIF(CAST(team_key AS VARCHAR),''),NULLIF(CAST(team_name AS VARCHAR),''),NULLIF(CAST(manager AS VARCHAR),'')) team_id,COUNT(*) qb_count
                FROM lake.public.player_fantasy
                WHERE is_started=1 AND UPPER(TRIM(CAST(fantasy_position AS VARCHAR)))='QB'
                GROUP BY 1,2,3,4
              ) q ON q.db_name=p.db_name AND q.year=p.year AND q.week=p.week AND q.team_id=COALESCE(NULLIF(CAST(p.team_key AS VARCHAR),''),NULLIF(CAST(p.team_name AS VARCHAR),''),NULLIF(CAST(p.manager AS VARCHAR),''))
              WHERE t.roster_IDP IS NULL AND t.roster_DL IS NULL AND t.roster_LB IS NULL
                AND t.roster_DB IS NULL AND t.roster_DB_LB IS NULL AND t.roster_DL_LB IS NULL
                AND t.roster_SUPER_FLEX IS NULL AND t.roster_FLX IS NULL
              GROUP BY 1,2,3
            """)
            detail["roster"] = {
                "by_evidence": {str(r[0]): int(r[1]) for r in q(con, """
                  SELECT CASE WHEN idp_slot=1 THEN 'idp'
                              WHEN sflex_slot=1 OR max_qbs_started>=2 THEN 'sflx'
                              WHEN flex_slot=1 THEN 'flx'
                              ELSE 'unresolved' END, COUNT(*)
                  FROM slot_evidence GROUP BY 1 ORDER BY 1
                """)},
                "by_platform": {str(r[0] or 'NULL'): int(r[1]) for r in q(con, "SELECT platform,COUNT(*) FROM slot_evidence GROUP BY 1 ORDER BY 1")},
                "rows": [dict(zip(("db_name","year","platform","week_count","started_week_count","idp_slot","sflex_slot","flex_slot","qb_slot","max_qbs_started","started_slot_types","started_cells"), r)) for r in q(con, "SELECT * FROM slot_evidence ORDER BY platform,year,db_name")],
            }
            detail["started_slot_domain"] = {str(r[0] or 'NULL'): int(r[1]) for r in q(con, """
              SELECT UPPER(TRIM(CAST(fantasy_position AS VARCHAR))),COUNT(*)
              FROM lake.public.player_fantasy p JOIN targets t USING(db_name,year)
              WHERE t.roster_IDP IS NULL AND t.roster_DL IS NULL AND t.roster_LB IS NULL
                AND t.roster_DB IS NULL AND t.roster_DB_LB IS NULL AND t.roster_DL_LB IS NULL
                AND t.roster_SUPER_FLEX IS NULL AND t.roster_FLX IS NULL AND p.is_started=1
              GROUP BY 1 ORDER BY 2 DESC
            """)}

        # Best-ball evidence: a changed started-player set is recorded per
        # league-year.  It is not silently treated as proof of best ball.
        if {"week", "team_key", "NFL_player_id", "is_started"} <= p:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE lineup_evidence AS
              WITH cells AS (
                SELECT t.db_name,t.year,t.platform,p.week,p.team_key,
                       STRING_AGG(CAST(p.NFL_player_id AS VARCHAR),',' ORDER BY p.NFL_player_id) lineup
                FROM targets t JOIN lake.public.player_fantasy p USING(db_name,year)
                WHERE t.sleeper_best_ball IS NULL AND p.is_started=1
                GROUP BY 1,2,3,4,5
              )
              SELECT db_name,year,platform,COUNT(DISTINCT week) week_count,
                     COUNT(DISTINCT CONCAT(CAST(week AS VARCHAR),'|',CAST(team_key AS VARCHAR))) team_weeks,
                     COUNT(DISTINCT lineup) distinct_lineups,
                     CASE WHEN COUNT(DISTINCT lineup)>1 THEN 1 ELSE 0 END changed_started_set
              FROM cells GROUP BY 1,2,3
            """)
            detail["best_ball"] = {
                "by_evidence": {str(r[0]): int(r[1]) for r in q(con, "SELECT CASE WHEN changed_started_set=1 THEN 'changed_started_set' ELSE 'stable_or_empty' END,COUNT(*) FROM lineup_evidence GROUP BY 1")},
                "by_platform": {str(r[0] or 'NULL'): int(r[1]) for r in q(con, "SELECT platform,COUNT(*) FROM lineup_evidence GROUP BY 1")},
                "rows": [dict(zip(("db_name","year","platform","week_count","team_weeks","distinct_lineups","changed_started_set"),r)) for r in q(con, "SELECT * FROM lineup_evidence ORDER BY platform,year,db_name")],
            }

        # Bracket evidence from player flags, accepted only for 4/6/8.
        if {"team_key", "is_playoffs", "made_playoffs"} <= p:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE bracket_evidence AS
              SELECT t.db_name,t.year,t.platform,
                COUNT(DISTINCT CASE WHEN p.made_playoffs=1 THEN p.team_key END) made_teams,
                COUNT(DISTINCT CASE WHEN p.is_playoffs=1 THEN p.team_key END) playoff_teams_seen
              FROM targets t JOIN lake.public.player_fantasy p USING(db_name,year)
              WHERE t.playoff_teams IS NULL GROUP BY 1,2,3
            """)
            detail["bracket"] = {
                "by_evidence": {str(r[0]): int(r[1]) for r in q(con, """
                  SELECT CASE WHEN COALESCE(NULLIF(made_teams,0),NULLIF(playoff_teams_seen,0)) IN (4,6,8)
                              THEN CAST(COALESCE(NULLIF(made_teams,0),NULLIF(playoff_teams_seen,0)) AS VARCHAR)
                              ELSE 'unresolved' END,COUNT(*) FROM bracket_evidence GROUP BY 1
                """)},
                "rows": [dict(zip(("db_name","year","platform","made_teams","playoff_teams_seen"),r)) for r in q(con, "SELECT * FROM bracket_evidence ORDER BY platform,year,db_name")],
            }

        out = {"status":"detail_audit_complete_no_promotion", "base":base, "missing":miss, "detail":detail}
        args.out.parent.mkdir(parents=True,exist_ok=True)
        args.out.write_text(json.dumps(out,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")
        print(json.dumps({"status":out["status"],"missing":miss,"detail_counts":{k:len(v.get("rows",[])) if isinstance(v,dict) else None for k,v in detail.items()}},sort_keys=True))
    finally:
        con.close()


if __name__ == '__main__':
    main()
