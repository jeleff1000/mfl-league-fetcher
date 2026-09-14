"""Build conservative cohort-dimension fill sidecars from the research lake.

Read-only: the corpus and ops databases are attached READ_ONLY and never
rewritten.  Outputs are keyed overlays with explicit evidence sources.
Only values supported by an unambiguous join/evidence rule are emitted.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def esc(p: Path) -> str:
    return str(p.resolve()).replace("'", "''")


def norm(expr: str) -> str:
    return f"CASE WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DST','D/ST','DEFENSE','DEFENCE') THEN 'DEF' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DEFENSIVE TACKLE','DEFENSIVE END','DEFENSIVE LINEMAN') THEN 'DL' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('LINEBACKER','OUTSIDE LINEBACKER','INSIDE LINEBACKER') THEN 'LB' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('PK','PLACEKICKER') THEN 'K' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('FB','HB') THEN 'RB' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('CB','S','SAFETY','FS','SS') THEN 'DB' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DE','DT','NT','DL') THEN 'DL' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('MLB','OLB','ILB','LB') THEN 'LB' " \
        f"ELSE UPPER(TRIM(CAST({expr} AS VARCHAR))) END"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base', type=Path, required=True)
    ap.add_argument('--ops', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH '{esc(args.base)}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{esc(args.ops)}' AS ops (READ_ONLY)")
    try:
        pcols = {r[0] for r in con.execute('DESCRIBE lake.public.player_fantasy').fetchall()}
        scols = {r[0] for r in con.execute('DESCRIBE lake.public.league_settings').fetchall()}
        ocols = {r[0] for r in con.execute('DESCRIBE ops.nfl_historical.player_bio').fetchall()}
        stcols = {r[0] for r in con.execute('DESCRIBE ops.nfl_historical.nfl_player_stats_all').fetchall()}

        # Canonical fantasy grouping comes from the player-bio fantasy
        # position taxonomy when present (for example DE->DL and CB->DB).
        # Do not use player_fantasy.fantasy_position here: that is a lineup
        # slot, not the player's NFL/fantasy position.  Season stats are only
        # a fallback for IDs absent from the bio cache; using them first lets
        # season-specific raw labels override the canonical bio taxonomy.
        bio_candidates = [c for c in ('position', 'fantasy_position', 'position_category', 'nfl_position') if c in ocols]
        if not bio_candidates:
            raise SystemExit('player_bio has no canonical position field')
        bio_expr = 'COALESCE(' + ', '.join(
            f"NULLIF(TRIM(CAST({c} AS VARCHAR)), '')" for c in bio_candidates
        ) + ')'
        bio_expr_qualified = 'COALESCE(' + ', '.join(
            f"NULLIF(TRIM(CAST(b.{c} AS VARCHAR)), '')" for c in bio_candidates
        ) + ')'
        bio_source = f"player_bio.{bio_candidates[0]}"
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE bio_pos AS
          SELECT CAST(NFL_player_id AS VARCHAR) nfl_id, {norm(bio_expr)} AS position_fill
          FROM ops.nfl_historical.player_bio
          WHERE NFL_player_id IS NOT NULL AND {bio_expr} IS NOT NULL
          QUALIFY COUNT(DISTINCT {norm(bio_expr)}) OVER (PARTITION BY NFL_player_id)=1
        """)
        if 'position' in stcols and {'NFL_player_id','year'} <= stcols:
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE stats_pos AS
              WITH raw AS (
                SELECT CAST(NFL_player_id AS VARCHAR) nfl_id, CAST(year AS INTEGER) season_year,
                       UPPER(TRIM(tok)) tok
                FROM ops.nfl_historical.nfl_player_stats_all,
                     UNNEST(str_split(CAST("position" AS VARCHAR), ',')) AS g(tok)
                WHERE NFL_player_id IS NOT NULL AND "position" IS NOT NULL
              ), mapped AS (
                SELECT nfl_id,season_year,
                  CASE
                    WHEN tok IN ('DST','D/ST','DEFENSE','DEFENCE') THEN 'DEF'
                    WHEN tok IN ('DE','DT','NT','DL') THEN 'DL'
                    WHEN tok IN ('CB','S','SS','FS','SAF','DB') THEN 'DB'
                    WHEN tok IN ('OLB','ILB','MLB','LB') THEN 'LB'
                    WHEN tok IN ('FB','HB') THEN 'RB'
                    WHEN tok IN ('PK','PLACEKICKER') THEN 'K'
                    ELSE tok
                  END broad
                FROM raw
              )
              SELECT nfl_id,season_year,
                CASE
                  WHEN BOOL_OR(broad='QB') THEN 'QB'
                  WHEN BOOL_OR(broad='RB') THEN 'RB'
                  WHEN BOOL_OR(broad='WR') THEN 'WR'
                  WHEN BOOL_OR(broad='TE') THEN 'TE'
                  WHEN BOOL_OR(broad='K') THEN 'K'
                  WHEN BOOL_OR(broad='DEF') THEN 'DEF'
                  WHEN BOOL_OR(broad='DL') THEN 'DL'
                  WHEN BOOL_OR(broad='LB') THEN 'LB'
                  WHEN BOOL_OR(broad='DB') THEN 'DB'
                  ELSE MIN(broad)
                END position_fill
              FROM mapped GROUP BY 1,2
            """)
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE stats_pos AS SELECT CAST(NULL AS VARCHAR) nfl_id, CAST(NULL AS INTEGER) AS season_year, CAST(NULL AS VARCHAR) AS position_fill WHERE false")
        con.execute("""
          COPY (
            SELECT nfl_id,season_year,position_fill,position_source
              FROM (
                SELECT DISTINCT CAST(p.NFL_player_id AS VARCHAR) nfl_id,
                     CAST(p.year AS INTEGER) AS season_year,
                     COALESCE(b.position_fill,sp.position_fill) position_fill,
                     CASE WHEN b.nfl_id IS NOT NULL THEN '{bio_source}' WHEN sp.nfl_id IS NOT NULL THEN 'super_table.position' END position_source
              FROM lake.public.player_fantasy p
              JOIN lake.public.league_settings ls ON ls.db_name=p.db_name AND ls.year=p.year
              LEFT JOIN bio_pos b ON b.nfl_id=CAST(p.NFL_player_id AS VARCHAR)
              LEFT JOIN stats_pos sp ON sp.nfl_id=CAST(p.NFL_player_id AS VARCHAR) AND sp.season_year=CAST(p.year AS INTEGER)
              WHERE p.NFL_player_id IS NOT NULL
                AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(sp.position_fill,b.position_fill) IS NOT NULL
            ) candidates
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY nfl_id, season_year
              ORDER BY CASE WHEN position_source LIKE 'player_bio.%' THEN 0 ELSE 1 END,
                       position_fill
            )=1
          ) TO ? (FORMAT PARQUET)
        """, [str(args.out/'player_position_updates.parquet')])

        # Conservative fallback for rows without an NFL_player_id: use only
        # a unique normalized player-name/year/position identity from the
        # canonical stats table.  This is a row locator sidecar, not a schema
        # change, and it never overrides a populated position.
        con.execute("CREATE OR REPLACE TEMP TABLE name_position_updates AS SELECT CAST(NULL AS VARCHAR) row_key, CAST(NULL AS VARCHAR) db_name, CAST(NULL AS INTEGER) AS \"year\", CAST(NULL AS INTEGER) AS \"week\", CAST(NULL AS VARCHAR) player, CAST(NULL AS VARCHAR) manager, CAST(NULL AS VARCHAR) team_name, CAST(NULL AS VARCHAR) team_key, CAST(NULL AS VARCHAR) nfl_id, CAST(NULL AS VARCHAR) position_fill, CAST(NULL AS VARCHAR) position_source WHERE false")
        if {'player','year','week','db_name'} <= pcols:
            stats_name_pos_expr = norm('"nfl_position"') if 'nfl_position' in stcols else norm('"position"')
            stats_name_candidates = """
                SELECT regexp_replace(lower(trim(CAST(st.player AS VARCHAR))),'[^a-z0-9]','','g') name_key,
                       CAST(st.year AS INTEGER) season_year,
                       CAST(st.NFL_player_id AS VARCHAR) nfl_id,
                       {stats_name_pos_expr} position_fill
                FROM ops.nfl_historical.nfl_player_stats_all st
                WHERE st.player IS NOT NULL AND TRIM(CAST(st.player AS VARCHAR))<>''
                  AND st.NFL_player_id IS NOT NULL
                  AND st.year IS NOT NULL
                  AND {stats_name_pos_expr} IS NOT NULL
            """.format(stats_name_pos_expr=stats_name_pos_expr) if {'player','year','NFL_player_id'} <= stcols else """
                SELECT CAST(NULL AS VARCHAR) name_key, CAST(NULL AS INTEGER) season_year,
                       CAST(NULL AS VARCHAR) nfl_id, CAST(NULL AS VARCHAR) position_fill
                WHERE false
            """
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE name_position_updates AS
              WITH candidate_rows AS (
                SELECT regexp_replace(lower(trim(CAST(b.player AS VARCHAR))),'[^a-z0-9]','','g') name_key,
                       CAST(NULL AS INTEGER) season_year,
                       CAST(b.NFL_player_id AS VARCHAR) nfl_id,
                       {norm(bio_expr_qualified)} position_fill
                FROM ops.nfl_historical.player_bio b
                WHERE b.player IS NOT NULL AND TRIM(CAST(b.player AS VARCHAR))<>''
                  AND b.NFL_player_id IS NOT NULL
                  AND {norm(bio_expr_qualified)} IS NOT NULL
                UNION ALL
                {stats_name_candidates}
              ), p_rows AS (
                SELECT p.db_name,CAST(p.year AS INTEGER) AS season_year,CAST(p.week AS INTEGER) AS season_week,p.player,p.manager,p.team_name,p.team_key,
                       regexp_replace(lower(trim(CAST(p.player AS VARCHAR))),'[^a-z0-9]','','g') name_key,
                       md5(concat_ws('|',CAST(p.db_name AS VARCHAR),CAST(p.year AS VARCHAR),CAST(p.week AS VARCHAR),COALESCE(CAST(p.player AS VARCHAR),''),COALESCE(CAST(p.manager AS VARCHAR),''),COALESCE(CAST(p.team_name AS VARCHAR),''),COALESCE(CAST(p.team_key AS VARCHAR),''))) row_key
                FROM lake.public.player_fantasy p
                JOIN lake.public.league_settings ls ON ls.db_name=p.db_name AND ls.year=p.year
                WHERE (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                  AND (p.NFL_player_id IS NULL OR CAST(p.NFL_player_id AS VARCHAR)='')
                  AND p.player IS NOT NULL AND TRIM(CAST(p.player AS VARCHAR))<>''
              ), matches AS (
                SELECT p.*,c.nfl_id,c.position_fill
                FROM p_rows p JOIN candidate_rows c
                  ON c.name_key=p.name_key AND (c.season_year IS NULL OR c.season_year=p.season_year)
              )
              SELECT DISTINCT row_key,db_name,season_year AS "year",season_week AS "week",player,manager,team_name,team_key,nfl_id,position_fill,
                     'unique bio/stats player identity' position_source
              FROM matches
              QUALIFY COUNT(DISTINCT nfl_id) OVER (PARTITION BY name_key,season_year)=1
                  AND COUNT(DISTINCT position_fill) OVER (PARTITION BY name_key,season_year)=1
            """)
        con.execute("COPY name_position_updates TO ? (FORMAT PARQUET)", [str(args.out/'player_position_name_updates.parquet')])

        # Settings sidecar starts with one row per missing setting and only
        # emits columns whose evidence is independently deterministic.
        con.execute("""
          CREATE OR REPLACE TEMP TABLE setting_updates AS
          SELECT s.db_name,CAST(s.year AS INTEGER) AS season_year,
                 CAST(NULL AS DOUBLE) pass_td_fill,
                 CAST(NULL AS INTEGER) playoff_teams_fill,
                 CAST(NULL AS INTEGER) roster_FLX_fill,
                 CAST(NULL AS INTEGER) roster_SUPER_FLEX_fill,
                 CAST(NULL AS INTEGER) roster_IDP_fill,
                 CAST(NULL AS BOOLEAN) best_ball_fill,
                 CAST(NULL AS VARCHAR) pass_td_source,
                 CAST(NULL AS VARCHAR) playoff_source,
                 CAST(NULL AS VARCHAR) roster_source,
                 CAST(NULL AS VARCHAR) best_ball_source
          FROM lake.public.league_settings s
          WHERE EXISTS (SELECT 1 FROM lake.public.player_fantasy p WHERE p.db_name=s.db_name AND p.year=s.year)
        """)
        con.execute("CREATE OR REPLACE TEMP TABLE pass_evidence AS SELECT CAST(NULL AS VARCHAR) db_name, CAST(NULL AS INTEGER) season_year, CAST(NULL AS DOUBLE) e4, CAST(NULL AS DOUBLE) e6, CAST(NULL AS INTEGER) compared WHERE false")
        con.execute("CREATE OR REPLACE TEMP TABLE bracket_evidence AS SELECT CAST(NULL AS VARCHAR) db_name, CAST(NULL AS INTEGER) season_year, CAST(NULL AS INTEGER) n WHERE false")
        con.execute("CREATE OR REPLACE TEMP TABLE roster_evidence AS SELECT CAST(NULL AS VARCHAR) db_name, CAST(NULL AS INTEGER) season_year, CAST(NULL AS INTEGER) flex_slots, CAST(NULL AS INTEGER) sflex, CAST(NULL AS INTEGER) idp, CAST(NULL AS INTEGER) max_qb, CAST(NULL AS INTEGER) explicit_flex, CAST(NULL AS INTEGER) explicit_sflex, CAST(NULL AS INTEGER) explicit_idp WHERE false")
        con.execute("CREATE OR REPLACE TEMP TABLE lineup_season_evidence AS SELECT CAST(NULL AS VARCHAR) db_name, CAST(NULL AS INTEGER) AS season_year, CAST(NULL AS INTEGER) changed_started_set, CAST(NULL AS INTEGER) changed_roster_set, CAST(NULL AS INTEGER) team_count, CAST(NULL AS INTEGER) team_weeks WHERE false")

        # Pass-TD: compare the stored QB fantasy points to both canonical
        # scoring variants.  Require a strict error gap and usable rows.
        fcols = {x for x in ('fpts_4pt_0ppr','fpts_4pt_half','fpts_4pt_ppr','fpts_6pt_0ppr','fpts_6pt_half','fpts_6pt_ppr') if x in stcols}
        if len(fcols)==6 and {'scoring_pass_td','scoring_rec'} <= scols and {'NFL_player_id','year','week','fantasy_points'} <= pcols:
            def composite(pass_col: str, rec_col: str, fpts_col: str) -> str:
                raw_cols = [pass_col, 'pts_rush', rec_col, 'pts_misc', 'pts_k_std', 'pts_def_std']
                raw = ' + '.join(f'COALESCE(st."{c}",0)' for c in raw_cols if c in stcols) or '0'
                return f'COALESCE(st."{fpts_col}",({raw}))'
            four_0 = composite('pts_pass_4pt', 'pts_rec_0ppr', 'fpts_4pt_0ppr')
            four_h = composite('pts_pass_4pt', 'pts_rec_half', 'fpts_4pt_half')
            four_p = composite('pts_pass_4pt', 'pts_rec_ppr', 'fpts_4pt_ppr')
            six_0 = composite('pts_pass_6pt', 'pts_rec_0ppr', 'fpts_6pt_0ppr')
            six_h = composite('pts_pass_6pt', 'pts_rec_half', 'fpts_6pt_half')
            six_p = composite('pts_pass_6pt', 'pts_rec_ppr', 'fpts_6pt_ppr')
            four=f"CASE WHEN s.scoring_rec=0 THEN {four_0} WHEN s.scoring_rec<0.75 THEN {four_h} ELSE {four_p} END"
            six=f"CASE WHEN s.scoring_rec=0 THEN {six_0} WHEN s.scoring_rec<0.75 THEN {six_h} ELSE {six_p} END"
            qb_fields = [c for c in ('nfl_position','position','primary_position','fantasy_position') if c in stcols]
            qb_predicate = '(' + ' OR '.join([f"UPPER(TRIM(CAST(st.\"{c}\" AS VARCHAR)))='QB'" for c in qb_fields]) + ')' if qb_fields else 'FALSE'
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE pass_evidence AS
              SELECT p.db_name,CAST(p.year AS INTEGER) AS season_year,
                     SUM(CASE WHEN p.fantasy_points IS NOT NULL AND ({four}) IS NOT NULL
                              THEN ABS(p.fantasy_points-({four})) END) e4,
                     SUM(CASE WHEN p.fantasy_points IS NOT NULL AND ({six}) IS NOT NULL
                              THEN ABS(p.fantasy_points-({six})) END) e6,
                     COUNT(*) FILTER (WHERE p.fantasy_points IS NOT NULL
                                      AND ({four}) IS NOT NULL AND ({six}) IS NOT NULL) compared
              FROM lake.public.player_fantasy p
              JOIN lake.public.league_settings s ON s.db_name=p.db_name AND s.year=p.year
              JOIN ops.nfl_historical.nfl_player_stats_all st ON st.NFL_player_id=p.NFL_player_id AND st.year=p.year AND st.week=p.week
              WHERE s.scoring_pass_td IS NULL AND {qb_predicate}
              GROUP BY 1,2
            """)
            con.execute("""
              UPDATE setting_updates u SET pass_td_fill=CASE WHEN e.e4+0.10<e.e6 THEN 4.0 WHEN e.e6+0.10<e.e4 THEN 6.0 END,
                pass_td_source=CASE WHEN e.e4+0.10<e.e6 THEN 'QB fantasy-point fit:4pt' WHEN e.e6+0.10<e.e4 THEN 'QB fantasy-point fit:6pt' END
              FROM pass_evidence e WHERE u.db_name=e.db_name AND u.season_year=e.season_year
                AND e.compared>=10 AND e.e4 IS NOT NULL AND e.e6 IS NOT NULL
            """)

        # Bracket: accept only exact 4/6/8 team counts from player flags.
        if {'playoff_teams'} <= scols and {'team_key','team_name','manager','is_playoffs','made_playoffs','final_playoff_seed'} <= pcols:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE bracket_evidence AS
              SELECT s.db_name,CAST(s.year AS INTEGER) AS season_year,
                COALESCE(
                  NULLIF(COUNT(DISTINCT CASE WHEN p.made_playoffs=1 THEN
                    COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),'')) END),0),
                  CASE WHEN MAX(p.final_playoff_seed) IN (4,6,8) THEN MAX(p.final_playoff_seed) END,
                  NULLIF(COUNT(DISTINCT CASE WHEN p.final_playoff_seed BETWEEN 1 AND 32 THEN
                    COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),'')) END),0),
                  NULLIF(COUNT(DISTINCT CASE WHEN p.is_playoffs=1 THEN
                    COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),'')) END),0)
                ) n
              FROM lake.public.league_settings s JOIN lake.public.player_fantasy p USING(db_name,year)
              WHERE s.playoff_teams IS NULL GROUP BY 1,2
            """)
            con.execute("""
              UPDATE setting_updates u SET playoff_teams_fill=e.n, playoff_source='player_fantasy playoff-team count'
              FROM bracket_evidence e WHERE u.db_name=e.db_name AND u.season_year=e.season_year AND e.n IN (4,6,8)
            """)

        # Roster config: explicit lineup slot names and two-QB weeks are hard
        # evidence.  Do not infer a flex from ordinary RB/WR/TE positions.
        if {'roster_FLX','roster_SUPER_FLEX','roster_IDP'} <= scols and {'week','team_key','is_started','NFL_player_id'} <= pcols:
            rb_base = "COALESCE(TRY_CAST(s.roster_RB AS INTEGER),2)" if 'roster_RB' in scols else "2"
            wr_base = "COALESCE(TRY_CAST(s.roster_WR AS INTEGER),2)" if 'roster_WR' in scols else "2"
            te_base = "COALESCE(TRY_CAST(s.roster_TE AS INTEGER),1)" if 'roster_TE' in scols else "1"
            slot_expr = 'CAST(p.fantasy_position AS VARCHAR)' if 'fantasy_position' in pcols else "''"
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE roster_evidence AS
              WITH player_positions AS (
                SELECT p.db_name,p.year,p.week,
                  COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),'')) team_id,
                  p.is_started,
                  COALESCE(NULLIF({norm('p."position"')},''),b.position_fill,sp.position_fill) actual_position,
                  UPPER(TRIM({slot_expr})) lineup_slot
                FROM lake.public.player_fantasy p
                JOIN lake.public.league_settings ls ON ls.db_name=p.db_name AND ls.year=p.year
                LEFT JOIN bio_pos b ON b.nfl_id=CAST(p.NFL_player_id AS VARCHAR)
                LEFT JOIN stats_pos sp ON sp.nfl_id=CAST(p.NFL_player_id AS VARCHAR) AND sp.season_year=CAST(p.year AS INTEGER)
                WHERE p.is_started=1
                  AND ls.roster_IDP IS NULL AND ls.roster_DL IS NULL AND ls.roster_LB IS NULL AND ls.roster_DB IS NULL
                  AND ls.roster_DB_LB IS NULL AND ls.roster_DL_LB IS NULL AND ls.roster_SUPER_FLEX IS NULL AND ls.roster_FLX IS NULL
                  AND COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),'')) IS NOT NULL
              ), q AS (
                SELECT db_name,year AS season_year,week,team_id,COUNT(*) qb
                FROM player_positions
                WHERE actual_position='QB'
                GROUP BY 1,2,3,4
              ), sw AS (
                SELECT db_name,year AS season_year,week,team_id,COUNT(*) skill
                FROM player_positions
                WHERE actual_position IN ('RB','WR','TE')
                GROUP BY 1,2,3,4
              ), idp AS (
                SELECT db_name,year AS season_year,week,team_id,COUNT(*) idp
                FROM player_positions
                WHERE actual_position IN ('DL','LB','DB','DEF')
                GROUP BY 1,2,3,4
              ), slots AS (
                SELECT db_name,year AS season_year,
                  MAX(CASE WHEN lineup_slot IN ('FLEX','FLX','REC_FLEX','W/R','WR/RB','RB/WR','WRRB') THEN 1 ELSE 0 END) explicit_flex,
                  MAX(CASE WHEN lineup_slot IN ('SFLEX','SFLX','SUPERFLEX','SUPER_FLEX','SUPER FLEX','SUPER-FLEX') THEN 1 ELSE 0 END) explicit_sflex,
                  MAX(CASE WHEN lineup_slot IN ('DL','LB','DB','IDP','DE','DT','NT') THEN 1 ELSE 0 END) explicit_idp
                FROM player_positions WHERE is_started=1 GROUP BY 1,2
              )
              SELECT s.db_name,CAST(s.year AS INTEGER) AS season_year,
                MAX(CASE WHEN COALESCE(sw.skill,0) - ({rb_base}+{wr_base}+{te_base}) > 0 THEN COALESCE(sw.skill,0)-({rb_base}+{wr_base}+{te_base}) ELSE 0 END) flex_slots,
                MAX(CASE WHEN COALESCE(q.qb,0)>=2 THEN 1 ELSE 0 END) sflex,
                MAX(CASE WHEN COALESCE(idp.idp,0)>0 THEN 1 ELSE 0 END) idp,
                MAX(COALESCE(q.qb,0)) max_qb,
                MAX(COALESCE(slots.explicit_flex,0)) explicit_flex,
                MAX(COALESCE(slots.explicit_sflex,0)) explicit_sflex,
                MAX(COALESCE(slots.explicit_idp,0)) explicit_idp
              FROM lake.public.league_settings s JOIN player_positions p USING(db_name,year)
              LEFT JOIN q ON q.db_name=p.db_name AND q.season_year=p.year AND q.week=p.week AND q.team_id=p.team_id
              LEFT JOIN sw ON sw.db_name=p.db_name AND sw.season_year=p.year AND sw.week=p.week AND sw.team_id=p.team_id
              LEFT JOIN idp ON idp.db_name=p.db_name AND idp.season_year=p.year AND idp.week=p.week AND idp.team_id=p.team_id
              LEFT JOIN slots ON slots.db_name=p.db_name AND slots.season_year=p.year
              WHERE s.roster_IDP IS NULL AND s.roster_DL IS NULL AND s.roster_LB IS NULL AND s.roster_DB IS NULL
                AND s.roster_DB_LB IS NULL AND s.roster_DL_LB IS NULL AND s.roster_SUPER_FLEX IS NULL AND s.roster_FLX IS NULL
              GROUP BY 1,2
            """)
            con.execute("""
              UPDATE setting_updates u SET roster_FLX_fill=CASE WHEN e.explicit_flex=1 THEN 1 WHEN e.flex_slots>0 THEN e.flex_slots END,
                roster_SUPER_FLEX_fill=CASE WHEN e.explicit_sflex=1 OR e.sflex=1 OR e.max_qb>=2 THEN 1 END,
                roster_IDP_fill=CASE WHEN e.explicit_idp=1 OR e.idp=1 THEN 1 END,
                roster_source=CASE WHEN e.explicit_idp=1 THEN 'explicit started IDP slot' WHEN e.idp=1 THEN 'started defensive position' WHEN e.explicit_sflex=1 THEN 'explicit started super-flex slot' WHEN e.sflex=1 OR e.max_qb>=2 THEN 'two-QB lineup' WHEN e.explicit_flex=1 THEN 'explicit started flex slot' WHEN e.flex_slots>0 THEN 'skill starter excess over base slots' END
              FROM roster_evidence e WHERE u.db_name=e.db_name AND u.season_year=e.season_year
            """)

        # Best-ball remains a classification ledger.  A changing lineup is
        # evidence of dynamic lineups, not proof of best-ball versus managed,
        # so no fabricated boolean is emitted here.
        best_ball_audit = {'status': 'skipped_missing_columns'}
        if 'sleeper_best_ball' in scols and {'db_name','year','week','NFL_player_id','is_started','team_key','team_name','manager'} <= pcols:
            platform_expr = 'CAST(s.platform AS VARCHAR)' if 'platform' in scols else "'unknown'"
            con.execute("""
              CREATE OR REPLACE TEMP TABLE lineup_season_evidence AS
              WITH weekly AS (
                SELECT p.db_name,p.year,p.week,
                  COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),'')) team_id,
                  md5(string_agg(DISTINCT CASE WHEN p.is_started=1 THEN CAST(p.NFL_player_id AS VARCHAR) END, ',' ORDER BY CASE WHEN p.is_started=1 THEN CAST(p.NFL_player_id AS VARCHAR) END)) lineup_sig,
                  md5(string_agg(DISTINCT CASE WHEN p.is_rostered=1 THEN CAST(p.NFL_player_id AS VARCHAR) END, ',' ORDER BY CASE WHEN p.is_rostered=1 THEN CAST(p.NFL_player_id AS VARCHAR) END)) roster_sig
                FROM lake.public.player_fantasy p
                WHERE p.NFL_player_id IS NOT NULL
                GROUP BY 1,2,3,4
              ), team_seasons AS (
                SELECT db_name,year,team_id,COUNT(DISTINCT lineup_sig) distinct_lineups,
                       COUNT(DISTINCT roster_sig) distinct_rosters,COUNT(*) team_weeks
                FROM weekly
                WHERE team_id IS NOT NULL
                GROUP BY 1,2,3
              )
              SELECT db_name,year AS season_year,
                MAX(CASE WHEN distinct_lineups>1 THEN 1 ELSE 0 END) changed_started_set,
                MAX(CASE WHEN distinct_rosters>1 THEN 1 ELSE 0 END) changed_roster_set,
                COUNT(*) team_count,
                SUM(team_weeks) team_weeks
              FROM team_seasons
              GROUP BY 1,2
            """)
            best_ball_audit = {
              'status': 'calibrated_against_existing_values',
              'by_value_and_lineup_and_roster_signal': [
                {'platform': r[0], 'best_ball': r[1], 'changed_started_set': r[2], 'changed_roster_set': r[3], 'league_seasons': r[4]}
                for r in con.execute("""
                  SELECT %s,CAST(s.sleeper_best_ball AS VARCHAR),e.changed_started_set,e.changed_roster_set,COUNT(*)
                  FROM lake.public.league_settings s JOIN lineup_season_evidence e ON e.db_name=s.db_name AND e.season_year=s.year
                  WHERE s.sleeper_best_ball IS NOT NULL
                  GROUP BY 1,2,3,4 ORDER BY 1,2,3,4
                """ % platform_expr).fetchall()
              ],
              'missing_value_by_lineup_and_roster_signal': [
                {'platform': r[0], 'changed_started_set': r[1], 'changed_roster_set': r[2], 'league_seasons': r[3]}
                for r in con.execute("""
                  SELECT %s,e.changed_started_set,e.changed_roster_set,COUNT(*)
                  FROM lake.public.league_settings s JOIN lineup_season_evidence e ON e.db_name=s.db_name AND e.season_year=s.year
                  WHERE s.sleeper_best_ball IS NULL
                  GROUP BY 1,2,3 ORDER BY 1,2,3
                """ % platform_expr).fetchall()
              ],
              'rule_status': 'do_not_fill_until_signal_is_specific'
            }
            if all(x.get('platform') == 'fleaflicker' for x in best_ball_audit.get('missing_value_by_lineup_and_roster_signal', [])):
                best_ball_audit['rule_status'] = 'not_applicable_for_fleaflicker_sleeper_best_ball_field'
        con.execute("""
          COPY (SELECT db_name,season_year AS year,pass_td_fill,playoff_teams_fill,roster_FLX_fill,roster_SUPER_FLEX_fill,roster_IDP_fill,best_ball_fill,
                       pass_td_source,playoff_source,roster_source,best_ball_source
                FROM setting_updates
                WHERE pass_td_fill IS NOT NULL OR playoff_teams_fill IS NOT NULL OR roster_FLX_fill IS NOT NULL OR roster_SUPER_FLEX_fill IS NOT NULL OR roster_IDP_fill IS NOT NULL)
          TO ? (FORMAT PARQUET)
        """, [str(args.out/'league_settings_updates.parquet')])
        con.execute("""
          COPY (
            SELECT u.db_name,u.season_year AS year,
              CASE WHEN s.scoring_pass_td IS NULL AND u.pass_td_fill IS NULL THEN
                CASE WHEN pe.db_name IS NULL OR pe.compared=0 THEN 'no_qb_evidence'
                     WHEN pe.e4 IS NULL OR pe.e6 IS NULL THEN 'no_usable_qb_scores'
                     WHEN pe.compared<10 THEN 'insufficient_qb_evidence'
                     ELSE 'ambiguous_qb_fit' END END pass_td_reason,
              CASE WHEN s.playoff_teams IS NULL AND u.playoff_teams_fill IS NULL THEN 'no_valid_4_6_8_playoff_team_count' END playoff_teams_reason,
              CASE WHEN s.roster_FLX IS NULL AND s.roster_SUPER_FLEX IS NULL AND s.roster_IDP IS NULL
                         AND s.roster_DL IS NULL AND s.roster_LB IS NULL AND s.roster_DB IS NULL
                         AND s.roster_DB_LB IS NULL AND s.roster_DL_LB IS NULL
                         AND u.roster_FLX_fill IS NULL AND u.roster_SUPER_FLEX_fill IS NULL AND u.roster_IDP_fill IS NULL
                   THEN 'no_definitive_starter_position_signal' END roster_reason,
              CASE WHEN s.sleeper_best_ball IS NULL THEN
                CASE WHEN LOWER(CAST(s.platform AS VARCHAR))='fleaflicker' THEN 'not_applicable_sleeper_best_ball_field'
                     WHEN le.db_name IS NULL THEN 'no_lineup_identity_evidence'
                     ELSE 'lineup_change_not_specific_to_best_ball' END END best_ball_reason
            FROM lake.public.league_settings s JOIN setting_updates u ON u.db_name=s.db_name AND u.season_year=s.year
            LEFT JOIN pass_evidence pe ON pe.db_name=u.db_name AND pe.season_year=u.season_year
            LEFT JOIN bracket_evidence be ON be.db_name=u.db_name AND be.season_year=u.season_year
            LEFT JOIN roster_evidence re ON re.db_name=u.db_name AND re.season_year=u.season_year
            LEFT JOIN lineup_season_evidence le ON le.db_name=u.db_name AND le.season_year=u.season_year
            WHERE (s.scoring_pass_td IS NULL AND u.pass_td_fill IS NULL)
               OR (s.playoff_teams IS NULL AND u.playoff_teams_fill IS NULL)
               OR (s.sleeper_best_ball IS NULL)
               OR (s.roster_FLX IS NULL AND s.roster_SUPER_FLEX IS NULL AND s.roster_IDP IS NULL
                   AND s.roster_DL IS NULL AND s.roster_LB IS NULL AND s.roster_DB IS NULL
                   AND s.roster_DB_LB IS NULL AND s.roster_DL_LB IS NULL
                   AND u.roster_FLX_fill IS NULL AND u.roster_SUPER_FLEX_fill IS NULL AND u.roster_IDP_fill IS NULL)
          ) TO ? (FORMAT PARQUET)
        """, [str(args.out/'unresolved_settings.parquet')])
        report = {
            'status':'fill_sidecars_built_no_promotion',
            'position_update_rows': int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(args.out/'player_position_updates.parquet')]).fetchone()[0]),
            'position_name_update_rows': int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(args.out/'player_position_name_updates.parquet')]).fetchone()[0]),
            'settings_update_rows': int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(args.out/'league_settings_updates.parquet')]).fetchone()[0]),
            'position_row_coverage': dict(zip(
              ('target_null_rows','fillable_by_sidecar_rows','unresolved_after_sidecar_rows'),
              con.execute(f"""
                WITH target AS (
                  SELECT p.NFL_player_id, CAST(p.year AS INTEGER) AS year,
                         md5(concat_ws('|',CAST(p.db_name AS VARCHAR),CAST(p.year AS VARCHAR),CAST(p.week AS VARCHAR),COALESCE(CAST(p.player AS VARCHAR),''),COALESCE(CAST(p.manager AS VARCHAR),''),COALESCE(CAST(p.team_name AS VARCHAR),''),COALESCE(CAST(p.team_key AS VARCHAR),''))) row_key
                  FROM lake.public.player_fantasy p
                  JOIN lake.public.league_settings ls ON ls.db_name=p.db_name AND ls.year=p.year
                  LEFT JOIN bio_pos b ON b.nfl_id=CAST(p.NFL_player_id AS VARCHAR)
                  LEFT JOIN stats_pos sp ON sp.nfl_id=CAST(p.NFL_player_id AS VARCHAR) AND sp.season_year=CAST(p.year AS INTEGER)
                  WHERE (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                    AND (b.nfl_id IS NOT NULL OR sp.nfl_id IS NOT NULL
                         OR p.sleeper_player_id IS NOT NULL OR p.mfl_player_id IS NOT NULL
                         OR p.fleaflicker_player_id IS NOT NULL OR p.espn_player_id IS NOT NULL
                         OR p.yahoo_player_id IS NOT NULL OR p.team_name IS NOT NULL OR p.team_key IS NOT NULL
                         OR (p.player IS NOT NULL AND LOWER(TRIM(CAST(p.player AS VARCHAR))) <> 'duplicate player'))
                ), covered_rows AS (
                  SELECT COUNT(*) AS n
                  FROM target t JOIN read_parquet(?) u
                    ON CAST(t.NFL_player_id AS VARCHAR)=u.nfl_id
                   AND CAST(t.year AS INTEGER)=u.season_year
                  UNION ALL
                  SELECT COUNT(*) AS n
                  FROM target t JOIN read_parquet(?) u
                    ON t.row_key=u.row_key
                   AND t.NFL_player_id IS NULL
                )
                SELECT (SELECT COUNT(*) FROM target),
                       (SELECT SUM(n) FROM covered_rows),
                       (SELECT COUNT(*) FROM target)-(SELECT SUM(n) FROM covered_rows)
              """, [str(args.out/'player_position_updates.parquet'),str(args.out/'player_position_name_updates.parquet')]).fetchone()
            )),
            'position_target_breakdown': [
              dict(zip(
                ('platform','target_rows','rows_with_nfl_id','distinct_nfl_ids',
                 'rows_with_bio_position','rows_with_stats_position',
                 'rows_with_any_position_source','rows_without_position_source',
                 'rows_with_sidecar_match','distinct_row_keys','distinct_covered_row_keys'),
                row
              ))
              for row in con.execute(f"""
                WITH target AS (
                  SELECT LOWER(TRIM(CAST(ls.platform AS VARCHAR))) AS platform,
                         p.NFL_player_id,
                         CAST(p.year AS INTEGER) AS season_year,
                         md5(concat_ws('|',CAST(p.db_name AS VARCHAR),CAST(p.year AS VARCHAR),CAST(p.week AS VARCHAR),COALESCE(CAST(p.player AS VARCHAR),''),COALESCE(CAST(p.manager AS VARCHAR),''),COALESCE(CAST(p.team_name AS VARCHAR),''),COALESCE(CAST(p.team_key AS VARCHAR),''))) AS row_key,
                         b.nfl_id AS bio_nfl_id,
                         sp.nfl_id AS stats_nfl_id,
                         u.nfl_id AS sidecar_nfl_id
                  FROM lake.public.player_fantasy p
                  JOIN lake.public.league_settings ls ON ls.db_name=p.db_name AND ls.year=p.year
                  LEFT JOIN bio_pos b ON b.nfl_id=CAST(p.NFL_player_id AS VARCHAR)
                  LEFT JOIN stats_pos sp ON sp.nfl_id=CAST(p.NFL_player_id AS VARCHAR) AND sp.season_year=CAST(p.year AS INTEGER)
                  LEFT JOIN read_parquet('{esc(args.out/'player_position_updates.parquet')}') u
                    ON CAST(p.NFL_player_id AS VARCHAR)=u.nfl_id AND CAST(p.year AS INTEGER)=u.season_year
                  WHERE (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                    AND (b.nfl_id IS NOT NULL OR sp.nfl_id IS NOT NULL
                         OR p.sleeper_player_id IS NOT NULL OR p.mfl_player_id IS NOT NULL
                         OR p.fleaflicker_player_id IS NOT NULL OR p.espn_player_id IS NOT NULL
                         OR p.yahoo_player_id IS NOT NULL OR p.team_name IS NOT NULL OR p.team_key IS NOT NULL
                         OR (p.player IS NOT NULL AND LOWER(TRIM(CAST(p.player AS VARCHAR))) <> 'duplicate player'))
                )
                SELECT platform, COUNT(*), COUNT(NFL_player_id), COUNT(DISTINCT NFL_player_id),
                       COUNT(*) FILTER (WHERE bio_nfl_id IS NOT NULL),
                       COUNT(*) FILTER (WHERE stats_nfl_id IS NOT NULL),
                       COUNT(*) FILTER (WHERE bio_nfl_id IS NOT NULL OR stats_nfl_id IS NOT NULL),
                       COUNT(*) FILTER (WHERE bio_nfl_id IS NULL AND stats_nfl_id IS NULL),
                       COUNT(*) FILTER (WHERE sidecar_nfl_id IS NOT NULL),
                       COUNT(DISTINCT row_key),
                       COUNT(DISTINCT CASE WHEN sidecar_nfl_id IS NOT NULL THEN row_key END)
                FROM target GROUP BY 1 ORDER BY 1
              """).fetchall()
            ],
            'missing_setting_value_counts': dict(zip(
              ('pass_td','playoff_teams','roster_FLX','roster_SUPER_FLEX','roster_IDP','roster_any_config','best_ball'),
              con.execute("""
                SELECT COUNT(*) FILTER (WHERE scoring_pass_td IS NULL),
                       COUNT(*) FILTER (WHERE playoff_teams IS NULL),
                       COUNT(*) FILTER (WHERE roster_FLX IS NULL),
                       COUNT(*) FILTER (WHERE roster_SUPER_FLEX IS NULL),
                       COUNT(*) FILTER (WHERE roster_IDP IS NULL),
                       COUNT(*) FILTER (WHERE roster_FLX IS NULL AND roster_SUPER_FLEX IS NULL AND roster_IDP IS NULL
                                         AND roster_DL IS NULL AND roster_LB IS NULL AND roster_DB IS NULL
                                         AND roster_DB_LB IS NULL AND roster_DL_LB IS NULL),
                       COUNT(*) FILTER (WHERE sleeper_best_ball IS NULL)
                FROM lake.public.league_settings s
                WHERE EXISTS (SELECT 1 FROM lake.public.player_fantasy p WHERE p.db_name=s.db_name AND p.year=s.year)
              """).fetchone()
            )),
            'settings_updates_by_field': {k:int(v) for k,v in con.execute("""
              SELECT 'pass_td',COUNT(*) FROM setting_updates WHERE pass_td_fill IS NOT NULL
              UNION ALL SELECT 'playoff_teams',COUNT(*) FROM setting_updates WHERE playoff_teams_fill IS NOT NULL
              UNION ALL SELECT 'roster_FLX',COUNT(*) FROM setting_updates WHERE roster_FLX_fill IS NOT NULL
              UNION ALL SELECT 'roster_SUPER_FLEX',COUNT(*) FROM setting_updates WHERE roster_SUPER_FLEX_fill IS NOT NULL
              UNION ALL SELECT 'roster_IDP',COUNT(*) FROM setting_updates WHERE roster_IDP_fill IS NOT NULL
            """).fetchall()},
            'pass_td_fit_residuals': [
              {'db_name': r[0], 'year': int(r[1]), 'scoring_rec': float(r[2]) if r[2] is not None else None,
               'e4': float(r[3]) if r[3] is not None else None, 'e6': float(r[4]) if r[4] is not None else None,
               'compared': int(r[5]), 'absolute_gap': float(r[6]) if r[6] is not None else None}
              for r in con.execute("""
                SELECT e.db_name,e.season_year,s.scoring_rec,e.e4,e.e6,e.compared,ABS(e.e4-e.e6)
                FROM pass_evidence e
                JOIN lake.public.league_settings s ON s.db_name=e.db_name AND s.year=e.season_year
                JOIN setting_updates u ON u.db_name=e.db_name AND u.season_year=e.season_year
                WHERE s.scoring_pass_td IS NULL AND u.pass_td_fill IS NULL
                ORDER BY e.season_year,e.db_name
              """).fetchall()
            ],
            'pass_td_residual_diagnostics': [
              {'db_name': r[0], 'year': int(r[1]), 'player_rows': int(r[2]),
               'player_points_nonnull': int(r[3]), 'stats_rows': int(r[4]),
               'stats_qb_rows': int(r[5]), 'stats_component_rows': int(r[6]),
               'player_qb_rows': int(r[7])}
              for r in con.execute(f"""
                SELECT s.db_name,CAST(s.year AS INTEGER),COUNT(*) player_rows,
                       COUNT(p.fantasy_points) player_points_nonnull,
                       COUNT(st.NFL_player_id) stats_rows,
                       COUNT(*) FILTER (WHERE {qb_predicate}) stats_qb_rows,
                       COUNT(*) FILTER (WHERE st.fpts_4pt_0ppr IS NOT NULL
                                             OR st.pts_pass_4pt IS NOT NULL) stats_component_rows,
                       COUNT(*) FILTER (WHERE UPPER(TRIM(CAST(p.position AS VARCHAR)))='QB') player_qb_rows
                FROM lake.public.league_settings s
                JOIN lake.public.player_fantasy p ON p.db_name=s.db_name AND p.year=s.year
                LEFT JOIN ops.nfl_historical.nfl_player_stats_all st
                  ON st.NFL_player_id=p.NFL_player_id AND st.year=p.year AND st.week=p.week
                LEFT JOIN setting_updates u ON u.db_name=s.db_name AND u.season_year=s.year
                WHERE s.scoring_pass_td IS NULL AND u.pass_td_fill IS NULL
                GROUP BY 1,2 ORDER BY 2,1
              """).fetchall()
            ] if len(fcols)==6 and {'scoring_pass_td','scoring_rec'} <= scols and {'NFL_player_id','year','week','fantasy_points'} <= pcols else [],
            'bracket_evidence_distribution': [
              {'playoff_team_count': int(r[0]) if r[0] is not None else None,
               'league_seasons': int(r[1])}
              for r in con.execute("""
                SELECT n, COUNT(*) FROM bracket_evidence
                WHERE n IS NOT NULL GROUP BY 1 ORDER BY 1
              """).fetchall()
            ],
            'roster_evidence_distribution': [
              {'flex_slots': int(r[0] or 0), 'super_flex': int(r[1] or 0),
               'idp': int(r[2] or 0), 'max_qb': int(r[3] or 0),
               'league_seasons': int(r[4])}
              for r in con.execute("""
                SELECT flex_slots,sflex,idp,max_qb,COUNT(*)
                FROM roster_evidence GROUP BY 1,2,3,4 ORDER BY 1,2,3,4
              """).fetchall()
            ],
            'unresolved_settings_rows': int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(args.out/'unresolved_settings.parquet')]).fetchone()[0]),
            'non_applicable_best_ball_rows': int(con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE best_ball_reason='not_applicable_sleeper_best_ball_field'", [str(args.out/'unresolved_settings.parquet')]).fetchone()[0]),
            'true_unresolved_settings_rows': int(con.execute("""
              SELECT COUNT(*) FROM read_parquet(?)
              WHERE pass_td_reason IS NOT NULL OR playoff_teams_reason IS NOT NULL OR roster_reason IS NOT NULL
                 OR (best_ball_reason IS NOT NULL AND best_ball_reason<>'not_applicable_sleeper_best_ball_field')
            """, [str(args.out/'unresolved_settings.parquet')]).fetchone()[0]),
            'unresolved_settings_by_reason': {str(k): int(v) for k,v in con.execute("""
              SELECT reason,COUNT(*) FROM (
                SELECT pass_td_reason reason FROM read_parquet(?) WHERE pass_td_reason IS NOT NULL
                UNION ALL SELECT playoff_teams_reason FROM read_parquet(?) WHERE playoff_teams_reason IS NOT NULL
                UNION ALL SELECT roster_reason FROM read_parquet(?) WHERE roster_reason IS NOT NULL
                UNION ALL SELECT best_ball_reason FROM read_parquet(?) WHERE best_ball_reason IS NOT NULL
              ) GROUP BY 1 ORDER BY 1
            """, [str(args.out/'unresolved_settings.parquet')]*4).fetchall()},
            'best_ball': best_ball_audit,
            'new_columns': [], 'cache_mutated': False, 'new_lineage': False,
        }
        (args.out/'fill_report.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n',encoding='utf-8')
        print(json.dumps(report,sort_keys=True))
    finally:
        con.close()


if __name__ == '__main__':
    main()
