"""Who fills the IDP flex? The R17 measurement, applied to defence.

IDP leagues carry catch-all slots -- roster_IDP (any defender) and the DB_LB / DL_LB combos --
exactly as offensive leagues carry FLEX. R17 measured the offensive flex as 47/47/6 RB/WR/TE in
standard and 33/60/7 in PPR. This is the same inference: starts beyond the dedicated slots.
"""
import duckdb, pandas as pd
con = duckdb.connect(config={"memory_limit":"1400MB","threads":2,
    "temp_directory":"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/idpflex"})
for s in ("SET enable_progress_bar=false","SET preserve_insertion_order=false",
          "PRAGMA max_temp_directory_size='3GB'"): con.execute(s)
con.execute("ATTACH 'D:/league-history-data/fantasy_leagues/tmp/research_public_lake_20260730/corpus_snapshot.duckdb' AS l (READ_ONLY)")
con.execute("ATTACH 'D:/tmp/research_public_lake_stage_20260722/ops_cache.duckdb' AS o (READ_ONLY)")
con.execute("""CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid,
  MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all
  WHERE "year"=2024 AND NFL_player_id IS NOT NULL AND position NOT LIKE '%,%' GROUP BY 1""")
con.execute("""CREATE OR REPLACE TEMP TABLE lg AS
 SELECT s.db_name, s.num_teams,
   COALESCE(s.roster_DL,0) dDL, COALESCE(s.roster_LB,0) dLB, COALESCE(s.roster_DB,0) dDB,
   COALESCE(s.roster_IDP,0) fIDP, COALESCE(s.roster_DB_LB,0) fDBLB,
   COALESCE(s.roster_DL_LB,0) fDLLB,
   COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DB_LB,0)+COALESCE(s.roster_DL_LB,0) flex_all
 FROM l.public.league_settings s
 WHERE s.year=2024 AND s.num_teams BETWEEN 8 AND 14
   AND COALESCE(s.sleeper_best_ball,false)=false AND COALESCE(s.is_dynasty,false)=false
   AND (COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL,0)+COALESCE(s.roster_LB,0)
        +COALESCE(s.roster_DB,0)+COALESCE(s.roster_DB_LB,0)+COALESCE(s.roster_DL_LB,0))>0""")
pd.set_option("display.width",210)
print("IDP SLOT SHAPES (2024, managed redraft, IDP leagues):")
print(con.execute("""SELECT COUNT(*) leagues, MEDIAN(dDL) DL, MEDIAN(dLB) LB, MEDIAN(dDB) DB,
  MEDIAN(fIDP) IDP_flex, MEDIAN(fDBLB) DB_LB, MEDIAN(fDLLB) DL_LB, MEDIAN(flex_all) flex_total
 FROM lg""").fetchdf().to_string(index=False))
print("\nhow many IDP leagues even HAVE a catch-all/combo slot?")
print(con.execute("""SELECT CASE WHEN flex_all>0 THEN 'has IDP flex' ELSE 'dedicated only' END k,
  COUNT(*) leagues, MEDIAN(dDL+dLB+dDB) dedicated FROM lg GROUP BY 1""").fetchdf().to_string(index=False))
# R17 inference: starts beyond the dedicated slots are the flex occupants
con.execute("""CREATE OR REPLACE TEMP TABLE tw AS
 SELECT g.db_name AS db_name, g.flex_all AS flex_all,
   GREATEST(SUM(CASE WHEN pos.p='DL' AND pf.is_started=1 THEN 1 ELSE 0 END)-MAX(g.dDL)*MAX(g.num_teams),0) xDL,
   GREATEST(SUM(CASE WHEN pos.p='LB' AND pf.is_started=1 THEN 1 ELSE 0 END)-MAX(g.dLB)*MAX(g.num_teams),0) xLB,
   GREATEST(SUM(CASE WHEN pos.p='DB' AND pf.is_started=1 THEN 1 ELSE 0 END)-MAX(g.dDB)*MAX(g.num_teams),0) xDB,
   MAX(g.flex_all)*MAX(g.num_teams) flex_slots
 FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
 JOIN lg g ON g.db_name=pf.db_name
 WHERE pf.year=2024 AND pf.week BETWEEN 1 AND 17 AND pos.p IN ('DL','LB','DB')
 GROUP BY 1, 2, pf.week""")
d=con.execute("""SELECT SUM(xDL) DL, SUM(xLB) LB, SUM(xDB) DB, SUM(flex_slots) slots
 FROM tw WHERE flex_all>0""").fetchdf().iloc[0]
tot=d.DL+d.LB+d.DB
print(f"\nWHO FILLS THE IDP FLEX (starts beyond dedicated slots):")
print(f"  DL {100*d.DL/tot:.1f}%   LB {100*d.LB/tot:.1f}%   DB {100*d.DB/tot:.1f}%")
print(f"  inferred occupants {tot:,.0f} vs actual flex slots {d.slots:,.0f}"
      f"  -> ratio {tot/d.slots:.2f} (R17's WR check was 2.21/2.25 = 0.98)")
con.close()
