"""RB step 1: the lineup mechanism, exactly as R17 did it for WR.

Infer flex occupancy as starts beyond the dedicated slots, then DERIVE starting capacity from
that mechanism and CHECK it against what is actually started. WR's derivation was trusted
because the inferred occupancy reproduced the slot count it never read (2.21 vs 2.25).
"""
import duckdb, pandas as pd
con = duckdb.connect(config={"memory_limit":"1400MB","threads":2,
    "temp_directory":"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/rb1"})
for s in ("SET enable_progress_bar=false","SET preserve_insertion_order=false",
          "PRAGMA max_temp_directory_size='3GB'"): con.execute(s)
con.execute("ATTACH 'D:/league-history-data/fantasy_leagues/tmp/research_public_lake_20260730/corpus_snapshot.duckdb' AS l (READ_ONLY)")
con.execute("ATTACH 'D:/tmp/research_public_lake_stage_20260722/ops_cache.duckdb' AS o (READ_ONLY)")
con.execute("""CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid,
  MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all
  WHERE "year"=2024 AND NFL_player_id IS NOT NULL AND position NOT LIKE '%,%' GROUP BY 1""")
# the CLEAN lane R17 used: managed, redraft, flx (no superflex, no IDP)
con.execute("""CREATE OR REPLACE TEMP TABLE lg AS
 SELECT s.db_name, s.num_teams,
   CASE WHEN COALESCE(s.scoring_rec,0)=0 THEN 'std'
        WHEN COALESCE(s.scoring_rec,0)<0.75 THEN 'half' ELSE 'ppr' END ppr,
   COALESCE(s.roster_RB,0) sRB, COALESCE(s.roster_FLX,0) sFLX, COALESCE(s.roster_BN,0) sBN
 FROM l.public.league_settings s
 WHERE s.year=2024 AND COALESCE(s.roster_SUPER_FLEX,0)=0 AND COALESCE(s.roster_IDP,0)=0
   AND COALESCE(s.roster_DL,0)=0 AND COALESCE(s.roster_LB,0)=0 AND COALESCE(s.roster_DB,0)=0
   AND COALESCE(s.sleeper_best_ball,false)=false AND COALESCE(s.is_dynasty,false)=false
   AND s.num_teams BETWEEN 8 AND 14 AND COALESCE(s.roster_FLX,0)>0""")
pd.set_option("display.width",200)
print("DECLARED RB + FLEX slots in the clean lane (managed, redraft, flx):")
print(con.execute("""SELECT ppr, COUNT(*) leagues, MEDIAN(sRB) rb_slots, MEDIAN(sFLX) flex_slots,
  MEDIAN(sBN) bench FROM lg GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))
# per team-week: starts by position, minus the dedicated slots -> who took the flex
con.execute("""CREATE OR REPLACE TEMP TABLE tw AS
 SELECT g.ppr, g.sFLX, g.sRB,
   GREATEST(SUM(CASE WHEN pos.p='RB' AND pf.is_started=1 THEN 1 ELSE 0 END) - MAX(g.sRB)*MAX(g.num_teams),0) rb_flex,
   SUM(CASE WHEN pos.p='RB' AND pf.is_started=1 THEN 1 ELSE 0 END)::DOUBLE/MAX(g.num_teams) rb_started_pt
 FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
 JOIN lg g ON g.db_name=pf.db_name
 WHERE pf.year=2024 AND pf.week BETWEEN 1 AND 17 GROUP BY pf.db_name, pf.week, 1,2,3""")
print("\nMECHANISM vs OBSERVED, RB started per team:")
d=con.execute("""SELECT ppr, COUNT(*) tw, MEDIAN(sRB) ded, MEDIAN(sFLX) flex,
   ROUND(MEDIAN(rb_started_pt),2) observed_started_pt FROM tw GROUP BY 1 ORDER BY 1""").fetchdf()
# R17 flex shares, measured for all three positions at once
RB_FLEX_SHARE = {"std": 0.468, "half": 0.332, "ppr": 0.320}
d["rb_flex_share"] = d.ppr.map(RB_FLEX_SHARE)
d["derived_started_pt"] = (d.ded + d.flex * d.rb_flex_share).round(2)
d["gap"] = (d.observed_started_pt - d.derived_started_pt).round(2)
print(d.to_string(index=False))
con.close()
