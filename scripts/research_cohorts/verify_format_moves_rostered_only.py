"""Does FORMAT move started, or only rostered?

Joe: start% should stay fairly consistent -- it is rostered that moves. If true, the tier cuts
only need keying on format for ROSTERED, and started stays one set per position.
"""
import duckdb, pandas as pd
con = duckdb.connect(config={"memory_limit":"1400MB","threads":2,
    "temp_directory":"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/fmtsplit"})
for s in ("SET enable_progress_bar=false","SET preserve_insertion_order=false",
          "PRAGMA max_temp_directory_size='3GB'"): con.execute(s)
con.execute("ATTACH 'D:/league-history-data/fantasy_leagues/tmp/research_public_lake_20260730/corpus_snapshot.duckdb' AS l (READ_ONLY)")
con.execute("ATTACH 'D:/tmp/research_public_lake_stage_20260722/ops_cache.duckdb' AS o (READ_ONLY)")
con.execute("""CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid,
  MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all
  WHERE "year"=2024 AND NFL_player_id IS NOT NULL AND position NOT LIKE '%,%' GROUP BY 1""")
con.execute("""CREATE OR REPLACE TEMP TABLE cap AS
 SELECT c.db_name, s.num_teams, c.pos_p,
   CASE WHEN COALESCE(s.sleeper_best_ball,false) THEN 'bestball'
        WHEN COALESCE(s.is_dynasty,false) THEN 'dynasty' ELSE 'redraft' END fmtx,
   c.ros/s.num_teams rpt, c.st/s.num_teams spt
 FROM (SELECT pf.db_name, pos.p pos_p, AVG(nros) ros, AVG(nst) st FROM
        (SELECT pf.db_name, pf.week, pos.p, COUNT(*) nros,
                SUM(CASE WHEN pf.is_started=1 THEN 1 ELSE 0 END) nst
         FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
         WHERE pf.year=2024 AND pf.week BETWEEN 1 AND 17
           AND pos.p IN ('QB','RB','WR','TE') GROUP BY 1,2,3) pf
       JOIN pos ON pos.p=pf.p GROUP BY 1,2) c
 JOIN l.public.league_settings s ON s.db_name=c.db_name AND s.year=2024
 WHERE s.num_teams BETWEEN 8 AND 14 AND COALESCE(s.roster_SUPER_FLEX,0)=0
   AND COALESCE(s.roster_IDP,0)=0 AND COALESCE(s.roster_DL,0)=0""")
pd.set_option("display.width",200)
d=con.execute("""SELECT pos_p AS pos_name, fmtx, COUNT(*) leagues,
   ROUND(MEDIAN(spt),2) started_pt, ROUND(MEDIAN(rpt),2) rostered_pt
  FROM cap GROUP BY 1,2 HAVING COUNT(*)>=100 ORDER BY 1,2""").fetchdf()
print("STARTED vs ROSTERED per team, by FORMAT (2024, 1QB non-IDP)\n")
print(d.to_string(index=False))
print("\nspread across formats, per position:")
for p,g in d.groupby("pos_name"):
    ss=100*(g.started_pt.max()-g.started_pt.min())/g.started_pt.mean()
    rr=100*(g.rostered_pt.max()-g.rostered_pt.min())/g.rostered_pt.mean()
    print(f"  {p}: started {ss:.0f}%   rostered {rr:.0f}%")
con.close()
