"""RB steps 3-5: observed capacity -> cutoffs -> stability + dumping-ground checks.

Mirrors what WR got in R21/R22/R23. Cutoffs are quantile-matched to the literal num_teams
shape so the population stays recognisable while membership differs on real capacity, then
checked for year-to-year drift and for the dumping-ground failure the declared-slot tier had
(hi/lo 15.9x, log SD 0.246 -- the observed tier fixed it to 1.5-1.8x / 0.08).
"""
import duckdb, numpy as np, pandas as pd
rows, tops, memb = [], [], []
for YR in range(2021, 2026):
    con = duckdb.connect(config={"memory_limit":"1400MB","threads":2,
        "temp_directory":f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/rb3{YR}"})
    for s in ("SET enable_progress_bar=false","SET preserve_insertion_order=false",
              "PRAGMA max_temp_directory_size='3GB'"): con.execute(s)
    con.execute("ATTACH 'D:/league-history-data/fantasy_leagues/tmp/research_public_lake_20260730/corpus_snapshot.duckdb' AS l (READ_ONLY)")
    con.execute("ATTACH 'D:/tmp/research_public_lake_stage_20260722/ops_cache.duckdb' AS o (READ_ONLY)")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid,
      MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all
      WHERE "year"={YR} AND NFL_player_id IS NOT NULL AND position NOT LIKE '%,%' GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cap AS
     SELECT c.db_name, s.num_teams, c.ros, c.st FROM
       (SELECT pf.db_name, AVG(nros) ros, AVG(nst) st FROM
          (SELECT pf.db_name, pf.week, COUNT(*) nros,
                  SUM(CASE WHEN pf.is_started=1 THEN 1 ELSE 0 END) nst
           FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
           WHERE pf.year={YR} AND pf.week BETWEEN 1 AND 17 AND pos.p='RB' GROUP BY 1,2) pf
        GROUP BY 1) c
     JOIN l.public.league_settings s ON s.db_name=c.db_name AND s.year={YR}
     WHERE s.num_teams BETWEEN 4 AND 24""")
    t = con.execute("""SELECT SUM(CASE WHEN num_teams<=8 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE a,
       SUM(CASE WHEN num_teams<=10 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE b,
       SUM(CASE WHEN num_teams<=12 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE c FROM cap""").fetchdf().iloc[0]
    for stat, col in (("rostered","ros"), ("started","st")):
        q = con.execute(f"""SELECT COUNT(*) n, MEDIAN({col}) med,
             quantile_cont({col},{t.a}) c1, quantile_cont({col},{t.b}) c2,
             quantile_cont({col},{t.c}) c3 FROM cap""").fetchdf().iloc[0]
        rows.append({"year":YR,"stat":stat,"leagues":int(q.n),"median":round(q.med,1),
                     "c1":round(q.c1,1),"c2":round(q.c2,1),"c3":round(q.c3,1)})
        tp = con.execute(f"""SELECT COUNT(*) n, MIN({col}) lo, MAX({col}) hi,
              stddev(ln(NULLIF({col},0))) lsd FROM cap WHERE {col}>={q.c3}""").fetchdf().iloc[0]
        tops.append({"year":YR,"stat":stat,"top_n":int(tp.n),
                     "ratio":round(tp.hi/max(tp.lo,0.01),1),"log_sd":round(tp.lsd,3)})
        if YR==2024:
            m=con.execute(f"""SELECT num_teams, COUNT(*) n,
                 SUM(CASE WHEN {col}<{q.c1} THEN 1 ELSE 0 END) t08,
                 SUM(CASE WHEN {col}>={q.c1} AND {col}<{q.c2} THEN 1 ELSE 0 END) t10,
                 SUM(CASE WHEN {col}>={q.c2} AND {col}<{q.c3} THEN 1 ELSE 0 END) t12,
                 SUM(CASE WHEN {col}>={q.c3} THEN 1 ELSE 0 END) t14
               FROM cap GROUP BY 1 HAVING COUNT(*)>=300 ORDER BY 1""").fetchdf()
            m["stat"]=stat; memb.append(m)
    con.close()
pd.set_option("display.width",210)
r=pd.DataFrame(rows)
print("STEP 3 - observed RB capacity and quantile cutoffs, by year\n")
print(r.to_string(index=False))
p=r.groupby("stat")[["median","c1","c2","c3"]].median().round(1)
print("\nPOOLED 2021-2025:"); print(p.to_string())
print("\nSTEP 4 - year-to-year drift (WR was accepted at 6-19%):")
for stat,g in r.groupby("stat"):
    for c in ("c1","c2","c3"):
        print(f"  {stat:8s} {c}: {100*(g[c].max()-g[c].min())/g[c].mean():.0f}%")
print("\nSTEP 5 - top tier: dumping ground? (declared-slot WR was 15.9x / 0.246)")
print(pd.DataFrame(tops).to_string(index=False))
print("\nMEMBERSHIP 2024 - literal team count vs derived tier:")
for m in memb:
    print(f"\n  --- {m.stat.iloc[0]} ---"); print(m.drop(columns=['stat']).to_string(index=False))
