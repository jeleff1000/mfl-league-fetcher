"""Validate improved pbp drive reconstruction vs 1998+ drives-table truth.
Improvements over the naive version:
  - snap count excludes punt/FG/penalty/kickoff/XP/kneel rows (fixes the play-count bug)
  - period-end drive termination via the quarter column (halftime q2->q3, game end last row)
  - fumble is a drive-end ONLY when recoverer's team != fumbler's team (roster team-match),
    so own/teammate recoveries don't over-segment
  - fourth_down_stop = 4th-down failure (gain<ytg, not punt/FG/TD/kneel/first-down)
Writes accuracy to _validate_drive_out.txt.
"""
import platform
try: platform.uname()
except Exception: pass
import duckdb, re
B='D:/league-history-data/nfl/raw/pfr/boxscores/tables'
d=duckdb.connect(); d.execute("PRAGMA threads=4"); d.execute("SET memory_limit='6GB'")
P=f"read_parquet('{B}/pbp/_combined.parquet')"
LO,HI=1998,2024

ocols=[c[0] for c in d.execute(f"DESCRIBE SELECT * FROM read_parquet('{B}/player_offense/_combined.parquet')").fetchall()]
dcols=[c[0] for c in d.execute(f"DESCRIBE SELECT * FROM read_parquet('{B}/player_defense/_combined.parquet')").fetchall()]
def pick(cols,*cands):
    for c in cands:
        if c in cols: return c
    return None
onm=pick(ocols,'player','player_name','name'); otm=pick(ocols,'team','tm','team_abbr')
dnm=pick(dcols,'player','player_name','name'); dtm=pick(dcols,'team','tm','team_abbr')
NN="lower(regexp_replace({},'[^A-Za-z]','','g'))"
# per-boxscore roster: normalized name -> team (union offense+defense)
d.execute(f"""CREATE TEMP TABLE roster AS
   SELECT boxscore_id, {NN.format(onm)} nn, ANY_VALUE({otm}) team FROM read_parquet('{B}/player_offense/_combined.parquet')
     WHERE {onm} IS NOT NULL GROUP BY 1,2
   UNION
   SELECT boxscore_id, {NN.format(dnm)} nn, ANY_VALUE({dtm}) team FROM read_parquet('{B}/player_defense/_combined.parquet')
     WHERE {dnm} IS NOT NULL GROUP BY 1,2""")
# dedup: a name could appear on both rosters for a team; keep one team per (boxscore,nn)
d.execute("""CREATE TEMP TABLE rost AS SELECT boxscore_id, nn, ANY_VALUE(team) team FROM roster GROUP BY 1,2""")

NAME=r"([A-Z][A-Za-z.''\-]+(?: [A-Z][A-Za-z.''\-]+){0,2})"
d.execute(f"""CREATE TEMP TABLE pp AS SELECT boxscore_id, row_index_in_table ri, quarter q, TRY_CAST(down AS INT) dn,
   TRY_CAST(yds_to_go AS INT) ytg, lower(detail) ld, detail,
   CASE WHEN lower(detail) LIKE '%for no gain%' THEN 0 ELSE TRY_CAST(regexp_extract(lower(detail),'for (-?\\d+) yard',1) AS INT) END gain,
   {NN.format("regexp_extract(detail,'"+NAME+" fumbles',1)")} fumbler_nn,
   {NN.format("regexp_extract(detail,'recovered by "+NAME+"',1)")} recov_nn
   FROM {P} WHERE CAST(season AS INT) BETWEEN {LO} AND {HI} AND detail IS NOT NULL AND detail<>''""")
# attach teams for fumbler / recoverer
d.execute("""CREATE TEMP TABLE pj AS SELECT pp.*, rf.team ftm, rr.team rtm
   FROM pp LEFT JOIN rost rf ON rf.boxscore_id=pp.boxscore_id AND rf.nn=pp.fumbler_nn
           LEFT JOIN rost rr ON rr.boxscore_id=pp.boxscore_id AND rr.nn=pp.recov_nn""")
d.execute("""CREATE TEMP TABLE ev AS SELECT *,
   CASE WHEN dn IS NULL THEN 0 WHEN ld LIKE 'penalty on%' THEN 0
        WHEN ld LIKE '%(no play)%' THEN 0 WHEN ld LIKE '%timeout%' THEN 0
        WHEN ld LIKE '%punt%' OR ld LIKE '%field goal%' OR ld LIKE '%kicks off%' OR ld LIKE '%extra point%' OR ld LIKE '%kneel%' THEN 0 ELSE 1 END is_snap,
   CASE WHEN ld LIKE '%punt%' AND ld NOT LIKE '%fake%' THEN 'Punt'
        WHEN ld LIKE '%intercepted%' THEN 'Int'
        WHEN ld LIKE '%fumble%' AND ld LIKE '%recovered by%' AND ftm IS NOT NULL AND rtm IS NOT NULL AND ftm<>rtm THEN 'Fum'
        WHEN ld LIKE '%field goal%' THEN 'FG' WHEN ld LIKE '%touchdown%' THEN 'TD' WHEN ld LIKE '%safety%' THEN 'Saf'
        -- turnover on downs: 4th-down EXPLICIT failure (incomplete / sacked / stuffed run with parsed gain<ytg),
        -- never a conversion, TD, kneel, or any penalty play (penalties replay or grant 1st down)
        WHEN dn=4 AND ld NOT LIKE '%first down%' AND ld NOT LIKE '%touchdown%' AND ld NOT LIKE '%kneel%'
             AND ld NOT LIKE '%penalty%' AND ld NOT LIKE '%aborted%'
             AND (ld LIKE '%incomplete%' OR ld LIKE '%sacked%'
                  OR (gain IS NOT NULL AND ytg IS NOT NULL AND gain<ytg)) THEN 'Downs'
        ELSE NULL END endev0
   FROM pj""")
# period-end: terminate open drive at halftime (last row with q='2' before q='3') and at game end (last row of boxscore)
d.execute("""CREATE TEMP TABLE ev2 AS SELECT *,
   CASE WHEN endev0 IS NOT NULL THEN endev0
        WHEN q='2' AND LEAD(q) OVER (PARTITION BY boxscore_id ORDER BY ri)='3' THEN 'Half'
        WHEN ri = MAX(ri) OVER (PARTITION BY boxscore_id) THEN 'End'
        ELSE NULL END endev FROM ev""")
d.execute("""CREATE TEMP TABLE seg AS SELECT *, COALESCE(SUM(CASE WHEN endev IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY boxscore_id ORDER BY ri ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) drive_id FROM ev2""")
d.execute("""CREATE TEMP TABLE drv AS SELECT boxscore_id, drive_id, SUM(is_snap) plays, MAX(endev) FILTER(WHERE endev IS NOT NULL) e FROM seg GROUP BY 1,2""")
d.execute("""CREATE TEMP TABLE recon AS SELECT boxscore_id, SUM(CASE WHEN plays<=3 AND e='Punt' THEN 1 ELSE 0 END) r_to, SUM(CASE WHEN e='Downs' THEN 1 ELSE 0 END) r_fds FROM drv GROUP BY 1""")
d.execute(f"""CREATE TEMP TABLE truth AS SELECT boxscore_id,
   SUM(CASE WHEN TRY_CAST(play_count_tip AS INT)<=3 AND end_event='Punt' THEN 1 ELSE 0 END) t_to,
   SUM(CASE WHEN end_event='Downs' THEN 1 ELSE 0 END) t_fds
   FROM (SELECT boxscore_id,play_count_tip,end_event FROM read_parquet('{B}/home_drives/_combined.parquet') WHERE CAST(season AS INT) BETWEEN {LO} AND {HI}
         UNION ALL SELECT boxscore_id,play_count_tip,end_event FROM read_parquet('{B}/vis_drives/_combined.parquet') WHERE CAST(season AS INT) BETWEEN {LO} AND {HI}) GROUP BY 1""")
res=d.execute("""SELECT 'three_out' m, ROUND(100.0*AVG((r_to=t_to)::INT),1) exact, ROUND(100.0*AVG((abs(r_to-t_to)<=1)::INT),1) w1, SUM(r_to) recon, SUM(t_to) truth FROM truth JOIN recon USING(boxscore_id)
   UNION ALL SELECT 'fourth_down_stop', ROUND(100.0*AVG((r_fds=t_fds)::INT),1), ROUND(100.0*AVG((abs(r_fds-t_fds)<=1)::INT),1), SUM(r_fds), SUM(t_fds) FROM truth JOIN recon USING(boxscore_id)""").fetchall()
out=[f"roster cols: off({onm},{otm}) def({dnm},{dtm})","metric            exact  w1   recon  truth"]
for r in res: out.append(f"{r[0]:<16} {r[1]:>5} {r[2]:>5}  {r[3]} {r[4]}")
open("scripts/sota_recon/_validate_drive_out.txt","w").write("\n".join(out)+"\nDONE\n")
