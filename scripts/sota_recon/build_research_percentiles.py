"""Generate research-LAMAR replacement percentiles from the live league population.

QB/RB/WR/TE use season-level distinct rostered counts. K/DEF use eligible
league-week roster depth divided by the weekly NFL pool, which avoids treating
season-long streaming churn as replacement depth.

Scope: 56 configs = base 36 (flx/sflx/idp x 0/half/ppr) + TEP (half,ppr) +
PPFD (flx/sflx/idp). Writes _percentiles_out.txt for review.
"""
import sys
from pathlib import Path
_FFS = Path(__file__).resolve().parent.parent.parent / "fantasy_football_data_scripts"
sys.path.insert(0, str(_FFS))
from multi_league.core.db_reader import get_reader

CENSUS_YEAR_FILTER = "BETWEEN 2022 AND 2025"
OFF_POS = ("QB", "RB", "WR", "TE")
SPECIAL_POS = ("K", "DEF")

NUM_OFF_SQL = f"""
WITH cfg AS (
  SELECT db_name, year,
    CASE WHEN TRY_CAST(num_teams AS INT)<=11 THEN 10 ELSE 12 END sz,
    CASE WHEN COALESCE(TRY_CAST(scoring_bonus_rec_te AS DOUBLE),0)>0 THEN 'tep'
         WHEN COALESCE(TRY_CAST(roster_DL AS INT),0)+COALESCE(TRY_CAST(roster_LB AS INT),0)+COALESCE(TRY_CAST(roster_DB AS INT),0)+COALESCE(TRY_CAST(roster_IDP AS INT),0)>0 THEN 'idp'
         WHEN COALESCE(TRY_CAST(roster_SUPER_FLEX AS INT),0)>0 OR COALESCE(TRY_CAST(roster_QB AS INT),0)>=2 THEN 'sflx'
         ELSE 'flx' END roster,
    CASE WHEN COALESCE(TRY_CAST(scoring_rush_fd AS DOUBLE),0)<>0 OR COALESCE(TRY_CAST(scoring_rec_fd AS DOUBLE),0)<>0 THEN 'ppfd'
         WHEN TRY_CAST(scoring_rec AS DOUBLE)=1 THEN 'ppr' WHEN TRY_CAST(scoring_rec AS DOUBLE)=0.5 THEN 'half' ELSE '0ppr' END ppr,
    CASE WHEN TRY_CAST(scoring_pass_td AS DOUBLE)=6 THEN 6 ELSE 4 END td
  FROM public.league_settings
  WHERE year {CENSUS_YEAR_FILTER} ),
ros AS (
  SELECT db_name, year, position pos, COUNT(DISTINCT NFL_player_id) ncnt
  FROM public.player_fantasy
  WHERE year {CENSUS_YEAR_FILTER}
    AND LOWER(CAST(is_rostered AS VARCHAR)) IN ('true','1')
    AND position IN ('QB','RB','WR','TE')
  GROUP BY 1,2,3 )
SELECT c.sz, c.roster, c.ppr, c.td, ros.pos, COUNT(*) lg_yrs, AVG(ros.ncnt) avg_rostered
FROM ros JOIN cfg c ON c.db_name=ros.db_name AND c.year=ros.year
GROUP BY 1,2,3,4,5"""
NUM_SPECIAL_SQL = f"""
WITH cfg AS (
  SELECT db_name, year,
    CASE WHEN TRY_CAST(num_teams AS INT)<=11 THEN 10 ELSE 12 END sz,
    CASE WHEN COALESCE(TRY_CAST(scoring_bonus_rec_te AS DOUBLE),0)>0 THEN 'tep'
         WHEN COALESCE(TRY_CAST(roster_DL AS INT),0)+COALESCE(TRY_CAST(roster_LB AS INT),0)+COALESCE(TRY_CAST(roster_DB AS INT),0)+COALESCE(TRY_CAST(roster_IDP AS INT),0)>0 THEN 'idp'
         WHEN COALESCE(TRY_CAST(roster_SUPER_FLEX AS INT),0)>0 OR COALESCE(TRY_CAST(roster_QB AS INT),0)>=2 THEN 'sflx'
         ELSE 'flx' END roster,
    CASE WHEN COALESCE(TRY_CAST(scoring_rush_fd AS DOUBLE),0)<>0 OR COALESCE(TRY_CAST(scoring_rec_fd AS DOUBLE),0)<>0 THEN 'ppfd'
         WHEN TRY_CAST(scoring_rec AS DOUBLE)=1 THEN 'ppr' WHEN TRY_CAST(scoring_rec AS DOUBLE)=0.5 THEN 'half' ELSE '0ppr' END ppr,
    CASE WHEN TRY_CAST(scoring_pass_td AS DOUBLE)=6 THEN 6 ELSE 4 END td
  FROM public.league_settings
  WHERE year {CENSUS_YEAR_FILTER} ),
league_weeks AS (
  SELECT DISTINCT db_name, year, week FROM public.player_fantasy
  WHERE year {CENSUS_YEAR_FILTER} AND week IS NOT NULL ),
counts AS (
  SELECT db_name, year, week, position pos, COUNT(DISTINCT NFL_player_id) ncnt
  FROM public.player_fantasy
  WHERE year {CENSUS_YEAR_FILTER}
    AND LOWER(CAST(is_rostered AS VARCHAR)) IN ('true','1')
    AND position IN ('K','DEF')
  GROUP BY 1,2,3,4 ),
seen AS (
  SELECT DISTINCT db_name, year, position pos
  FROM public.player_fantasy
  WHERE year {CENSUS_YEAR_FILTER}
    AND LOWER(CAST(is_rostered AS VARCHAR)) IN ('true','1')
    AND position IN ('K','DEF') ),
elig AS (
  SELECT db_name, year, pos FROM seen )
SELECT c.sz, c.roster, c.ppr, c.td, elig.pos,
       COUNT(DISTINCT elig.db_name || ':' || CAST(elig.year AS VARCHAR)) lg_yrs,
       AVG(COALESCE(counts.ncnt,0)) avg_rostered
FROM elig
JOIN cfg c ON c.db_name=elig.db_name AND c.year=elig.year
JOIN league_weeks lw ON lw.db_name=elig.db_name AND lw.year=elig.year
LEFT JOIN counts ON counts.db_name=lw.db_name AND counts.year=lw.year AND counts.week=lw.week AND counts.pos=elig.pos
GROUP BY 1,2,3,4,5"""
DEN_OFF_SQL = f"""SELECT position pos, AVG(cnt) pool FROM (
  SELECT CAST(year AS INT) yr, position, COUNT(DISTINCT NFL_player_id) cnt FROM nfl_historical.nfl_player_stats_all
  WHERE CAST(year AS INT) {CENSUS_YEAR_FILTER} AND position IN ('QB','RB','WR','TE')
    AND TRY_CAST(fpts_4pt_ppr AS DOUBLE) IS NOT NULL
  GROUP BY 1,2) GROUP BY 1"""
DEN_SPECIAL_SQL = f"""SELECT position pos, AVG(cnt) pool FROM (
  SELECT CAST(year AS INT) yr, week, position, COUNT(DISTINCT NFL_player_id) cnt
  FROM nfl_historical.nfl_player_stats_all
  WHERE CAST(year AS INT) {CENSUS_YEAR_FILTER} AND season_type='REG' AND position IN ('K','DEF')
    AND ((position='K' AND TRY_CAST(pts_k_yds AS DOUBLE) IS NOT NULL)
      OR (position='DEF' AND TRY_CAST(pts_def_std AS DOUBLE) IS NOT NULL))
  GROUP BY 1,2,3) GROUP BY 1"""
r=get_reader()
num=r.query(NUM_OFF_SQL, database="___leagues") + r.query(NUM_SPECIAL_SQL, database="___leagues")
den={x["pos"]: float(x["pool"]) for x in r.query(DEN_OFF_SQL, database="___ops")}
den.update({x["pos"]: float(x["pool"]) for x in r.query(DEN_SPECIAL_SQL, database="___ops")})
PPR_KEY={"0ppr":0.0,"half":0.5,"ppr":1.0,"ppfd":"ppfd"}
# which positions belong to each roster type
POS_BY_ROSTER={"flx":["QB","RB","WR","TE","K","DEF"],"sflx":["QB","RB","WR","TE","K","DEF"],
  "tep":["QB","RB","WR","TE","K","DEF"],"idp":["QB","RB","WR","TE","K","DEF","DL","LB","DB"]}
def in_scope(roster,ppr):
    if roster in ("flx","sflx","idp") and ppr in ("0ppr","half","ppr"): return True   # base 36
    if roster=="tep" and ppr in ("half","ppr"): return True                            # +TEP
    if roster in ("flx","sflx","idp") and ppr=="ppfd": return True                     # +PPFD
    return False
agg={}
for x in num:
    k=(int(x["sz"]),x["roster"],x["ppr"],int(x["td"]))
    if not in_scope(x["roster"],x["ppr"]): continue
    agg.setdefault(k,{})[x["pos"]]={"avg":float(x["avg_rostered"]),"lg":int(x["lg_yrs"])}
out=[f"# season pools: {den}",f"# configs in scope: {len(agg)}","CENSUS_PERCENTILES = {"]
for k in sorted(agg):
    sz,roster,ppr,td=k; pk=PPR_KEY[ppr]
    pcts={}; supp={}
    for pos in POS_BY_ROSTER[roster]:
        d=agg[k].get(pos)
        if d and den.get(pos):
            pcts[pos]=round(min(0.95, d["avg"]/den[pos]),2); supp[pos]=d["lg"]
    keystr=f"({sz}, {roster!r}, {pk!r}, {td})" if isinstance(pk,str) else f"({sz}, {roster!r}, {pk}, {td})"
    minlg=min(supp.values()) if supp else 0
    out.append(f"    {keystr}: {pcts},  # n>={minlg}")
out.append("}")
open(str(Path(__file__).resolve().parent/"_percentiles_out.txt"),"w").write("\n".join(out)+"\n")
print("configs:",len(agg))
