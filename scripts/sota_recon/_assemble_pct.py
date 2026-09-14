"""Assemble FINAL research_lamar CENSUS_PERCENTILES for the 56-config target.

Offense (QB/RB/WR/TE) = season distinct rostered population re-derivation.
K/DEF = eligible league-week roster depth, so streaming churn does not turn K/DST
replacement into the bottom of the NFL pool.
IDP (DL/LB/DB) for idp roster = existing hardcoded (full-pool denom wrong for IDP).
ppr 'ppfd' borrows IDP from ppr=1.0.

Thin n<8 league-years fall back to the densest (size, roster, position) sibling.
Writes _final_pct.txt (ready to wire into research_lamar.py).
"""
import sys
from pathlib import Path
_FFS = Path(__file__).resolve().parent.parent.parent / "fantasy_football_data_scripts"
sys.path.insert(0, str(_FFS))
from multi_league.core.db_reader import get_reader
from multi_league.data_fetchers.research_lamar import CENSUS_PERCENTILES as OLD
OFF=["QB","RB","WR","TE"]; SPECIAL=["K","DEF"]; IDP=["DL","LB","DB"]
PPR_KEY={"0ppr":0.0,"half":0.5,"ppr":1.0,"ppfd":"ppfd"}
CENSUS_YEAR_FILTER="BETWEEN 2022 AND 2025"
MIN_LG=8
NUM_OFF=f"""WITH cfg AS (SELECT db_name, year,
   CASE WHEN TRY_CAST(num_teams AS INT)<=11 THEN 10 ELSE 12 END sz,
   CASE WHEN COALESCE(TRY_CAST(scoring_bonus_rec_te AS DOUBLE),0)>0 THEN 'tep'
        WHEN COALESCE(TRY_CAST(roster_DL AS INT),0)+COALESCE(TRY_CAST(roster_LB AS INT),0)+COALESCE(TRY_CAST(roster_DB AS INT),0)+COALESCE(TRY_CAST(roster_IDP AS INT),0)>0 THEN 'idp'
        WHEN COALESCE(TRY_CAST(roster_SUPER_FLEX AS INT),0)>0 OR COALESCE(TRY_CAST(roster_QB AS INT),0)>=2 THEN 'sflx' ELSE 'flx' END roster,
   CASE WHEN COALESCE(TRY_CAST(scoring_rush_fd AS DOUBLE),0)<>0 OR COALESCE(TRY_CAST(scoring_rec_fd AS DOUBLE),0)<>0 THEN 'ppfd'
        WHEN TRY_CAST(scoring_rec AS DOUBLE)=1 THEN 'ppr' WHEN TRY_CAST(scoring_rec AS DOUBLE)=0.5 THEN 'half' ELSE '0ppr' END ppr,
   CASE WHEN TRY_CAST(scoring_pass_td AS DOUBLE)=6 THEN 6 ELSE 4 END td
   FROM public.league_settings WHERE year {CENSUS_YEAR_FILTER}),
ros AS (SELECT db_name, year, position pos, COUNT(DISTINCT NFL_player_id) ncnt FROM public.player_fantasy
   WHERE year {CENSUS_YEAR_FILTER} AND LOWER(CAST(is_rostered AS VARCHAR)) IN ('true','1') AND position IN ('QB','RB','WR','TE') GROUP BY 1,2,3)
SELECT c.sz, c.roster, c.ppr, c.td, ros.pos, COUNT(*) lg, AVG(ros.ncnt) avgr FROM ros JOIN cfg c ON c.db_name=ros.db_name AND c.year=ros.year
GROUP BY 1,2,3,4,5"""
NUM_SPECIAL=f"""WITH cfg AS (SELECT db_name, year,
   CASE WHEN TRY_CAST(num_teams AS INT)<=11 THEN 10 ELSE 12 END sz,
   CASE WHEN COALESCE(TRY_CAST(scoring_bonus_rec_te AS DOUBLE),0)>0 THEN 'tep'
        WHEN COALESCE(TRY_CAST(roster_DL AS INT),0)+COALESCE(TRY_CAST(roster_LB AS INT),0)+COALESCE(TRY_CAST(roster_DB AS INT),0)+COALESCE(TRY_CAST(roster_IDP AS INT),0)>0 THEN 'idp'
        WHEN COALESCE(TRY_CAST(roster_SUPER_FLEX AS INT),0)>0 OR COALESCE(TRY_CAST(roster_QB AS INT),0)>=2 THEN 'sflx' ELSE 'flx' END roster,
   CASE WHEN COALESCE(TRY_CAST(scoring_rush_fd AS DOUBLE),0)<>0 OR COALESCE(TRY_CAST(scoring_rec_fd AS DOUBLE),0)<>0 THEN 'ppfd'
        WHEN TRY_CAST(scoring_rec AS DOUBLE)=1 THEN 'ppr' WHEN TRY_CAST(scoring_rec AS DOUBLE)=0.5 THEN 'half' ELSE '0ppr' END ppr,
   CASE WHEN TRY_CAST(scoring_pass_td AS DOUBLE)=6 THEN 6 ELSE 4 END td,
   FROM public.league_settings WHERE year {CENSUS_YEAR_FILTER}),
league_weeks AS (
   SELECT DISTINCT db_name, year, week FROM public.player_fantasy
   WHERE year {CENSUS_YEAR_FILTER} AND week IS NOT NULL),
counts AS (
   SELECT db_name, year, week, position pos, COUNT(DISTINCT NFL_player_id) ncnt
   FROM public.player_fantasy
   WHERE year {CENSUS_YEAR_FILTER} AND position IN ('K','DEF')
     AND LOWER(CAST(is_rostered AS VARCHAR)) IN ('true','1')
   GROUP BY 1,2,3,4),
seen AS (
   SELECT DISTINCT db_name, year, position pos
   FROM public.player_fantasy
   WHERE year {CENSUS_YEAR_FILTER} AND position IN ('K','DEF')
     AND LOWER(CAST(is_rostered AS VARCHAR)) IN ('true','1')),
elig AS (
   SELECT db_name, year, pos FROM seen)
SELECT c.sz, c.roster, c.ppr, c.td, elig.pos,
       COUNT(DISTINCT elig.db_name || ':' || CAST(elig.year AS VARCHAR)) lg,
       COUNT(*) league_weeks,
       AVG(COALESCE(counts.ncnt,0)) avgr
FROM elig
JOIN cfg c ON c.db_name=elig.db_name AND c.year=elig.year
JOIN league_weeks lw ON lw.db_name=elig.db_name AND lw.year=elig.year
LEFT JOIN counts ON counts.db_name=lw.db_name AND counts.year=lw.year AND counts.week=lw.week AND counts.pos=elig.pos
GROUP BY 1,2,3,4,5"""
DEN_OFF=f"""SELECT position pos, AVG(cnt) pool FROM (SELECT CAST(year AS INT) yr, position, COUNT(DISTINCT NFL_player_id) cnt FROM nfl_historical.nfl_player_stats_all
  WHERE CAST(year AS INT) {CENSUS_YEAR_FILTER} AND position IN ('QB','RB','WR','TE') AND fpts_4pt_ppr IS NOT NULL GROUP BY 1,2) GROUP BY 1"""
DEN_SPECIAL=f"""SELECT position pos, AVG(cnt) pool FROM (
  SELECT CAST(year AS INT) yr, week, position, COUNT(DISTINCT NFL_player_id) cnt
  FROM nfl_historical.nfl_player_stats_all
  WHERE CAST(year AS INT) {CENSUS_YEAR_FILTER} AND season_type='REG' AND position IN ('K','DEF')
    AND ((position='K' AND pts_k_yds IS NOT NULL) OR (position='DEF' AND pts_def_std IS NOT NULL))
  GROUP BY 1,2,3) GROUP BY 1"""
r=get_reader()
den={x["pos"]:float(x["pool"]) for x in r.query(DEN_OFF,database="___ops")}
den.update({x["pos"]:float(x["pool"]) for x in r.query(DEN_SPECIAL,database="___ops")})
raw={}
for x in r.query(NUM_OFF,database="___leagues"):
    raw.setdefault((int(x["sz"]),x["roster"],x["ppr"],int(x["td"])),{})[x["pos"]]={"pct":min(0.95,float(x["avgr"])/den[x["pos"]]),"lg":int(x["lg"])}
for x in r.query(NUM_SPECIAL,database="___leagues"):
    raw.setdefault((int(x["sz"]),x["roster"],x["ppr"],int(x["td"])),{})[x["pos"]]={"pct":min(0.95,float(x["avgr"])/den[x["pos"]]),"lg":int(x["lg"]),"league_weeks":int(x["league_weeks"])}
# target 56 configs
targets=[]
for sz in (10,12):
  for td in (4,6):
    for roster in ("flx","sflx","idp"):
      for ppr in ("0ppr","half","ppr"): targets.append((sz,roster,ppr,td))   # base 36
    for ppr in ("half","ppr"): targets.append((sz,"tep",ppr,td))             # +TEP 8
    for roster in ("flx","sflx","idp"): targets.append((sz,roster,"ppfd",td))# +PPFD 12
def densest_off(sz,roster):  # best (ppr,td) sibling for this (size,roster) by min lg across offense
    best=None;bestlg=-1
    for (s,ro,pp,t),d in raw.items():
        if s==sz and ro==roster:
            lg=min((v["lg"] for k,v in d.items() if k in OFF), default=0)
            if lg>bestlg: bestlg=lg; best=(s,ro,pp,t)
    return best
def densest_pos(sz,roster,pos):
    best=None;bestlg=-1
    for (s,ro,pp,t),d in raw.items():
        if s==sz and ro==roster and pos in d and d[pos]["lg"]>bestlg:
            bestlg=d[pos]["lg"]; best=(s,ro,pp,t)
    return best
def idp_from_old(sz,ppr,td):  # existing hardcoded idp values; ppfd->1.0
    pf=1.0 if ppr in ("ppr","ppfd") else (0.5 if ppr=="half" else 0.0)
    for cand in [(sz,"idp",pf,td),(sz,"idp",pf,4 if td==6 else 6),(sz,"idp",1.0,td),(sz,"idp",0.5,td),(10,"idp",1.0,4)]:
        if cand in OLD: return {p:OLD[cand][p] for p in IDP if p in OLD[cand]}
    return {}
out=["CENSUS_PERCENTILES = {"]
for k in targets:
    sz,roster,ppr,td=k; pk=PPR_KEY[ppr]; pcts={}
    src=raw.get(k,{})
    for pos in OFF:
        d=src.get(pos)
        if d and d["lg"]>=MIN_LG and den.get(pos): pcts[pos]=round(d["pct"],2)
        else:  # thin -> densest sibling
            sib=densest_off(sz,roster); sd=raw.get(sib,{}).get(pos) if sib else None
            if sd and den.get(pos): pcts[pos]=round(min(0.95,sd["pct"]),2)
    for pos in SPECIAL:
        d=src.get(pos)
        if d and d["lg"]>=MIN_LG and den.get(pos):
            pcts[pos]=round(d["pct"],2)
        else:
            sib=densest_pos(sz,roster,pos); sd=raw.get(sib,{}).get(pos) if sib else None
            if sd and den.get(pos): pcts[pos]=round(min(0.95,sd["pct"]),2)
    if roster=="idp": pcts.update(idp_from_old(sz,ppr,td))
    ks=f"({sz}, {roster!r}, {pk!r}, {td})" if isinstance(pk,str) else f"({sz}, {roster!r}, {pk}, {td})"
    out.append(f"    {ks}: {pcts},")
out.append("}")
out.append(f"# {len(targets)} configs")
open(str(Path(__file__).resolve().parent/"_final_pct.txt"),"w").write("\n".join(out)+"\n")
print("targets:",len(targets))
