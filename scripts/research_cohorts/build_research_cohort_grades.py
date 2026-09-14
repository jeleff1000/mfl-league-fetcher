"""Layer 1: bake the grade columns into the research cohort tables (ALL cohorts, local).
Reads the local cohort parquets, computes per-cohort grades, writes *_graded.parquet + a
local research.duckdb the frontend's DuckDB server can serve as the ___research schema.

  draft   += draft_score (LAMAR-drift pctile), draft_score_healthy (durability-neutral), best_pick
  txn     += title_run (0.65 value + 0.35 title-leverage clutch), waiver_value
  matchup += won_pct, lost_pct (start-weighted; sum to start_rate_pct)
"""
import os
import duckdb, numpy as np, pandas as pd
from pathlib import Path
from sklearn.isotonic import IsotonicRegression
from draft_grades import LAMAR_SLUGS, draft_score_from_expected, select_grade_lamar
from cohort_format_sql import SEASON_GRAIN
from grade_join_sql import matchup_clutch_join_sql
import argparse
# --only (2026-07-29): the MATCHUP lane is a passthrough of the assembled season parquet,
# so after a shard rebuild it is the only fresh input. Regrading draft/txn from their older
# cycle parquets would republish stale rows under a fresh timestamp -- the same hazard
# build_research_career_rollup already guards with --only.
_ap = argparse.ArgumentParser(description=__doc__)
_ap.add_argument("--only", action="append", choices=["draft","transactions","matchup"],
                 help="grade only these datasets (repeatable); default all three")
ONLY = set(_ap.parse_args().only or ["draft","transactions","matchup"])

CO=Path(os.environ.get("RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
D=CO/'research_draft_player_season.parquet'; TX=CO/'research_txn_player_season.parquet'; MC=CO/'research_matchup_player_season.parquet'
OPS='D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb'
OUTDB=CO/'research.duckdb'
con=duckdb.connect(); con.execute("SET memory_limit='3000MB'")
o=duckdb.connect(OPS, read_only=True)
gp=o.execute(f"""SELECT NFL_player_id id, year yr, COUNT(*) FILTER (WHERE COALESCE(offense_snaps,0)>0 OR COALESCE(special_teams_snaps,0)>0
    OR COALESCE(defense_snaps,0)>0 OR COALESCE(fantasy_points_ppr,0)<>0) gp
    {''.join(f', SUM(lamar_{slug}) AS canonical_lamar_{slug}' for slug in LAMAR_SLUGS)}
  FROM nfl_historical.nfl_player_stats_all WHERE season_type='REG' GROUP BY 1,2""").df(); o.close()
GP={(r.id,int(r.yr)):int(r.gp) for r in gp.itertuples()}

def pctile(a):
    # tie-AVERAGED ranks: argsort-position ranks hand tied values (e.g. an all-zero clutch
    # column) arbitrary distinct percentiles spread 0..1, which turned into a random ±20%
    # cmult at coarse rungs (ledger D11).
    a=np.asarray(a,float); a=np.where(np.isnan(a),-np.inf,a)
    r=pd.Series(a).rank(method='average').to_numpy()-1.0
    return r/max(len(a)-1,1)

def clutch_pctile(clu):
    # missing clutch is NEUTRAL (0.5 -> cmult 1.0), never ranked-as-zero (ledger D11)
    s=pd.Series(np.asarray(clu,float))
    out=np.full(len(s),0.5)
    m=s.notna().to_numpy()
    if m.sum()>1:
        out[m]=(s[m].rank(method='average').to_numpy()-1.0)/max(m.sum()-1,1)
    return out
KEYS=list(SEASON_GRAIN[:-1])

if 'draft' in ONLY:
    # ---------- DRAFT (join matchup clutch for the title-leverage overlay, §2) ----------
    # clutch joins at the SAME cohort_level -- a rung-4-only join matches zero coarse-rung rows
    # and every score there degrades to noise (ledger D11)
    d=con.execute(matchup_clutch_join_sql(D.as_posix(), MC.as_posix())).df()
    d=d.merge(gp, left_on=['NFL_player_id','year'], right_on=['id','yr'], how='left').drop(columns=['id','yr','gp'])
    d['grade_lamar']=select_grade_lamar(d)
    d['gp']=[GP.get((pid,int(y)),0) for pid,y in zip(d.NFL_player_id, d.year)]
    d['draft_score']=np.nan; d['draft_score_healthy']=np.nan; d['best_pick']=np.nan
    for _,idx in d.groupby(KEYS).groups.items():
        g=d.loc[idx]; m=g.grade_lamar.notna() & g.adp.notna()
        if m.sum()<20: continue
        sub=g[m]; val=sub.grade_lamar.values.astype(float); adp=sub.adp.values.astype(float); gpv=sub.gp.values.astype(float)
        imp=pctile(val)
        eL=IsotonicRegression(increasing=False,out_of_bounds='clip').fit(adp,val).predict(adp)
        surplus=val-eL                                     # §1 RAW LAMAR-away-from-curve drift (uncapped, +/-)
        cl=clutch_pctile(sub.clu.values)                   # cohort clutch percentile, 0.5-neutral when missing
        per=np.where(gpv>0,val/np.where(gpv>0,gpv,1),np.nan); mm=~np.isnan(per)
        if mm.sum()>=8:
            prior=IsotonicRegression(increasing=False,out_of_bounds='clip').fit(adp[mm],per[mm]).predict(adp)
        else:
            prior=np.full(len(val), np.nanmedian(per) if mm.any() else 0.0)
        dn=(val+4.0*prior)/(np.where(gpv>0,gpv,17.0)+4.0)*17.0
        dn_surplus=dn-IsotonicRegression(increasing=False,out_of_bounds='clip').fit(adp,dn).predict(adp)
        # Draft Score = DISTANCE FROM THE ISOTONIC LINE x CLUTCH FACTOR (Joe 2026-07-20).
        # Absolute LAMAR units, UNCAPPED: a league-altering season must not be compressed against
        # a ceiling next to a merely-great one, which is exactly what percentiling did. Cross-cohort
        # comparability does not need the percentile any more -- canonical LAMAR is scoring-
        # normalized by construction.
        #
        # The multiplier is SIGN-AWARE. A plain positive multiplier inverts the clutch effect
        # below the line: a bust that delivered in clutch weeks would be pushed farther down.
        d.loc[sub.index,'best_pick']=(imp*100).round(1)
        d.loc[sub.index,'draft_score']=draft_score_from_expected(val, eL, cl).round(1)
        d.loc[sub.index,'draft_score_healthy']=draft_score_from_expected(
            dn, dn - dn_surplus, cl).round(1)
    con.register('draft_g', d.drop(columns=['clu','grade_lamar', *[f'canonical_lamar_{s}' for s in LAMAR_SLUGS]])); con.execute(f"COPY draft_g TO '{(CO/'research_draft_graded.parquet').as_posix()}'")

if 'transactions' in ONLY:
    # ---------- TXN (join matchup clutch for title-leverage) ----------
    t=con.execute(matchup_clutch_join_sql(TX.as_posix(), MC.as_posix())).df()
    W_CLU_TXN=0.35   # runbook §4: waiver clutch weight (heavier than draft's 0.20; Joe 2026-07-16)
    t['title_run']=np.nan; t['waiver_value']=np.nan
    for _,idx in t.groupby(KEYS).groups.items():
        g=t.loc[idx]; m=g.avg_add_lamar.notna()
        if m.sum()<15: continue
        sub=g[m]; avr=sub.avg_add_lamar.values.astype(float); cl=clutch_pctile(sub.clu.values)
        # Waivers stay ABSOLUTE and uncapped too (Joe 2026-07-20): a league-altering pickup must
        # not sit two points above a useful streamer. Canonical add-LAMAR is already scoring-
        # normalized by construction, so it IS the scoring-rule-adjusted value.
        #
        # ADDITIVE here, unlike the draft board's multiplier -- and deliberately so (runbook §4):
        # a championship-week streamer can carry ~0 season add-LAMAR, so value x clutch would
        # multiply the highest-leverage move in fantasy down to nothing. Clutch is its own axis,
        # scaled into LAMAR units so the two terms are commensurable.
        scale=np.nanpercentile(np.clip(avr,0,None),90); scale=scale if scale and scale>0 else 1.0
        t.loc[sub.index,'waiver_value']=avr.round(1)
        t.loc[sub.index,'title_run']=((1-W_CLU_TXN)*avr + W_CLU_TXN*cl*scale).round(1)
    con.register('txn_g', t.drop(columns=['clu'])); con.execute(f"COPY txn_g TO '{(CO/'research_txn_graded.parquet').as_posix()}'")

if 'matchup' in ONLY:
    # ---------- MATCHUP ----------
    # won_pct/lost_pct now come from the cohort builder bottom-up (weekly win%*start% aggregated
    # on the eligibility denominator, Joe 2026-07-17) — pass through, do NOT recompute.
    mm=con.execute(f"SELECT * FROM '{MC.as_posix()}'").df()
    con.register('mt_g', mm); con.execute(f"COPY mt_g TO '{(CO/'research_matchup_graded.parquet').as_posix()}'")

# ---------- local research_cohorts.duckdb (tables the API/local server will serve) ----------
# CREATE OR REPLACE (not unlink) so the weekly/career tables built by the other scripts survive a rerun.
OUTDB=CO/'research_cohorts.duckdb'
w=duckdb.connect(str(OUTDB))
if "draft" in ONLY:
    w.execute(f"CREATE OR REPLACE TABLE draft AS SELECT * FROM '{(CO/'research_draft_graded.parquet').as_posix()}'")
if "transactions" in ONLY:
    w.execute(f"CREATE OR REPLACE TABLE transactions AS SELECT * FROM '{(CO/'research_txn_graded.parquet').as_posix()}'")
if "matchup" in ONLY:
    w.execute(f"CREATE OR REPLACE TABLE matchup AS SELECT * FROM '{(CO/'research_matchup_graded.parquet').as_posix()}'")
for tb in sorted(ONLY & {'draft','transactions','matchup'}):
    n=w.execute(f"SELECT COUNT(*), COUNT(*) FILTER (WHERE cohort_level=4) FROM {tb}").fetchone()
    print(f"{tb}: {n[0]:,} rows ({n[1]:,} at cohort_level 4)")
if "draft" in ONLY:
    gd=w.execute("SELECT COUNT(*) FROM draft WHERE draft_score IS NOT NULL").fetchone()[0]
    print(f"draft rows with grade: {gd:,}")
w.close()
print("wrote", OUTDB)
