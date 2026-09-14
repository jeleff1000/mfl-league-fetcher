"""Consolidated grade-shape verdict on the existing cache: current z / rank / NEW probit-rank,
plus Joe's ISOTONIC-expectation grade (delta + spread-standardized delta). Shape + behavior +
cross-cohort curve consistency. (Corpus is all-startup best-ball; population machinery noted.)"""
import duckdb, statistics, numpy as np, pandas as pd
from sklearn.isotonic import IsotonicRegression
from pathlib import Path
SP = Path(r"C:/Users/joeye/AppData/Local/Temp/claude/d--yahoo-oauth/060754dc-69b7-4693-8b84-32608250d8ea/scratchpad")
con = duckdb.connect(); con.execute("SET memory_limit='3000MB'")
df = con.execute(f"""
  SELECT db, year, NFL_player_id, position, teams, roster, ppr, td, pick, manager_lamar, games_played,
    PERCENT_RANK() OVER (PARTITION BY db,year ORDER BY manager_lamar) AS impact
  FROM '{(SP/'corpus_draft_cache.parquet').as_posix()}'
  WHERE pick IS NOT NULL AND games_played>0
""").df()
print(f"{len(df):,} picks, {df.db.nunique()} leagues\n")
probit = lambda s: pd.Series([statistics.NormalDist().inv_cdf(min(max(x,1e-4),1-1e-4)) for x in s], index=s.index)
def shp(s, lab):
    s=s.replace([np.inf,-np.inf],np.nan).dropna(); print(f"  {lab:38s} skew={s.skew():>6.3f} kurt={s.kurtosis():>6.3f} sd={s.std():>5.2f}")

# isotonic E[impact|pick] (monotone decreasing) -> residual + spread-standardized
iso = IsotonicRegression(increasing=False, out_of_bounds='clip')
df['e_iso'] = iso.fit_transform(df['pick'].astype(float), df['impact'])
df['resid'] = df['impact'] - df['e_iso']
df['pbin'] = pd.qcut(df['pick'].rank(method='first'), 20, labels=False)
sd = df.groupby('pbin')['resid'].transform('std').replace(0,np.nan)
df['iso_std'] = df['resid']/sd

print("=== SHAPE race (want skew~0; for a graded SCORE want kurt~0 = bell curve) ===")
print("  -- BEST-PICKS framing (raw value) --")
shp((df.impact-df.groupby(['db','year']).impact.transform('mean'))/df.groupby(['db','year']).impact.transform('std'), "value z-score (current)")
shp(df.impact, "value rank (proposed)")
shp(probit(df.impact), "value probit-rank (NEW: normal scores)")
print("  -- STEALS / slot-adjusted framing --")
shp(df.impact-df.e_iso, "iso delta (impact - E_iso)")
shp(df.iso_std, "iso STANDARDIZED delta (Joe's)")
shp(probit(df.iso_std.rank(pct=True)), "iso-std -> probit (normal)")

print("\n=== STABILITY of the two leading SCORES across sample sizes ===")
for name,col in [("iso-standardized", df.iso_std),("probit-rank(best-pick)", probit(df.impact))]:
    ks=[col.replace([np.inf,-np.inf],np.nan).dropna().sample(frac=f,random_state=1).kurtosis() for f in (.25,.5,.75,1.)]
    print(f"    {name:22s} kurt@25/50/75/100% = {[round(k,2) for k in ks]}")

print("\n=== BEHAVIOR of Joe's iso-standardized score (does it bury elite 1.01s?) ===")
for lab,m in [("early DELIVERED (pick<=12, impact>.9)",(df.pick<=12)&(df.impact>.9)),
              ("late STEAL (pick>108, impact>.9)",(df.pick>108)&(df.impact>.9)),
              ("early BUST (pick<=12, impact<.3)",(df.pick<=12)&(df.impact<.3))]:
    s=df.loc[m,'iso_std'].replace([np.inf,-np.inf],np.nan).dropna(); print(f"    {lab:40s} avg={s.mean():>6.2f} (n={len(s)})")

print("\n=== CURVE CONSISTENCY: E[impact|pick] global vs per-cohort (need stratification?) ===")
picks=np.array([1,6,12,24,48,96,150]); print("    picks:", list(picks))
g=IsotonicRegression(increasing=False,out_of_bounds='clip').fit(df.pick.astype(float),df.impact)
print(f"      GLOBAL            : {np.round(g.predict(picks),3)}")
for (t,r,p),sub in df.groupby(['teams','roster','ppr']):
    if len(sub)<3000: continue
    gi=IsotonicRegression(increasing=False,out_of_bounds='clip').fit(sub.pick.astype(float),sub.impact)
    print(f"      {t}/{r}/{p:4s} n={len(sub):>6,}: {np.round(gi.predict(picks),3)}")
