"""All-Time (career) research tables — roll up the season graded tables to player-career,
per rule-set cohort. Sample-weighted means for rates/grades, sums for counts, re-gated for
confidence on the pooled career sample. Local only. Writes *_career parquets + adds career
tables to research_cohorts.duckdb.

  py -3 scripts/research_cohorts/build_research_career_rollup.py
  py -3 scripts/research_cohorts/build_research_career_rollup.py --only matchup \
      --matchup-source D:/tmp/.../assembled/research_matchup_player_season.parquet

`--only` exists for the fleet path: after a shard rebuild the assembled matchup season
parquet is the only fresh input, and the draft/txn graded parquets belong to a different
(possibly older) cycle. Rolling them up again from stale inputs would republish stale career
rows under a fresh timestamp, so the matchup lane has to be runnable on its own. Matchup
needs no grading step -- build_research_cohort_grades passes it straight through -- so the
assembled season parquet IS its graded input.
"""
import argparse
import os
import duckdb
from pathlib import Path
from draft_career_sql import draft_career_rollup_sql

from matchup_career_sql import matchup_career_sql
from cohort_format_sql import CAREER_GRAIN
ap=argparse.ArgumentParser(description=__doc__)
ap.add_argument("--only", action="append", choices=["draft","transactions","matchup"],
                help="roll up only these tables (repeatable); default is all three")
ap.add_argument("--matchup-source", type=Path, default=None,
                help="assembled matchup season parquet to roll up instead of the graded one")
ap.add_argument("--out-dir", type=Path, default=None,
                help="write the career parquets here instead of RESEARCH_OUT_DIR")
ARGS=ap.parse_args()
WANT=set(ARGS.only or ["draft","transactions","matchup"])

CO=Path(os.environ.get("RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
OUT=Path(ARGS.out_dir) if ARGS.out_dir else CO
OUT.mkdir(parents=True, exist_ok=True)
DB=CO/'research_cohorts.duckdb'
con=duckdb.connect(); con.execute("SET memory_limit='3000MB'")
D=(CO/'research_draft_graded.parquet').as_posix()
TX=(CO/'research_txn_graded.parquet').as_posix()
MC=(Path(ARGS.matchup_source) if ARGS.matchup_source
    else CO/'research_matchup_graded.parquet').as_posix()
G=",".join(CAREER_GRAIN)

# Weighted mean with a lane-matched denominator: seasons where the lane is NULL contribute
# to NEITHER side (an unconditional SUM(w) denominator silently deflates every lane with
# NULL seasons -- ledger D13).
def wm(x, w, nd=1):
    return f"ROUND(SUM({x}*{w})/NULLIF(SUM(CASE WHEN {x} IS NOT NULL THEN {w} END),0),{nd})"

# ---- Draft career: weighted averages plus season extrema and their associated years. ----
# No draft_score filter: ungraded seasons still belong to counts/ADP; score lanes exclude
# NULL seasons through their metric-specific conditional denominators.
draft_career = draft_career_rollup_sql(D)

# ---- Transactions career: lifetime waiver profile ----
txn_career=f"""
SELECT {G},
  COUNT(DISTINCT year) AS n_years,
  SUM(n_leagues) AS n_leagues,
  {wm('add_rate_pct','n_leagues')} AS add_rate_pct,
  SUM(COALESCE(n_add_leagues,0)) AS n_add_leagues,
  {wm('avg_faab_pct','COALESCE(n_add_leagues,0)')} AS avg_faab_pct,
  {wm('avg_faab_bid','COALESCE(n_add_leagues,0)')} AS avg_faab_bid,
  {wm('avg_add_lamar','COALESCE(n_add_leagues,0)')} AS avg_add_lamar,
  {wm('title_run','COALESCE(n_add_leagues,0)')} AS title_run,
  {wm('avg_drop_regret','COALESCE(n_drop_leagues,0)')} AS avg_drop_regret,
  CASE WHEN SUM(COALESCE(n_add_leagues,0)) >= 35 THEN 'confident' WHEN SUM(COALESCE(n_add_leagues,0)) >= 10 THEN 'mushy' ELSE 'insufficient' END AS confidence
FROM '{TX}' GROUP BY {G}"""

# ---- Matchup career: bottom-up weekly components, totals, and expected counts ----
mt_career = matchup_career_sql(f"'{MC}'")

JOBS=[("draft","draft_career","draft_career",draft_career),
      ("transactions","transactions_career","txn_career",txn_career),
      ("matchup","matchup_career","matchup_career",mt_career)]
built=[]
for lane, table, stem, sql in JOBS:
    if lane not in WANT:
        continue
    con.execute(f"COPY ({sql}) TO '{(OUT/(stem+'.parquet')).as_posix()}'")
    built.append((table, stem))

# add to research_cohorts.duckdb. Only the lanes that were rebuilt are replaced -- a
# partial run must never blank the career tables it did not compute.
w=duckdb.connect(str(DB))
for table, stem in built:
    w.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM '{(OUT/(stem+'.parquet')).as_posix()}'")
    n=w.execute(f"SELECT COUNT(*), COUNT(*) FILTER (WHERE cohort_level=4 AND confidence='confident') FROM {table}").fetchone()
    print(f"{table}: {n[0]:,} rows ({n[1]:,} confident @ level4)")
if "draft" in WANT:
    # spot check: LT-style career draft (12t/flx/ppr top career draft-score)
    print("\n12t/flx/ppr/4pt career draft top-5 (by avg draft_score, confident):")
    print(w.execute("""SELECT NFL_player_id, n_years, adp, draft_score, avg_manager_lamar
      FROM draft_career WHERE cohort_level=4 AND teams='12t' AND roster='flx' AND ppr='ppr' AND td='4pt'
        AND confidence='confident' ORDER BY draft_score DESC LIMIT 5""").fetchdf().to_string(index=False))
w.close()
print("done")
