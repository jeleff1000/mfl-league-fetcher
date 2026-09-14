"""build_league_cohort_map.py -- classify every (db_name, year) into its research cohort.

Foundation for the research-mode population aggregates (draft/waiver/matchup behavior).
Reads ___leagues.league_settings + per-league draft sizes from Fly (read-only, one small
scan each) and writes a local parquet:

    D:/league-history-data/fantasy_leagues/cohort_aggregates/league_cohort_map.parquet

Cohort vocabulary MATCHES the super-table lamar_<slug> columns so behavior rows join
value columns on the same identity:  {teams}t_{roster}_{ppr}_{td}pt  (roster in
flx/sflx/idp; ppr in std/half/ppr; td in 4pt/6pt). Plus:
  * format: single_season (redraft + keeper -- they fold, ADP corr 0.988) vs dynasty.
  * full_draft: draft had >= 10 rounds (num_teams*10 picks) -- the ADP-eligibility gate
    that drops dynasty rookie drafts and heavy-keeper stubs.
  * roster_shape: starter-count signature (reference; drives finer LAMAR later, not a split).

Read-only against Fly. No writes to Fly. Output is local parquet only.

    py -3 scripts/research_cohorts/build_league_cohort_map.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

OUT_DIR = Path("D:/league-history-data/fantasy_leagues/cohort_aggregates")
OUT = OUT_DIR / "league_cohort_map.parquet"


def load_env() -> None:
    import os
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


# One scan of league_settings does the whole classification; keeper folds into
# single_season; teams/roster/ppr/td use the same buckets as the lamar slugs.
CLASSIFY_SQL = """
WITH ls AS (
  SELECT db_name, year, num_teams,
    CASE WHEN COALESCE(is_dynasty,false) THEN 'dynasty' ELSE 'single_season' END AS format,
    CASE WHEN num_teams <= 11 THEN '10t' ELSE '12t' END AS teams,
    CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)
              +COALESCE(roster_DB,0)+COALESCE(roster_DB_LB,0)+COALESCE(roster_DL_LB,0) > 0 THEN 'idp'
         WHEN COALESCE(roster_SUPER_FLEX,0) > 0 THEN 'sflx'
         ELSE 'flx' END AS roster,
    CASE WHEN COALESCE(scoring_rec,0) = 0 THEN 'std'
         WHEN COALESCE(scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END AS ppr,
    CASE WHEN COALESCE(scoring_pass_td,4) >= 5 THEN '6pt' ELSE '4pt' END AS td,
    COALESCE(roster_QB,0) AS s_qb, COALESCE(roster_RB,0) AS s_rb, COALESCE(roster_WR,0) AS s_wr,
    COALESCE(roster_TE,0) AS s_te, COALESCE(roster_FLX,0) AS s_flx,
    COALESCE(roster_SUPER_FLEX,0) AS s_sflx
  FROM public.league_settings
),
dsize AS (
  SELECT db_name, year, COUNT(*) AS picks
  FROM public.draft WHERE pick IS NOT NULL GROUP BY 1, 2
)
SELECT ls.db_name, ls.year, ls.format, ls.teams, ls.roster, ls.ppr, ls.td,
       ls.teams || '_' || ls.roster || '_' || ls.ppr || '_' || ls.td AS cohort_slug,
       ls.num_teams,
       CASE WHEN d.picks IS NOT NULL AND d.picks >= 10 * NULLIF(ls.num_teams,0)
            THEN true ELSE false END AS full_draft,
       'QB' || ls.s_qb || '_RB' || ls.s_rb || '_WR' || ls.s_wr || '_TE' || ls.s_te
            || '_FLX' || ls.s_flx || '_SF' || ls.s_sflx AS roster_shape
FROM ls LEFT JOIN dsize d ON ls.db_name = d.db_name AND ls.year = d.year
ORDER BY ls.year, ls.db_name
"""


def main() -> None:
    load_env()
    from multi_league.core.fly_writer import FlyWriter

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = FlyWriter().execute(CLASSIFY_SQL, database="___leagues")
    if not rows:
        raise SystemExit("classify returned 0 rows")
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, OUT)

    # quick provenance summary
    n = table.num_rows
    fmts = {}
    for r in rows:
        fmts[r["format"]] = fmts.get(r["format"], 0) + 1
    full = sum(1 for r in rows if r["full_draft"])
    print(f"[cohort-map] {n:,} league-years -> {OUT}")
    print(f"[cohort-map] format: {fmts} | full_draft(>=10rd): {full:,}")
    print(f"[cohort-map] distinct cohort_slug: {len({r['cohort_slug'] for r in rows})}")


if __name__ == "__main__":
    main()
