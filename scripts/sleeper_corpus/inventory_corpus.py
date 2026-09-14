"""inventory_corpus.py -- what is actually in the D: corpus lake, and is it enough to build on?

Answers, from the central corpus_snapshot.duckdb:
  1. how many leagues, split by the partition the cohort builders care about
     (managed single-season is the ONLY partition draft/txn/matchup all keep --
      best-ball and dynasty are filtered out by every builder)
  2. cohort-year coverage vs the confident gate (n>=35), which is what actually
     decides whether a research page can show a cell
  3. row counts per table

    py -3 scripts/sleeper_corpus/inventory_corpus.py
    py -3 scripts/sleeper_corpus/inventory_corpus.py --flex-only
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from build_corpus_snapshot import OUT, TABLES

MIN_STABLE, MIN_DISPLAY = 35, 10

# Same classification the cohort builders use (build_research_draft_cohort.py etc.),
# so the numbers here predict what those builders will actually produce.
CLASSIFY = """
SELECT db_name, year,
  CASE WHEN COALESCE(sleeper_best_ball,false) THEN 'best_ball'
       WHEN COALESCE(is_dynasty,false)        THEN 'dynasty'
       ELSE 'managed_single_season' END AS partition,
  CASE WHEN num_teams <= 11 THEN '10t' ELSE '12t' END AS teams,
  CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)
            +COALESCE(roster_DB,0)+COALESCE(roster_DB_LB,0)+COALESCE(roster_DL_LB,0) > 0 THEN 'idp'
       WHEN COALESCE(roster_SUPER_FLEX,0) > 0 THEN 'sflx' ELSE 'flx' END AS roster,
  CASE WHEN COALESCE(scoring_rec,0) = 0 THEN 'std'
       WHEN COALESCE(scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END AS ppr,
  CASE WHEN COALESCE(scoring_pass_td,4) >= 5 THEN '6pt' ELSE '4pt' END AS td
FROM public.league_settings
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(OUT))
    ap.add_argument("--flex-only", action="store_true",
                    help="only flx cohorts (the ones the research routes currently serve)")
    args = ap.parse_args()

    if not Path(args.db).exists():
        raise SystemExit(f"no corpus snapshot at {args.db}")
    con = duckdb.connect(args.db, read_only=True)
    con.execute(f"CREATE OR REPLACE TEMP VIEW cls AS {CLASSIFY}")

    # _sources is a corpus-fold artifact; the real-league snapshot has no such ledger, so fall
    # back to the league_settings roster. Lets this run against either snapshot unchanged.
    try:
        n_leagues = con.execute("SELECT COUNT(*) FROM _sources").fetchone()[0]
        src = "folded"
    except duckdb.CatalogException:
        n_leagues = con.execute("SELECT COUNT(DISTINCT db_name) FROM public.league_settings").fetchone()[0]
        src = "in league_settings"
    print(f"=== lake: {args.db}")
    print(f"    {n_leagues:,} leagues {src}\n")

    print("--- league-YEARS by partition (only managed survives every cohort builder) ---")
    print(con.execute("""
        SELECT partition, COUNT(*) AS league_years, COUNT(DISTINCT db_name) AS leagues
        FROM cls GROUP BY 1 ORDER BY 2 DESC""").df().to_string(index=False))

    where = "WHERE partition='managed_single_season'" + (" AND roster='flx'" if args.flex_only else "")
    print(f"\n--- managed cohort-year cells vs the confident gate (n>={MIN_STABLE})"
          f"{' [flx only]' if args.flex_only else ''} ---")
    cells = con.execute(f"""
        WITH cell AS (
          SELECT teams, roster, ppr, td, year, COUNT(DISTINCT db_name) AS n
          FROM cls {where} GROUP BY 1,2,3,4,5
        )
        SELECT
          COUNT(*) FILTER (WHERE n >= {MIN_STABLE})                      AS confident,
          COUNT(*) FILTER (WHERE n >= {MIN_DISPLAY} AND n < {MIN_STABLE}) AS mushy,
          COUNT(*) FILTER (WHERE n < {MIN_DISPLAY})                       AS insufficient,
          COUNT(*)                                                        AS total_cells
        FROM cell""").df()
    print(cells.to_string(index=False))

    print(f"\n--- best managed cells (top 12 by league count) ---")
    print(con.execute(f"""
        SELECT teams, roster, ppr, td, year, COUNT(DISTINCT db_name) AS n,
               CASE WHEN COUNT(DISTINCT db_name) >= {MIN_STABLE} THEN 'confident'
                    WHEN COUNT(DISTINCT db_name) >= {MIN_DISPLAY} THEN 'mushy'
                    ELSE 'insufficient' END AS confidence
        FROM cls {where}
        GROUP BY 1,2,3,4,5 ORDER BY n DESC LIMIT 12""").df().to_string(index=False))

    print("\n--- rows per table ---")
    for t in TABLES:
        rows, lgs = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT db_name) FROM public.{t}").fetchone()
        print(f"   {t:16s} {rows:12,d} rows / {lgs:6,d} leagues")
    con.close()


if __name__ == "__main__":
    main()
