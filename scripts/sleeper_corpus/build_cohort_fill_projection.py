"""build_cohort_fill_projection.py -- what the discovery crawl can do for cohort confidence.

Joins the discovery ledger (newly-findable public Sleeper leagues) against our CURRENT cohort
sample sizes (from the local draft-cohort parquet -- draft is the tightest gate since it needs
full-draft single-season leagues) and reports, per (cohort x year), current vs projected league
count and whether it crosses the confident gate (>=35).

    py -3 scripts/sleeper_corpus/build_cohort_fill_projection.py
"""
from __future__ import annotations
import os, sys
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
LEDGER = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/discovery/discovered_leagues.parquet")
DRAFT = Path("D:/league-history-data/fantasy_leagues/cohort_aggregates/research_draft_player_season.parquet")
MIN_DISPLAY, MIN_STABLE = 10, 35


def known_seed_ids() -> set[str]:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    from multi_league.core.readers.fly_reader import FlyReader
    rows = FlyReader().query(
        "SELECT DISTINCT league_key FROM public.league_settings "
        "WHERE LOWER(platform) LIKE '%sleep%' AND league_key IS NOT NULL", "___leagues")
    return {str(r["league_key"]) for r in rows if r.get("league_key")}


def main() -> None:
    con = duckdb.connect(); con.execute("SET memory_limit='2000MB'")
    con.execute(f"CREATE VIEW led AS SELECT * FROM '{LEDGER.as_posix()}' WHERE season IS NOT NULL")
    con.execute(f"CREATE VIEW draft AS SELECT * FROM '{DRAFT.as_posix()}'")
    known = known_seed_ids()
    con.execute("CREATE TABLE known(league_id VARCHAR)")
    con.executemany("INSERT INTO known VALUES (?)", [[k] for k in known])
    print(f"[known] {len(known):,} of our own Sleeper league_ids excluded from 'new'")

    # current per-cohort sample (draft denominator, full 4-dim cohort_level=4)
    con.execute("""CREATE TABLE cur AS
      SELECT teams, roster, ppr, td, year, MAX(n_leagues) AS cur_n
      FROM draft WHERE cohort_level = 4 GROUP BY 1,2,3,4,5""")
    # newly-findable single-season leagues per cohort-year (exclude dynasty + our own)
    con.execute("""CREATE TABLE new AS
      SELECT teams, roster, ppr, td, season AS year, COUNT(DISTINCT league_id) AS new_n
      FROM led WHERE is_dynasty = false AND league_id NOT IN (SELECT league_id FROM known)
        AND season BETWEEN 2017 AND 2025
      GROUP BY 1,2,3,4,5""")

    con.execute("""CREATE TABLE proj AS
      SELECT COALESCE(c.teams,n.teams) teams, COALESCE(c.roster,n.roster) roster,
             COALESCE(c.ppr,n.ppr) ppr, COALESCE(c.td,n.td) td, COALESCE(c.year,n.year) AS "year",
             COALESCE(c.cur_n,0) cur_n, COALESCE(n.new_n,0) new_n,
             COALESCE(c.cur_n,0)+COALESCE(n.new_n,0) proj_n
      FROM cur c FULL OUTER JOIN new n USING (teams, roster, ppr, td, year)""")

    def band(col): return (f"COUNT(*) FILTER (WHERE {col} >= {MIN_STABLE}) conf, "
                           f"COUNT(*) FILTER (WHERE {col} >= {MIN_DISPLAY} AND {col} < {MIN_STABLE}) mushy, "
                           f"COUNT(*) FILTER (WHERE {col} < {MIN_DISPLAY}) insuf")

    print("\n== cohort-year cells by confidence band (single-season, 2017-2025) ==")
    print("  NOW:      ", con.execute(f"SELECT {band('cur_n')} FROM proj").fetchdf().to_string(index=False))
    print("  PROJECTED:", con.execute(f"SELECT {band('proj_n')} FROM proj").fetchdf().to_string(index=False))

    print("\n== biggest lifts: cohorts that CROSS into confident (cur<35 -> proj>=35) ==")
    print(con.execute("""
      SELECT teams,roster,ppr,td,year,cur_n,new_n,proj_n FROM proj
      WHERE cur_n < 35 AND proj_n >= 35 ORDER BY roster, ppr, td, teams, year LIMIT 40
    """).fetchdf().to_string(index=False))

    print("\n== by cohort (all years pooled): current vs new available ==")
    print(con.execute("""
      SELECT teams,roster,ppr,td, SUM(cur_n) cur_total, SUM(new_n) new_total,
             COUNT(*) FILTER (WHERE proj_n>=35) yrs_confident_proj,
             COUNT(*) yrs
      FROM proj GROUP BY 1,2,3,4 ORDER BY roster, ppr, td, teams
    """).fetchdf().to_string(index=False))

    print("\n== dynasty pool discovered (separate partition, for later) ==")
    print(con.execute("""
      SELECT roster, ppr, td, COUNT(DISTINCT league_id) dyn_leagues
      FROM led WHERE is_dynasty = true GROUP BY 1,2,3 ORDER BY dyn_leagues DESC LIMIT 15
    """).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
