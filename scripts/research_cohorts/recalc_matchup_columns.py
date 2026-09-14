"""recalc_matchup_columns.py -- bring a built matchup parquet up to the current definitions
WITHOUT re-running the ~16h whale.

Joe 2026-07-20: "cant we do focused recalcs of those columns that should be much faster?"
Yes -- verified, not assumed. Three things changed after the running build started, and only
one of them is a recompute:

  1. ELIGIBILITY GATE (+ dual-position handling) -- a pure POST-FILTER. The predicate depends
     only on (player, year), and neither DENOM_SQL nor DENOM_WEEK_SQL joins the position view,
     so dropping an ineligible player's rows cannot alter an eligible player's numbers. No
     recompute at all.
  2. active_weeks -- FINAL_SQL already selects pd.nfl_active_weeks. Already present.
  3. CLUTCH -- genuinely needs new input: the numerator moved from "started" to "started AND
     active", and the denominator from the player's own starts to eligible leagues. That is
     ONE narrow aggregate per year, not the full 20-column pass with the MATERIALIZED slug
     join that dominates the whale's runtime.

What this does NOT fix: anything that changes an eligible player's other aggregates. If a
future change touches the pf aggregate itself, this script is the wrong tool -- re-run.

    py -3 scripts/research_cohorts/recalc_matchup_columns.py            # in place, with backup
    py -3 scripts/research_cohorts/recalc_matchup_columns.py --dry-run  # report only
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))

OUT_DIR = Path(os.environ.get(
    "RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
TARGET = OUT_DIR / "research_matchup_player_season.parquet"
YEARS = range(2015, 2026)

_LS_MIN = """
  SELECT db_name, year,
    CASE WHEN num_teams <= 11 THEN '10t' ELSE '12t' END AS teams,
    CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)
              +COALESCE(roster_DB,0)+COALESCE(roster_DB_LB,0)+COALESCE(roster_DL_LB,0) > 0 THEN 'idp'
         WHEN COALESCE(roster_SUPER_FLEX,0) > 0 THEN 'sflx' ELSE 'flx' END AS roster,
    CASE WHEN COALESCE(scoring_rec,0) = 0 THEN 'std'
         WHEN COALESCE(scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END AS ppr,
    CASE WHEN COALESCE(scoring_pass_td,4) >= 5 THEN '6pt' ELSE '4pt' END AS td
  FROM public.league_settings WHERE NOT COALESCE(sleeper_best_ball, false)
"""


def clutch_sql(year: int) -> str:
    """The ONLY recompute: start+active clutch, on the same GROUPING SETS lattice the builder
    uses, so coarse rungs stay consistent with rung 4."""
    return f"""
    WITH ls AS ({_LS_MIN.replace('WHERE NOT', 'WHERE year = ' + str(year) + ' AND NOT')}),
    act AS (SELECT NFL_player_id, week FROM public.player_active_week WHERE year = {year}),
    pw AS (
      SELECT p.NFL_player_id, ls.teams, ls.roster, ls.ppr, ls.td, p.clutch_equity
      FROM public.player_fantasy p
      JOIN ls ON ls.db_name = p.db_name AND ls.year = p.year
      JOIN act a ON a.NFL_player_id = p.NFL_player_id AND a.week = p.week
      WHERE p.year = {year} AND CAST(p.is_rostered AS INT) = 1
        AND CAST(p.is_started AS INT) = 1 AND p.NFL_player_id IS NOT NULL)
    SELECT COALESCE(teams,'ALL') teams, COALESCE(roster,'ALL') roster,
           COALESCE(ppr,'ALL') ppr, COALESCE(td,'ALL') td, {year} AS year, NFL_player_id,
           SUM(clutch_equity) AS sum_clutch_started_active
    FROM pw GROUP BY GROUPING SETS (
      (teams, roster, ppr, td, NFL_player_id), (teams, roster, ppr, NFL_player_id),
      (teams, roster, NFL_player_id), (NFL_player_id))
    """


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target", type=Path, default=TARGET)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.target.exists():
        raise SystemExit(f"not found: {args.target} (let the build finish first)")

    from local_reader import LocalReader
    fly = LocalReader()
    con = duckdb.connect()
    con.execute("SET memory_limit='3000MB'")
    con.execute(f"CREATE TABLE m AS SELECT * FROM read_parquet('{args.target.as_posix()}')")
    before = con.execute("SELECT COUNT(*) FROM m").fetchone()[0]
    print(f"[recalc] {before:,} rows in {args.target.name}", flush=True)

    # ---- 1. eligibility post-filter -------------------------------------------------
    con.register("pos", pa.Table.from_pylist(fly.query(
        "SELECT NFL_player_id, year, broad_positions FROM public.player_position "
        "WHERE year BETWEEN 2015 AND 2025", "___leagues")))
    con.execute("""CREATE TABLE keep AS
      SELECT m.rowid AS rid FROM m
      JOIN pos ON pos.NFL_player_id = m.NFL_player_id AND pos.year = m.year
      WHERE list_has_any(pos.broad_positions, ['QB','RB','WR','TE','K','DEF'])
         OR (m.roster = 'idp' AND list_has_any(pos.broad_positions, ['DL','LB','DB']))""")
    drop_n = con.execute("SELECT COUNT(*) FROM m WHERE rowid NOT IN (SELECT rid FROM keep)").fetchone()[0]
    print(f"[recalc] eligibility post-filter would drop {drop_n:,} rows "
          f"({100.0*drop_n/max(before,1):.2f}%)", flush=True)

    # ---- 2. clutch recompute --------------------------------------------------------
    con.execute("""CREATE TABLE cl (teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR,
        year INTEGER, NFL_player_id VARCHAR, sum_clutch_started_active DOUBLE)""")
    for y in YEARS:
        rows = fly.query(clutch_sql(y), "___leagues")
        if rows:
            con.register("_cy", pa.Table.from_pylist(rows))
            con.execute("""INSERT INTO cl SELECT teams, roster, ppr, td, year, NFL_player_id,
                sum_clutch_started_active FROM _cy""")
            con.unregister("_cy")
        print(f"  [clutch] {y}: {len(rows):,} cells", flush=True)

    if args.dry_run:
        print("[recalc] --dry-run: nothing written"); fly.close(); return

    bak = args.target.with_suffix(".pre-recalc.parquet")
    shutil.copy2(args.target, bak)
    print(f"[recalc] backup -> {bak.name}", flush=True)

    con.execute("""CREATE TABLE out AS
      SELECT m.* REPLACE (
        ROUND(cl.sum_clutch_started_active / NULLIF(m.n_leagues, 0), 4) AS avg_clutch_started)
      FROM m
      JOIN keep k ON k.rid = m.rowid
      LEFT JOIN cl USING (teams, roster, ppr, td, year, NFL_player_id)""")
    after = con.execute("SELECT COUNT(*) FROM out").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM out ORDER BY year, cohort_level DESC, roster_rate_pct DESC) "
                f"TO '{args.target.as_posix()}'")
    lo, hi = con.execute(
        "SELECT MIN(avg_clutch_started), MAX(avg_clutch_started) FROM out").fetchone()
    print(f"[recalc] wrote {after:,} rows (from {before:,}); "
          f"clutch range {lo:.3f} .. {hi:.3f}", flush=True)
    fly.close()


if __name__ == "__main__":
    main()
