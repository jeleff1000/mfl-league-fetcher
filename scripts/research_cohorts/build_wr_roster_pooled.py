"""Precompute WR weekly roster% per pool, so the serving layer is a lookup and never a scan.

TWO OUTPUTS.

  wr_roster_pool_map     (year, cohort, pool_key, split_on, leagues)
      Which pool serves a given cohort. 141 pools across 2010-2025, produced by
      wr_roster_weekly_pools.py -- every league-year lands in exactly one, so a cohort can
      always be answered and nothing is ever suppressed.

  wr_roster_pooled       (year, week, pool_key, NFL_player_id, n_rostered, n_leagues, roster_pct)
      The number itself, already aggregated. ~1.4M rows.

WHY PRECOMPUTE. A request for "WR roster% in 12tm|flx|ppr|4pt|redraft, 2024 week 8" would
otherwise aggregate across 15,037 leagues at request time. Precomputed it is one indexed
lookup. This is the CLAUDE.md rule -- never do at runtime what can be done once at build.

It also settles where the pool map belongs: NOT in the frontend. Once the values are
precomputed the map is just key resolution, and it lives next to the data it keys so the
client asks for a cohort and gets a number without needing to know pooling exists.

DENOMINATOR IS THE ELIGIBLE LIVE LEAGUE SET (R9). A league in the pool that never rostered
the player contributes a real zero, not a missing row. Absence is the zero.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

import position_slots_contract as PS
from cohort_format_sql import cohort_league_settings_sql

AXES = ["fmtx", "tier", "roster", "ppr", "td", "bracket"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--pools", type=Path, required=True,
                    help="output of wr_roster_weekly_pools.py")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--first-year", type=int, default=2010)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    pools = pd.read_parquet(a.pools)
    pools = pools[pools.verdict == "POOL"] if "verdict" in pools else pools
    c1, c2, c3 = PS.tier_cuts("WR", "rostered")

    map_rows, agg = [], []
    for yr in sorted(pools.year.unique()):
        if yr < a.first_year:
            continue
        con = duckdb.connect(config={
            "memory_limit": "1400MB", "threads": 2,
            "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/pool{yr}"})
        try:
            for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                      "PRAGMA max_temp_directory_size='3GB'"):
                con.execute(s)
            con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS l (READ_ONLY)")
            con.execute(f"ATTACH '{a.ops.as_posix()}' AS ops (READ_ONLY)")
            con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                        cohort_league_settings_sql(position_slots=True, year=int(yr))
                        .replace("public.", "l.public."))
            con.execute(f"""CREATE OR REPLACE TEMP TABLE wr AS SELECT NFL_player_id pid
              FROM ops.nfl_historical.nfl_player_stats_all WHERE "year"={yr}
                AND NFL_player_id IS NOT NULL AND UPPER(TRIM(position))='WR'
                AND position NOT LIKE '%,%' GROUP BY 1""")
            # every live league in the year, with its cohort key
            con.execute(f"""CREATE OR REPLACE TEMP TABLE lgc AS
              SELECT c.db_name,
                CASE WHEN c.wr_spots<{c1} THEN '08tm' WHEN c.wr_spots<{c2} THEN '10tm'
                     WHEN c.wr_spots<{c3} THEN '12tm' ELSE '14tm' END tier,
                f.roster, f.ppr, f.td, f.bracket,
                CASE WHEN f.lineup_mode='best_ball' THEN 'bestball'
                     WHEN f.league_type='dynasty' THEN 'dynasty' ELSE 'redraft' END fmtx
              FROM (SELECT db_name, AVG(nwr) wr_spots FROM
                     (SELECT pf.db_name, pf.week, COUNT(*) nwr
                      FROM l.public.player_fantasy pf JOIN wr ON wr.pid=pf.NFL_player_id
                      WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17 GROUP BY 1,2)
                    GROUP BY 1) c
              JOIN fmt f ON f.db_name=c.db_name AND f.year={yr}
              WHERE f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL
                AND f.bracket IS NOT NULL""")
            # ATTACH EACH LEAGUE TO ITS POOL. A pool key names only the axes that were
            # actually split -- "fmtx=bestball|roster=flx|td=4pt" has no tier and no ppr -- so
            # a prefix LIKE cannot work. It collapsed 2024 from 26 pools to 2 because almost
            # every cohort missed and fell through to ALL. Match on the axes the key names,
            # and let the MOST SPECIFIC key win.
            lg = con.execute("SELECT * FROM lgc").fetchdf()
            lg["cohort"] = lg[AXES].astype(str).agg("|".join, axis=1)
            keys = sorted(pools[pools.year == yr].pool_key.dropna().unique(),
                          key=lambda k: -len(k.split("|")))

            def _pool_for(row):
                for k in keys:                       # most specific first
                    if k == "ALL":
                        continue
                    if all(row.get(ax) == val
                           for ax, val in (p.split("=", 1) for p in k.split("|"))):
                        return k
                return "ALL"

            lg["pool_key"] = [_pool_for(r) for r in lg.to_dict("records")]
            con.register("lgp_df", lg)
            con.execute("CREATE OR REPLACE TEMP TABLE lgp AS SELECT * FROM lgp_df")
            m = con.execute("""SELECT cohort, pool_key, COUNT(*) leagues
                               FROM lgp GROUP BY 1,2""").fetchdf()
            m["year"] = int(yr)
            map_rows.append(m)
            # THE NUMBER. Denominator is every live league in the pool (R9) -- a league that
            # never rostered the player is a real zero, which is why this is a LEFT JOIN from
            # the pool size rather than a count of rostered rows.
            d = con.execute(f"""
              WITH sz AS (SELECT pool_key, COUNT(*) n_leagues FROM lgp GROUP BY 1),
                   ros AS (SELECT g.pool_key, pf.week, pf.NFL_player_id pid, COUNT(*) n
                           FROM l.public.player_fantasy pf JOIN lgp g ON g.db_name=pf.db_name
                           JOIN wr ON wr.pid=pf.NFL_player_id
                           WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17
                           GROUP BY 1,2,3)
              SELECT {yr} AS year, ros.week, ros.pool_key, ros.pid AS NFL_player_id,
                     ros.n AS n_rostered, sz.n_leagues,
                     ROUND(ros.n::DOUBLE / sz.n_leagues, 5) AS roster_pct
              FROM ros JOIN sz ON sz.pool_key = ros.pool_key""").fetchdf()
            agg.append(d)
            print(f"  {yr}: {len(m)} cohorts -> {m.pool_key.nunique()} pools, "
                  f"{len(d):,} pooled rows", flush=True)
        finally:
            con.close()

    pool_map = pd.concat(map_rows, ignore_index=True)
    pooled = pd.concat(agg, ignore_index=True)
    pool_map.to_parquet(a.out_dir / "wr_roster_pool_map.parquet", index=False)
    pooled.to_parquet(a.out_dir / "wr_roster_pooled.parquet", index=False)
    print(f"\nwr_roster_pool_map : {len(pool_map):,} cohort-years -> "
          f"{pool_map.pool_key.nunique()} distinct pools")
    print(f"wr_roster_pooled   : {len(pooled):,} rows, "
          f"{pooled.NFL_player_id.nunique()} players")
    print(f"\nevery cohort has a pool: {pool_map.pool_key.notna().all()}")


if __name__ == "__main__":
    main()
