"""Derive the v2 tier cutoffs for EVERY position, ROSTERED and STARTED.

R18 fixed the shape of this: the tier is a derived capacity unit, and it means something
different per stat. Roster% is denominated on total capacity (starting slots + bench), start%
on starting capacity alone. So the SAME league lands in a different tier for the two stats,
and that is correct -- the tier answers "how much demand exists for THIS stat".

R22 fixed how capacity is measured: observed, not declared. spots = the count of rows for that
position in a league-week, averaged over weeks. `is_started=1` gives the started variant.

R21 fixed how the cutoffs are chosen: quantile-match to the literal num_teams distribution, so
the population SHAPE matches what users expect (mostly 10 and 12 team) while MEMBERSHIP
differs on real capacity. That beat equal-spacing by 22.7% on within-tier heterogeneity.

Emits a table of cutoffs, one row per (position, stat), ready to paste into
position_slots_contract.TIER_CUTS.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

# Broad fantasy position from the super table. DST is stored as DEF; IDP classes are their own.
POSITIONS = ["QB", "RB", "WR", "TE", "K", "DL", "LB", "DB", "DEF"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for yr in (2023, 2024, 2025):
        con = duckdb.connect(config={
            "memory_limit": "1400MB", "threads": 2,
            "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/tiers{yr}"})
        try:
            for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                      "PRAGMA max_temp_directory_size='3GB'"):
                con.execute(s)
            con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS l (READ_ONLY)")
            con.execute(f"ATTACH '{a.ops.as_posix()}' AS o (READ_ONLY)")
            con.execute(f"""CREATE OR REPLACE TEMP TABLE pos AS
              SELECT NFL_player_id pid, MAX(UPPER(TRIM(position))) p
              FROM o.nfl_historical.nfl_player_stats_all
              WHERE "year"={yr} AND NFL_player_id IS NOT NULL AND position NOT LIKE '%,%'
              GROUP BY 1""")
            # the target shape: the literal num_teams distribution, live leagues only
            t = con.execute(f"""
              SELECT SUM(CASE WHEN num_teams<=8 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE a,
                     SUM(CASE WHEN num_teams<=10 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE b,
                     SUM(CASE WHEN num_teams<=12 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE c
              FROM l.public.league_settings s
              WHERE s.year={yr} AND s.num_teams BETWEEN 4 AND 24
                AND EXISTS (SELECT 1 FROM l.public.player_fantasy pf
                            WHERE pf.db_name=s.db_name AND pf.year={yr})""").fetchdf().iloc[0]
            for pos in POSITIONS:
                for stat, filt in (("rostered", ""), ("started", "AND pf.is_started = 1")):
                    q = con.execute(f"""
                      WITH cap AS (
                        SELECT db_name, AVG(nspots) spots FROM (
                          SELECT pf.db_name, pf.week, COUNT(*) nspots
                          FROM l.public.player_fantasy pf
                          JOIN pos ON pos.pid = pf.NFL_player_id
                          WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17
                            AND pos.p = '{pos}' {filt}
                          GROUP BY 1,2) GROUP BY 1)
                      SELECT COUNT(*) n, MEDIAN(spots) med,
                             quantile_cont(spots,{t.a}) c1,
                             quantile_cont(spots,{t.b}) c2,
                             quantile_cont(spots,{t.c}) c3
                      FROM cap""").fetchdf().iloc[0]
                    if pd.isna(q.med) or q.n < 200:
                        continue
                    rows.append({"year": yr, "position": pos, "stat": stat,
                                 "leagues": int(q.n), "median_spots": round(q.med, 1),
                                 "c1": round(q.c1, 1), "c2": round(q.c2, 1),
                                 "c3": round(q.c3, 1)})
        finally:
            con.close()
        print(f"  {yr} done")

    d = pd.DataFrame(rows)
    d.to_parquet(a.out, index=False)
    pd.set_option("display.width", 210)
    # pool across years: the cutoffs must not wobble year to year (R23 stability check)
    pooled = (d.groupby(["position", "stat"])
                .agg(years=("year", "size"), leagues=("leagues", "median"),
                     median_spots=("median_spots", "median"),
                     c1=("c1", "median"), c2=("c2", "median"), c3=("c3", "median"))
                .round(1))
    print("\nPOOLED 2023-2025 CUTOFFS -- one row per (position, stat)\n")
    print(pooled.to_string())
    print("\nyear-to-year drift in c2 (the middle cut), as % of its own value:")
    drift = (d.groupby(["position", "stat"]).c2.agg(lambda s: 100 * (s.max() - s.min()) / s.mean())
             .round(0).sort_values(ascending=False))
    print(drift.head(8).to_string())


if __name__ == "__main__":
    main()
