"""WR roster%, WEEKLY: the complete rule set. Every cohort-year 2010-2025 gets an answer.

Grid is 216 cohorts = teams(4) x roster(3) x ppr(3) x td(2) x format(3), playoff bracket
ALWAYS collapsed (Joe: it has no causal route to rostership). 15 years, and every populated
cell is resolved to SEPARATE, POOL(+axes) or SUPPRESS. No cell is left undecided.

Teams is the OBSERVED-capacity tier (R22): wr_spots = avg over weeks of the count of WR rows
in that league-week. No declared slots, no flex/bench weights, no `manager` column. Cutoffs
are the pooled 2021-2025 quantile match to the num_teams shape (R21 method, R23 stability
check): 54.2 / 79.4 / 134.5.

Target N is R23's p90 -- the worst cell a rule must cover, not the median (R2):
    +/-5 @85% = 140   +/-5 @95% = 180
    +/-3 @85% = 220   +/-3 @95% = 260
    +/-1 @85% = 560   +/-1 @95% = 650

Collapse decision is Joe's rule -- collapse WHEN IT GIVES BETTER ACCURACY THAN LEAVING THEM
APART -- evaluated per cell as bias-variance:
    err_separate = z*SD/sqrt(n_own)
    err_pooled   = bias_imported + z*SD/sqrt(n_pooled)
    collapse iff err_pooled < err_separate
Order: bracket (always, untested) -> td -> roster -> ppr -> tier. `format` NEVER collapses
(A2 structural, and 24.9 pts in R19).

BIAS values are R19 deviations with the ~4.6pt noise floor removed in quadrature. THEY WERE
MEASURED ON THE DECLARED-SLOT KEY and are the last input here not yet re-measured on the
observed-capacity key. Flagged in the output rather than silently carried.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

C1, C2, C3 = 54.2, 79.4, 134.5           # R22/R23 pooled observed-capacity cutoffs
NOISE = 0.046                            # R19 noise floor
_MEAS = {"td": 0.049, "roster": 0.087, "ppr": 0.058, "tier": 0.246}
BIAS = {k: math.sqrt(max(v * v - NOISE ** 2, 0.0)) for k, v in _MEAS.items()}
BIAS["bracket"] = 0.0                    # declared zero (Joe); R19 could not rule on 9 pairs
BIAS["fmtx"] = 0.242                     # R19 format deviation, noise removed
ORDER = ["td", "roster", "ppr", "tier"]  # Joe's stated order, cheapest first
# Last resorts, used ONLY when a cell is still under the floor: format then tier. Both are
# expensive (24.2pts) and neither is ever taken by a cell that already clears the floor.
FULL_ORDER = ["td", "roster", "ppr", "fmtx", "tier"]
AXES = ["tier", "roster", "ppr", "td", "fmtx"]
SD_WEEK = 0.50                           # per-league weekly value is 0/1 at the contested band
TARGETS = {("5", "85"): 140, ("5", "95"): 180, ("3", "85"): 220,
           ("3", "95"): 260, ("1", "85"): 560, ("1", "95"): 650}
# THE FLOOR IS THE TARGET (Joe, 2026-08-02). 30 was the SEASON coherence floor and does not
# belong at the weekly grain. A weekly cell that cannot reach its target N cannot be shown at
# that tolerance -- there is no separate, lower bar.


def hw(n: int, conf: float) -> float:
    """Sampling half-width for n leagues at the weekly grain."""
    if n <= 0:
        return 9.9
    return NormalDist().inv_cdf((1 + conf) / 2) * SD_WEEK / math.sqrt(n)


def year_grid(snapshot: Path, ops: Path, yr: int) -> pd.DataFrame:
    con = duckdb.connect(config={
        "memory_limit": "2000MB", "threads": 3,
        "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/wk{yr}"})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  "PRAGMA max_temp_directory_size='8GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS l (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS o (READ_ONLY)")
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True).replace("public.", "l.public."))
        con.execute(
            'CREATE OR REPLACE TEMP TABLE wr AS SELECT NFL_player_id pid '
            'FROM o.nfl_historical.nfl_player_stats_all WHERE "year"=' + str(yr) +
            " AND NFL_player_id IS NOT NULL AND UPPER(TRIM(position))='WR' "
            "AND position NOT LIKE '%,%' GROUP BY 1")
        g = con.execute(f"""
          SELECT CASE WHEN c.wr_spots<{C1} THEN '08tm' WHEN c.wr_spots<{C2} THEN '10tm'
                      WHEN c.wr_spots<{C3} THEN '12tm' ELSE '14tm' END tier,
                 f.roster, f.ppr, f.td,
                 CASE WHEN f.lineup_mode='best_ball' THEN 'bestball'
                      WHEN f.league_type='dynasty' THEN 'dynasty' ELSE 'redraft' END fmtx,
                 COUNT(*) n
          FROM (SELECT db_name, AVG(nwr) wr_spots FROM
                  (SELECT pf.db_name, pf.week, COUNT(*) nwr FROM l.public.player_fantasy pf
                   JOIN wr ON wr.pid=pf.NFL_player_id
                   WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17 GROUP BY 1,2)
                GROUP BY 1) c
          JOIN fmt f ON f.db_name=c.db_name AND f.year={yr}
          WHERE f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL
          GROUP BY ALL""").fetchdf()
        g["year"] = yr
        return g
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--margin", choices=["5", "3", "1"], default="5")
    ap.add_argument("--conf", choices=["85", "95"], default="85")
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    target, conf = TARGETS[(a.margin, a.conf)], float(a.conf) / 100
    FLOOR = target

    g_all = pd.concat([year_grid(a.snapshot, a.ops, y) for y in range(2010, 2026)],
                      ignore_index=True)

    rows = []
    for yr, gy in g_all.groupby("year"):
        for _, r in gy.iterrows():
            cur = {ax: r[ax] for ax in AXES}

            def merged(used):
                m = gy
                for k, v in cur.items():
                    if k not in used:
                        m = m[m[k] == v]
                return int(m.n.sum())

            # PROGRESSIVE REFINEMENT (Joe, 2026-08-02). Collapsing is not a fixed ladder that
            # gives up -- it keeps going until the pool clears the floor, ALL THE WAY to a
            # single pool for the whole year if that is what it takes. A year starts as one
            # pool and splits into finer strata only as the corpus populates. 2011 has 384
            # leagues, which IS a valid pool of 384; suppressing its 45 cells was wrong.
            #
            # Two regimes:
            #   below the floor -> collapse the next axis UNCONDITIONALLY. Nothing beats
            #                      showing nothing, so the bias-variance test does not apply.
            #   at or above it  -> collapse only if it genuinely improves accuracy (Joe's rule).
            # Suppress ONLY when the fully-collapsed year pool is itself under the floor.
            used, bias = ["bracket"], 0.0
            size = merged(used)
            for ax in FULL_ORDER:
                if size >= FLOOR:
                    cand = used + [ax]
                    if bias + BIAS[ax] + hw(merged(cand), conf) < bias + hw(size, conf):
                        used, bias, size = cand, bias + BIAS[ax], merged(cand)
                    else:
                        continue                 # try the next axis, do not give up
                else:
                    cand = used + [ax]           # forced: below floor, take the leagues
                    used, bias, size = cand, bias + BIAS[ax], merged(cand)
            err = bias + hw(size, conf)
            rows.append({"year": int(yr), "cohort": "|".join(str(cur[x]) for x in AXES),
                         **{f"c_{k}": v for k, v in cur.items()},
                         "own": int(r.n), "pooled": size,
                         "collapse": "+".join(used), "rescue": rescue,
                         "bias_pts": round(100 * bias, 1),
                         "err_pts": round(100 * err, 1),
                         "verdict": ("SUPPRESS" if size < FLOOR
                                     else "RESCUED" if rescue
                                     else "SEPARATE" if used == ["bracket"] else "POOL")})
    out = pd.DataFrame(rows).sort_values(["year", "cohort"])
    out.to_parquet(a.out, index=False)
    pd.set_option("display.width", 250)
    print(f"WR roster% WEEKLY rules - +/-{a.margin}% @ {a.conf}% conf, target N={target}\n")
    print(out.pivot_table(index="year", columns="verdict", values="cohort",
                          aggfunc="count").fillna(0).astype(int).to_string())
    print(f"\nTOTAL cell-years resolved: {len(out):,}   distinct cohorts seen: {out.cohort.nunique()}")
    print("\ncollapse depth chosen (bracket is always in):")
    print(out.groupby("collapse").agg(cells=("year", "size"), med_err=("err_pts", "median"),
                                      med_pooled=("pooled", "median")).to_string())
    print(f"\nachieved error: p50 {out.err_pts.median():.1f}pts  p90 {out.err_pts.quantile(.9):.1f}pts")
    print(f"cells meeting +/-{a.margin}%: {100*(out.err_pts <= float(a.margin)).mean():.0f}%")


if __name__ == "__main__":
    main()
