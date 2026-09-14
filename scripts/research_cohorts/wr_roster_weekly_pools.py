"""WR roster%, WEEKLY: progressive refinement. Every league-year lands in exactly one pool.

THE MODEL (Joe, 2026-08-02). A year does not start as 216 cohorts that get collapsed until
they die. It starts as ONE POOL and SPLITS into finer strata as the corpus populates. 2011
holds 384 leagues -- that IS a valid pool of 384, and suppressing its 45 cohorts was wrong.
Nothing is suppressed unless the entire year holds fewer leagues than the floor.

SPLIT ORDER -- what earns its own bucket first, as sample allows (Joe):
    0. format       structural, never pooled (A2): redraft / dynasty / bestball
    1. league size  the capacity tier -- the biggest single driver (R19: 24.6 pts)
    2. ppr scoring
    3. roster style flex / superflex / IDP
    4. pass TD
    5. playoff teams   only with crazy sample; last out, first in

That is the exact inverse of the old collapse order, which is the consistency check: the
cheapest axis to collapse is the last one that deserves its own bucket.

A node splits ONLY IF EVERY resulting child clears the floor. One thin child blocks that axis
for that node, and we try the next axis on the same node. So a fat corner of the grid can be
sliced finely while a thin corner stays coarse, in the same year.

Floor is the target N from R23 -- the sample a cell needs to hold its tolerance:
    +/-5 @85% = 140   +/-5 @95% = 180
    +/-3 @85% = 220   +/-3 @95% = 260
    +/-1 @85% = 560   +/-1 @95% = 650

Teams is the observed-capacity tier (R22): wr_spots = avg over weeks of the count of WR rows
in that league-week. Cutoffs 54.2 / 79.4 / 134.5, quantile-matched to the num_teams shape.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

# READ FROM THE CONTRACT so the pools cannot drift from the tier definition.
import position_slots_contract as _PS
C1, C2, C3 = _PS.tier_cuts("WR", "rostered")
# SPLIT ORDER (Joe): start as ONE TOTAL POOL and break out only when a split beats the level
# before it. Nothing is ever suppressed -- there is always a pool to fall back to.
#   total pool -> league size -> roster config -> scoring -> pass TD -> playoff teams
# format leads because it is structural (A2) and never merges; TD is LAST because it is the
# cheapest axis at 2.3 pts and so the last thing that earns its own bucket.
SPLIT_ORDER = ["fmtx", "tier", "roster", "ppr", "td", "bracket"]

# THE SPLIT TEST IS NOT "does the child clear the floor" (Joe, 2026-08-02). It is: does
# splitting produce a BETTER NUMBER than staying pooled? A pool at +/-1 @95% beats a split at
# +/-5 @85% unless the bias the pool carries is worse than the precision the split gives up.
# That trade is the reason the whole thing was modelled.
#
# A node that has NOT split on axis X carries X's bias, because it is mixing X's levels:
#     err(node) = sum(BIAS[ax] for ax not yet split) + moe(n_node)
# Splitting on X removes BIAS[X] and costs sample. Split iff the WORST child beats the node.
#
# BIAS is the R19 level deviation with the ~4.6pt noise floor removed in quadrature -- the raw
# deviation contains noise and charging it whole triple-counts noise as error.
NOISE = 0.046
_MEAS = {"td": 0.049, "roster": 0.087, "ppr": 0.058, "tier": 0.246, "fmtx": 0.249}
BIAS = {k: math.sqrt(max(v * v - NOISE ** 2, 0.0)) for k, v in _MEAS.items()}
BIAS["bracket"] = 0.0          # declared zero (Joe): no causal route to rostership
SD_WEEK = 0.50                 # per-league weekly value is 0/1 at the contested band


def moe(n: int, conf: float) -> float:
    """Achievable margin of error, in rate points, from n leagues at this confidence."""
    if n <= 0:
        return 9.9
    return NormalDist().inv_cdf((1 + conf) / 2) * SD_WEEK / math.sqrt(n)


def node_err(n: int, unsplit: list, conf: float) -> float:
    """Total error: bias from every axis still being mixed, plus sampling."""
    return sum(BIAS[ax] for ax in unsplit) + moe(n, conf)
TARGETS = {("5", "85"): 140, ("5", "95"): 180, ("3", "85"): 220,
           ("3", "95"): 260, ("1", "85"): 560, ("1", "95"): 650}


def year_grid(snapshot: Path, ops: Path, yr: int) -> pd.DataFrame:
    con = duckdb.connect(config={
        "memory_limit": "2000MB", "threads": 3,
        "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/pr{yr}"})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  "PRAGMA max_temp_directory_size='3GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS l (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
        # PASS THE YEAR. Without it the capacity CTEs aggregate all 16 years x 9 positions in
        # one query, which crashes the runtime rather than merely running slowly.
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True, year=yr)
                    .replace("public.", "l.public."))
        con.execute(
            'CREATE OR REPLACE TEMP TABLE wr AS SELECT NFL_player_id pid '
            'FROM ops.nfl_historical.nfl_player_stats_all WHERE "year"=' + str(yr) +
            " AND NFL_player_id IS NOT NULL AND UPPER(TRIM(position))='WR' "
            "AND position NOT LIKE '%,%' GROUP BY 1")
        g = con.execute(f"""
          SELECT CASE WHEN c.wr_spots<{C1} THEN '08tm' WHEN c.wr_spots<{C2} THEN '10tm'
                      WHEN c.wr_spots<{C3} THEN '12tm' ELSE '14tm' END tier,
                 f.roster, f.ppr, f.td, f.bracket,
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
            AND f.bracket IS NOT NULL
          GROUP BY ALL""").fetchdf()
        g["year"] = yr
        return g
    finally:
        con.close()


def refine(node: pd.DataFrame, axes: list, floor: int, done: list, fixed: dict,
           conf: float) -> list:
    """Split only when the worst resulting child is more accurate than staying pooled."""
    here = node_err(int(node.n.sum()), axes, conf)
    best = None
    for i, ax in enumerate(axes):
        parts = list(node.groupby(ax))
        if len(parts) < 2:
            continue
        rest = [x for x in axes if x != ax]
        # the split is only as good as its WORST child -- one thin child ruins it
        worst = max(node_err(int(p.n.sum()), rest, conf) for _, p in parts)
        if all(int(p.n.sum()) >= floor for _, p in parts) and worst < here:
            if best is None or worst < best[0]:
                best = (worst, i, ax, parts, rest)
    if best is None:
        return [{"split_on": "+".join(done) if done else "(whole year)",
                 "pool_key": "|".join(f"{k}={fixed[k]}" for k in SPLIT_ORDER if k in fixed)
                             or "ALL",
                 "leagues": int(node.n.sum()), "cohorts": len(node),
                 "still_mixed": "+".join(axes) or "-",
                 "err_pts": round(100 * here, 2)}]
    _, i, ax, parts, rest = best
    out = []
    for lvl, p in parts:
        out.extend(refine(p, rest, floor, done + [ax], {**fixed, ax: lvl}, conf))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--margin", choices=["5", "3", "1"], default="5")
    ap.add_argument("--conf", choices=["85", "95"], default="85")
    ap.add_argument("--first-year", type=int, default=2009)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    floor = TARGETS[(a.margin, a.conf)]

    rows = []
    for yr in range(a.first_year, 2026):
        g = year_grid(a.snapshot, a.ops, yr)
        total = int(g.n.sum())
        if total < floor:                        # the whole year cannot support one pool
            rows.append({"year": yr, "pool_key": "ALL", "split_on": "-", "leagues": total,
                         "cohorts": len(g), "verdict": "SUPPRESS (year under floor)"})
            continue
        for pool in refine(g, SPLIT_ORDER, floor, [], {}, float(a.conf) / 100):
            rows.append({"year": yr, **pool, "verdict": "POOL"})
    out = pd.DataFrame(rows)
    out.to_parquet(a.out, index=False)

    pd.set_option("display.width", 215)
    print(f"WR roster% WEEKLY - progressive refinement, +/-{a.margin}% @ {a.conf}%, "
          f"floor {floor} leagues\n")
    s = out.groupby("year").agg(pools=("pool_key", "size"), leagues=("leagues", "sum"),
                                cohorts=("cohorts", "sum"), smallest_pool=("leagues", "min"))
    s["axes_split"] = out.groupby("year").split_on.apply(
        lambda v: max((len(x.split("+")) if x not in ("(whole year)", "-") else 0) for x in v))
    if "err_pts" in out:
        s["worst_err"] = out.groupby("year").err_pts.max()
    print(s.to_string())
    ok = out[out.verdict == "POOL"]
    print(f"\nTOTAL pools 2010-2025: {len(ok):,}   "
          f"leagues with a home: {ok.leagues.sum():,} of {out.leagues.sum():,} "
          f"({100*ok.leagues.sum()/out.leagues.sum():.1f}%)")
    print("\nwhat each year splits on:")
    for yr, v in out.groupby("year").split_on:
        print(f"  {yr}: " + " | ".join(sorted(set(v))))


if __name__ == "__main__":
    main()
