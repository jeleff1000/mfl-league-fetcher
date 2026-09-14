"""WR roster%: the collapse rule set. Joe's policy, 2026-08-02.

The decision rule, stated by Joe: collapse an axis WHEN IT GIVES A BETTER LEVEL OF ACCURACY
THAN LEAVING THEM APART. That is bias-variance, evaluated per cell:

    err_separate = z * sqrt(p(1-p)/n_own)    * fpc(n_own)
    err_pooled   = bias_imported
                 + z * sqrt(p(1-p)/n_pooled) * fpc(n_pooled)
    collapse iff err_pooled < err_separate

Splitting a cohort buys precision only until the cell is too small to estimate; merging buys
sample only until the merged cells disagree. Every axis is tested, none is assumed.

ORDER (Joe):
    0. bracket  -- ALWAYS collapsed, no test. Playoff-team count has no route to rostership.
    1. td       -- 4.9 pts, sitting ON the ~4.6 noise floor (R19)
    2. roster   -- 8.7 pts
    3. ppr      -- 5.8 pts (residual only; the tier already carries the ppr flex weight, R17/R19)
    4. tier     -- 24.6 pts, the last resort
`format` is NEVER collapsed (A2, and 24.9 pts in R19).

NOTE ON ORDER: measured bias says ppr (5.8) is cheaper than roster (8.7), so testing ppr
BEFORE roster would collapse strictly better under Joe's own rule. Joe's stated order is
followed here; `--measured-order` swaps them so the difference can be seen rather than argued.

Bias for a multi-axis collapse is SUMMED, which is an UPPER BOUND, not a measurement -- real
merged-vs-true deviation does not accumulate additively. Cells that collapse deeply are
therefore conservative, and are flagged in the output.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

# R19 deviations are MEASURED |delta|, which CONTAINS the ~4.6pt noise floor: two cohort
# estimates differ by that much under the null. True bias removes it in quadrature --
# charging the raw deviation triple-counts noise as if it were error.
#     td 4.9->1.7   ppr 5.8->3.5   roster 8.7->7.4   tier 24.6->24.2
# bracket is 0 by DECLARATION (Joe): always collapsed, no causal route to rostership, and
# R19 measured it on 9 pairs and explicitly declined to rule.
NOISE_FLOOR = 0.046
_MEASURED = {"td": 0.049, "roster": 0.087, "ppr": 0.058, "tier": 0.246}
BIAS = {k: math.sqrt(max(v*v - NOISE_FLOOR**2, 0.0)) for k, v in _MEASURED.items()}
BIAS["bracket"] = 0.0
JOE_ORDER = ["td", "roster", "ppr", "tier"]
MEASURED_ORDER = ["td", "ppr", "roster", "tier"]
ALWAYS = ["bracket"]
NEVER = {"fmtx"}
AXES = ["tier", "roster", "ppr", "td", "bracket", "fmtx"]
P = 0.50          # the contested band this rule set is built for
# THE TWO GRAINS HAVE DIFFERENT SPREADS AND DIFFERENT FLOORS (Joe, 2026-08-02).
# Weekly: the per-league value is 0/1, so SD = sqrt(p(1-p)) = 0.500 at the contested band.
# Season: the per-league value is a RATE in [0,1] and its measured between-league SD is 0.319
# (R16a) -- leagues sit near the ends but not at them. Season therefore needs (0.319/0.500)^2
# = 41% as many leagues for the same error.
# The 30-league coherence floor is a SEASON floor. Its weekly equivalent is 30/0.41 = 73.
# MEASURED on the derived-capacity key, 40-60% band, cohorts >=150, 2022-2025: season SD is
# 0.398, NOT the 0.319 quoted from R16a (that was a tighter 45-55% band). The capacity tier did
# NOT shrink it -- old key 0.385 vs new 0.398, indistinguishable. Diagnostic, not a failure: the
# tier homogenises league SETTINGS, while the season spread is manager BEHAVIOUR (some leagues
# hold a player all year, some never touch him). Structurally alike leagues do not act alike.
# So season needs (0.398/0.500)^2 = 63% of weekly leagues, a 1.6x saving -- the same 1.6 that
# R14b and R16 reached independently. The 41% was the outlier.
SD = {"week": 0.500, "season": 0.398}
FLOOR = {"week": 48, "season": 30}


def err(n: int, conf: float, pop: int) -> float:
    """Half-width of the interval at cohort size n, finite-population corrected."""
    if n <= 0:
        return 9.9
    z = NormalDist().inv_cdf((1 + conf) / 2)
    fpc = math.sqrt(max((pop - n) / (pop - 1), 0.0)) if pop > 1 else 0.0
    return z * math.sqrt(P * (1 - P) / n) * (fpc if pop > n else 1.0) if pop > n else \
           z * math.sqrt(P * (1 - P) / n) * 0.0 if pop == n else z * math.sqrt(P * (1 - P) / n)


def half_width(n: int, conf: float, grain: str) -> float:
    """Sampling half-width for n leagues at this grain."""
    if n <= 0:
        return 9.9
    z = NormalDist().inv_cdf((1 + conf) / 2)
    return z * SD[grain] / math.sqrt(n)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--conf", type=float, default=0.85)
    ap.add_argument("--measured-order", action="store_true")
    ap.add_argument("--grain", choices=["week", "season"], default="season")
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    order = MEASURED_ORDER if a.measured_order else JOE_ORDER

    con = duckdb.connect(config={"memory_limit": "1600MB", "threads": 2})
    con.execute("SET enable_progress_bar=false")
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS l (READ_ONLY)")
    con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                cohort_league_settings_sql(position_slots=True).replace("public.", "l.public."))
    g = con.execute("""
      SELECT year,
        CASE WHEN cap<93 THEN '08tm' WHEN cap<113 THEN '10tm'
             WHEN cap<134 THEN '12tm' ELSE '14tm' END tier,
        roster, ppr, td, bracket,
        CASE WHEN lineup_mode='best_ball' THEN 'bestball'
             WHEN league_type='dynasty' THEN 'dynasty' ELSE 'redraft' END fmtx,
        COUNT(DISTINCT db_name) n
      FROM (SELECT f.*, s.num_teams*(COALESCE(s.roster_WR,0)
              + COALESCE(s.roster_FLX,0)*CASE WHEN f.ppr='std' THEN 0.47 ELSE 0.60 END
              + COALESCE(s.roster_BN,0)) cap
            FROM fmt f JOIN l.public.league_settings s
              ON s.db_name=f.db_name AND s.year=f.year
            WHERE f.year>=2009 AND f.roster IS NOT NULL AND f.ppr IS NOT NULL
              AND f.td IS NOT NULL AND f.bracket IS NOT NULL)
      JOIN (SELECT DISTINCT db_name, year FROM l.public.player_fantasy) USING (db_name, year)
      GROUP BY ALL
    """).fetchdf()
    con.close()

    rows = []
    for yr, gy in g.groupby("year"):
        for _, r in gy.iterrows():
            cur = {ax: r[ax] for ax in AXES}
            own = int(r.n)

            def merged(used: list[str]) -> int:
                m = gy
                for k, v in cur.items():
                    if k not in used:
                        m = m[m[k] == v]
                return int(m.n.sum())

            used = list(ALWAYS)                       # bracket always, untested
            bias = BIAS["bracket"]
            size = merged(used)
            for ax in order:
                e_sep = bias + half_width(size, a.conf, a.grain)
                cand = used + [ax]
                e_pool = bias + BIAS[ax] + half_width(merged(cand), a.conf, a.grain)
                if e_pool < e_sep:                    # Joe's rule, tested per axis
                    used, bias, size = cand, bias + BIAS[ax], merged(cand)
                else:
                    break
            final_err = bias + half_width(size, a.conf, a.grain)
            rows.append({
                "year": int(yr), "cohort": "|".join(str(cur[ax]) for ax in AXES),
                "own": own, "pooled": size, "collapse": "+".join(used),
                "bias_pts": round(100 * bias, 1),
                "err_pts": round(100 * final_err, 1),
                "verdict": ("SUPPRESS (below floor)" if size < FLOOR[a.grain]
                            else "SEPARATE" if used == ALWAYS else "POOL"),
            })
    out = pd.DataFrame(rows)
    out.to_parquet(a.out, index=False)
    pd.set_option("display.width", 250)
    print(f"WR roster% collapse rules — {a.conf:.0%} confidence, "
          f"order={'MEASURED' if a.measured_order else 'JOE'}\n")
    print(out.pivot_table(index="year", columns="verdict", values="cohort",
                          aggfunc="count").fillna(0).astype(int).to_string())
    print("\ncollapse depth chosen (bracket is always in):")
    print(out.groupby("collapse").agg(cells=("year", "size"), med_err=("err_pts", "median"),
                                      med_pooled=("pooled", "median")).to_string())
    print(f"\ntotal error at the chosen depth: p50 {out.err_pts.median():.1f} pts, "
          f"p90 {out.err_pts.quantile(.9):.1f} pts")


if __name__ == "__main__":
    main()
