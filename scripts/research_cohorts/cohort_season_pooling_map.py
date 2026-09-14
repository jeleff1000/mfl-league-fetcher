"""WR roster%, SEASON tier: which cohort-years stand alone, which pool, and onto what.

Target is 150 leagues -- Joe's ruling 2026-08-02, which is R16's "+/-5 points at 85%
confidence, season grain" (150 season / 200 weekly). Not derived here; read from R16.

Collapse order is A4's measured intercohort similarity, cheapest first:
    td -> ppr -> bracket -> roster
and two axes never collapse for this stat: `format` (A2, structural -- dynasty keeps the
roster and best ball never moves it) and `teams` (A4 -- correlates 0.879 but shifts the level
29 points; merging preserves the ordering and destroys the number).

R15 gates every pool on BIAS as well as size. Measured single-axis deviations on a fixed panel
are td 7.8pts, ppr 7.6, roster 14.9, teams 22.2 -- all larger than the +/-5 tolerance on
average, though 40% of td pairs and 33% of ppr pairs do fit inside it. So a pool is reported
with the deviation it imports, and a pool that imports more than the tolerance is called
SUPPRESS: buying 5 points of precision for 15 points of bias is not a trade we make.

Denominator is the eligible LIVE league set (R9) -- joined to player_fantasy, never
league_settings alone, which carries 770 dataless league-years in 2024 alone.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

AXES = ("teams", "roster", "ppr", "td", "bracket", "format")
COLLAPSE_ORDER = ["td", "ppr", "bracket", "roster"]
NEVER = {"format", "teams"}
TARGET_N = 150            # R16, season, +/-5 @ 85%
FIRST_YEAR = 2009         # A1
# R15 measured mean |delta| in roster% points imported by collapsing each axis
AXIS_BIAS = {"td": 0.078, "ppr": 0.076, "bracket": None, "roster": 0.149}
TOLERANCE = 0.05


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--target", type=int, default=TARGET_N)
    # Generous pooling: a level shift moves every player in a cohort the same way, so it
    # does not corrupt SHAPE -- the rank ordering survives. Raise the tolerance to pool for
    # coherence (the 30-league floor) rather than for a precise level.
    ap.add_argument("--tolerance", type=float, default=TOLERANCE)
    ap.add_argument("--floor", type=int, default=30)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(config={"memory_limit": "1500MB", "threads": 2})
    con.execute("SET enable_progress_bar=false")
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                cohort_league_settings_sql(position_slots=True).replace("public.", "lake.public."))
    con.execute("""CREATE OR REPLACE TEMP TABLE live AS
        SELECT DISTINCT db_name, year FROM lake.public.player_fantasy""")
    g = con.execute(f"""
        SELECT year, teams_WR AS teams, roster, ppr, td, bracket,
               CASE WHEN lineup_mode='best_ball' OR league_type='dynasty'
                    THEN 'deep' ELSE 'redraft' END AS format,
               COUNT(DISTINCT db_name) AS n
        FROM fmt JOIN live USING (db_name, year)
        WHERE year >= {FIRST_YEAR} AND teams_WR <> 'ALL' AND roster IS NOT NULL
          AND ppr IS NOT NULL AND td IS NOT NULL AND bracket IS NOT NULL
        GROUP BY ALL
    """).fetchdf()
    con.close()

    rows = []
    for yr, gy in g.groupby("year"):
        for _, r in gy.iterrows():
            cur = {ax: r[ax] for ax in AXES}
            size, used, bias = int(r.n), [], 0.0
            for ax in COLLAPSE_ORDER:
                if size >= a.target:
                    break
                used.append(ax)
                bias += AXIS_BIAS.get(ax) or 0.0
                m = gy
                for k, v in cur.items():
                    if k not in used:
                        m = m[m[k] == v]
                size = int(m.n.sum())
            if not used and size >= a.target:
                verdict = "SEPARATE"
            elif size >= a.target and bias <= a.tolerance:
                verdict = "POOL"
            elif size >= a.target:
                verdict = "SUPPRESS (pool too biased)"
            elif size >= a.floor:
                verdict = "POOL (coherence floor only)"
            else:
                verdict = "SUPPRESS (below 30-league floor)"
            rows.append({"year": int(yr), "cohort": "|".join(cur[ax] for ax in AXES),
                         **{f"c_{k}": v for k, v in cur.items()},
                         "own": int(r.n), "pooled": size, "collapse": "+".join(used) or "(none)",
                         "bias_pts": round(100 * bias, 1), "verdict": verdict})
    out = pd.DataFrame(rows)
    out.to_parquet(a.out, index=False)
    pd.set_option("display.width", 250)
    print(f"WR roster% SEASON — target {a.target} leagues (R16: ±5 @ 85%), tolerance ±{a.tolerance:.0%}\n")
    piv = out.pivot_table(index="year", columns="verdict", values="cohort",
                          aggfunc="count").fillna(0).astype(int)
    piv["cells"] = out.groupby("year").size()
    print(piv.to_string())
    print("\nWHERE the pooling happens (cells that need a collapse to reach target):")
    need = out[out.collapse != "(none)"]
    print(need.groupby(["collapse", "verdict"]).size().unstack(fill_value=0).to_string())
    print("\nby year — how deep a collapse each year needs:")
    print(out[out.year >= 2015].groupby(["year", "collapse"]).size().unstack(fill_value=0).to_string())


if __name__ == "__main__":
    main()
