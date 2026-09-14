"""The pooling map: which cohorts pool cleanly, per (stat, position, grain).

Each stat is stress-tested on ITS OWN events at ITS OWN grain -- specific player-weeks or
player-seasons chosen because that stat can actually move on them. A stud's start% is 100%
in every cohort and proves nothing; the flex-or-bench call at ~50% is where a 10-team and a
12-team market genuinely diverge. One dramatic playoff week holds the football constant, so
the spread across cohorts IS the cohort effect.

Events are chosen ONCE on the pooled cross-cohort value, then measured everywhere. Choosing
per cohort would pick different events in each and reintroduce the confound.

The cohort dimension is NEVER crossed with the whole corpus: selecting events is a pass with
no cohort at all, and only the chosen events are then read per cohort. The other order
materialises ~23M (player, week, cohort) groups and spills.

Healthy Start% is not measured -- it inherits Start%'s groups (same shape, per Joe).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from cohort_format_sql import cohort_league_settings_sql

TOP_N, MIN_LG, MIN_EVENTS, MIN_POOL = 12, 20, 4, 200
POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB")

# stat -> (grain, event placement, pooled-selection expr, per-cohort key, tolerance)
SPEC = {
    # COUNT(*) not COUNT(DISTINCT db_name): one row per (league, year, week, player), so they
    # are equivalent, but the distinct form holds a string-set per group and blows the budget.
    "roster_pct":  ("week",   "middle",  "COUNT(*)",                       "roster",  0.03),
    "start_pct":   ("week",   "middle",  "AVG(CAST(is_started AS DOUBLE))", "start",  0.03),
    "win_pct":     ("week",   "extreme", "AVG(TRY_CAST(win AS DOUBLE))",   "win",     0.03),
    "clutch_po":   ("po",     "extreme", "AVG(clutch_equity)",             "clutch",  0.50),
    "exp_starts":  ("season", "middle",  "AVG(starts)",                    "starts",  0.50),
    "exp_wl":      ("season", "extreme", "AVG(winr*starts/NULLIF(wks,0))", "expwl",   0.03),
    "clutch_szn":  ("season", "extreme", "AVG(clutch)",                    "clutchs", 0.50),
    # champ/playoff are zero-inflated: most players never start one, so the BOTTOM extreme
    # is a huge tie at zero. Selecting "nearest either extreme" fills the panel with events
    # that score 0 in every cohort, and every pair then agrees at distance exactly 0.0000 --
    # which is not agreement, it is measuring nothing. Top extreme only.
    "champ_pct":   ("season", "top",     "AVG(champ)",                     "champ",   0.03),
    "playoff_pct": ("season", "top",     "AVG(po)",                        "po",      0.03),
}
COHORT_EXPR = {
    "roster":  "COUNT(*)::DOUBLE / MAX(d.elig)",
    "start":   "AVG(CAST(pf.is_started AS DOUBLE))",
    "win":     "AVG(TRY_CAST(pf.win AS DOUBLE))",
    "clutch":  "AVG(pf.clutch_equity)",
    "starts":  "AVG(s.starts)",
    "expwl":   "AVG(COALESCE(s.winr,0)*s.starts/NULLIF(s.wks,0))",
    "clutchs": "AVG(s.clutch)",
    "champ":   "AVG(s.champ::DOUBLE)",
    "po":      "AVG(s.po::DOUBLE)",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--years", default="2021,2022,2023,2024")
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    Y = a.years

    con = duckdb.connect(config={"memory_limit": "2500MB", "threads": 3,
                                 "temp_directory": "C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp"})
    for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
              "PRAGMA max_temp_directory_size='6GB'"):
        con.execute(s)
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{a.ops.as_posix()}' AS ops (READ_ONLY)")
    con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                cohort_league_settings_sql(position_slots=True).replace("public.", "lake.public."))
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pos_map AS
    SELECT NFL_player_id pid, CAST("year" AS INTEGER) yr,
           MAX(CASE WHEN UPPER(TRIM(position)) IN ('DST','D/ST','DEF') THEN 'DEF'
                    ELSE UPPER(TRIM(position)) END) pos, MAX(player) pname
    FROM ops.nfl_historical.nfl_player_stats_all
    WHERE "year" IN ({Y}) AND NFL_player_id IS NOT NULL AND position IS NOT NULL
      AND position NOT LIKE '%,%'
    GROUP BY 1,2
    """)
    CELL = ("concat_ws('|', CASE p.pos WHEN 'QB' THEN f.teams_QB WHEN 'RB' THEN f.teams_RB "
            "WHEN 'WR' THEN f.teams_WR WHEN 'TE' THEN f.teams_TE ELSE f.teams END, "
            "f.roster, f.ppr, f.td, f.bracket)")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lg_cell AS
    SELECT f.db_name, f.year yr, p.pos, {CELL} cell
    FROM fmt f CROSS JOIN (SELECT DISTINCT pos FROM pos_map) p
    WHERE f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL
      AND f.bracket IS NOT NULL AND f.year IN ({Y})
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE den AS SELECT cell, pos, yr, "
                "COUNT(DISTINCT db_name) elig FROM lg_cell GROUP BY 1,2,3")
    print("cohort cells:", con.execute("SELECT COUNT(DISTINCT cell) FROM lg_cell").fetchone()[0],
          flush=True)

    # One year at a time: grouping ~177M rows by a string db_name across all years at once
    # spills past any sane temp budget. Per-year each GROUP BY is bounded.
    # Season length comes from a small per-league aggregate. COUNT(DISTINCT week) inside the
    # per-player GROUP BY holds a distinct-set for every one of millions of groups and is
    # what blows the temp budget -- the rest of this aggregate is cheap.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lg_wks AS
    SELECT db_name, year yr, COUNT(DISTINCT week) wks
    FROM lake.public.player_fantasy WHERE year IN ({Y}) GROUP BY 1,2
    """)
    szn_sel = """
    SELECT pf.db_name, pf.year yr, pf.NFL_player_id pid,
           SUM(CAST(pf.is_started AS INTEGER)) starts, MAX(w.wks) wks,
           SUM(pf.clutch_equity) clutch,
           AVG(TRY_CAST(pf.win AS DOUBLE)) FILTER (WHERE pf.is_started=1) winr,
           MAX(CASE WHEN pf.champion=1 AND pf.is_started=1 THEN 1 ELSE 0 END) champ,
           MAX(CASE WHEN pf.is_playoffs=1 AND pf.is_started=1
                    THEN 1 ELSE 0 END) po
    FROM lake.public.player_fantasy pf
    JOIN lg_wks w ON w.db_name=pf.db_name AND w.yr=pf.year
    WHERE pf.year = {y} AND pf.NFL_player_id IS NOT NULL
    GROUP BY 1,2,3
    """
    years = [int(v) for v in Y.split(",")]
    con.execute(f"CREATE OR REPLACE TEMP TABLE szn AS {szn_sel.format(y=years[0])}")
    for y in years[1:]:
        con.execute(f"INSERT INTO szn {szn_sel.format(y=y)}")
        print(f"  szn {y} done", flush=True)
    print("szn rows:", con.execute("SELECT COUNT(*) FROM szn").fetchone()[0], flush=True)

    rows = []
    for stat, (grain, place, sel, ckey, tol) in SPEC.items():
        order = {"middle": "ABS(pr-0.5)", "top": "(1-pr)"}.get(place, "LEAST(pr, 1-pr)")
        po_w = "AND is_playoffs=1" if grain == "po" else ""
        po_p = "AND pf.is_playoffs=1" if grain == "po" else ""
        if grain == "season":
            con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ev AS
            WITH p AS (SELECT s.pid, s.yr, m.pos, MAX(m.pname) pname, {sel} v
                       FROM szn s JOIN pos_map m ON m.pid=s.pid AND m.yr=s.yr
                       GROUP BY 1,2,3 HAVING COUNT(*) >= {MIN_POOL}),
            r AS (SELECT *, PERCENT_RANK() OVER (PARTITION BY pos ORDER BY v) pr FROM p)
            SELECT pid, yr, pos, pname FROM
              (SELECT *, ROW_NUMBER() OVER (PARTITION BY pos ORDER BY {order}) rn FROM r)
            WHERE rn <= {TOP_N}
            """)
        else:
            con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ev AS
            WITH p AS (SELECT pf.NFL_player_id pid, pf.year yr, pf.week wk, m.pos,
                              MAX(m.pname) pname, {sel} v
                       FROM lake.public.player_fantasy pf
                       JOIN pos_map m ON m.pid=pf.NFL_player_id AND m.yr=pf.year
                       WHERE pf.year IN ({Y}) {po_w}
                       GROUP BY 1,2,3,4 HAVING COUNT(*) >= {MIN_POOL}),
            r AS (SELECT *, PERCENT_RANK() OVER (PARTITION BY pos ORDER BY v) pr FROM p)
            SELECT pid, yr, wk, pos, pname FROM
              (SELECT *, ROW_NUMBER() OVER (PARTITION BY pos ORDER BY {order}) rn FROM r)
            WHERE rn <= {TOP_N}
            """)
        nev = con.execute("SELECT COUNT(*) FROM ev").fetchone()[0]
        expr = COHORT_EXPR[ckey]
        if grain == "season":
            vals = con.execute(f"""
            SELECT e.pos, e.pname||' '||e.yr AS event, g.cell,
                   COUNT(*) lg, {expr} AS val
            FROM ev e JOIN szn s ON s.pid=e.pid AND s.yr=e.yr
                      JOIN lg_cell g ON g.db_name=s.db_name AND g.yr=s.yr AND g.pos=e.pos
            GROUP BY 1,2,3 HAVING COUNT(*) >= {MIN_LG}
            """).fetchdf()
        else:
            vals = con.execute(f"""
            SELECT e.pos, e.pname||' '||e.yr||' wk'||e.wk AS event, g.cell,
                   COUNT(*) lg, {expr} AS val
            FROM ev e
            JOIN lake.public.player_fantasy pf
              ON pf.NFL_player_id=e.pid AND pf.year=e.yr AND pf.week=e.wk
            JOIN lg_cell g ON g.db_name=pf.db_name AND g.yr=pf.year AND g.pos=e.pos
            JOIN den d ON d.cell=g.cell AND d.pos=e.pos AND d.yr=e.yr
            WHERE TRUE {po_p}
            GROUP BY 1,2,3 HAVING COUNT(*) >= {MIN_LG}
            """).fetchdf()
        n = 0
        for pos, d in vals.groupby("pos"):
            w = d.pivot_table(index="event", columns="cell", values="val")
            w = w.loc[:, w.notna().sum() >= MIN_EVENTS]
            cols = list(w.columns)
            for i, x in enumerate(cols):
                for y in cols[i + 1:]:
                    pp = w[[x, y]].dropna()
                    if len(pp) < MIN_EVENTS:
                        continue
                    rows.append({"stat": stat, "grain": grain, "pos": pos, "a": x, "b": y,
                                 "events": len(pp),
                                 "dist": float((pp[x] - pp[y]).abs().mean()), "tol": tol})
                    n += 1
        print(f"  {stat:12s} grain={grain:6s} events={nev:>3} pairs={n:,}", flush=True)
    con.close()

    r = pd.DataFrame(rows)
    inh = r[r.stat == "start_pct"].copy()
    inh["stat"] = "healthy_start_pct"
    r = pd.concat([r, inh], ignore_index=True)
    r["pool"] = r.dist <= r.tol
    r.to_parquet(a.out, index=False)
    pd.set_option("display.width", 250)
    print(f"\n{len(r):,} cohort-pair scores across {r.stat.nunique()} stats\n")
    t = r.groupby(["stat", "pos"]).agg(pairs=("pool", "size"), pool=("pool", "sum"),
                                       med=("dist", "median"))
    t["pct"] = (100 * t.pool / t.pairs).round(1)
    piv = t.reset_index().pivot(index="stat", columns="pos", values="pct")
    print("% of cohort pairs that POOL cleanly:")
    print(piv[[c for c in POSITIONS if c in piv.columns]].to_string())
    print("\nmedian distance (stat's own units):")
    piv2 = t.reset_index().pivot(index="stat", columns="pos", values="med")
    print(piv2[[c for c in POSITIONS if c in piv2.columns]].round(3).to_string())


if __name__ == "__main__":
    main()
