"""STANDING POOLING RULES: start pooled, add a dimension when it earns it, 1 through 6.

Joe's procedure, followed literally:

  1. DEFAULT IS POOLED. Nothing is split until it earns its way in. We ADD, never dissolve.
  2. PER-RUNG AND INDEPENDENT. Earning `teams` splits teams and leaves the rest pooled.
  3. ADD WHEN the accuracy gained beats the precision lost at that dimension's observed n.
  4. THE PANEL IS PLAYERS HOVERING AT p, COMPARED ONLY TO THEMSELVES, IN THAT WEEK.
     Legette week 9 vs Legette week 9. Never Legette vs Johnston.
  5. FOUR TARGETS, always: 45-55 and 47-53, each at 85% and 95%.

HOW EACH DIMENSION IS MEASURED -- as an independent variable, on the player against himself.
Inside the cell he is already in, split by the candidate dimension and read HIS rate in each
resulting level. The move is how far those levels sit from the number he was being served.
That is the accuracy the pooled cell was costing him, measured on him, not inferred from an
axis-level average and not an |bias| that a null dimension can earn out of pure noise.

    accuracy gained = |his rate in the child cell - his rate in the parent cell|
    precision lost  = z*sd*(1/sqrt(n_child) - 1/sqrt(n_parent))
    ADD IT IF gained > lost

Then the same question again with that dimension already in, and again, to 6. What comes out
is a standing rule per (level, dimension): the move it makes, the n it leaves behind, and the
pooled n at which it starts paying.

ONE BASE TABLE. Per (player, week, full 7-axis cohort) the rostered-league count, and per
cohort the league count. Every subset of axes is a group-by over that, so the corpus is read
once and the 6-level accumulation is arithmetic.

UNKNOWN IS NOT A LEVEL (R9 sibling): 370 league-years never captured playoff teams. That cell
is dropped, not treated as a fourth bracket -- otherwise a 30-league junk cell decides whether
bracket splits.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from statistics import NormalDist

import duckdb
import numpy as np
import pandas as pd

import position_slots_contract as PS
from cohort_format_sql import cohort_league_settings_sql

AXES = ("teams", "roster", "ppr", "td", "bracket", "lineup_mode", "league_type")
SD_WEEK = 0.50          # a weekly league value is 0/1; at p=.5 its sd is exactly .5
BAND = (0.45, 0.55)     # "hovering at 50%" -- the panel, measured in the POOLED population


def load(snapshot: Path, ops: Path, pos: str, year: int, tmp: Path):
    con = duckdb.connect(config={"memory_limit": "2500MB", "threads": 3,
                                 "temp_directory": str(tmp / f"{pos}{year}")})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  "PRAGMA max_temp_directory_size='20GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True, year=year)
                    .replace("public.", "lake.public."))
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lgc AS
          SELECT f.db_name, f.teams_{pos} AS teams, f.roster, f.ppr, f.td, f.bracket,
                 f.lineup_mode, f.league_type
          FROM fmt f
          JOIN (SELECT DISTINCT db_name FROM lake.public.player_fantasy WHERE year={year}) lv
            ON lv.db_name = f.db_name
          WHERE f.year={year} AND f.teams_{pos} <> 'ALL'
            AND f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL
            AND f.bracket IS NOT NULL AND f.lineup_mode IS NOT NULL
            AND f.league_type IS NOT NULL
            AND ({PS.position_eligibility_sql(pos, 'f')})""")
        lg = con.execute(f"SELECT {', '.join(AXES)}, COUNT(*) AS n FROM lgc GROUP BY ALL").fetchdf()
        base = con.execute(f"""
          SELECT {', '.join('g.'+a for a in AXES)}, pf.NFL_player_id AS pid, pf.week,
                 COUNT(DISTINCT pf.db_name) AS k
          FROM lake.public.player_fantasy pf
          JOIN (SELECT NFL_player_id pid FROM ops.nfl_historical.nfl_player_stats_all
                WHERE "year"={year} AND NFL_player_id IS NOT NULL
                  AND UPPER(TRIM(position))='{pos}' AND position NOT LIKE '%,%'
                GROUP BY 1) n ON n.pid = pf.NFL_player_id
          JOIN lgc g ON g.db_name = pf.db_name
          WHERE pf.year={year} AND pf.week BETWEEN 1 AND 17
          GROUP BY ALL""").fetchdf()
        return lg, base
    finally:
        con.close()


def rates(base, lg, axes):
    """His rate in every cell defined by `axes` -- k summed over the cell, n likewise."""
    if axes:
        k = base.groupby(list(axes) + ["pid", "week"], observed=True).k.sum().reset_index()
        n = lg.groupby(list(axes), observed=True).n.sum().reset_index()
        r = k.merge(n, on=list(axes))
    else:
        k = base.groupby(["pid", "week"], observed=True).k.sum().reset_index()
        r = k.assign(n=lg.n.sum())
    r["rate"] = r.k / r.n
    return r


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--position", default="WR", choices=list(PS.TIER_POSITIONS))
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--conf", type=float, default=0.85)
    ap.add_argument("--depth", type=int, default=6)
    ap.add_argument("--tmp", type=Path, default=Path("D:/tmp/ddbtmp/rules"))
    a = ap.parse_args()
    a.tmp.mkdir(parents=True, exist_ok=True)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    z = NormalDist().inv_cdf(1 - (1 - a.conf) / 2)

    lg, base = load(a.snapshot, a.ops, a.position, a.year, a.tmp)
    for ax in AXES:                       # unknown is not a level
        lg = lg[lg[ax] != "(unknown)"]
        base = base[base[ax] != "(unknown)"]
    print(f"{a.position} {a.year}: {int(lg.n.sum()):,} leagues, "
          f"{base.pid.nunique()} players, {len(lg)} full cohorts\n")

    # THE PANEL: hovering at 50% in the POOLED population, that week.
    pooled = rates(base, lg, ())
    panel = pooled[pooled.rate.between(*BAND)][["pid", "week"]]
    print(f"panel: {len(panel):,} player-weeks at {BAND[0]:.0%}-{BAND[1]:.0%} pooled\n")

    chosen, rows = [], []
    for level in range(a.depth):
        parent = rates(base, lg, tuple(chosen)).merge(panel, on=["pid", "week"])
        best = None
        for cand in [x for x in AXES if x not in chosen]:
            child = rates(base, lg, tuple(chosen) + (cand,)).merge(panel, on=["pid", "week"])
            j = child.merge(parent[list(chosen) + ["pid", "week", "rate", "n"]],
                            on=list(chosen) + ["pid", "week"], suffixes=("_c", "_p"))
            if j.empty:
                continue
            # HIM vs HIMSELF: same player, same week, parent cell vs child cell.
            gained = (j.rate_c - j.rate_p).abs().median()
            lost = z * SD_WEEK * (1 / np.sqrt(j.n_c) - 1 / np.sqrt(j.n_p)).median()
            # the pooled size at which this dimension starts paying, given the move it makes
            k = base[cand].nunique()
            n_pay = np.inf if gained <= 0 else (z * SD_WEEK * (np.sqrt(k) - 1) / gained) ** 2
            rec = {"level": level + 1, "add": cand, "on_top_of": "+".join(chosen) or "(pooled)",
                   "levels": k, "n_parent": int(j.n_p.median()),
                   "n_child_worst": int(child.n.min()), "moves_pts": 100 * gained,
                   "costs_pts": 100 * lost, "net_pts": 100 * (gained - lost),
                   "pays_above_n": n_pay, "verdict": "ADD" if gained > lost else "keep pooled"}
            rows.append(rec)
            if best is None or rec["net_pts"] > best["net_pts"]:
                best = rec
        if best is None:
            break
        chosen.append(best["add"])
        print(f"  level {level+1}: add {best['add']:<12} moves {best['moves_pts']:5.2f}p, "
              f"costs {best['costs_pts']:5.2f}p, net {best['net_pts']:+5.2f}p -> "
              f"{best['verdict']}", flush=True)

    out = pd.DataFrame(rows)
    out["position"], out["year"] = a.position, a.year
    out.to_parquet(a.out, index=False)
    pd.set_option("display.width", 220)
    print(f"\nSTANDING RULES -- {a.position} roster% weekly {a.year}\n")
    print(out[["level", "on_top_of", "add", "levels", "n_parent", "n_child_worst",
               "moves_pts", "costs_pts", "net_pts", "pays_above_n", "verdict"]]
          .round(2).to_string(index=False))
    print(f"\nchosen order: {' -> '.join(chosen)}")


if __name__ == "__main__":
    main()
