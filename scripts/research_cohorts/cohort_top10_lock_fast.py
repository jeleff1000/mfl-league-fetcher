"""At what cohort size does the top 10 lock into the population's order?

Vectorised: top-30 candidates only (player #200 cannot alter the top 10), numpy scatter
instead of a per-row Python fill, one batched (reps, n) sample per size, and a binary
search over n because lock probability is monotone in sample size.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from cohort_precision_closed_form import COHORTS, ELIGIBILITY

LOCK_LEVELS = (0.75, 0.85, 0.95)
CANDIDATES = 30
REPS = 400


def lock_probs(matrix: np.ndarray, pop_order: np.ndarray, n: int,
               rng: np.random.Generator) -> tuple[float, float]:
    """P(sampled top-10 == population top-10) for exact order and for membership."""
    L = matrix.shape[0]
    if n < 2 or n > L:
        return 0.0, 0.0
    idx = np.argsort(rng.random((REPS, L)), axis=1)[:, :n]      # n distinct leagues per rep
    means = matrix[idx].mean(axis=1)                            # (REPS, candidates)
    tie_break = np.arange(means.shape[1])
    order = np.lexsort((np.tile(tie_break, (REPS, 1)), -means), axis=1)[:, :10]
    exact = float(np.all(order == pop_order[None, :], axis=1).mean())
    member = float(np.array([set(r) == set(pop_order.tolist()) for r in order]).mean())
    return exact, member


def smallest_locking(matrix: np.ndarray, pop_order: np.ndarray, level: float,
                     rng: np.random.Generator, membership: bool) -> int | None:
    """Binary search the smallest n whose lock probability clears `level`."""
    L = matrix.shape[0]
    pick = (lambda p: p[1]) if membership else (lambda p: p[0])
    if pick(lock_probs(matrix, pop_order, L, rng)) < level:
        return None
    lo, hi = 2, L
    while lo < hi:
        mid = (lo + hi) // 2
        if pick(lock_probs(matrix, pop_order, mid, rng)) >= level:
            hi = mid
        else:
            lo = mid + 1
    return lo


def build(args: argparse.Namespace) -> pd.DataFrame:
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB", "threads": args.threads,
                                 "temp_directory": str(args.tmp)})
    con.execute("SET enable_progress_bar=false")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")

    rows = []
    for year in args.years:
        elig = ",\n".join(f"  CASE WHEN ({e})>0 THEN '{p}' END" for p, e in ELIGIBILITY.items())
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE league_cohort AS
        SELECT db_name, cohort, position FROM lake.public.league_settings,
          {COHORTS}, UNNEST(list_filter([\n{elig}\n], x -> x IS NOT NULL)) AS p(position)
        WHERE "year"={year} AND NOT COALESCE(sleeper_best_ball, FALSE)
        """)
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE nfl AS
        SELECT NFL_player_id, CAST("week" AS INTEGER) AS week,
               MAX(CASE WHEN UPPER(TRIM(position)) IN ('DST','D/ST','DEF') THEN 'DEF'
                        ELSE UPPER(TRIM(position)) END) AS position
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE "year"={year} AND NFL_player_id IS NOT NULL AND position IS NOT NULL
        GROUP BY 1,2
        """)

        cohorts = [r[0] for r in con.execute(
            "SELECT DISTINCT cohort FROM league_cohort ORDER BY 1").fetchall()]
        for cohort in cohorts:
            # One scan per cohort covering every position.  Scanning per cohort AND per
            # position means 60 full passes over player_fantasy for a single season.
            all_cells = con.execute(f"""
            SELECT lc.position, pf.db_name, pf.NFL_player_id AS player,
                   SUM(CAST(pf.is_started AS DOUBLE)) AS v
            FROM lake.public.player_fantasy pf
            JOIN nfl n ON n.NFL_player_id=pf.NFL_player_id AND n.week=pf.week
            JOIN league_cohort lc ON lc.db_name=pf.db_name AND lc.cohort='{cohort}'
                                 AND lc.position=n.position
            WHERE pf.year={year}
            GROUP BY 1,2,3
            """).fetchdf()
            league_map = con.execute(
                "SELECT position, db_name FROM league_cohort WHERE cohort=? ORDER BY 1,2",
                [cohort]).fetchdf()

            for position in args.positions:
                leagues = league_map[league_map.position == position].db_name.to_numpy()
                if len(leagues) < 20:
                    continue
                cell = all_cells[all_cells.position == position]
                if cell.empty:
                    continue

                # Population value denominates on every eligible league, not on the
                # leagues that happen to hold a row.
                pop = (cell.groupby("player").v.sum() / len(leagues)).sort_values(ascending=False)
                top = pop.head(CANDIDATES)
                if len(top) < 11:
                    continue
                players = top.index.to_numpy()

                sub = cell[cell.player.isin(players)]
                li = pd.Index(leagues).get_indexer(sub.db_name.to_numpy())
                pi = pd.Index(players).get_indexer(sub.player.to_numpy())
                keep = li >= 0
                matrix = np.zeros((len(leagues), len(players)), dtype=np.float32)
                matrix[li[keep], pi[keep]] = sub.v.to_numpy(dtype=np.float32)[keep]

                pop_vals = matrix.mean(axis=0)
                pop_order = np.lexsort((np.arange(len(players)), -pop_vals))[:10]
                rng = np.random.default_rng(20260801 + year + len(cohort) + len(position))

                rec = {"year": year, "cohort": cohort, "position": position,
                       "available_leagues": len(leagues), "candidates": len(players),
                       "gap_10_11": float(pop_vals[pop_order[9]] -
                                          np.sort(pop_vals)[::-1][10]) if len(players) > 10 else None}
                for level in LOCK_LEVELS:
                    tag = int(level * 100)
                    rec[f"order_lock_{tag}"] = smallest_locking(matrix, pop_order, level, rng, False)
                    rec[f"member_lock_{tag}"] = smallest_locking(matrix, pop_order, level, rng, True)
                rows.append(rec)
                print(f"[{year}] {cohort}/{position}: L={len(leagues)} "
                      f"order85={rec['order_lock_85']} member85={rec['member_lock_85']}", flush=True)
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--years", type=int, nargs="+", required=True)
    ap.add_argument("--positions", nargs="+", default=["QB", "RB", "WR", "TE", "K", "DEF"])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--memory-mb", type=int, default=5000)
    ap.add_argument("--threads", type=int, default=3)
    ap.add_argument("--tmp", type=Path, default=Path("C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp"))
    args = ap.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df = build(args)
    df.to_parquet(args.out, index=False)
    print(f"\nwrote {len(df):,} top10 lock rows -> {args.out}")


if __name__ == "__main__":
    main()
