"""Pooled top-10 order and membership stability for the cached league population.

Leagues are the sampling unit.  Weekly results are calculated per year/week and
then summarized across weeks; season results pool player-season cells by league.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from build_cohort_decile_bootstrap import _weekly_cells, metric_cell_columns
from build_cohort_top10_lock import sample_sizes

METRICS = ("roster_pct", "start_pct", "healthy_start_pct", "win_pct")
LOCK_LEVELS = (0.75, 0.85, 0.95)


def metric_source(cells: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Extract one metric from the wide aggregated cell table."""
    columns = metric_cell_columns(metric)
    source = cells[[
        "cohort_base", "db_name", "player", "year", "week",
        columns["numerator"], columns["observations"],
    ]].rename(columns={
        columns["numerator"]: "numerator",
        columns["observations"]: "observations",
    })
    source["value"] = source["numerator"] / source["observations"].replace(0, np.nan)
    return source[source["observations"] > 0].copy()


def _lock_probability(matrix: np.ndarray, population_order: np.ndarray, n: int,
                      rng: np.random.Generator, membership: bool = False) -> float:
    if n < 2 or matrix.shape[0] < n:
        return 0.0
    reps = min(100, max(40, 1200 // max(1, n // 10)))
    population_set = set(population_order.tolist())
    hits = 0
    for _ in range(reps):
        take = rng.choice(matrix.shape[0], size=n, replace=False)
        values = np.nanmean(matrix[take], axis=0)
        order = np.lexsort((np.arange(values.size), -np.nan_to_num(values, nan=-np.inf)))[:10]
        hit = set(order.tolist()) == population_set if membership else np.array_equal(order, population_order)
        hits += int(hit)
    return hits / reps


def _one_matrix(group: pd.DataFrame, metric: str, rng: np.random.Generator) -> dict | None:
    leagues = sorted(group.db_name.unique())
    players = sorted(group.player.unique())
    if len(leagues) < 2 or len(players) < 10:
        return None
    li = {name: i for i, name in enumerate(leagues)}
    pi = {name: i for i, name in enumerate(players)}
    matrix = np.full((len(leagues), len(players)), np.nan)
    for row in group[["db_name", "player", "value"]].itertuples(index=False):
        matrix[li[row.db_name], pi[row.player]] = row.value
    population = np.nanmean(matrix, axis=0)
    order = np.lexsort((np.arange(len(players)), -np.nan_to_num(population, nan=-np.inf)))[:10]
    sizes = sample_sizes(len(leagues))
    order_probs = [(n, _lock_probability(matrix, order, n, rng)) for n in sizes]
    member_probs = [(n, _lock_probability(matrix, order, n, rng, membership=True)) for n in sizes]
    out = {"available_leagues": len(leagues), "eligible_players": len(players),
           "population_top10": ",".join(players[i] for i in order)}
    for level in LOCK_LEVELS:
        suffix = str(int(level * 100))
        out[f"order_lock_{suffix}_leagues"] = next((n for n, p in order_probs if p >= level), None)
        out[f"membership_lock_{suffix}_leagues"] = next((n for n, p in member_probs if p >= level), None)
    return out


def build(args: argparse.Namespace) -> None:
    args.out.parent.mkdir(parents=True, exist_ok=True)
    (args.out.parent / "duckdb_temp").mkdir(exist_ok=True)
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB",
                                 "temp_directory": str(args.out.parent / "duckdb_temp")})
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    if args.ops:
        con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")
    cells = _weekly_cells(con, args.years, args.position)
    if cells.empty:
        pd.DataFrame().to_parquet(args.out, index=False)
        return
    label = str(args.years[0]) if len(args.years) == 1 else f"{min(args.years)}-{max(args.years)}_pooled"
    rng = np.random.default_rng(args.seed + sum(args.years) + sum(map(ord, args.position)))
    rows: list[dict] = []
    for metric in METRICS:
        source = metric_source(cells, metric)
        # One league/player value per year-week: the weekly answer is a distribution
        # of weekly cells, not a single giant row-level population.
        weekly = (source.groupby(["cohort_base", "db_name", "player", "year", "week"], as_index=False)
                         .value.mean())
        for keys, group in weekly.groupby(["cohort_base", "year", "week"], dropna=False):
            result = _one_matrix(group, metric, rng)
            if result:
                result.update({"year": label, "position": args.position, "grain": "weekly",
                               "metric": metric, "cohort_key": keys[0], "period_year": int(keys[1]),
                               "period_week": int(keys[2])})
                rows.append(result)
        season = (source.groupby(["cohort_base", "db_name", "player"], as_index=False)
                        .agg(value=("numerator", "sum"), observations=("observations", "sum")))
        season["value"] = season.value / season.observations.replace(0, np.nan)
        for cohort, group in season.groupby("cohort_base", dropna=False):
            result = _one_matrix(group, metric, rng)
            if result:
                result.update({"year": label, "position": args.position, "grain": "season",
                               "metric": metric, "cohort_key": cohort,
                               "period_year": None, "period_week": None})
                rows.append(result)
    pd.DataFrame(rows).to_parquet(args.out, index=False)
    print(f"wrote {len(rows):,} top10 stability rows to {args.out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path)
    ap.add_argument("--years", required=True)
    ap.add_argument("--position", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--memory-mb", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=20260731)
    args = ap.parse_args()
    args.years = [int(x) for x in json.loads(args.years)]
    build(args)


if __name__ == "__main__":
    main()
