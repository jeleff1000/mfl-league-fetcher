"""Execute annual rank-only stability over cached Draft/Transaction event facts."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

import duckdb
import pandas as pd

from top10_generic_matrix import SparseResearchBoard
from top10_stability_runner import (
    annual_candidate_grid,
    evaluate_candidate_batched,
    select_annual_threshold,
)


@dataclass(frozen=True)
class LightMetric:
    dataset: str
    grain: str
    metric: str
    numerator: str
    denominator: str
    support: str
    pool_support: str
    minimum_pool_rate: float
    direction: str = "desc"


LIGHT_METRICS = (
    LightMetric("draft", "season", "adp", "sum_adp_pick", "n_adp_pick", "n_drafted", "n_drafted", 0.03),
    LightMetric("draft", "season", "draft_rate", "n_drafted", "eligible_leagues", "n_drafted", "n_drafted", 0.03),
    LightMetric("draft", "season", "cost", "sum_auction_cost_pct", "n_auction_cost_pct", "n_auction_cost_pct", "n_drafted", 0.03),
    LightMetric("transactions", "season", "add_rate", "n_add_leagues", "eligible_leagues", "n_add_leagues", "n_add_leagues", 0.03),
    LightMetric("transactions", "season", "faab", "sum_faab_pct", "n_faab_pct", "n_add_leagues", "n_add_leagues", 0.03),
)


def supported_candidates(population: int) -> tuple[int, ...]:
    return annual_candidate_grid(population)


def _eligibility(population: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(
        [
            population[["db_name"]].assign(position_group="SKILL", eligible=population.skill_eligible),
            population[["db_name"]].assign(position_group="K", eligible=population.k_eligible),
            population[["db_name"]].assign(position_group="DEF", eligible=population.def_eligible),
        ],
        ignore_index=True,
    )


def load_light_board(
    cache: Path,
    *,
    dataset: str,
    year: int,
    direct_values: pd.DataFrame | None = None,
) -> SparseResearchBoard:
    con = duckdb.connect()
    population = con.execute(
        "SELECT * FROM read_parquet(?)", [str(cache / f"{dataset}_population_{year}.parquet")]
    ).fetchdf()
    if dataset == "draft":
        facts = con.execute(
            """
            SELECT db_name,NFL_player_id,MAX(position) AS "position",
                   SUM(COALESCE(sum_adp_pick,0)) sum_adp_pick,
                   SUM(COALESCE(n_adp_pick,0)) n_adp_pick,
                   MAX(COALESCE(n_drafted,0)) n_drafted,
                   SUM(COALESCE(sum_auction_cost_pct,0)) sum_auction_cost_pct,
                   SUM(COALESCE(n_auction_cost_pct,0)) n_auction_cost_pct
            FROM read_parquet(?) GROUP BY 1,2
            """,
            [str(cache / f"draft_season_{year}.parquet")],
        ).fetchdf()
        columns = (
            "sum_adp_pick", "n_adp_pick", "n_drafted",
            "sum_auction_cost_pct", "n_auction_cost_pct",
        )
    elif dataset == "transactions":
        facts = con.execute(
            """
            SELECT db_name,NFL_player_id,MAX(position) AS "position",
                   SUM(COALESCE(n_add_events,0)) n_add_events,
                   MAX(COALESCE(n_add_leagues,0)) n_add_leagues,
                   SUM(COALESCE(sum_faab_pct,0)) sum_faab_pct,
                   SUM(COALESCE(n_faab_pct,0)) n_faab_pct
            FROM read_parquet(?) GROUP BY 1,2
            """,
            [str(cache / f"transactions_weekly_{year}.parquet")],
        ).fetchdf()
        columns = ("n_add_events", "n_add_leagues", "sum_faab_pct", "n_faab_pct")
    else:
        raise ValueError(f"unsupported light dataset: {dataset}")
    return SparseResearchBoard.from_frame(
        facts,
        fact_columns=columns,
        eligibility=_eligibility(population),
        direct_values=direct_values,
    )


def run_light_study(
    cache: Path,
    *,
    years: Sequence[int],
    datasets: Sequence[str],
    positions: Sequence[str],
    reps: int,
    seed: int,
    out_dir: Path,
    max_only: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out_dir.mkdir(parents=True, exist_ok=True)
    candidate_rows: list[dict[str, object]] = []
    annual_rows: list[dict[str, object]] = []
    for year in years:
        for dataset in datasets:
            board = load_light_board(cache, dataset=dataset, year=int(year))
            candidates = supported_candidates(len(board.leagues))
            if max_only and candidates:
                candidates = candidates[-1:]
            for metric in (item for item in LIGHT_METRICS if item.dataset == dataset):
                for position in positions:
                    results = []
                    kwargs = dict(
                        numerator=metric.numerator,
                        denominator=metric.denominator,
                        support=metric.support,
                        pool_support=metric.pool_support,
                        minimum_pool_rate=metric.minimum_pool_rate,
                        position=position,
                        direction=metric.direction,
                    )
                    for candidate in candidates:
                        result = evaluate_candidate_batched(
                            board.leagues,
                            n=candidate,
                            reps=reps,
                            seed=seed + year * 10_000 + candidate,
                            board_builder=lambda samples, args=kwargs: board.batched_top10(samples, **args),
                        )
                        results.append(result)
                        candidate_rows.append(
                            {
                                "dataset": dataset, "grain": metric.grain,
                                "metric": metric.metric, "position": position,
                                "pool": "all_gated_formats", "year": year,
                                "population": len(board.leagues), **asdict(result),
                            }
                        )
                        pd.DataFrame(candidate_rows).to_csv(out_dir / "candidate_evidence.csv", index=False)
                    threshold = select_annual_threshold(results)
                    annual_rows.append(
                        {
                            "dataset": dataset, "grain": metric.grain,
                            "metric": metric.metric, "position": position,
                            "pool": "all_gated_formats", "year": year,
                            "population": len(board.leagues), "state": threshold.state,
                            "min_n": threshold.min_n,
                        }
                    )
                    pd.DataFrame(annual_rows).to_csv(out_dir / "annual_evidence.csv", index=False)
    return pd.DataFrame(candidate_rows), pd.DataFrame(annual_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--datasets", nargs="+", choices=("draft", "transactions"), required=True)
    parser.add_argument("--positions", nargs="+", default=("ALL",))
    parser.add_argument("--reps", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260727)
    parser.add_argument("--max-only", action="store_true")
    args = parser.parse_args()
    run_light_study(
        args.cache, years=args.years, datasets=args.datasets, positions=args.positions,
        reps=args.reps, seed=args.seed, out_dir=args.out, max_only=args.max_only,
    )


if __name__ == "__main__":
    main()
