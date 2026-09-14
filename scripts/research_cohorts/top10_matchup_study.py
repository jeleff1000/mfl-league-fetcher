"""Run one durable annual Matchup ordered-top-10 stability board."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence
import zlib

import duckdb
import pandas as pd

from top10_evidence_checkpoint import CandidateCheckpoint
from top10_matchup_matrix import MatchupSparseBoard
from top10_stability_runner import evaluate_annual_threshold, evaluate_candidate_batched
from top10_stability_source import (
    cohort_member_from_slug,
    load_approved_pools,
    pool_for_base_cohort,
)


def league_members_from_population(
    paths: Sequence[Path | str],
) -> dict[str, str]:
    """Read one stable four-dimension cohort member for every annual league."""
    if not paths:
        raise ValueError("population paths cannot be empty")
    con = duckdb.connect()
    con.from_parquet([str(path) for path in paths]).create_view("population")
    frame = con.execute(
        """
        SELECT CAST(db_name AS VARCHAR) AS db_name,
               MAX(teams)||'/'||MAX(roster)||'/'||MAX(ppr)||'/'||MAX(td) AS member,
               COUNT(DISTINCT teams||'/'||roster||'/'||ppr||'/'||td) AS n_members
        FROM population
        GROUP BY db_name
        ORDER BY db_name
        """
    ).fetchdf()
    con.close()
    if (frame["n_members"] != 1).any():
        bad = frame.loc[frame["n_members"] != 1, "db_name"].head().tolist()
        raise ValueError(f"leagues have multiple cohort members: {bad}")
    return dict(zip(frame["db_name"].astype(str), frame["member"].astype(str)))


def eligible_leagues_from_population(
    paths: Sequence[Path | str], position: str
) -> set[str]:
    """Return annual leagues eligible to start the requested player position."""
    if not paths:
        raise ValueError("population paths cannot be empty")
    selected = str(position).upper()
    if selected == "ALL":
        predicate = "TRUE"
    elif selected == "K":
        predicate = "COALESCE(k_eligible,0)>0"
    elif selected == "DEF":
        predicate = "COALESCE(def_eligible,0)>0"
    elif selected in {"DL", "LB", "DB"}:
        predicate = "roster='idp' AND COALESCE(skill_eligible,0)>0"
    else:
        predicate = "COALESCE(skill_eligible,0)>0"
    con = duckdb.connect()
    con.from_parquet([str(path) for path in paths]).create_view("population")
    rows = con.execute(
        f"SELECT DISTINCT CAST(db_name AS VARCHAR) FROM population WHERE {predicate}"
    ).fetchall()
    con.close()
    return {str(row[0]) for row in rows}


def _cluster_metrics(path: Path) -> set[str]:
    frame = pd.read_csv(path, usecols=["metric"])
    metrics = set(frame["metric"].dropna().astype(str))
    if not metrics:
        raise ValueError("cluster map has no metrics")
    return metrics


def run_board(
    *,
    weekly_paths: Sequence[Path],
    population_paths: Sequence[Path],
    active_paths: Sequence[Path],
    cluster_map: Path,
    year: int,
    grain: str,
    metric: str,
    cluster_metric: str,
    position: str,
    base_cohort: str,
    reps: int,
    seed: int,
    out_dir: Path,
    week: int | None = None,
    direct_values: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Load, filter, and durably evaluate one annual board."""
    if reps < 500:
        raise ValueError("authoritative studies require at least 500 repetitions")
    selected_position = position.upper()
    base_member = cohort_member_from_slug(base_cohort)
    metrics = _cluster_metrics(cluster_map)
    if selected_position == "ALL" or cluster_metric == "exact":
        members = (base_member,)
    else:
        pools = load_approved_pools(cluster_map, valid_metrics=metrics)
        members = pool_for_base_cohort(
            pools, cluster_metric, selected_position, base_member
        ).members

    board = MatchupSparseBoard.from_parquet_shards(
        weekly_paths, population_paths, active_paths
    )
    if direct_values is not None:
        board = board.with_direct_values(direct_values)
    member_by_league = league_members_from_population(population_paths)
    position_eligible = eligible_leagues_from_population(
        population_paths, selected_position
    )
    allowed = set(members)
    leagues = tuple(
        league
        for league in board.leagues
        if league in position_eligible and member_by_league.get(league) in allowed
    )
    if len(leagues) < 10:
        raise ValueError(
            f"approved pool has only {len(leagues)} leagues for {metric}/{selected_position}"
        )
    board = board.subset_leagues(leagues)
    identity = {
        "dataset": "matchup",
        "grain": grain,
        "metric": metric,
        "cluster_metric": cluster_metric,
        "position": selected_position,
        "base_cohort": base_cohort,
        "members": members,
        "year": int(year),
        "week": week,
        "population": len(leagues),
        "repetitions": int(reps),
        "seed": int(seed),
    }
    checkpoint = CandidateCheckpoint(out_dir / "candidate_evidence.csv", identity)
    salt = zlib.crc32(
        "|".join(
            [grain, metric, cluster_metric, selected_position, base_cohort, str(week)]
        ).encode()
    )

    def evaluate(n: int):
        return checkpoint.measure(
            n,
            lambda selected: evaluate_candidate_batched(
                board.leagues,
                n=selected,
                reps=reps,
                seed=seed + int(year) * 100_000 + salt + selected,
                board_builder=lambda samples: board.batched_top10(
                    samples,
                    grain=grain,
                    metric=metric,
                    position=selected_position,
                    week=week,
                ),
            ),
        )

    threshold = evaluate_annual_threshold(len(board.leagues), evaluate)
    annual = {
        **identity,
        "members": "|".join(members),
        "state": threshold.state,
        "min_n": threshold.min_n,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([annual]).to_csv(out_dir / "annual_evidence.csv", index=False)
    return annual


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weekly", nargs="+", type=Path, required=True)
    parser.add_argument("--population", nargs="+", type=Path, required=True)
    parser.add_argument("--active", nargs="+", type=Path, required=True)
    parser.add_argument("--cluster-map", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--grain", choices=("weekly", "season"), required=True)
    parser.add_argument("--metric", required=True)
    parser.add_argument("--cluster-metric", required=True)
    parser.add_argument("--position", required=True)
    parser.add_argument("--base-cohort", default="12t_flx_ppr_4pt")
    parser.add_argument("--week", type=int)
    parser.add_argument("--reps", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260727)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = run_board(
        weekly_paths=args.weekly,
        population_paths=args.population,
        active_paths=args.active,
        cluster_map=args.cluster_map,
        year=args.year,
        grain=args.grain,
        metric=args.metric,
        cluster_metric=args.cluster_metric,
        position=args.position,
        base_cohort=args.base_cohort,
        week=args.week,
        reps=args.reps,
        seed=args.seed,
        out_dir=args.out,
    )
    print(result)


if __name__ == "__main__":
    main()
