"""Disjoint within-year resampling for ordered top-10 stability."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Literal, TypeVar

import numpy as np
from scipy.stats import binomtest

from top10_rank import top10_rank_rho


LeagueId = TypeVar("LeagueId")
CANDIDATE_GRID = (
    5,
    10,
    15,
    20,
    25,
    35,
    50,
    75,
    100,
    150,
    200,
    300,
    400,
    600,
    800,
    1200,
    1600,
    2400,
    3200,
    4800,
    6400,
)
ThresholdState = Literal[
    "stable",
    "left_censored",
    "right_censored",
    "structurally_unstable",
    "not_applicable",
]


@dataclass(frozen=True)
class CandidateResult:
    n: int
    total_trials: int
    valid_trials: int
    invalid_trials: int
    median_rho: float
    p10_rho: float
    pass_share: float
    pass_share_lower_95: float
    passes: bool


@dataclass(frozen=True)
class AnnualThreshold:
    state: ThresholdState
    min_n: int | None
    evidence: tuple[CandidateResult, ...]


def annual_candidate_grid(population: int) -> tuple[int, ...]:
    """Return the locked grid plus the exact two-half population ceiling."""
    ceiling = int(population) // 2
    if ceiling < CANDIDATE_GRID[0]:
        return ()
    candidates = [candidate for candidate in CANDIDATE_GRID if candidate <= ceiling]
    if ceiling not in candidates:
        candidates.append(ceiling)
    return tuple(candidates)


def paired_draws(
    population: Sequence[LeagueId],
    *,
    n: int,
    reps: int,
    seed: int,
) -> Iterator[tuple[tuple[LeagueId, ...], tuple[LeagueId, ...]]]:
    """Yield seeded pairs of disjoint samples from one annual population."""

    if n < 1:
        raise ValueError("sample size must be positive")
    if reps < 1:
        raise ValueError("repetitions must be positive")
    unique = tuple(dict.fromkeys(population))
    if len(unique) != len(population):
        raise ValueError("annual population contains duplicate league IDs")
    if 2 * n > len(unique):
        raise ValueError(
            f"population of {len(unique)} cannot support two disjoint samples of {n}"
        )
    rng = np.random.default_rng(seed)
    for _ in range(reps):
        chosen = rng.choice(len(unique), size=2 * n, replace=False)
        yield (
            tuple(unique[int(index)] for index in chosen[:n]),
            tuple(unique[int(index)] for index in chosen[n:]),
        )


def _lower_binomial_bound(successes: int, trials: int) -> float:
    if trials <= 0:
        return 0.0
    return float(
        binomtest(successes, trials, alternative="greater")
        .proportion_ci(confidence_level=0.95, method="exact")
        .low
    )


def evaluate_candidate(
    population: Sequence[LeagueId],
    *,
    n: int,
    reps: int,
    seed: int,
    board_builder: Callable[[tuple[LeagueId, ...]], Sequence[str]],
    rho_target: float = 0.85,
    required_share: float = 0.90,
    absent_rank: int = 11,
) -> CandidateResult:
    """Evaluate one annual candidate size against every statistical gate."""

    rhos: list[float] = []
    invalid = 0
    successes = 0
    for left_leagues, right_leagues in paired_draws(
        population, n=n, reps=reps, seed=seed
    ):
        left = tuple(board_builder(left_leagues))
        right = tuple(board_builder(right_leagues))
        if len(left) < 10 or len(right) < 10:
            invalid += 1
            rhos.append(-1.0)
            continue
        rho = top10_rank_rho(left[:10], right[:10], absent_rank=absent_rank)
        if not np.isfinite(rho):
            invalid += 1
            rhos.append(-1.0)
            continue
        rhos.append(float(rho))
        if rho >= rho_target:
            successes += 1

    total = len(rhos)
    valid = total - invalid
    median = float(np.median(rhos)) if rhos else -1.0
    p10 = float(np.quantile(rhos, 0.10)) if rhos else -1.0
    pass_share = successes / total if total else 0.0
    lower = _lower_binomial_bound(successes, total)
    passes = (
        total == reps
        and p10 >= rho_target
        and pass_share >= required_share
        and lower >= required_share
    )
    return CandidateResult(
        n=n,
        total_trials=total,
        valid_trials=valid,
        invalid_trials=invalid,
        median_rho=median,
        p10_rho=p10,
        pass_share=pass_share,
        pass_share_lower_95=lower,
        passes=passes,
    )


def evaluate_candidate_batched(
    population: Sequence[LeagueId],
    *,
    n: int,
    reps: int,
    seed: int,
    board_builder: Callable[[Sequence[tuple[LeagueId, ...]]], Sequence[Sequence[str]]],
    rho_target: float = 0.85,
    required_share: float = 0.90,
    absent_rank: int = 11,
) -> CandidateResult:
    """Evaluate a candidate after rebuilding all 2*reps boards in one batch."""
    draws = tuple(paired_draws(population, n=n, reps=reps, seed=seed))
    samples = tuple(sample for pair in draws for sample in pair)
    boards = tuple(tuple(board) for board in board_builder(samples))
    if len(boards) != 2 * reps:
        raise ValueError("batched board builder returned the wrong number of boards")

    rhos: list[float] = []
    invalid = 0
    successes = 0
    for index in range(reps):
        left, right = boards[2 * index], boards[2 * index + 1]
        if len(left) < 10 or len(right) < 10:
            invalid += 1
            rhos.append(-1.0)
            continue
        rho = top10_rank_rho(left[:10], right[:10], absent_rank=absent_rank)
        if not np.isfinite(rho):
            invalid += 1
            rhos.append(-1.0)
            continue
        rhos.append(float(rho))
        successes += int(rho >= rho_target)
    total = len(rhos)
    median = float(np.median(rhos)) if rhos else -1.0
    p10 = float(np.quantile(rhos, 0.10)) if rhos else -1.0
    share = successes / total if total else 0.0
    lower = _lower_binomial_bound(successes, total)
    return CandidateResult(
        n=n,
        total_trials=total,
        valid_trials=total - invalid,
        invalid_trials=invalid,
        median_rho=median,
        p10_rho=p10,
        pass_share=share,
        pass_share_lower_95=lower,
        passes=(
            total == reps
            and p10 >= rho_target
            and share >= required_share
            and lower >= required_share
        ),
    )


def select_annual_threshold(results: Sequence[CandidateResult]) -> AnnualThreshold:
    """Select the first passing size whose entire tested suffix also passes."""

    ordered = tuple(sorted(results, key=lambda result: result.n))
    if not ordered:
        return AnnualThreshold(state="not_applicable", min_n=None, evidence=ordered)
    for index, result in enumerate(ordered):
        if result.passes and all(candidate.passes for candidate in ordered[index:]):
            state: ThresholdState = "left_censored" if index == 0 else "stable"
            return AnnualThreshold(state=state, min_n=result.n, evidence=ordered)
    state = "structurally_unstable" if any(result.passes for result in ordered) else "right_censored"
    return AnnualThreshold(state=state, min_n=None, evidence=ordered)


def evaluate_annual_threshold(
    population: int,
    evaluate: Callable[[int], CandidateResult],
    *,
    refinement_window: int = 10,
) -> AnnualThreshold:
    """Evaluate the full grid and refine a stable crossing to an exact integer.

    Grid points above the crossing remain in the evidence, so an isolated noisy
    pass can never become the reported minimum. Binary narrowing limits the
    expensive part of the run; every integer in the final bracket is tested.
    """
    if refinement_window < 1:
        raise ValueError("refinement_window must be positive")
    results: dict[int, CandidateResult] = {}

    def measured(n: int) -> CandidateResult:
        if n not in results:
            result = evaluate(n)
            if int(result.n) != int(n):
                raise ValueError(
                    f"candidate evaluator returned n={result.n} for requested n={n}"
                )
            results[n] = result
        return results[n]

    grid = annual_candidate_grid(population)
    for candidate in grid:
        measured(candidate)
    threshold = select_annual_threshold(tuple(results.values()))
    if threshold.state != "stable" or threshold.min_n is None:
        return threshold

    upper = int(threshold.min_n)
    lower_candidates = [candidate for candidate in grid if candidate < upper]
    if not lower_candidates:
        return threshold
    lower = max(lower_candidates)
    while upper - lower > refinement_window:
        midpoint = (lower + upper) // 2
        if measured(midpoint).passes:
            upper = midpoint
        else:
            lower = midpoint
    for candidate in range(lower + 1, upper + 1):
        measured(candidate)
    return select_annual_threshold(tuple(results.values()))
