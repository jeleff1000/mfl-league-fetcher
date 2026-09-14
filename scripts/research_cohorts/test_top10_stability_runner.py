from __future__ import annotations

import pytest

from top10_stability_runner import (
    annual_candidate_grid,
    CandidateResult,
    evaluate_annual_threshold,
    evaluate_candidate,
    evaluate_candidate_batched,
    paired_draws,
    select_annual_threshold,
)


def _candidate(n: int, passes: bool) -> CandidateResult:
    return CandidateResult(
        n=n,
        total_trials=500,
        valid_trials=500,
        invalid_trials=0,
        median_rho=0.95 if passes else 0.70,
        p10_rho=0.90 if passes else 0.60,
        pass_share=0.97 if passes else 0.50,
        pass_share_lower_95=0.95 if passes else 0.46,
        passes=passes,
    )


def test_paired_draws_are_disjoint_and_reproducible() -> None:
    first = list(paired_draws(tuple(range(40)), n=10, reps=25, seed=11))
    second = list(paired_draws(tuple(range(40)), n=10, reps=25, seed=11))

    assert first == second
    assert all(set(left).isdisjoint(right) for left, right in first)
    assert all(len(left) == len(right) == 10 for left, right in first)


def test_paired_draws_reject_sample_larger_than_half_population() -> None:
    with pytest.raises(ValueError, match="two disjoint samples"):
        list(paired_draws(tuple(range(19)), n=10, reps=1, seed=11))


def test_annual_grid_includes_exact_disjoint_ceiling() -> None:
    assert annual_candidate_grid(81)[-2:] == (35, 40)
    assert annual_candidate_grid(9) == ()


def test_exact_refinement_finds_first_passing_integer() -> None:
    evaluated: list[int] = []

    def evaluate(n: int) -> CandidateResult:
        evaluated.append(n)
        return _candidate(n, n >= 31)

    threshold = evaluate_annual_threshold(100, evaluate, refinement_window=5)

    assert threshold.state == "stable"
    assert threshold.min_n == 31
    assert {25, 35, 50}.issubset(evaluated)
    assert {30, 31}.issubset(evaluated)


def test_refinement_keeps_nonmonotonic_board_structurally_unstable() -> None:
    def evaluate(n: int) -> CandidateResult:
        return _candidate(n, n == 25)

    threshold = evaluate_annual_threshold(100, evaluate)

    assert threshold.state == "structurally_unstable"
    assert threshold.min_n is None


def test_batched_candidate_matches_scalar_candidate() -> None:
    population = tuple(range(40))

    def scalar(sample):
        return tuple(f"p{value:02d}" for value in sorted(sample)[:10])

    def batched(samples):
        return tuple(scalar(sample) for sample in samples)

    scalar_result = evaluate_candidate(
        population, n=10, reps=25, seed=17, board_builder=scalar
    )
    batch_result = evaluate_candidate_batched(
        population, n=10, reps=25, seed=17, board_builder=batched
    )

    assert batch_result == scalar_result


def test_candidate_with_identical_boards_passes_every_gate() -> None:
    population = tuple(f"l{i}" for i in range(40))
    board = tuple(f"p{i}" for i in range(10))

    result = evaluate_candidate(
        population,
        n=10,
        reps=500,
        seed=11,
        board_builder=lambda _: board,
    )

    assert result.median_rho == pytest.approx(1.0)
    assert result.p10_rho == pytest.approx(1.0)
    assert result.pass_share == pytest.approx(1.0)
    assert result.pass_share_lower_95 >= 0.99
    assert result.passes is True


def test_invalid_trials_count_as_failures_in_pass_share_and_p10() -> None:
    population = tuple(f"l{i}" for i in range(40))
    calls = 0

    def board_builder(_: tuple[str, ...]) -> tuple[str, ...]:
        nonlocal calls
        calls += 1
        return tuple(f"p{i}" for i in range(9 if calls % 10 == 0 else 10))

    result = evaluate_candidate(
        population,
        n=10,
        reps=100,
        seed=11,
        board_builder=board_builder,
    )

    assert result.invalid_trials > 0
    assert result.pass_share < 0.90
    assert result.p10_rho < 0.85
    assert result.passes is False


def test_first_passing_suffix_is_the_authoritative_minimum() -> None:
    result = select_annual_threshold(
        [_candidate(25, True), _candidate(35, False), _candidate(50, True), _candidate(75, True)]
    )

    assert result.state == "stable"
    assert result.min_n == 50


def test_isolated_crossing_that_fails_at_larger_n_is_structurally_unstable() -> None:
    result = select_annual_threshold([_candidate(25, True), _candidate(35, False)])

    assert result.state == "structurally_unstable"
    assert result.min_n is None


def test_no_crossing_in_available_population_is_right_censored() -> None:
    result = select_annual_threshold([_candidate(25, False), _candidate(35, False)])

    assert result.state == "right_censored"
    assert result.min_n is None


def test_passing_at_smallest_tested_size_is_left_censored() -> None:
    result = select_annual_threshold([_candidate(5, True), _candidate(10, True)])

    assert result.state == "left_censored"
    assert result.min_n == 5
