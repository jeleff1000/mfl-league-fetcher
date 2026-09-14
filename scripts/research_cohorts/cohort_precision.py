"""Precision planning for proportion metrics over clustered weekly observations."""
from __future__ import annotations

import math
from statistics import NormalDist


def decile_band(target_pct: int) -> tuple[float, float]:
    """Return the rate interval used to estimate a target decile's rho."""
    if target_pct not in range(10, 101, 10):
        raise ValueError("target_pct must be a decile from 10 through 100")
    return max(0.0, (target_pct - 5) / 100.0), min(1.0, (target_pct + 5) / 100.0)


def z_for_confidence(confidence: float) -> float:
    """Two-sided normal critical value for a confidence level in (0, 1)."""
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    return NormalDist().inv_cdf((1.0 + confidence) / 2.0)


def independent_observations(rate: float, margin: float, confidence: float = 0.85) -> int:
    """Planning n for an absolute proportion error margin.

    This is the normal-approximation planning formula.  ``rate`` is the target
    proportion, ``margin`` is an absolute proportion (0.05 = five percentage
    points), and the result is rounded up.
    """
    if not 0 <= rate <= 1:
        raise ValueError("rate must be between 0 and 1")
    if not 0 < margin < 1:
        raise ValueError("margin must be strictly between 0 and 1")
    # The normal planning variance is zero at 0% and 100%, so it cannot be
    # used for the boundary deciles.  Use the exact all-success/all-failure
    # bound, retaining the two-sided confidence convention (alpha/2).
    if rate in (0, 1):
        alpha = (1.0 - confidence) / 2.0
        return math.ceil(math.log(alpha) / math.log(1.0 - margin))
    z = z_for_confidence(confidence)
    return math.ceil((z * z * rate * (1.0 - rate)) / (margin * margin))


def design_effect(periods: int, rho: float) -> float:
    """One-way equal-period cluster design effect."""
    if periods < 1:
        raise ValueError("periods must be positive")
    if not 0 <= rho <= 1:
        raise ValueError("rho must be between 0 and 1")
    return 1.0 + (periods - 1) * rho


def season_leagues(
    weekly_observations: int,
    periods: int,
    rho: float,
) -> int:
    """Convert independent weekly observations into clustered season leagues."""
    if weekly_observations < 1:
        raise ValueError("weekly_observations must be positive")
    return math.ceil(weekly_observations * design_effect(periods, rho) / periods)
