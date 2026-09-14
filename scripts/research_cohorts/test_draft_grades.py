from __future__ import annotations

import numpy as np
import pytest

import pandas as pd

from draft_grades import apply_clutch_multiplier, draft_score_from_expected, select_grade_lamar


def test_draft_score_is_lamar_distance_from_isotonic_expectation() -> None:
    actual = np.array([110.0, 40.0, -5.0])
    expected = np.array([90.0, 45.0, 5.0])

    score = draft_score_from_expected(actual, expected, np.array([0.5, 0.5, 0.5]))

    np.testing.assert_allclose(score, np.array([20.0, -5.0, -10.0]))


def test_clutch_multiplier_is_sign_aware_and_bounded_to_twenty_percent() -> None:
    surplus = np.array([10.0, -10.0, 10.0, -10.0])
    clutch_percentile = np.array([1.0, 1.0, 0.0, 0.0])

    adjusted = apply_clutch_multiplier(surplus, clutch_percentile)

    np.testing.assert_allclose(adjusted, np.array([12.0, -8.0, 8.0, -12.0]))


def test_neutral_clutch_does_not_change_score() -> None:
    adjusted = apply_clutch_multiplier(np.array([17.0, -9.0]), np.array([0.5, 0.5]))

    assert adjusted.tolist() == pytest.approx([17.0, -9.0])


def test_exact_flex_cohort_grades_use_canonical_slug_lamar() -> None:
    frame = pd.DataFrame({
        "teams": ["12t", "ALL"],
        "roster": ["flx", "ALL"],
        "ppr": ["ppr", "ALL"],
        "td": ["4pt", "ALL"],
        "avg_manager_lamar": [220.8, 180.0],
        "canonical_lamar_12t_flx_ppr_4pt": [179.9, 179.9],
    })

    selected = select_grade_lamar(frame)

    assert selected.tolist() == pytest.approx([179.9, 180.0])
