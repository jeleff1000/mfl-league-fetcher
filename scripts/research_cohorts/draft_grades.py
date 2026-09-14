"""Pure draft-grade calculations shared by the research cohort builder and tests."""
from __future__ import annotations

import numpy as np
import pandas as pd

CLUTCH_WEIGHT = 0.20
LAMAR_SLUGS = [
    f"{teams}_flx_{ppr}_{td}"
    for teams in ("10t", "12t")
    for ppr in ("std", "half", "ppr")
    for td in ("4pt", "6pt")
]


def select_grade_lamar(frame: pd.DataFrame) -> pd.Series:
    """Use super-table season LAMAR for exact flex cohorts; retain observed value on coarse rows."""
    selected = frame["avg_manager_lamar"].copy()
    for slug in LAMAR_SLUGS:
        teams, roster, ppr, td = slug.split("_")
        canonical = f"canonical_lamar_{slug}"
        if canonical not in frame.columns:
            continue
        exact = (
            frame["teams"].eq(teams)
            & frame["roster"].eq(roster)
            & frame["ppr"].eq(ppr)
            & frame["td"].eq(td)
        )
        selected.loc[exact] = frame.loc[exact, canonical]
    return selected


def apply_clutch_multiplier(
    surplus: np.ndarray,
    clutch_percentile: np.ndarray,
) -> np.ndarray:
    """Apply the sign-aware +/-20% clutch adjustment to LAMAR surplus."""
    surplus_values = np.asarray(surplus, dtype=float)
    clutch_values = np.asarray(clutch_percentile, dtype=float)
    factor = 1.0 + np.sign(surplus_values) * 2.0 * CLUTCH_WEIGHT * (clutch_values - 0.5)
    return surplus_values * factor


def draft_score_from_expected(
    actual_lamar: np.ndarray,
    expected_lamar: np.ndarray,
    clutch_percentile: np.ndarray,
) -> np.ndarray:
    """Return uncapped isotonic LAMAR distance after the clutch adjustment."""
    surplus = np.asarray(actual_lamar, dtype=float) - np.asarray(expected_lamar, dtype=float)
    return apply_clutch_multiplier(surplus, clutch_percentile)
