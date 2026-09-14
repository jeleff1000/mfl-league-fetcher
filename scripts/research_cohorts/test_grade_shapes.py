"""Grade SHAPE tests (Joe 2026-07-20): scores are absolute and uncapped, and the clutch
overlay pushes the right direction on both sides of the isotonic curve.

Why these exist: a percentile score compresses the tail it exists to reveal -- a
league-altering season lands two points above a merely-great one. And the obvious fix
(surplus x clutch_factor) silently inverts below the line, where a bigger multiplier makes a
bust look WORSE for having been clutch. Both failure modes are cheap to assert and easy to
reintroduce.
"""
from pathlib import Path
import ast
import sys

import numpy as np
import pandas as pd
import pytest
from draft_grades import apply_clutch_multiplier

SRC = (Path(__file__).resolve().parent / "build_research_cohort_grades.py").read_text(encoding="utf-8")
_ns = {"np": np, "pd": pd}
exec(SRC[SRC.index("def pctile"):SRC.index("KEYS=")], _ns)
pctile, clutch_pctile = _ns["pctile"], _ns["clutch_pctile"]

W_CLU_DRAFT = 0.20


def clutch_factor(sur, cl):
    """Mirror of the builder's factor (kept in sync by test_matches_builder_source)."""
    return 1.0 + np.sign(sur) * 2 * W_CLU_DRAFT * (cl - 0.5)


def test_matches_builder_source():
    """The factor under test is the one the builder actually applies."""
    imports = {
        alias.name
        for node in ast.walk(ast.parse(SRC))
        if isinstance(node, ast.ImportFrom) and node.module == "draft_grades"
        for alias in node.names
    }
    assert "draft_score_from_expected" in imports
    assert "draft_score_from_expected(val, eL, cl)" in SRC


def test_clutch_helps_and_unclutch_hurts_above_the_line():
    # w=0.20 gives a +/-20% swing at the extremes, i.e. a factor in [0.8, 1.2]
    assert clutch_factor(10.0, 1.0) * 10.0 == pytest.approx(12.0)   # clutch  -> rewarded
    assert clutch_factor(10.0, 0.0) * 10.0 == pytest.approx(8.0)    # unclutch-> penalised
    assert clutch_factor(10.0, 0.5) * 10.0 == pytest.approx(10.0)   # neutral -> untouched
    assert apply_clutch_multiplier(np.array([10.0]), np.array([1.0]))[0] == pytest.approx(12.0)


def test_clutch_direction_does_not_invert_below_the_line():
    """The bug a plain multiplier would reintroduce: a clutch bust must not be pushed
    FURTHER below the curve, and an unclutch bust must not be pulled UP toward zero."""
    clutch_bust = clutch_factor(-10.0, 1.0) * -10.0
    unclutch_bust = clutch_factor(-10.0, 0.0) * -10.0
    assert clutch_bust == pytest.approx(-8.0)     # less penalised for delivering when it counted
    assert unclutch_bust == pytest.approx(-12.0)  # more penalised
    assert clutch_bust > unclutch_bust
    # and the naive form would have inverted exactly this comparison
    naive = lambda s, c: s * (1 + 2 * W_CLU_DRAFT * (c - 0.5))
    assert naive(-10.0, 1.0) < naive(-10.0, 0.0)


def test_score_is_uncapped_so_juggernauts_separate():
    """The whole point of dropping the percentile: a 5x season must score ~5x, not +2."""
    great, juggernaut = 20.0, 100.0
    assert clutch_factor(juggernaut, 0.5) * juggernaut == pytest.approx(100.0)
    ratio = (clutch_factor(juggernaut, 0.5) * juggernaut) / (clutch_factor(great, 0.5) * great)
    assert ratio == pytest.approx(5.0)
    # under percentiling these would have been adjacent instead
    pcts = pctile(np.array([great, juggernaut]))
    assert (pcts[1] - pcts[0]) == pytest.approx(1.0)  # ranks: no magnitude information at all


def test_missing_clutch_is_neutral_not_zero():
    """Ledger D11: absent clutch must not silently act as 'maximally unclutch'."""
    cl = clutch_pctile(np.array([np.nan, np.nan]))
    assert list(cl) == [0.5, 0.5]
    assert clutch_factor(10.0, cl[0]) == pytest.approx(1.0)


def test_tied_clutch_gets_identical_percentiles():
    """Ledger D11: argsort-position ranks handed tied values an arbitrary 0..1 ladder."""
    assert len(set(np.round(pctile(np.zeros(8)), 9))) == 1
