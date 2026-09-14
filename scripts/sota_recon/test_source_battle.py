"""Tests for the source tournament v0 (O.5): LOROO harness spec, tolerance rule,
transposition signature, and vote-clustering reuse in the flip report's naive arm.

Run:  python -m pytest scripts/sota_recon/test_source_battle.py -q
"""

from __future__ import annotations

from .source_battle import _TRANSPOSE_DELTAS, _tol, loroo_consensus
from .witness_votes import _cluster


# ---------- LOROO (leave-one-root-out) ----------

def test_loroo_excludes_judged_root():
    votes = {"pfr": 100.0, "nflverse_pbp": 100.0, "newspaper": 100.0}
    cons, n, status = loroo_consensus(votes, "pfr", tol=0.5)
    assert status == "OK" and n == 2 and cons == 100.0


def test_loroo_excludes_descendants():
    votes = {"pfr": 100.0, "pfr_derived": 100.0, "newspaper": 90.0}
    cons, n, status = loroo_consensus(
        votes, "pfr", tol=0.5, descendants={"pfr": {"pfr_derived"}})
    assert n == 1 and status == "DEGENERATE_LT3_ROOTS" and cons == 90.0


def test_loroo_two_roots_is_degenerate():
    cons, n, status = loroo_consensus({"pfr": 10.0, "nflverse_pbp": 12.0}, "pfr", tol=0.5)
    assert status == "DEGENERATE_LT3_ROOTS" and n == 1 and cons == 12.0


def test_loroo_no_reference():
    cons, n, status = loroo_consensus({"pfr": 10.0}, "pfr", tol=0.5)
    assert status == "NO_REFERENCE" and cons is None and n == 0


def test_loroo_split_reference_yields_no_consensus():
    votes = {"a": 0.0, "b": 100.0, "c": 200.0, "judged": 100.0}
    cons, n, status = loroo_consensus(votes, "judged", tol=0.5)
    assert status == "NO_REFERENCE" and n == 3 and cons is None


# ---------- geometry constants ----------

def test_tolerance_is_declared_policy_not_unit_heuristic():
    """§17.1: default EXACT (float-eps only). The old ±1.5-yard / ±0.5-count lane rule is
    dead -- a ±1 disagreement between roots is a conflict, not noise. Any wider window
    must come from a receipted tolerance_policy in stat_contracts."""
    from .witness_votes import EXACT_EPS
    assert _tol("passing_yards") == EXACT_EPS
    assert _tol("sack_yards_lost") == EXACT_EPS
    assert _tol("receptions") == EXACT_EPS
    assert EXACT_EPS < 0.5  # a 1-unit integer disagreement can never pass EXACT


def test_transpose_signature_is_the_nine_multiples():
    assert _TRANSPOSE_DELTAS == (9, 18, 27, 36, 45, 54, 63, 72, 81)
    # |ab - ba| = 9|a-b|: 71 vs 17 -> 54, 92 vs 29 -> 63
    assert abs(71 - 17) in _TRANSPOSE_DELTAS
    assert abs(92 - 29) in _TRANSPOSE_DELTAS


# ---------- shared clustering (flip report naive arm) ----------

def test_cluster_verdicts():
    assert _cluster({}, 0.5)[0] == "NO-WITNESS"
    assert _cluster({"a": 5.0}, 0.5)[0] == "SINGLE"
    v, c, agree, dis = _cluster({"a": 5.0, "b": 5.0, "c": 5.0}, 0.5)
    assert (v, agree, dis) == ("UNANIMOUS", 3, 0)
    v, c, agree, dis = _cluster({"a": 5.0, "b": 5.0, "c": 9.0}, 0.5)
    assert (v, agree, dis) == ("MAJORITY", 2, 1)
    v, c, agree, dis = _cluster({"a": 5.0, "b": 9.0}, 0.5)
    assert (v, agree, dis) == ("SPLIT", 1, 1)
