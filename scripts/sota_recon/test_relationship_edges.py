"""Contract tests for the O.6 relationship-edge contract (§19.3 / §18 regen-diff law).

Run:  python -m pytest scripts/sota_recon/test_relationship_edges.py -q
"""

from __future__ import annotations

import os

import pytest

from . import relationship_edges as RE

pytestmark = pytest.mark.skipif(
    not os.path.exists(RE.CONTRACT_PATH) or not os.path.exists(RE.VERDICTS_PATH),
    reason="relationship edge contract not yet generated")


def test_committed_contract_matches_generator():
    """§18 regen-diff law: the committed contract is exactly what the committed
    composer produces from the committed verdict receipts."""
    assert RE.load() == RE.generate()


def test_contract_validates_clean():
    assert RE.validate(RE.load()) == []


def test_verdict_vocabulary_is_closed():
    for e in RE.load()["edges"]:
        assert e["verdict"] in RE.VERDICT_VOCAB, e["edge_id"]


def test_refuted_edges_carry_counterexample_or_receipt_pointer():
    """REFUTED = do-not-enforce + counterexample (handoff verdict law). Every
    refuted edge from a data run carries at least one concrete counterexample."""
    for e in RE.load()["edges"]:
        if e["verdict"] == "REFUTED":
            assert e.get("do_not_enforce") is True, e["edge_id"]
            assert e.get("counterexample"), e["edge_id"]


def test_undecidable_states_deficit():
    for e in RE.load()["edges"]:
        if e["verdict"] in ("UNDECIDABLE", "PENDING_TEST"):
            assert e.get("deficit"), e["edge_id"]


def test_newspaper_edges_stay_escalated():
    for e in RE.load()["edges"]:
        crosses = any("newspaper" in r for r in e["rhs"]) or "newspaper" in e.get("note", "")
        if crosses:
            assert e["verdict"] == "ESCALATED", e["edge_id"]
            assert not e["enforceable"], e["edge_id"]


def test_enforceable_requires_era_scope():
    for e in RE.enforceable_edges():
        assert e["era_scope"], e["edge_id"]


def test_counts_match_edges():
    doc = RE.load()
    c = doc["counts"]
    assert c["edges"] == len(doc["edges"])
    assert c["enforceable"] == sum(1 for e in doc["edges"] if e["enforceable"])
    assert c["do_not_enforce"] == sum(1 for e in doc["edges"] if e.get("do_not_enforce"))
