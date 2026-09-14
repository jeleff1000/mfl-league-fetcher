from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.newspaper_recon import NewspaperAtomCandidate, reconcile_to_fixed_point


def atom(
    atom_id: str,
    *,
    dependencies: tuple[str, ...] = (),
    confidence: float = 0.95,
    pfr_collision: bool = False,
    identity_status: str = "resolved",
    outcome: str = "corroborating",
) -> NewspaperAtomCandidate:
    return NewspaperAtomCandidate(
        candidate_id=f"candidate:{atom_id}",
        atom_id=atom_id,
        value=1,
        dependencies=dependencies,
        leaf_evidence=(f"leaf:newspaper:{atom_id}", "leaf:witness:fixture"),
        confidence=confidence,
        minimum_confidence=0.9,
        identity_status=identity_status,
        admissibility_outcome=outcome,
        pfr_collision=pfr_collision,
    )


def test_newspaper_recon_repeats_until_no_new_facts_unlock() -> None:
    candidates = (
        atom("a"),
        atom("b", dependencies=("a",)),
        atom("c", dependencies=("b",)),
    )

    result = reconcile_to_fixed_point(candidates, initial_facts={})

    assert result.iterations == 4
    assert result.promoted_atom_ids == ("a", "b", "c")
    assert [item.iteration for item in result.unlock_ledger] == [1, 2, 3]


def test_recon_keeps_collision_identity_and_confidence_lanes_separate() -> None:
    candidates = (
        atom("good"),
        atom("pfr", pfr_collision=True),
        atom("low", confidence=0.4),
        atom("identity", identity_status="collision_review"),
    )

    result = reconcile_to_fixed_point(candidates, initial_facts={})

    assert result.counts == {
        "promoted": 1,
        "fully_vetted": 0,
        "pfr_collision_review": 1,
        "identity_review": 1,
        "below_confidence": 1,
        "dependency_hold": 0,
        "inadmissible": 0,
    }


def test_existing_fact_is_fully_vetted_not_repromoted() -> None:
    result = reconcile_to_fixed_point((atom("known"),), initial_facts={"known": 1})

    assert result.counts["fully_vetted"] == 1
    assert result.counts["promoted"] == 0
    assert result.unlock_ledger == ()


def test_candidate_dependency_cycle_and_self_evidence_are_rejected() -> None:
    with pytest.raises(ValueError, match="newspaper candidate cycle"):
        reconcile_to_fixed_point(
            (atom("a", dependencies=("b",)), atom("b", dependencies=("a",))),
            initial_facts={},
        )

    bad = NewspaperAtomCandidate(
        candidate_id="candidate:a",
        atom_id="a",
        value=1,
        dependencies=(),
        leaf_evidence=("candidate:a",),
        confidence=1,
        minimum_confidence=0.9,
        identity_status="resolved",
        admissibility_outcome="authoritative",
        pfr_collision=False,
    )
    with pytest.raises(ValueError, match="leaf evidence"):
        reconcile_to_fixed_point((bad,), initial_facts={})
