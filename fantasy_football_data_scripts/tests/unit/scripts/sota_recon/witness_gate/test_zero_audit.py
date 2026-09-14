from __future__ import annotations

from scripts.sota_recon.witness_gate.zero_audit import (
    DerivedExactValue,
    ZeroMeaning,
    ZeroRule,
    assess_zero,
    detect_zero_coverage_boundaries,
)


def rule(meaning: ZeroMeaning) -> ZeroRule:
    return ZeroRule(
        rule_id=f"fixture.{meaning.value}",
        atom_id="fumbles.lost",
        dataset_id="fixture.logs",
        year_start=1920,
        year_end=2025,
        meaning=meaning,
    )


def test_zero_meanings_are_not_conflated() -> None:
    assert assess_zero(0, year=1940, rule=rule(ZeroMeaning.STRUCTURAL_ZERO)).classification == "structural_zero"
    assert assess_zero(0, year=1940, rule=rule(ZeroMeaning.OBSERVED_ZERO)).classification == "observed_zero"
    assert assess_zero(0, year=1940, rule=rule(ZeroMeaning.MISSING_AS_ZERO)).classification == "suspicious_missing"
    assert assess_zero(0, year=1940, rule=rule(ZeroMeaning.INAPPLICABLE)).classification == "inapplicable"


def test_zero_that_contradicts_derived_lower_bound_fails() -> None:
    result = assess_zero(
        0,
        year=1985,
        rule=rule(ZeroMeaning.OBSERVED_ZERO),
        derived_lower_bound=1,
        proof_root="proof:lower-bound",
    )

    assert result.classification == "contradiction"
    assert result.findings[0].code == "ZERO_CONTRADICTS_DERIVED_BOUND"
    assert result.findings[0].severity == "fail"


def test_candidate_repair_requires_an_exact_acyclic_proof() -> None:
    exact = assess_zero(
        0,
        year=1985,
        rule=rule(ZeroMeaning.MISSING_AS_ZERO),
        derived_exact=DerivedExactValue(value=2, proof_root="proof:exact", is_acyclic=True),
    )
    circular = assess_zero(
        0,
        year=1985,
        rule=rule(ZeroMeaning.MISSING_AS_ZERO),
        derived_exact=DerivedExactValue(value=2, proof_root="proof:circular", is_acyclic=False),
    )

    assert exact.candidate_value == 2
    assert exact.candidate_proof_root == "proof:exact"
    assert circular.candidate_value is None
    assert any(item.code == "DERIVED_REPAIR_PROOF_INVALID" for item in circular.findings)


def test_pre_1978_fumbles_lost_disappearance_is_a_hard_boundary_failure() -> None:
    findings = detect_zero_coverage_boundaries(
        atom_id="fumbles.lost",
        dataset_id="fixture.logs",
        yearly_values={
            1976: [0, 0, 0],
            1977: [0, 0, 0],
            1978: [0, 1, 0],
            1979: [0, 0, 2],
        },
    )

    assert len(findings) == 1
    assert findings[0].code == "ZERO_COVERAGE_DISCONTINUITY"
    assert findings[0].severity == "fail"
    assert findings[0].scope["last_all_zero_year"] == 1977


def test_uniformly_sparse_but_observed_series_does_not_false_alarm() -> None:
    findings = detect_zero_coverage_boundaries(
        atom_id="fumbles.lost",
        dataset_id="fixture.logs",
        yearly_values={1976: [0, 1, 0], 1977: [0, 0, 0], 1978: [0, 1, 0]},
    )

    assert findings == ()
