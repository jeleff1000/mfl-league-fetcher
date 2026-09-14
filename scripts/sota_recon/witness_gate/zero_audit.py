from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from .gate_planes import Finding


class ZeroMeaning(StrEnum):
    STRUCTURAL_ZERO = "structural_zero"
    OBSERVED_ZERO = "observed_zero"
    MISSING_AS_ZERO = "missing_as_zero"
    INAPPLICABLE = "inapplicable"


@dataclass(frozen=True)
class ZeroRule:
    rule_id: str
    atom_id: str
    dataset_id: str
    year_start: int
    year_end: int
    meaning: ZeroMeaning


@dataclass(frozen=True)
class DerivedExactValue:
    value: float
    proof_root: str
    is_acyclic: bool


@dataclass(frozen=True)
class ZeroAssessment:
    classification: str
    findings: tuple[Finding, ...]
    candidate_value: float | None = None
    candidate_proof_root: str | None = None


_CLASSIFICATION = {
    ZeroMeaning.STRUCTURAL_ZERO: "structural_zero",
    ZeroMeaning.OBSERVED_ZERO: "observed_zero",
    ZeroMeaning.MISSING_AS_ZERO: "suspicious_missing",
    ZeroMeaning.INAPPLICABLE: "inapplicable",
}


def assess_zero(
    value: float | None,
    *,
    year: int,
    rule: ZeroRule,
    derived_lower_bound: float | None = None,
    proof_root: str | None = None,
    derived_exact: DerivedExactValue | None = None,
) -> ZeroAssessment:
    if not rule.year_start <= year <= rule.year_end:
        raise ValueError(f"zero rule {rule.rule_id} does not cover year {year}")
    if value is None:
        return ZeroAssessment("missing", ())
    if value != 0:
        return ZeroAssessment("observed_nonzero", ())

    findings: list[Finding] = []
    if derived_lower_bound is not None and derived_lower_bound > 0:
        findings.append(
            Finding(
                code="ZERO_CONTRADICTS_DERIVED_BOUND",
                severity="fail",
                scope={
                    "atom_id": rule.atom_id,
                    "dataset_id": rule.dataset_id,
                    "year": year,
                    "derived_lower_bound": derived_lower_bound,
                },
                evidence_refs=(proof_root,) if proof_root else (),
                remediation="repair the zero or correct the versioned derivation contract",
            )
        )
        return ZeroAssessment("contradiction", tuple(findings))

    candidate_value: float | None = None
    candidate_proof_root: str | None = None
    if derived_exact is not None and derived_exact.value != value:
        if derived_exact.is_acyclic:
            candidate_value = derived_exact.value
            candidate_proof_root = derived_exact.proof_root
        else:
            findings.append(
                Finding(
                    code="DERIVED_REPAIR_PROOF_INVALID",
                    severity="fail",
                    scope={"atom_id": rule.atom_id, "dataset_id": rule.dataset_id, "year": year},
                    evidence_refs=(derived_exact.proof_root,),
                    remediation="remove the circular dependency and rebuild the proof DAG",
                )
            )
    return ZeroAssessment(
        classification=_CLASSIFICATION[rule.meaning],
        findings=tuple(findings),
        candidate_value=candidate_value,
        candidate_proof_root=candidate_proof_root,
    )


def _has_nonzero(values: Iterable[float | None]) -> bool:
    return any(value not in {None, 0} for value in values)


def detect_zero_coverage_boundaries(
    *,
    atom_id: str,
    dataset_id: str,
    yearly_values: dict[int, list[float | None]],
    minimum_run: int = 2,
) -> tuple[Finding, ...]:
    years = sorted(yearly_values)
    if len(years) < minimum_run * 2:
        return ()
    has_nonzero = {year: _has_nonzero(yearly_values[year]) for year in years}
    findings: list[Finding] = []
    for boundary_index in range(minimum_run, len(years) - minimum_run + 1):
        prior = years[boundary_index - minimum_run : boundary_index]
        following = years[boundary_index : boundary_index + minimum_run]
        if any(right != left + 1 for left, right in zip(prior + following, (prior + following)[1:])):
            continue
        if not any(has_nonzero[year] for year in prior) and all(has_nonzero[year] for year in following):
            findings.append(
                Finding(
                    code="ZERO_COVERAGE_DISCONTINUITY",
                    severity="fail",
                    scope={
                        "atom_id": atom_id,
                        "dataset_id": dataset_id,
                        "last_all_zero_year": prior[-1],
                        "first_observed_nonzero_year": following[0],
                    },
                    evidence_refs=(),
                    remediation="classify the zero era and recover missing witness coverage",
                )
            )
    return tuple(findings)
