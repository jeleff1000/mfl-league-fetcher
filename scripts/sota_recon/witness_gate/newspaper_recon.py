from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NewspaperAtomCandidate:
    candidate_id: str
    atom_id: str
    value: Any
    dependencies: tuple[str, ...]
    leaf_evidence: tuple[str, ...]
    confidence: float
    minimum_confidence: float
    identity_status: str
    admissibility_outcome: str
    pfr_collision: bool


@dataclass(frozen=True)
class UnlockLedgerEntry:
    iteration: int
    candidate_id: str
    atom_id: str
    value: Any
    dependency_atom_ids: tuple[str, ...]
    leaf_evidence: tuple[str, ...]


@dataclass(frozen=True)
class NewspaperReconResult:
    iterations: int
    promoted_atom_ids: tuple[str, ...]
    facts: dict[str, Any]
    unlock_ledger: tuple[UnlockLedgerEntry, ...]
    counts: dict[str, int]


_PROMOTABLE = {"authoritative", "corroborating", "fallback"}


def _validate_candidates(candidates: tuple[NewspaperAtomCandidate, ...]) -> None:
    atom_ids = [item.atom_id for item in candidates]
    candidate_ids = [item.candidate_id for item in candidates]
    if len(atom_ids) != len(set(atom_ids)):
        raise ValueError("duplicate newspaper atom candidate")
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("duplicate newspaper candidate_id")
    for item in candidates:
        if not item.leaf_evidence or any(not evidence.startswith("leaf:") for evidence in item.leaf_evidence):
            raise ValueError(f"candidate {item.candidate_id} has invalid leaf evidence")

    candidate_atoms = set(atom_ids)
    dependencies = {
        item.atom_id: set(item.dependencies) & candidate_atoms
        for item in candidates
    }
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(atom_id: str) -> None:
        status = state.get(atom_id, 0)
        if status == 2:
            return
        if status == 1:
            start = stack.index(atom_id)
            cycle = stack[start:] + [atom_id]
            raise ValueError(f"newspaper candidate cycle: {' -> '.join(cycle)}")
        state[atom_id] = 1
        stack.append(atom_id)
        for dependency in sorted(dependencies[atom_id]):
            visit(dependency)
        stack.pop()
        state[atom_id] = 2

    for atom_id in sorted(candidate_atoms):
        visit(atom_id)


def _static_hold(item: NewspaperAtomCandidate) -> str | None:
    if item.pfr_collision:
        return "pfr_collision_review"
    if item.identity_status != "resolved":
        return "identity_review"
    if item.confidence < item.minimum_confidence:
        return "below_confidence"
    if item.admissibility_outcome not in _PROMOTABLE:
        return "inadmissible"
    return None


def reconcile_to_fixed_point(
    candidates: tuple[NewspaperAtomCandidate, ...],
    *,
    initial_facts: dict[str, Any],
) -> NewspaperReconResult:
    _validate_candidates(candidates)
    facts = dict(initial_facts)
    initial_atom_ids = set(initial_facts)
    promoted: set[str] = set()
    ledger: list[UnlockLedgerEntry] = []
    iterations = 0

    while True:
        iterations += 1
        available_at_start = set(facts)
        unlocked: list[NewspaperAtomCandidate] = []
        for item in sorted(candidates, key=lambda candidate: candidate.atom_id):
            if item.atom_id in facts or _static_hold(item) is not None:
                continue
            if set(item.dependencies) <= available_at_start:
                unlocked.append(item)
        if not unlocked:
            break
        for item in unlocked:
            facts[item.atom_id] = item.value
            promoted.add(item.atom_id)
            ledger.append(
                UnlockLedgerEntry(
                    iteration=iterations,
                    candidate_id=item.candidate_id,
                    atom_id=item.atom_id,
                    value=item.value,
                    dependency_atom_ids=item.dependencies,
                    leaf_evidence=item.leaf_evidence,
                )
            )

    counts = {
        "promoted": 0,
        "fully_vetted": 0,
        "pfr_collision_review": 0,
        "identity_review": 0,
        "below_confidence": 0,
        "dependency_hold": 0,
        "inadmissible": 0,
    }
    for item in candidates:
        if item.atom_id in initial_atom_ids and _static_hold(item) is None:
            counts["fully_vetted"] += 1
        elif item.atom_id in promoted:
            counts["promoted"] += 1
        else:
            hold = _static_hold(item)
            counts[hold or "dependency_hold"] += 1
    return NewspaperReconResult(
        iterations=iterations,
        promoted_atom_ids=tuple(sorted(promoted)),
        facts=facts,
        unlock_ledger=tuple(ledger),
        counts=counts,
    )
