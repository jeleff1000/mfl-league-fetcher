from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.sota_recon.witness_gate.gate_planes import Finding, build_manifest
from scripts.sota_recon.witness_gate.materialize import advance_current, materialize_candidate
from scripts.sota_recon.witness_gate.models import GatePlane


def rows() -> list[dict[str, object]]:
    return [
        {
            "atom_id": "passing.attempts",
            "entity_key": "p1:2000:1",
            "value": 4,
            "evidence_id": "evidence:1",
            "proof_root": "proof:1",
        }
    ]


def release(status: str):
    findings = ()
    if status == "fail":
        findings = (
            Finding(
                code="FIXTURE_FAIL",
                severity="fail",
                scope={},
                evidence_refs=(),
                remediation="fix fixture",
            ),
        )
    return build_manifest(GatePlane.RELEASE, f"release:{status}", findings)


def test_failed_release_leaves_current_pointer_unchanged(tmp_path: Path) -> None:
    first = materialize_candidate(tmp_path, "candidate-v1", rows(), natural_keys=("atom_id", "entity_key"))
    advance_current(tmp_path, first, release("pass"))
    second = materialize_candidate(
        tmp_path,
        "candidate-v2",
        [{**rows()[0], "value": 5}],
        natural_keys=("atom_id", "entity_key"),
    )

    changed = advance_current(tmp_path, second, release("fail"))
    pointer = json.loads((tmp_path / "CURRENT.json").read_text(encoding="utf-8"))

    assert changed is False
    assert pointer["candidate_version"] == "candidate-v1"


def test_passed_release_advances_pointer_exactly_once(tmp_path: Path) -> None:
    candidate = materialize_candidate(tmp_path, "candidate-v1", rows(), natural_keys=("atom_id", "entity_key"))

    first = advance_current(tmp_path, candidate, release("pass"))
    second = advance_current(tmp_path, candidate, release("pass"))

    assert first is True
    assert second is False
    pointer = json.loads((tmp_path / "CURRENT.json").read_text(encoding="utf-8"))
    assert pointer["artifact_fingerprint"] == candidate.artifact_fingerprint


def test_candidate_requires_unique_keys_and_leaf_lineage_columns(tmp_path: Path) -> None:
    duplicate = rows() + rows()
    with pytest.raises(ValueError, match="duplicate natural key"):
        materialize_candidate(tmp_path, "duplicate", duplicate, natural_keys=("atom_id", "entity_key"))

    missing_proof = [{key: value for key, value in rows()[0].items() if key != "proof_root"}]
    with pytest.raises(ValueError, match="proof_root"):
        materialize_candidate(tmp_path, "missing-proof", missing_proof, natural_keys=("atom_id", "entity_key"))
