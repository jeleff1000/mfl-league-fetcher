from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import pytest

from . import composite_column_adjudication as CA
from .composite_witness_lane import InternalResolution, LaneObservation, load_specs
from .composite_column_adjudication import (
    apply_decisions,
    build_decisions,
    receipt_obligations,
)


ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_RECEIPT = ROOT / "docs" / "composite-witness-lane-receipt.json"
DISPOSITIONS = (
    ROOT / "scripts" / "sota_recon" / "witness_gate" / "contracts"
    / "column_dispositions.v1.json"
)


@pytest.fixture
def local_receipt() -> Path:
    return PRODUCTION_RECEIPT


def _passing_result(spec_id: str) -> dict[str, object]:
    spec = next(spec for spec in load_specs() if spec.spec_id == spec_id)
    external = tuple(target for target in spec.targets if target not in spec.internal_targets)
    constraint_id = {
        "pfr-field-goals-50-plus-v1": "pfr_fg_50_plus_made",
        "pfr-two-point-total-v1": "pfr_two_point_total",
    }.get(spec_id)
    observation_evidence = (
        (("constraint_id", constraint_id),)
        if constraint_id is not None
        else (("fixture", "semantically valid current PASS"),)
    )
    observations = () if not external else (
        LaneObservation(
            spec_id,
            spec.sources[0],
            spec.kind,
            external,
            1,
            0,
            "PASS",
            1,
            observation_evidence,
        ),
    )
    internal = () if not spec.internal_targets else (
        InternalResolution(
            spec_id,
            "player_bio",
            spec.internal_targets,
            1,
            0,
            "PASS",
            1,
            (("fixture", "internal resolution only"),),
        ),
    )
    return {
        "observations": [asdict(item) for item in observations],
        "internal_resolutions": [asdict(item) for item in internal],
        "compared_rows": 1,
        "rejected_rows": 0,
        "errors": [],
    }


@pytest.fixture
def ideal_receipt(tmp_path: Path) -> Path:
    payload = json.loads(PRODUCTION_RECEIPT.read_text(encoding="utf-8"))
    for spec_id, status in payload["spec_statuses"].items():
        if spec_id == "nflcom-l7-fg-buckets-v1":
            continue
        status["resolution_status"] = "PASS"
        status["license_status"] = "PASS"
        status["result"] = _passing_result(spec_id)
    payload["resolution_status"] = "PARTIAL"
    payload["license_status"] = "PARTIAL"
    path = tmp_path / "ideal-partial-receipt.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_failed_production_receipt_authorizes_no_closures_and_five_groups(local_receipt: Path):
    decisions, groups = build_decisions(local_receipt)

    assert decisions == ()
    assert len(groups) == 5
    assert sum(group["affected_rows"] for group in groups) == 126
    assert len({key for group in groups for key in group["row_keys"]}) == 126


def test_each_failure_is_one_spec_owned_machine_settleable_group(local_receipt: Path):
    _, groups = build_decisions(local_receipt)
    contract = {spec.spec_id: spec for spec in load_specs()}

    assert {group["spec_id"] for group in groups} == set(contract)
    for group in groups:
        assert tuple(group["row_keys"]) == contract[group["spec_id"]].row_keys
        assert group["affected_rows"] == len(group["row_keys"])
        assert group["receipt_resolution_status"] == "FAIL"
        assert group["receipt_license_status"] == "FAIL"
        assert group["blocker_codes"]
        assert group["evidence_summary"]
        assert group["settling_condition"]["predicate"] == "CURRENT_SPEC_RECEIPT_PASS"


def test_missing_receipt_fails_closed_as_five_groups_covering_the_contract(tmp_path: Path):
    decisions, groups = build_decisions(tmp_path / "missing.json")

    assert decisions == ()
    assert len(groups) == 5
    assert sum(group["affected_rows"] for group in groups) == 126
    assert len({key for group in groups for key in group["row_keys"]}) == 126
    assert {group["receipt_resolution_status"] for group in groups} == {"MISSING"}


def test_ideal_partial_fixture_can_still_close_only_passing_specs(ideal_receipt: Path):
    decisions, groups = build_decisions(ideal_receipt)

    assert len(decisions) == 48
    assert {decision["disposition"] for decision in decisions} == {"EXCLUDED_WITH_REASON"}
    assert len(groups) == 1
    assert groups[0]["spec_id"] == "nflcom-l7-fg-buckets-v1"
    assert groups[0]["affected_rows"] == 78


def test_ideal_passes_keep_scalar_constraint_and_internal_depth_separate(ideal_receipt: Path):
    scalar, constraints, internal = receipt_obligations(ideal_receipt)
    scalar_targets = {target for observation in scalar for target in observation.targets}
    constraint_ids = {dict(observation.evidence)["constraint_id"] for observation in constraints}

    assert {"allpro", "probowls", "fg_made_0_19", "fg_missed_0_19"} <= scalar_targets
    assert "hof" not in scalar_targets
    assert {"pfr_fg_50_plus_made", "pfr_two_point_total"} <= constraint_ids
    assert {resolution.targets for resolution in internal} == {("hof",)}


def test_failed_apply_is_byte_for_byte_noop_for_existing_dispositions(
    local_receipt: Path, tmp_path: Path
):
    dispositions = tmp_path / "column_dispositions.v1.json"
    dispositions.write_bytes(DISPOSITIONS.read_bytes())
    before = dispositions.read_bytes()

    result = apply_decisions(local_receipt, dispositions)

    assert result["written"] == 0
    assert dispositions.read_bytes() == before


def test_future_pass_preserves_an_existing_non_composite_owned_decision(
    ideal_receipt: Path, tmp_path: Path
):
    dispositions = tmp_path / "column_dispositions.v1.json"
    protected = {
        "key": "pfr_adj_passing|*|awards",
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "reviewed outside the composite generator",
        "evidence": "hand-reviewed evidence",
    }
    dispositions.write_text(
        json.dumps({"version": 1, "decisions": [protected]}, indent=2) + "\n",
        encoding="utf-8",
    )

    result = apply_decisions(ideal_receipt, dispositions)
    written = json.loads(dispositions.read_text(encoding="utf-8"))["decisions"]

    assert next(item for item in written if item["key"] == protected["key"]) == protected
    assert result["hand_decisions_preserved"] == 1


def test_cli_apply_without_path_uses_isolated_canonical_target_and_is_noop(
    local_receipt: Path, monkeypatch, tmp_path: Path
):
    dispositions = tmp_path / "column_dispositions.v1.json"
    dispositions.write_bytes(DISPOSITIONS.read_bytes())
    before = dispositions.read_bytes()
    monkeypatch.setattr(CA, "DISPOSITIONS_PATH", dispositions, raising=False)

    result = CA.main(["--receipt", str(local_receipt), "--apply"])

    assert result == 0
    assert dispositions.read_bytes() == before
