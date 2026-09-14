from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.models import (
    AdmissibilityRule,
    AtomContract,
    DatasetContract,
    Entity,
    GatePlane,
    GateResult,
    Grain,
    ObservationRole,
    ProofNode,
    ProofNodeKind,
    SourceClass,
    Unit,
)


def test_contract_models_round_trip_without_losing_type_information() -> None:
    dataset = DatasetContract(
        contract_version="1",
        dataset_id="nflcom.player_logs",
        source_class=SourceClass.CANONICAL,
        physical_globs=("nflcom/player_logs/*.parquet",),
        year_start=1920,
        year_end=2025,
    )
    atom = AtomContract(
        contract_version="1",
        atom_id="receiving.receptions",
        unit=Unit.COUNT,
        entity=Entity.PLAYER,
        grain=Grain.PLAYER_GAME,
        partition_keys=("game_id", "player_id"),
    )
    rule = AdmissibilityRule(
        contract_version="1",
        rule_id="receptions.nflcom.1978_plus",
        atom_id=atom.atom_id,
        dataset_id=dataset.dataset_id,
        role=ObservationRole.CORROBORATING,
        year_start=1978,
        year_end=2025,
        precedence=20,
    )
    proof = ProofNode(
        contract_version="1",
        node_id="leaf:nflcom:fixture",
        kind=ProofNodeKind.LEAF,
        atom_id=atom.atom_id,
        dependencies=(),
        lineage_id="nflcom-api",
        artifact_fingerprint="sha256:abc",
    )
    gate = GateResult(
        contract_version="1",
        gate_id="source-health:fixture",
        plane=GatePlane.SOURCE_HEALTH,
        status="pass",
        findings=(),
    )

    for model in (dataset, atom, rule, proof, gate):
        assert type(model).from_dict(model.to_dict()) == model


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (DatasetContract, {"contract_version": "1", "dataset_id": "x", "source_class": "mystery"}),
        (AtomContract, {"contract_version": "1", "atom_id": "x", "unit": "bananas"}),
        (GateResult, {"contract_version": "1", "gate_id": "x", "plane": "combined", "status": "pass"}),
    ],
)
def test_contract_models_reject_unknown_enum_values(model: type, payload: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        model.from_dict(payload)


def test_contract_models_reject_unknown_fields() -> None:
    with pytest.raises(ValueError, match="unknown fields"):
        DatasetContract.from_dict(
            {
                "contract_version": "1",
                "dataset_id": "nflcom.player_logs",
                "source_class": "canonical",
                "surprise": True,
            }
        )
