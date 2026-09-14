from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from .gate_planes import Finding


@dataclass(frozen=True)
class DatasetFieldContract:
    contract_version: str
    dataset_id: str
    reviewed_fields: tuple[str, ...]
    discriminator_fields: tuple[str, ...]
    atom_template: str
    excluded_fields: dict[str, str]
    field_set_fingerprint: str


@dataclass(frozen=True)
class FieldRegistry:
    contract_version: str
    datasets: dict[str, DatasetFieldContract]


@dataclass(frozen=True)
class FieldClosureResult:
    dataset_id: str
    raw_atom_ids: dict[str, str]
    findings: tuple[Finding, ...]


def _field_set_fingerprint(fields: tuple[str, ...]) -> str:
    canonical = json.dumps(list(fields), separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def load_field_registry(path: str | Path) -> FieldRegistry:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    allowed = {"contract_version", "field_sets", "datasets"}
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(f"unknown field registry keys: {sorted(unknown)}")
    field_sets: dict[str, tuple[str, ...]] = {}
    for field_set_id, values in payload.get("field_sets", {}).items():
        fields = tuple(values)
        if fields != tuple(sorted(fields)):
            raise ValueError(f"field set {field_set_id} must be sorted")
        if len(fields) != len(set(fields)):
            raise ValueError(f"field set {field_set_id} contains duplicates")
        field_sets[field_set_id] = fields

    datasets: dict[str, DatasetFieldContract] = {}
    dataset_allowed = {"field_set", "discriminator_fields", "atom_template", "excluded_fields"}
    for dataset_id, body in payload.get("datasets", {}).items():
        body_unknown = set(body) - dataset_allowed
        if body_unknown:
            raise ValueError(f"unknown fields for dataset {dataset_id}: {sorted(body_unknown)}")
        field_set_id = body.get("field_set")
        if field_set_id not in field_sets:
            raise ValueError(f"dataset {dataset_id} references unknown field set {field_set_id}")
        reviewed_fields = field_sets[field_set_id]
        discriminators = tuple(body.get("discriminator_fields", []))
        if not set(discriminators) <= set(reviewed_fields):
            raise ValueError(f"dataset {dataset_id} has unreviewed discriminator fields")
        template = str(body.get("atom_template", ""))
        if template.count("{field}") != 1:
            raise ValueError(f"dataset {dataset_id} atom_template must contain one {{field}} placeholder")
        excluded = dict(body.get("excluded_fields", {}))
        if not set(excluded) <= set(reviewed_fields):
            raise ValueError(f"dataset {dataset_id} excludes unknown fields")
        if any(not reason for reason in excluded.values()):
            raise ValueError(f"dataset {dataset_id} has an exclusion without a reason code")
        datasets[dataset_id] = DatasetFieldContract(
            contract_version=str(payload["contract_version"]),
            dataset_id=dataset_id,
            reviewed_fields=reviewed_fields,
            discriminator_fields=discriminators,
            atom_template=template,
            excluded_fields=excluded,
            field_set_fingerprint=_field_set_fingerprint(reviewed_fields),
        )
    return FieldRegistry(contract_version=str(payload["contract_version"]), datasets=datasets)


def audit_field_closure(contract: DatasetFieldContract, observed_fields: set[str]) -> FieldClosureResult:
    reviewed = set(contract.reviewed_fields)
    findings: list[Finding] = []
    for field in sorted(observed_fields - reviewed):
        findings.append(
            Finding(
                code="UNREGISTERED_FIELD",
                severity="fail",
                scope={"dataset_id": contract.dataset_id, "field": field},
                evidence_refs=(contract.field_set_fingerprint,),
                remediation="map or explicitly exclude the field in a new registry version",
            )
        )
    for field in sorted(reviewed - observed_fields):
        findings.append(
            Finding(
                code="CONTRACTED_FIELD_MISSING",
                severity="fail",
                scope={"dataset_id": contract.dataset_id, "field": field},
                evidence_refs=(contract.field_set_fingerprint,),
                remediation="restore the field or version the source schema contract",
            )
        )
    raw_atoms = {
        field: contract.atom_template.format(field=field)
        for field in contract.reviewed_fields
        if field not in contract.excluded_fields
    }
    if len(raw_atoms.values()) != len(set(raw_atoms.values())):
        raise ValueError(f"dataset {contract.dataset_id} atom template creates collisions")
    return FieldClosureResult(
        dataset_id=contract.dataset_id,
        raw_atom_ids=raw_atoms,
        findings=tuple(findings),
    )
