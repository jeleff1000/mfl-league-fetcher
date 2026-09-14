from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .admissibility import EvidenceCandidate, VerifiedDatasetCoverage, select_evidence
from .models import AdmissibilityRule, GatePlane, Grain


def _hash(payload: object) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(body.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class Finding:
    code: str
    severity: str
    scope: dict[str, Any]
    evidence_refs: tuple[str, ...]
    remediation: str

    def __post_init__(self) -> None:
        if self.severity not in {"info", "review", "fail"}:
            raise ValueError(f"unknown finding severity: {self.severity}")
        if not self.code:
            raise ValueError("finding code is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "scope": self.scope,
            "evidence_refs": list(self.evidence_refs),
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class UpstreamGateRef:
    gate_id: str
    plane: GatePlane
    status: str
    manifest_fingerprint: str

    def to_dict(self) -> dict[str, str]:
        return {
            "gate_id": self.gate_id,
            "plane": self.plane.value,
            "status": self.status,
            "manifest_fingerprint": self.manifest_fingerprint,
        }


@dataclass(frozen=True)
class GateManifest:
    contract_version: str
    plane: GatePlane
    gate_id: str
    status: str
    findings: tuple[Finding, ...]
    upstream: tuple[UpstreamGateRef, ...]
    content_fingerprint: str
    manifest_fingerprint: str

    @classmethod
    def empty(cls, plane: GatePlane, gate_id: str) -> GateManifest:
        return build_manifest(plane, gate_id, ())

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "plane": self.plane.value,
            "gate_id": self.gate_id,
            "status": self.status,
            "findings": [item.to_dict() for item in self.findings],
            "upstream": [item.to_dict() for item in self.upstream],
            "content_fingerprint": self.content_fingerprint,
            "manifest_fingerprint": self.manifest_fingerprint,
        }


_ALLOWED_UPSTREAM = {
    GatePlane.GLOBAL_RESEARCH_HEALTH: frozenset(),
    GatePlane.SOURCE_HEALTH: frozenset(),
    GatePlane.CANDIDATE_PROMOTION: frozenset(
        {GatePlane.GLOBAL_RESEARCH_HEALTH, GatePlane.SOURCE_HEALTH}
    ),
    GatePlane.RELEASE: frozenset(
        {
            GatePlane.GLOBAL_RESEARCH_HEALTH,
            GatePlane.SOURCE_HEALTH,
            GatePlane.CANDIDATE_PROMOTION,
        }
    ),
}


def _status(findings: tuple[Finding, ...]) -> str:
    if any(item.severity == "fail" for item in findings):
        return "fail"
    if any(item.severity == "review" for item in findings):
        return "review"
    return "pass"


def build_manifest(
    plane: GatePlane,
    gate_id: str,
    findings: Iterable[Finding],
    *,
    upstream: Iterable[GateManifest] = (),
) -> GateManifest:
    upstream_manifests = tuple(upstream)
    disallowed = sorted(
        {item.plane.value for item in upstream_manifests if item.plane not in _ALLOWED_UPSTREAM[plane]}
    )
    if disallowed:
        raise ValueError(f"{plane.value} cannot depend on gate planes: {disallowed}")

    materialized_findings = list(findings)
    if plane is GatePlane.RELEASE:
        for item in upstream_manifests:
            if item.status == "fail":
                materialized_findings.append(
                    Finding(
                        code="UPSTREAM_GATE_FAILED",
                        severity="fail",
                        scope={"gate_id": item.gate_id, "plane": item.plane.value},
                        evidence_refs=(item.manifest_fingerprint,),
                        remediation="resolve the required upstream gate and rerun release",
                    )
                )
            elif item.status == "review":
                materialized_findings.append(
                    Finding(
                        code="UPSTREAM_GATE_REVIEW",
                        severity="review",
                        scope={"gate_id": item.gate_id, "plane": item.plane.value},
                        evidence_refs=(item.manifest_fingerprint,),
                        remediation="complete required review before release",
                    )
                )
    final_findings = tuple(materialized_findings)
    upstream_refs = tuple(
        UpstreamGateRef(
            gate_id=item.gate_id,
            plane=item.plane,
            status=item.status,
            manifest_fingerprint=item.manifest_fingerprint,
        )
        for item in upstream_manifests
    )
    content_payload = {
        "contract_version": "1",
        "plane": plane.value,
        "findings": [item.to_dict() for item in final_findings],
    }
    content_fingerprint = _hash(content_payload)
    manifest_fingerprint = _hash(
        {
            **content_payload,
            "gate_id": gate_id,
            "upstream": [item.to_dict() for item in upstream_refs],
        }
    )
    return GateManifest(
        contract_version="1",
        plane=plane,
        gate_id=gate_id,
        status=_status(final_findings),
        findings=final_findings,
        upstream=upstream_refs,
        content_fingerprint=content_fingerprint,
        manifest_fingerprint=manifest_fingerprint,
    )


def build_gate_manifests(
    *,
    source_universe_version: str,
    candidate_version: str,
    global_findings: Iterable[Finding] = (),
    source_findings: Iterable[Finding] = (),
    candidate_findings: Iterable[Finding] = (),
    release_findings: Iterable[Finding] = (),
    unresolved_collision_ids: Iterable[str] = (),
) -> dict[GatePlane, GateManifest]:
    """Build the four gate planes with their declared one-way dependencies."""
    if (
        not isinstance(source_universe_version, str)
        or not source_universe_version.strip()
        or not isinstance(candidate_version, str)
        or not candidate_version.strip()
    ):
        raise ValueError("source_universe_version and candidate_version are required")

    collision_ids = tuple(unresolved_collision_ids)
    if any(not isinstance(item, str) or not item.strip() for item in collision_ids):
        raise ValueError("unresolved collision IDs must be nonempty strings")
    if len(collision_ids) != len(set(collision_ids)):
        raise ValueError("unresolved collision IDs must be unique")

    def stable(items: Iterable[Finding]) -> tuple[Finding, ...]:
        return tuple(
            sorted(
                items,
                key=lambda item: json.dumps(item.to_dict(), sort_keys=True, separators=(",", ":")),
            )
        )

    global_manifest = build_manifest(
        GatePlane.GLOBAL_RESEARCH_HEALTH,
        f"global:{source_universe_version}",
        stable(global_findings),
    )
    source_manifest = build_manifest(
        GatePlane.SOURCE_HEALTH,
        f"source:{source_universe_version}",
        stable(source_findings),
    )
    candidate_manifest = build_manifest(
        GatePlane.CANDIDATE_PROMOTION,
        f"candidate:{candidate_version}",
        stable(candidate_findings),
        upstream=(global_manifest, source_manifest),
    )
    collision_findings = tuple(
        Finding(
            code="UNRESOLVED_ROSTER_COLLISION",
            severity="fail",
            scope={"collision_id": collision_id},
            evidence_refs=(),
            remediation="resolve the roster collision and regenerate its decision ledger",
        )
        for collision_id in sorted(collision_ids)
    )
    release_manifest = build_manifest(
        GatePlane.RELEASE,
        f"release:{candidate_version}",
        stable((*release_findings, *collision_findings)),
        upstream=(global_manifest, source_manifest, candidate_manifest),
    )
    return {
        GatePlane.GLOBAL_RESEARCH_HEALTH: global_manifest,
        GatePlane.SOURCE_HEALTH: source_manifest,
        GatePlane.CANDIDATE_PROMOTION: candidate_manifest,
        GatePlane.RELEASE: release_manifest,
    }


def candidate_admission_finding(
    candidate_id: str,
    *,
    atom_id: str,
    year: int,
    grain: Grain,
    competition: str,
    season_type: str,
    candidates: Iterable[EvidenceCandidate],
    rules: Iterable[AdmissibilityRule],
    coverage_registry: dict[str, VerifiedDatasetCoverage],
) -> Finding:
    """Select verified evidence and translate the result into a promotion finding."""
    if not isinstance(candidate_id, str) or not candidate_id.strip():
        raise ValueError("candidate_id is required")
    decision = select_evidence(
        atom_id=atom_id,
        year=year,
        grain=grain,
        competition=competition,
        season_type=season_type,
        candidates=candidates,
        rules=rules,
        coverage_registry=coverage_registry,
    )
    successful = decision.outcome in {"authoritative", "corroborating", "fallback"}
    complete_selection = all(
        values and all(isinstance(value, str) and value.strip() for value in values)
        for values in (
            decision.selected_dataset_ids,
            decision.selected_lineage_ids,
            decision.rule_ids,
        )
    )
    if successful and complete_selection:
        code = "CANDIDATE_ADMISSIBLE"
        severity = "info"
        remediation = "none"
    elif decision.outcome == "collision_review":
        code = "CANDIDATE_EVIDENCE_COLLISION"
        severity = "fail"
        remediation = "resolve the equal-precedence evidence collision"
    elif decision.outcome == "inadmissible" or successful:
        code = "CANDIDATE_INADMISSIBLE"
        severity = "fail"
        remediation = "supply evidence admissible under the registered coverage contract"
    else:
        raise ValueError(f"unknown admission outcome: {decision.outcome!r}")
    return Finding(
        code=code,
        severity=severity,
        scope={
            "candidate_id": candidate_id,
            "outcome": decision.outcome,
            "dataset_ids": list(decision.selected_dataset_ids),
            "rule_ids": list(decision.rule_ids),
        },
        evidence_refs=decision.selected_lineage_ids,
        remediation=remediation,
    )
