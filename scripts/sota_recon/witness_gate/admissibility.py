from __future__ import annotations

import json
import re
import string
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .models import AdmissibilityRule, Grain, ObservationRole


_CANONICAL_DATASET_ID = re.compile(
    r"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?:[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$"
)
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


def _validate_dataset_id(dataset_id: object) -> str:
    if not isinstance(dataset_id, str) or not _CANONICAL_DATASET_ID.fullmatch(dataset_id):
        raise ValueError(f"coverage registry requires a canonical dataset ID: {dataset_id!r}")
    return dataset_id


def _validate_coverage_state(coverage: object) -> frozenset[str]:
    dataset_id = _validate_dataset_id(getattr(coverage, "dataset_id", None))
    coverage_complete = getattr(coverage, "coverage_complete", None)
    if type(coverage_complete) is not bool:
        raise ValueError(f"coverage_complete must be a boolean: {dataset_id}")
    manifest_pin = getattr(coverage, "manifest_pin", None)
    if (
        not isinstance(manifest_pin, str)
        or not manifest_pin.strip()
        or manifest_pin != manifest_pin.strip()
    ):
        raise ValueError(f"coverage manifest_pin must be a nonempty canonical string: {dataset_id}")
    observed = getattr(coverage, "observed_shard_manifest_ids", None)
    if not isinstance(observed, frozenset):
        raise TypeError(f"coverage observed shard manifests must be a frozenset: {dataset_id}")
    if not observed:
        raise ValueError(f"coverage observed shard manifests must be nonempty: {dataset_id}")
    if any(not isinstance(item, str) or not _SHA256.fullmatch(item) for item in observed):
        raise ValueError(f"coverage observed shard manifests must be valid 64-hex IDs: {dataset_id}")
    canonical = frozenset(item.lower() for item in observed)
    if len(canonical) != len(observed):
        raise ValueError(f"coverage observed shard manifests contain case-fold duplicates: {dataset_id}")
    return canonical


@dataclass(frozen=True)
class EvidenceCandidate:
    dataset_id: str
    value: Any
    lineage_id: str
    provenance_kind: Literal["unpartitioned", "partitioned"]
    shard_manifest_id: str | None = None
    claim_kind: Literal["positive", "absence"] = "positive"

    def __post_init__(self) -> None:
        if self.provenance_kind not in {"unpartitioned", "partitioned"}:
            raise ValueError(f"unknown provenance kind: {self.provenance_kind}")
        if self.claim_kind not in {"positive", "absence"}:
            raise ValueError(f"unknown claim kind: {self.claim_kind}")
        if self.provenance_kind == "unpartitioned" and self.shard_manifest_id is not None:
            raise ValueError("unpartitioned evidence cannot carry shard_manifest_id")


@dataclass(frozen=True)
class VerifiedDatasetCoverage:
    dataset_id: str
    coverage_complete: bool
    manifest_pin: str
    observed_shard_manifest_ids: frozenset[str]

    def __post_init__(self) -> None:
        canonical = _validate_coverage_state(self)
        object.__setattr__(self, "observed_shard_manifest_ids", canonical)


def validate_coverage_registry(
    coverage_registry: Mapping[str, VerifiedDatasetCoverage] | None,
) -> dict[str, VerifiedDatasetCoverage]:
    if coverage_registry is None:
        return {}
    if not isinstance(coverage_registry, Mapping):
        raise TypeError("coverage_registry must be a mapping")
    validated: dict[str, VerifiedDatasetCoverage] = {}
    for key, coverage in coverage_registry.items():
        dataset_id = _validate_dataset_id(key)
        if not isinstance(coverage, VerifiedDatasetCoverage):
            raise TypeError(f"coverage registry values must be VerifiedDatasetCoverage: {dataset_id}")
        canonical_leaves = _validate_coverage_state(coverage)
        if dataset_id != coverage.dataset_id:
            raise ValueError(
                "coverage registry key must match coverage dataset_id: "
                f"key={dataset_id!r} coverage={coverage.dataset_id!r}"
            )
        validated[dataset_id] = VerifiedDatasetCoverage(
            dataset_id=coverage.dataset_id,
            coverage_complete=coverage.coverage_complete,
            manifest_pin=coverage.manifest_pin,
            observed_shard_manifest_ids=canonical_leaves,
        )
    return validated


def _load_dataset_coverage(path: Path) -> VerifiedDatasetCoverage:
    payload = json.loads(path.read_text(encoding="utf-8"))
    source = str(payload.get("source", "")).strip()
    dataset = str(payload.get("dataset", "")).strip()
    if not source or not dataset:
        raise ValueError(f"import manifest is missing dataset identity: {path}")
    coverage_complete = payload.get("coverage_complete")
    if not isinstance(coverage_complete, bool):
        raise ValueError(f"import manifest has invalid coverage_complete: {path}")
    manifest_pin = str(payload.get("manifest_pin", "")).strip()
    if not manifest_pin:
        raise ValueError(f"import manifest is missing manifest_pin: {path}")
    shard_ids: list[int] = []
    manifest_ids: list[str] = []
    for item in payload.get("shard_manifests", []):
        shard_ids.append(int(item["shard_id"]))
        manifest_id = str(item.get("sha256", "")).lower()
        if len(manifest_id) != 64 or any(value not in string.hexdigits for value in manifest_id):
            raise ValueError(f"import manifest has invalid shard SHA256: {path}")
        manifest_ids.append(manifest_id)
    if not manifest_ids:
        raise ValueError(f"import manifest has no observed shard manifests: {path}")
    if len(shard_ids) != len(set(shard_ids)) or len(manifest_ids) != len(set(manifest_ids)):
        raise ValueError(f"import manifest has duplicate shard manifests: {path}")
    return VerifiedDatasetCoverage(
        dataset_id=f"{source}:{dataset}",
        coverage_complete=coverage_complete,
        manifest_pin=manifest_pin,
        observed_shard_manifest_ids=frozenset(manifest_ids),
    )


def load_coverage_registry(
    import_manifest_paths: Iterable[Path],
) -> dict[str, VerifiedDatasetCoverage]:
    registry: dict[str, VerifiedDatasetCoverage] = {}
    for path in import_manifest_paths:
        coverage = _load_dataset_coverage(path)
        if coverage.dataset_id in registry:
            raise ValueError(f"duplicate coverage dataset: {coverage.dataset_id}")
        registry[coverage.dataset_id] = coverage
    return registry


@dataclass(frozen=True)
class AdmissionDecision:
    outcome: str
    value: Any | None
    selected_dataset_ids: tuple[str, ...]
    selected_lineage_ids: tuple[str, ...]
    selected_role: ObservationRole | None
    rule_ids: tuple[str, ...]


def _matches(
    rule: AdmissibilityRule,
    *,
    atom_id: str,
    year: int,
    grain: Grain,
    competition: str,
    season_type: str,
) -> bool:
    return (
        rule.atom_id == atom_id
        and rule.year_start <= year <= rule.year_end
        and (rule.grain is None or rule.grain is grain)
        and (rule.competition is None or rule.competition == competition)
        and (rule.season_type is None or rule.season_type == season_type)
    )


_ROLE_STRENGTH = {
    ObservationRole.AUTHORITATIVE: 0,
    ObservationRole.CORROBORATING: 1,
    ObservationRole.IDENTITY_ONLY: 2,
    ObservationRole.FALLBACK: 3,
    ObservationRole.INADMISSIBLE: 4,
}

_OUTCOME = {
    ObservationRole.AUTHORITATIVE: "authoritative",
    ObservationRole.CORROBORATING: "corroborating",
    ObservationRole.IDENTITY_ONLY: "corroborating",
    ObservationRole.FALLBACK: "fallback",
}


def _has_admissible_coverage(
    candidate: EvidenceCandidate,
    coverage_registry: dict[str, VerifiedDatasetCoverage],
) -> bool:
    is_zero_or_absence = (
        candidate.claim_kind == "absence" or candidate.value is None or candidate.value == 0
    )
    if candidate.provenance_kind == "unpartitioned":
        return not is_zero_or_absence
    coverage = coverage_registry.get(candidate.dataset_id)
    if coverage is None or candidate.lineage_id != coverage.manifest_pin:
        return False
    if is_zero_or_absence:
        return coverage.coverage_complete
    return (
        candidate.shard_manifest_id is not None
        and candidate.shard_manifest_id.lower() in coverage.observed_shard_manifest_ids
    )


def _leaf_lineage_id(candidate: EvidenceCandidate) -> str:
    return candidate.shard_manifest_id or candidate.lineage_id


def select_evidence(
    *,
    atom_id: str,
    year: int,
    grain: Grain,
    competition: str,
    season_type: str,
    candidates: Iterable[EvidenceCandidate],
    rules: Iterable[AdmissibilityRule],
    coverage_registry: dict[str, VerifiedDatasetCoverage] | None = None,
) -> AdmissionDecision:
    verified_coverage = validate_coverage_registry(coverage_registry)
    applicable_rules = {
        rule.dataset_id: rule
        for rule in rules
        if _matches(
            rule,
            atom_id=atom_id,
            year=year,
            grain=grain,
            competition=competition,
            season_type=season_type,
        )
    }
    eligible: list[tuple[EvidenceCandidate, AdmissibilityRule]] = []
    for item in candidates:
        rule = applicable_rules.get(item.dataset_id)
        if (
            rule is not None
            and rule.role is not ObservationRole.INADMISSIBLE
            and _has_admissible_coverage(item, verified_coverage)
        ):
            eligible.append((item, rule))
    if not eligible:
        return AdmissionDecision("inadmissible", None, (), (), None, ())

    best_precedence = min(rule.precedence for _, rule in eligible)
    selected = [(item, rule) for item, rule in eligible if rule.precedence == best_precedence]
    distinct_values: list[Any] = []
    for item, _ in selected:
        if item.value not in distinct_values:
            distinct_values.append(item.value)
    dataset_ids = tuple(sorted(item.dataset_id for item, _ in selected))
    lineage_ids = tuple(sorted({_leaf_lineage_id(item) for item, _ in selected}))
    rule_ids = tuple(sorted(rule.rule_id for _, rule in selected))
    if len(distinct_values) != 1:
        return AdmissionDecision(
            "collision_review",
            None,
            dataset_ids,
            lineage_ids,
            None,
            rule_ids,
        )

    selected_role = min((rule.role for _, rule in selected), key=_ROLE_STRENGTH.__getitem__)
    return AdmissionDecision(
        _OUTCOME[selected_role],
        distinct_values[0],
        dataset_ids,
        lineage_ids,
        selected_role,
        rule_ids,
    )
