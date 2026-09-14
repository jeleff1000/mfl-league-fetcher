from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import DatasetContract


@dataclass(frozen=True)
class ProducerPin:
    kind: str
    manifest_pin: str | None = None
    pin_status: str | None = None
    local_only: bool = False

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ProducerPin:
        allowed = {"kind", "manifest_pin", "pin_status", "local_only"}
        unknown = set(payload) - allowed
        if unknown:
            raise ValueError(f"unknown producer fields: {sorted(unknown)}")
        pin = cls(**payload)
        if pin.kind == "ff_assets":
            has_pin = bool(pin.manifest_pin)
            is_pending = pin.pin_status == "pending_pin"
            if has_pin == is_pending:
                raise ValueError("ff_assets producer requires exactly one manifest pin or pending_pin status")
        elif pin.kind == "local_lake":
            if not pin.local_only:
                raise ValueError("local_lake producer must declare local_only")
        else:
            raise ValueError(f"unknown producer kind: {pin.kind}")
        return pin


@dataclass(frozen=True)
class CensusDataset:
    contract: DatasetContract
    producer: ProducerPin

    def __getattr__(self, name: str) -> Any:
        return getattr(self.contract, name)


@dataclass(frozen=True, order=True)
class Discovery:
    dataset_id: str
    url: str
    classification: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Discovery:
        allowed = {"dataset_id", "url", "classification"}
        unknown = set(payload) - allowed
        if unknown:
            raise ValueError(f"unknown discovery fields: {sorted(unknown)}")
        return cls(**payload)


@dataclass(frozen=True)
class DiscoveryDelta:
    added: tuple[Discovery, ...]
    removed: tuple[Discovery, ...]
    changed: tuple[tuple[Discovery, Discovery], ...]

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.changed)


@dataclass(frozen=True)
class SourceCensus:
    contract_version: str
    source_universe_version: str
    datasets: tuple[CensusDataset, ...]
    discoveries: tuple[Discovery, ...]
    fingerprint: str


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _parse_dataset(payload: dict[str, Any]) -> CensusDataset:
    allowed = {
        "contract_version",
        "dataset_id",
        "source_class",
        "physical_globs",
        "year_start",
        "year_end",
        "required_years",
        "year_field",
        "schema_variant_count",
        "producer",
    }
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(f"unknown census dataset fields: {sorted(unknown)}")
    contract_payload = {key: value for key, value in payload.items() if key != "producer"}
    contract = DatasetContract.from_dict(contract_payload)
    producer_payload = payload.get("producer")
    if not isinstance(producer_payload, dict):
        raise ValueError(f"dataset {contract.dataset_id} is missing producer metadata")
    producer = ProducerPin.from_dict(producer_payload)
    if contract.year_start is None or contract.year_end is None or contract.year_start > contract.year_end:
        raise ValueError(f"invalid year range for {contract.dataset_id}")
    if contract.required_years:
        if tuple(sorted(set(contract.required_years))) != contract.required_years:
            raise ValueError(f"required_years must be sorted and unique for {contract.dataset_id}")
        if contract.required_years[0] < contract.year_start or contract.required_years[-1] > contract.year_end:
            raise ValueError(f"required_years fall outside the year range for {contract.dataset_id}")
    if not contract.physical_globs:
        raise ValueError(f"dataset {contract.dataset_id} has no physical glob")
    if contract.schema_variant_count < 1:
        raise ValueError(f"dataset {contract.dataset_id} has invalid schema_variant_count")
    return CensusDataset(contract=contract, producer=producer)


def load_census(path: str | Path) -> SourceCensus:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    allowed = {"contract_version", "source_universe_version", "datasets", "discoveries"}
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(f"unknown census fields: {sorted(unknown)}")
    if not payload.get("source_universe_version"):
        raise ValueError("source_universe_version is required")

    datasets = tuple(_parse_dataset(item) for item in payload.get("datasets", []))
    ids = [dataset.dataset_id for dataset in datasets]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate dataset_id in source census")

    seen_globs: dict[str, str] = {}
    for dataset in datasets:
        for physical_glob in dataset.physical_globs:
            normalized = physical_glob.replace("\\", "/").casefold()
            if normalized in seen_globs:
                raise ValueError(
                    f"physical glob {physical_glob!r} is assigned to both "
                    f"{seen_globs[normalized]} and {dataset.dataset_id}"
                )
            seen_globs[normalized] = dataset.dataset_id

    discoveries = tuple(sorted(Discovery.from_dict(item) for item in payload.get("discoveries", [])))
    return SourceCensus(
        contract_version=str(payload["contract_version"]),
        source_universe_version=str(payload["source_universe_version"]),
        datasets=datasets,
        discoveries=discoveries,
        fingerprint=f"sha256:{hashlib.sha256(_canonical_json(payload)).hexdigest()}",
    )


def compare_discoveries(
    approved: tuple[Discovery, ...], current_payload: list[dict[str, Any]]
) -> DiscoveryDelta:
    current = tuple(sorted(Discovery.from_dict(item) for item in current_payload))
    approved_by_key = {(item.dataset_id, item.url): item for item in approved}
    current_by_key = {(item.dataset_id, item.url): item for item in current}
    added = tuple(current_by_key[key] for key in sorted(current_by_key.keys() - approved_by_key.keys()))
    removed = tuple(approved_by_key[key] for key in sorted(approved_by_key.keys() - current_by_key.keys()))
    changed = tuple(
        (approved_by_key[key], current_by_key[key])
        for key in sorted(approved_by_key.keys() & current_by_key.keys())
        if approved_by_key[key] != current_by_key[key]
    )
    return DiscoveryDelta(added=added, removed=removed, changed=changed)
