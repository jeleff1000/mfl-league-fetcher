"""Closed contracts, local execution, and receipts for composite witnesses."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from hashlib import sha256
import importlib
import json
from pathlib import Path
import sys
from typing import Literal


# ``python -m`` initially executes this file as ``__main__``.  Executor modules
# import its package name, so bind that name to the running module before their
# cycle-safe registration occurs; otherwise two distinct dataclass types make
# every exact closed-spec comparison fail as unauthorized.
if __name__ == "__main__":
    sys.modules.setdefault("scripts.sota_recon.composite_witness_lane", sys.modules[__name__])


ObservationKind = Literal["SCALAR_OBSERVATION", "CONSTRAINT_OBSERVATION"]

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "scripts" / "sota_recon" / "witness_gate" / "contracts" / "composite_witnesses.v1.json"
STAT_CONTRACT_PATH = ROOT / "scripts" / "sota_recon" / "witness_gate" / "contracts" / "stat_contracts.v1.json"
CROSSWALK_CONTRACT_PATH = ROOT / "scripts" / "sota_recon" / "witness_gate" / "contracts" / "crosswalk_receipts.v1.json"
DOSSIER_PATH = ROOT / "docs" / "column-dossier.json"
RECEIPT_PATH = ROOT / "docs" / "composite-witness-lane-receipt.json"
EXECUTOR_VERSION = "composite_witness_lane.v1"
_NORMALIZED_PENDING = "NORMALIZED_PENDING_GRAIN_RECONCILIATION"
_EXPECTED_SPEC_IDS = frozenset({
    "pfr-awards-v1",
    "nflcom-l7-fg-buckets-v1",
    "pfr-field-goals-buckets-1-4-v1",
    "pfr-field-goals-50-plus-v1",
    "pfr-two-point-total-v1",
})
_EXPECTED_ROW_KEY_COUNT = 126

_PFR_TYPED_AWARDS_TARGETS = ("hof", "allpro", "probowls")
_PFR_TYPED_AWARDS_SOURCES = ("pfr_all_pro_members", "pfr_pro_bowl_members")
_PFR_TYPED_AWARDS_CONTRACT = (None, "player_static", "MIXED_TARGET_CONTRACT")
_PFR_TYPED_AWARDS_INTERNAL_TARGETS = ("hof",)
_PFR_TYPED_AWARDS_TARGET_CONTRACTS = (
    ("hof", None, "player_static", "ANY"),
    ("allpro", "count", "player_static", "FIRST"),
    ("probowls", "count", "player_static", "FIRST"),
)


@dataclass(frozen=True)
class CompositeSpec:
    spec_id: str
    cohort: str
    row_keys: tuple[str, ...]
    sources: tuple[str, ...]
    derivation: str
    kind: ObservationKind
    targets: tuple[str, ...]
    unit: str | None
    natural_grain: str
    aggregation_class: str
    crosswalk_receipt: str
    evidence: str
    scalar_credit: bool = False
    internal_targets: tuple[str, ...] = ()


@dataclass(frozen=True)
class LaneObservation:
    spec_id: str
    source: str
    kind: ObservationKind
    targets: tuple[str, ...]
    compared_rows: int
    rejected_rows: int
    status: str
    value: int | None = None
    evidence: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class InternalResolution:
    spec_id: str
    source: str
    targets: tuple[str, ...]
    compared_rows: int
    rejected_rows: int
    status: str
    value: int | None = None
    evidence: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class LaneResult:
    observations: tuple[LaneObservation, ...]
    compared_rows: int
    rejected_rows: int
    errors: tuple[str, ...]
    internal_resolutions: tuple[InternalResolution, ...] = ()

    @property
    def scalar_targets(self) -> frozenset[str]:
        return frozenset(
            target
            for observation in self.observations
            if observation.kind == "SCALAR_OBSERVATION"
            for target in observation.targets
        )


@dataclass(frozen=True)
class LanePaths:
    dossier: Path
    v26: Path
    player_bio: Path
    receipt: Path
    source_paths: Mapping[str, Path]


@dataclass(frozen=True)
class SpecStatus:
    spec_id: str
    resolution_status: str
    license_status: str
    spec_contract_sha256: str
    source_manifest_sha256: str
    crosswalk_statuses: tuple[tuple[str, str], ...]
    result: LaneResult


# Executors register only after their module's closed, source-vs-canonical
# comparison contract has been imported.  A registered name is never itself a
# receipt or scalar-credit grant.
EXECUTORS: dict[str, Callable[[CompositeSpec, LanePaths], LaneResult]] = {}


def load_specs(path: Path = CONTRACT_PATH) -> tuple[CompositeSpec, ...]:
    """Read the checked-in literal enumeration without discovering new rows."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != 1:
        raise ValueError(f"unsupported composite witness contract version: {payload.get('version')!r}")
    return tuple(
        CompositeSpec(
            spec_id=item["spec_id"],
            cohort=item["cohort"],
            row_keys=tuple(item["row_keys"]),
            sources=tuple(item["sources"]),
            derivation=item["derivation"],
            kind=item["kind"],
            targets=tuple(item["targets"]),
            unit=item.get("unit"),
            natural_grain=item["natural_grain"],
            aggregation_class=item["aggregation_class"],
            crosswalk_receipt=item["crosswalk_receipt"],
            evidence=item["evidence"],
            scalar_credit=bool(item.get("scalar_credit", False)),
            internal_targets=tuple(item.get("internal_targets", ())),
        )
        for item in payload["specs"]
    )


def _dossier_key(row: Mapping[str, object]) -> str:
    return f"{row['source']}|{row['table_key']}|{row['column']}"


def _is_exact_pfr_typed_awards(spec: CompositeSpec) -> bool:
    return (
        spec.cohort == "pfr_awards"
        and spec.derivation == "pfr_typed_awards_local"
        and spec.kind == "SCALAR_OBSERVATION"
        and spec.sources == _PFR_TYPED_AWARDS_SOURCES
        and spec.targets == _PFR_TYPED_AWARDS_TARGETS
        and spec.internal_targets == _PFR_TYPED_AWARDS_INTERNAL_TARGETS
        and (spec.unit, spec.natural_grain, spec.aggregation_class) == _PFR_TYPED_AWARDS_CONTRACT
    )


def validate_specs(
    specs: Sequence[CompositeSpec],
    dossier_rows: Sequence[Mapping[str, object]],
    stat_contracts: Mapping[str, Mapping[str, object]],
) -> list[str]:
    """Validate ownership and canonical semantic compatibility, without I/O."""
    known_rows = {_dossier_key(row) for row in dossier_rows}
    owners: dict[str, str] = {}
    problems: list[str] = []

    for spec in specs:
        if not spec.row_keys:
            problems.append(f"{spec.spec_id}: no row keys")
        if not spec.sources:
            problems.append(f"{spec.spec_id}: no registered sources")
        if not spec.targets:
            problems.append(f"{spec.spec_id}: no canonical targets")
        if spec.kind not in ("SCALAR_OBSERVATION", "CONSTRAINT_OBSERVATION"):
            problems.append(f"{spec.spec_id}: unknown observation kind {spec.kind!r}")
        if spec.kind == "CONSTRAINT_OBSERVATION" and spec.scalar_credit:
            problems.append("constraint cannot claim scalar witness credit")
        if len(set(spec.internal_targets)) != len(spec.internal_targets):
            problems.append(f"{spec.spec_id}: duplicate internal target")
        for target in spec.internal_targets:
            if target not in spec.targets:
                problems.append(f"{spec.spec_id}: internal target {target!r} is not a canonical target")
        if spec.internal_targets and not _is_exact_pfr_typed_awards(spec):
            problems.append(
                f"{spec.spec_id}: internal targets are only allowed for pfr_awards/pfr_typed_awards_local"
            )

        for key in spec.row_keys:
            if key in owners:
                owner = owners[key]
                problems.append(f"duplicate row key {key}: {owner} and {spec.spec_id}")
            else:
                owners[key] = spec.spec_id
            if key not in known_rows:
                problems.append(f"unknown dossier row key {key}")

        contracts = []
        for target in spec.targets:
            contract = stat_contracts.get(target)
            if contract is None:
                problems.append(f"unknown canonical target {target}")
            else:
                contracts.append((target, contract))

        if len(contracts) != len(spec.targets):
            continue
        if _is_exact_pfr_typed_awards(spec):
            actual_target_contracts = tuple(
                (
                    target,
                    contract.get("unit"),
                    contract.get("natural_grain"),
                    contract.get("aggregation_class"),
                )
                for target, contract in contracts
            )
            if actual_target_contracts != _PFR_TYPED_AWARDS_TARGET_CONTRACTS:
                problems.append(f"{spec.spec_id}: pfr typed awards target contract mismatch")
            continue

        values = {key: [contract.get(key) for _, contract in contracts] for key in ("unit", "natural_grain", "aggregation_class")}
        if len(set(values["unit"])) > 1 or len(set(values["aggregation_class"])) > 1:
            problems.append(
                "heterogeneous target contract is only allowed for pfr_awards/pfr_typed_awards/hof-allpro-probowls"
            )
            continue
        for field, expected in (
            ("unit", spec.unit),
            ("natural_grain", spec.natural_grain),
            ("aggregation_class", spec.aggregation_class),
        ):
            if any(contract.get(field) != expected for _, contract in contracts):
                problems.append(f"{spec.spec_id}: {field} mismatch")
    return problems


def contract_sha256(path: Path = CONTRACT_PATH) -> str:
    """Return the byte-exact SHA-256 of the versioned, checked-in contract."""
    return sha256(path.read_bytes()).hexdigest()


def _canonical_json_sha256(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def _spec_contract_sha256(spec: CompositeSpec) -> str:
    return _canonical_json_sha256(asdict(spec))


def artifact_manifest_sha256(paths: Sequence[Path]) -> str:
    """Hash a stable manifest of resolved path, size, and nanosecond mtime."""
    manifest = []
    for path in paths:
        resolved = path.resolve(strict=True)
        stat = resolved.stat()
        manifest.append((str(resolved), stat.st_size, stat.st_mtime_ns))
    manifest.sort()
    encoded = json.dumps(manifest, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def _manifest_files(paths: Sequence[Path]) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in paths:
        resolved = path.resolve(strict=True)
        if resolved.is_dir():
            members = sorted(item.resolve() for item in resolved.rglob("*") if item.is_file())
            if not members:
                raise FileNotFoundError(f"local source directory contains no files: {resolved}")
            files.extend(members)
        else:
            files.append(resolved)
    return tuple(dict.fromkeys(files))


def _spec_source_paths(spec: CompositeSpec, paths: LanePaths) -> tuple[Path, ...]:
    required: list[Path] = []
    required.extend(paths.source_paths[source] for source in spec.sources if source in paths.source_paths)
    if spec.cohort == "nflcom_l7":
        crosswalk = paths.source_paths.get(spec.crosswalk_receipt)
        if crosswalk is not None:
            required.append(crosswalk)
    else:
        required.append(paths.player_bio)
        if spec.cohort != "pfr_awards":
            required.append(paths.v26)
    return tuple(dict.fromkeys(required))


def _spec_source_manifest(
    spec: CompositeSpec,
    paths: LanePaths,
    dossier_rows: Sequence[Mapping[str, object]],
    stat_contracts: Mapping[str, Mapping[str, object]],
    crosswalk_receipts: Mapping[str, Mapping[str, object]],
) -> tuple[str, tuple[str, ...]]:
    missing_sources = [source for source in spec.sources if source not in paths.source_paths]
    if spec.cohort == "nflcom_l7" and spec.crosswalk_receipt not in paths.source_paths:
        missing_sources.append(spec.crosswalk_receipt)
    if missing_sources:
        marker = {"missing_registered_sources": sorted(missing_sources)}
        return _canonical_json_sha256(marker), tuple(
            f"source: missing registered source path {source}" for source in sorted(missing_sources)
        )
    try:
        files = _manifest_files(_spec_source_paths(spec, paths))
        owned_rows = sorted(
            (dict(row) for row in dossier_rows if _dossier_key(row) in set(spec.row_keys)),
            key=_dossier_key,
        )
        dependency = {
            "dossier_rows": owned_rows,
            "stat_contracts": [
                (target, dict(stat_contracts[target]))
                for target in spec.targets
                if target in stat_contracts
            ],
            "crosswalk_receipt": dict(crosswalk_receipts.get(spec.crosswalk_receipt, {})),
            "local_artifact_manifest_sha256": artifact_manifest_sha256(files),
        }
        return _canonical_json_sha256(dependency), ()
    except OSError as exc:
        return _canonical_json_sha256({"missing_local_artifact": str(exc)}), (f"local artifact: {exc}",)


def _load_validation_inputs(paths: LanePaths) -> tuple[list[Mapping[str, object]], dict[str, Mapping[str, object]]]:
    dossier = json.loads(paths.dossier.read_text(encoding="utf-8"))
    rows = dossier.get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("column dossier requires a rows array")
    contracts = json.loads(STAT_CONTRACT_PATH.read_text(encoding="utf-8"))
    stats = contracts.get("stats")
    if not isinstance(stats, list) or not all(isinstance(item, dict) and isinstance(item.get("stat_id"), str) for item in stats):
        raise ValueError("stat contract requires a stats array")
    return rows, {str(item["stat_id"]): item for item in stats}


def _load_crosswalk_receipts() -> dict[str, Mapping[str, object]]:
    document = json.loads(CROSSWALK_CONTRACT_PATH.read_text(encoding="utf-8"))
    receipts = document.get("receipts")
    if not isinstance(receipts, list):
        raise ValueError("crosswalk contract requires a receipts array")
    receipts_by_id: dict[str, Mapping[str, object]] = {}
    for receipt in receipts:
        if not isinstance(receipt, dict) or not isinstance(receipt.get("receipt_id"), str) or not isinstance(receipt.get("status"), str):
            raise ValueError("malformed crosswalk receipt")
        receipt_id = str(receipt["receipt_id"])
        if receipt_id in receipts_by_id:
            raise ValueError(f"duplicate crosswalk receipt {receipt_id}")
        receipts_by_id[receipt_id] = receipt
    return receipts_by_id


def _closed_contract_problems(
    specs: Sequence[CompositeSpec],
    dossier_rows: Sequence[Mapping[str, object]],
    stat_contracts: Mapping[str, Mapping[str, object]],
) -> tuple[str, ...]:
    """Validate the whole closed ownership universe before any dispatch."""
    problems: list[str] = []
    ids = [spec.spec_id for spec in specs]
    row_keys = [key for spec in specs for key in spec.row_keys]
    if len(specs) != len(_EXPECTED_SPEC_IDS) or set(ids) != _EXPECTED_SPEC_IDS or len(set(ids)) != len(ids):
        problems.append("composite lane requires the exact five enrolled specification IDs")
    if len(row_keys) != _EXPECTED_ROW_KEY_COUNT:
        problems.append(
            f"composite lane requires exactly {_EXPECTED_ROW_KEY_COUNT} owned row keys, found {len(row_keys)}"
        )
    if len(set(row_keys)) != _EXPECTED_ROW_KEY_COUNT:
        problems.append(
            f"composite lane requires exactly {_EXPECTED_ROW_KEY_COUNT} unique row keys, found {len(set(row_keys))}"
        )
    problems.extend(validate_specs(specs, dossier_rows, stat_contracts))
    return tuple(problems)


_EXECUTOR_BY_COHORT = {
    "nflcom_l7": "nflcom_l7",
    "pfr_awards": "pfr_awards",
    "pfr_field_goals": "pfr_field_goals",
    "pfr_two_point_total": "pfr_two_point_total",
}


def _failed_result(*errors: str) -> LaneResult:
    return LaneResult((), 0, max(1, len(errors)), tuple(errors or ("local execution failed closed",)))


def _semantic_result_problems(spec: CompositeSpec, result: LaneResult) -> tuple[str, ...]:
    """Return receipt-semantic violations shared by execution, writing, and freshness."""
    problems: list[str] = []
    external_targets = set(spec.targets) - set(spec.internal_targets)
    internal_targets = set(spec.internal_targets)
    if type(result.compared_rows) is not int or result.compared_rows < 0:
        problems.append("result compared_rows must be a non-negative integer")
    if type(result.rejected_rows) is not int or result.rejected_rows < 0:
        problems.append("result rejected_rows must be a non-negative integer")
    if not isinstance(result.errors, tuple) or not all(isinstance(error, str) for error in result.errors):
        problems.append("result errors must be strings")
    if (
        not result.observations
        and not result.internal_resolutions
        and not result.errors
        and result.rejected_rows == 0
    ):
        problems.append("result cannot PASS/PENDING without required outputs")

    for item in result.observations:
        label = "observation"
        if item.spec_id != spec.spec_id:
            problems.append(f"{label} has foreign spec_id {item.spec_id!r}")
        if item.kind != spec.kind:
            problems.append(f"{label} kind {item.kind!r} is forbidden for {spec.kind}")
        if not item.targets or len(set(item.targets)) != len(item.targets):
            problems.append(f"{label} targets must be non-empty and unique")
        if not set(item.targets) <= external_targets:
            problems.append(f"{label} targets cross the external/internal target partition")
        if item.kind == "SCALAR_OBSERVATION" and spec.kind != "SCALAR_OBSERVATION":
            problems.append("constraint result cannot claim scalar credit")
        if item.status not in {"PASS", "MISMATCH", _NORMALIZED_PENDING}:
            problems.append(f"{label} has unknown status {item.status!r}")
        if type(item.compared_rows) is not int or item.compared_rows < 0:
            problems.append(f"{label} compared_rows must be a non-negative integer")
        if type(item.rejected_rows) is not int or item.rejected_rows < 0:
            problems.append(f"{label} rejected_rows must be a non-negative integer")
        if item.status in {"PASS", _NORMALIZED_PENDING} and item.rejected_rows != 0:
            problems.append(f"{label} cannot PASS/PENDING with rejected rows")
        if item.status in {"PASS", _NORMALIZED_PENDING} and item.compared_rows == 0:
            problems.append(f"{label} cannot PASS/PENDING without compared rows")

    for item in result.internal_resolutions:
        label = "internal resolution"
        if item.spec_id != spec.spec_id:
            problems.append(f"{label} has foreign spec_id {item.spec_id!r}")
        if not item.targets or len(set(item.targets)) != len(item.targets):
            problems.append(f"{label} targets must be non-empty and unique")
        if not set(item.targets) <= internal_targets:
            problems.append(f"{label} targets are not declared internal_targets")
        if item.status not in {"PASS", "MISMATCH"}:
            problems.append(f"{label} has unknown status {item.status!r}")
        if type(item.compared_rows) is not int or item.compared_rows < 0:
            problems.append(f"{label} compared_rows must be a non-negative integer")
        if type(item.rejected_rows) is not int or item.rejected_rows < 0:
            problems.append(f"{label} rejected_rows must be a non-negative integer")
        if item.status == "PASS" and item.rejected_rows != 0:
            problems.append(f"{label} cannot PASS with rejected rows")
        if item.status == "PASS" and item.compared_rows == 0:
            problems.append(f"{label} cannot PASS without compared rows")
    all_items = (*result.observations, *result.internal_resolutions)
    pass_candidate = bool(all_items) and all(item.status == "PASS" for item in all_items)
    pending_candidate = (
        bool(result.observations)
        and all(item.status == _NORMALIZED_PENDING for item in result.observations)
        and not result.internal_resolutions
    )
    if not result.errors and result.rejected_rows == 0 and (pass_candidate or pending_candidate):
        observed_external = {
            target for item in result.observations for target in item.targets
        }
        observed_internal = {
            target for item in result.internal_resolutions for target in item.targets
        }
        if observed_external != external_targets:
            problems.append("successful result does not cover every required external target")
        if pass_candidate and observed_internal != internal_targets:
            problems.append("successful result does not cover every required internal target")
    return tuple(dict.fromkeys(problems))


def _status_for_result(
    spec: CompositeSpec,
    result: LaneResult,
    *,
    crosswalk_status: str,
    dependency_errors: Sequence[str],
) -> tuple[str, LaneResult]:
    if dependency_errors:
        result = LaneResult(
            result.observations,
            result.compared_rows,
            max(result.rejected_rows, len(dependency_errors)),
            (*result.errors, *dependency_errors),
            result.internal_resolutions,
        )
    semantic_problems = _semantic_result_problems(spec, result)
    if semantic_problems:
        additions = tuple(
            f"semantic validation: {problem}"
            for problem in semantic_problems
            if f"semantic validation: {problem}" not in result.errors
        )
        result = LaneResult(
            result.observations,
            result.compared_rows,
            max(result.rejected_rows, 1),
            (*result.errors, *additions),
            result.internal_resolutions,
        )
    pending = bool(result.observations) and all(
        item.status == _NORMALIZED_PENDING for item in result.observations
    ) and not result.internal_resolutions
    passing = bool(result.observations or result.internal_resolutions) and all(
        item.status == "PASS" for item in (*result.observations, *result.internal_resolutions)
    )
    if (
        result.errors
        or result.rejected_rows != 0
        or crosswalk_status != "PASS"
    ):
        return "FAIL", result
    if pending:
        return "PENDING", result
    if passing:
        return "PASS", result
    return "FAIL", result


def execute_lane(paths: LanePaths) -> tuple[SpecStatus, ...]:
    """Run the five exact local specs independently and fail each one closed."""
    specs = load_specs()
    expected_ids = {spec.spec_id for spec in specs}
    if len(specs) != 5 or expected_ids != _EXPECTED_SPEC_IDS:
        raise ValueError("composite lane must remain closed over five unique specifications")
    try:
        dossier_rows, stat_contracts = _load_validation_inputs(paths)
        validation_error = _closed_contract_problems(specs, dossier_rows, stat_contracts)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        dossier_rows, stat_contracts = [], {}
        validation_error = (f"contract validation: {exc}",)
    try:
        crosswalk_receipts = _load_crosswalk_receipts()
        crosswalk_error: tuple[str, ...] = ()
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        crosswalk_receipts = {}
        crosswalk_error = (f"crosswalk contract: {exc}",)

    statuses: list[SpecStatus] = []
    for spec in specs:
        source_hash, source_errors = _spec_source_manifest(
            spec, paths, dossier_rows, stat_contracts, crosswalk_receipts
        )
        receipt = crosswalk_receipts.get(spec.crosswalk_receipt, {})
        required_crosswalk = str(receipt.get("status", "MISSING"))
        dependency_errors = (*validation_error, *crosswalk_error, *source_errors)
        executor_key = _EXECUTOR_BY_COHORT.get(spec.cohort)
        executor = EXECUTORS.get(executor_key) if executor_key is not None else None
        if validation_error:
            result = _failed_result(*(f"contract validation: {problem}" for problem in validation_error))
        elif executor is None:
            result = _failed_result(f"missing executor for {spec.spec_id}")
        else:
            try:
                result = executor(spec, paths)
            except Exception as exc:  # executor errors are receipt data, never implicit success
                result = _failed_result(f"executor error for {spec.spec_id}: {type(exc).__name__}: {exc}")
        status, result = _status_for_result(
            spec,
            result,
            crosswalk_status=required_crosswalk,
            dependency_errors=dependency_errors,
        )
        statuses.append(SpecStatus(
            spec_id=spec.spec_id,
            resolution_status=status,
            license_status=status,
            spec_contract_sha256=_spec_contract_sha256(spec),
            source_manifest_sha256=source_hash,
            crosswalk_statuses=((spec.crosswalk_receipt, required_crosswalk),),
            result=result,
        ))
    return tuple(statuses)


def _result_payload(result: LaneResult) -> dict[str, object]:
    return {
        "observations": [asdict(item) for item in result.observations],
        "internal_resolutions": [asdict(item) for item in result.internal_resolutions],
        "compared_rows": result.compared_rows,
        "rejected_rows": result.rejected_rows,
        "errors": list(result.errors),
    }


def _serialized_evidence(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise ValueError("evidence must be a list")
    pairs: list[tuple[str, str]] = []
    for pair in value:
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or not all(isinstance(part, str) for part in pair)
        ):
            raise ValueError("evidence entries must be string pairs")
        pairs.append((pair[0], pair[1]))
    return tuple(pairs)


def _serialized_targets(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(target, str) for target in value):
        raise ValueError("targets must be a string list")
    return tuple(value)


def _serialized_int(value: object, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be an integer")
    return value


def _lane_result_from_payload(payload: object) -> LaneResult:
    """Strictly rehydrate serialized result data before freshness derivation."""
    if not isinstance(payload, dict):
        raise ValueError("result must be an object")
    observations_raw = payload.get("observations")
    internal_raw = payload.get("internal_resolutions")
    errors_raw = payload.get("errors")
    if not isinstance(observations_raw, list) or not isinstance(internal_raw, list):
        raise ValueError("result items must be lists")
    if not isinstance(errors_raw, list) or not all(isinstance(error, str) for error in errors_raw):
        raise ValueError("result errors must be a string list")
    observations: list[LaneObservation] = []
    for item in observations_raw:
        if not isinstance(item, dict):
            raise ValueError("observation must be an object")
        value = item.get("value")
        if value is not None and type(value) is not int:
            raise ValueError("observation value must be an integer or null")
        required_strings = ("spec_id", "source", "kind", "status")
        if not all(isinstance(item.get(field), str) for field in required_strings):
            raise ValueError("observation identity fields must be strings")
        observations.append(LaneObservation(
            spec_id=str(item["spec_id"]),
            source=str(item["source"]),
            kind=str(item["kind"]),  # type: ignore[arg-type]
            targets=_serialized_targets(item.get("targets")),
            compared_rows=_serialized_int(item.get("compared_rows"), "observation compared_rows"),
            rejected_rows=_serialized_int(item.get("rejected_rows"), "observation rejected_rows"),
            status=str(item["status"]),
            value=value,
            evidence=_serialized_evidence(item.get("evidence")),
        ))
    internal: list[InternalResolution] = []
    for item in internal_raw:
        if not isinstance(item, dict):
            raise ValueError("internal resolution must be an object")
        value = item.get("value")
        if value is not None and type(value) is not int:
            raise ValueError("internal resolution value must be an integer or null")
        required_strings = ("spec_id", "source", "status")
        if not all(isinstance(item.get(field), str) for field in required_strings):
            raise ValueError("internal resolution identity fields must be strings")
        internal.append(InternalResolution(
            spec_id=str(item["spec_id"]),
            source=str(item["source"]),
            targets=_serialized_targets(item.get("targets")),
            compared_rows=_serialized_int(item.get("compared_rows"), "internal compared_rows"),
            rejected_rows=_serialized_int(item.get("rejected_rows"), "internal rejected_rows"),
            status=str(item["status"]),
            value=value,
            evidence=_serialized_evidence(item.get("evidence")),
        ))
    return LaneResult(
        observations=tuple(observations),
        compared_rows=_serialized_int(payload.get("compared_rows"), "result compared_rows"),
        rejected_rows=_serialized_int(payload.get("rejected_rows"), "result rejected_rows"),
        errors=tuple(errors_raw),
        internal_resolutions=tuple(internal),
    )


def write_lane_receipt(
    path: Path,
    *,
    contract_hash: str,
    executor_version: str,
    statuses: Sequence[SpecStatus],
) -> None:
    """Write a deterministic receipt closed over the current five specifications."""
    expected_ids = {spec.spec_id for spec in load_specs()}
    by_id = {status.spec_id: status for status in statuses}
    if len(by_id) != len(statuses) or set(by_id) != expected_ids:
        raise ValueError("lane receipt requires exactly the five enrolled specifications")
    for spec in load_specs():
        status = by_id[spec.spec_id]
        crosswalks = dict(status.crosswalk_statuses)
        if (
            len(crosswalks) != len(status.crosswalk_statuses)
            or set(crosswalks) != {spec.crosswalk_receipt}
        ):
            raise ValueError(f"status/result mismatch for {spec.spec_id}: malformed crosswalk map")
        derived, _ = _status_for_result(
            spec,
            status.result,
            crosswalk_status=crosswalks[spec.crosswalk_receipt],
            dependency_errors=(),
        )
        if status.resolution_status != derived or status.license_status != derived:
            raise ValueError(f"status/result mismatch for {spec.spec_id}")
    passing = [status for status in statuses if status.resolution_status == "PASS"]
    external = [
        asdict(item)
        for status in passing
        for item in status.result.observations
        if item.kind == "SCALAR_OBSERVATION"
    ]
    constraints = [
        asdict(item)
        for status in passing
        for item in status.result.observations
        if item.kind == "CONSTRAINT_OBSERVATION"
    ]
    internal = [asdict(item) for status in passing for item in status.result.internal_resolutions]
    payload = {
        "version": 1,
        "contract_sha256": contract_hash,
        "executor_version": executor_version,
        "external_scalar_observations": external,
        "constraint_observations": constraints,
        "internal_resolutions": internal,
        "spec_statuses": {
            spec_id: {
                "spec_id": status.spec_id,
                "spec_contract_sha256": status.spec_contract_sha256,
                "source_manifest_sha256": status.source_manifest_sha256,
                "resolution_status": status.resolution_status,
                "license_status": status.license_status,
                "crosswalk_statuses": dict(status.crosswalk_statuses),
                "result": _result_payload(status.result),
            }
            for spec_id, status in sorted(by_id.items())
        },
        "resolution_status": "PASS" if all(status.resolution_status == "PASS" for status in statuses) else "PARTIAL",
        "license_status": "PASS" if all(status.license_status == "PASS" for status in statuses) else "PARTIAL",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def spec_receipt_is_current(
    path: Path,
    *,
    spec_id: str,
    contract_hash: str,
    source_manifest_hash: str,
    executor_version: str,
    required_crosswalk_receipts: Mapping[str, str],
    require_license: bool,
) -> bool:
    """Check one enrolled specification without coupling it to unrelated specs."""
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        specs = {spec.spec_id: spec for spec in load_specs()}
        expected_ids = set(specs)
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return False
    statuses = receipt.get("spec_statuses")
    if (
        receipt.get("version") != 1
        or receipt.get("executor_version") != executor_version
        or not isinstance(statuses, dict)
        or set(statuses) != expected_ids
        or spec_id not in expected_ids
    ):
        return False
    status = statuses.get(spec_id)
    if not isinstance(status, dict):
        return False
    try:
        result = _lane_result_from_payload(status.get("result"))
    except (KeyError, TypeError, ValueError):
        return False
    required = dict(required_crosswalk_receipts)
    if (
        status.get("spec_id") != spec_id
        or status.get("spec_contract_sha256") != contract_hash
        or status.get("source_manifest_sha256") != source_manifest_hash
        or status.get("resolution_status") != "PASS"
        or status.get("crosswalk_statuses") != required
    ):
        return False
    if set(required) != {specs[spec_id].crosswalk_receipt}:
        return False
    derived, validated = _status_for_result(
        specs[spec_id],
        result,
        crosswalk_status=required[specs[spec_id].crosswalk_receipt],
        dependency_errors=(),
    )
    if derived != "PASS" or validated != result:
        return False
    return not require_license or status.get("license_status") == "PASS"


def receipt_is_current(
    path: Path = RECEIPT_PATH,
    *,
    contract_hash: str,
    source_manifest_hash: str,
    executor_version: str,
    required_crosswalk_receipts: Mapping[str, str],
) -> bool:
    """Fail closed unless every receipt dependency exactly matches."""
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        receipt.get("version") != 1
        or receipt.get("contract_sha256") != contract_hash
        or receipt.get("source_manifest_sha256") != source_manifest_hash
        or receipt.get("executor_version") != executor_version
        or receipt.get("resolution_status") != "PASS"
        or receipt.get("license_status") != "PASS"
    ):
        return False
    statuses = receipt.get("crosswalk_statuses")
    if not isinstance(statuses, dict):
        return False
    required_statuses = dict(required_crosswalk_receipts)
    if statuses != required_statuses:
        return False
    if receipt.get("crosswalk_receipts") != sorted(required_statuses):
        return False
    return all(statuses.get(name) == status for name, status in required_statuses.items())


def write_receipt(
    path: Path = RECEIPT_PATH,
    *,
    contract_hash: str,
    source_manifest_hash: str,
    executor_version: str,
    crosswalk_receipts: Mapping[str, str],
    observations: Sequence[LaneObservation],
    compared_rows: int,
    rejected_rows: int,
) -> None:
    """Write a deterministic successful receipt for an already-validated lane run."""
    if rejected_rows != 0:
        raise ValueError("cannot write PASS receipt with rejected rows")
    if any(observation.status != "PASS" for observation in observations):
        raise ValueError("cannot write PASS receipt with non-PASS observation")
    payload = {
        "version": 1,
        "contract_sha256": contract_hash,
        "source_manifest_sha256": source_manifest_hash,
        "executor_version": executor_version,
        "resolution_status": "PASS",
        "license_status": "PASS",
        "crosswalk_receipts": sorted(crosswalk_receipts),
        "crosswalk_statuses": dict(sorted(crosswalk_receipts.items())),
        "observations": [asdict(observation) for observation in observations],
        "row_counts": {"compared": compared_rows, "rejected": rejected_rows},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _register_builtin_executors() -> None:
    """Enroll the closed built-in executor set from the core entry point.

    Executor modules import this core for the contracts they implement, so the
    imports belong only after every core symbol has been defined.  Importing
    them here makes dispatch independent of whichever feature module happens
    to have been imported first.
    """
    builtin_symbols = {
        "nflcom_l7": ("scripts.sota_recon.composite_witness_nflcom", "execute_nflcom_l7"),
        "pfr_awards": ("scripts.sota_recon.composite_witness_pfr_awards", "execute_pfr_awards"),
        "pfr_field_goals": ("scripts.sota_recon.composite_witness_pfr_fg", "execute_pfr_field_goals"),
        "pfr_two_point_total": ("scripts.sota_recon.composite_witness_pfr_two_pt", "execute_pfr_two_point"),
    }
    if set(EXECUTORS) - set(builtin_symbols):
        raise RuntimeError("unknown composite executor registered before builtin bootstrap")
    for key, (module_name, symbol_name) in builtin_symbols.items():
        module = sys.modules.get(module_name)
        preexisting = module is not None
        if module is None:
            # Import errors deliberately propagate: only an already-running
            # feature may defer its own registration until module completion.
            module = importlib.import_module(module_name)
        executor = getattr(module, symbol_name, None)
        if executor is None:
            initializing = bool(getattr(getattr(module, "__spec__", None), "_initializing", False))
            if preexisting and initializing:
                continue
            raise RuntimeError(f"builtin composite executor {module_name}.{symbol_name} is missing")
        EXECUTORS[key] = executor


_register_builtin_executors()


def _production_paths() -> LanePaths:
    """Resolve production inputs strictly from the checked-in registry and local lake."""
    from .sources import latest_v26, registry

    registered = registry()
    specs = load_specs()
    required_sources = {source for spec in specs for source in spec.sources}
    required_sources.add("player_bio")
    missing = sorted(required_sources - set(registered))
    if missing:
        raise FileNotFoundError(f"required registered local sources are missing: {missing}")
    source_paths = {source: Path(registered[source].path) for source in required_sources if source != "player_bio"}
    # The licensed slug bridge is a local executor input. It must be registered
    # before production execution; tests inject their closed fixture path via
    # LanePaths and never weaken this production requirement.
    if "nflcom_slug_pfrid" not in registered:
        raise FileNotFoundError("required registered local source is missing: nflcom_slug_pfrid")
    source_paths["nflcom_slug_pfrid"] = Path(registered["nflcom_slug_pfrid"].path)
    paths = LanePaths(
        dossier=DOSSIER_PATH,
        v26=Path(latest_v26()),
        player_bio=Path(registered["player_bio"].path),
        receipt=RECEIPT_PATH,
        source_paths=source_paths,
    )
    resolved_sources = {name: path.resolve(strict=True) for name, path in paths.source_paths.items()}
    return LanePaths(
        dossier=paths.dossier.resolve(strict=True),
        v26=paths.v26.resolve(strict=True),
        player_bio=paths.player_bio.resolve(strict=True),
        receipt=paths.receipt.resolve(strict=False),
        source_paths=resolved_sources,
    )


def _resolved_test_paths(paths: LanePaths) -> LanePaths:
    return LanePaths(
        dossier=paths.dossier.resolve(strict=True),
        v26=paths.v26.resolve(strict=True),
        player_bio=paths.player_bio.resolve(strict=True),
        receipt=paths.receipt.resolve(strict=False),
        source_paths={name: path.resolve(strict=True) for name, path in paths.source_paths.items()},
    )


def main(argv: Sequence[str] | None = None, *, paths: LanePaths | None = None) -> int:
    """Execute only the closed local lane; no acquisition surface is exposed."""
    parser = argparse.ArgumentParser(description="Execute the closed local composite witness lane.")
    parser.add_argument("--execute-local", action="store_true", help="run registered local executors and write the receipt")
    args = parser.parse_args(argv)
    if not args.execute_local:
        parser.error("--execute-local is required")
    try:
        resolved = _production_paths() if paths is None else _resolved_test_paths(paths)
        statuses = execute_lane(resolved)
        write_lane_receipt(
            resolved.receipt,
            contract_hash=contract_sha256(),
            executor_version=EXECUTOR_VERSION,
            statuses=statuses,
        )
    except (OSError, ValueError, KeyError) as exc:
        print(f"local composite execution failed: {exc}", file=sys.stderr)
        return 1
    return 0 if all(status.resolution_status != "FAIL" for status in statuses) else 1


if __name__ == "__main__":
    raise SystemExit(main())
