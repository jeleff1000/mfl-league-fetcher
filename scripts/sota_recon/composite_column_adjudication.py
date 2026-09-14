"""Close only rows owned by a current, semantically valid composite spec receipt.

The five contract specifications own 126 exact dossier row keys.  A specification is
the unit of closure and escalation: one failed spec creates one blocker group, never one
queue record per raw row.  Receipt trust is delegated to ``composite_witness_lane`` so
adjudication cannot invent a weaker interpretation of PASS.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path

from . import composite_witness_lane as lane


GENERATOR = "scripts.sota_recon.composite_column_adjudication"
DISPOSITIONS_PATH = (
    lane.ROOT / "scripts" / "sota_recon" / "witness_gate" / "contracts"
    / "column_dispositions.v1.json"
)

BLOCKER_CODES_BY_SPEC: dict[str, tuple[str, ...]] = {
    "nflcom-l7-fg-buckets-v1": (
        "NO_LIKE_FOR_LIKE_NFLCOM_L7_GRAIN",
        "RESOLVED_CONFLICTING_NFLCOM_L7_GRAIN",
    ),
    "pfr-awards-v1": (
        "UNRESOLVED_POSITIVE_AWARD_MEMBERSHIPS",
        "NO_CANONICAL_AWARD_SURFACE",
    ),
    "pfr-field-goals-buckets-1-4-v1": (
        "NO_POSITIVE_PFR_FG_CANONICAL_SURFACE",
    ),
    "pfr-field-goals-50-plus-v1": (
        "PFR_50_PLUS_CONSTRAINT_INEQUALITIES",
    ),
    "pfr-two-point-total-v1": (
        "NO_POSITIVE_POSTSEASON_TWO_POINT_CANONICAL_SURFACE",
    ),
}

SETTLING_EVIDENCE_BY_SPEC: dict[str, str] = {
    "nflcom-l7-fg-buckets-v1": (
        "an executable canonical projection reproduces the resolved conditional NFL.com "
        "L7 split grain and a current local run passes without the duplicate grain"
    ),
    "pfr-awards-v1": (
        "all positive All-Pro and Pro Bowl memberships resolve collision-safely and the "
        "canonical award comparison surface is complete"
    ),
    "pfr-field-goals-buckets-1-4-v1": (
        "every positive PFR bucket observation has a like-for-like canonical aggregate"
    ),
    "pfr-field-goals-50-plus-v1": (
        "the PFR 50+ made and missed totals satisfy both canonical component inequalities"
    ),
    "pfr-two-point-total-v1": (
        "the positive postseason PFR two-point total has a canonical player-season aggregate"
    ),
}

# A projection is executable only when code explicitly enrolls it here.  The current
# empty registry is the measured boundary: parsing the NFL.com scalar exists, but no
# canonical conditional-split projection does.
CANONICAL_SPLIT_PROJECTIONS: dict[str, str] = {}


def _receipt_document(path: Path) -> Mapping[str, object] | None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return document if isinstance(document, dict) else None


def _current_inputs() -> tuple[
    dict[str, lane.CompositeSpec], dict[str, str], dict[str, dict[str, str]], lane.LanePaths | None
]:
    specs = {spec.spec_id: spec for spec in lane.load_specs()}
    source_hashes: dict[str, str] = {}
    crosswalks: dict[str, dict[str, str]] = {}
    try:
        paths = lane._production_paths()
        dossier_rows, stat_contracts = lane._load_validation_inputs(paths)
        receipts = lane._load_crosswalk_receipts()
        for spec in specs.values():
            source_hash, errors = lane._spec_source_manifest(
                spec, paths, dossier_rows, stat_contracts, receipts
            )
            if not errors:
                source_hashes[spec.spec_id] = source_hash
            crosswalks[spec.spec_id] = {
                spec.crosswalk_receipt: str(
                    receipts.get(spec.crosswalk_receipt, {}).get("status", "MISSING")
                )
            }
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        paths = None
    return specs, source_hashes, crosswalks, paths


def _validated_pass_results(
    receipt_path: Path,
) -> tuple[dict[str, lane.CompositeSpec], dict[str, lane.LaneResult], Mapping[str, object] | None, lane.LanePaths | None]:
    specs, source_hashes, crosswalks, paths = _current_inputs()
    document = _receipt_document(receipt_path)
    passing: dict[str, lane.LaneResult] = {}
    if (
        document is None
        or document.get("contract_sha256") != lane.contract_sha256()
        or document.get("executor_version") != lane.EXECUTOR_VERSION
    ):
        return specs, passing, document, paths
    statuses = document.get("spec_statuses")
    if not isinstance(statuses, dict):
        return specs, passing, document, paths
    for spec_id, spec in specs.items():
        status = statuses.get(spec_id)
        source_hash = source_hashes.get(spec_id)
        required = crosswalks.get(spec_id)
        if not isinstance(status, dict) or source_hash is None or required is None:
            continue
        if not lane.spec_receipt_is_current(
            receipt_path,
            spec_id=spec_id,
            contract_hash=lane._spec_contract_sha256(spec),
            source_manifest_hash=source_hash,
            executor_version=lane.EXECUTOR_VERSION,
            required_crosswalk_receipts=required,
            require_license=True,
        ):
            continue
        try:
            passing[spec_id] = lane._lane_result_from_payload(status.get("result"))
        except (KeyError, TypeError, ValueError):
            continue
    return specs, passing, document, paths


def spec_is_current_pass(receipt_path: Path, spec_id: str) -> bool:
    """Return whether one exact spec currently licenses closure."""
    specs, passing, _, _ = _validated_pass_results(receipt_path)
    return spec_id in specs and spec_id in passing


def _status_payload(document: Mapping[str, object] | None, spec_id: str) -> Mapping[str, object]:
    if document is None:
        return {}
    statuses = document.get("spec_statuses")
    if not isinstance(statuses, dict):
        return {}
    status = statuses.get(spec_id)
    return status if isinstance(status, dict) else {}


def _evidence_summary(status: Mapping[str, object]) -> dict[str, object]:
    result = status.get("result")
    if not isinstance(result, dict):
        return {"errors": ("receipt/result unavailable",), "observation_statuses": ()}
    errors = result.get("errors")
    observations = result.get("observations")
    error_values = tuple(str(error) for error in errors) if isinstance(errors, list) else ()
    observation_statuses = tuple(sorted({
        str(item.get("status"))
        for item in observations or ()
        if isinstance(item, dict) and item.get("status") is not None
    })) if isinstance(observations, list) else ()
    return {
        "compared_rows": result.get("compared_rows"),
        "rejected_rows": result.get("rejected_rows"),
        "errors": error_values,
        "observation_statuses": observation_statuses,
    }


def _artifact_paths(spec: lane.CompositeSpec, paths: lane.LanePaths | None) -> tuple[str, ...]:
    if paths is None:
        return ()
    try:
        return tuple(str(path) for path in lane._spec_source_paths(spec, paths))
    except (KeyError, OSError):
        return ()


def build_decisions(
    receipt_path: Path,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    """Return PASS-authorized closures and one group per non-PASS specification."""
    specs, passing, document, paths = _validated_pass_results(receipt_path)
    decisions: list[dict[str, object]] = []
    groups: list[dict[str, object]] = []
    for spec_id in sorted(specs):
        spec = specs[spec_id]
        status = _status_payload(document, spec_id)
        if spec_id in passing:
            artifacts = _artifact_paths(spec, paths)
            evidence = (
                f"receipt={receipt_path.resolve()}; contract_sha256={lane.contract_sha256()}; "
                f"specification_id={spec_id}; local_artifact_paths={artifacts!r}; "
                f"unit={spec.unit!r}; natural_grain={spec.natural_grain!r}; "
                f"aggregation_class={spec.aggregation_class!r}; "
                f"resolution_class={spec.kind!r}"
            )
            for key in spec.row_keys:
                decisions.append({
                    "key": key,
                    "disposition": "EXCLUDED_WITH_REASON",
                    "reason": (
                        "owned by a current PASS composite specification; its material is "
                        "resolved through the executable composite lane rather than a "
                        "lossy one-column scalar disposition"
                    ),
                    "evidence": evidence,
                    "spec_id": spec_id,
                })
            continue
        state = "MISSING" if document is None else str(status.get("resolution_status", "INVALID"))
        license_state = "MISSING" if document is None else str(status.get("license_status", "INVALID"))
        groups.append({
            "spec_id": spec_id,
            "row_keys": spec.row_keys,
            "affected_rows": len(spec.row_keys),
            "receipt_path": str(receipt_path.resolve()),
            "receipt_resolution_status": state,
            "receipt_license_status": license_state,
            "receipt_contract_sha256": None if document is None else document.get("contract_sha256"),
            "spec_contract_sha256": status.get("spec_contract_sha256"),
            "source_manifest_sha256": status.get("source_manifest_sha256"),
            "blocker_codes": BLOCKER_CODES_BY_SPEC[spec_id],
            "evidence_summary": _evidence_summary(status),
            "settling_condition": {
                "predicate": "CURRENT_SPEC_RECEIPT_PASS",
                "spec_id": spec_id,
                "requires_resolution_status": "PASS",
                "requires_license_status": "PASS",
                "requires_semantic_validation": True,
                "requires_current_contract_and_source_hashes": True,
                "evidence_required": SETTLING_EVIDENCE_BY_SPEC[spec_id],
            },
        })
    return tuple(decisions), tuple(groups)


def receipt_obligations(
    receipt_path: Path,
) -> tuple[
    tuple[lane.LaneObservation, ...],
    tuple[lane.LaneObservation, ...],
    tuple[lane.InternalResolution, ...],
]:
    """Return only obligations licensed by current PASS specifications."""
    _, passing, _, _ = _validated_pass_results(receipt_path)
    scalar = tuple(
        observation
        for spec_id in sorted(passing)
        for observation in passing[spec_id].observations
        if observation.kind == "SCALAR_OBSERVATION"
    )
    constraints = tuple(
        observation
        for spec_id in sorted(passing)
        for observation in passing[spec_id].observations
        if observation.kind == "CONSTRAINT_OBSERVATION"
    )
    internal = tuple(
        resolution
        for spec_id in sorted(passing)
        for resolution in passing[spec_id].internal_resolutions
    )
    return scalar, constraints, internal


def apply_decisions(receipt_path: Path, dispositions_path: Path) -> dict[str, int]:
    """Apply authorized decisions, with a byte-for-byte no-op when none exist."""
    decisions, _ = build_decisions(receipt_path)
    if not decisions:
        return {"written": 0, "hand_decisions_preserved": 0, "ledger_total": 0}
    ledger = json.loads(dispositions_path.read_text(encoding="utf-8"))
    existing = {entry["key"]: entry for entry in ledger.get("decisions", [])}
    written = preserved = 0
    for decision in decisions:
        key = str(decision["key"])
        previous = existing.get(key)
        if previous is not None and previous.get("generated_by") != GENERATOR:
            preserved += 1
            continue
        existing[key] = dict(decision) | {"generated_by": GENERATOR}
        written += 1
    ledger["decisions"] = sorted(existing.values(), key=lambda entry: entry["key"])
    dispositions_path.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    return {
        "written": written,
        "hand_decisions_preserved": preserved,
        "ledger_total": len(existing),
    }


def owned_row_keys() -> frozenset[str]:
    return frozenset(key for spec in lane.load_specs() for key in spec.row_keys)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, default=lane.RECEIPT_PATH)
    parser.add_argument(
        "--apply",
        nargs="?",
        type=Path,
        const=DISPOSITIONS_PATH,
        help="apply to the canonical disposition ledger, or to an optional explicit path",
    )
    args = parser.parse_args(argv)
    decisions, groups = build_decisions(args.receipt)
    print(f"authorized decisions: {len(decisions)}")
    print(f"blocker groups: {len(groups)} ({sum(group['affected_rows'] for group in groups)} rows)")
    if args.apply is not None:
        print(apply_decisions(args.receipt, args.apply))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
