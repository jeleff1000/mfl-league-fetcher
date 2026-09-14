from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from .census import load_census
from .field_registry import audit_field_closure, load_field_registry
from .gate_planes import Finding, GateManifest, build_gate_manifests
from .inventory import inventory_dataset
from .models import GatePlane
from .pbp_contracts import load_pbp_contract_registry, validate_pbp_rollup_schema
from .position_validation import validate_position_frame


def _write_manifest(path: Path, manifest: GateManifest) -> None:
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def _observed_schema(lake_root: Path, physical_globs: tuple[str, ...]) -> set[str]:
    fields: set[str] = set()
    for pattern in physical_globs:
        for item in glob.glob(str(lake_root / pattern), recursive=True):
            path = Path(item)
            if path.is_file() and path.suffix.casefold() == ".parquet":
                fields.update(pq.ParquetFile(path).schema_arrow.names)
    return fields


def run_all(
    *,
    lake_root: Path,
    source_census_path: Path,
    field_registry_path: Path,
    output_dir: Path,
    candidate_version: str,
    pbp_rollup_path: Path | None = None,
    pbp_contract_path: Path | None = None,
    position_frame_path: Path | None = None,
    position_col: str = "position",
    nfl_position_col: str = "nfl_position",
    unresolved_collision_ids: tuple[str, ...] = (),
) -> dict[GatePlane, GateManifest]:
    census = load_census(source_census_path)
    field_registry = load_field_registry(field_registry_path)
    global_findings: list[Finding] = []
    source_findings: list[Finding] = []
    candidate_findings: list[Finding] = []

    local_ids = {item.dataset_id for item in census.datasets if item.producer.kind == "local_lake"}
    for dataset_id in sorted(local_ids - set(field_registry.datasets)):
        global_findings.append(
            Finding(
                code="DATASET_WITHOUT_FIELD_CONTRACT",
                severity="fail",
                scope={"dataset_id": dataset_id},
                evidence_refs=(census.fingerprint,),
                remediation="add a versioned field contract",
            )
        )
    for dataset_id in sorted(set(field_registry.datasets) - local_ids):
        global_findings.append(
            Finding(
                code="FIELD_CONTRACT_WITHOUT_DATASET",
                severity="fail",
                scope={"dataset_id": dataset_id},
                evidence_refs=(census.fingerprint,),
                remediation="register the dataset or remove the stale field contract",
            )
        )

    for dataset in census.datasets:
        if dataset.producer.kind == "ff_assets":
            if dataset.producer.pin_status == "pending_pin":
                source_findings.append(
                    Finding(
                        code="SOURCE_MANIFEST_PIN_PENDING",
                        severity="fail",
                        scope={"dataset_id": dataset.dataset_id},
                        evidence_refs=(census.fingerprint,),
                        remediation="pin the completed ff-assets artifact manifest",
                    )
                )
            continue
        field_contract = field_registry.datasets.get(dataset.dataset_id)
        if field_contract is None:
            continue
        inventory = inventory_dataset(
            lake_root,
            dataset,
            registered_fields=set(field_contract.reviewed_fields),
        )
        for finding in inventory.findings:
            if finding.code in {"UNREGISTERED_FIELD", "SCHEMA_DRIFT"}:
                global_findings.append(finding)
            else:
                source_findings.append(finding)
        observed = _observed_schema(lake_root, dataset.physical_globs)
        global_findings.extend(audit_field_closure(field_contract, observed).findings)

    if pbp_rollup_path is not None and pbp_contract_path is not None:
        if not pbp_rollup_path.exists():
            source_findings.append(
                Finding(
                    code="PBP_ROLLUP_MISSING",
                    severity="fail",
                    scope={"path": str(pbp_rollup_path)},
                    evidence_refs=(),
                    remediation="rebuild the pinned PBP rollup",
                )
            )
        else:
            pbp_registry = load_pbp_contract_registry(pbp_contract_path)
            observed = set(pq.ParquetFile(pbp_rollup_path).schema_arrow.names)
            source_findings.extend(validate_pbp_rollup_schema(observed, pbp_registry))

    if (pbp_rollup_path is None) != (pbp_contract_path is None):
        raise ValueError("--pbp-rollup and --pbp-contracts must be supplied together")

    if position_frame_path is not None:
        position_findings = validate_position_frame(
            _read_frame(position_frame_path),
            position_col=position_col,
            nfl_position_col=nfl_position_col,
        )
        for finding in position_findings:
            if finding.scope["gate_plane"] == GatePlane.SOURCE_HEALTH.value:
                source_findings.append(finding)
            elif finding.scope["gate_plane"] == GatePlane.CANDIDATE_PROMOTION.value:
                candidate_findings.append(finding)
            else:
                raise ValueError(f"unknown position finding plane: {finding.scope['gate_plane']!r}")

    version = census.source_universe_version
    manifests = build_gate_manifests(
        source_universe_version=version,
        candidate_version=candidate_version,
        global_findings=global_findings,
        source_findings=source_findings,
        candidate_findings=candidate_findings,
        unresolved_collision_ids=unresolved_collision_ids,
    )
    _write_gate_outputs(
        output_dir,
        manifests,
        source_universe_version=version,
        candidate_version=candidate_version,
    )
    return manifests


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.casefold()
    if suffix == ".parquet":
        return pq.read_table(path).to_pandas()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, lines=suffix == ".jsonl")
    raise ValueError(f"unsupported position frame format: {path}")


def _unresolved_collisions(path: Path) -> tuple[str, ...]:
    frame = _read_frame(path)
    missing = sorted({"collision_id", "status"} - set(frame.columns))
    if missing:
        raise ValueError(f"collision ledger missing columns: {missing}")
    rows = frame[["collision_id", "status"]].to_dict("records")
    collision_ids: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if pd.isna(row["collision_id"]):
            raise ValueError("collision ledger contains a blank collision_id")
        if pd.isna(row["status"]):
            raise ValueError("collision ledger contains a blank status")
        collision_id = str(row["collision_id"]).strip()
        status = str(row["status"]).strip()
        if not collision_id:
            raise ValueError("collision ledger contains a blank collision_id")
        if collision_id in seen:
            raise ValueError(f"collision ledger contains duplicate collision_id: {collision_id}")
        if status not in {"resolved", "collision_review"}:
            raise ValueError(f"collision ledger contains unknown status: {status!r}")
        seen.add(collision_id)
        if status == "collision_review":
            collision_ids.append(collision_id)
    return tuple(sorted(collision_ids))


def _write_gate_outputs(
    output_dir: Path,
    manifests: dict[GatePlane, GateManifest],
    *,
    source_universe_version: str,
    candidate_version: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for plane, manifest in manifests.items():
        _write_manifest(output_dir / f"{plane.value}.json", manifest)
    rollup = {
        "source_universe_version": source_universe_version,
        "candidate_version": candidate_version,
        "release_status": manifests[GatePlane.RELEASE].status,
        "gates": {
            plane.value: {
                "status": manifest.status,
                "manifest_fingerprint": manifest.manifest_fingerprint,
                "finding_count": len(manifest.findings),
            }
            for plane, manifest in manifests.items()
        },
    }
    (output_dir / "witness_gate_rollup.json").write_text(
        json.dumps(rollup, indent=2, sort_keys=True), encoding="utf-8"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the versioned universal witness research gate")
    parser.add_argument(
        "command",
        choices=(
            "inventory",
            "global-health",
            "source-health",
            "promote",
            "release",
            "run-all",
            "validate-positions",
        ),
    )
    parser.add_argument("--lake-root", type=Path)
    parser.add_argument("--source-census", type=Path)
    parser.add_argument("--field-registry", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--candidate-version", default="candidate-v1")
    parser.add_argument("--pbp-rollup", type=Path)
    parser.add_argument("--pbp-contracts", type=Path)
    parser.add_argument("--position-frame", type=Path)
    parser.add_argument("--position-col", default="position")
    parser.add_argument("--nfl-position-col", default="nfl_position")
    parser.add_argument("--unresolved-collision-id", action="append", default=[])
    parser.add_argument("--collision-ledger", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    collision_ids = set(args.unresolved_collision_id)
    if args.collision_ledger is not None:
        collision_ids.update(_unresolved_collisions(args.collision_ledger))
    if args.command == "validate-positions":
        if args.position_frame is None:
            parser.error("validate-positions requires --position-frame")
        findings = validate_position_frame(
            _read_frame(args.position_frame),
            position_col=args.position_col,
            nfl_position_col=args.nfl_position_col,
        )
        manifests = build_gate_manifests(
            source_universe_version="position-validation-v1",
            candidate_version=args.candidate_version,
            source_findings=(
                item
                for item in findings
                if item.scope["gate_plane"] == GatePlane.SOURCE_HEALTH.value
            ),
            candidate_findings=(
                item
                for item in findings
                if item.scope["gate_plane"] == GatePlane.CANDIDATE_PROMOTION.value
            ),
            unresolved_collision_ids=tuple(sorted(collision_ids)),
        )
        _write_gate_outputs(
            args.output_dir,
            manifests,
            source_universe_version="position-validation-v1",
            candidate_version=args.candidate_version,
        )
        selected = manifests[GatePlane.RELEASE]
        print(f"{selected.plane.value}: {selected.status} ({len(selected.findings)} findings)")
        return 1 if selected.status == "fail" else 0
    required = {
        "--lake-root": args.lake_root,
        "--source-census": args.source_census,
        "--field-registry": args.field_registry,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        parser.error(f"{args.command} requires {', '.join(missing)}")
    manifests = run_all(
        lake_root=args.lake_root,
        source_census_path=args.source_census,
        field_registry_path=args.field_registry,
        output_dir=args.output_dir,
        candidate_version=args.candidate_version,
        pbp_rollup_path=args.pbp_rollup,
        pbp_contract_path=args.pbp_contracts,
        position_frame_path=args.position_frame,
        position_col=args.position_col,
        nfl_position_col=args.nfl_position_col,
        unresolved_collision_ids=tuple(sorted(collision_ids)),
    )
    selected_plane = {
        "global-health": GatePlane.GLOBAL_RESEARCH_HEALTH,
        "source-health": GatePlane.SOURCE_HEALTH,
        "promote": GatePlane.CANDIDATE_PROMOTION,
        "release": GatePlane.RELEASE,
    }.get(args.command, GatePlane.RELEASE)
    selected = manifests[selected_plane]
    print(f"{selected.plane.value}: {selected.status} ({len(selected.findings)} findings)")
    if args.command in {"run-all", "inventory"}:
        return 1 if any(manifest.status == "fail" for manifest in manifests.values()) else 0
    return 1 if selected.status == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
