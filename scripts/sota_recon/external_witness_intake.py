"""Validate ff-assets witness artifacts and copy them into immutable local intake.

This is deliberately separate from any Fly or super-table writer. Intake preserves
raw evidence and normalized records; reconciliation decides whether a witness can
support an identity candidate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_artifact(path: Path) -> dict:
    manifest_path = path / "ARTIFACT_MANIFEST.json"
    if not manifest_path.is_file():
        raise ValueError(f"manifest missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported manifest schema version")
    for entry in manifest.get("files", []):
        rel = Path(entry["path"])
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"unsafe manifest path: {rel}")
        payload = path / rel
        if not payload.is_file():
            raise ValueError(f"manifest file missing: {rel.as_posix()}")
        if _sha256(payload) != entry["sha256"]:
            raise ValueError(f"checksum mismatch: {rel.as_posix()}")
    return manifest


def ingest_artifacts(artifact_dirs: list[Path], intake_root: Path) -> dict:
    verified = [(path, verify_artifact(path)) for path in artifact_dirs]
    destinations: list[tuple[Path, Path, dict]] = []
    for source_path, manifest in verified:
        destination = (
            intake_root
            / "artifacts"
            / str(manifest["artifact_run_id"])
            / str(manifest["source"])
            / str(manifest["dataset"]).replace(":", "_")
            / f"shard-{int(manifest['shard_id']):05d}-of-{int(manifest['shard_count']):05d}"
        )
        if destination.exists():
            raise FileExistsError(f"immutable artifact already ingested: {destination}")
        destinations.append((source_path, destination, manifest))

    frames: list[pd.DataFrame] = []
    copied: list[dict] = []
    for source_path, destination, manifest in destinations:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_path, destination)
        records_path = destination / "records.parquet"
        record_count = 0
        if records_path.is_file():
            frame = pd.read_parquet(records_path)
            frame["intake_artifact_dir"] = str(destination)
            frame["intake_artifact_run_id"] = str(manifest["artifact_run_id"])
            frames.append(frame)
            record_count = len(frame)
        copied.append({"source": str(manifest["source"]), "dataset": str(manifest["dataset"]), "destination": str(destination), "record_count": record_count})

    existing_index = intake_root / "EXTERNAL_WITNESS_INDEX.parquet"
    all_frames = ([pd.read_parquet(existing_index)] if existing_index.is_file() else []) + frames
    if all_frames:
        pd.concat(all_frames, ignore_index=True, sort=False).to_parquet(existing_index, index=False)
    summary = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_count": len(copied),
        "record_count": sum(item["record_count"] for item in copied),
        "artifacts": copied,
        "index_path": str(existing_index),
    }
    intake_root.mkdir(parents=True, exist_ok=True)
    (intake_root / "LATEST_INTAKE_SUMMARY.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifacts", nargs="+", type=Path)
    parser.add_argument("--intake-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(ingest_artifacts(args.artifacts, args.intake_root), indent=2))


if __name__ == "__main__":
    main()
