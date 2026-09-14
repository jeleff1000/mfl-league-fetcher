from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import duckdb


@dataclass(frozen=True)
class VerifiedShard:
    source: str
    dataset: str
    shard_id: int
    shard_count: int
    run_id: str
    artifact_dir: Path
    manifest_path: Path
    manifest_sha256: str


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _resolve_manifest_payload(artifact_dir: Path, relative: Path) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"unsafe manifest path: {relative}")
    try:
        artifact_root = artifact_dir.resolve(strict=True)
        payload = (artifact_root / relative).resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise ValueError(f"manifest file missing: {artifact_dir / relative}") from exc
    if not payload.is_relative_to(artifact_root):
        raise ValueError(f"manifest payload escapes artifact root: {relative}")
    if not payload.is_file():
        raise ValueError(f"manifest file missing: {payload}")
    return payload


def _verify_manifest(path: Path, expected_run_id: str) -> VerifiedShard:
    try:
        manifest_path = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise ValueError(f"manifest file missing: {path}") from exc
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError(f"unsupported manifest schema: {path}")
    if str(manifest.get("artifact_run_id")) != expected_run_id:
        raise ValueError(f"artifact run mismatch: {path}")
    artifact_dir = manifest_path.parent.resolve(strict=True)
    for entry in manifest.get("files", []):
        relative = Path(entry["path"])
        payload = _resolve_manifest_payload(artifact_dir, relative)
        if payload.stat().st_size != int(entry["bytes"]):
            raise ValueError(f"size mismatch: {payload}")
        if _sha256(payload) != entry["sha256"]:
            raise ValueError(f"checksum mismatch: {payload}")
    return VerifiedShard(
        source=str(manifest["source"]),
        dataset=str(manifest["dataset"]),
        shard_id=int(manifest["shard_id"]),
        shard_count=int(manifest["shard_count"]),
        run_id=expected_run_id,
        artifact_dir=artifact_dir,
        manifest_path=manifest_path,
        manifest_sha256=_sha256(manifest_path),
    )


def _dataset_profile(shards: list[VerifiedShard]) -> tuple[int, list[int], list[str]]:
    record_paths = sorted(
        (shard.artifact_dir / "records.parquet").as_posix()
        for shard in shards
        if (shard.artifact_dir / "records.parquet").is_file()
    )
    if not record_paths:
        return 0, [], []
    connection = duckdb.connect()
    try:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)", [record_paths]
        ).fetchone()[0]
        columns = [
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)", [record_paths]
            ).fetchall()
        ]
        year_field = "season" if "season" in columns else "year" if "year" in columns else None
        years: list[int] = []
        if year_field is not None:
            years = [
                row[0]
                for row in connection.execute(
                    f'SELECT DISTINCT CAST("{year_field}" AS INTEGER) '
                    "FROM read_parquet(?, union_by_name=true) "
                    f'WHERE "{year_field}" IS NOT NULL ORDER BY 1',
                    [record_paths],
                ).fetchall()
            ]
    finally:
        connection.close()
    return int(row_count), years, columns


def import_harvest_run(
    downloads_root: Path,
    lake_root: Path,
    *,
    expected_run_id: str,
    expected_shards: int,
    expected_datasets: tuple[tuple[str, str], ...] | None = None,
    coverage_mode: Literal["complete", "verified_partitions"] = "complete",
) -> dict[str, Any]:
    if coverage_mode not in {"complete", "verified_partitions"}:
        raise ValueError(f"unsupported coverage mode: {coverage_mode}")
    if expected_shards <= 0:
        raise ValueError("expected_shards must be positive")
    if coverage_mode == "verified_partitions" and not expected_datasets:
        raise ValueError("verified_partitions requires an explicit expected_datasets allowlist")
    manifest_paths = sorted(downloads_root.rglob("ARTIFACT_MANIFEST.json"))
    if not manifest_paths:
        raise ValueError("no artifact manifests found")
    verified = [_verify_manifest(path, expected_run_id) for path in manifest_paths]
    grouped: dict[tuple[str, str], list[VerifiedShard]] = defaultdict(list)
    for shard in verified:
        grouped[(shard.source, shard.dataset)].append(shard)
    if expected_datasets is not None and set(grouped) != set(expected_datasets):
        raise ValueError(f"dataset set mismatch: observed={sorted(grouped)} expected={sorted(expected_datasets)}")

    for key, shards in grouped.items():
        observed_ids = [item.shard_id for item in shards]
        observed = set(observed_ids)
        expected = set(range(expected_shards))
        if len(observed_ids) != len(observed):
            raise ValueError(f"duplicate shard IDs for {key}: observed={sorted(observed_ids)}")
        if any(item.shard_count != expected_shards for item in shards):
            raise ValueError(f"shard count mismatch for {key}")
        invalid = observed - expected
        if invalid:
            raise ValueError(f"shard IDs out of range for {key}: observed={sorted(invalid)}")
        if coverage_mode == "complete" and observed != expected:
            raise ValueError(f"complete shard set required for {key}: observed={sorted(observed)}")

    manifest_hashes = sorted(item.manifest_sha256 for item in verified)
    aggregate = hashlib.sha256("\n".join(manifest_hashes).encode("ascii")).hexdigest()
    manifest_pin = f"ffassets-run:{expected_run_id}:sha256:{aggregate}"
    dataset_summaries: list[dict[str, Any]] = []
    for (source, dataset), shards in sorted(grouped.items()):
        observed_shards = sorted(item.shard_id for item in shards)
        missing_shards = sorted(set(range(expected_shards)) - set(observed_shards))
        coverage_complete = not missing_shards
        target = lake_root / "ff_assets" / source / dataset / expected_run_id
        staging = target.with_name(f"{target.name}.importing")
        if target.exists() or staging.exists():
            raise FileExistsError(f"immutable import target already exists: {target}")
        (staging / "shards").mkdir(parents=True)
        for shard in sorted(shards, key=lambda item: item.shard_id):
            shutil.copytree(shard.artifact_dir, staging / "shards" / f"shard-{shard.shard_id}")
        row_count, years, columns = _dataset_profile(shards)
        summary = {
            "source": source,
            "dataset": dataset,
            "run_id": expected_run_id,
            "manifest_pin": manifest_pin,
            "shard_count": len(shards),
            "expected_shards": expected_shards,
            "observed_shards": observed_shards,
            "missing_shards": missing_shards,
            "coverage_complete": coverage_complete,
            "positive_claims_admissible": True,
            "absence_claims_admissible": coverage_complete,
            "row_count": row_count,
            "observed_years": years,
            "columns": columns,
            "shard_manifests": [
                {
                    "shard_id": item.shard_id,
                    "sha256": item.manifest_sha256,
                    "source_path": str(item.manifest_path),
                }
                for item in sorted(shards, key=lambda value: value.shard_id)
            ],
        }
        (staging / "IMPORT_MANIFEST.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        staging.rename(target)
        summary["lake_path"] = str(target)
        dataset_summaries.append(summary)

    run_summary = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "run_id": expected_run_id,
        "manifest_pin": manifest_pin,
        "datasets": dataset_summaries,
    }
    run_dir = lake_root / "ff_assets" / "runs" / expected_run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "IMPORT_RUN_MANIFEST.json").write_text(json.dumps(run_summary, indent=2) + "\n", encoding="utf-8")
    return run_summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify and import an ff-assets harvest run.")
    parser.add_argument("--downloads-root", type=Path, required=True)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-shards", type=int, default=20)
    parser.add_argument(
        "--coverage-mode",
        choices=("complete", "verified_partitions"),
        default="complete",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        metavar="SOURCE:DATASET",
        help="Expected dataset; repeat to admit multiple datasets.",
    )
    args = parser.parse_args()
    if args.coverage_mode == "verified_partitions" and not args.dataset:
        parser.error("verified_partitions requires at least one --dataset allowlist entry")
    if args.dataset:
        expected_datasets_list: list[tuple[str, str]] = []
        for value in args.dataset:
            source, separator, dataset = value.partition(":")
            if not separator or not source or not dataset:
                parser.error(f"invalid --dataset {value!r}; expected SOURCE:DATASET")
            expected_datasets_list.append((source, dataset))
        expected_datasets = tuple(expected_datasets_list)
    else:
        expected_datasets = (
            ("nflcom", "team_season_roster"),
            ("statscrew", "team_season_roster"),
            ("profootballarchives", "player_game_participation"),
        )
    result = import_harvest_run(
        args.downloads_root,
        args.lake_root,
        expected_run_id=args.run_id,
        expected_shards=args.expected_shards,
        expected_datasets=expected_datasets,
        coverage_mode=args.coverage_mode,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
