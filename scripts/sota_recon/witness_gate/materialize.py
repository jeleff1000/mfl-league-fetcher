from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .gate_planes import GateManifest
from .models import GatePlane


@dataclass(frozen=True)
class CandidateArtifact:
    candidate_version: str
    path: Path
    row_count: int
    artifact_fingerprint: str
    natural_keys: tuple[str, ...]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def materialize_candidate(
    release_root: str | Path,
    candidate_version: str,
    rows: Iterable[dict[str, Any]],
    *,
    natural_keys: tuple[str, ...],
) -> CandidateArtifact:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", candidate_version):
        raise ValueError("candidate_version contains unsafe path characters")
    materialized = list(rows)
    required = set(natural_keys) | {"evidence_id", "proof_root"}
    for index, row in enumerate(materialized):
        missing = {field for field in required if field not in row or row[field] in {None, ""}}
        if missing:
            raise ValueError(f"candidate row {index} is missing required fields: {sorted(missing)}")
    seen: set[tuple[Any, ...]] = set()
    for row in materialized:
        key = tuple(row[field] for field in natural_keys)
        if key in seen:
            raise ValueError(f"duplicate natural key: {key}")
        seen.add(key)

    root = Path(release_root).resolve()
    candidate_dir = root / "candidates" / candidate_version
    candidate_dir.mkdir(parents=True, exist_ok=True)
    path = candidate_dir / "promoted_atoms.parquet"
    if path.exists():
        raise FileExistsError(f"candidate artifact already exists: {path}")
    table = pa.Table.from_pylist(materialized)
    pq.write_table(table, path, compression="zstd")
    return CandidateArtifact(
        candidate_version=candidate_version,
        path=path,
        row_count=table.num_rows,
        artifact_fingerprint=_file_sha256(path),
        natural_keys=natural_keys,
    )


def advance_current(
    release_root: str | Path,
    candidate: CandidateArtifact,
    release_manifest: GateManifest,
) -> bool:
    if release_manifest.plane is not GatePlane.RELEASE:
        raise ValueError("only a release gate manifest can advance CURRENT")
    if release_manifest.status != "pass":
        return False
    if not candidate.path.exists() or _file_sha256(candidate.path) != candidate.artifact_fingerprint:
        raise ValueError("candidate artifact is missing or its fingerprint changed")

    root = Path(release_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    pointer_path = root / "CURRENT.json"
    if pointer_path.exists():
        current = json.loads(pointer_path.read_text(encoding="utf-8"))
        if (
            current.get("candidate_version") == candidate.candidate_version
            and current.get("artifact_fingerprint") == candidate.artifact_fingerprint
        ):
            return False
    payload = {
        "candidate_version": candidate.candidate_version,
        "artifact_path": str(candidate.path),
        "artifact_fingerprint": candidate.artifact_fingerprint,
        "row_count": candidate.row_count,
        "natural_keys": list(candidate.natural_keys),
        "release_manifest_fingerprint": release_manifest.manifest_fingerprint,
    }
    temporary = root / f".CURRENT.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, pointer_path)
    return True
