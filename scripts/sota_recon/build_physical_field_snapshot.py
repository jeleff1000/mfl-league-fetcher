"""Immutable physical source snapshot (master plan section 25.15 item A1).

For every registered source (``sources.registry(include_subject=True)``) this
captures the physical layer exactly as it exists on disk right now:

- resolved file list (registry paths are files today; directories and glob
  patterns are resolved defensively rather than crashing),
- per-file sha256 (streamed), byte size, and mtime,
- per-file parquet FOOTER schema (column name -> type) and footer row count —
  metadata only, no data rows are scanned,
- a per-source ``source_snapshot_id`` = sha256 over the sorted per-file hashes.

Null/distinct counts are intentionally NOT computed here; they are deferred to
the census layer and marked ``deferred_to_census``.

Missing paths are reported as findings, never raised.
"""
from __future__ import annotations

import argparse
import glob as _glob
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from sources import registry

ROOT = Path(__file__).parents[2]
DEFAULT_OUT = ROOT / "docs" / "physical-field-snapshot-v0.json"

_CHUNK = 1 << 22  # 4 MiB streaming chunks


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_files(raw_path: str) -> list[Path]:
    """Resolve a registry path to concrete files (file, directory, or glob)."""
    path = Path(raw_path)
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(path.rglob("*.parquet"))
    if any(ch in raw_path for ch in "*?["):
        return sorted(Path(p) for p in _glob.glob(raw_path) if Path(p).is_file())
    return []


def _footer_schema(path: Path) -> tuple[dict[str, str], int | None]:
    """Parquet footer schema + footer row count. Metadata only; no row scans."""
    parquet_file = pq.ParquetFile(path)
    schema = parquet_file.schema_arrow
    columns = {field.name: str(field.type) for field in schema}
    row_count = parquet_file.metadata.num_rows if parquet_file.metadata is not None else None
    return columns, row_count


def build(skip_hash: bool = False) -> dict:
    source_entries = []
    findings = []
    n_files = 0
    n_columns_total = 0
    total_bytes = 0
    for key, source in sorted(registry(include_subject=True).items()):
        files = _resolve_files(source.path)
        if not files:
            findings.append(
                {
                    "source_id": key,
                    "finding": "missing_path",
                    "path": source.path,
                }
            )
            source_entries.append(
                {
                    "source_id": key,
                    "path": source.path,
                    "status": "missing",
                    "files": [],
                    "column_count": 0,
                    "source_snapshot_id": None,
                }
            )
            continue
        file_entries = []
        union_columns: dict[str, str] = {}
        for file_path in files:
            stat = file_path.stat()
            entry = {
                "path": str(file_path),
                "size_bytes": stat.st_size,
                "mtime_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "sha256": None if skip_hash else _sha256_file(file_path),
                "null_counts": "deferred_to_census",
                "distinct_counts": "deferred_to_census",
            }
            try:
                columns, row_count = _footer_schema(file_path)
                entry["columns"] = columns
                entry["footer_row_count"] = row_count
                for name, dtype in columns.items():
                    union_columns.setdefault(name, dtype)
            except Exception as exc:
                entry["schema_error"] = f"{type(exc).__name__}: {exc}"
                findings.append(
                    {
                        "source_id": key,
                        "finding": "schema_error",
                        "path": str(file_path),
                        "error": entry["schema_error"],
                    }
                )
            total_bytes += stat.st_size
            n_files += 1
            file_entries.append(entry)
        hashes = sorted(e["sha256"] for e in file_entries if e["sha256"])
        snapshot_id = hashlib.sha256("".join(hashes).encode("ascii")).hexdigest() if hashes else None
        n_columns_total += len(union_columns)
        source_entries.append(
            {
                "source_id": key,
                "path": source.path,
                "status": "captured",
                "year_min": source.year_min,
                "year_max": source.year_max,
                "join": source.join,
                "lineage": source.lineage,
                "witness_class": source.witness_class,
                "files": file_entries,
                "column_count": len(union_columns),
                "source_snapshot_id": snapshot_id,
            }
        )
    return {
        "snapshot_version": "v0",
        "generated_utc": datetime.now(tz=timezone.utc).isoformat(),
        "scope_note": (
            "Parquet footer metadata + streamed file hashes only; no data rows scanned. "
            "Null/distinct counts deferred_to_census."
        ),
        "summary": {
            "n_sources": len(source_entries),
            "n_files": n_files,
            "n_columns_total": n_columns_total,
            "total_bytes": total_bytes,
            "n_findings": len(findings),
        },
        "findings": findings,
        "sources": source_entries,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--skip-hash", action="store_true", help="debug only: skip sha256 hashing")
    args = ap.parse_args()
    result = build(skip_hash=args.skip_hash)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result["summary"], indent=2))
    for finding in result["findings"]:
        print("FINDING:", json.dumps(finding))
