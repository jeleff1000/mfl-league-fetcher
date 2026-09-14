"""Inventory registered and physical PFR lake tables without mutating source data."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from .sources import registry

ROOT = Path(r"D:/league-history-data/nfl/raw/pfr")
OUT = Path(__file__).resolve().parents[2] / "docs" / "pfr-lake-inventory.json"


def _logical_groups() -> dict[Path, list[Path]]:
    groups: dict[Path, list[Path]] = defaultdict(list)
    for path in ROOT.rglob("*.parquet"):
        rel = path.relative_to(ROOT)
        parts = rel.parts
        if parts[:2] in (("players", "tables"), ("boxscores", "tables"), ("context", "tables")):
            group = ROOT.joinpath(*parts[:3])
        elif parts[:2] == ("context", "pfr_context_smoke"):
            continue
        else:
            group = ROOT.joinpath(parts[0])
        groups[group].append(path)
    return groups


def _registered_by_path() -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for key, source in registry(include_subject=False).items():
        out[str(Path(source.path).parent).lower()].append(key)
    return out


def _sample_metadata(files: list[Path]) -> dict:
    combined = next((p for p in files if p.name == "_combined.parquet"), files[0])
    parquet = pq.ParquetFile(combined)
    names = parquet.schema.names
    # Production table directories have a canonical _combined.parquet.  Partition-only
    # directories can contain tens of thousands of files; do not open every shard during
    # inventory.  Their physical partition count is recorded and row count is explicitly
    # deferred to the table audit pass.
    row_count = parquet.metadata.num_rows if combined.name == "_combined.parquet" else None
    sample = {"file": str(combined), "columns": names, "row_count": row_count}
    # Subtable values are audited from the complete table in the next pass.  Inventory
    # records the available discriminator columns without scanning large combined files.
    sample["subtable_discriminator_columns"] = [
        key for key in ("table_id", "table_caption", "page_kind", "page_key", "year", "year_id", "season")
        if key in names
    ]
    return sample


def build() -> dict:
    registered = _registered_by_path()
    groups = _logical_groups()
    tables = []
    for group, files in sorted(groups.items(), key=lambda item: str(item[0]).lower()):
        keys = registered.get(str(group).lower(), [])
        record = {
            "physical_path": str(group),
            "relative_path": str(group.relative_to(ROOT)),
            "registered_source_keys": sorted(keys),
            "registration_status": "REGISTERED" if keys else "PHYSICAL_UNREGISTERED",
            "parquet_file_count": len(files),
        }
        try:
            record["sample"] = _sample_metadata(files)
        except Exception as exc:
            record["sample_error"] = repr(exc)
        tables.append(record)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake_root": str(ROOT),
        "registered_pfr_source_count": sum(1 for source in registry(include_subject=False).values() if source.lineage == "pfr"),
        "logical_physical_table_count": len(tables),
        "physical_unregistered_count": sum(t["registration_status"] == "PHYSICAL_UNREGISTERED" for t in tables),
        "tables": tables,
    }


def main() -> int:
    result = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: result[k] for k in result if k != "tables"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
