#!/usr/bin/env python3
"""Emit a deterministic snapshot of the canonical player supertable schema.

Reads only the parquet footer, so it never loads the ~700MB release. The
snapshot is the exhaustive column INVENTORY for the coverage audit; actual data
availability comes from profile_local_coverage.py.

    python scripts/player_column_coverage/extract_local_schema.py \
        --parquet <release>/tables/nfl_player_stats_all.parquet \
        --release-id <release id> \
        --output docs/player-column-coverage/source-schema.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


def extract_schema(parquet_path: Path, release_id: str) -> dict[str, Any]:
    """Return the ordered physical column inventory plus a stable fingerprint."""
    schema = pq.ParquetFile(parquet_path).schema_arrow
    columns = [
        {"ordinal": index + 1, "name": field.name, "type": str(field.type)}
        for index, field in enumerate(schema)
    ]
    fingerprint_input = json.dumps(columns, separators=(",", ":"), ensure_ascii=True)
    return {
        "release_id": release_id,
        "source_file": parquet_path.name,
        "schema_fingerprint": hashlib.sha256(fingerprint_input.encode("utf-8")).hexdigest(),
        "column_count": len(columns),
        "columns": columns,
    }


def write_json(payload: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    payload = extract_schema(args.parquet, args.release_id)
    write_json(payload, args.output)
    print(f"{payload['column_count']} columns -> {args.output}")
    print(f"fingerprint {payload['schema_fingerprint'][:16]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
