#!/usr/bin/env python3
"""Compare two offline DuckDB files with bounded logical evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import duckdb


def _qident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def inventory(path: Path, *, max_checksum_rows: int) -> dict:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        tables = conn.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE database_name=current_database() AND NOT internal "
            "ORDER BY schema_name, table_name"
        ).fetchall()
        evidence = {}
        for schema, table in tables:
            relation = f"{_qident(schema)}.{_qident(table)}"
            columns = conn.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
            schema_signature = [
                {"name": str(row[0]), "type": str(row[1]), "nullable": str(row[2])}
                for row in columns
            ]
            rows = int(conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])
            checksum = None
            checksum_status = "skipped_row_limit"
            if rows <= max_checksum_rows:
                args = ", ".join(_qident(str(row[0])) for row in columns)
                xor_hash, sum_hash = conn.execute(
                    f"SELECT COALESCE(BIT_XOR(HASH({args})), 0), "
                    f"COALESCE(SUM(HASH({args})), 0) FROM {relation}"
                ).fetchone()
                checksum = hashlib.sha256(
                    f"{rows}:{int(xor_hash)}:{int(sum_hash)}".encode("utf-8")
                ).hexdigest()
                checksum_status = "computed"
            evidence[f"{schema}.{table}"] = {
                "rows": rows,
                "schema": schema_signature,
                "logical_checksum": checksum,
                "checksum_status": checksum_status,
            }
        return {
            "file_bytes": path.stat().st_size,
            "table_count": len(evidence),
            "tables": evidence,
        }
    finally:
        conn.close()


def compare(source: dict, restored: dict) -> list[str]:
    mismatches = []
    names = sorted(set(source["tables"]) | set(restored["tables"]))
    for name in names:
        left = source["tables"].get(name)
        right = restored["tables"].get(name)
        if left is None or right is None:
            mismatches.append(f"{name}:missing")
            continue
        if left["schema"] != right["schema"]:
            mismatches.append(f"{name}:schema")
        if left["rows"] != right["rows"]:
            mismatches.append(f"{name}:rows")
        if (
            left["checksum_status"] == "computed"
            and right["checksum_status"] == "computed"
            and left["logical_checksum"] != right["logical_checksum"]
        ):
            mismatches.append(f"{name}:logical_checksum")
    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--restored", type=Path, required=True)
    parser.add_argument("--max-checksum-rows", type=int, default=100_000)
    args = parser.parse_args()
    if args.max_checksum_rows < 0:
        parser.error("--max-checksum-rows must be non-negative")
    source = inventory(args.source, max_checksum_rows=args.max_checksum_rows)
    restored = inventory(args.restored, max_checksum_rows=args.max_checksum_rows)
    mismatches = compare(source, restored)
    print(
        json.dumps(
            {
                "match": not mismatches,
                "mismatches": mismatches,
                "source": source,
                "restored": restored,
            },
            sort_keys=True,
        )
    )
    return 0 if not mismatches else 1


if __name__ == "__main__":
    sys.exit(main())
