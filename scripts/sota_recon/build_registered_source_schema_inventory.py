"""Inventory actual columns in every registered local source artifact.

The source-column matrix is a semantic mapping, not a filesystem schema census.
This inventory closes that distinction by reading Parquet metadata without scanning
rows and marking each physical column as mapped or absent from the matrix.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from build_lake_relationship_graph import _registry_matrix_aliases
from sources import registry

ROOT = Path(__file__).parents[2]
MATRIX = ROOT / "docs" / "source-column-matrix.csv"
ALIASES = ROOT / "scripts" / "sota_recon" / "witness_audit_v2" / "source_column_aliases.json"
DEFAULT_OUT = ROOT / "docs" / "registered-source-schema-inventory.json"


def build(matrix_path: Path = MATRIX) -> dict:
    matrix_columns = defaultdict(set)
    matrix_source_canonicals = defaultdict(lambda: defaultdict(set))
    matrix_raw_canonicals = defaultdict(set)
    matrix_raw_columns = set()
    matrix_canonical_columns = set()
    with matrix_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            source = row.get("source_id", "").strip()
            raw = row.get("raw_column", "").strip()
            if not source or not raw:
                continue
            matrix_columns[source].add(raw)
            matrix_raw_columns.add(raw)
            canonical = row.get("canonical_column", "").strip()
            matrix_canonical_columns.add(canonical)
            matrix_source_canonicals[source][raw].add(canonical)
            matrix_raw_canonicals[raw].add(canonical)
    alias_to_canonical = {}
    if ALIASES.exists():
        payload = json.loads(ALIASES.read_text(encoding="utf-8"))
        for canonical, aliases in payload.items():
            if canonical in matrix_canonical_columns:
                for alias in aliases:
                    alias_to_canonical[alias] = canonical

    source_rows = []
    column_rows = []
    for key, source in sorted(registry().items()):
        path = Path(source.path)
        row = {
            "source_id": key,
            "path": source.path,
            "exists": path.is_file(),
            "year_min": source.year_min,
            "year_max": source.year_max,
            "join": source.join,
            "lineage": source.lineage,
            "witness_class": source.witness_class,
            "column_count": 0,
            "mapped_column_count": 0,
            "unmapped_column_count": 0,
            "status": "missing",
        }
        if not path.is_file():
            source_rows.append(row)
            continue
        try:
            schema = pq.read_schema(path)
            aliases = _registry_matrix_aliases(key)
            mapped = matrix_columns
            for field in schema:
                name = field.name
                source_specific_mapped = any(name in mapped[a] for a in aliases)
                canonical_name_match = name in matrix_canonical_columns
                alias_canonical = alias_to_canonical.get(name)
                is_mapped = (
                    source_specific_mapped
                    or name in matrix_raw_columns
                    or canonical_name_match
                    or alias_canonical is not None
                )
                canonical_matches = set()
                for source_alias in aliases:
                    canonical_matches.update(matrix_source_canonicals[source_alias].get(name, set()))
                canonical_matches.update(matrix_raw_canonicals.get(name, set()))
                canonical_matches.update({x for x in matrix_canonical_columns if x == name})
                if alias_canonical:
                    canonical_matches.add(alias_canonical)
                column_rows.append({
                    "source_id": key,
                    "physical_column": name,
                    "dtype": str(field.type),
                    "matrix_source_ids": sorted(a for a in aliases if name in mapped[a]),
                    "matrix_mapped": is_mapped,
                    "source_specific_matrix_mapped": source_specific_mapped,
                    "matrix_raw_name_match": name in matrix_raw_columns,
                    "matrix_canonical_name_match": canonical_name_match,
                    "alias_canonical_match": alias_canonical,
                    "matrix_canonical_matches": sorted(canonical_matches),
                })
                row["column_count"] += 1
                row["mapped_column_count"] += int(is_mapped)
            row["unmapped_column_count"] = row["column_count"] - row["mapped_column_count"]
            row["status"] = "review" if row["unmapped_column_count"] else "mapped"
        except Exception as exc:
            row["status"] = "schema_error"
            row["error"] = f"{type(exc).__name__}: {exc}"
        source_rows.append(row)

    return {
        "status": "review" if any(r["status"] not in {"mapped"} for r in source_rows) else "pass",
        "registered_source_count": len(source_rows),
        "existing_source_count": sum(r["exists"] for r in source_rows),
        "physical_column_count": len(column_rows),
        "physical_columns_unmapped_from_matrix": sum(not r["matrix_mapped"] for r in column_rows),
        "physical_columns_without_any_matrix_raw_name": sum(not r["matrix_raw_name_match"] for r in column_rows),
        "physical_columns_without_any_matrix_name": sum(
            not (r["matrix_raw_name_match"] or r["matrix_canonical_name_match"]) for r in column_rows
        ),
        "physical_columns_matched_by_alias_registry": sum(bool(r.get("alias_canonical_match")) for r in column_rows),
        "physical_columns_without_source_specific_mapping": sum(
            not r["source_specific_matrix_mapped"] for r in column_rows
        ),
        "sources": source_rows,
        "columns": column_rows,
        "scope": {
            "matrix": str(matrix_path),
            "note": "Parquet schemas only; no data rows scanned.",
        },
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", type=Path, default=MATRIX)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    result = build(args.matrix)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in {"columns", "sources"}}, indent=2))
