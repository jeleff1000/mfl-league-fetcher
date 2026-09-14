"""Compute physical Parquet column density without scanning full row values."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyarrow import parquet as pq
import duckdb

from sources import registry

ROOT = Path(__file__).parents[2]
DEFAULT_OUT = ROOT / "docs" / "physical-column-coverage.json"


def _number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build() -> dict:
    rows = []
    source_summaries = []
    fallback_con = duckdb.connect()
    for source_id, source in sorted(registry().items()):
        path = Path(source.path)
        if not path.is_file():
            source_summaries.append({"source_id": source_id, "status": "missing", "path": source.path})
            continue
        try:
            meta = pq.ParquetFile(path).metadata
            total_rows = meta.num_rows
            fields = pq.read_schema(path).names
            source_rows = 0
            source_density_known = 0
            for index, name in enumerate(fields):
                nonnull = 0
                stats_known = True
                year_min = None
                year_max = None
                for rg_index in range(meta.num_row_groups):
                    chunk = meta.row_group(rg_index).column(index)
                    stats = chunk.statistics
                    if stats is None or stats.null_count is None:
                        stats_known = False
                    else:
                        nonnull += chunk.num_values - stats.null_count
                    if name.lower() in {"year", "season", "year_id"}:
                        if stats is None:
                            continue
                        lo = _number(stats.min)
                        hi = _number(stats.max)
                        if lo is not None:
                            year_min = lo if year_min is None else min(year_min, lo)
                        if hi is None:
                            continue
                        year_max = hi if year_max is None else max(year_max, hi)
                density = round(nonnull / total_rows, 6) if stats_known and total_rows else None
                density_basis = "parquet_row_group_statistics" if density is not None else "statistics_unavailable"
                if density is None and total_rows:
                    safe_path = str(path).replace("'", "''")
                    safe_name = name.replace('"', '""')
                    scanned_total, scanned_nonnull = fallback_con.execute(
                        f"SELECT COUNT(*), COUNT(\"{safe_name}\") FROM '{safe_path}'"
                    ).fetchone()
                    if scanned_total:
                        nonnull = int(scanned_nonnull)
                        density = round(nonnull / scanned_total, 6)
                        density_basis = "single_column_scan_fallback"
                rows.append({
                    "source_id": source_id,
                    "physical_column": name,
                    "source_year_min": source.year_min,
                    "source_year_max": source.year_max,
                    "column_year_min": int(year_min) if year_min is not None else None,
                    "column_year_max": int(year_max) if year_max is not None else None,
                    "row_count": total_rows,
                    "nonnull_row_count": nonnull if stats_known else None,
                    "density": density,
                    "density_basis": density_basis,
                })
                source_rows += 1
                source_density_known += int(density is not None)
            source_summaries.append({
                "source_id": source_id, "status": "scanned_metadata",
                "path": source.path, "row_count": total_rows,
                "column_count": source_rows,
                "density_known_columns": source_density_known,
            })
        except Exception as exc:
            source_summaries.append({"source_id": source_id, "status": "schema_error",
                                     "path": source.path, "error": f"{type(exc).__name__}: {exc}"})
            continue
    fallback_con.close()
    return {
        "status": "review" if any(row["density"] is None for row in rows) else "pass",
        "source_count": len(source_summaries),
        "physical_column_count": len(rows),
        "density_known_column_count": sum(row["density"] is not None for row in rows),
        "density_unknown_column_count": sum(row["density"] is None for row in rows),
        "sources": source_summaries,
        "columns": rows,
        "note": "Metadata-only density; values are non-null row fraction, not a semantic year-completeness claim.",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    result = build()
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in {"sources", "columns"}}, indent=2))
