"""Build the source x canonical-column witness gate matrix."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

from source_column_coverage import annotate_semantic_coverage, contract_coverage, parquet_coverage, year_to_int
from source_column_matrix import (
    build_canonical_registry,
    discover_columns,
    discover_sources,
    map_source_column,
)
from source_column_relationships import discover_relationships


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTRACTS = ROOT / "docs" / "witness-contracts-v2.json"
DEFAULT_ALIASES = Path(__file__).with_name("source_column_aliases.json")
DEFAULT_OUT = ROOT / "docs"


def hydrate_source_eras_from_coverage(sources: list[dict], coverage: list[dict]) -> None:
    """Fill missing source era bounds from measured integer-year coverage rows."""
    years_by_source = {}
    for row in coverage:
        year = year_to_int(row.get("year"))
        if year is None:
            continue
        years_by_source.setdefault(row["source_id"], set()).add(year)
    for source in sources:
        years = years_by_source.get(source["source_id"], set())
        if years:
            if source.get("era_min") is None:
                source["era_min"] = min(years)
            if source.get("era_max") is None:
                source["era_max"] = max(years)
        source["year_axis_status"] = (
            "bounded_observed_or_declared"
            if source.get("era_min") is not None and source.get("era_max") is not None
            else "unbounded_no_integer_year_axis"
        )


def build_matrix(
    contract_path: Path = DEFAULT_CONTRACTS,
    aliases_path: Path = DEFAULT_ALIASES,
    reuse_coverage_path: Path | None = None,
) -> dict:
    import duckdb

    sources = discover_sources(contract_path)
    included = [source for source in sources if not source["newspaper_excluded"]]
    excluded = [
        {"source_id": s["source_id"], "reason": "newspaper_excluded", "path": s["path"]}
        for s in sources
        if s["newspaper_excluded"]
    ]
    columns = [column for source in included for column in discover_columns(source)]
    aliases = json.loads(Path(aliases_path).read_text(encoding="utf-8"))
    canonical = build_canonical_registry(columns, set(aliases))
    observations = [map_source_column(column, canonical, aliases) for column in columns]
    coverage = []
    source_by_id = {s["source_id"]: s for s in included}
    columns_by_source = {}
    observations_by_key = {(o["source_id"], o["raw_column"]): o for o in observations}
    for column in columns:
        columns_by_source.setdefault(column["source_id"], []).append(column)

    cached_by_source = {}
    if reuse_coverage_path and Path(reuse_coverage_path).exists():
        cached = json.loads(Path(reuse_coverage_path).read_text(encoding="utf-8")).get("coverage", [])
        cached_by_source = {}
        for row in cached:
            if (row.get("unavailable_reason") or "").startswith("row_scan_error"):
                continue
            cached_by_source.setdefault(row["source_id"], []).append(row)
    con = duckdb.connect()
    try:
        for source in included:
            source_columns = columns_by_source.get(source["source_id"], [])
            if not source_columns:
                continue
            cached_rows = cached_by_source.get(source["source_id"], [])
            cached_keys = {(row["source_id"], row["raw_column"]) for row in cached_rows}
            for row in cached_rows:
                observation = observations_by_key.get((row["source_id"], row["raw_column"]))
                if not observation:
                    continue
                row["canonical_column"] = observation["canonical_column"]
            coverage.extend(cached_rows)
            remaining_columns = [
                column
                for column in source_columns
                if (source["source_id"], column["raw_column"]) not in cached_keys
            ]
            if not remaining_columns:
                continue
            scan_error = None
            try:
                measured = parquet_coverage(source, remaining_columns, con)
            except Exception as exc:
                measured = []
                scan_error = f"row_scan_error:{type(exc).__name__}"
            measured_keys = {(row["source_id"], row["raw_column"]) for row in measured}
            for row in measured:
                observation = observations_by_key[(row["source_id"], row["raw_column"])]
                row["canonical_column"] = observation["canonical_column"]
            coverage.extend(measured)
            for column in remaining_columns:
                key = (source["source_id"], column["raw_column"])
                if key in measured_keys:
                    continue
                observation = observations_by_key[key]
                rows = contract_coverage(source, column)
                for row in rows:
                    row["canonical_column"] = observation["canonical_column"]
                    if scan_error:
                        row["unavailable_reason"] = scan_error
                coverage.extend(rows)
    finally:
        con.close()

    hydrate_source_eras_from_coverage(included, coverage)
    declared_bounds = {}
    for column in columns:
        source = source_by_id[column["source_id"]]
        low = column.get("year_min") if column.get("year_min") is not None else source.get("era_min")
        high = column.get("year_max") if column.get("year_max") is not None else source.get("era_max")
        if low is None or high is None:
            continue
        declared_bounds[(column["source_id"], column["raw_column"])] = (int(low), int(high))
    semantic_coverage = annotate_semantic_coverage(observations, coverage, declared_bounds)
    coverage_by_key = {(row["source_id"], row["raw_column"]): row for row in semantic_coverage}
    for observation in observations:
        coverage_row = coverage_by_key.get((observation["source_id"], observation["raw_column"]))
        if not coverage_row:
            continue
        for key in ("coverage_union_years", "coverage_intersection_years", "true_missing_years", "source_partial_years", "missing_year_occurrence_count"):
            observation[key] = coverage_row[key]
    for row in coverage:
        coverage_row = coverage_by_key.get((row["source_id"], row["raw_column"]))
        if not coverage_row:
            continue
        for key in ("coverage_union_years", "coverage_intersection_years", "true_missing_years", "source_partial_years", "missing_year_occurrence_count"):
            row[key] = coverage_row[key]
    relationships = discover_relationships(observations)
    summary = {
        "source_count": len(included),
        "newspaper_excluded_source_count": len(excluded),
        "raw_column_observation_count": len(observations),
        "canonical_column_count": len(canonical),
        "accepted_observation_count": sum(o["gate_eligible"] for o in observations),
        "alias_observation_count": sum(o["relationship_type"] == "alias" for o in observations),
        "review_observation_count": sum(o["review_status"] != "accepted" for o in observations),
        "coverage_row_count": len(coverage),
        "relationship_count": len(relationships),
        "coverage_signal_density_available": sum(c["signal_density"] is not None for c in coverage),
        "coverage_nonnull_density_available": sum(c["all_row_density"] is not None for c in coverage),
    }
    return {
        "generated": "2026-07-21",
        "scope_note": "Newspaper sources are retained only in exclusions.",
        "sources": included,
        "canonical_columns": canonical,
        "observations": observations,
        "coverage": coverage,
        "relationships": relationships,
        "exclusions": excluded,
        "review_queue": [o for o in observations if o["review_status"] != "accepted"],
        "summary": summary,
    }


def write_outputs(matrix: dict, out_dir: Path = DEFAULT_OUT) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "source-column-matrix.json").write_text(
        json.dumps(matrix, indent=2, default=str) + "\n", encoding="utf-8"
    )
    _write_csv(out_dir / "source-column-matrix.csv", matrix["observations"])
    _write_csv(out_dir / "source-column-coverage.csv", matrix["coverage"])
    _write_csv(out_dir / "source-column-relationships.csv", matrix["relationships"])
    s = matrix["summary"]
    lines = [
        "# Source-column coverage matrix",
        "",
        "This matrix excludes newspaper sources from promotion and reconciliation. They are listed in the JSON exclusions inventory.",
        "",
        "## Summary",
        "",
        f"- Sources included: {s['source_count']}; newspaper sources excluded: {s['newspaper_excluded_source_count']}",
        f"- Raw column observations: {s['raw_column_observation_count']}; canonical columns: {s['canonical_column_count']}",
        f"- Accepted: {s['accepted_observation_count']}; aliases: {s['alias_observation_count']}; review queue: {s['review_observation_count']}",
        f"- Coverage rows: {s['coverage_row_count']}; relationships: {s['relationship_count']}",
        f"- Coverage with measured non-null density: {s['coverage_nonnull_density_available']}; signal density from contracts: {s['coverage_signal_density_available']}",
        "",
        "## Interpretation",
        "",
        "Contract-only rows expose signal/non-zero counts where available. Non-null density, eligible-row density, zero semantics, and row-level agreement remain explicitly unavailable until the underlying source rows are readable.",
        "",
        "Missing source-level era bounds are filled from measured integer year rows after coverage scanning; declared contract bounds are never overwritten.",
        "",
        "## Output files",
        "",
        "- `source-column-matrix.json`: complete machine-readable registry, observations, coverage, relationships, exclusions, and review queue.",
        "- `source-column-matrix.csv`: source-column mappings.",
        "- `source-column-coverage.csv`: year-aware coverage and density rows.",
        "- `source-column-relationships.csv`: reconciliation controls and component relationships.",
    ]
    (out_dir / "source-column-matrix.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", type=Path, default=DEFAULT_CONTRACTS)
    parser.add_argument("--aliases", type=Path, default=DEFAULT_ALIASES)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="reuse measured coverage from the existing output and rescan failed sources",
    )
    args = parser.parse_args()
    reuse = args.out_dir / "source-column-matrix.json" if args.reuse_existing else None
    matrix = build_matrix(args.contracts, args.aliases, reuse)
    write_outputs(matrix, args.out_dir)
    print(json.dumps(matrix["summary"], indent=2))


if __name__ == "__main__":
    main()
