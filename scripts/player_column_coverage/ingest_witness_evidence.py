#!/usr/bin/env python3
"""Join the witness/contract ecosystem into per-column evidence for the coverage audit.

The witness program already traced every supertable column to a family, a
verdict, a witnessed era, and (for computed columns) its lineage inputs. This
script makes that the SEED for the player column coverage ledger so the audit
never re-derives category or era by guessing at name prefixes.

Inputs (all committed or on the local release volume):
  docs/witness-column-master-matrix.json   family / verdict / era / witnesses / inputs
  docs/advanced-stats-registry.json        formula + public_from / we_extend_to
  <workdir>/super_column_era_coverage.json per-column per-year nonzero counts
  scripts/sota_recon/witness_gate/contracts/equations.v1.json  numeric laws

Availability is an EVIDENCE question, not a schema question: a column present in
the parquet footer with zero non-null years is not exposable, and this file is
what proves it either way.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

GRAIN_TABLES = {
    "weekly": "weekly",
    "season": "player_nfl_season",
    "career": "player_nfl_career",
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def era_record(year_counts: dict[str, int] | None) -> dict[str, Any]:
    """Collapse {year: nonzero_count} into a coverage record with interior gaps."""
    if not year_counts:
        return {
            "minYear": None,
            "maxYear": None,
            "coveredYearCount": 0,
            "missingYears": [],
            "totalNonZero": 0,
        }
    years = sorted(int(year) for year in year_counts)
    covered = set(years)
    missing = [year for year in range(years[0], years[-1] + 1) if year not in covered]
    return {
        "minYear": years[0],
        "maxYear": years[-1],
        "coveredYearCount": len(years),
        "missingYears": missing,
        "totalNonZero": sum(int(count) for count in year_counts.values()),
    }


def build_component_index(weekly: dict[str, Any]) -> dict[str, list[str]]:
    """Reverse the lineage graph: which columns consume this column as an input."""
    index: dict[str, list[str]] = defaultdict(list)
    for column, record in weekly.items():
        for source in record.get("inputs") or []:
            index[source].append(column)
    return {key: sorted(value) for key, value in index.items()}


def advanced_index(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        stat["name"]: {
            "family": stat.get("family"),
            "formula": stat.get("formula"),
            "formulaSource": stat.get("formula_source"),
            "publicFrom": stat.get("public_from"),
            "weExtendTo": stat.get("we_extend_to"),
            "status": stat.get("status"),
        }
        for stat in registry.get("stats", [])
    }


def build_evidence(
    schema: dict[str, Any],
    matrix: dict[str, Any],
    era_coverage: dict[str, Any],
    registry: dict[str, Any],
    equations: dict[str, Any],
) -> dict[str, Any]:
    tables = matrix["tables"]
    weekly = tables[GRAIN_TABLES["weekly"]]
    season = tables.get(GRAIN_TABLES["season"], {})
    career = tables.get(GRAIN_TABLES["career"], {})
    components = build_component_index(weekly)
    advanced = advanced_index(registry)

    columns: dict[str, Any] = {}
    drift: list[dict[str, str]] = []

    for entry in schema["columns"]:
        name = entry["name"]
        record = weekly.get(name)
        if record is None:
            drift.append({"column": name, "issue": "absentFromWitnessMatrix"})
            record = {}

        grains = [
            grain
            for grain, table in (
                ("weekly", weekly),
                ("season", season),
                ("career", career),
            )
            if name in table
        ]
        witnesses = record.get("witnesses") or []
        columns[name] = {
            "ordinal": entry["ordinal"],
            "type": entry["type"],
            "family": record.get("family"),
            "verdict": record.get("verdict"),
            "rule": record.get("rule"),
            "gameEra": record.get("game_era"),
            "seasonEra": record.get("season_era"),
            "primaryWitnessCount": record.get("n_primary"),
            "witnessCount": len(witnesses),
            "witnesses": sorted(witnesses),
            "note": record.get("note"),
            "witnessedGrains": grains,
            "eraCoverage": era_record(era_coverage.get(name)),
            "advanced": advanced.get(name),
            "relations": {
                "derivedFrom": sorted(record.get("inputs") or []),
                "componentOf": components.get(name, []),
            },
        }

    unmapped = sorted(
        name
        for name, record in columns.items()
        if record["verdict"] in (None, "UNMAPPED")
    )
    payload: dict[str, Any] = {
        "release_id": schema["release_id"],
        "schema_fingerprint": schema["schema_fingerprint"],
        "equations_contract_version": equations.get("contract_version"),
        "equation_law_count": len(equations.get("laws", [])),
        "column_count": len(columns),
        "summary": {
            "byFamily": _counter(columns, "family"),
            "byVerdict": _counter(columns, "verdict"),
            "unmappedColumns": unmapped,
            "zeroCoverageColumns": sorted(
                name
                for name, record in columns.items()
                if record["eraCoverage"]["totalNonZero"] == 0
            ),
            "driftFindings": drift,
        },
        "columns": dict(sorted(columns.items())),
    }
    fingerprint_input = json.dumps(payload["columns"], separators=(",", ":"), sort_keys=True)
    payload["evidence_fingerprint"] = hashlib.sha256(
        fingerprint_input.encode("utf-8")
    ).hexdigest()
    return payload


def _counter(columns: dict[str, Any], key: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for record in columns.values():
        counts[str(record.get(key))] += 1
    return dict(sorted(counts.items()))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--era-coverage", type=Path, required=True)
    parser.add_argument("--advanced-registry", type=Path, required=True)
    parser.add_argument("--equations", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    payload = build_evidence(
        load_json(args.schema),
        load_json(args.matrix),
        load_json(args.era_coverage),
        load_json(args.advanced_registry),
        load_json(args.equations),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    summary = payload["summary"]
    print(f"{payload['column_count']} columns -> {args.output}")
    print(f"  families: {summary['byFamily']}")
    print(f"  verdicts: {summary['byVerdict']}")
    print(f"  unmapped: {len(summary['unmappedColumns'])}")
    print(f"  zero-coverage: {len(summary['zeroCoverageColumns'])}")
    print(f"  drift findings: {len(summary['driftFindings'])}")
    return 1 if summary["driftFindings"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
