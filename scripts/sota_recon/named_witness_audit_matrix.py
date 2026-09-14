"""Build the field-level audit contract for StatsCrew, NFL.com, PFR, PFA, and NGS.

This is deliberately stricter than the source-level mapping-obligation gate.  One output
row represents one physical/logical dossier field.  A ledger declaration is not called
executable unless a real runner is attached to that exact row (or a legacy MapSpec reaches
the same source column and canonical target).

The generated matrix is read-only metadata.  It does not update the supertable.

Run::

    python -m scripts.sota_recon.named_witness_audit_matrix
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .column_dossier import row_key
from .witness_map import MapSpec, WITNESS_MAP


ROOT = Path(__file__).resolve().parents[2]
DOSSIER_PATH = ROOT / "docs" / "column-dossier.json"
STAT_CONTRACTS_PATH = (
    Path(__file__).resolve().parent
    / "witness_gate"
    / "contracts"
    / "stat_contracts.v1.json"
)
OUT_PATH = ROOT / "docs" / "named-witness-audit-matrix.json"

NAMED_PREFIXES = ("nflcom_", "statscrew_", "pfr_", "pfa_", "ancient_pfa", "ngs_")


def named_source(source_key: str) -> bool:
    """Whether a registry key belongs to one of the five user-requested families."""
    return source_key.startswith(NAMED_PREFIXES)


def _spec_payload(spec: MapSpec) -> dict[str, Any]:
    return {
        "executor_id": "witness_map.validate",
        "comparison_grain": spec.validation_grain or spec.grain,
        "source_grain": spec.grain,
        "source_shape": spec.shape,
        "source_aggregation": spec.agg,
        "source_scale": spec.scale,
        "source_filters": spec.filters or None,
        "season_type": spec.season_type,
        "blank_zero": spec.blank_zero,
        "validation_years": list(spec.validation_years) if spec.validation_years else None,
        "validation_referee": spec.validation_referee or None,
        "key_route": "legacy_mapspec",
        "crosswalk_receipt": None,
    }


def _status_for_disposition(disposition: str) -> str:
    if disposition == "NEW_SUPERTABLE_COLUMN_CANDIDATE":
        return "CANDIDATE_NO_TARGET"
    if disposition == "DUPLICATE_OF":
        return "DUPLICATE"
    if disposition == "OPEN":
        return "OPEN_UNADJUDICATED"
    if disposition.startswith("EXCLUDED_") or disposition == "EXCLUDED_WITH_REASON":
        return "EXCLUDED_NON_WITNESS"
    return "UNCLASSIFIED"


def build_rows(
    dossier_rows: Iterable[Mapping[str, Any]],
    map_specs: Iterable[MapSpec],
    stat_contracts: Mapping[str, Mapping[str, Any]],
    executor_catalog: Mapping[str, Mapping[str, Any]],
    lane_routes: Mapping[str, Mapping[str, Any]],
    blockers: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Join dossier decisions to executable routes without upgrading declarations.

    ``executor_catalog`` is keyed by the exact dossier ``source|table_key|column``.
    Source-level lane routes are allowed only for rows that remain OPEN; adjudicated
    stat mappings still require an exact column executor.
    """
    blockers = blockers or {}
    specs_by_pair: dict[tuple[str, str], list[MapSpec]] = defaultdict(list)
    for spec in map_specs:
        specs_by_pair[(spec.source_key, spec.source_col)].append(spec)

    result: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for source_row in dossier_rows:
        source = str(source_row["source"])
        table_key = str(source_row["table_key"])
        column = str(source_row["column"])
        key = row_key(source, table_key, column)
        if key in seen_keys:
            raise ValueError(f"duplicate dossier row {key}")
        seen_keys.add(key)

        disposition = str(source_row.get("disposition") or "OPEN")
        canonical = source_row.get("canonical")
        contract = stat_contracts.get(str(canonical), {}) if canonical else {}
        route: dict[str, Any] = {}
        blocker: Mapping[str, Any] = {}
        status: str

        pair_specs = specs_by_pair.get((source, column), [])
        matching_specs = [spec for spec in pair_specs if spec.v26_col == canonical]
        if disposition == "MAPPED_TO_CANONICAL":
            if pair_specs and not matching_specs:
                targets = sorted({spec.v26_col for spec in pair_specs})
                raise ValueError(
                    f"MapSpec for {source}.{column} targets {targets}, which contradicts "
                    f"dossier target {canonical!r} at {key}"
                )
            exact_route = executor_catalog.get(key)
            if exact_route:
                status = "EXECUTABLE"
                route = dict(exact_route)
            elif matching_specs:
                # Multiple equivalent specs can differ by validation stratum.  Keep every
                # stratum visible rather than picking one silently.
                payloads = [_spec_payload(spec) for spec in matching_specs]
                route = dict(payloads[0])
                route["legacy_routes"] = payloads
                status = "EXECUTABLE"
            else:
                status = "DECLARED_ONLY"
        else:
            status = _status_for_disposition(disposition)
            if status == "OPEN_UNADJUDICATED":
                if key in blockers:
                    status = "BLOCKED_NAMED"
                    blocker = blockers[key]
                elif source in lane_routes:
                    status = "LANE_EXECUTABLE"
                    route = dict(lane_routes[source])

        result.append(
            {
                "row_key": key,
                "source": source,
                "lineage": source_row.get("lineage"),
                "regime": source_row.get("regime"),
                "table_key": table_key,
                "source_column": column,
                "disposition": disposition,
                "canonical": canonical,
                "mapping_status": status,
                "executor_id": route.get("executor_id"),
                "comparison_grain": route.get("comparison_grain"),
                "source_grain": route.get("source_grain"),
                "source_shape": route.get("source_shape"),
                "source_aggregation": route.get("source_aggregation"),
                "source_scale": route.get("source_scale"),
                "source_filters": route.get("source_filters"),
                "season_type": route.get("season_type"),
                "blank_zero": route.get("blank_zero"),
                "key_route": route.get("key_route"),
                "crosswalk_receipt": route.get("crosswalk_receipt"),
                "validation_years": route.get("validation_years"),
                "validation_referee": route.get("validation_referee"),
                "legacy_routes": route.get("legacy_routes"),
                "blocker_generator": blocker.get("generator"),
                "blocker_question": blocker.get("question"),
                "canonical_grains": contract.get("grains"),
                "canonical_natural_grain": contract.get("natural_grain"),
                "canonical_aggregation": contract.get("aggregation_class"),
                "canonical_unit": contract.get("unit"),
                "tolerance_policy": contract.get("tolerance_policy"),
                "reason": source_row.get("reason"),
                "closed_by": source_row.get("closed_by"),
            }
        )
    return result


def _load_stat_contracts(path: Path = STAT_CONTRACTS_PATH) -> dict[str, dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    return {row["canonical_name"]: row for row in document["stats"]}


def build(
    dossier_path: Path = DOSSIER_PATH,
    executor_catalog: Mapping[str, Mapping[str, Any]] | None = None,
    lane_routes: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    dossier = json.loads(dossier_path.read_text(encoding="utf-8"))
    scoped = [row for row in dossier["rows"] if named_source(row["source"])]
    if lane_routes is None:
        lane_routes = {}
    from .column_escalation_census import build as build_escalation_census

    blockers = build_escalation_census()["escalations"]
    rows = build_rows(
        scoped,
        WITNESS_MAP,
        _load_stat_contracts(),
        executor_catalog or {},
        lane_routes,
        blockers,
    )
    status_counts = Counter(row["mapping_status"] for row in rows)
    by_source: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        by_source[row["source"]][row["mapping_status"]] += 1
    return {
        "contract_version": "1",
        "scope": ["StatsCrew", "NFL.com", "PFR", "PFA", "NGS"],
        "safety": "read-only audit contract; no supertable mutation",
        "counts": {
            "dossier_rows": len(rows),
            "by_mapping_status": dict(sorted(status_counts.items())),
            "declared_mappings_without_executor": status_counts["DECLARED_ONLY"],
            "named_blockers": status_counts["BLOCKED_NAMED"],
            "open_unadjudicated": status_counts["OPEN_UNADJUDICATED"],
            "unclassified": status_counts["UNCLASSIFIED"],
        },
        "zero_gates": {
            "every_row_classified": status_counts["UNCLASSIFIED"] == 0,
            "every_declared_mapping_executable": status_counts["DECLARED_ONLY"] == 0,
            "no_untriaged_open_rows": status_counts["OPEN_UNADJUDICATED"] == 0,
            "no_open_rows": (
                status_counts["OPEN_UNADJUDICATED"] + status_counts["BLOCKED_NAMED"]
            ) == 0,
        },
        "by_source": {
            source: dict(sorted(counts.items())) for source, counts in sorted(by_source.items())
        },
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    args = parser.parse_args()
    document = build()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.out}")
    for key, value in document["counts"].items():
        if not isinstance(value, dict):
            print(f"  {key}: {value}")
    print("  status:", document["counts"]["by_mapping_status"])
    print("  zero gates:", document["zero_gates"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
