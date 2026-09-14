"""Compare lake-source potentials with the columns exposed by our tracked tables.

This is intentionally a census, not a promotion job.  It identifies candidates
that exist in the source lake but are absent from the published/operational table
schemas, and also identifies exposed table columns that have no source-column
mapping yet.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parents[2]
MATRIX = ROOT / "docs" / "source-column-matrix.csv"
CENSUS = ROOT / "docs" / "hidden-column-exposure-census.json"
DEFAULT_OUT = ROOT / "docs" / "lake-exposure-gap-ledger.json"
GRAPH = ROOT / "docs" / "lake-column-relationship-graph.json"
SCHEMA = ROOT / "docs" / "registered-source-schema-inventory.json"


def _years(value: str) -> list[int]:
    try:
        return [int(x) for x in json.loads(value or "[]")]
    except (ValueError, TypeError, json.JSONDecodeError):
        return []


def _physical_family(source_id: str, column: str) -> tuple[str, str, bool]:
    name = column.lower()
    source = source_id.lower()
    if source.startswith("newspaper_"):
        return ("newspaper_sidecar", "retain_in_newspaper_witness_lane", False)
    if source == "player_bio" and re.search("hof|allpro|probowls|award|seasons_started|career_games", name):
        return ("awards_bio_membership", "materialize_bio_award_or_career_membership", True)
    if re.search("snap(_|$)|snap_pct|starter|starting", name):
        return ("snap_and_starter_context", "reconcile_and_expose_context", True)
    if re.search("epa|wpa|success|air_yd|air_yards|yac|pressure|pressures|qb_hurry|knockdown|tackle", name):
        return ("advanced_efficiency_or_defense", "reconcile_era_gates_then_expose", True)
    if re.search("rank_|_rank$|^ppg|fpts|lamar|rolling|consistency|weighted_ppg|bonus_|^pts_", name):
        high_value = not (source.startswith("legacy_") or source.startswith("ancient_"))
        return ("derived_fantasy_surface", "materialize_or_expose_with_scoring_contract", high_value)
    if re.search("(_id|_url|links|source|caption|table_|row_index|tr_data|description|detail|location|time)", name):
        return ("source_provenance_or_event_context", "retain_as_provenance_or_event_support", False)
    if re.search("team|opponent|position|home_away|season|week|game_date|player", name):
        return ("identity_or_game_context", "map_identity_context_before_promotion", False)
    return ("unclassified_physical_stat", "semantic_review_required", False)


def _target_surfaces(row: dict) -> tuple[list[str], str]:
    name = row.get("canonical_column", "").lower()
    family = row.get("physical_family", "")
    source = " ".join(row.get("source_ids", [])).lower()
    rel_types = {x.get("edge_type") for x in row.get("typed_relationships", [])}
    rel_lanes = {x.get("lane") for x in row.get("typed_relationships", [])}
    if family == "newspaper_sidecar" or source.startswith("newspaper_"):
        return (["newspaper_witness_sidecar"], "newspaper_only_until_promoted")
    if family == "awards_bio_membership" or re.search("hof|allpro|probowls|award|seasons_started", name):
        return ([*("player_bio", "season", "career")], "bio_membership_then_season_career_counts")
    if family == "snap_and_starter_context" or re.search("snap_pct|starter_position|is_starter|home_away", name):
        return ([*("weekly", "season", "career")], "weekly_witness_then_aggregate")
    if family == "advanced_efficiency_or_defense" or rel_lanes & {"pbp_terminal"}:
        return ([*("weekly", "season", "career")], "era_gated_weekly_atom_then_aggregate")
    if family == "derived_fantasy_surface" or re.search("rank_|ppg|fpts|lamar|rolling|consistency|bonus|^pts_", name):
        return ([*("weekly", "season", "career")], "scoring_or_rank_contract_then_expose")
    if "aggregate" in rel_types or rel_lanes & {"recon_aggregate"}:
        return (["season", "career"], "aggregate_from_witnessed_lower_grain")
    if family == "source_provenance_or_event_context":
        return (["provenance_context"], "retain_for_reconciliation_only")
    if family == "identity_or_game_context":
        return ([*("player_bio", "weekly", "season", "career")], "identity_join_context_review")
    if row.get("kind") == "exposed_without_lake_mapping":
        return (["existing_table"], "map_source_or_mark_derived")
    return ([*("weekly", "season", "career")], "semantic_review_before_surface_assignment")


def build(matrix_path: Path = MATRIX, census_path: Path = CENSUS,
          graph_path: Path = GRAPH, schema_path: Path = SCHEMA) -> dict:
    source_columns = {}
    raw_to_canon = defaultdict(set)
    with matrix_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            canonical = row.get("canonical_column", "").strip()
            raw = row.get("raw_column", "").strip()
            if not canonical:
                continue
            item = source_columns.setdefault(canonical, {
                "canonical_column": canonical, "sources": set(), "raw_columns": set(),
                "tables": set(), "years": set(), "domains": set(), "roles": set(),
                "confidence": set(), "semantic_ids": set(), "transformations": set(),
            })
            item["sources"].add(row.get("source_id", "").strip())
            item["raw_columns"].add(raw)
            item["tables"].add(row.get("raw_table_context", "").strip())
            item["years"].update(_years(row.get("coverage_union_years", "")))
            item["domains"].add(row.get("stat_domain", "").strip())
            item["roles"].add(row.get("stat_role", "").strip())
            item["confidence"].add(row.get("mapping_confidence", "").strip())
            item["semantic_ids"].add(row.get("canonical_semantic_id", "").strip())
            item["transformations"].add(row.get("transformation_class", "").strip())
            if not raw:
                continue
            raw_to_canon[raw].add(canonical)

    census = json.loads(census_path.read_text(encoding="utf-8"))
    exposed_by_name = defaultdict(list)
    census_columns = set()
    for col in census.get("columns", []):
        name = col.get("column", "").strip()
        if not name:
            continue
        census_columns.add(name)
        exposed_by_name[name].append(col)

    graph = json.loads(graph_path.read_text(encoding="utf-8")) if graph_path.exists() else {}
    related = defaultdict(list)
    related_seen = defaultdict(set)
    for edge in graph.get("formula_edges", []):
        rel = edge.get("relationship_id", "")
        for name in [edge.get("lhs", ""), *edge.get("component_columns", [])]:
            if not name:
                continue
            marker = (rel, edge.get("edge_type", ""))
            if marker in related_seen[name]:
                continue
            related_seen[name].add(marker)
            related[name].append({
                "relationship_id": rel,
                "edge_type": edge.get("edge_type", ""),
                "lane": edge.get("lane", ""),
                "formula": edge.get("formula", ""),
            })

    lake_names = set(source_columns)
    rows = []
    for canonical, item in sorted(source_columns.items()):
        exact = exposed_by_name.get(canonical, [])
        alias_hits = []
        for raw in sorted(item["raw_columns"]):
            if raw == canonical:
                continue
            alias_hits.extend(exposed_by_name.get(raw, []))
        hits = exact or alias_hits
        status = "exposed" if exact else "alias_exposed" if alias_hits else "not_exposed"
        years = sorted(item["years"])
        if hits:
            exposure_class = "already_exposed"
            priority = "none"
        elif related.get(canonical):
            exposure_class = "derivable_or_witnessed_not_exposed"
            priority = "high"
        elif len(item["sources"]) >= 2:
            exposure_class = "multi_source_not_exposed"
            priority = "high"
        else:
            exposure_class = "single_source_not_exposed"
            priority = "gated_review"
        rows.append({
            "kind": "lake_potential",
            "canonical_column": canonical,
            "status": status,
            "exposure_class": exposure_class,
            "priority": priority,
            "source_count": len(item["sources"]),
            "source_ids": sorted(item["sources"]),
            "raw_columns": sorted(item["raw_columns"]),
            "source_tables": sorted(x for x in item["tables"] if x),
            "year_min": years[0] if years else None,
            "year_max": years[-1] if years else None,
            "domains": sorted(x for x in item["domains"] if x),
            "roles": sorted(x for x in item["roles"] if x),
            "mapping_confidence": sorted(x for x in item["confidence"] if x),
            "semantic_ids": sorted(x for x in item["semantic_ids"] if x),
            "transformation_classes": sorted(x for x in item["transformations"] if x),
            "typed_relationships": related.get(canonical, []),
            "exposed_tables": sorted({x["tableId"] for x in hits}),
            "exposed_routes": sorted({route for x in hits for route in x.get("apiPayloadRoutes", [])}),
            "recommendation": (
                "already_available" if hits
                else "materialize_or_expose_existing_relationship" if related.get(canonical)
                else "reconcile_sources_then_expose" if len(item["sources"]) >= 2
                else "add_with_single_source_gate"
            ),
        })

    mapped_names = lake_names | set(raw_to_canon)
    for name, cols in sorted(exposed_by_name.items()):
        if name in mapped_names:
            continue
        rows.append({
            "kind": "exposed_without_lake_mapping",
            "canonical_column": name,
            "status": "unmapped_exposed_column",
            "exposure_class": "exposed_without_source_mapping",
            "priority": "map_or_mark_derived",
            "source_count": 0,
            "source_ids": [],
            "raw_columns": [name],
            "source_tables": [],
            "year_min": None,
            "year_max": None,
            "domains": [],
            "roles": [],
            "exposed_tables": sorted({x["tableId"] for x in cols}),
            "exposed_routes": sorted({route for x in cols for route in x.get("apiPayloadRoutes", [])}),
            "mapping_confidence": [],
            "semantic_ids": [],
            "transformation_classes": [],
            "typed_relationships": [],
            "recommendation": "map_to_lake_source_or_mark_derived",
        })

    physical_unmapped = []
    physical_crosswalk_gaps = []
    if schema_path.exists():
        schema_inventory = json.loads(schema_path.read_text(encoding="utf-8"))
        for col in schema_inventory.get("columns", []):
            if col.get("matrix_mapped") and not col.get("source_specific_matrix_mapped"):
                exposed_hits = exposed_by_name.get(col["physical_column"], [])
                family, action, high_value = _physical_family(col["source_id"], col["physical_column"])
                physical_crosswalk_gaps.append({
                    "kind": "physical_source_crosswalk_gap",
                    "canonical_column": col["physical_column"],
                    "status": "source_crosswalk_gap",
                    "exposure_class": "known_column_unmapped_to_registered_source",
                    "priority": "lineage_crosswalk_required",
                    "physical_family": family,
                    "recommended_action": "add_registered_source_to_matrix_crosswalk",
                    "high_value_candidate": high_value,
                    "source_count": 1,
                    "source_ids": [col["source_id"]],
                    "raw_columns": [col["physical_column"]],
                    "source_tables": [],
                    "canonical_matches": col.get("matrix_canonical_matches", []),
                    "year_min": None,
                    "year_max": None,
                    "domains": [],
                    "roles": [],
                    "mapping_confidence": [],
                    "semantic_ids": [],
                    "transformation_classes": [],
                    "typed_relationships": [],
                    "exposed_tables": sorted({x["tableId"] for x in exposed_hits}),
                    "exposed_routes": sorted({route for x in exposed_hits for route in x.get("apiPayloadRoutes", [])}),
                    "recommendation": "register_source_lineage_crosswalk",
                })
                continue
            if col.get("matrix_mapped"):
                continue
            exposed_hits = exposed_by_name.get(col["physical_column"], [])
            family, action, high_value = _physical_family(col["source_id"], col["physical_column"])
            physical_unmapped.append({
                "kind": "physical_lake_column_unmapped",
                "canonical_column": col["physical_column"],
                "status": "exposed_without_lake_mapping" if exposed_hits else "not_exposed",
                "exposure_class": "exposed_without_source_mapping" if exposed_hits else "physical_source_unmapped",
                "priority": "map_or_mark_derived" if exposed_hits else "semantic_mapping_required",
                "physical_family": family,
                "recommended_action": action,
                "high_value_candidate": high_value,
                "source_count": 1,
                "source_ids": [col["source_id"]],
                "raw_columns": [col["physical_column"]],
                "canonical_matches": col.get("matrix_canonical_matches", []),
                "source_tables": [],
                "year_min": None,
                "year_max": None,
                "domains": [],
                "roles": [],
                "mapping_confidence": [],
                "semantic_ids": [],
                "transformation_classes": [],
                "typed_relationships": [],
                "exposed_tables": sorted({x["tableId"] for x in exposed_hits}),
                "exposed_routes": sorted({route for x in exposed_hits for route in x.get("apiPayloadRoutes", [])}),
                "recommendation": ("map_to_lake_source_or_mark_derived" if exposed_hits
                                   else "register_semantics_and_source_mapping"),
            })

    rows.extend(physical_unmapped)
    rows.extend(physical_crosswalk_gaps)
    for row in rows:
        surfaces, path = _target_surfaces(row)
        row["target_surfaces"] = surfaces
        row["exposure_path"] = path

    summary = {
        "status": ("review" if any(r["status"] in {"not_exposed", "unmapped_exposed_column"} for r in rows)
                   else "pass"),
        "matrix_canonical_count": len(source_columns),
        "tracked_census_column_count": len(census_columns),
        "lake_potential_exposed": sum(r["kind"] == "lake_potential" and r["status"] == "exposed" for r in rows),
        "lake_potential_alias_exposed": sum(r["kind"] == "lake_potential" and r["status"] == "alias_exposed" for r in rows),
        "lake_potential_not_exposed": sum(r["kind"] == "lake_potential" and r["status"] == "not_exposed" for r in rows),
        "not_exposed_by_class": {
            k: sum(r.get("kind") == "lake_potential" and r.get("status") == "not_exposed"
                   and r.get("exposure_class") == k for r in rows)
            for k in sorted({r.get("exposure_class") for r in rows
                             if r.get("kind") == "lake_potential" and r.get("status") == "not_exposed"})
        },
        "exposed_without_lake_mapping": sum(r["status"] == "unmapped_exposed_column" for r in rows),
        "physical_columns_not_mapped_to_source_matrix": len(physical_unmapped),
        "physical_source_crosswalk_gap_count": len(physical_crosswalk_gaps),
        "physical_unmapped_by_exposure_status": {
            "not_exposed": sum(r.get("kind") == "physical_lake_column_unmapped"
                               and r.get("status") == "not_exposed" for r in rows),
            "exposed_without_lake_mapping": sum(r.get("kind") == "physical_lake_column_unmapped"
                                                and r.get("status") == "exposed_without_lake_mapping" for r in rows),
        },
        "physical_unmapped_by_family": {
            k: sum(r.get("kind") == "physical_lake_column_unmapped"
                   and r.get("physical_family") == k for r in rows)
            for k in sorted({r.get("physical_family") for r in rows
                             if r.get("kind") == "physical_lake_column_unmapped"})
        },
        "candidate_target_surface_counts": {
            surface: sum(surface in r.get("target_surfaces", [])
                         and r.get("status") in {"not_exposed", "not_mapped_to_source_matrix"} for r in rows)
            for surface in sorted({s for r in rows for s in r.get("target_surfaces", [])})
        },
        "rows": rows,
        "scope": {
            "source_matrix": str(matrix_path),
            "exposure_census": str(census_path),
            "registered_schema_inventory": str(schema_path),
            "note": "Exposure is measured against the tracked table/API census; absence means candidate, not automatic approval for promotion.",
        },
    }
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", type=Path, default=MATRIX)
    ap.add_argument("--census", type=Path, default=CENSUS)
    ap.add_argument("--graph", type=Path, default=GRAPH)
    ap.add_argument("--schema", type=Path, default=SCHEMA)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--csv", type=Path, default=None)
    args = ap.parse_args()
    result = build(args.matrix, args.census, args.graph, args.schema)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if args.csv:
        list_fields = {"domains", "canonical_matches", "roles", "source_ids", "target_surfaces",
                       "typed_relationships", "exposed_tables", "source_tables", "exposed_routes",
                       "raw_columns"}
        fields = sorted({k for row in result["rows"] for k in row if k not in list_fields})
        with args.csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in result["rows"]:
                writer.writerow({k: row.get(k) for k in fields})
    print(json.dumps({k: v for k, v in result.items() if k != "rows"}, indent=2))
