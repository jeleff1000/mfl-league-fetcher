"""Build the unresolved canonical-column relationship ledger."""

from __future__ import annotations

import argparse
import ast
import csv
import glob
import json
import os
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parents[2]
MATRIX = ROOT / "docs" / "source-column-matrix.csv"
GRAPH = ROOT / "docs" / "lake-column-relationship-graph.json"
DEFAULT_OUT = ROOT / "docs" / "relationship-closure-ledger.json"
TERMINAL_GLOB = "D:\\league-history-data\\nfl\\derived\\validation\\sota_recon_master\\*\\pbp_terminal_state.json"


def _years(value: str) -> list[int]:
    try:
        parsed = ast.literal_eval(value or "[]")
        return sorted({int(x) for x in parsed})
    except (ValueError, SyntaxError, TypeError):
        return []


def _context_candidate(canonical: str, item: dict) -> bool:
    names = " ".join([canonical, *item["raw"], *item["tables"], *item["sources"]]).lower()
    if any(x in names for x in ("newspaper", "pfr_context", "source_doc", "review_", "witness_")):
        return True
    return bool(re.search(
        r"(^|_)(player|team|opponent|nfl_team|franchise|source|status|coverage|resolution|metadata|url|link|caption|description|detail|college|conference|birth|coach|draft|date|game_date|season_type|home_away|boxscore|table|row_index|play_id|drive_id|year|week)(_|$)",
        canonical.lower(),
    ))


def _context_reason(canonical: str, item: dict) -> str:
    names = " ".join([canonical, *item["raw"], *item["tables"], *item["sources"]]).lower()
    if any(x in names for x in ("url", "link", "source", "row_index", "table_", "caption", "description")):
        return "provenance_or_source_locator_not_statistical_atom"
    if any(x in names for x in ("pfr_context", "combine", "coaches", "pro_bowl", "school", "college", "draft")):
        return "bio_award_combine_or_coach_context"
    if any(x in names for x in ("team_games", "master_schedule", "opponent", "home_team", "vis_team", "score", "points")):
        return "game_or_team_context_not_player_stat"
    if any(x in names for x in ("ancient_bundle", "coverage", "identity_resolution", "fact_cells")):
        return "ingestion_witness_metadata"
    return "context_field_not_a_stat_relationship"


def _load_pbp_terminal() -> dict:
    paths = sorted(glob.glob(TERMINAL_GLOB), key=lambda p: os.path.getmtime(p))
    if not paths:
        return {}
    try:
        return json.loads(Path(paths[-1]).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def build(matrix_path: Path = MATRIX, graph_path: Path = GRAPH) -> dict:
    columns = defaultdict(lambda: {"sources": set(), "raw": set(), "years": set(),
                                   "domains": set(), "roles": set(), "tables": set()})
    with matrix_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            canonical = row.get("canonical_column", "").strip()
            if not canonical:
                continue
            item = columns[canonical]
            item["sources"].add(row.get("source_id", "").strip())
            item["raw"].add(row.get("raw_column", "").strip())
            item["years"].update(_years(row.get("coverage_union_years", "")))
            item["domains"].add(row.get("stat_domain", "").strip())
            item["roles"].add(row.get("stat_role", "").strip())
            item["tables"].add(row.get("raw_table_context", "").strip())

    graph = json.loads(graph_path.read_text(encoding="utf-8")) if graph_path.exists() else {}
    terminal = _load_pbp_terminal()
    related = defaultdict(list)
    for edge in graph.get("formula_edges", []):
        rel_id = edge.get("relationship_id", "")
        for name in [edge.get("lhs", ""), *edge.get("component_columns", [])]:
            if not name:
                continue
            if any(x["relationship_id"] == rel_id for x in related[name]):
                continue
            related[name].append({
                "relationship_id": rel_id,
                "edge_type": edge.get("edge_type", ""),
                "lane": edge.get("lane", ""),
                "formula": edge.get("formula", ""),
            })

    rows = []
    for canonical, item in sorted(columns.items()):
        rels = related.get(canonical, [])
        pbp_raw = terminal.get("raw_field_census", {}).get(canonical, {})
        pbp_extension = terminal.get("extensions", {}).get(canonical, {})
        years = sorted(item["years"])
        source_count = len(item["sources"])
        if rels:
            status = "typed_relationship_present"
            priority = "none"
            closure_class = "typed_relationship_covered"
        elif source_count >= 2:
            status = "source_witness_only"
            priority = "high"
            closure_class = "source_alias_or_witness_only"
        elif _context_candidate(canonical, item):
            status = "explicit_context_exclusion"
            priority = "none"
            closure_class = "context_or_provenance_explicitly_excluded"
        else:
            status = "single_source_no_typed_relationship"
            priority = "review"
            closure_class = "stat_relationship_review"
        if not rels and pbp_raw:
            if pbp_raw.get("category") in {"identity_or_technical", "game_context_or_model", "classified_event_or_model_input"}:
                closure_class = "pbp_terminal_context_or_model_input"
                priority = "explicit_pbp_disposition"
            elif pbp_raw.get("category") == "stat_input":
                closure_class = "pbp_terminal_stat_input_review"
                priority = "pbp_definition_or_rollup_review"
        if not rels and pbp_extension:
            closure_class = ("pbp_terminal_extension_excluded"
                             if pbp_extension.get("status") == "excluded"
                             else "pbp_terminal_extension_witnessed")
            priority = "none" if pbp_extension.get("status") == "excluded" else "pbp_definition_or_rollup_review"
        rows.append({
            "canonical_column": canonical,
            "status": status,
            "priority": priority,
            "closure_class": closure_class,
            "source_count": source_count,
            "source_ids": sorted(item["sources"]),
            "raw_columns": sorted(item["raw"]),
            "source_tables": sorted(x for x in item["tables"] if x),
            "year_min": years[0] if years else None,
            "year_max": years[-1] if years else None,
            "domains": sorted(x for x in item["domains"] if x),
            "roles": sorted(x for x in item["roles"] if x),
            "typed_relationships": rels,
            "pbp_terminal_category": pbp_raw.get("category"),
            "pbp_terminal_disposition": pbp_raw.get("disposition"),
            "pbp_terminal_extension_status": pbp_extension.get("status"),
            "recommendation": (
                "record_pbp_terminal_disposition" if closure_class.startswith("pbp_terminal_")
                else "excluded_from_stat_relationships" if closure_class == "context_or_provenance_explicitly_excluded"
                else "add_alias_mirror_aggregate_bound_or_formula_edge" if not rels
                else "covered"
            ),
            "exclusion_reason": (_context_reason(canonical, item)
                                 if closure_class == "context_or_provenance_explicitly_excluded"
                                 else None),
        })

    summary = {
        "status": "review" if any(r["status"] != "typed_relationship_present" for r in rows) else "pass",
        "canonical_count": len(rows),
        "typed_relationship_present": sum(1 for r in rows if r["status"] == "typed_relationship_present"),
        "source_witness_only": sum(1 for r in rows if r["status"] == "source_witness_only"),
        "single_source_no_typed_relationship": sum(1 for r in rows if r["status"] == "single_source_no_typed_relationship"),
        "closure_class_counts": {
            k: sum(1 for r in rows if r["closure_class"] == k)
            for k in sorted({r["closure_class"] for r in rows})
        },
        "rows": rows,
        "scope": {
            "matrix": str(matrix_path),
            "graph": str(graph_path),
            "note": "Source comparisons do not count as typed relationships; unresolved rows require semantic adjudication or an explicit exclusion.",
        },
    }
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", type=Path, default=MATRIX)
    ap.add_argument("--graph", type=Path, default=GRAPH)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--csv", type=Path, default=None)
    args = ap.parse_args()
    result = build(args.matrix, args.graph)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if args.csv:
        fields = ["canonical_column", "status", "closure_class", "priority",
                  "source_count", "year_min", "year_max", "recommendation"]
        with args.csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in result["rows"]:
                writer.writerow({key: row.get(key) for key in fields})
    print(json.dumps({k: v for k, v in result.items() if k != "rows"}, indent=2))
