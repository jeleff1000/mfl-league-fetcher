"""Build the lake-wide typed column relationship graph.

Input is the existing source-column matrix.  That matrix is the source/witness
inventory; this layer adds explicit edge types and identifies canonical columns
that are only witnessed by one source or have no formula/mirror relationship.
It does not infer equality from similar names.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

from .relationship_registry import RELATIONSHIPS, Relationship

ROOT = Path(__file__).parents[2]
DEFAULT_MATRIX = ROOT / "docs" / "source-column-matrix.csv"
DEFAULT_OUT = ROOT / "docs" / "lake-column-relationship-graph.json"
DEFAULT_SCHEMA = ROOT / "docs" / "registered-source-schema-inventory.json"
DEFAULT_COVERAGE = ROOT / "docs" / "physical-column-coverage.json"


def build(matrix_path: Path = DEFAULT_MATRIX, schema_path: Path = DEFAULT_SCHEMA,
          coverage_path: Path = DEFAULT_COVERAGE) -> dict:
    by_canonical = defaultdict(list)
    by_source = defaultdict(int)
    with matrix_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            canonical = row.get("canonical_column", "").strip()
            source = row.get("source_id", "").strip()
            raw = row.get("raw_column", "").strip()
            if not (canonical and source and raw):
                continue
            by_canonical[canonical].append({
                "source_id": source,
                "raw_column": raw,
                "raw_table_context": row.get("raw_table_context", ""),
                "canonical_semantic_id": row.get("canonical_semantic_id", ""),
                "mapping_confidence": row.get("mapping_confidence", ""),
                "join_key": row.get("join_key", ""),
                "stat_unit": row.get("stat_unit", ""),
                "stat_domain": row.get("stat_domain", ""),
                "stat_role": row.get("stat_role", ""),
                "coverage_union_years": _years(row.get("coverage_union_years", "")),
                "true_missing_years": _years(row.get("true_missing_years", "")),
                "gate_eligible": row.get("gate_eligible", ""),
                "relationship_type": row.get("relationship_type", ""),
            })
            by_source[source] += 1

    edges = []
    witnessed = set()
    for canonical, observations in sorted(by_canonical.items()):
        sources = sorted({x["source_id"] for x in observations})
        witnessed.add(canonical)
        for i, left in enumerate(observations):
            for right in observations[i + 1:]:
                if left["source_id"] == right["source_id"]:
                    continue
                edge_type = "alias" if left["raw_column"] != right["raw_column"] else "source_witness"
                years = sorted(set(left["coverage_union_years"]) & set(right["coverage_union_years"]))
                edges.append({
                    "edge_type": edge_type,
                    "canonical_column": canonical,
                    "left": left,
                    "right": right,
                    "year_min": years[0] if years else None,
                    "year_max": years[-1] if years else None,
                    "density": _density(years, left, right),
                    "join_key": left["join_key"] or right["join_key"],
                    "status": "contracted_source_comparison",
                })

    formula_edges = []
    formula_canonicals = set()
    typed_relationships = _typed_relationships()
    for rel in typed_relationships:
        formula_canonicals.add(rel.lhs)
        formula_canonicals.update(_column_tokens(rel.rhs, by_canonical))
        formula_edges.append({
            "edge_type": rel.kind,
            "relationship_id": rel.key,
            "lhs": rel.lhs,
            "rhs": rel.rhs,
            "formula": rel.formula,
            "component_columns": _column_tokens(rel.rhs, by_canonical),
            "grain": rel.grain,
            "lane": rel.lane,
            "year_min": rel.year_min,
            "year_max": rel.year_max,
            "tolerance": rel.tolerance,
            "severity": rel.severity,
            "note": rel.note,
        })

    single_source = []
    no_typed_formula = []
    for canonical, observations in sorted(by_canonical.items()):
        source_count = len({x["source_id"] for x in observations})
        if source_count == 1:
            single_source.append(canonical)
        if canonical not in formula_canonicals:
            no_typed_formula.append(canonical)

    physical_schema = _physical_schema_summary(schema_path)
    physical_edges = _physical_source_edges(schema_path, by_canonical, coverage_path)

    return {
        "status": "review" if single_source or no_typed_formula else "pass",
        "source_column_matrix": str(matrix_path),
        "source_count": len(by_source),
        "raw_observation_count": sum(len(v) for v in by_canonical.values()),
        "canonical_column_count": len(by_canonical),
        "source_edge_count": len(edges),
        "formula_edge_count": len(formula_edges),
        "typed_relationship_registry_count": len(typed_relationships),
        "source_edges": edges,
        "formula_edges": formula_edges,
        "source_inventory": _source_inventory(by_canonical),
        "registered_source_inventory": _registered_source_inventory(set(by_source)),
        "physical_schema_inventory": physical_schema,
        "physical_source_edge_count": len(physical_edges),
        "physical_source_edges": physical_edges,
        "physical_density_known_edge_count": sum(e.get("physical_density") is not None for e in physical_edges),
        "physical_density_unknown_edge_count": sum(e.get("physical_density") is None for e in physical_edges),
        "single_source_canonicals": single_source,
        "canonicals_without_typed_formula_edge": no_typed_formula,
        "closure_rule": "every canonical column must have source witness/alias edges or an explicit single-source/excluded status, and every derived relationship must have a typed formula edge",
    }


def _years(value: str) -> list[int]:
    """Parse the matrix's serialized year arrays without executing arbitrary text."""
    try:
        parsed = ast.literal_eval(value) if value else []
        return sorted({int(y) for y in parsed if str(y).isdigit()})
    except (ValueError, SyntaxError, TypeError):
        return []


def _typed_relationships() -> tuple[Relationship, ...]:
    """Union every existing recon lane into the graph's typed edge catalog."""
    result = list(RELATIONSHIPS)
    keys = {r.key for r in result}
    try:
        from .recon_row_identities import IDENTITIES
        for key, col, formula, _guard, tolerance in IDENTITIES:
            if key in keys:
                continue
            result.append(Relationship(
                key, "identity", col, formula, f"{col} = {formula}",
                tolerance=tolerance,
                severity="hard" if tolerance == 0 else "review",
                note="Imported from recon_row_identities.IDENTITY registry.",
                lane="recon_row_identities",
            ))
            keys.add(key)
    except ImportError:
        pass
    try:
        from scripts.aggregate_merged_pbp_for_supertable_audit import STAT_COLUMNS
        for atom in STAT_COLUMNS:
            key = f"pbp_atom:{atom}"
            if key in keys:
                continue
            result.append(Relationship(
                key, "source_definition", atom, f"SUM(PBP.{atom})",
                f"{atom} = SUM(PBP.{atom})",
                year_min=1978,
                severity="review",
                note="PBP rollup atom; applicability and source-era gate lives in pbp_terminal_state.",
                grain="player_week",
                lane="pbp_terminal",
            ))
            keys.add(key)
    except ImportError:
        pass
    try:
        from .recon_bounds import HARD_BOUNDS
        for key, child, parent in HARD_BOUNDS:
            if key in keys:
                continue
            result.append(Relationship(
                key, "bound", child, parent, f"{child} <= {parent}",
                year_min=1920,
                severity="hard",
                note="Imported from recon_bounds.HARD_BOUNDS.",
                grain="player_week",
                lane="recon_bounds",
            ))
            keys.add(key)
    except ImportError:
        pass
    try:
        from .recon_conservation import STATS
        for label, authority_col, v26_col in STATS:
            key = f"conservation:{label.lower()}"
            if key in keys:
                continue
            result.append(Relationship(
                key, "conservation", v26_col, f"SUM({authority_col}) authority",
                f"SUM({v26_col}) = {authority_col} authority",
                year_min=1920,
                severity="review",
                note="Imported from recon_conservation.STATS; era tolerance is lane-defined.",
                grain="league_year",
                lane="recon_conservation",
            ))
            keys.add(key)
    except ImportError:
        pass
    try:
        from .build_allowed_mirror import MIRROR
        for child, expression in MIRROR.items():
            key = f"allowed_mirror:{child}"
            if key in keys:
                continue
            result.append(Relationship(
                key, "mirror", child, f"opponent {expression}",
                f"{child} = opponent SUM({expression})",
                year_min=1950,
                severity="review",
                note="Imported from build_allowed_mirror.MIRROR.",
                grain="team_game",
                lane="recon_allowed_mirror",
            ))
            keys.add(key)
    except ImportError:
        pass
    try:
        from .golden_points import SIMPLE_RATE_NUMDEN
        for rate, (num, den) in SIMPLE_RATE_NUMDEN.items():
            key = f"rate:{rate}"
            if key in keys:
                continue
            result.append(Relationship(
                key, "identity", rate, f"{num} / {den}",
                f"{rate} = {num} / {den}",
                year_min=1920,
                tolerance=0.01,
                severity="review",
                note="Imported from golden_points.SIMPLE_RATE_NUMDEN; rounded rate.",
                grain="player_season",
                lane="golden_points",
            ))
            keys.add(key)
    except ImportError:
        pass
    return tuple(result)


def _density(years: list[int], left: dict, right: dict) -> float:
    if not years:
        return 0.0
    missing = set(left["true_missing_years"]) | set(right["true_missing_years"])
    return round(sum(y not in missing for y in years) / len(years), 6)


def _column_tokens(expression: str, by_canonical: dict[str, list[dict]]) -> list[str]:
    """Return observed canonical columns referenced by a registered expression."""
    names = set(by_canonical)
    return sorted(
        name for name in names
        if re.search(f"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", expression)
    )


def _source_inventory(by_canonical: dict[str, list[dict]]) -> list[dict]:
    rows = defaultdict(lambda: {"columns": set(), "years": set(), "tables": set()})
    for canonical, observations in by_canonical.items():
        for obs in observations:
            row = rows[obs["source_id"]]
            row["columns"].add(canonical)
            row["years"].update(obs["coverage_union_years"])
            if not obs["raw_table_context"]:
                continue
            row["tables"].add(obs["raw_table_context"])
    return [
        {
            "source_id": source,
            "column_count": len(v["columns"]),
            "year_min": min(v["years"]) if v["years"] else None,
            "year_max": max(v["years"]) if v["years"] else None,
            "tables": sorted(v["tables"]),
        }
        for source, v in sorted(rows.items())
    ]


def _registered_source_inventory(observed: set[str]) -> dict:
    """Compare the column matrix against the authoritative source registry.

    A registered artifact with zero matrix rows is a closure defect, not evidence
    that the artifact has no columns.  Keep it visible for the unresolved ledger.
    """
    try:
        from .sources import registry
        sources = registry()
    except (ImportError, FileNotFoundError, OSError):
        return {"status": "unavailable", "count": 0, "without_matrix_observations": []}
    rows = []
    for key, source in sorted(sources.items()):
        aliases = _registry_matrix_aliases(key)
        matched = sorted(({key} | aliases) & observed)
        rows.append({
            "source_id": key,
            "role": source.role,
            "path": source.path,
            "exists": source.exists(),
            "year_min": source.year_min,
            "year_max": source.year_max,
            "join": source.join,
            "lineage": source.lineage,
            "witness_class": source.witness_class,
            "matrix_source_ids": matched,
            "in_source_column_matrix": bool(matched),
        })
    return {
        "status": "review" if any(not r["in_source_column_matrix"] for r in rows) else "pass",
        "count": len(rows),
        "without_matrix_observations": [r["source_id"] for r in rows if not r["in_source_column_matrix"]],
        "sources": rows,
    }


def _registry_matrix_aliases(key: str) -> set[str]:
    """Map physical registry ids to the matrix's table-family ids."""
    aliases = {key}
    replacements = {
        "pfr_box_home_snaps": "pfr_box:home_snap_counts",
        "pfr_box_vis_snaps": "pfr_box:vis_snap_counts",
        "pfr_player_offense_box": "pfr_box:player_offense",
        "pfr_player_defense_box": "pfr_box:player_defense",
        "pfr_player_season_passing": "pfr_season:passing",
        "pfr_player_season_rec_rush": "pfr_season:receiving_and_rushing",
        "pfr_player_season_rush_rec": "pfr_season:rushing_and_receiving",
        "pfr_adj_passing": "pfr_season:adj_passing",
        "pfr_adv_recrush": "pfr_season:adv_receiving_and_rushing",
        "pfr_adv_rushrec": "pfr_season:adv_rushing_and_receiving",
        "pfr_passing_adv_season": "pfr_season:passing_advanced",
        "pfr_passing_adv_post": "pfr_season:passing_advanced_post",
        "pfr_adv_defense": "pfr_season:adv_defense",
        "pfr_adv_defense_post": "pfr_season:adv_defense_post",
        "pfr_adv_recrush_post": "pfr_season:adv_receiving_and_rushing_post",
        "pfr_adv_rushrec_post": "pfr_season:adv_rushing_and_receiving_post",
        "pfr_box_pbp": "pfr_box:pbp",
        "pfr_box_scoring": "pfr_box:scoring",
        "pfr_box_expected_points": "pfr_box:expected_points",
        "ngs_season_published": "ngs_season",
        "ngs_weekly_raw": "ngs_weekly",
        "pbp_player_week_rollup": "pbp_weekly_rollup",
        "pbp_merged_1978_2025": "pbp_merged",
    }
    if key in replacements:
        aliases.add(replacements[key])
    if key.startswith("pfr_box_"):
        aliases.add("pfr_box:" + key.removeprefix("pfr_box_"))
    if key == "pfr_snap_counts":
        aliases.add("pfr_season:snap_counts")
    if key == "pfr_team_games":
        aliases.add("team_games_all")
    if key == "schedule_master":
        aliases.add("master_schedule")
    player_season_map = {
        "pfr_player_scoring": "pfr_season:scoring",
        "pfr_player_kicking": "pfr_season:kicking",
        "pfr_player_punting": "pfr_season:punting",
        "pfr_player_returns": "pfr_season:returns",
        "pfr_player_defense": "pfr_season:defense",
        "pfr_player_fantasy": "pfr_season:fantasy",
        "pfr_games_played": "pfr_season:games_played",
        "pfr_games_played_post": "pfr_season:games_played_playoffs",
    }
    if key in player_season_map:
        aliases.add(player_season_map[key])
    return aliases


def _physical_schema_summary(schema_path: Path) -> dict:
    if not schema_path.exists():
        return {"status": "missing", "physical_column_count": 0,
                "unmapped_physical_column_count": 0, "unmapped_physical_columns": []}
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "error": str(exc), "physical_column_count": 0,
                "unmapped_physical_column_count": 0, "unmapped_physical_columns": []}
    unmapped = [
        {
            "source_id": row.get("source_id", ""),
            "physical_column": row.get("physical_column", ""),
            "dtype": row.get("dtype", ""),
            "source_specific_matrix_mapped": row.get("source_specific_matrix_mapped", False),
            "matrix_raw_name_match": row.get("matrix_raw_name_match", False),
            "matrix_canonical_name_match": row.get("matrix_canonical_name_match", False),
        }
        for row in payload.get("columns", [])
        if not row.get("matrix_mapped")
    ]
    return {
        "status": "review" if unmapped else "pass",
        "schema_inventory": str(schema_path),
        "physical_column_count": payload.get("physical_column_count", 0),
        "unmapped_physical_column_count": len(unmapped),
        "unmapped_physical_columns": unmapped,
    }


def _physical_source_edges(schema_path: Path, by_canonical: dict[str, list[dict]],
                           coverage_path: Path) -> list[dict]:
    """Create one explicit edge for every registered physical column."""
    if not schema_path.exists():
        return []
    try:
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    raw_to_canonical = defaultdict(set)
    for canonical, observations in by_canonical.items():
        raw_to_canonical[canonical].add(canonical)
        for observation in observations:
            raw_to_canonical[observation["raw_column"]].add(canonical)
    try:
        from .sources import registry
        source_registry = registry()
    except (ImportError, FileNotFoundError, OSError):
        source_registry = {}
    physical_coverage = {}
    if coverage_path.exists():
        try:
            coverage_payload = json.loads(coverage_path.read_text(encoding="utf-8"))
            physical_coverage = {
                (r.get("source_id"), r.get("physical_column")): r
                for r in coverage_payload.get("columns", [])
            }
        except (OSError, json.JSONDecodeError):
            pass

    def coverage(canonical: str, source_id: str) -> dict:
        source = source_registry.get(source_id)
        source_min = source.year_min if source else None
        source_max = source.year_max if source else None
        observed = set()
        for observation in by_canonical.get(canonical, []):
            observed.update(observation.get("coverage_union_years", []))
        if source_min is not None and source_max is not None:
            observed = {y for y in observed if source_min <= y <= source_max}
        if not observed:
            return {"source_year_min": source_min, "source_year_max": source_max,
                    "coverage_year_min": None, "coverage_year_max": None,
                    "density": None, "density_basis": "no_registered_matrix_years"}
        lo, hi = min(observed), max(observed)
        span = hi - lo + 1
        return {"source_year_min": source_min, "source_year_max": source_max,
                "coverage_year_min": lo, "coverage_year_max": hi,
                "density": round(len(observed) / span, 6),
                "density_basis": "matrix_union_years_over_coverage_span"}

    edges = []
    for row in payload.get("columns", []):
        physical = row.get("physical_column", "")
        pcov = physical_coverage.get((row.get("source_id", ""), physical), {})
        physical_fields = {
            "physical_density": pcov.get("density"),
            "physical_density_basis": pcov.get("density_basis"),
            "physical_column_year_min": pcov.get("column_year_min"),
            "physical_column_year_max": pcov.get("column_year_max"),
            "physical_row_count": pcov.get("row_count"),
            "physical_nonnull_row_count": pcov.get("nonnull_row_count"),
        }
        candidates = sorted(raw_to_canonical.get(physical, set()))
        alias = row.get("alias_canonical_match")
        if alias and alias not in candidates:
            candidates.append(alias)
        if not candidates:
            cov = coverage("", row.get("source_id", ""))
            edges.append({
                "edge_type": "physical_unmapped",
                "source_id": row.get("source_id", ""),
                "physical_column": physical,
                "dtype": row.get("dtype", ""),
                "source_specific_matrix_mapped": row.get("source_specific_matrix_mapped", False),
                "status": "unresolved_physical_column",
                **cov,
                **physical_fields,
            })
            continue
        for canonical in sorted(set(candidates)):
            cov = coverage(canonical, row.get("source_id", ""))
            edges.append({
                "edge_type": "physical_source_mapping",
                "source_id": row.get("source_id", ""),
                "physical_column": physical,
                "canonical_column": canonical,
                "dtype": row.get("dtype", ""),
                "source_specific_matrix_mapped": row.get("source_specific_matrix_mapped", False),
                "status": "mapped_physical_column",
                **cov,
                **physical_fields,
            })
    return edges


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    ap.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    args = ap.parse_args()
    result = build(args.matrix, args.schema, args.coverage)
    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items()
                      if k not in {"formula_edges", "source_edges"}}, indent=2))
