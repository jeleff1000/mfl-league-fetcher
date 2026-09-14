"""Cross the current witness gate against the full source-column census."""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path

from .source_column_matrix import canonical_alias, normalize_column_name

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GATE = ROOT / "docs" / "witness-column-master-matrix.json"
DEFAULT_SOURCES = ROOT / "docs" / "source-column-matrix.json"
DEFAULT_EXTERNAL_CROSSWALK = ROOT / "docs" / "external-witness-crosswalk.json"
DEFAULT_OUT = ROOT / "docs"


def source_gap_disposition(canonical: str) -> str:
    """Triage source-only columns without silently treating support fields as stats."""
    name = str(canonical or "").lower()
    if re.fullmatch(r"(?:\d+|\d+_\d+|\d+_[a-z_]+)", name) or name.endswith("_a_m"):
        return "semantic_bucket_or_scraped_header"
    support_tokens = ("_id", "_url", "_urls", "_links", "_link_ids", "_link_texts", "_json", "_name", "_abbr")
    support_names = {"bundle_source", "coverage_statuses", "already_in_supertable", "date", "category", "computed"}
    if name in support_names or name.endswith(support_tokens):
        return "raw_support_or_lineage_field"
    return "candidate_stat_or_derivation"


def source_gap_action(disposition: str) -> str:
    return {
        "candidate_stat_or_derivation": "review_add_gate_stat_or_declared_derivation",
        "raw_support_or_lineage_field": "retain_as_support_or_lineage_not_user_stat",
        "semantic_bucket_or_scraped_header": "normalize_semantic_bucket_or_exclude_structural_field",
    }[disposition]


def source_gap_evidence_role(obs: list[dict], source_by_id: dict, disposition: str) -> str:
    """Describe how source-only evidence can be used without calling it a gate stat."""
    if disposition == "raw_support_or_lineage_field":
        return "support_or_lineage"
    source_ids = {o.get("source_id", "") for o in obs}
    if any("pbp" in sid or ":pbp" in sid for sid in source_ids):
        return "play_level_input_or_aggregation_input"
    grains = {source_by_id.get(sid, {}).get("grain") for sid in source_ids}
    if "game" in grains:
        return "game_or_context_stat"
    if "season" in grains or "season_post" in grains:
        return "season_stat_witness"
    if "career" in grains:
        return "career_stat_witness"
    return "source_stat_or_context_evidence"


def gate_gap_action(column: str, detail: dict, declared_status: str, status: str) -> str | None:
    if status != "not_sourced":
        return None
    name = str(column or "").lower()
    if ("recomputed_at" in name or "repaired_at" in name or "_merged_at_" in name
            or "_populated_at_" in name or name == "last_updated" or name.endswith("_log")):
        return "register_pipeline_provenance_control"
    if declared_status.startswith("excluded_source"):
        return "replace_excluded_newspaper_witness"
    if "registered_source" in declared_status:
        if detail.get("verdict") in {"derived_witnessed", "text_derived"}:
            if any(token in name for token in ("allpro", "all_pro", "pro_bowl", "cpoy", "dpoy", "droy", "mvp", "opoy", "oroy")):
                return "materialize_awards_membership"
            if name in {"rushing_tds_40plus", "rushing_tds_50plus"}:
                return "materialize_pbp_td_yard_bucket"
            if "fg_" in name or name.startswith("fgm"):
                return "materialize_field_goal_bucket"
            if name.endswith("snap_pct"):
                return "derive_snap_share_rate"
            return "materialize_declared_derivation"
        return "map_or_materialize_declared_source_column"
    if detail.get("verdict") in {"derived_witnessed", "text_derived"}:
        return "declare_and_materialize_derivation"
    if detail.get("family") in {"flag_provenance", "identity"}:
        return "register_identity_or_flag_witness"
    return "register_direct_witness"


def gate_gap_blocker(column: str, detail: dict, action: str | None) -> str | None:
    if not action or action == "register_pipeline_provenance_control":
        return None
    name = str(column or "").lower()
    if action == "materialize_awards_membership":
        return "registered_award_tables_expose_support_rows_but_not_semantic_membership_atoms"
    if action == "materialize_pbp_td_yard_bucket":
        return "requires_play_level_rush_td_and_yards_gained_aggregation"
    if action == "materialize_field_goal_bucket":
        return "requires_field_goal_distance_bucket_count_not_just_fg_long"
    if action == "derive_snap_share_rate":
        return "requires_offense_defense_special_teams_snap_denominator_reconciliation"
    if name in {"receiving_completed_air_yards", "passing_completed_air_yards"}:
        return "registered_air_yard_source_lacks_completed_play_qualifier_column"
    if name == "seasons_started":
        return "requires_distinct_started_seasons_not_games_started"
    if action == "replace_excluded_newspaper_witness":
        return "newspaper_evidence_excluded_by_policy"
    return "source_column_not_semantically_mapped"


_GATE_GAP_CANDIDATES = {
    "nfl_team": ("existing_source_candidate_needs_semantic_review", "team", "verify player-team grain before accepting"),
    "nfl_teams": ("derive_from_existing_source_columns", "team", "derive distinct team history"),
    "nfl_team_count": ("derive_from_existing_source_columns", "team", "derive distinct team count"),
    "opponent_nfl_team": ("existing_source_candidate_needs_semantic_review", "team", "requires opponent context"),
    "opponent_nfl_franchise_number": ("existing_source_candidate_needs_semantic_review", "opponent_franchise_id", "verify identity meaning and grain"),
    "nfl_franchise_number": ("existing_source_candidate_needs_semantic_review", "franchise_id", "verify identity meaning and grain"),
    "primary_position": ("existing_source_candidate_needs_semantic_review", "position", "actual NFL position, not lineup slot"),
    "position_candidates": ("existing_source_candidate_needs_semantic_review", "position", "candidate identity evidence; verify candidate-generation rule"),
    "season_positions": ("derive_from_existing_source_columns", "position", "aggregate distinct positions by season"),
    "career_positions": ("derive_from_existing_source_columns", "position", "aggregate distinct positions across career"),
    "starter_position": ("existing_source_candidate_needs_semantic_review", "position", "verify starter-position semantics"),
    "home_away": ("existing_source_candidate_needs_semantic_review", "is_away", "normalize boolean away flag to home/away label"),
    "rookie_year": ("derive_from_existing_source_columns", "first_year;draft_year", "derive only after defining undrafted/first-season rule"),
    "years_active": ("derive_from_existing_source_columns", "first_year;last_year", "derive inclusive active-year span"),
    "years_active_1": ("derive_from_existing_source_columns", "first_year;last_year", "verify duplicate/legacy meaning before deriving"),
    "probowls": ("existing_source_candidate_needs_semantic_review", "pro_bowl", "membership/count semantics still require award materialization"),
}


def gate_gap_resolution(column: str, detail: dict, action: str | None, status: str) -> tuple[str | None, str | None, str | None]:
    """Classify the next resolution without silently converting a candidate to a witness."""
    if status != "not_sourced":
        return (None, None, None)
    name = str(column or "").lower()
    if action == "register_pipeline_provenance_control":
        return ("pipeline_control_registration", None, "register the transformation/provenance control that creates this field")
    if name == "last_updated" or any(token in name for token in ("_repaired_at_", "_recomputed_at_", "_merged_at_", "_populated_at_")):
        return ("pipeline_control_registration", None, "register the pipeline timestamp/control that creates this field")
    if name == "fantasy_position":
        return ("local_lineup_slot_witness_required", None, "witness the league lineup-slot assignment; do not map to NFL position")
    if action == "replace_excluded_newspaper_witness":
        return ("policy_replacement", None, "replace excluded newspaper evidence with an allowed witness")
    if name in _GATE_GAP_CANDIDATES:
        return _GATE_GAP_CANDIDATES[name]
    if action in {"materialize_awards_membership", "materialize_pbp_td_yard_bucket", "materialize_field_goal_bucket", "derive_snap_share_rate", "materialize_declared_derivation"}:
        return ("new_derivation_required", None, "implement and register the declared derivation")
    if action == "map_or_materialize_declared_source_column":
        return ("registered_source_requires_semantic_mapping", None, "map the declared source column or materialize its semantic projection")
    if detail.get("family") in {"flag_provenance", "identity"}:
        return ("new_identity_source_required", None, "register the identity/flag-producing witness")
    return ("new_stat_source_required", None, "locate or harvest a witness with the same semantic definition")


def declared_source_candidates(column: str, inventory: list[dict]) -> list[dict]:
    """List near-miss columns exposed by declared pages; these are not aliases."""
    name = str(column or "").lower()
    if any(token in name for token in ("allpro", "all_pro", "pro_bowl", "probowls", "mvp", "opoy", "oroy", "cpoy", "dpoy", "droy")):
        candidates = [("votes", "voting/support evidence, not membership"), ("votes_first", "voting/support evidence, not membership"), ("pos", "award-page position context"), ("year", "award year context")]
    elif name in {"receiving_completed_air_yards", "passing_completed_air_yards"}:
        candidates = [("pass_air_yds" if name.startswith("passing") else "rec_air_yds", "air-yard total; completed-play qualifier still required"),
                      ("pass_cmp" if name.startswith("passing") else "rec", "completion/reception denominator candidate")]
    elif name in {"rushing_tds_40plus", "rushing_tds_50plus"}:
        candidates = [("rush_td", "touchdown count candidate"), ("rushing_yards", "play-yard threshold candidate"), ("yards_gained", "play-yard threshold candidate")]
    elif name in {"fg_yards_over_30_canonical", "fg_yds_over_30", "fg_missed_0_19", "fg_yards_canonical"}:
        candidates = [("fg_long", "longest field goal only; insufficient for bucket count"), ("fgm", "made count candidate"), ("fga", "attempt count candidate")]
    elif name in {"defense_snap_pct", "special_teams_snap_pct", "offense_snap_pct"}:
        candidates = [("defense", "snap numerator candidate"), ("offense", "snap numerator candidate"), ("special_teams", "snap numerator candidate"), ("g", "denominator candidate")]
    elif name == "seasons_started":
        candidates = [("gs", "games-started candidate; not distinct started seasons"), ("g", "games-played denominator/context")]
    else:
        return []
    available = {c for entry in inventory for c in entry.get("available_columns", [])}
    return [{"column": col, "present": col in available, "note": note} for col, note in candidates]


def _witness_testimony(witnesses: list[tuple[dict, list[dict]]], source_by_id: dict) -> list[dict]:
    """Return compact, per-source testimony for a canonical column.

    The source-column matrix remains the year-level authority.  This rollup is
    deliberately a projection of that matrix so a gate row can be audited
    without resolving source IDs back through a second file.
    """
    testimony = []
    for observation, rows in sorted(witnesses, key=lambda item: (item[0]["source_id"], item[0]["raw_column"])):
        years = sorted({r["year"] for r in rows if isinstance(r.get("year"), int)})
        source = source_by_id.get(observation["source_id"], {})
        atom = next((a for key, a in source.get("atoms", {}).items()
                     if a.get("col", key) == observation["raw_column"]), {})
        expected_min = atom.get("era_min") if atom.get("era_min") is not None else source.get("era_min")
        expected_max = atom.get("era_max") if atom.get("era_max") is not None else source.get("era_max")
        expected_years = set()
        if expected_min is not None and expected_max is not None:
            expected_years = set(range(int(expected_min), int(expected_max) + 1))
        density_fields = {
            "all_row_density": "all",
            "eligible_row_density": "eligible",
            "signal_density": "signal",
            "year_coverage_density": "year",
        }
        density_ranges = {}
        for field, label in density_fields.items():
            values = [r[field] for r in rows if r.get(field) is not None]
            density_ranges[label] = {
                "min": min(values) if values else None,
                "max": max(values) if values else None,
            }
        testimony.append({
            "source_id": observation["source_id"],
            "source_family": source.get("source_family"),
            "source_year_axis_status": source.get("year_axis_status") or (
                "bounded_observed_or_declared"
                if source.get("era_min") is not None and source.get("era_max") is not None
                else "unbounded_no_integer_year_axis"
            ),
            "raw_column": observation["raw_column"],
            "canonical_column": observation.get("canonical_column"),
            "relationship_type": observation.get("relationship_type"),
            "alias_family": observation.get("alias_family"),
            "mapping_confidence": observation.get("mapping_confidence"),
            "transformation_class": observation.get("transformation_class"),
            "expected_year_min": expected_min,
            "expected_year_max": expected_max,
            "observed_year_min": min(years, default=None),
            "observed_year_max": max(years, default=None),
            "observed_year_count": len(years),
            "missing_years": sorted(expected_years - set(years)) if expected_years else [],
            "coverage_rows": len(rows),
            "density": density_ranges,
        })
    return testimony


def _testimony_json(testimony: list[dict]) -> str:
    return json.dumps(testimony, separators=(",", ":"), sort_keys=True)


def _aggregate_density(testimony: list[dict]) -> dict:
    result = {}
    for label in ("all", "eligible", "signal", "year"):
        values = [
            entry["density"][label][bound]
            for entry in testimony for bound in ("min", "max")
            if entry["density"][label][bound] is not None
        ]
        result[f"min_{label}_density"] = min(values) if values else None
        result[f"max_{label}_density"] = max(values) if values else None
    return result


def build_crosswalk(gate_path: Path = DEFAULT_GATE, source_path: Path = DEFAULT_SOURCES,
                    external_crosswalk_path: Path = DEFAULT_EXTERNAL_CROSSWALK) -> dict:
    gate = json.loads(Path(gate_path).read_text(encoding="utf-8"))
    source = json.loads(Path(source_path).read_text(encoding="utf-8"))
    external_columns = set()
    if Path(external_crosswalk_path).exists():
        external = json.loads(Path(external_crosswalk_path).read_text(encoding="utf-8"))
        external_columns = {row.get("canonical_column") for row in external.get("rows", [])
                            if row.get("row_type") == "workbook_column" and row.get("canonical_column")}
    observations = source["observations"]
    coverage = source["coverage"]
    source_by_id = {row["source_id"]: row for row in source.get("sources", [])}
    registered_source_ids = set(source_by_id)
    excluded_source_ids = {row.get("source_id") for row in source.get("exclusions", []) if row.get("source_id")}
    by_canonical = {}
    for row in observations:
        by_canonical.setdefault(row["canonical_column"], []).append(row)
    coverage_by_key = {}
    for row in coverage:
        coverage_by_key.setdefault((row["source_id"], row["raw_column"]), []).append(row)
    gate_definitions = {}
    for columns in gate["tables"].values():
        for column, detail in columns.items():
            gate_definitions.setdefault(_canonical_gate_name(column), detail)
    resolve_cache = {}

    def declared_witness_status(refs: list[str]) -> str:
        if not refs:
            return "none_declared"
        statuses = set()
        for ref in refs:
            if ref.startswith("newspaper_") or ref in excluded_source_ids:
                statuses.add("excluded_source")
            elif ref in registered_source_ids or any(sid.startswith(ref + " ") for sid in registered_source_ids):
                statuses.add("registered_source_column_unmapped")
            elif ref == "pfr_context:voting_pages" and any(sid.startswith("pfr_context:voting_") for sid in registered_source_ids):
                statuses.add("registered_source_family_column_unmapped")
            else:
                statuses.add("unregistered_source")
        return ";".join(sorted(statuses))

    def declared_source_summary(refs: list[str]) -> list[dict]:
        """Summarize declared source pages and the columns they actually expose."""
        summaries = []
        for ref in refs:
            source_ids = sorted(
                sid for sid in registered_source_ids
                if sid == ref or sid.startswith(ref + " ")
                or (ref == "pfr_context:voting_pages" and sid.startswith("pfr_context:voting_"))
            )
            for source_id in source_ids:
                source_row = source_by_id.get(source_id, {})
                columns = sorted({o["raw_column"] for o in observations if o.get("source_id") == source_id})
                summaries.append({
                    "declared_ref": ref,
                    "source_id": source_id,
                    "source_family": source_row.get("source_family"),
                    "source_year_axis_status": source_row.get("year_axis_status") or (
                        "bounded_observed_or_declared"
                        if source_row.get("era_min") is not None and source_row.get("era_max") is not None
                        else "unbounded_no_integer_year_axis"
                    ),
                    "grain": source_row.get("grain"),
                    "era_min": source_row.get("era_min"),
                    "era_max": source_row.get("era_max"),
                    "n_rows": source_row.get("n_rows"),
                    "available_column_count": len(columns),
                    "available_columns": columns,
                })
        return summaries

    def resolves(name: str, stack: tuple[str, ...] = ()) -> bool:
        canonical = _canonical_gate_name(name)
        if canonical in resolve_cache:
            return resolve_cache[canonical]
        if canonical in by_canonical:
            resolve_cache[canonical] = True
            return True
        if canonical in stack:
            return False
        detail = gate_definitions.get(canonical)
        if not detail or detail.get("verdict") != "derived_witnessed":
            resolve_cache[canonical] = False
            return False
        result = all(resolves(input_name, stack + (canonical,)) for input_name in detail.get("inputs", []))
        resolve_cache[canonical] = result
        return result

    direct_input_cache = {}

    def direct_input_observations(names: list[str], stack: tuple[str, ...] = ()) -> list[tuple[dict, list[dict]]]:
        """Expand a derivation chain to the direct source testimony beneath it."""
        result = []
        for name in names:
            canonical = _canonical_gate_name(name)
            if canonical in stack:
                continue
            if canonical not in direct_input_cache:
                direct = by_canonical.get(canonical, [])
                if direct:
                    expanded = [
                        (observation, coverage_by_key.get((observation["source_id"], observation["raw_column"]), []))
                        for observation in direct
                    ]
                else:
                    detail = gate_definitions.get(canonical, {})
                    expanded = direct_input_observations(detail.get("inputs", []), stack + (canonical,))
                direct_input_cache[canonical] = expanded
            result.extend(direct_input_cache[canonical])
        unique = {}
        for observation, rows in result:
            unique[(observation["source_id"], observation["raw_column"])] = (observation, rows)
        return [unique[key] for key in sorted(unique)]

    gate_rows = []
    gate_canonical = set()
    for table, columns in gate["tables"].items():
        for column, detail in columns.items():
            canonical = _canonical_gate_name(column)
            gate_canonical.add(canonical)
            witnesses = []
            for observation in by_canonical.get(canonical, []):
                rows = coverage_by_key.get((observation["source_id"], observation["raw_column"]), [])
                witnesses.append((observation, rows))
            all_years = sorted({r["year"] for _, rows in witnesses for r in rows if isinstance(r["year"], int)})
            expected_years = sorted({r["year"] for _, rows in witnesses for r in rows if isinstance(r["year"], int) and r["year"] is not None})



            sources = sorted({o["source_id"] for o, _ in witnesses})
            witness_keys = sorted({f"{o['source_id']}:{o['raw_column']}" for o, _ in witnesses})
            inputs = detail.get("inputs", [])
            testimony = _witness_testimony(witnesses, source_by_id)
            direct_input_witnesses = []
            for input_name in inputs:
                input_witnesses = by_canonical.get(_canonical_gate_name(input_name), [])
                direct_input_witnesses.extend(
                    (observation, coverage_by_key.get((observation["source_id"], observation["raw_column"]), []))
                    for observation in input_witnesses
                )
            input_testimony = _witness_testimony(direct_input_witnesses, source_by_id)
            transitive_input_witnesses = direct_input_observations(inputs, (canonical,))
            input_witness_keys = sorted({
                f"{observation['source_id']}:{observation['raw_column']}"
                for observation, _ in transitive_input_witnesses
            })
            expected_ranges = []
            for observation, _ in witnesses:
                source_row = source_by_id.get(observation["source_id"], {})
                atom = next((a for key, a in source_row.get("atoms", {}).items()
                             if a.get("col", key) == observation["raw_column"]), {})
                era_min = atom.get("era_min") if atom.get("era_min") is not None else source_row.get("era_min")
                era_max = atom.get("era_max") if atom.get("era_max") is not None else source_row.get("era_max")
                if era_min is None or era_max is None: continue
                expected_ranges.append((int(era_min), int(era_max)))
            expected_year_min = min((r[0] for r in expected_ranges), default=None)
            expected_year_max = max((r[1] for r in expected_ranges), default=None)
            measured_rows = [r for _, rows in witnesses for r in rows if r["all_row_density"] is not None]
            declared_refs = detail.get("witnesses", [])
            declared_source_inventory = declared_source_summary(declared_refs)
            sourced_inputs = [name for name in inputs if resolves(name)]
            missing_inputs = [name for name in inputs if not resolves(name)]
            missing_year_count = 0
            missing_years = set()
            missing_year_shapes = set()
            for observation, rows in witnesses:
                source_row = source_by_id.get(observation["source_id"], {})
                atom = next((a for key, a in source_row.get("atoms", {}).items()
                             if a.get("col", key) == observation["raw_column"]), {})
                source_years = {r["year"] for r in rows if isinstance(r["year"], int)}
                era_min = atom.get("era_min") if atom.get("era_min") is not None else source_row.get("era_min")
                era_max = atom.get("era_max") if atom.get("era_max") is not None else source_row.get("era_max")
                if era_min is None or era_max is None: continue
                era_min, era_max = int(era_min), int(era_max)
                missing_for_observation = set(range(era_min, era_max + 1)) - source_years
                missing_years.update(missing_for_observation)
                missing_year_count += len(missing_for_observation)
                if missing_for_observation and not source_years:
                    missing_year_shapes.add("unobserved_year_axis")
                    continue
                if not missing_for_observation or not source_years:
                    continue
                observed_min, observed_max = min(source_years), max(source_years)
                shape = []
                if any(year < observed_min for year in missing_for_observation):
                    shape.append("leading")
                if any(observed_min < year < observed_max for year in missing_for_observation):
                    shape.append("interior")
                if any(year > observed_max for year in missing_for_observation):
                    shape.append("trailing")
                missing_year_shapes.add("+".join(shape) or "full_span")
            if not witnesses and detail.get("verdict") == "derived_witnessed" and not missing_inputs:
                status, gap = "derived_inputs_sourced", None
            elif not witnesses and detail.get("verdict") == "derived_witnessed":
                status, gap = "derivation_input_gap", "missing_derivation_inputs"
            elif not witnesses:
                status, gap = "not_sourced", "no_source_column"
            elif not measured_rows:
                status, gap = "contract_only", "density_unmeasured"
            elif missing_year_count:
                status, gap = "partial_year_coverage", "missing_years_within_witness_span"
            else:
                status, gap = "sourced", None
            action = gate_gap_action(column, detail, declared_witness_status(declared_refs), status)
            resolution_class, candidate_source_columns, resolution_note = gate_gap_resolution(column, detail, action, status)
            candidate_witnesses = []
            for candidate in (candidate_source_columns or "").split(";"):
                if not candidate:
                    continue
                candidate_canonical = canonical_alias(normalize_column_name(candidate))
                candidate_witnesses.extend(
                    (observation, coverage_by_key.get((observation["source_id"], observation["raw_column"]), []))
                    for observation in by_canonical.get(candidate_canonical, [])
                )
            candidate_testimony = _witness_testimony(candidate_witnesses, source_by_id)
            declared_candidates = declared_source_candidates(column, declared_source_inventory)
            gate_rows.append({
                "row_type": "gate_column", "gate_table": table, "gate_column": column,
                "gate_canonical_column": canonical, "gate_verdict": detail.get("verdict"),
                "gate_family": detail.get("family"), "gate_rule": detail.get("rule"),
                "gate_inputs": detail.get("inputs", []), "gate_unwitnessed_inputs": detail.get("unwitnessed_inputs", []),
                "declared_witness_refs": ";".join(declared_refs),
                "declared_witness_status": declared_witness_status(declared_refs),
                "declared_source_inventory_json": _testimony_json(declared_source_inventory),
                "declared_source_candidate_columns_json": _testimony_json(declared_candidates),
                "gate_gap_action": action,
                "gate_gap_blocker": gate_gap_blocker(column, detail, action),
                "gate_gap_resolution_class": resolution_class,
                "gate_gap_candidate_source_columns": candidate_source_columns,
                **{
                    "gate_gap_resolution_note": resolution_note,
                    "gate_gap_candidate_source_testimony_json": _testimony_json(candidate_testimony),
                    "source_gap_evidence_role": None,
                    "sourced_input_count": len(sourced_inputs), "missing_input_count": len(missing_inputs),
                    "missing_input_names": ";".join(missing_inputs),
                    "source_count": len(sources), "source_ids": ";".join(sources),
                    "witness_observation_keys": ";".join(witness_keys),
                    "witness_testimony_json": _testimony_json(testimony),
                    "derivation_input_testimony_json": _testimony_json(input_testimony),
                    "derivation_input_witness_keys": ";".join(input_witness_keys),
                },
                **_aggregate_density(testimony),
                **{
                    "expected_year_min": expected_year_min, "expected_year_max": expected_year_max,
                    "observed_year_min": min(all_years, default=None), "observed_year_max": max(all_years, default=None),
                    "missing_year_count": missing_year_count, "missing_years": ";".join(map(str, sorted(missing_years))),
                    "missing_year_shape": ";".join(sorted(missing_year_shapes)),
                    "measured_coverage_rows": len(measured_rows),
                    "status": status, "gap_type": gap,
                },
            })

    not_derived = []
    for canonical, obs in sorted(by_canonical.items()):
        if canonical in gate_canonical:
            continue
        rows = [r for o in obs for r in coverage_by_key.get((o["source_id"], o["raw_column"]), [])]
        years = sorted({r["year"] for r in rows if isinstance(r["year"], int)})
        testimony = _witness_testimony(
            [(o, coverage_by_key.get((o["source_id"], o["raw_column"]), [])) for o in obs],
            source_by_id,
        )
        disposition = source_gap_disposition(canonical)
        not_derived.append({
            "row_type": "source_not_in_gate", "gate_table": None, "gate_column": None,
            "gate_canonical_column": canonical, "gate_verdict": None, "gate_family": None,
            "gate_rule": None, "gate_inputs": [], "gate_unwitnessed_inputs": [],
            "declared_witness_refs": None, "declared_witness_status": None,
            "declared_source_inventory_json": "[]",
            "declared_source_candidate_columns_json": "[]",
            "gate_gap_action": None,
            "gate_gap_blocker": None,
            "gate_gap_resolution_class": None,
            "gate_gap_candidate_source_columns": None,
            **{
                "gate_gap_resolution_note": None,
                "gate_gap_candidate_source_testimony_json": "[]",
                "source_gap_evidence_role": source_gap_evidence_role(obs, source_by_id, disposition),
                "sourced_input_count": None, "missing_input_count": None, "missing_input_names": None,
                "source_count": len({o["source_id"] for o in obs}),
                "source_ids": ";".join(sorted({o["source_id"] for o in obs})),
                "witness_observation_keys": ";".join(sorted({f"{o['source_id']}:{o['raw_column']}" for o in obs})),
                "witness_testimony_json": _testimony_json(testimony),
                "derivation_input_testimony_json": "[]",
                "derivation_input_witness_keys": "",
            },
            **_aggregate_density(testimony),
            **{
                "expected_year_min": min((source_by_id.get(o["source_id"], {}).get("era_min") for o in obs if source_by_id.get(o["source_id"], {}).get("era_min") is not None), default=None),
                "expected_year_max": max((source_by_id.get(o["source_id"], {}).get("era_max") for o in obs if source_by_id.get(o["source_id"], {}).get("era_max") is not None), default=None),
                "observed_year_min": min(years, default=None), "observed_year_max": max(years, default=None),
                "missing_year_count": None, "missing_years": None,
                "missing_year_shape": None,
                "source_gap_disposition": disposition,
                "source_gap_action": source_gap_action(disposition),
                "source_gap_evidence_role": source_gap_evidence_role(obs, source_by_id, disposition),
                "external_universe_status": "in_external_universe" if canonical in external_columns else "lake_only",
                "measured_coverage_rows": sum(r["all_row_density"] is not None for r in rows),
                "status": "source_stat_not_yet_derived", "gap_type": "available_source_stat_not_in_gate",
            },
        })

    rows = gate_rows + not_derived
    return {
        "generated": "2026-07-21", "external_artifact": "comparison_pending_403",
        "gate_source": str(gate_path), "source_matrix": str(source_path),
        "rows": rows,
        "summary": {
            "gate_column_count": len(gate_rows), "source_not_in_gate_count": len(not_derived),
            "sourced_gate_column_count": sum(r["status"] == "sourced" for r in gate_rows),
            "derived_inputs_sourced_count": sum(r["status"] == "derived_inputs_sourced" for r in gate_rows),
            "derivation_input_gap_count": sum(r["status"] == "derivation_input_gap" for r in gate_rows),
            "partial_year_gate_column_count": sum(r["status"] == "partial_year_coverage" for r in gate_rows),
            "contract_only_gate_column_count": sum(r["status"] == "contract_only" for r in gate_rows),
            "not_sourced_gate_column_count": sum(r["status"] == "not_sourced" for r in gate_rows),
            "source_gap_disposition_counts": dict(Counter(r["source_gap_disposition"] for r in not_derived)),
            "source_gap_external_universe_counts": dict(Counter(r["external_universe_status"] for r in not_derived)),
            "source_gap_action_counts": dict(Counter(r["source_gap_action"] for r in not_derived)),
            "source_gap_evidence_role_counts": dict(Counter(r["source_gap_evidence_role"] for r in not_derived)),
            "gate_gap_declared_witness_status_counts": dict(Counter(r["declared_witness_status"] for r in gate_rows if r["status"] == "not_sourced")),
            "gate_gap_action_counts": dict(Counter(r["gate_gap_action"] for r in gate_rows if r["status"] == "not_sourced")),
            "gate_gap_resolution_class_counts": dict(Counter(r["gate_gap_resolution_class"] for r in gate_rows if r["status"] == "not_sourced")),
            "gate_gap_blocker_counts": dict(Counter(r["gate_gap_blocker"] for r in gate_rows if r["status"] == "not_sourced" and r["gate_gap_blocker"])),
        },
    }


def _canonical_gate_name(value: str) -> str:
    normalized = normalize_column_name(value)
    return canonical_alias(normalized)


def write_outputs(crosswalk: dict, out_dir: Path = DEFAULT_OUT) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "witness-gate-crosswalk.json").write_text(json.dumps(crosswalk, indent=2) + "\n", encoding="utf-8")
    fields = sorted({key for row in crosswalk["rows"] for key in row})
    with (out_dir / "witness-gate-crosswalk.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader(); writer.writerows(crosswalk["rows"])
    gap_rows = []
    priorities = {
        "missing_derivation_inputs": "P0", "missing_years_within_witness_span": "P1",
        "no_source_column": "P2", "available_source_stat_not_in_gate": "P3",
    }
    for row in crosswalk["rows"]:
        if row.get("gap_type") in priorities:
            gap = dict(row)
            gap["priority"] = priorities[row["gap_type"]]
            gap_rows.append(gap)
    gap_fields = sorted({key for row in gap_rows for key in row})
    with (out_dir / "witness-gate-gap-ledger.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=gap_fields)
        writer.writeheader(); writer.writerows(gap_rows)
    s = crosswalk["summary"]
    lines = [
        "# Witness gate crosswalk", "",
        "This compares the current table-column gate with the complete source-column matrix.",
        "The supplied external column universe is crosswalked separately in `external-witness-crosswalk.md`; this report remains the local gate-versus-lake comparison.", "",
        "## Summary", "",
        f"- Current gate columns: {s['gate_column_count']}",
        f"- Sourced: {s['sourced_gate_column_count']}; partial year coverage: {s['partial_year_gate_column_count']}",
        f"- Derived through sourced inputs: {s['derived_inputs_sourced_count']}; derivation input gaps: {s['derivation_input_gap_count']}",
        f"- Contract-only: {s['contract_only_gate_column_count']}; not sourced: {s['not_sourced_gate_column_count']}",
        f"- Source columns available but not represented/derived in the gate: {s['source_not_in_gate_count']}", "",
        f"- Source-only triage: {s['source_gap_disposition_counts']}", "",
        f"- Source-only required actions: {s['source_gap_action_counts']}", "",
        f"- Source-only evidence roles: {s['source_gap_evidence_role_counts']}", "",
        f"- Source-only external-universe split: {s['source_gap_external_universe_counts']}", "",
        f"- Unsourced gate declared-witness split: {s['gate_gap_declared_witness_status_counts']}", "",
        f"- Unsourced gate required actions: {s['gate_gap_action_counts']}", "",
        f"- Unsourced gate resolution classes: {s['gate_gap_resolution_class_counts']}", "",
        f"- Unsourced gate blockers: {s['gate_gap_blocker_counts']}", "",
        "## Gap meanings", "",
        "- `no_source_column`: gate output has no matching witness column.",
        "- `derived_inputs_sourced`: gate output is derived, and every declared input has source testimony.",
        "- `missing_derivation_inputs`: a declared derived-field input has no source testimony.",
        "- `missing_years_within_witness_span`: a source witness has gaps between its observed year endpoints.",
        "- `missing_year_shape`: distinguishes `leading`, `interior`, `trailing`, and `unobserved_year_axis` coverage defects.",
        "- `available_source_stat_not_in_gate`: a source column exists but no current gate column/derivation represents it.", "",
        "Every row carries `witness_testimony_json` with per-source expected/observed year ranges and all/eligible/signal/year density ranges. Derived rows additionally carry `derivation_input_witness_keys`; their direct-input testimony is in `derivation_input_testimony_json`, while the source matrix remains the year-level authority.", "",
        "Rows with declared witnesses also carry `declared_source_inventory_json`, listing the registered source page's grain, era, row count, and exposed columns even when no semantic gate mapping exists.", "",
        "Each testimony entry also carries `source_year_axis_status`; `unbounded_no_integer_year_axis` is an explicit limitation, never an inferred full-era claim.", "",
        "Blocked semantic rows also carry `declared_source_candidate_columns_json`; `present: true` means the page exposes a near-miss input, not that the gate definition is proven.", "",
        "Source-only rows also carry `source_gap_disposition`: `candidate_stat_or_derivation`, `raw_support_or_lineage_field`, or `semantic_bucket_or_scraped_header`.", "",
        "## Prioritized gap ledger", "",
        "- P0: derivation inputs missing from all source testimony.",
        "- P1: source testimony has missing years inside its declared era.",
        "- P2: current gate columns have no matching source testimony.",
        "- P3: source columns exist but are not represented or derived by the current gate.", "",
        "See `witness-gate-gap-ledger.csv` for the complete prioritized list.",
    ]
    (out_dir / "witness-gate-crosswalk.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gate", type=Path, default=DEFAULT_GATE)
    parser.add_argument("--source-matrix", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    result = build_crosswalk(args.gate, args.source_matrix)
    write_outputs(result, args.out_dir)
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()
