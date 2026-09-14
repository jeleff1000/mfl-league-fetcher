"""Core normalization and source-column mapping helpers."""
from __future__ import annotations

import re
import unicodedata
import json
from pathlib import Path

from source_column_matrix_schema import COLUMN_FIELDS, OBSERVATION_FIELDS, SOURCE_FIELDS, validate_fields


def is_newspaper_source(source_id: str) -> bool:
    value = str(source_id).strip().lower()
    return value.startswith("newspaper") or "/newspaper" in value or "\\newspaper" in value


def normalize_column_name(raw_name: str) -> str:
    value = unicodedata.normalize("NFKC", str(raw_name)).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


_KNOWN_ALIASES = {
    "pass_yds": "passing_yards",
    "passing_yds": "passing_yards",
    "pass_yards": "passing_yards",
    "rec_yds": "receiving_yards",
    "receiving_yds": "receiving_yards",
    "rush_yds": "rushing_yards",
    "rushing_yds": "rushing_yards",
    "fg_made_60_plus_canonical": "fg_made_60plus",
    "fgm": "fg_made",
    "completion_pct": "comp_pct",
    "rushing_yards_per_carry": "yards_per_carry",
    "yards_per_target": "receiving_yards_per_target",
}


def _semantic_metadata(column: dict, canonical: str) -> dict:
    lower = canonical.lower()
    if lower.startswith("pass") or lower.startswith("qb_"):
        domain = "passing"
    elif lower.startswith(("rush", "carry", "carries")):
        domain = "rushing"
    elif lower.startswith(("rec", "target", "catch")):
        domain = "receiving"
    elif lower.startswith(("fg_", "pat_", "xp_")):
        domain = "kicking"
    elif lower.startswith(("def_", "tackle")):
        domain = "defense"
    elif lower.startswith(("punt", "kickoff", "return", "special_teams")):
        domain = "special_teams"
    else:
        domain = "general"
    role = "rate" if any(token in lower for token in ("pct", "percent", "rate", "per_", "average")) else "atom"
    if "td" in lower or "touchdown" in lower:
        role = "touchdown"
    elif "yard" in lower or lower.endswith(("yds", "long")):
        role = "yardage"
    elif lower.endswith(("attempts", "_att", "carries", "receptions", "targets")):
        role = "count"
    unit = "percent" if role == "rate" else "yards" if role == "yardage" else "count"
    return {
        "stat_domain": domain,
        "stat_role": role,
        "stat_unit": unit,
        "stat_credit_type": "player" if "player" in str(column.get("grain", "")).lower() else "source_defined",
        "canonical_semantic_id": f"{domain}:{canonical}",
        "raw_table_context": column.get("source_id"),
        "source_definition_version": column.get("source_definition_version") or "unversioned",
    }


def canonical_alias(raw_name: str) -> str:
    normalized = normalize_column_name(raw_name)
    return _KNOWN_ALIASES.get(normalized, normalized)


def build_canonical_registry(columns: list[dict], canonical_columns: set[str] | None = None) -> list[dict]:
    names = set(canonical_columns or set())
    names.update(canonical_alias(c.get("canonical_hint") or c["raw_column"]) for c in columns)
    return [
        {
            "canonical_column": name,
            "family": name.split("_", 1)[0],
            "dtype": None,
            "unit": None,
            "scope": None,
            "expected_grain": None,
            "null_semantics": "unknown",
            "zero_semantics": "explicit_zero",
            "source_of_truth_status": "candidate",
            "definition": None,
            **_semantic_metadata({"raw_column": name}, name),
        }
        for name in sorted(names)
    ]


def map_source_column(column: dict, canonical_registry: list[dict], alias_registry: dict | None = None) -> dict:
    aliases = alias_registry or {}
    raw = column["raw_column"]
    normalized = normalize_column_name(raw)
    canonical = column.get("canonical_hint")
    if canonical is None:
        for target, values in aliases.items():
            if normalized == normalize_column_name(target) or normalized in {normalize_column_name(v) for v in values}:
                canonical = target
                break
    if canonical is None:
        canonical = canonical_alias(raw)
    registry_names = {r["canonical_column"] for r in canonical_registry}
    if canonical not in registry_names:
        return {
            "source_id": column["source_id"],
            "raw_column": raw,
            "canonical_column": canonical,
            "relationship_type": "not_comparable",
            "alias_family": None,
            "mapping_confidence": "review",
            "review_status": "unmatched_canonical",
            "raw_dtype": column.get("dtype"),
            "normalized_dtype": column.get("dtype"),
            "unit_rule": None,
            "join_key": None,
            "identity_requirements": None,
            "transformation_class": "unknown",
            "gate_eligible": False,
            **_semantic_metadata(column, canonical),
        }
    relation = "exact_match" if normalized == normalize_column_name(canonical) else "alias"
    return {
        "source_id": column["source_id"],
        "raw_column": raw,
        "canonical_column": canonical,
        "relationship_type": relation,
        "alias_family": canonical if relation == "alias" else None,
        "mapping_confidence": "high",
        "review_status": "accepted",
        "raw_dtype": column.get("dtype"),
        "normalized_dtype": column.get("dtype"),
        "unit_rule": None,
        "join_key": None,
        "identity_requirements": None,
        "transformation_class": "renamed" if relation == "alias" else "direct",
        "gate_eligible": True,
        **_semantic_metadata(column, canonical),
    }


def relationship_type(left: str, right: str) -> str:
    left_norm, right_norm = normalize_column_name(left), normalize_column_name(right)
    if left_norm == right_norm:
        return "exact_match"
    if {left_norm, right_norm} == {"passing_yards", "receiving_yards"}:
        return "not_comparable"
    if canonical_alias(left_norm) == canonical_alias(right_norm):
        return "alias"
    return "not_comparable"


def validate_source(record: dict) -> None:
    validate_fields(record, SOURCE_FIELDS)


def validate_column(record: dict) -> None:
    validate_fields(record, COLUMN_FIELDS)


def validate_observation(record: dict) -> None:
    validate_fields(record, OBSERVATION_FIELDS)


def discover_sources(contract_path: Path) -> list[dict]:
    payload = json.loads(Path(contract_path).read_text(encoding="utf-8"))
    contracts = payload.get("contracts", payload)
    out = []
    for source_id, body in sorted(contracts.items()):
        path = body.get("path") or body.get("bundle") or ""
        source = {
            "source_id": source_id,
            "source_family": source_id.split(":", 1)[0],
            "display_name": source_id,
            "path": path,
            "table_name": source_id.split(":", 1)[1] if ":" in source_id else source_id,
            "grain": body.get("grain", "other"),
            "source_type": "contracted_source",
            "lineage_class": "unknown",
            "authority_class": body.get("cls", "unknown"),
            "snapshot_id": None,
            "newspaper_excluded": is_newspaper_source(source_id),
            "discovery_status": "contract_metadata",
            **{
                "n_rows": body.get("n_rows"),
                "era_min": min(
                    (a.get("era_min") for a in body.get("atoms", {}).values() if a.get("era_min") is not None),
                    default=None,
                ),
                "era_max": max(
                    (a.get("era_max") for a in body.get("atoms", {}).values() if a.get("era_max") is not None),
                    default=None,
                ),
                "atoms": body.get("atoms", {}),
                "by_year": body.get("by_year", {}),
                "meta_cols": body.get("meta_cols", []),
            },
        }
        out.append(source)
    return out


def discover_columns(source: dict) -> list[dict]:
    columns = []
    seen = set()
    for raw_name, atom in sorted(source.get("atoms", {}).items()):
        raw_column = atom.get("col", raw_name)
        # Source-specific semantic hints keep generic raw names from collapsing into
        # unrelated canonicals.
        canonical_hint = None
        if source.get("source_id") == "nflcom_season:rushing" and raw_name == "rushing__40" and raw_column == "40":
            canonical_hint = "rushing_40plus"
        elif source.get("source_id") in {"pfr_season:passing", "pfr_season:passing_post"} and raw_name == "pass_yds_per_att":
            canonical_hint = "yards_per_attempt"
        seen.add(raw_column)
        columns.append(
            {
                "source_id": source["source_id"],
                "raw_column": raw_column,
                "normalized_column": normalize_column_name(raw_column),
                "dtype": atom.get("dtype"),
                "year_min": atom.get("era_min"),
                "year_max": atom.get("era_max"),
                "total_nonzero": atom.get("total_nonzero"),
                "grain": source.get("grain"),
                "column_kind": "stat_atom",
                "canonical_hint": canonical_hint,
                "newspaper_excluded": source.get("newspaper_excluded", False),
            }
        )
    for raw_column in sorted(source.get("meta_cols", [])):
        if raw_column in seen:
            continue
        columns.append(
            {
                "source_id": source["source_id"],
                "raw_column": raw_column,
                "normalized_column": normalize_column_name(raw_column),
                "dtype": None,
                "year_min": source.get("era_min"),
                "year_max": source.get("era_max"),
                "total_nonzero": None,
                "grain": source.get("grain"),
                "column_kind": "metadata",
                "newspaper_excluded": source.get("newspaper_excluded", False),
            }
        )
    return columns
