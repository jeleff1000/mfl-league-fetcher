"""Configured cross-column reconciliation relationships."""
from __future__ import annotations

from source_column_matrix import normalize_column_name


def discover_relationships(observations: list[dict]) -> list[dict]:
    rows = []
    by_canonical = {}
    seen = set()
    for observation in observations:
        key = (observation["source_id"], observation["canonical_column"])
        if key in seen:
            continue
        by_canonical.setdefault(observation["canonical_column"], []).append(observation)
        seen.add(key)

    for canonical, items in sorted(by_canonical.items()):
        for left_index, left in enumerate(items):
            for right in items[left_index + 1 :]:
                if left["source_id"] == right["source_id"]:
                    continue
                rows.append(
                    _relationship(
                        left, right, "reconciliation_control",
                        f"compare({canonical})", "exact_or_unit_tolerance", "review",
                    )
                )

    component_groups = {
        "offensive_touchdowns": ("passing_touchdowns", "rushing_touchdowns", "receiving_touchdowns"),
        "offensive_yards": ("passing_yards", "rushing_yards", "receiving_yards"),
    }
    for total, components in component_groups.items():
        if total not in by_canonical:
            continue
        for component in components:
            for left in by_canonical.get(component, []):
                for right in by_canonical[total]:
                    rows.append(
                        _relationship(
                            left, right, "component_of",
                            f"{component} contributes to {total}",
                            "exact_or_unit_tolerance", "review",
                        )
                    )

    return rows


def _relationship(left: dict, right: dict, kind: str, formula: str, tolerance: str, status: str) -> dict:
    return {
        "left_source_id": left["source_id"],
        "left_column": left["raw_column"],
        "left_canonical_column": left["canonical_column"],
        "right_source_id": right["source_id"],
        "right_column": right["raw_column"],
        "right_canonical_column": right["canonical_column"],
        "relationship_type": kind,
        "formula": formula,
        "tolerance": tolerance,
        "overlap_rows": None,
        "agreement_rows": None,
        "conflict_rows": None,
        "agreement_density": None,
        "conflict_density": None,
        "systematic_bias": None,
        "adjudication_status": status,
    }
