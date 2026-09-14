"""Explicit source aliases and witness-independence rules."""

from __future__ import annotations

from typing import Any

SOURCE_ALIASES = {
    "pbp_merged": "pbp_merged_1978_2025",
    "pbp_weekly_rollup": "pbp_player_week_rollup",
    # OQ-LR-5: the composite key retired 2026-07-26; historic "ancient_bundle" refs
    # meant the PFA-gamelog-carrying bundle -- alias to the pfa stream source.
    "ancient_bundle": "ancient_pfa_gamelog",
    "ancient_ready_bundle": "ancient_pfa_gamelog",
    "legacy_supertable": "legacy_motherduck_supertable",
    "pfr_season:passing": "pfr_player_season_passing",
}


def _canonical_alias(source_id: str) -> str:
    seen: set[str] = set()
    current = source_id
    while current in SOURCE_ALIASES and current not in seen:
        seen.add(current)
        current = SOURCE_ALIASES[current]
    return current


def build_lineage_nodes(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    sources = inventory if isinstance(inventory, list) else inventory.get("sources") or inventory.get("registered_sources") or inventory.get("artifacts") or []
    nodes: list[dict[str, Any]] = []
    for source in sources:
        source_id = str(source.get("source_id") or source.get("id") or source.get("name"))
        alias_target = _canonical_alias(source_id)
        parent = source.get("source_parent_id") or source.get("parent_source_id")
        is_output = bool(source.get("is_materialized_output") or source.get("derived") or source.get("is_derived"))
        independent = bool(source.get("is_independent_authority", not is_output)) and not is_output
        root = str(source.get("lineage_root_id") or alias_target)
        nodes.append(
            {
                "source_id": source_id,
                "lineage_root_id": root,
                "source_parent_id": str(parent) if parent else None,
                "derivation_run_id": source.get("derivation_run_id"),
                "is_materialized_output": is_output,
                "is_independent_authority": independent,
                "witness_independence_group": str(source.get("witness_independence_group") or root),
                "alias_target": alias_target if alias_target != source_id else None,
            }
        )
    return nodes


def collapse_witness_roots(nodes: list[dict[str, Any]]) -> dict[str, str]:
    by_id = {node["source_id"]: node for node in nodes}

    def root(source_id: str, seen: set[str] | None = None) -> str:
        seen = seen or set()
        if source_id in seen or source_id not in by_id:
            return source_id
        seen.add(source_id)
        node = by_id[source_id]
        parent = node.get("source_parent_id")
        if parent:
            return root(str(parent), seen)
        alias = node.get("alias_target")
        if alias and alias in by_id:
            return root(str(alias), seen)
        return str(node.get("lineage_root_id") or source_id)

    return {source_id: root(source_id) for source_id in by_id}


def independent_witness_count(source_ids: list[str], root_map: dict[str, str]) -> int:
    return len({root_map.get(source_id, source_id) for source_id in source_ids})
