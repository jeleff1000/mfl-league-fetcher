"""Materialize the source lineage DAG used for independent witness counts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from .source_lineage import build_lineage_nodes, collapse_witness_roots


def _load_inventory(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_lineage_dag(inventory_path: Path, output_json: Path, output_csv: Path) -> dict[str, Any]:
    inventory = _load_inventory(inventory_path)
    nodes = build_lineage_nodes(inventory)
    root_map = collapse_witness_roots(nodes)
    for node in nodes:
        node["collapsed_lineage_root_id"] = root_map[node["source_id"]]
    # O.5: for registered sources, root identity comes from the committed
    # lineage_roots.v1.json contract (family x era scoped) -- the alias/parent walk
    # above survives only for inventory entries outside the registry.
    contract_version = None
    try:
        from .lineage_roots import load as _load_roots
        roots_doc = _load_roots()
        contract_version = "lineage_roots." + roots_doc["version"]
        by_sid = {e["source_id"]: e["assignments"] for e in roots_doc["sources"]}
        for node in nodes:
            asg = by_sid.get(node["source_id"])
            if not asg:
                continue
            node["root_assignments"] = asg
            rset = sorted({a["root"] for a in asg})
            rid = rset[0] if len(rset) == 1 else "era_split:" + "+".join(rset)
            node["lineage_root_id"] = rid
            node["collapsed_lineage_root_id"] = rid
            node["witness_independence_group"] = rid
            root_map[node["source_id"]] = rid
    except FileNotFoundError:
        pass
    result = {
        "schema_version": 2,
        "lineage_roots_contract": contract_version,
        "inventory_path": str(inventory_path),
        "node_count": len(nodes),
        "independent_authority_count": sum(1 for node in nodes if node["is_independent_authority"]),
        "lineage_root_count": len(set(root_map.values())),
        "nodes": nodes,
        "edges": [
            {"source_id": node["source_id"], "target_id": node["source_parent_id"], "edge_type": "derived_from"}
            for node in nodes
            if node["source_parent_id"]
        ],
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, sort_keys=True, default=str), encoding="utf-8")
    fieldnames = sorted({key for node in nodes for key in node})
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(node for node in nodes)
    return result


def build_witness_audit_manifest(
    *,
    census_path: Path,
    lineage_path: Path,
    inventory_path: Path,
    closure_ledger_path: Path | None = None,
    output_path: Path,
) -> dict[str, Any]:
    manifest = {
        "schema_version": 1,
        "audit_only": True,
        "census_path": str(census_path),
        "lineage_path": str(lineage_path),
        "inventory_path": str(inventory_path),
        "closure_ledger_path": str(closure_ledger_path) if closure_ledger_path else None,
        "inputs_present": {
            "census": census_path.exists(),
            "lineage": lineage_path.exists(),
            "inventory": inventory_path.exists(),
            "closure_ledger": bool(closure_ledger_path and closure_ledger_path.exists()),
        },
        "production_promotion_performed": False,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--output-json", type=Path, default=Path("docs/source-lineage-dag.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("docs/source-lineage-dag.csv"))
    args = parser.parse_args()
    print(json.dumps(build_lineage_dag(args.inventory, args.output_json, args.output_csv), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
