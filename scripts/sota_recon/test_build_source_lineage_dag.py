import json
from pathlib import Path

from scripts.sota_recon.build_source_lineage_dag import build_lineage_dag, build_witness_audit_manifest


def test_build_lineage_dag_writes_outputs(tmp_path: Path):
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"sources": [{"source_id": "a"}, {"source_id": "b", "is_materialized_output": True}]}), encoding="utf-8")
    output_json = tmp_path / "dag.json"
    output_csv = tmp_path / "dag.csv"
    result = build_lineage_dag(inventory, output_json, output_csv)
    assert result["node_count"] == 2
    assert result["independent_authority_count"] == 1
    assert output_json.exists() and output_csv.exists()


def test_manifest_is_explicitly_audit_only(tmp_path: Path):
    output = tmp_path / "manifest.json"
    result = build_witness_audit_manifest(
        census_path=tmp_path / "census.json",
        lineage_path=tmp_path / "lineage.json",
        inventory_path=tmp_path / "inventory.json",
        output_path=output,
    )
    assert result["audit_only"] is True
    assert result["production_promotion_performed"] is False
