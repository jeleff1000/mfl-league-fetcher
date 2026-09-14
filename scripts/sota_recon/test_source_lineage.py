from scripts.sota_recon.source_lineage import (
    build_lineage_nodes,
    collapse_witness_roots,
    independent_witness_count,
)


def test_alias_and_derived_output_share_one_root():
    inventory = {
        "sources": [
            {"source_id": "pbp_merged"},
            {"source_id": "pbp_merged_1978_2025"},
            {"source_id": "v26_release", "source_parent_id": "pbp_merged_1978_2025", "is_materialized_output": True},
            {"source_id": "official_box"},
        ]
    }
    nodes = build_lineage_nodes(inventory)
    roots = collapse_witness_roots(nodes)
    assert roots["pbp_merged"] == roots["pbp_merged_1978_2025"] == roots["v26_release"]
    assert independent_witness_count(["pbp_merged", "v26_release", "official_box"], roots) == 2


def test_unrelated_authorities_remain_independent():
    nodes = build_lineage_nodes({"sources": [{"source_id": "a"}, {"source_id": "b"}]})
    roots = collapse_witness_roots(nodes)
    assert independent_witness_count(["a", "b"], roots) == 2
