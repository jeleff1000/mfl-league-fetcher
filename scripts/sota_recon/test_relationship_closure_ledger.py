import json

from .build_relationship_closure_ledger import build


def test_closure_ledger_distinguishes_typed_and_source_only(tmp_path):
    p = tmp_path / "matrix.csv"
    p.write_text(
        "canonical_column,raw_column,source_id,raw_table_context,coverage_union_years,stat_domain,stat_role\n"
        "passing_yards,pass_yds,a,box,[2000],passing,atom\n"
        "passing_yards,passing_yards,b,box,[2000],passing,atom\n"
        "targets,targets,c,box,[2000],receiving,atom\n",
        encoding="utf-8",
    )
    g = tmp_path / "graph.json"
    g.write_text(
        json.dumps({"formula_edges": [{
            "relationship_id": "receptions_le_targets",
            "edge_type": "bound",
            "lhs": "targets",
            "lane": "recon_bounds",
            "formula": "receptions <= targets",
        }]}),
        encoding="utf-8",
    )
    result = build(p, g)
    rows = {r["canonical_column"]: r for r in result["rows"]}
    assert rows["passing_yards"]["status"] == "source_witness_only"
    assert rows["targets"]["status"] == "typed_relationship_present"
    assert rows["targets"]["closure_class"] == "typed_relationship_covered"
