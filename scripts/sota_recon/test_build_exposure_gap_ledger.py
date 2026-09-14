import json

from .build_exposure_gap_ledger import build


def test_exposure_gap_classifies_source_candidates(tmp_path):
    matrix = tmp_path / "matrix.csv"
    matrix.write_text(
        "canonical_column,raw_column,source_id,raw_table_context,coverage_union_years,"
        "stat_domain,stat_role,mapping_confidence,canonical_semantic_id,transformation_class\n"
        "air_yards,air_yards,pbp,pbp,[2006, 2007],passing,atom,high,passing:air_yards,direct\n"
        "air_yards,air_yds,pfr,pfr,[2006, 2007],passing,atom,high,passing:air_yards,renamed\n"
        "rare_stat,rare_stat,one,box,[1980],general,atom,high,general:rare_stat,direct\n",
        encoding="utf-8",
    )
    census = tmp_path / "census.json"
    census.write_text(
        json.dumps({"columns": [{"tableId": "weekly", "column": "air_yards"}]}),
        encoding="utf-8",
    )
    graph = tmp_path / "graph.json"
    graph.write_text(
        json.dumps({"formula_edges": [{
            "relationship_id": "air",
            "edge_type": "source_definition",
            "lhs": "rare_stat",
            "component_columns": [],
        }]}),
        encoding="utf-8",
    )
    result = build(matrix, census, graph)
    rows = {r["canonical_column"]: r for r in result["rows"] if r["kind"] == "lake_potential"}
    assert rows["air_yards"]["status"] == "exposed"
    assert rows["rare_stat"]["exposure_class"] == "derivable_or_witnessed_not_exposed"
    assert result["not_exposed_by_class"]["derivable_or_witnessed_not_exposed"] == 1
