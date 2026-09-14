from .build_lake_relationship_graph import build


def test_graph_builds_typed_edges_from_fixture(tmp_path):
    p = tmp_path / "matrix.csv"
    p.write_text(
        "source_id,raw_column,canonical_column,raw_table_context\n"
        "a,pass_yds,passing_yards,box\n"
        "b,passing_yards,passing_yards,box\n"
        "c,rush_yds,rushing_yards,box\n",
        encoding="utf-8",
    )
    result = build(p)
    assert result["source_count"] == 3
    assert result["source_edge_count"] == 1
    assert result["formula_edge_count"] >= 27
    assert "passing_yards" not in result["single_source_canonicals"]
    assert "rushing_yards" in result["single_source_canonicals"]
