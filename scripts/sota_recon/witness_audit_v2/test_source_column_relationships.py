from source_column_relationships import discover_relationships


def _o(source, raw, canonical):
    return {"source_id": source, "raw_column": raw, "canonical_column": canonical}


def test_relationships_keep_components_distinct():
    rows = discover_relationships(
        [
            _o("a", "pass_yds", "passing_yards"),
            _o("b", "passing_yards", "passing_yards"),
            _o("a", "offensive_yards", "offensive_yards"),
        ]
    )
    assert any(r["relationship_type"] == "reconciliation_control" for r in rows)
    assert any(r["relationship_type"] == "component_of" for r in rows)
    assert not any(
        r["left_canonical_column"] == r["right_canonical_column"] and r["relationship_type"] == "component_of"
        for r in rows
    )
