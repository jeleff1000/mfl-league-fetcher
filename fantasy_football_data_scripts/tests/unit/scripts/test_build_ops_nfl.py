from scripts.build_ops_nfl_and_replace import yds_allowed_bucket_expr


def test_yds_allowed_bucket_expr_preserves_nulls_and_boundaries():
    expr = yds_allowed_bucket_expr("total_yds_allowed", "yds_allow_300_349")
    assert '"total_yds_allowed" IS NULL THEN NULL' in expr
    assert 'BETWEEN 300 AND 349' in expr
    assert "ELSE 0" in expr
