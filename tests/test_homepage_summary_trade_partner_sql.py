from multi_league.transformations.aggregation.homepage_summary import _trade_partner_sql


def test_trade_partner_sql_casts_platform_specific_manager_ids_to_text() -> None:
    select_sql, filter_sql = _trade_partner_sql({"source_manager"})

    assert "CAST(t.source_manager AS VARCHAR)" in select_sql
    assert "TRIM(CAST(t.source_manager AS VARCHAR))" in filter_sql


def test_trade_partner_sql_handles_missing_source_manager() -> None:
    select_sql, filter_sql = _trade_partner_sql(set())

    assert select_sql == "NULL as partner"
    assert filter_sql == ""
