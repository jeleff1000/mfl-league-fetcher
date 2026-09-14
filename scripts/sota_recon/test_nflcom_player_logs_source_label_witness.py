from scripts.sota_recon.nflcom_player_logs_source_label_witness import (
    SOURCE_VALUE_COLUMNS,
    _exact_source_match_query,
)


def test_source_label_witness_is_source_only_and_value_exact():
    query = _exact_source_match_query()
    assert "parquet_scan" in query
    assert "IS NOT DISTINCT FROM" in query
    assert "NFL_player_id" not in query
    assert "v26" not in query
    assert "source_layout_n" in query
    assert "conflicting_direct_matches" in query
    assert "game_date" in SOURCE_VALUE_COLUMNS
    assert "yds_2" in SOURCE_VALUE_COLUMNS
