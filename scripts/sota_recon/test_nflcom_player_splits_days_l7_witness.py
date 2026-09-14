from scripts.sota_recon.nflcom_player_splits_days_l7_witness import measure


def test_days_l7_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["layout"] == "player_splits_L7"
    assert result["source_rows_after_exact_dedup"] > 0
    assert {row["source_field"] for row in result["fields"]} == {"fg_att", "fgm"}
    assert result["unmapped_fields"]["g"]["schema_matches"] == []
    assert result["excluded_pct"]["populated_n"] == 0
