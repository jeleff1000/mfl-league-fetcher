from scripts.sota_recon.nflcom_player_splits_opponents_group_l4_witness import measure


def test_opponents_group_l4_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Opponents by Group"
    assert result["source_split_values"] == 2
    assert result["source_rows_after_exact_dedup"] > 0
    assert result["detail_fields"]["fum"]["status"] == "CAPTURE_LOST"
