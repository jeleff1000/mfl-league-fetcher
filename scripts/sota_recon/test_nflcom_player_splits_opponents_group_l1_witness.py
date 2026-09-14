from scripts.sota_recon.nflcom_player_splits_opponents_group_l1_witness import measure


def test_opponents_group_l1_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Opponents by Group"
    assert result["source_split_values"] == 2
    assert result["source_rows_after_exact_dedup"] > 0
    assert result["denominator_checks"]["source"]["satisfy_lost_le_fum"] == result["denominator_checks"]["source"]["rows"]
