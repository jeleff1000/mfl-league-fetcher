from scripts.sota_recon.nflcom_player_splits_opponents_group_l5_witness import measure


def test_opponents_group_l5_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Opponents by Group"
    assert result["source_split_values"] == 2
    assert len(result["rates"]) == 3
    assert result["excluded_rate"]["populated_n"] == 0
