from scripts.sota_recon.nflcom_player_splits_opponents_team_l5_witness import measure


def test_opponents_team_l5_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Opponents by Team"
    assert result["source_split_values"] == 48
    assert len(result["rates"]) == 3
    assert result["excluded_rate"]["populated_n"] == 0
