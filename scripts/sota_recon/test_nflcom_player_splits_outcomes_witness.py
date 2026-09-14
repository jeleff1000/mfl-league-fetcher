from scripts.sota_recon.nflcom_player_splits_outcomes_witness import measure


def test_outcomes_witness_is_measurement_only_and_partitioned():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Outcomes"
    assert result["partition"] == "Wins + Losses + Ties"
    assert result["source_split_values"] == 3
    assert set(result["layouts"]) == {"L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7"}
    assert result["source_rows_after_exact_dedup"] > 0
