from scripts.sota_recon.nflcom_player_splits_opponents_team_l1_witness import measure


def test_opponents_team_l1_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Opponents by Team"
    assert result["source_split_values"] == 48
    assert result["source_rows_after_exact_dedup"] > 0
    checks = result["denominator_checks"]["source"]
    assert checks["satisfy_lost_le_fum"] <= checks["rows"]
    assert checks["rows"] - checks["satisfy_lost_le_fum"] == 3
