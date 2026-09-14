from scripts.sota_recon.nflcom_player_splits_stadiums_witness import measure_stadiums


def test_stadiums_witness_is_measurement_only():
    result = measure_stadiums()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Stadiums"
    assert result["partition"] == "all nonblank stadium labels; no partition claim"
    assert result["source_split_values"] > 2
    assert set(result["layouts"]) == {"L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7"}
