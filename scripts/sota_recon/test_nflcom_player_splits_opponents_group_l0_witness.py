from scripts.sota_recon.nflcom_player_splits_opponents_group_l0_witness import measure


def test_opponents_group_l0_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["source_table"] == "Opponents by Group"
    assert result["source_split_values"] == 2
    assert result["excluded_nested_rows"] > 0
    assert result["identity"]["complete_n"] > 0
    assert result["identity"]["total_eq_solo_plus_ast"] == result["identity"]["complete_n"]
    assert all(row["candidate_role"] == "natural" for row in result["candidate_matrix"]["total"][:1])
