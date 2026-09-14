from scripts.sota_recon.nflcom_player_splits_days_l6_witness import measure


def test_days_l6_witness_is_measurement_only():
    result = measure()
    assert result["measurement_only"] is True
    assert result["no_state_change"] is True
    assert result["layout"] == "player_splits_L6"
    assert result["source_rows_after_exact_dedup"] > 0
    assert {row["source_field"] for row in result["fields"]} == {"blk", "lng", "punts", "yds", "avg"}
    assert result["unmapped_fields"]["g"]["schema_matches"] == []
    assert result["unmapped_fields"]["ret"]["schema_matches"] == []
    assert result["unmapped_fields"]["rety"]["schema_matches"] == []
    assert result["unmapped_fields"]["in_20"]["schema_matches"] == []
