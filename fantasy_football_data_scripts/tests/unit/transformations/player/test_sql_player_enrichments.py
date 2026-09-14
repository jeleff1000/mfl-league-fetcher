def test_filter_multiplier_targets_drops_missing_and_non_numeric(caplog):
    from multi_league.transformations.player.sql_player_enrichments import _filter_multiplier_targets_to_available

    multiplier_targets = {"pts_def_sack", "pts_def_does_not_exist", "fg_label"}
    super_cols = {"pts_def_sack", "pts_def_int", "fg_label"}
    super_col_types = {
        "pts_def_sack": "DOUBLE",
        "pts_def_int": "DOUBLE",
        "fg_label": "VARCHAR",
    }

    with caplog.at_level("WARNING"):
        filtered = _filter_multiplier_targets_to_available(
            multiplier_targets,
            super_cols,
            league="test",
            year=2024,
            super_col_types=super_col_types,
        )

    assert filtered == {"pts_def_sack"}
    assert "pts_def_does_not_exist" in caplog.text
    assert "fg_label" in caplog.text


def test_filter_multiplier_targets_keeps_numeric_present_columns():
    from multi_league.transformations.player.sql_player_enrichments import _filter_multiplier_targets_to_available

    multiplier_targets = {"pts_def_sack", "pts_def_int"}
    super_cols = {"pts_def_sack", "pts_def_int", "pts_def_ff"}
    super_col_types = {
        "pts_def_sack": "DOUBLE",
        "pts_def_int": "INTEGER",
        "pts_def_ff": "DOUBLE",
    }

    filtered = _filter_multiplier_targets_to_available(
        multiplier_targets,
        super_cols,
        league="test",
        year=2024,
        super_col_types=super_col_types,
    )

    assert filtered == multiplier_targets
