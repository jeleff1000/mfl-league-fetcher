from .nflcom_player_season_regular_witness import CANDIDATE_COLUMNS, witness_spec


def test_tackle_solo_uses_the_internal_combined_minus_assists_witness():
    assert witness_spec("tackles", "solo") == ("RECOMPUTE", "comb - asst")


def test_long_fields_use_max_not_sum():
    assert witness_spec("punts", "lng") == ("MAX", None)


def test_counts_use_exact_cells():
    assert witness_spec("rushing", "att") == ("EXACT", None)


def test_candidate_contrast_sets_keep_the_natural_canonical_first():
    assert CANDIDATE_COLUMNS[("passing", "1st")][0] == "passing_first_downs"
    assert CANDIDATE_COLUMNS[("receiving", "rec_fum")][0] == "receiving_fumbles"
    assert CANDIDATE_COLUMNS[("rushing", "rush_1st")][0] == "rushing_first_downs"
    assert CANDIDATE_COLUMNS[("tackles", "asst")][0] == "def_tackle_assists"


def test_candidate_contrast_sets_cover_all_nonclean_2025_fields():
    assert set(CANDIDATE_COLUMNS) == {
        ("fumbles", "fr"),
        ("passing", "1st"),
        ("punts", "punts"),
        ("receiving", "20"),
        ("receiving", "40"),
        ("receiving", "rec_1st"),
        ("receiving", "rec_fum"),
        ("rushing", "40"),
        ("rushing", "rush_1st"),
        ("rushing", "rush_fum"),
        ("tackles", "asst"),
    }
