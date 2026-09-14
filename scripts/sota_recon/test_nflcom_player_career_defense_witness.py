from .nflcom_player_career_defense_witness import identity_counts


def test_identity_prefers_tkl_plus_assist():
    rows = [(5, 2, 1, 7), (3, 1, 1, 4), (0, 0, 0, 0)]
    result = identity_counts(rows)
    assert result == {
        "complete_n": 3,
        "combined_eq_tkl_plus_ast": 3,
        "combined_eq_solo_plus_ast": 1,
    }


def test_identity_does_not_invent_missing_rows():
    assert identity_counts([])["complete_n"] == 0
