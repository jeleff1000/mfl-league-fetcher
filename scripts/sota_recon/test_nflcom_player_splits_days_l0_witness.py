from .nflcom_player_splits_days_l0_witness import compare_identity


def test_days_tackle_identity_is_explicit():
    assert compare_identity([(7, 5, 2), (4, 3, 1), (4, 4, 0)]) == {
        "complete_n": 3,
        "total_eq_solo_plus_ast": 3,
    }
