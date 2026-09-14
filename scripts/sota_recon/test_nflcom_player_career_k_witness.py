from .nflcom_player_career_k_witness import close_rate, rate_from_counts


def test_rate_is_recomputed_in_percent_units():
    assert rate_from_counts(14, 27) == 100.0 * 14 / 27
    assert close_rate(51.8, rate_from_counts(14, 27))


def test_zero_attempts_are_unknown_not_zero():
    assert rate_from_counts(0, 0) is None
    assert not close_rate(None, 0.0)
