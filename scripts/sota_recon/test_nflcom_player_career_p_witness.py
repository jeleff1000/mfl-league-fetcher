from .nflcom_player_career_p_witness import close_rate, rate_from_counts


def test_punt_average_uses_punt_denominator():
    assert rate_from_counts(510, 13) == 510 / 13
    assert close_rate(39.2, rate_from_counts(510, 13))


def test_zero_punts_are_unknown_not_zero():
    assert rate_from_counts(0, 0) is None
