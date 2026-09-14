from .nflcom_player_career_qb_witness import close_value, ratio


def test_ratio_uses_operand_denominator():
    assert ratio(708, 115) == 708 / 115
    assert close_value(6.2, ratio(708, 115))


def test_zero_denominator_is_unknown():
    assert ratio(0, 0) is None
