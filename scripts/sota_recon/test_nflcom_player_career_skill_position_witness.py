from .nflcom_player_career_skill_position_witness import close_value, ratio


def test_ratio_does_not_swap_rush_and_receive_operands():
    assert ratio(125, 46) == 125 / 46
    assert close_value(2.7, ratio(125, 46))


def test_zero_denominator_is_unknown():
    assert ratio(0, 0) is None
