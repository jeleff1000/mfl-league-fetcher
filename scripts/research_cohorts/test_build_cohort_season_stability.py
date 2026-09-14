import numpy as np

from build_cohort_season_stability import _decile_lock, _lock_probability, _threshold


def test_threshold_returns_first_supported_sample_size():
    assert _threshold([(5, .70), (10, .86), (20, .99)], .85) == 10
    assert _threshold([(5, .70)], .85) is None


def test_top_and_bottom_order_use_the_correct_tail():
    matrix = np.asarray([
        [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
        [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
        [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
    ])
    rng = np.random.default_rng(1)
    top = np.asarray([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    bottom = np.asarray([9, 8, 7, 6, 5, 4, 3, 2, 1, 0])
    assert _lock_probability(matrix, top, 2, rng, "order") == 1.0
    assert _lock_probability(matrix, bottom, 2, rng, "order", bottom=True) == 1.0


def test_decile_lock_is_one_for_identical_league_values():
    matrix = np.asarray([
        [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
        [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
        [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
    ])
    labels = np.asarray([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    assert _decile_lock(matrix, labels, 50, 2, np.random.default_rng(2)) == 1.0
