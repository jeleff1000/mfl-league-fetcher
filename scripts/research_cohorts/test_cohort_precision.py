from cohort_precision import decile_band, design_effect, independent_observations, season_leagues


def test_decile_band_is_rate_local_and_handles_boundaries():
    assert decile_band(10) == (0.05, 0.15)
    assert decile_band(50) == (0.45, 0.55)
    assert decile_band(100) == (0.95, 1.0)


def test_85_percent_five_point_deciles():
    assert [independent_observations(p / 100, 0.05) for p in range(10, 100, 10)] == [
        75, 133, 175, 199, 208, 199, 175, 133, 75
    ]


def test_other_error_bands_at_the_midpoint():
    assert independent_observations(0.50, 0.01) == 5181
    assert independent_observations(0.50, 0.03) == 576
    assert independent_observations(0.50, 0.05) == 208


def test_boundary_targets_use_exact_confidence_bound():
    assert independent_observations(1.0, 0.05) == 51
    assert independent_observations(0.0, 0.05) == 51


def test_cluster_conversion():
    assert design_effect(17, 0.0) == 1.0
    assert design_effect(17, 0.25) == 5.0
    assert season_leagues(208, 17, 0.25) == 62
    assert season_leagues(208, 17, 1.0) == 208
