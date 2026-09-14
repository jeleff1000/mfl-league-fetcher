import pytest


def test_pre_flight_assertion_raises_on_unknown_key():
    """A populated scoring_* key not in any *_COL_MAP must raise."""
    from multi_league.transformations.common.sql_base import _assert_known_scoring_keys

    bad_settings = {
        "rec": 0.5,  # known (offense)
        "totally_made_up_key": 1.0,  # unknown — must raise
    }
    with pytest.raises(ValueError, match="totally_made_up_key"):
        _assert_known_scoring_keys(bad_settings, league="test_league", year=2024)


def test_pre_flight_assertion_passes_when_all_known():
    from multi_league.transformations.common.sql_base import _assert_known_scoring_keys

    ok_settings = {"rec": 0.5, "pass_td": 4, "sack": 1.0, "int": 2.0}
    # Should not raise
    _assert_known_scoring_keys(ok_settings, league="test_league", year=2024)


def test_pre_flight_assertion_ignores_zero_keys():
    """Zero-value keys are acceptable even if unknown — they're no-ops."""
    from multi_league.transformations.common.sql_base import _assert_known_scoring_keys

    settings_with_zero_unknown = {"rec": 0.5, "deprecated_key": 0}
    # Should not raise
    _assert_known_scoring_keys(settings_with_zero_unknown, league="test_league", year=2024)


def test_mode_detect_zeros_def_td_when_splits_present():
    """When any split TD key is populated, unified def_td is zeroed to prevent double-count."""
    from multi_league.transformations.common.sql_base import _build_def_multipliers

    # Both unified and split set with different values
    settings = {"def_td": 6.0, "def_int_ret_td": 8.0, "def_fum_ret_td": 6.0}
    mults = _build_def_multipliers(settings)

    # Unified is zeroed
    assert mults.get("pts_def_td", 0) == 0
    # Splits use their own values
    assert mults["pts_def_int_ret_td"] == 8.0
    assert mults["pts_def_fum_ret_td"] == 6.0


def test_mode_detect_uses_unified_when_no_splits():
    from multi_league.transformations.common.sql_base import _build_def_multipliers

    settings = {"def_td": 6.0}  # only unified
    mults = _build_def_multipliers(settings)
    assert mults["pts_def_td"] == 6.0


def test_mode_detect_does_not_zero_def_td_for_st_td():
    """def_st_td is a separate ST category — must NOT trigger mode-detect on def_td."""
    from multi_league.transformations.common.sql_base import _build_def_multipliers

    settings = {"def_td": 6.0, "def_st_td": 8.0}
    mults = _build_def_multipliers(settings)
    # Both categories coexist — neither should zero the other
    assert mults["pts_def_td"] == 6.0
    assert mults["pts_def_st_td"] == 8.0


def test_build_def_multipliers_excludes_idp_bonus_keys():
    """bonus_sack_2p, bonus_tkl_10p etc. are IDP bonuses and must NOT be included in def_multipliers.

    These are per-defender bonuses (individual player with 10+ tackles, 2+ sacks) that
    Sleeper applies to IDP scoring, not team DST. Our pts_def_bonus_* super_table columns
    are team-level flags that produce wrong values when applied to DST (e.g., team tackles
    >=10 every game). Phase 1 skips them; follow-up PR can add IDP-level wiring.
    """
    from multi_league.transformations.common.sql_base import _build_def_multipliers

    settings = {"sack": 1.0, "bonus_sack_2p": 2.0, "bonus_tkl_10p": 5.0}
    mults = _build_def_multipliers(settings)

    assert mults.get("pts_def_sack") == 1.0
    # IDP bonus keys must NOT appear in def_multipliers
    assert "pts_def_bonus_sack_2p" not in mults
    assert "pts_def_bonus_tkl_10p" not in mults


def test_build_def_multipliers_treats_yds_allow_neg_as_legacy_low_bucket_alias():
    from multi_league.transformations.common.sql_base import _build_def_multipliers

    mults = _build_def_multipliers({"yds_allow_neg": 10.0})

    assert mults["yds_allow_0_99"] == 10.0
    assert "pts_def_ya" not in mults


def test_build_def_multipliers_prefers_canonical_low_yards_allowed_bucket():
    from multi_league.transformations.common.sql_base import _build_def_multipliers

    mults = _build_def_multipliers({"yds_allow_neg": 10.0, "yds_allow_0_100": 6.0})

    assert mults["yds_allow_0_99"] == 6.0
