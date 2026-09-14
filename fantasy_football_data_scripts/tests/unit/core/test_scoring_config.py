"""Tests for IRREDUCIBLE_SCORING_KEYS constant.

Run with:
    cd fantasy_football_data_scripts && python -m pytest tests/unit/core/test_scoring_config.py -v
"""


def test_irreducible_scoring_keys_exists_and_is_dict():
    """Verify the constant exists and is the correct type."""
    from multi_league.core.scoring_config import IRREDUCIBLE_SCORING_KEYS

    assert isinstance(IRREDUCIBLE_SCORING_KEYS, dict)


def test_irreducible_scoring_keys_entries_have_reasons():
    """Every entry must have a non-empty reason string."""
    from multi_league.core.scoring_config import IRREDUCIBLE_SCORING_KEYS

    for key, reason in IRREDUCIBLE_SCORING_KEYS.items():
        assert isinstance(reason, str), f"{key}: reason must be a string"
        assert len(reason) > 10, f"{key}: reason too short ('{reason}')"


def test_irreducible_scoring_keys_below_threshold():
    """If this fails, we are cutting corners. Investigate before raising."""
    from multi_league.core.scoring_config import IRREDUCIBLE_SCORING_KEYS

    assert len(IRREDUCIBLE_SCORING_KEYS) <= 5, (
        f"IRREDUCIBLE_SCORING_KEYS has {len(IRREDUCIBLE_SCORING_KEYS)} entries; "
        "investigate whether we're skipping computable keys."
    )


def test_yahoo_first_down_stat_ids_normalize():
    from multi_league.core.scoring_config import normalize_yahoo

    canonical = normalize_yahoo(
        [
            {"stat_id": "79", "name": "Passing 1st Downs", "points": 0.5},
            {"stat_id": "80", "name": "Receiving 1st Downs", "points": 0.5},
            {"stat_id": "81", "name": "Rushing 1st Downs", "points": 0.5},
        ]
    )

    assert canonical["pass_fd"] == 0.5
    assert canonical["rec_fd"] == 0.5
    assert canonical["rush_fd"] == 0.5


def test_yahoo_turnover_return_yards_expands_to_canonical_return_yards():
    from multi_league.core.scoring_config import normalize_yahoo

    canonical = normalize_yahoo([{"stat_id": "66", "name": "Turnover Return Yards", "points": 0.1}])

    assert canonical["int_ret_yd"] == 0.1
    assert canonical["fum_ret_yd"] == 0.1


def test_yahoo_two_point_conversions_expand_to_typed_canonical_keys():
    from multi_league.core.scoring_config import normalize_yahoo

    canonical = normalize_yahoo([{"stat_id": "16", "name": "2-Point Conversions", "points": 2}])

    assert canonical["pass_2pt"] == 2
    assert canonical["rush_2pt"] == 2
    assert canonical["rec_2pt"] == 2


def test_yahoo_modifier_bonus_rule_uses_canonical_key():
    from multi_league.core.scoring_config import normalize_yahoo

    canonical = normalize_yahoo(
        [
            {
                "stat_id": "4:bonus:300",
                "name": "Pass Yds 300+ Bonus",
                "points": 5,
                "canonical_key": "bonus_pass_yd_300",
            }
        ]
    )

    assert canonical["bonus_pass_yd_300"] == 5


def test_yahoo_modifier_bonus_thresholds_found_in_live_problem_leagues():
    from multi_league.core.yahoo_league_settings import _canonical_bonus_key

    expected = {
        ("4", 200): "bonus_pass_yd_200",
        ("4", 250): "bonus_pass_yd_250",
        ("9", 75): "bonus_rush_yd_75",
        ("9", 85): "bonus_rush_yd_85",
        ("9", 125): "bonus_rush_yd_125",
        ("12", 75): "bonus_rec_yd_75",
        ("12", 125): "bonus_rec_yd_125",
        ("12", 140): "bonus_rec_yd_140",
        ("14", 75): "bonus_st_yd_75",
        ("4", 350): "bonus_pass_yd_350",
        ("4", 375): "bonus_pass_yd_375",
        ("4", 425): "bonus_pass_yd_425",
        ("4", 450): "bonus_pass_yd_450",
        ("4", 500): "bonus_pass_yd_500",
        ("9", 150): "bonus_rush_yd_150",
        ("9", 175): "bonus_rush_yd_175",
        ("9", 297): "bonus_rush_yd_297",
        ("9", 250): "bonus_rush_yd_250",
        ("9", 300): "bonus_rush_yd_300",
        ("12", 150): "bonus_rec_yd_150",
        ("12", 175): "bonus_rec_yd_175",
        ("12", 180): "bonus_rec_yd_180",
        ("12", 250): "bonus_rec_yd_250",
        ("12", 300): "bonus_rec_yd_300",
        ("14", 100): "bonus_st_yd_100",
        ("14", 125): "bonus_st_yd_125",
        ("14", 175): "bonus_st_yd_175",
        ("14", 200): "bonus_st_yd_200",
        ("14", 225): "bonus_st_yd_225",
        ("48", 250): "bonus_def_st_yd_250",
    }

    for (stat_id, target), canonical_key in expected.items():
        assert _canonical_bonus_key(stat_id, target) == canonical_key

    assert _canonical_bonus_key("9", 123) == "bonus_rush_yd_123"
    assert _canonical_bonus_key("48", 275) == "bonus_def_st_yd_275"


def test_yahoo_modifier_bonus_map_has_flat_or_custom_scorer_coverage():
    from multi_league.core.canonical_settings import ALL_SCORING_KEYS
    from multi_league.core.scoring_config import (
        YAHOO_STAT_MODIFIER_BONUS_KEY_MAP,
        yahoo_stat_modifier_bonus_source_threshold,
        yahoo_stat_modifier_bonus_stat_threshold,
    )
    from multi_league.transformations.player.modules.scoring_calculator import (
        _BONUS_KEY_TO_COL,
        _BONUS_THRESHOLD_KEY_TO_SOURCE,
        _YAHOO_BONUS_KEY_TO_STAT_THRESHOLD,
    )

    bonus_keys = set(YAHOO_STAT_MODIFIER_BONUS_KEY_MAP.values())
    scorer_keys = set(_BONUS_KEY_TO_COL) | set(_BONUS_THRESHOLD_KEY_TO_SOURCE)
    flat_or_custom = set(ALL_SCORING_KEYS) | {
        key for key in bonus_keys if yahoo_stat_modifier_bonus_source_threshold(key) is not None
    }

    assert sorted(bonus_keys - flat_or_custom) == []
    assert sorted(bonus_keys - set(_YAHOO_BONUS_KEY_TO_STAT_THRESHOLD)) == []
    assert sorted(bonus_keys - scorer_keys) == []

    assert yahoo_stat_modifier_bonus_source_threshold("bonus_rush_yd_123") == ("rush_yd", 123)
    assert yahoo_stat_modifier_bonus_stat_threshold("bonus_rush_yd_123") == ("9", 123)
    assert yahoo_stat_modifier_bonus_source_threshold("bonus_def_st_yd_275") == ("def_st_yd", 275)
    assert yahoo_stat_modifier_bonus_stat_threshold("bonus_def_st_yd_275") == ("48", 275)


def test_yahoo_flatten_settings_preserves_new_stat_ids_in_ddl():
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.scoring_config import normalize_yahoo

    canonical = normalize_yahoo(
        [
            {"stat_id": "66", "name": "Turnover Return Yards", "points": 0.1},
            {"stat_id": "79", "name": "Passing 1st Downs", "points": 0.5},
            {"stat_id": "80", "name": "Receiving 1st Downs", "points": 0.5},
            {"stat_id": "81", "name": "Rushing 1st Downs", "points": 0.5},
            {"stat_id": "16", "name": "2-Point Conversions", "points": 2},
            {
                "stat_id": "4:bonus:300",
                "name": "Pass Yds 300+ Bonus",
                "points": 5,
                "canonical_key": "bonus_pass_yd_300",
            },
        ]
    )
    flat = flatten_settings(
        {
            "metadata": {"num_teams": 12, "scoring_type": "head"},
            "canonical_scoring": canonical,
            "roster_position_counts": {"QB": 1, "RB": 2, "WR": 2},
        },
        platform="yahoo",
        year=2025,
        league_key="461.l.12345",
    )

    assert flat["scoring_pass_fd"] == 0.5
    assert flat["scoring_rec_fd"] == 0.5
    assert flat["scoring_rush_fd"] == 0.5
    assert flat["scoring_int_ret_yd"] == 0.1
    assert flat["scoring_fum_ret_yd"] == 0.1
    assert flat["scoring_pass_2pt"] == 2
    assert flat["scoring_rush_2pt"] == 2
    assert flat["scoring_rec_2pt"] == 2
    assert flat["scoring_bonus_pass_yd_300"] == 5


def test_player_fantasy_ddl_preserves_yahoo_native_stat_substrate():
    from multi_league.core.canonical_player import PLAYER_FANTASY_COLUMNS

    assert "yahoo_official_points" in PLAYER_FANTASY_COLUMNS
    assert "yahoo_stats_available" in PLAYER_FANTASY_COLUMNS
    for stat_id in range(1, 85):
        assert f"yahoo_stat_{stat_id}" in PLAYER_FANTASY_COLUMNS
