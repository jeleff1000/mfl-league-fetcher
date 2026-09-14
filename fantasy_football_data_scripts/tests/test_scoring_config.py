"""Tests for multi_league.core.scoring_config — canonical scoring normalizers.

TDD: These tests are written before the implementation. Run with:
    cd fantasy_football_data_scripts && python -m pytest tests/test_scoring_config.py -v
"""
# ---------------------------------------------------------------------------
# Yahoo normalizer tests
# ---------------------------------------------------------------------------


def test_normalize_yahoo_half_ppr():
    """the_league scoring rules: 0.5 PPR, 4pt pass TD, -1.5 INT."""
    from multi_league.core.scoring_config import normalize_yahoo

    rules = [
        {"stat_id": "4", "name": "Pass Yds", "points": 0.04},
        {"stat_id": "5", "name": "Pass TD", "points": 4.0},
        {"stat_id": "6", "name": "Int", "points": -1.5},
        {"stat_id": "9", "name": "Rush Yds", "points": 0.1},
        {"stat_id": "10", "name": "Rush TD", "points": 6.0},
        {"stat_id": "11", "name": "Rec", "points": 0.5},
        {"stat_id": "12", "name": "Rec Yds", "points": 0.1},
        {"stat_id": "13", "name": "Rec TD", "points": 6.0},
        {"stat_id": "18", "name": "Fum Lost", "points": -1.5},
    ]
    canonical = normalize_yahoo(rules)
    assert canonical["rec"] == 0.5
    assert canonical["pass_td"] == 4.0
    assert canonical["pass_int"] == -1.5
    assert canonical["fum_lost"] == -1.5
    assert canonical["rush_td"] == 6.0


def test_normalize_yahoo_idp():
    """keeper_league: IDP + DEF + kicker scoring."""
    from multi_league.core.scoring_config import normalize_yahoo

    rules = [
        {"stat_id": "38", "name": "Tack Solo", "points": 1.0},
        {"stat_id": "40", "name": "Sack", "points": 3.0},
        {"stat_id": "41", "name": "Int", "points": 2.0},
        {"stat_id": "32", "name": "Sack", "points": 0.5},
        {"stat_id": "50", "name": "Pts Allow 0", "points": 3.5},
        {"stat_id": "84", "name": "FG Yds", "points": 0.05},
    ]
    canonical = normalize_yahoo(rules)
    assert canonical["idp_tkl_solo"] == 1.0
    assert canonical["idp_sack"] == 3.0
    assert canonical["sack"] == 0.5  # DST sack
    assert canonical["pts_allow_0"] == 3.5
    assert canonical["fgm_yds"] == 0.05


def test_normalize_yahoo_skips_zero_points():
    from multi_league.core.scoring_config import normalize_yahoo

    rules = [{"stat_id": "11", "name": "Rec", "points": 0}]
    canonical = normalize_yahoo(rules)
    assert "rec" not in canonical


# ---------------------------------------------------------------------------
# ESPN normalizer tests
# ---------------------------------------------------------------------------


def test_normalize_espn_with_stat_ids():
    """ESPN league with opaque stat_NNN keys.

    Updated 2026-05-02 per ESPN_STAT_ID_MAP audit:
    - stat_206 = "2pt Return" (was wrongly mapped to fgm_50_59_alt; now def_2pt)
    - stat_140 = "Punts Inside the 10" (was wrongly mapped to idp_tkl_solo;
      now removed from map — punter scoring not in DDL)
    - stat_109 = "Total Tackles" (real IDP tackle stat; replaces the fictional 140)
    """
    from multi_league.core.scoring_config import normalize_espn

    settings = {
        "receiving_receptions": 0.5,
        "passing_td": 4.0,
        "stat_206": 4.0,  # 2pt Return (corrected)
        "stat_128": 5.0,  # YA 0-99
        "stat_109": 1.0,  # Total Tackles (real IDP, replaces fictional stat_140)
        "dst_sacks": 1.0,
    }
    canonical = normalize_espn(settings)
    assert canonical["rec"] == 0.5
    assert canonical["pass_td"] == 4.0
    assert canonical["def_2pt"] == 4.0
    assert canonical["yds_allow_0_100"] == 5.0
    assert canonical["idp_tkl"] == 1.0
    assert canonical["sack"] == 1.0


def test_normalize_espn_mixed_keys():
    """ESPN has both native keys and already-normalized keys."""
    from multi_league.core.scoring_config import normalize_espn

    settings = {
        "rushing_td": 6.0,
        "rush_td": 6.0,  # Already normalized duplicate
        "dst_points_allowed_0": 10.0,
    }
    canonical = normalize_espn(settings)
    assert canonical["rush_td"] == 6.0
    assert canonical["pts_allow_0"] == 10.0


# ---------------------------------------------------------------------------
# Sleeper normalizer tests
# ---------------------------------------------------------------------------


def test_normalize_sleeper_passthrough():
    from multi_league.core.scoring_config import normalize_sleeper

    settings = {"rec": 1.0, "pass_td": 6, "bonus_rec_te": 0.5, "fum_lost": -2}
    canonical = normalize_sleeper(settings)
    assert canonical["rec"] == 1.0
    assert canonical["pass_td"] == 6.0
    assert canonical["bonus_rec_te"] == 0.5
    assert canonical["fum_lost"] == -2.0


def test_normalize_sleeper_strips_zeros():
    from multi_league.core.scoring_config import normalize_sleeper

    settings = {"rec": 1.0, "pass_inc": 0, "fum": 0.0}
    canonical = normalize_sleeper(settings)
    assert "rec" in canonical
    assert "pass_inc" not in canonical
    assert "fum" not in canonical


# ---------------------------------------------------------------------------
# get_fpts_column tests
# ---------------------------------------------------------------------------


def test_get_fpts_column_half_ppr():
    from multi_league.core.scoring_config import get_fpts_column

    assert get_fpts_column({"rec": 0.5, "pass_td": 4.0}) == "fpts_4pt_half"


def test_get_fpts_column_full_ppr_6pt():
    from multi_league.core.scoring_config import get_fpts_column

    assert get_fpts_column({"rec": 1.0, "pass_td": 6.0}) == "fpts_6pt_ppr"


def test_get_fpts_column_standard():
    from multi_league.core.scoring_config import get_fpts_column

    assert get_fpts_column({"rec": 0.0, "pass_td": 4.0}) == "fpts_4pt_0ppr"


def test_get_fpts_column_with_return_yards():
    from multi_league.core.scoring_config import get_fpts_column

    assert get_fpts_column({"rec": 0.5, "pass_td": 4.0, "st_yd": 0.04}) == "fpts_4pt_half_ret"


def test_get_fpts_column_te_premium():
    from multi_league.core.scoring_config import get_fpts_column

    assert get_fpts_column({"rec": 1.0, "pass_td": 4.0, "bonus_rec_te": 0.5}) == "fpts_4pt_tep"


def test_get_fpts_column_nonstandard_td_rounds():
    from multi_league.core.scoring_config import get_fpts_column

    # 5.5pt pass TD should round to 6pt
    assert get_fpts_column({"rec": 0.5, "pass_td": 5.5}) == "fpts_6pt_half"


# ---------------------------------------------------------------------------
# is_standard_scoring tests
# ---------------------------------------------------------------------------


def test_is_standard_scoring_true():
    from multi_league.core.scoring_config import is_standard_scoring

    canonical = {
        "pass_yd": 0.04,
        "rush_yd": 0.1,
        "rec_yd": 0.1,
        "pass_td": 4,
        "pass_int": -1,
        "rush_td": 6,
        "rec_td": 6,
        "fum_lost": -2,
    }
    assert is_standard_scoring(canonical) is True


def test_is_standard_nonstandard_int():
    from multi_league.core.scoring_config import is_standard_scoring

    canonical = {
        "pass_yd": 0.04,
        "rush_yd": 0.1,
        "rec_yd": 0.1,
        "pass_td": 4,
        "pass_int": -1.5,
        "rush_td": 6,
        "rec_td": 6,
        "fum_lost": -2,
    }
    assert is_standard_scoring(canonical) is False


# ---------------------------------------------------------------------------
# normalize_scoring_settings dispatch tests
# ---------------------------------------------------------------------------


def test_normalize_scoring_settings_dispatch():
    from multi_league.core.scoring_config import normalize_scoring_settings

    yahoo_settings = {"scoring_rules": [{"stat_id": "11", "name": "Rec", "points": 0.5}]}
    result = normalize_scoring_settings("yahoo", yahoo_settings)
    assert result["rec"] == 0.5

    sleeper_settings = {"scoring_settings": {"rec": 1.0, "pass_td": 6}}
    result = normalize_scoring_settings("sleeper", sleeper_settings)
    assert result["rec"] == 1.0
