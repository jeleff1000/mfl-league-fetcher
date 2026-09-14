"""L1.d IDP component tests (added 2026-05-03).

Tests the 4 new IDP cols added by L1.d Phase A:
- pts_idp_blk_kick (bug-fix)
- pts_idp_int_ret_yd (bug-fix)
- pts_idp_fum_rec_yd (bug-fix)
- pts_idp_tkl_combined (new derived helper)

Plus position-guard extension covering all 4 new cols.

See: docs/superpowers/specs/2026-05-03-pts-idp-component-precompute-design.md
"""

from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_fantasy_points,
)


def _idp_fixture(**overrides) -> pd.DataFrame:
    """Build a single-row IDP-position fixture; overrides any column.

    Default position is 'LB' (an IDP position). Use position='DEF' for
    DST-row tests; position='QB' for offensive-row tests.
    """
    base = {
        "position": "LB",
        "nfl_position": "LB",
        "year": 2024,
        "week": 1,
        "NFL_player_id": "TEST-LB-1",
        "player_week": "TEST-LB-1_2024_1",
        # IDP source cols (all default 0)
        "def_tackles_solo": 0.0,
        "def_tackle_assists": 0.0,
        "def_tackles_with_assist": 0.0,
        "def_sacks": 0.0,
        "def_interceptions": 0.0,
        "def_interception_yards": 0.0,
        "def_fumbles_forced": 0.0,
        "def_fumbles": 0.0,
        "fum_rec": 0.0,
        "fum_ret_td": 0.0,
        "fum_rec_yds": 0.0,
        "def_pass_defended": 0.0,
        "def_qb_hits": 0.0,
        "def_tackles_for_loss": 0.0,
        "def_safeties": 0.0,
        "def_tds": 0.0,
        "def_blk_kick": 0.0,
        "def_blk_kick_td": 0.0,
        # DEF/DST source cols (kept 0 for IDP fixture)
        "fg_blocked": 0.0,
    }
    base.update(overrides)
    return pd.DataFrame([base])


def test_pts_idp_blk_kick_equals_def_blk_kick():
    """pts_idp_blk_kick = def_blk_kick exactly (raw count, × 1)."""
    df = _idp_fixture(def_blk_kick=2.0)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_blk_kick"] == 2.0


def test_pts_idp_int_ret_yd_equals_def_interception_yards():
    """pts_idp_int_ret_yd = def_interception_yards exactly."""
    df = _idp_fixture(def_interception_yards=37.0)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_int_ret_yd"] == 37.0


def test_pts_idp_fum_rec_yd_equals_fum_rec_yds():
    """pts_idp_fum_rec_yd = fum_rec_yds exactly."""
    df = _idp_fixture(fum_rec_yds=22.0)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_fum_rec_yd"] == 22.0


def test_pts_idp_fr_uses_fum_rec_not_def_fumbles():
    """IDP fumble recoveries come from the populated `fum_rec` source column."""
    df = _idp_fixture(def_fumbles=0.0, fum_rec=2.0)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_fr"] == 2.0


def test_pts_idp_td_includes_fumble_return_tds():
    """IDP defensive TD scoring must include both INT-return and fumble-return TDs."""
    df = _idp_fixture(def_tds=1.0, fum_ret_td=1.0)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_td"] == 2.0
    assert out.loc[0, "pts_idp_fum_ret_td"] == 1.0


def test_pts_idp_pass_def_3p_caps_passes_defended_at_three():
    df = _idp_fixture(def_pass_defended=5.0)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_pass_def_3p"] == 3.0


def test_pts_idp_tkl_combined_uses_solo_plus_assist_when_splits_exist():
    """When split tackle atoms exist, combined tackles are solo + assist."""
    df = _idp_fixture(
        def_tackles_solo=5.0,
        def_tackle_assists=3.0,
        def_tackles_with_assist=2.0,
    )
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_tkl_combined"] == 8.0
    assert out.loc[0, "pts_idp_std"] == 6.5


def test_pts_idp_tkl_combined_falls_back_to_reported_combined_without_splits():
    """Rows with no split tackle atoms can still carry combined-tackle scoring."""
    df = _idp_fixture(
        def_tackles_solo=0.0,
        def_tackle_assists=0.0,
        def_tackles_with_assist=8.0,
    )
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_tkl_combined"] == 8.0
    assert out.loc[0, "pts_idp_std"] == 8.0


def test_position_guard_zeros_new_idp_cols_on_def_row():
    """DEF rows must have the 4 new IDP cols zeroed (team-aggregate stats
    would inflate IDP scoring otherwise)."""
    df = _idp_fixture(
        position="DEF",
        nfl_position="DEF",
        # Team-aggregate values that should be zeroed for IDP cols
        def_blk_kick=2.0,
        def_interception_yards=45.0,
        fum_rec_yds=15.0,
        def_tackles_with_assist=50.0,
        fum_ret_td=1.0,
        def_pass_defended=5.0,
    )
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_idp_blk_kick"] == 0.0
    assert out.loc[0, "pts_idp_int_ret_yd"] == 0.0
    assert out.loc[0, "pts_idp_fum_rec_yd"] == 0.0
    assert out.loc[0, "pts_idp_tkl_combined"] == 0.0
    assert out.loc[0, "pts_idp_fum_ret_td"] == 0.0
    assert out.loc[0, "pts_idp_pass_def_3p"] == 0.0
