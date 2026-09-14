from __future__ import annotations

import pandas as pd
import pytest

from multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_fantasy_points,
    calculate_composite_fantasy_points,
)


def _score(row: dict) -> pd.Series:
    base = {
        "year": 2024,
        "week": 1,
        "player_week": "TEST_2024_1",
        "NFL_player_id": "TEST",
        "position": "WR",
        "nfl_position": "WR",
    }
    base.update(row)
    out = calculate_composite_fantasy_points(
        calculate_all_fantasy_points(pd.DataFrame([base])),
    )
    return out.iloc[0]


def test_composite_qb_punter_scores_offensive_box_score_atoms():
    row = _score(
        {
            "player": "Norm Van Brocklin",
            "position": "P,QB",
            "nfl_position": "QB",
            "passing_yards": 554,
            "passing_tds": 5,
            "passing_interceptions": 2,
            "rushing_yards": -3,
            "rushing_tds": 1,
            "fumbles_lost": 0,
        }
    )

    assert row["pts_pass_4pt"] == pytest.approx(38.16)
    assert row["pts_rush"] == pytest.approx(5.7)
    assert row["fpts_4pt_half"] == pytest.approx(43.86)
    assert row["fpts_6pt_half"] == pytest.approx(53.86)
    assert row["pts_idp_std"] == 0.0


def test_dual_wr_db_keeps_offense_and_idp_in_separate_lanes():
    row = _score(
        {
            "position": "WR,DB",
            "nfl_position": "WR",
            "receptions": 8,
            "receiving_yards": 100,
            "receiving_tds": 1,
            "def_tackles_solo": 5,
            "def_sacks": 1,
            "def_interceptions": 1,
        }
    )

    assert row["fpts_4pt_half"] == pytest.approx(20.0)
    assert row["pts_idp_std"] == pytest.approx(15.0)
    assert row["fpts_4pt_half"] < row["fpts_4pt_half"] + row["pts_idp_std"]


def test_idp_standard_uses_modal_sleeper_weights_and_dedupes_defensive_tds():
    pick_six = _score(
        {
            "position": "DB",
            "nfl_position": "DB",
            "def_int_ret_td": 1,
            "def_tds": 0,
            "fum_ret_td": 0,
        }
    )
    fumble_return = _score(
        {
            "position": "DB",
            "nfl_position": "DB",
            "def_int_ret_td": 0,
            "def_tds": 1,
            "fum_ret_td": 1,
        }
    )

    assert pick_six["pts_idp_td"] == pytest.approx(1.0)
    assert pick_six["pts_idp_std"] == pytest.approx(6.0)
    assert fumble_return["pts_idp_td"] == pytest.approx(1.0)
    assert fumble_return["pts_idp_std"] == pytest.approx(6.0)


def test_wr_only_with_defensive_atoms_does_not_gain_idp_points():
    row = _score(
        {
            "position": "WR",
            "nfl_position": "WR",
            "receptions": 5,
            "receiving_yards": 80,
            "def_tackles_solo": 9,
            "def_sacks": 2,
            "def_interceptions": 1,
        }
    )

    assert row["fpts_4pt_half"] == pytest.approx(10.5)
    assert row["pts_idp_std"] == 0.0


def test_kicker_trick_play_touchdown_counts_in_offensive_composite_points():
    row = _score(
        {
            "position": "K",
            "nfl_position": "K",
            "rushing_yards": 10,
            "rushing_tds": 1,
            "pat_made": 3,
        }
    )

    assert row["pts_rush"] == pytest.approx(7.0)
    assert row["pts_k_std"] == pytest.approx(3.0)
    assert row["fpts_4pt_half"] == pytest.approx(10.0)
