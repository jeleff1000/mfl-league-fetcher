"""Per-component TDD tests for L1.b.1 component-precompute refactor.

Each test asserts a single component col equals atomic_stat * mult on a small
synthetic frame. Tests share a fixture `sample_df` with one row per scoring scenario.
Tests are added across Tasks A.1–A.10; this file grows as components ship.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fantasy_football_data_scripts.multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_fantasy_points,
)


@pytest.fixture
def sample_df():
    """3-row fixture covering QB / WR / TE scoring scenarios.

    Row 0: QB with 300 pass yds, 3 pass TDs, 1 INT, 1 pass 2PT, 22 cmp,
           14 pass FDs, 1 rush FD, 4 carries, 1 pick6, 2 sacks suffered,
           1 pass TD 40+, 1 cmp 40+.
    Row 1: WR with 150 rec yds, 2 rec TDs, 1 rec fum_lost, 8 receptions,
           1 rec 2PT, 30 KR yds, 7 rec FDs, 1 rec TD 40+, 1 rec TD 50+,
           1 rec 40+.
    Row 2: TE with 80 rec yds, 1 rec TD, 6 receptions, 1 ST TD, 4 rec FDs.
    """
    cols = [
        "position",
        "passing_yards",
        "passing_tds",
        "passing_interceptions",
        "rushing_yards",
        "rushing_tds",
        "rushing_fumbles_lost",
        "sack_fumbles_lost",
        "receiving_yards",
        "receiving_tds",
        "receiving_fumbles_lost",
        "receptions",
        "passing_2pt_conversions",
        "rushing_2pt_conversions",
        "receiving_2pt_conversions",
        "special_teams_tds",
        "fum_ret_td",
        "kickoff_return_yards",
        "punt_return_yards",
        "completions",
        "passing_first_downs",
        "rushing_first_downs",
        "receiving_first_downs",
        "carries",
        "pick6",
        "sacks_suffered",
        "passing_tds_40plus",
        "passing_tds_50plus",
        "rushing_tds_40plus",
        "rushing_tds_50plus",
        "receiving_tds_40plus",
        "receiving_tds_50plus",
        "completions_40plus",
        "rushing_40plus",
        "receptions_40plus",
    ]
    rows = [
        # QB
        {
            "position": "QB",
            "passing_yards": 300,
            "passing_tds": 3,
            "passing_interceptions": 1,
            "rushing_yards": 0,
            "rushing_tds": 0,
            "rushing_fumbles_lost": 0,
            "sack_fumbles_lost": 0,
            "receiving_yards": 0,
            "receiving_tds": 0,
            "receiving_fumbles_lost": 0,
            "receptions": 0,
            "passing_2pt_conversions": 1,
            "rushing_2pt_conversions": 0,
            "receiving_2pt_conversions": 0,
            "special_teams_tds": 0,
            "fum_ret_td": 0,
            "kickoff_return_yards": 0,
            "punt_return_yards": 0,
            "completions": 22,
            "passing_first_downs": 14,
            "rushing_first_downs": 1,
            "receiving_first_downs": 0,
            "carries": 4,
            "pick6": 1,
            "sacks_suffered": 2,
            "passing_tds_40plus": 1,
            "passing_tds_50plus": 0,
            "rushing_tds_40plus": 0,
            "rushing_tds_50plus": 0,
            "receiving_tds_40plus": 0,
            "receiving_tds_50plus": 0,
            "completions_40plus": 1,
            "rushing_40plus": 0,
            "receptions_40plus": 0,
        },
        # WR
        {
            "position": "WR",
            "passing_yards": 0,
            "passing_tds": 0,
            "passing_interceptions": 0,
            "rushing_yards": 0,
            "rushing_tds": 0,
            "rushing_fumbles_lost": 0,
            "sack_fumbles_lost": 0,
            "receiving_yards": 150,
            "receiving_tds": 2,
            "receiving_fumbles_lost": 1,
            "receptions": 8,
            "passing_2pt_conversions": 0,
            "rushing_2pt_conversions": 0,
            "receiving_2pt_conversions": 1,
            "special_teams_tds": 0,
            "fum_ret_td": 0,
            "kickoff_return_yards": 30,
            "punt_return_yards": 0,
            "completions": 0,
            "passing_first_downs": 0,
            "rushing_first_downs": 0,
            "receiving_first_downs": 7,
            "carries": 0,
            "pick6": 0,
            "sacks_suffered": 0,
            "passing_tds_40plus": 0,
            "passing_tds_50plus": 0,
            "rushing_tds_40plus": 0,
            "rushing_tds_50plus": 0,
            "receiving_tds_40plus": 1,
            "receiving_tds_50plus": 1,
            "completions_40plus": 0,
            "rushing_40plus": 0,
            "receptions_40plus": 1,
        },
        # TE
        {
            "position": "TE",
            "passing_yards": 0,
            "passing_tds": 0,
            "passing_interceptions": 0,
            "rushing_yards": 0,
            "rushing_tds": 0,
            "rushing_fumbles_lost": 0,
            "sack_fumbles_lost": 0,
            "receiving_yards": 80,
            "receiving_tds": 1,
            "receiving_fumbles_lost": 0,
            "receptions": 6,
            "passing_2pt_conversions": 0,
            "rushing_2pt_conversions": 0,
            "receiving_2pt_conversions": 0,
            "special_teams_tds": 1,
            "fum_ret_td": 0,
            "kickoff_return_yards": 0,
            "punt_return_yards": 0,
            "completions": 0,
            "passing_first_downs": 0,
            "rushing_first_downs": 0,
            "receiving_first_downs": 4,
            "carries": 0,
            "pick6": 0,
            "sacks_suffered": 0,
            "passing_tds_40plus": 0,
            "passing_tds_50plus": 0,
            "rushing_tds_40plus": 0,
            "rushing_tds_50plus": 0,
            "receiving_tds_40plus": 0,
            "receiving_tds_50plus": 0,
            "completions_40plus": 0,
            "rushing_40plus": 0,
            "receptions_40plus": 0,
        },
    ]
    return pd.DataFrame(rows, columns=cols)


@pytest.fixture
def calc_df(sample_df):
    return calculate_all_fantasy_points(sample_df)


# === Task A.1: passing_yards + passing_tds components ===


def test_pts_pass_yd_p04(calc_df):
    # row 0 QB: 300 * 0.04 = 12.0
    assert calc_df.loc[0, "pts_pass_yd_p04"] == pytest.approx(12.0, abs=0.001)
    # row 1 WR: 0 yards
    assert calc_df.loc[1, "pts_pass_yd_p04"] == pytest.approx(0.0, abs=0.001)


def test_pts_pass_td_4(calc_df):
    # row 0: 3 * 4 = 12
    assert calc_df.loc[0, "pts_pass_td_4"] == pytest.approx(12.0, abs=0.001)
    assert calc_df.loc[1, "pts_pass_td_4"] == pytest.approx(0.0, abs=0.001)


def test_pts_pass_td_6(calc_df):
    # row 0: 3 * 6 = 18
    assert calc_df.loc[0, "pts_pass_td_6"] == pytest.approx(18.0, abs=0.001)


# === Task A.2: passing_interceptions components ===


def test_pts_pass_int_n2(calc_df):
    # row 0 QB: 1 INT * -2 = -2.0
    assert calc_df.loc[0, "pts_pass_int_n2"] == pytest.approx(-2.0, abs=0.001)
    # row 1 WR: 0 INTs
    assert calc_df.loc[1, "pts_pass_int_n2"] == pytest.approx(0.0, abs=0.001)


def test_pts_pass_int_n1(calc_df):
    # row 0 QB: 1 INT * -1 = -1.0
    assert calc_df.loc[0, "pts_pass_int_n1"] == pytest.approx(-1.0, abs=0.001)
    assert calc_df.loc[1, "pts_pass_int_n1"] == pytest.approx(0.0, abs=0.001)


# === Task A.3: rushing components ===


def test_pts_rush_yd_p1_zero_rusher(calc_df):
    # All sample_df rows have 0 rush yds; verify zeros
    assert calc_df.loc[0, "pts_rush_yd_p1"] == pytest.approx(0.0, abs=0.001)
    assert calc_df.loc[1, "pts_rush_yd_p1"] == pytest.approx(0.0, abs=0.001)


def test_pts_rush_td_6_zero(calc_df):
    assert calc_df.loc[0, "pts_rush_td_6"] == pytest.approx(0.0, abs=0.001)


def test_pts_rush_components_with_rusher():
    # Standalone test with a real rusher row
    df = pd.DataFrame(
        [
            {
                "position": "RB",
                "rushing_yards": 120,
                "rushing_tds": 2,
                "rushing_fumbles_lost": 1,
                "sack_fumbles_lost": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_rush_yd_p1"] == pytest.approx(12.0, abs=0.001)  # 120 * 0.1
    assert out.loc[0, "pts_rush_td_6"] == pytest.approx(12.0, abs=0.001)  # 2 * 6


# === Task A.4: receiving components (incl TE-conditional bonus) ===


def test_pts_rec_yd_p1(calc_df):
    # row 1 WR: 150 * 0.1 = 15.0
    assert calc_df.loc[1, "pts_rec_yd_p1"] == pytest.approx(15.0, abs=0.001)
    # row 2 TE: 80 * 0.1 = 8.0
    assert calc_df.loc[2, "pts_rec_yd_p1"] == pytest.approx(8.0, abs=0.001)


def test_pts_rec_td_6(calc_df):
    # row 1 WR: 2 * 6 = 12.0
    assert calc_df.loc[1, "pts_rec_td_6"] == pytest.approx(12.0, abs=0.001)
    # row 2 TE: 1 * 6 = 6.0
    assert calc_df.loc[2, "pts_rec_td_6"] == pytest.approx(6.0, abs=0.001)


def test_pts_rec_1(calc_df):
    # row 1 WR: 8 * 1 = 8.0
    assert calc_df.loc[1, "pts_rec_1"] == pytest.approx(8.0, abs=0.001)
    # row 2 TE: 6 * 1 = 6.0
    assert calc_df.loc[2, "pts_rec_1"] == pytest.approx(6.0, abs=0.001)


def test_pts_rec_p5(calc_df):
    # row 1 WR: 8 * 0.5 = 4.0
    assert calc_df.loc[1, "pts_rec_p5"] == pytest.approx(4.0, abs=0.001)
    # row 2 TE: 6 * 0.5 = 3.0
    assert calc_df.loc[2, "pts_rec_p5"] == pytest.approx(3.0, abs=0.001)


def test_pts_rec_te_bonus_p5_te_only(calc_df):
    # row 0 QB: 0 receptions → 0
    assert calc_df.loc[0, "pts_rec_te_bonus_p5"] == pytest.approx(0.0, abs=0.001)
    # row 1 WR: 8 receptions but NOT TE → 0 (position-conditional)
    assert calc_df.loc[1, "pts_rec_te_bonus_p5"] == pytest.approx(0.0, abs=0.001)
    # row 2 TE: 6 receptions × 0.5 = 3.0
    assert calc_df.loc[2, "pts_rec_te_bonus_p5"] == pytest.approx(3.0, abs=0.001)


# === Task A.5: 2PT conversion components ===


def test_pts_pass_2pt_2(calc_df):
    # row 0 QB: 1 * 2 = 2
    assert calc_df.loc[0, "pts_pass_2pt_2"] == pytest.approx(2.0, abs=0.001)
    # row 1 WR: 0
    assert calc_df.loc[1, "pts_pass_2pt_2"] == pytest.approx(0.0, abs=0.001)


def test_pts_rush_2pt_2(calc_df):
    # all rows have 0 rush 2PT
    assert calc_df.loc[0, "pts_rush_2pt_2"] == pytest.approx(0.0, abs=0.001)
    assert calc_df.loc[1, "pts_rush_2pt_2"] == pytest.approx(0.0, abs=0.001)


def test_pts_rec_2pt_2(calc_df):
    # row 1 WR: 1 * 2 = 2
    assert calc_df.loc[1, "pts_rec_2pt_2"] == pytest.approx(2.0, abs=0.001)
    # row 0 QB: 0
    assert calc_df.loc[0, "pts_rec_2pt_2"] == pytest.approx(0.0, abs=0.001)


# === Task A.6: fumbles_lost components (multi-source sum of 3 atomic cols) ===


def test_pts_fum_lost_n2():
    df = pd.DataFrame(
        [
            {
                "position": "RB",
                "rushing_fumbles_lost": 1,
                "sack_fumbles_lost": 0,
                "receiving_fumbles_lost": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    # (1 + 0 + 1) * -2 = -4
    assert out.loc[0, "pts_fum_lost_n2"] == pytest.approx(-4.0, abs=0.001)


def test_pts_fum_lost_n1():
    df = pd.DataFrame(
        [
            {
                "position": "QB",
                "rushing_fumbles_lost": 0,
                "sack_fumbles_lost": 2,
                "receiving_fumbles_lost": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    # (0 + 2 + 0) * -1 = -2
    assert out.loc[0, "pts_fum_lost_n1"] == pytest.approx(-2.0, abs=0.001)


def test_pts_fum_lost_zero_rows(calc_df):
    # All sample_df rows have 0 fumbles except WR row 1 has 1 receiving_fumbles_lost
    # Row 0 QB: 0 rush_fum + 0 sack_fum + 0 rec_fum = 0
    assert calc_df.loc[0, "pts_fum_lost_n2"] == pytest.approx(0.0, abs=0.001)
    # Row 1 WR: 0 + 0 + 1 = 1; * -2 = -2
    assert calc_df.loc[1, "pts_fum_lost_n2"] == pytest.approx(-2.0, abs=0.001)
    # Row 2 TE: all zero
    assert calc_df.loc[2, "pts_fum_lost_n2"] == pytest.approx(0.0, abs=0.001)


# === Task A.7: completions + carries components ===


def test_pts_pass_cmp_p25(calc_df):
    # row 0 QB: 22 * 0.25 = 5.5
    assert calc_df.loc[0, "pts_pass_cmp_p25"] == pytest.approx(5.5, abs=0.001)


def test_pts_pass_cmp_p1(calc_df):
    # 22 * 0.1 = 2.2
    assert calc_df.loc[0, "pts_pass_cmp_p1"] == pytest.approx(2.2, abs=0.001)


def test_pts_pass_cmp_p5(calc_df):
    # 22 * 0.5 = 11.0
    assert calc_df.loc[0, "pts_pass_cmp_p5"] == pytest.approx(11.0, abs=0.001)


def test_pts_rush_att_p1(calc_df):
    # row 0 QB: 4 carries * 0.1 = 0.4
    assert calc_df.loc[0, "pts_rush_att_p1"] == pytest.approx(0.4, abs=0.001)


def test_pts_rush_att_p2(calc_df):
    # 4 * 0.2 = 0.8
    assert calc_df.loc[0, "pts_rush_att_p2"] == pytest.approx(0.8, abs=0.001)


def test_pts_rush_att_p25(calc_df):
    # 4 * 0.25 = 1.0
    assert calc_df.loc[0, "pts_rush_att_p25"] == pytest.approx(1.0, abs=0.001)


# === Task A.8: first-down components (3 atomic × 2 mults each) ===


def test_pts_pass_fd_p5(calc_df):
    # row 0 QB: 14 * 0.5 = 7.0
    assert calc_df.loc[0, "pts_pass_fd_p5"] == pytest.approx(7.0, abs=0.001)


def test_pts_pass_fd_p25(calc_df):
    # 14 * 0.25 = 3.5
    assert calc_df.loc[0, "pts_pass_fd_p25"] == pytest.approx(3.5, abs=0.001)


def test_pts_rush_fd_p5(calc_df):
    # row 0 QB: 1 * 0.5 = 0.5
    assert calc_df.loc[0, "pts_rush_fd_p5"] == pytest.approx(0.5, abs=0.001)


def test_pts_rush_fd_p25(calc_df):
    assert calc_df.loc[0, "pts_rush_fd_p25"] == pytest.approx(0.25, abs=0.001)


def test_pts_rec_fd_p5(calc_df):
    # row 1 WR: 7 * 0.5 = 3.5
    assert calc_df.loc[1, "pts_rec_fd_p5"] == pytest.approx(3.5, abs=0.001)
    # row 2 TE: 4 * 0.5 = 2.0
    assert calc_df.loc[2, "pts_rec_fd_p5"] == pytest.approx(2.0, abs=0.001)


def test_pts_rec_fd_p25(calc_df):
    # row 1 WR: 7 * 0.25 = 1.75
    assert calc_df.loc[1, "pts_rec_fd_p25"] == pytest.approx(1.75, abs=0.001)


# === Task A.9: pick6 + sack_taken + ST + fum_ret + return yards components ===


def test_pts_pick6_n2(calc_df):
    # row 0 QB: 1 * -2 = -2
    assert calc_df.loc[0, "pts_pick6_n2"] == pytest.approx(-2.0, abs=0.001)


def test_pts_pick6_n1(calc_df):
    assert calc_df.loc[0, "pts_pick6_n1"] == pytest.approx(-1.0, abs=0.001)


def test_pts_sack_taken_n1(calc_df):
    # row 0 QB: 2 sacks * -1 = -2
    assert calc_df.loc[0, "pts_sack_taken_n1"] == pytest.approx(-2.0, abs=0.001)


def test_pts_sack_taken_np5(calc_df):
    # 2 * -0.5 = -1
    assert calc_df.loc[0, "pts_sack_taken_np5"] == pytest.approx(-1.0, abs=0.001)


def test_pts_st_td_6(calc_df):
    # row 2 TE: 1 ST TD * 6 = 6
    assert calc_df.loc[2, "pts_st_td_6"] == pytest.approx(6.0, abs=0.001)
    # row 0 QB: 0 ST TDs
    assert calc_df.loc[0, "pts_st_td_6"] == pytest.approx(0.0, abs=0.001)


def test_pts_fum_ret_td_6():
    # Standalone: fum_ret_td not in fixture. Build single row.
    df = pd.DataFrame([{"position": "RB", "fum_ret_td": 1}])
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_fum_ret_td_6"] == pytest.approx(6.0, abs=0.001)


def test_pts_kr_yd_p04(calc_df):
    # row 1 WR: 30 KR yds * 0.04 = 1.2
    assert calc_df.loc[1, "pts_kr_yd_p04"] == pytest.approx(1.2, abs=0.001)


def test_pts_pr_yd_p04(calc_df):
    # All rows have 0 PR yds
    assert calc_df.loc[0, "pts_pr_yd_p04"] == pytest.approx(0.0, abs=0.001)
    assert calc_df.loc[1, "pts_pr_yd_p04"] == pytest.approx(0.0, abs=0.001)


# === Task A.10: Phase 4 bracket components (9 total — ship as components from start) ===


def test_pts_pass_td_40plus_2(calc_df):
    # row 0 QB: 1 * 2 = 2
    assert calc_df.loc[0, "pts_pass_td_40plus_2"] == pytest.approx(2.0, abs=0.001)


def test_pts_pass_td_50plus_1(calc_df):
    # row 0 QB: 0
    assert calc_df.loc[0, "pts_pass_td_50plus_1"] == pytest.approx(0.0, abs=0.001)


def test_pts_rush_td_40plus_2(calc_df):
    assert calc_df.loc[0, "pts_rush_td_40plus_2"] == pytest.approx(0.0, abs=0.001)


def test_pts_rush_td_50plus_1(calc_df):
    assert calc_df.loc[0, "pts_rush_td_50plus_1"] == pytest.approx(0.0, abs=0.001)


def test_pts_rec_td_40plus_1(calc_df):
    # row 1 WR: 1 * 1 = 1
    assert calc_df.loc[1, "pts_rec_td_40plus_1"] == pytest.approx(1.0, abs=0.001)


def test_pts_rec_td_50plus_1(calc_df):
    # row 1 WR: 1 * 1 = 1
    assert calc_df.loc[1, "pts_rec_td_50plus_1"] == pytest.approx(1.0, abs=0.001)


def test_pts_pass_cmp_40plus_1(calc_df):
    # row 0 QB: 1 * 1 = 1
    assert calc_df.loc[0, "pts_pass_cmp_40plus_1"] == pytest.approx(1.0, abs=0.001)


def test_pts_rush_40plus_1(calc_df):
    # All rows have 0 rushing_40plus
    assert calc_df.loc[0, "pts_rush_40plus_1"] == pytest.approx(0.0, abs=0.001)


def test_pts_rec_40plus_1(calc_df):
    # row 1 WR: 1 * 1 = 1
    assert calc_df.loc[1, "pts_rec_40plus_1"] == pytest.approx(1.0, abs=0.001)


def test_def_block_split_components_feed_std_points():
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "fg_blocked": 0,
                "pts_def_fg_block": 0,
                "pts_def_punt_block": 1,
                "pts_def_pat_block": 1,
            }
        ]
    )

    out = calculate_all_fantasy_points(df)

    assert out.loc[0, "pts_def_fg_block"] == pytest.approx(0.0, abs=0.001)
    assert out.loc[0, "pts_def_punt_block"] == pytest.approx(1.0, abs=0.001)
    assert out.loc[0, "pts_def_pat_block"] == pytest.approx(1.0, abs=0.001)
    assert out.loc[0, "pts_def_block"] == pytest.approx(2.0, abs=0.001)
    assert out.loc[0, "pts_def_std"] == pytest.approx(4.0, abs=0.001)
