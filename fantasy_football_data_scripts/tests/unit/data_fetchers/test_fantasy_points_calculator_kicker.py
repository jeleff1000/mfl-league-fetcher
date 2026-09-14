from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_ranks,
    calculate_alltime_position_ranks,
    calculate_season_position_ranks,
)


def _kicker_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "NFL_player_id": "K1",
                "player_week": "K1_2025_1",
                "year": 2025,
                "week": 1,
                "nfl_position": "K",
                "pts_k_std": 20.0,
                "pts_k_yds": 18.0,
                "fpts_4pt_half": 0.0,
                "rolling_total_k": 18.0,
            },
            {
                "NFL_player_id": "K2",
                "player_week": "K2_2025_1",
                "year": 2025,
                "week": 1,
                "nfl_position": "K",
                "pts_k_std": 19.0,
                "pts_k_yds": 19.5,
                "fpts_4pt_half": 0.0,
                "rolling_total_k": 19.5,
            },
            {
                "NFL_player_id": "K1",
                "player_week": "K1_2025_2",
                "year": 2025,
                "week": 2,
                "nfl_position": "K",
                "pts_k_std": 8.0,
                "pts_k_yds": 7.0,
                "fpts_4pt_half": 0.0,
                "rolling_total_k": 25.0,
            },
            {
                "NFL_player_id": "K2",
                "player_week": "K2_2025_2",
                "year": 2025,
                "week": 2,
                "nfl_position": "K",
                "pts_k_std": 7.0,
                "pts_k_yds": 6.0,
                "fpts_4pt_half": 0.0,
                "rolling_total_k": 25.5,
            },
        ]
    )


def test_weekly_kicker_rank_uses_pts_k_yds():
    ranked = calculate_all_ranks(_kicker_rows())

    week1 = ranked[ranked["week"] == 1].set_index("NFL_player_id")
    assert week1.loc["K2", "rank_k"] == 1
    assert week1.loc["K1", "rank_k"] == 2


def test_season_and_alltime_kicker_ranks_use_pts_k_yds():
    df = _kicker_rows()
    season_ranked = calculate_season_position_ranks(df)
    alltime_ranked = calculate_alltime_position_ranks(df)

    season_rows = season_ranked[season_ranked["week"] == 1].set_index("NFL_player_id")
    assert season_rows.loc["K2", "rank_season_k"] == 1
    assert season_rows.loc["K1", "rank_season_k"] == 2

    alltime_rows = alltime_ranked[alltime_ranked["week"] == 1].set_index("NFL_player_id")
    assert alltime_rows.loc["K2", "rank_alltime_k"] == 1
    assert alltime_rows.loc["K1", "rank_alltime_k"] == 2


# ============================================================================
# L1.c kicker components (added 2026-05-02). Each component must equal
# source x multiplier exactly. See:
# docs/superpowers/specs/2026-05-02-pts-k-component-precompute-design.md
# ============================================================================

import pytest
from multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_fantasy_points,
)


def _l1c_kicker_fixture(**overrides) -> pd.DataFrame:
    """Build a single-row K-position fixture; overrides any column."""
    base = {
        "position": "K",
        "year": 2024,
        "week": 1,
        "fg_made_0_19": 0,
        "fg_made_20_29": 0,
        "fg_made_30_39": 0,
        "fg_made_40_49": 0,
        "fg_made_50_59": 0,
        "fg_made_60_plus_canonical": 0,
        "fg_made": 0,
        "fg_made_distance": 0,
        "fg_yards_canonical": 0,
        "fg_yards_over_30_canonical": 0,
        "pat_made": 0,
        "pat_missed": 0,
        "fg_missed": 0,
    }
    base.update(overrides)
    return pd.DataFrame([base])


@pytest.mark.parametrize(
    "source,mult,component,value",
    [
        ("fg_made_0_19", 3, "pts_k_fgm_0_19_3", 2),
        ("fg_made_20_29", 3, "pts_k_fgm_20_29_3", 3),
        ("fg_made_30_39", 3, "pts_k_fgm_30_39_3", 1),
        ("fg_made_40_49", 4, "pts_k_fgm_40_49_4", 2),
        ("fg_made_40_49", 3, "pts_k_fgm_40_49_3", 2),
        ("fg_made_50_59", 5, "pts_k_fgm_50_59_5", 1),
        ("fg_made_60_plus_canonical", 6, "pts_k_fgm_60p_6", 1),
        ("fg_made_60_plus_canonical", 5, "pts_k_fgm_60p_5", 1),
        ("pat_made", 1, "pts_k_xpm_1", 3),
        ("pat_missed", -1, "pts_k_xpmiss_n1", 2),
        ("fg_missed", -1, "pts_k_fgmiss_n1", 1),
        ("fg_yards_canonical", 0.1, "pts_k_fgm_yd_p1", 150),
        ("fg_yards_over_30_canonical", 0.1, "pts_k_fgm_yd_over30_p1", 100),
    ],
)
def test_l1c_kicker_component_atomic(source, mult, component, value):
    """Each L1.c kicker component is source x multiplier exactly."""
    df = _l1c_kicker_fixture(**{source: value})
    out = calculate_all_fantasy_points(df)
    expected = value * mult
    assert out.loc[0, component] == pytest.approx(
        expected, abs=1e-6
    ), f"{component} = {out.loc[0, component]}, expected {expected}"


def test_l1c_kicker_component_zero_when_source_zero():
    """M6 zero-source guard: every component is 0 when its source col is 0."""
    df = _l1c_kicker_fixture()  # all sources 0
    out = calculate_all_fantasy_points(df)
    components = [
        "pts_k_fgm_0_19_3",
        "pts_k_fgm_20_29_3",
        "pts_k_fgm_30_39_3",
        "pts_k_fgm_40_49_4",
        "pts_k_fgm_40_49_3",
        "pts_k_fgm_50_59_5",
        "pts_k_fgm_60p_6",
        "pts_k_fgm_60p_5",
        "pts_k_xpm_1",
        "pts_k_xpmiss_n1",
        "pts_k_fgmiss_n1",
        "pts_k_fgm_yd_p1",
        "pts_k_fgm_yd_over30_p1",
    ]
    for component in components:
        assert out.loc[0, component] == 0, f"{component} != 0 with all-zero source"


def test_l1c_kicker_component_position_guard_documented():
    """Calculator emits component values regardless of position; super_table
    recompute UPDATE (Task 18) applies position='K' filter. This test pins the contract."""
    df = _l1c_kicker_fixture(position="QB", fg_made_0_19=1)
    out = calculate_all_fantasy_points(df)
    assert out.loc[0, "pts_k_fgm_0_19_3"] == 3
