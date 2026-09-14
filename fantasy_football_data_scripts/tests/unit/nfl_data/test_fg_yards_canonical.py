"""L1.c Phase 0.3: tests for fg_yards_canonical + fg_yards_over_30_canonical.

Per design doc 2026-05-02-fg-yards-canonical-design.md, these cols use a 3-stage
fallback. Tests cover each stage and edge cases.
"""

from __future__ import annotations

import pandas as pd

from fantasy_football_data_scripts.nfl_data.build_nfl_super_table import (
    populate_fg_yards_canonical,
    populate_fg_yards_over_30_canonical,
)


# -----------------------------------------------------------------------------
# fg_yards_canonical
# -----------------------------------------------------------------------------


def test_fg_yards_stage1_uses_fg_made_distance():
    """Stage 1: fg_made_distance > 0 -> use it directly."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [120],
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [3],
        }
    )
    out = populate_fg_yards_canonical(df)
    assert list(out["fg_yards_canonical"]) == [120]


def test_fg_yards_stage2_bucket_midpoints():
    """Stage 2: fg_made_distance = 0, buckets > 0 -> bucket-midpoint sum."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_made_0_19": [1],
            "fg_made_20_29": [1],
            "fg_made_30_39": [1],
            "fg_made_40_49": [1],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [4],
        }
    )
    out = populate_fg_yards_canonical(df)
    # 17 + 25 + 35 + 45 = 122
    assert list(out["fg_yards_canonical"]) == [122]


def test_fg_yards_stage3_pre_bucket_era():
    """Stage 3: all distance + buckets = 0, fg_made > 0 -> fg_made * 35."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [3],
        }
    )
    out = populate_fg_yards_canonical(df)
    assert list(out["fg_yards_canonical"]) == [105]  # 3 * 35


def test_fg_yards_no_fgs_made():
    """All zeros -> 0."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [0],
        }
    )
    out = populate_fg_yards_canonical(df)
    assert list(out["fg_yards_canonical"]) == [0]


def test_fg_yards_handles_missing_cols():
    """Missing optional cols (e.g., fg_made_distance) default to 0 via .get()."""
    df = pd.DataFrame(
        {
            "fg_made": [2],
        }
    )
    out = populate_fg_yards_canonical(df)
    # Stage 3 fires: fg_made * 35 = 70
    assert list(out["fg_yards_canonical"]) == [70]


def test_fg_yards_tucker_2013_wk13():
    """Justin Tucker 2013 wk13 BAL@PIT: 6 FGs at 61, 50, 22, 29, 32, 24.
    fg_made_distance should be 218 (sum of distances). Stage 1 returns 218.
    """
    df = pd.DataFrame(
        {
            "fg_made_distance": [218],
            "fg_made_0_19": [0],
            "fg_made_20_29": [3],
            "fg_made_30_39": [1],
            "fg_made_40_49": [0],
            "fg_made_50_59": [1],
            "fg_made_60_plus_canonical": [1],
            "fg_made": [6],
        }
    )
    out = populate_fg_yards_canonical(df)
    assert list(out["fg_yards_canonical"]) == [218]


# -----------------------------------------------------------------------------
# fg_yards_over_30_canonical
# -----------------------------------------------------------------------------


def test_fg_yards_over_30_stage1_subtracts_sub30_estimate():
    """Stage 1: fg_made_distance > 0, sub-30 buckets present -> subtract estimate."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [218],
            "fg_yards_canonical": [218],
            "fg_made_0_19": [0],
            "fg_made_20_29": [3],
            "fg_made_30_39": [1],
            "fg_made_40_49": [0],
            "fg_made_50_59": [1],
            "fg_made_60_plus_canonical": [1],
            "fg_made": [6],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    # Tucker 2013 wk13: 218 - (17*0 + 25*3) = 218 - 75 = 143
    assert list(out["fg_yards_over_30_canonical"]) == [143]


def test_fg_yards_over_30_stage1_floor_at_zero():
    """Stage 1 floor: pre-modern data with sub-30 bucket midpoints can over-estimate
    the actual sub-30 yardage. Result must clamp to 0 (not negative).

    Real example: 1990 wk17 NFL_player_id 00-0002422 — fg_made=1, distance=24,
    buckets=[1, 2, 0, 0, 0, 0]. sub_30_estimate = 17*1 + 25*2 = 67. Raw:
    24 - 67 = -43. Expected: clamped to 0.
    """
    df = pd.DataFrame(
        {
            "fg_made_distance": [24],
            "fg_yards_canonical": [24],
            "fg_made_0_19": [1],
            "fg_made_20_29": [2],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [1],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    assert list(out["fg_yards_over_30_canonical"]) == [0]


def test_fg_yards_over_30_stage1_simplified_no_sub30():
    """Stage 1 simplified: fg_made_distance > 0, no sub-30 FGs -> entire distance is over 30."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [48],
            "fg_yards_canonical": [48],
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [1],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [1],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    # Vinatieri SB36: 48 yd FG, no sub-30 FGs -> 48
    assert list(out["fg_yards_over_30_canonical"]) == [48]


def test_fg_yards_over_30_stage2_bucket_midpoints():
    """Stage 2: fg_made_distance = 0, buckets populated -> sum 30+ buckets at midpoints."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_yards_canonical": [122],  # Stage 2 of fg_yards (1+1+1+1 buckets)
            "fg_made_0_19": [1],
            "fg_made_20_29": [1],
            "fg_made_30_39": [1],
            "fg_made_40_49": [1],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [4],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    # 30+ buckets only: 35 + 45 = 80
    assert list(out["fg_yards_over_30_canonical"]) == [80]


def test_fg_yards_over_30_stage3_pre_bucket_era():
    """Stage 3: pre-bucket era -> MAX(fg_yards_canonical - fg_made * 30, 0)."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_yards_canonical": [105],  # Stage 3 of fg_yards (3 * 35)
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [3],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    # MAX(105 - 3*30, 0) = MAX(15, 0) = 15
    assert list(out["fg_yards_over_30_canonical"]) == [15]


def test_fg_yards_over_30_stage3_floor_at_zero():
    """Stage 3 floor: if fg_yards < fg_made * 30, result is 0 (not negative)."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_yards_canonical": [50],  # implausibly low for 3 FGs but tests the floor
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [3],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    # MAX(50 - 90, 0) = 0
    assert list(out["fg_yards_over_30_canonical"]) == [0]


def test_fg_yards_over_30_no_fgs():
    """All zeros -> 0."""
    df = pd.DataFrame(
        {
            "fg_made_distance": [0],
            "fg_yards_canonical": [0],
            "fg_made_0_19": [0],
            "fg_made_20_29": [0],
            "fg_made_30_39": [0],
            "fg_made_40_49": [0],
            "fg_made_50_59": [0],
            "fg_made_60_plus_canonical": [0],
            "fg_made": [0],
        }
    )
    out = populate_fg_yards_over_30_canonical(df)
    assert list(out["fg_yards_over_30_canonical"]) == [0]
