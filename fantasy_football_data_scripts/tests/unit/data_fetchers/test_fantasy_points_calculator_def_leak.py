"""Task 0.2 — non-DEF leak guard for pts_def_* columns.

L1.a recompute SQL nulled pts_def_* on non-DEF rows; calculator continued
to write nonzero values for QB/RB/WR rows where pts_allow_* tier brackets
fired. Without this guard, weekly update_nfl_super_table.py runs
re-introduce the leak (256k+ rows in L1.a's pre-fix state).

See:
- docs/superpowers/findings/2026-04-30-pts-def-audit-post-recompute.md (M6 leak)
- docs/superpowers/specs/2026-05-01-pts-off-audit-design.md Phase 0 Task 0.2
"""

from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.fantasy_points_calculator import calculate_all_fantasy_points


PTS_DEF_COLS_GUARDED = [
    "pts_def_sack",
    "pts_def_int",
    "pts_def_ff",
    "pts_def_fr",
    "pts_def_td",
    "pts_def_safety",
    "pts_def_block",
    "pts_def_tfl",
    "pts_def_3out",
    "pts_def_4stop",
    "pts_def_ret_yd",
    "pts_def_ret_td",
    "pts_def_std",
    "pts_def_ya",
]


def test_pts_def_cols_null_on_qb_row():
    """A QB row should have NULL pts_def_* values regardless of source-stat presence.

    Pre-fix: pts_def_std on this QB row would be ~10 (pts_allow_0 * 10 fires).
    Post-fix: NULL.
    """
    df = pd.DataFrame(
        [
            {
                "position": "QB",
                "year": 2020,
                "week": 1,
                "nfl_team": "TB",
                "passing_yards": 250,
                "passing_tds": 2,
                "passing_interceptions": 0,
                "rushing_yards": 0,
                "rushing_tds": 0,
                "receiving_yards": 0,
                "receiving_tds": 0,
                "receptions": 0,
                # Junk values that would produce nonzero pts_def_std without leak guard
                "pts_allow_0": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    for col in PTS_DEF_COLS_GUARDED:
        if col in out.columns:
            val = out[col].iloc[0]
            assert pd.isna(val), f"{col} on QB row should be NULL (pd.NA), got {val}"


def test_pts_def_cols_null_on_wr_row():
    """A WR row should have NULL pts_def_* values."""
    df = pd.DataFrame(
        [
            {
                "position": "WR",
                "year": 2020,
                "week": 1,
                "nfl_team": "DET",
                "receptions": 8,
                "receiving_yards": 120,
                "receiving_tds": 1,
                "passing_yards": 0,
                "rushing_yards": 0,
                "def_sacks": 0,
                "def_interceptions": 0,
                "pts_allow_7_13": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    for col in PTS_DEF_COLS_GUARDED:
        if col in out.columns:
            val = out[col].iloc[0]
            assert pd.isna(val), f"{col} on WR row should be NULL, got {val}"


def test_pts_def_cols_populated_on_def_row():
    """A DEF row should have pts_def_* populated as before — leak guard must NOT zero DEF."""
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2020,
                "week": 1,
                "nfl_team": "PIT",
                "def_sacks": 4,
                "def_interceptions": 2,
                "fum_rec": 1,
                "def_fumbles_forced": 1,
                "def_safeties": 0,
                "fg_blocked": 0,
                "def_tds": 1,
                "pts_def_int_ret_td": 1,
                "pts_def_fum_ret_td": 0,
                "pts_def_blk_kick_td": 0,
                "pts_allow_0": 0,
                "pts_allow_7_13": 1,
                "pts_allow_14_20": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_sack"].iloc[0] == 4
    assert out["pts_def_int"].iloc[0] == 2
    # base + tier bonus should be > 0
    assert out["pts_def_std"].iloc[0] != 0


def test_pts_def_cols_handle_mixed_position_dataframe():
    """DataFrame with both DEF and non-DEF rows: leak guard NULLs only non-DEF."""
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2020,
                "week": 1,
                "nfl_team": "PIT",
                "def_sacks": 3,
                "def_interceptions": 1,
                "pts_allow_7_13": 1,
            },
            {
                "position": "QB",
                "year": 2020,
                "week": 1,
                "nfl_team": "TB",
                "passing_yards": 250,
                "passing_tds": 2,
                "pts_allow_7_13": 1,
            },
        ]
    )
    out = calculate_all_fantasy_points(df)
    # DEF row keeps pts_def_*
    assert out.iloc[0]["pts_def_sack"] == 3
    assert out.iloc[0]["pts_def_int"] == 1
    # QB row has NULL pts_def_*
    assert pd.isna(out.iloc[1]["pts_def_sack"])
    assert pd.isna(out.iloc[1]["pts_def_int"])
    assert pd.isna(out.iloc[1]["pts_def_std"])
