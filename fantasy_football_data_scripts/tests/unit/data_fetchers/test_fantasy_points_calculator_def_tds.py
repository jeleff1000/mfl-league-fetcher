"""Task 0.1 — pts_def_td canonical-component-sum guards against fum_ret_td double-count.

L1.a recompute SQL had this fix; the calculator code did not. Without these
tests + the matching calculator change, weekly update_nfl_super_table.py runs
re-introduce the bug shipped in L1.a.

See:
- docs/superpowers/specs/2026-04-29-pts-def-audit-design.md §M12 (TD double-count)
- docs/superpowers/specs/2026-05-01-pts-off-audit-design.md Phase 0 Task 0.1
"""

from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_fantasy_points,
    calculate_composite_fantasy_points,
)


def test_pts_def_td_no_double_count_fum_only_modern():
    """Modern (1999+) row where def_tds already includes fum_ret_td.

    Component sum (pts_def_int_ret_td + pts_def_fum_ret_td + pts_def_blk_kick_td)
    = 0 + 1 + 0 = 1. def_tds = 1. Pre-fix calculator returns 1 + 1 = 2 (double-count).
    Post-fix returns 1.
    """
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2020,
                "week": 13,
                "nfl_team": "BAL",
                "def_tds": 1,
                "fum_ret_td": 1,
                "pts_def_int_ret_td": 0,
                "pts_def_fum_ret_td": 1,
                "pts_def_blk_kick_td": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_td"].iloc[0] == 1, f"Expected 1 (no double-count), got {out['pts_def_td'].iloc[0]}"


def test_pts_def_td_pre_1999_falls_back_to_def_tds():
    """Pre-1999 row with no component cols populated (PFR-merge era).

    Component sum = 0; calculator must fall back to def_tds.
    """
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 1972,
                "week": 17,
                "nfl_team": "MIA",
                "def_tds": 2,
                "fum_ret_td": 0,
                "pts_def_int_ret_td": 0,
                "pts_def_fum_ret_td": 0,
                "pts_def_blk_kick_td": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_td"].iloc[0] == 2, f"Expected 2 (def_tds fallback), got {out['pts_def_td'].iloc[0]}"


def test_pts_def_td_split_incomplete_uses_box_score():
    """Pre-2000 row where the play-by-play split is present but INCOMPLETE.

    1950 Lions wk1 vs GNB: box score has def_tds=3, but the reconstructed split
    only classified 1 (pts_def_int_ret_td=1). The pre-fix `.where(sum > 0, def_tds)`
    picked the split sum (1) and silently dropped 2 TDs (-12 pts). GREATEST must
    take the larger box-score aggregate.
    """
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 1950,
                "week": 1,
                "nfl_team": "DET",
                "def_tds": 3,
                "fum_ret_td": 0,
                "pts_def_int_ret_td": 1,
                "pts_def_fum_ret_td": 0,
                "pts_def_blk_kick_td": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_td"].iloc[0] == 3, f"Expected 3 (box-score wins), got {out['pts_def_td'].iloc[0]}"


def test_pts_def_std_1950_lions_full_line():
    """End-to-end std score for the reported 1950 Lions wk1 vs GNB DST line.

    7 INT (14) + 1 FR (2) + 3 def TDs (18) + PA 7 -> 7-13 tier (4) = 38.
    Pre-fix credited only 1 TD -> 26 (the reported bug).
    """
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 1950,
                "week": 1,
                "nfl_team": "DET",
                "def_sacks": 0,
                "def_interceptions": 7,
                "fum_rec": 1,
                "def_safeties": 0,
                "def_tds": 3,
                "fum_ret_td": 0,
                "pts_def_int_ret_td": 1,
                "pts_def_fum_ret_td": 0,
                "pts_def_blk_kick_td": 0,
                "pts_allow_7_13": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_td"].iloc[0] == 3
    assert out["pts_def_std"].iloc[0] == 38, f"Expected 38, got {out['pts_def_std'].iloc[0]}"


def test_pts_def_td_int_only_modern():
    """Modern row with only INT-return TD populated."""
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2020,
                "week": 1,
                "nfl_team": "PIT",
                "def_tds": 1,
                "fum_ret_td": 0,
                "pts_def_int_ret_td": 1,
                "pts_def_fum_ret_td": 0,
                "pts_def_blk_kick_td": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_td"].iloc[0] == 1


def test_pts_def_td_blk_kick_only_modern():
    """Modern row with only blocked-kick-return TD populated."""
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2019,
                "week": 5,
                "nfl_team": "NE",
                "def_tds": 1,
                "fum_ret_td": 0,
                "pts_def_int_ret_td": 0,
                "pts_def_fum_ret_td": 0,
                "pts_def_blk_kick_td": 1,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    assert out["pts_def_td"].iloc[0] == 1


def test_pts_def_td_multiple_components():
    """Modern row with INT-TD + fum-TD on same team-game (rare but valid)."""
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2018,
                "week": 14,
                "nfl_team": "CHI",
                "def_tds": 2,
                "fum_ret_td": 1,
                "pts_def_int_ret_td": 1,
                "pts_def_fum_ret_td": 1,
                "pts_def_blk_kick_td": 0,
            }
        ]
    )
    out = calculate_all_fantasy_points(df)
    # Component sum = 2; takes precedence over def_tds=2 (which already includes both)
    # Pre-fix would return def_tds + fum_ret_td = 3 (one extra fum-TD double-count)
    assert out["pts_def_td"].iloc[0] == 2, f"Expected 2, got {out['pts_def_td'].iloc[0]}"


def test_def_fumble_return_tds_do_not_double_count_in_default_fpts():
    """DST fumble-return TDs are already in pts_def_std via def_tds.

    They must not also flow through generic pts_misc / pts_fum_ret_td_6, which
    is for non-DST player fumble return TD scoring. 1989 CLE W1 is the live
    regression shape: shutout, 6 sacks, 3 INT, 5 FR, 3 defensive TDs, 2 of
    which were fumble-return TDs.
    """
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 1989,
                "week": 1,
                "nfl_team": "CLE",
                "def_sacks": 6,
                "def_interceptions": 3,
                "fum_rec": 5,
                "def_fumbles_forced": 0,
                "def_safeties": 0,
                "def_tds": 3,
                "fum_ret_td": 2,
                "pts_allow_0": 1,
            }
        ]
    )

    out = calculate_composite_fantasy_points(calculate_all_fantasy_points(df))

    assert out["pts_def_std"].iloc[0] == 50
    assert out["pts_misc"].iloc[0] == 0
    assert out["pts_fum_ret_td_6"].iloc[0] == 0
    assert out["fpts_4pt_half"].iloc[0] == 50


def test_forced_fumbles_are_not_baked_into_default_dst_points():
    """Forced fumbles stay as pts_def_ff for custom leagues, not pts_def_std."""
    df = pd.DataFrame(
        [
            {
                "position": "DEF",
                "year": 2024,
                "week": 1,
                "nfl_team": "NYJ",
                "def_fumbles_forced": 3,
                "fum_rec": 1,
            }
        ]
    )

    out = calculate_all_fantasy_points(df)

    assert out["pts_def_ff"].iloc[0] == 3
    assert out["pts_def_fr"].iloc[0] == 1
    assert out["pts_def_std"].iloc[0] == 2


def test_non_def_fumble_return_tds_still_score_in_misc():
    df = pd.DataFrame([{"position": "WR", "year": 2024, "week": 1, "fum_ret_td": 1}])

    out = calculate_composite_fantasy_points(calculate_all_fantasy_points(df))

    assert out["pts_misc"].iloc[0] == 6
    assert out["pts_fum_ret_td_6"].iloc[0] == 6
    assert out["fpts_4pt_half"].iloc[0] == 6
