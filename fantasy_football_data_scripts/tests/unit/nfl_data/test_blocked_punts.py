from __future__ import annotations

import pandas as pd

from nfl_data.blocked_punts import apply_historical_dst_punt_blocks, load_historical_blocked_punts


def test_historical_blocked_punts_source_loads_with_season_year():
    source = load_historical_blocked_punts()

    assert len(source) == 761
    assert source["punt_blocks"].sum() == 791
    assert source["season"].min() == 1936
    assert source["season"].max() == 1999


def test_apply_historical_dst_punt_blocks_matches_display_and_raw_teams():
    defensive = pd.DataFrame(
        [
            {
                "year": 1999,
                "week": 15,
                "nfl_team": "NE",
                "opponent_nfl_team": "PHI",
                "fg_blocked": 0,
            },
            {
                "year": 1939,
                "week": 14,
                "nfl_team": "NYG",
                "opponent_nfl_team": "GB",
                "fg_blocked": 0,
            },
        ]
    )

    out = apply_historical_dst_punt_blocks(defensive)

    assert out.loc[0, "pts_def_punt_block"] == 1
    assert out.loc[0, "pts_def_block"] == 1
    assert out.loc[1, "pts_def_punt_block"] == 2
    assert out.loc[1, "pts_def_block"] == 2
