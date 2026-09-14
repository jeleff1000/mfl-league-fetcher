from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.combine_dst_to_nfl import defense_to_player_shape
from nfl_data.nfl_franchises import get_def_display_name, get_def_player_id


def test_lvr_alias_maps_to_raiders_dst_identity() -> None:
    assert get_def_player_id("LVR", 2024) == "DEF-31"
    assert get_def_display_name("LVR", 2024) == "Raiders DST"


def test_defense_rows_always_use_nickname_dst_display_name() -> None:
    df = pd.DataFrame(
        [
            {
                "NFL_player_id": "DEF-BAL",
                "player": "BAL Defense",
                "nfl_team": "BAL",
                "year": 2024,
                "week": 1,
                "nfl_position": "DEF",
            },
            {
                "NFL_player_id": "DEF-LVR",
                "player": "LVR Defense",
                "nfl_team": "LVR",
                "year": 2024,
                "week": 1,
                "nfl_position": "DEF",
            },
        ]
    )

    out = defense_to_player_shape(df)

    assert out["player"].tolist() == ["Ravens DST", "Raiders DST"]
