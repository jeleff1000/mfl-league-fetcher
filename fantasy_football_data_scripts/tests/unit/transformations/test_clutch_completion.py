import pandas as pd

from multi_league.transformations.player.clutch_to_player import (
    build_clutch_row_keys,
    fill_missing_clutch_equity,
)


def test_missing_clutch_is_neutral_zero_when_no_lamar_evidence_exists():
    frame = pd.DataFrame(
        {
            "clutch_equity": [1.25, None],
            "is_rostered": [1, 1],
            "fantasy_position": ["RB", "IDP"],
        }
    )

    result = fill_missing_clutch_equity(frame)

    assert result["clutch_equity"].tolist() == [1.25, 0.0]


def test_clutch_update_keys_fall_back_when_canonical_player_ids_are_null():
    frame = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 1,
                "player_week": "2024_1_00-0033873",
                "NFL_player_id": "00-0033873",
                "fleaflicker_player_id": "3452",
                "manager": "Alpha",
                "position": "QB",
                "player": "Lamar Jackson",
            },
            {
                "year": 2024,
                "week": 1,
                "player_week": None,
                "NFL_player_id": None,
                "fleaflicker_player_id": "3452",
                "manager": "Alpha",
                "position": "QB",
                "player": "Lamar Jackson",
            },
        ]
    )

    keys = build_clutch_row_keys(frame)

    assert keys.tolist() == ["pw:2024_1_00-0033873", "fleaflicker_player_id:3452:2024:1"]
