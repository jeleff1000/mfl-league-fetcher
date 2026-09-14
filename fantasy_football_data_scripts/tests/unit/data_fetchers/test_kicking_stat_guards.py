import sys
from pathlib import Path

import pandas as pd

SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.kicking_stat_guards import sanitize_non_kicker_kicking_leaks


def test_sanitize_non_kicker_kicking_leaks_preserves_raw_confirmed_emergency_kicks():
    target = pd.DataFrame(
        [
            {
                "player": "Bob Christian",
                "NFL_player_id": "00-bob",
                "player_week": "00-bob_1999_1",
                "position": "FB",
                "year": 1999,
                "week": 1,
                "fg_att": 3,
                "fg_made": 3,
                "pat_att": 3,
                "pat_made": 3,
                "fg_yards": 117,
                "pts_k_std": 15,
            },
            {
                "player": "Morris Unutoa",
                "NFL_player_id": "00-morris",
                "player_week": "00-morris_1999_12",
                "position": "C",
                "year": 1999,
                "week": 12,
                "fg_att": 0,
                "fg_made": 0,
                "pat_att": 1,
                "pat_made": 1,
                "fg_yards": 0,
                "pts_k_std": 1,
            },
            {
                "player": "Gary Anderson",
                "NFL_player_id": "00-kicker",
                "player_week": "00-kicker_1999_1",
                "position": "K",
                "year": 1999,
                "week": 1,
                "fg_att": 3,
                "fg_made": 1,
                "pat_att": 2,
                "pat_made": 2,
                "fg_yards": 40,
                "pts_k_std": 5,
            },
            {
                "player": "George Blanda",
                "NFL_player_id": "00-historical",
                "player_week": "00-historical_1978_1",
                "position": "QB",
                "year": 1978,
                "week": 1,
                "fg_att": 2,
                "fg_made": 2,
                "pat_att": 1,
                "pat_made": 1,
                "fg_yards": 70,
                "pts_k_std": 7,
            },
            {
                "player": "Position Missing Kicker",
                "NFL_player_id": "00-missing-pos",
                "player_week": "00-missing-pos_1999_1",
                "position": None,
                "year": 1999,
                "week": 1,
                "fg_att": 1,
                "fg_made": 1,
                "pat_att": 0,
                "pat_made": 0,
                "fg_yards": 45,
                "pts_k_std": 3,
            },
        ]
    )
    raw_source = pd.DataFrame(
        [
            {
                "player_display_name": "Bob Christian",
                "player_id": "00-bob",
                "position": "FB",
                "season": 1999,
                "week": 1,
                "fg_att": 0,
                "fg_made": 0,
                "pat_att": 0,
                "pat_made": 0,
            },
            {
                "player_display_name": "Morris Unutoa",
                "player_id": "00-morris",
                "position": "C",
                "season": 1999,
                "week": 12,
                "fg_att": 0,
                "fg_made": 0,
                "pat_att": 1,
                "pat_made": 1,
            },
        ]
    )

    result = sanitize_non_kicker_kicking_leaks(target, source_df=raw_source, min_year=1999)

    bob = result.loc[result["player"] == "Bob Christian"].iloc[0]
    assert bob["fg_att"] == 0
    assert bob["fg_made"] == 0
    assert bob["pat_att"] == 0
    assert bob["pat_made"] == 0
    assert bob["fg_yards"] == 0
    assert bob["pts_k_std"] == 0

    emergency = result.loc[result["player"] == "Morris Unutoa"].iloc[0]
    assert emergency["pat_att"] == 1
    assert emergency["pat_made"] == 1
    assert emergency["pts_k_std"] == 1

    kicker = result.loc[result["player"] == "Gary Anderson"].iloc[0]
    assert kicker["fg_att"] == 3
    assert kicker["pat_att"] == 2

    historical = result.loc[result["player"] == "George Blanda"].iloc[0]
    assert historical["fg_att"] == 2
    assert historical["pts_k_std"] == 7

    missing_position = result.loc[result["player"] == "Position Missing Kicker"].iloc[0]
    assert missing_position["fg_att"] == 1
    assert missing_position["pts_k_std"] == 3


def test_sanitize_non_kicker_kicking_leaks_requires_source_allow_list_by_default():
    target = pd.DataFrame(
        [
            {
                "player": "Andrew Glover",
                "NFL_player_id": "00-glover",
                "player_week": "00-glover_1999_1",
                "position": "TE",
                "year": 1999,
                "week": 1,
                "fg_att": 3,
                "fg_made": 2,
                "pat_att": 3,
                "pat_made": 3,
            }
        ]
    )

    result = sanitize_non_kicker_kicking_leaks(target)

    assert result.loc[0, "fg_att"] == 3
    assert result.loc[0, "fg_made"] == 2
    assert result.loc[0, "pat_att"] == 3
