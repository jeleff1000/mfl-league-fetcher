from __future__ import annotations

import pandas as pd
import pytest

from multi_league.core.league_update_ownership import PreservationError, assert_refresh_preservation


def _player_rank_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    before = pd.DataFrame(
        [
            {
                "db_name": "league_a",
                "year": 2026,
                "week": 1,
                "player_week": "00-0000001_2026_1",
                "position_alltime_rank": 120,
            },
            {
                "db_name": "league_a",
                "year": 2026,
                "week": 1,
                "player_week": "00-0000002_2026_1",
                "position_alltime_rank": 121,
            },
        ]
    )
    after = before.copy()
    after.loc[1, "position_alltime_rank"] = None
    return before, after


def test_refresh_allows_rank_to_clear_only_without_a_finalized_ops_player_week():
    before, after = _player_rank_frames()

    receipt = assert_refresh_preservation(
        {"player_fantasy": before},
        {"player_fantasy": after},
        active_year=2026,
        finalized_ops_player_weeks={"00-0000001_2026_1"},
    )

    assert receipt["semantic_ops_rank_deselections"] == 1


def test_refresh_rejects_rank_loss_for_a_player_with_a_finalized_ops_fact():
    before, after = _player_rank_frames()
    after.loc[0, "position_alltime_rank"] = None

    with pytest.raises(PreservationError, match="position_alltime_rank"):
        assert_refresh_preservation(
            {"player_fantasy": before},
            {"player_fantasy": after},
            active_year=2026,
            finalized_ops_player_weeks={"00-0000001_2026_1"},
        )
