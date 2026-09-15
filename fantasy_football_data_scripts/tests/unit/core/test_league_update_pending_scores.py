import pandas as pd

from multi_league.core.league_refresh import pending_provider_nfl_teams
from scripts.refresh_espn_active_season import espn_source_manifest_complete


def test_live_espn_score_from_missing_monday_game_keeps_manifest_pending():
    roster = pd.DataFrame([
        {"nfl_team": "KC", "fantasy_points": 14.0, "espn_player_id": "-16012"},
        {"nfl_team": "DEN", "fantasy_points": 7.2, "espn_player_id": "p-den"},
        {"nfl_team": "BUF", "fantasy_points": 19.0, "espn_player_id": "p-buf"},
        {"nfl_team": "WSH", "fantasy_points": 11.0, "espn_player_id": "p-wsh"},
        {"nfl_team": "BYE", "fantasy_points": 0.0, "espn_player_id": "p-bye"},
    ])
    nfl_ops = pd.DataFrame([
        {"nfl_team": "BUF", "opponent_nfl_team": "HOU"},
        {"nfl_team": "WAS", "opponent_nfl_team": "PHI"},
    ])

    assert pending_provider_nfl_teams(roster, nfl_ops) == ("DEN", "KC")
    assert pending_provider_nfl_teams(
        roster,
        pd.concat([
            nfl_ops,
            pd.DataFrame([{"nfl_team": "KC", "opponent_nfl_team": "DEN"}]),
        ], ignore_index=True),
    ) == ()


def test_espn_manifest_requires_every_changed_game_and_matchup_result():
    assert not espn_source_manifest_complete(
        refresh_weeks=[1],
        fetch_rows={"pending_nfl_teams": ["DEN", "KC"], "final_matchup_weeks": 0},
    )
    assert not espn_source_manifest_complete(
        refresh_weeks=[1],
        fetch_rows={"pending_nfl_teams": [], "final_matchup_weeks": 0},
    )
    assert espn_source_manifest_complete(
        refresh_weeks=[1],
        fetch_rows={"pending_nfl_teams": [], "final_matchup_weeks": 1},
    )
