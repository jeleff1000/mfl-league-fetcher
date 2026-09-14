import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.sleeper_corpus.playoff_backfill import championship_game_rosters_for_week


def test_only_championship_placement_games_are_playoff_games():
    bracket = [
        {"r": 1, "p": 1, "t1": 1, "t2": 2, "w": 1},
        {"r": 1, "p": 3, "t1": 3, "t2": 4, "w": 3},
        {"r": 2, "p": 1, "t1": 1, "t2": 5, "w": 1},
    ]
    settings = {"playoff_week_start": 14, "playoff_teams": 4, "playoff_round_type": 0}
    assert championship_game_rosters_for_week(bracket, settings, 14) == {1, 2}
    assert championship_game_rosters_for_week(bracket, settings, 15) == {1, 5}
    assert 3 not in championship_game_rosters_for_week(bracket, settings, 14)


def test_no_bracket_game_means_no_weekly_playoff_signal():
    settings = {"playoff_week_start": 14, "playoff_teams": 4, "playoff_round_type": 0}
    assert championship_game_rosters_for_week([], settings, 14) == set()
