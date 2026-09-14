"""Regression tests for source matchup championship-game attribution."""

from multi_league.data_fetchers.fleaflicker.fleaflicker_matchups import rows_for_game
from multi_league.data_fetchers.mfl.mfl_matchups import rows_for_week


def _mfl_side(fid: str, score: str, result: str) -> dict:
    return {"id": fid, "score": score, "result": result}


def test_mfl_marks_every_game_in_the_championship_bracket():
    rows = rows_for_week(
        {
            "matchup": [
                {
                    "regularSeason": "0",
                    "franchise": [
                        _mfl_side("1", "120", "W"),
                        _mfl_side("2", "100", "L"),
                    ],
                },
            ],
        },
        year=2003,
        week=16,
        league_id="season-league",
        seed_league_id="season-league",
        team_lookup={},
        last_reg_week=14,
        champ_pairs={(16, frozenset({"1", "2"}))},
    )

    assert len(rows) == 2
    assert {row["is_championship"] for row in rows} == {True}
    assert {row["is_consolation"] for row in rows} == {False}


def test_mfl_does_not_mark_consolation_game_as_championship():
    rows = rows_for_week(
        {
            "matchup": [
                {
                    "regularSeason": "0",
                    "franchise": [
                        _mfl_side("3", "90", "W"),
                        _mfl_side("4", "80", "L"),
                    ],
                },
            ],
        },
        year=2003,
        week=16,
        league_id="season-league",
        seed_league_id="season-league",
        team_lookup={},
        last_reg_week=14,
        champ_pairs={(16, frozenset({"1", "2"}))},
    )

    assert {row["is_championship"] for row in rows} == {False}
    assert {row["is_consolation"] for row in rows} == {True}


def test_fleaflicker_string_false_is_not_truthy_and_title_game_is_explicit():
    rows = rows_for_game(
        {
            "id": "game-1",
            "away": {"id": "1", "owners": [{"id": "a"}], "name": "A"},
            "home": {"id": "2", "owners": [{"id": "b"}], "name": "B"},
            "awayScore": {"score": 110},
            "homeScore": {"score": 100},
            "awayResult": "WIN",
            "homeResult": "LOSE",
            "isPlayoffs": "true",
            "isConsolation": "false",
        },
        year=2003,
        week=16,
        league_id="league",
        team_lookup={},
    )

    assert len(rows) == 2
    assert {row["is_playoffs"] for row in rows} == {True}
    assert {row["is_consolation"] for row in rows} == {False}
    assert {row["is_championship"] for row in rows} == {True}
