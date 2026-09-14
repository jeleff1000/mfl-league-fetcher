from __future__ import annotations

import ddl_sacko_bracket_results_audit as audit


def _pair(
    *,
    year: int,
    week: int,
    a: str,
    b: str,
    a_pts: float,
    b_pts: float,
    is_consolation: bool = False,
    consolation_round: str | None = None,
    placement_game: bool = False,
    sacko: str | None = None,
) -> list[dict]:
    base = {
        "db_name": "unit_league",
        "year": year,
        "week": week,
        "team_name": None,
        "tie": None,
        "is_playoffs": False,
        "is_consolation": is_consolation,
        "is_championship": False,
        "postseason": is_consolation,
        "playoff_round": None,
        "consolation_round": consolation_round,
        "consolation_final": consolation_round == "consolation_final",
        "final_playoff_seed": None,
        "playoff_seed": None,
        "placement_rank": None,
        "placement_game": placement_game,
        "is_bye_week": False,
        "champion": 0,
    }
    return [
        {
            **base,
            "manager": a,
            "franchise_id": a,
            "opponent": b,
            "opponent_franchise_id": b,
            "team_points": a_pts,
            "opponent_points": b_pts,
            "win": 1 if a_pts > b_pts else 0,
            "loss": 1 if a_pts < b_pts else 0,
            "sacko": 1 if sacko == a else 0,
        },
        {
            **base,
            "manager": b,
            "franchise_id": b,
            "opponent": a,
            "opponent_franchise_id": a,
            "team_points": b_pts,
            "opponent_points": a_pts,
            "win": 1 if b_pts > a_pts else 0,
            "loss": 1 if b_pts < a_pts else 0,
            "sacko": 1 if sacko == b else 0,
        },
    ]


def _settings(year: int = 2025) -> dict:
    return {
        "db_name": "unit_league",
        "year": year,
        "platform": "sleeper",
        "league_key": "unit",
        "num_teams": 8,
        "playoff_teams": 4,
        "bye_teams": 0,
        "playoff_start_week": 2,
        "regular_season_weeks": 1,
        "end_week": 3,
        "uses_median": False,
        "has_multiweek_championship": False,
        "uses_playoff_reseeding": False,
        "sleeper_playoff_type": 0,
        "sacko_mode": "consolation_bracket",
    }


def _consolation_rows(current_sacko: str = "D") -> list[dict]:
    rows = []
    rows += _pair(
        year=2025,
        week=2,
        a="A",
        b="D",
        a_pts=100.0,
        b_pts=80.0,
        is_consolation=True,
        consolation_round="consolation_semifinal",
    )
    rows += _pair(
        year=2025,
        week=2,
        a="C",
        b="B",
        a_pts=105.0,
        b_pts=90.0,
        is_consolation=True,
        consolation_round="consolation_semifinal",
    )
    # Winners' placement game: both teams already exited the Sacko path.
    rows += _pair(
        year=2025,
        week=3,
        a="A",
        b="C",
        a_pts=115.0,
        b_pts=108.0,
        is_consolation=True,
        consolation_round="consolation_final",
    )
    # Sacko Bowl: prior consolation losers meet; loser D is Sacko.
    rows += _pair(
        year=2025,
        week=3,
        a="D",
        b="B",
        a_pts=70.0,
        b_pts=85.0,
        is_consolation=True,
        consolation_round="consolation_final",
        sacko=current_sacko,
    )
    # Later placement game should not displace the Sacko Bowl.
    rows += _pair(
        year=2025,
        week=4,
        a="E",
        b="F",
        a_pts=122.0,
        b_pts=90.0,
        is_consolation=True,
        consolation_round="3rd_place_game",
        placement_game=True,
    )
    return rows


def test_trace_ddl_sacko_follows_loser_path_not_any_final_game():
    game, source = audit.trace_ddl_sacko(_consolation_rows())

    assert source == "ddl_loser_path"
    assert game is not None
    assert game.loser == "D"
    assert game.winner == "B"
    assert audit.format_sacko_score(game.game) == "70.00 - 85.00"


def test_audit_season_compares_current_sacko_to_ddl_loser_path():
    result = audit.audit_season(_settings(), _consolation_rows())

    assert result.status == "PASS"
    assert result.ddl_sacko == "D"
    assert result.current_sacko == "D"


def test_audit_season_does_not_trust_wrong_sacko_flag():
    result = audit.audit_season(_settings(), _consolation_rows(current_sacko="B"))

    assert result.status == "MISMATCH"
    assert result.ddl_sacko == "D"
    assert result.current_sacko == "B"


def test_single_sacko_final_label_can_pick_lower_seed_sacko():
    rows = _pair(
        year=2025,
        week=3,
        a="eight_seed",
        b="one_seed",
        a_pts=62.7,
        b_pts=114.0,
        is_consolation=True,
        consolation_round="consolation_final",
        sacko="eight_seed",
    )

    result = audit.audit_season(_settings(), rows)

    assert result.status == "PASS"
    assert result.ddl_sacko == "eight_seed"
    assert result.current_sacko == "eight_seed"
