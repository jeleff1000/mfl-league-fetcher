from __future__ import annotations

import ddl_bracket_results_audit as audit


def _pair(
    *,
    year: int,
    week: int,
    a: str,
    b: str,
    a_pts: float,
    b_pts: float,
    is_playoffs: bool = False,
    is_consolation: bool = False,
    is_championship: bool = False,
    playoff_round: str | None = None,
) -> list[dict]:
    base = {
        "db_name": "unit_league",
        "year": year,
        "week": week,
        "team_name": None,
        "win": None,
        "loss": None,
        "tie": None,
        "is_playoffs": is_playoffs,
        "is_consolation": is_consolation,
        "is_championship": is_championship,
        "postseason": is_playoffs or is_consolation,
        "playoff_round": playoff_round,
        "final_playoff_seed": None,
        "playoff_seed": None,
        "placement_rank": None,
        "is_bye_week": False,
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
        },
    ]


def _settings(year: int = 2025) -> dict:
    return {
        "db_name": "unit_league",
        "year": year,
        "platform": "sleeper",
        "league_key": "unit",
        "num_teams": 4,
        "playoff_teams": 4,
        "bye_teams": 0,
        "playoff_start_week": 2,
        "regular_season_weeks": 1,
        "end_week": 3,
        "uses_median": False,
        "has_multiweek_championship": False,
        "uses_playoff_reseeding": False,
        "sleeper_playoff_type": 0,
    }


def test_actual_championship_result_uses_scores_not_champion_flags():
    rows = _pair(
        year=2025,
        week=3,
        a="lower_seed_api_winner",
        b="high_seed_flagged_elsewhere",
        a_pts=101.0,
        b_pts=99.0,
        is_playoffs=True,
        is_championship=True,
        playoff_round="championship",
    )
    # This column is deliberately wrong; the audit should not read it.
    rows[1]["champion"] = 1

    result, source = audit.actual_championship_result(rows)

    assert source == "championship_label"
    assert result is not None
    assert result.winner == "lower_seed_api_winner"
    assert audit.format_score(result) == "101.00 - 99.00"


def test_collapse_games_does_not_double_count_reciprocal_rows():
    rows = _pair(
        year=2025,
        week=3,
        a="A",
        b="B",
        a_pts=120.0,
        b_pts=110.0,
        is_playoffs=True,
        is_championship=True,
        playoff_round="championship",
    )

    games = audit.collapse_games(rows, "unit")

    assert len(games) == 1
    assert games[0].points_a == 120.0
    assert games[0].points_b == 110.0
    assert games[0].winner == "A"


def test_multiweek_championship_sums_unique_team_week_scores():
    rows = []
    rows += _pair(
        year=2025,
        week=3,
        a="A",
        b="B",
        a_pts=80.0,
        b_pts=100.0,
        is_playoffs=True,
        is_championship=True,
        playoff_round="championship",
    )
    rows += _pair(
        year=2025,
        week=4,
        a="A",
        b="B",
        a_pts=130.0,
        b_pts=90.0,
        is_playoffs=True,
        is_championship=True,
        playoff_round="championship",
    )

    result, source = audit.actual_championship_result(rows)

    assert source == "championship_label"
    assert result is not None
    assert result.winner == "A"
    assert result.points_a == 210.0
    assert result.points_b == 190.0
    assert audit.format_weeks(result.weeks) == "3-4"


def test_audit_season_compares_tracer_think_to_final_game_did():
    rows = []
    # Regular season for seed calculation.
    rows += _pair(year=2025, week=1, a="A", b="B", a_pts=120.0, b_pts=100.0)
    rows += _pair(year=2025, week=1, a="C", b="D", a_pts=110.0, b_pts=90.0)
    # Semifinals.
    rows += _pair(
        year=2025,
        week=2,
        a="A",
        b="D",
        a_pts=90.0,
        b_pts=80.0,
        is_playoffs=True,
        playoff_round="semifinal",
    )
    rows += _pair(
        year=2025,
        week=2,
        a="C",
        b="B",
        a_pts=100.0,
        b_pts=95.0,
        is_playoffs=True,
        playoff_round="semifinal",
    )
    # Championship: lower seed C wins.
    rows += _pair(
        year=2025,
        week=3,
        a="A",
        b="C",
        a_pts=100.0,
        b_pts=110.0,
        is_playoffs=True,
        is_championship=True,
        playoff_round="championship",
    )
    rows.append(
        {
            "db_name": "unit_league",
            "year": 2025,
            "week": 2,
            "manager": "E",
            "franchise_id": "E",
            "opponent": None,
            "opponent_franchise_id": None,
            "team_points": None,
            "opponent_points": None,
            "is_bye_week": True,
            "is_playoffs": False,
            "is_consolation": False,
        }
    )

    result = audit.audit_season(_settings(), rows)

    assert result.status == "PASS"
    assert result.think_winner == "C"
    assert result.did_winner == "C"
    assert result.final_score == "110.00 - 100.00"
