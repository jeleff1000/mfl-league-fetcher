"""Regression coverage for leagues that disable playoffs."""

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(SCRIPTS_DIR / "multi_league"))
sys.path.insert(0, str(SCRIPTS_DIR / "multi_league" / "transformations" / "matchup"))

from multi_league.transformations.matchup import playoff_odds_import as poi


def test_fill_missing_title_odds_carries_values_across_bye_weeks():
    rows = pd.DataFrame(
        [
            {"year": 2025, "week": 14, "franchise_id": "alpha_0", "p_champ": 31.25, "is_bye_week": 0},
            {"year": 2025, "week": 15, "franchise_id": "alpha_0", "p_champ": None, "is_bye_week": 1},
            {"year": 2025, "week": 16, "franchise_id": "alpha_0", "p_champ": 68.75, "is_bye_week": 0},
            {"year": 2025, "week": 14, "franchise_id": "beta_0", "p_champ": None, "is_bye_week": 1},
            {"year": 2025, "week": 15, "franchise_id": "beta_0", "p_champ": 20.0, "is_bye_week": 0},
        ]
    )

    result = poi.fill_missing_title_odds(rows)

    assert result.loc[1, "p_champ"] == 31.25
    assert result.loc[3, "p_champ"] == 20.0


def test_projection_settings_infer_bracket_when_playoff_rows_are_absent():
    rows = []
    teams = [f"team_{i}" for i in range(8)]
    for week in range(1, 14):
        for i in range(0, 8, 2):
            rows.extend(
                [
                    {
                        "year": 2025,
                        "week": week,
                        "manager": teams[i],
                        "opponent": teams[i + 1],
                        "franchise_id": teams[i],
                        "opponent_franchise_id": teams[i + 1],
                        "team_points": 100.0,
                        "is_playoffs": 0,
                        "is_consolation": 0,
                    },
                    {
                        "year": 2025,
                        "week": week,
                        "manager": teams[i + 1],
                        "opponent": teams[i],
                        "franchise_id": teams[i + 1],
                        "opponent_franchise_id": teams[i],
                        "team_points": 90.0,
                        "is_playoffs": 0,
                        "is_consolation": 0,
                    },
                ]
            )

    config = poi.infer_projection_settings(pd.DataFrame(rows), 2025)

    assert config["num_teams"] == 8
    assert config["playoff_start_week"] == 14
    assert config["num_playoff_teams"] == 4
    assert config["bye_teams"] == 0


def test_projection_settings_uses_actual_playoff_field_when_present():
    rows = []
    for i in range(6):
        rows.append(
            {
                "year": 2025,
                "week": 14,
                "manager": f"team_{i}",
                "franchise_id": f"team_{i}",
                "opponent": f"team_{(i + 1) % 6}",
                "opponent_franchise_id": f"team_{(i + 1) % 6}",
                "team_points": 100.0,
                "is_playoffs": 1,
                "is_consolation": 0,
                "final_playoff_seed": i + 1,
            }
        )

    config = poi.infer_projection_settings(pd.DataFrame(rows), 2025)

    assert config["num_teams"] == 6
    assert config["playoff_start_week"] == 14
    assert config["num_playoff_teams"] == 6
    assert config["bye_teams"] == 2


def test_playoff_projection_can_seed_from_standings_columns_without_regular_rows():
    rows = pd.DataFrame(
        [
            {
                "franchise_id": "alpha_0",
                "manager": "Alpha",
                "is_playoffs": 1,
                "is_consolation": 0,
                "wins_to_date": 9,
                "points_scored_to_date": 1200,
            },
            {
                "franchise_id": "beta_0",
                "manager": "Beta",
                "is_playoffs": 1,
                "is_consolation": 0,
                "wins_to_date": 7,
                "points_scored_to_date": 1100,
            },
        ]
    )

    wins, points = poi.standings_for_projection(rows, pd.DataFrame())

    assert wins == {"alpha_0": 9.0, "beta_0": 7.0}
    assert points == {"alpha_0": 1200.0, "beta_0": 1100.0}


def test_playoff_projection_uses_nonconsolation_rows_as_seed_input_when_regular_is_empty():
    rows = pd.DataFrame(
        [
            {
                "franchise_id": "alpha_0",
                "opponent_franchise_id": "beta_0",
                "is_playoffs": 1,
                "is_consolation": 0,
            },
            {
                "franchise_id": "beta_0",
                "opponent_franchise_id": "alpha_0",
                "is_playoffs": 1,
                "is_consolation": 0,
            },
        ]
    )

    seed_input = poi.seed_input_for_projection(rows, pd.DataFrame())

    assert len(seed_input) == 2
    assert set(seed_input["franchise_id"]) == {"alpha_0", "beta_0"}


def test_regular_week_odds_handles_disabled_playoffs(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 16)

    reg_to_date = pd.DataFrame(
        [
            {
                "year": 2025,
                "week": 1,
                "manager": "Alpha",
                "opponent": "Beta",
                "franchise_id": "alpha_0",
                "opponent_franchise_id": "beta_0",
                "team_points": 101.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 1,
                "manager": "Beta",
                "opponent": "Alpha",
                "franchise_id": "beta_0",
                "opponent_franchise_id": "alpha_0",
                "team_points": 98.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
        ]
    )

    series_pack, seed_df, win_df = poi._vectorized_regular_and_bracket(
        reg_to_date,
        pd.DataFrame(),
        {"alpha_0": 101.0, "beta_0": 98.0},
        {"alpha_0": 12.0, "beta_0": 12.0},
        season=2025,
        week=1,
        playoff_slots=0,
        bye_slots=0,
        bracket_reseed=False,
        regular_season_weeks=18,
        num_teams=2,
        use_median=False,
    )

    assert not seed_df.empty
    assert not win_df.empty
    assert series_pack["P_Playoffs_SIM"].sum() == 0
    assert series_pack["P_Bye_SIM"].sum() == 0
    assert series_pack["P_Semis_SIM"].sum() == 0
    assert series_pack["P_Final"].sum() == 0
    assert series_pack["P_Champ"].sum() == 100.0
    assert series_pack["P_Champ"].idxmax() == "alpha_0"


def test_disabled_playoff_champion_odds_respect_best_score_seeding(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 8)

    reg_to_date = pd.DataFrame(
        [
            {
                "year": 2025,
                "week": 1,
                "manager": "Alpha",
                "opponent": "Beta",
                "franchise_id": "alpha_0",
                "opponent_franchise_id": "beta_0",
                "team_points": 80.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 1,
                "manager": "Beta",
                "opponent": "Alpha",
                "franchise_id": "beta_0",
                "opponent_franchise_id": "alpha_0",
                "team_points": 70.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 1,
                "manager": "Charlie",
                "opponent": "Delta",
                "franchise_id": "charlie_0",
                "opponent_franchise_id": "delta_0",
                "team_points": 150.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 1,
                "manager": "Delta",
                "opponent": "Charlie",
                "franchise_id": "delta_0",
                "opponent_franchise_id": "charlie_0",
                "team_points": 140.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 2,
                "manager": "Alpha",
                "opponent": "Charlie",
                "franchise_id": "alpha_0",
                "opponent_franchise_id": "charlie_0",
                "team_points": 80.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 2,
                "manager": "Charlie",
                "opponent": "Alpha",
                "franchise_id": "charlie_0",
                "opponent_franchise_id": "alpha_0",
                "team_points": 70.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 2,
                "manager": "Delta",
                "opponent": "Beta",
                "franchise_id": "delta_0",
                "opponent_franchise_id": "beta_0",
                "team_points": 220.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2025,
                "week": 2,
                "manager": "Beta",
                "opponent": "Delta",
                "franchise_id": "beta_0",
                "opponent_franchise_id": "delta_0",
                "team_points": 60.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
        ]
    )

    record_pack, _, _ = poi._vectorized_regular_and_bracket(
        reg_to_date,
        pd.DataFrame(),
        {},
        {},
        season=2025,
        week=2,
        playoff_slots=0,
        bye_slots=0,
        bracket_reseed=False,
        regular_season_weeks=2,
        num_teams=4,
        use_median=False,
    )
    points_pack, _, _ = poi._vectorized_regular_and_bracket(
        reg_to_date,
        pd.DataFrame(),
        {},
        {},
        season=2025,
        week=2,
        playoff_slots=0,
        bye_slots=0,
        bracket_reseed=False,
        regular_season_weeks=2,
        num_teams=4,
        use_median=False,
        seeding_rule="TOTAL_POINTS_SCORED",
    )

    assert record_pack["P_Champ"].idxmax() == "alpha_0"
    assert points_pack["P_Champ"].idxmax() == "delta_0"


def test_regular_week_odds_handles_two_team_championship(monkeypatch):
    monkeypatch.setattr(poi, "N_SIMS", 16)

    reg_to_date = pd.DataFrame(
        [
            {
                "year": 2013,
                "week": 14,
                "manager": "Alpha",
                "opponent": "Beta",
                "franchise_id": "alpha_0",
                "opponent_franchise_id": "beta_0",
                "team_points": 101.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
            {
                "year": 2013,
                "week": 14,
                "manager": "Beta",
                "opponent": "Alpha",
                "franchise_id": "beta_0",
                "opponent_franchise_id": "alpha_0",
                "team_points": 98.0,
                "is_playoffs": 0,
                "is_consolation": 0,
            },
        ]
    )

    series_pack, seed_df, win_df = poi._vectorized_regular_and_bracket(
        reg_to_date,
        pd.DataFrame(),
        {"alpha_0": 101.0, "beta_0": 98.0},
        {"alpha_0": 12.0, "beta_0": 12.0},
        season=2013,
        week=14,
        playoff_slots=2,
        bye_slots=0,
        bracket_reseed=False,
        regular_season_weeks=14,
        num_teams=2,
        use_median=False,
    )

    assert not seed_df.empty
    assert not win_df.empty
    assert series_pack["P_Final"].sum() == 200.0
    assert series_pack["P_Champ"].sum() == 100.0
