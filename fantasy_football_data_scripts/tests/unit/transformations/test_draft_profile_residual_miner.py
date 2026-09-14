import sys
from pathlib import Path

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_profile_events_translate_raw_features_to_actionable_labels():
    from multi_league.transformations.draft.profile_residual_miner import build_profile_events

    df = pd.DataFrame(
        {
            "position_group": ["RB", "WR", "QB"],
            "year": [2025, 2025, 2025],
            "draft__draft_age": [30, 23, 22],
            "bio__rookie_year": [2018, 2024, 2025],
            "bio__draft_round": [2, 1, 1],
            "bio__draft_overall": [45, 12, 1],
            "bio__is_undrafted": [0, 0, 0],
            "bio__ras_score": [6.5, 9.2, 8.5],
            "player_prev__fantasy_points": [40, 125, 0],
            "player_prev__games_started": [2, 6, 0],
            "player_prev__optimal_player_count": [1, 4, 0],
            "player_prev__clutch_equity": [-0.1, 0.2, 0],
            "player_prior__avg_fantasy_points": [80, 110, 0],
            "player_prior__avg_player_lamar": [10, 25, 0],
            "player_prior__max_player_lamar": [95, 30, 0],
            "player_prior__avg_games_started": [3, 5, 0],
            "player_prior__avg_games_rostered": [10, 12, 0],
            "player_prior__league_seasons": [5, 1, 0],
            "player_prior__optimal_player_count": [2, 4, 0],
            "player_prior__avg_clutch_equity": [0, 0.1, 0],
        }
    )

    events = build_profile_events(df)
    labels = {(row["row_index"], row["profile_label"]) for row in events}

    assert (0, "Name-value rebound") in labels
    assert (0, "Aging name value") in labels
    assert (1, "Young breakout profile") in labels
    assert (1, "Rookie pedigree bet") in labels
    assert (2, "Rookie pedigree bet") in labels


def test_position_market_scores_against_capital_not_position():
    from multi_league.transformations.draft.profile_residual_miner import (
        build_position_market_events,
        score_residual_events,
    )

    df = pd.DataFrame(
        {
            "db_name": ["demo"] * 8,
            "year": [2024] * 4 + [2025] * 4,
            "position_group": ["RB", "RB", "WR", "WR"] * 2,
            "capital_bucket": ["snake_r1_2"] * 8,
            "capital_weight": [1.0] * 8,
            "manager_lamar": [20, 22, 0, 1, 21, 23, 0, 2],
            "expected_lamar": [10] * 8,
        }
    )
    events = build_position_market_events(df)

    rows = score_residual_events(
        df,
        events,
        baseline_keys=["db_name", "year", "capital_bucket"],
        scope_label="demo",
        min_picks=2,
        min_years=2,
        min_abs_z=0.1,
        limit=10,
    )

    rb = next(row for row in rows if row["feature_value"] == "RB|snake_r1_2")
    wr = next(row for row in rows if row["feature_value"] == "WR|snake_r1_2")

    assert rb["excess_residual"] > 0
    assert wr["excess_residual"] < 0
    assert rb["years_seen"] == 2
    assert rb["earliest_year"] == 2024
    assert rb["latest_year"] == 2025
    assert 0 < rb["recency_weight"] <= 1


def test_state_registry_classifies_profile_and_position_market():
    from multi_league.transformations.draft.state_registry import classify_feature

    profile = classify_feature("profile.role_proof_missing", "Role proof missing")
    position = classify_feature("position_market", "RB|snake_r1_2")

    assert profile["state_family"] == "draft_profile"
    assert position["state_family"] == "position_market"
