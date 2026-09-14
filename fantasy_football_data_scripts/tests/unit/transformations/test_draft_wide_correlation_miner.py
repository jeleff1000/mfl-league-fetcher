import sys
from pathlib import Path

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_wide_feature_generation_excludes_posthoc_bio_state():
    from multi_league.transformations.draft.wide_correlation_miner import build_wide_features

    df = pd.DataFrame(
        {
            "draft__position": ["RB", "RB", "WR", "WR"],
            "bio__status": ["CUT", "CUT", "ACT", "ACT"],
            "bio__latest_team": ["BAL", "BAL", "KC", "KC"],
            "bio__ras_score": [9.5, 9.2, 4.0, 4.2],
        }
    )

    features = build_wide_features(df, min_support=1)
    feature_types = {row["feature_type"] for row in features}

    assert "bio.status" not in feature_types
    assert "bio.latest_team" not in feature_types
    assert "bio.ras_score_bin" in feature_types


def test_wide_manager_affinity_excludes_structural_draft_order():
    from multi_league.transformations.draft.wide_correlation_miner import (
        _is_league_inefficiency_feature,
        _is_manager_affinity_feature,
    )

    assert not _is_manager_affinity_feature("draft.draft_slot_bin")
    assert not _is_manager_affinity_feature("draft.pick_bin")
    assert not _is_manager_affinity_feature("manager_matchup_prior.win_pct_bin")
    assert not _is_manager_affinity_feature("league.scoring_type")
    assert _is_manager_affinity_feature("bio.ras_score_bin")
    assert _is_manager_affinity_feature("draft.position_percentile_bin")
    assert not _is_league_inefficiency_feature("draft.pick_bin")
    assert not _is_league_inefficiency_feature("draft.round_bin")
    assert _is_league_inefficiency_feature("player_prior.avg_player_lamar_bin")


def test_wide_league_inefficiency_baselines_within_league_year():
    from multi_league.transformations.draft.wide_correlation_miner import _score_league_inefficiencies

    df = pd.DataFrame(
        {
            "db_name": ["league_a", "league_a", "league_b", "league_b"],
            "year": [2025, 2025, 2025, 2025],
            "manager_lamar": [100.0, 100.0, 0.0, 0.0],
            "expected_lamar": [0.0, 0.0, 0.0, 0.0],
            "pick_score": [90.0, 90.0, 10.0, 10.0],
            "position_group": ["RB", "RB", "RB", "RB"],
            "capital_bucket": ["snake_r1_2", "snake_r1_2", "snake_r1_2", "snake_r1_2"],
            "capital_weight": [1.0, 1.0, 1.0, 1.0],
        }
    )
    feature_rows = [
        {"row_index": 0, "feature_type": "league.scoring_type", "feature_value": "ppr"},
        {"row_index": 1, "feature_type": "league.scoring_type", "feature_value": "ppr"},
        {"row_index": 2, "feature_type": "league.scoring_type", "feature_value": "standard"},
        {"row_index": 3, "feature_type": "league.scoring_type", "feature_value": "standard"},
    ]

    result = _score_league_inefficiencies(
        df,
        feature_rows,
        min_picks=2,
        min_years=1,
        min_abs_z=0.1,
        limit=10,
    )

    assert result == []


def test_state_registry_classifies_discovered_cross_table_families():
    from multi_league.transformations.draft.state_registry import classify_feature

    prior = classify_feature("player_prior.league_wide_optimal_count_bin", "2_to_4")
    entity = classify_feature("bio.conference", "Southeastern Conference")
    combine = classify_feature("bio.ras_score_bin", "8_to_10")

    assert prior["state_family"] == "prior_league_production"
    assert entity["state_family"] == "entity_affinity"
    assert combine["state_family"] == "athletic_profile"
