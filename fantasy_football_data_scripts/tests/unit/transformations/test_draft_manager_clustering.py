import sys
from pathlib import Path

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_choose_k_elbow_detects_bend():
    from multi_league.transformations.draft.manager_clustering import choose_k_elbow

    assert choose_k_elbow([2, 3, 4, 5, 6], [100.0, 55.0, 40.0, 34.0, 31.0]) == 3


def test_regularization_shrinks_small_samples_more():
    from multi_league.transformations.draft.manager_clustering import regularize_feature_matrix

    df = pd.DataFrame(
        {
            "total_picks": [5, 200],
            "cap_qb": [1.0, 0.0],
            "cap_rb": [0.0, 1.0],
        }
    )

    _, info = regularize_feature_matrix(df, ["cap_qb", "cap_rb"], shrinkage_k=40)

    assert info["min_reliability"] < info["max_reliability"]
    assert info["min_reliability"] < 0.2
    assert info["max_reliability"] > 0.8


def test_fit_manager_clusters_regularized_kmeans():
    from multi_league.transformations.draft.manager_clustering import fit_manager_clusters

    df = pd.DataFrame(
        [
            {
                "db_name": "a",
                "manager_key": "a1",
                "manager": "A1",
                "total_picks": 80,
                "years_active": 5,
                "total_capital": 100,
                "cap_qb": 0.7,
                "cap_rb": 0.1,
                "cap_wr": 0.1,
                "qb_mobile_share": 0.9,
            },
            {
                "db_name": "a",
                "manager_key": "a2",
                "manager": "A2",
                "total_picks": 70,
                "years_active": 5,
                "total_capital": 100,
                "cap_qb": 0.65,
                "cap_rb": 0.15,
                "cap_wr": 0.1,
                "qb_mobile_share": 0.8,
            },
            {
                "db_name": "b",
                "manager_key": "b1",
                "manager": "B1",
                "total_picks": 75,
                "years_active": 5,
                "total_capital": 100,
                "cap_qb": 0.1,
                "cap_rb": 0.7,
                "cap_wr": 0.1,
                "qb_mobile_share": 0.0,
            },
            {
                "db_name": "b",
                "manager_key": "b2",
                "manager": "B2",
                "total_picks": 75,
                "years_active": 5,
                "total_capital": 100,
                "cap_qb": 0.1,
                "cap_rb": 0.65,
                "cap_wr": 0.15,
                "qb_mobile_share": 0.0,
            },
            {
                "db_name": "c",
                "manager_key": "c1",
                "manager": "C1",
                "total_picks": 75,
                "years_active": 5,
                "total_capital": 100,
                "cap_qb": 0.1,
                "cap_rb": 0.1,
                "cap_wr": 0.7,
                "qb_mobile_share": 0.0,
            },
            {
                "db_name": "c",
                "manager_key": "c2",
                "manager": "C2",
                "total_picks": 75,
                "years_active": 5,
                "total_capital": 100,
                "cap_qb": 0.15,
                "cap_rb": 0.1,
                "cap_wr": 0.65,
                "qb_mobile_share": 0.0,
            },
        ]
    )

    result = fit_manager_clusters(df, min_k=2, max_k=5, shrinkage_k=10, pca_variance=0.95)

    assert 2 <= result["chosen_k"] <= 5
    assert len(result["assignments"]) == 6
    assert result["pca"]["components"] >= 1
    assert result["diagnostics"]
