#!/usr/bin/env python3
"""Regularized manager clustering for draft behavior.

KMeans is useful here only after we tame the input space. This module builds
capital-weighted manager vectors, shrinks noisy small samples toward the global
mean, standardizes features, reduces correlated dimensions with PCA, and then
selects ``k`` using an elbow diagnostic.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path

setup_module_path()

from multi_league.core.db_utils import get_pipeline_connection
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    make_logger,
)

log = make_logger("DRAFT-CLUSTER")

MODEL_VERSION = "draft-manager-cluster-v0.1"

OUTCOME_FEATURE_COLUMNS = {"avg_lamar_residual", "avg_pick_score"}


def _q(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _draft_table() -> str:
    return central_table("draft")


def _bio_table() -> str:
    return "___ops.nfl_historical.player_bio"


def _stats_table() -> str:
    return "___ops.nfl_historical.nfl_player_stats_all"


def build_manager_feature_sql(
    db_name: str | None = None,
    *,
    min_picks: int = 20,
    limit: int | None = None,
) -> str:
    """Build a SQL feature matrix for manager clustering.

    Features are mostly capital shares so they compare managers with different
    league sizes and draft histories. Heavy correlation is expected; PCA handles
    that before KMeans.
    """

    where_db = f"AND d.db_name = {_q(db_name)}" if db_name else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    min_picks = int(min_picks)

    return f"""
WITH draft_base AS (
    SELECT
        d.db_name,
        d.year,
        COALESCE(d.franchise_id, d.manager) AS manager_key,
        d.manager,
        d.NFL_player_id,
        UPPER(COALESCE(NULLIF(d.position, ''), 'UNK')) AS position_group,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.manager_lamar,
        d.expected_lamar,
        d.pick_score,
        d.draft_age,
        d.draft_age_zscore,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper
    FROM {_draft_table()} d
    WHERE d.db_name IS NOT NULL
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
      {where_db}
),
with_capital AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.db_name, b.year) AS max_pick,
        CASE WHEN COALESCE(b.cost, 0) > 0 THEN 'auction' ELSE 'snake' END AS draft_mode
    FROM draft_base b
),
prior_stats AS (
    SELECT
        NFL_player_id,
        CAST(year AS INTEGER) + 1 AS draft_year,
        SUM(COALESCE(rushing_yards, 0)) AS prior_rush_yards,
        SUM(COALESCE(carries, 0)) AS prior_carries,
        SUM(COALESCE(targets, 0)) AS prior_targets,
        SUM(COALESCE(receptions, 0)) AS prior_receptions,
        MAX(COALESCE(target_share, 0)) AS prior_target_share,
        MAX(COALESCE(wopr, 0)) AS prior_wopr,
        COUNT(DISTINCT week) AS prior_games
    FROM {_stats_table()}
    WHERE NFL_player_id IS NOT NULL
      AND year IS NOT NULL
      AND (season_type IS NULL OR season_type = 'REG')
    GROUP BY NFL_player_id, CAST(year AS INTEGER) + 1
),
enriched AS (
    SELECT
        b.*,
        CASE
            WHEN b.cost > 0 THEN GREATEST(b.cost, 1)
            ELSE GREATEST(b.max_pick + 1 - COALESCE(b.pick, b.round, b.max_pick), 1)
        END AS capital_weight,
        CASE
            WHEN pb.rookie_year IS NULL THEN NULL
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 0 THEN 'rookie'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) = 1 THEN 'year_2'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 3 THEN 'year_3_4'
            ELSE 'veteran'
        END AS experience_bucket,
        CASE
            WHEN COALESCE(pb.is_undrafted, 0) = 1 THEN 'undrafted'
            WHEN pb.draft_round IS NULL THEN NULL
            WHEN pb.draft_round <= 1 THEN 'round_1'
            WHEN pb.draft_round <= 3 THEN 'round_2_3'
            WHEN pb.draft_round <= 7 THEN 'round_4_7'
            ELSE 'other'
        END AS nfl_draft_capital,
        CASE
            WHEN pb.ras_score IS NULL THEN NULL
            WHEN pb.ras_score >= 9 THEN 'ras_9_plus'
            WHEN pb.ras_score >= 8 THEN 'ras_8_9'
            WHEN pb.ras_score >= 6 THEN 'ras_6_8'
            ELSE 'ras_under_6'
        END AS ras_bucket,
        COALESCE(ps.prior_rush_yards, 0) AS prior_rush_yards,
        COALESCE(ps.prior_carries, 0) AS prior_carries,
        COALESCE(ps.prior_targets, 0) AS prior_targets,
        COALESCE(ps.prior_receptions, 0) AS prior_receptions,
        COALESCE(ps.prior_target_share, 0) AS prior_target_share,
        COALESCE(ps.prior_wopr, 0) AS prior_wopr,
        COALESCE(ps.prior_games, 0) AS prior_games
    FROM with_capital b
    LEFT JOIN {_bio_table()} pb ON b.NFL_player_id = pb.NFL_player_id
    LEFT JOIN prior_stats ps ON b.NFL_player_id = ps.NFL_player_id AND b.year = ps.draft_year
),
manager_rollup AS (
    SELECT
        db_name,
        manager_key,
        ANY_VALUE(manager) AS manager,
        COUNT(*) AS total_picks,
        COUNT(DISTINCT year) AS years_active,
        SUM(capital_weight) AS total_capital,
        AVG(CASE WHEN draft_mode = 'auction' THEN 1.0 ELSE 0.0 END) AS auction_pick_share,
        AVG(is_keeper) AS keeper_pick_share,
        AVG(manager_lamar - expected_lamar) AS avg_lamar_residual,
        AVG(pick_score) AS avg_pick_score,
        SUM(CASE WHEN position_group = 'QB' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_qb,
        SUM(CASE WHEN position_group = 'RB' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_rb,
        SUM(CASE WHEN position_group = 'WR' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_wr,
        SUM(CASE WHEN position_group = 'TE' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_te,
        SUM(CASE WHEN position_group IN ('K', 'DEF') THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_special,
        SUM(CASE WHEN draft_age IS NOT NULL AND draft_age <= 23 THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN draft_age IS NOT NULL THEN capital_weight ELSE 0 END), 0) AS cap_age_23_under,
        SUM(CASE WHEN draft_age IS NOT NULL AND draft_age >= 30 THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN draft_age IS NOT NULL THEN capital_weight ELSE 0 END), 0) AS cap_age_30_plus,
        AVG(draft_age_zscore) FILTER (WHERE draft_age_zscore IS NOT NULL) AS avg_age_z,
        SUM(CASE WHEN experience_bucket = 'rookie' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_rookie,
        SUM(CASE WHEN experience_bucket = 'veteran' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_veteran,
        SUM(CASE WHEN nfl_draft_capital = 'round_1' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_nfl_round_1,
        SUM(CASE WHEN nfl_draft_capital = 'undrafted' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_undrafted,
        SUM(CASE WHEN ras_bucket = 'ras_9_plus' THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_elite_ras,
        SUM(CASE WHEN position_group = 'QB' AND (prior_rush_yards >= 350 OR prior_carries >= 60) THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN position_group = 'QB' THEN capital_weight ELSE 0 END), 0) AS qb_mobile_share,
        SUM(CASE WHEN position_group = 'RB' AND experience_bucket = 'rookie' THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN position_group = 'RB' THEN capital_weight ELSE 0 END), 0) AS rb_rookie_share,
        SUM(CASE WHEN position_group = 'RB' AND (prior_receptions >= 40 OR prior_targets >= 55) THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN position_group = 'RB' THEN capital_weight ELSE 0 END), 0) AS rb_pass_catcher_share,
        SUM(CASE WHEN position_group = 'RB' AND prior_carries >= 200 THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN position_group = 'RB' THEN capital_weight ELSE 0 END), 0) AS rb_workhorse_share,
        SUM(CASE WHEN position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55) THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN position_group = 'WR' THEN capital_weight ELSE 0 END), 0) AS wr_target_earner_share,
        SUM(CASE WHEN position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45) THEN capital_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN position_group = 'TE' THEN capital_weight ELSE 0 END), 0) AS te_target_earner_share,
        SUM(CASE WHEN prior_games BETWEEN 1 AND 8 THEN capital_weight ELSE 0 END) / NULLIF(SUM(capital_weight), 0) AS cap_prior_injury_discount
    FROM enriched
    GROUP BY db_name, manager_key
)
SELECT *
FROM manager_rollup
WHERE total_picks >= {min_picks}
ORDER BY db_name, manager
{limit_sql}
"""


def _feature_columns(df) -> list[str]:
    return [
        col
        for col in df.columns
        if col
        not in {
            "db_name",
            "manager_key",
            "manager",
            "total_picks",
            "years_active",
            "total_capital",
            *OUTCOME_FEATURE_COLUMNS,
        }
    ]


def choose_k_elbow(k_values: list[int], inertias: list[float]) -> int:
    """Pick k by max distance from the line between first/last inertia."""
    import numpy as np

    if not k_values:
        raise ValueError("k_values cannot be empty")
    if len(k_values) == 1:
        return int(k_values[0])

    x = np.asarray(k_values, dtype=float)
    y = np.asarray(inertias, dtype=float)
    x = (x - x.min()) / max(x.max() - x.min(), 1e-9)
    y = (y - y.min()) / max(y.max() - y.min(), 1e-9)
    start = np.array([x[0], y[0]])
    end = np.array([x[-1], y[-1]])
    line = end - start
    line_norm = np.linalg.norm(line)
    if line_norm == 0:
        return int(k_values[0])
    points = np.column_stack([x, y])
    distances = np.abs(np.cross(line, start - points)) / line_norm
    return int(k_values[int(np.argmax(distances))])


def regularize_feature_matrix(df, feature_cols: list[str], *, shrinkage_k: float = 40.0):
    """Shrink small-sample managers toward global means before scaling."""
    import numpy as np
    from sklearn.preprocessing import StandardScaler

    if not feature_cols:
        raise ValueError("No feature columns available for clustering")

    raw = df[feature_cols].fillna(0).astype(float).to_numpy()
    weights = df["total_picks"].fillna(0).astype(float).clip(lower=1).to_numpy()
    global_mean = np.average(raw, axis=0, weights=weights)
    reliability = weights / (weights + float(shrinkage_k))
    shrunk = raw * reliability[:, None] + global_mean[None, :] * (1.0 - reliability[:, None])
    scaled = StandardScaler().fit_transform(shrunk)
    return scaled, {
        "global_mean": dict(zip(feature_cols, global_mean.tolist())),
        "avg_reliability": float(np.mean(reliability)),
        "min_reliability": float(np.min(reliability)),
        "max_reliability": float(np.max(reliability)),
    }


def reduce_correlated_features(x, *, variance: float = 0.9, max_components: int = 10):
    """Use PCA as regularization against correlated noisy feature families."""
    import numpy as np
    from sklearn.decomposition import PCA

    n_samples, n_features = x.shape
    if n_samples < 3 or n_features < 2:
        return x, {"components": int(n_features), "explained_variance": 1.0}

    max_possible = max(1, min(max_components, n_samples - 1, n_features))
    pca = PCA(n_components=max_possible, random_state=42)
    transformed = pca.fit_transform(x)
    cumulative = np.cumsum(pca.explained_variance_ratio_)
    keep = int(np.searchsorted(cumulative, variance) + 1)
    keep = max(1, min(keep, max_possible))
    return transformed[:, :keep], {
        "components": keep,
        "explained_variance": float(cumulative[keep - 1]),
        "all_explained_variance": pca.explained_variance_ratio_.tolist(),
    }


def fit_manager_clusters(
    df,
    *,
    min_k: int = 2,
    max_k: int = 10,
    shrinkage_k: float = 40.0,
    pca_variance: float = 0.9,
    silhouette_sample_size: int = 2000,
    random_state: int = 42,
) -> dict[str, Any]:
    """Fit regularized KMeans manager clusters and return diagnostics."""
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    if len(df) < 2:
        raise ValueError("Need at least two managers to cluster")

    feature_cols = _feature_columns(df)
    scaled, shrinkage_info = regularize_feature_matrix(df, feature_cols, shrinkage_k=shrinkage_k)
    x, pca_info = reduce_correlated_features(scaled, variance=pca_variance)

    upper_k = min(int(max_k), len(df) - 1)
    lower_k = min(int(min_k), upper_k)
    k_values = list(range(lower_k, upper_k + 1))
    diagnostics: list[dict[str, Any]] = []
    models: dict[int, KMeans] = {}

    for k in k_values:
        model = KMeans(n_clusters=k, random_state=random_state, n_init=25)
        labels = model.fit_predict(x)
        inertia = float(model.inertia_)
        silhouette = (
            float(
                silhouette_score(
                    x,
                    labels,
                    sample_size=min(int(silhouette_sample_size), len(df)),
                    random_state=random_state,
                )
            )
            if 1 < k < len(df)
            else None
        )
        diagnostics.append({"k": k, "inertia": inertia, "silhouette": silhouette})
        models[k] = model

    chosen_k = choose_k_elbow(k_values, [row["inertia"] for row in diagnostics])
    chosen_model = models[chosen_k]
    assignments = df[["db_name", "manager_key", "manager", "total_picks", "years_active"]].copy()
    assignments["cluster"] = chosen_model.labels_.astype(int)
    assignments["model_version"] = MODEL_VERSION

    global_means = df[feature_cols].fillna(0).mean()
    outcome_cols = [col for col in OUTCOME_FEATURE_COLUMNS if col in df.columns]
    profile_rows = []
    for cluster_id, group in assignments.groupby("cluster"):
        source = df.loc[group.index, feature_cols]
        means = source.fillna(0).mean()
        deltas = (means - global_means).sort_values(key=lambda series: series.abs(), ascending=False)
        outcome_source = df.loc[group.index, outcome_cols] if outcome_cols else None
        profile_rows.append(
            {
                "cluster": int(cluster_id),
                "manager_count": int(len(group)),
                "outcomes": {col: float(outcome_source[col].fillna(0).mean()) for col in outcome_cols}
                if outcome_source is not None
                else {},
                "top_features": [
                    {
                        "feature": str(name),
                        "mean": float(means[name]),
                        "global_mean": float(global_means[name]),
                        "delta": float(value),
                        "lift": float(means[name] / global_means[name]) if float(global_means[name]) else None,
                    }
                    for name, value in deltas.head(8).items()
                ],
            }
        )

    return {
        "chosen_k": int(chosen_k),
        "diagnostics": diagnostics,
        "feature_count": len(feature_cols),
        "shrinkage": shrinkage_info,
        "pca": pca_info,
        "assignments": assignments.to_dict("records"),
        "profiles": profile_rows,
    }


def load_manager_features(conn, db_name: str | None = None, *, min_picks: int = 20, limit: int | None = None):
    import pandas as pd

    configure_table_catalog(conn)
    rows = conn.execute(build_manager_feature_sql(db_name, min_picks=min_picks, limit=limit)).fetchall()
    cols = [desc[0] for desc in conn.description]
    return pd.DataFrame(rows, columns=cols)


def load_manager_features_fly(db_name: str | None = None, *, min_picks: int = 20, limit: int | None = None):
    import pandas as pd
    from multi_league.core.readers.fly_reader import FlyReader

    rows = FlyReader().query(
        build_manager_feature_sql(db_name, min_picks=min_picks, limit=limit), database="___leagues"
    )
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Cluster draft managers using regularized KMeans.")
    parser.add_argument("--db", help="Optional single league db_name. Omit to cluster all managers.")
    parser.add_argument("--data-dir", help="Use local DuckDB instead of Fly read API")
    parser.add_argument("--min-picks", type=int, default=20)
    parser.add_argument("--min-k", type=int, default=2)
    parser.add_argument("--max-k", type=int, default=10)
    parser.add_argument("--shrinkage-k", type=float, default=40.0)
    parser.add_argument("--pca-variance", type=float, default=0.9)
    parser.add_argument("--silhouette-sample-size", type=int, default=2000)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.data_dir:
        if not args.db:
            raise RuntimeError("--data-dir mode requires --db because local files are per league")
        conn = get_pipeline_connection(args.db, data_dir=args.data_dir, qualified=True)
        try:
            df = load_manager_features(conn, args.db, min_picks=args.min_picks, limit=args.limit)
        finally:
            conn.close()
    else:
        df = load_manager_features_fly(args.db, min_picks=args.min_picks, limit=args.limit)

    result = fit_manager_clusters(
        df,
        min_k=args.min_k,
        max_k=args.max_k,
        shrinkage_k=args.shrinkage_k,
        pca_variance=args.pca_variance,
        silhouette_sample_size=args.silhouette_sample_size,
    )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"chosen_k={result['chosen_k']} managers={len(result['assignments'])} features={result['feature_count']}")
        print("diagnostics:")
        for row in result["diagnostics"]:
            print(f"  k={row['k']} inertia={row['inertia']:.2f} silhouette={row['silhouette']}")
        print("clusters:")
        for profile in result["profiles"]:
            top = ", ".join(item["feature"] for item in profile["top_features"][:4])
            print(f"  cluster={profile['cluster']} managers={profile['manager_count']} top={top}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
