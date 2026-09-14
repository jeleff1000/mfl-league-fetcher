#!/usr/bin/env python3
"""Cross-table draft intelligence miner.

This extends wide draft discovery across league tables. Each draft pick is
joined to draft-time player/bio features, prior player performance in that
league, prior manager draft history, prior manager matchup history, and league
format settings. Same-season and career tables that include the drafted season
are intentionally avoided on the predictor side.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
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
    resolve_db_name,
)
from multi_league.transformations.draft.draft_feature_mining import quote_sql
from multi_league.transformations.draft.wide_correlation_miner import (
    BIO_FEATURE_COLUMNS,
    DRAFT_FEATURE_COLUMNS,
    _qident,
    build_wide_features,
    _score_league_inefficiencies,
    _score_manager_affinities,
)

log = make_logger("DRAFT-XTAB")

MODEL_VERSION = "draft-cross-table-correlation-v0.1"

LEAGUE_FEATURE_COLUMNS = [
    "platform",
    "num_teams",
    "scoring_type",
    "uses_median",
    "draft_type",
    "playoff_teams",
    "regular_season_weeks",
    "waiver_type",
    "league_type",
    "max_keepers",
    "sleeper_best_ball",
    "sleeper_taxi_slots",
    "sleeper_pick_trading",
    "draft_rounds",
    "reversal_round",
    "is_dynasty",
    "scoring_variant",
]


def _draft_table() -> str:
    return central_table("draft")


def _player_season_table() -> str:
    return central_table("player_fantasy_season")


def _draft_manager_season_table() -> str:
    return central_table("draft_manager_season")


def _matchup_season_table() -> str:
    return central_table("matchup_season")


def _league_settings_table() -> str:
    return central_table("league_settings")


def _where_db(db_name: str | None, db_names: list[str] | None = None) -> str:
    if db_name:
        return f"AND d.db_name = {quote_sql(db_name)}"
    if db_names:
        values = ", ".join(quote_sql(name) for name in db_names)
        return f"AND d.db_name IN ({values})"
    return ""


def build_cross_table_base_sql(
    db_name: str | None = None,
    *,
    row_limit: int | None = None,
    db_names: list[str] | None = None,
) -> str:
    """Build a point-in-time draft feature matrix across league tables."""
    draft_select = ",\n        ".join(f"d.{_qident(col)} AS draft__{col}" for col in DRAFT_FEATURE_COLUMNS)
    bio_select = ",\n    ".join(f"pb.{_qident(col)} AS bio__{col}" for col in BIO_FEATURE_COLUMNS)
    league_select = ",\n    ".join(f"ls.{_qident(col)} AS league__{col}" for col in LEAGUE_FEATURE_COLUMNS)
    limit_sql = f"LIMIT {int(row_limit)}" if row_limit else ""

    return f"""
WITH draft_base AS (
    SELECT
        d.db_name,
        d.year,
        COALESCE(d.franchise_id, d.manager) AS manager_key,
        d.manager,
        d.NFL_player_id,
        d.position,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.manager_lamar,
        d.expected_lamar,
        d.pick_score,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper,
        {draft_select}
    FROM {_draft_table()} d
    WHERE d.db_name IS NOT NULL
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
      {_where_db(db_name, db_names)}
),
with_capital AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.db_name, b.year) AS max_pick,
        CASE WHEN COALESCE(b.cost, 0) > 0 THEN 'auction' ELSE 'snake' END AS pick_mode,
        UPPER(COALESCE(NULLIF(b.position, ''), 'UNK')) AS position_group
    FROM draft_base b
    WHERE b.is_keeper = 0
),
player_prev AS (
    SELECT
        b.db_name,
        b.year,
        b.NFL_player_id,
        ANY_VALUE(pfs.fantasy_position) AS player_prev__fantasy_position,
        AVG(pfs.fantasy_points) AS player_prev__fantasy_points,
        AVG(pfs.player_lamar) AS player_prev__player_lamar,
        AVG(pfs.manager_lamar) AS player_prev__manager_lamar,
        AVG(pfs.clutch_equity) AS player_prev__clutch_equity,
        AVG(pfs.games_started) AS player_prev__games_started,
        AVG(pfs.games_rostered) AS player_prev__games_rostered,
        AVG(pfs.optimal_player_count) AS player_prev__optimal_player_count,
        AVG(pfs.league_wide_optimal_count) AS player_prev__league_wide_optimal_count
    FROM with_capital b
    LEFT JOIN {_player_season_table()} pfs
      ON pfs.db_name = b.db_name
     AND pfs.NFL_player_id = b.NFL_player_id
     AND pfs.year = b.year - 1
    GROUP BY b.db_name, b.year, b.NFL_player_id
),
player_prior AS (
    SELECT
        b.db_name,
        b.year,
        b.NFL_player_id,
        COUNT(DISTINCT pfs.year) AS player_prior__league_seasons,
        AVG(pfs.fantasy_points) AS player_prior__avg_fantasy_points,
        AVG(pfs.player_lamar) AS player_prior__avg_player_lamar,
        MAX(pfs.player_lamar) AS player_prior__max_player_lamar,
        AVG(pfs.manager_lamar) AS player_prior__avg_manager_lamar,
        AVG(pfs.clutch_equity) AS player_prior__avg_clutch_equity,
        AVG(pfs.games_started) AS player_prior__avg_games_started,
        AVG(pfs.games_rostered) AS player_prior__avg_games_rostered,
        SUM(pfs.optimal_player_count) AS player_prior__optimal_player_count,
        SUM(pfs.league_wide_optimal_count) AS player_prior__league_wide_optimal_count
    FROM with_capital b
    LEFT JOIN {_player_season_table()} pfs
      ON pfs.db_name = b.db_name
     AND pfs.NFL_player_id = b.NFL_player_id
     AND pfs.year < b.year
    GROUP BY b.db_name, b.year, b.NFL_player_id
),
manager_draft_prior AS (
    SELECT
        b.db_name,
        b.year,
        b.manager_key,
        COUNT(DISTINCT dms.year) AS manager_draft_prior__seasons,
        SUM(dms.picks) AS manager_draft_prior__picks,
        SUM(dms.keeper_picks) / NULLIF(SUM(dms.picks), 0) AS manager_draft_prior__keeper_pick_share,
        AVG(dms.avg_manager_lamar) AS manager_draft_prior__avg_manager_lamar,
        AVG(dms.hit_rate) AS manager_draft_prior__hit_rate,
        SUM(dms.busts) / NULLIF(SUM(dms.picks), 0) AS manager_draft_prior__bust_share,
        SUM(dms.breakouts) / NULLIF(SUM(dms.picks), 0) AS manager_draft_prior__breakout_share,
        AVG(dms.avg_pick_quality_zscore) AS manager_draft_prior__avg_pick_quality_zscore,
        AVG(dms.avg_games_played) AS manager_draft_prior__avg_games_played,
        AVG(dms.avg_lamar_per_dollar) AS manager_draft_prior__avg_lamar_per_dollar,
        AVG(dms.manager_draft_percentile) AS manager_draft_prior__manager_draft_percentile
    FROM with_capital b
    LEFT JOIN {_draft_manager_season_table()} dms
      ON dms.db_name = b.db_name
     AND COALESCE(dms.franchise_id, dms.manager) = b.manager_key
     AND dms.year < b.year
    GROUP BY b.db_name, b.year, b.manager_key
),
manager_matchup_prior AS (
    SELECT
        b.db_name,
        b.year,
        b.manager_key,
        COUNT(DISTINCT ms.year) AS manager_matchup_prior__seasons,
        AVG(ms.win_pct) AS manager_matchup_prior__win_pct,
        AVG(ms.avg_team_points) AS manager_matchup_prior__avg_team_points,
        AVG(ms.avg_margin) AS manager_matchup_prior__avg_margin,
        AVG(ms.std_dev_team_points) AS manager_matchup_prior__std_dev_team_points,
        AVG(ms.close_win_pct) AS manager_matchup_prior__close_win_pct,
        AVG(ms.optimal_efficiency) AS manager_matchup_prior__optimal_efficiency,
        AVG(ms.made_playoffs) AS manager_matchup_prior__made_playoffs_rate,
        AVG(ms.is_champion) AS manager_matchup_prior__championship_rate,
        AVG(ms.wins_vs_shuffle_wins) AS manager_matchup_prior__wins_vs_shuffle_wins,
        AVG(ms.seed_vs_shuffle_seed) AS manager_matchup_prior__seed_vs_shuffle_seed
    FROM with_capital b
    LEFT JOIN {_matchup_season_table()} ms
      ON ms.db_name = b.db_name
     AND COALESCE(ms.franchise_id, ms.manager) = b.manager_key
     AND ms.year < b.year
    GROUP BY b.db_name, b.year, b.manager_key
)
SELECT
    b.db_name,
    b.year,
    b.manager_key,
    b.manager,
    b.NFL_player_id,
    b.position_group,
    CASE
        WHEN b.cost > 0 THEN GREATEST(b.cost, 1)
        ELSE GREATEST(b.max_pick + 1 - COALESCE(b.pick, b.round, b.max_pick), 1)
    END AS capital_weight,
    CASE
        WHEN b.cost > 0 THEN
            CASE
                WHEN b.cost >= 50 THEN 'auction_50_plus'
                WHEN b.cost >= 30 THEN 'auction_30_49'
                WHEN b.cost >= 16 THEN 'auction_16_29'
                WHEN b.cost >= 6 THEN 'auction_6_15'
                ELSE 'auction_1_5'
            END
        ELSE
            CASE
                WHEN COALESCE(b.round, 99) <= 2 THEN 'snake_r1_2'
                WHEN COALESCE(b.round, 99) <= 5 THEN 'snake_r3_5'
                WHEN COALESCE(b.round, 99) <= 9 THEN 'snake_r6_9'
                ELSE 'snake_r10_plus'
            END
    END AS capital_bucket,
    b.manager_lamar,
    b.expected_lamar,
    b.pick_score,
    {", ".join(f"b.draft__{col}" for col in DRAFT_FEATURE_COLUMNS)},
    {bio_select},
    pp.* EXCLUDE (db_name, year, NFL_player_id),
    pc.* EXCLUDE (db_name, year, NFL_player_id),
    mdp.* EXCLUDE (db_name, year, manager_key),
    mmp.* EXCLUDE (db_name, year, manager_key),
    {league_select}
FROM with_capital b
LEFT JOIN ___ops.nfl_historical.player_bio pb ON b.NFL_player_id = pb.NFL_player_id
LEFT JOIN player_prev pp ON pp.db_name = b.db_name AND pp.year = b.year AND pp.NFL_player_id = b.NFL_player_id
LEFT JOIN player_prior pc ON pc.db_name = b.db_name AND pc.year = b.year AND pc.NFL_player_id = b.NFL_player_id
LEFT JOIN manager_draft_prior mdp ON mdp.db_name = b.db_name AND mdp.year = b.year AND mdp.manager_key = b.manager_key
LEFT JOIN manager_matchup_prior mmp ON mmp.db_name = b.db_name AND mmp.year = b.year AND mmp.manager_key = b.manager_key
LEFT JOIN {_league_settings_table()} ls ON ls.db_name = b.db_name AND ls.year = b.year
{limit_sql}
"""


def run_cross_table_miner(
    conn,
    db_name: str | None = None,
    *,
    row_limit: int | None = None,
    min_support: int = 6,
    min_picks: int = 8,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 200,
) -> dict[str, list[dict[str, Any]]]:
    """Run cross-table manager and league-wide discovery."""

    configure_table_catalog(conn)
    df = conn.execute(build_cross_table_base_sql(db_name, row_limit=row_limit)).fetchdf()
    if df.empty:
        return {"manager_tendencies": [], "league_inefficiencies": []}
    features = build_wide_features(df, min_support=min_support)
    return {
        "manager_tendencies": _score_manager_affinities(
            df,
            features,
            min_picks=min_picks,
            min_abs_z=min_abs_z,
            limit=limit,
        ),
        "league_inefficiencies": _score_league_inefficiencies(
            df,
            features,
            min_picks=min_picks,
            min_years=min_years,
            min_abs_z=min_abs_z,
            limit=limit,
        ),
    }


def run_cross_table_miner_fly(
    db_name: str | None = None,
    *,
    row_limit: int | None = None,
    min_support: int = 6,
    min_picks: int = 8,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 200,
) -> dict[str, list[dict[str, Any]]]:
    """Run cross-table discovery through the read-only Fly API."""
    import pandas as pd
    from multi_league.core.readers.fly_reader import FlyReader

    rows = FlyReader().query(build_cross_table_base_sql(db_name, row_limit=row_limit), database="___leagues")
    df = pd.DataFrame(rows)
    if df.empty:
        return {"manager_tendencies": [], "league_inefficiencies": []}
    features = build_wide_features(df, min_support=min_support)
    return {
        "manager_tendencies": _score_manager_affinities(
            df,
            features,
            min_picks=min_picks,
            min_abs_z=min_abs_z,
            limit=limit,
        ),
        "league_inefficiencies": _score_league_inefficiencies(
            df,
            features,
            min_picks=min_picks,
            min_years=min_years,
            min_abs_z=min_abs_z,
            limit=limit,
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Mine cross-table draft correlations.")
    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--db", help="Optional league db_name. Omit with --fleet for fleet sample.")
    parser.add_argument("--fleet", action="store_true", help="Run across all leagues instead of one db")
    parser.add_argument("--data-dir", default=None, help="Local data dir for DuckDB-backed runs")
    parser.add_argument("--row-limit", type=int)
    parser.add_argument("--min-support", type=int, default=6)
    parser.add_argument("--min-picks", type=int, default=8)
    parser.add_argument("--min-years", type=int, default=2)
    parser.add_argument("--min-abs-z", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    db_name = None if args.fleet else resolve_db_name(args)[0]
    conn = None
    try:
        if args.data_dir:
            if db_name is None:
                raise RuntimeError("--data-dir mode requires a single --db")
            conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
            result = run_cross_table_miner(
                conn,
                db_name,
                row_limit=args.row_limit,
                min_support=args.min_support,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )
        else:
            result = run_cross_table_miner_fly(
                db_name,
                row_limit=args.row_limit,
                min_support=args.min_support,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )

        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print(
                f"manager_tendencies={len(result['manager_tendencies'])} "
                f"league_inefficiencies={len(result['league_inefficiencies'])}"
            )
            for row in result["manager_tendencies"][:10]:
                print(
                    f"T {row['scope_label']} {row['feature_type']}={row['feature_value']} "
                    f"z={row['capital_z_score']} repeat={row['repeatability']} picks={row['picks']}"
                )
            for row in result["league_inefficiencies"][:10]:
                print(
                    f"I {row['feature_type']}={row['feature_value']} "
                    f"z={row['value_z_score']} repeat={row['repeatability']} delta={row['excess_residual']}"
                )
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
