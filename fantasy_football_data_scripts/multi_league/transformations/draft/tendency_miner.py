#!/usr/bin/env python3
"""Draft tendency discovery miner.

This module scans draft picks for manager-level affinities that are stronger
than expected after controlling for year, position, and draft-capital bucket.
It is intentionally a discovery layer: results are candidates for validation
and briefing promotion, not final product copy.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
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
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    make_logger,
    resolve_db_name,
)

log = make_logger("DRAFT-TENDENCY")

MODEL_VERSION = "draft-tendency-v0.2"
RECENCY_HALF_LIFE_YEARS = 3.0

DRAFT_TENDENCY_COLUMNS: dict[str, str] = {
    "db_name": "VARCHAR",
    "scope_type": "VARCHAR",
    "scope_key": "VARCHAR",
    "scope_label": "VARCHAR",
    "feature_type": "VARCHAR",
    "feature_value": "VARCHAR",
    "picks": "INTEGER",
    "years_seen": "INTEGER",
    "earliest_year": "INTEGER",
    "latest_year": "INTEGER",
    "recency_weight": "DOUBLE",
    "positive_years": "INTEGER",
    "negative_years": "INTEGER",
    "repeatability": "DOUBLE",
    "observed_capital": "DOUBLE",
    "expected_capital": "DOUBLE",
    "excess_capital": "DOUBLE",
    "observed_share": "DOUBLE",
    "expected_share": "DOUBLE",
    "lift": "DOUBLE",
    "capital_z_score": "DOUBLE",
    "confidence": "VARCHAR",
    "evidence_level": "VARCHAR",
    "model_version": "VARCHAR",
}


def _q(value: str) -> str:
    """Quote a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _draft_table() -> str:
    return central_table("draft")


def _bio_table() -> str:
    return "___ops.nfl_historical.player_bio"


def _stats_table() -> str:
    return "___ops.nfl_historical.nfl_player_stats_all"


def create_draft_tendency_table(conn) -> None:
    """Create the candidate output table when a caller opts into writes."""
    configure_table_catalog(conn)
    columns_sql = ",\n        ".join(f"{name} {dtype}" for name, dtype in DRAFT_TENDENCY_COLUMNS.items())
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS public;
        CREATE TABLE IF NOT EXISTS {central_table("draft_tendency_candidate")} (
            {columns_sql}
        )
    """)
    for name, dtype in DRAFT_TENDENCY_COLUMNS.items():
        conn.execute(f"ALTER TABLE {central_table('draft_tendency_candidate')} ADD COLUMN IF NOT EXISTS {name} {dtype}")


def build_manager_tendency_sql(
    db_name: str,
    *,
    min_picks: int = 4,
    min_expected_capital: float = 8.0,
    min_abs_z: float = 2.0,
    limit: int = 500,
    model_version: str = MODEL_VERSION,
) -> str:
    """Build capital-weighted manager affinity discovery SQL.

    Expectations are controlled by year, position, and capital bucket. The
    baseline is the same league-year population, so manager signals are framed
    as over/under-selection relative to this league's own market.
    """

    db_lit = _q(db_name)
    model_lit = _q(model_version)
    min_picks = int(min_picks)
    min_expected_capital = float(min_expected_capital)
    min_abs_z = float(min_abs_z)
    limit = int(limit)

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
        d.cost_bucket,
        d.nfl_team,
        d.nfl_team_api,
        d.draft_age,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper
    FROM {_draft_table()} d
    WHERE d.db_name = {db_lit}
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
),
with_capital AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.year) AS max_pick,
        ROW_NUMBER() OVER (
            PARTITION BY b.year
            ORDER BY COALESCE(b.pick, b.round, 9999), b.manager_key, b.NFL_player_id
        ) AS pick_order,
        COUNT(*) OVER (PARTITION BY b.year) AS picks_in_year,
        CASE
            WHEN COALESCE(b.cost, 0) > 0 THEN 'auction'
            ELSE 'snake'
        END AS pick_mode,
        UPPER(COALESCE(NULLIF(b.position, ''), 'UNK')) AS position_group
    FROM draft_base b
    WHERE b.is_keeper = 0
),
prior_stats AS (
    SELECT
        NFL_player_id,
        CAST(year AS INTEGER) + 1 AS draft_year,
        SUM(COALESCE(rushing_yards, 0)) AS prior_rush_yards,
        SUM(COALESCE(carries, 0)) AS prior_carries,
        SUM(COALESCE(targets, 0)) AS prior_targets,
        SUM(COALESCE(receptions, 0)) AS prior_receptions,
        SUM(COALESCE(receiving_yards, 0)) AS prior_receiving_yards,
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
        CASE
            WHEN b.pick_order <= b.picks_in_year * 0.25 THEN 'phase_early'
            WHEN b.pick_order <= b.picks_in_year * 0.65 THEN 'phase_middle'
            ELSE 'phase_late'
        END AS draft_phase,
        CASE
            WHEN (
                SELECT COUNT(*)
                FROM with_capital prev
                WHERE prev.year = b.year
                  AND prev.pick_order BETWEEN b.pick_order - 5 AND b.pick_order - 1
                  AND prev.position_group = b.position_group
            ) >= 3 THEN 'same_position_run'
            WHEN (
                SELECT COUNT(*)
                FROM with_capital prev
                WHERE prev.year = b.year
                  AND prev.pick_order BETWEEN b.pick_order - 5 AND b.pick_order - 1
                  AND prev.position_group = b.position_group
            ) >= 1 THEN 'same_position_recent'
            ELSE 'no_recent_same_position'
        END AS position_run_state,
        UPPER(COALESCE(NULLIF(b.nfl_team, ''), NULLIF(b.nfl_team_api, ''), NULLIF(pb.latest_team, ''))) AS current_nfl_team,
        UPPER(NULLIF(pb.nfl_draft_team, '')) AS nfl_draft_team,
        NULLIF(pb.college, '') AS college,
        NULLIF(pb.conference, '') AS conference,
        CASE
            WHEN pb.rookie_year IS NULL THEN NULL
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 0 THEN 'experience_rookie'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) = 1 THEN 'experience_year_2'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 3 THEN 'experience_year_3_4'
            ELSE 'experience_veteran'
        END AS experience_bucket,
        CASE
            WHEN COALESCE(pb.is_undrafted, 0) = 1 THEN 'undrafted'
            WHEN pb.draft_round IS NULL THEN NULL
            WHEN pb.draft_round <= 1 THEN 'nfl_round_1'
            WHEN pb.draft_round <= 3 THEN 'nfl_round_2_3'
            WHEN pb.draft_round <= 7 THEN 'nfl_round_4_7'
            ELSE 'nfl_other'
        END AS nfl_draft_capital,
        CASE
            WHEN pb.ras_score IS NULL THEN NULL
            WHEN pb.ras_score >= 9 THEN 'ras_9_plus'
            WHEN pb.ras_score >= 8 THEN 'ras_8_9'
            WHEN pb.ras_score >= 6 THEN 'ras_6_8'
            ELSE 'ras_under_6'
        END AS ras_bucket,
        CASE
            WHEN b.draft_age IS NULL OR b.draft_age <= 0 THEN NULL
            WHEN b.draft_age <= 23 THEN 'age_23_under'
            WHEN b.draft_age <= 26 THEN 'age_24_26'
            WHEN b.draft_age <= 29 THEN 'age_27_29'
            ELSE 'age_30_plus'
        END AS age_bucket,
        CASE
            WHEN pb.height IS NULL THEN NULL
            WHEN pb.height >= 77 THEN 'height_77_plus'
            WHEN pb.height >= 73 THEN 'height_73_76'
            WHEN pb.height >= 69 THEN 'height_69_72'
            ELSE 'height_under_69'
        END AS height_bucket,
        CASE
            WHEN pb.weight IS NULL THEN NULL
            WHEN pb.weight >= 250 THEN 'weight_250_plus'
            WHEN pb.weight >= 220 THEN 'weight_220_249'
            WHEN pb.weight >= 200 THEN 'weight_200_219'
            ELSE 'weight_under_200'
        END AS weight_bucket,
        COALESCE(ps.prior_rush_yards, 0) AS prior_rush_yards,
        COALESCE(ps.prior_carries, 0) AS prior_carries,
        COALESCE(ps.prior_targets, 0) AS prior_targets,
        COALESCE(ps.prior_receptions, 0) AS prior_receptions,
        COALESCE(ps.prior_receiving_yards, 0) AS prior_receiving_yards,
        COALESCE(ps.prior_target_share, 0) AS prior_target_share,
        COALESCE(ps.prior_wopr, 0) AS prior_wopr,
        COALESCE(ps.prior_games, 0) AS prior_games
    FROM with_capital b
    LEFT JOIN {_bio_table()} pb ON b.NFL_player_id = pb.NFL_player_id
    LEFT JOIN prior_stats ps ON b.NFL_player_id = ps.NFL_player_id AND b.year = ps.draft_year
),
features AS (
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'current_nfl_team' AS feature_type, current_nfl_team AS feature_value
    FROM enriched WHERE current_nfl_team IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'nfl_draft_team', nfl_draft_team
    FROM enriched WHERE nfl_draft_team IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'college', college
    FROM enriched WHERE college IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'conference', conference
    FROM enriched WHERE conference IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'nfl_draft_capital', nfl_draft_capital
    FROM enriched WHERE nfl_draft_capital IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'ras_bucket', ras_bucket
    FROM enriched WHERE ras_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'age_bucket', age_bucket
    FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'height_bucket', height_bucket
    FROM enriched WHERE height_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'weight_bucket', weight_bucket
    FROM enriched WHERE weight_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'experience_bucket', experience_bucket
    FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'position_age', position_group || '_' || age_bucket
    FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'position_experience', position_group || '_' || experience_bucket
    FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'draft_state', draft_phase || '_' || position_run_state
    FROM enriched
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'QB_mobile'
    FROM enriched WHERE position_group = 'QB' AND (prior_rush_yards >= 350 OR prior_carries >= 60)
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'RB_rookie'
    FROM enriched WHERE position_group = 'RB' AND experience_bucket = 'experience_rookie'
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'RB_pass_catcher'
    FROM enriched WHERE position_group = 'RB' AND (prior_receptions >= 40 OR prior_targets >= 55)
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'RB_workhorse'
    FROM enriched WHERE position_group = 'RB' AND prior_carries >= 200
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'WR_target_earner'
    FROM enriched WHERE position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55)
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'TE_target_earner'
    FROM enriched WHERE position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45)
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'TE_elite_athlete'
    FROM enriched WHERE position_group = 'TE' AND ras_bucket = 'ras_9_plus'
    UNION ALL
    SELECT year, manager_key, manager, position_group, capital_bucket, draft_phase, position_run_state, capital_weight,
           'archetype', 'prior_injury_discount'
    FROM enriched WHERE prior_games BETWEEN 1 AND 8
),
manager_strata AS (
    SELECT
        year, manager_key, ANY_VALUE(manager) AS manager, feature_type,
        position_group, capital_bucket,
        COUNT(*) AS manager_stratum_picks,
        SUM(capital_weight) AS manager_stratum_capital,
        SUM(capital_weight * capital_weight) AS manager_stratum_weight_sq
    FROM features
    GROUP BY year, manager_key, feature_type, position_group, capital_bucket
),
manager_feature AS (
    SELECT
        year, manager_key, ANY_VALUE(manager) AS manager, feature_type, feature_value,
        position_group, capital_bucket,
        COUNT(*) AS picks,
        SUM(capital_weight) AS observed_capital
    FROM features
    GROUP BY year, manager_key, feature_type, feature_value, position_group, capital_bucket
),
league_feature_rates AS (
    SELECT
        year, feature_type, feature_value, position_group, capital_bucket,
        COUNT(*) AS league_picks,
        SUM(capital_weight) AS feature_capital,
        SUM(SUM(capital_weight)) OVER (
            PARTITION BY year, feature_type, position_group, capital_bucket
        ) AS total_capital
    FROM features
    GROUP BY year, feature_type, feature_value, position_group, capital_bucket
),
scored_strata AS (
    SELECT
        mf.manager_key,
        mf.manager,
        mf.feature_type,
        mf.feature_value,
        mf.year,
        mf.picks,
        mf.observed_capital,
        ms.manager_stratum_capital,
        GREATEST(
            (ms.manager_stratum_capital * ms.manager_stratum_capital)
            / NULLIF(ms.manager_stratum_weight_sq, 0),
            1.0
        ) AS effective_n,
        lfr.feature_capital / NULLIF(lfr.total_capital, 0) AS expected_share,
        ms.manager_stratum_capital * (lfr.feature_capital / NULLIF(lfr.total_capital, 0)) AS expected_capital
    FROM manager_feature mf
    JOIN manager_strata ms
      ON mf.year = ms.year
     AND mf.manager_key = ms.manager_key
     AND mf.feature_type = ms.feature_type
     AND mf.position_group = ms.position_group
     AND mf.capital_bucket = ms.capital_bucket
    JOIN league_feature_rates lfr
      ON mf.year = lfr.year
     AND mf.feature_type = lfr.feature_type
     AND mf.feature_value = lfr.feature_value
     AND mf.position_group = lfr.position_group
     AND mf.capital_bucket = lfr.capital_bucket
),
candidate_year AS (
    SELECT
        manager_key,
        ANY_VALUE(manager) AS manager,
        feature_type,
        feature_value,
        year,
        SUM(picks) AS picks,
        SUM(observed_capital) AS observed_capital,
        SUM(expected_capital) AS expected_capital,
        SUM(manager_stratum_capital) AS manager_stratum_capital,
        SUM((observed_capital / NULLIF(manager_stratum_capital, 0)) * effective_n) AS observed_effective,
        SUM(expected_share * effective_n) AS expected_effective,
        SUM(effective_n * expected_share * (1 - expected_share)) AS approx_variance
    FROM scored_strata
    GROUP BY manager_key, feature_type, feature_value, year
),
rollup AS (
    SELECT
        manager_key,
        ANY_VALUE(manager) AS manager,
        feature_type,
        feature_value,
        SUM(picks) AS picks,
        COUNT(DISTINCT year) AS years_seen,
        MIN(year) AS earliest_year,
        MAX(year) AS latest_year,
        SUM(observed_capital * POWER(0.5, ((SELECT MAX(year) FROM candidate_year) - year) / {RECENCY_HALF_LIFE_YEARS}))
            / NULLIF(SUM(observed_capital), 0) AS recency_weight,
        SUM(CASE WHEN observed_capital > expected_capital THEN 1 ELSE 0 END) AS positive_years,
        SUM(CASE WHEN observed_capital < expected_capital THEN 1 ELSE 0 END) AS negative_years,
        SUM(observed_capital) AS observed_capital,
        SUM(expected_capital) AS expected_capital,
        SUM(manager_stratum_capital) AS manager_total_capital,
        SUM(observed_effective) AS observed_effective,
        SUM(expected_effective) AS expected_effective,
        SUM(approx_variance) AS approx_variance
    FROM candidate_year
    GROUP BY manager_key, feature_type, feature_value
),
ranked AS (
    SELECT
        {db_lit} AS db_name,
        'manager' AS scope_type,
        CAST(manager_key AS VARCHAR) AS scope_key,
        CAST(manager AS VARCHAR) AS scope_label,
        CAST(feature_type AS VARCHAR) AS feature_type,
        CAST(feature_value AS VARCHAR) AS feature_value,
        CAST(picks AS INTEGER) AS picks,
        CAST(years_seen AS INTEGER) AS years_seen,
        CAST(earliest_year AS INTEGER) AS earliest_year,
        CAST(latest_year AS INTEGER) AS latest_year,
        ROUND(recency_weight, 4) AS recency_weight,
        CAST(positive_years AS INTEGER) AS positive_years,
        CAST(negative_years AS INTEGER) AS negative_years,
        ROUND(
            CASE
                WHEN observed_capital >= expected_capital THEN positive_years / NULLIF(years_seen, 0)
                ELSE negative_years / NULLIF(years_seen, 0)
            END,
            4
        ) AS repeatability,
        ROUND(observed_capital, 3) AS observed_capital,
        ROUND(expected_capital, 3) AS expected_capital,
        ROUND(observed_capital - expected_capital, 3) AS excess_capital,
        ROUND(observed_capital / NULLIF(manager_total_capital, 0), 4) AS observed_share,
        ROUND(expected_capital / NULLIF(manager_total_capital, 0), 4) AS expected_share,
        ROUND(observed_capital / NULLIF(expected_capital, 0), 4) AS lift,
        ROUND(
            (observed_effective - expected_effective)
            / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0),
            3
        ) AS capital_z_score,
        CASE
            WHEN years_seen >= 3 AND picks >= 8 AND expected_capital >= 20
              AND ABS((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0)) >= 4
            THEN 'high'
            WHEN years_seen >= 2 AND picks >= 5
              AND ABS((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0)) >= 3
            THEN 'medium'
            ELSE 'low'
        END AS confidence,
        CASE
            WHEN years_seen >= 3 AND picks >= 8 AND expected_capital >= 20
              AND ABS((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0)) >= 4
            THEN 'briefing_candidate'
            WHEN years_seen >= 2 AND picks >= 5
              AND ABS((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0)) >= 3
            THEN 'likely'
            ELSE 'explore'
        END AS evidence_level,
        {model_lit} AS model_version
    FROM rollup
    WHERE picks >= {min_picks}
      AND expected_capital >= {min_expected_capital}
)
SELECT *
FROM ranked
WHERE ABS(capital_z_score) >= {min_abs_z}
ORDER BY ABS(capital_z_score) DESC, ABS(excess_capital) DESC
LIMIT {limit}
"""


def run_manager_tendency_miner(
    conn,
    db_name: str,
    *,
    min_picks: int = 4,
    min_expected_capital: float = 8.0,
    min_abs_z: float = 2.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Run the manager tendency discovery query and return dict rows."""
    configure_table_catalog(conn)
    sql = build_manager_tendency_sql(
        db_name,
        min_picks=min_picks,
        min_expected_capital=min_expected_capital,
        min_abs_z=min_abs_z,
        limit=limit,
    )
    result = conn.execute(sql)
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def run_manager_tendency_miner_fly(
    db_name: str,
    *,
    min_picks: int = 4,
    min_expected_capital: float = 8.0,
    min_abs_z: float = 2.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Run the discovery query through the read-only Fly API."""
    from multi_league.core.readers.fly_reader import FlyReader

    sql = build_manager_tendency_sql(
        db_name,
        min_picks=min_picks,
        min_expected_capital=min_expected_capital,
        min_abs_z=min_abs_z,
        limit=limit,
    )
    return FlyReader().query(sql, database="___leagues")


def replace_manager_tendency_candidates(
    conn,
    db_name: str,
    *,
    min_picks: int = 4,
    min_expected_capital: float = 8.0,
    min_abs_z: float = 2.0,
    limit: int = 500,
) -> int:
    """Replace one league's manager tendency candidate rows."""
    configure_table_catalog(conn)
    create_draft_tendency_table(conn)
    select_sql = build_manager_tendency_sql(
        db_name,
        min_picks=min_picks,
        min_expected_capital=min_expected_capital,
        min_abs_z=min_abs_z,
        limit=limit,
    )
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('draft_tendency_candidate')} WHERE db_name = {_q(db_name)} AND scope_type = 'manager'",
        db_name,
        label="draft_tendency_candidate:delete-manager",
    )
    insert_columns = ", ".join(DRAFT_TENDENCY_COLUMNS)
    execute_scoped(
        conn,
        f"INSERT INTO {central_table('draft_tendency_candidate')} ({insert_columns}) {select_sql}",
        db_name,
        label="draft_tendency_candidate:insert-manager",
    )
    return conn.execute(
        f"SELECT COUNT(*) FROM {central_table('draft_tendency_candidate')} WHERE db_name = {_q(db_name)} AND scope_type = 'manager'"
    ).fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Mine capital-weighted draft tendency candidates.")
    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--db", help="League db_name")
    parser.add_argument("--data-dir", default=None, help="Local data dir for DuckDB-backed runs")
    parser.add_argument("--min-picks", type=int, default=4)
    parser.add_argument("--min-expected-capital", type=float, default=8.0)
    parser.add_argument("--min-abs-z", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--write", action="store_true", help="Write candidates to draft_tendency_candidate")
    parser.add_argument("--json", action="store_true", help="Print rows as JSON")
    args = parser.parse_args()

    db_name, _ = resolve_db_name(args)
    if args.write:
        conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
    else:
        conn = None
    try:
        if args.write:
            if conn is None:
                raise RuntimeError("--write requires a local pipeline connection")
            count = replace_manager_tendency_candidates(
                conn,
                db_name,
                min_picks=args.min_picks,
                min_expected_capital=args.min_expected_capital,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )
            log(f"wrote {count} manager tendency candidates for {db_name}")
            return 0

        if args.data_dir:
            conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
            rows = run_manager_tendency_miner(
                conn,
                db_name,
                min_picks=args.min_picks,
                min_expected_capital=args.min_expected_capital,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )
        else:
            rows = run_manager_tendency_miner_fly(
                db_name,
                min_picks=args.min_picks,
                min_expected_capital=args.min_expected_capital,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )
        if args.json:
            print(json.dumps(rows, indent=2, default=str))
        else:
            for row in rows:
                print(
                    f"{row['scope_label']}: {row['feature_type']}={row['feature_value']} "
                    f"lift={row['lift']} z={row['capital_z_score']} "
                    f"picks={row['picks']} years={row['years_seen']} "
                    f"evidence={row['evidence_level']}"
                )
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
