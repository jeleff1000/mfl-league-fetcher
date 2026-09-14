#!/usr/bin/env python3
"""League-wide draft inefficiency discovery miner.

This miner looks for draft-time features that repeatedly beat or lag the
league's own market after controlling for year, position, and draft-capital
bucket. It is discovery-only: results should pass the promotion gate before any
briefing or state-machine behavior is wired to the product.
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
from multi_league.transformations.draft.draft_feature_mining import (
    build_draft_feature_ctes,
    quote_sql,
)

log = make_logger("DRAFT-INEFF")

MODEL_VERSION = "draft-league-inefficiency-v0.2"
RECENCY_HALF_LIFE_YEARS = 3.0

DRAFT_INEFFICIENCY_COLUMNS: dict[str, str] = {
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
    "observed_residual": "DOUBLE",
    "expected_residual": "DOUBLE",
    "excess_residual": "DOUBLE",
    "observed_pick_score": "DOUBLE",
    "expected_pick_score": "DOUBLE",
    "pick_score_delta": "DOUBLE",
    "value_z_score": "DOUBLE",
    "confidence": "VARCHAR",
    "evidence_level": "VARCHAR",
    "model_version": "VARCHAR",
}


def create_draft_inefficiency_table(conn) -> None:
    """Create or migrate the league-wide inefficiency candidate table."""
    configure_table_catalog(conn)
    columns_sql = ",\n        ".join(f"{name} {dtype}" for name, dtype in DRAFT_INEFFICIENCY_COLUMNS.items())
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS public;
        CREATE TABLE IF NOT EXISTS {central_table("draft_inefficiency_candidate")} (
            {columns_sql}
        )
    """)
    for name, dtype in DRAFT_INEFFICIENCY_COLUMNS.items():
        conn.execute(
            f"ALTER TABLE {central_table('draft_inefficiency_candidate')} " f"ADD COLUMN IF NOT EXISTS {name} {dtype}"
        )


def build_league_inefficiency_sql(
    db_name: str,
    *,
    min_picks: int = 8,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 500,
    model_version: str = MODEL_VERSION,
) -> str:
    """Build capital-weighted league inefficiency discovery SQL."""
    db_lit = quote_sql(db_name)
    model_lit = quote_sql(model_version)
    feature_ctes = build_draft_feature_ctes(db_name)
    min_picks = int(min_picks)
    min_years = int(min_years)
    min_abs_z = float(min_abs_z)
    limit = int(limit)

    return f"""
{feature_ctes},
pick_values AS (
    SELECT
        *,
        CAST(manager_lamar AS DOUBLE) - CAST(expected_lamar AS DOUBLE) AS value_residual,
        CAST(pick_score AS DOUBLE) AS pick_score_value
    FROM features
    WHERE manager_lamar IS NOT NULL
      AND expected_lamar IS NOT NULL
      AND capital_weight > 0
),
baseline AS (
    SELECT
        year,
        feature_type,
        position_group,
        capital_bucket,
        SUM(capital_weight * value_residual) / NULLIF(SUM(capital_weight), 0) AS expected_residual,
        GREATEST(
            SUM(capital_weight * value_residual * value_residual) / NULLIF(SUM(capital_weight), 0)
            - POWER(SUM(capital_weight * value_residual) / NULLIF(SUM(capital_weight), 0), 2),
            0.01
        ) AS residual_variance,
        SUM(capital_weight * pick_score_value) / NULLIF(SUM(capital_weight), 0) AS expected_pick_score
    FROM pick_values
    GROUP BY year, feature_type, position_group, capital_bucket
),
scored_picks AS (
    SELECT
        p.*,
        b.expected_residual,
        b.residual_variance,
        b.expected_pick_score
    FROM pick_values p
    JOIN baseline b
      ON p.year = b.year
     AND p.feature_type = b.feature_type
     AND p.position_group = b.position_group
     AND p.capital_bucket = b.capital_bucket
),
candidate_year AS (
    SELECT
        feature_type,
        feature_value,
        year,
        COUNT(*) AS picks,
        SUM(capital_weight) AS observed_capital,
        SUM(capital_weight * value_residual) / NULLIF(SUM(capital_weight), 0) AS observed_residual,
        SUM(capital_weight * expected_residual) / NULLIF(SUM(capital_weight), 0) AS expected_residual,
        SUM(capital_weight * pick_score_value) / NULLIF(SUM(CASE WHEN pick_score_value IS NOT NULL THEN capital_weight ELSE 0 END), 0) AS observed_pick_score,
        SUM(capital_weight * expected_pick_score) / NULLIF(SUM(CASE WHEN expected_pick_score IS NOT NULL THEN capital_weight ELSE 0 END), 0) AS expected_pick_score,
        SUM(capital_weight * (value_residual - expected_residual)) / NULLIF(SUM(capital_weight), 0) AS excess_residual,
        SUM(capital_weight * capital_weight * residual_variance) / POWER(NULLIF(SUM(capital_weight), 0), 2) AS mean_variance
    FROM scored_picks
    GROUP BY feature_type, feature_value, year
),
rollup AS (
    SELECT
        feature_type,
        feature_value,
        SUM(picks) AS picks,
        COUNT(DISTINCT year) AS years_seen,
        MIN(year) AS earliest_year,
        MAX(year) AS latest_year,
        SUM(observed_capital * POWER(0.5, ((SELECT MAX(year) FROM candidate_year) - year) / {RECENCY_HALF_LIFE_YEARS}))
            / NULLIF(SUM(observed_capital), 0) AS recency_weight,
        SUM(CASE WHEN excess_residual > 0 THEN 1 ELSE 0 END) AS positive_years,
        SUM(CASE WHEN excess_residual < 0 THEN 1 ELSE 0 END) AS negative_years,
        SUM(observed_capital) AS observed_capital,
        SUM(observed_capital * observed_residual) / NULLIF(SUM(observed_capital), 0) AS observed_residual,
        SUM(observed_capital * expected_residual) / NULLIF(SUM(observed_capital), 0) AS expected_residual,
        SUM(observed_capital * observed_pick_score) / NULLIF(SUM(CASE WHEN observed_pick_score IS NOT NULL THEN observed_capital ELSE 0 END), 0) AS observed_pick_score,
        SUM(observed_capital * expected_pick_score) / NULLIF(SUM(CASE WHEN expected_pick_score IS NOT NULL THEN observed_capital ELSE 0 END), 0) AS expected_pick_score,
        SUM(observed_capital * excess_residual) / NULLIF(SUM(observed_capital), 0) AS excess_residual,
        SUM(mean_variance * observed_capital * observed_capital) / POWER(NULLIF(SUM(observed_capital), 0), 2) AS rollup_variance
    FROM candidate_year
    GROUP BY feature_type, feature_value
),
ranked AS (
    SELECT
        {db_lit} AS db_name,
        'league_inefficiency' AS scope_type,
        {db_lit} AS scope_key,
        {db_lit} AS scope_label,
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
                WHEN excess_residual >= 0 THEN positive_years / NULLIF(years_seen, 0)
                ELSE negative_years / NULLIF(years_seen, 0)
            END,
            4
        ) AS repeatability,
        ROUND(observed_capital, 3) AS observed_capital,
        ROUND(observed_residual, 4) AS observed_residual,
        ROUND(expected_residual, 4) AS expected_residual,
        ROUND(excess_residual, 4) AS excess_residual,
        ROUND(observed_pick_score, 3) AS observed_pick_score,
        ROUND(expected_pick_score, 3) AS expected_pick_score,
        ROUND(observed_pick_score - expected_pick_score, 3) AS pick_score_delta,
        ROUND(excess_residual / NULLIF(SQRT(GREATEST(rollup_variance, 0.01)), 0), 3) AS value_z_score,
        CASE
            WHEN years_seen >= 3 AND picks >= 12
              AND ABS(excess_residual / NULLIF(SQRT(GREATEST(rollup_variance, 0.01)), 0)) >= 4
            THEN 'high'
            WHEN years_seen >= 2 AND picks >= 8
              AND ABS(excess_residual / NULLIF(SQRT(GREATEST(rollup_variance, 0.01)), 0)) >= 3
            THEN 'medium'
            ELSE 'low'
        END AS confidence,
        CASE
            WHEN years_seen >= 3 AND picks >= 12
              AND ABS(excess_residual / NULLIF(SQRT(GREATEST(rollup_variance, 0.01)), 0)) >= 4
            THEN 'briefing_candidate'
            WHEN years_seen >= 2 AND picks >= 8
              AND ABS(excess_residual / NULLIF(SQRT(GREATEST(rollup_variance, 0.01)), 0)) >= 3
            THEN 'likely'
            ELSE 'explore'
        END AS evidence_level,
        {model_lit} AS model_version
    FROM rollup
    WHERE picks >= {min_picks}
      AND years_seen >= {min_years}
)
SELECT *
FROM ranked
WHERE ABS(value_z_score) >= {min_abs_z}
ORDER BY ABS(value_z_score) DESC, ABS(excess_residual) DESC
LIMIT {limit}
"""


def run_league_inefficiency_miner(
    conn,
    db_name: str,
    *,
    min_picks: int = 8,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Run the league-wide inefficiency discovery query."""
    configure_table_catalog(conn)
    result = conn.execute(
        build_league_inefficiency_sql(
            db_name,
            min_picks=min_picks,
            min_years=min_years,
            min_abs_z=min_abs_z,
            limit=limit,
        )
    )
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def run_league_inefficiency_miner_fly(
    db_name: str,
    *,
    min_picks: int = 8,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Run the discovery query through the read-only Fly API."""
    from multi_league.core.readers.fly_reader import FlyReader

    sql = build_league_inefficiency_sql(
        db_name,
        min_picks=min_picks,
        min_years=min_years,
        min_abs_z=min_abs_z,
        limit=limit,
    )
    return FlyReader().query(sql, database="___leagues")


def replace_league_inefficiency_candidates(
    conn,
    db_name: str,
    *,
    min_picks: int = 8,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 500,
) -> int:
    """Replace one league's league-wide inefficiency candidate rows."""
    configure_table_catalog(conn)
    create_draft_inefficiency_table(conn)
    select_sql = build_league_inefficiency_sql(
        db_name,
        min_picks=min_picks,
        min_years=min_years,
        min_abs_z=min_abs_z,
        limit=limit,
    )
    execute_scoped(
        conn,
        f"DELETE FROM {central_table('draft_inefficiency_candidate')} WHERE db_name = {quote_sql(db_name)}",
        db_name,
        label="draft_inefficiency_candidate:delete",
    )
    insert_columns = ", ".join(DRAFT_INEFFICIENCY_COLUMNS)
    execute_scoped(
        conn,
        f"INSERT INTO {central_table('draft_inefficiency_candidate')} ({insert_columns}) {select_sql}",
        db_name,
        label="draft_inefficiency_candidate:insert",
    )
    return conn.execute(
        f"SELECT COUNT(*) FROM {central_table('draft_inefficiency_candidate')} WHERE db_name = {quote_sql(db_name)}"
    ).fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Mine league-wide draft inefficiency candidates.")
    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--db", help="League db_name")
    parser.add_argument("--data-dir", default=None, help="Local data dir for DuckDB-backed runs")
    parser.add_argument("--min-picks", type=int, default=8)
    parser.add_argument("--min-years", type=int, default=2)
    parser.add_argument("--min-abs-z", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--write", action="store_true", help="Write candidates to draft_inefficiency_candidate")
    parser.add_argument("--json", action="store_true", help="Print rows as JSON")
    args = parser.parse_args()

    db_name, _ = resolve_db_name(args)
    conn = None
    try:
        if args.write:
            conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
            count = replace_league_inefficiency_candidates(
                conn,
                db_name,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )
            log(f"wrote {count} league inefficiency candidates for {db_name}")
            return 0

        if args.data_dir:
            conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
            rows = run_league_inefficiency_miner(
                conn,
                db_name,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )
        else:
            rows = run_league_inefficiency_miner_fly(
                db_name,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
            )

        if args.json:
            print(json.dumps(rows, indent=2, default=str))
        else:
            for row in rows:
                print(
                    f"{row['feature_type']}={row['feature_value']} "
                    f"excess={row['excess_residual']} z={row['value_z_score']} "
                    f"score_delta={row['pick_score_delta']} picks={row['picks']} "
                    f"years={row['years_seen']} evidence={row['evidence_level']}"
                )
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
