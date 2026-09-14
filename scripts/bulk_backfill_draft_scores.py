#!/usr/bin/env python3
"""Bulk recalibrate draft score columns directly in Fly DuckDB.

This is the fleet-safe path for the blended draft pick score rollout. The older
per-league script remains useful for isolated debugging, but it is too chatty
for a full fleet update because it writes many VALUES chunks per league. This
script keeps the work table-native:

- stage eligible rows from public.draft once
- compute universal, similar-profile, and local expectations in DuckDB
- update only existing score columns by rowid, with pick/manager/keeper
  scores stored as 100-centered indexes
- rebuild the dependent draft aggregate tables
- refresh homepage draft highlights that depend on score ordering

No permanent DDL or schema changes are performed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

LEAGUES_DB = "___leagues"
OPS_DB = "___ops"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parse_db_names(raw_values: list[str] | None) -> list[str]:
    if not raw_values:
        return []
    names: list[str] = []
    for raw in raw_values:
        for part in str(raw).replace("\n", ",").split(","):
            name = part.strip()
            if name:
                names.append(name)
    return sorted(dict.fromkeys(names))


def db_filter(alias: str, db_names: list[str]) -> str:
    if not db_names:
        return f"{alias}.db_name IS NOT NULL"
    values = ", ".join(sql_literal(name) for name in db_names)
    return f"{alias}.db_name IN ({values})"


def fly_read_query(sql: str, *, database: str, timeout: int = 120, attempts: int = 5) -> list[dict]:
    server_url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not server_url or not read_token:
        raise RuntimeError("DATABASE_SERVER_URL/DATABASE_READ_TOKEN not set")

    payload = json.dumps({"sql": sql, "database": database}).encode("utf-8")
    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        req = Request(
            f"{server_url}/query",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {read_token}",
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                if resp.status >= 400:
                    raise RuntimeError(f"Fly read failed ({resp.status}): {body}")
                return json.loads(body)
        except (HTTPError, URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            transient = isinstance(exc, (URLError, TimeoutError, ConnectionError)) or (
                isinstance(exc, HTTPError) and exc.code in {429, 500, 502, 503, 504}
            )
            if not transient or attempt >= attempts:
                raise
            time.sleep(min(2 ** (attempt - 1), 10))

    raise RuntimeError(f"Fly read failed after retries: {last_error}")


def chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def fetch_draft_headshot_map(db_names: list[str]) -> dict[str, str]:
    rows = fly_read_query(
        f"""
        SELECT DISTINCT CAST(d.NFL_player_id AS VARCHAR) AS NFL_player_id
        FROM public.draft d
        WHERE {db_filter('d', db_names)}
          AND d.NFL_player_id IS NOT NULL
          AND TRIM(CAST(d.NFL_player_id AS VARCHAR)) <> ''
        """,
        database=LEAGUES_DB,
    )
    ids = sorted({str(row.get("NFL_player_id") or "").strip() for row in rows if row.get("NFL_player_id")})
    if not ids:
        return {}

    headshots: dict[str, str] = {}
    for batch in chunks(ids, 500):
        values = ", ".join(sql_literal(value) for value in batch)
        bio_rows = fly_read_query(
            f"""
            SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                   ARG_MAX(headshot_url, headshot_url) AS headshot_url
            FROM nfl_historical.player_bio
            WHERE NFL_player_id IN ({values})
              AND headshot_url IS NOT NULL
              AND TRIM(CAST(headshot_url AS VARCHAR)) <> ''
            GROUP BY NFL_player_id
            """,
            database=OPS_DB,
        )
        for row in bio_rows:
            nfl_id = str(row.get("NFL_player_id") or "").strip()
            # The Fly write endpoint splits multi-statement SQL before DuckDB
            # parsing, so a raw semicolon inside a URL literal can break the
            # injected temp VALUES table. Keep the URL equivalent but safe.
            headshot = str(row.get("headshot_url") or "").strip().replace(";", "%3B")
            if nfl_id and headshot:
                headshots[nfl_id] = headshot
    return headshots


def headshot_values_sql(headshots: dict[str, str] | None) -> str:
    if not headshots:
        return """
        DROP TABLE IF EXISTS _draft_highlight_headshots;
        CREATE TEMP TABLE _draft_highlight_headshots (
            NFL_player_id VARCHAR,
            headshot_url VARCHAR
        );
        """

    values = ",\n        ".join(
        f"({sql_literal(nfl_id)}, {sql_literal(headshot)})" for nfl_id, headshot in sorted(headshots.items())
    )
    return f"""
    DROP TABLE IF EXISTS _draft_highlight_headshots;
    CREATE TEMP TABLE _draft_highlight_headshots AS
    SELECT *
    FROM (VALUES
        {values}
    ) AS v(NFL_player_id, headshot_url);
    """


def grade_from_percentile_sql(prefix: str = "") -> str:
    return f"""CASE
        WHEN {prefix}pctile >= 95 THEN 'A+'
        WHEN {prefix}pctile >= 85 THEN 'A'
        WHEN {prefix}pctile >= 75 THEN 'A-'
        WHEN {prefix}pctile >= 65 THEN 'B+'
        WHEN {prefix}pctile >= 50 THEN 'B'
        WHEN {prefix}pctile >= 35 THEN 'B-'
        WHEN {prefix}pctile >= 20 THEN 'C'
        WHEN {prefix}pctile >= 10 THEN 'D'
        ELSE 'F'
    END"""


def draft_index_sql(expr: str, *, precision: int = 3) -> str:
    return f"ROUND(100.0 + 15.0 * CAST({expr} AS DOUBLE), {precision})"


def grade_from_draft_score_sql(expr: str) -> str:
    z_expr = f"""(CASE
        WHEN {expr} IS NULL THEN NULL
        WHEN ABS(CAST({expr} AS DOUBLE)) > 10 THEN (CAST({expr} AS DOUBLE) - 100.0) / 15.0
        ELSE CAST({expr} AS DOUBLE)
    END)"""
    return f"""CASE
        WHEN {z_expr} IS NULL THEN NULL
        WHEN {z_expr} >= 1.5 THEN 'A+'
        WHEN {z_expr} >= 1.0 THEN 'A'
        WHEN {z_expr} >= 0.7 THEN 'A-'
        WHEN {z_expr} >= 0.45 THEN 'B+'
        WHEN {z_expr} >= 0.15 THEN 'B'
        WHEN {z_expr} >= -0.15 THEN 'B-'
        WHEN {z_expr} >= -0.45 THEN 'C'
        WHEN {z_expr} >= -0.9 THEN 'D'
        ELSE 'F'
    END"""


def grade_from_gpa_sql(expr: str) -> str:
    return f"""CASE
        WHEN {expr} IS NULL THEN NULL
        WHEN {expr} >= 3.85 THEN 'A+'
        WHEN {expr} >= 3.50 THEN 'A'
        WHEN {expr} >= 3.15 THEN 'A-'
        WHEN {expr} >= 2.85 THEN 'B+'
        WHEN {expr} >= 2.50 THEN 'B'
        WHEN {expr} >= 2.15 THEN 'B-'
        WHEN {expr} >= 1.85 THEN 'C+'
        WHEN {expr} >= 1.50 THEN 'C'
        WHEN {expr} >= 1.15 THEN 'C-'
        WHEN {expr} >= 0.85 THEN 'D+'
        WHEN {expr} >= 0.50 THEN 'D'
        WHEN {expr} >= 0.15 THEN 'D-'
        ELSE 'F'
    END"""


def scoring_sql(db_names: list[str], *, apply: bool) -> str:
    draft_target = f"{db_filter('d', db_names)} AND d.player IS NOT NULL"
    if not apply:
        return f"""
        DROP TABLE IF EXISTS _draft_score_rows;
        CREATE TEMP TABLE _draft_score_rows AS
        {draft_score_rows_select(db_names)};
        CREATE INDEX _idx_draft_score_rows_db ON _draft_score_rows(db_name, year, draft_market, draft_kind, cohort);
        CREATE INDEX _idx_draft_score_rows_profile ON _draft_score_rows(
            draft_market, draft_kind, cohort, position, capital_bucket,
            teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
            superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median
        );
        {draft_scored_tables_sql()}
        SELECT
            'dry_run_scores' AS phase,
            COUNT(*) AS staged_rows,
            COUNT(*) FILTER (WHERE draft_value_zscore IS NOT NULL) AS scored_rows,
            MIN(draft_value_zscore) AS min_score,
            MAX(draft_value_zscore) AS max_score
        FROM _draft_scored_final;
        """

    return f"""
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS _draft_score_rows;
    CREATE TEMP TABLE _draft_score_rows AS
    {draft_score_rows_select(db_names)};
    CREATE INDEX _idx_draft_score_rows_rowid ON _draft_score_rows(draft_rowid);
    CREATE INDEX _idx_draft_score_rows_db ON _draft_score_rows(db_name, year, draft_market, draft_kind, cohort);
    CREATE INDEX _idx_draft_score_rows_profile ON _draft_score_rows(
        draft_market, draft_kind, cohort, position, capital_bucket,
        teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
        superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median
    );
    {draft_scored_tables_sql()}

    UPDATE public.draft AS d
    SET expected_lamar = NULL,
        draft_value_zscore = NULL,
        pick_quality_zscore = NULL,
        pick_score = NULL,
        draft_grade = NULL,
        manager_draft_score = NULL,
        manager_draft_grade = NULL,
        manager_draft_percentile_alltime = NULL,
        keeper_draft_score = NULL,
        keeper_draft_grade = NULL
    WHERE {draft_target};

    UPDATE public.draft AS d
    SET expected_lamar = s.expected_lamar,
        draft_value_zscore = s.draft_value_zscore,
        pick_quality_zscore = s.draft_value_zscore,
        pick_score = CASE
            WHEN s.draft_value_zscore IS NULL THEN NULL
            ELSE {draft_index_sql('s.draft_value_zscore')}
        END,
        draft_grade = s.draft_grade
    FROM _draft_scored_final AS s
    WHERE d.rowid = s.draft_rowid;

    DROP TABLE IF EXISTS _draft_manager_scores;
    CREATE TEMP TABLE _draft_manager_scores AS
    SELECT db_name, franchise_id, year, draft_category,
           {draft_index_sql('AVG(draft_value_zscore)')} AS manager_draft_score
    FROM _draft_scored_final
    WHERE cohort = 'draft'
      AND draft_value_zscore IS NOT NULL
      AND franchise_id IS NOT NULL
      AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
    GROUP BY db_name, franchise_id, year, draft_category;

    DROP TABLE IF EXISTS _draft_manager_ranked;
    CREATE TEMP TABLE _draft_manager_ranked AS
    WITH ranked AS (
        SELECT *,
               ROUND(PERCENT_RANK() OVER (
                   PARTITION BY db_name, draft_category
                   ORDER BY manager_draft_score ASC
               ) * 100, 1) AS pctile
        FROM _draft_manager_scores
        WHERE manager_draft_score IS NOT NULL
    )
    SELECT db_name, franchise_id, year, draft_category, manager_draft_score,
           pctile AS manager_draft_percentile_alltime,
           {grade_from_percentile_sql()} AS manager_draft_grade
    FROM ranked;

    CREATE INDEX _idx_draft_manager_ranked
        ON _draft_manager_ranked(db_name, franchise_id, year, draft_category);

    UPDATE public.draft AS d
    SET manager_draft_score = mr.manager_draft_score,
        manager_draft_percentile_alltime = mr.manager_draft_percentile_alltime,
        manager_draft_grade = mr.manager_draft_grade
    FROM _draft_score_rows AS r
    JOIN _draft_manager_ranked AS mr
      ON r.db_name = mr.db_name
     AND r.franchise_id = mr.franchise_id
     AND r.year = mr.year
     AND r.draft_category = mr.draft_category
    WHERE d.rowid = r.draft_rowid;

    DROP TABLE IF EXISTS _draft_keeper_scores;
    CREATE TEMP TABLE _draft_keeper_scores AS
    SELECT db_name, franchise_id, year, draft_category,
           {draft_index_sql('AVG(draft_value_zscore)')} AS keeper_draft_score
    FROM _draft_scored_final
    WHERE cohort = 'keeper'
      AND draft_value_zscore IS NOT NULL
      AND franchise_id IS NOT NULL
      AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
    GROUP BY db_name, franchise_id, year, draft_category;

    DROP TABLE IF EXISTS _draft_keeper_ranked;
    CREATE TEMP TABLE _draft_keeper_ranked AS
    WITH base AS (
        SELECT *,
               COUNT(*) OVER (PARTITION BY db_name, draft_category) AS keeper_pool,
               PERCENT_RANK() OVER (
                   PARTITION BY db_name, draft_category
                   ORDER BY keeper_draft_score ASC
               ) * 100 AS pctile
        FROM _draft_keeper_scores
        WHERE keeper_draft_score IS NOT NULL
    )
    SELECT db_name, franchise_id, year, draft_category, keeper_draft_score,
           CASE WHEN keeper_pool >= 5 THEN {grade_from_percentile_sql()} END AS keeper_draft_grade
    FROM base;

    CREATE INDEX _idx_draft_keeper_ranked
        ON _draft_keeper_ranked(db_name, franchise_id, year, draft_category);

    UPDATE public.draft AS d
    SET keeper_draft_score = kr.keeper_draft_score,
        keeper_draft_grade = kr.keeper_draft_grade
    FROM _draft_score_rows AS r
    JOIN _draft_keeper_ranked AS kr
      ON r.db_name = kr.db_name
     AND r.franchise_id = kr.franchise_id
     AND r.year = kr.year
     AND r.draft_category = kr.draft_category
    WHERE d.rowid = r.draft_rowid;
    COMMIT;

    SELECT
        'scores' AS phase,
        COUNT(*) AS target_rows,
        COUNT(draft_value_zscore) AS scored_rows,
        MIN(draft_value_zscore) AS min_score,
        MAX(draft_value_zscore) AS max_score,
        COUNT(*) FILTER (
            WHERE pick_quality_zscore IS DISTINCT FROM draft_value_zscore
               OR pick_score IS DISTINCT FROM CASE
                   WHEN draft_value_zscore IS NULL THEN NULL
                   ELSE {draft_index_sql('draft_value_zscore')}
               END
        ) AS score_drift
    FROM public.draft AS d
    WHERE {draft_target};
    """


def draft_score_rows_select(db_names: list[str]) -> str:
    target = f"{db_filter('d', db_names)} AND d.player IS NOT NULL"
    return f"""
    WITH raw AS (
        SELECT d.rowid AS draft_rowid,
               d.db_name, d.year, TRY_CAST(d.round AS INTEGER) AS round,
               TRY_CAST(d.pick AS INTEGER) AS pick, d.manager, d.franchise_id,
               d.player, d.draft_id, d.NFL_player_id,
               COALESCE(CAST(d.draft_category AS VARCHAR), 'standard') AS draft_category,
               CASE
                 WHEN UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1))) IN ('D/ST', 'DST', 'D')
                   THEN 'DEF'
                 WHEN UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1))) = ''
                   THEN 'UNK'
                 ELSE UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1)))
               END AS position,
               CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1 THEN 'keeper' ELSE 'draft' END AS cohort,
               CASE
                 WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1
                   THEN 'keeper'
                 WHEN LOWER(TRIM(COALESCE(CAST(d.draft_category AS VARCHAR), ''))) IN ('startup', 'rookie', 'veteran')
                   THEN LOWER(TRIM(COALESCE(CAST(d.draft_category AS VARCHAR), '')))
                 ELSE 'redraft'
               END AS draft_market,
               CASE
                 WHEN LOWER(TRIM(COALESCE(CAST(d.draft_type AS VARCHAR), ''))) = 'auction'
                   OR COALESCE(TRY_CAST(d.cost AS DOUBLE), 0) > 0
                 THEN 'auction' ELSE 'snake'
               END AS draft_kind,
               COALESCE(TRY_CAST(d.cost AS DOUBLE), 0) AS cost,
               TRY_CAST(d.manager_lamar AS DOUBLE) AS manager_lamar
        FROM public.draft AS d
        WHERE {target}
          AND d.year IS NOT NULL
          AND d.manager_lamar IS NOT NULL
    ),
    valid_years AS (
        SELECT db_name, year
        FROM raw
        GROUP BY db_name, year
        HAVING SUM(ABS(COALESCE(manager_lamar, 0))) > 0
    ),
    eligible_raw AS (
        SELECT r.*
        FROM raw r
        JOIN valid_years v
          ON r.db_name = v.db_name AND r.year = v.year
    ),
    year_stats AS (
        SELECT db_name, year, draft_market, draft_kind, cohort,
               COUNT(*) AS picks_in_market,
               COUNT(DISTINCT manager) AS managers_in_market,
               MAX(COALESCE(round, 0)) AS max_round,
               SUM(COALESCE(cost, 0)) AS total_cost
        FROM eligible_raw
        GROUP BY db_name, year, draft_market, draft_kind, cohort
    ),
    settings AS (
        SELECT db_name, year,
               MAX(COALESCE(num_teams, 0)) AS num_teams,
               MAX(COALESCE(draft_rounds, 0)) AS draft_rounds,
               MAX(COALESCE(scoring_rec, 0)) AS scoring_rec,
               MAX(COALESCE(scoring_pass_td, 4)) AS scoring_pass_td,
               MAX(COALESCE(roster_QB, 0)) AS roster_qb,
               MAX(COALESCE(roster_RB, 0)) AS roster_rb,
               MAX(COALESCE(roster_WR, 0)) AS roster_wr,
               MAX(COALESCE(roster_TE, 0)) AS roster_te,
               MAX(COALESCE(roster_K, 0)) AS roster_k,
               MAX(COALESCE(roster_DEF, 0)) AS roster_def,
               MAX(COALESCE(roster_FLX, 0)) AS roster_flex,
               MAX(COALESCE(roster_SUPER_FLEX, 0)) AS roster_super_flex,
               MAX(COALESCE(roster_REC_FLEX, 0)) AS roster_rec_flex,
               MAX(COALESCE(roster_BN, 0)) AS roster_bn,
               MAX(COALESCE(roster_LB, 0) + COALESCE(roster_DL, 0) + COALESCE(roster_DB, 0)
                   + COALESCE(roster_IDP, 0) + COALESCE(roster_DB_LB, 0) + COALESCE(roster_DL_LB, 0)) AS idp_slots,
               MAX(COALESCE(max_keepers, 0)) AS max_keepers,
               MAX(CASE WHEN COALESCE(is_dynasty, false) THEN 1 ELSE 0 END) AS is_dynasty,
               MAX(COALESCE(sleeper_taxi_slots, 0)) AS taxi_slots,
               MAX(CASE WHEN COALESCE(sleeper_pick_trading, false) THEN 1 ELSE 0 END) AS pick_trading,
               MAX(CASE WHEN COALESCE(uses_median, false) THEN 1 ELSE 0 END) AS uses_median
        FROM public.league_settings
        GROUP BY db_name, year
    ),
    profiled AS (
        SELECT r.*,
               COALESCE(NULLIF(s.num_teams, 0), NULLIF(ys.managers_in_market, 0), 12) AS teams,
               COALESCE(NULLIF(s.draft_rounds, 0), NULLIF(ys.max_round, 0), 16) AS draft_rounds,
               COALESCE(s.scoring_rec, 0) AS scoring_rec,
               COALESCE(s.scoring_pass_td, 4) AS pass_td_pts,
               CASE WHEN COALESCE(s.roster_super_flex, 0) > 0 OR COALESCE(s.roster_qb, 0) >= 2 THEN 1 ELSE 0 END AS superflex,
               CASE WHEN COALESCE(s.idp_slots, 0) > 0 THEN 1 ELSE 0 END AS idp,
               COALESCE(s.roster_bn, 0) AS bench_count,
               COALESCE(s.roster_flex, 0) + COALESCE(s.roster_super_flex, 0) + COALESCE(s.roster_rec_flex, 0) AS flex_count,
               COALESCE(s.roster_qb, 0) + COALESCE(s.roster_rb, 0) + COALESCE(s.roster_wr, 0)
                 + COALESCE(s.roster_te, 0) + COALESCE(s.roster_k, 0) + COALESCE(s.roster_def, 0)
                 + COALESCE(s.roster_flex, 0) + COALESCE(s.roster_super_flex, 0)
                 + COALESCE(s.roster_rec_flex, 0) + COALESCE(s.idp_slots, 0) + COALESCE(s.roster_bn, 0)
                 AS total_roster_slots,
               CASE
                 WHEN COALESCE(s.is_dynasty, 0) = 1 OR COALESCE(s.taxi_slots, 0) > 0
                   OR COALESCE(s.pick_trading, 0) = 1 OR r.draft_market IN ('startup', 'rookie', 'veteran')
                 THEN 1 ELSE 0
               END AS dynasty_like,
               CASE WHEN COALESCE(s.max_keepers, 0) > 0 OR r.cohort = 'keeper' THEN 1 ELSE 0 END AS keeper_like,
               COALESCE(s.uses_median, 0) AS uses_median,
               ys.picks_in_market, ys.managers_in_market, ys.total_cost
        FROM eligible_raw r
        JOIN year_stats ys
          ON r.db_name = ys.db_name AND r.year = ys.year
         AND r.draft_market = ys.draft_market AND r.draft_kind = ys.draft_kind AND r.cohort = ys.cohort
        LEFT JOIN settings s ON r.db_name = s.db_name AND r.year = s.year
    )
    SELECT *,
           CASE
             WHEN draft_kind = 'auction' THEN
               CASE
                 WHEN cost <= 0 THEN 'a_free'
                 WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.30 THEN 'a_30p'
                 WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.20 THEN 'a_20_29p'
                 WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.10 THEN 'a_10_19p'
                 WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.05 THEN 'a_05_09p'
                 ELSE 'a_01_04p'
               END
             ELSE
               's_' || LPAD(CAST(LEAST(20, GREATEST(1,
                 CEIL(((COALESCE(pick, round, picks_in_market) - 1) * 20.0)
                 / GREATEST(picks_in_market, 1)))) AS VARCHAR), 2, '0')
           END AS capital_bucket,
           CASE
             WHEN teams <= 8 THEN '08'
             WHEN teams <= 10 THEN '10'
             WHEN teams <= 12 THEN '12'
             WHEN teams <= 14 THEN '14'
             ELSE '16p'
           END AS teams_bucket,
           CASE
             WHEN draft_rounds <= 8 THEN '08'
             WHEN draft_rounds <= 12 THEN '12'
             WHEN draft_rounds <= 16 THEN '16'
             WHEN draft_rounds <= 20 THEN '20'
             ELSE '21p'
           END AS rounds_bucket,
           CASE
             WHEN total_roster_slots <= 14 THEN '14'
             WHEN total_roster_slots <= 18 THEN '18'
             WHEN total_roster_slots <= 22 THEN '22'
             WHEN total_roster_slots <= 26 THEN '26'
             ELSE '27p'
           END AS roster_slots_bucket,
           CASE
             WHEN bench_count <= 4 THEN '04'
             WHEN bench_count <= 7 THEN '07'
             ELSE '08p'
           END AS bench_bucket,
           CASE
             WHEN scoring_rec >= 0.9 THEN 'ppr'
             WHEN scoring_rec >= 0.4 THEN 'half'
             ELSE 'std'
           END AS scoring_rec_bucket
    FROM profiled
    """


def draft_scored_tables_sql() -> str:
    return """
    DROP TABLE IF EXISTS _draft_universal_exact;
    CREATE TEMP TABLE _draft_universal_exact AS
    SELECT draft_market, draft_kind, cohort, position, capital_bucket,
           COUNT(*) AS sample_size,
           AVG(manager_lamar) AS mean_lamar,
           SQRT(GREATEST(AVG(manager_lamar * manager_lamar) - AVG(manager_lamar) * AVG(manager_lamar), 0.0)) AS std_lamar
    FROM _draft_score_rows
    GROUP BY draft_market, draft_kind, cohort, position, capital_bucket;

    DROP TABLE IF EXISTS _draft_universal_pos;
    CREATE TEMP TABLE _draft_universal_pos AS
    SELECT draft_market, draft_kind, cohort, position,
           COUNT(*) AS sample_size,
           AVG(manager_lamar) AS mean_lamar,
           SQRT(GREATEST(AVG(manager_lamar * manager_lamar) - AVG(manager_lamar) * AVG(manager_lamar), 0.0)) AS std_lamar
    FROM _draft_score_rows
    GROUP BY draft_market, draft_kind, cohort, position;

    DROP TABLE IF EXISTS _draft_peer_exact;
    CREATE TEMP TABLE _draft_peer_exact AS
    SELECT draft_market, draft_kind, cohort, position, capital_bucket,
           teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
           superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median,
           COUNT(*) AS sample_size,
           AVG(manager_lamar) AS mean_lamar,
           SQRT(GREATEST(AVG(manager_lamar * manager_lamar) - AVG(manager_lamar) * AVG(manager_lamar), 0.0)) AS std_lamar
    FROM _draft_score_rows
    GROUP BY draft_market, draft_kind, cohort, position, capital_bucket,
             teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
             superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median;

    DROP TABLE IF EXISTS _draft_peer_pos;
    CREATE TEMP TABLE _draft_peer_pos AS
    SELECT draft_market, draft_kind, cohort, position,
           teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
           superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median,
           COUNT(*) AS sample_size,
           AVG(manager_lamar) AS mean_lamar,
           SQRT(GREATEST(AVG(manager_lamar * manager_lamar) - AVG(manager_lamar) * AVG(manager_lamar), 0.0)) AS std_lamar
    FROM _draft_score_rows
    GROUP BY draft_market, draft_kind, cohort, position,
             teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
             superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median;

    CREATE INDEX _idx_universal_exact ON _draft_universal_exact(draft_market, draft_kind, cohort, position, capital_bucket);
    CREATE INDEX _idx_universal_pos ON _draft_universal_pos(draft_market, draft_kind, cohort, position);
    CREATE INDEX _idx_peer_exact ON _draft_peer_exact(
        draft_market, draft_kind, cohort, position, capital_bucket,
        teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
        superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median
    );
    CREATE INDEX _idx_peer_pos ON _draft_peer_pos(
        draft_market, draft_kind, cohort, position,
        teams_bucket, rounds_bucket, roster_slots_bucket, bench_bucket, flex_count,
        superflex, idp, scoring_rec_bucket, pass_td_pts, dynasty_like, keeper_like, uses_median
    );

    DROP TABLE IF EXISTS _draft_local_exact_stats;
    CREATE TEMP TABLE _draft_local_exact_stats AS
    SELECT db_name, draft_market, draft_kind, cohort, position, capital_bucket,
           COUNT(*) AS sample_size,
           SUM(manager_lamar) AS sum_lamar,
           SUM(manager_lamar * manager_lamar) AS sum_lamar_sq
    FROM _draft_score_rows
    GROUP BY db_name, draft_market, draft_kind, cohort, position, capital_bucket;

    DROP TABLE IF EXISTS _draft_local_pos_stats;
    CREATE TEMP TABLE _draft_local_pos_stats AS
    SELECT db_name, draft_market, draft_kind, cohort, position,
           COUNT(*) AS sample_size,
           SUM(manager_lamar) AS sum_lamar,
           SUM(manager_lamar * manager_lamar) AS sum_lamar_sq
    FROM _draft_score_rows
    GROUP BY db_name, draft_market, draft_kind, cohort, position;

    CREATE INDEX _idx_local_exact ON _draft_local_exact_stats(db_name, draft_market, draft_kind, cohort, position, capital_bucket);
    CREATE INDEX _idx_local_pos ON _draft_local_pos_stats(db_name, draft_market, draft_kind, cohort, position);

    DROP TABLE IF EXISTS _draft_scored_base;
    CREATE TEMP TABLE _draft_scored_base AS
    WITH local_exact AS (
        SELECT r.draft_rowid,
               CASE WHEN le.sample_size > 1 THEN (le.sum_lamar - r.manager_lamar) / (le.sample_size - 1) END AS mean_lamar,
               CASE WHEN le.sample_size > 2 THEN
                   SQRT(GREATEST(
                       (le.sum_lamar_sq - r.manager_lamar * r.manager_lamar) / (le.sample_size - 1)
                       - POWER((le.sum_lamar - r.manager_lamar) / (le.sample_size - 1), 2),
                       0.0
                   ))
               END AS std_lamar,
               le.sample_size - 1 AS effective_n
        FROM _draft_score_rows r
        JOIN _draft_local_exact_stats le
          ON r.db_name = le.db_name
         AND r.draft_market = le.draft_market
         AND r.draft_kind = le.draft_kind
         AND r.cohort = le.cohort
         AND r.position = le.position
         AND r.capital_bucket = le.capital_bucket
        WHERE le.sample_size >= 3
    ),
    local_pos AS (
        SELECT r.draft_rowid,
               CASE WHEN lp.sample_size > 1 THEN (lp.sum_lamar - r.manager_lamar) / (lp.sample_size - 1) END AS mean_lamar,
               CASE WHEN lp.sample_size > 2 THEN
                   SQRT(GREATEST(
                       (lp.sum_lamar_sq - r.manager_lamar * r.manager_lamar) / (lp.sample_size - 1)
                       - POWER((lp.sum_lamar - r.manager_lamar) / (lp.sample_size - 1), 2),
                       0.0
                   ))
               END AS std_lamar,
               lp.sample_size - 1 AS effective_n
        FROM _draft_score_rows r
        JOIN _draft_local_pos_stats lp
          ON r.db_name = lp.db_name
         AND r.draft_market = lp.draft_market
         AND r.draft_kind = lp.draft_kind
         AND r.cohort = lp.cohort
         AND r.position = lp.position
        WHERE lp.sample_size >= 3
    ),
    blended AS (
        SELECT r.*,
               COALESCE(le.mean_lamar, lp.mean_lamar) AS local_mean,
               GREATEST(COALESCE(le.std_lamar, lp.std_lamar, 1.0), 1.0) AS local_std,
               GREATEST(COALESCE(le.effective_n, lp.effective_n, 0), 0) AS local_n,
               pe.mean_lamar AS peer_exact_mean,
               GREATEST(pe.std_lamar, 1.0) AS peer_exact_std,
               pe.sample_size AS peer_exact_n,
               pp.mean_lamar AS peer_pos_mean,
               GREATEST(pp.std_lamar, 1.0) AS peer_pos_std,
               pp.sample_size AS peer_pos_n,
               ue.mean_lamar AS universal_exact_mean,
               GREATEST(ue.std_lamar, 1.0) AS universal_exact_std,
               ue.sample_size AS universal_exact_n,
               up.mean_lamar AS universal_pos_mean,
               GREATEST(up.std_lamar, 1.0) AS universal_pos_std,
               up.sample_size AS universal_pos_n
        FROM _draft_score_rows r
        LEFT JOIN local_exact le ON r.draft_rowid = le.draft_rowid
        LEFT JOIN local_pos lp ON r.draft_rowid = lp.draft_rowid
        LEFT JOIN _draft_peer_exact pe
          ON r.draft_market = pe.draft_market
         AND r.draft_kind = pe.draft_kind
         AND r.cohort = pe.cohort
         AND r.position = pe.position
         AND r.capital_bucket = pe.capital_bucket
         AND r.teams_bucket = pe.teams_bucket
         AND r.rounds_bucket = pe.rounds_bucket
         AND r.roster_slots_bucket = pe.roster_slots_bucket
         AND r.bench_bucket = pe.bench_bucket
         AND r.flex_count = pe.flex_count
         AND r.superflex = pe.superflex
         AND r.idp = pe.idp
         AND r.scoring_rec_bucket = pe.scoring_rec_bucket
         AND r.pass_td_pts = pe.pass_td_pts
         AND r.dynasty_like = pe.dynasty_like
         AND r.keeper_like = pe.keeper_like
         AND r.uses_median = pe.uses_median
        LEFT JOIN _draft_peer_pos pp
          ON r.draft_market = pp.draft_market
         AND r.draft_kind = pp.draft_kind
         AND r.cohort = pp.cohort
         AND r.position = pp.position
         AND r.teams_bucket = pp.teams_bucket
         AND r.rounds_bucket = pp.rounds_bucket
         AND r.roster_slots_bucket = pp.roster_slots_bucket
         AND r.bench_bucket = pp.bench_bucket
         AND r.flex_count = pp.flex_count
         AND r.superflex = pp.superflex
         AND r.idp = pp.idp
         AND r.scoring_rec_bucket = pp.scoring_rec_bucket
         AND r.pass_td_pts = pp.pass_td_pts
         AND r.dynasty_like = pp.dynasty_like
         AND r.keeper_like = pp.keeper_like
         AND r.uses_median = pp.uses_median
        LEFT JOIN _draft_universal_exact ue
          ON r.draft_market = ue.draft_market
         AND r.draft_kind = ue.draft_kind
         AND r.cohort = ue.cohort
         AND r.position = ue.position
         AND r.capital_bucket = ue.capital_bucket
        LEFT JOIN _draft_universal_pos up
          ON r.draft_market = up.draft_market
         AND r.draft_kind = up.draft_kind
         AND r.cohort = up.cohort
         AND r.position = up.position
    ),
    priors AS (
        SELECT *,
               COALESCE(
                   CASE WHEN peer_exact_n >= 5 THEN peer_exact_mean END,
                   CASE WHEN peer_pos_n >= 12 THEN peer_pos_mean END
               ) AS peer_mean,
               COALESCE(
                   CASE WHEN peer_exact_n >= 5 THEN peer_exact_std END,
                   CASE WHEN peer_pos_n >= 12 THEN peer_pos_std END,
                   1.0
               ) AS peer_std,
               COALESCE(
                   CASE WHEN peer_exact_n >= 5 THEN peer_exact_n END,
                   CASE WHEN peer_pos_n >= 12 THEN peer_pos_n END,
                   0
               ) AS peer_n,
               COALESCE(
                   CASE WHEN universal_exact_n >= 20 THEN universal_exact_mean END,
                   universal_pos_mean
               ) AS universal_mean,
               COALESCE(
                   CASE WHEN universal_exact_n >= 20 THEN universal_exact_std END,
                   universal_pos_std,
                   1.0
               ) AS universal_std,
               COALESCE(
                   CASE WHEN universal_exact_n >= 20 THEN universal_exact_n END,
                   universal_pos_n,
                   0
               ) AS universal_n
        FROM blended
    ),
    blended_priors AS (
        SELECT *,
               CASE
                   WHEN peer_mean IS NOT NULL AND universal_mean IS NOT NULL THEN peer_n / (peer_n + 80.0)
                   WHEN peer_mean IS NOT NULL THEN 1.0
                   ELSE 0.0
               END AS peer_alpha
        FROM priors
        WHERE peer_mean IS NOT NULL OR universal_mean IS NOT NULL OR local_mean IS NOT NULL
    ),
    expected AS (
        SELECT *,
               CASE
                   WHEN peer_mean IS NOT NULL AND universal_mean IS NOT NULL
                       THEN universal_mean * (1.0 - peer_alpha) + peer_mean * peer_alpha
                   WHEN peer_mean IS NOT NULL THEN peer_mean
                   ELSE universal_mean
               END AS global_mean,
               GREATEST(
                   CASE
                       WHEN peer_mean IS NOT NULL AND universal_mean IS NOT NULL
                           THEN universal_std * (1.0 - peer_alpha) + peer_std * peer_alpha
                       WHEN peer_mean IS NOT NULL THEN peer_std
                       ELSE universal_std
                   END,
                   1.0
               ) AS global_std
        FROM blended_priors
    ),
    final AS (
        SELECT *,
               CASE
                   WHEN global_mean IS NOT NULL AND local_mean IS NOT NULL THEN local_n / (local_n + 24.0)
                   WHEN local_mean IS NOT NULL THEN 1.0
                   ELSE 0.0
               END AS local_alpha
        FROM expected
    )
    SELECT draft_rowid, db_name, year, round, pick, manager, franchise_id, player, draft_id,
           NFL_player_id, draft_category, draft_market, draft_kind, cohort, position, manager_lamar,
           ROUND(CAST(
               CASE
                   WHEN global_mean IS NOT NULL AND local_mean IS NOT NULL
                       THEN global_mean * (1.0 - local_alpha) + local_mean * local_alpha
                   WHEN local_mean IS NOT NULL THEN local_mean
                   ELSE global_mean
               END AS DOUBLE
           ), 3) AS expected_lamar,
           GREATEST(CAST(
               CASE
                   WHEN global_mean IS NOT NULL AND local_mean IS NOT NULL
                       THEN global_std * (1.0 - local_alpha) + local_std * local_alpha
                   WHEN local_mean IS NOT NULL THEN local_std
                   ELSE global_std
               END AS DOUBLE
           ), 1.0) AS blended_std
    FROM final
    WHERE global_mean IS NOT NULL OR local_mean IS NOT NULL;

    DROP TABLE IF EXISTS _draft_scored_final;
    CREATE TEMP TABLE _draft_scored_final AS
    WITH scored AS (
        SELECT *,
               ROUND(CAST((manager_lamar - expected_lamar) / GREATEST(blended_std, 1.0) AS DOUBLE), 3)
                   AS draft_value_zscore
        FROM _draft_scored_base
        WHERE expected_lamar IS NOT NULL
    ),
    grade_signal AS (
        SELECT db_name, draft_category, cohort,
               COUNT(DISTINCT COALESCE(draft_value_zscore, -999999.0)) AS distinct_scores
        FROM scored
        GROUP BY db_name, draft_category, cohort
    ),
    ranked AS (
        SELECT s.*,
               PERCENT_RANK() OVER (
                   PARTITION BY s.db_name, s.draft_category, s.cohort
                   ORDER BY COALESCE(s.draft_value_zscore, -999999.0) ASC
               ) * 100 AS pctile,
               gs.distinct_scores
        FROM scored s
        JOIN grade_signal gs
          ON s.db_name = gs.db_name
         AND s.draft_category = gs.draft_category
         AND s.cohort = gs.cohort
    )
    SELECT *,
           CASE
               WHEN distinct_scores < 2 THEN NULL
               WHEN pctile >= 95 THEN 'A+'
               WHEN pctile >= 85 THEN 'A'
               WHEN pctile >= 75 THEN 'A-'
               WHEN pctile >= 65 THEN 'B+'
               WHEN pctile >= 50 THEN 'B'
               WHEN pctile >= 35 THEN 'B-'
               WHEN pctile >= 20 THEN 'C'
               WHEN pctile >= 10 THEN 'D'
               ELSE 'F'
           END AS draft_grade
    FROM ranked;

    CREATE INDEX _idx_draft_scored_final_rowid ON _draft_scored_final(draft_rowid);
    CREATE INDEX _idx_draft_scored_final_manager ON _draft_scored_final(db_name, franchise_id, year, draft_category);
    """


def aggregate_sql(db_names: list[str], *, apply: bool) -> str:
    d_filter = db_filter("d", db_names)

    def table_filter(alias: str) -> str:
        return db_filter(alias, db_names)

    if not apply:
        return f"""
        WITH target AS (
            SELECT COUNT(*) AS draft_rows,
                   COUNT(DISTINCT db_name || '|' || CAST(year AS VARCHAR) || '|' || COALESCE(franchise_id, '')) AS manager_season_groups,
                   COUNT(DISTINCT db_name || '|' || COALESCE(franchise_id, '')) AS manager_groups,
                   COUNT(DISTINCT db_name || '|' || COALESCE(player, '') || '|' || COALESCE(position, '')) AS player_groups
            FROM public.draft AS d
            WHERE {d_filter}
        )
        SELECT 'dry_run_aggregates' AS phase, * FROM target;
        """

    career_grade = grade_from_gpa_sql("career_gpa")
    return f"""
    BEGIN TRANSACTION;
    DELETE FROM public.draft_manager_season AS s WHERE {table_filter('s')};
    DELETE FROM public.draft_manager_career AS c WHERE {table_filter('c')};
    DELETE FROM public.draft_player_career AS p WHERE {table_filter('p')};

    INSERT INTO public.draft_manager_season (
        db_name, manager, year, franchise_id, draft_category, picks, keeper_picks, total_cost,
        total_manager_lamar, avg_manager_lamar,
        total_player_lamar, total_fantasy_points, avg_season_ppg,
        hits, hit_rate, busts, breakouts,
        avg_pick_quality_zscore, starters_drafted, avg_games_played, avg_lamar_per_dollar,
        manager_draft_grade, manager_draft_score, manager_draft_percentile,
        best_pick_player, best_pick_lamar, worst_pick_player, worst_pick_lamar,
        last_updated
    )
    WITH non_keeper AS (
        SELECT d.rowid AS draft_rowid, d.*,
               COALESCE(CAST(d.draft_category AS VARCHAR), 'standard') AS _cat
        FROM public.draft AS d
        WHERE {d_filter}
          AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0
          AND d.manager IS NOT NULL AND TRIM(d.manager) <> ''
          AND d.franchise_id IS NOT NULL AND TRIM(CAST(d.franchise_id AS VARCHAR)) <> ''
    ),
    keeper AS (
        SELECT d.db_name, d.franchise_id, d.year,
               COALESCE(CAST(d.draft_category AS VARCHAR), 'standard') AS _cat,
               COUNT(*) AS cnt
        FROM public.draft AS d
        WHERE {d_filter}
          AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1
          AND d.manager IS NOT NULL AND TRIM(d.manager) <> ''
          AND d.franchise_id IS NOT NULL AND TRIM(CAST(d.franchise_id AS VARCHAR)) <> ''
        GROUP BY d.db_name, d.franchise_id, d.year, COALESCE(CAST(d.draft_category AS VARCHAR), 'standard')
    ),
    best_worst AS (
        SELECT db_name, franchise_id, year, _cat,
               FIRST(player ORDER BY COALESCE(manager_lamar, 0) DESC) AS best_player,
               MAX(COALESCE(manager_lamar, 0)) AS best_lamar,
               FIRST(player ORDER BY COALESCE(manager_lamar, 0) ASC) AS worst_player,
               MIN(COALESCE(manager_lamar, 0)) AS worst_lamar
        FROM non_keeper
        WHERE player IS NOT NULL
        GROUP BY db_name, franchise_id, year, _cat
    )
    SELECT nk.db_name,
           ARG_MAX(nk.manager, nk.draft_rowid) AS manager,
           nk.year,
           nk.franchise_id,
           nk._cat AS draft_category,
           COUNT(*) AS picks,
           COALESCE(k.cnt, 0) AS keeper_picks,
           SUM(COALESCE(nk.cost, 0)) AS total_cost,
           SUM(COALESCE(nk.manager_lamar, 0)) AS total_manager_lamar,
           AVG(COALESCE(nk.manager_lamar, 0)) AS avg_manager_lamar,
           SUM(COALESCE(nk.player_lamar, 0)) AS total_player_lamar,
           SUM(COALESCE(nk.total_fantasy_points, 0)) AS total_fantasy_points,
           AVG(COALESCE(nk.season_ppg, 0)) AS avg_season_ppg,
           SUM(CASE WHEN COALESCE(nk.manager_lamar, 0) > 0 THEN 1 ELSE 0 END) AS hits,
           CAST(SUM(CASE WHEN COALESCE(nk.manager_lamar, 0) > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS hit_rate,
           0 AS busts,
           0 AS breakouts,
           AVG(nk.pick_quality_zscore) AS avg_pick_quality_zscore,
           SUM(CASE WHEN COALESCE(nk.drafted_as_starter, 0) = 1 THEN 1 ELSE 0 END) AS starters_drafted,
           AVG(COALESCE(nk.games_played, 0)) AS avg_games_played,
           AVG(CASE WHEN COALESCE(nk.cost, 0) > 0 THEN nk.manager_lamar / NULLIF(nk.cost, 0) END) AS avg_lamar_per_dollar,
           MAX(nk.manager_draft_grade) AS manager_draft_grade,
           MAX(nk.manager_draft_score) AS manager_draft_score,
           MAX(nk.manager_draft_percentile_alltime) AS manager_draft_percentile,
           bw.best_player,
           bw.best_lamar,
           bw.worst_player,
           bw.worst_lamar,
           CURRENT_TIMESTAMP AS last_updated
    FROM non_keeper nk
    LEFT JOIN keeper k
      ON nk.db_name = k.db_name AND nk.franchise_id = k.franchise_id
     AND nk.year = k.year AND nk._cat = k._cat
    LEFT JOIN best_worst bw
      ON nk.db_name = bw.db_name AND nk.franchise_id = bw.franchise_id
     AND nk.year = bw.year AND nk._cat = bw._cat
    GROUP BY nk.db_name, nk.franchise_id, nk.year, nk._cat, k.cnt,
             bw.best_player, bw.best_lamar, bw.worst_player, bw.worst_lamar;

    INSERT INTO public.draft_manager_career (
        db_name, manager, franchise_id, draft_category, years_active, total_picks,
        total_keeper_picks, total_cost, total_manager_lamar, avg_manager_lamar,
        total_fantasy_points, avg_season_ppg, career_hit_rate, total_busts,
        total_breakouts, avg_pick_quality_zscore, avg_games_played,
        avg_lamar_per_dollar, career_gpa, career_grade, best_pick_player,
        best_pick_lamar, worst_pick_player, worst_pick_lamar, last_updated
    )
    WITH season AS (
        SELECT * FROM public.draft_manager_season AS s WHERE {table_filter('s')}
    ),
    career_raw AS (
        SELECT db_name,
               ARG_MAX(manager, year) AS manager,
               franchise_id,
               draft_category,
               COUNT(DISTINCT year) AS years_active,
               SUM(picks) AS total_picks,
               SUM(keeper_picks) AS total_keeper_picks,
               SUM(total_cost) AS total_cost,
               SUM(total_manager_lamar) AS total_manager_lamar,
               SUM(total_manager_lamar) / NULLIF(SUM(picks), 0) AS avg_manager_lamar,
               SUM(total_fantasy_points) AS total_fantasy_points,
               SUM(avg_season_ppg * picks) / NULLIF(SUM(picks), 0) AS avg_season_ppg,
               CAST(SUM(hits) AS DOUBLE) / NULLIF(SUM(picks), 0) AS career_hit_rate,
               SUM(busts) AS total_busts,
               SUM(breakouts) AS total_breakouts,
               SUM(avg_pick_quality_zscore * picks) / NULLIF(SUM(picks), 0) AS avg_pick_quality_zscore,
               SUM(avg_games_played * picks) / NULLIF(SUM(picks), 0) AS avg_games_played,
               SUM(total_manager_lamar) / NULLIF(SUM(total_cost), 0) AS avg_lamar_per_dollar,
               SUM(
                   CASE manager_draft_grade
                       WHEN 'A+' THEN 4.0 WHEN 'A' THEN 3.67 WHEN 'A-' THEN 3.33
                       WHEN 'B+' THEN 3.0 WHEN 'B' THEN 2.67 WHEN 'B-' THEN 2.33
                       WHEN 'C+' THEN 2.0 WHEN 'C' THEN 1.67 WHEN 'C-' THEN 1.33
                       WHEN 'D+' THEN 1.0 WHEN 'D' THEN 0.67 WHEN 'D-' THEN 0.33
                       WHEN 'F' THEN 0.0 ELSE NULL
                   END * picks
               ) / NULLIF(SUM(CASE WHEN manager_draft_grade IS NOT NULL THEN picks ELSE 0 END), 0) AS career_gpa
        FROM season
        GROUP BY db_name, franchise_id, draft_category
    ),
    best_worst AS (
        SELECT db_name, franchise_id, draft_category,
               FIRST(best_pick_player ORDER BY best_pick_lamar DESC) AS best_pick_player,
               MAX(best_pick_lamar) AS best_pick_lamar,
               FIRST(worst_pick_player ORDER BY worst_pick_lamar ASC) AS worst_pick_player,
               MIN(worst_pick_lamar) AS worst_pick_lamar
        FROM season
        WHERE best_pick_player IS NOT NULL
        GROUP BY db_name, franchise_id, draft_category
    )
    SELECT c.db_name, c.manager, c.franchise_id, c.draft_category, c.years_active,
           c.total_picks, c.total_keeper_picks, c.total_cost, c.total_manager_lamar,
           c.avg_manager_lamar, c.total_fantasy_points, c.avg_season_ppg,
           c.career_hit_rate, c.total_busts, c.total_breakouts,
           c.avg_pick_quality_zscore, c.avg_games_played, c.avg_lamar_per_dollar,
           c.career_gpa,
           {career_grade} AS career_grade,
           bw.best_pick_player, bw.best_pick_lamar,
           bw.worst_pick_player, bw.worst_pick_lamar,
           CURRENT_TIMESTAMP AS last_updated
    FROM career_raw c
    LEFT JOIN best_worst bw
      ON c.db_name = bw.db_name AND c.franchise_id = bw.franchise_id
     AND c.draft_category = bw.draft_category;

    INSERT INTO public.draft_player_career (
        db_name, player, position, draft_category, times_drafted, times_kept,
        total_cost, keeper_cost, total_manager_lamar, avg_manager_lamar,
        lamar_per_dollar, total_fantasy_points, avg_season_ppg,
        avg_pick_quality_zscore, best_manager, managers, franchise_ids, years,
        last_updated
    )
    SELECT d.db_name,
           d.player,
           d.position,
           COALESCE(CAST(d.draft_category AS VARCHAR), 'standard') AS draft_category,
           SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 THEN 1 ELSE 0 END) AS times_drafted,
           SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1 THEN 1 ELSE 0 END) AS times_kept,
           SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 THEN COALESCE(d.cost, 0) ELSE 0 END) AS total_cost,
           SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1 THEN COALESCE(d.cost, 0) ELSE 0 END) AS keeper_cost,
           SUM(COALESCE(d.manager_lamar, 0)) AS total_manager_lamar,
           AVG(COALESCE(d.manager_lamar, 0)) AS avg_manager_lamar,
           SUM(COALESCE(d.manager_lamar, 0)) / NULLIF(SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 THEN COALESCE(d.cost, 0) ELSE 0 END), 0) AS lamar_per_dollar,
           SUM(COALESCE(d.total_fantasy_points, 0)) AS total_fantasy_points,
           AVG(COALESCE(d.season_ppg, 0)) AS avg_season_ppg,
           AVG(d.pick_quality_zscore) AS avg_pick_quality_zscore,
           FIRST(d.manager ORDER BY COALESCE(d.manager_lamar, 0) DESC) AS best_manager,
           STRING_AGG(DISTINCT d.manager, ', ' ORDER BY d.manager) AS managers,
           STRING_AGG(DISTINCT d.franchise_id, ', ' ORDER BY d.franchise_id) AS franchise_ids,
           STRING_AGG(DISTINCT CAST(d.year AS VARCHAR), ', ' ORDER BY CAST(d.year AS VARCHAR)) AS years,
           CURRENT_TIMESTAMP AS last_updated
    FROM public.draft AS d
    WHERE {d_filter}
      AND d.player IS NOT NULL AND TRIM(d.player) <> ''
      AND d.position IS NOT NULL AND TRIM(d.position) <> ''
    GROUP BY d.db_name, d.player, d.position, COALESCE(CAST(d.draft_category AS VARCHAR), 'standard');
    COMMIT;

    SELECT 'aggregates' AS phase,
           (SELECT COUNT(*) FROM public.draft_manager_season AS s WHERE {table_filter('s')}) AS draft_manager_season_rows,
           (SELECT COUNT(*) FROM public.draft_manager_career AS c WHERE {table_filter('c')}) AS draft_manager_career_rows,
           (SELECT COUNT(*) FROM public.draft_player_career AS p WHERE {table_filter('p')}) AS draft_player_career_rows;
    """


def homepage_sql(
    db_names: list[str],
    *,
    apply: bool,
    headshots: dict[str, str] | None = None,
) -> str:
    if not apply:
        return f"""
        SELECT 'dry_run_homepage' AS phase,
               (SELECT COUNT(*) FROM public.homepage_league_summary AS h WHERE {db_filter('h', db_names)}) AS league_summary_rows,
               (SELECT COUNT(*) FROM public.homepage_manager_profiles AS p WHERE {db_filter('p', db_names)}) AS manager_profile_rows;
        """

    h_filter = db_filter("h", db_names)
    p_filter = db_filter("p", db_names)
    d_filter = db_filter("d", db_names)
    return f"""
    BEGIN TRANSACTION;
    DROP TABLE IF EXISTS _draft_latest_homepage_year;
    CREATE TEMP TABLE _draft_latest_homepage_year AS
    SELECT db_name, MAX(latest_year) AS latest_year
    FROM (
        SELECT h.db_name, MAX(h.data_year) AS latest_year
        FROM public.homepage_league_summary h
        WHERE {h_filter}
        GROUP BY h.db_name
        UNION ALL
        SELECT m.db_name, MAX(m.year) AS latest_year
        FROM public.matchup m
        WHERE {db_filter('m', db_names)}
        GROUP BY m.db_name
        UNION ALL
        SELECT d.db_name, MAX(d.year) AS latest_year
        FROM public.draft d
        WHERE {d_filter}
        GROUP BY d.db_name
    )
    WHERE latest_year IS NOT NULL
    GROUP BY db_name;

    DROP TABLE IF EXISTS _draft_highlight_eligible;
    CREATE TEMP TABLE _draft_highlight_eligible AS
    SELECT d.db_name, d.rowid AS draft_rowid, d.player, d.manager, d.franchise_id,
           d.position, d.year, d.round, d.pick, d.cost, d.NFL_player_id,
           COALESCE(d.manager_lamar, 0) AS lamar,
           d.draft_value_zscore
    FROM public.draft d
    WHERE {d_filter}
      AND d.player IS NOT NULL
      AND d.draft_value_zscore IS NOT NULL
      AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0
      AND COALESCE(UPPER(TRIM(d.position)), '') NOT IN ('DEF', 'DST', 'D/ST', 'K');

    DROP TABLE IF EXISTS _draft_highlights;
    CREATE TEMP TABLE _draft_highlights AS
    WITH alltime AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY db_name ORDER BY draft_value_zscore DESC, lamar DESC) AS alltime_best_rank,
               ROW_NUMBER() OVER (PARTITION BY db_name ORDER BY draft_value_zscore ASC, lamar ASC) AS alltime_worst_rank
        FROM _draft_highlight_eligible
    ),
    season AS (
        SELECT e.*,
               ROW_NUMBER() OVER (PARTITION BY e.db_name ORDER BY e.draft_value_zscore DESC, e.lamar DESC) AS season_best_rank,
               ROW_NUMBER() OVER (PARTITION BY e.db_name ORDER BY e.draft_value_zscore ASC, e.lamar ASC) AS season_worst_rank
        FROM _draft_highlight_eligible e
        JOIN _draft_latest_homepage_year y
          ON e.db_name = y.db_name AND e.year = y.latest_year
    )
    SELECT 'alltime_best' AS slot, * EXCLUDE (alltime_best_rank, alltime_worst_rank)
    FROM alltime WHERE alltime_best_rank = 1
    UNION ALL
    SELECT 'alltime_worst' AS slot, * EXCLUDE (alltime_best_rank, alltime_worst_rank)
    FROM alltime WHERE alltime_worst_rank = 1
    UNION ALL
    SELECT 'season_best' AS slot, * EXCLUDE (season_best_rank, season_worst_rank)
    FROM season WHERE season_best_rank = 1
    UNION ALL
    SELECT 'season_worst' AS slot, * EXCLUDE (season_best_rank, season_worst_rank)
    FROM season WHERE season_worst_rank = 1;

    {headshot_values_sql(headshots)}

    DROP TABLE IF EXISTS _draft_highlight_payload;
    CREATE TEMP TABLE _draft_highlight_payload AS
    SELECT h.*, hs.headshot_url
    FROM _draft_highlights h
    LEFT JOIN _draft_highlight_headshots hs
      ON h.NFL_player_id = hs.NFL_player_id;

    UPDATE public.homepage_league_summary AS h
    SET alltime_best_pick_player = ab.player,
        alltime_best_pick_manager = ab.manager,
        alltime_best_pick_position = ab.position,
        alltime_best_pick_round = ab.round,
        alltime_best_pick_year = ab.year,
        alltime_best_pick_lamar = ROUND(CAST(ab.lamar AS DOUBLE), 2),
        alltime_best_pick_headshot = COALESCE(ab.headshot_url, CASE
            WHEN h.alltime_best_pick_player IS NOT DISTINCT FROM ab.player THEN h.alltime_best_pick_headshot
            ELSE NULL
        END),
        alltime_best_pick_cost = ROUND(CAST(ab.cost AS DOUBLE), 0),
        alltime_best_pick_pick = ab.pick,
        alltime_worst_pick_player = aw.player,
        alltime_worst_pick_manager = aw.manager,
        alltime_worst_pick_position = aw.position,
        alltime_worst_pick_round = aw.round,
        alltime_worst_pick_year = aw.year,
        alltime_worst_pick_lamar = ROUND(CAST(aw.lamar AS DOUBLE), 2),
        alltime_worst_pick_headshot = COALESCE(aw.headshot_url, CASE
            WHEN h.alltime_worst_pick_player IS NOT DISTINCT FROM aw.player THEN h.alltime_worst_pick_headshot
            ELSE NULL
        END),
        alltime_worst_pick_cost = ROUND(CAST(aw.cost AS DOUBLE), 0),
        alltime_worst_pick_pick = aw.pick,
        season_best_pick_player = sb.player,
        season_best_pick_manager = sb.manager,
        season_best_pick_position = sb.position,
        season_best_pick_round = sb.round,
        season_best_pick_year = sb.year,
        season_best_pick_lamar = ROUND(CAST(sb.lamar AS DOUBLE), 2),
        season_best_pick_headshot = COALESCE(sb.headshot_url, CASE
            WHEN h.season_best_pick_player IS NOT DISTINCT FROM sb.player THEN h.season_best_pick_headshot
            ELSE NULL
        END),
        season_best_pick_cost = ROUND(CAST(sb.cost AS DOUBLE), 0),
        season_best_pick_pick = sb.pick,
        season_worst_pick_player = sw.player,
        season_worst_pick_manager = sw.manager,
        season_worst_pick_position = sw.position,
        season_worst_pick_round = sw.round,
        season_worst_pick_year = sw.year,
        season_worst_pick_lamar = ROUND(CAST(sw.lamar AS DOUBLE), 2),
        season_worst_pick_headshot = COALESCE(sw.headshot_url, CASE
            WHEN h.season_worst_pick_player IS NOT DISTINCT FROM sw.player THEN h.season_worst_pick_headshot
            ELSE NULL
        END),
        season_worst_pick_cost = ROUND(CAST(sw.cost AS DOUBLE), 0),
        season_worst_pick_pick = sw.pick,
        last_updated = CURRENT_TIMESTAMP
    FROM public.homepage_league_summary base
    LEFT JOIN _draft_highlight_payload ab ON base.db_name = ab.db_name AND ab.slot = 'alltime_best'
    LEFT JOIN _draft_highlight_payload aw ON base.db_name = aw.db_name AND aw.slot = 'alltime_worst'
    LEFT JOIN _draft_highlight_payload sb ON base.db_name = sb.db_name AND sb.slot = 'season_best'
    LEFT JOIN _draft_highlight_payload sw ON base.db_name = sw.db_name AND sw.slot = 'season_worst'
    WHERE h.db_name = base.db_name
      AND {h_filter};

    DROP TABLE IF EXISTS _draft_profile_highlights;
    CREATE TEMP TABLE _draft_profile_highlights AS
    WITH alltime AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY db_name, franchise_id ORDER BY draft_value_zscore DESC, lamar DESC) AS best_rank,
               ROW_NUMBER() OVER (PARTITION BY db_name, franchise_id ORDER BY draft_value_zscore ASC, lamar ASC) AS worst_rank
        FROM _draft_highlight_eligible
        WHERE franchise_id IS NOT NULL
    ),
    season AS (
        SELECT e.*,
               ROW_NUMBER() OVER (PARTITION BY e.db_name, e.franchise_id ORDER BY e.draft_value_zscore DESC, e.lamar DESC) AS best_rank,
               ROW_NUMBER() OVER (PARTITION BY e.db_name, e.franchise_id ORDER BY e.draft_value_zscore ASC, e.lamar ASC) AS worst_rank
        FROM _draft_highlight_eligible e
        JOIN _draft_latest_homepage_year y
          ON e.db_name = y.db_name AND e.year = y.latest_year
        WHERE e.franchise_id IS NOT NULL
    ),
    highlights AS (
        SELECT 'alltime_best' AS slot, * EXCLUDE (best_rank, worst_rank) FROM alltime WHERE best_rank = 1
        UNION ALL
        SELECT 'alltime_worst' AS slot, * EXCLUDE (best_rank, worst_rank) FROM alltime WHERE worst_rank = 1
        UNION ALL
        SELECT 'season_best' AS slot, * EXCLUDE (best_rank, worst_rank) FROM season WHERE best_rank = 1
        UNION ALL
        SELECT 'season_worst' AS slot, * EXCLUDE (best_rank, worst_rank) FROM season WHERE worst_rank = 1
    )
    SELECT h.*, hs.headshot_url
    FROM highlights h
    LEFT JOIN _draft_highlight_headshots hs
      ON h.NFL_player_id = hs.NFL_player_id;

    DROP TABLE IF EXISTS _draft_profile_grades;
    CREATE TEMP TABLE _draft_profile_grades AS
    WITH manager_years AS (
        SELECT d.db_name, d.franchise_id, d.year,
               COALESCE(CAST(d.draft_category AS VARCHAR), 'standard') AS draft_category,
               MAX(d.manager_draft_score) AS manager_draft_score,
               ARG_MAX(d.manager_draft_grade, d.manager_draft_score) AS manager_draft_grade
        FROM public.draft d
        WHERE {d_filter}
          AND d.franchise_id IS NOT NULL
          AND TRIM(CAST(d.franchise_id AS VARCHAR)) <> ''
          AND d.manager_draft_score IS NOT NULL
        GROUP BY d.db_name, d.franchise_id, d.year, COALESCE(CAST(d.draft_category AS VARCHAR), 'standard')
    ),
    career AS (
        SELECT db_name, franchise_id,
               {grade_from_draft_score_sql('AVG(manager_draft_score)')} AS draft_career_grade
        FROM manager_years
        GROUP BY db_name, franchise_id
    ),
    season AS (
        SELECT my.db_name, my.franchise_id,
               ARG_MAX(my.manager_draft_grade, my.manager_draft_score) AS season_draft_grade
        FROM manager_years my
        JOIN _draft_latest_homepage_year y
          ON my.db_name = y.db_name AND my.year = y.latest_year
        GROUP BY my.db_name, my.franchise_id
    )
    SELECT COALESCE(c.db_name, s.db_name) AS db_name,
           COALESCE(c.franchise_id, s.franchise_id) AS franchise_id,
           c.draft_career_grade,
           s.season_draft_grade
    FROM career c
    FULL OUTER JOIN season s
      ON c.db_name = s.db_name AND c.franchise_id = s.franchise_id;

    UPDATE public.homepage_manager_profiles AS p
    SET best_pick_player = ab.player,
        best_pick_round = ab.round,
        best_pick_year = ab.year,
        best_pick_lamar = ROUND(CAST(ab.lamar AS DOUBLE), 2),
        best_pick_headshot = COALESCE(ab.headshot_url, CASE
            WHEN p.best_pick_player IS NOT DISTINCT FROM ab.player THEN p.best_pick_headshot
            ELSE NULL
        END),
        best_pick_cost = ROUND(CAST(ab.cost AS DOUBLE), 0),
        best_pick_pick = ab.pick,
        worst_pick_player = aw.player,
        worst_pick_round = aw.round,
        worst_pick_year = aw.year,
        worst_pick_lamar = ROUND(CAST(aw.lamar AS DOUBLE), 2),
        worst_pick_headshot = COALESCE(aw.headshot_url, CASE
            WHEN p.worst_pick_player IS NOT DISTINCT FROM aw.player THEN p.worst_pick_headshot
            ELSE NULL
        END),
        worst_pick_cost = ROUND(CAST(aw.cost AS DOUBLE), 0),
        worst_pick_pick = aw.pick,
        draft_career_grade = g.draft_career_grade,
        season_best_pick_player = sb.player,
        season_best_pick_round = sb.round,
        season_best_pick_year = sb.year,
        season_best_pick_lamar = ROUND(CAST(sb.lamar AS DOUBLE), 2),
        season_best_pick_headshot = COALESCE(sb.headshot_url, CASE
            WHEN p.season_best_pick_player IS NOT DISTINCT FROM sb.player THEN p.season_best_pick_headshot
            ELSE NULL
        END),
        season_best_pick_cost = ROUND(CAST(sb.cost AS DOUBLE), 0),
        season_best_pick_pick = sb.pick,
        season_worst_pick_player = sw.player,
        season_worst_pick_round = sw.round,
        season_worst_pick_year = sw.year,
        season_worst_pick_lamar = ROUND(CAST(sw.lamar AS DOUBLE), 2),
        season_worst_pick_headshot = COALESCE(sw.headshot_url, CASE
            WHEN p.season_worst_pick_player IS NOT DISTINCT FROM sw.player THEN p.season_worst_pick_headshot
            ELSE NULL
        END),
        season_worst_pick_cost = ROUND(CAST(sw.cost AS DOUBLE), 0),
        season_worst_pick_pick = sw.pick,
        season_draft_grade = g.season_draft_grade
    FROM public.homepage_manager_profiles base
    LEFT JOIN _draft_profile_highlights ab
      ON base.db_name = ab.db_name AND base.franchise_id = ab.franchise_id AND ab.slot = 'alltime_best'
    LEFT JOIN _draft_profile_highlights aw
      ON base.db_name = aw.db_name AND base.franchise_id = aw.franchise_id AND aw.slot = 'alltime_worst'
    LEFT JOIN _draft_profile_highlights sb
      ON base.db_name = sb.db_name AND base.franchise_id = sb.franchise_id AND sb.slot = 'season_best'
    LEFT JOIN _draft_profile_highlights sw
      ON base.db_name = sw.db_name AND base.franchise_id = sw.franchise_id AND sw.slot = 'season_worst'
    LEFT JOIN _draft_profile_grades g
      ON base.db_name = g.db_name AND base.franchise_id = g.franchise_id
    WHERE p.db_name = base.db_name
      AND p.franchise_id = base.franchise_id
      AND {p_filter};
    COMMIT;

    SELECT 'homepage' AS phase,
           (SELECT COUNT(*) FROM public.homepage_league_summary AS h WHERE {h_filter}) AS league_summary_rows,
           (SELECT COUNT(*) FROM public.homepage_manager_profiles AS p WHERE {p_filter}) AS manager_profile_rows;
    """


def run_phase(writer, sql: str, phase: str) -> list[dict]:
    print(f"Running {phase}...", flush=True)
    rows = writer.execute(sql, database=LEAGUES_DB)
    print(f"{phase}: {rows}", flush=True)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk backfill blended draft score columns in Fly")
    parser.add_argument("--db", action="append", help="Optional db_name, comma list, or newline list. Omit for fleet.")
    parser.add_argument("--apply", action="store_true", help="Apply writes. Default is dry-run.")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run.")
    parser.add_argument("--skip-aggregates", action="store_true", help="Skip draft aggregate table rebuilds.")
    parser.add_argument("--skip-homepage", action="store_true", help="Skip homepage draft highlight refresh.")
    parser.add_argument("--timeout-seconds", type=int, default=1800, help="Fly write timeout per phase.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    db_names = parse_db_names(args.db)
    apply_writes = bool(args.apply and not args.dry_run)

    from multi_league.core.fly_writer import FlyWriter

    FlyWriter.TIMEOUT_SECONDS = max(FlyWriter.TIMEOUT_SECONDS, int(args.timeout_seconds))
    writer = FlyWriter()
    writer.max_retries = max(writer.max_retries, int(os.environ.get("DRAFT_SCORE_BULK_MAX_RETRIES", "2")))

    target_label = ", ".join(db_names) if db_names else "fleet"
    mode_label = "apply" if apply_writes else "dry-run"
    print(f"Bulk draft score backfill: target={target_label}, mode={mode_label}", flush=True)

    run_phase(writer, scoring_sql(db_names, apply=apply_writes), "scores")
    if not args.skip_aggregates:
        run_phase(writer, aggregate_sql(db_names, apply=apply_writes), "aggregates")
    if not args.skip_homepage:
        headshots = fetch_draft_headshot_map(db_names) if apply_writes else {}
        if apply_writes:
            print(f"Fetched {len(headshots)} draft headshot URL(s)", flush=True)
        run_phase(writer, homepage_sql(db_names, apply=apply_writes, headshots=headshots), "homepage")

    print("Bulk draft score backfill complete.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
