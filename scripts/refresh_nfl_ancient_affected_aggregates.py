#!/usr/bin/env python3
"""Refresh only aggregate cache rows touched by an ancient upsert.

This is the surgical companion to the full aggregate rebuild. It uses the
ancient upsert audit tables to find affected weekly rows, recomputes complete
season/career aggregate rows for only those players/years, and then refreshes
rank columns only in the impacted rank scopes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.data_lake_paths import ancient_apply_output_root  # noqa: E402
from multi_league.data_fetchers.aggregate_nfl_stats_fly import (  # noqa: E402
    CAREER_ALL_TABLE,
    CAREER_TABLE,
    PPG_VARIANTS,
    PUBLIC,
    SEASON_ALL_TABLE,
    SEASON_FACT_TABLE,
    SEASON_TABLE,
    SUPER_TABLE,
    LongFlyWriter,
    aggregate_expr,
    build_filtered_where,
    chunk_names,
    consistency_expr,
    discover_aggregate_columns,
    distinct_text_count_expr,
    distinct_text_list_expr,
    fetch_columns,
    fetch_rows,
    load_env,
    ppg_alltime_col,
    ppg_cols_for_variant,
    q,
    q_ident,
    rank_specs_for_scope,
    season_fact_adjustment_cols,
    table_exists,
    validate_rank_specs,
    weighted_expr,
)


CONFIRM_TOKEN = "REFRESH_NFL_ANCIENT_AFFECTED_AGGREGATES"
EXPORT_DIR = ancient_apply_output_root()
AUDIT_SCHEMA = "___ops.nfl_historical"


TARGETS = {
    "season": {
        "table": SEASON_TABLE,
        "include_playoffs": False,
        "season": True,
        "rank_scope": "season",
    },
    "season_all": {
        "table": SEASON_ALL_TABLE,
        "include_playoffs": True,
        "season": True,
        "rank_scope": "season",
    },
    "career": {
        "table": CAREER_TABLE,
        "include_playoffs": False,
        "season": False,
        "rank_scope": "alltime",
    },
    "career_all": {
        "table": CAREER_ALL_TABLE,
        "include_playoffs": True,
        "season": False,
        "rank_scope": "alltime",
    },
}


def normalize_prefix(prefix: str) -> str:
    return prefix if prefix.startswith("ancient_upsert_") else f"ancient_upsert_{prefix}"


def suffix(prefix: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", normalize_prefix(prefix))


def audit_table(prefix: str, name: str) -> str:
    return f"{AUDIT_SCHEMA}.{normalize_prefix(prefix)}_{name}"


def stage_table(prefix: str, short: str, name: str) -> str:
    return f"{PUBLIC}.{suffix(prefix)}_{short}_{name}"


def phase_filter(include_playoffs: bool, alias: str | None = None) -> str:
    col = f"{alias}.season_type" if alias else "season_type"
    return f"{col} IN ('REG', 'POST')" if include_playoffs else f"{col} = 'REG'"


def key_join_sql(key_table: str, *, season: bool, source_alias: str = "s", key_alias: str = "k") -> str:
    if season:
        return (
            f"JOIN {key_table} AS {key_alias} "
            f"ON {key_alias}.NFL_player_id = {source_alias}.NFL_player_id "
            f"AND {key_alias}.year = CAST({source_alias}.year AS INTEGER)"
        )
    return f"JOIN {key_table} AS {key_alias} ON {key_alias}.NFL_player_id = {source_alias}.NFL_player_id"


def target_join_condition(*, season: bool, left_alias: str = "t", right_alias: str = "k") -> str:
    if season:
        return f"{left_alias}.NFL_player_id = {right_alias}.NFL_player_id AND {left_alias}.year = {right_alias}.year"
    return f"{left_alias}.NFL_player_id = {right_alias}.NFL_player_id"


def count_rows(writer: LongFlyWriter, table: str) -> int:
    rows = fetch_rows(writer, f"SELECT COUNT(*) AS n FROM {table}")
    return int(rows[0].get("n") or 0) if rows else 0


def create_touch_weekly(writer: LongFlyWriter, prefix: str) -> str:
    insert_stage = audit_table(prefix, "weekly_insert_stage")
    null_fill = audit_table(prefix, "null_fill_cells")
    for table in (insert_stage, null_fill):
        if not table_exists(writer, table):
            raise RuntimeError(f"Missing apply audit table: {table}")

    touch = stage_table(prefix, "weekly", "touch")
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {touch} AS
        WITH affected AS (
          SELECT DISTINCT player_week
          FROM {insert_stage}
          WHERE player_week IS NOT NULL
          UNION
          SELECT DISTINCT player_week
          FROM {null_fill}
          WHERE player_week IS NOT NULL
        )
        SELECT DISTINCT
          s.player_week,
          s.NFL_player_id,
          CAST(s.year AS INTEGER) AS year,
          s.season_type,
          COALESCE(s.nfl_position, s.position) AS nfl_position
        FROM {SUPER_TABLE} AS s
        JOIN affected AS a
          ON a.player_week = s.player_week
        WHERE s.NFL_player_id IS NOT NULL
          AND s.year IS NOT NULL
          AND s.week IS NOT NULL
        """,
        database="___ops",
    )
    return touch


def create_season_key_tables(writer: LongFlyWriter, prefix: str, short: str, *, include_playoffs: bool) -> tuple[str, str]:
    touch = stage_table(prefix, "weekly", "touch")
    output_keys = stage_table(prefix, short, "keys")
    calc_keys = stage_table(prefix, short, "calc_keys")
    phase = phase_filter(include_playoffs)
    super_phase = phase_filter(include_playoffs, alias="s")

    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {output_keys} AS
        WITH base AS (
          SELECT DISTINCT NFL_player_id, year
          FROM {touch}
          WHERE {phase}
        )
        SELECT NFL_player_id, year
        FROM base
        UNION
        SELECT b.NFL_player_id, b.year - 1 AS year
        FROM base AS b
        WHERE EXISTS (
          SELECT 1
          FROM {SUPER_TABLE} AS s
          WHERE s.NFL_player_id = b.NFL_player_id
            AND CAST(s.year AS INTEGER) = b.year - 1
            AND s.week IS NOT NULL
            AND {super_phase}
        )
        """,
        database="___ops",
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {calc_keys} AS
        SELECT NFL_player_id, year
        FROM {output_keys}
        UNION
        SELECT k.NFL_player_id, k.year + 1 AS year
        FROM {output_keys} AS k
        WHERE EXISTS (
          SELECT 1
          FROM {SUPER_TABLE} AS s
          WHERE s.NFL_player_id = k.NFL_player_id
            AND CAST(s.year AS INTEGER) = k.year + 1
            AND s.week IS NOT NULL
            AND {super_phase}
        )
        """,
        database="___ops",
    )
    return output_keys, calc_keys


def create_career_key_table(writer: LongFlyWriter, prefix: str, short: str, *, include_playoffs: bool) -> str:
    touch = stage_table(prefix, "weekly", "touch")
    keys = stage_table(prefix, short, "keys")
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {keys} AS
        SELECT DISTINCT NFL_player_id
        FROM {touch}
        WHERE {phase_filter(include_playoffs)}
        """,
        database="___ops",
    )
    return keys


def create_season_base_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    key_table: str,
    *,
    include_playoffs: bool,
) -> str:
    base = stage_table(prefix, short, "base")
    where = build_filtered_where(include_playoffs, alias="s")
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {base} AS
        WITH filtered AS (
          SELECT
            s.*,
            CAST(s.year AS BIGINT) * 1000000
              + CAST(s.week AS BIGINT) * 1000
              + ROW_NUMBER() OVER (
                  PARTITION BY s.NFL_player_id, s.year, s.week
                  ORDER BY s.player_week
                ) AS row_sort
          FROM {SUPER_TABLE} AS s
          {key_join_sql(key_table, season=True)}
          WHERE {where}
        )
        SELECT
          NFL_player_id,
          CAST(year AS INTEGER) AS year,
          ARG_MAX(player, row_sort) AS player,
          MODE(COALESCE(nfl_position, position)) AS nfl_position,
          ARG_MAX(nfl_team, row_sort) AS nfl_team,
          {distinct_text_list_expr("nfl_team")} AS nfl_teams,
          CAST({distinct_text_count_expr("nfl_team")} AS INTEGER) AS nfl_team_count,
          CAST(ARG_MAX(nfl_franchise_number, row_sort) AS INTEGER) AS nfl_franchise_number,
          ARG_MAX(headshot_url, row_sort) AS headshot_url,
          CAST(COUNT(*) AS INTEGER) AS games_played,
          CURRENT_TIMESTAMP AS last_updated
        FROM filtered
        GROUP BY NFL_player_id, year
        """,
        database="___ops",
    )
    return base


def create_career_base_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    key_table: str,
    *,
    include_playoffs: bool,
) -> str:
    base = stage_table(prefix, short, "base")
    where = build_filtered_where(include_playoffs, alias="s")
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {base} AS
        WITH filtered AS (
          SELECT
            s.*,
            CAST(s.year AS BIGINT) * 1000000
              + CAST(s.week AS BIGINT) * 1000
              + ROW_NUMBER() OVER (
                  PARTITION BY s.NFL_player_id, s.year, s.week
                  ORDER BY s.player_week
                ) AS row_sort
          FROM {SUPER_TABLE} AS s
          {key_join_sql(key_table, season=False)}
          WHERE {where}
        )
        SELECT
          NFL_player_id,
          ARG_MAX(player, row_sort) AS player,
          MODE(COALESCE(nfl_position, position)) AS nfl_position,
          ARG_MAX(nfl_team, row_sort) AS nfl_team,
          {distinct_text_list_expr("nfl_team")} AS nfl_teams,
          CAST({distinct_text_count_expr("nfl_team")} AS INTEGER) AS nfl_team_count,
          CAST(ARG_MAX(nfl_franchise_number, row_sort) AS INTEGER) AS nfl_franchise_number,
          ARG_MAX(headshot_url, row_sort) AS headshot_url,
          CAST(MIN(year) AS INTEGER) AS first_year,
          CAST(MAX(year) AS INTEGER) AS last_year,
          CAST(COUNT(DISTINCT year) AS INTEGER) AS years_active,
          CAST(COUNT(*) AS INTEGER) AS games_played,
          CURRENT_TIMESTAMP AS last_updated
        FROM filtered
        GROUP BY NFL_player_id
        """,
        database="___ops",
    )
    return base


def season_fact_adjustment_cte_for_keys(
    cols: list[str],
    key_table: str,
    *,
    include_playoffs: bool,
    season: bool,
) -> str:
    key_select = "f.NFL_player_id, CAST(f.year AS INTEGER) AS year" if season else "f.NFL_player_id"
    group_by = "f.NFL_player_id, f.year" if season else "f.NFL_player_id"
    join_sql = (
        "JOIN {key_table} AS k ON k.NFL_player_id = f.NFL_player_id AND k.year = CAST(f.year AS INTEGER)"
        if season
        else "JOIN {key_table} AS k ON k.NFL_player_id = f.NFL_player_id"
    ).format(key_table=key_table)
    phase = "f.season_type IN ('REG', 'POST')" if include_playoffs else "f.season_type = 'REG'"
    stat_filter = ", ".join(q(col) for col in cols)
    selects = ",\n        ".join(
        f"ROUND(SUM(CASE WHEN f.stat_name = {q(col)} THEN COALESCE(f.adjustment_value, 0) ELSE 0 END), 4) AS {q_ident(col)}"
        for col in cols
    )
    return f"""
    season_adj AS (
      SELECT
        {key_select},
        {selects}
      FROM {SEASON_FACT_TABLE} AS f
      {join_sql}
      WHERE f.arbitration_status = {q('season_only_no_weekly_distribution')}
        AND {phase}
        AND f.stat_name IN ({stat_filter})
      GROUP BY {group_by}
    )
    """


def create_aggregate_chunk_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    key_table: str,
    *,
    idx: int,
    cols: list[str],
    include_playoffs: bool,
    season: bool,
    use_season_fact_adjustments: bool,
) -> tuple[str, list[str]]:
    chunk = stage_table(prefix, short, f"agg_{idx}")
    where = build_filtered_where(include_playoffs, alias="s")
    key_select = "s.NFL_player_id, CAST(s.year AS INTEGER) AS year" if season else "s.NFL_player_id"
    group_by = "s.NFL_player_id, s.year" if season else "s.NFL_player_id"
    adjustment_cols = season_fact_adjustment_cols(cols) if use_season_fact_adjustments else []
    adjustment_alias = "season_adj" if adjustment_cols else None
    cte_sql = ""
    join_adjustment_sql = ""
    if adjustment_cols:
        cte_sql = (
            "WITH "
            + season_fact_adjustment_cte_for_keys(
                adjustment_cols,
                key_table,
                include_playoffs=include_playoffs,
                season=season,
            )
        )
        if season:
            join_adjustment_sql = (
                "LEFT JOIN season_adj ON season_adj.NFL_player_id = s.NFL_player_id "
                "AND season_adj.year = CAST(s.year AS INTEGER)"
            )
        else:
            join_adjustment_sql = "LEFT JOIN season_adj ON season_adj.NFL_player_id = s.NFL_player_id"
    agg_selects = ",\n        ".join(
        aggregate_expr(col, source_alias="s", adjustment_alias=adjustment_alias) for col in cols
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {chunk} AS
        {cte_sql}
        SELECT
          {key_select},
          {agg_selects}
        FROM {SUPER_TABLE} AS s
        {key_join_sql(key_table, season=season)}
        {join_adjustment_sql}
        WHERE {where}
        GROUP BY {group_by}
        """,
        database="___ops",
    )
    return chunk, cols


def create_lamar_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    key_table: str,
    *,
    lamar_cols: list[str],
    include_playoffs: bool,
    season: bool,
) -> tuple[str, list[str]] | None:
    if not lamar_cols:
        return None
    table = stage_table(prefix, short, "lamar")
    where = build_filtered_where(include_playoffs, alias="s")
    key_select = "s.NFL_player_id, CAST(s.year AS INTEGER) AS year" if season else "s.NFL_player_id"
    group_by = "s.NFL_player_id, s.year" if season else "s.NFL_player_id"
    lamar_ppg_cols = [col.replace("lamar_", "lamar_ppg_", 1) for col in lamar_cols]
    selects = ",\n        ".join(
        [f"ROUND(SUM(COALESCE(s.{q_ident(col)}, 0)), 4) AS {q_ident(col)}" for col in lamar_cols]
        + [f"AVG(s.{q_ident(col)}) AS {q_ident(ppg_col)}" for col, ppg_col in zip(lamar_cols, lamar_ppg_cols)]
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
          {key_select},
          {selects}
        FROM {SUPER_TABLE} AS s
        {key_join_sql(key_table, season=season)}
        WHERE {where}
        GROUP BY {group_by}
        """,
        database="___ops",
    )
    return table, lamar_cols + lamar_ppg_cols


def create_season_ppg_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    output_keys: str,
    calc_keys: str,
    *,
    include_playoffs: bool,
) -> tuple[str, list[str]]:
    table = stage_table(prefix, short, "ppg")
    where = build_filtered_where(include_playoffs, alias="s")
    point_select = ",\n            ".join(f"s.{q_ident(points_col)} AS {q_ident(points_col)}" for _, _, points_col in PPG_VARIANTS)
    ppg_selects = ",\n            ".join(
        f"ROUND(AVG(CAST({q_ident(points_col)} AS DOUBLE)), 2) AS {q_ident(ppg_col)},\n"
        f"            {consistency_expr(points_col)} AS {q_ident(consistency_col)}"
        for td, ppr, points_col in PPG_VARIANTS
        for ppg_col, consistency_col, _, _ in [ppg_cols_for_variant(td, ppr)]
    )
    weighted_selects = ",\n            ".join(
        f"{weighted_expr(points_col)} AS {q_ident(weighted_col)}"
        for td, ppr, points_col in PPG_VARIANTS
        for _, _, weighted_col, _ in [ppg_cols_for_variant(td, ppr)]
    )
    next_selects = ",\n          ".join(
        f"next_g.{q_ident(ppg_col)} AS {q_ident(next_col)}"
        for td, ppr, _ in PPG_VARIANTS
        for ppg_col, _, _, next_col in [ppg_cols_for_variant(td, ppr)]
    )
    weighted_cols = [
        weighted_col for td, ppr, _ in PPG_VARIANTS for _, _, weighted_col, _ in [ppg_cols_for_variant(td, ppr)]
    ]
    cols: list[str] = []
    for td, ppr, _ in PPG_VARIANTS:
        cols.extend(ppg_cols_for_variant(td, ppr))
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        WITH filtered AS (
          SELECT
            s.NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            s.week,
            s.player_week,
            {point_select},
            CAST(s.year AS BIGINT) * 1000000
              + CAST(s.week AS BIGINT) * 1000
              + ROW_NUMBER() OVER (
                  PARTITION BY s.NFL_player_id, s.year, s.week
                  ORDER BY s.player_week
                ) AS row_sort
          FROM {SUPER_TABLE} AS s
          {key_join_sql(calc_keys, season=True)}
          WHERE {where}
        ),
        grouped AS (
          SELECT
            NFL_player_id,
            year,
            {ppg_selects}
          FROM filtered
          GROUP BY NFL_player_id, year
        ),
        ordered AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY NFL_player_id, year
              ORDER BY row_sort DESC
            ) AS rn_desc
          FROM filtered
        ),
        weighted AS (
          SELECT
            NFL_player_id,
            year,
            {weighted_selects}
          FROM ordered
          WHERE rn_desc <= 5
          GROUP BY NFL_player_id, year
        )
        SELECT
          g.*,
          {', '.join(f'w.{q_ident(col)}' for col in weighted_cols)},
          {next_selects}
        FROM grouped AS g
        JOIN {output_keys} AS ok
          ON ok.NFL_player_id = g.NFL_player_id
         AND ok.year = g.year
        LEFT JOIN weighted AS w
          ON w.NFL_player_id = g.NFL_player_id
         AND w.year = g.year
        LEFT JOIN grouped AS next_g
          ON next_g.NFL_player_id = g.NFL_player_id
         AND next_g.year = g.year + 1
        """,
        database="___ops",
    )
    return table, cols


def create_career_ppg_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    key_table: str,
    *,
    include_playoffs: bool,
) -> tuple[str, list[str]]:
    table = stage_table(prefix, short, "ppg")
    where = build_filtered_where(include_playoffs, alias="s")
    cols = [ppg_alltime_col(td, ppr) for td, ppr, _ in PPG_VARIANTS]
    selects = ",\n        ".join(
        f"ROUND(AVG(CAST(s.{q_ident(points_col)} AS DOUBLE)), 2) AS {q_ident(ppg_alltime_col(td, ppr))}"
        for td, ppr, points_col in PPG_VARIANTS
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
          s.NFL_player_id,
          {selects}
        FROM {SUPER_TABLE} AS s
        {key_join_sql(key_table, season=False)}
        WHERE {where}
        GROUP BY s.NFL_player_id
        """,
        database="___ops",
    )
    return table, cols


def create_row_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    base_table: str,
    *,
    season: bool,
    join_specs: list[tuple[str, str, list[str]]],
) -> str:
    table = stage_table(prefix, short, "row_stage")
    selects = ["b.*"]
    joins = []
    for alias, join_table, cols in join_specs:
        selects.extend(f"{alias}.{q_ident(col)} AS {q_ident(col)}" for col in cols)
        if season:
            joins.append(
                f"""
                LEFT JOIN {join_table} AS {alias}
                  ON {alias}.NFL_player_id = b.NFL_player_id
                 AND {alias}.year = b.year
                """
            )
        else:
            joins.append(
                f"""
                LEFT JOIN {join_table} AS {alias}
                  ON {alias}.NFL_player_id = b.NFL_player_id
                """
            )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
          {', '.join(selects)}
        FROM {base_table} AS b
        {' '.join(joins)}
        """,
        database="___ops",
    )
    return table


def build_row_stage(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    key_table: str,
    calc_key_table: str | None,
    *,
    target_table: str,
    aggregate_cols: list[str],
    lamar_cols: list[str],
    include_playoffs: bool,
    season: bool,
    aggregate_chunk_size: int,
    use_season_fact_adjustments: bool,
) -> str:
    if season:
        base = create_season_base_stage(writer, prefix, short, key_table, include_playoffs=include_playoffs)
    else:
        base = create_career_base_stage(writer, prefix, short, key_table, include_playoffs=include_playoffs)

    join_specs: list[tuple[str, str, list[str]]] = []
    for idx, cols in enumerate(chunk_names(aggregate_cols, aggregate_chunk_size), start=1):
        chunk_table, chunk_cols = create_aggregate_chunk_stage(
            writer,
            prefix,
            short,
            key_table,
            idx=idx,
            cols=cols,
            include_playoffs=include_playoffs,
            season=season,
            use_season_fact_adjustments=use_season_fact_adjustments,
        )
        join_specs.append((f"a{idx}", chunk_table, chunk_cols))

    if season:
        if calc_key_table is None:
            raise RuntimeError(f"{target_table} season target missing calculation key table")
        ppg_table, ppg_cols = create_season_ppg_stage(
            writer,
            prefix,
            short,
            key_table,
            calc_key_table,
            include_playoffs=include_playoffs,
        )
    else:
        ppg_table, ppg_cols = create_career_ppg_stage(
            writer,
            prefix,
            short,
            key_table,
            include_playoffs=include_playoffs,
        )
    join_specs.append(("ppg", ppg_table, ppg_cols))

    lamar_spec = create_lamar_stage(
        writer,
        prefix,
        short,
        key_table,
        lamar_cols=lamar_cols,
        include_playoffs=include_playoffs,
        season=season,
    )
    if lamar_spec is not None:
        lamar_table, lamar_join_cols = lamar_spec
        join_specs.append(("lm", lamar_table, lamar_join_cols))

    return create_row_stage(writer, prefix, short, base, season=season, join_specs=join_specs)


def backup_target_rows(writer: LongFlyWriter, prefix: str, short: str, target: str, key_table: str, *, season: bool) -> str:
    backup = stage_table(prefix, short, "backup")
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {backup} AS
        SELECT t.*
        FROM {target} AS t
        JOIN {key_table} AS k
          ON {target_join_condition(season=season, left_alias='t', right_alias='k')}
        """,
        database="___ops",
    )
    return backup


def delete_target_rows(writer: LongFlyWriter, target: str, key_table: str, *, season: bool) -> None:
    writer.execute(
        f"""
        DELETE FROM {target} AS t
        WHERE EXISTS (
          SELECT 1
          FROM {key_table} AS k
          WHERE {target_join_condition(season=season, left_alias='t', right_alias='k')}
        )
        """,
        database="___ops",
    )


def merge_target_rows(writer: LongFlyWriter, target: str, row_stage: str, *, season: bool) -> None:
    target_cols = [col.name for col in fetch_columns(writer, target)]
    stage_cols = {col.name for col in fetch_columns(writer, row_stage)}
    non_rank_cols = [col for col in target_cols if not col.startswith("rank_")]
    missing = [col for col in non_rank_cols if col not in stage_cols]
    if missing:
        raise RuntimeError(f"{target} row stage missing non-rank target columns: {', '.join(missing)}")
    update_cols = [col for col in non_rank_cols if col not in {"NFL_player_id", "year"}]
    match = target_join_condition(season=season, left_alias="t", right_alias="s")
    assignments = ",\n          ".join(f"{q_ident(col)} = s.{q_ident(col)}" for col in update_cols)
    insert_cols = ", ".join(q_ident(col) for col in non_rank_cols)
    insert_values = ", ".join(f"s.{q_ident(col)}" for col in non_rank_cols)
    writer.execute(
        f"""
        MERGE INTO {target} AS t
        USING {row_stage} AS s
          ON {match}
        WHEN MATCHED THEN UPDATE SET
          {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
          VALUES ({insert_values})
        """,
        database="___ops",
    )


def touched_positions(writer: LongFlyWriter, row_stage: str) -> set[str]:
    rows = fetch_rows(
        writer,
        f"""
        SELECT DISTINCT nfl_position
        FROM {row_stage}
        WHERE nfl_position IS NOT NULL
          AND TRIM(CAST(nfl_position AS VARCHAR)) != ''
        """,
    )
    return {str(row["nfl_position"]) for row in rows}


def create_rank_backup(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    target: str,
    key_table: str,
    *,
    season: bool,
    positions: set[str],
) -> str | None:
    if not positions:
        return None
    backup = stage_table(prefix, short, "rank_backup")
    pos_sql = ", ".join(q(pos) for pos in sorted(positions))
    if season:
        writer.execute(
            f"""
            CREATE OR REPLACE TABLE {backup} AS
            SELECT t.*
            FROM {target} AS t
            WHERE t.year IN (SELECT DISTINCT year FROM {key_table})
              AND t.nfl_position IN ({pos_sql})
            """,
            database="___ops",
        )
    else:
        writer.execute(
            f"""
            CREATE OR REPLACE TABLE {backup} AS
            SELECT t.*
            FROM {target} AS t
            WHERE t.nfl_position IN ({pos_sql})
            """,
            database="___ops",
        )
    return backup


def update_rank_columns(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    target: str,
    key_table: str,
    *,
    season: bool,
    rank_scope: str,
    row_stage: str,
    rank_chunk_size: int,
) -> dict[str, Any]:
    target_cols = {col.name for col in fetch_columns(writer, target)}
    positions = touched_positions(writer, row_stage)
    specs = [
        spec
        for spec in rank_specs_for_scope(rank_scope)
        if spec.col in target_cols
        and spec.points_col in target_cols
        and positions.intersection(spec.positions)
    ]
    affected_positions = sorted({pos for spec in specs for pos in spec.positions})
    stage = stage_table(prefix, short, "rank_stage")
    updated_cols: list[str] = []
    for chunk in chunk_names(specs, rank_chunk_size):
        for spec in chunk:
            partition = "PARTITION BY year" if season else ""
            key_select = "NFL_player_id, year" if season else "NFL_player_id"
            year_filter = f"AND year IN (SELECT DISTINCT year FROM {key_table})" if season else ""
            writer.execute(
                f"""
                CREATE OR REPLACE TABLE {stage} AS
                SELECT
                  {key_select},
                  CAST(ROW_NUMBER() OVER (
                    {partition}
                    ORDER BY {q_ident(spec.points_col)} DESC, NFL_player_id ASC
                  ) AS INTEGER) AS {q_ident(spec.col)}
                FROM {target}
                WHERE NFL_player_id IS NOT NULL
                  AND nfl_position IN ({', '.join(q(pos) for pos in spec.positions)})
                  {year_filter}
                """,
                database="___ops",
            )
            if season:
                writer.execute(
                    f"""
                    UPDATE {target} AS t
                    SET {q_ident(spec.col)} = st.{q_ident(spec.col)}
                    FROM {stage} AS st
                    WHERE t.NFL_player_id = st.NFL_player_id
                      AND t.year = st.year
                    """,
                    database="___ops",
                )
            else:
                writer.execute(
                    f"""
                    UPDATE {target} AS t
                    SET {q_ident(spec.col)} = st.{q_ident(spec.col)}
                    FROM {stage} AS st
                    WHERE t.NFL_player_id = st.NFL_player_id
                    """,
                    database="___ops",
                )
            updated_cols.append(spec.col)
    return {
        "rank_columns_updated": updated_cols,
        "rank_backup": None,
        "rank_positions": sorted(positions),
        "rank_scope_positions": affected_positions,
    }


def duplicate_key_count(writer: LongFlyWriter, target: str, *, season: bool) -> int:
    if season:
        sql = f"""
        SELECT COUNT(*) AS n
        FROM (
          SELECT NFL_player_id, year, COUNT(*) AS c
          FROM {target}
          GROUP BY NFL_player_id, year
          HAVING COUNT(*) > 1
        )
        """
    else:
        sql = f"""
        SELECT COUNT(*) AS n
        FROM (
          SELECT NFL_player_id, COUNT(*) AS c
          FROM {target}
          GROUP BY NFL_player_id
          HAVING COUNT(*) > 1
        )
        """
    rows = fetch_rows(writer, sql)
    return int(rows[0].get("n") or 0) if rows else 0


def refresh_target(
    writer: LongFlyWriter,
    prefix: str,
    short: str,
    spec: dict[str, Any],
    *,
    aggregate_cols: list[str],
    lamar_cols: list[str],
    aggregate_chunk_size: int,
    rank_chunk_size: int,
    use_season_fact_adjustments: bool,
    skip_ranks: bool,
    reuse_stages: bool,
) -> dict[str, Any]:
    target = str(spec["table"])
    season = bool(spec["season"])
    include_playoffs = bool(spec["include_playoffs"])
    if season:
        key_table, calc_key_table = create_season_key_tables(
            writer,
            prefix,
            short,
            include_playoffs=include_playoffs,
        )
    else:
        key_table = create_career_key_table(writer, prefix, short, include_playoffs=include_playoffs)
        calc_key_table = None

    key_rows = count_rows(writer, key_table)
    report: dict[str, Any] = {
        "target": target,
        "key_table": key_table,
        "key_rows": key_rows,
        "skipped": key_rows == 0,
    }
    if key_rows == 0:
        return report

    row_stage = stage_table(prefix, short, "row_stage")
    reused_row_stage = bool(reuse_stages and table_exists(writer, row_stage))
    if not reused_row_stage:
        row_stage = build_row_stage(
            writer,
            prefix,
            short,
            key_table,
            calc_key_table,
            target_table=target,
            aggregate_cols=aggregate_cols,
            lamar_cols=lamar_cols,
            include_playoffs=include_playoffs,
            season=season,
            aggregate_chunk_size=aggregate_chunk_size,
            use_season_fact_adjustments=use_season_fact_adjustments,
        )
    stage_rows = count_rows(writer, row_stage)
    before_rows = count_rows(writer, target)
    merge_target_rows(writer, target, row_stage, season=season)
    rank_report = (
        {
            "rank_columns_updated": [],
            "rank_backup": None,
            "rank_positions": [],
            "rank_scope_positions": [],
        }
        if skip_ranks
        else update_rank_columns(
            writer,
            prefix,
            short,
            target,
            key_table,
            season=season,
            rank_scope=str(spec["rank_scope"]),
            row_stage=row_stage,
            rank_chunk_size=rank_chunk_size,
        )
    )
    after_rows = count_rows(writer, target)
    refreshed_rows = count_rows(
        writer,
        f"""
        (
          SELECT t.*
          FROM {target} AS t
          JOIN {key_table} AS k
            ON {target_join_condition(season=season, left_alias='t', right_alias='k')}
        ) AS refreshed
        """,
    )
    report.update(
        {
            "row_stage": row_stage,
            "reused_row_stage": reused_row_stage,
            "row_stage_rows": stage_rows,
            "backup": None,
            "backup_rows": 0,
            "rows_before": before_rows,
            "rows_after": after_rows,
            "refreshed_rows": refreshed_rows,
            "duplicate_key_count": duplicate_key_count(writer, target, season=season),
            **rank_report,
        }
    )
    if refreshed_rows != stage_rows:
        raise RuntimeError(f"{target} refreshed row count mismatch: stage={stage_rows}, live={refreshed_rows}")
    return report


def summarize_touch(writer: LongFlyWriter, touch: str) -> dict[str, Any]:
    rows = fetch_rows(
        writer,
        f"""
        SELECT
          COUNT(*) AS rows,
          COUNT(DISTINCT player_week) AS player_weeks,
          COUNT(DISTINCT NFL_player_id) AS players,
          MIN(year) AS min_year,
          MAX(year) AS max_year
        FROM {touch}
        """,
    )
    by_phase = fetch_rows(
        writer,
        f"""
        SELECT season_type, COUNT(*) AS rows, COUNT(DISTINCT NFL_player_id) AS players
        FROM {touch}
        GROUP BY season_type
        ORDER BY season_type
        """,
    )
    payload = dict(rows[0]) if rows else {}
    payload["by_phase"] = by_phase
    return payload


def dry_run_summary(writer: LongFlyWriter, prefix: str) -> dict[str, Any]:
    insert_stage = audit_table(prefix, "weekly_insert_stage")
    null_fill = audit_table(prefix, "null_fill_cells")
    touched_rows = fetch_rows(
        writer,
        f"""
        WITH affected AS (
          SELECT DISTINCT player_week
          FROM {insert_stage}
          WHERE player_week IS NOT NULL
          UNION
          SELECT DISTINCT player_week
          FROM {null_fill}
          WHERE player_week IS NOT NULL
        )
        SELECT
          COUNT(*) AS affected_rows,
          COUNT(DISTINCT s.player_week) AS player_weeks,
          COUNT(DISTINCT s.NFL_player_id) AS players,
          MIN(CAST(s.year AS INTEGER)) AS min_year,
          MAX(CAST(s.year AS INTEGER)) AS max_year
        FROM {SUPER_TABLE} AS s
        JOIN affected AS a
          ON a.player_week = s.player_week
        WHERE s.NFL_player_id IS NOT NULL
          AND s.year IS NOT NULL
          AND s.week IS NOT NULL
        """,
    )
    return dict(touched_rows[0]) if touched_rows else {}


def write_report(payload: dict[str, Any], prefix: str) -> Path:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORT_DIR / f"{normalize_prefix(prefix)}_affected_aggregate_refresh_report.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True, help="Ancient upsert audit prefix, with or without ancient_upsert_.")
    parser.add_argument("--execute", action="store_true", help="Apply targeted aggregate refresh.")
    parser.add_argument("--confirm", default=None, help=f"Required token for writes: {CONFIRM_TOKEN}")
    parser.add_argument("--aggregate-chunk-size", type=int, default=8)
    parser.add_argument("--rank-chunk-size", type=int, default=8)
    parser.add_argument("--skip-ranks", action="store_true", help="Refresh aggregate rows but leave rank columns unchanged.")
    parser.add_argument("--reuse-stages", action="store_true", help="Reuse existing row stage tables for this prefix when present.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env()
    validate_rank_specs()
    writer = LongFlyWriter()
    prefix = normalize_prefix(args.prefix)

    if not args.execute:
        payload = {
            "mode": "dry_run",
            "prefix": prefix,
            "touch": dry_run_summary(writer, prefix),
            "note": "Run with --execute --confirm REFRESH_NFL_ANCIENT_AFFECTED_AGGREGATES to update affected aggregate rows.",
        }
        report_path = write_report(payload, prefix)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
        return

    if args.confirm != CONFIRM_TOKEN:
        raise RuntimeError(f"Live writes require --confirm {CONFIRM_TOKEN}")

    aggregate_cols, lamar_cols, _fpts_cols, _bonus_cols = discover_aggregate_columns(writer)
    use_season_fact_adjustments = table_exists(writer, SEASON_FACT_TABLE)
    touch = create_touch_weekly(writer, prefix)
    payload: dict[str, Any] = {
        "mode": "execute",
        "prefix": prefix,
        "touch_table": touch,
        "touch": summarize_touch(writer, touch),
        "aggregate_columns": len(aggregate_cols),
        "lamar_columns": len(lamar_cols),
        "season_fact_adjustments": use_season_fact_adjustments,
        "targets": {},
    }
    for short, spec in TARGETS.items():
        payload["targets"][short] = refresh_target(
            writer,
            prefix,
            short,
            spec,
            aggregate_cols=aggregate_cols,
            lamar_cols=lamar_cols,
            aggregate_chunk_size=max(1, int(args.aggregate_chunk_size)),
            rank_chunk_size=max(1, int(args.rank_chunk_size)),
            use_season_fact_adjustments=use_season_fact_adjustments,
            skip_ranks=bool(args.skip_ranks),
            reuse_stages=bool(args.reuse_stages),
        )
    report_path = write_report(payload, prefix)
    payload["report_path"] = str(report_path)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
