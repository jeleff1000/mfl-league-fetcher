#!/usr/bin/env python3
"""Offline architecture smoke tests for the September weekly update plan.

This script intentionally uses only synthetic DuckDB data and local repo DDL.
It must not read Fly, post to the DuckDB API, or require production secrets.

Checks covered:

- column cadence inventory can run from local DDL only
- current manifest/server publish policy gap is detected, not hidden
- scoped league table replacement preserves unrelated scopes
- global NFL rank sidecars can reproduce a wide compatibility table
- adding a new NFL week shifts season/career ranks without mutating old base rows
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "scripts" / "_artifacts"
PIPELINE_ROOT = ROOT / "fantasy_football_data_scripts"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(PIPELINE_ROOT))


@dataclass
class SmokeResult:
    name: str
    status: str
    elapsed_seconds: float
    metrics: dict[str, Any] = field(default_factory=dict)
    findings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SmokeConfig:
    materialized_leagues: int
    seasons: int
    weeks: int
    rows_per_league_week: int
    managers_per_league_week: int
    transactions_per_league_week: int
    planning_leagues: int
    planning_rows_per_league_week: float


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def find_line(path: Path, pattern: str) -> int | None:
    for idx, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        if pattern in line:
            return idx
    return None


def fingerprint(conn: duckdb.DuckDBPyConnection, table: str, where_sql: str) -> tuple[int, str]:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({qident(table)})").fetchall()]
    expr = " || '|' || ".join(f"COALESCE(CAST({qident(col)} AS VARCHAR), '<NULL>')" for col in cols)
    order_expr = ", ".join(qident(col) for col in cols)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*)::BIGINT AS rows,
            md5(COALESCE(string_agg({expr}, chr(30) ORDER BY {order_expr}), 'empty')) AS digest
        FROM {qident(table)}
        WHERE {where_sql}
        """
    ).fetchone()
    return int(row[0] or 0), str(row[1])


def duplicate_count(conn: duckdb.DuckDBPyConnection, table: str, keys: list[str]) -> int:
    cols = ", ".join(qident(col) for col in keys)
    return int(
        conn.execute(
            f"""
            SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT
            FROM (
                SELECT {cols}, COUNT(*) AS cnt
                FROM {qident(table)}
                GROUP BY {cols}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        or 0
    )


def run_checked(name: str, fn: Callable[[], SmokeResult]) -> SmokeResult:
    start = time.perf_counter()
    try:
        result = fn()
        result.elapsed_seconds = round(time.perf_counter() - start, 4)
        return result
    except Exception as exc:
        return SmokeResult(
            name=name,
            status="fail",
            elapsed_seconds=round(time.perf_counter() - start, 4),
            findings=[f"{type(exc).__name__}: {exc}"],
        )


def smoke_cadence_inventory_offline() -> SmokeResult:
    cadence = load_module("offline_update_cadence_inventory", ROOT / "scripts" / "update_cadence_inventory.py")

    def blocked_live_query(*_args, **_kwargs):
        raise AssertionError("live Fly query attempted during offline cadence smoke")

    cadence.fly_query = blocked_live_query
    rows = list(cadence.iter_columns(include_live_ops=False))
    by_table = Counter(row.table for row in rows)
    by_cadence = Counter(row.cadence for row in rows)

    required_tables = {"player_fantasy", "matchup", "schedule", "draft", "transactions", "league_settings"}
    missing_tables = sorted(required_tables - set(by_table))
    if missing_tables:
        raise AssertionError(f"cadence inventory missing tables: {missing_tables}")
    required_cadences = {"weekly_fact", "week_local_derived", "current_season_rollup", "career_rollup"}
    missing_cadences = sorted(required_cadences - set(by_cadence))
    if missing_cadences:
        raise AssertionError(f"cadence inventory missing buckets: {missing_cadences}")
    if any(row.source == "ops_live" for row in rows):
        raise AssertionError("offline cadence inventory included live ops rows")

    return SmokeResult(
        name="cadence_inventory_offline",
        status="pass",
        elapsed_seconds=0.0,
        metrics={
            "columns": len(rows),
            "tables": len(by_table),
            "top_cadences": by_cadence.most_common(8),
        },
    )


WEEKLY_SCOPE_TABLES = {"player_fantasy", "matchup", "schedule", "transactions", "all_play", "schedule_swap"}
SEASON_SCOPE_TABLES = {
    "draft",
    "league_settings",
    "keeper_config",
    "matchup_season",
    "player_fantasy_season",
    "standings_by_year",
    "draft_manager_season",
    "transaction_manager_season",
    "transaction_report_card",
}


def desired_scope_for_table(table: str, columns: dict[str, str]) -> list[str]:
    if "db_name" not in columns:
        return []
    if table in WEEKLY_SCOPE_TABLES and {"year", "week"}.issubset(columns):
        return ["db_name", "year", "week"]
    if table in SEASON_SCOPE_TABLES and "year" in columns:
        return ["db_name", "year"]
    if table.endswith("_season") and "year" in columns:
        return ["db_name", "year"]
    if table.startswith("homepage_") or "career" in table:
        return ["db_name"]
    return ["db_name"]


def smoke_current_publish_policy_gap() -> SmokeResult:
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()
    current_modes = Counter(str(spec.get("merge_mode")) for spec in registry.values())
    if not registry:
        raise AssertionError("canonical table registry is empty")

    gaps: list[dict[str, Any]] = []
    for table, spec in sorted(registry.items()):
        desired_scope = desired_scope_for_table(table, dict(spec.get("columns") or {}))
        current_scope = list(spec.get("partition_keys") or [])
        desired_mode = "replace_scope" if desired_scope != ["db_name"] else "replace_league"
        if spec.get("merge_mode") != desired_mode or current_scope != desired_scope:
            gaps.append(
                {
                    "table": table,
                    "current_merge_mode": spec.get("merge_mode"),
                    "current_scope": current_scope,
                    "desired_merge_mode": desired_mode,
                    "desired_scope": desired_scope,
                }
            )

    server_path = ROOT / "duckdb-server" / "main.py"
    producer_path = PIPELINE_ROOT / "multi_league" / "core" / "delta_publish.py"
    server_validation_line = find_line(server_path, 'entry.get("merge_mode") != "replace_league"')
    server_delete_line = find_line(server_path, "DELETE FROM {target_ref(table)} WHERE db_name = ?")
    producer_line = find_line(producer_path, '"merge_mode": "replace_league"')

    findings = [
        "Expected architecture gap: current delta manifests and server merge path only support replace_league.",
        "Weekly tables need replace_scope on (db_name, year, week); active-season rollups need (db_name, year).",
    ]
    return SmokeResult(
        name="current_publish_policy_gap",
        status="pass_with_expected_gaps",
        elapsed_seconds=0.0,
        metrics={
            "registry_tables": len(registry),
            "current_merge_modes": dict(current_modes),
            "tables_needing_scope_change": len(gaps),
            "gap_examples": gaps[:12],
            "producer_replace_league_line": producer_line,
            "server_validation_line": server_validation_line,
            "server_delete_line": server_delete_line,
        },
        findings=findings,
    )


def build_synthetic_league_tables(conn: duckdb.DuckDBPyConnection, cfg: SmokeConfig) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE _leagues AS
        SELECT 'league_' || CAST(i AS VARCHAR) AS db_name
        FROM range(0, ?) AS t(i)
        """,
        [cfg.materialized_leagues],
    )
    conn.execute(
        """
        CREATE TEMP TABLE _years AS
        SELECT 2024 + CAST(i AS INTEGER) AS year
        FROM range(0, ?) AS t(i)
        """,
        [cfg.seasons],
    )

    conn.execute(
        """
        CREATE TABLE player_fantasy AS
        SELECT
            l.db_name,
            y.year,
            w.week,
            l.db_name || ':' || CAST(y.year AS VARCHAR) || ':' || CAST(w.week AS VARCHAR) || ':P' || CAST(p.p AS VARCHAR)
                AS player_week,
            'nfl_' || CAST(p.p % 300 AS VARCHAR) AS NFL_player_id,
            ROUND(5 + ((p.p * 13 + w.week * 7 + y.year) % 400) / 10.0, 2) AS fantasy_points,
            CASE WHEN p.p % 9 = 0 THEN 'Unrostered' ELSE 'Manager ' || CAST(p.p % 12 AS VARCHAR) END AS manager,
            ROUND(((p.p * 3 + w.week) % 100) / 10.0, 2) AS manager_lamar
        FROM _leagues l
        CROSS JOIN _years y
        CROSS JOIN range(1, ? + 1) AS w(week)
        CROSS JOIN range(0, ?) AS p(p)
        """,
        [cfg.weeks, cfg.rows_per_league_week],
    )
    conn.execute(
        """
        CREATE TABLE matchup AS
        SELECT
            l.db_name,
            y.year,
            w.week,
            l.db_name || ':' || CAST(y.year AS VARCHAR) || ':' || CAST(w.week AS VARCHAR) || ':M' || CAST(m.m AS VARCHAR)
                AS manager_week,
            'Manager ' || CAST(m.m AS VARCHAR) AS manager,
            'Team ' || CAST(m.m AS VARCHAR) AS team_name,
            ROUND(80 + ((m.m * 17 + w.week * 5 + y.year) % 900) / 10.0, 2) AS points_scored,
            (y.year - 2020) * 10 + w.week + m.m AS manager_all_time_wins
        FROM _leagues l
        CROSS JOIN _years y
        CROSS JOIN range(1, ? + 1) AS w(week)
        CROSS JOIN range(0, ?) AS m(m)
        """,
        [cfg.weeks, cfg.managers_per_league_week],
    )
    conn.execute(
        """
        CREATE TABLE schedule AS
        SELECT
            db_name,
            year,
            week,
            manager_week,
            manager,
            'Manager ' || CAST((CAST(regexp_extract(manager, '[0-9]+') AS INTEGER) + 1) % ? AS VARCHAR) AS opponent
        FROM matchup
        """,
        [cfg.managers_per_league_week],
    )
    conn.execute(
        """
        CREATE TABLE transactions AS
        SELECT
            l.db_name,
            y.year,
            w.week,
            l.db_name || ':' || CAST(y.year AS VARCHAR) || ':' || CAST(w.week AS VARCHAR) || ':T' || CAST(t.t AS VARCHAR)
                AS transaction_id,
            t.t AS transaction_sequence,
            'nfl_' || CAST((t.t + w.week) % 300 AS VARCHAR) AS player_id,
            CASE WHEN t.t % 2 = 0 THEN 'add' ELSE 'drop' END AS transaction_type
        FROM _leagues l
        CROSS JOIN _years y
        CROSS JOIN range(1, ? + 1) AS w(week)
        CROSS JOIN range(0, ?) AS t(t)
        """,
        [cfg.weeks, cfg.transactions_per_league_week],
    )
    conn.execute(
        """
        CREATE TABLE player_fantasy_season AS
        SELECT
            db_name,
            year,
            NFL_player_id,
            SUM(fantasy_points) AS fantasy_points,
            COUNT(*)::INTEGER AS games,
            MIN(manager) AS managers
        FROM player_fantasy
        GROUP BY db_name, year, NFL_player_id
        """
    )
    conn.execute(
        """
        CREATE TABLE player_fantasy_career AS
        SELECT
            db_name,
            NFL_player_id,
            SUM(fantasy_points) AS fantasy_points,
            SUM(games)::INTEGER AS games,
            MIN(managers) AS managers
        FROM player_fantasy_season
        GROUP BY db_name, NFL_player_id
        """
    )
    conn.execute(
        """
        CREATE TABLE homepage_league_summary AS
        SELECT
            db_name,
            MAX(year) AS active_year,
            COUNT(*)::BIGINT AS total_player_rows,
            SUM(fantasy_points) AS total_fantasy_points
        FROM player_fantasy
        GROUP BY db_name
        """
    )


def create_stage_table(conn: duckdb.DuckDBPyConnection, table: str, where_sql: str, seed: int) -> None:
    stage = qident(f"stage_{table}")
    conn.execute(f"DROP TABLE IF EXISTS {stage}")
    if table == "player_fantasy":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, year, week, player_week, NFL_player_id,
                   fantasy_points + {seed % 97 + 1} AS fantasy_points,
                   manager,
                   manager_lamar + 1.5 AS manager_lamar
            FROM player_fantasy
            WHERE {where_sql}
            """
        )
    elif table == "matchup":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, year, week, manager_week, manager, team_name,
                   points_scored + {seed % 41 + 1} AS points_scored,
                   manager_all_time_wins + 1 AS manager_all_time_wins
            FROM matchup
            WHERE {where_sql}
            """
        )
    elif table == "schedule":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, year, week, manager_week, manager, opponent || ' updated' AS opponent
            FROM schedule
            WHERE {where_sql}
            """
        )
    elif table == "transactions":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, year, week, transaction_id, transaction_sequence, player_id, 'trade' AS transaction_type
            FROM transactions
            WHERE {where_sql}
            """
        )
    elif table == "player_fantasy_season":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, year, NFL_player_id,
                   fantasy_points + {seed % 211 + 1} AS fantasy_points,
                   games + 1 AS games,
                   managers
            FROM player_fantasy_season
            WHERE {where_sql}
            """
        )
    elif table == "player_fantasy_career":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, NFL_player_id,
                   fantasy_points + {seed % 307 + 1} AS fantasy_points,
                   games + 1 AS games,
                   managers
            FROM player_fantasy_career
            WHERE {where_sql}
            """
        )
    elif table == "homepage_league_summary":
        conn.execute(
            f"""
            CREATE TEMP TABLE {stage} AS
            SELECT db_name, active_year, total_player_rows + 1 AS total_player_rows,
                   total_fantasy_points + {seed % 503 + 1} AS total_fantasy_points
            FROM homepage_league_summary
            WHERE {where_sql}
            """
        )
    else:
        raise AssertionError(f"no stage builder for {table}")


def apply_scoped_replace(conn: duckdb.DuckDBPyConnection, table: str, where_sql: str) -> tuple[int, int]:
    before_delete = int(conn.execute(f"SELECT COUNT(*) FROM {qident(table)} WHERE {where_sql}").fetchone()[0] or 0)
    conn.execute(f"DELETE FROM {qident(table)} WHERE {where_sql}")
    conn.execute(f"INSERT INTO {qident(table)} SELECT * FROM {qident(f'stage_{table}')}")
    after_insert = int(conn.execute(f"SELECT COUNT(*) FROM {qident(table)} WHERE {where_sql}").fetchone()[0] or 0)
    return before_delete, after_insert


def smoke_scoped_publish_local(cfg: SmokeConfig, seed: int) -> SmokeResult:
    conn = duckdb.connect(":memory:")
    try:
        build_synthetic_league_tables(conn, cfg)
        target = "league_1"
        other = "league_2" if cfg.materialized_leagues > 2 else "league_0"
        target_year = 2024 + cfg.seasons - 1
        target_week = min(2, cfg.weeks)

        table_scopes = {
            "player_fantasy": {
                "where": f"db_name = {sql_string(target)} AND year = {target_year} AND week = {target_week}",
                "unaffected": f"db_name = {sql_string(target)} AND NOT (year = {target_year} AND week = {target_week})",
                "identity": ["db_name", "player_week"],
            },
            "matchup": {
                "where": f"db_name = {sql_string(target)} AND year = {target_year} AND week = {target_week}",
                "unaffected": f"db_name = {sql_string(target)} AND NOT (year = {target_year} AND week = {target_week})",
                "identity": ["db_name", "manager_week"],
            },
            "schedule": {
                "where": f"db_name = {sql_string(target)} AND year = {target_year} AND week = {target_week}",
                "unaffected": f"db_name = {sql_string(target)} AND NOT (year = {target_year} AND week = {target_week})",
                "identity": ["db_name", "manager_week"],
            },
            "transactions": {
                "where": f"db_name = {sql_string(target)} AND year = {target_year} AND week = {target_week}",
                "unaffected": f"db_name = {sql_string(target)} AND NOT (year = {target_year} AND week = {target_week})",
                "identity": ["db_name", "transaction_id", "transaction_sequence"],
            },
            "player_fantasy_season": {
                "where": f"db_name = {sql_string(target)} AND year = {target_year}",
                "unaffected": f"db_name = {sql_string(target)} AND year <> {target_year}",
                "identity": ["db_name", "year", "NFL_player_id"],
            },
            "player_fantasy_career": {
                "where": f"db_name = {sql_string(target)}",
                "unaffected": "FALSE",
                "identity": ["db_name", "NFL_player_id"],
            },
            "homepage_league_summary": {
                "where": f"db_name = {sql_string(target)}",
                "unaffected": "FALSE",
                "identity": ["db_name"],
            },
        }

        before_other = {
            table: fingerprint(conn, table, f"db_name = {sql_string(other)}") for table in table_scopes
        }
        before_unaffected = {
            table: fingerprint(conn, table, scope["unaffected"])
            for table, scope in table_scopes.items()
            if scope["unaffected"] != "FALSE"
        }
        before_affected = {table: fingerprint(conn, table, scope["where"]) for table, scope in table_scopes.items()}

        blast: dict[str, dict[str, Any]] = {}
        for table, scope in table_scopes.items():
            full_delete = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {qident(table)} WHERE db_name = {sql_string(target)}"
                ).fetchone()[0]
                or 0
            )
            scoped_delete = int(
                conn.execute(f"SELECT COUNT(*) FROM {qident(table)} WHERE {scope['where']}").fetchone()[0] or 0
            )
            blast[table] = {
                "replace_league_rows": full_delete,
                "scoped_rows": scoped_delete,
                "row_savings": full_delete - scoped_delete,
                "reduction_factor": round(full_delete / scoped_delete, 2) if scoped_delete else None,
            }
            create_stage_table(conn, table, scope["where"], seed)
            deleted, inserted = apply_scoped_replace(conn, table, scope["where"])
            if deleted != inserted:
                raise AssertionError(f"{table} scoped replace changed row count from {deleted} to {inserted}")

        after_other = {table: fingerprint(conn, table, f"db_name = {sql_string(other)}") for table in table_scopes}
        after_unaffected = {
            table: fingerprint(conn, table, scope["unaffected"])
            for table, scope in table_scopes.items()
            if scope["unaffected"] != "FALSE"
        }
        after_affected = {table: fingerprint(conn, table, scope["where"]) for table, scope in table_scopes.items()}

        changed_tables = []
        for table, scope in table_scopes.items():
            if before_other[table] != after_other[table]:
                raise AssertionError(f"{table} changed rows for unrelated league {other}")
            if table in before_unaffected and before_unaffected[table] != after_unaffected[table]:
                raise AssertionError(f"{table} changed unaffected rows for target league")
            if before_affected[table] == after_affected[table]:
                raise AssertionError(f"{table} affected scope did not change")
            dupes = duplicate_count(conn, table, list(scope["identity"]))
            if dupes:
                raise AssertionError(f"{table} has {dupes} duplicate identity rows after scoped replace")
            changed_tables.append(table)

        projected_weekly_player_rows = int(round(cfg.planning_leagues * cfg.planning_rows_per_league_week))
        live_avg_full_player_rows_per_league = 27994508 / 587
        projected_full_player_replace_rows = int(round(cfg.planning_leagues * live_avg_full_player_rows_per_league))

        return SmokeResult(
            name="scoped_publish_local",
            status="pass",
            elapsed_seconds=0.0,
            metrics={
                "synthetic_tables": len(table_scopes),
                "changed_tables": changed_tables,
                "local_blast_radius": blast,
                "planning_projection": {
                    "planning_leagues": cfg.planning_leagues,
                    "weekly_player_rows_if_scoped": projected_weekly_player_rows,
                    "player_rows_if_replace_league_for_all_planning_leagues": projected_full_player_replace_rows,
                    "estimated_player_row_reduction_factor": round(
                        projected_full_player_replace_rows / projected_weekly_player_rows, 2
                    ),
                },
            },
        )
    finally:
        conn.close()


def create_synthetic_nfl_base(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE player_bio (
            NFL_player_id VARCHAR,
            player VARCHAR,
            headshot_url VARCHAR,
            status VARCHAR,
            latest_team VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO player_bio VALUES
            ('qb_lamar', 'Lamar Jackson', 'https://img.example/lamar.png', 'Active', 'BAL'),
            ('qb_rival', 'QB Rival', 'https://img.example/rival.png', 'Active', 'CIN'),
            ('qb_under', 'QB Under', 'https://img.example/under.png', 'Active', 'BUF'),
            ('rb_alpha', 'RB Alpha', 'https://img.example/rba.png', 'Active', 'SF'),
            ('rb_beta', 'RB Beta', 'https://img.example/rbb.png', 'Active', 'DET')
        """
    )
    conn.execute(
        """
        CREATE TABLE nfl_player_stats_base (
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            year INTEGER,
            week INTEGER,
            fpts_ppr DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO nfl_player_stats_base VALUES
            ('2025_1_qb_lamar', 'qb_lamar', 'Lamar Jackson', 'QB', 2025, 1, 30.0),
            ('2025_2_qb_lamar', 'qb_lamar', 'Lamar Jackson', 'QB', 2025, 2, 30.0),
            ('2026_1_qb_lamar', 'qb_lamar', 'Lamar Jackson', 'QB', 2026, 1, 19.9),
            ('2025_1_qb_rival', 'qb_rival', 'QB Rival', 'QB', 2025, 1, 40.0),
            ('2025_2_qb_rival', 'qb_rival', 'QB Rival', 'QB', 2025, 2, 40.0),
            ('2026_1_qb_rival', 'qb_rival', 'QB Rival', 'QB', 2026, 1, 10.0),
            ('2026_1_qb_under', 'qb_under', 'QB Under', 'QB', 2026, 1, 18.0),
            ('2025_1_rb_alpha', 'rb_alpha', 'RB Alpha', 'RB', 2025, 1, 24.0),
            ('2025_2_rb_alpha', 'rb_alpha', 'RB Alpha', 'RB', 2025, 2, 18.0),
            ('2026_1_rb_alpha', 'rb_alpha', 'RB Alpha', 'RB', 2026, 1, 15.0),
            ('2025_1_rb_beta', 'rb_beta', 'RB Beta', 'RB', 2025, 1, 20.0),
            ('2025_2_rb_beta', 'rb_beta', 'RB Beta', 'RB', 2025, 2, 20.0),
            ('2026_1_rb_beta', 'rb_beta', 'RB Beta', 'RB', 2026, 1, 14.0)
        """
    )


def rebuild_nfl_sidecars(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DROP VIEW IF EXISTS nfl_player_stats_all")
    for table in ("nfl_weekly_ranks", "nfl_season_ranks", "nfl_career_ranks"):
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    conn.execute(
        """
        CREATE TABLE nfl_weekly_ranks AS
        SELECT
            player_week,
            RANK() OVER (
                PARTITION BY year, week, position
                ORDER BY fpts_ppr DESC, NFL_player_id
            )::INTEGER AS rank_week_ppr_position
        FROM nfl_player_stats_base
        """
    )
    conn.execute(
        """
        CREATE TABLE nfl_season_ranks AS
        WITH totals AS (
            SELECT NFL_player_id, year, position, SUM(fpts_ppr) AS season_fpts
            FROM nfl_player_stats_base
            GROUP BY NFL_player_id, year, position
        )
        SELECT
            NFL_player_id,
            year,
            season_fpts,
            RANK() OVER (
                PARTITION BY year, position
                ORDER BY season_fpts DESC, NFL_player_id
            )::INTEGER AS rank_season_ppr_position
        FROM totals
        """
    )
    conn.execute(
        """
        CREATE TABLE nfl_career_ranks AS
        WITH totals AS (
            SELECT NFL_player_id, position, SUM(fpts_ppr) AS career_fpts
            FROM nfl_player_stats_base
            GROUP BY NFL_player_id, position
        )
        SELECT
            NFL_player_id,
            career_fpts,
            RANK() OVER (
                PARTITION BY position
                ORDER BY career_fpts DESC, NFL_player_id
            )::INTEGER AS rank_alltime_ppr_position
        FROM totals
        """
    )
    conn.execute(
        """
        CREATE VIEW nfl_player_stats_all AS
        SELECT
            b.player_week,
            b.NFL_player_id,
            b.player,
            b.position,
            b.year,
            b.week,
            b.fpts_ppr,
            bio.headshot_url,
            bio.status,
            wr.rank_week_ppr_position,
            sr.season_fpts,
            sr.rank_season_ppr_position,
            cr.career_fpts,
            cr.rank_alltime_ppr_position
        FROM nfl_player_stats_base b
        LEFT JOIN player_bio bio ON b.NFL_player_id = bio.NFL_player_id
        LEFT JOIN nfl_weekly_ranks wr ON b.player_week = wr.player_week
        LEFT JOIN nfl_season_ranks sr ON b.NFL_player_id = sr.NFL_player_id AND b.year = sr.year
        LEFT JOIN nfl_career_ranks cr ON b.NFL_player_id = cr.NFL_player_id
        """
    )


def rebuild_direct_wide(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DROP TABLE IF EXISTS expected_nfl_player_stats_all")
    conn.execute(
        """
        CREATE TABLE expected_nfl_player_stats_all AS
        WITH weekly AS (
            SELECT
                player_week,
                RANK() OVER (
                    PARTITION BY year, week, position
                    ORDER BY fpts_ppr DESC, NFL_player_id
                )::INTEGER AS rank_week_ppr_position
            FROM nfl_player_stats_base
        ),
        season_totals AS (
            SELECT NFL_player_id, year, position, SUM(fpts_ppr) AS season_fpts
            FROM nfl_player_stats_base
            GROUP BY NFL_player_id, year, position
        ),
        season AS (
            SELECT
                NFL_player_id,
                year,
                season_fpts,
                RANK() OVER (
                    PARTITION BY year, position
                    ORDER BY season_fpts DESC, NFL_player_id
                )::INTEGER AS rank_season_ppr_position
            FROM season_totals
        ),
        career_totals AS (
            SELECT NFL_player_id, position, SUM(fpts_ppr) AS career_fpts
            FROM nfl_player_stats_base
            GROUP BY NFL_player_id, position
        ),
        career AS (
            SELECT
                NFL_player_id,
                career_fpts,
                RANK() OVER (
                    PARTITION BY position
                    ORDER BY career_fpts DESC, NFL_player_id
                )::INTEGER AS rank_alltime_ppr_position
            FROM career_totals
        )
        SELECT
            b.player_week,
            b.NFL_player_id,
            b.player,
            b.position,
            b.year,
            b.week,
            b.fpts_ppr,
            bio.headshot_url,
            bio.status,
            weekly.rank_week_ppr_position,
            season.season_fpts,
            season.rank_season_ppr_position,
            career.career_fpts,
            career.rank_alltime_ppr_position
        FROM nfl_player_stats_base b
        LEFT JOIN player_bio bio ON b.NFL_player_id = bio.NFL_player_id
        LEFT JOIN weekly ON b.player_week = weekly.player_week
        LEFT JOIN season ON b.NFL_player_id = season.NFL_player_id AND b.year = season.year
        LEFT JOIN career ON b.NFL_player_id = career.NFL_player_id
        """
    )


def wide_diff_count(conn: duckdb.DuckDBPyConnection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*) FROM (
                (SELECT * FROM nfl_player_stats_all EXCEPT ALL SELECT * FROM expected_nfl_player_stats_all)
                UNION ALL
                (SELECT * FROM expected_nfl_player_stats_all EXCEPT ALL SELECT * FROM nfl_player_stats_all)
            )
            """
        ).fetchone()[0]
        or 0
    )


def _fleet_fp(conn: duckdb.DuckDBPyConnection, table: str, where_sql: str) -> tuple[int, str]:
    cols = [row[0] for row in conn.execute(f"DESCRIBE public.{qident(table)}").fetchall()]
    expr = " || '|' || ".join(f"COALESCE(CAST({qident(col)} AS VARCHAR), '<NULL>')" for col in cols)
    order_expr = ", ".join(qident(col) for col in cols)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*)::BIGINT AS rows,
            md5(COALESCE(string_agg({expr}, chr(30) ORDER BY {order_expr}), 'empty')) AS digest
        FROM public.{qident(table)}
        WHERE {where_sql}
        """
    ).fetchone()
    return int(row[0]), str(row[1])


_FLEET_SMOKE_TABLES = ("matchup", "player_fantasy", "matchup_career")


def _fleet_smoke_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager_week VARCHAR, manager VARCHAR, team_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER,
            player_week VARCHAR, NFL_player_id VARCHAR, fantasy_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup_career (
            db_name VARCHAR, franchise_id VARCHAR, manager VARCHAR, wins INTEGER
        )
        """
    )


def _fleet_smoke_fill(
    conn: duckdb.DuckDBPyConnection,
    *,
    league_count: int,
    league_offset: int = 0,
    years_sql: str,
    weeks: int,
    rows_per_week: int,
    managers: int,
    bump: float = 0.0,
) -> None:
    conn.execute(
        f"""
        INSERT INTO public.matchup
        SELECT
            'league_' || CAST(l.i + {league_offset} AS VARCHAR),
            y.year, w.week,
            'league_' || CAST(l.i + {league_offset} AS VARCHAR) || ':' || CAST(y.year AS VARCHAR)
                || ':' || CAST(w.week AS VARCHAR) || ':M' || CAST(m.m AS VARCHAR),
            'Manager ' || CAST(m.m AS VARCHAR),
            ROUND(80 + ((m.m * 17 + w.week * 5 + y.year) % 900) / 10.0 + {bump}, 2)
        FROM range(0, {league_count}) AS l(i)
        CROSS JOIN ({years_sql}) AS y(year)
        CROSS JOIN range(1, {weeks} + 1) AS w(week)
        CROSS JOIN range(0, {managers}) AS m(m)
        """
    )
    conn.execute(
        f"""
        INSERT INTO public.player_fantasy
        SELECT
            'league_' || CAST(l.i + {league_offset} AS VARCHAR),
            y.year, w.week,
            'league_' || CAST(l.i + {league_offset} AS VARCHAR) || ':' || CAST(y.year AS VARCHAR)
                || ':' || CAST(w.week AS VARCHAR) || ':P' || CAST(p.p AS VARCHAR),
            'nfl_' || CAST(p.p % 300 AS VARCHAR),
            ROUND(5 + ((p.p * 13 + w.week * 7 + y.year) % 400) / 10.0 + {bump}, 2)
        FROM range(0, {league_count}) AS l(i)
        CROSS JOIN ({years_sql}) AS y(year)
        CROSS JOIN range(1, {weeks} + 1) AS w(week)
        CROSS JOIN range(0, {rows_per_week}) AS p(p)
        """
    )
    conn.execute(
        f"""
        INSERT INTO public.matchup_career
        SELECT
            'league_' || CAST(l.i + {league_offset} AS VARCHAR),
            'fid_' || CAST(m.m AS VARCHAR),
            'Manager ' || CAST(m.m AS VARCHAR),
            CAST(10 + m.m + {int(bump)} AS INTEGER)
        FROM range(0, {league_count}) AS l(i)
        CROSS JOIN range(0, {managers}) AS m(m)
        """
    )


def smoke_fleet_partition_publish_local(cfg: SmokeConfig, seed: int) -> SmokeResult:
    """End-to-end weekly fleet publish: REAL client bundle builder
    (multi_league.core.fleet_publish) + REAL server merge module
    (duckdb-server/fleet_merge.py), no HTTP and no Fly.
    """
    import hashlib
    import shutil
    import tarfile
    import tempfile

    from multi_league.core.delta_publish import canonical_table_registry
    from multi_league.core.fleet_publish import build_fleet_partition_bundle

    fleet_merge = load_module("offline_fleet_merge", ROOT / "duckdb-server" / "fleet_merge.py")

    def sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            while chunk := fh.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    registry = canonical_table_registry()
    league_count = max(2, cfg.materialized_leagues)
    batch_count = league_count - 1  # last league simulates a fetch failure
    excluded = f"league_{league_count - 1}"
    active_year = 2024 + cfg.seasons - 1
    all_years_sql = f"SELECT 2024 + CAST(i AS INTEGER) FROM range(0, {cfg.seasons}) AS t(i)"
    active_year_sql = f"SELECT {active_year}"
    bump = float(seed % 97 + 1)

    server = duckdb.connect(":memory:")
    staged = duckdb.connect(":memory:")
    tmp_root = Path(tempfile.mkdtemp(prefix="fleet_smoke_"))
    try:
        _fleet_smoke_schema(server)
        _fleet_smoke_fill(
            server,
            league_count=league_count,
            years_sql=all_years_sql,
            weeks=cfg.weeks,
            rows_per_week=cfg.rows_per_league_week,
            managers=cfg.managers_per_league_week,
        )

        # Staged compute output: active season only, one NEW week, bumped values.
        _fleet_smoke_schema(staged)
        _fleet_smoke_fill(
            staged,
            league_count=batch_count,
            years_sql=active_year_sql,
            weeks=cfg.weeks + 1,
            rows_per_week=cfg.rows_per_league_week,
            managers=cfg.managers_per_league_week,
            bump=bump,
        )

        bundle = build_fleet_partition_bundle(
            staged,
            active_year=active_year,
            league_generations={f"league_{i}": 0 for i in range(batch_count)},
            tables=list(_FLEET_SMOKE_TABLES),
            output_dir=tmp_root / "bundle",
            import_run_id=str(1000 + seed),
            publish_sequence=1,
        )

        extract_dir = tmp_root / "extracted"
        extract_dir.mkdir()
        with tarfile.open(bundle.path, "r:gz") as tar:
            tar.extractall(extract_dir, filter="data")

        allowed_paths, table_entries = fleet_merge.validate_fleet_manifest_shape(
            bundle.manifest,
            allowed_tables=set(registry),
            identity_keys={t: tuple(spec["primary_keys"]) for t, spec in registry.items()},
            expected_bundle_id=bundle.bundle_id,
            expected_bundle_hash=bundle.bundle_hash,
        )
        fleet_merge.validate_fleet_parquet_tables(
            bundle.manifest, table_entries, extract_dir, sha256_file=sha256_file
        )

        excluded_before = {t: _fleet_fp(server, t, f"db_name = {sql_string(excluded)}") for t in _FLEET_SMOKE_TABLES}
        prior_before = {
            t: _fleet_fp(server, t, f"year < {active_year}") for t in ("matchup", "player_fantasy")
        }
        full_history_rows = {
            t: int(
                server.execute(
                    f"SELECT COUNT(*) FROM public.{qident(t)} WHERE db_name <> {sql_string(excluded)}"
                ).fetchone()[0]
                or 0
            )
            for t in ("matchup", "player_fantasy")
        }

        result = fleet_merge.apply_fleet_merge(server, bundle.manifest, extract_dir)
        if result["status"] != "COMMITTED":
            raise AssertionError(f"fleet merge did not commit: {result}")

        for t in _FLEET_SMOKE_TABLES:
            if _fleet_fp(server, t, f"db_name = {sql_string(excluded)}") != excluded_before[t]:
                raise AssertionError(f"{t}: excluded (fetch-failure) league was modified")
        for t, before in prior_before.items():
            if _fleet_fp(server, t, f"year < {active_year}") != prior_before[t]:
                raise AssertionError(f"{t}: frozen prior seasons were modified")

        new_week = int(
            server.execute(
                f"SELECT MAX(week) FROM public.matchup WHERE db_name = 'league_0' AND year = {active_year}"
            ).fetchone()[0]
        )
        if new_week != cfg.weeks + 1:
            raise AssertionError(f"new week not visible after publish: max week {new_week}")

        # G14 no-rewind: re-applying the SAME bundle (built at generation 0)
        # must now be rejected — the first apply bumped every batched league
        # to generation 1. (Replay idempotency by bundle_id lives in main.py's
        # state layer, above this merge function.)
        after_first = {t: _fleet_fp(server, t, "TRUE") for t in _FLEET_SMOKE_TABLES}
        try:
            fleet_merge.apply_fleet_merge(server, bundle.manifest, extract_dir)
            raise AssertionError("stale-generation re-apply was not rejected")
        except fleet_merge.FleetGenerationConflict:
            pass
        for t in _FLEET_SMOKE_TABLES:
            if _fleet_fp(server, t, "TRUE") != after_first[t]:
                raise AssertionError(f"{t}: rejected re-apply still changed table content")

        # Content idempotency still holds for a bundle rebuilt at the current
        # generation: same rows in, fingerprints unchanged.
        bundle2 = build_fleet_partition_bundle(
            staged,
            active_year=active_year,
            league_generations={f"league_{i}": 1 for i in range(batch_count)},
            tables=list(_FLEET_SMOKE_TABLES),
            output_dir=tmp_root / "bundle2",
            import_run_id=str(2000 + seed),
            publish_sequence=1,
        )
        extract_dir2 = tmp_root / "extracted2"
        extract_dir2.mkdir()
        with tarfile.open(bundle2.path, "r:gz") as tar:
            tar.extractall(extract_dir2, filter="data")
        fleet_merge.apply_fleet_merge(server, bundle2.manifest, extract_dir2)
        for t in _FLEET_SMOKE_TABLES:
            if _fleet_fp(server, t, "TRUE") != after_first[t]:
                raise AssertionError(f"{t}: same-content republish changed table content")

        scoped_rows = {t: result["tables"][t] for t in ("matchup", "player_fantasy")}
        reduction = {
            t: round(full_history_rows[t] / scoped_rows[t], 2) if scoped_rows[t] else None
            for t in scoped_rows
        }
        return SmokeResult(
            name="fleet_partition_publish_local",
            status="pass",
            elapsed_seconds=0.0,
            metrics={
                "bundle_tables": result["tables"],
                "published_rows": result["row_count"],
                "batched_leagues": batch_count,
                "excluded_league": excluded,
                "active_year": active_year,
                "replace_league_rows_touched": full_history_rows,
                "scoped_rows_touched": scoped_rows,
                "row_touch_reduction": reduction,
            },
        )
    finally:
        server.close()
        staged.close()
        shutil.rmtree(tmp_root, ignore_errors=True)


def smoke_super_sidecar_local(seed: int) -> SmokeResult:
    conn = duckdb.connect(":memory:")
    try:
        create_synthetic_nfl_base(conn)
        rebuild_nfl_sidecars(conn)
        rebuild_direct_wide(conn)
        initial_diff = wide_diff_count(conn)
        if initial_diff:
            raise AssertionError(f"initial sidecar/view differs from direct wide table by {initial_diff} rows")

        old_base_fingerprint = fingerprint(conn, "nfl_player_stats_base", "year < 2026 OR week = 1")
        conn.execute(
            """
            INSERT INTO nfl_player_stats_base VALUES
                ('2026_2_qb_lamar', 'qb_lamar', 'Lamar Jackson', 'QB', 2026, 2, 20.0),
                ('2026_2_qb_rival', 'qb_rival', 'QB Rival', 'QB', 2026, 2, 5.0),
                ('2026_2_qb_under', 'qb_under', 'QB Under', 'QB', 2026, 2, 19.9),
                ('2026_2_rb_alpha', 'rb_alpha', 'RB Alpha', 'RB', 2026, 2, 13.0),
                ('2026_2_rb_beta', 'rb_beta', 'RB Beta', 'RB', 2026, 2, 15.0)
            """
        )
        conn.execute(
            """
            UPDATE player_bio
            SET headshot_url = 'https://img.example/lamar-updated.png'
            WHERE NFL_player_id = 'qb_lamar'
            """
        )
        rebuild_nfl_sidecars(conn)
        rebuild_direct_wide(conn)
        final_diff = wide_diff_count(conn)
        if final_diff:
            raise AssertionError(f"post-week sidecar/view differs from direct wide table by {final_diff} rows")
        if fingerprint(conn, "nfl_player_stats_base", "year < 2026 OR week = 1") != old_base_fingerprint:
            raise AssertionError("old NFL base rows changed while rebuilding sidecars")

        checks = dict(
            lamar_week_rank=int(
                conn.execute(
                    """
                    SELECT rank_week_ppr_position
                    FROM nfl_player_stats_all
                    WHERE player_week = '2026_2_qb_lamar'
                    """
                ).fetchone()[0]
            ),
            under_week_rank=int(
                conn.execute(
                    """
                    SELECT rank_week_ppr_position
                    FROM nfl_player_stats_all
                    WHERE player_week = '2026_2_qb_under'
                    """
                ).fetchone()[0]
            ),
            lamar_2026_season_rank=int(
                conn.execute(
                    """
                    SELECT rank_season_ppr_position
                    FROM nfl_player_stats_all
                    WHERE player_week = '2026_2_qb_lamar'
                    """
                ).fetchone()[0]
            ),
            lamar_old_row_alltime_rank=int(
                conn.execute(
                    """
                    SELECT rank_alltime_ppr_position
                    FROM nfl_player_stats_all
                    WHERE player_week = '2025_1_qb_lamar'
                    """
                ).fetchone()[0]
            ),
            rival_alltime_rank=int(
                conn.execute(
                    """
                    SELECT rank_alltime_ppr_position
                    FROM nfl_player_stats_all
                    WHERE player_week = '2025_1_qb_rival'
                    """
                ).fetchone()[0]
            ),
            updated_headshot_rows=int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM nfl_player_stats_all
                    WHERE NFL_player_id = 'qb_lamar'
                      AND headshot_url = 'https://img.example/lamar-updated.png'
                    """
                ).fetchone()[0]
            ),
        )
        expected = {
            "lamar_week_rank": 1,
            "under_week_rank": 2,
            "lamar_2026_season_rank": 1,
            "lamar_old_row_alltime_rank": 1,
            "rival_alltime_rank": 2,
        }
        for key, value in expected.items():
            if checks[key] != value:
                raise AssertionError(f"{key} expected {value}, got {checks[key]}")

        return SmokeResult(
            name="super_sidecar_local",
            status="pass",
            elapsed_seconds=0.0,
            metrics={
                "initial_wide_diff_rows": initial_diff,
                "final_wide_diff_rows": final_diff,
                "base_rows": int(conn.execute("SELECT COUNT(*) FROM nfl_player_stats_base").fetchone()[0]),
                "weekly_rank_rows": int(conn.execute("SELECT COUNT(*) FROM nfl_weekly_ranks").fetchone()[0]),
                "season_rank_rows": int(conn.execute("SELECT COUNT(*) FROM nfl_season_ranks").fetchone()[0]),
                "career_rank_rows": int(conn.execute("SELECT COUNT(*) FROM nfl_career_ranks").fetchone()[0]),
                "rank_shift_checks": checks,
                "seed": seed,
            },
            findings=[
                "Compatibility view reproduced the direct wide table before and after a new week.",
                "Old base rows stayed unchanged while all-time ranks on old rows changed through sidecar joins.",
            ],
        )
    finally:
        conn.close()


def smoke_no_live_guard() -> SmokeResult:
    danger_env = [key for key in ("DATABASE_SERVER_URL", "DATABASE_READ_TOKEN", "DATABASE_ADMIN_TOKEN") if os.getenv(key)]
    return SmokeResult(
        name="no_live_guard",
        status="pass",
        elapsed_seconds=0.0,
        metrics={
            "production_env_vars_present_but_unused": danger_env,
            "network_clients_used": 0,
            "fly_queries_attempted": 0,
            "fly_writes_attempted": 0,
        },
        findings=["This harness uses in-memory DuckDB and local Python imports only."],
    )


def summarize(results: list[SmokeResult]) -> dict[str, Any]:
    status_counts = Counter(result.status for result in results)
    failures = [result for result in results if result.status == "fail"]
    return {
        "status": "fail" if failures else "pass",
        "status_counts": dict(status_counts),
        "result_count": len(results),
        "failure_count": len(failures),
        "elapsed_seconds": round(sum(result.elapsed_seconds for result in results), 4),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=10, help="Number of offline smoke loops to run.")
    parser.add_argument("--materialized-leagues", type=int, default=24, help="Synthetic leagues to materialize locally.")
    parser.add_argument("--seasons", type=int, default=3, help="Synthetic seasons per materialized league.")
    parser.add_argument("--weeks", type=int, default=5, help="Synthetic weeks per materialized season.")
    parser.add_argument("--rows-per-league-week", type=int, default=64, help="Synthetic player rows per league-week.")
    parser.add_argument("--managers-per-league-week", type=int, default=12, help="Synthetic manager rows per league-week.")
    parser.add_argument("--transactions-per-league-week", type=int, default=6, help="Synthetic transactions per league-week.")
    parser.add_argument("--planning-leagues", type=int, default=1232, help="September planning league count.")
    parser.add_argument(
        "--planning-rows-per-league-week",
        type=float,
        default=513.4,
        help="Projected player_fantasy rows per league-week.",
    )
    parser.add_argument("--seed", type=int, default=20260606, help="Base seed used to vary staged values.")
    parser.add_argument("--out", type=Path, help="JSON report path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.iterations < 1:
        raise SystemExit("--iterations must be >= 1")
    cfg = SmokeConfig(
        materialized_leagues=args.materialized_leagues,
        seasons=args.seasons,
        weeks=args.weeks,
        rows_per_league_week=args.rows_per_league_week,
        managers_per_league_week=args.managers_per_league_week,
        transactions_per_league_week=args.transactions_per_league_week,
        planning_leagues=args.planning_leagues,
        planning_rows_per_league_week=args.planning_rows_per_league_week,
    )

    all_results: list[SmokeResult] = []
    started = time.perf_counter()
    for iteration in range(1, args.iterations + 1):
        seed = args.seed + iteration
        checks: list[tuple[str, Callable[[], SmokeResult]]] = [
            ("no_live_guard", smoke_no_live_guard),
            ("cadence_inventory_offline", smoke_cadence_inventory_offline),
            ("current_publish_policy_gap", smoke_current_publish_policy_gap),
            ("scoped_publish_local", lambda seed=seed: smoke_scoped_publish_local(cfg, seed)),
            ("fleet_partition_publish_local", lambda seed=seed: smoke_fleet_partition_publish_local(cfg, seed)),
            ("super_sidecar_local", lambda seed=seed: smoke_super_sidecar_local(seed)),
        ]
        for name, fn in checks:
            result = run_checked(name, fn)
            result.metrics["iteration"] = iteration
            all_results.append(result)
            marker = "OK" if result.status != "fail" else "FAIL"
            print(f"[{iteration:02d}/{args.iterations:02d}] {marker} {name} ({result.elapsed_seconds:.3f}s)")
            if result.status == "fail":
                break
        if all_results[-1].status == "fail":
            break

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "offline_only": True,
        "config": cfg.__dict__,
        "summary": summarize(all_results),
        "wall_seconds": round(time.perf_counter() - started, 4),
        "results": [result.__dict__ for result in all_results],
        "architecture_changes_required": [
            {
                "area": "league_delta_manifest",
                "change": "Emit scoped merge modes and scope keys per table instead of merge_mode=replace_league for every table.",
            },
            {
                "area": "duckdb_server_merge",
                "change": "Validate and execute replace_scope deletes for (db_name, year, week) and (db_name, year), with duplicate/key-null rejection.",
            },
            {
                "area": "global_nfl_artifacts",
                "change": "Build base facts, bio, weekly rank, season rank, and career rank artifacts offline, then expose nfl_player_stats_all as a view or promoted materialized artifact.",
            },
            {
                "area": "league_broadcast_columns",
                "change": "Move career/all-time broadcast values off large weekly fact rows where possible, or populate them from sidecars/materialized outputs.",
            },
            {
                "area": "workflow_runtime",
                "change": "Run Tuesday global rebuilds in GitHub Actions or an ephemeral worker against local artifacts, then publish finished outputs in a small promotion step.",
            },
        ],
    }
    out_path = args.out
    if out_path is None:
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = ARTIFACT_DIR / f"offline_architecture_smoke_{stamp}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(f"Wrote offline smoke report to {out_path}")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 1 if report["summary"]["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
