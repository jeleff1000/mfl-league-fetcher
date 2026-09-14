#!/usr/bin/env python3
"""Apply the combined identity promotion plan to Fly, with stage-first safety.

Default mode is read-only validation.  With --apply, the script:
1. uploads the 414 staged full-width rows into a Fly staging table,
2. builds a full corrected supertable stage,
3. refreshes dependent PPG/rolling/rank columns on that stage,
4. validates counts/keys,
5. creates a full live backup,
6. swaps the staged table into nfl_historical.nfl_player_stats_all.

The season/career derived aggregate tables are not rebuilt unless
--rebuild-aggregates is also supplied.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_ROOT = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
COMBINED_DIR = AUDIT_ROOT / "combined_identity_promotion_stage_20260509"
SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
VALIDATION_CHUNK_SIZE = 1_000
SUPER_TABLE_QUALIFIED = "___ops.nfl_historical.nfl_player_stats_all"

FPTS_VARIANTS = [
    ("4pt", "0ppr", "fpts_4pt_0ppr"),
    ("4pt", "half", "fpts_4pt_half"),
    ("4pt", "ppr", "fpts_4pt_ppr"),
    ("5pt", "0ppr", "fpts_5pt_0ppr"),
    ("5pt", "half", "fpts_5pt_half"),
    ("5pt", "ppr", "fpts_5pt_ppr"),
    ("6pt", "0ppr", "fpts_6pt_0ppr"),
    ("6pt", "half", "fpts_6pt_half"),
    ("6pt", "ppr", "fpts_6pt_ppr"),
    ("4pt", "tep", "fpts_4pt_tep"),
    ("5pt", "tep", "fpts_5pt_tep"),
    ("6pt", "tep", "fpts_6pt_tep"),
]


@dataclass(frozen=True)
class RankSpec:
    col: str
    positions: tuple[str, ...]
    points_col: str
    tiebreaker_col: str | None = None


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def safe_sql_text(value: Any) -> str:
    # The Fly /query-rw endpoint intentionally uses a simple semicolon splitter
    # for admin scripts, so semicolon-bearing literals must be normalized before
    # transport.  This mainly affects odd external headshot URLs.
    return str(value).replace(";", "%3B")


def sql_in(values: list[str]) -> str:
    if not values:
        return "('')"
    return "(" + ",".join(q_lit(value) for value in values) + ")"


def chunks(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_scalar(writer: Any, sql: str, key: str = "n") -> int:
    rows = writer.execute(sql, database="___ops")
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        return int(first.get(key) or 0)
    return int(first[0] or 0)


def schema_df() -> pd.DataFrame:
    stage_schema = COMBINED_DIR / "live_schema_columns.csv"
    if stage_schema.exists():
        return pd.read_csv(stage_schema)
    return pd.read_csv(AUDIT_ROOT / "stat_family_split_live_stage_20260509" / "live_schema_columns.csv")


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    promotion = pd.read_parquet(COMBINED_DIR / "combined_promotion_rows_full.parquet")
    delete_keys = pd.read_csv(COMBINED_DIR / "delete_player_weeks.csv")
    metric_scope = pd.read_csv(COMBINED_DIR / "metric_recompute_scope.csv")
    manifest = json.loads((COMBINED_DIR / "manifest.json").read_text(encoding="utf-8"))
    return promotion, delete_keys, metric_scope, manifest


def is_nullish(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def sql_value(value: Any, data_type: str) -> str:
    if is_nullish(value):
        return "NULL"
    dtype = str(data_type).upper()
    if dtype == "BOOLEAN":
        return "TRUE" if bool(value) else "FALSE"
    if dtype in {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"}:
        try:
            num = pd.to_numeric(value, errors="coerce")
            if pd.isna(num):
                return "NULL"
            return str(int(num))
        except Exception:
            return "NULL"
    if dtype in {"FLOAT", "REAL", "DOUBLE", "DECIMAL"} or dtype.startswith("DECIMAL"):
        try:
            num = float(value)
            if not math.isfinite(num):
                return "NULL"
            return repr(num)
        except Exception:
            return "NULL"
    if dtype.startswith("TIMESTAMP") or dtype == "DATE":
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return "NULL"
        return q_lit(safe_sql_text(value.isoformat() if hasattr(value, "isoformat") else value))
    return q_lit(safe_sql_text(value))


def row_values(row: pd.Series, columns: list[str], type_by_col: dict[str, str]) -> str:
    return "(" + ",".join(sql_value(row.get(col), type_by_col.get(col, "VARCHAR")) for col in columns) + ")"


def weekly_rank_specs() -> list[RankSpec]:
    specs = [
        RankSpec("rank_qb_4pt", ("QB",), "fpts_4pt_half", "rolling_total_4pt_half"),
        RankSpec("rank_qb_5pt", ("QB",), "fpts_5pt_half", "rolling_total_5pt_half"),
        RankSpec("rank_qb_6pt", ("QB",), "fpts_6pt_half", "rolling_total_6pt_half"),
        RankSpec("rank_k", ("K",), "pts_k_yds", "rolling_total_k"),
        RankSpec("rank_def", ("DEF",), "pts_def_std", "rolling_total_def"),
    ]
    for pos in ("RB", "WR", "TE"):
        lower = pos.lower()
        for suffix, points in (("0ppr", "fpts_4pt_0ppr"), ("half", "fpts_4pt_half"), ("ppr", "fpts_4pt_ppr")):
            specs.append(RankSpec(f"rank_{lower}_{suffix}", (pos,), points, f"rolling_total_4pt_{suffix}"))
    specs.append(RankSpec("rank_te_tep", ("TE",), "fpts_4pt_tep", "rolling_total_4pt_tep"))
    for group, positions in {
        "lb": ("LB", "ILB", "OLB", "MLB"),
        "dl": ("DL", "DE", "DT", "NT", "ED"),
        "db": ("DB", "CB", "S", "SS", "FS", "SAF"),
    }.items():
        for suffix, points in (
            ("std", "pts_idp_std"),
            ("premium", "pts_idp_premium"),
            ("tackle_heavy", "pts_idp_tackle_heavy"),
            ("big_play", "pts_idp_big_play"),
        ):
            specs.append(RankSpec(f"rank_{group}_{suffix}", positions, points))
    flex_groups = {
        "flex": ("RB", "WR", "TE"),
        "recflex": ("WR", "TE"),
        "wrflex": ("RB", "WR"),
        "rtflex": ("RB", "TE"),
    }
    for name, positions in flex_groups.items():
        for suffix, points in (("0ppr", "fpts_4pt_0ppr"), ("half", "fpts_4pt_half"), ("ppr", "fpts_4pt_ppr")):
            specs.append(RankSpec(f"rank_{name}_{suffix}", positions, points, f"rolling_total_4pt_{suffix}"))
    specs.append(RankSpec("rank_flex_tep", ("RB", "WR", "TE"), "fpts_4pt_tep", "rolling_total_4pt_ppr"))
    specs.append(RankSpec("rank_recflex_tep", ("WR", "TE"), "fpts_4pt_tep", "rolling_total_4pt_ppr"))
    for td in ("4pt", "5pt", "6pt"):
        for ppr in ("0ppr", "half", "ppr"):
            specs.append(
                RankSpec(
                    f"rank_sflex_{td}_{ppr}", ("QB", "RB", "WR", "TE"), f"fpts_{td}_{ppr}", f"rolling_total_{td}_{ppr}"
                )
            )
    for suffix, points in (
        ("std", "pts_idp_std"),
        ("premium", "pts_idp_premium"),
        ("tackle_heavy", "pts_idp_tackle_heavy"),
        ("big_play", "pts_idp_big_play"),
    ):
        specs.append(
            RankSpec(
                f"rank_idp_flex_{suffix}",
                ("LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "DB", "CB", "S", "SS", "FS", "SAF"),
                points,
            )
        )
    return specs


def aggregate_rank_specs(prefix: str) -> list[RankSpec]:
    return [
        RankSpec(f"{prefix}{spec.col.removeprefix('rank')}", spec.positions, spec.points_col)
        for spec in weekly_rank_specs()
    ]


def matching_rank_specs(scope: str, affected_cols: set[str]) -> list[RankSpec]:
    if scope == "weekly":
        specs = weekly_rank_specs()
    elif scope == "season":
        specs = aggregate_rank_specs("rank_season")
    elif scope == "alltime":
        specs = aggregate_rank_specs("rank_alltime")
    else:
        raise ValueError(scope)
    return [spec for spec in specs if spec.col in affected_cols]


def affected_rank_columns(scope: str) -> set[str]:
    path = COMBINED_DIR / "rank_recompute_scope.csv"
    df = pd.read_csv(path).fillna("")
    subset = df[df["scope"].astype(str).eq(scope)]
    cols: set[str] = set()
    for value in subset["rank_columns"].astype(str):
        cols.update(col for col in value.split(",") if col)
    manifest_path = COMBINED_DIR / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            manifest = {}
        exclusions = {str(value).lower() for value in manifest.get("exclusions", [])}
        if "idp/defense-only rows" in exclusions:
            defensive_tokens = ("_lb_", "_dl_", "_db_", "_idp_flex_")
            cols = {
                col
                for col in cols
                if not (
                    col.endswith("_def")
                    or col in {"rank_def", "rank_season_def", "rank_alltime_def"}
                    or any(token in col for token in defensive_tokens)
                )
            }
    return cols


def upload_promotion_rows(
    writer: Any, table: str, promotion: pd.DataFrame, schema: pd.DataFrame, chunk_size: int
) -> None:
    columns = schema["column_name"].astype(str).tolist()
    type_by_col = dict(zip(schema["column_name"].astype(str), schema["data_type"].astype(str)))
    select_cols = ", ".join(q_ident(col) for col in columns)
    writer.execute(
        f"CREATE OR REPLACE TABLE {table} AS SELECT {select_cols} FROM {SUPER_TABLE} WHERE 1=0", database="___ops"
    )
    rows = list(promotion.iterrows())
    for chunk_no, chunk in enumerate(chunks(rows, chunk_size), start=1):
        values_sql = ",\n".join(row_values(row, columns, type_by_col) for _, row in chunk)
        writer.execute(
            f"INSERT INTO {table} ({select_cols}) VALUES {values_sql}",
            database="___ops",
        )
        print(f"[upload] rows chunk {chunk_no}: {len(chunk)}")


def append_missing_promotion_rows(
    writer: Any, table: str, promotion: pd.DataFrame, schema: pd.DataFrame, chunk_size: int
) -> None:
    columns = schema["column_name"].astype(str).tolist()
    type_by_col = dict(zip(schema["column_name"].astype(str), schema["data_type"].astype(str)))
    select_cols = ", ".join(q_ident(col) for col in columns)
    existing_rows = writer.execute(f"SELECT player_week FROM {table}", database="___ops")
    existing_keys = {str(row["player_week"]) for row in existing_rows if row.get("player_week") is not None}
    missing = promotion[~promotion["player_week"].dropna().astype(str).isin(existing_keys)].copy()
    print(f"[upload resume] existing remote rows: {len(existing_keys):,}; remaining local rows: {len(missing):,}")
    rows = list(missing.iterrows())
    for chunk_no, chunk in enumerate(chunks(rows, chunk_size), start=1):
        values_sql = ",\n".join(row_values(row, columns, type_by_col) for _, row in chunk)
        writer.execute(
            f"INSERT INTO {table} ({select_cols}) VALUES {values_sql}",
            database="___ops",
        )
        print(f"[upload resume] rows chunk {chunk_no}: {len(chunk)}")

    remote_count = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {table}")
    if remote_count != len(promotion):
        raise RuntimeError(
            f"promotion table row count mismatch after resume: remote={remote_count}, local={len(promotion)}"
        )


def create_scope_tables(writer: Any, prefix: str, delete_keys: list[str], affected_ids: list[str]) -> None:
    delete_table = f"___ops.public.{prefix}_delete_keys"
    ids_table = f"___ops.public.{prefix}_affected_ids"
    writer.execute(f"CREATE OR REPLACE TABLE {delete_table}(player_week VARCHAR)", database="___ops")
    for chunk_no, chunk in enumerate(chunks(delete_keys, 5000), start=1):
        delete_values = ",".join(f"({q_lit(key)})" for key in chunk)
        writer.execute(f"INSERT INTO {delete_table} VALUES {delete_values}", database="___ops")
        print(f"[scope] delete key chunk {chunk_no}: {len(chunk)}")
    writer.execute(f"CREATE OR REPLACE TABLE {ids_table}(NFL_player_id VARCHAR)", database="___ops")
    for chunk_no, chunk in enumerate(chunks(affected_ids, 5000), start=1):
        id_values = ",".join(f"({q_lit(value)})" for value in chunk)
        writer.execute(f"INSERT INTO {ids_table} VALUES {id_values}", database="___ops")
        print(f"[scope] affected ID chunk {chunk_no}: {len(chunk)}")


def create_corrected_stage(
    writer: Any, stage_table: str, promotion_table: str, prefix: str, schema: pd.DataFrame
) -> None:
    columns = schema["column_name"].astype(str).tolist()
    select_cols = ", ".join(q_ident(col) for col in columns)
    select_cols_s = ", ".join(f"s.{q_ident(col)}" for col in columns)
    delete_table = f"___ops.public.{prefix}_delete_keys"
    expected = fetch_scalar(
        writer,
        f"""
        SELECT
          (SELECT COUNT(*)
           FROM {SUPER_TABLE}
           WHERE player_week NOT IN (SELECT player_week FROM {delete_table})
             AND player_week NOT IN (SELECT player_week FROM {promotion_table})) +
          (SELECT COUNT(*) FROM {promotion_table}) AS n
        """,
    )
    writer.execute(
        f"CREATE OR REPLACE TABLE {stage_table} AS SELECT {select_cols} FROM {SUPER_TABLE} WHERE 1=0",
        database="___ops",
    )
    promo_bounds = writer.execute(
        f"SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM {promotion_table}",
        database="___ops",
    )
    if promo_bounds and promo_bounds[0].get("min_year") is not None:
        min_year = int(promo_bounds[0]["min_year"])
        max_year = int(promo_bounds[0]["max_year"])
        for start in range(min_year, max_year + 1, 5):
            end = min(start + 4, max_year)
            writer.execute(
                f"""
                INSERT INTO {stage_table} ({select_cols})
                SELECT {select_cols}
                FROM {promotion_table}
                WHERE year BETWEEN {start} AND {end}
                """,
                database="___ops",
            )
            print(f"[stage] promotion years {start}-{end}")
    else:
        writer.execute(
            f"INSERT INTO {stage_table} ({select_cols}) SELECT {select_cols} FROM {promotion_table}",
            database="___ops",
        )
        print("[stage] promotion rows")

    live_bounds = writer.execute(
        f"SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM {SUPER_TABLE}",
        database="___ops",
    )
    if live_bounds and live_bounds[0].get("min_year") is not None:
        min_year = int(live_bounds[0]["min_year"])
        max_year = int(live_bounds[0]["max_year"])
        for start in range(min_year, max_year + 1, 5):
            end = min(start + 4, max_year)
            writer.execute(
                f"""
                INSERT INTO {stage_table} ({select_cols})
                SELECT {select_cols_s}
                FROM {SUPER_TABLE} AS s
                LEFT JOIN {delete_table} AS d ON s.player_week = d.player_week
                LEFT JOIN {promotion_table} AS p ON s.player_week = p.player_week
                WHERE s.year BETWEEN {start} AND {end}
                  AND d.player_week IS NULL
                  AND p.player_week IS NULL
                """,
                database="___ops",
            )
            print(f"[stage] live years {start}-{end}")

    writer.execute(
        f"""
        INSERT INTO {stage_table} ({select_cols})
        SELECT {select_cols_s}
        FROM {SUPER_TABLE} AS s
        LEFT JOIN {delete_table} AS d ON s.player_week = d.player_week
        LEFT JOIN {promotion_table} AS p ON s.player_week = p.player_week
        WHERE s.year IS NULL
          AND d.player_week IS NULL
          AND p.player_week IS NULL
        """,
        database="___ops",
    )
    actual = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {stage_table}")
    if actual != expected:
        raise RuntimeError(f"stage count mismatch: expected={expected}, actual={actual}")
    dupes = fetch_scalar(
        writer,
        f"SELECT COUNT(*) AS n FROM (SELECT player_week FROM {stage_table} GROUP BY player_week HAVING COUNT(*) > 1)",
    )
    if dupes:
        raise RuntimeError(f"stage has duplicate player_week keys: {dupes}")


def refresh_ppg_metrics(writer: Any, stage_table: str, prefix: str, affected_ids: list[str]) -> None:
    ids_sql = sql_in(affected_ids)
    metric_cols = []
    for td, ppr, _ in FPTS_VARIANTS:
        metric_cols.extend(
            [
                f"ppg_season_{td}_{ppr}",
                f"ppg_alltime_{td}_{ppr}",
                f"rolling_total_{td}_{ppr}",
                f"rolling_3_{td}_{ppr}",
                f"rolling_5_{td}_{ppr}",
                f"weighted_ppg_{td}_{ppr}",
                f"consistency_{td}_{ppr}",
                f"avg_pts_next_year_{td}_{ppr}",
            ]
        )
    metric_cols.extend(["rolling_total_def", "rolling_total_k"])
    set_null = ", ".join(f"{q_ident(col)} = NULL" for col in metric_cols)
    writer.execute(f"UPDATE {stage_table} SET {set_null} WHERE NFL_player_id IN {ids_sql}", database="___ops")
    for td, ppr, fpts_col in FPTS_VARIANTS:
        print(f"[metrics] {td}_{ppr}")
        season_col = f"ppg_season_{td}_{ppr}"
        alltime_col = f"ppg_alltime_{td}_{ppr}"
        rolling_total_col = f"rolling_total_{td}_{ppr}"
        rolling_3_col = f"rolling_3_{td}_{ppr}"
        rolling_5_col = f"rolling_5_{td}_{ppr}"
        weighted_col = f"weighted_ppg_{td}_{ppr}"
        consistency_col = f"consistency_{td}_{ppr}"
        next_year_col = f"avg_pts_next_year_{td}_{ppr}"
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(season_col)} = ROUND(calc.avg_pts, 2)
            FROM (
              SELECT NFL_player_id, year, AVG({q_ident(fpts_col)}) AS avg_pts
              FROM {stage_table}
              WHERE NFL_player_id IN {ids_sql} AND {q_ident(fpts_col)} IS NOT NULL
              GROUP BY NFL_player_id, year
            ) calc
            WHERE t.NFL_player_id = calc.NFL_player_id AND t.year = calc.year
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(alltime_col)} = ROUND(calc.avg_pts, 2)
            FROM (
              SELECT NFL_player_id, AVG({q_ident(fpts_col)}) AS avg_pts
              FROM {stage_table}
              WHERE NFL_player_id IN {ids_sql} AND {q_ident(fpts_col)} IS NOT NULL
              GROUP BY NFL_player_id
            ) calc
            WHERE t.NFL_player_id = calc.NFL_player_id
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            CREATE OR REPLACE TABLE ___ops.public.{prefix}_metric_calc AS
            SELECT
              player_week,
              SUM({q_ident(fpts_col)}) OVER (
                PARTITION BY NFL_player_id, year
                ORDER BY week
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
              ) AS rolling_total,
              AVG({q_ident(fpts_col)}) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
              ) AS rolling_3,
              AVG({q_ident(fpts_col)}) OVER (
                PARTITION BY NFL_player_id
                ORDER BY year, week
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
              ) AS rolling_5,
              (
                COALESCE(LAG({q_ident(fpts_col)}, 0) OVER w * 5, 0) +
                COALESCE(LAG({q_ident(fpts_col)}, 1) OVER w * 4, 0) +
                COALESCE(LAG({q_ident(fpts_col)}, 2) OVER w * 3, 0) +
                COALESCE(LAG({q_ident(fpts_col)}, 3) OVER w * 2, 0) +
                COALESCE(LAG({q_ident(fpts_col)}, 4) OVER w * 1, 0)
              ) / NULLIF(
                (CASE WHEN LAG({q_ident(fpts_col)}, 0) OVER w IS NOT NULL THEN 5 ELSE 0 END) +
                (CASE WHEN LAG({q_ident(fpts_col)}, 1) OVER w IS NOT NULL THEN 4 ELSE 0 END) +
                (CASE WHEN LAG({q_ident(fpts_col)}, 2) OVER w IS NOT NULL THEN 3 ELSE 0 END) +
                (CASE WHEN LAG({q_ident(fpts_col)}, 3) OVER w IS NOT NULL THEN 2 ELSE 0 END) +
                (CASE WHEN LAG({q_ident(fpts_col)}, 4) OVER w IS NOT NULL THEN 1 ELSE 0 END), 0
              ) AS weighted
            FROM {stage_table}
            WHERE NFL_player_id IN {ids_sql} AND {q_ident(fpts_col)} IS NOT NULL
            WINDOW w AS (PARTITION BY NFL_player_id ORDER BY year, week)
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET
              {q_ident(rolling_total_col)} = ROUND(calc.rolling_total, 2),
              {q_ident(rolling_3_col)} = ROUND(calc.rolling_3, 2),
              {q_ident(rolling_5_col)} = ROUND(calc.rolling_5, 2),
              {q_ident(weighted_col)} = ROUND(calc.weighted, 2)
            FROM ___ops.public.{prefix}_metric_calc AS calc
            WHERE t.player_week = calc.player_week
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(consistency_col)} = ROUND(calc.cv, 3)
            FROM (
              SELECT NFL_player_id, year,
                     CASE WHEN AVG({q_ident(fpts_col)}) > 0
                          THEN STDDEV({q_ident(fpts_col)}) / AVG({q_ident(fpts_col)})
                          ELSE 0 END AS cv
              FROM {stage_table}
              WHERE NFL_player_id IN {ids_sql} AND {q_ident(fpts_col)} IS NOT NULL
              GROUP BY NFL_player_id, year
            ) calc
            WHERE t.NFL_player_id = calc.NFL_player_id AND t.year = calc.year
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(next_year_col)} = ROUND(next_season.avg_pts, 2)
            FROM (
              SELECT NFL_player_id, year, AVG({q_ident(fpts_col)}) AS avg_pts
              FROM {stage_table}
              WHERE NFL_player_id IN {ids_sql} AND {q_ident(fpts_col)} IS NOT NULL
              GROUP BY NFL_player_id, year
            ) next_season
            WHERE t.NFL_player_id = next_season.NFL_player_id
              AND t.year = next_season.year - 1
              AND t.NFL_player_id IN {ids_sql}
            """,
            database="___ops",
        )
    for rolling_col, points_col, pos in (
        ("rolling_total_def", "pts_def_std", "DEF"),
        ("rolling_total_k", "pts_k_yds", "K"),
    ):
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(rolling_col)} = ROUND(calc.rolling_sum, 2)
            FROM (
              SELECT player_week,
                     SUM({q_ident(points_col)}) OVER (
                       PARTITION BY NFL_player_id, year
                       ORDER BY week
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS rolling_sum
              FROM {stage_table}
              WHERE NFL_player_id IN {ids_sql}
                AND nfl_position = {q_lit(pos)}
                AND {q_ident(points_col)} IS NOT NULL
            ) calc
            WHERE t.player_week = calc.player_week
            """,
            database="___ops",
        )


def positions_sql(positions: tuple[str, ...]) -> str:
    return "(" + ",".join(q_lit(pos) for pos in positions) + ")"


def order_sql(spec: RankSpec, aggregate: bool = False, table_alias: str | None = None) -> str:
    prefix = f"{table_alias}." if table_alias else ""
    points = f"COALESCE({prefix}{q_ident(spec.points_col)}, -1000000000)"
    if aggregate or not spec.tiebreaker_col:
        return f"{points} DESC, {prefix}{q_ident('NFL_player_id')} ASC"
    tie = f"COALESCE({prefix}{q_ident(spec.tiebreaker_col)}, -1000000000)"
    return f"{points} DESC, {tie} DESC, {prefix}{q_ident('NFL_player_id')} ASC"


def refresh_weekly_ranks(writer: Any, stage_table: str, prefix: str) -> None:
    rank_scope = pd.read_csv(COMBINED_DIR / "rank_recompute_scope.csv").fillna("")
    weekly_scope = rank_scope[rank_scope["scope"].eq("weekly")][["year", "week"]].drop_duplicates()
    week_values = ",".join(f"({int(r.year)}, {int(r.week)})" for r in weekly_scope.itertuples(index=False))
    week_table = f"___ops.public.{prefix}_weekly_rank_scope"
    writer.execute(f"CREATE OR REPLACE TABLE {week_table}(year INTEGER, week INTEGER)", database="___ops")
    writer.execute(f"INSERT INTO {week_table} VALUES {week_values}", database="___ops")
    affected_cols = affected_rank_columns("weekly")
    for spec in matching_rank_specs("weekly", affected_cols):
        print(f"[rank weekly] {spec.col}")
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(spec.col)} = NULL
            FROM {week_table} AS scope
            WHERE t.year = scope.year AND t.week = scope.week
            """,
            database="___ops",
        )
        rank_table = f"___ops.public.{prefix}_{spec.col}_rank"
        writer.execute(
            f"""
            CREATE OR REPLACE TABLE {rank_table} AS
            SELECT
              player_week,
              CAST(ROW_NUMBER() OVER (
                PARTITION BY s.year, s.week
                ORDER BY {order_sql(spec, table_alias="s")}
              ) AS INTEGER) AS new_rank
            FROM {stage_table} AS s
            JOIN {week_table} AS scope
              ON s.year = scope.year AND s.week = scope.week
            WHERE s.nfl_position IN {positions_sql(spec.positions)}
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(spec.col)} = r.new_rank
            FROM {rank_table} AS r
            WHERE t.player_week = r.player_week
            """,
            database="___ops",
        )


def refresh_season_ranks(writer: Any, stage_table: str, prefix: str) -> None:
    rank_scope = pd.read_csv(COMBINED_DIR / "rank_recompute_scope.csv").fillna("")
    years = sorted(
        {
            int(v)
            for v in pd.to_numeric(rank_scope.loc[rank_scope["scope"].eq("season"), "year"], errors="coerce").dropna()
        }
    )
    if not years:
        return
    year_sql = "(" + ",".join(str(year) for year in years) + ")"
    affected_cols = affected_rank_columns("season")
    for spec in matching_rank_specs("season", affected_cols):
        print(f"[rank season] {spec.col}")
        writer.execute(
            f"UPDATE {stage_table} SET {q_ident(spec.col)} = NULL WHERE year IN {year_sql}", database="___ops"
        )
        rank_table = f"___ops.public.{prefix}_{spec.col}_rank"
        writer.execute(
            f"""
            CREATE OR REPLACE TABLE {rank_table} AS
            WITH totals AS (
              SELECT NFL_player_id, year, SUM({q_ident(spec.points_col)}) AS points
              FROM {stage_table}
              WHERE year IN {year_sql}
                AND nfl_position IN {positions_sql(spec.positions)}
                AND NFL_player_id IS NOT NULL
              GROUP BY NFL_player_id, year
            )
            SELECT
              NFL_player_id,
              year,
              CAST(ROW_NUMBER() OVER (
                PARTITION BY year
                ORDER BY COALESCE(points, -1000000000) DESC, NFL_player_id ASC
              ) AS INTEGER) AS new_rank
            FROM totals
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(spec.col)} = r.new_rank
            FROM {rank_table} AS r
            WHERE t.NFL_player_id = r.NFL_player_id AND t.year = r.year
            """,
            database="___ops",
        )


def refresh_alltime_ranks(writer: Any, stage_table: str, prefix: str) -> None:
    affected_cols = affected_rank_columns("alltime")
    for spec in matching_rank_specs("alltime", affected_cols):
        print(f"[rank alltime] {spec.col}")
        writer.execute(f"UPDATE {stage_table} SET {q_ident(spec.col)} = NULL", database="___ops")
        rank_table = f"___ops.public.{prefix}_{spec.col}_rank"
        writer.execute(
            f"""
            CREATE OR REPLACE TABLE {rank_table} AS
            WITH totals AS (
              SELECT NFL_player_id, SUM({q_ident(spec.points_col)}) AS points
              FROM {stage_table}
              WHERE nfl_position IN {positions_sql(spec.positions)}
                AND NFL_player_id IS NOT NULL
              GROUP BY NFL_player_id
            )
            SELECT
              NFL_player_id,
              CAST(ROW_NUMBER() OVER (
                ORDER BY COALESCE(points, -1000000000) DESC, NFL_player_id ASC
              ) AS INTEGER) AS new_rank
            FROM totals
            """,
            database="___ops",
        )
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(spec.col)} = r.new_rank
            FROM {rank_table} AS r
            WHERE t.NFL_player_id = r.NFL_player_id
            """,
            database="___ops",
        )


def validate_live_inputs(
    writer: Any, promotion: pd.DataFrame, delete_keys: list[str], manifest: dict[str, Any]
) -> dict[str, Any]:
    live_rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE}")
    live_delete_found = 0
    for chunk in chunks(delete_keys, VALIDATION_CHUNK_SIZE):
        live_delete_found += fetch_scalar(
            writer, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}"
        )
    promotion_keys = promotion["player_week"].dropna().astype(str).tolist()
    live_promotion_key_found = 0
    for chunk in chunks(promotion_keys, VALIDATION_CHUNK_SIZE):
        live_promotion_key_found += fetch_scalar(
            writer, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}"
        )
    live_replaced_or_deleted_found = 0
    for chunk in chunks(sorted(set(delete_keys).union(promotion_keys)), VALIDATION_CHUNK_SIZE):
        live_replaced_or_deleted_found += fetch_scalar(
            writer, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}"
        )
    return {
        "live_rows_before": live_rows,
        "delete_keys": len(delete_keys),
        "live_delete_found": live_delete_found,
        "promotion_rows": len(promotion),
        "live_promotion_key_found": live_promotion_key_found,
        "live_replaced_or_deleted_found": live_replaced_or_deleted_found,
        "expected_stage_rows": live_rows - live_replaced_or_deleted_found + len(promotion),
        "local_manifest_promotion_rows": manifest.get("promotion_rows"),
        "local_manifest_delete_player_weeks": manifest.get("delete_player_weeks"),
        "pass": live_delete_found == len(delete_keys) and len(promotion) == manifest.get("promotion_rows"),
    }


def validate_live_inputs_remote(
    writer: Any,
    promotion_table: str,
    promotion: pd.DataFrame,
    delete_keys: list[str],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    live_rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE}")
    remote_rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {promotion_table}")
    remote_distinct_keys = fetch_scalar(
        writer,
        f"SELECT COUNT(DISTINCT player_week) AS n FROM {promotion_table} WHERE player_week IS NOT NULL",
    )
    remote_null_keys = fetch_scalar(
        writer,
        f"SELECT COUNT(*) AS n FROM {promotion_table} WHERE player_week IS NULL",
    )
    live_promotion_key_found = fetch_scalar(
        writer,
        f"""
        SELECT COUNT(*) AS n
        FROM {promotion_table} p
        INNER JOIN {SUPER_TABLE} s
          ON p.player_week = s.player_week
        """,
    )
    live_delete_found = 0
    for chunk in chunks(delete_keys, VALIDATION_CHUNK_SIZE):
        live_delete_found += fetch_scalar(
            writer, f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}"
        )
    live_replaced_or_deleted_found = live_promotion_key_found
    for chunk in chunks(delete_keys, VALIDATION_CHUNK_SIZE):
        live_replaced_or_deleted_found += fetch_scalar(
            writer,
            f"""
            SELECT COUNT(*) AS n
            FROM {SUPER_TABLE}
            WHERE player_week IN {sql_in(chunk)}
              AND player_week NOT IN (SELECT player_week FROM {promotion_table})
            """,
        )
    return {
        "live_rows_before": live_rows,
        "delete_keys": len(delete_keys),
        "live_delete_found": live_delete_found,
        "promotion_rows": len(promotion),
        "remote_promotion_rows": remote_rows,
        "remote_distinct_player_week": remote_distinct_keys,
        "remote_null_player_week": remote_null_keys,
        "live_promotion_key_found": live_promotion_key_found,
        "live_replaced_or_deleted_found": live_replaced_or_deleted_found,
        "expected_stage_rows": live_rows - live_replaced_or_deleted_found + remote_rows,
        "local_manifest_promotion_rows": manifest.get("promotion_rows"),
        "local_manifest_delete_player_weeks": manifest.get("delete_player_weeks"),
        "pass": (
            live_delete_found == len(delete_keys)
            and len(promotion) == manifest.get("promotion_rows")
            and remote_rows == len(promotion)
            and remote_distinct_keys == len(promotion)
            and remote_null_keys == 0
        ),
    }


def validate_stage(writer: Any, stage_table: str, promotion: pd.DataFrame, delete_keys: list[str]) -> dict[str, Any]:
    promotion_keys = set(promotion["player_week"].dropna().astype(str))
    delete_only = sorted(set(delete_keys) - promotion_keys)
    promo_found = 0
    for chunk in chunks(sorted(promotion_keys), VALIDATION_CHUNK_SIZE):
        promo_found += fetch_scalar(
            writer, f"SELECT COUNT(*) AS n FROM {stage_table} WHERE player_week IN {sql_in(chunk)}"
        )
    delete_only_found = 0
    for chunk in chunks(delete_only, VALIDATION_CHUNK_SIZE):
        delete_only_found += fetch_scalar(
            writer, f"SELECT COUNT(*) AS n FROM {stage_table} WHERE player_week IN {sql_in(chunk)}"
        )
    dupes = fetch_scalar(
        writer,
        f"SELECT COUNT(*) AS n FROM (SELECT player_week FROM {stage_table} GROUP BY player_week HAVING COUNT(*) > 1)",
    )
    rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {stage_table}")
    return {
        "stage_rows": rows,
        "promotion_keys_present": promo_found,
        "delete_only_keys_present": delete_only_found,
        "duplicate_player_week_keys": dupes,
        "pass": promo_found == len(promotion_keys) and delete_only_found == 0 and dupes == 0,
    }


def can_reuse_stage(writer: Any, stage_table: str, expected_rows: int) -> bool:
    table_name = stage_table.split(".")[-1]
    exists = fetch_scalar(
        writer,
        f"""
        SELECT COUNT(*) AS n
        FROM information_schema.tables
        WHERE table_catalog = '___ops'
          AND table_schema = 'public'
          AND table_name = {q_lit(table_name)}
        """,
    )
    if not exists:
        return False
    rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {stage_table}")
    dupes = fetch_scalar(
        writer,
        f"SELECT COUNT(*) AS n FROM (SELECT player_week FROM {stage_table} GROUP BY player_week HAVING COUNT(*) > 1)",
    )
    reusable = rows == expected_rows and dupes == 0
    print(f"[stage] reuse check: exists=1 rows={rows:,} expected={expected_rows:,} dupes={dupes}")
    return reusable


def can_reuse_backup(writer: Any, backup_table: str, expected_rows: int) -> bool:
    table_name = backup_table.split(".")[-1]
    exists = fetch_scalar(
        writer,
        f"""
        SELECT COUNT(*) AS n
        FROM information_schema.tables
        WHERE table_catalog = '___ops'
          AND table_schema = 'public'
          AND table_name = {q_lit(table_name)}
        """,
    )
    if not exists:
        return False
    rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {backup_table}")
    dupes = fetch_scalar(
        writer,
        f"SELECT COUNT(*) AS n FROM (SELECT player_week FROM {backup_table} GROUP BY player_week HAVING COUNT(*) > 1)",
    )
    reusable = rows == expected_rows and dupes == 0
    print(f"[backup] reuse check: exists=1 rows={rows:,} expected={expected_rows:,} dupes={dupes}")
    return reusable


def main() -> int:
    global COMBINED_DIR

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply", action="store_true", help="Build staged table and swap it into live after validation."
    )
    parser.add_argument(
        "--rebuild-aggregates", action="store_true", help="Rebuild derived season/career ops tables after live swap."
    )
    parser.add_argument("--insert-chunk-size", type=int, default=8)
    parser.add_argument(
        "--reuse-promotion-table",
        default="",
        help="Existing ___ops promotion rows table to reuse instead of creating a new upload table.",
    )
    parser.add_argument(
        "--resume-upload",
        action="store_true",
        help="When --reuse-promotion-table is set, append only promotion rows missing from that remote table before staging.",
    )
    parser.add_argument(
        "--reuse-stage-table",
        action="store_true",
        help="Reuse an existing valid stage table and continue with metric/rank refresh plus swap.",
    )
    parser.add_argument(
        "--reuse-refreshed-stage",
        action="store_true",
        help="Reuse an existing already-refreshed stage table and continue with validation, backup, swap, and aggregates.",
    )
    parser.add_argument(
        "--stage-dir",
        type=Path,
        default=COMBINED_DIR,
        help="Directory containing combined_promotion_rows_full.parquet, delete_player_weeks.csv, metric_recompute_scope.csv, rank_recompute_scope.csv, and manifest.json.",
    )
    args = parser.parse_args()
    COMBINED_DIR = args.stage_dir
    created_at_utc = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    load_env()
    from multi_league.core.fly_writer import FlyWriter

    FlyWriter.TIMEOUT_SECONDS = max(FlyWriter.TIMEOUT_SECONDS, 600)
    promotion, delete_df, metric_scope, manifest = load_inputs()
    delete_keys = delete_df["player_week"].dropna().astype(str).tolist()
    affected_ids = sorted(metric_scope["NFL_player_id"].dropna().astype(str).unique().tolist())
    schema = schema_df()
    if args.reuse_promotion_table:
        promotion_table = args.reuse_promotion_table
        table_name = promotion_table.split(".")[-1]
        if not table_name.endswith("_rows"):
            raise SystemExit("--reuse-promotion-table must point to a table ending in _rows")
        prefix = table_name.removesuffix("_rows")
    else:
        prefix = f"identity_promotion_20260509_{created_at_utc}"
        promotion_table = f"___ops.public.{prefix}_rows"
    stage_table = f"___ops.public.{prefix}_stage"
    backup_table = f"___ops.public.{prefix}_live_backup"

    writer = FlyWriter()
    stage_validation = None
    aggregate_counts = None
    if args.apply:
        if args.reuse_promotion_table:
            if args.resume_upload:
                print("[apply] resuming promotion row upload")
                append_missing_promotion_rows(writer, promotion_table, promotion, schema, args.insert_chunk_size)
            else:
                remote_count = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {promotion_table}")
                if remote_count != len(promotion):
                    raise SystemExit(
                        f"Existing promotion table row count mismatch: remote={remote_count}, local={len(promotion)}. "
                        "Use --resume-upload to append missing rows."
                    )
        else:
            print("[apply] uploading promotion rows")
            upload_promotion_rows(writer, promotion_table, promotion, schema, args.insert_chunk_size)
        live_validation = validate_live_inputs_remote(writer, promotion_table, promotion, delete_keys, manifest)
        if not live_validation["pass"]:
            raise SystemExit("Live input validation failed: " + json.dumps(live_validation, sort_keys=True))
        if args.reuse_stage_table and can_reuse_stage(writer, stage_table, live_validation["expected_stage_rows"]):
            print("[apply] reusing corrected full stage table")
        else:
            create_scope_tables(writer, prefix, delete_keys, affected_ids)
            print("[apply] creating corrected full stage table")
            create_corrected_stage(writer, stage_table, promotion_table, prefix, schema)
        if args.reuse_refreshed_stage:
            print("[apply] reusing already-refreshed PPG/rank columns")
        else:
            print("[apply] refreshing PPG/rolling metrics")
            refresh_ppg_metrics(writer, stage_table, prefix, affected_ids)
            print("[apply] refreshing weekly ranks")
            refresh_weekly_ranks(writer, stage_table, prefix)
            print("[apply] refreshing season ranks")
            refresh_season_ranks(writer, stage_table, prefix)
            print("[apply] refreshing all-time ranks")
            refresh_alltime_ranks(writer, stage_table, prefix)
        stage_validation = validate_stage(writer, stage_table, promotion, delete_keys)
        if not stage_validation["pass"]:
            raise SystemExit("Stage validation failed: " + json.dumps(stage_validation, sort_keys=True))
        if can_reuse_backup(writer, backup_table, live_validation["live_rows_before"]):
            print("[apply] reusing live supertable backup")
        else:
            print("[apply] backing up live supertable")
            writer.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {SUPER_TABLE}", database="___ops")
        print("[apply] swapping stage into live supertable")
        writer.execute(f"CREATE OR REPLACE TABLE {SUPER_TABLE} AS SELECT * FROM {stage_table}", database="___ops")
        if args.rebuild_aggregates:
            from multi_league.data_fetchers.aggregate_nfl_stats_fly import update_aggregates

            aggregate_counts = update_aggregates(rebuild_all_years=True)
    else:
        live_validation = validate_live_inputs(writer, promotion, delete_keys, manifest)
        if not live_validation["pass"]:
            raise SystemExit("Live input validation failed: " + json.dumps(live_validation, sort_keys=True))

    result = {
        "created_at_utc": created_at_utc,
        "applied": args.apply,
        "rebuild_aggregates": args.rebuild_aggregates,
        "promotion_table": promotion_table if args.apply else None,
        "stage_table": stage_table if args.apply else None,
        "backup_table": backup_table if args.apply else None,
        "affected_ids": affected_ids,
        "live_validation": live_validation,
        "stage_validation": stage_validation,
        "aggregate_counts": aggregate_counts,
    }
    out_path = COMBINED_DIR / ("apply_manifest.json" if args.apply else "apply_dry_run_manifest.json")
    out_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
