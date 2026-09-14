#!/usr/bin/env python3
"""Add and backfill missing PBP-derived schema columns in Fly supertable.

This is an additive live repair. It does not overwrite official box-score
columns; it only creates/fills columns that were absent from the supertable
schema but present in the cleaned PBP weekly rollup.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.data_fetchers.pbp_schema_backfill import (  # noqa: E402
    DEFAULT_ROLLUP_PATH,
    PBP_SCHEMA_BACKFILL_COLUMN_TYPES,
    PBP_SCHEMA_BACKFILL_COLUMNS,
    load_pbp_schema_backfill,
)


SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
SUPER_TABLE_FULL = "___ops.nfl_historical.nfl_player_stats_all"
ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
OUT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507" / "pbp_schema_backfill_20260508"


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
    return '"' + str(name).replace('"', '""') + '"'


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def num_lit(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    if pd.isna(numeric):
        numeric = 0.0
    return repr(numeric)


def fetch_schema(reader: Any) -> set[str]:
    schema = reader.query_df(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        """,
        database="___ops",
    )
    return set(schema["column_name"].astype(str)) if not schema.empty else set()


def summarize_live(reader: Any, columns: list[str]) -> dict[str, dict[str, float | int]]:
    existing = fetch_schema(reader)
    rows: dict[str, str] = {}
    for col in columns:
        if col in existing:
            col_sql = q_ident(col)
            rows[f"{col}__nonzero_rows"] = f"SUM(CASE WHEN COALESCE({col_sql}, 0) <> 0 THEN 1 ELSE 0 END)"
            rows[f"{col}__total"] = f"SUM(COALESCE({col_sql}, 0))"
        else:
            rows[f"{col}__nonzero_rows"] = "0"
            rows[f"{col}__total"] = "0"
    select_sql = ",\n".join(f"{expr} AS {q_ident(alias)}" for alias, expr in rows.items())
    live = reader.query_df(f"SELECT {select_sql} FROM {SUPER_TABLE}", database="___ops")
    if live.empty:
        return {col: {"nonzero_rows": 0, "total": 0.0} for col in columns}
    record = live.iloc[0].to_dict()
    return {
        col: {
            "nonzero_rows": int(record.get(f"{col}__nonzero_rows") or 0),
            "total": float(record.get(f"{col}__total") or 0.0),
        }
        for col in columns
    }


def summarize_source(source: pd.DataFrame, columns: list[str]) -> dict[str, dict[str, float | int]]:
    return {
        col: {
            "nonzero_rows": int(source[col].ne(0).sum()),
            "total": float(source[col].sum()),
        }
        for col in columns
    }


def ensure_columns(writer: Any, reader: Any, columns: list[str]) -> list[str]:
    existing = fetch_schema(reader)
    added: list[str] = []
    for col in columns:
        if col in existing:
            continue
        col_type = PBP_SCHEMA_BACKFILL_COLUMN_TYPES.get(col, "DOUBLE")
        try:
            writer.execute(
                f"ALTER TABLE {SUPER_TABLE} ADD COLUMN {q_ident(col)} {col_type} DEFAULT 0",
                database="___ops",
            )
        except RuntimeError:
            writer.execute(
                f"ALTER TABLE {SUPER_TABLE} ADD COLUMN {q_ident(col)} {col_type}",
                database="___ops",
            )
            writer.execute(
                f"UPDATE {SUPER_TABLE} SET {q_ident(col)} = 0 WHERE {q_ident(col)} IS NULL",
                database="___ops",
            )
        added.append(col)
    return added


def create_backup_table(writer: Any, columns: list[str], min_year: int, max_year: int, stamp: str) -> str:
    backup_table = f"nfl_historical.pbp_schema_backfill_backup_{stamp}"
    select_cols = ", ".join(
        q_ident(col) for col in ["player_week", "NFL_player_id", "player", "year", "week", *columns]
    )
    writer.execute(
        f"""
        CREATE TABLE {backup_table} AS
        SELECT {select_cols}
        FROM {SUPER_TABLE}
        WHERE year BETWEEN {min_year} AND {max_year}
        """,
        database="___ops",
    )
    return f"___ops.{backup_table}"


def zero_existing_values(writer: Any, columns: list[str], min_year: int, max_year: int) -> None:
    set_clause = ", ".join(f"{q_ident(col)} = 0" for col in columns)
    writer.execute(
        f"""
        UPDATE {SUPER_TABLE}
        SET {set_clause}
        WHERE year BETWEEN {min_year} AND {max_year}
        """,
        database="___ops",
    )


def nonzero_source_rows(source: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    mask = source[columns].ne(0).any(axis=1)
    rows = source.loc[mask, ["player_week", *columns]].copy()
    return rows.sort_values("player_week").reset_index(drop=True)


def apply_updates(writer: Any, rows: pd.DataFrame, columns: list[str], chunk_size: int) -> int:
    set_clause = ", ".join(f"{q_ident(col)} = v.{q_ident(col)}" for col in columns)
    updated_chunks = 0
    for start in range(0, len(rows), chunk_size):
        chunk = rows.iloc[start : start + chunk_size]
        values_sql = ",\n".join(
            "("
            + ", ".join(
                [
                    q_lit(row.player_week),
                    *[num_lit(getattr(row, col)) for col in columns],
                ]
            )
            + ")"
            for row in chunk.itertuples(index=False)
        )
        alias_cols = ", ".join(q_ident(col) for col in ["player_week", *columns])
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE} AS t
            SET {set_clause}
            FROM (VALUES {values_sql}) AS v({alias_cols})
            WHERE t.player_week = v.player_week
            """,
            database="___ops",
        )
        updated_chunks += 1
        if updated_chunks % 25 == 0:
            print(f"[update] chunks={updated_chunks} rows={min(start + chunk_size, len(rows)):,}/{len(rows):,}")
    return updated_chunks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply the live Fly schema/backfill.")
    parser.add_argument("--rollup", type=Path, default=DEFAULT_ROLLUP_PATH)
    parser.add_argument("--chunk-size", type=int, default=750)
    args = parser.parse_args()

    load_env()
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.readers.fly_reader import FlyReader

    columns = list(PBP_SCHEMA_BACKFILL_COLUMNS)
    source = load_pbp_schema_backfill(args.rollup, columns)
    if source.empty:
        raise SystemExit(f"PBP rollup is missing or empty: {args.rollup}")
    source["year"] = source["player_week"].str.extract(r"_(\d{4})_\d+$")[0].astype(int)
    min_year = int(source["year"].min())
    max_year = int(source["year"].max())
    source = source.drop(columns=["year"])
    source_nonzero = nonzero_source_rows(source, columns)

    reader = FlyReader()
    writer = FlyWriter()
    before_schema = sorted(fetch_schema(reader) & set(columns))
    before_totals = summarize_live(reader, columns)

    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_table = None
    added_columns: list[str] = []
    updated_chunks = 0
    if args.apply:
        added_columns = ensure_columns(writer, reader, columns)
        backup_table = create_backup_table(writer, columns, min_year, max_year, stamp)
        zero_existing_values(writer, columns, min_year, max_year)
        updated_chunks = apply_updates(writer, source_nonzero, columns, args.chunk_size)

    after_schema = sorted(fetch_schema(reader) & set(columns))
    after_totals = summarize_live(reader, columns)
    manifest = {
        "created_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "applied": args.apply,
        "super_table": SUPER_TABLE_FULL,
        "rollup": str(args.rollup),
        "columns": columns,
        "before_schema_columns": before_schema,
        "after_schema_columns": after_schema,
        "added_columns": added_columns,
        "covered_years": {"min": min_year, "max": max_year},
        "source_rows": int(len(source)),
        "source_nonzero_any_rows": int(len(source_nonzero)),
        "updated_chunks": updated_chunks,
        "chunk_size": args.chunk_size,
        "backup_table": backup_table,
        "source_totals": summarize_source(source, columns),
        "before_live_totals": before_totals,
        "after_live_totals": after_totals,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"manifest_{stamp}.json"
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (OUT_DIR / "latest_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
