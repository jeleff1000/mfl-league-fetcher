#!/usr/bin/env python3
"""Apply only missing live PFR usage/advanced values from an audit folder."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402
from scripts.apply_pfr_usage_advanced_backfill import (  # noqa: E402
    NEW_SUPERTABLE_COLUMNS,
    TARGET_TABLE,
    fetch_schema,
    load_env,
    stage_frame,
)
from scripts.apply_supertable_raw_atom_packages import q_ident, q_table  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_usage_gap_cleanup_biobridge_20260513T1655Z"
CONFIRM_TOKEN = "APPLY_PFR_USAGE_NULL_DELTA"


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def build_null_delta(audit_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    stage_path = audit_dir / "pfr_usage_advanced_update_stage.parquet"
    live_path = audit_dir / "live_supertable_usage_compare.parquet"
    if not stage_path.exists():
        raise FileNotFoundError(stage_path)
    if not live_path.exists():
        raise FileNotFoundError(live_path)

    stage = pd.read_parquet(stage_path).copy()
    live = pd.read_parquet(live_path).copy()
    cols = [col for col in NEW_SUPERTABLE_COLUMNS if col in stage.columns and col in live.columns]
    live_cols = ["player_week", *cols]
    joined = stage.merge(
        live[live_cols].add_prefix("live_"), left_on="player_week", right_on="live_player_week", how="left"
    )

    row_mask = pd.Series(False, index=joined.index)
    fill_rows: list[dict[str, Any]] = []
    for col in cols:
        col_mask = joined[col].notna() & joined[f"live_{col}"].isna()
        row_mask = row_mask | col_mask
        fill_rows.append({"field": col, "fill_values": int(col_mask.sum())})

    delta = joined[row_mask].copy()
    identity_cols = ["player_week", "year", "week", "nfl_team", "opponent_nfl_team"]
    keep_cols = [col for col in [*identity_cols, *cols] if col in delta.columns]
    delta = delta[keep_cols].drop_duplicates("player_week", keep="first").copy()
    summary = pd.DataFrame(fill_rows).sort_values(["fill_values", "field"], ascending=[False, True])
    return delta, summary


def apply_delta(delta: pd.DataFrame, audit_dir: Path, batch_rows: int) -> dict[str, Any]:
    reader = FlyReader()
    writer = FlyWriter()
    schema = fetch_schema(reader, TARGET_TABLE)
    stamp = now_stamp().lower()
    stage_table = f"nfl_historical._pfr_usage_null_delta_stage_{stamp}"
    backup_table = f"nfl_historical.pfr_usage_null_delta_backup_{stamp}"

    stage_cols = [col for col in delta.columns if col in schema]
    if "player_week" not in stage_cols:
        raise RuntimeError("Delta stage is missing player_week")
    stage_frame(writer, stage_table, delta, stage_cols, schema, batch_rows=batch_rows)

    backup_cols = ["player_week", *[col for col in NEW_SUPERTABLE_COLUMNS if col in schema]]
    writer.execute(
        f"""
        CREATE TABLE {q_table(backup_table)} AS
        SELECT {", ".join("t." + q_ident(col) for col in backup_cols)}
        FROM {q_table(TARGET_TABLE)} t
        WHERE EXISTS (
            SELECT 1
            FROM {q_table(stage_table)} s
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
        )
        """,
        database="___ops",
    )

    update_cols = [col for col in NEW_SUPERTABLE_COLUMNS if col in stage_cols]
    executed: list[dict[str, Any]] = []

    def record(operation: str, detail: str, rows: int | None = None) -> None:
        executed.append(
            {
                "operation": operation,
                "detail": detail,
                "rows": rows,
                "completed_at_utc": datetime.now(UTC).isoformat(),
            }
        )
        pd.DataFrame(executed).to_csv(audit_dir / "pfr_usage_null_delta_executed_operations.csv", index=False)

    record("stage_pfr_usage_null_delta", stage_table, len(delta))
    record("backup_pfr_usage_null_delta_rows", backup_table, len(delta))
    for idx, start in enumerate(range(0, len(update_cols), 8), start=1):
        part = update_cols[start : start + 8]
        assignments = ", ".join(f"{q_ident(col)} = COALESCE(t.{q_ident(col)}, s.{q_ident(col)})" for col in part)
        writer.execute(
            f"""
            UPDATE {q_table(TARGET_TABLE)} AS t
            SET {assignments}
            FROM {q_table(stage_table)} AS s
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
              AND t.{q_ident('year')} = s.{q_ident('year')}
              AND t.{q_ident('week')} = s.{q_ident('week')}
            """,
            database="___ops",
        )
        record(f"update_pfr_usage_null_delta_part_{idx}", ",".join(part), len(delta))

    remaining_exprs = [
        f"SUM(CASE WHEN s.{q_ident(col)} IS NOT NULL AND t.{q_ident(col)} IS NULL THEN 1 ELSE 0 END) AS {q_ident(col)}"
        for col in update_cols
    ]
    verify = reader.query_df(
        f"""
        SELECT
            COUNT(*) AS stage_rows,
            COUNT(t.player_week) AS matched_live_rows,
            {", ".join(remaining_exprs)}
        FROM {q_table(stage_table)} s
        LEFT JOIN {q_table(TARGET_TABLE)} t
          ON t.{q_ident('player_week')} = s.{q_ident('player_week')}
        """,
        database="___ops",
    )
    row = verify.iloc[0].to_dict()
    remaining_null_values = int(sum(int(row.get(col, 0) or 0) for col in update_cols))
    payload = {
        "stage_table": stage_table,
        "backup_table": backup_table,
        "stage_rows": int(row["stage_rows"]),
        "matched_live_rows": int(row["matched_live_rows"]),
        "remaining_null_values": remaining_null_values,
        "update_columns": update_cols,
    }
    write_json(audit_dir / "pfr_usage_null_delta_live_verification.json", payload)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--batch-rows", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env()
    delta, summary = build_null_delta(args.audit_dir)
    delta.to_parquet(args.audit_dir / "pfr_usage_null_delta.parquet", index=False)
    summary.to_csv(args.audit_dir / "pfr_usage_null_delta_summary.csv", index=False)
    payload = {
        "audit_dir": str(args.audit_dir),
        "delta_rows": int(len(delta)),
        "distinct_player_weeks": int(delta["player_week"].nunique(dropna=True)) if not delta.empty else 0,
        "fill_values": int(summary["fill_values"].sum()) if not summary.empty else 0,
    }
    write_json(args.audit_dir / "pfr_usage_null_delta_local_validation.json", payload)
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    if not args.execute:
        print("[dry-run] use --execute --confirm APPLY_PFR_USAGE_NULL_DELTA to update live rows", flush=True)
        return
    if args.confirm != CONFIRM_TOKEN:
        raise RuntimeError(f"Live update requires --confirm {CONFIRM_TOKEN}")
    live_payload = apply_delta(delta, args.audit_dir, args.batch_rows)
    print(json.dumps(live_payload, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
