#!/usr/bin/env python3
"""Apply missing player_bio combine measurables from a PFR audit stage."""

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
    BIO_COMBINE_MAP,
    BIO_TABLE,
    fetch_schema,
    load_env,
    stage_frame,
)
from scripts.apply_supertable_raw_atom_packages import q_ident, q_table  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_usage_gap_cleanup_final_stage2_20260513T1750Z"
CONFIRM_TOKEN = "APPLY_PFR_BIO_COMBINE_FILL"


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_stage(audit_dir: Path) -> pd.DataFrame:
    path = audit_dir / "pfr_player_bio_combine_fill_stage.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path).copy()
    cols = ["NFL_player_id", *BIO_COMBINE_MAP.values()]
    cols = [col for col in cols if col in frame.columns]
    return frame[cols].drop_duplicates("NFL_player_id", keep="first")


def local_validation(frame: pd.DataFrame) -> dict[str, Any]:
    fields = [col for col in BIO_COMBINE_MAP.values() if col in frame.columns]
    counts = {col: int(frame[col].notna().sum()) for col in fields}
    return {
        "stage_rows": int(len(frame)),
        "duplicate_nfl_player_ids": int(frame.duplicated("NFL_player_id").sum())
        if "NFL_player_id" in frame.columns
        else None,
        "fill_counts": counts,
        "fill_values": int(sum(counts.values())),
    }


def apply_stage(frame: pd.DataFrame, audit_dir: Path, batch_rows: int) -> dict[str, Any]:
    reader = FlyReader()
    writer = FlyWriter()
    bio_schema = fetch_schema(reader, BIO_TABLE)
    stamp = now_stamp().lower()
    stage_table = f"nfl_historical._pfr_bio_combine_fill_stage_{stamp}"
    backup_table = f"nfl_historical.pfr_bio_combine_fill_backup_{stamp}"
    cols = [col for col in frame.columns if col in bio_schema]
    if "NFL_player_id" not in cols:
        raise RuntimeError("Combine stage missing NFL_player_id")
    stage_frame(writer, stage_table, frame, cols, bio_schema, batch_rows=batch_rows)

    writer.execute(
        f"""
        CREATE TABLE {q_table(backup_table)} AS
        SELECT {", ".join("b." + q_ident(col) for col in cols)}
        FROM {q_table(BIO_TABLE)} b
        WHERE EXISTS (
            SELECT 1
            FROM {q_table(stage_table)} s
            WHERE b.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
        )
        """,
        database="___ops",
    )
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
        pd.DataFrame(executed).to_csv(audit_dir / "pfr_bio_combine_fill_executed_operations.csv", index=False)

    record("stage_pfr_bio_combine_fill", stage_table, len(frame))
    record("backup_pfr_bio_combine_fill", backup_table, len(frame))
    fields = [col for col in cols if col != "NFL_player_id"]
    for col in fields:
        writer.execute(
            f"""
            UPDATE {q_table(BIO_TABLE)} AS b
            SET {q_ident(col)} = s.{q_ident(col)}
            FROM {q_table(stage_table)} AS s
            WHERE b.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
              AND b.{q_ident(col)} IS NULL
              AND s.{q_ident(col)} IS NOT NULL
            """,
            database="___ops",
        )
        record("update_pfr_bio_combine_missing_only", col, len(frame))

    remaining_exprs = [
        f"SUM(CASE WHEN s.{q_ident(col)} IS NOT NULL AND b.{q_ident(col)} IS NULL THEN 1 ELSE 0 END) AS {q_ident(col)}"
        for col in fields
    ]
    verify = reader.query_df(
        f"""
        SELECT
            COUNT(*) AS stage_rows,
            COUNT(b.NFL_player_id) AS matched_bio_rows,
            {", ".join(remaining_exprs)}
        FROM {q_table(stage_table)} s
        LEFT JOIN {q_table(BIO_TABLE)} b
          ON b.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
        """,
        database="___ops",
    )
    row = verify.iloc[0].to_dict()
    payload = {
        "stage_table": stage_table,
        "backup_table": backup_table,
        "stage_rows": int(row["stage_rows"]),
        "matched_bio_rows": int(row["matched_bio_rows"]),
        "remaining_null_values": int(sum(int(row.get(col, 0) or 0) for col in fields)),
        "fields": fields,
    }
    write_json(audit_dir / "pfr_bio_combine_fill_live_verification.json", payload)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--batch-rows", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env()
    frame = load_stage(args.audit_dir)
    validation = local_validation(frame)
    write_json(args.audit_dir / "pfr_bio_combine_fill_local_validation.json", validation)
    print(json.dumps(validation, indent=2, sort_keys=True), flush=True)
    if not args.execute:
        print("[dry-run] use --execute --confirm APPLY_PFR_BIO_COMBINE_FILL to update player_bio", flush=True)
        return
    if args.confirm != CONFIRM_TOKEN:
        raise RuntimeError(f"Live update requires --confirm {CONFIRM_TOKEN}")
    payload = apply_stage(frame, args.audit_dir, args.batch_rows)
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
