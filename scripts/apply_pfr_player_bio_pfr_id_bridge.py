#!/usr/bin/env python3
"""Safely fill blank player_bio.pfr_id values from PFR usage gap analysis.

This is intentionally narrower than a general identity merge. It only updates
existing player_bio rows where the audit produced a one-to-one exact-name,
career-overlap match and the target bio row currently has no PFR id.
"""

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
    BIO_TABLE,
    fetch_schema,
    load_env,
    stage_frame,
)
from scripts.apply_supertable_raw_atom_packages import q_ident, q_table  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_usage_gap_cleanup_post_insert_20260513T1645Z"
DEFAULT_STAGE_FILE = DEFAULT_AUDIT_DIR / "pfr_player_bio_pfr_id_blank_safe_update_stage.csv"
CONFIRM_TOKEN = "APPLY_PFR_PLAYER_BIO_PFR_ID_BRIDGE"


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_stage(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path, dtype=str).copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "NFL_player_id",
                "pfr_id_new",
                "pfr_player",
                "bio_player",
                "no_identity_rows",
                "min_year",
                "max_year",
                "teams",
            ]
        )
    required = ["NFL_player_id", "pfr_id", "player", "bio_player", "no_identity_rows", "min_year", "max_year", "teams"]
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"Stage CSV is missing required columns: {missing}")
    frame = frame.rename(columns={"pfr_id": "pfr_id_new", "player": "pfr_player"})
    frame["NFL_player_id"] = frame["NFL_player_id"].astype(str).str.strip()
    frame["pfr_id_new"] = frame["pfr_id_new"].astype(str).str.strip()
    frame = frame[(frame["NFL_player_id"] != "") & (frame["pfr_id_new"] != "")].copy()
    frame["no_identity_rows"] = pd.to_numeric(frame["no_identity_rows"], errors="coerce").fillna(0).astype(int)
    frame["min_year"] = pd.to_numeric(frame["min_year"], errors="coerce").astype("Int64")
    frame["max_year"] = pd.to_numeric(frame["max_year"], errors="coerce").astype("Int64")
    return frame[
        [
            "NFL_player_id",
            "pfr_id_new",
            "pfr_player",
            "bio_player",
            "no_identity_rows",
            "min_year",
            "max_year",
            "teams",
        ]
    ].drop_duplicates()


def local_validation(frame: pd.DataFrame) -> dict[str, Any]:
    duplicate_ids = int(frame.duplicated("NFL_player_id", keep=False).sum())
    duplicate_pfr = int(frame.duplicated("pfr_id_new", keep=False).sum())
    if duplicate_ids or duplicate_pfr:
        raise ValueError(f"Non-unique bridge stage: duplicate_ids={duplicate_ids}, duplicate_pfr={duplicate_pfr}")
    return {
        "stage_rows": int(len(frame)),
        "no_identity_rows_covered": int(frame["no_identity_rows"].sum()) if not frame.empty else 0,
        "duplicate_nfl_player_ids": duplicate_ids,
        "duplicate_pfr_ids": duplicate_pfr,
        "min_year": int(frame["min_year"].min()) if not frame.empty and frame["min_year"].notna().any() else None,
        "max_year": int(frame["max_year"].max()) if not frame.empty and frame["max_year"].notna().any() else None,
    }


def apply_updates(frame: pd.DataFrame, output_dir: Path, batch_rows: int) -> dict[str, Any]:
    reader = FlyReader()
    writer = FlyWriter()
    bio_schema = fetch_schema(reader, BIO_TABLE)
    stamp = now_stamp().lower()
    stage_table = f"nfl_historical._pfr_bio_pfr_id_bridge_stage_{stamp}"
    backup_table = f"nfl_historical.pfr_bio_pfr_id_bridge_backup_{stamp}"
    conflict_table = f"nfl_historical.pfr_bio_pfr_id_bridge_conflicts_{stamp}"

    stage_schema = {
        "NFL_player_id": "VARCHAR",
        "pfr_id_new": "VARCHAR",
        "pfr_player": "VARCHAR",
        "bio_player": "VARCHAR",
        "no_identity_rows": "BIGINT",
        "min_year": "BIGINT",
        "max_year": "BIGINT",
        "teams": "VARCHAR",
    }
    stage_cols = list(stage_schema)
    stage_frame(writer, stage_table, frame, stage_cols, stage_schema, batch_rows=batch_rows)

    writer.execute(
        f"""
        CREATE TABLE {q_table(conflict_table)} AS
        SELECT
            s.NFL_player_id,
            s.pfr_id_new,
            b.pfr_id AS current_target_pfr_id,
            used.NFL_player_id AS used_by_nfl_player_id,
            used.player AS used_by_player
        FROM {q_table(stage_table)} s
        LEFT JOIN {q_table(BIO_TABLE)} b
          ON b.NFL_player_id = s.NFL_player_id
        LEFT JOIN {q_table(BIO_TABLE)} used
          ON LOWER(TRIM(CAST(used.pfr_id AS VARCHAR))) = LOWER(TRIM(s.pfr_id_new))
         AND used.NFL_player_id <> s.NFL_player_id
        WHERE b.NFL_player_id IS NULL
           OR COALESCE(TRIM(CAST(b.pfr_id AS VARCHAR)), '') <> ''
           OR used.NFL_player_id IS NOT NULL
        """,
        database="___ops",
    )
    conflict_rows = int(
        reader.query_df(f"SELECT COUNT(*) AS n FROM {q_table(conflict_table)}", database="___ops").iloc[0, 0]
    )
    if conflict_rows:
        raise RuntimeError(f"Refusing player_bio pfr_id update; conflicts found: {conflict_rows} in {conflict_table}")

    writer.execute(
        f"""
        CREATE TABLE {q_table(backup_table)} AS
        SELECT b.*
        FROM {q_table(BIO_TABLE)} b
        JOIN {q_table(stage_table)} s
          ON b.NFL_player_id = s.NFL_player_id
        """,
        database="___ops",
    )

    writer.execute(
        f"""
        UPDATE {q_table(BIO_TABLE)} AS b
        SET pfr_id = s.pfr_id_new
        FROM {q_table(stage_table)} AS s
        WHERE b.NFL_player_id = s.NFL_player_id
          AND COALESCE(TRIM(CAST(b.pfr_id AS VARCHAR)), '') = ''
        """,
        database="___ops",
    )

    verify = reader.query_df(
        f"""
        SELECT
            COUNT(*) AS stage_rows,
            SUM(CASE WHEN b.pfr_id = s.pfr_id_new THEN 1 ELSE 0 END) AS rows_with_expected_pfr_id,
            SUM(CASE WHEN b.pfr_id IS NULL OR TRIM(CAST(b.pfr_id AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS rows_still_blank
        FROM {q_table(stage_table)} s
        LEFT JOIN {q_table(BIO_TABLE)} b
          ON b.NFL_player_id = s.NFL_player_id
        """,
        database="___ops",
    )
    payload = {
        "stage_table": stage_table,
        "backup_table": backup_table,
        "conflict_table": conflict_table,
        "conflict_rows": conflict_rows,
        "stage_rows": int(verify["stage_rows"].iloc[0]),
        "rows_with_expected_pfr_id": int(verify["rows_with_expected_pfr_id"].iloc[0]),
        "rows_still_blank": int(verify["rows_still_blank"].iloc[0]),
        "bio_schema_had_pfr_id": "pfr_id" in bio_schema,
    }
    write_json(output_dir / "pfr_player_bio_pfr_id_bridge_live_verification.json", payload)
    pd.DataFrame(
        [
            {
                "operation": "stage_player_bio_pfr_id_bridge",
                "detail": stage_table,
                "rows": len(frame),
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
            {
                "operation": "conflict_check",
                "detail": conflict_table,
                "rows": conflict_rows,
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
            {
                "operation": "backup_player_bio_rows",
                "detail": backup_table,
                "rows": len(frame),
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
            {
                "operation": "update_player_bio_pfr_id",
                "detail": "blank pfr_id only",
                "rows": payload["rows_with_expected_pfr_id"],
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
        ]
    ).to_csv(output_dir / "pfr_player_bio_pfr_id_bridge_executed_operations.csv", index=False)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-file", type=Path, default=DEFAULT_STAGE_FILE)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--batch-rows", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env()
    output_dir = args.output_dir or args.stage_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = load_stage(args.stage_file)
    validation = local_validation(frame)
    write_json(output_dir / "pfr_player_bio_pfr_id_bridge_local_validation.json", validation)
    print(json.dumps(validation, indent=2, sort_keys=True), flush=True)
    if not args.execute:
        print(
            "[dry-run] use --execute --confirm APPLY_PFR_PLAYER_BIO_PFR_ID_BRIDGE to update live player_bio", flush=True
        )
        return
    if args.confirm != CONFIRM_TOKEN:
        raise RuntimeError(f"Live update requires --confirm {CONFIRM_TOKEN}")
    payload = apply_updates(frame, output_dir, args.batch_rows)
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
