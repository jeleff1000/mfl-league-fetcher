#!/usr/bin/env python3
"""Stage and optionally insert PFR usage-only missing player-week rows.

These rows come from the PFR boxscore starter/snap/advanced audit after
identity/context cleanup. They are missing from the live supertable by
`player_week`, but have a resolved player id and unique key.

Default mode is read-only/local validation. Live writes require:

    --execute --confirm APPLY_PFR_USAGE_GAP_INSERTS
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
    TARGET_TABLE,
    fetch_schema,
    load_env,
    stage_frame,
)
from scripts.apply_supertable_raw_atom_packages import q_ident, q_table  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_GAP_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_usage_gap_cleanup_final_20260513T1610Z"
DEFAULT_CANDIDATE_FILE = DEFAULT_GAP_DIR / "pfr_usage_missing_player_week_insert_candidates.parquet"
CONFIRM_TOKEN = "APPLY_PFR_USAGE_GAP_INSERTS"
LIVE_DATA_SOURCE = "pfr_usage_gap_insert_stage"


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_candidates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path).copy()
    frame["player_week"] = frame["player_week"].astype("string")
    if "data_source" in frame.columns:
        frame["data_source"] = LIVE_DATA_SOURCE
    return frame


def local_validation(frame: pd.DataFrame) -> dict[str, Any]:
    required = ["player_week", "NFL_player_id", "player", "year", "week", "nfl_team", "season_type"]
    missing_required = [col for col in required if col not in frame.columns]
    if missing_required:
        raise ValueError(f"Candidate file is missing required columns: {missing_required}")

    duplicate_rows = int(frame.duplicated("player_week").sum())
    null_counts = {col: int(frame[col].isna().sum()) for col in required}
    if duplicate_rows:
        raise ValueError(f"Candidate file has duplicate player_week rows: {duplicate_rows}")
    fatal_nulls = {
        col: count
        for col, count in null_counts.items()
        if col in {"player_week", "NFL_player_id", "year", "week"} and count
    }
    if fatal_nulls:
        raise ValueError(f"Candidate file has required-key nulls: {fatal_nulls}")

    year_num = pd.to_numeric(frame["year"], errors="coerce")
    return {
        "candidate_rows": int(len(frame)),
        "distinct_player_weeks": int(frame["player_week"].nunique(dropna=True)),
        "duplicate_player_week_rows": duplicate_rows,
        "null_counts": null_counts,
        "min_year": int(year_num.min()) if year_num.notna().any() else None,
        "max_year": int(year_num.max()) if year_num.notna().any() else None,
        "data_source": LIVE_DATA_SOURCE,
    }


def apply_inserts(frame: pd.DataFrame, output_dir: Path, batch_rows: int) -> dict[str, Any]:
    reader = FlyReader()
    writer = FlyWriter()
    schema = fetch_schema(reader, TARGET_TABLE)
    stamp = now_stamp().lower()
    stage_table = f"nfl_historical._pfr_usage_gap_insert_stage_{stamp}"
    collision_table = f"nfl_historical.pfr_usage_gap_insert_collisions_{stamp}"

    stage_cols = list(frame.columns)
    stage_schema = schema | {
        "boxscore_id": "VARCHAR",
        "game_date": "DATE",
        "pfr_id": "VARCHAR",
        "team": "VARCHAR",
        "opponent": "VARCHAR",
    }
    stage_frame(writer, stage_table, frame, stage_cols, stage_schema, batch_rows=batch_rows)

    writer.execute(
        f"""
        CREATE TABLE {q_table(collision_table)} AS
        SELECT t.player_week, t.NFL_player_id, t.player, t.year, t.week, t.nfl_team, t.opponent_nfl_team, t.data_source
        FROM {q_table(TARGET_TABLE)} t
        JOIN {q_table(stage_table)} s
          ON t.{q_ident('player_week')} = s.{q_ident('player_week')}
        """,
        database="___ops",
    )
    collision_rows = int(
        reader.query_df(f"SELECT COUNT(*) AS n FROM {q_table(collision_table)}", database="___ops").iloc[0, 0]
    )
    if collision_rows:
        raise RuntimeError(f"Refusing insert because {collision_rows} stage player_week keys already exist live")

    insert_cols = [col for col in stage_cols if col in schema]
    if "data_source" not in insert_cols:
        raise RuntimeError("Refusing insert because live target lacks data_source")
    col_sql = ", ".join(q_ident(col) for col in insert_cols)
    select_sql = ", ".join(f"s.{q_ident(col)}" for col in insert_cols)
    writer.execute(
        f"""
        INSERT INTO {q_table(TARGET_TABLE)} ({col_sql})
        SELECT {select_sql}
        FROM {q_table(stage_table)} s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {q_table(TARGET_TABLE)} t
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
        )
        """,
        database="___ops",
    )

    verify = reader.query_df(
        f"""
        SELECT
            COUNT(*) AS inserted_rows_found,
            COUNT(DISTINCT t.player_week) AS distinct_inserted_player_weeks,
            SUM(CASE WHEN t.data_source = '{LIVE_DATA_SOURCE}' THEN 1 ELSE 0 END) AS rows_with_expected_source
        FROM {q_table(TARGET_TABLE)} t
        JOIN {q_table(stage_table)} s
          ON t.{q_ident('player_week')} = s.{q_ident('player_week')}
        """,
        database="___ops",
    )
    duplicate_check = reader.query_df(
        f"""
        SELECT COUNT(*) AS duplicate_player_week_keys
        FROM (
            SELECT player_week, COUNT(*) AS n
            FROM {q_table(TARGET_TABLE)}
            WHERE player_week IS NOT NULL
            GROUP BY player_week
            HAVING COUNT(*) > 1
        )
        """,
        database="___ops",
    )
    live_count = reader.query_df(f"SELECT COUNT(*) AS rows FROM {q_table(TARGET_TABLE)}", database="___ops")

    verify_payload = {
        "stage_table": stage_table,
        "collision_table": collision_table,
        "collision_rows": collision_rows,
        "insert_columns": insert_cols,
        "inserted_rows_found": int(verify["inserted_rows_found"].iloc[0]),
        "distinct_inserted_player_weeks": int(verify["distinct_inserted_player_weeks"].iloc[0]),
        "rows_with_expected_source": int(verify["rows_with_expected_source"].iloc[0]),
        "duplicate_player_week_keys": int(duplicate_check["duplicate_player_week_keys"].iloc[0]),
        "live_row_count": int(live_count["rows"].iloc[0]),
    }
    write_json(output_dir / "pfr_usage_gap_insert_live_verification.json", verify_payload)
    pd.DataFrame(
        [
            {
                "operation": "stage_pfr_usage_gap_inserts",
                "detail": stage_table,
                "rows": len(frame),
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
            {
                "operation": "collision_check",
                "detail": collision_table,
                "rows": collision_rows,
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
            {
                "operation": "insert_pfr_usage_gap_rows",
                "detail": ",".join(insert_cols),
                "rows": verify_payload["inserted_rows_found"],
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
            {
                "operation": "verify_pfr_usage_gap_rows",
                "detail": str(output_dir / "pfr_usage_gap_insert_live_verification.json"),
                "rows": verify_payload["duplicate_player_week_keys"],
                "completed_at_utc": datetime.now(UTC).isoformat(),
            },
        ]
    ).to_csv(output_dir / "pfr_usage_gap_insert_executed_operations.csv", index=False)
    return verify_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-file", type=Path, default=DEFAULT_CANDIDATE_FILE)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--batch-rows", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env()
    output_dir = args.output_dir or args.candidate_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = load_candidates(args.candidate_file)
    validation = local_validation(frame)
    validation["candidate_file"] = str(args.candidate_file)
    validation["generated_at_utc"] = datetime.now(UTC).isoformat()
    write_json(output_dir / "pfr_usage_gap_insert_local_validation.json", validation)
    print(json.dumps(validation, indent=2, sort_keys=True), flush=True)

    if args.execute:
        if args.confirm != CONFIRM_TOKEN:
            raise RuntimeError(f"Live insert requires --confirm {CONFIRM_TOKEN}")
        verify = apply_inserts(frame, output_dir, batch_rows=args.batch_rows)
        print(json.dumps(verify, indent=2, sort_keys=True), flush=True)
    else:
        print("[dry-run] no Fly writes performed", flush=True)


if __name__ == "__main__":
    main()
