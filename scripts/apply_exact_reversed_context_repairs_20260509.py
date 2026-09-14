#!/usr/bin/env python3
"""Repair exact same-key team/opponent context errors found by the PBP audit.

This intentionally handles only the safest context bucket:
- same player_week already exists in PBP and supertable
- shared stat vector is exactly equal

No IDs or stat columns are changed.
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

SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
INPUT_PATH = AUDIT_DIR / "weekly_exact_key_context_mismatch_value_deltas.csv"
LOCAL_SUPER_SNAPSHOT = AUDIT_DIR / "fly_supertable_weekly_selected_1978_2025.parquet"
OUT_DIR = AUDIT_DIR / "context_truth_20260509"


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


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: list[str]) -> str:
    if not values:
        return "('')"
    return "(" + ",".join(q_lit(value) for value in values) + ")"


def load_repairs(mode: str) -> pd.DataFrame:
    df = pd.read_csv(INPUT_PATH, dtype=str).fillna("")
    df["abs_delta"] = pd.to_numeric(df["abs_delta"], errors="coerce").fillna(float("inf"))
    exact_context = (
        df["abs_delta"].le(1e-9)
        & df["pbp_team"].astype(str).ne("")
        & df["pbp_opp"].astype(str).ne("")
        & df["pbp_team"].astype(str).ne(df["super_team"].astype(str))
    )
    if mode == "reversed":
        exact_context &= df["pbp_team"].astype(str).eq(df["super_opp"].astype(str))
        exact_context &= df["pbp_opp"].astype(str).eq(df["super_team"].astype(str))
        truth_reason = "exact_same_player_week_stat_vector_with_reversed_team_opponent"
    elif mode == "all-exact":
        truth_reason = "exact_same_player_week_stat_vector_with_pbp_team_context"
    else:
        raise ValueError(mode)

    repairs = df[exact_context].copy()
    repairs = repairs.sort_values(["year", "week", "player_week"])
    repairs["new_nfl_team"] = repairs["pbp_team"]
    repairs["new_opponent_nfl_team"] = repairs["pbp_opp"]
    repairs["old_nfl_team"] = repairs["super_team"]
    repairs["old_opponent_nfl_team"] = repairs["super_opp"]
    out_cols = [
        "player_week",
        "player",
        "position_pbp",
        "year",
        "week",
        "old_nfl_team",
        "old_opponent_nfl_team",
        "new_nfl_team",
        "new_opponent_nfl_team",
        "event_roles",
        "source",
        "active_stats",
        "truth_reason",
    ]
    repairs["truth_reason"] = truth_reason
    return repairs[out_cols].drop_duplicates("player_week", keep="first")


def create_backup_table(writer: Any, repairs: pd.DataFrame, backup_table: str) -> None:
    keys = repairs["player_week"].astype(str).tolist()
    writer.execute(
        f"CREATE TABLE {backup_table} AS SELECT * FROM {SUPER_TABLE} WHERE 1=0",
        database="___ops",
    )
    for start in range(0, len(keys), 500):
        chunk = keys[start : start + 500]
        writer.execute(
            f"""
            INSERT INTO {backup_table}
            SELECT *
            FROM {SUPER_TABLE}
            WHERE player_week IN {sql_in(chunk)}
            """,
            database="___ops",
        )


def apply_updates(writer: Any, repairs: pd.DataFrame) -> None:
    rows = repairs.to_dict("records")
    for start in range(0, len(rows), 200):
        chunk = rows[start : start + 200]
        keys = [row["player_week"] for row in chunk]
        team_case = " ".join(f"WHEN {q_lit(row['player_week'])} THEN {q_lit(row['new_nfl_team'])}" for row in chunk)
        opp_case = " ".join(
            f"WHEN {q_lit(row['player_week'])} THEN {q_lit(row['new_opponent_nfl_team'])}" for row in chunk
        )
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE}
            SET
              nfl_team = CASE player_week {team_case} ELSE nfl_team END,
              opponent_nfl_team = CASE player_week {opp_case} ELSE opponent_nfl_team END
            WHERE player_week IN {sql_in(keys)}
            """,
            database="___ops",
        )


def verify_live(writer: Any, repairs: pd.DataFrame) -> dict[str, Any]:
    rows = repairs.to_dict("records")
    old_remaining = 0
    new_present = 0
    current_rows = 0
    for start in range(0, len(rows), 200):
        chunk = rows[start : start + 200]
        old_values = ", ".join(
            f"({q_lit(row['player_week'])}, {q_lit(row['old_nfl_team'])}, {q_lit(row['old_opponent_nfl_team'])})"
            for row in chunk
        )
        new_values = ", ".join(
            f"({q_lit(row['player_week'])}, {q_lit(row['new_nfl_team'])}, {q_lit(row['new_opponent_nfl_team'])})"
            for row in chunk
        )
        old = writer.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM (VALUES {old_values}) AS v(player_week, nfl_team, opponent_nfl_team)
            JOIN {SUPER_TABLE} s
              ON s.player_week = v.player_week
             AND s.nfl_team = v.nfl_team
             AND s.opponent_nfl_team = v.opponent_nfl_team
            """,
            database="___ops",
        )
        new = writer.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM (VALUES {new_values}) AS v(player_week, nfl_team, opponent_nfl_team)
            JOIN {SUPER_TABLE} s
              ON s.player_week = v.player_week
             AND s.nfl_team = v.nfl_team
             AND s.opponent_nfl_team = v.opponent_nfl_team
            """,
            database="___ops",
        )
        keys = [row["player_week"] for row in chunk]
        current = writer.execute(
            f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(keys)}",
            database="___ops",
        )
        old_remaining += int(old[0]["n"] if isinstance(old[0], dict) else old[0][0])
        new_present += int(new[0]["n"] if isinstance(new[0], dict) else new[0][0])
        current_rows += int(current[0]["n"] if isinstance(current[0], dict) else current[0][0])
    return {
        "repair_rows": int(len(repairs)),
        "current_rows_with_player_week": current_rows,
        "old_context_remaining": old_remaining,
        "new_context_present": new_present,
    }


def patch_local_snapshot(repairs: pd.DataFrame, apply: bool) -> str | None:
    if not apply or not LOCAL_SUPER_SNAPSHOT.exists():
        return None
    backup = LOCAL_SUPER_SNAPSHOT.with_name(
        LOCAL_SUPER_SNAPSHOT.stem + "_pre_exact_reversed_context_patch" + LOCAL_SUPER_SNAPSHOT.suffix
    )
    if not backup.exists():
        backup.write_bytes(LOCAL_SUPER_SNAPSHOT.read_bytes())
    df = pd.read_parquet(LOCAL_SUPER_SNAPSHOT)
    repair_by_key = repairs.set_index("player_week")
    keys = df["player_week"].astype(str)
    mask = keys.isin(repair_by_key.index)
    df.loc[mask, "nfl_team"] = keys[mask].map(repair_by_key["new_nfl_team"]).to_numpy()
    df.loc[mask, "opponent_nfl_team"] = keys[mask].map(repair_by_key["new_opponent_nfl_team"]).to_numpy()
    df.to_parquet(LOCAL_SUPER_SNAPSHOT, index=False)
    return str(backup)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--mode", choices=["reversed", "all-exact"], default="reversed")
    args = parser.parse_args()

    repairs = load_repairs(args.mode)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    repairs.to_csv(OUT_DIR / f"exact_context_repair_rows_{args.mode}.csv", index=False)

    load_env()
    from multi_league.core.fly_writer import FlyWriter

    writer = FlyWriter()
    live_before = verify_live(writer, repairs)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_table = f"nfl_historical.context_exact_{args.mode.replace('-', '_')}_backup_{stamp}"
    local_snapshot_backup = None
    live_after = None

    if args.apply:
        create_backup_table(writer, repairs, backup_table)
        apply_updates(writer, repairs)
        live_after = verify_live(writer, repairs)
        if live_after["old_context_remaining"] != 0 or live_after["new_context_present"] != len(repairs):
            raise SystemExit("Context repair verification failed: " + json.dumps(live_after, sort_keys=True))
        local_snapshot_backup = patch_local_snapshot(repairs, True)

    manifest = {
        "created_at_utc": stamp,
        "applied": args.apply,
        "mode": args.mode,
        "repair_rows": int(len(repairs)),
        "backup_table": f"___ops.{backup_table}" if args.apply else None,
        "local_snapshot_backup": local_snapshot_backup,
        "live_before": live_before,
        "live_after": live_after,
        "input": str(INPUT_PATH),
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
