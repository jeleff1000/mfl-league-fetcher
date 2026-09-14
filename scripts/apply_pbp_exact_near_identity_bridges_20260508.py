#!/usr/bin/env python3
"""Apply PBP/supertable ID bridge repairs.

These modes are intentionally narrow:
- only unique PBP/supertable weekly pairs that match on player, year, week,
  season type, position, team, and opponent are eligible
- exact-near mode applies exact/near stat-vector rows
- stale-same-id mode applies rows where the NFL_player_id already matches and
  only the player_week key is stale
- known-cross-id-diagnostic mode applies diagnostic rows only when that same
  from/to ID pair is already present in the repair map
- inverse conflicts with the existing repair map are replaced, not stacked
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
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
BASE_OUT_DIR = AUDIT_DIR / "identity_truth_20260508"
PAIR_PATH = AUDIT_DIR / "weekly_altkey_id_reconciliation_pairs.csv"
REPAIR_MAP_PATH = ROOT / "ops_data" / "nfl_historical" / "known_identity_player_week_repair_map.csv"

REPAIR_MAP_COLS = [
    "player",
    "from_player_week",
    "to_player_week",
    "from_NFL_player_id",
    "to_NFL_player_id",
    "year",
    "week",
    "season_type",
    "repair_type",
    "truth_confidence",
    "truth_reason",
]


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


def load_existing_repair_map() -> pd.DataFrame:
    existing = pd.read_csv(REPAIR_MAP_PATH, dtype=str).fillna("")
    missing = sorted(set(REPAIR_MAP_COLS) - set(existing.columns))
    if missing:
        raise RuntimeError(f"Existing repair map is missing columns: {missing}")
    return existing[REPAIR_MAP_COLS].copy()


def build_bridge_repair(mode: str, existing: pd.DataFrame | None = None) -> pd.DataFrame:
    pairs = pd.read_csv(PAIR_PATH, dtype=str).fillna("")
    if mode == "exact-near":
        safe = pairs[pairs["id_pair_quality"].isin(["exact_vector", "near_vector_le_5"])].copy()
        repair_type = "pbp_team_opp_exact_near_id_bridge"
        confidence = safe["id_pair_quality"].map({"exact_vector": "hard", "near_vector_le_5": "high"})
        reason = (
            "PBP and supertable rows uniquely match on normalized "
            "player/year/week/season_type/position/team/opponent with "
            "exact or near stat vector."
        )
    elif mode == "stale-same-id":
        safe = pairs[pairs["NFL_player_id_pbp"].eq(pairs["NFL_player_id_super"])].copy()
        repair_type = "pbp_team_opp_stale_player_week_bridge"
        confidence = "hard"
        reason = (
            "PBP and supertable rows uniquely match on player/year/week/"
            "season_type/position/team/opponent and already share "
            "NFL_player_id; only player_week key is stale."
        )
    elif mode == "known-cross-id-diagnostic":
        if existing is None:
            raise ValueError("known-cross-id-diagnostic mode requires existing repair map")
        proven_pairs = set(
            zip(
                existing["from_NFL_player_id"].astype(str),
                existing["to_NFL_player_id"].astype(str),
                strict=False,
            )
        )
        cross = pairs[~pairs["NFL_player_id_pbp"].eq(pairs["NFL_player_id_super"])].copy()
        safe = cross[
            [
                (from_id, to_id) in proven_pairs
                for from_id, to_id in zip(
                    cross["NFL_player_id_super"].astype(str),
                    cross["NFL_player_id_pbp"].astype(str),
                    strict=False,
                )
            ]
        ].copy()
        repair_type = "pbp_team_opp_known_cross_id_diagnostic_bridge"
        confidence = "probable"
        reason = (
            "PBP and supertable rows uniquely match on player/year/week/"
            "season_type/position/team/opponent, and this from/to NFL_player_id "
            "pair is already proven by exact/near bridge rows."
        )
    else:
        raise ValueError(mode)

    repair = pd.DataFrame(
        {
            "player": safe["player_pbp"],
            "from_player_week": safe["player_week_super"],
            "to_player_week": safe["player_week_pbp"],
            "from_NFL_player_id": safe["NFL_player_id_super"],
            "to_NFL_player_id": safe["NFL_player_id_pbp"],
            "year": safe["year_pbp"],
            "week": safe["week_pbp"],
            "season_type": safe["season_type_pbp"],
            "repair_type": repair_type,
            "truth_confidence": confidence,
            "truth_reason": reason,
        }
    ).drop_duplicates("from_player_week", keep="first")

    if repair["from_player_week"].duplicated().any() or repair["to_player_week"].duplicated().any():
        raise RuntimeError("Bridge repair contains duplicate source or target player_week keys.")
    return repair[REPAIR_MAP_COLS].copy()


def merge_repair_maps(existing: pd.DataFrame, bridge: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if bridge["from_player_week"].isin(set(existing["from_player_week"])).any():
        overlap = bridge[bridge["from_player_week"].isin(set(existing["from_player_week"]))]
        raise RuntimeError("Bridge sources already exist in repair map:\n" + overlap.to_csv(index=False))

    target_as_existing_source = existing[existing["from_player_week"].isin(set(bridge["to_player_week"]))].copy()
    if not target_as_existing_source.empty:
        inverse = bridge.merge(
            target_as_existing_source,
            left_on=["from_player_week", "to_player_week"],
            right_on=["to_player_week", "from_player_week"],
            how="inner",
            suffixes=("_bridge", "_existing"),
        )
        if len(inverse) != len(target_as_existing_source):
            raise RuntimeError(
                "Bridge targets collide with existing repair sources in a non-inverse way:\n"
                + target_as_existing_source.to_csv(index=False)
            )
        existing = existing[
            ~existing["from_player_week"].isin(set(target_as_existing_source["from_player_week"]))
        ].copy()

    combined = pd.concat([existing, bridge], ignore_index=True)
    if combined["from_player_week"].duplicated().any():
        dupes = combined[combined["from_player_week"].duplicated(keep=False)]
        raise RuntimeError("Combined repair map has duplicate source keys:\n" + dupes.to_csv(index=False))
    if combined["to_player_week"].duplicated().any():
        dupes = combined[combined["to_player_week"].duplicated(keep=False)]
        raise RuntimeError("Combined repair map has duplicate target keys:\n" + dupes.to_csv(index=False))
    return combined.sort_values(
        ["repair_type", "player", "year", "week", "from_player_week"]
    ), target_as_existing_source


def verify_no_target_collisions(writer: Any, repair: pd.DataFrame) -> None:
    old_keys = set(repair["from_player_week"].astype(str))
    new_keys = sorted(set(repair["to_player_week"].astype(str)) - old_keys)
    collisions = []
    for start in range(0, len(new_keys), 500):
        chunk = new_keys[start : start + 500]
        rows = writer.execute(
            f"""
            SELECT player_week, NFL_player_id, player, year, week
            FROM {SUPER_TABLE}
            WHERE player_week IN {sql_in(chunk)}
            """,
            database="___ops",
        )
        if rows:
            collisions.extend(rows)
    if collisions:
        raise RuntimeError("Target player_week collisions found:\n" + json.dumps(collisions[:50], default=str))


def create_backup_table(writer: Any, repair: pd.DataFrame, backup_table: str) -> None:
    keys = sorted(repair["from_player_week"].astype(str).unique().tolist())
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


def apply_live_updates(writer: Any, repair: pd.DataFrame) -> None:
    rows = repair.to_dict("records")
    for start in range(0, len(rows), 200):
        chunk = rows[start : start + 200]
        keys = [row["from_player_week"] for row in chunk]
        id_case = " ".join(
            f"WHEN {q_lit(row['from_player_week'])} THEN {q_lit(row['to_NFL_player_id'])}" for row in chunk
        )
        week_case = " ".join(
            f"WHEN {q_lit(row['from_player_week'])} THEN {q_lit(row['to_player_week'])}" for row in chunk
        )
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE}
            SET
              NFL_player_id = CASE player_week {id_case} ELSE NFL_player_id END,
              player_week = CASE player_week {week_case} ELSE player_week END
            WHERE player_week IN {sql_in(keys)}
            """,
            database="___ops",
        )


def count_query(writer: Any, sql: str) -> int:
    rows = writer.execute(sql, database="___ops")
    if not rows:
        return 0
    row = rows[0]
    return int(row["n"] if isinstance(row, dict) else row[0])


def verify_live(writer: Any, repair: pd.DataFrame) -> dict[str, int]:
    repair = repair.copy()
    old_keys = sorted(repair["from_player_week"].astype(str).unique().tolist())
    new_keys = sorted(repair["to_player_week"].astype(str).unique().tolist())
    old_remaining = 0
    new_present = 0
    old_identity_remaining = 0
    target_identity_present = 0

    for start in range(0, len(old_keys), 500):
        chunk = old_keys[start : start + 500]
        old_remaining += count_query(
            writer,
            f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}",
        )
    for start in range(0, len(new_keys), 500):
        chunk = new_keys[start : start + 500]
        new_present += count_query(
            writer,
            f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}",
        )

    for start in range(0, len(repair), 500):
        chunk = repair.iloc[start : start + 500]
        old_values = ", ".join(
            f"({q_lit(row.from_player_week)}, {q_lit(row.from_NFL_player_id)})" for row in chunk.itertuples(index=False)
        )
        new_values = ", ".join(
            f"({q_lit(row.to_player_week)}, {q_lit(row.to_NFL_player_id)})" for row in chunk.itertuples(index=False)
        )
        old_identity_remaining += count_query(
            writer,
            f"""
            SELECT COUNT(*) AS n
            FROM (VALUES {old_values}) AS v(player_week, nfl_player_id)
            JOIN {SUPER_TABLE} s
              ON s.player_week = v.player_week
             AND s.NFL_player_id = v.nfl_player_id
            """,
        )
        target_identity_present += count_query(
            writer,
            f"""
            SELECT COUNT(*) AS n
            FROM (VALUES {new_values}) AS v(player_week, nfl_player_id)
            JOIN {SUPER_TABLE} s
              ON s.player_week = v.player_week
             AND s.NFL_player_id = v.nfl_player_id
            """,
        )

    return {
        "repair_rows": int(len(repair)),
        "old_player_week_remaining": old_remaining,
        "new_player_week_present": new_present,
        "old_identity_remaining": old_identity_remaining,
        "target_identity_present": target_identity_present,
    }


def write_repair_map(path: Path, combined: pd.DataFrame, apply: bool) -> str | None:
    if not apply:
        return None
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(path.stem + f"_pre_pbp_exact_near_bridge_{stamp}" + path.suffix)
    shutil.copy2(path, backup)
    combined.to_csv(path, index=False)
    return str(backup)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--mode",
        choices=["exact-near", "stale-same-id", "known-cross-id-diagnostic"],
        default="exact-near",
    )
    args = parser.parse_args()

    load_env()
    from multi_league.core.fly_writer import FlyWriter

    writer = FlyWriter()
    existing = load_existing_repair_map()
    bridge = build_bridge_repair(args.mode, existing)
    combined, replaced_existing = merge_repair_maps(existing, bridge)
    verify_no_target_collisions(writer, bridge)

    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = BASE_OUT_DIR / (
        "pbp_exact_near_bridge_repair_20260508"
        if args.mode == "exact-near"
        else (
            "pbp_stale_player_week_bridge_repair_20260508"
            if args.mode == "stale-same-id"
            else "pbp_known_cross_id_diagnostic_bridge_repair_20260508"
        )
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    bridge.to_csv(out_dir / f"pbp_{args.mode.replace('-', '_')}_bridge_repair_rows.csv", index=False)
    replaced_existing.to_csv(out_dir / "replaced_existing_inverse_repair_rows.csv", index=False)
    combined.to_csv(out_dir / "combined_known_identity_player_week_repair_map.preview.csv", index=False)

    backup_table = f"nfl_historical.identity_pbp_exact_near_bridge_backup_{stamp}"
    repair_map_backup = write_repair_map(REPAIR_MAP_PATH, combined, args.apply)
    live_verify = {"repair_rows": int(len(bridge))}
    if args.apply:
        create_backup_table(writer, bridge, backup_table)
        apply_live_updates(writer, bridge)
        live_verify = verify_live(writer, bridge)
        if (
            live_verify["old_player_week_remaining"] != 0
            or live_verify["old_identity_remaining"] != 0
            or live_verify["target_identity_present"] != len(bridge)
        ):
            raise SystemExit("Live bridge repair verification failed: " + json.dumps(live_verify, sort_keys=True))

    manifest = {
        "created_at_utc": stamp,
        "applied": args.apply,
        "mode": args.mode,
        "bridge_repair_rows": int(len(bridge)),
        "bridge_repair_types": bridge["truth_confidence"].value_counts().to_dict(),
        "existing_repair_rows_before": int(len(existing)),
        "combined_repair_rows_after": int(len(combined)),
        "replaced_existing_inverse_rows": int(len(replaced_existing)),
        "repair_map": str(REPAIR_MAP_PATH),
        "repair_map_backup": repair_map_backup,
        "backup_table": f"___ops.{backup_table}" if args.apply else None,
        "live_verify": live_verify,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
