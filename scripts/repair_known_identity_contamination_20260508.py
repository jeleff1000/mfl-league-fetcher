#!/usr/bin/env python3
"""Repair confirmed player identity contamination in local sources and Fly.

Inputs come from the identity audit artifacts in
fantasy_football_data_organized/_catalog/pbp_supertable_gap_audit_20260507.

Repairs applied:
- row-level safe same-name reassignments, e.g. Kellen Winslow Jr ID rows from
  1979-1987 become Kellen Winslow Sr ID rows
- duplicate-ID bridges, e.g. Freeman McNeil HIST-* rows become canonical PFR/
  nflverse IDs
- local player_bio first/last-year windows for the affected IDs

The script writes an exact player_week repair map used by pipeline guards.
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
TRUTH_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507" / "identity_truth_20260508"
OUT_DIR = TRUTH_DIR / "live_identity_repair_20260508"
BIO_PATHS = [
    ROOT / "ops_data" / "nfl_historical" / "player_bio.parquet",
    ORGANIZED_ROOT / "nfl_historical_repair_artifacts_20260508" / "player_bio_stathead_pfr_repaired.parquet",
]
REPAIR_MAP_PATH = ROOT / "ops_data" / "nfl_historical" / "known_identity_player_week_repair_map.csv"

IDENTITY_COLS = [
    "player_week",
    "NFL_player_id",
    "player",
    "position",
    "nfl_team",
    "year",
    "week",
    "season_type",
]
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


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: list[str]) -> str:
    if not values:
        return "('')"
    return "(" + ",".join(q_lit(value) for value in values) + ")"


def clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def clean_int(value: Any) -> int | None:
    if value is None or pd.isna(value) or clean_text(value) == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def player_week(player_id: str, year: Any, week: Any) -> str:
    return f"{player_id}_{int(float(year))}_{int(float(week))}"


def query_df(writer: Any, sql: str) -> pd.DataFrame:
    rows = writer.execute(sql, database="___ops")
    return pd.DataFrame(rows)


def load_simple_safe_rows() -> pd.DataFrame:
    path = TRUTH_DIR / "live_simple_identity_safe_reassignment_rows.csv"
    rows = pd.read_csv(path, dtype=str).fillna("")
    rows["repair_type"] = "same_name_pre_rookie_reassignment"
    rows["truth_confidence"] = "hard"
    rows["from_player_week"] = rows["player_week"].astype(str)
    rows["to_player_week"] = rows["proposed_player_week"].astype(str)
    rows["truth_reason"] = rows.get("truth_reason", "")
    return rows[
        [
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
    ].copy()


def load_duplicate_scope() -> pd.DataFrame:
    path = TRUTH_DIR / "live_duplicate_id_bridge_scope.csv"
    scope = pd.read_csv(path, dtype=str).fillna("")
    numeric_cols = [
        "target_key_collisions",
        "proposed_player_week_collisions",
        "live_rows",
    ]
    for col in numeric_cols:
        scope[col] = pd.to_numeric(scope[col], errors="coerce").fillna(0)
    bad = scope[scope["target_key_collisions"].ne(0) | scope["proposed_player_week_collisions"].ne(0)]
    if not bad.empty:
        raise RuntimeError(
            "Duplicate-ID bridge scope has collisions; refusing live repair: "
            + bad[["player", "from_NFL_player_id", "to_NFL_player_id"]].to_csv(index=False)
        )
    return scope


def fetch_duplicate_rows(writer: Any, duplicate_scope: pd.DataFrame) -> pd.DataFrame:
    from_ids = sorted(duplicate_scope["from_NFL_player_id"].dropna().astype(str).unique().tolist())
    if not from_ids:
        return pd.DataFrame()
    cols = ", ".join(q_ident(col) for col in IDENTITY_COLS)
    live = query_df(
        writer,
        f"""
        SELECT {cols}
        FROM {SUPER_TABLE}
        WHERE NFL_player_id IN {sql_in(from_ids)}
        ORDER BY year, week, player, NFL_player_id
        """,
    )
    if live.empty:
        return live
    mapping = duplicate_scope[
        [
            "player",
            "from_NFL_player_id",
            "to_NFL_player_id",
            "truth_confidence",
            "truth_reason",
        ]
    ].drop_duplicates()
    rows = live.merge(mapping, left_on="NFL_player_id", right_on="from_NFL_player_id", how="inner")
    rows["repair_type"] = "duplicate_id_bridge"
    rows["from_player_week"] = rows["player_week"].astype(str)
    rows["to_player_week"] = [
        player_week(to_id, year, week)
        for to_id, year, week in zip(rows["to_NFL_player_id"], rows["year"], rows["week"], strict=False)
    ]
    rows["player"] = rows["player_y"].where(rows["player_y"].astype(str).ne(""), rows["player_x"])
    return rows[
        [
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
    ].copy()


def build_repair_map(writer: Any) -> pd.DataFrame:
    simple = load_simple_safe_rows()
    duplicates = fetch_duplicate_rows(writer, load_duplicate_scope())
    if duplicates.empty and REPAIR_MAP_PATH.exists():
        existing = load_existing_repair_map()
        existing_duplicates = existing[existing["repair_type"].eq("duplicate_id_bridge")].copy()
        if not existing_duplicates.empty:
            duplicates = existing_duplicates
    repair = pd.concat([simple, duplicates], ignore_index=True)
    repair = repair.drop_duplicates("from_player_week", keep="first")
    if repair["to_player_week"].duplicated().any():
        dupes = repair[repair["to_player_week"].duplicated(keep=False)]
        raise RuntimeError("Repair map has duplicate target player_week keys:\n" + dupes.to_csv(index=False))
    repair = repair.sort_values(["repair_type", "player", "year", "week", "from_player_week"])
    return repair


def load_existing_repair_map() -> pd.DataFrame:
    repair = pd.read_csv(REPAIR_MAP_PATH, dtype=str).fillna("")
    missing = sorted(set(REPAIR_MAP_COLS) - set(repair.columns))
    if missing:
        raise RuntimeError(f"Existing repair map is missing required columns: {missing}")
    return repair[REPAIR_MAP_COLS].copy()


def verify_no_target_collisions(writer: Any, repair: pd.DataFrame) -> None:
    old_keys = set(repair["from_player_week"].astype(str))
    new_keys = set(repair["to_player_week"].astype(str))
    collision_keys = sorted(new_keys - old_keys)
    if not collision_keys:
        return
    collisions = []
    for start in range(0, len(collision_keys), 500):
        chunk = collision_keys[start : start + 500]
        df = query_df(
            writer,
            f"""
            SELECT player_week, NFL_player_id, player, year, week
            FROM {SUPER_TABLE}
            WHERE player_week IN {sql_in(chunk)}
            """,
        )
        if not df.empty:
            collisions.append(df)
    if collisions:
        all_collisions = pd.concat(collisions, ignore_index=True)
        raise RuntimeError("Target player_week collisions found:\n" + all_collisions.to_csv(index=False))


def write_local_repair_map(repair: pd.DataFrame) -> None:
    REPAIR_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    repair.to_csv(REPAIR_MAP_PATH, index=False)


def backup_and_repair_bio(path: Path, repair: pd.DataFrame, apply: bool) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    bio = pd.read_parquet(path)
    if "NFL_player_id" not in bio.columns:
        return {"path": str(path), "exists": True, "updated_rows": 0, "reason": "missing_NFL_player_id"}

    out = bio.copy()
    out["NFL_player_id"] = out["NFL_player_id"].astype(str)
    updated_ids: set[str] = set()
    dropped_duplicate_ids: set[str] = set()

    simple = repair[repair["repair_type"].eq("same_name_pre_rookie_reassignment")].copy()
    simple_pairs = simple[["from_NFL_player_id", "to_NFL_player_id", "year"]].drop_duplicates()
    simple_windows = (
        simple_pairs.groupby(["from_NFL_player_id", "to_NFL_player_id"], dropna=False)
        .agg(
            target_first_year=("year", lambda s: min(clean_int(v) for v in s if clean_int(v) is not None)),
            target_last_year=("year", lambda s: max(clean_int(v) for v in s if clean_int(v) is not None)),
        )
        .reset_index()
    )

    for row in simple_windows.to_dict("records"):
        from_id = clean_text(row["from_NFL_player_id"])
        to_id = clean_text(row["to_NFL_player_id"])
        target_first = clean_int(row["target_first_year"])
        target_last = clean_int(row["target_last_year"])
        from_mask = out["NFL_player_id"].eq(from_id)
        to_mask = out["NFL_player_id"].eq(to_id)
        if from_mask.any():
            rookie = clean_int(out.loc[from_mask, "rookie_year"].iloc[0]) if "rookie_year" in out.columns else None
            first = clean_int(out.loc[from_mask, "first_year"].iloc[0]) if "first_year" in out.columns else None
            if rookie is not None and (first is None or first < rookie):
                out.loc[from_mask, "first_year"] = rookie
                updated_ids.add(from_id)
        if to_mask.any():
            if target_first is not None and "first_year" in out.columns:
                existing = clean_int(out.loc[to_mask, "first_year"].iloc[0])
                out.loc[to_mask, "first_year"] = target_first if existing is None else min(existing, target_first)
            if target_last is not None and "last_year" in out.columns:
                existing = clean_int(out.loc[to_mask, "last_year"].iloc[0])
                out.loc[to_mask, "last_year"] = target_last if existing is None else max(existing, target_last)
            updated_ids.add(to_id)

    duplicate_pairs = repair[repair["repair_type"].eq("duplicate_id_bridge")][
        ["from_NFL_player_id", "to_NFL_player_id", "year"]
    ].drop_duplicates()
    duplicate_windows = (
        duplicate_pairs.groupby(["from_NFL_player_id", "to_NFL_player_id"], dropna=False)
        .agg(
            first_year=("year", lambda s: min(clean_int(v) for v in s if clean_int(v) is not None)),
            last_year=("year", lambda s: max(clean_int(v) for v in s if clean_int(v) is not None)),
        )
        .reset_index()
    )
    for row in duplicate_windows.to_dict("records"):
        to_id = clean_text(row["to_NFL_player_id"])
        first_year = clean_int(row["first_year"])
        last_year = clean_int(row["last_year"])
        to_mask = out["NFL_player_id"].eq(to_id)
        if not to_mask.any():
            continue
        if first_year is not None and "first_year" in out.columns:
            existing = clean_int(out.loc[to_mask, "first_year"].iloc[0])
            out.loc[to_mask, "first_year"] = first_year if existing is None else min(existing, first_year)
        if last_year is not None and "last_year" in out.columns:
            existing = clean_int(out.loc[to_mask, "last_year"].iloc[0])
            out.loc[to_mask, "last_year"] = last_year if existing is None else max(existing, last_year)
        updated_ids.add(to_id)

    if "years_active" in out.columns and {"first_year", "last_year"}.issubset(out.columns):
        mask = out["NFL_player_id"].isin(updated_ids)
        first = pd.to_numeric(out.loc[mask, "first_year"], errors="coerce")
        last = pd.to_numeric(out.loc[mask, "last_year"], errors="coerce")
        out.loc[mask, "years_active"] = (last - first + 1).where(first.notna() & last.notna())

    drop_duplicate_ids: set[str] = set()
    for row in duplicate_pairs[["from_NFL_player_id", "to_NFL_player_id"]].drop_duplicates().to_dict("records"):
        from_id = clean_text(row["from_NFL_player_id"])
        to_id = clean_text(row["to_NFL_player_id"])
        if from_id and to_id and out["NFL_player_id"].eq(to_id).any():
            drop_duplicate_ids.add(from_id)
    if drop_duplicate_ids:
        drop_mask = out["NFL_player_id"].isin(drop_duplicate_ids)
        dropped_duplicate_ids.update(out.loc[drop_mask, "NFL_player_id"].astype(str).unique().tolist())
        out = out.loc[~drop_mask].copy()

    if out.shape == bio.shape and out.index.equals(bio.index) and list(out.columns) == list(bio.columns):
        changed_cells: int | None = int(out.compare(bio, keep_shape=False, keep_equal=False).size)
    else:
        changed_cells = None
    changed = not out.equals(bio)
    if apply and changed:
        backup = path.with_name(path.stem + "_pre_identity_repair_20260508" + path.suffix)
        if not backup.exists():
            shutil.copy2(path, backup)
        out.to_parquet(path, index=False)

    return {
        "path": str(path),
        "exists": True,
        "changed_cells": changed_cells,
        "changed": changed,
        "dropped_duplicate_ids": sorted(dropped_duplicate_ids),
        "updated_ids": sorted(updated_ids),
        "applied": apply,
    }


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


def verify_live(writer: Any, repair: pd.DataFrame) -> dict[str, Any]:
    repair = repair.copy()
    for col in ["from_player_week", "to_player_week", "from_NFL_player_id", "to_NFL_player_id"]:
        repair[col] = repair[col].astype(str)

    old_keys = sorted(repair["from_player_week"].unique().tolist())
    changed_old_keys = sorted(
        repair.loc[repair["from_player_week"].ne(repair["to_player_week"]), "from_player_week"].unique().tolist()
    )
    new_keys = sorted(repair["to_player_week"].unique().tolist())

    old_player_week_rows_present_total = 0
    old_changed_player_week_remaining = 0
    new_player_week_present = 0
    target_identity_present = 0
    old_from_identity_remaining = 0

    for start in range(0, len(old_keys), 500):
        chunk = old_keys[start : start + 500]
        rows = writer.execute(
            f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}",
            database="___ops",
        )
        old_player_week_rows_present_total += int(rows[0]["n"] if isinstance(rows[0], dict) else rows[0][0])
    for start in range(0, len(changed_old_keys), 500):
        chunk = changed_old_keys[start : start + 500]
        rows = writer.execute(
            f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}",
            database="___ops",
        )
        old_changed_player_week_remaining += int(rows[0]["n"] if isinstance(rows[0], dict) else rows[0][0])
    for start in range(0, len(new_keys), 500):
        chunk = new_keys[start : start + 500]
        rows = writer.execute(
            f"SELECT COUNT(*) AS n FROM {SUPER_TABLE} WHERE player_week IN {sql_in(chunk)}",
            database="___ops",
        )
        new_player_week_present += int(rows[0]["n"] if isinstance(rows[0], dict) else rows[0][0])

    for start in range(0, len(repair), 500):
        chunk = repair.iloc[start : start + 500]
        old_values = ", ".join(
            f"({q_lit(row.from_player_week)}, {q_lit(row.from_NFL_player_id)})" for row in chunk.itertuples(index=False)
        )
        new_values = ", ".join(
            f"({q_lit(row.to_player_week)}, {q_lit(row.to_NFL_player_id)})" for row in chunk.itertuples(index=False)
        )
        rows = writer.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM (VALUES {old_values}) AS v(player_week, nfl_player_id)
            JOIN {SUPER_TABLE} s
              ON s.player_week = v.player_week
             AND s.NFL_player_id = v.nfl_player_id
            """,
            database="___ops",
        )
        old_from_identity_remaining += int(rows[0]["n"] if isinstance(rows[0], dict) else rows[0][0])
        rows = writer.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM (VALUES {new_values}) AS v(player_week, nfl_player_id)
            JOIN {SUPER_TABLE} s
              ON s.player_week = v.player_week
             AND s.NFL_player_id = v.nfl_player_id
            """,
            database="___ops",
        )
        target_identity_present += int(rows[0]["n"] if isinstance(rows[0], dict) else rows[0][0])

    return {
        "repair_rows": int(len(repair)),
        "old_player_week_rows_present_total": old_player_week_rows_present_total,
        "old_changed_player_week_remaining": old_changed_player_week_remaining,
        "old_from_identity_remaining": old_from_identity_remaining,
        "new_player_week_present": new_player_week_present,
        "target_identity_present": target_identity_present,
        "same_player_week_repairs": int(repair["from_player_week"].eq(repair["to_player_week"]).sum()),
    }


def write_artifacts(repair: pd.DataFrame, manifest: dict[str, Any]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    repair.to_csv(OUT_DIR / "known_identity_player_week_repair_map.csv", index=False)
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply local parquet and live Fly repairs.")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify the already-written repair map against Fly without rebuilding or applying it.",
    )
    parser.add_argument(
        "--bio-only",
        action="store_true",
        help="Repair local bio parquet files from the already-written repair map without touching Fly.",
    )
    args = parser.parse_args()

    load_env()
    from multi_league.core.fly_writer import FlyWriter

    writer = FlyWriter()
    if args.verify_only:
        repair = load_existing_repair_map()
        live_verify = verify_live(writer, repair)
        if (
            live_verify["old_changed_player_week_remaining"] != 0
            or live_verify["old_from_identity_remaining"] != 0
            or live_verify["target_identity_present"] != len(repair)
        ):
            raise SystemExit("Live identity repair verification failed: " + json.dumps(live_verify, sort_keys=True))
        print(json.dumps(live_verify, indent=2, sort_keys=True))
        return 0
    if args.bio_only:
        repair = load_existing_repair_map()
        bio_results = [backup_and_repair_bio(path, repair, args.apply) for path in BIO_PATHS]
        print(json.dumps({"applied": args.apply, "bio_results": bio_results}, indent=2, sort_keys=True))
        return 0

    repair = build_repair_map(writer)
    verify_no_target_collisions(writer, repair)
    write_local_repair_map(repair)

    bio_results = [backup_and_repair_bio(path, repair, args.apply) for path in BIO_PATHS]
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_table = f"nfl_historical.identity_known_name_repair_backup_{stamp}"
    live_verify = {"repair_rows": int(len(repair)), "old_player_week_remaining": None, "new_player_week_present": None}

    if args.apply:
        create_backup_table(writer, repair, backup_table)
        apply_live_updates(writer, repair)
        live_verify = verify_live(writer, repair)
        if (
            live_verify["old_changed_player_week_remaining"] != 0
            or live_verify["old_from_identity_remaining"] != 0
            or live_verify["target_identity_present"] != len(repair)
        ):
            raise SystemExit("Live identity repair verification failed: " + json.dumps(live_verify, sort_keys=True))

    manifest = {
        "created_at_utc": stamp,
        "applied": args.apply,
        "repair_rows": int(len(repair)),
        "repair_types": repair["repair_type"].value_counts().to_dict(),
        "repair_map": str(REPAIR_MAP_PATH),
        "backup_table": f"___ops.{backup_table}" if args.apply else None,
        "bio_results": bio_results,
        "live_verify": live_verify,
    }
    write_artifacts(repair, manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
