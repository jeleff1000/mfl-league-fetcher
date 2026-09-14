#!/usr/bin/env python3
"""Dry-run or apply reviewed raw-atom packages to the NFL supertable.

Default mode is read-only. It validates the local package artifacts, performs
live read-only key checks when Fly read credentials are available, and writes a
report folder.

Live writes require both:

    --execute --confirm APPLY_RAW_ATOMS

The live path stages package rows into server-side tables, applies set-based
updates/inserts/deletes, and leaves derived fantasy/aggregate/rank columns for a
separate recompute pass.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
CATALOG_ROOT = ORGANIZED_ROOT / "_catalog"
PBP_ROOT = ORGANIZED_ROOT / "stathead_generated"
TARGET_TABLE = "nfl_historical.nfl_player_stats_all"
BIO_TABLE = "nfl_historical.player_bio"
CONFIRM_TOKEN = "APPLY_RAW_ATOMS"

# These are true raw atoms present in the reviewed package but absent from the
# current live table. Live execution only creates them when explicitly allowed.
SCHEMA_ADDITION_COLUMNS = {
    "kickoff_return_long": "DOUBLE",
    "punt_return_long": "DOUBLE",
}

# Historical builders used different names for the same 60+ FG atom. Keep both
# live canonical columns in sync from the staged PBP source column.
LIVE_COLUMN_ALIASES = {
    "fg_made_60plus": ["fg_made_60_", "fg_made_60_plus_canonical"],
}

TEAM_CODE_ALIASES = {
    "GNB": "GB",
    "KAN": "KC",
    "NOR": "NO",
    "NWE": "NE",
    "SFO": "SF",
    "TAM": "TB",
}

PFR_SAFE_UPDATE_FILE = "pfr_supertable_safe_stat_updates_wide.parquet"
PFR_INSERT_FILE = "pfr_supertable_insert_ready_rows.parquet"
HOLDOUT_CONTEXT_FILE = "pfr_holdout_context_overwrite_rows.parquet"
HOLDOUT_RETARGET_FILE = "pfr_holdout_identity_retarget_rows.parquet"
HOLDOUT_IDENTITY_FIELD_FILE = "pfr_holdout_identity_field_repairs.csv"
HOLDOUT_BIO_PFR_FILE = "pfr_holdout_bio_pfr_id_repairs.csv"
HOLDOUT_HEADSHOT_FILE = "pfr_holdout_headshot_url_repairs.csv"
HOLDOUT_DELETE_FILE = "pfr_holdout_delete_or_rekey_rows.csv"
PBP_WEEKLY_FILE = "pbp_player_week_rollup.parquet"

PFR_WEEKLY_STAT_COLUMNS = [
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "sack_yards_lost",
    "passing_long",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "rushing_long",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "receiving_long",
    "targets",
    "fumbles",
    "fumbles_lost",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_tds",
    "def_sacks",
    "def_tackles_with_assist",
    "def_tackles_solo",
    "def_tackle_assists",
    "fum_rec",
    "fum_rec_yds",
    "fum_ret_td",
    "def_fumbles_forced",
    "def_pass_defended",
    "def_tackles_for_loss",
    "def_qb_hits",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "kickoff_return_long",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "punt_return_long",
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "punts",
    "punt_yards",
    "punt_long",
]

PBP_ATOM_COLUMNS = [
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "completions_40plus",
    "completions_50plus",
    "passing_tds_40plus",
    "passing_tds_50plus",
    "receptions_0_4",
    "receptions_5_9",
    "receptions_10_19",
    "receptions_20_29",
    "receptions_30_39",
    "receptions_40plus",
    "receiving_tds_40plus",
    "receiving_tds_50plus",
    "fumbles",
    "fumbles_lost",
    "fum_rec",
    "fum_rec_yds",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_ret_td",
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_blocked",
    "fg_yards",
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
    "punts",
    "punt_yards",
    "punt_long",
    "punts_blocked",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "special_teams_tds",
    "def_sacks",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_fumbles_forced",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_tackles_for_loss",
    "def_pass_defended",
    "def_qb_hits",
    "def_safeties",
    "def_blk_kick",
    "special_teams_tackles_solo",
]

PBP_EXISTING_UPDATE_COLUMNS = [col for col in PBP_ATOM_COLUMNS if col not in set(PFR_WEEKLY_STAT_COLUMNS)]

CONTEXT_MATCH_COLUMNS = ["year", "week", "season_type", "nfl_team", "opponent_nfl_team"]


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


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def latest_dir(root: Path, glob_pattern: str, required_file: str) -> Path:
    dirs = sorted(root.glob(glob_pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in dirs:
        if (path / required_file).exists():
            return path
    raise FileNotFoundError(f"No {glob_pattern!r} directory under {root} containing {required_file}")


def default_pfr_package_dir() -> Path:
    preferred = CATALOG_ROOT / "pfr_supertable_update_package_sota_identity_overrides_trace_20260512T224000Z"
    if (preferred / PFR_SAFE_UPDATE_FILE).exists():
        return preferred
    return latest_dir(CATALOG_ROOT, "pfr_supertable_update_package_*", PFR_SAFE_UPDATE_FILE)


def default_holdout_package_dir() -> Path:
    preferred = CATALOG_ROOT / "supertable_holdout_resolution_sota_identity_overrides_headshots_20260512T231500Z"
    if (preferred / HOLDOUT_HEADSHOT_FILE).exists():
        return preferred
    return latest_dir(CATALOG_ROOT, "supertable_holdout_resolution_*", HOLDOUT_CONTEXT_FILE)


def default_pbp_rollup_dir() -> Path:
    preferred = PBP_ROOT / "pbp_supertable_audit_1978_1998_sota_bridge_v3_20260512T211500Z"
    if (preferred / PBP_WEEKLY_FILE).exists():
        return preferred
    return latest_dir(PBP_ROOT, "pbp_supertable_audit_1978_1998*", PBP_WEEKLY_FILE)


def parquet_rows(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def parquet_columns(path: Path) -> list[str]:
    return pq.read_schema(path).names


def read_parquet(path: Path, columns: Iterable[str] | None = None) -> pd.DataFrame:
    if columns is None:
        return pd.read_parquet(path)
    available = set(parquet_columns(path))
    cols = [col for col in columns if col in available]
    return pd.read_parquet(path, columns=cols)


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def clean_keys(series: pd.Series) -> pd.Series:
    return series.dropna().astype(str)


def unique_keys(frame: pd.DataFrame, col: str = "player_week") -> set[str]:
    if frame.empty or col not in frame.columns:
        return set()
    return set(clean_keys(frame[col]))


def duplicate_count(frame: pd.DataFrame, col: str = "player_week") -> int:
    if frame.empty or col not in frame.columns:
        return 0
    values = clean_keys(frame[col])
    return int(values.duplicated().sum())


def context_key_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "player_week" not in frame.columns:
        return pd.Series(dtype=str)
    parts = []
    for col in ["player_week", *CONTEXT_MATCH_COLUMNS]:
        if col in frame.columns:
            parts.append(frame[col].fillna("").astype(str).str.upper().str.strip())
        else:
            parts.append(pd.Series([""] * len(frame), index=frame.index, dtype=str))
    return parts[0].str.cat(parts[1:], sep="|")


def duplicate_context_count(frame: pd.DataFrame) -> int:
    keys = context_key_series(frame)
    if keys.empty:
        return 0
    return int(keys.duplicated().sum())


def duplicate_player_week_rows(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, frame in frames.items():
        if frame.empty or "player_week" not in frame.columns:
            continue
        values = frame["player_week"].fillna("").astype(str)
        dupes = frame[values.duplicated(keep=False)].copy()
        if dupes.empty:
            continue
        keep_cols = [
            col
            for col in [
                "player_week",
                "NFL_player_id",
                "player",
                "position",
                "nfl_team",
                "opponent_nfl_team",
                "year",
                "week",
                "season_type",
                "event_roles",
                "carries",
                "rushing_yards",
                "targets",
                "receptions",
                "receiving_yards",
                "def_tackles_solo",
                "def_sacks",
            ]
            if col in dupes.columns
        ]
        dupes = dupes[keep_cols].copy()
        dupes.insert(0, "package", name)
        dupes["context_key"] = context_key_series(frame.loc[dupes.index]).to_numpy()
        rows.append(dupes)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True, sort=False)


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q_table(name: str) -> str:
    return ".".join(q_ident(part) for part in name.split("."))


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def safe_sql_text(value: Any) -> str:
    return str(value).replace(";", "%3B")


def sql_in(values: Iterable[str]) -> str:
    cleaned = [q_lit(value) for value in values]
    return "(" + ",".join(cleaned or ["''"]) + ")"


def chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


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
    if dtype in {"FLOAT", "REAL", "DOUBLE"} or dtype.startswith("DECIMAL"):
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


def infer_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    return "VARCHAR"


def type_for_col(col: str, frame: pd.DataFrame, schema_types: dict[str, str]) -> str:
    if col in schema_types:
        return schema_types[col]
    if col in frame.columns:
        return infer_sql_type(frame[col])
    return "VARCHAR"


def rows_values_sql(frame: pd.DataFrame, columns: list[str], type_by_col: dict[str, str]) -> str:
    values = []
    for _, row in frame.iterrows():
        values.append("(" + ",".join(sql_value(row.get(col), type_by_col.get(col, "VARCHAR")) for col in columns) + ")")
    return ",".join(values)


def fly_reader_or_none() -> FlyReader | None:
    if not os.environ.get("DATABASE_SERVER_URL") or not os.environ.get("DATABASE_READ_TOKEN"):
        return None
    return FlyReader()


def schema_types_from_live(reader: FlyReader | None) -> dict[str, str]:
    if reader is None:
        return {}
    rows = reader.query(f"DESCRIBE {q_table(TARGET_TABLE)}", database="___ops")
    types: dict[str, str] = {}
    for row in rows:
        col = row.get("column_name") or row.get("Column") or row.get("column")
        typ = row.get("column_type") or row.get("Type") or row.get("type")
        if col and typ:
            types[str(col)] = str(typ)
    return types


def bio_schema_types_from_live(reader: FlyReader | None) -> dict[str, str]:
    if reader is None:
        return {}
    try:
        rows = reader.query(f"DESCRIBE {q_table(BIO_TABLE)}", database="___ops")
    except Exception:
        return {}
    types: dict[str, str] = {}
    for row in rows:
        col = row.get("column_name") or row.get("Column") or row.get("column")
        typ = row.get("column_type") or row.get("Type") or row.get("type")
        if col and typ:
            types[str(col)] = str(typ)
    return types


def count_live_keys(reader: FlyReader, keys: set[str], *, chunk_size: int) -> int:
    total = 0
    key_list = sorted(keys)
    for group in chunks(key_list, chunk_size):
        rows = reader.query(
            f"SELECT COUNT(*) AS n FROM {q_table(TARGET_TABLE)} WHERE player_week IN {sql_in(group)}",
            database="___ops",
        )
        total += int(rows[0].get("n", 0)) if rows else 0
    return total


def fetch_live_keys(reader: FlyReader, keys: set[str], *, chunk_size: int) -> set[str]:
    found: set[str] = set()
    key_list = sorted(keys)
    for group in chunks(key_list, chunk_size):
        rows = reader.query(
            f"SELECT player_week FROM {q_table(TARGET_TABLE)} WHERE player_week IN {sql_in(group)}",
            database="___ops",
        )
        found.update(str(row["player_week"]) for row in rows if row.get("player_week") is not None)
    return found


def live_pbp_keys_after_pfr(
    reader: FlyReader,
    pbp_frame: pd.DataFrame,
    pfr_insert_keys: set[str],
    *,
    chunk_size: int,
) -> tuple[dict[str, int], pd.DataFrame]:
    pbp_keys = unique_keys(pbp_frame)
    live_keys = fetch_live_keys(reader, pbp_keys, chunk_size=chunk_size)
    live = len(live_keys)
    missing_before_pfr = len(pbp_keys) - min(live, len(pbp_keys))
    package_insert_overlap = len(pbp_keys & pfr_insert_keys)
    missing_after_keys = pbp_keys - live_keys - pfr_insert_keys
    missing_after_rows = pbp_frame[pbp_frame["player_week"].astype(str).isin(missing_after_keys)].copy()
    estimated_missing_after_pfr = max(0, missing_before_pfr - package_insert_overlap)
    return (
        {
            "live_matching_pbp_keys": live,
            "pbp_keys_missing_before_pfr_inserts_est": missing_before_pfr,
            "pbp_keys_supplied_by_pfr_insert_package": package_insert_overlap,
            "pbp_keys_missing_after_pfr_inserts_est": estimated_missing_after_pfr,
        },
        missing_after_rows,
    )


def fetch_current_headshots(reader: FlyReader, ids: list[str]) -> pd.DataFrame:
    rows = reader.query(
        f"""
        SELECT NFL_player_id, player, COUNT(*) AS rows,
               ANY_VALUE(headshot_url) AS sample_headshot_url
        FROM {q_table(TARGET_TABLE)}
        WHERE NFL_player_id IN {sql_in(ids)}
        GROUP BY NFL_player_id, player
        ORDER BY NFL_player_id, player
        """,
        database="___ops",
    )
    return pd.DataFrame(rows)


def duplicate_live_player_week_count(reader: FlyReader) -> int:
    rows = reader.query(
        f"""
        SELECT COUNT(*) AS n
        FROM (
            SELECT player_week
            FROM {q_table(TARGET_TABLE)}
            WHERE player_week IS NOT NULL
            GROUP BY player_week
            HAVING COUNT(*) > 1
        ) d
        """,
        database="___ops",
    )
    return int(rows[0].get("n", 0)) if rows else 0


def load_package_frames(pfr_dir: Path, holdout_dir: Path, pbp_dir: Path) -> dict[str, pd.DataFrame]:
    pfr_safe_cols = [
        "player_week",
        *CONTEXT_MATCH_COLUMNS,
        *PFR_WEEKLY_STAT_COLUMNS,
    ]
    pfr_insert_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "year",
        "week",
        "season_type",
        "data_source",
        "pfr_id",
        *PFR_WEEKLY_STAT_COLUMNS,
    ]
    holdout_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "year",
        "week",
        "season_type",
        "data_source",
        "pfr_id",
        *PFR_WEEKLY_STAT_COLUMNS,
    ]
    pbp_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        *PBP_ATOM_COLUMNS,
    ]

    frames = {
        "pfr_safe_updates": read_parquet(pfr_dir / PFR_SAFE_UPDATE_FILE, pfr_safe_cols),
        "pfr_insert_rows": read_parquet(pfr_dir / PFR_INSERT_FILE, pfr_insert_cols),
        "holdout_context_overwrites": read_parquet(holdout_dir / HOLDOUT_CONTEXT_FILE, holdout_cols),
        "holdout_identity_retarget": read_parquet(holdout_dir / HOLDOUT_RETARGET_FILE, holdout_cols),
        "holdout_identity_field_repairs": read_csv_if_exists(holdout_dir / HOLDOUT_IDENTITY_FIELD_FILE),
        "holdout_bio_pfr_id_repairs": read_csv_if_exists(holdout_dir / HOLDOUT_BIO_PFR_FILE),
        "holdout_headshot_url_repairs": read_csv_if_exists(holdout_dir / HOLDOUT_HEADSHOT_FILE),
        "holdout_delete_or_rekey": read_csv_if_exists(holdout_dir / HOLDOUT_DELETE_FILE),
        "pbp_weekly_rollup": read_parquet(pbp_dir / PBP_WEEKLY_FILE, pbp_cols),
    }
    if "fantasy_position" not in frames["pfr_insert_rows"].columns and "position" in frames["pfr_insert_rows"].columns:
        frames["pfr_insert_rows"]["fantasy_position"] = frames["pfr_insert_rows"]["position"]
    pbp = frames["pbp_weekly_rollup"]
    if not pbp.empty:
        pbp["nfl_position"] = pbp.get("position")
        pbp["fantasy_position"] = pbp.get("position")
        pbp["data_source"] = "stathead_pbp_weekly_rollup_insert"
    return frames


def expanded_columns(candidates: Iterable[str], schema_types: dict[str, str]) -> list[str]:
    expanded: list[str] = []
    for col in candidates:
        if col not in expanded:
            expanded.append(col)
        for target in LIVE_COLUMN_ALIASES.get(col, []):
            if target not in expanded and (not schema_types or target in schema_types):
                expanded.append(target)
    return expanded


def apply_live_column_aliases(frames: dict[str, pd.DataFrame], schema_types: dict[str, str]) -> pd.DataFrame:
    if not schema_types:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for frame_name in ["pbp_weekly_rollup"]:
        frame = frames.get(frame_name, pd.DataFrame())
        if frame.empty:
            continue
        for source, targets in LIVE_COLUMN_ALIASES.items():
            if source not in frame.columns:
                continue
            for target in targets:
                if target not in schema_types:
                    continue
                frame[target] = frame[source]
                rows.append(
                    {
                        "package": frame_name,
                        "source_column": source,
                        "target_column": target,
                        "target_in_live_schema": True,
                        "rows_populated": len(frame),
                    }
                )
    return pd.DataFrame(rows)


def schema_addition_plan(frames: dict[str, pd.DataFrame], schema_types: dict[str, str]) -> pd.DataFrame:
    if not schema_types:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    package_specs = {
        "pfr_safe_updates": PFR_WEEKLY_STAT_COLUMNS,
        "pfr_insert_rows": PFR_WEEKLY_STAT_COLUMNS,
        "holdout_context_overwrites": PFR_WEEKLY_STAT_COLUMNS,
        "holdout_identity_retarget": PFR_WEEKLY_STAT_COLUMNS,
        "pbp_weekly_rollup": PBP_ATOM_COLUMNS,
    }
    for package, candidates in package_specs.items():
        frame = frames.get(package, pd.DataFrame())
        if frame.empty:
            continue
        for col in candidates:
            if col in SCHEMA_ADDITION_COLUMNS and col in frame.columns and col not in schema_types:
                rows.append(
                    {
                        "package": package,
                        "column": col,
                        "sql_type": SCHEMA_ADDITION_COLUMNS[col],
                        "rows_available": len(frame),
                    }
                )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(["column", "sql_type"]).reset_index(drop=True)


def targetable_columns(frame: pd.DataFrame, schema_types: dict[str, str], candidates: Iterable[str]) -> list[str]:
    return [col for col in candidates if col in frame.columns and (not schema_types or col in schema_types)]


def stat_column_plan(frames: dict[str, pd.DataFrame], schema_types: dict[str, str]) -> pd.DataFrame:
    live_cols = set(schema_types)
    rows = []
    for op, columns in {
        "pfr_safe_stat_update": PFR_WEEKLY_STAT_COLUMNS,
        "pfr_insert": PFR_WEEKLY_STAT_COLUMNS,
        "holdout_context_overwrite": PFR_WEEKLY_STAT_COLUMNS,
        "holdout_identity_retarget": PFR_WEEKLY_STAT_COLUMNS,
        "pbp_weekly_atom_update": PBP_EXISTING_UPDATE_COLUMNS,
        "pbp_insert": PBP_ATOM_COLUMNS,
    }.items():
        frame_name = {
            "pfr_safe_stat_update": "pfr_safe_updates",
            "pfr_insert": "pfr_insert_rows",
            "holdout_context_overwrite": "holdout_context_overwrites",
            "holdout_identity_retarget": "holdout_identity_retarget",
            "pbp_weekly_atom_update": "pbp_weekly_rollup",
            "pbp_insert": "pbp_weekly_rollup",
        }[op]
        frame_cols = set(frames[frame_name].columns)
        for col in expanded_columns(columns, schema_types):
            rows.append(
                {
                    "operation": op,
                    "column": col,
                    "in_package": col in frame_cols,
                    "in_live_schema": (col in live_cols) if live_cols else None,
                    "will_apply": (col in frame_cols and (not live_cols or col in live_cols)),
                }
            )
    return pd.DataFrame(rows)


def unresolved_missing_column_plan(
    column_plan: pd.DataFrame,
    schema_additions: pd.DataFrame,
    schema_types: dict[str, str],
) -> pd.DataFrame:
    if column_plan.empty or not schema_types:
        return pd.DataFrame()
    planned_additions = set(schema_additions["column"].astype(str)) if not schema_additions.empty else set()
    covered_alias_sources = {
        source for source, targets in LIVE_COLUMN_ALIASES.items() if any(target in schema_types for target in targets)
    }
    missing = column_plan[
        column_plan["in_package"].fillna(False)
        & column_plan["in_live_schema"].eq(False)
        & ~column_plan["column"].isin(planned_additions)
        & ~column_plan["column"].isin(covered_alias_sources)
    ].copy()
    if missing.empty:
        return pd.DataFrame()
    return missing[["operation", "column", "in_package", "in_live_schema", "will_apply"]].drop_duplicates()


def assert_execute_gates(
    frames: dict[str, pd.DataFrame],
    live_counts: pd.DataFrame,
    gates: dict[str, Any],
) -> None:
    if gates.get("live_read_skipped", True):
        raise SystemExit("Live write refused: live read gates were skipped")
    if int(gates.get("live_duplicate_player_week_keys", -1)) != 0:
        raise SystemExit("Live write refused: live supertable has duplicate player_week keys")
    if live_counts.empty:
        raise SystemExit("Live write refused: live key counts are missing")

    counts = {
        str(row["package"]): int(row["live_matching_rows"])
        for _, row in live_counts.iterrows()
        if "package" in row and "live_matching_rows" in row
    }
    expected_existing = [
        "pfr_safe_updates",
        "holdout_context_overwrites",
        "holdout_identity_retarget",
        "holdout_identity_field_repairs",
        "holdout_delete_or_rekey",
    ]
    for package in expected_existing:
        expected = len(unique_keys(frames[package]))
        if counts.get(package) != expected:
            raise SystemExit(
                f"Live write refused: {package} expected {expected} live matches, got {counts.get(package)}"
            )
    if counts.get("pfr_insert_rows") != 0:
        raise SystemExit(
            f"Live write refused: pfr_insert_rows already has {counts.get('pfr_insert_rows')} live matches"
        )


def operation_summary(frames: dict[str, pd.DataFrame], column_plan: pd.DataFrame) -> pd.DataFrame:
    pfr_delta_atoms = (
        int(frames["pfr_safe_updates"][PFR_WEEKLY_STAT_COLUMNS].ne(0).sum().sum())
        if not frames["pfr_safe_updates"].empty
        else 0
    )
    pbp_cols = column_plan[
        column_plan["operation"].eq("pbp_weekly_atom_update") & column_plan["will_apply"].fillna(False)
    ]["column"].tolist()
    pbp_insert_cols = column_plan[column_plan["operation"].eq("pbp_insert") & column_plan["will_apply"].fillna(False)][
        "column"
    ].tolist()
    pbp_nonzero_atoms = int(frames["pbp_weekly_rollup"][pbp_cols].fillna(0).ne(0).sum().sum()) if pbp_cols else 0
    pbp_insert_nonzero_atoms = (
        int(frames["pbp_weekly_rollup"][pbp_insert_cols].fillna(0).ne(0).sum().sum()) if pbp_insert_cols else 0
    )
    rows = [
        {
            "operation": "pfr_safe_stat_update",
            "rows": len(frames["pfr_safe_updates"]),
            "unique_player_week": len(unique_keys(frames["pfr_safe_updates"])),
            "duplicate_player_week": duplicate_count(frames["pfr_safe_updates"]),
            "duplicate_context_key": duplicate_context_count(frames["pfr_safe_updates"]),
            "stat_columns": int(
                column_plan.query("operation == 'pfr_safe_stat_update' and will_apply == True").shape[0]
            ),
            "nonzero_atom_cells": pfr_delta_atoms,
        },
        {
            "operation": "pfr_insert_missing_weekly_rows",
            "rows": len(frames["pfr_insert_rows"]),
            "unique_player_week": len(unique_keys(frames["pfr_insert_rows"])),
            "duplicate_player_week": duplicate_count(frames["pfr_insert_rows"]),
            "duplicate_context_key": duplicate_context_count(frames["pfr_insert_rows"]),
            "stat_columns": int(column_plan.query("operation == 'pfr_insert' and will_apply == True").shape[0]),
            "nonzero_atom_cells": int(frames["pfr_insert_rows"][PFR_WEEKLY_STAT_COLUMNS].fillna(0).ne(0).sum().sum()),
        },
        {
            "operation": "holdout_context_overwrite",
            "rows": len(frames["holdout_context_overwrites"]),
            "unique_player_week": len(unique_keys(frames["holdout_context_overwrites"])),
            "duplicate_player_week": duplicate_count(frames["holdout_context_overwrites"]),
            "duplicate_context_key": duplicate_context_count(frames["holdout_context_overwrites"]),
            "stat_columns": int(
                column_plan.query("operation == 'holdout_context_overwrite' and will_apply == True").shape[0]
            ),
            "nonzero_atom_cells": int(
                frames["holdout_context_overwrites"][PFR_WEEKLY_STAT_COLUMNS].fillna(0).ne(0).sum().sum()
            ),
        },
        {
            "operation": "holdout_identity_retarget_upsert",
            "rows": len(frames["holdout_identity_retarget"]),
            "unique_player_week": len(unique_keys(frames["holdout_identity_retarget"])),
            "duplicate_player_week": duplicate_count(frames["holdout_identity_retarget"]),
            "duplicate_context_key": duplicate_context_count(frames["holdout_identity_retarget"]),
            "stat_columns": int(
                column_plan.query("operation == 'holdout_identity_retarget' and will_apply == True").shape[0]
            ),
            "nonzero_atom_cells": int(
                frames["holdout_identity_retarget"][PFR_WEEKLY_STAT_COLUMNS].fillna(0).ne(0).sum().sum()
            ),
        },
        {
            "operation": "holdout_identity_field_assert",
            "rows": len(frames["holdout_identity_field_repairs"]),
            "unique_player_week": len(unique_keys(frames["holdout_identity_field_repairs"])),
            "duplicate_player_week": duplicate_count(frames["holdout_identity_field_repairs"]),
            "duplicate_context_key": duplicate_context_count(frames["holdout_identity_field_repairs"]),
            "stat_columns": 0,
            "nonzero_atom_cells": 0,
        },
        {
            "operation": "holdout_bio_pfr_id_repair",
            "rows": len(frames["holdout_bio_pfr_id_repairs"]),
            "unique_player_week": 0,
            "duplicate_player_week": 0,
            "duplicate_context_key": 0,
            "stat_columns": 0,
            "nonzero_atom_cells": 0,
        },
        {
            "operation": "holdout_headshot_url_repair",
            "rows": len(frames["holdout_headshot_url_repairs"]),
            "unique_player_week": 0,
            "duplicate_player_week": 0,
            "duplicate_context_key": 0,
            "stat_columns": 0,
            "nonzero_atom_cells": 0,
        },
        {
            "operation": "holdout_delete_or_rekey",
            "rows": len(frames["holdout_delete_or_rekey"]),
            "unique_player_week": len(unique_keys(frames["holdout_delete_or_rekey"])),
            "duplicate_player_week": duplicate_count(frames["holdout_delete_or_rekey"]),
            "duplicate_context_key": duplicate_context_count(frames["holdout_delete_or_rekey"]),
            "stat_columns": 0,
            "nonzero_atom_cells": 0,
        },
        {
            "operation": "pbp_weekly_atom_update_1978_1998",
            "rows": len(frames["pbp_weekly_rollup"]),
            "unique_player_week": len(unique_keys(frames["pbp_weekly_rollup"])),
            "duplicate_player_week": duplicate_count(frames["pbp_weekly_rollup"]),
            "duplicate_context_key": duplicate_context_count(frames["pbp_weekly_rollup"]),
            "stat_columns": len(pbp_cols),
            "nonzero_atom_cells": pbp_nonzero_atoms,
        },
        {
            "operation": "pbp_insert_missing_weekly_rows_after_pfr_candidate",
            "rows": len(frames["pbp_weekly_rollup"]),
            "unique_player_week": len(unique_keys(frames["pbp_weekly_rollup"])),
            "duplicate_player_week": duplicate_count(frames["pbp_weekly_rollup"]),
            "duplicate_context_key": duplicate_context_count(frames["pbp_weekly_rollup"]),
            "stat_columns": len(pbp_insert_cols),
            "nonzero_atom_cells": pbp_insert_nonzero_atoms,
        },
    ]
    return pd.DataFrame(rows)


def overlap_summary(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    key_sets = {name: unique_keys(frame) for name, frame in frames.items()}
    pairs = [
        ("pfr_safe_updates", "pfr_insert_rows"),
        ("pfr_insert_rows", "pbp_weekly_rollup"),
        ("pfr_safe_updates", "holdout_context_overwrites"),
        ("pfr_safe_updates", "pbp_weekly_rollup"),
        ("holdout_context_overwrites", "pbp_weekly_rollup"),
        ("holdout_identity_retarget", "holdout_identity_field_repairs"),
    ]
    return pd.DataFrame(
        [
            {
                "left": left,
                "right": right,
                "left_keys": len(key_sets[left]),
                "right_keys": len(key_sets[right]),
                "overlap_keys": len(key_sets[left] & key_sets[right]),
            }
            for left, right in pairs
        ]
    )


def live_key_counts(
    reader: FlyReader | None,
    frames: dict[str, pd.DataFrame],
    *,
    chunk_size: int,
) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if reader is None:
        return pd.DataFrame(), {"live_read_skipped": True}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    rows = []
    for name in [
        "pfr_safe_updates",
        "pfr_insert_rows",
        "holdout_context_overwrites",
        "holdout_identity_retarget",
        "holdout_identity_field_repairs",
        "holdout_delete_or_rekey",
    ]:
        keys = unique_keys(frames[name])
        rows.append(
            {
                "package": name,
                "package_unique_keys": len(keys),
                "live_matching_rows": count_live_keys(reader, keys, chunk_size=chunk_size) if keys else 0,
            }
        )

    pbp_counts, pbp_missing_after_pfr = live_pbp_keys_after_pfr(
        reader,
        frames["pbp_weekly_rollup"],
        unique_keys(frames["pfr_insert_rows"]),
        chunk_size=chunk_size,
    )
    rows.append(
        {
            "package": "pbp_weekly_rollup",
            "package_unique_keys": len(unique_keys(frames["pbp_weekly_rollup"])),
            "live_matching_rows": pbp_counts["live_matching_pbp_keys"],
        }
    )

    ids = (
        frames["holdout_headshot_url_repairs"].get("NFL_player_id", pd.Series(dtype=str)).dropna().astype(str).tolist()
    )
    headshots = fetch_current_headshots(reader, ids) if ids else pd.DataFrame()
    gates = {
        "live_read_skipped": False,
        "live_duplicate_player_week_keys": duplicate_live_player_week_count(reader),
        **pbp_counts,
    }
    try:
        bio_rows = reader.query(
            f"""
            SELECT NFL_player_id, player, pfr_id
            FROM {q_table(BIO_TABLE)}
            WHERE NFL_player_id IN {sql_in(frames['holdout_bio_pfr_id_repairs']['NFL_player_id'].dropna().astype(str).tolist())}
            ORDER BY NFL_player_id
            """,
            database="___ops",
        )
        bio = pd.DataFrame(bio_rows)
    except Exception as exc:
        bio = pd.DataFrame([{"error": str(exc)}])
    return pd.DataFrame(rows), gates, headshots, bio, pbp_missing_after_pfr


def create_stage_table(
    writer: FlyWriter, table: str, frame: pd.DataFrame, columns: list[str], schema_types: dict[str, str]
) -> None:
    defs = ", ".join(f"{q_ident(col)} {type_for_col(col, frame, schema_types)}" for col in columns)
    writer.execute(f"DROP TABLE IF EXISTS {q_table(table)}", database="___ops")
    writer.execute(f"CREATE TABLE {q_table(table)} ({defs})", database="___ops")


def insert_stage_rows(
    writer: FlyWriter,
    table: str,
    frame: pd.DataFrame,
    columns: list[str],
    schema_types: dict[str, str],
    *,
    batch_rows: int,
) -> None:
    if frame.empty:
        return
    type_by_col = {col: type_for_col(col, frame, schema_types) for col in columns}
    col_sql = ", ".join(q_ident(col) for col in columns)
    for batch in chunks(list(range(len(frame))), batch_rows):
        part = frame.iloc[batch][columns].copy()
        values = rows_values_sql(part, columns, type_by_col)
        writer.execute(f"INSERT INTO {q_table(table)} ({col_sql}) VALUES {values}", database="___ops")


def stage_frame(
    writer: FlyWriter,
    table: str,
    frame: pd.DataFrame,
    columns: list[str],
    schema_types: dict[str, str],
    *,
    batch_rows: int,
) -> None:
    create_stage_table(writer, table, frame, columns, schema_types)
    insert_stage_rows(writer, table, frame, columns, schema_types, batch_rows=batch_rows)


def normalized_team_sql(alias: str, col: str) -> str:
    raw = f"UPPER(TRIM(COALESCE({alias}.{q_ident(col)}, '')))"
    cases = " ".join(f"WHEN {q_lit(src)} THEN {q_lit(dst)}" for src, dst in TEAM_CODE_ALIASES.items())
    return f"CASE {raw} {cases} ELSE {raw} END"


def context_predicate(alias_left: str, alias_right: str, columns: list[str]) -> str:
    parts = [f"{alias_left}.{q_ident('player_week')} = {alias_right}.{q_ident('player_week')}"]
    for col in CONTEXT_MATCH_COLUMNS:
        if col not in columns:
            continue
        if col in {"year", "week"}:
            parts.append(f"{alias_left}.{q_ident(col)} = {alias_right}.{q_ident(col)}")
        elif col in {"nfl_team", "opponent_nfl_team"}:
            parts.append(f"{normalized_team_sql(alias_left, col)} = {normalized_team_sql(alias_right, col)}")
        else:
            parts.append(
                f"UPPER(TRIM(COALESCE({alias_left}.{q_ident(col)}, ''))) = UPPER(TRIM(COALESCE({alias_right}.{q_ident(col)}, '')))"
            )
    return " AND ".join(parts)


def update_from_stage(
    writer: FlyWriter,
    stage_table: str,
    set_columns: list[str],
    *,
    match_context: bool,
    stage_columns: list[str],
) -> None:
    if not set_columns:
        return
    assignments = ", ".join(f"{q_ident(col)} = s.{q_ident(col)}" for col in set_columns)
    predicate = (
        context_predicate("t", "s", stage_columns)
        if match_context
        else f"t.{q_ident('player_week')} = s.{q_ident('player_week')}"
    )
    writer.execute(
        f"""
        UPDATE {q_table(TARGET_TABLE)} AS t
        SET {assignments}
        FROM {q_table(stage_table)} AS s
        WHERE {predicate}
        """,
        database="___ops",
    )


def insert_missing_from_stage(writer: FlyWriter, stage_table: str, insert_columns: list[str]) -> None:
    cols = ", ".join(q_ident(col) for col in insert_columns)
    writer.execute(
        f"""
        INSERT INTO {q_table(TARGET_TABLE)} ({cols})
        SELECT {cols}
        FROM {q_table(stage_table)} AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {q_table(TARGET_TABLE)} AS t
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
        )
        """,
        database="___ops",
    )


def insert_missing_unique_from_stage(writer: FlyWriter, stage_table: str, insert_columns: list[str]) -> None:
    cols = ", ".join(q_ident(col) for col in insert_columns)
    writer.execute(
        f"""
        INSERT INTO {q_table(TARGET_TABLE)} ({cols})
        SELECT {cols}
        FROM {q_table(stage_table)} AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {q_table(TARGET_TABLE)} AS t
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
        )
        AND s.{q_ident('player_week')} IN (
            SELECT {q_ident('player_week')}
            FROM {q_table(stage_table)}
            GROUP BY {q_ident('player_week')}
            HAVING COUNT(*) = 1
        )
        """,
        database="___ops",
    )


def apply_schema_additions(writer: FlyWriter, schema_additions: pd.DataFrame, schema_types: dict[str, str]) -> None:
    if schema_additions.empty:
        return
    for _, row in schema_additions.drop_duplicates(["column", "sql_type"]).iterrows():
        col = str(row["column"])
        sql_type = str(row["sql_type"])
        writer.execute(
            f"ALTER TABLE {q_table(TARGET_TABLE)} ADD COLUMN IF NOT EXISTS {q_ident(col)} {sql_type}",
            database="___ops",
        )
        schema_types[col] = sql_type


def execute_packages(
    frames: dict[str, pd.DataFrame],
    schema_types: dict[str, str],
    bio_schema_types: dict[str, str],
    schema_additions: pd.DataFrame,
    *,
    stage_prefix: str,
    batch_rows: int,
    output_dir: Path,
) -> None:
    writer = FlyWriter()
    executed: list[dict[str, Any]] = []

    def record(operation: str, table: str | None, rows: int) -> None:
        executed.append(
            {
                "operation": operation,
                "stage_table": table or "",
                "rows": rows,
                "completed_at_utc": datetime.now(UTC).isoformat(),
            }
        )
        pd.DataFrame(executed).to_csv(output_dir / "executed_operations.csv", index=False)

    backup_suffix = re.sub(r"[^A-Za-z0-9_]", "_", stage_prefix.split(".")[-1])
    writer.execute(
        f"CREATE TABLE {q_table('nfl_historical.nfl_player_stats_all_backup_' + backup_suffix)} AS SELECT * FROM {q_table(TARGET_TABLE)}",
        database="___ops",
    )
    record("backup_supertable_full", f"nfl_historical.nfl_player_stats_all_backup_{backup_suffix}", -1)
    if bio_schema_types:
        writer.execute(
            f"CREATE TABLE {q_table('nfl_historical.player_bio_backup_' + backup_suffix)} AS SELECT * FROM {q_table(BIO_TABLE)}",
            database="___ops",
        )
        record("backup_player_bio_full", f"nfl_historical.player_bio_backup_{backup_suffix}", -1)

    if not schema_additions.empty:
        apply_schema_additions(writer, schema_additions, schema_types)
        record("schema_add_columns", TARGET_TABLE, int(schema_additions["column"].nunique()))

    pfr_safe = frames["pfr_safe_updates"]
    safe_cols = targetable_columns(
        pfr_safe, schema_types, ["player_week", *CONTEXT_MATCH_COLUMNS, *PFR_WEEKLY_STAT_COLUMNS]
    )
    safe_set_cols = [col for col in PFR_WEEKLY_STAT_COLUMNS if col in safe_cols]
    stage = f"{stage_prefix}_pfr_safe"
    stage_frame(writer, stage, pfr_safe, safe_cols, schema_types, batch_rows=batch_rows)
    update_from_stage(writer, stage, safe_set_cols, match_context=True, stage_columns=safe_cols)
    record("pfr_safe_stat_update", stage, len(pfr_safe))

    pfr_insert = frames["pfr_insert_rows"].copy()
    insert_candidates = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "fantasy_position",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "year",
        "week",
        "season_type",
        "data_source",
        "pfr_id",
        *PFR_WEEKLY_STAT_COLUMNS,
    ]
    insert_cols = targetable_columns(pfr_insert, schema_types, insert_candidates)
    stage = f"{stage_prefix}_pfr_insert"
    stage_frame(writer, stage, pfr_insert, insert_cols, schema_types, batch_rows=batch_rows)
    insert_missing_from_stage(writer, stage, insert_cols)
    record("pfr_insert_missing_weekly_rows", stage, len(pfr_insert))

    context_rows = frames["holdout_context_overwrites"]
    context_cols = targetable_columns(context_rows, schema_types, insert_candidates)
    context_set_cols = [col for col in context_cols if col != "player_week"]
    stage = f"{stage_prefix}_holdout_context"
    stage_frame(writer, stage, context_rows, context_cols, schema_types, batch_rows=batch_rows)
    update_from_stage(writer, stage, context_set_cols, match_context=False, stage_columns=context_cols)
    record("holdout_context_overwrite", stage, len(context_rows))

    retarget = frames["holdout_identity_retarget"]
    retarget_cols = targetable_columns(retarget, schema_types, insert_candidates)
    retarget_set_cols = [col for col in retarget_cols if col != "player_week"]
    stage = f"{stage_prefix}_holdout_retarget"
    stage_frame(writer, stage, retarget, retarget_cols, schema_types, batch_rows=batch_rows)
    update_from_stage(writer, stage, retarget_set_cols, match_context=False, stage_columns=retarget_cols)
    insert_missing_from_stage(writer, stage, retarget_cols)
    record("holdout_identity_retarget_upsert", stage, len(retarget))

    identity = frames["holdout_identity_field_repairs"].copy()
    if not identity.empty:
        identity_cols = ["player_week", "correct_NFL_player_id", "player", "position"]
        stage = f"{stage_prefix}_identity_fields"
        stage_frame(
            writer,
            stage,
            identity,
            identity_cols,
            schema_types | {"correct_NFL_player_id": "VARCHAR"},
            batch_rows=batch_rows,
        )
        writer.execute(
            f"""
            UPDATE {q_table(TARGET_TABLE)} AS t
            SET {q_ident('NFL_player_id')} = s.{q_ident('correct_NFL_player_id')},
                {q_ident('player')} = s.{q_ident('player')},
                {q_ident('position')} = s.{q_ident('position')},
                {q_ident('nfl_position')} = s.{q_ident('position')}
            FROM {q_table(stage)} AS s
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
            """,
            database="___ops",
        )
        record("holdout_identity_field_assert", stage, len(identity))

    bio = frames["holdout_bio_pfr_id_repairs"].copy()
    if not bio.empty and bio_schema_types:
        bio_cols = ["NFL_player_id", "correct_pfr_id"]
        stage = f"{stage_prefix}_bio_pfr"
        stage_frame(
            writer, stage, bio, bio_cols, bio_schema_types | {"correct_pfr_id": "VARCHAR"}, batch_rows=batch_rows
        )
        writer.execute(
            f"""
            UPDATE {q_table(BIO_TABLE)} AS b
            SET {q_ident('pfr_id')} = s.{q_ident('correct_pfr_id')}
            FROM {q_table(stage)} AS s
            WHERE b.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
            """,
            database="___ops",
        )
        record("holdout_bio_pfr_id_repair", stage, len(bio))

    headshots = frames["holdout_headshot_url_repairs"].copy()
    if not headshots.empty:
        headshot_cols = ["NFL_player_id", "correct_headshot_url"]
        stage = f"{stage_prefix}_headshots"
        stage_frame(
            writer,
            stage,
            headshots,
            headshot_cols,
            schema_types | {"correct_headshot_url": "VARCHAR"},
            batch_rows=batch_rows,
        )
        writer.execute(
            f"""
            UPDATE {q_table(TARGET_TABLE)} AS t
            SET {q_ident('headshot_url')} = s.{q_ident('correct_headshot_url')}
            FROM {q_table(stage)} AS s
            WHERE t.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
            """,
            database="___ops",
        )
        record("holdout_headshot_url_repair", stage, len(headshots))

    deletes = frames["holdout_delete_or_rekey"].copy()
    delete_keys = sorted(unique_keys(deletes))
    for group in chunks(delete_keys, 500):
        writer.execute(
            f"DELETE FROM {q_table(TARGET_TABLE)} WHERE {q_ident('player_week')} IN {sql_in(group)}",
            database="___ops",
        )
    record("holdout_delete_or_rekey", None, len(delete_keys))

    pbp = frames["pbp_weekly_rollup"]
    pbp_insert_candidates = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "fantasy_position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "data_source",
        *expanded_columns(PBP_ATOM_COLUMNS, schema_types),
    ]
    pbp_cols = targetable_columns(pbp, schema_types, pbp_insert_candidates)
    pbp_set_cols = [col for col in expanded_columns(PBP_EXISTING_UPDATE_COLUMNS, schema_types) if col in pbp_cols]
    stage = f"{stage_prefix}_pbp_weekly"
    stage_frame(writer, stage, pbp, pbp_cols, schema_types, batch_rows=batch_rows)
    update_from_stage(writer, stage, pbp_set_cols, match_context=True, stage_columns=pbp_cols)
    record("pbp_weekly_atom_update_1978_1998", stage, len(pbp))
    insert_missing_unique_from_stage(writer, stage, pbp_cols)
    record("pbp_insert_missing_weekly_rows_after_pfr", stage, len(pbp))

    pd.DataFrame(executed).to_csv(output_dir / "executed_operations.csv", index=False)


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    text_df = df.copy().astype("string").fillna("")
    headers = list(text_df.columns)
    values = text_df.values.tolist()
    widths = [max(len(str(header)), *(len(str(row[i])) for row in values)) for i, header in enumerate(headers)]

    def fmt(row: Iterable[Any]) -> str:
        return "| " + " | ".join(str(value).ljust(widths[i]) for i, value in enumerate(row)) + " |"

    return "\n".join(
        [
            fmt(headers),
            "| " + " | ".join("-" * width for width in widths) + " |",
            *[fmt(row) for row in values],
        ]
    )


def write_report(
    output_dir: Path,
    *,
    args: argparse.Namespace,
    summary: pd.DataFrame,
    overlaps: pd.DataFrame,
    columns: pd.DataFrame,
    schema_additions: pd.DataFrame,
    aliases: pd.DataFrame,
    unresolved_missing_columns: pd.DataFrame,
    duplicate_rows: pd.DataFrame,
    live_counts: pd.DataFrame,
    pbp_missing_after_pfr: pd.DataFrame,
    gates: dict[str, Any],
    headshots: pd.DataFrame,
    bio: pd.DataFrame,
    executed: bool,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_dir / "raw_atom_operation_summary.csv", index=False)
    overlaps.to_csv(output_dir / "raw_atom_overlap_summary.csv", index=False)
    columns.to_csv(output_dir / "raw_atom_column_plan.csv", index=False)
    schema_additions.to_csv(output_dir / "raw_atom_schema_additions.csv", index=False)
    aliases.to_csv(output_dir / "raw_atom_column_alias_plan.csv", index=False)
    unresolved_missing_columns.to_csv(output_dir / "raw_atom_unresolved_missing_columns.csv", index=False)
    duplicate_rows.to_csv(output_dir / "raw_atom_duplicate_player_week_rows.csv", index=False)
    live_counts.to_csv(output_dir / "raw_atom_live_key_counts.csv", index=False)
    pbp_missing_after_pfr.to_csv(output_dir / "pbp_missing_after_pfr_inserts.csv", index=False)
    if not pbp_missing_after_pfr.empty:
        (
            pbp_missing_after_pfr.groupby(["NFL_player_id", "player", "position"], dropna=False)
            .size()
            .reset_index(name="missing_rows")
            .sort_values("missing_rows", ascending=False)
            .to_csv(output_dir / "pbp_missing_after_pfr_inserts_top_players.csv", index=False)
        )
    headshots.to_csv(output_dir / "raw_atom_current_headshots.csv", index=False)
    bio.to_csv(output_dir / "raw_atom_current_bio_pfr_ids.csv", index=False)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "executed": executed,
        "pfr_package_dir": str(args.pfr_package_dir),
        "holdout_package_dir": str(args.holdout_package_dir),
        "pbp_rollup_dir": str(args.pbp_rollup_dir),
        "output_dir": str(output_dir),
        "gates": gates,
        "schema_additions": schema_additions.to_dict("records"),
        "column_aliases": aliases.to_dict("records"),
        "unresolved_missing_columns": unresolved_missing_columns.to_dict("records"),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    md = [
        "# Supertable Raw Atom Upsert Dry Run",
        "",
        f"Created: {manifest['created_at_utc']}",
        f"Executed live writes: `{executed}`",
        "",
        "## Inputs",
        "",
        f"- PFR package: `{args.pfr_package_dir}`",
        f"- Holdout package: `{args.holdout_package_dir}`",
        f"- PBP rollup: `{args.pbp_rollup_dir}`",
        "",
        "## Operation Summary",
        "",
        markdown_table(summary),
        "",
        "## Key Overlaps",
        "",
        markdown_table(overlaps),
        "",
        "## Schema Additions",
        "",
        markdown_table(schema_additions),
        "",
        "## Column Aliases",
        "",
        markdown_table(aliases),
        "",
        "## Unresolved Missing Columns",
        "",
        markdown_table(unresolved_missing_columns),
        "",
        "## Duplicate Player Week Rows",
        "",
        markdown_table(duplicate_rows.head(25)),
        "",
        "## Live Read Gates",
        "",
        markdown_table(pd.DataFrame([gates])),
        "",
        "## Live Key Counts",
        "",
        markdown_table(live_counts),
        "",
        "## PBP Missing After PFR Inserts",
        "",
        markdown_table(pbp_missing_after_pfr.head(25)),
        "",
        "## Current Headshots",
        "",
        markdown_table(headshots),
        "",
        "## Current Bio PFR IDs",
        "",
        markdown_table(bio),
        "",
        "## Notes",
        "",
        "- Default/dry-run mode performs no Fly writes.",
        "- Live writes require `--execute --confirm APPLY_RAW_ATOMS`.",
        "- The live write path inserts PBP-only rows only when their `player_week` is unique inside the PBP stage and still absent after the PFR insert step.",
        "- Derived fantasy points, PPG, ranks, season aggregates, career aggregates, and LAMAR are intentionally out of scope for this raw-atom runner.",
    ]
    (output_dir / "RAW_ATOM_UPSERT_DRY_RUN.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pfr-package-dir", type=Path, default=default_pfr_package_dir())
    parser.add_argument("--holdout-package-dir", type=Path, default=default_holdout_package_dir())
    parser.add_argument("--pbp-rollup-dir", type=Path, default=default_pbp_rollup_dir())
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--live-key-chunk-size", type=int, default=5000)
    parser.add_argument("--batch-rows", type=int, default=1000)
    parser.add_argument("--skip-live-read", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--allow-schema-additions", action="store_true")
    parser.add_argument("--confirm", default="")
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()
    output_dir = args.output_dir or (CATALOG_ROOT / f"supertable_raw_atom_upsert_dry_run_{now_stamp()}")
    output_dir.mkdir(parents=True, exist_ok=True)

    frames = load_package_frames(args.pfr_package_dir, args.holdout_package_dir, args.pbp_rollup_dir)
    reader = None if args.skip_live_read else fly_reader_or_none()
    schema_types = schema_types_from_live(reader)
    bio_schema_types = bio_schema_types_from_live(reader)
    aliases = apply_live_column_aliases(frames, schema_types)
    schema_additions = schema_addition_plan(frames, schema_types)
    columns = stat_column_plan(frames, schema_types)
    unresolved_missing_columns = unresolved_missing_column_plan(columns, schema_additions, schema_types)
    summary = operation_summary(frames, columns)
    overlaps = overlap_summary(frames)
    duplicates = duplicate_player_week_rows(frames)
    live_counts, gates, headshots, bio, pbp_missing_after_pfr = live_key_counts(
        reader, frames, chunk_size=args.live_key_chunk_size
    )

    executed = False
    if args.execute:
        if args.confirm != CONFIRM_TOKEN:
            raise SystemExit(f"Live write refused: pass --confirm {CONFIRM_TOKEN}")
        if not schema_types:
            raise SystemExit("Live write refused: live supertable schema could not be read")
        if not schema_additions.empty and not args.allow_schema_additions:
            raise SystemExit("Live write refused: pass --allow-schema-additions for reviewed return-long columns")
        if not unresolved_missing_columns.empty:
            raise SystemExit(f"Live write refused: unresolved package columns remain in {output_dir}")
        assert_execute_gates(frames, live_counts, gates)
        stage_prefix = f"nfl_historical._raw_atom_stage_{now_stamp().lower()}"
        execute_packages(
            frames,
            schema_types,
            bio_schema_types,
            schema_additions,
            stage_prefix=stage_prefix,
            batch_rows=args.batch_rows,
            output_dir=output_dir,
        )
        executed = True
        if reader is None:
            reader = fly_reader_or_none()
        if reader is not None:
            schema_types = schema_types_from_live(reader)
            aliases = apply_live_column_aliases(frames, schema_types)
            schema_additions = schema_addition_plan(frames, schema_types)
            columns = stat_column_plan(frames, schema_types)
            unresolved_missing_columns = unresolved_missing_column_plan(columns, schema_additions, schema_types)
            summary = operation_summary(frames, columns)
            live_counts, gates, headshots, bio, pbp_missing_after_pfr = live_key_counts(
                reader, frames, chunk_size=args.live_key_chunk_size
            )

    write_report(
        output_dir,
        args=args,
        summary=summary,
        overlaps=overlaps,
        columns=columns,
        schema_additions=schema_additions,
        aliases=aliases,
        unresolved_missing_columns=unresolved_missing_columns,
        duplicate_rows=duplicates,
        live_counts=live_counts,
        pbp_missing_after_pfr=pbp_missing_after_pfr,
        gates=gates,
        headshots=headshots,
        bio=bio,
        executed=executed,
    )

    print(f"output_dir={output_dir}")
    print(f"executed={executed}")
    print(summary.to_string(index=False))
    if gates:
        print(json.dumps(gates, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
