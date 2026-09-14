"""Repair confirmed kicking stat leakage in the live Fly super table.

Scope is intentionally narrow:
- 1999 nflverse rows where a non-K/P player has kicking stats in the super
  table but the raw nflverse player-week row has no kicking stat signal.
- Gary Anderson RB (00-0000311), whose kicking stats are confirmed to belong
  to Gary Anderson K (00-0000313).

The pipeline guard lives in multi_league.data_fetchers.kicking_stat_guards;
this script only repairs already-published rows.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.kicking_stat_guards import (  # noqa: E402
    KICKING_POSITIONS,
    KICKING_ZERO_COLUMNS,
    raw_confirmed_non_kicker_kick_keys,
)

SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
FULL_SUPER_TABLE = f"___ops.{SUPER_TABLE}"
BACKUP_TABLE = "nfl_historical.kicking_corruption_repair_backup_20260508"
CATALOG_DIR = ROOT.parent / "fantasy_football_data_organized" / "_catalog" / "pbp_supertable_gap_audit_20260507"
OUT_DIR = CATALOG_DIR / "kicking_corruption_live_repair_20260508"

KICKING_SIGNAL_COLUMNS = [
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
    "fg_made_60_",
    "fg_made_60plus",
    "fg_made_60_plus_canonical",
    "fg_made_distance",
    "fg_missed_distance",
    "fg_blocked_distance",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
]

EXTRA_KICKING_ZERO_COLUMNS = [
    "fg_made_60_",
    "fg_made_60_plus",
    "fg_yards_over_30_canonical",
    "fg_yds_over_30_canonical",
    "fg_made_distance",
    "fg_missed_distance",
    "fg_blocked_distance",
    "fg_missed_0_19",
    "fg_missed_20_29",
    "fg_missed_30_39",
    "fg_missed_40_49",
    "fg_missed_50_59",
    "fg_missed_60_",
    "gwfg_att",
    "gwfg_made",
    "gwfg_missed",
    "gwfg_blocked",
    "gwfg_distance",
    "pat_pct",
]

FPTS_FORMULAS = {
    "fpts_4pt_0ppr": "COALESCE(pts_pass_4pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_0ppr,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_4pt_half": "COALESCE(pts_pass_4pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_half,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_4pt_ppr": "COALESCE(pts_pass_4pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_ppr,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_5pt_0ppr": "COALESCE(pts_pass_5pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_0ppr,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_5pt_half": "COALESCE(pts_pass_5pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_half,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_5pt_ppr": "COALESCE(pts_pass_5pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_ppr,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_6pt_0ppr": "COALESCE(pts_pass_6pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_0ppr,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_6pt_half": "COALESCE(pts_pass_6pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_half,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_6pt_ppr": "COALESCE(pts_pass_6pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_ppr,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_4pt_tep": "COALESCE(pts_pass_4pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_tep,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_5pt_tep": "COALESCE(pts_pass_5pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_tep,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
    "fpts_6pt_tep": "COALESCE(pts_pass_6pt,0)+COALESCE(pts_rush,0)+COALESCE(pts_rec_tep,0)+COALESCE(pts_misc,0)+COALESCE(pts_def_std,0)",
}


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_in(values: list[str]) -> str:
    if not values:
        return "('')"
    return "(" + ",".join(sql_string(value) for value in values) + ")"


def position_tokens(value: Any) -> set[str]:
    if pd.isna(value):
        return set()
    return {token for token in re.split(r"[^A-Za-z0-9]+", str(value).upper()) if token}


def is_non_kicker_position(value: Any) -> bool:
    tokens = position_tokens(value)
    return bool(tokens) and not (tokens & KICKING_POSITIONS) and "DEF" not in tokens


def query_df(writer: Any, sql: str) -> pd.DataFrame:
    rows = writer.execute(sql, database="___ops")
    return pd.DataFrame(rows)


def get_schema(writer: Any) -> pd.DataFrame:
    return query_df(
        writer,
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        ORDER BY ordinal_position
        """,
    )


def existing_cols(schema: pd.DataFrame, candidates: list[str] | tuple[str, ...]) -> list[str]:
    available = set(schema["column_name"])
    return [col for col in dict.fromkeys(candidates) if col in available]


def signal_sql(cols: list[str]) -> str:
    return " + ".join(f"ABS(COALESCE(TRY_CAST({q_ident(col)} AS DOUBLE), 0))" for col in cols) or "0"


def fetch_live_suspects(writer: Any, schema: pd.DataFrame) -> pd.DataFrame:
    select_cols = existing_cols(
        schema,
        [
            "player_week",
            "player",
            "NFL_player_id",
            "position",
            "nfl_position",
            "nfl_team",
            "year",
            "week",
            *KICKING_SIGNAL_COLUMNS,
            *KICKING_ZERO_COLUMNS,
            *EXTRA_KICKING_ZERO_COLUMNS,
        ],
    )
    signal_cols = existing_cols(schema, KICKING_SIGNAL_COLUMNS)
    where_signal = signal_sql(signal_cols)
    sql = f"""
    SELECT {", ".join(q_ident(col) for col in select_cols)}
    FROM {SUPER_TABLE}
    WHERE (
        year = 1999
        AND ({where_signal}) > 0
        AND COALESCE(position, nfl_position, '') NOT IN ('K', 'P', 'DEF')
      )
      OR (
        NFL_player_id = '00-0000311'
        AND ({where_signal}) > 0
      )
    ORDER BY year, week, player, NFL_player_id
    """
    return query_df(writer, sql)


def fetch_raw_1999_allow_keys() -> set[str]:
    from multi_league.data_fetchers.nfl_offense_stats import fetch_nflverse_player_stats

    raw = fetch_nflverse_player_stats(1999, use_cache=True)
    return raw_confirmed_non_kicker_kick_keys(raw, min_year=1999)


def classify_targets(suspects: pd.DataFrame, raw_allow_keys: set[str]) -> pd.DataFrame:
    if suspects.empty:
        return suspects.assign(repair_reason=pd.Series(dtype="string"))

    out = suspects.copy()
    out["player_week"] = out["player_week"].astype(str)
    out["year_int"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["is_gary_rb_collision"] = out["NFL_player_id"].eq("00-0000311")
    out["is_1999_non_kicker_raw_zero"] = (
        out["year_int"].eq(1999)
        & out["player_week"].map(lambda key: key not in raw_allow_keys)
        & out["position"].map(is_non_kicker_position)
    )
    out["repair_reason"] = ""
    out.loc[out["is_gary_rb_collision"], "repair_reason"] = "gary_anderson_rb_k_collision"
    out.loc[out["is_1999_non_kicker_raw_zero"], "repair_reason"] = "1999_raw_nflverse_zero_non_kicker_leak"
    return out[out["repair_reason"].ne("")].copy()


def write_artifacts(suspects: pd.DataFrame, targets: pd.DataFrame, dry_run: bool) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    suspects.to_csv(OUT_DIR / f"live_kicking_suspects_{stamp}.csv", index=False)
    targets.to_csv(OUT_DIR / f"live_kicking_repair_targets_{stamp}.csv", index=False)
    manifest = {
        "created_at_utc": stamp,
        "dry_run": dry_run,
        "suspect_rows": int(len(suspects)),
        "target_rows": int(len(targets)),
        "reasons": targets["repair_reason"].value_counts().to_dict() if not targets.empty else {},
        "preserved_rows": int(len(suspects) - len(targets)),
    }
    (OUT_DIR / f"manifest_{stamp}.json").write_text(json.dumps(manifest, indent=2))


def create_backup_table(writer: Any, target_keys: list[str]) -> None:
    writer.execute(f"DROP TABLE IF EXISTS {BACKUP_TABLE}", database="___ops")
    writer.execute(
        f"""
        CREATE TABLE {BACKUP_TABLE} AS
        SELECT *
        FROM {SUPER_TABLE}
        WHERE player_week IN {sql_in(target_keys)}
        """,
        database="___ops",
    )


def apply_zero_updates(writer: Any, schema: pd.DataFrame, target_keys: list[str]) -> None:
    type_by_col = dict(zip(schema["column_name"], schema["data_type"]))
    zero_cols = existing_cols(
        schema,
        [
            *KICKING_ZERO_COLUMNS,
            *EXTRA_KICKING_ZERO_COLUMNS,
            "fg%",
            "xp%",
            "fgm",
            "xpm",
            "xpa",
            "fga",
        ],
    )
    for start in range(0, len(zero_cols), 20):
        chunk = zero_cols[start : start + 20]
        assignments = []
        for col in chunk:
            data_type = str(type_by_col.get(col, "")).upper()
            value = "NULL" if "CHAR" in data_type or "VARCHAR" in data_type else "0"
            assignments.append(f"{q_ident(col)} = {value}")
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE}
            SET {", ".join(assignments)}
            WHERE player_week IN {sql_in(target_keys)}
            """,
            database="___ops",
        )


def apply_fpts_updates(writer: Any, schema: pd.DataFrame, target_keys: list[str]) -> None:
    available = set(schema["column_name"])
    formulas = {col: expr for col, expr in FPTS_FORMULAS.items() if col in available}
    for base_col, expr in list(formulas.items()):
        if f"{base_col}_ret" in available:
            formulas[f"{base_col}_ret"] = f"({expr})+COALESCE(pts_ret_yds,0)"

    for start in range(0, len(formulas), 18):
        chunk = list(formulas.items())[start : start + 18]
        assignments = ", ".join(f"{q_ident(col)} = {expr}" for col, expr in chunk)
        writer.execute(
            f"""
            UPDATE {SUPER_TABLE}
            SET {assignments}
            WHERE player_week IN {sql_in(target_keys)}
            """,
            database="___ops",
        )


def verify(writer: Any, schema: pd.DataFrame, target_keys: list[str]) -> dict[str, Any]:
    signal_cols = existing_cols(schema, [*KICKING_SIGNAL_COLUMNS, *KICKING_ZERO_COLUMNS, *EXTRA_KICKING_ZERO_COLUMNS])
    pts_cols = existing_cols(schema, ["pts_k_std", "pts_k_yds", "pts_k_flat"])
    signal = signal_sql(signal_cols)
    pts_signal = signal_sql(pts_cols)
    rows = writer.execute(
        f"""
        SELECT
          COUNT(*) AS rows_checked,
          SUM(CASE WHEN ({signal}) > 0 THEN 1 ELSE 0 END) AS rows_with_kicking_signal,
          SUM(CASE WHEN ({pts_signal}) <> 0 THEN 1 ELSE 0 END) AS rows_with_kicker_points,
          ROUND(SUM({signal}), 4) AS remaining_kicking_signal,
          ROUND(SUM({pts_signal}), 4) AS remaining_kicker_points
        FROM {SUPER_TABLE}
        WHERE player_week IN {sql_in(target_keys)}
        """,
        database="___ops",
    )
    return rows[0] if rows else {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply", action="store_true", help="Apply the live repair. Without this, only writes artifacts."
    )
    args = parser.parse_args()

    load_env()
    from multi_league.core.fly_writer import FlyWriter

    writer = FlyWriter()
    schema = get_schema(writer)
    suspects = fetch_live_suspects(writer, schema)
    raw_allow_keys = fetch_raw_1999_allow_keys()
    targets = classify_targets(suspects, raw_allow_keys)
    write_artifacts(suspects, targets, dry_run=not args.apply)

    print(f"suspect_rows={len(suspects)}")
    print(f"target_rows={len(targets)}")
    if not targets.empty:
        print(
            targets[["player_week", "player", "NFL_player_id", "position", "year", "week", "repair_reason"]].to_string(
                index=False
            )
        )

    if not args.apply:
        print(f"dry_run=true artifacts={OUT_DIR}")
        return 0

    target_keys = sorted(targets["player_week"].astype(str).unique().tolist())
    if not target_keys:
        print("No target keys; nothing to update")
        return 0

    create_backup_table(writer, target_keys)
    apply_zero_updates(writer, schema, target_keys)
    apply_fpts_updates(writer, schema, target_keys)
    result = verify(writer, schema, target_keys)
    print("verify=" + json.dumps(result, sort_keys=True))
    if (
        float(result.get("remaining_kicking_signal") or 0) != 0
        or float(result.get("remaining_kicker_points") or 0) != 0
    ):
        raise SystemExit("Repair verification failed: target rows still have kicking signal")
    print(f"backup_table=___ops.{BACKUP_TABLE}")
    print(f"artifacts={OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
