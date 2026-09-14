#!/usr/bin/env python3
"""Stage confirmed special-teams identity repairs.

Non-mutating.  Current scoped repair:
- Steve Jordan TE/K same-name collision in 1987.

Mike Michel and Dave Green are intentionally emitted as dual P/K alias
decisions, not promoted here, because they are the same person split across
P/K source IDs and need a broader alias policy before live merge.
"""

from __future__ import annotations

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

ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_ROOT = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
OUT_DIR = AUDIT_ROOT / "special_teams_identity_stage_20260509"
SUPER_TABLE = "nfl_historical.nfl_player_stats_all"

AFFECTED_PLAYERS = ("Steve Jordan", "Mike Michel", "Dave Green")
AFFECTED_IDS = (
    "00-0008927",
    "JOR577792",
    "MIC488570",
    "MichMi21",
    "GRE162282",
    "GreeDa20",
)

STEVE_TE_ID = "00-0008927"
STEVE_K_ID = "JOR577792"
STEVE_K_WEEKS = {4, 5, 6}
STEVE_TE_KICKING_LEAK_WEEKS = {1, 2}

KICKING_COLUMNS = [
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
    "fg_made_60_",
    "fg_made_60_plus",
    "fg_made_60_plus_canonical",
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
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
    "pat_pct",
]

RAW_STAT_PREFIXES = (
    "passing_",
    "rushing_",
    "receiving_",
    "kickoff_",
    "punt_",
    "def_",
    "fumble_",
    "fg_",
    "pat_",
    "gwfg_",
    "pts_",
    "fpts_",
)
RAW_STAT_NAMES = {
    "attempts",
    "completions",
    "carries",
    "targets",
    "receptions",
    "fumbles",
    "fumbles_lost",
    "fum_rec",
    "fum_rec_yds",
    "fum_ret_td",
    "punts",
    "punt_yards",
    "punt_long",
    "punts_blocked",
    "special_teams_tds",
    "special_teams_tackles_solo",
}


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


def q_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: tuple[str, ...]) -> str:
    return "(" + ",".join(q_lit(value) for value in values) + ")"


def fetch_schema(reader: Any) -> pd.DataFrame:
    return reader.query_df(
        """
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        ORDER BY ordinal_position
        """,
        database="___ops",
    )


def fetch_live_rows(reader: Any, columns: list[str]) -> pd.DataFrame:
    select_cols = ", ".join(q_ident(col) for col in columns)
    return reader.query_df(
        f"""
        SELECT {select_cols}
        FROM {SUPER_TABLE}
        WHERE player IN {sql_in(AFFECTED_PLAYERS)}
           OR NFL_player_id IN {sql_in(AFFECTED_IDS)}
           OR player_week LIKE 'JOR577792_%'
           OR player_week LIKE 'MIC488570_%'
           OR player_week LIKE 'GRE162282_%'
        ORDER BY player, NFL_player_id, year, week, player_week
        """,
        database="___ops",
    )


def load_pbp_targets() -> pd.DataFrame:
    cols = [
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
        *KICKING_COLUMNS,
    ]
    try:
        import pyarrow.parquet as pq

        available = set(pq.read_schema(ROLLUP_DIR / "pbp_player_week_rollup.parquet").names)
    except Exception:
        available = set(pd.read_parquet(ROLLUP_DIR / "pbp_player_week_rollup.parquet").columns)
    pbp = pd.read_parquet(
        ROLLUP_DIR / "pbp_player_week_rollup.parquet",
        columns=[col for col in cols if col in available],
    )
    return pbp[pbp["NFL_player_id"].astype(str).isin(AFFECTED_IDS)].copy()


def recompute_points_without_ranks(df: pd.DataFrame) -> pd.DataFrame:
    from multi_league.data_fetchers.fantasy_points_calculator import (
        calculate_all_fantasy_points,
        calculate_composite_fantasy_points,
    )

    out = calculate_all_fantasy_points(df)
    out = calculate_composite_fantasy_points(out)
    return out


def stat_like_columns(columns: list[str]) -> list[str]:
    out: list[str] = []
    for col in columns:
        if col.startswith("rank_"):
            continue
        if (
            "recomputed_at" in col
            or "_repaired_at" in col
            or "_populated_at" in col
            or "_merged_at" in col
            or col.endswith("_checksum")
        ):
            continue
        if col in RAW_STAT_NAMES or col.startswith(RAW_STAT_PREFIXES):
            out.append(col)
    return out


def zero_columns(row: pd.Series, columns: list[str]) -> pd.Series:
    for col in columns:
        if col in row.index:
            row[col] = 0.0
    return row


def set_identity(row: pd.Series, pbp_row: pd.Series, player_id: str) -> pd.Series:
    row["NFL_player_id"] = player_id
    row["player_week"] = f"{player_id}_{int(pbp_row['year'])}_{int(pbp_row['week'])}"
    row["player"] = pbp_row.get("player", row.get("player"))
    for col in ("position", "nfl_position", "fantasy_position"):
        if col in row.index:
            row[col] = pbp_row.get("position", row.get(col))
    for col in ("nfl_team", "opponent_nfl_team", "season_type", "year", "week"):
        if col in row.index and col in pbp_row.index:
            row[col] = pbp_row[col]
    return row


def set_kicking_from_pbp(row: pd.Series, pbp_row: pd.Series) -> pd.Series:
    for col in KICKING_COLUMNS:
        if col in row.index:
            row[col] = pd.to_numeric(pd.Series([pbp_row.get(col)]), errors="coerce").fillna(0.0).iloc[0]
    return row


def stage_steve_jordan(live: pd.DataFrame, pbp: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    staged = live.copy()
    changed_keys: set[str] = set()
    new_rows: list[pd.Series] = []

    year = pd.to_numeric(staged.get("year"), errors="coerce")
    week = pd.to_numeric(staged.get("week"), errors="coerce")
    player = staged.get("player", pd.Series("", index=staged.index)).astype(str)
    position = staged.get("position", pd.Series("", index=staged.index)).astype(str)
    nfl_id = staged.get("NFL_player_id", pd.Series("", index=staged.index)).astype(str)
    player_week = staged.get("player_week", pd.Series("", index=staged.index)).astype(str)

    leak_mask = (
        player.eq("Steve Jordan")
        & nfl_id.eq(STEVE_TE_ID)
        & position.eq("TE")
        & year.eq(1987)
        & week.isin(STEVE_TE_KICKING_LEAK_WEEKS)
    )
    staged.loc[leak_mask, [col for col in KICKING_COLUMNS if col in staged.columns]] = 0.0
    changed_keys.update(staged.loc[leak_mask, "player_week"].astype(str).tolist())

    pbp_steve_k = pbp[
        pbp["NFL_player_id"].astype(str).eq(STEVE_K_ID)
        & pd.to_numeric(pbp["year"], errors="coerce").eq(1987)
        & pd.to_numeric(pbp["week"], errors="coerce").isin(list(STEVE_K_WEEKS))
    ].copy()
    pbp_by_week = {int(row["week"]): row for _, row in pbp_steve_k.iterrows()}

    k_mask = (
        player.eq("Steve Jordan")
        & year.eq(1987)
        & (
            position.eq("K")
            | player_week.str.startswith(f"{STEVE_K_ID}_")
            | (nfl_id.eq(STEVE_TE_ID) & week.isin(list(STEVE_K_WEEKS)))
        )
    )
    for idx in staged.index[k_mask]:
        wk = int(pd.to_numeric(pd.Series([staged.at[idx, "week"]]), errors="coerce").iloc[0])
        pbp_row = pbp_by_week.get(wk)
        if pbp_row is None:
            continue
        row = staged.loc[idx].copy()
        row = set_identity(row, pbp_row, STEVE_K_ID)
        row = zero_columns(row, [col for col in stat_like_columns(columns) if col not in KICKING_COLUMNS])
        row = set_kicking_from_pbp(row, pbp_row)
        staged.loc[idx, row.index] = row
        changed_keys.add(str(row["player_week"]))

    existing_keys = set(staged.get("player_week", pd.Series(dtype=str)).dropna().astype(str))
    template_rows = staged[k_mask].copy()
    if template_rows.empty:
        template_rows = staged[leak_mask].copy()
    template = template_rows.iloc[0].copy() if not template_rows.empty else None
    for wk, pbp_row in pbp_by_week.items():
        key = f"{STEVE_K_ID}_1987_{wk}"
        if key in existing_keys or template is None:
            continue
        row = template.copy()
        row = set_identity(row, pbp_row, STEVE_K_ID)
        row = zero_columns(row, stat_like_columns(columns))
        row = set_kicking_from_pbp(row, pbp_row)
        new_rows.append(row)
        changed_keys.add(key)

    if new_rows:
        staged = pd.concat([staged, pd.DataFrame(new_rows)], ignore_index=True, sort=False)

    changed = staged[staged["player_week"].astype(str).isin(changed_keys)].copy()
    return staged, changed


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "fg_att",
        "fg_made",
        "fg_missed",
        "fg_yards",
        "fg_long",
        "pat_att",
        "pat_made",
        "punts",
        "punt_yards",
        "punt_long",
        "fpts_4pt_half",
        "fpts_4pt_ppr",
    ]
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return (
        out.groupby(["player", "NFL_player_id", "position", "nfl_team", "year"], dropna=False)
        .agg(row_count=("player_week", "size"), **{col: (col, "sum") for col in cols})
        .reset_index()
        .sort_values(["player", "year", "NFL_player_id", "nfl_team"], kind="stable")
    )


def write_dual_alias_decisions(pbp: pd.DataFrame) -> pd.DataFrame:
    rows = [
        {
            "player": "Mike Michel",
            "ids": "MIC488570;MichMi21",
            "decision": "dual_p_k_same_person_alias_not_same_name_corruption",
            "recommended_next": "defer live merge until P/K alias policy decides whether fantasy K ranks should use K row or unified P/K row",
        },
        {
            "player": "Dave Green",
            "ids": "GRE162282;GreeDa20",
            "decision": "dual_p_k_same_person_alias_not_same_name_corruption",
            "recommended_next": "defer live merge until P/K alias policy decides whether fantasy K ranks should use K row or unified P/K row",
        },
    ]
    out = pd.DataFrame(rows)
    out.to_csv(OUT_DIR / "dual_pk_alias_decisions.csv", index=False)
    return out


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    load_env()

    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()
    schema = fetch_schema(reader)
    columns = schema["column_name"].astype(str).tolist()
    schema.to_csv(OUT_DIR / "live_schema_columns.csv", index=False)

    live = fetch_live_rows(reader, columns)
    live.to_parquet(OUT_DIR / "live_affected_rows_full.parquet", index=False)
    summarize(live).to_csv(OUT_DIR / "live_affected_summary.csv", index=False)

    pbp = load_pbp_targets()
    pbp.to_csv(OUT_DIR / "pbp_affected_targets.csv", index=False)

    staged, changed = stage_steve_jordan(live, pbp, columns)
    staged = recompute_points_without_ranks(staged)
    changed = staged[staged["player_week"].astype(str).isin(changed["player_week"].astype(str))]

    staged.to_parquet(OUT_DIR / "staged_affected_rows_full_fpts_recomputed.parquet", index=False)
    changed.to_parquet(OUT_DIR / "staged_rows_to_promote_full.parquet", index=False)
    summarize(staged).to_csv(OUT_DIR / "staged_affected_summary.csv", index=False)
    summarize(changed).to_csv(OUT_DIR / "staged_rows_to_promote_summary.csv", index=False)
    changed[
        ["player_week", "player", "NFL_player_id", "position", "nfl_team", "opponent_nfl_team", "year", "week"]
    ].to_csv(
        OUT_DIR / "staged_player_weeks_to_promote.csv",
        index=False,
    )
    dual_alias = write_dual_alias_decisions(pbp)

    live_keys = set(live["player_week"].dropna().astype(str))
    staged_keys = set(staged["player_week"].dropna().astype(str))
    manifest = {
        "generated_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "live_rows": int(len(live)),
        "staged_rows": int(len(staged)),
        "row_delta": int(len(staged) - len(live)),
        "duplicate_staged_player_week": int(staged["player_week"].duplicated().sum()),
        "staged_rows_to_promote": int(len(changed)),
        "new_staged_player_weeks": sorted(staged_keys - live_keys),
        "dual_alias_decisions": int(len(dual_alias)),
        "live_rows_path": str(OUT_DIR / "live_affected_rows_full.parquet"),
        "staged_rows_path": str(OUT_DIR / "staged_affected_rows_full_fpts_recomputed.parquet"),
        "staged_rows_to_promote_path": str(OUT_DIR / "staged_rows_to_promote_full.parquet"),
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    lines = [
        "# Special Teams Identity Staging",
        "",
        "This is non-mutating.",
        "",
        "## Counts",
        "",
        f"- Live affected rows: {manifest['live_rows']:,}",
        f"- Staged affected rows: {manifest['staged_rows']:,}",
        f"- Row delta: {manifest['row_delta']:+,}",
        f"- Duplicate staged player_week keys: {manifest['duplicate_staged_player_week']:,}",
        f"- Staged rows to promote: {manifest['staged_rows_to_promote']:,}",
        f"- New staged player_week keys: {', '.join(manifest['new_staged_player_weeks']) or 'none'}",
        "",
        "## Decisions",
        "",
        "- Steve Jordan TE/K is a confirmed same-name collision: TE rows keep TE receiving, TE kicking leakage is zeroed, and Colts replacement-kicker rows move to `JOR577792` using PBP kicking values.",
        "- Mike Michel and Dave Green are dual P/K aliases for the same person, not same-name corruption. They need a broader P/K alias/rank policy before live merge.",
        "",
        "## Files",
        "",
        f"- Staged rows: `{manifest['staged_rows_path']}`",
        f"- Promotion rows: `{manifest['staged_rows_to_promote_path']}`",
        f"- Dual alias decisions: `{OUT_DIR / 'dual_pk_alias_decisions.csv'}`",
        "",
    ]
    (OUT_DIR / "SPECIAL_TEAMS_IDENTITY_STAGE.md").write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
