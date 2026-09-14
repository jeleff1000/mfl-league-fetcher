#!/usr/bin/env python3
"""Stage full-width live rows for confirmed stat-family identity splits.

This is deliberately non-mutating. It fetches only the affected live supertable
rows, applies the build-time split guard locally, recomputes point columns for
the staged rows, and writes comparison artifacts.
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
OUT_DIR = AUDIT_ROOT / "stat_family_split_live_stage_20260509"
SUPER_TABLE = "nfl_historical.nfl_player_stats_all"

AFFECTED_PLAYERS = ("Mark Carrier", "Kevin Williams")
AFFECTED_IDS = ("00-0002687", "00-0002688", "WillKe00", "00-0017861")
SPLIT_SOURCE_SPECS = [
    {"player": "Mark Carrier", "NFL_player_id": "00-0002688", "min_year": 1987, "max_year": 1998},
    {"player": "Kevin Williams", "NFL_player_id": "00-0017861", "min_year": 1993, "max_year": 1998},
]
SUMMARY_STATS = [
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "special_teams_tds",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_sacks",
    "def_fumbles_forced",
    "fum_rec",
    "fum_rec_yds",
    "fumble_recovery_yards",
    "fpts_4pt_half",
    "fpts_4pt_ppr",
    "fpts_4pt_half_ret",
    "fpts_4pt_ppr_ret",
    "pts_idp_std",
    "pts_idp_tackle_heavy",
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


def fetch_live_affected_rows(reader: Any, columns: list[str]) -> pd.DataFrame:
    select_cols = ", ".join(q_ident(col) for col in columns)
    sql = f"""
        SELECT {select_cols}
        FROM {SUPER_TABLE}
        WHERE player IN {sql_in(AFFECTED_PLAYERS)}
           OR NFL_player_id IN {sql_in(AFFECTED_IDS)}
        ORDER BY player, NFL_player_id, year, week, player_week
    """
    return reader.query_df(sql, database="___ops")


def affected_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in SUMMARY_STATS:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    for col in ["player", "NFL_player_id", "position", "nfl_team", "year"]:
        if col not in out.columns:
            out[col] = pd.NA
    return (
        out.groupby(["player", "NFL_player_id", "position", "nfl_team", "year"], dropna=False)
        .agg(row_count=("player_week", "size"), **{col: (col, "sum") for col in SUMMARY_STATS})
        .reset_index()
        .sort_values(["player", "year", "NFL_player_id", "nfl_team"], kind="stable")
    )


def recompute_points_without_ranks(df: pd.DataFrame) -> pd.DataFrame:
    from multi_league.data_fetchers.fantasy_points_calculator import (
        calculate_all_fantasy_points,
        calculate_composite_fantasy_points,
    )

    out = calculate_all_fantasy_points(df)
    out = calculate_composite_fantasy_points(out)
    return out


def build_rank_invalidation(staged: pd.DataFrame) -> pd.DataFrame:
    rank_cols = [col for col in staged.columns if col.startswith("rank_")]
    rows = []
    staged_years = sorted(pd.to_numeric(staged.get("year"), errors="coerce").dropna().astype(int).unique())
    positions = sorted(
        {
            str(value)
            for value in staged.get("nfl_position", staged.get("position", pd.Series(dtype=object))).dropna().unique()
            if str(value)
        }
    )
    for year in staged_years:
        year_rows = staged[pd.to_numeric(staged["year"], errors="coerce").eq(year)]
        weeks = sorted(pd.to_numeric(year_rows["week"], errors="coerce").dropna().astype(int).unique())
        rows.append(
            {
                "scope": "weekly",
                "year": year,
                "weeks": ",".join(str(week) for week in weeks),
                "positions": ",".join(positions),
                "rank_columns": ",".join(
                    col
                    for col in rank_cols
                    if not col.startswith("rank_season_") and not col.startswith("rank_alltime_")
                ),
                "reason": "weekly rank columns depend on all players in the same year/week/position or flex pool",
            }
        )
        rows.append(
            {
                "scope": "season",
                "year": year,
                "weeks": "",
                "positions": ",".join(positions),
                "rank_columns": ",".join(col for col in rank_cols if col.startswith("rank_season_")),
                "reason": "season rank columns depend on full season totals for all players in the same year/position or flex pool",
            }
        )
    rows.append(
        {
            "scope": "alltime",
            "year": "",
            "weeks": "",
            "positions": ",".join(positions),
            "rank_columns": ",".join(col for col in rank_cols if col.startswith("rank_alltime_")),
            "reason": "all-time rank columns depend on career totals across the full supertable",
        }
    )
    return pd.DataFrame(rows)


def source_split_candidate_mask(df: pd.DataFrame) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    years = pd.to_numeric(df.get("year"), errors="coerce")
    players = df.get("player", pd.Series("", index=df.index)).astype(str)
    ids = df.get("NFL_player_id", pd.Series("", index=df.index)).astype(str)
    for spec in SPLIT_SOURCE_SPECS:
        mask |= (
            players.eq(spec["player"])
            & ids.eq(spec["NFL_player_id"])
            & years.between(spec["min_year"], spec["max_year"])
        )
    return mask


def changed_source_player_weeks(live: pd.DataFrame, staged: pd.DataFrame) -> pd.DataFrame:
    """Find original live rows that are replaced by the split staging pass."""

    candidates = live[source_split_candidate_mask(live)].copy()
    if candidates.empty:
        return pd.DataFrame(columns=["player_week", "change_reason"])

    compare_cols = [
        "player",
        "NFL_player_id",
        "position",
        "nfl_position",
        "fantasy_position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        *SUMMARY_STATS,
    ]
    compare_cols = [col for col in compare_cols if col in live.columns and col in staged.columns]
    staged_by_key = staged.drop_duplicates("player_week", keep="first").set_index("player_week")
    rows = []
    for _, row in candidates.iterrows():
        player_week = str(row["player_week"])
        if player_week not in staged_by_key.index:
            rows.append({"player_week": player_week, "change_reason": "source_row_split_without_same_key"})
            continue
        staged_row = staged_by_key.loc[player_week]
        changed = False
        for col in compare_cols:
            left = row.get(col)
            right = staged_row.get(col)
            if pd.isna(left) and pd.isna(right):
                continue
            if str(left) != str(right):
                changed = True
                break
        if changed:
            rows.append({"player_week": player_week, "change_reason": "source_row_identity_or_stat_family_changed"})
    return pd.DataFrame(rows)


def write_markdown(manifest: dict[str, Any], id_summary: pd.DataFrame, rank_scope: pd.DataFrame) -> None:
    lines = [
        "# Stat-Family Split Live Staging",
        "",
        "This is a non-mutating staging artifact. It fetched only affected live rows from Fly with explicit column names.",
        "",
        f"Generated at: {manifest['generated_at_utc']}",
        "",
        "## Counts",
        "",
        f"- Live affected rows: {manifest['live_rows']:,}",
        f"- Staged affected rows: {manifest['staged_rows']:,}",
        f"- Row delta: {manifest['row_delta']:+,}",
        f"- Duplicate staged player_week keys: {manifest['duplicate_staged_player_week']:,}",
        f"- Live columns fetched: {manifest['live_columns']:,}",
        f"- Live player_week rows to replace: {manifest['live_player_weeks_to_replace']:,}",
        f"- New staged player_week rows to insert: {manifest['staged_player_weeks_to_insert']:,}",
        f"- Staged rows in scoped promotion set: {manifest['staged_rows_to_promote']:,}",
        "",
        "## ID Summary",
        "",
        id_summary.to_csv(index=False).strip(),
        "",
        "## Rank Recompute",
        "",
        "The staged rows have `pts_*` and `fpts_*` recomputed, but rank columns are not production-ready from a scoped slice.",
        "Promotion requires a full rank recompute for the scopes below.",
        "",
        rank_scope.to_csv(index=False).strip(),
        "",
        "## Files",
        "",
        f"- Live rows: `{manifest['live_rows_path']}`",
        f"- Staged rows: `{manifest['staged_rows_path']}`",
        f"- Scoped promotion rows: `{manifest['staged_rows_to_promote_path']}`",
        f"- Before summary: `{manifest['before_summary_path']}`",
        f"- After summary: `{manifest['after_summary_path']}`",
    ]
    (OUT_DIR / "STAT_FAMILY_SPLIT_LIVE_STAGE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    load_env()

    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.data_fetchers.player_identity_guards import apply_known_stat_family_splits

    reader = FlyReader()
    schema = fetch_schema(reader)
    schema.to_csv(OUT_DIR / "live_schema_columns.csv", index=False)
    columns = schema["column_name"].astype(str).tolist()
    live = fetch_live_affected_rows(reader, columns)

    live.to_parquet(OUT_DIR / "live_affected_rows_full.parquet", index=False)
    before_summary = affected_summary(live)
    before_summary.to_csv(OUT_DIR / "live_affected_summary.csv", index=False)

    staged = apply_known_stat_family_splits(live, log_fn=print)
    staged = recompute_points_without_ranks(staged)
    staged.to_parquet(OUT_DIR / "staged_affected_rows_full_fpts_recomputed.parquet", index=False)
    after_summary = affected_summary(staged)
    after_summary.to_csv(OUT_DIR / "staged_affected_summary.csv", index=False)

    before_ids = live.groupby(["player", "NFL_player_id"], dropna=False).size().reset_index(name="rows_live")
    after_ids = staged.groupby(["player", "NFL_player_id"], dropna=False).size().reset_index(name="rows_staged")
    id_summary = before_ids.merge(after_ids, on=["player", "NFL_player_id"], how="outer").fillna(0)
    id_summary.to_csv(OUT_DIR / "staged_id_summary.csv", index=False)

    live_pws = set(live["player_week"].dropna().astype(str))
    staged_pws = set(staged["player_week"].dropna().astype(str))
    replace_pws = changed_source_player_weeks(live, staged)
    replace_set = set(replace_pws["player_week"].astype(str)) if not replace_pws.empty else set()
    added_rows = staged[staged["player_week"].astype(str).isin(staged_pws - live_pws)].copy()
    promotion_rows = staged[
        staged["player_week"].astype(str).isin(replace_set | set(added_rows["player_week"].astype(str)))
    ]

    replace_pws.to_csv(OUT_DIR / "live_player_weeks_to_replace.csv", index=False)
    added_rows[["player_week", "player", "NFL_player_id", "position", "nfl_team", "year", "week"]].to_csv(
        OUT_DIR / "staged_player_weeks_to_insert.csv",
        index=False,
    )
    promotion_rows.to_parquet(OUT_DIR / "staged_rows_to_promote_full.parquet", index=False)

    rank_scope = build_rank_invalidation(promotion_rows)
    rank_scope.to_csv(OUT_DIR / "rank_recompute_scope.csv", index=False)

    manifest = {
        "generated_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "live_rows": int(len(live)),
        "staged_rows": int(len(staged)),
        "row_delta": int(len(staged) - len(live)),
        "duplicate_staged_player_week": int(staged["player_week"].duplicated().sum()),
        "live_columns": int(len(columns)),
        "live_player_weeks_to_replace": int(len(replace_pws)),
        "staged_player_weeks_to_insert": int(len(added_rows)),
        "staged_rows_to_promote": int(len(promotion_rows)),
        "live_rows_path": str(OUT_DIR / "live_affected_rows_full.parquet"),
        "staged_rows_path": str(OUT_DIR / "staged_affected_rows_full_fpts_recomputed.parquet"),
        "staged_rows_to_promote_path": str(OUT_DIR / "staged_rows_to_promote_full.parquet"),
        "before_summary_path": str(OUT_DIR / "live_affected_summary.csv"),
        "after_summary_path": str(OUT_DIR / "staged_affected_summary.csv"),
        "id_summary_path": str(OUT_DIR / "staged_id_summary.csv"),
        "rank_recompute_scope_path": str(OUT_DIR / "rank_recompute_scope.csv"),
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_markdown(manifest, id_summary, rank_scope)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
