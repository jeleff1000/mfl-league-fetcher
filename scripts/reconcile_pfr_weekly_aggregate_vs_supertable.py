#!/usr/bin/env python3
"""Aggregate PFR boxscore player-games to supertable weekly keys.

The row-level PFR audit is useful for finding missing boxscore atoms, but the
supertable is keyed by player_week. This script reconciles at that same weekly
grain so multi-game/old-era rows do not masquerade as duplicates.

Local-only. Does not write to Fly.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_RETAB_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_retabs_20260512T134853Z"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_supertable_audit_zero_bridge_20260512T154117Z"
DEFAULT_BIO = (
    ORGANIZED_ROOT
    / "_catalog"
    / "pfr_remaining_bridge_stubs_20260512T154036Z"
    / "player_bio_pfr_bridge_zero_gap.parquet"
)
DEFAULT_CLASSIFICATION = (
    ORGANIZED_ROOT
    / "_catalog"
    / "pfr_remaining_bridge_stubs_20260512T154036Z"
    / "pfr_remaining_bridge_classification.csv"
)
DEFAULT_OUTPUT_ROOT = ORGANIZED_ROOT / "_catalog"

sys.path.insert(0, str(ROOT))
from scripts.audit_pfr_boxscore_retabs_vs_fly_supertable import STAT_MAP  # noqa: E402
from scripts.stage_pfr_boxscore_missing_weekly_rows import (  # noqa: E402
    MANUAL_REVIEW_CLASSES,
    apply_confirmed_pfr_identity_overrides,
    apply_live_season_type,
    build_bio_bridge,
    build_classification,
    last_name_key,
    load_bio,
    load_live_selected_rows,
    load_live_year_week_season_types,
    markdown_table,
    normalize_name,
    normalize_pfr_id,
    stat_columns,
)


LONG_STATS = {
    "passing_long",
    "rushing_long",
    "receiving_long",
    "kickoff_return_long",
    "punt_return_long",
    "punt_long",
}


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def latest_retab_dir() -> Path:
    dirs = sorted(
        DEFAULT_OUTPUT_ROOT.glob("pfr_boxscore_retabs_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in dirs:
        if (path / "pfr_player_game_fact.parquet").exists():
            return path
    return DEFAULT_RETAB_DIR


def normalize_team_family(team: Any, year: Any) -> str:
    if team is None or pd.isna(team):
        return ""
    code = str(team).strip().upper()
    try:
        season = int(year)
    except (TypeError, ValueError):
        season = 0

    if code in {"NWE", "NE", "BOS"}:
        return "NE"
    if code in {"GNB", "GB"}:
        return "GB"
    if code in {"KAN", "KC"}:
        return "KC"
    if code in {"NOR", "NO"}:
        return "NO"
    if code in {"SFO", "SF"}:
        return "SF"
    if code in {"TAM", "TB"}:
        return "TB"
    if code in {"SDG", "LAC", "SD"}:
        return "LAC"
    if code in {"RAI", "OAK", "LVR", "LV", "LAS"}:
        return "LV"
    if code in {"WAS", "WSH", "WFT"}:
        return "WAS"
    if code in {"NYJ", "NYT", "TIT"}:
        return "NYJ"
    if code in {"NYG"}:
        return "NYG"
    if code in {"JAC", "JAX"}:
        return "JAX"

    if code in {"CRD", "PHO", "ARI"}:
        return "ARI"
    if code == "STL":
        if 1995 <= season <= 2015:
            return "LAR"
        return "ARI"
    if code in {"RAM", "LAR", "LA"}:
        return "LAR"

    if code == "HOU":
        if season and season < 2002:
            return "TEN"
        return "HOU"
    if code in {"TEN", "OIL"}:
        return "TEN"

    if code == "BAL":
        if season and season < 1984:
            return "IND"
        return "BAL"
    if code in {"CLT", "IND"}:
        return "IND"

    return code


def normalize_season_type_for_calendar(year: Any, week: Any, season_type: Any) -> str:
    """Normalize known NFL calendar exceptions before comparing row context."""
    if season_type is None or pd.isna(season_type):
        value = ""
    else:
        value = str(season_type).strip().upper()

    try:
        season = int(year)
        nfl_week = int(week)
    except (TypeError, ValueError):
        return value

    # 1982 Week 17 was regular season after the strike gap; 1993 Week 18
    # was regular season during the NFL's two-bye experiment.
    if (season, nfl_week) in {(1982, 17), (1993, 18)}:
        return "REG"
    return value


def unique_join(values: pd.Series) -> str:
    clean = sorted({str(value).strip() for value in values.dropna() if str(value).strip()})
    return ";".join(clean)


def first_nonblank(values: pd.Series) -> Any:
    for value in values:
        if value is not None and not pd.isna(value) and str(value).strip():
            return value
    return None


def fact_path_for(retab_dir: Path) -> Path:
    fact_path = retab_dir / "pfr_player_game_fact.parquet"
    if not fact_path.exists():
        raise FileNotFoundError(f"Missing PFR fact parquet: {fact_path}")
    return fact_path


def available_fact_years(retab_dir: Path) -> list[int]:
    fact_path = fact_path_for(retab_dir)
    fact_sql_path = str(fact_path).replace("\\", "/").replace("'", "''")
    df = duckdb.connect().execute(f"SELECT DISTINCT season FROM read_parquet('{fact_sql_path}') ORDER BY season").df()
    return [int(year) for year in df["season"].dropna().tolist()]


def load_fact_year(retab_dir: Path, year: int) -> pd.DataFrame:
    fact_path = fact_path_for(retab_dir)
    pfr_stat_cols = list(dict.fromkeys([pfr_col for pfr_col, _ in STAT_MAP]))
    cols = [
        "boxscore_id",
        "game_date",
        "season",
        "team",
        "pfr_id",
        "player",
        "source_tables",
        "pfr_week_num",
        "opponent",
        "missing_pfr_id",
        "kickoff_return_long",
        "punt_return_long",
        *pfr_stat_cols,
    ]
    cols = list(dict.fromkeys(cols))
    fact_sql_path = str(fact_path).replace("\\", "/").replace("'", "''")
    select_cols = ", ".join(cols)
    sql = f"""
        SELECT {select_cols}
        FROM read_parquet('{fact_sql_path}')
        WHERE season = {int(year)}
    """
    return duckdb.connect().execute(sql).df()


def build_active_bio_lookup(
    bio_all: pd.DataFrame,
) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], dict[str, Any]]]:
    cols = [
        "NFL_player_id",
        "player",
        "nfl_position",
        "status",
        "first_year",
        "last_year",
        "position_category",
        "position_side",
        "latest_team",
    ]
    bio = bio_all[[col for col in cols if col in bio_all.columns]].copy()
    bio = bio[bio["NFL_player_id"].notna() & bio["player"].notna()].copy()
    bio["name_norm_fallback"] = bio["player"].map(normalize_name)
    bio["last_name_fallback"] = bio["player"].map(last_name_key)
    bio = bio[bio["name_norm_fallback"].ne("")].copy()
    bio["first_year_num"] = pd.to_numeric(bio.get("first_year"), errors="coerce").fillna(1920).clip(1920, 2026)
    bio["last_year_num"] = pd.to_numeric(bio.get("last_year"), errors="coerce").fillna(2026).clip(1920, 2026)
    bio["first_year_num"] = bio["first_year_num"].astype(int)
    bio["last_year_num"] = bio["last_year_num"].astype(int)

    exact_ids: defaultdict[tuple[str, int], set[str]] = defaultdict(set)
    last_ids: defaultdict[tuple[str, int], set[str]] = defaultdict(set)
    exact_records: dict[tuple[str, int], dict[str, Any]] = {}
    last_records: dict[tuple[str, int], dict[str, Any]] = {}

    for row in bio.to_dict("records"):
        first_year = int(row["first_year_num"])
        last_year = int(row["last_year_num"])
        if last_year < first_year:
            last_year = first_year
        record = {
            "NFL_player_id": row["NFL_player_id"],
            "player": row["player"],
            "nfl_position": row.get("nfl_position"),
            "status": row.get("status"),
            "first_year": row.get("first_year"),
            "last_year": row.get("last_year"),
            "position_category": row.get("position_category"),
            "position_side": row.get("position_side"),
            "latest_team": row.get("latest_team"),
            "candidate_count": 1,
        }
        for year in range(first_year, last_year + 1):
            exact_key = (row["name_norm_fallback"], year)
            exact_ids[exact_key].add(str(row["NFL_player_id"]))
            exact_records.setdefault(exact_key, record)
            if row["last_name_fallback"]:
                last_key = (row["last_name_fallback"], year)
                last_ids[last_key].add(str(row["NFL_player_id"]))
                last_records.setdefault(last_key, record)

    exact = {key: exact_records[key] for key, ids in exact_ids.items() if len(ids) == 1}
    last = {key: last_records[key] for key, ids in last_ids.items() if len(ids) == 1}
    return exact, last


def apply_fast_name_year_fallback(
    rows: pd.DataFrame,
    bio_all: pd.DataFrame,
    fallback_lookups: tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], dict[str, Any]]] | None = None,
) -> pd.DataFrame:
    rows = rows.copy()
    rows["exact_name_year_fallback"] = 0
    rows["exact_name_year_candidate_count"] = 0
    rows["name_year_fallback_method"] = ""
    unmapped = rows["bio_NFL_player_id"].isna()
    if not unmapped.any():
        return rows

    exact_lookup, last_lookup = fallback_lookups or build_active_bio_lookup(bio_all)

    for idx, row in rows.loc[unmapped, ["season", "player"]].iterrows():
        year_value = pd.to_numeric(pd.Series([row["season"]]), errors="coerce").iloc[0]
        if pd.isna(year_value):
            continue
        year = int(year_value)
        name_key = normalize_name(row["player"])
        last_key = last_name_key(row["player"])
        record = exact_lookup.get((name_key, year))
        method = "exact_name_year_single_candidate"
        if record is None and last_key:
            record = last_lookup.get((last_key, year))
            method = "last_name_year_single_candidate"
        if record is None:
            continue
        rows.at[idx, "bio_NFL_player_id"] = record.get("NFL_player_id")
        rows.at[idx, "bio_player"] = record.get("player")
        rows.at[idx, "bio_nfl_position"] = record.get("nfl_position")
        rows.at[idx, "bio_status"] = record.get("status")
        rows.at[idx, "bio_first_year"] = record.get("first_year")
        rows.at[idx, "bio_last_year"] = record.get("last_year")
        rows.at[idx, "bio_position_category"] = record.get("position_category")
        rows.at[idx, "bio_position_side"] = record.get("position_side")
        rows.at[idx, "bio_latest_team"] = record.get("latest_team")
        rows.at[idx, "exact_name_year_fallback"] = 1
        rows.at[idx, "exact_name_year_candidate_count"] = record.get("candidate_count", 1)
        rows.at[idx, "name_year_fallback_method"] = method
    return rows


def attach_identity(
    fact: pd.DataFrame,
    bio_bridge: pd.DataFrame,
    bio_all: pd.DataFrame,
    classified: pd.DataFrame,
    fallback_lookups: tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], dict[str, Any]]] | None = None,
) -> pd.DataFrame:
    rows = fact.copy()
    rows["pfr_id_norm"] = rows["pfr_id"].map(normalize_pfr_id)
    rows = rows.merge(
        bio_bridge.add_prefix("bio_"),
        left_on="pfr_id_norm",
        right_on="bio_pfr_id_norm",
        how="left",
    )
    rows = rows.merge(classified, on="pfr_id_norm", how="left")
    rows = apply_fast_name_year_fallback(rows, bio_all, fallback_lookups)
    rows = apply_confirmed_pfr_identity_overrides(rows)

    rows["NFL_player_id"] = rows["bio_NFL_player_id"]
    rows["player"] = rows["bio_player"].combine_first(rows["player"])
    rows["position"] = rows["bio_nfl_position"]
    rows["nfl_position"] = rows["position"]
    rows["bridge_classification"] = rows["classification"].fillna("existing_bio_bridge")
    rows.loc[rows["exact_name_year_fallback"].eq(1), "bridge_classification"] = (
        "exact_name_year_single_candidate_bridge"
    )
    rows.loc[rows["name_year_fallback_method"].eq("last_name_year_single_candidate"), "bridge_classification"] = (
        "last_name_year_single_candidate_bridge"
    )
    confirmed_mask = rows["confirmed_pfr_identity_action"].astype("string").str.strip().ne("")
    rows.loc[confirmed_mask, "bridge_classification"] = rows.loc[confirmed_mask, "confirmed_pfr_identity_action"]
    rows.loc[rows["NFL_player_id"].isna(), "bridge_classification"] = "identity_missing_or_ambiguous"

    for pfr_col, super_col in STAT_MAP:
        rows[super_col] = pd.to_numeric(rows.get(pfr_col, 0), errors="coerce").fillna(0)
    rows["def_tds"] = pd.to_numeric(rows.get("def_tds", 0), errors="coerce").fillna(0)
    for col in ["kickoff_return_long", "punt_return_long"]:
        rows[col] = pd.to_numeric(rows.get(col, 0), errors="coerce").fillna(0)

    rows["year"] = pd.to_numeric(rows["season"], errors="coerce").astype("Int64")
    rows["week"] = pd.to_numeric(rows["pfr_week_num"], errors="coerce").astype("Int64")
    key_mask = rows["NFL_player_id"].notna() & rows["year"].notna() & rows["week"].notna()
    rows["player_week"] = None
    rows.loc[key_mask, "player_week"] = (
        rows.loc[key_mask, "NFL_player_id"].astype(str)
        + "_"
        + rows.loc[key_mask, "year"].astype(str)
        + "_"
        + rows.loc[key_mask, "week"].astype(str)
    )
    return rows


def aggregate_weekly(rows: pd.DataFrame) -> pd.DataFrame:
    stat_cols = stat_columns()
    mapped = rows[rows["player_week"].notna()].copy()

    first_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "year",
        "week",
        "pfr_id",
        "pfr_id_norm",
        "bridge_classification",
        "confirmed_pfr_identity_action",
        "name_year_fallback_method",
        "boxscore_id",
        "source_tables",
        "team",
        "opponent",
    ]
    weekly = mapped[first_cols].drop_duplicates("player_week", keep="first").copy()

    present_stats = [col for col in stat_cols if col in mapped.columns]
    sum_cols = [col for col in present_stats if col not in LONG_STATS]
    max_cols = [col for col in present_stats if col in LONG_STATS]
    grouped = mapped.groupby("player_week", sort=False)
    if sum_cols:
        weekly = weekly.merge(grouped[sum_cols].sum().reset_index(), on="player_week", how="left")
    if max_cols:
        weekly = weekly.merge(grouped[max_cols].max().reset_index(), on="player_week", how="left")

    count_spec: dict[str, tuple[str, str]] = {
        "game_date_min": ("game_date", "min"),
        "game_date_max": ("game_date_max" if "game_date_max" in mapped.columns else "game_date", "max"),
        "missing_pfr_game_rows": ("missing_pfr_id", "sum"),
        "exact_name_year_fallback": ("exact_name_year_fallback", "sum"),
    }
    count_spec["pfr_game_rows"] = ("player_week", "size")
    count_spec["pfr_boxscores"] = ("boxscore_id", "nunique")
    count_spec["pfr_teams"] = ("team", "nunique")
    count_spec["pfr_opponents"] = ("opponent", "nunique")

    counts = grouped.agg(**count_spec).reset_index()
    weekly = weekly.merge(counts, on="player_week", how="left")

    weekly["pfr_team_list"] = weekly["team"].where(weekly["pfr_teams"].eq(1), "MULTI")
    weekly["pfr_opponent_list"] = weekly["opponent"].where(weekly["pfr_opponents"].eq(1), "MULTI")
    weekly["nfl_team"] = weekly["team"].where(weekly["pfr_teams"].eq(1), None)
    weekly["opponent_nfl_team"] = weekly["opponent"].where(weekly["pfr_opponents"].eq(1), None)
    weekly["season_type"] = "REG"
    weekly["season_type_source"] = "pfr_week_num_reg_assumed"
    weekly["pfr_multi_game_week"] = (
        weekly["pfr_boxscores"].gt(1) | weekly["pfr_teams"].gt(1) | weekly["pfr_opponents"].gt(1)
    ).astype(int)
    return weekly


def compare_weekly_to_live(weekly: pd.DataFrame, audit_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    season_type_map = load_live_year_week_season_types_for_years(
        audit_dir,
        [int(year) for year in weekly["year"].dropna().unique().tolist()],
    )
    weekly = apply_live_season_type(weekly, season_type_map)

    live = load_live_selected_rows_for_years(
        audit_dir,
        [int(year) for year in weekly["year"].dropna().unique().tolist()],
        set(weekly["player_week"].dropna().astype(str)),
    )
    stat_cols = [col for col in stat_columns() if col in weekly.columns and col in live.columns]
    live_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "data_source",
        *stat_cols,
    ]
    live = live[[col for col in live_cols if col in live.columns]].drop_duplicates("player_week", keep="first")
    live = live.add_prefix("live_")
    rec = weekly.merge(live, left_on="player_week", right_on="live_player_week", how="left")
    rec["live_exists"] = rec["live_player_week"].notna().astype(int)

    diff_df = pd.DataFrame(index=rec.index)
    long_frames: list[pd.DataFrame] = []
    for col in stat_cols:
        pfr_value = pd.to_numeric(rec[col], errors="coerce").fillna(0)
        live_value = pd.to_numeric(rec[f"live_{col}"], errors="coerce").fillna(0)
        diff = (pfr_value - live_value).where(rec["live_exists"].eq(1), 0)
        changed = diff.abs().gt(1e-9)
        diff_df[col] = changed
        if changed.any():
            frame = rec.loc[
                changed,
                [
                    "player_week",
                    "NFL_player_id",
                    "player",
                    "position",
                    "year",
                    "week",
                    "season_type",
                    "pfr_team_list",
                    "pfr_opponent_list",
                    "live_nfl_team",
                    "live_opponent_nfl_team",
                    "live_season_type",
                    "pfr_multi_game_week",
                    "bridge_classification",
                ],
            ].copy()
            frame["stat"] = col
            frame["pfr_value"] = pfr_value[changed].to_numpy()
            frame["live_value"] = live_value[changed].to_numpy()
            frame["delta"] = diff[changed].to_numpy()
            long_frames.append(frame)

    rec["stat_diff_count"] = diff_df.sum(axis=1).astype(int) if not diff_df.empty else 0
    rec["diff_cols"] = diff_df.apply(lambda row: ";".join(row.index[row].tolist()), axis=1) if not diff_df.empty else ""

    rec["pfr_team_family"] = [
        normalize_team_family(team, year) if ";" not in str(team) else ""
        for team, year in zip(rec["pfr_team_list"], rec["year"], strict=False)
    ]
    rec["pfr_opponent_family"] = [
        normalize_team_family(team, year) if ";" not in str(team) else ""
        for team, year in zip(rec["pfr_opponent_list"], rec["year"], strict=False)
    ]
    rec["live_team_family"] = [
        normalize_team_family(team, year) for team, year in zip(rec["live_nfl_team"], rec["year"], strict=False)
    ]
    rec["live_opponent_family"] = [
        normalize_team_family(team, year)
        for team, year in zip(rec["live_opponent_nfl_team"], rec["year"], strict=False)
    ]
    rec["context_same_family"] = (
        rec["live_exists"].eq(1)
        & rec["pfr_team_family"].ne("")
        & rec["pfr_opponent_family"].ne("")
        & rec["pfr_team_family"].eq(rec["live_team_family"])
        & rec["pfr_opponent_family"].eq(rec["live_opponent_family"])
    ).astype(int)
    rec["pfr_calendar_season_type"] = [
        normalize_season_type_for_calendar(year, week, season_type)
        for year, week, season_type in zip(rec["year"], rec["week"], rec["season_type"], strict=False)
    ]
    rec["live_calendar_season_type"] = [
        normalize_season_type_for_calendar(year, week, season_type)
        for year, week, season_type in zip(rec["year"], rec["week"], rec["live_season_type"], strict=False)
    ]
    rec["season_type_same"] = (
        rec["live_exists"].eq(1)
        & rec["pfr_calendar_season_type"]
        .astype("string")
        .fillna("")
        .eq(rec["live_calendar_season_type"].astype("string").fillna(""))
    ).astype(int)

    rec["weekly_bucket"] = "weekly_unclassified"
    rec.loc[rec["NFL_player_id"].isna(), "weekly_bucket"] = "weekly_identity_unresolved"
    rec.loc[
        rec["bridge_classification"].astype(str).str.contains("|".join(MANUAL_REVIEW_CLASSES), regex=True, na=False),
        "weekly_bucket",
    ] = "weekly_identity_manual_review"
    mapped = rec["weekly_bucket"].eq("weekly_unclassified")

    rec.loc[mapped & rec["live_exists"].eq(0) & rec["pfr_multi_game_week"].eq(0), "weekly_bucket"] = (
        "weekly_insert_ready"
    )
    rec.loc[mapped & rec["live_exists"].eq(0) & rec["pfr_multi_game_week"].eq(1), "weekly_bucket"] = (
        "weekly_insert_multi_game_review"
    )
    rec.loc[mapped & rec["live_exists"].eq(1) & rec["stat_diff_count"].eq(0), "weekly_bucket"] = "weekly_live_noop"
    rec.loc[
        mapped
        & rec["live_exists"].eq(1)
        & rec["stat_diff_count"].gt(0)
        & rec["pfr_multi_game_week"].eq(0)
        & rec["context_same_family"].eq(1)
        & rec["season_type_same"].eq(1),
        "weekly_bucket",
    ] = "weekly_update_safe_stat_delta"
    rec.loc[
        mapped & rec["live_exists"].eq(1) & rec["stat_diff_count"].gt(0) & rec["pfr_multi_game_week"].eq(1),
        "weekly_bucket",
    ] = "weekly_update_multi_game_review"
    rec.loc[
        mapped
        & rec["live_exists"].eq(1)
        & rec["stat_diff_count"].gt(0)
        & rec["weekly_bucket"].eq("weekly_unclassified"),
        "weekly_bucket",
    ] = "weekly_update_context_review"

    long = pd.concat(long_frames, ignore_index=True) if long_frames else pd.DataFrame()
    return rec, long


def live_read_cols() -> list[str]:
    return [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "data_source",
        *stat_columns(),
    ]


def load_live_selected_rows_for_years(audit_dir: Path, years: list[int], keys: set[str] | None = None) -> pd.DataFrame:
    parts_dir = audit_dir / "fly_supertable_weekly_selected"
    frames: list[pd.DataFrame] = []
    for year in sorted(set(years)):
        path = parts_dir / f"year={int(year)}.parquet"
        if not path.exists():
            continue
        import pyarrow.parquet as pq

        available = set(pq.read_schema(path).names)
        cols = [col for col in live_read_cols() if col in available]
        if "player_week" not in cols:
            continue
        df = pd.read_parquet(path, columns=cols)
        if keys is not None:
            df = df[df["player_week"].astype(str).isin(keys)].copy()
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=live_read_cols())
    return pd.concat(frames, ignore_index=True)


def load_live_year_week_season_types_for_years(audit_dir: Path, years: list[int]) -> dict[tuple[int, int], str]:
    live = load_live_selected_rows_for_years(audit_dir, years, keys=None)
    if live.empty or not {"year", "week", "season_type"}.issubset(live.columns):
        return {}
    live["year"] = pd.to_numeric(live["year"], errors="coerce").astype("Int64")
    live["week"] = pd.to_numeric(live["week"], errors="coerce").astype("Int64")
    live["season_type"] = live["season_type"].astype("string").fillna("")
    live = live[live["year"].notna() & live["week"].notna() & live["season_type"].ne("")]
    if live.empty:
        return {}
    modes = (
        live.groupby(["year", "week"])["season_type"]
        .agg(lambda values: values.mode().iloc[0] if not values.mode().empty else values.iloc[0])
        .reset_index()
    )
    return {(int(row["year"]), int(row["week"])): str(row["season_type"]) for row in modes.to_dict("records")}


def write_outputs(
    rec: pd.DataFrame, long: pd.DataFrame, unmapped: pd.DataFrame, output_dir: Path, args: argparse.Namespace
) -> None:
    rec.to_parquet(output_dir / "pfr_weekly_reconciliation.parquet", index=False)
    long.to_csv(output_dir / "pfr_weekly_stat_diffs_long.csv", index=False)
    unmapped.to_csv(output_dir / "pfr_weekly_identity_unmapped_player_games.csv", index=False)

    summary = (
        rec.groupby("weekly_bucket", dropna=False)
        .agg(
            rows=("player_week", "size"),
            players=("NFL_player_id", pd.Series.nunique),
            min_year=("year", "min"),
            max_year=("year", "max"),
            pfr_game_rows=("pfr_game_rows", "sum"),
            stat_diff_rows=("stat_diff_count", lambda values: int((values > 0).sum())),
        )
        .reset_index()
        .sort_values(["rows", "weekly_bucket"], ascending=[False, True])
    )
    summary.to_csv(output_dir / "pfr_weekly_gap_bucket_summary.csv", index=False)

    by_year = rec.groupby(["weekly_bucket", "year"], dropna=False).size().reset_index(name="rows")
    by_year.sort_values(["weekly_bucket", "year"]).to_csv(output_dir / "pfr_weekly_gap_by_year.csv", index=False)

    if not long.empty:
        stat_summary = (
            long.groupby(["stat"], dropna=False)
            .agg(rows=("player_week", "size"), abs_delta=("delta", lambda values: values.abs().sum()))
            .reset_index()
            .sort_values(["rows", "stat"], ascending=[False, True])
        )
    else:
        stat_summary = pd.DataFrame(columns=["stat", "rows", "abs_delta"])
    stat_summary.to_csv(output_dir / "pfr_weekly_stat_diff_summary.csv", index=False)

    for bucket in sorted(rec["weekly_bucket"].dropna().unique()):
        path = output_dir / f"{bucket}.csv"
        rec[rec["weekly_bucket"].eq(bucket)].head(2000).to_csv(path, index=False)

    top_unresolved = (
        unmapped.groupby(["player", "pfr_id", "season"], dropna=False)
        .size()
        .reset_index(name="game_rows")
        .sort_values("game_rows", ascending=False)
        .head(100)
    )
    top_unresolved.to_csv(output_dir / "pfr_weekly_top_identity_unresolved.csv", index=False)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "retab_dir": str(args.retab_dir),
        "audit_dir": str(args.audit_dir),
        "bio_path": str(args.bio_path),
        "classification_path": str(args.classification_path) if args.classification_path else None,
        "weekly_rows": int(len(rec)),
        "stat_diff_atoms": int(len(long)),
        "unmapped_player_game_rows": int(len(unmapped)),
        "output_dir": str(output_dir),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    bucket_counts = rec["weekly_bucket"].value_counts().sort_index()
    bridge_counts = rec["bridge_classification"].value_counts().sort_index()
    markdown = [
        "# PFR Weekly Aggregate Reconciliation",
        "",
        f"Created: {manifest['created_at_utc']}",
        "",
        "## Buckets",
        "",
        markdown_table(bucket_counts.to_frame("weekly_rows")),
        "",
        "## Bridge Classes",
        "",
        markdown_table(bridge_counts.to_frame("weekly_rows")),
        "",
        "## Top Stat Diffs",
        "",
        markdown_table(stat_summary.head(25).set_index("stat")) if not stat_summary.empty else "_No stat diffs._",
        "",
        "## Outputs",
        "",
        "- `pfr_weekly_reconciliation.parquet`: full weekly reconciliation table.",
        "- `pfr_weekly_stat_diffs_long.csv`: one row per changed weekly atom.",
        "- `pfr_weekly_gap_bucket_summary.csv`: bucket counts and year ranges.",
        "- `weekly_*.csv`: sampled worklists by bucket.",
        "- `pfr_weekly_identity_unmapped_player_games.csv`: raw player-games still not mapped to a weekly identity.",
    ]
    (output_dir / "PFR_WEEKLY_AGGREGATE_RECONCILIATION.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retab-dir", type=Path, default=latest_retab_dir())
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--bio-path", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--classification-path", type=Path, default=DEFAULT_CLASSIFICATION)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--force-output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.force_output_dir or (args.output_root / f"pfr_weekly_reconciliation_{now_stamp()}")
    output_dir.mkdir(parents=True, exist_ok=False)

    bio_all = load_bio(args.bio_path)
    bio_bridge = build_bio_bridge(bio_all)
    classified = build_classification(args.classification_path)
    fallback_lookups = build_active_bio_lookup(bio_all)

    rec_parts: list[pd.DataFrame] = []
    long_parts: list[pd.DataFrame] = []
    unmapped_parts: list[pd.DataFrame] = []
    for year in available_fact_years(args.retab_dir):
        fact = load_fact_year(args.retab_dir, year)
        identified = attach_identity(fact, bio_bridge, bio_all, classified, fallback_lookups)
        unmapped_parts.append(identified[identified["player_week"].isna()].copy())
        weekly = aggregate_weekly(identified)
        rec, long = compare_weekly_to_live(weekly, args.audit_dir)
        rec_parts.append(rec)
        if not long.empty:
            long_parts.append(long)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
            category=FutureWarning,
        )
        rec = pd.concat(rec_parts, ignore_index=True) if rec_parts else pd.DataFrame()
        long = pd.concat(long_parts, ignore_index=True) if long_parts else pd.DataFrame()
        unmapped = pd.concat(unmapped_parts, ignore_index=True) if unmapped_parts else pd.DataFrame()
    write_outputs(rec, long, unmapped, output_dir, args)

    print(f"Wrote {len(rec):,} weekly PFR rows to {output_dir}")
    print(rec["weekly_bucket"].value_counts().sort_index().to_string())
    print(f"Unmapped player-game rows: {len(unmapped):,}")
    if not long.empty:
        print(f"Changed weekly stat atoms: {len(long):,}")


if __name__ == "__main__":
    main()
