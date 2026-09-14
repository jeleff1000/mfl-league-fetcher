#!/usr/bin/env python3
"""Build a local insert stage for PFR boxscore rows missing from supertable.

This script is intentionally local-only. It turns the zero-bridge PFR audit
into reviewable parquet/csv artifacts:

* pfr_weekly_missing_insert_stage.parquet
* pfr_weekly_missing_insert_ready.parquet
* pfr_weekly_missing_review_rows.csv
* pfr_weekly_missing_stage_summary.csv
* PFR_WEEKLY_MISSING_INSERT_STAGE.md

It does not write to Fly.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
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


MANUAL_REVIEW_CLASSES = {
    "existing_supertable_identity_review",
    "same_name_bio_review_or_split",
    "same_name_no_overlap_review",
}

CONFIRMED_PFR_ID_OVERRIDES = {
    # PFR IDs that should bridge to an existing live/player-bio identity instead
    # of the first same-name bio row selected by the generic bridge.
    "browch25": {"NFL_player_id": "00-0021457", "player": "Chris Brown", "nfl_position": "DB"},
    "davich03": {"NFL_player_id": "00-0025794", "player": "Chris Davis", "nfl_position": "WR"},
    "willch06": {"NFL_player_id": "00-0026691", "player": "Chris Williams", "nfl_position": "WR"},
    "carttj00": {"NFL_player_id": "00-0037052", "player": "T.J. Carter", "nfl_position": "DB"},
    "martad00": {"NFL_player_id": "00-0038658", "player": "Adrian Martinez", "nfl_position": "QB"},
    "joneja16": {"NFL_player_id": "00-0040317", "player": "Jacoby Jones", "nfl_position": "WR"},
    "whitre20": {"NFL_player_id": "00-0020125", "player": "Reggie White", "nfl_position": "RB"},
    # The zero-gap bio bridge had the two Jonah Williams PFR IDs swapped.
    "willjo16": {"NFL_player_id": "00-0035944", "player": "Jonah Williams", "nfl_position": "DL"},
    "willjo10": {"NFL_player_id": "00-0035629", "player": "Jonah Williams", "nfl_position": "OT"},
}

CONFIRMED_DISTINCT_PFR_IDS = {
    "johndo99": "confirmed_distinct_pfr_identity_bridge",
    "browjo03": "confirmed_distinct_pfr_identity_bridge",
    "stewja20": "confirmed_distinct_pfr_identity_bridge",
}

NUMERIC_IDENTITY_COLS = {
    "year",
    "week",
    "nfl_franchise_number",
    "opponent_nfl_franchise_number",
    "pfr_team_franchise_number",
    "pfr_opponent_franchise_number",
}


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def normalize_pfr_id(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()


def normalize_name(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def last_name_key(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    tokens = re.findall(r"[a-z]+", str(value).lower())
    return tokens[-1] if tokens else ""


def pfr_id_like(value: Any) -> bool:
    text = "" if value is None or pd.isna(value) else str(value).strip()
    return bool(re.match(r"^[A-Za-z][A-Za-z]+[A-Za-z]?[A-Za-z]?[0-9][0-9]$", text))


def clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def apply_confirmed_pfr_identity_overrides(rows: pd.DataFrame) -> pd.DataFrame:
    rows["confirmed_pfr_identity_action"] = ""
    if "pfr_id_norm" not in rows.columns:
        return rows

    for pfr_id_norm, override in CONFIRMED_PFR_ID_OVERRIDES.items():
        mask = rows["pfr_id_norm"].eq(pfr_id_norm)
        if not mask.any():
            continue
        rows.loc[mask, "bio_NFL_player_id"] = override["NFL_player_id"]
        rows.loc[mask, "bio_player"] = override["player"]
        rows.loc[mask, "bio_nfl_position"] = override["nfl_position"]
        rows.loc[mask, "confirmed_pfr_identity_action"] = "manual_pfr_id_override_bridge"

    distinct_mask = rows["pfr_id_norm"].isin(CONFIRMED_DISTINCT_PFR_IDS)
    if distinct_mask.any():
        missing_id = rows["bio_NFL_player_id"].isna() | rows["bio_NFL_player_id"].astype("string").str.strip().eq("")
        rows.loc[distinct_mask & missing_id, "bio_NFL_player_id"] = rows.loc[distinct_mask & missing_id, "pfr_id"]
        rows.loc[distinct_mask, "confirmed_pfr_identity_action"] = rows.loc[distinct_mask, "pfr_id_norm"].map(
            CONFIRMED_DISTINCT_PFR_IDS
        )
    return rows


def latest_zero_audit_dir() -> Path:
    dirs = sorted(
        (DEFAULT_OUTPUT_ROOT).glob("pfr_boxscore_supertable_audit_zero_bridge_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in dirs:
        if (path / "pfr_missing_from_supertable.csv").exists():
            return path
    if DEFAULT_AUDIT_DIR.exists():
        return DEFAULT_AUDIT_DIR
    raise FileNotFoundError("No zero-bridge PFR audit directory found")


def latest_zero_gap_bio() -> Path:
    dirs = sorted(
        (DEFAULT_OUTPUT_ROOT).glob("pfr_remaining_bridge_stubs_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in dirs:
        bio = path / "player_bio_pfr_bridge_zero_gap.parquet"
        if bio.exists():
            return bio
    if DEFAULT_BIO.exists():
        return DEFAULT_BIO
    raise FileNotFoundError("No zero-gap PFR bridge bio parquet found")


def latest_classification() -> Path | None:
    dirs = sorted(
        (DEFAULT_OUTPUT_ROOT).glob("pfr_remaining_bridge_stubs_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in dirs:
        csv_path = path / "pfr_remaining_bridge_classification.csv"
        if csv_path.exists():
            return csv_path
    return DEFAULT_CLASSIFICATION if DEFAULT_CLASSIFICATION.exists() else None


def load_live_selected_rows(audit_dir: Path, keys: set[str] | None = None) -> pd.DataFrame:
    parts_dir = audit_dir / "fly_supertable_weekly_selected"
    if not parts_dir.exists():
        return pd.DataFrame()

    desired_cols = [
        "player_week",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "season_type",
        *stat_columns(),
    ]
    frames: list[pd.DataFrame] = []
    for path in sorted(parts_dir.glob("year=*.parquet")):
        available_cols = set(pq.read_schema(path).names)
        read_cols = [col for col in desired_cols if col in available_cols]
        if "player_week" not in read_cols:
            continue
        df = pd.read_parquet(path, columns=read_cols)
        if keys is not None:
            df = df[df["player_week"].astype(str).isin(keys)].copy()
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
            category=FutureWarning,
        )
        return pd.concat(frames, ignore_index=True)


def load_live_year_week_season_types(audit_dir: Path) -> dict[tuple[int, int], str]:
    parts_dir = audit_dir / "fly_supertable_weekly_selected"
    if not parts_dir.exists():
        return {}

    frames: list[pd.DataFrame] = []
    desired_cols = ["year", "week", "season_type"]
    for path in sorted(parts_dir.glob("year=*.parquet")):
        available_cols = set(pq.read_schema(path).names)
        read_cols = [col for col in desired_cols if col in available_cols]
        if len(read_cols) != len(desired_cols):
            continue
        df = pd.read_parquet(path, columns=read_cols)
        if not df.empty:
            frames.append(df)
    if not frames:
        return {}

    live = pd.concat(frames, ignore_index=True)
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


def apply_live_season_type(stage: pd.DataFrame, season_type_map: dict[tuple[int, int], str]) -> pd.DataFrame:
    if not season_type_map:
        return stage

    inferred = []
    for row in stage[["year", "week"]].to_dict("records"):
        year = row.get("year")
        week = row.get("week")
        if pd.isna(year) or pd.isna(week):
            inferred.append(None)
        else:
            inferred.append(season_type_map.get((int(year), int(week))))
    inferred_series = pd.Series(inferred, index=stage.index)
    mask = inferred_series.notna()
    stage.loc[mask, "season_type"] = inferred_series[mask].to_numpy()
    stage.loc[mask, "season_type_source"] = "live_supertable_year_week_mode"
    return stage


def load_bio(bio_path: Path) -> pd.DataFrame:
    cols = [
        "NFL_player_id",
        "player",
        "nfl_position",
        "position_category",
        "position_side",
        "latest_team",
        "status",
        "first_year",
        "last_year",
        "pfr_id",
    ]
    all_bio = pd.read_parquet(bio_path)
    return all_bio[[col for col in cols if col in all_bio.columns]].copy()


def build_bio_bridge(bio_all: pd.DataFrame) -> pd.DataFrame:
    bio = bio_all.copy()
    bio["pfr_id_norm"] = bio["pfr_id"].map(normalize_pfr_id)
    fallback_mask = bio["pfr_id_norm"].eq("") & bio["NFL_player_id"].map(pfr_id_like)
    bio.loc[fallback_mask, "pfr_id_norm"] = bio.loc[fallback_mask, "NFL_player_id"].map(normalize_pfr_id)
    bio = bio[bio["pfr_id_norm"].ne("")].copy()
    bio = bio.drop_duplicates("pfr_id_norm", keep="first")
    return bio


def build_classification(classification_path: Path | None) -> pd.DataFrame:
    if classification_path is None or not classification_path.exists():
        return pd.DataFrame(columns=["pfr_id_norm", "classification", "recommended_action"])
    cols = ["pfr_id_norm", "classification", "recommended_action"]
    classified = pd.read_csv(classification_path, usecols=lambda col: col in cols)
    classified["pfr_id_norm"] = classified["pfr_id_norm"].map(normalize_pfr_id)
    return classified.drop_duplicates("pfr_id_norm", keep="first")


def stat_columns() -> list[str]:
    cols = [super_col for _, super_col in STAT_MAP]
    extras = [
        "def_tds",
        "kickoff_return_long",
        "punt_return_long",
    ]
    return list(dict.fromkeys([*cols, *extras]))


def apply_exact_name_year_fallback(rows: pd.DataFrame, bio_all: pd.DataFrame) -> pd.DataFrame:
    rows["exact_name_year_fallback"] = 0
    rows["exact_name_year_candidate_count"] = 0
    rows["name_year_fallback_method"] = ""
    unmapped = rows["bio_NFL_player_id"].isna()
    if not unmapped.any():
        return rows

    bio = bio_all.copy()
    bio["bio_name_norm_fallback"] = bio["player"].map(normalize_name)
    bio["bio_last_name_fallback"] = bio["player"].map(last_name_key)
    bio["first_year_num_fallback"] = pd.to_numeric(bio.get("first_year"), errors="coerce")
    bio["last_year_num_fallback"] = pd.to_numeric(bio.get("last_year"), errors="coerce")
    bio = bio[bio["bio_name_norm_fallback"].ne("")].copy()

    for idx, row in rows.loc[unmapped].iterrows():
        year = pd.to_numeric(pd.Series([row.get("season")]), errors="coerce").iloc[0]
        if pd.isna(year):
            continue
        name_norm = normalize_name(row.get("player"))
        if not name_norm:
            continue
        candidates = bio[bio["bio_name_norm_fallback"].eq(name_norm)].copy()
        if not candidates.empty:
            candidates = candidates[
                (candidates["first_year_num_fallback"].isna() | candidates["first_year_num_fallback"].le(year))
                & (candidates["last_year_num_fallback"].isna() | candidates["last_year_num_fallback"].ge(year))
            ].copy()
        if candidates.empty:
            last_name = last_name_key(row.get("player"))
            if last_name:
                candidates = bio[bio["bio_last_name_fallback"].eq(last_name)].copy()
                candidates = candidates[
                    (candidates["first_year_num_fallback"].isna() | candidates["first_year_num_fallback"].le(year))
                    & (candidates["last_year_num_fallback"].isna() | candidates["last_year_num_fallback"].ge(year))
                ].copy()
                fallback_method = "last_name_year_single_candidate"
            else:
                fallback_method = ""
        else:
            fallback_method = "exact_name_year_single_candidate"

        if candidates.empty:
            continue
        candidates = candidates[
            (candidates["first_year_num_fallback"].isna() | candidates["first_year_num_fallback"].le(year))
            & (candidates["last_year_num_fallback"].isna() | candidates["last_year_num_fallback"].ge(year))
        ].copy()
        unique_ids = candidates["NFL_player_id"].dropna().astype(str).unique()
        rows.at[idx, "exact_name_year_candidate_count"] = len(unique_ids)
        if len(unique_ids) != 1:
            continue
        candidate = candidates[candidates["NFL_player_id"].astype(str).eq(unique_ids[0])].iloc[0]
        rows.at[idx, "bio_NFL_player_id"] = candidate.get("NFL_player_id")
        rows.at[idx, "bio_player"] = candidate.get("player")
        rows.at[idx, "bio_nfl_position"] = candidate.get("nfl_position")
        rows.at[idx, "bio_status"] = candidate.get("status")
        rows.at[idx, "bio_first_year"] = candidate.get("first_year")
        rows.at[idx, "bio_last_year"] = candidate.get("last_year")
        rows.at[idx, "bio_position_category"] = candidate.get("position_category")
        rows.at[idx, "bio_position_side"] = candidate.get("position_side")
        rows.at[idx, "bio_latest_team"] = candidate.get("latest_team")
        rows.at[idx, "exact_name_year_fallback"] = 1
        rows.at[idx, "name_year_fallback_method"] = fallback_method

    return rows


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    text_df = df.reset_index()
    headers = [str(col) for col in text_df.columns]
    rows = text_df.astype("string").fillna("").values.tolist()
    widths = [max(len(str(header)), *(len(str(row[i])) for row in rows)) for i, header in enumerate(headers)]

    def fmt_row(values: list[Any]) -> str:
        cells = [str(value).ljust(widths[i]) for i, value in enumerate(values)]
        return "| " + " | ".join(cells) + " |"

    header_row = fmt_row(headers)
    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    body = [fmt_row(row) for row in rows]
    return "\n".join([header_row, separator, *body])


def build_stage(
    missing: pd.DataFrame,
    bio_bridge: pd.DataFrame,
    bio_all: pd.DataFrame,
    classified: pd.DataFrame,
) -> pd.DataFrame:
    rows = missing.copy()
    rows["pfr_id_norm"] = rows["pfr_id"].map(normalize_pfr_id)
    rows = rows.merge(
        bio_bridge.add_prefix("bio_"),
        left_on="pfr_id_norm",
        right_on="bio_pfr_id_norm",
        how="left",
    )
    rows = rows.merge(classified, on="pfr_id_norm", how="left")
    rows = apply_exact_name_year_fallback(rows, bio_all)
    rows = apply_confirmed_pfr_identity_overrides(rows)

    rows["NFL_player_id"] = rows["bio_NFL_player_id"].map(clean_text)
    rows["player"] = rows["bio_player"].combine_first(rows["player"])
    rows["position"] = rows["bio_nfl_position"].map(clean_text)
    rows["nfl_position"] = rows["position"]
    rows["year"] = pd.to_numeric(rows["season"], errors="coerce").astype("Int64")
    rows["week"] = pd.to_numeric(rows["pfr_week_num"], errors="coerce").astype("Int64")
    rows["nfl_team"] = rows["team"].map(clean_text)
    rows["opponent_nfl_team"] = rows["opponent"].map(clean_text)
    rows["nfl_franchise_number"] = pd.to_numeric(rows["pfr_team_franchise_number"], errors="coerce").astype("Int64")
    rows["opponent_nfl_franchise_number"] = pd.to_numeric(
        rows["pfr_opponent_franchise_number"], errors="coerce"
    ).astype("Int64")
    rows["season_type"] = "REG"
    rows["season_type_source"] = "pfr_week_num_reg_assumed"
    rows["data_source"] = "pfr_boxscore_missing_stage"

    has_key_parts = (
        rows["NFL_player_id"].notna()
        & rows["NFL_player_id"].astype(str).str.strip().ne("")
        & rows["year"].notna()
        & rows["week"].notna()
    )
    rows["player_week"] = None
    rows.loc[has_key_parts, "player_week"] = (
        rows.loc[has_key_parts, "NFL_player_id"].astype(str)
        + "_"
        + rows.loc[has_key_parts, "year"].astype(str)
        + "_"
        + rows.loc[has_key_parts, "week"].astype(str)
    )

    for pfr_col, super_col in STAT_MAP:
        rows[super_col] = pd.to_numeric(rows.get(pfr_col, 0), errors="coerce").fillna(0)
    rows["def_tds"] = pd.to_numeric(rows.get("def_tds", 0), errors="coerce").fillna(0)
    for col in ["kickoff_return_long", "punt_return_long"]:
        rows[col] = pd.to_numeric(rows.get(col, 0), errors="coerce").fillna(0)

    rows["bridge_classification"] = rows["classification"].fillna("existing_bio_bridge")
    rows.loc[
        rows["exact_name_year_fallback"].eq(1),
        "bridge_classification",
    ] = "exact_name_year_single_candidate_bridge"
    rows.loc[
        rows["name_year_fallback_method"].eq("last_name_year_single_candidate"),
        "bridge_classification",
    ] = "last_name_year_single_candidate_bridge"
    rows.loc[rows["pfr_id_norm"].eq(""), "bridge_classification"] = "identity_missing_name_only"
    rows.loc[
        rows["exact_name_year_fallback"].eq(1),
        "bridge_classification",
    ] = "exact_name_year_single_candidate_bridge"
    rows.loc[
        rows["name_year_fallback_method"].eq("last_name_year_single_candidate"),
        "bridge_classification",
    ] = "last_name_year_single_candidate_bridge"
    confirmed_mask = rows["confirmed_pfr_identity_action"].astype("string").str.strip().ne("")
    rows.loc[confirmed_mask, "bridge_classification"] = rows.loc[confirmed_mask, "confirmed_pfr_identity_action"]
    rows["recommended_action"] = rows["recommended_action"].fillna("insert_missing_weekly_rows")
    rows.loc[
        rows["bridge_classification"].isin(MANUAL_REVIEW_CLASSES),
        "recommended_action",
    ] = "review_identity_before_weekly_insert"

    rows["stage_bucket"] = "insert_ready"
    rows.loc[rows["NFL_player_id"].isna(), "stage_bucket"] = "identity_missing_name_only"
    rows.loc[
        rows["bridge_classification"].isin(MANUAL_REVIEW_CLASSES) & rows["stage_bucket"].eq("insert_ready"),
        "stage_bucket",
    ] = "manual_identity_review"

    duplicated_stage_key = rows["player_week"].notna() & rows.duplicated("player_week", keep=False)
    rows.loc[
        duplicated_stage_key & rows["stage_bucket"].eq("insert_ready"),
        "stage_bucket",
    ] = "duplicate_stage_player_week_review"

    output_cols = [
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
        *stat_columns(),
        "pfr_id",
        "pfr_player_game_key",
        "boxscore_id",
        "game_date",
        "team",
        "opponent",
        "is_home",
        "is_neutral",
        "game_result",
        "source_tables",
        "pfr_activity",
        "missing_pfr_id",
        "pfr_id_norm",
        "bridge_classification",
        "confirmed_pfr_identity_action",
        "recommended_action",
        "stage_bucket",
        "exact_name_year_fallback",
        "exact_name_year_candidate_count",
        "name_year_fallback_method",
        "season_type_source",
        "bio_status",
        "bio_first_year",
        "bio_last_year",
        "bio_position_category",
        "bio_position_side",
        "bio_latest_team",
    ]
    for col in output_cols:
        if col not in rows.columns:
            rows[col] = None

    return rows[output_cols].copy()


def apply_collision_bucket(stage: pd.DataFrame, live_selected: pd.DataFrame) -> pd.DataFrame:
    if live_selected.empty or "player_week" not in live_selected.columns:
        stage["live_player_week_collision"] = 0
        stage["collision_stat_diff_count"] = 0
        stage["collision_diff_cols"] = ""
        stage["collision_team_diff"] = 0
        stage["collision_franchise_diff"] = 0
        stage["collision_season_type_diff"] = 0
        return stage

    live_player_weeks = set(live_selected["player_week"].dropna().astype(str))
    collision = stage["player_week"].notna() & stage["player_week"].isin(live_player_weeks)
    stage["live_player_week_collision"] = collision.astype(int)

    stage["collision_stat_diff_count"] = 0
    stage["collision_diff_cols"] = ""
    stage["collision_team_diff"] = 0
    stage["collision_franchise_diff"] = 0
    stage["collision_season_type_diff"] = 0

    collision_rows = stage[collision].copy()
    if collision_rows.empty:
        return stage

    stat_cols = [col for col in stat_columns() if col in stage.columns and col in live_selected.columns]
    live_cols = [
        "player_week",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "season_type",
        *stat_cols,
    ]
    live_cols = [col for col in live_cols if col in live_selected.columns]
    live = live_selected[live_cols].drop_duplicates("player_week", keep="first").add_prefix("live_")
    merge_cols = [
        "player_week",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "season_type",
        *stat_cols,
    ]
    merge_cols = [col for col in merge_cols if col in collision_rows.columns]
    merged = collision_rows[merge_cols].merge(
        live,
        left_on="player_week",
        right_on="live_player_week",
        how="left",
    )

    diff_df = pd.DataFrame(index=merged.index)
    for col in stat_cols:
        stage_val = pd.to_numeric(merged[col], errors="coerce").fillna(0)
        live_val = pd.to_numeric(merged[f"live_{col}"], errors="coerce").fillna(0)
        diff_df[col] = (stage_val - live_val).abs().gt(1e-9)
    diff_counts = diff_df.sum(axis=1).astype(int)
    diff_names = diff_df.apply(lambda row: ";".join(row.index[row].tolist()), axis=1)

    team_diff = (
        merged["nfl_team"].astype("string").fillna("")
        != merged.get("live_nfl_team", pd.Series("", index=merged.index)).astype("string").fillna("")
    ) | (
        merged["opponent_nfl_team"].astype("string").fillna("")
        != merged.get("live_opponent_nfl_team", pd.Series("", index=merged.index)).astype("string").fillna("")
    )
    if {
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "live_nfl_franchise_number",
        "live_opponent_nfl_franchise_number",
    }.issubset(merged.columns):
        franchise_diff = (
            pd.to_numeric(merged["nfl_franchise_number"], errors="coerce").fillna(-1).astype(int)
            != pd.to_numeric(merged["live_nfl_franchise_number"], errors="coerce").fillna(-1).astype(int)
        ) | (
            pd.to_numeric(merged["opponent_nfl_franchise_number"], errors="coerce").fillna(-1).astype(int)
            != pd.to_numeric(merged["live_opponent_nfl_franchise_number"], errors="coerce").fillna(-1).astype(int)
        )
    else:
        franchise_diff = pd.Series(False, index=merged.index)
    season_type_diff = merged["season_type"].astype("string").fillna("") != merged.get(
        "live_season_type", pd.Series("", index=merged.index)
    ).astype("string").fillna("")

    stage.loc[collision_rows.index, "collision_stat_diff_count"] = diff_counts.to_numpy()
    stage.loc[collision_rows.index, "collision_diff_cols"] = diff_names.to_numpy()
    stage.loc[collision_rows.index, "collision_team_diff"] = team_diff.astype(int).to_numpy()
    stage.loc[collision_rows.index, "collision_franchise_diff"] = franchise_diff.astype(int).to_numpy()
    stage.loc[collision_rows.index, "collision_season_type_diff"] = season_type_diff.astype(int).to_numpy()

    ready_collision = collision & stage["stage_bucket"].eq("insert_ready")
    stage.loc[
        ready_collision & stage["collision_stat_diff_count"].eq(0),
        "stage_bucket",
    ] = "player_week_collision_noop"
    stage.loc[
        ready_collision & stage["collision_stat_diff_count"].gt(0),
        "stage_bucket",
    ] = "player_week_collision_update_candidate"
    return stage


def write_collision_stat_diffs(stage: pd.DataFrame, audit_dir: Path, output_dir: Path) -> None:
    candidates = stage[stage["stage_bucket"].eq("player_week_collision_update_candidate")].copy()
    if candidates.empty:
        pd.DataFrame(
            columns=[
                "player_week",
                "NFL_player_id",
                "player",
                "year",
                "week",
                "stat",
                "pfr_value",
                "live_value",
                "delta",
            ]
        ).to_csv(output_dir / "pfr_weekly_collision_stat_diffs_long.csv", index=False)
        return

    keys = set(candidates["player_week"].dropna().astype(str))
    live_selected = load_live_selected_rows(audit_dir, keys)
    if live_selected.empty:
        return

    stat_cols = [col for col in stat_columns() if col in candidates.columns and col in live_selected.columns]
    live_cols = [
        "player_week",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "season_type",
        *stat_cols,
    ]
    live = live_selected[[col for col in live_cols if col in live_selected.columns]]
    live = live.drop_duplicates("player_week", keep="first").add_prefix("live_")
    merged = candidates.merge(live, left_on="player_week", right_on="live_player_week", how="left")

    diff_frames: list[pd.DataFrame] = []
    for col in stat_cols:
        pfr_value = pd.to_numeric(merged[col], errors="coerce").fillna(0)
        live_value = pd.to_numeric(merged[f"live_{col}"], errors="coerce").fillna(0)
        delta = pfr_value - live_value
        mask = delta.abs().gt(1e-9)
        if not mask.any():
            continue
        diff = merged.loc[
            mask,
            [
                "player_week",
                "NFL_player_id",
                "player",
                "position",
                "year",
                "week",
                "season_type",
                "nfl_team",
                "opponent_nfl_team",
                "live_nfl_team",
                "live_opponent_nfl_team",
                "nfl_franchise_number",
                "opponent_nfl_franchise_number",
                "live_nfl_franchise_number",
                "live_opponent_nfl_franchise_number",
                "live_season_type",
                "source_tables",
                "bridge_classification",
            ],
        ].copy()
        diff["stat"] = col
        diff["pfr_value"] = pfr_value[mask].to_numpy()
        diff["live_value"] = live_value[mask].to_numpy()
        diff["delta"] = delta[mask].to_numpy()
        diff_frames.append(diff)

    if diff_frames:
        long = pd.concat(diff_frames, ignore_index=True)
    else:
        long = pd.DataFrame()
    long.to_csv(output_dir / "pfr_weekly_collision_stat_diffs_long.csv", index=False)


def write_summary_files(stage: pd.DataFrame, bio: pd.DataFrame, output_dir: Path, args: argparse.Namespace) -> None:
    summary = (
        stage.groupby(["stage_bucket", "bridge_classification"], dropna=False)
        .agg(
            rows=("player_week", "size"),
            players=("NFL_player_id", pd.Series.nunique),
            pfr_ids=("pfr_id_norm", pd.Series.nunique),
            min_year=("year", "min"),
            max_year=("year", "max"),
        )
        .reset_index()
        .sort_values(["stage_bucket", "rows"], ascending=[True, False])
    )
    summary.to_csv(output_dir / "pfr_weekly_missing_stage_summary.csv", index=False)

    year_summary = (
        stage.groupby(["stage_bucket", "year"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["stage_bucket", "year"])
    )
    year_summary.to_csv(output_dir / "pfr_weekly_missing_stage_by_year.csv", index=False)

    stat_cols = stat_columns()
    stat_summary = (
        stage.groupby("stage_bucket", dropna=False)[stat_cols]
        .sum(numeric_only=True)
        .reset_index()
        .sort_values("stage_bucket")
    )
    stat_summary.to_csv(output_dir / "pfr_weekly_missing_stat_summary.csv", index=False)

    ready = stage[stage["stage_bucket"].eq("insert_ready")].copy()
    if ready.empty:
        recompute_scope = pd.DataFrame(columns=["year", "week", "rows"])
        metric_scope = pd.DataFrame(columns=["NFL_player_id", "player", "min_year", "max_year", "weekly_rows"])
    else:
        recompute_scope = (
            ready.groupby(["year", "week"], dropna=False).size().reset_index(name="rows").sort_values(["year", "week"])
        )
        metric_scope = (
            ready.groupby(["NFL_player_id", "player"], dropna=False)
            .agg(min_year=("year", "min"), max_year=("year", "max"), weekly_rows=("player_week", "size"))
            .reset_index()
            .sort_values(["weekly_rows", "NFL_player_id"], ascending=[False, True])
        )
    recompute_scope.to_csv(output_dir / "pfr_weekly_missing_rank_recompute_scope.csv", index=False)
    metric_scope.to_csv(output_dir / "pfr_weekly_missing_metric_recompute_scope.csv", index=False)

    required_ids = ready["NFL_player_id"].dropna().astype(str).unique().tolist()
    bio_required = bio[bio["NFL_player_id"].astype(str).isin(required_ids)].copy()
    bio_required.to_parquet(output_dir / "pfr_bio_rows_required_for_insert_ready.parquet", index=False)

    review = stage[stage["stage_bucket"].ne("insert_ready")].copy()
    review.to_csv(output_dir / "pfr_weekly_missing_review_rows.csv", index=False)
    stage[stage["stage_bucket"].eq("player_week_collision_update_candidate")].to_parquet(
        output_dir / "pfr_weekly_collision_update_candidates.parquet",
        index=False,
    )
    stage[stage["stage_bucket"].eq("player_week_collision_noop")].to_parquet(
        output_dir / "pfr_weekly_collision_noop_rows.parquet",
        index=False,
    )
    write_collision_stat_diffs(stage, args.audit_dir, output_dir)

    top_ready = (
        ready.groupby(["NFL_player_id", "player", "position"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "NFL_player_id"], ascending=[False, True])
        .head(20)
    )
    top_ready.to_csv(output_dir / "pfr_weekly_missing_top_insert_ready_players.csv", index=False)

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "audit_dir": str(args.audit_dir),
        "bio_path": str(args.bio_path),
        "classification_path": str(args.classification_path) if args.classification_path else None,
        "rows_total": int(len(stage)),
        "rows_insert_ready": int(stage["stage_bucket"].eq("insert_ready").sum()),
        "rows_review": int(stage["stage_bucket"].ne("insert_ready").sum()),
        "live_player_week_collisions": int(stage["live_player_week_collision"].sum()),
        "collision_noop_rows": int(stage["stage_bucket"].eq("player_week_collision_noop").sum()),
        "collision_update_candidate_rows": int(
            stage["stage_bucket"].eq("player_week_collision_update_candidate").sum()
        ),
        "output_dir": str(output_dir),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    bucket_counts = stage["stage_bucket"].value_counts().sort_index()
    class_counts = stage["bridge_classification"].value_counts().sort_index()
    stat_totals = ready[stat_cols].sum(numeric_only=True).sort_values(ascending=False)
    stat_totals = stat_totals[stat_totals.ne(0)].head(20)

    markdown = [
        "# PFR Weekly Missing Insert Stage",
        "",
        f"Created: {manifest['created_at_utc']}",
        "",
        "## Inputs",
        "",
        f"- Audit dir: `{args.audit_dir}`",
        f"- Zero-gap bio: `{args.bio_path}`",
        f"- Classification: `{args.classification_path}`",
        "",
        "## Row Buckets",
        "",
        markdown_table(bucket_counts.to_frame("rows")),
        "",
        "## Bridge Classification",
        "",
        markdown_table(class_counts.to_frame("rows")),
        "",
        "## Insert-Ready Stat Totals",
        "",
        markdown_table(stat_totals.to_frame("sum")) if not stat_totals.empty else "_No insert-ready stat totals._",
        "",
        "## Outputs",
        "",
        "- `pfr_weekly_missing_insert_stage.parquet`: all staged rows, including review buckets.",
        "- `pfr_weekly_missing_insert_ready.parquet`: rows ready for local merge testing.",
        "- `pfr_weekly_missing_review_rows.csv`: rows blocked by identity/collision/duplicate review.",
        "- `pfr_weekly_collision_update_candidates.parquet`: existing player-week rows where PFR has different stat atoms.",
        "- `pfr_weekly_collision_stat_diffs_long.csv`: one row per changed atom in the collision update candidates.",
        "- `pfr_weekly_collision_noop_rows.parquet`: existing player-week rows where PFR already agrees with live selected atoms.",
        "- `pfr_bio_rows_required_for_insert_ready.parquet`: bio rows required before inserting ready rows.",
        "- `pfr_weekly_missing_rank_recompute_scope.csv`: year/week windows that would need rank recompute.",
        "",
        "## Notes",
        "",
        "- This stage is local-only and does not write to Fly.",
        "- `season_type` is inferred from live supertable year/week labels when available; remaining rows keep `REG` with `season_type_source = pfr_week_num_reg_assumed`.",
        "- Kicking rows do not yet contain made field-goal distance buckets, so yardage-sensitive kicker scoring should be recomputed only after distance parsing is added.",
    ]
    (output_dir / "PFR_WEEKLY_MISSING_INSERT_STAGE.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=latest_zero_audit_dir())
    parser.add_argument("--bio-path", type=Path, default=latest_zero_gap_bio())
    parser.add_argument("--classification-path", type=Path, default=latest_classification())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--force-output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    missing_path = args.audit_dir / "pfr_missing_from_supertable.csv"
    if not missing_path.exists():
        raise FileNotFoundError(f"Missing audit input: {missing_path}")

    output_dir = args.force_output_dir or (args.output_root / f"pfr_weekly_missing_insert_stage_{now_stamp()}")
    output_dir.mkdir(parents=True, exist_ok=False)

    missing = pd.read_csv(missing_path)
    bio_all = load_bio(args.bio_path)
    bio = build_bio_bridge(bio_all)
    classified = build_classification(args.classification_path)
    stage = build_stage(missing, bio, bio_all, classified)
    season_type_map = load_live_year_week_season_types(args.audit_dir)
    stage = apply_live_season_type(stage, season_type_map)
    candidate_keys = set(stage.loc[stage["stage_bucket"].eq("insert_ready"), "player_week"].dropna().astype(str))
    live_selected = load_live_selected_rows(args.audit_dir, candidate_keys)
    stage = apply_collision_bucket(stage, live_selected)

    for col in NUMERIC_IDENTITY_COLS:
        if col in stage.columns:
            stage[col] = pd.to_numeric(stage[col], errors="coerce").astype("Int64")

    stage.to_parquet(output_dir / "pfr_weekly_missing_insert_stage.parquet", index=False)
    stage[stage["stage_bucket"].eq("insert_ready")].to_parquet(
        output_dir / "pfr_weekly_missing_insert_ready.parquet",
        index=False,
    )
    write_summary_files(stage, bio_all, output_dir, args)

    print(f"Wrote {len(stage):,} staged rows to {output_dir}")
    print(stage["stage_bucket"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
