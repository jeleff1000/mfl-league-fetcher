#!/usr/bin/env python3
"""Regenerate PFR usage/advanced gap buckets from an audit output folder.

The main PFR usage backfill script writes a full PFR fact table plus cached
live supertable/player-bio/franchise extracts. This script rebuilds the same
identity/context match stage and writes focused review artifacts for the rows
that still cannot be updated or inserted cleanly.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS_ROOT))

from scripts.apply_pfr_usage_advanced_backfill import (  # noqa: E402
    NEW_SUPERTABLE_COLUMNS,
    OVERLAP_COLUMNS,
    TEAM_CODE_ALIASES,
    build_identity_bridge,
    map_franchise_numbers,
)


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_usage_gap_cleanup_post_insert_20260513T1645Z"

FAMILIES: dict[str, list[str]] = {
    "usage_starter_snap": [
        "is_starter",
        "offense_snaps",
        "offense_snap_pct",
        "defense_snaps",
        "defense_snap_pct",
        "special_teams_snaps",
        "special_teams_snap_pct",
    ],
    "advanced_receiving": [
        "receiving_adot",
        "receiving_broken_tackles",
        "receiving_drops",
        "receiving_target_interceptions",
        "receiving_pass_rating",
    ],
    "advanced_rushing": [
        "rushing_yards_before_contact",
        "rushing_yards_after_contact",
        "rushing_broken_tackles",
    ],
    "advanced_passing": [
        "passing_drops",
        "passing_poor_throws",
        "passing_blitzed",
        "passing_hurried",
        "passing_hits",
        "passing_pressured",
        "rushing_scrambles",
    ],
    "advanced_defense": [
        "def_targets_allowed",
        "def_completions_allowed",
        "def_completion_yards_allowed",
        "def_completion_tds_allowed",
        "def_passer_rating_allowed",
        "def_air_yards_allowed",
        "def_yards_after_catch_allowed",
        "def_blitzes",
        "def_hurries",
        "def_knockdowns",
        "def_pressures",
        "def_tackles_missed",
    ],
}

INSERT_IDENTITY_COLUMNS = [
    "player_week",
    "NFL_player_id",
    "player",
    "year",
    "week",
    "season_type",
    "position",
    "nfl_position",
    "nfl_team",
    "opponent_nfl_team",
    "nfl_franchise_number",
    "opponent_nfl_franchise_number",
    "boxscore_id",
    "game_date",
    "pfr_id",
    "team",
    "opponent",
    "data_source",
]


def unique_join(values: pd.Series, *, limit: int | None = None) -> str:
    clean = [str(v) for v in values.dropna().astype(str).unique() if str(v).strip()]
    clean = sorted(clean)
    if limit is not None:
        clean = clean[:limit]
    return ";".join(clean)


def read_required(audit_dir: Path, name: str) -> pd.DataFrame:
    path = audit_dir / name
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


def build_stage(audit_dir: Path) -> pd.DataFrame:
    pfr = read_required(audit_dir, "pfr_usage_advanced_fact.parquet")
    bio = read_required(audit_dir, "live_player_bio_usage_compare.parquet")
    live = read_required(audit_dir, "live_supertable_usage_compare.parquet")
    franchise_history = read_required(audit_dir, "live_franchise_history.parquet")

    pfr["pfr_id_norm"] = pfr["pfr_id"].astype("string").str.strip().str.lower()
    bridge = build_identity_bridge(bio, live)
    bridge.to_parquet(audit_dir / "pfr_usage_identity_bridge_rebuilt_for_gap_buckets.parquet", index=False)

    pfr = pfr.merge(bridge, on="pfr_id_norm", how="left")
    season_num = pd.to_numeric(pfr["season"], errors="coerce")
    first = pd.to_numeric(pfr["bridge_first_year"], errors="coerce")
    last = pd.to_numeric(pfr["bridge_last_year"], errors="coerce")
    pfr["_bridge_in_range"] = (
        pfr["bridge_NFL_player_id"].notna()
        & (first.isna() | last.isna() | ((season_num >= first - 1) & (season_num <= last + 1)))
    ).astype(int)
    pfr["_bridge_priority"] = pfr["bridge_source"].map({"bio_pfr_id": 1, "bio_nfl_id_is_pfr": 2}).fillna(9)
    pfr = (
        pfr.sort_values(
            ["boxscore_id", "team", "pfr_id", "_bridge_in_range", "_bridge_priority"],
            ascending=[True, True, True, False, True],
        )
        .drop_duplicates(["boxscore_id", "team", "pfr_id"])
        .copy()
    )

    pfr = map_franchise_numbers(pfr, franchise_history)
    pfr["year"] = pd.to_numeric(pfr["season"], errors="coerce").astype("Int64")
    pfr["week"] = pd.to_numeric(pfr["pfr_week_num"], errors="coerce").astype("Int64")
    pfr["NFL_player_id"] = pfr["bridge_NFL_player_id"]
    pfr["player_week"] = (
        pfr["NFL_player_id"].astype("string") + "_" + pfr["year"].astype("string") + "_" + pfr["week"].astype("string")
    )
    pfr.loc[pfr["NFL_player_id"].isna() | pfr["year"].isna() | pfr["week"].isna(), "player_week"] = pd.NA
    pfr["nfl_team"] = (
        pfr["team"]
        .astype("string")
        .str.upper()
        .map(lambda x: TEAM_CODE_ALIASES.get(str(x), str(x)) if pd.notna(x) else x)
    )
    pfr["opponent_nfl_team"] = (
        pfr["opponent"]
        .astype("string")
        .str.upper()
        .map(lambda x: TEAM_CODE_ALIASES.get(str(x), str(x)) if pd.notna(x) else x)
    )
    pfr["nfl_franchise_number"] = pfr["pfr_team_franchise_number"]
    pfr["opponent_nfl_franchise_number"] = pfr["pfr_opponent_franchise_number"]
    pfr["season_type"] = "REG"
    pfr.loc[pfr["week"] > 22, "season_type"] = "POST"
    pfr["data_source"] = "pfr_usage_gap_insert_stage"
    if "position" not in pfr.columns:
        pfr["position"] = pfr.get("position_hint")
    if "nfl_position" not in pfr.columns:
        pfr["nfl_position"] = pfr.get("position_hint")

    live["player_week"] = live["player_week"].astype("string")
    stage = pfr.merge(
        live.add_prefix("live_"),
        left_on="player_week",
        right_on="live_player_week",
        how="left",
        indicator=True,
    )
    stage["matched_existing_row"] = stage["_merge"].eq("both")
    stage["strict_context_match"] = (
        stage["matched_existing_row"]
        & (pd.to_numeric(stage["live_year"], errors="coerce") == pd.to_numeric(stage["year"], errors="coerce"))
        & (pd.to_numeric(stage["live_week"], errors="coerce") == pd.to_numeric(stage["week"], errors="coerce"))
        & (
            stage["pfr_team_franchise_number"].isna()
            | stage["live_nfl_franchise_number"].isna()
            | (
                pd.to_numeric(stage["pfr_team_franchise_number"], errors="coerce")
                == pd.to_numeric(stage["live_nfl_franchise_number"], errors="coerce")
            )
        )
        & (
            stage["pfr_opponent_franchise_number"].isna()
            | stage["live_opponent_nfl_franchise_number"].isna()
            | (
                pd.to_numeric(stage["pfr_opponent_franchise_number"], errors="coerce")
                == pd.to_numeric(stage["live_opponent_nfl_franchise_number"], errors="coerce")
            )
        )
    )

    dup_mask = (
        stage["strict_context_match"] & stage["player_week"].notna() & stage.duplicated("player_week", keep=False)
    )
    stage["gap_bucket"] = "candidate_missing_player_week"
    stage.loc[stage["strict_context_match"] & ~dup_mask, "gap_bucket"] = "matched_existing_clean"
    stage.loc[dup_mask, "gap_bucket"] = "duplicate_context_blocked"
    stage.loc[stage["bridge_NFL_player_id"].isna() | stage["player_week"].isna(), "gap_bucket"] = "no_identity_bridge"
    stage.loc[
        (~stage["strict_context_match"])
        & stage["bridge_NFL_player_id"].notna()
        & stage["player_week"].notna()
        & stage["matched_existing_row"],
        "gap_bucket",
    ] = "context_blocked_existing_key"

    for family, cols in FAMILIES.items():
        present = [col for col in cols if col in stage.columns]
        stage[f"has_{family}"] = stage[present].notna().any(axis=1) if present else False
    return stage


def write_bucket_summary(stage: pd.DataFrame, audit_dir: Path) -> pd.DataFrame:
    rows = []
    for bucket, grp in stage.groupby("gap_bucket", dropna=False):
        year = pd.to_numeric(grp["year"], errors="coerce")
        rows.append(
            {
                "gap_bucket": bucket,
                "rows": int(len(grp)),
                "players": int(grp["pfr_id"].nunique(dropna=True)),
                "player_weeks": int(grp["player_week"].nunique(dropna=True)),
                "min_year": int(year.min()) if year.notna().any() else None,
                "max_year": int(year.max()) if year.notna().any() else None,
                "boxscores": int(grp["boxscore_id"].nunique(dropna=True)),
            }
        )
    summary = pd.DataFrame(rows).sort_values(["rows", "gap_bucket"], ascending=[False, True])
    summary.to_csv(audit_dir / "pfr_usage_gap_bucket_summary.csv", index=False)
    return summary


def write_family_summary(stage: pd.DataFrame, audit_dir: Path) -> pd.DataFrame:
    rows = []
    for family in FAMILIES:
        fam = stage[stage[f"has_{family}"]].copy()
        for bucket, grp in fam.groupby("gap_bucket", dropna=False):
            rows.append({"family": family, "gap_bucket": bucket, "rows": int(len(grp))})
    summary = pd.DataFrame(rows).sort_values(["family", "rows"], ascending=[True, False])
    summary.to_csv(audit_dir / "pfr_usage_gap_bucket_by_family.csv", index=False)
    return summary


def write_review_artifacts(stage: pd.DataFrame, audit_dir: Path) -> dict[str, Any]:
    no_id = stage[stage["gap_bucket"].eq("no_identity_bridge")].copy()
    if no_id.empty:
        no_id_summary = pd.DataFrame(columns=["pfr_id", "player", "rows", "min_year", "max_year", "teams"])
    else:
        no_id_summary = (
            no_id.groupby(["pfr_id", "player"], dropna=False)
            .agg(
                rows=("pfr_id", "size"),
                min_year=("year", "min"),
                max_year=("year", "max"),
                teams=("team", unique_join),
            )
            .reset_index()
            .sort_values(["rows", "pfr_id"], ascending=[False, True])
        )
    no_id_summary.to_csv(audit_dir / "pfr_usage_no_identity_bridge_player_summary.csv", index=False)

    context = stage[stage["gap_bucket"].eq("context_blocked_existing_key")].copy()
    if context.empty:
        context_summary = pd.DataFrame(
            columns=[
                "season",
                "pfr_week_num",
                "team",
                "opponent",
                "live_nfl_team",
                "live_opponent_nfl_team",
                "rows",
                "players",
                "boxscore_id",
                "sample_players",
            ]
        )
    else:
        context_summary = (
            context.groupby(
                [
                    "season",
                    "pfr_week_num",
                    "team",
                    "opponent",
                    "live_nfl_team",
                    "live_opponent_nfl_team",
                ],
                dropna=False,
            )
            .agg(
                rows=("pfr_id", "size"),
                players=("pfr_id", "nunique"),
                boxscore_id=("boxscore_id", unique_join),
                sample_players=("player", lambda s: unique_join(s, limit=8)),
            )
            .reset_index()
            .sort_values(["rows", "season"], ascending=[False, True])
        )
    context_summary.to_csv(audit_dir / "pfr_usage_context_blocked_game_summary.csv", index=False)

    context_cols = [
        "boxscore_id",
        "game_date",
        "season",
        "pfr_week_num",
        "team",
        "opponent",
        "pfr_id",
        "player",
        "bridge_NFL_player_id",
        "player_week",
        "live_player",
        "live_nfl_team",
        "live_opponent_nfl_team",
        "pfr_team_franchise_number",
        "pfr_opponent_franchise_number",
        "live_nfl_franchise_number",
        "live_opponent_nfl_franchise_number",
    ]
    context[[col for col in context_cols if col in context.columns]].head(1000).to_csv(
        audit_dir / "pfr_usage_context_blocked_rows_sample.csv",
        index=False,
    )

    candidates = stage[stage["gap_bucket"].eq("candidate_missing_player_week")].copy()
    has_new_value = candidates[[col for col in NEW_SUPERTABLE_COLUMNS if col in candidates.columns]].notna().any(axis=1)
    candidates = candidates[
        has_new_value & candidates["player_week"].notna() & candidates["NFL_player_id"].notna()
    ].copy()
    duplicate_candidates = candidates[candidates.duplicated("player_week", keep=False)].copy()
    duplicate_cols = [
        "boxscore_id",
        "game_date",
        "season",
        "pfr_week_num",
        "team",
        "opponent",
        "pfr_id",
        "player",
        "bridge_NFL_player_id",
        "player_week",
    ]
    duplicate_candidates[[col for col in duplicate_cols if col in duplicate_candidates.columns]].to_csv(
        audit_dir / "pfr_usage_missing_player_week_duplicate_candidates.csv",
        index=False,
    )

    safe_candidates = candidates[~candidates.duplicated("player_week", keep=False)].copy()
    candidate_cols = [
        *INSERT_IDENTITY_COLUMNS,
        *[col for col in NEW_SUPERTABLE_COLUMNS if col in safe_candidates.columns],
        *[col for col in OVERLAP_COLUMNS.values() if col in safe_candidates.columns],
    ]
    candidate_cols = [col for col in dict.fromkeys(candidate_cols) if col in safe_candidates.columns]
    safe_candidates[candidate_cols].to_parquet(
        audit_dir / "pfr_usage_missing_player_week_insert_candidates.parquet", index=False
    )
    safe_candidates[candidate_cols].head(1000).to_csv(
        audit_dir / "pfr_usage_missing_player_week_insert_candidates_sample.csv",
        index=False,
    )

    return {
        "no_identity_bridge_rows": int(len(no_id)),
        "no_identity_bridge_players": int(no_id["pfr_id"].nunique(dropna=True)),
        "context_blocked_existing_key_rows": int(len(context)),
        "candidate_missing_player_week_rows": int(len(candidates)),
        "safe_insert_candidate_rows": int(len(safe_candidates)),
        "duplicate_insert_candidate_rows": int(len(duplicate_candidates)),
    }


def write_manifest(
    audit_dir: Path, summary: pd.DataFrame, family_summary: pd.DataFrame, review: dict[str, Any]
) -> None:
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "audit_dir": str(audit_dir),
        "bucket_summary": summary.to_dict("records"),
        "family_summary": family_summary.to_dict("records"),
        "review": review,
        "artifacts": [
            "pfr_usage_gap_bucket_summary.csv",
            "pfr_usage_gap_bucket_by_family.csv",
            "pfr_usage_no_identity_bridge_player_summary.csv",
            "pfr_usage_context_blocked_game_summary.csv",
            "pfr_usage_context_blocked_rows_sample.csv",
            "pfr_usage_missing_player_week_duplicate_candidates.csv",
            "pfr_usage_missing_player_week_insert_candidates.parquet",
            "pfr_usage_missing_player_week_insert_candidates_sample.csv",
            "pfr_usage_identity_bridge_rebuilt_for_gap_buckets.parquet",
        ],
    }
    (audit_dir / "pfr_usage_gap_cleanup_manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit_dir = args.audit_dir
    if not audit_dir.exists():
        raise FileNotFoundError(audit_dir)

    stage = build_stage(audit_dir)
    summary = write_bucket_summary(stage, audit_dir)
    family_summary = write_family_summary(stage, audit_dir)
    review = write_review_artifacts(stage, audit_dir)
    write_manifest(audit_dir, summary, family_summary, review)

    print(json.dumps({"audit_dir": str(audit_dir), "rows": len(stage), **review}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
