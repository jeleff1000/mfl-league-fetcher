#!/usr/bin/env python3
"""
Find likely same-name ID collisions and position-incompatible stat leakage.

This is a local audit only. It reads cached PBP/supertable weekly parquet files and
writes CSV/Markdown reports into the existing PBP audit catalog.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"

KICKING_STATS = [
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
]

KICKING_POSITION_CHECK_STATS = [
    "fg_att",
    "fg_made",
    "fg_missed",
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
]

PUNTING_STATS = ["punts", "punt_yards", "punt_long", "punts_blocked"]

OFFENSE_STATS = [
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
]

RETURN_STATS = [
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
]

IDP_STATS = [
    "fum_rec",
    "fum_rec_yds",
    "fum_ret_td",
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
    "special_teams_tds",
]

ALL_STATS = list(dict.fromkeys([*OFFENSE_STATS, *KICKING_STATS, *PUNTING_STATS, *RETURN_STATS, *IDP_STATS]))
KICKING_OK_POSITIONS = {"K", "P"}
PUNTING_OK_POSITIONS = {"P", "K"}
EPSILON = 1e-9


def numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def clean_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def has_allowed_position(value: Any, allowed: set[str]) -> bool:
    if pd.isna(value):
        return False
    tokens = {part for part in re.split(r"[^A-Z0-9]+", str(value).upper()) if part}
    return bool(tokens & allowed)


def stat_family(stat: str) -> str:
    if stat in KICKING_STATS:
        return "kicking"
    if stat in PUNTING_STATS:
        return "punting"
    if stat in OFFENSE_STATS:
        return "offense"
    if stat in RETURN_STATS:
        return "return"
    if stat in IDP_STATS:
        return "idp_or_fumble"
    return "other"


def read_inputs(rollup_dir: Path, audit_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    pbp = pd.read_parquet(rollup_dir / "pbp_player_week_rollup.parquet")
    super_df = pd.read_parquet(audit_dir / "fly_supertable_weekly_selected_1978_2025.parquet")
    return pbp, super_df


def aggregate_source(df: pd.DataFrame, source: str, stats: list[str]) -> pd.DataFrame:
    present_stats = [stat for stat in stats if stat in df.columns]
    keys = ["player", "NFL_player_id", "position"]
    keep = [*keys, "year", "player_week", *present_stats]
    small = df[[col for col in keep if col in df.columns]].copy()
    small["source"] = source
    for key in keys:
        small[key] = clean_text(small[key]) if key in small.columns else pd.NA
    small = small[small["player"].notna() & small["NFL_player_id"].notna()].copy()
    small = numeric(small, present_stats)

    meta = (
        small.groupby(keys, dropna=False)
        .agg(
            source=("source", "first"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            weekly_rows=("player_week", "nunique") if "player_week" in small.columns else ("source", "size"),
        )
        .reset_index()
    )
    sums = small.groupby(keys, dropna=False)[present_stats].sum(min_count=1).reset_index()
    return meta.merge(sums, on=keys, how="left")


def position_stat_contamination(df: pd.DataFrame, source: str) -> pd.DataFrame:
    present_kicking = [stat for stat in KICKING_STATS if stat in df.columns]
    present_kicking_check = [stat for stat in KICKING_POSITION_CHECK_STATS if stat in df.columns]
    present_punting = [stat for stat in PUNTING_STATS if stat in df.columns]
    rows: list[pd.DataFrame] = []
    base_cols = [
        col
        for col in ["player", "NFL_player_id", "position", "year", "week", "season_type", "player_week"]
        if col in df.columns
    ]
    small = df[base_cols + present_kicking + present_punting].copy()
    small["source"] = source
    small["player"] = clean_text(small["player"])
    small["NFL_player_id"] = clean_text(small["NFL_player_id"])
    small["position"] = clean_text(small["position"]).str.upper()
    small = numeric(small, [*present_kicking, *present_punting])

    if present_kicking_check:
        kick_total = small[present_kicking_check].abs().sum(axis=1)
        kick_position_ok = small["position"].map(lambda value: has_allowed_position(value, KICKING_OK_POSITIONS))
        bad_kick = kick_position_ok.eq(False) & kick_total.gt(EPSILON)
        if bad_kick.any():
            part = small.loc[bad_kick].copy()
            part["stat_group"] = "kicking_on_non_k_or_p"
            rows.append(part)

    if present_punting:
        punt_total = small[present_punting].abs().sum(axis=1)
        punt_position_ok = small["position"].map(lambda value: has_allowed_position(value, PUNTING_OK_POSITIONS))
        bad_punt = punt_position_ok.eq(False) & punt_total.gt(EPSILON)
        if bad_punt.any():
            part = small.loc[bad_punt].copy()
            part["stat_group"] = "punting_on_non_p_or_k"
            rows.append(part)

    if not rows:
        return pd.DataFrame()

    suspect = pd.concat(rows, ignore_index=True)
    stat_cols = [col for col in [*present_kicking, *present_punting] if col in suspect.columns]
    group_cols = ["source", "stat_group", "player", "NFL_player_id", "position"]
    summary = (
        suspect.groupby(group_cols, dropna=False)
        .agg(
            first_year=("year", "min"),
            last_year=("year", "max"),
            weekly_rows=("player_week", "nunique") if "player_week" in suspect.columns else ("source", "size"),
        )
        .reset_index()
    )
    sums = suspect.groupby(group_cols, dropna=False)[stat_cols].sum(min_count=1).reset_index()
    return summary.merge(sums, on=group_cols, how="left")


def same_name_multi_id_summary(pbp: pd.DataFrame, super_df: pd.DataFrame, stats: list[str]) -> pd.DataFrame:
    combined = pd.concat(
        [aggregate_source(pbp, "pbp", stats), aggregate_source(super_df, "super", stats)],
        ignore_index=True,
    )
    id_counts = combined.groupby("player", dropna=False)["NFL_player_id"].nunique()
    multi_names = id_counts[id_counts.gt(1)].index
    out = combined[combined["player"].isin(multi_names)].copy()
    stat_cols = [col for col in stats if col in out.columns]
    out["_activity"] = out[stat_cols].abs().sum(axis=1)
    return out.sort_values(["player", "NFL_player_id", "source"])


def same_name_stat_collision_suspects(pbp: pd.DataFrame, super_df: pd.DataFrame, stats: list[str]) -> pd.DataFrame:
    combined = pd.concat(
        [aggregate_source(pbp, "pbp", stats), aggregate_source(super_df, "super", stats)],
        ignore_index=True,
    )
    stat_cols = [stat for stat in stats if stat in combined.columns]
    id_counts = combined.groupby("player", dropna=False)["NFL_player_id"].nunique()
    multi_names = set(id_counts[id_counts.gt(1)].index)
    multi = combined[combined["player"].isin(multi_names)].copy()
    if multi.empty:
        return pd.DataFrame()

    long = multi.melt(
        id_vars=["source", "player", "NFL_player_id", "position", "first_year", "last_year", "weekly_rows"],
        value_vars=stat_cols,
        var_name="stat",
        value_name="total",
    )
    long["total"] = pd.to_numeric(long["total"], errors="coerce").fillna(0.0)
    long = long[long["total"].abs().gt(EPSILON)].copy()
    if long.empty:
        return pd.DataFrame()

    values = (
        long.groupby(["player", "NFL_player_id", "position", "stat", "source"], dropna=False)["total"]
        .sum()
        .unstack("source", fill_value=0.0)
        .reset_index()
    )
    if "pbp" not in values.columns:
        values["pbp"] = 0.0
    if "super" not in values.columns:
        values["super"] = 0.0

    meta = (
        multi.groupby(["player", "NFL_player_id", "position"], dropna=False)
        .agg(
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            weekly_rows=("weekly_rows", "sum"),
        )
        .reset_index()
    )
    values = values.merge(meta, on=["player", "NFL_player_id", "position"], how="left")

    rows: list[dict[str, Any]] = []
    for (player, stat), group in values.groupby(["player", "stat"], dropna=False):
        if group["NFL_player_id"].nunique() < 2:
            continue
        pbp_positive = group[group["pbp"].abs().gt(EPSILON)]
        super_positive = group[group["super"].abs().gt(EPSILON)]
        if pbp_positive.empty or super_positive.empty:
            continue

        for _, row in group.iterrows():
            sibling_pbp = pbp_positive[pbp_positive["NFL_player_id"] != row["NFL_player_id"]]
            sibling_super = super_positive[super_positive["NFL_player_id"] != row["NFL_player_id"]]
            if abs(float(row["super"])) > EPSILON and abs(float(row["pbp"])) <= EPSILON and not sibling_pbp.empty:
                rows.append(
                    {
                        "direction": "super_stat_on_id_but_pbp_stat_on_sibling",
                        "player": player,
                        "NFL_player_id": row["NFL_player_id"],
                        "position": row["position"],
                        "stat_family": stat_family(str(stat)),
                        "stat": stat,
                        "pbp_total": float(row["pbp"]),
                        "super_total": float(row["super"]),
                        "first_year": row["first_year"],
                        "last_year": row["last_year"],
                        "sibling_pbp_ids": "; ".join(
                            f"{r.NFL_player_id} ({r.position}, {r.pbp:g})" for r in sibling_pbp.itertuples()
                        ),
                        "sibling_super_ids": "; ".join(
                            f"{r.NFL_player_id} ({r.position}, {r.super:g})" for r in sibling_super.itertuples()
                        ),
                    }
                )
            if abs(float(row["pbp"])) > EPSILON and abs(float(row["super"])) <= EPSILON and not sibling_super.empty:
                rows.append(
                    {
                        "direction": "pbp_stat_on_id_but_super_stat_on_sibling",
                        "player": player,
                        "NFL_player_id": row["NFL_player_id"],
                        "position": row["position"],
                        "stat_family": stat_family(str(stat)),
                        "stat": stat,
                        "pbp_total": float(row["pbp"]),
                        "super_total": float(row["super"]),
                        "first_year": row["first_year"],
                        "last_year": row["last_year"],
                        "sibling_pbp_ids": "; ".join(
                            f"{r.NFL_player_id} ({r.position}, {r.pbp:g})" for r in sibling_pbp.itertuples()
                        ),
                        "sibling_super_ids": "; ".join(
                            f"{r.NFL_player_id} ({r.position}, {r.super:g})" for r in sibling_super.itertuples()
                        ),
                    }
                )

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out["_priority"] = (
        out["stat_family"].map({"kicking": 0, "punting": 1, "offense": 2, "return": 3, "idp_or_fumble": 4}).fillna(9)
    )
    out["_abs_total"] = out[["pbp_total", "super_total"]].abs().max(axis=1)
    return out.sort_values(["_priority", "_abs_total"], ascending=[True, False]).drop(
        columns=["_priority", "_abs_total"]
    )


def write_notes(
    audit_dir: Path,
    position_suspects: pd.DataFrame,
    multi_id_summary: pd.DataFrame,
    collision_suspects: pd.DataFrame,
) -> None:
    lines = [
        "# Same-Name ID Collision Audit",
        "",
        "Local audit of cached weekly PBP and Fly supertable snapshots. This does not mutate Fly.",
        "",
        "## Counts",
        "",
        f"- Position/stat contamination suspect groups: {len(position_suspects):,}",
        f"- Same-name multi-ID source summary rows: {len(multi_id_summary):,}",
        f"- Same-name stat collision suspect rows: {len(collision_suspects):,}",
        "",
        "## Position/Stat Suspects",
        "",
        position_suspects.head(40).to_csv(index=False) if not position_suspects.empty else "None found.",
        "",
        "## Same-Name Stat Collision Suspects",
        "",
        collision_suspects.head(80).to_csv(index=False) if not collision_suspects.empty else "None found.",
        "",
    ]
    gary = (
        collision_suspects[collision_suspects["player"].eq("Gary Anderson")]
        if not collision_suspects.empty
        else pd.DataFrame()
    )
    if not gary.empty:
        lines.extend(
            [
                "## Gary Anderson Spot Check",
                "",
                gary.to_csv(index=False),
                "",
                "Gary Anderson is a confirmed collision pattern: the RB and K are distinct player IDs, and kicking stats appearing on the RB ID should be treated as identity leakage until the source join is corrected.",
                "",
            ]
        )
    (audit_dir / "SAME_NAME_ID_COLLISION_AUDIT.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollup-dir", type=Path, default=DEFAULT_ROLLUP_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.audit_dir.mkdir(parents=True, exist_ok=True)
    pbp, super_df = read_inputs(args.rollup_dir, args.audit_dir)
    stats = [stat for stat in ALL_STATS if stat in pbp.columns or stat in super_df.columns]

    position_suspects = pd.concat(
        [
            position_stat_contamination(pbp, "pbp"),
            position_stat_contamination(super_df, "super"),
        ],
        ignore_index=True,
    )
    multi_id_summary = same_name_multi_id_summary(pbp, super_df, stats)
    collision_suspects = same_name_stat_collision_suspects(pbp, super_df, stats)

    position_suspects.to_csv(args.audit_dir / "position_stat_contamination_suspects.csv", index=False)
    multi_id_summary.to_csv(args.audit_dir / "same_name_multi_id_summary.csv", index=False)
    collision_suspects.to_csv(args.audit_dir / "same_name_stat_collision_suspects.csv", index=False)
    write_notes(args.audit_dir, position_suspects, multi_id_summary, collision_suspects)

    print("position_stat_contamination_suspects", len(position_suspects))
    print("same_name_multi_id_summary", len(multi_id_summary))
    print("same_name_stat_collision_suspects", len(collision_suspects))
    print(args.audit_dir / "SAME_NAME_ID_COLLISION_AUDIT.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
