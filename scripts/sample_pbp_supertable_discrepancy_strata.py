#!/usr/bin/env python3
"""
Stratified sampler for PBP vs supertable weekly discrepancies.

Uses the local reconciliation artifacts and emits:
  - summaries by era/position group/stat family/stat/discrepancy type/reason
  - representative samples per stratum
  - "juicy" largest examples for human review
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from reconcile_pbp_supertable_weekly_ids import (
    DEFAULT_AUDIT_DIR,
    DEFAULT_ROLLUP_DIR,
    build_reconciled_merge,
    numeric,
    read_inputs,
)


EPSILON = 1e-9

OFFICIAL_PASS = {
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
}
OFFICIAL_RUSH = {"carries", "rushing_yards", "rushing_tds"}
OFFICIAL_REC = {"targets", "receptions", "receiving_yards", "receiving_tds"}
OFFICIAL_KICK = {
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_blocked",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
}
OFFICIAL_RETURN = {
    "kickoff_returns",
    "kickoff_return_yards",
    "punt_returns",
    "punt_return_yards",
}
DERIVED_PASS_REC = {
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
}
DERIVED_KICK = {
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
}
DERIVED_FUMBLE = {
    "fum_rec_yds",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_ret_td",
}
IDP = {
    "fum_rec",
    "def_sacks",
    "def_interceptions",
    "def_interception_yards",
    "def_fumbles_forced",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_tackles_for_loss",
    "def_pass_defended",
    "def_qb_hits",
    "def_safeties",
    "def_blk_kick",
}
SPECIAL_TEAMS = {"special_teams_tackles_solo", "special_teams_tds"}


def coalesce(df: pd.DataFrame, name: str) -> pd.Series:
    pbp = f"{name}_pbp"
    sup = f"{name}_super"
    if pbp in df.columns and sup in df.columns:
        return df[pbp].combine_first(df[sup])
    if pbp in df.columns:
        return df[pbp]
    if sup in df.columns:
        return df[sup]
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index)


def position_group(position: Any) -> str:
    pos = "" if pd.isna(position) else str(position).upper().strip()
    if pos == "DEF":
        return "DST"
    if pos in {"QB"}:
        return "QB"
    if pos in {"RB", "FB"}:
        return "RB"
    if pos in {"WR"}:
        return "WR"
    if pos in {"TE"}:
        return "TE"
    if pos in {"K"}:
        return "K"
    if pos in {"P"}:
        return "P"
    if pos in {"DB", "CB", "S", "FS", "SS"}:
        return "DB"
    if pos in {"LB", "ILB", "OLB"}:
        return "LB"
    if pos in {"DL", "DE", "DT", "NT"}:
        return "DL"
    if pos in {"C", "G", "T", "OT", "OG", "OL", "LS"}:
        return "OL"
    if not pos:
        return "UNKNOWN"
    return "OTHER"


def era(year: Any) -> str:
    try:
        y = int(float(year))
    except (TypeError, ValueError):
        return "UNKNOWN"
    if 1978 <= y <= 1993:
        return "1978-1993"
    if 1994 <= y <= 1998:
        return "1994-1998"
    if 1999 <= y <= 2025:
        return "1999-2025"
    return "OTHER"


def stat_family(stat: str) -> str:
    if stat in OFFICIAL_PASS:
        return "official_passing"
    if stat in OFFICIAL_RUSH:
        return "official_rushing"
    if stat in OFFICIAL_REC:
        return "official_receiving"
    if stat in OFFICIAL_KICK:
        return "official_kicking"
    if stat in OFFICIAL_RETURN:
        return "official_return"
    if stat in DERIVED_PASS_REC:
        return "derived_pass_rec_bucket"
    if stat in DERIVED_KICK:
        return "derived_kicking"
    if stat in DERIVED_FUMBLE:
        return "derived_fumble"
    if stat in IDP:
        return "idp_defense"
    if stat in SPECIAL_TEAMS:
        return "special_teams"
    return "other"


def discrepancy_type(row_status: str, p: pd.Series, s: pd.Series) -> pd.Series:
    p_nz = p.abs().gt(EPSILON)
    s_nz = s.abs().gt(EPSILON)
    out = pd.Series("both_nonzero_diff", index=p.index, dtype="object")
    out.loc[row_status == "left_only"] = "missing_supertable_row"
    out.loc[row_status == "right_only"] = "super_only_row"
    out.loc[(row_status == "both") & p_nz & ~s_nz] = "pbp_nonzero_super_zero"
    out.loc[(row_status == "both") & ~p_nz & s_nz] = "super_nonzero_pbp_zero"
    out.loc[(row_status == "both") & p_nz & s_nz] = "both_nonzero_diff"
    return out


def likely_reason(row: pd.Series) -> str:
    stat = str(row["stat"])
    family = str(row["stat_family"])
    pos_group = str(row["position_group"])
    disc = str(row["discrepancy_type"])
    match_type = str(row["reconcile_match_type"])
    row_status = str(row["row_status"])
    year = row.get("year")
    week = row.get("week")
    season_type = str(row.get("season_type") or "")
    try:
        y = int(float(year))
    except (TypeError, ValueError):
        y = None
    try:
        w = int(float(week))
    except (TypeError, ValueError):
        w = None

    if match_type.startswith("alt_key_"):
        return "id drift paired by same name/year/week/position; inspect remaining stat deltas"
    if pos_group == "DST":
        return "team DEF aggregate mixed with player-event PBP; audit DST separately"
    if y is not None and y < 1999 and (season_type == "POST" or (w is not None and w >= 18)):
        if family.startswith("official_"):
            return "pre-1999 postseason coverage/gap; validate game/date before choosing source"
    if family.startswith("official_"):
        if disc in {"both_nonzero_diff", "pbp_nonzero_super_zero", "super_nonzero_pbp_zero"}:
            return "official box-score atom; prefer supertable unless game alignment is wrong"
        if disc in {"missing_supertable_row", "super_only_row"}:
            return "official atom on unmatched row; resolve ID/game/week before choosing source"
    if family in {"derived_pass_rec_bucket", "derived_kicking", "derived_fumble", "special_teams"}:
        if disc == "pbp_nonzero_super_zero":
            return "play-level derived atom absent/zero in supertable"
        if disc == "missing_supertable_row":
            return "PBP has derived atom on missing player-week row"
        return "derived play-level atom; PBP likely source after parser QA"
    if family == "idp_defense":
        if y is not None and y < 1999:
            return "pre-1999 player IDP gap or sparse official defensive source"
        return "1999+ player IDP exists; audit player deltas separately from DST rows"
    return "manual review"


def sample_groups(df: pd.DataFrame, group_cols: list[str], n: int) -> pd.DataFrame:
    samples = []
    for _, group in df.groupby(group_cols, dropna=False, sort=False):
        top = group.sort_values("abs_delta", ascending=False).head(n)
        samples.append(top)
    if not samples:
        return pd.DataFrame()
    return pd.concat(samples, ignore_index=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollup-dir", type=Path, default=DEFAULT_ROLLUP_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--per-stratum", type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pbp, super_df, shared = read_inputs(args.rollup_dir, args.audit_dir)
    pairs = pd.read_csv(args.audit_dir / "weekly_altkey_id_reconciliation_pairs.csv")
    all_qualities = {"exact_vector", "near_vector_le_5", "same_name_week_with_stat_overlap", "review"}
    merged = build_reconciled_merge(pbp, super_df, shared, pairs, all_qualities)

    base = pd.DataFrame(
        {
            "player_week": coalesce(merged, "player_week"),
            "NFL_player_id": coalesce(merged, "NFL_player_id"),
            "player": coalesce(merged, "player"),
            "position": coalesce(merged, "position"),
            "year": coalesce(merged, "year"),
            "week": coalesce(merged, "week"),
            "season_type": coalesce(merged, "season_type"),
            "row_status": merged["_merge"].astype(str),
            "reconcile_match_type": merged["reconcile_match_type"].astype(str),
        }
    )
    base["era"] = base["year"].map(era)
    base["position_group"] = base["position"].map(position_group)

    summaries: list[pd.DataFrame] = []
    samples: list[pd.DataFrame] = []
    juicy: list[pd.DataFrame] = []

    for stat in shared:
        p = numeric(merged.get(f"{stat}_pbp"), merged.index)
        s = numeric(merged.get(f"{stat}_super"), merged.index)
        d = p - s
        discrep = d.abs().gt(EPSILON)
        if not discrep.any():
            continue
        detail = base.loc[discrep].copy()
        detail["stat"] = stat
        detail["stat_family"] = stat_family(stat)
        detail["pbp_value"] = p.loc[discrep].to_numpy()
        detail["super_value"] = s.loc[discrep].to_numpy()
        detail["delta"] = d.loc[discrep].to_numpy()
        detail["abs_delta"] = d.loc[discrep].abs().to_numpy()
        detail["discrepancy_type"] = discrepancy_type(
            detail["row_status"],
            detail["pbp_value"],
            detail["super_value"],
        ).to_numpy()
        detail["likely_reason"] = detail.apply(likely_reason, axis=1)

        group_cols = [
            "era",
            "position_group",
            "stat_family",
            "stat",
            "discrepancy_type",
            "likely_reason",
        ]
        summary = (
            detail.groupby(group_cols, dropna=False)
            .agg(
                rows=("stat", "size"),
                pbp_total=("pbp_value", "sum"),
                super_total=("super_value", "sum"),
                delta_total=("delta", "sum"),
                abs_delta_total=("abs_delta", "sum"),
                max_abs_delta=("abs_delta", "max"),
            )
            .reset_index()
        )
        summaries.append(summary)
        samples.append(sample_groups(detail, group_cols, args.per_stratum))
        juicy.append(detail.sort_values("abs_delta", ascending=False).head(100))

    summary_all = pd.concat(summaries, ignore_index=True).sort_values("abs_delta_total", ascending=False)
    sample_all = pd.concat(samples, ignore_index=True).sort_values(
        ["likely_reason", "abs_delta"], ascending=[True, False]
    )
    juicy_all = pd.concat(juicy, ignore_index=True).sort_values("abs_delta", ascending=False)

    summary_all.to_csv(args.audit_dir / "stratified_discrepancy_summary.csv", index=False)
    sample_all.to_csv(args.audit_dir / "stratified_discrepancy_samples.csv", index=False)
    juicy_all.head(5000).to_csv(args.audit_dir / "stratified_discrepancy_juicy_examples.csv", index=False)

    notes = [
        "# Stratified Discrepancy Samples",
        "",
        "Generated from weekly reconciled PBP/supertable rows after unique same-name/year/week/position ID pairing.",
        "",
        "## Biggest Strata",
        "",
        summary_all.head(60).to_csv(index=False),
        "",
        "## Juicy Examples",
        "",
        juicy_all.head(60).to_csv(index=False),
        "",
    ]
    (args.audit_dir / "STRATIFIED_DISCREPANCY_SAMPLES.md").write_text("\n".join(notes), encoding="utf-8")
    print(summary_all.head(40).to_string(index=False))
    print(args.audit_dir / "STRATIFIED_DISCREPANCY_SAMPLES.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
