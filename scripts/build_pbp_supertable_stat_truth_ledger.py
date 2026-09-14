#!/usr/bin/env python3
"""
Build a stat-by-stat truth ledger for PBP rollups vs the supertable.

This is audit-only. It summarizes the refreshed comparison outputs and assigns a
recommended source-of-truth policy for each stat.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
DEFAULT_OUTPUT_DIR = DEFAULT_AUDIT_DIR / "stat_truth_20260508"


PBP_ONLY_SCHEMA_STATS = {
    "fumbles",
    "fumbles_lost",
    "fg_yards",
    "punts",
    "punt_yards",
    "punt_long",
    "punts_blocked",
    "kickoff_return_tds",
    "punt_return_tds",
    "def_int_ret_td",
}

OFFICIAL_PASSING = {
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
}
OFFICIAL_RUSHING = {"carries", "rushing_yards", "rushing_tds"}
OFFICIAL_RECEIVING = {"receptions", "receiving_yards", "receiving_tds"}
TARGET_STATS = {"targets"}
RETURN_STATS = {
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
}
RETURN_COUNT_STATS = {"kickoff_returns", "punt_returns"}
KICKING_COUNT_STATS = {
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_blocked",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
}
KICKING_DISTANCE_STATS = {
    "fg_yards",
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
}
PUNTING_STATS = {"punts", "punt_yards", "punt_long", "punts_blocked"}
PASS_REC_BUCKET_STATS = {
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
FUMBLE_EVENT_STATS = {
    "fumbles",
    "fumbles_lost",
    "fum_rec",
    "fum_rec_yds",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_ret_td",
}
IDP_STATS = {
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
}
SPECIAL_TEAMS_EVENT_STATS = {"special_teams_tackles_solo"}
SPECIAL_TEAMS_SPLIT_STATS = {"special_teams_tds"}


def clean_float(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def stat_family(stat: str) -> str:
    if stat in OFFICIAL_PASSING:
        return "official_passing"
    if stat in OFFICIAL_RUSHING:
        return "official_rushing"
    if stat in OFFICIAL_RECEIVING:
        return "official_receiving"
    if stat in TARGET_STATS:
        return "target_events"
    if stat in RETURN_STATS:
        return "return_events"
    if stat in KICKING_COUNT_STATS:
        return "kicking_counts"
    if stat in KICKING_DISTANCE_STATS:
        return "kicking_distance"
    if stat in PUNTING_STATS:
        return "punting"
    if stat in PASS_REC_BUCKET_STATS:
        return "pass_receive_distance_buckets"
    if stat in FUMBLE_EVENT_STATS:
        return "fumble_events"
    if stat in IDP_STATS:
        return "idp_defense"
    if stat in SPECIAL_TEAMS_EVENT_STATS:
        return "special_teams_events"
    if stat in SPECIAL_TEAMS_SPLIT_STATS:
        return "special_teams_events"
    return "other"


def truth_policy(stat: str) -> tuple[str, str, str, str]:
    family = stat_family(stat)
    if stat in PBP_ONLY_SCHEMA_STATS:
        return (
            family,
            "pbp_event_preferred",
            "high_for_1999plus_medium_pre1999",
            "Add missing supertable column and backfill from PBP/nflverse; validate parser edge cases before scoring use.",
        )
    if stat in OFFICIAL_PASSING | OFFICIAL_RUSHING | OFFICIAL_RECEIVING:
        return (
            family,
            "supertable_official_preferred",
            "high",
            "Keep supertable official box-score values on existing matched rows; use PBP for missing-row discovery, ID repair, and postseason/week alignment only.",
        )
    if stat in TARGET_STATS:
        return (
            family,
            "raw_pbp_event_preferred_parser_fix_first",
            "medium",
            "Targets are event-level and supertable has many true zero/missing target rows, but current rollup can miss incomplete targets when receiver_player_id is null; fix parser/name fallback before overlay.",
        )
    if stat in RETURN_STATS:
        if stat in RETURN_COUNT_STATS:
            return (
                family,
                "raw_pbp_event_preferred_parser_fix_first",
                "medium",
                "Return counts are event-level, but current rollup can count fair catches/returner-tagged non-returns; fix actual-return filtering before overlay.",
            )
        return (
            family,
            "pbp_event_preferred",
            "medium_high",
            "Return stats are heavily missing/zeroed in supertable for return specialists/DBs; prefer nflverse/PBP after identity and game alignment checks.",
        )
    if stat in KICKING_COUNT_STATS:
        return (
            family,
            "supertable_official_preferred",
            "high",
            "Kicking count atoms should stay official on matched rows; PBP is useful for missing rows and parser-derived distance scoring.",
        )
    if stat in KICKING_DISTANCE_STATS:
        return (
            family,
            "pbp_event_preferred",
            "high",
            "Distance atoms come from play-level field-goal strings and are missing/zero in many supertable rows; backfill/overlay from PBP after kicker-ID QA.",
        )
    if stat in PUNTING_STATS:
        return (
            family,
            "pbp_event_preferred",
            "high_for_1999plus_medium_pre1999",
            "Punting atoms are absent from the supertable schema; add columns and populate from nflverse/PBP.",
        )
    if stat in PASS_REC_BUCKET_STATS:
        return (
            family,
            "pbp_event_preferred",
            "high",
            "Distance bucket atoms are play-level derivatives; use PBP, not box-score totals, once parser no-play handling is validated.",
        )
    if stat in FUMBLE_EVENT_STATS:
        return (
            family,
            "split_decision",
            "medium",
            "Keep DST/team aggregate fumble stats separate; use PBP for player fumble/lost/recovery yards and missing schema atoms.",
        )
    if stat in IDP_STATS:
        return (
            family,
            "split_decision",
            "medium",
            "Do not compare DST team aggregates with player IDP. Use official supertable for existing 1999+ player IDP where present, PBP to fill pre-1999/player-event gaps.",
        )
    if stat in SPECIAL_TEAMS_EVENT_STATS:
        return (
            family,
            "pbp_event_preferred",
            "medium",
            "Special-teams player events are play-level parser outputs; add/backfill from PBP after tackle/TD role QA.",
        )
    if stat in SPECIAL_TEAMS_SPLIT_STATS:
        return (
            "special_teams_events",
            "split_decision",
            "medium",
            "Separate player special-teams TD events from DST/team special-teams TD aggregates before overlay.",
        )
    return (
        family,
        "manual_review",
        "unknown",
        "No default policy assigned.",
    )


def dominant_shape(row: pd.Series) -> str:
    candidates = {
        "missing_supertable_row": clean_float(row.get("missing_supertable_row_rows")),
        "super_only_row": clean_float(row.get("super_only_row_rows")),
        "pbp_nonzero_super_zero": clean_float(row.get("pbp_nonzero_super_zero_rows")),
        "super_nonzero_pbp_zero": clean_float(row.get("super_nonzero_pbp_zero_rows")),
        "both_nonzero_diff": clean_float(row.get("both_nonzero_diff_rows")),
    }
    return max(candidates.items(), key=lambda item: item[1])[0]


def load_pbp_only_totals(rollup_dir: Path, missing_stats: list[str]) -> pd.DataFrame:
    pbp = pd.read_parquet(rollup_dir / "pbp_player_week_rollup.parquet", columns=["year", *missing_stats])
    rows: list[dict[str, Any]] = []
    for stat in missing_stats:
        values = pd.to_numeric(pbp[stat], errors="coerce").fillna(0.0)
        rows.append(
            {
                "stat": stat,
                "weekly_pbp_total": float(values.sum()),
                "weekly_super_total": 0.0,
                "weekly_delta_total": float(values.sum()),
                "weekly_abs_delta_total": float(values.abs().sum()),
                "weekly_discrepant_rows": int(values.abs().gt(1e-9).sum()),
                "weekly_missing_supertable_row_rows": 0,
                "weekly_super_only_row_rows": 0,
                "weekly_pbp_nonzero_super_zero_rows": int(values.abs().gt(1e-9).sum()),
                "weekly_super_nonzero_pbp_zero_rows": 0,
                "weekly_both_nonzero_diff_rows": 0,
                "weekly_dominant_shape": "missing_schema",
                "weekly_max_abs_delta": float(values.abs().max()) if len(values) else 0.0,
            }
        )
    return pd.DataFrame(rows)


def build_ledger(rollup_dir: Path, audit_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    shared = pd.read_csv(audit_dir / "weekly_reconciled_value_discrepancy_by_stat_exact_near_id_bridge.csv")
    missing = pd.read_csv(audit_dir / "pbp_columns_missing_from_supertable_schema.csv")["pbp_col"].astype(str).tolist()
    missing_totals = load_pbp_only_totals(rollup_dir, missing)

    shared_rows: list[dict[str, Any]] = []
    for row in shared.to_dict("records"):
        stat = str(row["stat"])
        family, preferred, confidence, action = truth_policy(stat)
        shared_rows.append(
            {
                "stat": stat,
                "stat_family": family,
                "truth_source": preferred,
                "truth_confidence": confidence,
                "recommended_action": action,
                "weekly_pbp_total": clean_float(row.get("pbp_total")),
                "weekly_super_total": clean_float(row.get("super_total")),
                "weekly_delta_total": clean_float(row.get("delta_total")),
                "weekly_abs_delta_total": clean_float(row.get("abs_delta_total")),
                "weekly_discrepant_rows": int(clean_float(row.get("discrepant_rows"))),
                "weekly_missing_supertable_row_rows": int(clean_float(row.get("missing_supertable_row_rows"))),
                "weekly_super_only_row_rows": int(clean_float(row.get("super_only_row_rows"))),
                "weekly_pbp_nonzero_super_zero_rows": int(clean_float(row.get("pbp_nonzero_super_zero_rows"))),
                "weekly_super_nonzero_pbp_zero_rows": int(clean_float(row.get("super_nonzero_pbp_zero_rows"))),
                "weekly_both_nonzero_diff_rows": int(clean_float(row.get("both_nonzero_diff_rows"))),
                "weekly_dominant_shape": dominant_shape(pd.Series(row)),
                "weekly_max_abs_delta": clean_float(row.get("max_abs_delta")),
            }
        )

    missing_rows: list[dict[str, Any]] = []
    for row in missing_totals.to_dict("records"):
        stat = str(row["stat"])
        family, preferred, confidence, action = truth_policy(stat)
        missing_rows.append(
            {
                "stat": stat,
                "stat_family": family,
                "truth_source": preferred,
                "truth_confidence": confidence,
                "recommended_action": action,
                **{key: row[key] for key in row if key != "stat"},
            }
        )

    ledger = pd.concat([pd.DataFrame(shared_rows), pd.DataFrame(missing_rows)], ignore_index=True)
    source_order = {
        "split_decision": 0,
        "pbp_event_preferred": 1,
        "raw_pbp_event_preferred_parser_fix_first": 2,
        "supertable_official_preferred": 3,
        "manual_review": 4,
    }
    ledger["_source_order"] = ledger["truth_source"].map(source_order).fillna(9)
    ledger = ledger.sort_values(
        ["_source_order", "weekly_abs_delta_total", "stat"],
        ascending=[True, False, True],
    ).drop(columns=["_source_order"])

    stratified = pd.read_csv(audit_dir / "stratified_discrepancy_summary.csv")
    stratified["truth_source"] = stratified["stat"].map(lambda stat: truth_policy(str(stat))[1])
    stratified["truth_policy"] = stratified["stat"].map(lambda stat: truth_policy(str(stat))[3])
    return ledger, stratified


def write_notes(output_dir: Path, ledger: pd.DataFrame, stratified: pd.DataFrame) -> None:
    counts = (
        ledger.groupby(["truth_source", "stat_family"], dropna=False)
        .size()
        .reset_index(name="stats")
        .sort_values(["truth_source", "stats"], ascending=[True, False])
    )
    top_pbp = ledger[ledger["truth_source"].eq("pbp_event_preferred")].head(30)
    top_super = ledger[ledger["truth_source"].eq("supertable_official_preferred")].head(30)
    split = ledger[ledger["truth_source"].eq("split_decision")]
    top_strata = stratified.sort_values("abs_delta_total", ascending=False).head(40)
    target_gap_path = output_dir / "nflverse_target_receiver_id_gap_by_season.csv"
    target_gap = pd.read_csv(target_gap_path) if target_gap_path.exists() else pd.DataFrame()
    target_gap_total = (
        int(target_gap["target_name_missing_receiver_id_rows"].sum())
        if not target_gap.empty and "target_name_missing_receiver_id_rows" in target_gap.columns
        else 0
    )
    punt_fc_path = output_dir / "nflverse_punt_returner_fair_catch_by_season.csv"
    punt_fc = pd.read_csv(punt_fc_path) if punt_fc_path.exists() else pd.DataFrame()
    punt_fc_total = (
        int(punt_fc["punt_returner_fair_catch_rows"].sum())
        if not punt_fc.empty and "punt_returner_fair_catch_rows" in punt_fc.columns
        else 0
    )
    ko_nonreturn_path = output_dir / "nflverse_kickoff_returner_nonreturn_by_season.csv"
    ko_nonreturn = pd.read_csv(ko_nonreturn_path) if ko_nonreturn_path.exists() else pd.DataFrame()
    ko_nonreturn_total = (
        int(ko_nonreturn["kickoff_returner_tagged_nonreturn_zero_yard_rows"].sum())
        if not ko_nonreturn.empty and "kickoff_returner_tagged_nonreturn_zero_yard_rows" in ko_nonreturn.columns
        else 0
    )

    lines = [
        "# PBP vs Supertable Stat Truth Ledger",
        "",
        f"Generated UTC: {datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "",
        "This ledger is audit-only. It adjudicates source preference by stat; it does not mutate Fly.",
        "",
        "## Source Counts",
        "",
        counts.to_csv(index=False),
        "",
        "## Main Truth Calls",
        "",
        "- Supertable wins for matched official passing/rushing/receiving/kicking count atoms.",
        "- PBP/nflverse wins for targets, return events, field-goal distance atoms, pass/receive distance buckets, punting, missing schema atoms, and special-teams player events.",
        "- Targets specifically need a parser fix first: raw nflverse has incomplete-pass receiver names even when receiver_player_id is null, and the current rollup can miss those targets.",
        "- Return counts also need a parser fix first: nflverse punt_returner_player_id includes fair catches, so current punt_returns can overcount unless actual-return filtering is applied.",
        "- IDP/fumble atoms are split: separate DST team aggregates from player rows before any overwrite.",
        "- Row identity, same-name collisions, and postseason/week alignment must be fixed before treating missing rows as stat truth.",
        "",
        "## Target Parser Gap",
        "",
        f"- Raw nflverse pass attempts with receiver text but missing receiver_player_id: {target_gap_total:,}",
        "- This is concentrated in 2003-2008 and means current target rollups should not be overlaid until receiver-name fallback maps those incomplete targets.",
        "",
        target_gap.to_csv(index=False) if not target_gap.empty else "Target receiver-ID gap audit not found.",
        "",
        "## Punt Return Parser Gap",
        "",
        f"- Raw nflverse punt rows with returner tagged on fair catches: {punt_fc_total:,}",
        "- These should not count as punt returns. Current punt-return count overlays need actual-return filtering first.",
        "",
        punt_fc.to_csv(index=False) if not punt_fc.empty else "Punt returner fair-catch audit not found.",
        "",
        "## Kickoff Return Parser Gap",
        "",
        f"- Raw nflverse kickoff rows with returner tagged on zero-yard non-return descriptions: {ko_nonreturn_total:,}",
        "- This is much smaller than the punt-return issue but still supports fixing return-count filters before overlay.",
        "",
        ko_nonreturn.to_csv(index=False) if not ko_nonreturn.empty else "Kickoff returner non-return audit not found.",
        "",
        "## PBP/Event Preferred Stats",
        "",
        pd.concat(
            [
                ledger[ledger["truth_source"].eq("pbp_event_preferred")],
                ledger[ledger["truth_source"].eq("raw_pbp_event_preferred_parser_fix_first")],
            ],
            ignore_index=True,
        )
        .head(30)[
            [
                "stat",
                "stat_family",
                "truth_confidence",
                "weekly_pbp_total",
                "weekly_super_total",
                "weekly_abs_delta_total",
                "weekly_dominant_shape",
                "recommended_action",
            ]
        ]
        .to_csv(index=False),
        "",
        "## Supertable Official Preferred Stats",
        "",
        top_super[
            [
                "stat",
                "stat_family",
                "truth_confidence",
                "weekly_pbp_total",
                "weekly_super_total",
                "weekly_abs_delta_total",
                "weekly_dominant_shape",
                "recommended_action",
            ]
        ].to_csv(index=False),
        "",
        "## Split Decision Stats",
        "",
        split[
            [
                "stat",
                "stat_family",
                "truth_confidence",
                "weekly_pbp_total",
                "weekly_super_total",
                "weekly_abs_delta_total",
                "weekly_dominant_shape",
                "recommended_action",
            ]
        ].to_csv(index=False),
        "",
        "## Biggest Discrepancy Strata",
        "",
        top_strata[
            [
                "era",
                "position_group",
                "stat",
                "stat_family",
                "discrepancy_type",
                "likely_reason",
                "rows",
                "pbp_total",
                "super_total",
                "delta_total",
                "abs_delta_total",
                "truth_source",
            ]
        ].to_csv(index=False),
        "",
    ]
    (output_dir / "STAT_TRUTH_LEDGER.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollup-dir", type=Path, default=DEFAULT_ROLLUP_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    ledger, stratified = build_ledger(args.rollup_dir, args.audit_dir)
    ledger.to_csv(args.output_dir / "stat_truth_ledger.csv", index=False)
    stratified.to_csv(args.output_dir / "stat_truth_by_discrepancy_stratum.csv", index=False)
    write_notes(args.output_dir, ledger, stratified)
    print(ledger.groupby("truth_source").size().reset_index(name="stats").to_string(index=False))
    print(args.output_dir / "STAT_TRUTH_LEDGER.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
