#!/usr/bin/env python3
"""Classify remaining post-split PBP/supertable context mismatches.

This is a non-mutating audit.  It starts from the current post stat-family
split reconciliation artifacts and separates true supertable context repairs
from PBP role-side context artifacts.  The key lesson is that a defensive or
recovery role can legitimately carry the opposite side from an offensive play
row, so those rows should not be treated as missing player-week rows.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507" / "after_stat_family_split_20260509"
OUT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507" / "context_finish_20260509"
SCHEDULE_PATH = (
    REPO_ROOT
    / "fantasy_football_data"
    / "cache"
    / "pfr_excel"
    / "_master_schedule_1920_2025.parquet"
)

EPSILON = 1e-9


def norm_code(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def canonical_team_code(value: Any, year: Any) -> str:
    code = norm_code(value)
    try:
        year_int = int(float(year))
    except Exception:
        year_int = 0

    replacements = {
        "ARZ": "ARI",
        "CRD": "ARI",
        "JAC": "JAX",
        "GNB": "GB",
        "KAN": "KC",
        "NWE": "NE",
        "NOR": "NO",
        "SFO": "SF",
        "TAM": "TB",
        "WSH": "WAS",
        "SD": "LAC",
        "SDG": "LAC",
        "OAK": "LV",
        "RAI": "LV",
        "RAV": "BAL",
    }
    code = replacements.get(code, code)
    if code in {"LA", "LAR", "RAM"} or (code == "STL" and 1995 <= year_int <= 2015):
        return "LAR"
    if code in {"ARI", "ARZ", "CRD", "PHO"} or (code == "STL" and year_int <= 1987):
        return "ARI"
    if code == "TEN" or (code == "HOU" and year_int <= 1998):
        return "TEN"
    if code == "CLT" or (code == "BAL" and year_int <= 1983):
        return "IND"
    return code


def role_bucket(event_roles: Any) -> str:
    roles = str(event_roles or "")
    defense_tokens = (
        "solo_tackle",
        "assist_tackle",
        "tackle_with_assist",
        "forced_fumble",
        "interceptor",
        "sack",
        "blocked_kick",
    )
    offense_tokens = (
        "passer",
        "rusher",
        "receiver",
        "kicker",
        "punter",
        "kickoff_returner",
        "punt_returner",
        "fumbler",
    )
    has_defense = any(token in roles for token in defense_tokens) or "fumble_recovery" in roles
    has_offense = any(token in roles for token in offense_tokens)
    if has_defense and not has_offense:
        return "defense_or_recovery_only"
    if has_offense and not has_defense:
        return "offense_or_special_only"
    if has_defense and has_offense:
        return "mixed_role"
    return "other"


def load_schedule_pairs() -> set[tuple[int, int, str, str, str]]:
    if not SCHEDULE_PATH.exists():
        return set()
    sched = pd.read_parquet(SCHEDULE_PATH)
    pairs: set[tuple[int, int, str, str, str]] = set()
    for _, row in sched.iterrows():
        year = pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").iloc[0]
        week = pd.to_numeric(pd.Series([row.get("week")]), errors="coerce").iloc[0]
        if pd.isna(year) or pd.isna(week):
            continue
        phase = norm_code(row.get("season_phase") or "REG") or "REG"
        team = canonical_team_code(row.get("nfl_team"), year)
        opp = canonical_team_code(row.get("opponent_nfl_team"), year)
        if team and opp:
            pairs.add((int(year), int(week), phase, team, opp))
    return pairs


def schedule_valid(
    pairs: set[tuple[int, int, str, str, str]],
    year: Any,
    week: Any,
    season_type: Any,
    team: Any,
    opp: Any,
) -> bool:
    try:
        year_int = int(float(year))
        week_int = int(float(week))
    except Exception:
        return False
    phase = norm_code(season_type) or "REG"
    team_code = canonical_team_code(team, year_int)
    opp_code = canonical_team_code(opp, year_int)
    return (year_int, week_int, phase, team_code, opp_code) in pairs


def numeric(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(0.0, index=index)
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def diff_sample(row: pd.Series, stat_cols: list[str], limit: int = 10) -> str:
    parts: list[str] = []
    for stat in stat_cols:
        p = pd.to_numeric(pd.Series([row.get(f"{stat}_pbp")]), errors="coerce").fillna(0.0).iloc[0]
        s = pd.to_numeric(pd.Series([row.get(f"{stat}_super")]), errors="coerce").fillna(0.0).iloc[0]
        if abs(float(p) - float(s)) > EPSILON:
            parts.append(f"{stat}:{p:g}->{s:g}")
        if len(parts) >= limit:
            break
    return ";".join(parts)


def classify_row(row: pd.Series) -> str:
    role = row["role_bucket"]
    canonical_reversed = bool(row["canonical_reversed"])
    pbp_valid = bool(row["schedule_pbp_valid"])
    super_valid = bool(row["schedule_super_valid"])
    abs_delta = float(row["abs_delta"])

    if canonical_reversed and role == "defense_or_recovery_only":
        return "vouched_role_side_context_super_context_ok"
    if canonical_reversed and role == "mixed_role":
        return "mixed_role_side_context_value_review"
    if role == "offense_or_special_only" and pbp_valid and not super_valid:
        return "candidate_super_context_wrong_pbp_schedule_valid"
    if role == "offense_or_special_only" and super_valid and not pbp_valid:
        return "candidate_pbp_context_wrong_super_schedule_valid"
    if role == "offense_or_special_only" and pbp_valid and super_valid:
        return "same_player_week_multi_context_collision"
    if abs_delta <= 5 and role in {"defense_or_recovery_only", "mixed_role"}:
        return "small_idp_or_fumble_atom_value_only"
    return "manual_or_golden_sample_review"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=AUDIT_DIR)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit_dir = args.audit_dir
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    adjudicated = pd.read_csv(audit_dir / "weekly_missing_rows_adjudicated.csv")
    mismatches = adjudicated[adjudicated["row_adjudication"].eq("exact_key_context_mismatch")].copy()

    mapping = pd.read_csv(audit_dir / "pbp_to_supertable_column_mapping.csv")
    stat_cols = mapping["pbp_col"].astype(str).tolist()
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
        "event_roles",
        *stat_cols,
    ]
    super_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        *stat_cols,
    ]
    pbp = pd.read_parquet(ROLLUP_DIR / "pbp_player_week_rollup.parquet", columns=[c for c in pbp_cols if c])
    super_df = pd.read_parquet(
        audit_dir / "fly_supertable_weekly_selected_1978_2025.parquet",
        columns=[c for c in super_cols if c],
    )
    shared = [stat for stat in stat_cols if stat in pbp.columns and stat in super_df.columns]

    detail = (
        mismatches[["player_week", "row_adjudication"]]
        .merge(pbp, on="player_week", how="left", suffixes=("", "_pbp0"))
        .merge(super_df, on="player_week", how="left", suffixes=("_pbp", "_super"))
    )
    pairs = load_schedule_pairs()
    detail["role_bucket"] = detail["event_roles"].map(role_bucket)
    detail["canonical_pbp_team"] = [
        canonical_team_code(team, year) for team, year in zip(detail["nfl_team_pbp"], detail["year_pbp"], strict=False)
    ]
    detail["canonical_pbp_opp"] = [
        canonical_team_code(team, year)
        for team, year in zip(detail["opponent_nfl_team_pbp"], detail["year_pbp"], strict=False)
    ]
    detail["canonical_super_team"] = [
        canonical_team_code(team, year)
        for team, year in zip(detail["nfl_team_super"], detail["year_pbp"], strict=False)
    ]
    detail["canonical_super_opp"] = [
        canonical_team_code(team, year)
        for team, year in zip(detail["opponent_nfl_team_super"], detail["year_pbp"], strict=False)
    ]
    detail["canonical_reversed"] = detail["canonical_pbp_team"].eq(detail["canonical_super_opp"]) & detail[
        "canonical_pbp_opp"
    ].eq(detail["canonical_super_team"])
    detail["schedule_pbp_valid"] = [
        schedule_valid(pairs, y, w, st, t, o)
        for y, w, st, t, o in zip(
            detail["year_pbp"],
            detail["week_pbp"],
            detail["season_type_pbp"],
            detail["nfl_team_pbp"],
            detail["opponent_nfl_team_pbp"],
            strict=False,
        )
    ]
    detail["schedule_super_valid"] = [
        schedule_valid(pairs, y, w, st, t, o)
        for y, w, st, t, o in zip(
            detail["year_pbp"],
            detail["week_pbp"],
            detail["season_type_pbp"],
            detail["nfl_team_super"],
            detail["opponent_nfl_team_super"],
            strict=False,
        )
    ]

    active_stats = pd.Series(0, index=detail.index)
    exact_active_stats = pd.Series(0, index=detail.index)
    pbp_only_diff_stats = pd.Series(0, index=detail.index)
    super_only_diff_stats = pd.Series(0, index=detail.index)
    both_nonzero_diff_stats = pd.Series(0, index=detail.index)
    abs_delta = pd.Series(0.0, index=detail.index)
    for stat in shared:
        p = numeric(detail.get(f"{stat}_pbp"), detail.index)
        s = numeric(detail.get(f"{stat}_super"), detail.index)
        active = p.abs().gt(EPSILON) | s.abs().gt(EPSILON)
        diff = (p - s).abs().gt(EPSILON)
        active_stats += active.astype(int)
        exact_active_stats += (active & ~diff).astype(int)
        pbp_only_diff_stats += (p.abs().gt(EPSILON) & s.abs().le(EPSILON)).astype(int)
        super_only_diff_stats += (p.abs().le(EPSILON) & s.abs().gt(EPSILON)).astype(int)
        both_nonzero_diff_stats += (p.abs().gt(EPSILON) & s.abs().gt(EPSILON) & diff).astype(int)
        abs_delta += (p - s).abs()

    detail["active_stats"] = active_stats
    detail["exact_active_stats"] = exact_active_stats
    detail["pbp_only_diff_stats"] = pbp_only_diff_stats
    detail["super_only_diff_stats"] = super_only_diff_stats
    detail["both_nonzero_diff_stats"] = both_nonzero_diff_stats
    detail["abs_delta"] = abs_delta
    detail["diff_stats_sample"] = detail.apply(lambda row: diff_sample(row, shared), axis=1)
    detail["truth_bucket"] = detail.apply(classify_row, axis=1)

    out_cols = [
        "truth_bucket",
        "player_week",
        "NFL_player_id_pbp",
        "player_pbp",
        "position_pbp",
        "year_pbp",
        "week_pbp",
        "season_type_pbp",
        "nfl_team_pbp",
        "opponent_nfl_team_pbp",
        "nfl_team_super",
        "opponent_nfl_team_super",
        "role_bucket",
        "event_roles",
        "canonical_reversed",
        "schedule_pbp_valid",
        "schedule_super_valid",
        "active_stats",
        "exact_active_stats",
        "pbp_only_diff_stats",
        "super_only_diff_stats",
        "both_nonzero_diff_stats",
        "abs_delta",
        "diff_stats_sample",
    ]
    detail[out_cols].sort_values(["truth_bucket", "abs_delta"], ascending=[True, False]).to_csv(
        out_dir / "remaining_context_mismatch_classification.csv",
        index=False,
    )

    summary = (
        detail.groupby("truth_bucket", dropna=False)
        .agg(
            rows=("player_week", "size"),
            players=("player_pbp", "nunique"),
            min_year=("year_pbp", "min"),
            max_year=("year_pbp", "max"),
            abs_delta_total=("abs_delta", "sum"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    summary.to_csv(out_dir / "remaining_context_mismatch_summary.csv", index=False)

    player_summary = (
        detail.groupby(["truth_bucket", "player_pbp", "NFL_player_id_pbp", "position_pbp"], dropna=False)
        .agg(
            rows=("player_week", "size"),
            min_year=("year_pbp", "min"),
            max_year=("year_pbp", "max"),
            abs_delta_total=("abs_delta", "sum"),
            sample_event_roles=("event_roles", "first"),
            sample_pbp_team=("nfl_team_pbp", "first"),
            sample_super_team=("nfl_team_super", "first"),
        )
        .reset_index()
        .sort_values(["rows", "abs_delta_total"], ascending=[False, False])
    )
    player_summary.to_csv(out_dir / "remaining_context_mismatch_player_summary.csv", index=False)

    manifest = {
        "rows": int(len(detail)),
        "canonical_reversed_rows": int(detail["canonical_reversed"].sum()),
        "schedule_path": str(SCHEDULE_PATH),
        "audit_dir": str(audit_dir),
        "out_dir": str(out_dir),
        "summary_path": str(out_dir / "remaining_context_mismatch_summary.csv"),
        "classification_path": str(out_dir / "remaining_context_mismatch_classification.csv"),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    lines = [
        "# Remaining Context Mismatch Classification",
        "",
        "This is a non-mutating post-split audit of the 226 exact-key context mismatches.",
        "",
        f"- Rows classified: {len(detail):,}",
        f"- Canonically reversed team/opponent rows: {int(detail['canonical_reversed'].sum()):,}",
        "",
        "## Summary",
        "",
        summary.to_csv(index=False).strip(),
        "",
        "## Interpretation",
        "",
        "- `vouched_role_side_context_super_context_ok`: PBP row is a defensive/recovery event whose side is the opposite of the offensive play row; keep supertable context and treat this as a value-level atom, not a missing row.",
        "- `candidate_super_context_wrong_pbp_schedule_valid`: likely true supertable context cleanup candidates.",
        "- `mixed_role_side_context_value_review` and `same_player_week_multi_context_collision`: needs row-level review before promotion.",
        "- `manual_or_golden_sample_review`: do not promote without an external/statline check.",
        "",
    ]
    (out_dir / "REMAINING_CONTEXT_MISMATCH_CLASSIFICATION.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print(json.dumps(manifest, indent=2))
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
