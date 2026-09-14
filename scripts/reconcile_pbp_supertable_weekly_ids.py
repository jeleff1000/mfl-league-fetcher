#!/usr/bin/env python3
"""
Build local ID reconciliation candidates and rerun weekly value deltas after pairing
obvious same-player rows whose player_week/NFL_player_id disagree.

This script does not query or mutate Fly. It uses the local outputs from
compare_pbp_rollups_to_fly_supertable.py.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
EPSILON = 1e-9


def norm_name(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.lower().str.replace(r"[^a-z0-9]+", "", regex=True)


def norm_code(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series("", index=index)
    return series.fillna("").astype(str).str.strip().str.upper()


def canonical_team_code(series: pd.Series | None, years: pd.Series | None, index: pd.Index) -> pd.Series:
    codes = norm_code(series, index)
    year_values = pd.to_numeric(years, errors="coerce") if years is not None else pd.Series(pd.NA, index=index)
    out = codes.replace(
        {
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
    )

    rams = codes.isin(["LA", "LAR", "RAM"]) | (codes.eq("STL") & year_values.ge(1995) & year_values.le(2015))
    out.loc[rams] = "LAR"

    cardinals = codes.isin(["ARI", "ARZ", "CRD", "PHO"]) | (codes.eq("STL") & year_values.le(1987))
    out.loc[cardinals] = "ARI"

    oilers_titans = codes.eq("TEN") | (codes.eq("HOU") & year_values.le(1998))
    out.loc[oilers_titans] = "TEN"

    colts = codes.eq("CLT") | (codes.eq("BAL") & year_values.le(1983))
    out.loc[colts] = "IND"
    return out


def numeric(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(0.0, index=index)
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def add_alt_key(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["_player_norm"] = norm_name(out["player"])
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["week"] = pd.to_numeric(out["week"], errors="coerce").astype("Int64")
    out["_position"] = out["position"].fillna("").astype(str)
    out["_nfl_team_norm"] = canonical_team_code(out.get("nfl_team"), out["year"], out.index)
    out["_opponent_nfl_team_norm"] = canonical_team_code(out.get("opponent_nfl_team"), out["year"], out.index)
    out["_season_type_norm"] = norm_code(out.get("season_type"), out.index)
    out["_context_complete"] = (
        out["_nfl_team_norm"].ne("") & out["_opponent_nfl_team_norm"].ne("") & out["_season_type_norm"].ne("")
    )
    out["_alt_key"] = (
        out["_player_norm"]
        + "|"
        + out["year"].astype(str)
        + "|"
        + out["week"].astype(str)
        + "|"
        + out["_season_type_norm"]
        + "|"
        + out["_position"]
        + "|"
        + out["_nfl_team_norm"]
        + "|"
        + out["_opponent_nfl_team_norm"]
    )
    return out


def read_inputs(rollup_dir: Path, audit_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    mapping = pd.read_csv(audit_dir / "pbp_to_supertable_column_mapping.csv")
    stat_cols = mapping["pbp_col"].astype(str).tolist()

    pbp = pd.read_parquet(rollup_dir / "pbp_player_week_rollup.parquet")
    super_df = pd.read_parquet(audit_dir / "fly_supertable_weekly_selected_1978_2025.parquet")
    shared = [col for col in stat_cols if col in pbp.columns and col in super_df.columns]

    pbp = add_alt_key(pbp)
    super_df = add_alt_key(super_df)
    return pbp, super_df, shared


def row_activity(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    activity = pd.Series(0.0, index=df.index)
    for col in cols:
        if col in df.columns:
            activity += numeric(df[col], df.index).abs()
    return activity


def build_alt_pairs(pbp: pd.DataFrame, super_df: pd.DataFrame, stat_cols: list[str]) -> pd.DataFrame:
    pbp_keys = set(pbp["player_week"].dropna().astype(str))
    super_keys = set(super_df["player_week"].dropna().astype(str))

    pbp_unmatched = pbp[~pbp["player_week"].astype(str).isin(super_keys) & pbp["_context_complete"]].copy()
    super_unmatched = super_df[
        ~super_df["player_week"].astype(str).isin(pbp_keys) & super_df["_context_complete"]
    ].copy()

    pbp_unmatched["_n_alt_pbp"] = pbp_unmatched.groupby("_alt_key")["player_week"].transform("count")
    super_unmatched["_n_alt_super"] = super_unmatched.groupby("_alt_key")["player_week"].transform("count")
    pbp_unique = pbp_unmatched[pbp_unmatched["_n_alt_pbp"].eq(1)].copy()
    super_unique = super_unmatched[super_unmatched["_n_alt_super"].eq(1)].copy()

    left_cols = [
        "player_week",
        "NFL_player_id",
        "pbp_player_id",
        "pbp_player_name",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "_alt_key",
        *stat_cols,
    ]
    right_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "_alt_key",
        *stat_cols,
    ]
    left_cols = [col for col in left_cols if col in pbp_unique.columns]
    right_cols = [col for col in right_cols if col in super_unique.columns]
    pairs = pbp_unique[left_cols].merge(
        super_unique[right_cols],
        on="_alt_key",
        how="inner",
        suffixes=("_pbp", "_super"),
    )
    if pairs.empty:
        return pairs

    abs_delta = pd.Series(0.0, index=pairs.index)
    signed_delta = pd.Series(0.0, index=pairs.index)
    active_stats = pd.Series(0, index=pairs.index)
    exact_stat_matches = pd.Series(0, index=pairs.index)
    pbp_nonzero_stats = pd.Series(0, index=pairs.index)
    super_nonzero_stats = pd.Series(0, index=pairs.index)

    for stat in stat_cols:
        p = numeric(pairs.get(f"{stat}_pbp"), pairs.index)
        s = numeric(pairs.get(f"{stat}_super"), pairs.index)
        d = p - s
        active = p.abs().gt(EPSILON) | s.abs().gt(EPSILON)
        abs_delta += d.abs()
        signed_delta += d
        active_stats += active.astype(int)
        exact_stat_matches += (active & d.abs().le(EPSILON)).astype(int)
        pbp_nonzero_stats += p.abs().gt(EPSILON).astype(int)
        super_nonzero_stats += s.abs().gt(EPSILON).astype(int)

    pairs["active_stats"] = active_stats
    pairs["exact_stat_matches"] = exact_stat_matches
    pairs["pbp_nonzero_stats"] = pbp_nonzero_stats
    pairs["super_nonzero_stats"] = super_nonzero_stats
    pairs["value_abs_delta_total"] = abs_delta
    pairs["value_signed_delta_total"] = signed_delta
    pairs["id_pair_quality"] = "review"
    pairs.loc[(active_stats.gt(0)) & abs_delta.le(EPSILON), "id_pair_quality"] = "exact_vector"
    pairs.loc[
        (active_stats.gt(0)) & abs_delta.gt(EPSILON) & abs_delta.le(5),
        "id_pair_quality",
    ] = "near_vector_le_5"
    pairs.loc[
        (active_stats.gt(0)) & exact_stat_matches.ge(1) & abs_delta.gt(5),
        "id_pair_quality",
    ] = "same_name_week_with_stat_overlap"
    return pairs


def aggregate_player_bridge(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pd.DataFrame()
    work = pairs.copy()
    work["super_NFL_player_id"] = work.get("NFL_player_id_super", "")
    work["pbp_NFL_player_id"] = work.get("NFL_player_id_pbp", "")
    work["super_player_week"] = work.get("player_week_super", "")
    work["pbp_player_week"] = work.get("player_week_pbp", "")
    work["player"] = work.get("player_pbp", work.get("player_super", ""))
    work["position"] = work.get("position_pbp", work.get("position_super", ""))
    work["nfl_team"] = work.get("nfl_team_pbp", work.get("nfl_team_super", ""))
    work["opponent_nfl_team"] = work.get("opponent_nfl_team_pbp", work.get("opponent_nfl_team_super", ""))
    grouped = (
        work.groupby(
            [
                "super_NFL_player_id",
                "pbp_NFL_player_id",
                "player",
                "position",
                "nfl_team",
                "opponent_nfl_team",
            ],
            dropna=False,
        )
        .agg(
            paired_weeks=("pbp_player_week", "count"),
            min_year=("year_pbp", "min"),
            max_year=("year_pbp", "max"),
            exact_vector_weeks=("id_pair_quality", lambda s: int((s == "exact_vector").sum())),
            near_vector_weeks=("id_pair_quality", lambda s: int((s == "near_vector_le_5").sum())),
            stat_overlap_weeks=("id_pair_quality", lambda s: int((s == "same_name_week_with_stat_overlap").sum())),
            total_abs_delta=("value_abs_delta_total", "sum"),
            total_signed_delta=("value_signed_delta_total", "sum"),
            sample_super_player_week=("super_player_week", "first"),
            sample_pbp_player_week=("pbp_player_week", "first"),
        )
        .reset_index()
    )
    grouped["bridge_confidence"] = "review"
    grouped.loc[
        grouped["paired_weeks"].ge(2)
        & grouped["total_abs_delta"].le(10)
        & grouped["pbp_NFL_player_id"].astype(str).ne("")
        & grouped["super_NFL_player_id"].astype(str).ne(grouped["pbp_NFL_player_id"].astype(str)),
        "bridge_confidence",
    ] = "high"
    grouped.loc[
        grouped["exact_vector_weeks"].ge(1)
        & grouped["pbp_NFL_player_id"].astype(str).ne("")
        & grouped["super_NFL_player_id"].astype(str).ne(grouped["pbp_NFL_player_id"].astype(str)),
        "bridge_confidence",
    ] = "high"
    return grouped.sort_values(
        ["bridge_confidence", "paired_weeks", "exact_vector_weeks"],
        ascending=[True, False, False],
    )


def write_missing_row_adjudication(
    pbp: pd.DataFrame,
    super_df: pd.DataFrame,
    pairs: pd.DataFrame,
    stat_cols: list[str],
    audit_dir: Path,
) -> pd.DataFrame:
    super_lookup_cols = [
        col
        for col in [
            "player_week",
            "NFL_player_id",
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
        ]
        if col in super_df.columns
    ]
    merged = pbp.merge(
        super_df[super_lookup_cols].drop_duplicates(subset=["player_week"], keep="first"),
        on="player_week",
        how="left",
        suffixes=("_pbp", "_super"),
        indicator=True,
    )

    active = row_activity(merged, [col for col in stat_cols if col in merged.columns]).gt(EPSILON)
    fly_present = merged["_merge"].eq("both")
    pbp_team = canonical_team_code(merged.get("nfl_team_pbp"), merged.get("year_pbp", merged.get("year")), merged.index)
    pbp_opp = canonical_team_code(
        merged.get("opponent_nfl_team_pbp"),
        merged.get("year_pbp", merged.get("year")),
        merged.index,
    )
    super_team = canonical_team_code(
        merged.get("nfl_team_super"),
        merged.get("year_pbp", merged.get("year")),
        merged.index,
    )
    super_opp = canonical_team_code(
        merged.get("opponent_nfl_team_super"),
        merged.get("year_pbp", merged.get("year")),
        merged.index,
    )
    context_known = pbp_team.ne("") & pbp_opp.ne("") & super_team.ne("") & super_opp.ne("")
    context_match = fly_present & context_known & pbp_team.eq(super_team) & pbp_opp.eq(super_opp)
    needs_adjudication = active & (~fly_present | ~context_match)

    pair_quality = {}
    if not pairs.empty and "player_week_pbp" in pairs.columns:
        quality_order = {
            "exact_vector": 0,
            "near_vector_le_5": 1,
            "same_name_week_with_stat_overlap": 2,
            "review": 3,
        }
        ranked = pairs.copy()
        ranked["_quality_rank"] = ranked["id_pair_quality"].map(quality_order).fillna(99)
        ranked = ranked.sort_values(["player_week_pbp", "_quality_rank", "value_abs_delta_total"])
        pair_quality = (
            ranked.drop_duplicates("player_week_pbp").set_index("player_week_pbp")["id_pair_quality"].to_dict()
        )

    out_cols = [
        col
        for col in [
            "NFL_player_id_pbp",
            "player_week",
            "player_pbp",
            "position_pbp",
            "nfl_team_pbp",
            "opponent_nfl_team_pbp",
            "year_pbp",
            "week_pbp",
            "season_type_pbp",
            "pbp_player_id",
            "pbp_player_name",
            "event_roles",
            "pbp_source_systems",
            "player_id_namespaces",
            "NFL_player_id_super",
            "player_super",
            "position_super",
            "nfl_team_super",
            "opponent_nfl_team_super",
        ]
        if col in merged.columns
    ]
    adjudicated = merged.loc[needs_adjudication, out_cols].copy()
    adjudicated["row_adjudication"] = "true_key_missing_unbridged"
    adjudicated.loc[
        fly_present[needs_adjudication].to_numpy() & ~context_known[needs_adjudication].to_numpy(), "row_adjudication"
    ] = "exact_key_context_unknown"
    adjudicated.loc[
        fly_present[needs_adjudication].to_numpy()
        & context_known[needs_adjudication].to_numpy()
        & ~context_match[needs_adjudication].to_numpy(),
        "row_adjudication",
    ] = "exact_key_context_mismatch"
    qualities = adjudicated["player_week"].astype(str).map(pair_quality)
    adjudicated.loc[
        adjudicated["row_adjudication"].eq("true_key_missing_unbridged")
        & qualities.isin(["exact_vector", "near_vector_le_5"]),
        "row_adjudication",
    ] = "id_bridge_exact_near"
    adjudicated.loc[
        adjudicated["row_adjudication"].eq("true_key_missing_unbridged") & qualities.notna(),
        "row_adjudication",
    ] = "id_bridge_diagnostic"
    adjudicated["id_pair_quality"] = qualities
    adjudicated["canonical_nfl_team_pbp"] = pbp_team[needs_adjudication].to_numpy()
    adjudicated["canonical_opponent_nfl_team_pbp"] = pbp_opp[needs_adjudication].to_numpy()
    adjudicated["canonical_nfl_team_super"] = super_team[needs_adjudication].to_numpy()
    adjudicated["canonical_opponent_nfl_team_super"] = super_opp[needs_adjudication].to_numpy()
    adjudicated.to_csv(audit_dir / "weekly_missing_rows_adjudicated.csv", index=False)

    summary = (
        adjudicated.groupby("row_adjudication", dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )
    summary.to_csv(audit_dir / "weekly_missing_rows_adjudicated_summary.csv", index=False)
    return summary


def build_reconciled_merge(
    pbp: pd.DataFrame,
    super_df: pd.DataFrame,
    stat_cols: list[str],
    pairs: pd.DataFrame,
    pair_qualities: set[str],
) -> pd.DataFrame:
    pair_use = pairs[pairs["id_pair_quality"].isin(pair_qualities)].copy() if not pairs.empty else pairs
    paired_pbp = set(pair_use.get("player_week_pbp", pd.Series(dtype=str)).dropna().astype(str))
    paired_super = set(pair_use.get("player_week_super", pd.Series(dtype=str)).dropna().astype(str))

    pbp_cols = [
        "player_week",
        "NFL_player_id",
        "pbp_player_id",
        "pbp_player_name",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "event_roles",
        "pbp_source_systems",
        "player_id_namespaces",
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
    pbp_cols = [col for col in pbp_cols if col in pbp.columns]
    super_cols = [col for col in super_cols if col in super_df.columns]

    # Direct player_week matches.
    direct_pbp = pbp[~pbp["player_week"].astype(str).isin(paired_pbp)][pbp_cols].copy()
    direct_super = super_df[~super_df["player_week"].astype(str).isin(paired_super)][super_cols].copy()
    direct = direct_pbp.merge(
        direct_super,
        on="player_week",
        how="outer",
        suffixes=("_pbp", "_super"),
        indicator=True,
    )
    direct["reconcile_match_type"] = direct["_merge"].astype(str)

    if pair_use.empty:
        return direct

    pbp_pair = pbp[pbp_cols].copy()
    pbp_pair = pbp_pair.rename(
        columns={col: ("player_week_pbp_key" if col == "player_week" else f"{col}_pbp") for col in pbp_pair.columns}
    )
    super_pair = super_df[super_cols].copy()
    super_pair = super_pair.rename(
        columns={
            col: ("player_week_super_key" if col == "player_week" else f"{col}_super") for col in super_pair.columns
        }
    )
    paired = (
        pair_use[["player_week_pbp", "player_week_super", "id_pair_quality"]]
        .merge(
            pbp_pair,
            left_on="player_week_pbp",
            right_on="player_week_pbp_key",
            how="left",
        )
        .merge(
            super_pair,
            left_on="player_week_super",
            right_on="player_week_super_key",
            how="left",
        )
    )
    paired["_merge"] = "both"
    paired["reconcile_match_type"] = "alt_key_" + paired["id_pair_quality"].astype(str)
    if "player_week_pbp" in paired.columns:
        paired["player_week"] = paired["player_week_pbp"]
    return pd.concat([direct, paired], ignore_index=True, sort=False)


def coalesce(df: pd.DataFrame, name: str) -> pd.Series:
    a = f"{name}_pbp"
    b = f"{name}_super"
    if a in df.columns and b in df.columns:
        return df[a].combine_first(df[b])
    if a in df.columns:
        return df[a]
    if b in df.columns:
        return df[b]
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index)


def summarize_reconciled(
    merged: pd.DataFrame,
    stat_cols: list[str],
    audit_dir: Path,
    suffix: str,
) -> pd.DataFrame:
    pbp_present = merged["_merge"].ne("right_only")
    super_present = merged["_merge"].ne("left_only")
    both = merged["_merge"].eq("both")

    rows: list[dict[str, Any]] = []
    examples: list[pd.DataFrame] = []
    for stat in stat_cols:
        p = numeric(merged.get(f"{stat}_pbp"), merged.index)
        s = numeric(merged.get(f"{stat}_super"), merged.index)
        d = p - s
        p_nz = p.abs().gt(EPSILON)
        s_nz = s.abs().gt(EPSILON)
        discrep = d.abs().gt(EPSILON)
        masks = {
            "missing_supertable_row": pbp_present & ~super_present & p_nz,
            "super_only_row": ~pbp_present & super_present & s_nz,
            "pbp_nonzero_super_zero": both & p_nz & ~s_nz,
            "super_nonzero_pbp_zero": both & ~p_nz & s_nz,
            "both_nonzero_diff": both & p_nz & s_nz & discrep,
        }
        rows.append(
            {
                "stat": stat,
                "pbp_total": float(p.sum()),
                "super_total": float(s.sum()),
                "delta_total": float(d.sum()),
                "abs_delta_total": float(d.abs().sum()),
                "discrepant_rows": int(discrep.sum()),
                **{f"{name}_rows": int(mask.sum()) for name, mask in masks.items()},
                **{f"{name}_delta": float(d[mask].sum()) for name, mask in masks.items()},
            }
        )
        if discrep.any():
            detail = pd.DataFrame(
                {
                    "stat": stat,
                    "player_week": coalesce(merged, "player_week"),
                    "NFL_player_id": coalesce(merged, "NFL_player_id"),
                    "player": coalesce(merged, "player"),
                    "position": coalesce(merged, "position"),
                    "nfl_team": coalesce(merged, "nfl_team"),
                    "opponent_nfl_team": coalesce(merged, "opponent_nfl_team"),
                    "year": coalesce(merged, "year"),
                    "week": coalesce(merged, "week"),
                    "pbp_value": p,
                    "super_value": s,
                    "delta": d,
                    "abs_delta": d.abs(),
                    "row_status": merged["_merge"].astype(str),
                    "reconcile_match_type": merged["reconcile_match_type"].astype(str),
                }
            )
            examples.append(detail[discrep].sort_values("abs_delta", ascending=False).head(300))

    summary = pd.DataFrame(rows).sort_values("abs_delta_total", ascending=False)
    summary.to_csv(audit_dir / f"weekly_reconciled_value_discrepancy_by_stat_{suffix}.csv", index=False)
    if examples:
        pd.concat(examples, ignore_index=True).sort_values("abs_delta", ascending=False).head(20_000).to_csv(
            audit_dir / f"weekly_reconciled_value_discrepancy_examples_{suffix}.csv",
            index=False,
        )
    return summary


def write_notes(audit_dir: Path, exact_summary: pd.DataFrame, alt_summary: pd.DataFrame) -> None:
    lines = [
        "# Weekly ID Reconciliation And Remaining Value Deltas",
        "",
        "This pass pairs same normalized player name + year + week + season type + position + team + opponent rows when `player_week` differs.",
        "It is local-only and does not update Fly.",
        "",
        "## After Exact/Near ID Pairing",
        "",
        exact_summary.head(30).to_csv(index=False),
        "",
        "## After All Unique Alt-Key Pairing",
        "",
        alt_summary.head(30).to_csv(index=False),
        "",
        "Interpretation:",
        "",
        "- Exact/near paired rows are ID cleanup candidates, not stat corrections.",
        "- All unique alt-key pairing is diagnostic: it shows what remains if we assume same name/year/week/season/team/opponent/position is the same player.",
        "- Remaining zero gaps in derived/bucket columns are usually missing derived atoms in the supertable, not proof the raw box score is wrong.",
        "- `position = DEF` team-defense rows need their own team-level PBP audit and should not be mixed with player/IDP row reconciliation.",
        "",
    ]
    (audit_dir / "WEEKLY_ID_RECONCILIATION_NOTES.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollup-dir", type=Path, default=DEFAULT_ROLLUP_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pbp, super_df, shared = read_inputs(args.rollup_dir, args.audit_dir)
    pairs = build_alt_pairs(pbp, super_df, shared)
    pairs.to_csv(args.audit_dir / "weekly_altkey_id_reconciliation_pairs.csv", index=False)

    bridge = aggregate_player_bridge(pairs)
    bridge.to_csv(args.audit_dir / "weekly_player_id_bridge_candidates.csv", index=False)
    adjudication_summary = write_missing_row_adjudication(pbp, super_df, pairs, shared, args.audit_dir)

    exact_near = {"exact_vector", "near_vector_le_5"}
    all_diagnostic = {"exact_vector", "near_vector_le_5", "same_name_week_with_stat_overlap", "review"}
    exact_merge = build_reconciled_merge(pbp, super_df, shared, pairs, exact_near)
    exact_summary = summarize_reconciled(exact_merge, shared, args.audit_dir, "exact_near_id_bridge")

    alt_merge = build_reconciled_merge(pbp, super_df, shared, pairs, all_diagnostic)
    alt_summary = summarize_reconciled(alt_merge, shared, args.audit_dir, "all_unique_altkey")

    write_notes(args.audit_dir, exact_summary, alt_summary)
    print(f"pairs: {len(pairs):,}")
    if "id_pair_quality" in pairs.columns:
        print(pairs["id_pair_quality"].value_counts(dropna=False).to_string())
    else:
        print("id_pair_quality: none")
    print(f"bridge candidates: {len(bridge):,}")
    print("row adjudication:")
    print(adjudication_summary.to_string(index=False))
    print(f"notes: {args.audit_dir / 'WEEKLY_ID_RECONCILIATION_NOTES.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
