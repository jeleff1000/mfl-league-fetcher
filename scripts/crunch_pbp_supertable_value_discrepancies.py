#!/usr/bin/env python3
"""
Crunch value discrepancies between local PBP rollups and cached Fly supertable rollups.

This is the second-pass audit after compare_pbp_rollups_to_fly_supertable.py.
It does not query or mutate Fly; it uses the local audit cache created by that script.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"

ROLLUP_FILES = {
    "weekly": "pbp_player_week_rollup.parquet",
    "season_regular": "pbp_player_nfl_season.parquet",
    "season_all": "pbp_player_nfl_season_all.parquet",
    "career_regular": "pbp_player_nfl_career.parquet",
    "career_all": "pbp_player_nfl_career_all.parquet",
}

LEVEL_KEYS = {
    "weekly": ["player_week"],
    "season_regular": ["NFL_player_id", "year"],
    "season_all": ["NFL_player_id", "year"],
    "career_regular": ["NFL_player_id"],
    "career_all": ["NFL_player_id"],
}

MAX_EXAMPLES_PER_STAT = 750
MAX_EXAMPLES_PER_LEVEL = 50_000
EPSILON = 1e-9


def numeric(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(0.0, index=index)
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def coalesce_column(df: pd.DataFrame, name: str) -> pd.Series:
    left = f"{name}_pbp"
    right = f"{name}_super"
    if left in df.columns and right in df.columns:
        return df[left].combine_first(df[right])
    if left in df.columns:
        return df[left]
    if right in df.columns:
        return df[right]
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index)


def level_merge(
    pbp: pd.DataFrame,
    super_df: pd.DataFrame,
    level: str,
    stat_cols: list[str],
) -> pd.DataFrame:
    keys = LEVEL_KEYS[level]
    pbp = pbp[pbp[keys].notna().all(axis=1)].copy()
    super_df = super_df[super_df[keys].notna().all(axis=1)].copy()

    id_cols = [
        "NFL_player_id",
        "player_week",
        "player",
        "position",
        "year",
        "week",
        "season_type",
        "pbp_player_id",
        "pbp_player_name",
        "event_roles",
        "pbp_source_systems",
        "player_id_namespaces",
    ]
    pbp_cols = [col for col in dict.fromkeys([*keys, *id_cols, *stat_cols]) if col in pbp.columns]
    super_cols = [col for col in dict.fromkeys([*keys, *id_cols, *stat_cols]) if col in super_df.columns]

    pbp_small = pbp[pbp_cols].copy()
    super_small = super_df[super_cols].copy()
    return pbp_small.merge(
        super_small,
        on=keys,
        how="outer",
        suffixes=("_pbp", "_super"),
        indicator=True,
    )


def derive_year_era(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    year = coalesce_column(out, "year")
    out["_year"] = pd.to_numeric(year, errors="coerce")
    out["_era"] = pd.cut(
        out["_year"],
        bins=[1977, 1993, 1998, 2025],
        labels=["1978-1993", "1994-1998", "1999-2025"],
    ).astype("string")
    return out


def summarize_level(
    level: str,
    merged: pd.DataFrame,
    stat_cols: list[str],
    audit_dir: Path,
) -> tuple[pd.DataFrame, list[pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    pbp_present = merged["_merge"].ne("right_only")
    super_present = merged["_merge"].ne("left_only")
    both_present = merged["_merge"].eq("both")
    summaries: list[dict[str, Any]] = []
    examples: list[pd.DataFrame] = []
    year_rows: list[dict[str, Any]] = []
    era_rows: list[dict[str, Any]] = []

    with_year = derive_year_era(merged)
    base_example_cols = [
        col
        for col in [
            "NFL_player_id",
            "player_week",
            "player",
            "position",
            "year",
            "week",
            "season_type",
            "pbp_player_id",
            "pbp_player_name",
            "event_roles",
            "pbp_source_systems",
            "player_id_namespaces",
        ]
        if col in merged.columns or f"{col}_pbp" in merged.columns or f"{col}_super" in merged.columns
    ]

    for stat in stat_cols:
        p_col = f"{stat}_pbp" if f"{stat}_pbp" in merged.columns else stat
        s_col = f"{stat}_super" if f"{stat}_super" in merged.columns else stat
        p = numeric(merged[p_col] if p_col in merged.columns else None, merged.index)
        s = numeric(merged[s_col] if s_col in merged.columns else None, merged.index)
        delta = p - s
        abs_delta = delta.abs()
        pbp_nz = p.abs().gt(EPSILON)
        super_nz = s.abs().gt(EPSILON)

        missing_row = pbp_present & ~super_present & pbp_nz
        super_only_row = ~pbp_present & super_present & super_nz
        pbp_nonzero_super_zero = both_present & pbp_nz & ~super_nz
        super_nonzero_pbp_zero = both_present & ~pbp_nz & super_nz
        both_nonzero_diff = both_present & pbp_nz & super_nz & abs_delta.gt(EPSILON)
        any_discrepancy = abs_delta.gt(EPSILON)

        summaries.append(
            {
                "level": level,
                "stat": stat,
                "outer_rows": int(len(merged)),
                "matched_rows": int(both_present.sum()),
                "pbp_total": float(p.sum()),
                "super_total": float(s.sum()),
                "delta_total": float(delta.sum()),
                "abs_delta_total": float(abs_delta.sum()),
                "discrepant_rows": int(any_discrepancy.sum()),
                "missing_supertable_row_rows": int(missing_row.sum()),
                "missing_supertable_row_delta": float(delta[missing_row].sum()),
                "super_only_row_rows": int(super_only_row.sum()),
                "super_only_row_delta": float(delta[super_only_row].sum()),
                "pbp_nonzero_super_zero_rows": int(pbp_nonzero_super_zero.sum()),
                "pbp_nonzero_super_zero_delta": float(delta[pbp_nonzero_super_zero].sum()),
                "super_nonzero_pbp_zero_rows": int(super_nonzero_pbp_zero.sum()),
                "super_nonzero_pbp_zero_delta": float(delta[super_nonzero_pbp_zero].sum()),
                "both_nonzero_diff_rows": int(both_nonzero_diff.sum()),
                "both_nonzero_diff_delta": float(delta[both_nonzero_diff].sum()),
                "max_abs_delta": float(abs_delta.max()) if len(abs_delta) else 0.0,
            }
        )

        if any_discrepancy.any():
            detail = pd.DataFrame(index=merged.index)
            for col in base_example_cols:
                detail[col] = coalesce_column(merged, col)
            detail["level"] = level
            detail["stat"] = stat
            detail["pbp_value"] = p
            detail["super_value"] = s
            detail["delta"] = delta
            detail["abs_delta"] = abs_delta
            detail["row_status"] = merged["_merge"].astype("string")
            detail["discrepancy_type"] = "both_nonzero_diff"
            detail.loc[missing_row, "discrepancy_type"] = "missing_supertable_row"
            detail.loc[super_only_row, "discrepancy_type"] = "super_only_row"
            detail.loc[pbp_nonzero_super_zero, "discrepancy_type"] = "pbp_nonzero_super_zero"
            detail.loc[super_nonzero_pbp_zero, "discrepancy_type"] = "super_nonzero_pbp_zero"
            examples.append(
                detail.loc[any_discrepancy].sort_values("abs_delta", ascending=False).head(MAX_EXAMPLES_PER_STAT)
            )

        if "_year" in with_year.columns and with_year["_year"].notna().any():
            for year, idx in with_year.groupby("_year", dropna=True).groups.items():
                idx = list(idx)
                if not idx:
                    continue
                year_rows.append(
                    {
                        "level": level,
                        "year": int(year),
                        "stat": stat,
                        "pbp_total": float(p.loc[idx].sum()),
                        "super_total": float(s.loc[idx].sum()),
                        "delta_total": float(delta.loc[idx].sum()),
                        "abs_delta_total": float(abs_delta.loc[idx].sum()),
                        "discrepant_rows": int(any_discrepancy.loc[idx].sum()),
                        "missing_supertable_row_rows": int(missing_row.loc[idx].sum()),
                        "pbp_nonzero_super_zero_rows": int(pbp_nonzero_super_zero.loc[idx].sum()),
                        "super_nonzero_pbp_zero_rows": int(super_nonzero_pbp_zero.loc[idx].sum()),
                        "both_nonzero_diff_rows": int(both_nonzero_diff.loc[idx].sum()),
                    }
                )
            for era, idx in with_year.groupby("_era", dropna=True).groups.items():
                idx = list(idx)
                if not idx:
                    continue
                era_rows.append(
                    {
                        "level": level,
                        "era": str(era),
                        "stat": stat,
                        "pbp_total": float(p.loc[idx].sum()),
                        "super_total": float(s.loc[idx].sum()),
                        "delta_total": float(delta.loc[idx].sum()),
                        "abs_delta_total": float(abs_delta.loc[idx].sum()),
                        "discrepant_rows": int(any_discrepancy.loc[idx].sum()),
                        "missing_supertable_row_rows": int(missing_row.loc[idx].sum()),
                        "pbp_nonzero_super_zero_rows": int(pbp_nonzero_super_zero.loc[idx].sum()),
                        "super_nonzero_pbp_zero_rows": int(super_nonzero_pbp_zero.loc[idx].sum()),
                        "both_nonzero_diff_rows": int(both_nonzero_diff.loc[idx].sum()),
                    }
                )

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(audit_dir / f"{level}_value_discrepancy_by_stat.csv", index=False)

    if examples:
        examples_df = (
            pd.concat(examples, ignore_index=True)
            .sort_values("abs_delta", ascending=False)
            .head(MAX_EXAMPLES_PER_LEVEL)
        )
    else:
        examples_df = pd.DataFrame()
    examples_df.to_csv(audit_dir / f"{level}_top_value_discrepancy_examples.csv", index=False)

    year_df = pd.DataFrame(year_rows)
    if not year_df.empty:
        year_df.to_csv(audit_dir / f"{level}_value_discrepancy_by_year_stat.csv", index=False)

    era_df = pd.DataFrame(era_rows)
    if not era_df.empty:
        era_df.to_csv(audit_dir / f"{level}_value_discrepancy_by_era_stat.csv", index=False)

    return summary_df, examples, year_df, era_df


def write_summary_md(audit_dir: Path, all_stats: pd.DataFrame) -> None:
    weekly = all_stats[all_stats["level"].eq("weekly")].copy()
    top_abs = weekly.sort_values("abs_delta_total", ascending=False).head(25)
    top_missing = weekly.sort_values("missing_supertable_row_delta", ascending=False).head(20)
    top_zero = weekly.sort_values("pbp_nonzero_super_zero_delta", ascending=False).head(20)
    top_super_only = weekly.sort_values("super_nonzero_pbp_zero_rows", ascending=False).head(20)

    lines = [
        "# PBP vs Supertable Value Discrepancies",
        "",
        "This is a full outer value comparison using cached local PBP rollups and cached Fly supertable rollups.",
        "",
        "## Weekly Biggest Absolute Deltas",
        "",
        top_abs[
            [
                "stat",
                "pbp_total",
                "super_total",
                "delta_total",
                "abs_delta_total",
                "discrepant_rows",
                "missing_supertable_row_rows",
                "pbp_nonzero_super_zero_rows",
                "super_nonzero_pbp_zero_rows",
                "both_nonzero_diff_rows",
            ]
        ].to_csv(index=False),
        "",
        "## Weekly Missing-Row Delta Drivers",
        "",
        top_missing[["stat", "missing_supertable_row_rows", "missing_supertable_row_delta", "delta_total"]].to_csv(
            index=False
        ),
        "",
        "## Weekly Existing-Row Zero Drivers",
        "",
        top_zero[["stat", "pbp_nonzero_super_zero_rows", "pbp_nonzero_super_zero_delta", "delta_total"]].to_csv(
            index=False
        ),
        "",
        "## Weekly Super-Only Value Drivers",
        "",
        top_super_only[["stat", "super_nonzero_pbp_zero_rows", "super_nonzero_pbp_zero_delta", "delta_total"]].to_csv(
            index=False
        ),
        "",
    ]

    era_path = audit_dir / "weekly_value_discrepancy_by_era_stat.csv"
    if era_path.exists():
        era = pd.read_csv(era_path)
        lines.extend(
            [
                "## Weekly Era Totals By Biggest Delta",
                "",
                era.sort_values("abs_delta_total", ascending=False)
                .head(40)[
                    [
                        "era",
                        "stat",
                        "pbp_total",
                        "super_total",
                        "delta_total",
                        "abs_delta_total",
                        "discrepant_rows",
                    ]
                ]
                .to_csv(index=False),
                "",
            ]
        )

    (audit_dir / "PBP_VALUE_DISCREPANCIES.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollup-dir", type=Path, default=DEFAULT_ROLLUP_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rollup_dir = args.rollup_dir
    audit_dir = args.audit_dir

    mapping = pd.read_csv(audit_dir / "pbp_to_supertable_column_mapping.csv")
    stat_cols = mapping["pbp_col"].astype(str).tolist()

    summaries = []
    for level, filename in ROLLUP_FILES.items():
        print(f"[crunch] {level}", flush=True)
        pbp = pd.read_parquet(rollup_dir / filename)
        super_df = pd.read_parquet(audit_dir / f"fly_supertable_derived_{level}.parquet")
        shared = [col for col in stat_cols if col in pbp.columns and col in super_df.columns]
        merged = level_merge(pbp, super_df, level, shared)
        summary, _, _, _ = summarize_level(level, merged, shared, audit_dir)
        summaries.append(summary)

    all_stats = pd.concat(summaries, ignore_index=True)
    all_stats.to_csv(audit_dir / "all_levels_value_discrepancy_by_stat.csv", index=False)
    write_summary_md(audit_dir, all_stats)
    print(f"[done] {audit_dir / 'PBP_VALUE_DISCREPANCIES.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
