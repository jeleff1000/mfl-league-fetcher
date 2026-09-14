#!/usr/bin/env python3
"""Build reviewable PFR identity bridge repairs from the boxscore audit.

This is a local artifact builder only. It does not write to Fly.

Outputs:
* pfr_bridge_fill_existing_high_confidence.csv
* pfr_bridge_review_*.csv
* player_bio_stathead_pfr_bridge_repaired.parquet
* PFR_IDENTITY_BRIDGE_REPAIR.md
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_supertable_audit_20260512T142219Z"
DEFAULT_BIO = ORGANIZED_ROOT / "nfl_historical_repair_artifacts_20260508" / "player_bio_stathead_pfr_repaired.parquet"
DEFAULT_OUTPUT_ROOT = ORGANIZED_ROOT / "_catalog"


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def normalize_name(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"[^a-zA-Z0-9]+", "", str(value)).lower()


def pfr_blank(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    return text.eq("") | text.str.lower().eq("none")


def add_span_metrics(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in ["first_season", "last_season", "first_year", "last_year"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    result["overlap_start"] = result[["first_season", "first_year"]].max(axis=1)
    result["overlap_end"] = result[["last_season", "last_year"]].min(axis=1)
    result["overlap_years"] = (result["overlap_end"] - result["overlap_start"] + 1).clip(lower=0)
    result["pfr_span_years"] = (result["last_season"] - result["first_season"] + 1).clip(lower=1)
    result["bio_span_years"] = (result["last_year"] - result["first_year"] + 1).clip(lower=1)
    result["span_overlap_pct"] = (
        result["overlap_years"] / result[["pfr_span_years", "bio_span_years"]].min(axis=1)
    ).fillna(0)
    result["first_year_abs_delta"] = (result["first_season"] - result["first_year"]).abs()
    result["last_year_abs_delta"] = (result["last_season"] - result["last_year"]).abs()
    return result


def load_name_context_evidence(audit_dir: Path) -> pd.DataFrame:
    matched_path = audit_dir / "matched_pfr_super_player_games.parquet"
    cols = [
        "pfr_id",
        "pfr_player",
        "match_method",
        "super_NFL_player_id",
        "super_player",
        "total_abs_delta",
        "season",
    ]
    matched = pd.read_parquet(matched_path, columns=cols)
    matched = matched[matched["pfr_id"].notna() & matched["match_method"].eq("name_context")].copy()
    if matched.empty:
        return pd.DataFrame(
            columns=[
                "pfr_id_norm",
                "name_context_rows",
                "name_context_unique_ids",
                "name_context_super_ids",
                "name_context_exact_rows",
                "name_context_min_delta",
                "name_context_max_delta",
                "name_context_first_season",
                "name_context_last_season",
            ]
        )

    matched["pfr_id_norm"] = matched["pfr_id"].astype(str).str.lower()
    evidence = (
        matched.groupby("pfr_id_norm")
        .agg(
            name_context_rows=("pfr_id", "size"),
            name_context_unique_ids=("super_NFL_player_id", "nunique"),
            name_context_super_ids=(
                "super_NFL_player_id",
                lambda s: ";".join(sorted(set(str(v) for v in s if pd.notna(v)))),
            ),
            name_context_exact_rows=(
                "total_abs_delta",
                lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0) <= 1e-9).sum()),
            ),
            name_context_min_delta=("total_abs_delta", "min"),
            name_context_max_delta=("total_abs_delta", "max"),
            name_context_first_season=("season", "min"),
            name_context_last_season=("season", "max"),
        )
        .reset_index()
    )
    return evidence


def build_repairs(audit_dir: Path, bio_path: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    missing = pd.read_csv(audit_dir / "pfr_ids_missing_identity_bridge.csv")
    bio = pd.read_parquet(bio_path)

    missing["pfr_id_norm"] = missing["pfr_id"].astype(str).str.lower()
    missing["player_norm"] = missing["player"].map(normalize_name)
    bio["player_norm"] = bio["player"].map(normalize_name)
    bio["pfr_id_blank"] = pfr_blank(bio["pfr_id"])

    name_context = load_name_context_evidence(audit_dir)

    blank_bio = bio[bio["pfr_id_blank"]].copy()
    exact_candidates = missing.merge(
        blank_bio,
        on="player_norm",
        suffixes=("_pfr", "_bio"),
        how="inner",
    )
    exact_candidates = add_span_metrics(exact_candidates)
    exact_candidates = exact_candidates.merge(name_context, on="pfr_id_norm", how="left")
    exact_candidates["name_context_conflict"] = exact_candidates["name_context_unique_ids"].fillna(0).gt(
        0
    ) & exact_candidates["name_context_super_ids"].ne(exact_candidates["NFL_player_id"].astype(str))

    overlap_counts = (
        exact_candidates.assign(overlaps=exact_candidates["overlap_years"].gt(0))
        .groupby("pfr_id_norm")
        .agg(
            exact_blank_name_candidates=("NFL_player_id", "nunique"),
            overlapping_blank_name_candidates=("overlaps", "sum"),
            max_overlap_years=("overlap_years", "max"),
            max_span_overlap_pct=("span_overlap_pct", "max"),
        )
        .reset_index()
    )
    exact_candidates = exact_candidates.merge(overlap_counts, on="pfr_id_norm", how="left")

    high_confidence = exact_candidates[
        exact_candidates["overlap_years"].gt(0)
        & exact_candidates["overlapping_blank_name_candidates"].eq(1)
        & ~exact_candidates["name_context_conflict"]
    ].copy()
    high_confidence["repair_action"] = "fill_existing_bio_pfr_id"
    high_confidence["repair_confidence"] = "high"
    high_confidence["repair_reason"] = "exact normalized player name, one overlapping blank-pfr_id bio row"

    name_context_candidates = missing.merge(name_context, on="pfr_id_norm", how="inner")
    name_context_candidates = name_context_candidates[name_context_candidates["name_context_unique_ids"].eq(1)].copy()
    name_context_candidates["name_context_NFL_player_id"] = name_context_candidates["name_context_super_ids"]
    name_context_candidates = name_context_candidates.merge(
        blank_bio,
        left_on="name_context_NFL_player_id",
        right_on="NFL_player_id",
        suffixes=("_pfr", "_bio"),
        how="inner",
    )
    name_context_candidates = name_context_candidates[
        ~name_context_candidates["pfr_id_norm"].isin(high_confidence["pfr_id_norm"])
    ].copy()
    for col in ["first_season", "last_season", "first_year", "last_year"]:
        name_context_candidates[col] = pd.to_numeric(name_context_candidates[col], errors="coerce")
    name_context_candidates["pfr_span_years"] = (
        name_context_candidates["last_season"] - name_context_candidates["first_season"] + 1
    ).clip(lower=1)
    name_context_candidates["bio_span_years"] = (
        name_context_candidates["last_year"] - name_context_candidates["first_year"] + 1
    )
    name_context_candidates["within_bio_span"] = name_context_candidates["first_season"].ge(
        name_context_candidates["first_year"]
    ) & name_context_candidates["last_season"].le(name_context_candidates["last_year"])
    name_context_candidates["span_ok"] = name_context_candidates["bio_span_years"].isna() | name_context_candidates[
        "bio_span_years"
    ].le(name_context_candidates["pfr_span_years"] + 10)
    high_confidence_context = name_context_candidates[
        name_context_candidates["name_context_rows"].ge(2)
        & (
            name_context_candidates["name_context_exact_rows"].ge(1)
            | name_context_candidates["name_context_max_delta"].le(5)
        )
        & name_context_candidates["within_bio_span"]
        & name_context_candidates["span_ok"]
    ].copy()
    high_confidence_context["repair_action"] = "fill_existing_bio_pfr_id"
    high_confidence_context["repair_confidence"] = "high"
    high_confidence_context["repair_reason"] = "unique name-context supertable match to blank-pfr_id bio row"
    high_confidence_context = high_confidence_context.rename(
        columns={
            "pfr_id": "pfr_id_pfr",
            "player_pfr": "player_pfr",
            "pfr_id_bio": "pfr_id_bio",
        }
    )
    context_target_counts = high_confidence_context.groupby("NFL_player_id")["pfr_id_norm"].transform("nunique")
    high_confidence_context = high_confidence_context[
        context_target_counts.eq(1) & ~high_confidence_context["NFL_player_id"].isin(high_confidence["NFL_player_id"])
    ].copy()

    high_confidence_all = pd.concat(
        [high_confidence, high_confidence_context],
        ignore_index=True,
        sort=False,
    )
    high_confidence_all = high_confidence_all.drop_duplicates(["pfr_id_norm", "NFL_player_id"], keep="first")

    review_ambiguous = exact_candidates[
        exact_candidates["overlap_years"].gt(0) & exact_candidates["overlapping_blank_name_candidates"].gt(1)
    ].copy()
    review_ambiguous["review_reason"] = "multiple overlapping blank-pfr_id bio rows"

    review_conflicts = exact_candidates[
        exact_candidates["overlap_years"].gt(0) & exact_candidates["name_context_conflict"]
    ].copy()
    review_conflicts["review_reason"] = "exact-name bio candidate conflicts with name-context match"

    review_no_overlap = exact_candidates[exact_candidates["overlap_years"].le(0)].copy()
    review_no_overlap["review_reason"] = "exact normalized name but no active-year overlap"

    all_exact_ids = set(exact_candidates["pfr_id_norm"])
    no_exact = missing[~missing["pfr_id_norm"].isin(all_exact_ids)].copy()

    existing_name_any = missing.merge(
        bio,
        on="player_norm",
        suffixes=("_pfr", "_bio"),
        how="inner",
    )
    existing_name_any = add_span_metrics(existing_name_any)
    existing_name_any = existing_name_any[~existing_name_any["pfr_id_blank"]].copy()
    existing_name_any = existing_name_any.merge(name_context, on="pfr_id_norm", how="left")
    existing_name_any["review_reason"] = "same normalized name exists with a different nonblank pfr_id"

    # Apply only the high-confidence fill set to a derived local bio file.
    repaired = bio.drop(columns=["player_norm", "pfr_id_blank"]).copy()
    fill_map = high_confidence_all.set_index("NFL_player_id")["pfr_id_pfr"].to_dict()
    repaired["pfr_id"] = repaired.apply(
        lambda row: fill_map.get(row["NFL_player_id"], row["pfr_id"]),
        axis=1,
    )

    column_order = [
        "pfr_id_pfr",
        "player_pfr",
        "first_season",
        "last_season",
        "fact_rows",
        "NFL_player_id",
        "player_bio",
        "nfl_position",
        "first_year",
        "last_year",
        "career_games",
        "pfr_id_bio",
        "overlap_years",
        "span_overlap_pct",
        "name_context_rows",
        "name_context_super_ids",
        "name_context_exact_rows",
        "name_context_min_delta",
        "name_context_max_delta",
        "repair_action",
        "repair_confidence",
        "repair_reason",
    ]
    review_cols = [
        "pfr_id_pfr",
        "player_pfr",
        "first_season",
        "last_season",
        "fact_rows",
        "NFL_player_id",
        "player_bio",
        "nfl_position",
        "first_year",
        "last_year",
        "career_games",
        "pfr_id_bio",
        "overlap_years",
        "span_overlap_pct",
        "name_context_rows",
        "name_context_super_ids",
        "name_context_exact_rows",
        "name_context_min_delta",
        "name_context_max_delta",
        "review_reason",
    ]

    def write_csv(df: pd.DataFrame, name: str, cols: list[str]) -> None:
        present = [col for col in cols if col in df.columns]
        df[present].sort_values(
            [col for col in ["fact_rows", "pfr_id_pfr", "NFL_player_id"] if col in present],
            ascending=[False, True, True][
                : len([col for col in ["fact_rows", "pfr_id_pfr", "NFL_player_id"] if col in present])
            ],
        ).to_csv(output_dir / name, index=False)

    write_csv(
        high_confidence_all,
        "pfr_bridge_fill_existing_high_confidence.csv",
        column_order,
    )
    write_csv(
        high_confidence_context,
        "pfr_bridge_fill_existing_name_context_high_confidence.csv",
        column_order,
    )
    write_csv(
        review_ambiguous,
        "pfr_bridge_review_ambiguous_exact_name.csv",
        review_cols,
    )
    write_csv(
        review_conflicts,
        "pfr_bridge_review_name_context_conflicts.csv",
        review_cols,
    )
    write_csv(
        review_no_overlap,
        "pfr_bridge_review_exact_name_no_overlap.csv",
        review_cols,
    )

    no_exact.merge(name_context, on="pfr_id_norm", how="left").sort_values(
        ["fact_rows", "pfr_id"],
        ascending=[False, True],
    ).to_csv(output_dir / "pfr_bridge_review_no_exact_bio_name.csv", index=False)
    existing_name_any.sort_values(
        ["fact_rows", "pfr_id_pfr", "NFL_player_id"],
        ascending=[False, True, True],
    ).to_csv(output_dir / "pfr_bridge_review_existing_name_pfr_conflicts.csv", index=False)

    repaired_bio_path = output_dir / "player_bio_stathead_pfr_bridge_repaired.parquet"
    repaired.to_parquet(repaired_bio_path, index=False)

    counts = {
        "missing_bridge_ids": int(len(missing)),
        "high_confidence_fill_ids": int(high_confidence_all["pfr_id_norm"].nunique()),
        "high_confidence_exact_name_fill_ids": int(high_confidence["pfr_id_norm"].nunique()),
        "high_confidence_name_context_fill_ids": int(high_confidence_context["pfr_id_norm"].nunique()),
        "high_confidence_fact_rows": int(high_confidence_all.drop_duplicates("pfr_id_norm")["fact_rows"].sum()),
        "ambiguous_exact_name_ids": int(review_ambiguous["pfr_id_norm"].nunique()),
        "name_context_conflict_ids": int(review_conflicts["pfr_id_norm"].nunique()),
        "exact_name_no_overlap_ids": int(review_no_overlap["pfr_id_norm"].nunique()),
        "no_exact_bio_name_ids": int(no_exact["pfr_id_norm"].nunique()),
        "existing_name_pfr_conflict_ids": int(existing_name_any["pfr_id_norm"].nunique()),
        "repaired_bio_path": str(repaired_bio_path),
    }

    manifest = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "audit_dir": str(audit_dir),
        "bio_path": str(bio_path),
        "output_dir": str(output_dir),
        "counts": counts,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    top_fills = high_confidence_all.sort_values("fact_rows", ascending=False).head(12)
    lines = [
        "# PFR Identity Bridge Repair",
        "",
        f"Generated: {manifest['generated_at_utc']}",
        f"Audit dir: `{audit_dir}`",
        f"Base bio: `{bio_path}`",
        "",
        "## Summary",
        "",
        f"- Missing PFR bridge IDs reviewed: {counts['missing_bridge_ids']:,}",
        f"- High-confidence existing bio fills: {counts['high_confidence_fill_ids']:,}",
        f"  - Exact-name fills: {counts['high_confidence_exact_name_fill_ids']:,}",
        f"  - Name-context fills: {counts['high_confidence_name_context_fill_ids']:,}",
        f"- PFR fact rows covered by those fills: {counts['high_confidence_fact_rows']:,}",
        f"- Ambiguous exact-name IDs left for review: {counts['ambiguous_exact_name_ids']:,}",
        f"- Name-context conflicts left for review: {counts['name_context_conflict_ids']:,}",
        f"- No exact bio-name IDs left for review/stubs: {counts['no_exact_bio_name_ids']:,}",
        "",
        "## Top Safe Fills",
        "",
        top_fills[
            [
                "pfr_id_pfr",
                "player_pfr",
                "first_season",
                "last_season",
                "fact_rows",
                "NFL_player_id",
                "player_bio",
                "first_year",
                "last_year",
            ]
        ].to_csv(index=False),
        "",
        f"Derived repaired bio: `{repaired_bio_path}`",
    ]
    (output_dir / "PFR_IDENTITY_BRIDGE_REPAIR.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--bio-path", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir or (args.output_root / f"pfr_identity_bridge_repairs_{now_stamp()}")
    manifest = build_repairs(args.audit_dir, args.bio_path, output_dir)
    print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
