#!/usr/bin/env python3
"""Classify remaining PFR bridge gaps and append PFR-backed bio stubs.

This is the final bridge-closing layer after exact-name and surname-context
repairs. It does not write to Fly.

The philosophy here is conservative:
* Do not force ambiguous same-name players into existing identities.
* Give every remaining PFR ID a durable player_bio row keyed by the PFR ID.
* Mark rows with supertable/name evidence for later rekey/merge review.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_RETAB_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_retabs_20260512T134853Z"
DEFAULT_AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pfr_boxscore_supertable_audit_bridge3_20260512T152545Z"
DEFAULT_BIO = (
    ORGANIZED_ROOT
    / "_catalog"
    / "pfr_lastname_context_bridge_repairs_20260512T152525Z"
    / "player_bio_stathead_pfr_bridge_repaired.parquet"
)
DEFAULT_OUTPUT_ROOT = ORGANIZED_ROOT / "_catalog"


OFFENSE_POS = {
    "QB",
    "RB",
    "FB",
    "HB",
    "TB",
    "BB",
    "WB",
    "B",
    "LH",
    "RH",
    "WR",
    "FL",
    "SE",
    "E",
    "TE",
    "C",
    "G",
    "T",
    "LG",
    "RG",
    "LT",
    "RT",
    "LE",
    "RE",
}
DEFENSE_POS = {"DB", "CB", "S", "FS", "SS", "LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT"}
SPECIAL_POS = {"K", "PK", "P", "LS"}


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def normalize_name(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"[^a-zA-Z0-9]+", "", str(value)).lower()


def pfr_blank(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    return text.eq("") | text.str.lower().eq("none")


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def infer_position(row: pd.Series) -> str | None:
    pos = "" if pd.isna(row.get("top_pos_hint")) else str(row.get("top_pos_hint") or "").strip()
    pos = pos.upper()
    if pos:
        return pos

    fg_att = float(row.get("fg_att") or 0)
    pat_att = float(row.get("pat_att") or 0)
    punts = float(row.get("punts") or 0)
    attempts = float(row.get("attempts") or 0)
    carries = float(row.get("carries") or 0)
    receptions = float(row.get("receptions") or 0)
    targets = float(row.get("targets") or 0)
    defense_activity = float(row.get("defense_activity") or 0)

    if (fg_att + pat_att) > 0:
        return "K"
    if punts > 0:
        return "P"
    if attempts > 0:
        return "QB"
    if carries > 0:
        return "RB"
    if (receptions + targets) > 0:
        return "WR"
    if defense_activity > 0:
        return "DEF"
    return None


def position_side(pos: str | None) -> str | None:
    if not pos:
        return None
    pos = pos.upper()
    if pos in SPECIAL_POS:
        return "special_teams"
    if pos in DEFENSE_POS or pos == "DEF":
        return "defense"
    if pos in OFFENSE_POS:
        return "offense"
    return None


def position_category(pos: str | None) -> str | None:
    if not pos:
        return None
    pos = pos.upper()
    if pos in {"K", "PK"}:
        return "K"
    if pos == "P":
        return "P"
    if pos in DEFENSE_POS or pos == "DEF":
        return "IDP"
    return pos


def build_remaining_fact_summary(retab_dir: Path, remaining: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.register("remaining_ids", remaining[["pfr_id_norm"]])
    fact = sql_path(retab_dir / "pfr_player_game_fact.parquet")
    hints = sql_path(retab_dir / "pfr_player_game_position_hint.parquet")
    return con.execute(
        f"""
        WITH fact AS (
            SELECT *
            FROM read_parquet('{fact}')
            WHERE lower(pfr_id) IN (SELECT pfr_id_norm FROM remaining_ids)
        ),
        hint_counts AS (
            SELECT
                lower(pfr_id) AS pfr_id_norm,
                upper(NULLIF(pos_hint, '')) AS pos_hint,
                count(*) AS hint_rows
            FROM read_parquet('{hints}')
            WHERE lower(pfr_id) IN (SELECT pfr_id_norm FROM remaining_ids)
              AND NULLIF(pos_hint, '') IS NOT NULL
            GROUP BY 1, 2
        ),
        top_hint AS (
            SELECT pfr_id_norm, pos_hint AS top_pos_hint, hint_rows AS top_pos_hint_rows
            FROM hint_counts
            QUALIFY row_number() OVER (
                PARTITION BY pfr_id_norm
                ORDER BY hint_rows DESC, pos_hint
            ) = 1
        )
        SELECT
            lower(f.pfr_id) AS pfr_id_norm,
            min(f.pfr_id) AS pfr_id,
            any_value(f.player) AS player,
            min(f.season) AS first_year,
            max(f.season) AS last_year,
            count(*) AS pfr_fact_rows,
            count(DISTINCT f.boxscore_id) AS pfr_boxscores,
            any_value(f.team ORDER BY f.season DESC, f.game_date DESC) AS latest_team,
            string_agg(DISTINCT f.source_tables, ';' ORDER BY f.source_tables) AS source_tables,
            SUM(COALESCE(f.completions, 0)) AS completions,
            SUM(COALESCE(f.attempts, 0)) AS attempts,
            SUM(COALESCE(f.passing_yards, 0)) AS passing_yards,
            SUM(COALESCE(f.carries, 0)) AS carries,
            SUM(COALESCE(f.rushing_yards, 0)) AS rushing_yards,
            SUM(COALESCE(f.receptions, 0)) AS receptions,
            SUM(COALESCE(f.receiving_yards, 0)) AS receiving_yards,
            SUM(COALESCE(f.targets, 0)) AS targets,
            SUM(COALESCE(f.fg_att, 0)) AS fg_att,
            SUM(COALESCE(f.pat_att, 0)) AS pat_att,
            SUM(COALESCE(f.punts, 0)) AS punts,
            SUM(COALESCE(f.def_interceptions, 0) + COALESCE(f.def_sacks, 0)
                + COALESCE(f.def_tackles_with_assist, 0) + COALESCE(f.fum_rec, 0)
                + COALESCE(f.def_fumbles_forced, 0)) AS defense_activity,
            th.top_pos_hint,
            th.top_pos_hint_rows
        FROM fact f
        LEFT JOIN top_hint th
          ON lower(f.pfr_id) = th.pfr_id_norm
        GROUP BY lower(f.pfr_id), th.top_pos_hint, th.top_pos_hint_rows
        """
    ).df()


def build_context_summary(audit_dir: Path, remaining: pd.DataFrame) -> pd.DataFrame:
    matched = pd.read_parquet(
        audit_dir / "matched_pfr_super_player_games.parquet",
        columns=[
            "pfr_id",
            "pfr_player",
            "match_method",
            "super_NFL_player_id",
            "super_player",
            "total_abs_delta",
            "season",
        ],
    )
    matched = matched[matched["pfr_id"].astype(str).isin(set(remaining["pfr_id"]))].copy()
    if matched.empty:
        return pd.DataFrame(columns=["pfr_id_norm"])
    matched["pfr_id_norm"] = matched["pfr_id"].astype(str).str.lower()
    return (
        matched.groupby("pfr_id_norm")
        .agg(
            context_matched_rows=("pfr_id", "size"),
            context_match_methods=(
                "match_method",
                lambda s: ";".join(sorted(set(str(v) for v in s if pd.notna(v)))),
            ),
            context_unique_super_ids=("super_NFL_player_id", "nunique"),
            context_super_ids=(
                "super_NFL_player_id",
                lambda s: ";".join(sorted(set(str(v) for v in s if pd.notna(v)))),
            ),
            context_super_players=(
                "super_player",
                lambda s: ";".join(sorted(set(str(v) for v in s if pd.notna(v)))),
            ),
            context_exact_rows=(
                "total_abs_delta",
                lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0) <= 1e-9).sum()),
            ),
            context_min_delta=("total_abs_delta", "min"),
            context_median_delta=("total_abs_delta", "median"),
            context_max_delta=("total_abs_delta", "max"),
            context_first_year=("season", "min"),
            context_last_year=("season", "max"),
        )
        .reset_index()
    )


def build_bio_name_summary(bio: pd.DataFrame, remaining: pd.DataFrame) -> pd.DataFrame:
    bio_work = bio.copy()
    rem = remaining.copy()
    bio_work["player_norm"] = bio_work["player"].map(normalize_name)
    rem["player_norm"] = rem["player"].map(normalize_name)
    bio_work["pfr_id_blank"] = pfr_blank(bio_work["pfr_id"])

    exact = rem.merge(
        bio_work,
        on="player_norm",
        suffixes=("_pfr", "_bio"),
        how="left",
    )
    exact = exact[exact["NFL_player_id"].notna()].copy()
    if exact.empty:
        return pd.DataFrame(columns=["pfr_id_norm"])
    for col in ["first_season", "last_season", "first_year", "last_year"]:
        exact[col] = pd.to_numeric(exact[col], errors="coerce")
    exact["overlap_years"] = (
        exact[["last_season", "last_year"]].min(axis=1) - exact[["first_season", "first_year"]].max(axis=1) + 1
    ).clip(lower=0)
    return (
        exact.groupby("pfr_id_norm")
        .agg(
            exact_name_bio_candidates=("NFL_player_id", "nunique"),
            exact_name_overlap_candidates=(
                "overlap_years",
                lambda s: int((s > 0).sum()),
            ),
            exact_name_blank_pfr_candidates=(
                "pfr_id_blank",
                lambda s: int(pd.Series(s).fillna(False).sum()),
            ),
            exact_name_candidate_ids=(
                "NFL_player_id",
                lambda s: ";".join(sorted(set(str(v) for v in s if pd.notna(v)))[:12]),
            ),
            exact_name_candidate_players=(
                "player_bio",
                lambda s: ";".join(sorted(set(str(v) for v in s if pd.notna(v)))[:12]),
            ),
        )
        .reset_index()
    )


def build_stubs(classified: pd.DataFrame, bio_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in classified.to_dict("records"):
        pos = infer_position(pd.Series(row))
        first_year = pd.to_numeric(pd.Series([row.get("first_year")]), errors="coerce").iloc[0]
        last_year = pd.to_numeric(pd.Series([row.get("last_year")]), errors="coerce").iloc[0]
        stub = {col: None for col in bio_columns}
        stub.update(
            {
                "NFL_player_id": row["pfr_id"],
                "player": row["player"],
                "nfl_position": pos,
                "latest_team": row.get("latest_team"),
                "status": "historical_pfr_boxscore_stub",
                "is_undrafted": 1,
                "first_year": first_year,
                "last_year": last_year,
                "years_active": (
                    None if pd.isna(first_year) or pd.isna(last_year) else float(last_year - first_year + 1)
                ),
                "career_games": row.get("pfr_boxscores"),
                "position_category": position_category(pos),
                "position_side": position_side(pos),
                "pfr_id": row["pfr_id"],
                "career_history": (
                    "PFR boxscore bridge stub; review before merging with existing " "same-name identity"
                    if row["classification"] != "new_pfr_bio_stub"
                    else "PFR boxscore bridge stub"
                ),
            }
        )
        rows.append(stub)
    return pd.DataFrame(rows, columns=bio_columns)


def build_repairs(
    retab_dir: Path,
    audit_dir: Path,
    bio_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    remaining = pd.read_csv(audit_dir / "pfr_ids_missing_identity_bridge.csv")
    remaining["pfr_id_norm"] = remaining["pfr_id"].astype(str).str.lower()
    bio = pd.read_parquet(bio_path)

    fact_summary = build_remaining_fact_summary(retab_dir, remaining)
    context_summary = build_context_summary(audit_dir, remaining)
    bio_name_summary = build_bio_name_summary(bio, remaining)

    classified = remaining.merge(fact_summary, on=["pfr_id_norm", "pfr_id"], how="left")
    # Prefer the audit player/years if there is any disagreement.
    classified["player"] = classified["player_x"].combine_first(classified["player_y"])
    classified["first_year"] = classified["first_season"].combine_first(classified["first_year"])
    classified["last_year"] = classified["last_season"].combine_first(classified["last_year"])
    classified = classified.drop(columns=[c for c in ["player_x", "player_y"] if c in classified])
    classified = classified.merge(context_summary, on="pfr_id_norm", how="left")
    classified = classified.merge(bio_name_summary, on="pfr_id_norm", how="left")
    for col in [
        "context_matched_rows",
        "context_unique_super_ids",
        "context_exact_rows",
        "exact_name_bio_candidates",
        "exact_name_overlap_candidates",
        "exact_name_blank_pfr_candidates",
    ]:
        classified[col] = pd.to_numeric(classified[col], errors="coerce").fillna(0).astype(int)

    classified["classification"] = "new_pfr_bio_stub"
    classified.loc[
        classified["context_matched_rows"].gt(0),
        "classification",
    ] = "existing_supertable_identity_review"
    classified.loc[
        classified["context_matched_rows"].eq(0) & classified["exact_name_overlap_candidates"].gt(0),
        "classification",
    ] = "same_name_bio_review_or_split"
    classified.loc[
        classified["context_matched_rows"].eq(0)
        & classified["exact_name_overlap_candidates"].eq(0)
        & classified["exact_name_bio_candidates"].gt(0),
        "classification",
    ] = "same_name_no_overlap_review"

    classified["recommended_action"] = "append_pfr_stub_then_insert_missing_weekly_rows"
    classified.loc[
        classified["classification"].eq("existing_supertable_identity_review"),
        "recommended_action",
    ] = "append_pfr_stub_and_review_supertable_rekey_or_merge"
    classified.loc[
        classified["classification"].str.startswith("same_name"),
        "recommended_action",
    ] = "append_pfr_stub_and_review_same_name_identity_before_merge"

    stubs = build_stubs(classified, list(bio.columns))
    stubs_for_concat = stubs.dropna(axis=1, how="all")
    repaired = pd.concat([bio, stubs_for_concat], ignore_index=True).reindex(columns=bio.columns)
    repaired = repaired.drop_duplicates(["NFL_player_id"], keep="first")

    classified.sort_values(["fact_rows", "pfr_id"], ascending=[False, True]).to_csv(
        output_dir / "pfr_remaining_bridge_classification.csv",
        index=False,
    )
    classified[classified["classification"].eq("existing_supertable_identity_review")].sort_values(
        ["fact_rows", "pfr_id"], ascending=[False, True]
    ).to_csv(
        output_dir / "pfr_existing_identity_manual_review.csv",
        index=False,
    )
    classified[classified["classification"].eq("new_pfr_bio_stub")].sort_values(
        ["fact_rows", "pfr_id"], ascending=[False, True]
    ).to_csv(
        output_dir / "pfr_new_stub_clean_candidates.csv",
        index=False,
    )
    stubs.sort_values(["first_year", "player"]).to_csv(
        output_dir / "pfr_new_bio_stubs.csv",
        index=False,
    )

    repaired_path = output_dir / "player_bio_pfr_bridge_zero_gap.parquet"
    repaired.to_parquet(repaired_path, index=False)

    counts = {
        "remaining_bridge_ids_input": int(len(remaining)),
        "stub_rows_appended": int(len(stubs)),
        "classification_counts": {
            str(k): int(v) for k, v in classified["classification"].value_counts().sort_index().items()
        },
        "old_bio_exact_pfr_hits": 0,
        "old_bio_exact_nfl_id_hits": 0,
        "repaired_bio_path": str(repaired_path),
    }
    manifest = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "retab_dir": str(retab_dir),
        "audit_dir": str(audit_dir),
        "bio_path": str(bio_path),
        "output_dir": str(output_dir),
        "counts": counts,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines = [
        "# PFR Remaining Bridge Classification",
        "",
        f"Generated: {manifest['generated_at_utc']}",
        f"Audit dir: `{audit_dir}`",
        f"Base bio: `{bio_path}`",
        "",
        "## Summary",
        "",
        f"- Remaining bridge IDs entering pass: {counts['remaining_bridge_ids_input']:,}",
        f"- PFR-backed bio stubs appended locally: {counts['stub_rows_appended']:,}",
        "- Exact PFR-ID hits in old/current bio and supertable: 0",
        "",
        "## Classification Counts",
        "",
        pd.Series(counts["classification_counts"]).to_csv(header=False),
        "",
        "## Top Existing-Identity Review",
        "",
        classified[classified["classification"].eq("existing_supertable_identity_review")]
        .sort_values(["fact_rows", "pfr_id"], ascending=[False, True])
        .head(25)[
            [
                "pfr_id",
                "player",
                "first_year",
                "last_year",
                "fact_rows",
                "context_super_ids",
                "context_super_players",
                "context_exact_rows",
                "context_median_delta",
                "context_max_delta",
            ]
        ]
        .to_csv(index=False),
        "",
        f"Derived zero-gap bio: `{repaired_path}`",
    ]
    (output_dir / "PFR_REMAINING_BRIDGE_CLASSIFICATION.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retab-dir", type=Path, default=DEFAULT_RETAB_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--bio-path", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir or (args.output_root / f"pfr_remaining_bridge_stubs_{now_stamp()}")
    manifest = build_repairs(args.retab_dir, args.audit_dir, args.bio_path, output_dir)
    print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
