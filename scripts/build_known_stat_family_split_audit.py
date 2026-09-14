#!/usr/bin/env python3
"""Dry-run and document confirmed same-name stat-family splits.

This validates the build-time guard for cases where one supertable row contains
stat families from two different players with the same display name.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
OUT_DIR = AUDIT_DIR / "context_truth_20260509"

SUPER_PATH = AUDIT_DIR / "fly_supertable_weekly_selected_1978_2025.parquet"
PBP_PATH = ROLLUP_DIR / "pbp_player_week_rollup.parquet"

AFFECTED_PLAYERS = {"Mark Carrier", "Kevin Williams"}
AFFECTED_IDS = {"00-0002687", "00-0002688", "WillKe00", "00-0017861"}
SUMMARY_STATS = [
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "special_teams_tds",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_sacks",
    "def_fumbles_forced",
    "fum_rec",
    "fum_rec_yds",
    "fumble_recovery_yards",
]


def canonical_team(series: pd.Series) -> pd.Series:
    codes = series.fillna("").astype(str).str.upper()
    return codes.replace(
        {
            "ARZ": "ARI",
            "CRD": "ARI",
            "PHO": "ARI",
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
            "RAM": "LAR",
            "LA": "LAR",
            "STL": "LAR",
            "HOU": "TEN",
        }
    )


def affected(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["player"].isin(AFFECTED_PLAYERS) | df["NFL_player_id"].astype(str).isin(AFFECTED_IDS)].copy()


def affected_split_ids(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["player"].isin(AFFECTED_PLAYERS) & df["NFL_player_id"].astype(str).isin(AFFECTED_IDS)].copy()


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    out = affected(df)
    for col in SUMMARY_STATS:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return (
        out.groupby(["player", "NFL_player_id", "position", "nfl_team", "year"], dropna=False)
        .agg(row_count=("player_week", "size"), **{col: (col, "sum") for col in SUMMARY_STATS})
        .reset_index()
        .sort_values(["player", "year", "NFL_player_id", "nfl_team"], kind="stable")
    )


def validate_against_pbp(split_df: pd.DataFrame, pbp: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    pbp_affected = affected_split_ids(pbp)
    split_affected = affected_split_ids(split_df)
    joined = pbp_affected[
        [
            "player_week",
            "NFL_player_id",
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
        ]
    ].merge(
        split_affected[
            [
                "player_week",
                "NFL_player_id",
                "player",
                "position",
                "nfl_team",
                "opponent_nfl_team",
                "year",
                "week",
            ]
        ],
        on="player_week",
        how="left",
        suffixes=("_pbp", "_split"),
        indicator=True,
    )
    joined["pbp_team_canonical"] = canonical_team(joined["nfl_team_pbp"])
    joined["split_team_canonical"] = canonical_team(joined["nfl_team_split"])
    joined["pbp_opp_canonical"] = canonical_team(joined["opponent_nfl_team_pbp"])
    joined["split_opp_canonical"] = canonical_team(joined["opponent_nfl_team_split"])
    joined["is_missing_after_split"] = joined["_merge"].eq("left_only")
    context_known = (
        joined["pbp_team_canonical"].ne("") & joined["pbp_opp_canonical"].ne("") & ~joined["is_missing_after_split"]
    )
    joined["is_known_context_mismatch_after_split"] = context_known & (
        joined["pbp_team_canonical"].ne(joined["split_team_canonical"])
        | joined["pbp_opp_canonical"].ne(joined["split_opp_canonical"])
    )
    metrics = {
        "pbp_affected_rows": int(len(pbp_affected)),
        "split_affected_rows": int(len(split_affected)),
        "missing_after_split": int(joined["is_missing_after_split"].sum()),
        "known_context_mismatch_after_split": int(joined["is_known_context_mismatch_after_split"].sum()),
    }
    return joined, metrics


def main() -> int:
    from multi_league.data_fetchers.player_identity_guards import apply_known_stat_family_splits

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    super_df = pd.read_parquet(SUPER_PATH)
    pbp = pd.read_parquet(PBP_PATH)

    split_df = apply_known_stat_family_splits(super_df, log_fn=print)
    before = affected(super_df)
    after = affected(split_df)

    before_summary = summarize(super_df)
    after_summary = summarize(split_df)
    validation, validation_metrics = validate_against_pbp(split_df, pbp)

    before_summary.to_csv(OUT_DIR / "known_stat_family_split_before_summary.csv", index=False)
    after_summary.to_csv(OUT_DIR / "known_stat_family_split_after_summary.csv", index=False)
    validation.to_csv(OUT_DIR / "known_stat_family_split_pbp_join_validation.csv", index=False)
    split_df.to_parquet(OUT_DIR / "known_stat_family_split_dryrun_weekly_selected.parquet", index=False)

    before_ids = before.groupby(["player", "NFL_player_id"], dropna=False).size().reset_index(name="rows_before")
    after_ids = after.groupby(["player", "NFL_player_id"], dropna=False).size().reset_index(name="rows_after")
    id_summary = before_ids.merge(after_ids, on=["player", "NFL_player_id"], how="outer").fillna(0)
    id_summary.to_csv(OUT_DIR / "known_stat_family_split_id_summary.csv", index=False)

    manifest = {
        "source_super_path": str(SUPER_PATH),
        "source_pbp_path": str(PBP_PATH),
        "dryrun_super_path": str(OUT_DIR / "known_stat_family_split_dryrun_weekly_selected.parquet"),
        "total_rows_before": int(len(super_df)),
        "total_rows_after": int(len(split_df)),
        "row_delta": int(len(split_df) - len(super_df)),
        "affected_rows_before": int(len(before)),
        "affected_rows_after": int(len(after)),
        "duplicate_player_week_after": int(split_df["player_week"].duplicated().sum()),
        **validation_metrics,
    }
    (OUT_DIR / "known_stat_family_split_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    md_lines = [
        "# Known Stat-Family Identity Split Audit",
        "",
        f"Rows before/after: {manifest['total_rows_before']:,} -> {manifest['total_rows_after']:,} "
        f"(delta {manifest['row_delta']:+,}).",
        "",
        f"Affected rows before/after: {manifest['affected_rows_before']:,} -> " f"{manifest['affected_rows_after']:,}.",
        "",
        f"Duplicate player_week keys after split: {manifest['duplicate_player_week_after']:,}.",
        "",
        f"PBP affected rows: {manifest['pbp_affected_rows']:,}.",
        f"Missing after split: {manifest['missing_after_split']:,}.",
        f"Known-context mismatches after split: {manifest['known_context_mismatch_after_split']:,}.",
        "",
        "## ID Summary",
        "",
        id_summary.to_csv(index=False).strip(),
        "",
        "## Notes",
        "",
        "- Mark Carrier is split into WR `00-0002687` and DB `00-0002688`.",
        "- Kevin Williams is split into WR/KR `WillKe00` and DB `00-0017861`.",
        "- Existing source stat-family values are preserved where present; PBP fills missing counterpart rows/context.",
    ]
    (OUT_DIR / "KNOWN_STAT_FAMILY_SPLIT_AUDIT.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
