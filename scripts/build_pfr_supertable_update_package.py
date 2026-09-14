#!/usr/bin/env python3
"""Build a local PFR supertable update package from weekly reconciliation.

Local-only. This script does not write to Fly. It emits:

* insert-ready weekly rows
* existing-row safe stat overlays
* long-form stat deltas
* review queues
* recompute scopes
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_OUTPUT_ROOT = ORGANIZED_ROOT / "_catalog"
DEFAULT_AUDIT_DIR = DEFAULT_OUTPUT_ROOT / "pfr_boxscore_supertable_audit_zero_bridge_20260512T154117Z"
DEFAULT_BIO = (
    DEFAULT_OUTPUT_ROOT / "pfr_remaining_bridge_stubs_20260512T154036Z" / "player_bio_pfr_bridge_zero_gap.parquet"
)


STAT_COLS = [
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "sack_yards_lost",
    "passing_long",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "rushing_long",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "receiving_long",
    "targets",
    "fumbles",
    "fumbles_lost",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_tds",
    "def_sacks",
    "def_tackles_with_assist",
    "def_tackles_solo",
    "def_tackle_assists",
    "fum_rec",
    "fum_rec_yds",
    "fum_ret_td",
    "def_fumbles_forced",
    "def_pass_defended",
    "def_tackles_for_loss",
    "def_qb_hits",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "kickoff_return_long",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "punt_return_long",
    "pat_made",
    "pat_att",
    "fg_made",
    "fg_att",
    "punts",
    "punt_yards",
    "punt_long",
]

CORE_SCORING_STATS = {
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "rushing_yards",
    "rushing_tds",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "fumbles_lost",
}
IDP_STATS = {
    "def_tackles_with_assist",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_sacks",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_tds",
    "fum_rec",
    "fum_rec_yds",
    "fum_ret_td",
    "def_fumbles_forced",
    "def_pass_defended",
    "def_tackles_for_loss",
    "def_qb_hits",
}
RETURN_STATS = {
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "kickoff_return_long",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "punt_return_long",
}
KICKING_STATS = {"pat_made", "pat_att", "fg_made", "fg_att"}
PUNTING_STATS = {"punts", "punt_yards", "punt_long"}
LONG_BONUS_STATS = {"passing_long", "rushing_long", "receiving_long", "kickoff_return_long", "punt_return_long"}
VOLUME_ENRICHMENT_STATS = {"completions", "attempts", "sacks_suffered", "sack_yards_lost", "targets", "fumbles"}


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def latest_weekly_reconciliation() -> Path:
    dirs = sorted(
        DEFAULT_OUTPUT_ROOT.glob("pfr_weekly_reconciliation_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in dirs:
        if (path / "pfr_weekly_reconciliation.parquet").exists():
            return path
    raise FileNotFoundError("No pfr_weekly_reconciliation_* directory found")


def available_columns(path: Path) -> set[str]:
    return set(pq.read_schema(path).names)


def load_reconciliation(recon_dir: Path) -> pd.DataFrame:
    path = recon_dir / "pfr_weekly_reconciliation.parquet"
    cols = [
        "weekly_bucket",
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "year",
        "week",
        "season_type",
        "season_type_source",
        "nfl_team",
        "opponent_nfl_team",
        "pfr_team_list",
        "pfr_opponent_list",
        "pfr_multi_game_week",
        "pfr_game_rows",
        "pfr_boxscores",
        "boxscore_id",
        "source_tables",
        "pfr_id",
        "pfr_id_norm",
        "bridge_classification",
        "confirmed_pfr_identity_action",
        "name_year_fallback_method",
        "stat_diff_count",
        "diff_cols",
        "context_same_family",
        "season_type_same",
        "live_exists",
        "game_date_min",
        "game_date_max",
        *STAT_COLS,
    ]
    cols = [col for col in cols if col in available_columns(path)]
    return pd.read_parquet(path, columns=cols)


def load_stat_diffs(recon_dir: Path) -> pd.DataFrame:
    path = recon_dir / "pfr_weekly_stat_diffs_long.csv"
    return pd.read_csv(path)


def load_franchise_history(audit_dir: Path) -> pd.DataFrame:
    path = audit_dir / "fly_nfl_franchise_history.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing franchise history cache: {path}")
    hist = pd.read_parquet(path)
    extra = pd.DataFrame(
        [
            {"nfl_franchise_number": 31, "abbrev": "LVR", "start_year": 2020, "end_year": 9999, "is_canonical": True},
            {"nfl_franchise_number": 14, "abbrev": "RAM", "start_year": 1936, "end_year": 1945, "is_canonical": True},
            {"nfl_franchise_number": 148, "abbrev": "BOS", "start_year": 1944, "end_year": 1945, "is_canonical": True},
        ]
    )
    hist = pd.concat([hist, extra], ignore_index=True)
    hist["abbrev"] = hist["abbrev"].astype("string").str.upper()
    hist["start_year"] = pd.to_numeric(hist["start_year"], errors="coerce").fillna(0).astype(int)
    hist["end_year"] = pd.to_numeric(hist["end_year"], errors="coerce").fillna(9999).astype(int)
    return hist


def franchise_number(hist: pd.DataFrame, abbrev: Any, year: Any) -> int | None:
    if abbrev is None or pd.isna(abbrev) or year is None or pd.isna(year):
        return None
    code = str(abbrev).strip().upper()
    season = int(year)
    candidates = hist[hist["abbrev"].eq(code) & hist["start_year"].le(season) & hist["end_year"].ge(season)].copy()
    if candidates.empty:
        return None
    candidates = candidates.sort_values(["is_canonical", "nfl_franchise_number"], ascending=[False, True])
    return int(candidates.iloc[0]["nfl_franchise_number"])


def franchise_lookup(hist: pd.DataFrame, min_year: int, max_year: int) -> dict[tuple[str, int], int]:
    """Expand the small franchise history table into an abbrev/year lookup."""
    hist = hist.copy()
    hist["is_canonical"] = hist["is_canonical"].fillna(False).astype(bool)
    hist = hist.sort_values(["is_canonical", "nfl_franchise_number"], ascending=[False, True])

    lookup: dict[tuple[str, int], int] = {}
    for row in hist.itertuples(index=False):
        start_year = max(int(row.start_year), min_year)
        end_year = min(int(row.end_year), max_year)
        if end_year < start_year:
            continue
        abbrev = str(row.abbrev).strip().upper()
        franchise_id = int(row.nfl_franchise_number)
        for season in range(start_year, end_year + 1):
            lookup.setdefault((abbrev, season), franchise_id)
    return lookup


def enrich_franchise_numbers(df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    seasons = pd.to_numeric(out["year"], errors="coerce")
    if seasons.notna().any():
        lookup = franchise_lookup(hist, int(seasons.min()), int(seasons.max()))
    else:
        lookup = {}

    def map_franchise_numbers(team_col: str) -> list[int | None]:
        teams = out[team_col].astype("string").str.strip().str.upper()
        values: list[int | None] = []
        for team, season in zip(teams, seasons, strict=False):
            if pd.isna(team) or pd.isna(season):
                values.append(None)
                continue
            values.append(lookup.get((str(team), int(season))))
        return values

    out["nfl_franchise_number"] = map_franchise_numbers("nfl_team")
    out["opponent_nfl_franchise_number"] = map_franchise_numbers("opponent_nfl_team")
    out["nfl_franchise_number"] = pd.to_numeric(out["nfl_franchise_number"], errors="coerce").astype("Int64")
    out["opponent_nfl_franchise_number"] = pd.to_numeric(out["opponent_nfl_franchise_number"], errors="coerce").astype(
        "Int64"
    )
    return out


def stat_action_category(stat: str) -> str:
    if stat in CORE_SCORING_STATS:
        return "core_offense_scoring"
    if stat in IDP_STATS:
        return "idp_scoring"
    if stat in RETURN_STATS:
        return "return_scoring"
    if stat in KICKING_STATS:
        return "kicking_scoring"
    if stat in PUNTING_STATS:
        return "punting"
    if stat in LONG_BONUS_STATS:
        return "long_bonus"
    if stat in VOLUME_ENRICHMENT_STATS:
        return "volume_enrichment"
    return "other"


def build_insert_rows(rec: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    rows = rec[rec["weekly_bucket"].eq("weekly_insert_ready")].copy()
    rows = enrich_franchise_numbers(rows, hist)
    rows["data_source"] = "pfr_boxscore_weekly_insert_stage"
    rows["pfr_update_action"] = "insert_ready"
    rows["pfr_update_source"] = "pfr_weekly_reconciliation"
    for col in STAT_COLS:
        if col not in rows.columns:
            rows[col] = 0.0
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0)
    output_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "year",
        "week",
        "season_type",
        "data_source",
        *STAT_COLS,
        "pfr_update_action",
        "pfr_update_source",
        "pfr_id",
        "pfr_id_norm",
        "bridge_classification",
        "confirmed_pfr_identity_action",
        "source_tables",
        "boxscore_id",
        "pfr_game_rows",
        "pfr_boxscores",
        "game_date_min",
        "game_date_max",
        "season_type_source",
    ]
    return rows[[col for col in output_cols if col in rows.columns]].copy()


def build_safe_update_rows(rec: pd.DataFrame) -> pd.DataFrame:
    rows = rec[rec["weekly_bucket"].eq("weekly_update_safe_stat_delta")].copy()
    rows["pfr_update_action"] = "safe_stat_overlay"
    rows["pfr_update_source"] = "pfr_weekly_reconciliation"
    for col in STAT_COLS:
        if col not in rows.columns:
            rows[col] = 0.0
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0)
    output_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "year",
        "week",
        "season_type",
        "nfl_team",
        "opponent_nfl_team",
        "stat_diff_count",
        "diff_cols",
        *STAT_COLS,
        "pfr_update_action",
        "pfr_update_source",
        "pfr_id",
        "pfr_id_norm",
        "bridge_classification",
        "confirmed_pfr_identity_action",
        "source_tables",
        "boxscore_id",
        "season_type_source",
    ]
    return rows[[col for col in output_cols if col in rows.columns]].copy()


def build_review_rows(rec: pd.DataFrame) -> pd.DataFrame:
    review_buckets = [
        "weekly_insert_multi_game_review",
        "weekly_update_multi_game_review",
        "weekly_update_context_review",
        "weekly_identity_manual_review",
    ]
    rows = rec[rec["weekly_bucket"].isin(review_buckets)].copy()
    output_cols = [
        "weekly_bucket",
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "year",
        "week",
        "season_type",
        "nfl_team",
        "opponent_nfl_team",
        "pfr_team_list",
        "pfr_opponent_list",
        "stat_diff_count",
        "diff_cols",
        "pfr_multi_game_week",
        "context_same_family",
        "season_type_same",
        "bridge_classification",
        "confirmed_pfr_identity_action",
        "source_tables",
        "boxscore_id",
    ]
    return rows[[col for col in output_cols if col in rows.columns]].copy()


def write_summary(
    output_dir: Path,
    insert_rows: pd.DataFrame,
    safe_updates: pd.DataFrame,
    safe_deltas: pd.DataFrame,
    review_rows: pd.DataFrame,
    rec: pd.DataFrame,
    recon_dir: Path,
) -> None:
    bucket_summary = rec["weekly_bucket"].value_counts().rename_axis("weekly_bucket").reset_index(name="rows")
    bucket_summary.to_csv(output_dir / "pfr_supertable_update_bucket_summary.csv", index=False)

    insert_summary = (
        insert_rows.groupby(["year", "season_type"], dropna=False)
        .size()
        .reset_index(name="insert_rows")
        .sort_values(["year", "season_type"])
    )
    insert_summary.to_csv(output_dir / "pfr_supertable_insert_scope_by_year.csv", index=False)

    safe_deltas["action_category"] = safe_deltas["stat"].map(stat_action_category)
    update_summary = (
        safe_deltas.groupby(["action_category", "stat"], dropna=False)
        .agg(
            delta_atoms=("player_week", "size"),
            weekly_rows=("player_week", "nunique"),
            abs_delta=("delta", lambda values: values.abs().sum()),
            min_year=("year", "min"),
            max_year=("year", "max"),
        )
        .reset_index()
        .sort_values(["delta_atoms", "stat"], ascending=[False, True])
    )
    update_summary.to_csv(output_dir / "pfr_supertable_safe_update_delta_summary.csv", index=False)

    recompute_weekly = pd.concat(
        [
            insert_rows[["year", "week", "season_type"]],
            safe_updates[["year", "week", "season_type"]],
        ],
        ignore_index=True,
    ).drop_duplicates()
    recompute_weekly.sort_values(["year", "week", "season_type"]).to_csv(
        output_dir / "pfr_supertable_weekly_recompute_scope.csv",
        index=False,
    )

    recompute_players = pd.concat(
        [
            insert_rows[["NFL_player_id", "player", "year"]],
            safe_updates[["NFL_player_id", "player", "year"]],
        ],
        ignore_index=True,
    )
    recompute_players = (
        recompute_players.groupby(["NFL_player_id", "player"], dropna=False)
        .agg(min_year=("year", "min"), max_year=("year", "max"), touched_years=("year", pd.Series.nunique))
        .reset_index()
        .sort_values(["touched_years", "NFL_player_id"], ascending=[False, True])
    )
    recompute_players.to_csv(output_dir / "pfr_supertable_player_recompute_scope.csv", index=False)

    review_rows.groupby(["weekly_bucket"], dropna=False).size().reset_index(name="rows").to_csv(
        output_dir / "pfr_supertable_review_queue_summary.csv",
        index=False,
    )

    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "reconciliation_dir": str(recon_dir),
        "insert_ready_rows": int(len(insert_rows)),
        "safe_update_rows": int(len(safe_updates)),
        "safe_update_delta_atoms": int(len(safe_deltas)),
        "review_rows": int(len(review_rows)),
        "output_dir": str(output_dir),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    top_updates = update_summary.head(20).to_string(index=False)
    markdown = [
        "# PFR Supertable Update Package",
        "",
        f"Created: {manifest['created_at_utc']}",
        f"Source reconciliation: `{recon_dir}`",
        "",
        "## Package Counts",
        "",
        f"- Insert-ready rows: {len(insert_rows):,}",
        f"- Safe existing-row stat overlays: {len(safe_updates):,}",
        f"- Safe update delta atoms: {len(safe_deltas):,}",
        f"- Review rows held out: {len(review_rows):,}",
        "",
        "## Top Safe Update Deltas",
        "",
        top_updates,
        "",
        "## Outputs",
        "",
        "- `pfr_supertable_insert_ready_rows.parquet`",
        "- `pfr_supertable_safe_stat_updates_wide.parquet`",
        "- `pfr_supertable_safe_stat_updates_long.parquet`",
        "- `pfr_supertable_review_rows.parquet`",
        "- `pfr_supertable_weekly_recompute_scope.csv`",
        "- `pfr_supertable_player_recompute_scope.csv`",
        "",
        "## Notes",
        "",
        "- Local-only package; no Fly writes.",
        "- Review buckets are intentionally held out from apply candidates.",
        "- Kicker field-goal distance buckets still require a separate distance parser before yardage-sensitive kicker scoring is final.",
    ]
    (output_dir / "PFR_SUPERTABLE_UPDATE_PACKAGE.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reconciliation-dir", type=Path, default=latest_weekly_reconciliation())
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--bio-path", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--force-output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.force_output_dir or (args.output_root / f"pfr_supertable_update_package_{now_stamp()}")
    output_dir.mkdir(parents=True, exist_ok=False)

    rec = load_reconciliation(args.reconciliation_dir)
    deltas = load_stat_diffs(args.reconciliation_dir)
    hist = load_franchise_history(args.audit_dir)

    insert_rows = build_insert_rows(rec, hist)
    safe_updates = build_safe_update_rows(rec)
    review_rows = build_review_rows(rec)
    safe_deltas = deltas[deltas["player_week"].isin(set(safe_updates["player_week"]))].copy()

    insert_rows.to_parquet(output_dir / "pfr_supertable_insert_ready_rows.parquet", index=False)
    safe_updates.to_parquet(output_dir / "pfr_supertable_safe_stat_updates_wide.parquet", index=False)
    safe_deltas.to_parquet(output_dir / "pfr_supertable_safe_stat_updates_long.parquet", index=False)
    review_rows.to_parquet(output_dir / "pfr_supertable_review_rows.parquet", index=False)

    # Convenience CSV samples for quick inspection without loading the full parquet.
    insert_rows.head(5000).to_csv(output_dir / "pfr_supertable_insert_ready_rows_sample.csv", index=False)
    safe_updates.head(5000).to_csv(output_dir / "pfr_supertable_safe_stat_updates_sample.csv", index=False)
    review_rows.head(5000).to_csv(output_dir / "pfr_supertable_review_rows_sample.csv", index=False)

    write_summary(output_dir, insert_rows, safe_updates, safe_deltas, review_rows, rec, args.reconciliation_dir)

    print(f"Wrote PFR supertable update package to {output_dir}")
    print(f"insert_ready_rows={len(insert_rows):,}")
    print(f"safe_update_rows={len(safe_updates):,}")
    print(f"safe_delta_atoms={len(safe_deltas):,}")
    print(f"review_rows={len(review_rows):,}")


if __name__ == "__main__":
    main()
