#!/usr/bin/env python3
"""Combine confirmed identity-repair staging artifacts into one promotion plan.

This script is deliberately non-mutating. It reads the already-staged full-width
repair rows, builds the exact live player_week delete set, validates shape/key
safety, and writes one combined dry-run package for a later Fly promotion.
"""

from __future__ import annotations

import datetime as dt
import json
import warnings
from pathlib import Path
from typing import Any

import pandas as pd


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_ROOT = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
STAT_STAGE = AUDIT_ROOT / "stat_family_split_live_stage_20260509"
SPECIAL_STAGE = AUDIT_ROOT / "special_teams_identity_stage_20260509"
OUT_DIR = AUDIT_ROOT / "combined_identity_promotion_stage_20260509"

FPTS_VARIANTS = [
    "4pt_0ppr",
    "4pt_half",
    "4pt_ppr",
    "5pt_0ppr",
    "5pt_half",
    "5pt_ppr",
    "6pt_0ppr",
    "6pt_half",
    "6pt_ppr",
    "4pt_tep",
    "5pt_tep",
    "6pt_tep",
]

POSITION_RANK_COLUMNS = {
    "QB": ["rank_qb_4pt", "rank_qb_5pt", "rank_qb_6pt"],
    "RB": ["rank_rb_0ppr", "rank_rb_half", "rank_rb_ppr"],
    "WR": ["rank_wr_0ppr", "rank_wr_half", "rank_wr_ppr"],
    "TE": ["rank_te_0ppr", "rank_te_half", "rank_te_ppr", "rank_te_tep"],
    "K": ["rank_k"],
    "DEF": ["rank_def"],
}

IDP_POS_GROUPS = {
    "LB": {"LB", "ILB", "OLB", "MLB"},
    "DL": {"DL", "DE", "DT", "NT", "ED"},
    "DB": {"DB", "CB", "S", "SS", "FS", "SAF"},
}


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_column(path: Path, column: str) -> set[str]:
    if not path.exists():
        return set()
    df = pd.read_csv(path)
    if column not in df.columns:
        return set()
    return set(df[column].dropna().astype(str))


def load_stage_rows(path: Path, source: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = df.copy()
    df["_promotion_source"] = source
    return df


def concat_nonempty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    nonempty = [frame for frame in frames if not frame.empty]
    if not nonempty:
        return pd.DataFrame()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated",
            category=FutureWarning,
        )
        return pd.concat(nonempty, ignore_index=True, sort=False)


def coerce_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("player_week", "player", "NFL_player_id", "position", "nfl_position", "nfl_team", "opponent_nfl_team"):
        if col in out.columns:
            out[col] = out[col].astype("string")
    for col in ("year", "week"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    return out


def schema_columns() -> list[str]:
    schema = pd.read_csv(STAT_STAGE / "live_schema_columns.csv")
    return schema["column_name"].astype(str).tolist()


def align_to_schema(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    missing = [col for col in columns if col not in out.columns]
    for col in missing:
        out[col] = pd.NA
    return out[columns]


def live_delete_rows(delete_keys: set[str]) -> pd.DataFrame:
    frames = []
    for stage in (STAT_STAGE, SPECIAL_STAGE):
        path = stage / "live_affected_rows_full.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    live = concat_nonempty(frames)
    live = live.drop_duplicates("player_week", keep="first")
    return live[live["player_week"].astype(str).isin(delete_keys)].copy()


def rank_columns_for_position(position: str) -> list[str]:
    pos = str(position or "").upper()
    cols = list(POSITION_RANK_COLUMNS.get(pos, []))
    for group, members in IDP_POS_GROUPS.items():
        if pos in members:
            prefix = f"rank_{group.lower()}"
            cols.extend(
                [
                    f"{prefix}_std",
                    f"{prefix}_premium",
                    f"{prefix}_tackle_heavy",
                    f"{prefix}_big_play",
                ]
            )
            break
    if pos in {"RB", "WR", "TE"}:
        cols.extend(["rank_flex_0ppr", "rank_flex_half", "rank_flex_ppr", "rank_flex_tep"])
    if pos in {"WR", "TE"}:
        cols.extend(["rank_recflex_0ppr", "rank_recflex_half", "rank_recflex_ppr", "rank_recflex_tep"])
    if pos in {"RB", "WR"}:
        cols.extend(["rank_wrflex_0ppr", "rank_wrflex_half", "rank_wrflex_ppr"])
    if pos in {"RB", "TE"}:
        cols.extend(["rank_rtflex_0ppr", "rank_rtflex_half", "rank_rtflex_ppr"])
    if pos in {"QB", "RB", "WR", "TE"}:
        for td in ("4pt", "5pt", "6pt"):
            for ppr in ("0ppr", "half", "ppr"):
                cols.append(f"rank_sflex_{td}_{ppr}")
    if any(pos in members for members in IDP_POS_GROUPS.values()):
        cols.extend(
            [
                "rank_idp_flex_std",
                "rank_idp_flex_premium",
                "rank_idp_flex_tackle_heavy",
                "rank_idp_flex_big_play",
            ]
        )
    return sorted(set(cols))


def rank_scope(promotion_rows: pd.DataFrame, delete_rows: pd.DataFrame) -> pd.DataFrame:
    rows = []
    scope_source = concat_nonempty([promotion_rows, delete_rows])
    scope_source = coerce_key_columns(scope_source)
    if "nfl_position" not in scope_source.columns and "position" in scope_source.columns:
        scope_source["nfl_position"] = scope_source["position"]
    scope_source["rank_pos"] = scope_source.get("nfl_position", scope_source.get("position")).fillna("")

    weekly = scope_source.dropna(subset=["year", "week"]).copy()
    for (year, week), group in weekly.groupby(["year", "week"], dropna=True):
        positions = sorted({str(v) for v in group["rank_pos"].dropna() if str(v)})
        cols = sorted(
            {
                col
                for pos in positions
                for col in rank_columns_for_position(pos)
                if not col.startswith("rank_season_") and not col.startswith("rank_alltime_")
            }
        )
        rows.append(
            {
                "scope": "weekly",
                "year": int(year),
                "week": int(week),
                "positions": ",".join(positions),
                "rank_columns": ",".join(cols),
                "reason": "affected raw/fantasy row can change weekly position/flex ranks for this game week",
            }
        )

    season = scope_source.dropna(subset=["year"]).copy()
    for year, group in season.groupby("year", dropna=True):
        positions = sorted({str(v) for v in group["rank_pos"].dropna() if str(v)})
        cols = sorted(
            {col.replace("rank_", "rank_season_", 1) for pos in positions for col in rank_columns_for_position(pos)}
        )
        rows.append(
            {
                "scope": "season",
                "year": int(year),
                "week": "",
                "positions": ",".join(positions),
                "rank_columns": ",".join(cols),
                "reason": "affected player season totals can change season position/flex ranks for this year",
            }
        )

    positions = sorted({str(v) for v in scope_source["rank_pos"].dropna() if str(v)})
    cols = sorted(
        {col.replace("rank_", "rank_alltime_", 1) for pos in positions for col in rank_columns_for_position(pos)}
    )
    rows.append(
        {
            "scope": "alltime",
            "year": "",
            "week": "",
            "positions": ",".join(positions),
            "rank_columns": ",".join(cols),
            "reason": "affected player career totals can change all-time position/flex ranks",
        }
    )
    return pd.DataFrame(rows)


def metric_scope(promotion_rows: pd.DataFrame, delete_rows: pd.DataFrame) -> pd.DataFrame:
    scope_source = concat_nonempty([promotion_rows, delete_rows])
    scope_source = coerce_key_columns(scope_source)
    rows = []
    ids = sorted({str(v) for v in scope_source.get("NFL_player_id", pd.Series(dtype=object)).dropna() if str(v)})
    for player_id in ids:
        group = scope_source[scope_source["NFL_player_id"].astype(str).eq(player_id)]
        years = sorted(pd.to_numeric(group.get("year"), errors="coerce").dropna().astype(int).unique())
        weeks = sorted(pd.to_numeric(group.get("week"), errors="coerce").dropna().astype(int).unique())
        rows.append(
            {
                "NFL_player_id": player_id,
                "players": ",".join(sorted({str(v) for v in group.get("player", pd.Series(dtype=object)).dropna()})),
                "years_touched": ",".join(str(year) for year in years),
                "weeks_touched": ",".join(str(week) for week in weeks),
                "fpts_columns": ",".join(f"fpts_{suffix}" for suffix in FPTS_VARIANTS),
                "ppg_columns": ",".join(
                    [f"ppg_season_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"ppg_alltime_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"rolling_3_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"rolling_5_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"rolling_total_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"weighted_ppg_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"consistency_{suffix}" for suffix in FPTS_VARIANTS]
                    + [f"avg_pts_next_year_{suffix}" for suffix in FPTS_VARIANTS]
                    + ["rolling_total_def", "rolling_total_k"]
                ),
                "reason": "PPG/rolling/consistency columns are player-career or player-season dependent, not row-local",
            }
        )
    return pd.DataFrame(rows)


def write_markdown(manifest: dict[str, Any], summary: pd.DataFrame, validation: pd.DataFrame) -> None:
    lines = [
        "# Combined Identity Promotion Dry Run",
        "",
        "This is a non-mutating package that combines the confirmed identity repair staging artifacts.",
        "",
        f"Generated at: {manifest['generated_at_utc']}",
        "",
        "## Counts",
        "",
        f"- Promotion rows: {manifest['promotion_rows']:,}",
        f"- Delete player_week keys: {manifest['delete_player_weeks']:,}",
        f"- Live delete keys found in local stage artifacts: {manifest['live_delete_keys_found']:,}",
        f"- New inserted player_week keys: {manifest['new_insert_player_weeks']:,}",
        f"- Duplicate promotion player_week keys: {manifest['duplicate_promotion_player_week']:,}",
        f"- Promotion rows missing schema columns: {manifest['missing_schema_columns']:,}",
        "",
        "## Source Summary",
        "",
        summary.to_csv(index=False).strip(),
        "",
        "## Validation",
        "",
        validation.to_csv(index=False).strip(),
        "",
        "## Important",
        "",
        "The promotion rows have raw `pts_*` and `fpts_*` columns recomputed, but this package still requires a promotion-time refresh of PPG, rolling totals, weekly ranks, season ranks, and all-time ranks.",
        "",
        "## Files",
        "",
        f"- Promotion rows: `{manifest['promotion_rows_path']}`",
        f"- Annotated promotion rows: `{manifest['promotion_rows_annotated_path']}`",
        f"- Delete keys: `{manifest['delete_player_weeks_path']}`",
        f"- Live delete rows: `{manifest['live_delete_rows_path']}`",
        f"- Rank scope: `{manifest['rank_scope_path']}`",
        f"- Metric scope: `{manifest['metric_scope_path']}`",
    ]
    (OUT_DIR / "COMBINED_IDENTITY_PROMOTION_DRY_RUN.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    stat_manifest = load_manifest(STAT_STAGE / "manifest.json")
    special_manifest = load_manifest(SPECIAL_STAGE / "manifest.json")
    columns = schema_columns()

    stat_rows = load_stage_rows(Path(stat_manifest["staged_rows_to_promote_path"]), "stat_family_split")
    special_rows = load_stage_rows(Path(special_manifest["staged_rows_to_promote_path"]), "special_teams_identity")
    annotated = concat_nonempty([stat_rows, special_rows])
    annotated = coerce_key_columns(annotated)

    promotion_keys = set(annotated["player_week"].dropna().astype(str))
    stat_delete = read_csv_column(STAT_STAGE / "live_player_weeks_to_replace.csv", "player_week")
    special_live = pd.read_parquet(SPECIAL_STAGE / "live_affected_rows_full.parquet")
    special_delete = set(special_live["player_week"].dropna().astype(str)) & promotion_keys
    delete_keys = sorted(stat_delete | special_delete)

    delete_rows = live_delete_rows(set(delete_keys))
    missing_delete_keys = sorted(
        set(delete_keys) - set(delete_rows.get("player_week", pd.Series(dtype=object)).dropna().astype(str))
    )
    schema_missing = [col for col in columns if col not in annotated.columns]

    promotion_rows = align_to_schema(annotated.drop(columns=["_promotion_source"]), columns)

    annotated.to_parquet(OUT_DIR / "combined_promotion_rows_annotated.parquet", index=False)
    promotion_rows.to_parquet(OUT_DIR / "combined_promotion_rows_full.parquet", index=False)
    pd.DataFrame({"player_week": delete_keys}).to_csv(OUT_DIR / "delete_player_weeks.csv", index=False)
    delete_rows.to_parquet(OUT_DIR / "live_delete_rows_full.parquet", index=False)

    summary = (
        annotated.groupby(["_promotion_source", "player", "NFL_player_id", "position"], dropna=False)
        .agg(
            rows=("player_week", "size"),
            years=(
                "year",
                lambda s: ",".join(map(str, sorted(set(pd.to_numeric(s, errors="coerce").dropna().astype(int))))),
            ),
        )
        .reset_index()
        .sort_values(["_promotion_source", "player", "NFL_player_id", "position"], kind="stable")
    )
    summary.to_csv(OUT_DIR / "combined_promotion_summary.csv", index=False)

    validation = pd.DataFrame(
        [
            {
                "check": "duplicate_promotion_player_week",
                "value": int(annotated["player_week"].duplicated().sum()),
                "pass": int(annotated["player_week"].duplicated().sum()) == 0,
            },
            {
                "check": "missing_delete_keys_in_local_live_artifacts",
                "value": len(missing_delete_keys),
                "pass": len(missing_delete_keys) == 0,
            },
            {
                "check": "promotion_rows_without_player_week",
                "value": int(annotated["player_week"].isna().sum()),
                "pass": int(annotated["player_week"].isna().sum()) == 0,
            },
            {
                "check": "promotion_rows_missing_schema_columns",
                "value": len(schema_missing),
                "pass": len(schema_missing) == 0,
            },
        ]
    )
    validation.to_csv(OUT_DIR / "dry_run_validation.csv", index=False)
    if missing_delete_keys:
        pd.DataFrame({"player_week": missing_delete_keys}).to_csv(OUT_DIR / "missing_delete_keys.csv", index=False)

    rank_df = rank_scope(annotated, delete_rows)
    metric_df = metric_scope(annotated, delete_rows)
    rank_df.to_csv(OUT_DIR / "rank_recompute_scope.csv", index=False)
    metric_df.to_csv(OUT_DIR / "metric_recompute_scope.csv", index=False)

    new_keys = promotion_keys - set(delete_rows.get("player_week", pd.Series(dtype=object)).dropna().astype(str))
    manifest = {
        "generated_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "promotion_rows": int(len(promotion_rows)),
        "delete_player_weeks": int(len(delete_keys)),
        "live_delete_keys_found": int(len(delete_rows)),
        "new_insert_player_weeks": int(len(new_keys)),
        "duplicate_promotion_player_week": int(annotated["player_week"].duplicated().sum()),
        "missing_schema_columns": int(len(schema_missing)),
        "missing_delete_keys": int(len(missing_delete_keys)),
        "affected_player_ids": int(metric_df["NFL_player_id"].nunique()) if not metric_df.empty else 0,
        "rank_scope_rows": int(len(rank_df)),
        "promotion_rows_path": str(OUT_DIR / "combined_promotion_rows_full.parquet"),
        "promotion_rows_annotated_path": str(OUT_DIR / "combined_promotion_rows_annotated.parquet"),
        "delete_player_weeks_path": str(OUT_DIR / "delete_player_weeks.csv"),
        "live_delete_rows_path": str(OUT_DIR / "live_delete_rows_full.parquet"),
        "rank_scope_path": str(OUT_DIR / "rank_recompute_scope.csv"),
        "metric_scope_path": str(OUT_DIR / "metric_recompute_scope.csv"),
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_markdown(manifest, summary, validation)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
