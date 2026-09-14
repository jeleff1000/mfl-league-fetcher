#!/usr/bin/env python3
"""Stage residual same-name/context identity repairs after PBP audit.

This is non-mutating. It rebuilds a small set of confirmed rows from the local
PFR/Stathead source parquet files and writes a promotion bundle compatible with
scripts/apply_combined_identity_promotion_20260509.py.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
AUDIT_ROOT = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
OUT_DIR = AUDIT_ROOT / "residual_context_identity_stage_20260509"
SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
PFR_CACHE = ROOT / "fantasy_football_data" / "cache" / "pfr_excel"
PBP_ROLLUP = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025" / "pbp_player_week_rollup.parquet"
SCHEMA_PATH = AUDIT_ROOT / "stat_family_split_live_stage_20260509" / "live_schema_columns.csv"

SOURCE_SPECS = [
    {
        "source_file": "flx1980s.parquet",
        "pfr_player_id": "JoneKe00",
        "canonical_id": "JoneKe00",
        "player": "Keith Jones",
        "year": 1989,
        "position": "RB",
        "reason": "Browns Keith Jones rows were partially overwritten by Falcons Keith Jones context/stat lines.",
    },
    {
        "source_file": "flx1980s.parquet",
        "pfr_player_id": "JoneKe01",
        "canonical_id": "JoneKe01",
        "player": "Keith Jones",
        "year": 1989,
        "position": "RB",
        "reason": "Falcons Keith Jones is a different player from Browns Keith Jones.",
    },
    {
        "source_file": "flx 70s.parquet",
        "pfr_player_id": "WashGe00",
        "canonical_id": "WashGe00",
        "player": "Gene Washington",
        "year": 1979,
        "position": "WR",
        "reason": "Detroit Gene Washington rows were partially overwritten by a Giants Gene Washington context.",
    },
    {
        "source_file": "flx 70s.parquet",
        "pfr_player_id": "WashGe20",
        "canonical_id": "WashGe20",
        "player": "Gene Washington",
        "year": 1979,
        "position": "WR",
        "reason": "Giants Gene Washington is a separate two-game 1979 player.",
    },
    {
        "source_file": "kickers.parquet",
        "pfr_player_id": "davisgre01",
        "canonical_id": "00-0003985",
        "player": "Greg Davis",
        "year": 1989,
        "position": "K",
        "reason": "Greg Davis changed teams in 1989; one stale PFR-style key and several missing weeks remain.",
    },
]

DELETE_ONLY_KEYS = ["davisgre01_1989_10"]

PFR_TO_SUPER = {
    "Passing_Cmp": "completions",
    "Passing_Att": "attempts",
    "Passing_Yds": "passing_yards",
    "Passing_TD": "passing_tds",
    "Passing_Int": "passing_interceptions",
    "Passing_Sk": "sacks_suffered",
    "Rushing_Att": "carries",
    "Rushing_Yds": "rushing_yards",
    "Rushing_TD": "rushing_tds",
    "Receiving_Tgt": "targets",
    "Receiving_Rec": "receptions",
    "Receiving_Yds": "receiving_yards",
    "Receiving_TD": "receiving_tds",
    "Kick Returns_Ret": "kickoff_returns",
    "Kick Returns_Yds": "kickoff_return_yards",
    "Kick Returns_KRTD": "kickoff_return_tds",
    "Punt Returns_Ret": "punt_returns",
    "Punt Returns_Yds": "punt_return_yards",
    "Punt Returns_PRTD": "punt_return_tds",
    "XPM": "pat_made",
    "XPA": "pat_att",
    "FGM.1": "fg_made",
    "FGA": "fg_att",
}

POINT_PREFIXES = (
    "pts_",
    "fpts_",
    "rank_",
    "rolling_",
    "avg_pts_next_year_",
    "weighted_ppg_",
    "ppg_",
)


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_in(values: list[str]) -> str:
    return "(" + ",".join(q_lit(value) for value in values) + ")"


def fetch_schema(reader: Any) -> pd.DataFrame:
    schema = reader.query_df(
        """
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        ORDER BY ordinal_position
        """,
        database="___ops",
    )
    schema.to_csv(OUT_DIR / "live_schema_columns.csv", index=False)
    return schema


def fetch_live_rows(reader: Any, columns: list[str]) -> pd.DataFrame:
    target_ids = sorted({spec["canonical_id"] for spec in SOURCE_SPECS} | {"davisgre01"})
    target_players = sorted({spec["player"] for spec in SOURCE_SPECS})
    select_cols = ", ".join(q_ident(col) for col in columns)
    sql = f"""
        SELECT {select_cols}
        FROM {SUPER_TABLE}
        WHERE year IN (1979, 1989)
          AND (
            NFL_player_id IN {sql_in(target_ids)}
            OR player IN {sql_in(target_players)}
            OR player_week IN {sql_in(DELETE_ONLY_KEYS)}
          )
        ORDER BY player, NFL_player_id, year, week, player_week
    """
    return reader.query_df(sql, database="___ops")


def pfr_year_rows(spec: dict[str, Any]) -> pd.DataFrame:
    df = pd.read_parquet(PFR_CACHE / spec["source_file"])
    out = df[df["pfr_player_id"].astype(str).eq(spec["pfr_player_id"])].copy()
    out["__year"] = pd.to_datetime(out["Date"], errors="coerce").dt.year
    out = out[out["__year"].eq(spec["year"])].copy()
    out["__week"] = pd.to_numeric(out["Week"], errors="coerce").astype("Int64")
    return out.sort_values(["__year", "__week", "Date"], kind="stable")


def load_truth_sources() -> pd.DataFrame:
    rows = []
    for spec in SOURCE_SPECS:
        source = pfr_year_rows(spec)
        for _, row in source.iterrows():
            week = row["__week"]
            if pd.isna(week):
                continue
            item = row.to_dict()
            item.update(
                {
                    "__canonical_id": spec["canonical_id"],
                    "__source_pfr_id": spec["pfr_player_id"],
                    "__truth_player": spec["player"],
                    "__truth_position": spec["position"],
                    "__truth_reason": spec["reason"],
                    "__truth_source_file": spec["source_file"],
                }
            )
            rows.append(item)
    return pd.DataFrame(rows)


def stat_like_columns(columns: list[str]) -> set[str]:
    from multi_league.data_fetchers.player_identity_guards import (
        DEFENSE_STAT_COLUMNS,
        KICKING_STAT_COLUMNS,
        OFFENSE_STAT_COLUMNS,
        RETURN_STAT_COLUMNS,
    )

    base = set(OFFENSE_STAT_COLUMNS) | set(RETURN_STAT_COLUMNS) | set(KICKING_STAT_COLUMNS) | set(DEFENSE_STAT_COLUMNS)
    extras = {
        "punts",
        "punt_yards",
        "punt_long",
        "punts_blocked",
        "fg_missed",
        "fantasy_points_ppr",
    }
    return {col for col in columns if col in base or col in extras}


def template_for(
    live: pd.DataFrame,
    *,
    player: str,
    canonical_id: str,
    year: int,
    week: int,
) -> pd.Series:
    key = f"{canonical_id}_{year}_{week}"
    if "player_week" in live.columns:
        exact = live[live["player_week"].astype(str).eq(key)]
        if not exact.empty:
            return exact.iloc[0].copy()

    same_id = live[live["NFL_player_id"].astype(str).eq(canonical_id)]
    if not same_id.empty:
        return same_id.iloc[(pd.to_numeric(same_id["week"], errors="coerce") - week).abs().argsort().iloc[0]].copy()

    same_player = live[live["player"].astype(str).eq(player)]
    if not same_player.empty:
        return same_player.iloc[
            (pd.to_numeric(same_player["week"], errors="coerce") - week).abs().argsort().iloc[0]
        ].copy()

    raise ValueError(f"No template row available for {player} {canonical_id} {year} week {week}")


def numeric_value(value: Any) -> float:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return 0.0
    return float(number)


def pfr_position(row: pd.Series, fallback: str) -> str:
    for col in ("Pos.", "Punt Returns_Pos.", "Rushing_Pos.", "Receiving_Pos.", "Pos"):
        value = row.get(col)
        if pd.notna(value) and str(value).strip():
            return str(value).strip()
    return fallback


def age_years(value: Any) -> int | pd.NA:
    if pd.isna(value):
        return pd.NA
    text = str(value)
    if "-" in text:
        text = text.split("-", 1)[0]
    number = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    return pd.NA if pd.isna(number) else int(number)


def super_team_code(value: Any) -> Any:
    if pd.isna(value):
        return value
    code = str(value).strip()
    replacements = {
        "GNB": "GB",
        "KAN": "KC",
        "NWE": "NE",
        "NOR": "NO",
        "SFO": "SF",
        "TAM": "TB",
    }
    return replacements.get(code, code)


def load_pbp_rows(needed_keys: set[str], stat_cols: set[str]) -> pd.DataFrame:
    if not PBP_ROLLUP.exists():
        return pd.DataFrame()
    try:
        import pyarrow.parquet as pq

        available = set(pq.read_schema(PBP_ROLLUP).names)
    except Exception:
        available = set(pd.read_parquet(PBP_ROLLUP).columns)
    cols = [
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
        if col in available
    ]
    for col in sorted(stat_cols):
        if col in available and col not in cols:
            cols.append(col)
    pbp = pd.read_parquet(PBP_ROLLUP, columns=cols)
    return pbp[pbp["player_week"].astype(str).isin(needed_keys)].copy()


def build_truth_rows(live: pd.DataFrame, truth_sources: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    from multi_league.data_fetchers.fantasy_points_calculator import (
        calculate_all_fantasy_points,
        calculate_composite_fantasy_points,
    )

    stat_cols = stat_like_columns(columns)
    truth_keys = {
        f"{row['__canonical_id']}_{int(row['__year'])}_{int(row['__week'])}" for _, row in truth_sources.iterrows()
    }
    pbp = load_pbp_rows(truth_keys, stat_cols)
    pbp_by_key = (
        pbp.drop_duplicates("player_week", keep="first").set_index("player_week") if not pbp.empty else pd.DataFrame()
    )

    repaired_rows: list[pd.Series] = []
    for _, source in truth_sources.iterrows():
        canonical_id = str(source["__canonical_id"])
        player = str(source["__truth_player"])
        year = int(source["__year"])
        week = int(source["__week"])
        player_week = f"{canonical_id}_{year}_{week}"
        row = template_for(live, player=player, canonical_id=canonical_id, year=year, week=week)

        for col in stat_cols:
            if col in row.index:
                row[col] = 0.0
        for col in row.index:
            if str(col).startswith(POINT_PREFIXES):
                row[col] = pd.NA

        row["NFL_player_id"] = canonical_id
        row["player_week"] = player_week
        row["player"] = player
        row["year"] = year
        row["week"] = week
        row["season_type"] = "REG"
        row["nfl_team"] = super_team_code(source.get("Team"))
        row["opponent_nfl_team"] = super_team_code(source.get("Opp"))
        position = pfr_position(source, str(source["__truth_position"]))
        for pos_col in ("position", "nfl_position", "fantasy_position"):
            if pos_col in row.index:
                row[pos_col] = position
        if "age" in row.index:
            row["age"] = age_years(source.get("Age"))
        if "pts" in row.index and pd.notna(source.get("Pts")):
            row["pts"] = str(int(numeric_value(source.get("Pts"))))

        for pfr_col, target_col in PFR_TO_SUPER.items():
            if pfr_col in source.index and target_col in row.index:
                row[target_col] = numeric_value(source.get(pfr_col))
        if "fg_att" in row.index and "fg_missed" in row.index:
            row["fg_missed"] = max(0.0, numeric_value(row["fg_att"]) - numeric_value(row.get("fg_made")))
        if "pat_att" in row.index and "pat_missed" in row.index:
            row["pat_missed"] = max(0.0, numeric_value(row["pat_att"]) - numeric_value(row.get("pat_made")))

        if not pbp_by_key.empty and player_week in pbp_by_key.index:
            pbp_row = pbp_by_key.loc[player_week]
            pfr_targets = {
                target_col
                for pfr_col, target_col in PFR_TO_SUPER.items()
                if pfr_col in source.index and target_col in row.index
            } | {"fg_missed", "pat_missed"}
            for col in stat_cols:
                # PFR game-log columns remain authoritative for box-score
                # counts. PBP fills derived atoms absent from those game logs
                # such as FG distance buckets, reception bands, and fumbles.
                if col in pfr_targets:
                    continue
                if col in row.index and col in pbp_row.index:
                    row[col] = numeric_value(pbp_row.get(col))

        repaired_rows.append(row)

    repaired = pd.DataFrame(repaired_rows).reindex(columns=columns)
    repaired = calculate_all_fantasy_points(repaired)
    repaired = calculate_composite_fantasy_points(repaired)
    return repaired.reindex(columns=columns)


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    stats = [
        "carries",
        "rushing_yards",
        "rushing_tds",
        "targets",
        "receptions",
        "receiving_yards",
        "kickoff_returns",
        "kickoff_return_yards",
        "fg_att",
        "fg_made",
        "pat_att",
        "pat_made",
        "fpts_4pt_half",
        "pts_k_yds",
    ]
    out = df.copy()
    for col in stats:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return (
        out.groupby(["player", "NFL_player_id", "position", "nfl_team", "year"], dropna=False)
        .agg(rows=("player_week", "size"), **{col: (col, "sum") for col in stats})
        .reset_index()
        .sort_values(["player", "year", "NFL_player_id", "nfl_team"], kind="stable")
    )


def rank_scope(promotion: pd.DataFrame, delete_keys: list[str]) -> pd.DataFrame:
    affected = promotion[["NFL_player_id", "year", "week", "nfl_position"]].copy()
    rank_cols = [col for col in promotion.columns if str(col).startswith("rank_")]
    weekly_cols = [
        col for col in rank_cols if not str(col).startswith("rank_season_") and not str(col).startswith("rank_alltime_")
    ]
    season_cols = [col for col in rank_cols if str(col).startswith("rank_season_")]
    alltime_cols = [col for col in rank_cols if str(col).startswith("rank_alltime_")]
    rows = []
    for year, group in affected.groupby("year", dropna=True):
        year_int = int(year)
        weeks = sorted(pd.to_numeric(group["week"], errors="coerce").dropna().astype(int).unique())
        positions = sorted({str(v) for v in group["nfl_position"].dropna().unique() if str(v)})
        for week in weeks:
            rows.append(
                {
                    "scope": "weekly",
                    "year": year_int,
                    "week": week,
                    "positions": ",".join(positions),
                    "rank_columns": ",".join(weekly_cols),
                    "reason": "residual context identity repairs affect weekly rank pools",
                }
            )
        rows.append(
            {
                "scope": "season",
                "year": year_int,
                "week": "",
                "positions": ",".join(positions),
                "rank_columns": ",".join(season_cols),
                "reason": "residual context identity repairs affect season totals/ranks",
            }
        )
    rows.append(
        {
            "scope": "alltime",
            "year": "",
            "week": "",
            "positions": ",".join(sorted({str(v) for v in affected["nfl_position"].dropna().unique() if str(v)})),
            "rank_columns": ",".join(alltime_cols),
            "reason": "residual context identity repairs affect all-time totals/ranks",
        }
    )
    return pd.DataFrame(rows)


def write_markdown(manifest: dict[str, Any], before: pd.DataFrame, after: pd.DataFrame) -> None:
    lines = [
        "# Residual Context Identity Stage",
        "",
        "Non-mutating stage for the remaining confirmed duplicate-name/context repairs.",
        "",
        f"Generated at: {manifest['generated_at_utc']}",
        "",
        "## Counts",
        "",
        f"- Live affected rows: {manifest['live_rows']:,}",
        f"- Truth source rows: {manifest['truth_source_rows']:,}",
        f"- Promotion rows: {manifest['promotion_rows']:,}",
        f"- Delete player_weeks: {manifest['delete_player_weeks']:,}",
        f"- Existing live delete keys found: {manifest['live_delete_keys_found']:,}",
        f"- Duplicate promotion player_week keys: {manifest['duplicate_promotion_player_week']:,}",
        "",
        "## Before Summary",
        "",
        before.to_csv(index=False).strip(),
        "",
        "## After Summary",
        "",
        after.to_csv(index=False).strip(),
        "",
        "## Files",
        "",
        f"- Promotion rows: `{manifest['combined_promotion_rows_path']}`",
        f"- Delete keys: `{manifest['delete_player_weeks_path']}`",
        f"- Metric scope: `{manifest['metric_recompute_scope_path']}`",
        f"- Rank scope: `{manifest['rank_recompute_scope_path']}`",
    ]
    (OUT_DIR / "RESIDUAL_CONTEXT_IDENTITY_STAGE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    load_env()

    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()
    schema = fetch_schema(reader)
    columns = schema["column_name"].astype(str).tolist()
    live = fetch_live_rows(reader, columns)
    live.to_parquet(OUT_DIR / "live_affected_rows_full.parquet", index=False)
    before = summarize(live)
    before.to_csv(OUT_DIR / "live_affected_summary.csv", index=False)

    truth_sources = load_truth_sources()
    truth_sources.to_csv(OUT_DIR / "truth_source_rows.csv", index=False)
    promotion = build_truth_rows(live, truth_sources, columns)
    promotion.to_parquet(OUT_DIR / "combined_promotion_rows_full.parquet", index=False)
    after = summarize(promotion)
    after.to_csv(OUT_DIR / "promotion_summary.csv", index=False)

    live_keys = set(live["player_week"].dropna().astype(str))
    promotion_keys = set(promotion["player_week"].dropna().astype(str))
    delete_keys = sorted((live_keys & promotion_keys) | (set(DELETE_ONLY_KEYS) & live_keys))
    pd.DataFrame({"player_week": delete_keys}).to_csv(OUT_DIR / "delete_player_weeks.csv", index=False)

    metric_scope = pd.DataFrame(
        {
            "NFL_player_id": sorted(set(promotion["NFL_player_id"].dropna().astype(str))),
            "reason": "residual_context_identity_repair",
        }
    )
    metric_scope.to_csv(OUT_DIR / "metric_recompute_scope.csv", index=False)
    rank_df = rank_scope(promotion, delete_keys)
    rank_df.to_csv(OUT_DIR / "rank_recompute_scope.csv", index=False)

    manifest = {
        "generated_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "live_rows": int(len(live)),
        "truth_source_rows": int(len(truth_sources)),
        "promotion_rows": int(len(promotion)),
        "delete_player_weeks": int(len(delete_keys)),
        "live_delete_keys_found": int(len(delete_keys)),
        "new_insert_player_weeks": int(len(promotion_keys - live_keys)),
        "duplicate_promotion_player_week": int(promotion["player_week"].duplicated().sum()),
        "affected_player_ids": sorted(set(promotion["NFL_player_id"].dropna().astype(str))),
        "delete_only_keys": sorted(set(DELETE_ONLY_KEYS) & live_keys),
        "combined_promotion_rows_path": str(OUT_DIR / "combined_promotion_rows_full.parquet"),
        "delete_player_weeks_path": str(OUT_DIR / "delete_player_weeks.csv"),
        "metric_recompute_scope_path": str(OUT_DIR / "metric_recompute_scope.csv"),
        "rank_recompute_scope_path": str(OUT_DIR / "rank_recompute_scope.csv"),
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(manifest, before, after)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
