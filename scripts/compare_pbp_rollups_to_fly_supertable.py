#!/usr/bin/env python3
"""
Compare merged-PBP player rollups against the Fly ___ops supertable.

This script does not mutate Fly. It:
  1. loads local PBP-derived player rollups,
  2. downloads only explicit matching columns from ___ops.nfl_historical.nfl_player_stats_all,
  3. re-aggregates the supertable weekly rows to the same weekly/season/career levels,
  4. writes gap summaries for missing rows, missing columns, and PBP-nonzero/supertable-zero cells.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_ROLLUP_DIR = ORGANIZED_ROOT / "stathead_generated" / "pbp_supertable_audit_1978_2025"
DEFAULT_OUTPUT_DIR = ORGANIZED_ROOT / "_catalog" / "pbp_supertable_gap_audit_20260507"
SUPER_TABLE = "___ops.nfl_historical.nfl_player_stats_all"

STAT_COLUMNS = [
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
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
    "fumbles",
    "fumbles_lost",
    "fum_rec",
    "fum_rec_yds",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_ret_td",
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_blocked",
    "fg_yards",
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
    "punts",
    "punt_yards",
    "punt_long",
    "punts_blocked",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "special_teams_tds",
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
    "special_teams_tackles_solo",
]

MAX_COLUMNS = {"fg_long", "punt_long"}

COLUMN_ALIASES = {
    "fg_made_60plus": ["fg_made_60plus", "fg_made_60_plus", "fg_made_60_plus_canonical"],
    "punts_blocked": ["punts_blocked", "punt_blocked"],
    "kickoff_returns": ["kickoff_returns", "kick_returns"],
    "kickoff_return_yards": ["kickoff_return_yards", "kick_return_yards"],
    "kickoff_return_tds": ["kickoff_return_tds", "kick_return_tds"],
    "def_interceptions": ["def_interceptions", "def_int"],
    "def_interception_yards": ["def_interception_yards", "def_int_yards"],
    "def_int_ret_td": ["def_int_ret_td", "def_interception_tds", "def_int_tds"],
    "def_fumbles_forced": ["def_fumbles_forced", "forced_fumbles"],
    "def_blk_kick": ["def_blk_kick", "blocked_kicks"],
    "fum_rec_yds": ["fum_rec_yds", "fumble_recovery_yards"],
}

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

LEVEL_IDENTITY = {
    "weekly": [
        "NFL_player_id",
        "player_week",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
    ],
    "season_regular": ["NFL_player_id", "player", "position", "year"],
    "season_all": ["NFL_player_id", "player", "position", "year"],
    "career_regular": ["NFL_player_id", "player", "position"],
    "career_all": ["NFL_player_id", "player", "position"],
}


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


def choose_column_pairs(pbp_cols: list[str], fly_cols: set[str]) -> tuple[list[dict[str, str]], list[str]]:
    pairs: list[dict[str, str]] = []
    missing: list[str] = []
    used_fly_cols: set[str] = set()
    for pbp_col in pbp_cols:
        candidates = COLUMN_ALIASES.get(pbp_col, [pbp_col])
        if pbp_col not in candidates:
            candidates = [pbp_col, *candidates]
        selected = next((col for col in candidates if col in fly_cols and col not in used_fly_cols), None)
        if selected is None:
            missing.append(pbp_col)
            continue
        used_fly_cols.add(selected)
        pairs.append({"pbp_col": pbp_col, "fly_col": selected})
    return pairs, missing


def fetch_schema(reader: FlyReader) -> pd.DataFrame:
    sql = """
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = 'nfl_historical'
          AND table_name = 'nfl_player_stats_all'
        ORDER BY ordinal_position
    """
    return reader.query_df(sql, database="___ops")


def fetch_super_weekly(
    reader: FlyReader,
    schema_cols: set[str],
    pairs: list[dict[str, str]],
    years: list[int],
    output_dir: Path,
) -> pd.DataFrame:
    identity_candidates = [
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
    identity_cols = [col for col in identity_candidates if col in schema_cols]
    if "player_week" not in identity_cols:
        raise RuntimeError("Supertable is missing player_week; cannot compare weekly rollups.")
    if "year" not in identity_cols:
        raise RuntimeError("Supertable is missing year; cannot fetch by year.")

    stat_selects = [f"{q_ident(pair['fly_col'])} AS {q_ident(pair['pbp_col'])}" for pair in pairs]
    selects = [q_ident(col) for col in identity_cols] + stat_selects
    frames: list[pd.DataFrame] = []
    stat_cols = [pair["pbp_col"] for pair in pairs]
    for year in years:
        sql = f"""
            SELECT {", ".join(selects)}
            FROM {SUPER_TABLE}
            WHERE year = {int(year)}
              AND NFL_player_id IS NOT NULL
        """
        df = reader.query_df(sql, database="___ops")
        if df.empty:
            continue
        for col in stat_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df)
        print(f"[fly] {year}: {len(df):,} supertable rows", flush=True)
    if not frames:
        return pd.DataFrame(columns=identity_cols + [pair["pbp_col"] for pair in pairs])
    out = pd.concat(frames, ignore_index=True)
    out.to_parquet(output_dir / "fly_supertable_weekly_selected_1978_2025.parquet", index=False)
    return out


def numeric_frame(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for col in cols:
        out[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) if col in df.columns else 0.0
    return out


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


def aggregate_level(df: pd.DataFrame, level: str, stat_cols: list[str]) -> pd.DataFrame:
    if level == "weekly":
        keys = LEVEL_KEYS[level]
        source = df.copy()
    elif level == "season_regular":
        keys = LEVEL_KEYS[level]
        source = df[df.get("season_type", "").eq("REG")].copy()
    elif level == "season_all":
        keys = LEVEL_KEYS[level]
        source = df.copy()
    elif level == "career_regular":
        keys = LEVEL_KEYS[level]
        source = df[df.get("season_type", "").eq("REG")].copy()
    elif level == "career_all":
        keys = LEVEL_KEYS[level]
        source = df.copy()
    else:
        raise ValueError(level)

    for key in keys:
        if key not in source.columns:
            raise RuntimeError(f"Missing key {key} for {level}")
    source = source[source[keys].notna().all(axis=1)].copy()

    agg_map: dict[str, str] = {}
    for col in stat_cols:
        if col in source.columns:
            agg_map[col] = "max" if col in MAX_COLUMNS else "sum"
    if not agg_map:
        return source[keys].drop_duplicates().reset_index(drop=True)

    identity_cols = [
        col
        for col in [
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
        ]
        if col in source.columns and col not in keys
    ]
    grouped = source.groupby(keys, dropna=False).agg(agg_map).reset_index()
    if identity_cols:
        identity = source.groupby(keys, dropna=False)[identity_cols].first().reset_index()
        grouped = grouped.merge(identity, on=keys, how="left")
    return grouped


def pbp_activity(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    if not cols:
        return pd.Series(False, index=df.index)
    numeric = numeric_frame(df, [col for col in cols if col in df.columns])
    if numeric.empty:
        return pd.Series(False, index=df.index)
    return numeric.abs().sum(axis=1) > 1e-9


def compact_row_columns(level: str, df: pd.DataFrame) -> list[str]:
    preferred = LEVEL_IDENTITY[level] + [
        "pbp_player_id",
        "pbp_player_name",
        "event_roles",
        "pbp_source_systems",
        "player_id_namespaces",
    ]
    return [col for col in preferred if col in df.columns]


def compare_level(
    level: str,
    pbp: pd.DataFrame,
    super_df: pd.DataFrame,
    stat_cols: list[str],
    missing_schema_cols: list[str],
    output_dir: Path,
    detail_limit: int,
) -> dict[str, Any]:
    keys = LEVEL_KEYS[level]
    pbp = pbp[pbp[keys].notna().all(axis=1)].copy()
    super_df = super_df[super_df[keys].notna().all(axis=1)].copy()

    shared_cols = [col for col in stat_cols if col in pbp.columns and col in super_df.columns]
    pbp_only_cols = [col for col in stat_cols if col in pbp.columns and col not in shared_cols]

    if shared_cols:
        super_numeric = numeric_frame(super_df, shared_cols)
        super_df = pd.concat(
            [super_df.drop(columns=[c for c in shared_cols if c in super_df.columns]), super_numeric], axis=1
        )
    super_df = super_df.drop_duplicates(subset=keys, keep="first").copy()
    super_df["_fly_present"] = 1

    context_cols = [
        col
        for col in ["nfl_team", "opponent_nfl_team"]
        if level == "weekly" and col in pbp.columns and col in super_df.columns
    ]
    merge_cols = keys + ["_fly_present"] + context_cols + shared_cols
    merged = pbp.merge(super_df[merge_cols], on=keys, how="left", suffixes=("_pbp", "_super"))
    fly_present = merged["_fly_present"].fillna(0).astype(int).eq(1)
    present = fly_present
    if level == "weekly" and set(context_cols) == {"nfl_team", "opponent_nfl_team"}:
        pbp_team = canonical_team_code(merged.get("nfl_team_pbp"), merged.get("year"), merged.index)
        pbp_opp = canonical_team_code(merged.get("opponent_nfl_team_pbp"), merged.get("year"), merged.index)
        super_team = canonical_team_code(merged.get("nfl_team_super"), merged.get("year"), merged.index)
        super_opp = canonical_team_code(merged.get("opponent_nfl_team_super"), merged.get("year"), merged.index)
        context_known = pbp_team.ne("") & pbp_opp.ne("") & super_team.ne("") & super_opp.ne("")
        context_match = context_known & pbp_team.eq(super_team) & pbp_opp.eq(super_opp)
        context_mismatch = fly_present & context_known & ~context_match
        context_unknown = fly_present & ~context_known
        present = fly_present & context_match

        merged["nfl_team"] = merged["nfl_team_pbp"]
        merged["opponent_nfl_team"] = merged["opponent_nfl_team_pbp"]
        context_detail_cols = [
            col
            for col in [
                "NFL_player_id",
                "player_week",
                "player",
                "position",
                "year",
                "week",
                "season_type",
                "nfl_team_pbp",
                "opponent_nfl_team_pbp",
                "nfl_team_super",
                "opponent_nfl_team_super",
            ]
            if col in merged.columns
        ]
        mismatch_path = output_dir / "weekly_player_week_context_mismatches.csv"
        if context_mismatch.any():
            merged.loc[context_mismatch, context_detail_cols].to_csv(mismatch_path, index=False)
        else:
            pd.DataFrame(columns=context_detail_cols).to_csv(mismatch_path, index=False)
        pd.DataFrame(
            [
                {
                    "level": level,
                    "fly_present_rows": int(fly_present.sum()),
                    "context_match_rows": int((fly_present & context_match).sum()),
                    "context_mismatch_rows": int(context_mismatch.sum()),
                    "context_unknown_rows": int(context_unknown.sum()),
                }
            ]
        ).to_csv(output_dir / "weekly_player_week_context_summary.csv", index=False)

    all_pbp_stats = [col for col in stat_cols if col in pbp.columns]
    active = pbp_activity(merged, [f"{col}_pbp" if col in shared_cols else col for col in all_pbp_stats])
    missing_row_mask = ~present & active

    row_cols = compact_row_columns(level, merged)
    missing_rows = merged.loc[missing_row_mask, row_cols].copy()
    if not missing_rows.empty:
        missing_rows.to_csv(output_dir / f"{level}_missing_supertable_rows.csv", index=False)
    else:
        pd.DataFrame(columns=row_cols).to_csv(output_dir / f"{level}_missing_supertable_rows.csv", index=False)

    summaries: list[dict[str, Any]] = []
    details: list[pd.DataFrame] = []
    for col in shared_cols:
        pbp_col = f"{col}_pbp"
        super_col = f"{col}_super"
        p = pd.to_numeric(merged[pbp_col], errors="coerce").fillna(0.0)
        s = pd.to_numeric(merged[super_col], errors="coerce").fillna(0.0)
        delta = p - s
        pbp_nonzero = p.abs() > 1e-9
        super_nonzero = s.abs() > 1e-9
        zero_gap = pbp_nonzero & (~super_nonzero)
        mismatch = delta.abs() > 1e-9
        summaries.append(
            {
                "level": level,
                "pbp_col": col,
                "super_col": col,
                "status": "shared",
                "pbp_nonzero_rows": int(pbp_nonzero.sum()),
                "super_nonzero_rows": int(super_nonzero.sum()),
                "missing_row_nonzero_rows": int((pbp_nonzero & ~present).sum()),
                "super_zero_pbp_nonzero_rows": int((zero_gap & present).sum()),
                "mismatch_rows": int(mismatch.sum()),
                "pbp_total": float(p.sum()),
                "super_total": float(s.sum()),
                "delta_total": float(delta.sum()),
                "max_abs_delta": float(delta.abs().max()) if len(delta) else 0.0,
            }
        )
        gap_mask = zero_gap & present
        if gap_mask.any() and detail_limit > 0:
            detail_cols = compact_row_columns(level, merged)
            d = merged.loc[gap_mask, detail_cols].copy()
            d["level"] = level
            d["stat"] = col
            d["pbp_value"] = p[gap_mask].to_numpy()
            d["super_value"] = s[gap_mask].to_numpy()
            d["abs_gap"] = d["pbp_value"].abs()
            details.append(d.sort_values("abs_gap", ascending=False).head(detail_limit))

    for col in sorted(set(pbp_only_cols) | set(missing_schema_cols)):
        if col not in pbp.columns:
            continue
        p = pd.to_numeric(pbp[col], errors="coerce").fillna(0.0)
        summaries.append(
            {
                "level": level,
                "pbp_col": col,
                "super_col": None,
                "status": "missing_in_super_schema",
                "pbp_nonzero_rows": int((p.abs() > 1e-9).sum()),
                "super_nonzero_rows": None,
                "missing_row_nonzero_rows": None,
                "super_zero_pbp_nonzero_rows": None,
                "mismatch_rows": None,
                "pbp_total": float(p.sum()),
                "super_total": None,
                "delta_total": None,
                "max_abs_delta": None,
            }
        )

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(output_dir / f"{level}_column_gap_summary.csv", index=False)

    if details:
        detail_df = pd.concat(details, ignore_index=True)
        detail_df = detail_df.sort_values("abs_gap", ascending=False).head(detail_limit)
        detail_df.to_csv(output_dir / f"{level}_value_gap_examples.csv", index=False)
        detail_rows = len(detail_df)
    else:
        pd.DataFrame().to_csv(output_dir / f"{level}_value_gap_examples.csv", index=False)
        detail_rows = 0

    return {
        "level": level,
        "pbp_rows": int(len(pbp)),
        "super_rows": int(len(super_df)),
        "shared_stat_columns": len(shared_cols),
        "missing_schema_columns": len([col for col in missing_schema_cols if col in pbp.columns]),
        "missing_supertable_rows": int(missing_row_mask.sum()),
        "value_gap_example_rows": detail_rows,
    }


def write_readme(output_dir: Path, manifest: dict[str, Any]) -> None:
    column_summary = pd.read_csv(output_dir / "all_levels_column_gap_summary.csv")
    missing_cols = column_summary[column_summary["status"].eq("missing_in_super_schema")].copy()
    zero_gaps = column_summary[
        column_summary["status"].eq("shared") & column_summary["super_zero_pbp_nonzero_rows"].fillna(0).gt(0)
    ].copy()
    zero_gaps = zero_gaps.sort_values(["level", "super_zero_pbp_nonzero_rows"], ascending=[True, False])

    lines = [
        "# PBP vs Supertable Gap Audit",
        "",
        f"Generated: {manifest['generated_at_utc']}",
        f"Rollup dir: `{manifest['rollup_dir']}`",
        "",
        "## Level Summary",
        "",
    ]
    for level in manifest["levels"]:
        lines.append(
            f"- `{level['level']}`: PBP {level['pbp_rows']:,} rows, "
            f"supertable-derived {level['super_rows']:,} rows, "
            f"missing rows {level['missing_supertable_rows']:,}, "
            f"shared stats {level['shared_stat_columns']:,}, "
            f"missing stat columns {level['missing_schema_columns']:,}"
        )

    lines.extend(["", "## Biggest Shared-Column Zero Gaps", ""])
    if zero_gaps.empty:
        lines.append("_No shared-column zero gaps found._")
    else:
        cols = [
            "level",
            "pbp_col",
            "super_zero_pbp_nonzero_rows",
            "pbp_total",
            "super_total",
            "delta_total",
        ]
        lines.append(zero_gaps[cols].head(40).to_csv(index=False))

    lines.extend(["", "## Missing Supertable Columns", ""])
    if missing_cols.empty:
        lines.append("_No PBP rollup columns were missing from the supertable schema._")
    else:
        cols = ["level", "pbp_col", "pbp_nonzero_rows", "pbp_total"]
        lines.append(missing_cols[cols].drop_duplicates().to_csv(index=False))

    era_path = output_dir / "weekly_missing_rows_by_era.csv"
    if era_path.exists():
        lines.extend(["", "## Weekly Missing Rows By Era", ""])
        lines.append(pd.read_csv(era_path).to_csv(index=False))

    role_path = output_dir / "weekly_missing_rows_by_event_role.csv"
    if role_path.exists():
        lines.extend(["", "## Weekly Missing Rows By Event Role", ""])
        lines.append(pd.read_csv(role_path).head(30).to_csv(index=False))

    context_path = output_dir / "weekly_player_week_context_summary.csv"
    if context_path.exists():
        lines.extend(["", "## Weekly Player-Week Context Match", ""])
        lines.append(pd.read_csv(context_path).to_csv(index=False))

    (output_dir / "PBP_VS_SUPERTABLE_GAP_AUDIT.md").write_text("\n".join(lines), encoding="utf-8")


def write_weekly_missing_pivots(output_dir: Path) -> None:
    path = output_dir / "weekly_missing_supertable_rows.csv"
    if not path.exists():
        return
    missing = pd.read_csv(path)
    if missing.empty or "year" not in missing.columns:
        return

    missing["year"] = pd.to_numeric(missing["year"], errors="coerce")
    by_year = (
        missing.groupby("year", dropna=False)
        .size()
        .reset_index(name="missing_rows")
        .sort_values("missing_rows", ascending=False)
    )
    by_year.to_csv(output_dir / "weekly_missing_rows_by_year.csv", index=False)

    if "position" in missing.columns:
        (
            missing.groupby("position", dropna=False)
            .size()
            .reset_index(name="missing_rows")
            .sort_values("missing_rows", ascending=False)
            .to_csv(output_dir / "weekly_missing_rows_by_position.csv", index=False)
        )

    if "event_roles" in missing.columns:
        roles = missing.assign(event_roles=missing["event_roles"].fillna("").str.split(";")).explode("event_roles")
        (
            roles.groupby("event_roles", dropna=False)
            .size()
            .reset_index(name="missing_rows")
            .sort_values("missing_rows", ascending=False)
            .to_csv(output_dir / "weekly_missing_rows_by_event_role.csv", index=False)
        )

    eras = [
        ("1978-1993", 1978, 1993),
        ("1994-1998", 1994, 1998),
        ("1999-2025", 1999, 2025),
    ]
    era_rows = [
        {
            "era": label,
            "missing_rows": int(missing[missing["year"].between(start, end, inclusive="both")].shape[0]),
        }
        for label, start, end in eras
    ]
    pd.DataFrame(era_rows).to_csv(output_dir / "weekly_missing_rows_by_era.csv", index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rollup-dir", type=Path, default=DEFAULT_ROLLUP_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--season-min", type=int, default=1978)
    parser.add_argument("--season-max", type=int, default=2025)
    parser.add_argument("--detail-limit", type=int, default=10000)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()
    rollup_dir = args.rollup_dir
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.force and any(output_dir.iterdir()):
        raise FileExistsError(f"Output directory is not empty. Use --force: {output_dir}")

    pbp_rollups: dict[str, pd.DataFrame] = {}
    for level, filename in ROLLUP_FILES.items():
        path = rollup_dir / filename
        if not path.exists():
            raise FileNotFoundError(path)
        pbp_rollups[level] = pd.read_parquet(path)

    years = list(range(args.season_min, args.season_max + 1))
    pbp_stat_cols = [col for col in STAT_COLUMNS if col in pbp_rollups["weekly"].columns]

    reader = FlyReader()
    schema = fetch_schema(reader)
    schema.to_csv(output_dir / "fly_supertable_schema_columns.csv", index=False)
    schema_cols = set(schema["column_name"].astype(str))

    pairs, missing_schema_cols = choose_column_pairs(pbp_stat_cols, schema_cols)
    pd.DataFrame(pairs).to_csv(output_dir / "pbp_to_supertable_column_mapping.csv", index=False)
    pd.DataFrame({"pbp_col": missing_schema_cols}).to_csv(
        output_dir / "pbp_columns_missing_from_supertable_schema.csv", index=False
    )

    super_weekly_raw = fetch_super_weekly(reader, schema_cols, pairs, years, output_dir)
    shared_stat_cols = [pair["pbp_col"] for pair in pairs]
    super_levels = {
        "weekly": aggregate_level(super_weekly_raw, "weekly", shared_stat_cols),
        "season_regular": aggregate_level(super_weekly_raw, "season_regular", shared_stat_cols),
        "season_all": aggregate_level(super_weekly_raw, "season_all", shared_stat_cols),
        "career_regular": aggregate_level(super_weekly_raw, "career_regular", shared_stat_cols),
        "career_all": aggregate_level(super_weekly_raw, "career_all", shared_stat_cols),
    }
    for level, df in super_levels.items():
        df.to_parquet(output_dir / f"fly_supertable_derived_{level}.parquet", index=False)

    level_summaries = []
    for level, pbp_df in pbp_rollups.items():
        level_summary = compare_level(
            level,
            pbp_df,
            super_levels[level],
            pbp_stat_cols,
            missing_schema_cols,
            output_dir,
            args.detail_limit,
        )
        level_summaries.append(level_summary)

    all_column_summaries = []
    for level in ROLLUP_FILES:
        path = output_dir / f"{level}_column_gap_summary.csv"
        if path.exists():
            all_column_summaries.append(pd.read_csv(path))
    if all_column_summaries:
        pd.concat(all_column_summaries, ignore_index=True).to_csv(
            output_dir / "all_levels_column_gap_summary.csv", index=False
        )
    write_weekly_missing_pivots(output_dir)

    manifest = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rollup_dir": str(rollup_dir),
        "output_dir": str(output_dir),
        "super_table": SUPER_TABLE,
        "season_min": args.season_min,
        "season_max": args.season_max,
        "column_mappings": len(pairs),
        "missing_schema_columns": missing_schema_cols,
        "levels": level_summaries,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    write_readme(output_dir, manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
