"""PBP-derived scoring columns for super-table builds.

NFLverse weekly player stats already carry the common fantasy aggregates, but a
few platform scoring rules need play-level context: 50+ completions, reception
yardage buckets, and special-teams solo tackles.  This module derives those
columns from nflverse play-by-play data and merges them onto player/week rows.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PBP_SCORING_COLUMNS = (
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
    "special_teams_tackles_solo",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_rec_yds",
)

PBP_ROLLUP_COLUMNS = (
    "completions_50plus",
    "receptions_0_4",
    "receptions_5_9",
    "receptions_10_19",
    "receptions_20_29",
    "receptions_30_39",
    "special_teams_tackles_solo",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
)

DEFAULT_PBP_SCORING_ROLLUP = (
    Path(__file__).resolve().parents[3]
    / "fantasy_football_data"
    / "cache"
    / "nflverse"
    / "scoring_events"
    / "rollups"
    / "pbp_scoring_player_week_rollup.parquet"
)


def ensure_pbp_scoring_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure PBP-derived scoring columns exist, zero-filled when unavailable."""
    out = df.copy()
    for col in PBP_SCORING_COLUMNS:
        if col not in out.columns:
            out[col] = 0.0
    return out


def _series(df: pd.DataFrame, col: str, default=0) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(default, index=df.index)


def _player_id_col(df: pd.DataFrame) -> str | None:
    for col in ("NFL_player_id", "player_id", "gsis_id"):
        if col in df.columns:
            return col
    return None


def _year_col(df: pd.DataFrame) -> str | None:
    for col in ("year", "season"):
        if col in df.columns:
            return col
    return None


def _load_pbp(year: int, cache_dir: Path | None, use_cache: bool) -> pd.DataFrame:
    from multi_league.data_fetchers.defense_stats import fetch_nflverse_pbp_data

    return fetch_nflverse_pbp_data(year, cache_dir=cache_dir, use_cache=use_cache)


def _prep_left_keys(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]] | None:
    player_col = _player_id_col(df)
    year_col = _year_col(df)
    if not player_col or not year_col or "week" not in df.columns:
        return None

    out = df.copy()
    out["__pbp_year"] = pd.to_numeric(out[year_col], errors="coerce").astype("Int64")
    out["__pbp_week"] = pd.to_numeric(out["week"], errors="coerce").astype("Int64")
    out["__pbp_player_id"] = out[player_col].astype("string")
    keys = ["__pbp_year", "__pbp_week", "__pbp_player_id"]

    if "season_type" in out.columns:
        out["__pbp_season_type"] = out["season_type"].astype("string")
        keys.append("__pbp_season_type")

    return out, keys


def _prep_pbp_keys(pbp_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    pbp = pbp_df.copy()
    year_col = _year_col(pbp)
    if year_col is None or "week" not in pbp.columns:
        return pbp, []

    pbp["__pbp_year"] = pd.to_numeric(pbp[year_col], errors="coerce").astype("Int64")
    pbp["__pbp_week"] = pd.to_numeric(pbp["week"], errors="coerce").astype("Int64")
    keys = ["__pbp_year", "__pbp_week"]

    if "season_type" in pbp.columns:
        pbp["__pbp_season_type"] = pbp["season_type"].astype("string")
        keys.append("__pbp_season_type")

    return pbp, keys


def _aggregate_passers(pbp: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    passer_col = "passer_player_id" if "passer_player_id" in pbp.columns else "passer_id"
    if passer_col not in pbp.columns:
        return pd.DataFrame(columns=keys + ["__pbp_player_id"])

    plays = pbp[pbp[passer_col].notna()].copy()
    if plays.empty:
        return pd.DataFrame(columns=keys + ["__pbp_player_id"])

    complete = _series(plays, "complete_pass").fillna(0).astype(float) == 1
    yards = pd.to_numeric(_series(plays, "yards_gained"), errors="coerce").fillna(0)
    pass_td = _series(plays, "pass_touchdown").fillna(0).astype(float) == 1

    plays["__pbp_player_id"] = plays[passer_col].astype("string")
    plays["completions_40plus"] = (complete & (yards >= 40)).astype(float)
    plays["completions_50plus"] = (complete & (yards >= 50)).astype(float)
    plays["passing_tds_40plus"] = (pass_td & (yards >= 40)).astype(float)
    plays["passing_tds_50plus"] = (pass_td & (yards >= 50)).astype(float)

    return (
        plays.groupby(keys + ["__pbp_player_id"], dropna=False)[
            ["completions_40plus", "completions_50plus", "passing_tds_40plus", "passing_tds_50plus"]
        ]
        .sum()
        .reset_index()
    )


def _aggregate_receivers(pbp: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    receiver_col = "receiver_player_id" if "receiver_player_id" in pbp.columns else "receiver_id"
    if receiver_col not in pbp.columns:
        return pd.DataFrame(columns=keys + ["__pbp_player_id"])

    plays = pbp[pbp[receiver_col].notna()].copy()
    if plays.empty:
        return pd.DataFrame(columns=keys + ["__pbp_player_id"])

    complete = _series(plays, "complete_pass").fillna(0).astype(float) == 1
    rec_yards = pd.to_numeric(_series(plays, "receiving_yards"), errors="coerce")
    gained = pd.to_numeric(_series(plays, "yards_gained"), errors="coerce")
    rec_yards = rec_yards.fillna(gained).fillna(0)
    pass_td = _series(plays, "pass_touchdown").fillna(0).astype(float) == 1

    plays["__pbp_player_id"] = plays[receiver_col].astype("string")
    plays["receptions_0_4"] = (complete & rec_yards.between(0, 4, inclusive="both")).astype(float)
    plays["receptions_5_9"] = (complete & rec_yards.between(5, 9, inclusive="both")).astype(float)
    plays["receptions_10_19"] = (complete & rec_yards.between(10, 19, inclusive="both")).astype(float)
    plays["receptions_20_29"] = (complete & rec_yards.between(20, 29, inclusive="both")).astype(float)
    plays["receptions_30_39"] = (complete & rec_yards.between(30, 39, inclusive="both")).astype(float)
    plays["receptions_40plus"] = (complete & (rec_yards >= 40)).astype(float)
    plays["receiving_tds_40plus"] = (complete & pass_td & (rec_yards >= 40)).astype(float)
    plays["receiving_tds_50plus"] = (complete & pass_td & (rec_yards >= 50)).astype(float)

    return (
        plays.groupby(keys + ["__pbp_player_id"], dropna=False)[
            [
                "receptions_0_4",
                "receptions_5_9",
                "receptions_10_19",
                "receptions_20_29",
                "receptions_30_39",
                "receptions_40plus",
                "receiving_tds_40plus",
                "receiving_tds_50plus",
            ]
        ]
        .sum()
        .reset_index()
    )


def _aggregate_special_teams_tackles(pbp: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if "special_teams_play" not in pbp.columns:
        return pd.DataFrame(columns=keys + ["__pbp_player_id", "special_teams_tackles_solo"])

    frames: list[pd.DataFrame] = []
    special = _series(pbp, "special_teams_play").fillna(0).astype(float) == 1
    for col in ("solo_tackle_1_player_id", "solo_tackle_2_player_id"):
        if col not in pbp.columns:
            continue
        tackles = pbp[special & pbp[col].notna()].copy()
        if tackles.empty:
            continue
        tackles["__pbp_player_id"] = tackles[col].astype("string")
        frames.append(tackles[keys + ["__pbp_player_id"]])

    if not frames:
        return pd.DataFrame(columns=keys + ["__pbp_player_id", "special_teams_tackles_solo"])

    all_tackles = pd.concat(frames, ignore_index=True)
    out = (
        all_tackles.groupby(keys + ["__pbp_player_id"], dropna=False)
        .size()
        .reset_index(name="special_teams_tackles_solo")
    )
    out["special_teams_tackles_solo"] = out["special_teams_tackles_solo"].astype(float)
    return out


def _merge_aggregates(frames: list[pd.DataFrame], keys: list[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=keys + ["__pbp_player_id", *PBP_SCORING_COLUMNS])

    merged = non_empty[0]
    for frame in non_empty[1:]:
        merged = merged.merge(frame, how="outer", on=keys + ["__pbp_player_id"])

    for col in PBP_SCORING_COLUMNS:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)

    return merged[keys + ["__pbp_player_id", *PBP_SCORING_COLUMNS]]


def enrich_with_pbp_scoring_stats(
    df: pd.DataFrame,
    year: int,
    week: int | None = None,
    *,
    pbp_df: pd.DataFrame | None = None,
    cache_dir: Path | None = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Merge PBP-derived scoring stats onto player/week rows.

    The source exists for nflverse-era seasons. For pre-1999 seasons or when PBP
    is unavailable, the output still contains zero-filled columns so downstream
    SQL can safely reference them.
    """
    prepared = _prep_left_keys(ensure_pbp_scoring_columns(df))
    if prepared is None:
        return ensure_pbp_scoring_columns(df)

    out, left_keys = prepared
    if year < 1999:
        return out.drop(columns=[c for c in out.columns if c.startswith("__pbp_")])

    try:
        pbp = pbp_df.copy() if pbp_df is not None else _load_pbp(year, cache_dir, use_cache)
    except Exception:
        return out.drop(columns=[c for c in out.columns if c.startswith("__pbp_")])

    if pbp.empty:
        return out.drop(columns=[c for c in out.columns if c.startswith("__pbp_")])

    pbp, pbp_keys = _prep_pbp_keys(pbp)
    if not pbp_keys:
        return out.drop(columns=[c for c in out.columns if c.startswith("__pbp_")])
    if week is not None:
        pbp = pbp[pbp["__pbp_week"] == int(week)]

    common_keys = [key for key in left_keys if key in pbp_keys]
    if not common_keys:
        return out.drop(columns=[c for c in out.columns if c.startswith("__pbp_")])
    left_keys = common_keys
    pbp_keys = common_keys

    derived = _merge_aggregates(
        [
            _aggregate_passers(pbp, pbp_keys),
            _aggregate_receivers(pbp, pbp_keys),
            _aggregate_special_teams_tackles(pbp, pbp_keys),
        ],
        pbp_keys,
    )

    merged = out.merge(derived, how="left", on=left_keys + ["__pbp_player_id"], suffixes=("", "__pbp"))
    for col in PBP_SCORING_COLUMNS:
        pbp_col = f"{col}__pbp"
        if pbp_col in merged.columns:
            merged[col] = pd.to_numeric(merged[pbp_col], errors="coerce").fillna(merged[col]).fillna(0.0)
            merged = merged.drop(columns=[pbp_col])
        else:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)

    return merged.drop(columns=[c for c in merged.columns if c.startswith("__pbp_")])


def enrich_with_pbp_scoring_rollup(
    df: pd.DataFrame,
    *,
    rollup_path: Path | str | None = None,
    log_fn=None,
) -> pd.DataFrame:
    """Left-join local PBP scoring rollups onto existing super-table rows.

    This is intentionally not an expansion step.  The rollup is collapsed to one
    row per ``player_week`` before merging, and only columns on existing rows are
    updated.  PBP-era rows with no matching event row receive zeroes, which is
    the correct score for count/bucket columns.
    """
    out = ensure_pbp_scoring_columns(df)
    if "player_week" not in out.columns:
        if log_fn:
            log_fn("  PBP scoring rollup skipped: player_week column missing")
        return out

    path = Path(rollup_path) if rollup_path is not None else DEFAULT_PBP_SCORING_ROLLUP
    if not path.exists():
        if log_fn:
            log_fn(f"  PBP scoring rollup skipped: {path} not found")
        return out

    rollup = pd.read_parquet(path)
    if rollup.empty or "player_week" not in rollup.columns:
        if log_fn:
            log_fn("  PBP scoring rollup skipped: rollup file is empty or missing player_week")
        return out

    for col in PBP_ROLLUP_COLUMNS:
        if col not in rollup.columns:
            rollup[col] = 0.0
    rollup = rollup[["player_week", *PBP_ROLLUP_COLUMNS]].copy()
    rollup["player_week"] = rollup["player_week"].astype("string")
    for col in PBP_ROLLUP_COLUMNS:
        rollup[col] = pd.to_numeric(rollup[col], errors="coerce").fillna(0.0)
    rollup = rollup.groupby("player_week", dropna=False)[list(PBP_ROLLUP_COLUMNS)].sum().reset_index()

    out["__pbp_row_id"] = range(len(out))
    out["__pbp_player_week_key"] = out["player_week"].astype("string")
    before_rows = len(out)
    left_keys = set(out["__pbp_player_week_key"].dropna())
    matched_rollup_rows = int(rollup["player_week"].isin(left_keys).sum())
    unmatched_rollup_rows = int(len(rollup) - matched_rollup_rows)

    merged = out.merge(
        rollup,
        how="left",
        left_on="__pbp_player_week_key",
        right_on="player_week",
        suffixes=("", "__pbp_rollup"),
    ).sort_values("__pbp_row_id")
    if len(merged) != before_rows:
        raise ValueError(f"PBP scoring rollup changed row count: before={before_rows:,} after={len(merged):,}")

    year = pd.to_numeric(merged.get("year", pd.Series(pd.NA, index=merged.index)), errors="coerce")
    pbp_era = year.ge(1999).fillna(False)
    matched = merged["player_week__pbp_rollup"].notna()

    for col in PBP_ROLLUP_COLUMNS:
        rollup_col = f"{col}__pbp_rollup"
        current = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
        pbp_values = pd.to_numeric(merged[rollup_col], errors="coerce")
        merged[col] = current
        merged.loc[pbp_era, col] = pbp_values.loc[pbp_era].fillna(0.0)
        merged.loc[matched, col] = pbp_values.loc[matched].fillna(0.0)

    total = pd.to_numeric(merged["fumble_recovery_yards_own"], errors="coerce").fillna(0.0) + pd.to_numeric(
        merged["fumble_recovery_yards_opp"], errors="coerce"
    ).fillna(0.0)
    merged.loc[pbp_era | matched, "fumble_recovery_yards"] = total.loc[pbp_era | matched]
    merged.loc[pbp_era | matched, "fum_rec_yds"] = total.loc[pbp_era | matched]
    merged["fumble_recovery_yards"] = pd.to_numeric(merged["fumble_recovery_yards"], errors="coerce").fillna(0.0)
    merged["fum_rec_yds"] = pd.to_numeric(merged["fum_rec_yds"], errors="coerce").fillna(0.0)

    drop_cols = [
        "__pbp_row_id",
        "__pbp_player_week_key",
        "player_week__pbp_rollup",
        *[f"{col}__pbp_rollup" for col in PBP_ROLLUP_COLUMNS],
    ]
    result = merged.drop(columns=[col for col in drop_cols if col in merged.columns])

    if log_fn:
        nonzero = {
            col: int((pd.to_numeric(result[col], errors="coerce").fillna(0) != 0).sum()) for col in PBP_ROLLUP_COLUMNS
        }
        log_fn(
            "  PBP scoring rollup applied: "
            f"{matched_rollup_rows:,} matched, {unmatched_rollup_rows:,} ignored missing rows"
        )
        log_fn(
            "  PBP scoring nonzero rows: " + ", ".join(f"{col}={count:,}" for col, count in nonzero.items() if count)
        )

    return result
