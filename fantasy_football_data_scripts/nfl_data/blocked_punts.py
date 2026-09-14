"""Historical DST punt-block source and team-week overlay helpers."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

try:
    from nfl_data.nfl_franchises import get_display_abbrev, get_nfl_franchise_number
except ImportError:
    from .nfl_franchises import get_display_abbrev, get_nfl_franchise_number


SOURCE_PATH = Path(__file__).resolve().parent / "sources" / "historical_blocked_punts_1936_2000.csv"

# The source's playoff week labels occasionally differ from the repaired
# super-table week key. Keep these explicit so matching stays auditable.
SUPER_TABLE_WEEK_OVERRIDES = {
    (1993, "1994-01-02", "RAI", "DEN"): 19,
}


def _clean_team(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip().upper()


def _safe_franchise_number(value: object, year: object) -> int | None:
    try:
        return get_nfl_franchise_number(_clean_team(value), int(year))
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=4)
def _load_source_cached(path: str) -> pd.DataFrame:
    source = pd.read_csv(path)
    required = {"season", "week", "punting_team_pfr", "dst_team_pfr", "punt_blocks"}
    missing = required.difference(source.columns)
    if missing:
        raise ValueError(f"Blocked-punts source missing required columns: {sorted(missing)}")

    source = source.copy()
    source["season"] = pd.to_numeric(source["season"], errors="coerce").astype("Int64")
    source["week"] = pd.to_numeric(source["week"], errors="coerce").astype("Int64")
    source["punt_blocks"] = pd.to_numeric(source["punt_blocks"], errors="coerce").fillna(0).astype(float)
    source["dst_team_pfr"] = source["dst_team_pfr"].map(_clean_team)
    source["punting_team_pfr"] = source["punting_team_pfr"].map(_clean_team)
    source = source[source["season"].notna() & source["week"].notna() & (source["punt_blocks"] > 0)].copy()
    source["season"] = source["season"].astype(int)
    source["week"] = source["week"].astype(int)
    if "game_date" in source.columns:
        for (season, game_date, dst_team, punting_team), super_week in SUPER_TABLE_WEEK_OVERRIDES.items():
            mask = (
                (source["season"] == season)
                & (source["game_date"].astype(str) == game_date)
                & (source["dst_team_pfr"] == dst_team)
                & (source["punting_team_pfr"] == punting_team)
            )
            source.loc[mask, "week"] = super_week

    source["dst_team_display"] = source["dst_team_pfr"].map(get_display_abbrev)
    source["punting_team_display"] = source["punting_team_pfr"].map(get_display_abbrev)
    source["dst_franchise_number"] = source.apply(
        lambda row: _safe_franchise_number(row["dst_team_pfr"], row["season"]),
        axis=1,
    )
    source["punting_franchise_number"] = source.apply(
        lambda row: _safe_franchise_number(row["punting_team_pfr"], row["season"]),
        axis=1,
    )
    return source


def load_historical_blocked_punts(source_path: Path | str | None = None) -> pd.DataFrame:
    """Load Stathead/PFR blocked punt rows as normalized source records."""
    path = Path(source_path) if source_path is not None else SOURCE_PATH
    return _load_source_cached(str(path.resolve())).copy()


def _source_group(source: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    grouped = (
        source.dropna(subset=key_cols)
        .groupby(key_cols, as_index=False, dropna=False)["punt_blocks"]
        .sum()
        .rename(columns={"punt_blocks": "__source_punt_blocks"})
    )
    return grouped[grouped["__source_punt_blocks"] > 0].copy()


def _numeric_series(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce").fillna(default)
    return pd.Series(default, index=frame.index)


def apply_historical_dst_punt_blocks(
    defensive_df: pd.DataFrame,
    source_path: Path | str | None = None,
) -> pd.DataFrame:
    """Overlay DST punt-block counts onto existing team-week DEF rows.

    The source names punters whose kicks were blocked. For fantasy DST scoring,
    that means the opponent defense receives one punt block. The overlay only
    writes rows already present in the destination data; it does not create games.
    """
    required = {"year", "week", "nfl_team", "opponent_nfl_team"}
    if defensive_df.empty or not required.issubset(defensive_df.columns):
        return defensive_df.copy()

    result = defensive_df.copy()
    source = load_historical_blocked_punts(source_path)
    year_values = pd.to_numeric(result["year"], errors="coerce")
    if source.empty or year_values.dropna().empty:
        return result
    if year_values.max() < int(source["season"].min()) or year_values.min() > int(source["season"].max()):
        return result

    lookup = pd.DataFrame(
        {
            "__idx": result.index,
            "__season": year_values.astype("Int64"),
            "__week": pd.to_numeric(result["week"], errors="coerce").astype("Int64"),
            "__dst_team": result["nfl_team"].map(_clean_team),
            "__punting_team": result["opponent_nfl_team"].map(_clean_team),
        }
    )
    lookup = lookup[lookup["__season"].notna() & lookup["__week"].notna()].copy()
    lookup["__season"] = lookup["__season"].astype(int)
    lookup["__week"] = lookup["__week"].astype(int)
    lookup["__dst_team_display"] = lookup["__dst_team"].map(get_display_abbrev)
    lookup["__punting_team_display"] = lookup["__punting_team"].map(get_display_abbrev)

    if {"nfl_franchise_number", "opponent_nfl_franchise_number"}.issubset(result.columns):
        lookup["__dst_franchise_number"] = pd.to_numeric(
            result.loc[lookup["__idx"], "nfl_franchise_number"].to_numpy(),
            errors="coerce",
        )
        lookup["__punting_franchise_number"] = pd.to_numeric(
            result.loc[lookup["__idx"], "opponent_nfl_franchise_number"].to_numpy(),
            errors="coerce",
        )
    else:
        lookup["__dst_franchise_number"] = lookup.apply(
            lambda row: _safe_franchise_number(row["__dst_team"], row["__season"]),
            axis=1,
        )
        lookup["__punting_franchise_number"] = lookup.apply(
            lambda row: _safe_franchise_number(row["__punting_team"], row["__season"]),
            axis=1,
        )
    lookup["__dst_franchise_number"] = pd.to_numeric(lookup["__dst_franchise_number"], errors="coerce")
    lookup["__punting_franchise_number"] = pd.to_numeric(
        lookup["__punting_franchise_number"],
        errors="coerce",
    )

    lookup["__source_punt_blocks"] = pd.NA

    def assign_matches(
        key_cols: list[str],
        left_cols: list[str],
        unresolved: pd.Series,
    ) -> pd.Series:
        if not unresolved.any():
            return lookup["__source_punt_blocks"]
        group = _source_group(source, key_cols)
        if group.empty:
            return lookup["__source_punt_blocks"]
        probe = lookup.loc[unresolved, ["__idx", *left_cols]].merge(
            group,
            left_on=left_cols,
            right_on=key_cols,
            how="left",
        )
        matched = probe[probe["__source_punt_blocks"].notna()]
        if not matched.empty:
            matched_map = matched.set_index("__idx")["__source_punt_blocks"]
            matched_mask = lookup["__idx"].isin(matched_map.index)
            lookup.loc[matched_mask, "__source_punt_blocks"] = (
                lookup.loc[matched_mask, "__idx"].map(matched_map).to_numpy()
            )
        return lookup["__source_punt_blocks"]

    unresolved = lookup["__source_punt_blocks"].isna()
    assign_matches(
        ["season", "week", "dst_franchise_number", "punting_franchise_number"],
        ["__season", "__week", "__dst_franchise_number", "__punting_franchise_number"],
        unresolved,
    )
    unresolved = lookup["__source_punt_blocks"].isna()
    assign_matches(
        ["season", "week", "dst_team_display", "punting_team_display"],
        ["__season", "__week", "__dst_team_display", "__punting_team_display"],
        unresolved,
    )
    unresolved = lookup["__source_punt_blocks"].isna()
    assign_matches(
        ["season", "week", "dst_team_pfr", "punting_team_pfr"],
        ["__season", "__week", "__dst_team", "__punting_team"],
        unresolved,
    )

    existing_punt_blocks = _numeric_series(result, "pts_def_punt_block")
    source_punt_blocks = pd.to_numeric(lookup.set_index("__idx")["__source_punt_blocks"], errors="coerce").reindex(
        result.index
    )
    result["pts_def_punt_block"] = existing_punt_blocks
    source_mask = source_punt_blocks.fillna(0) > 0
    result.loc[source_mask, "pts_def_punt_block"] = source_punt_blocks[source_mask].astype(float)

    if "pts_def_fg_block" not in result.columns:
        result["pts_def_fg_block"] = _numeric_series(result, "fg_blocked")
    else:
        result["pts_def_fg_block"] = _numeric_series(result, "pts_def_fg_block").where(
            _numeric_series(result, "pts_def_fg_block") != 0,
            _numeric_series(result, "fg_blocked"),
        )
    if "pts_def_pat_block" not in result.columns:
        result["pts_def_pat_block"] = 0.0
    else:
        result["pts_def_pat_block"] = _numeric_series(result, "pts_def_pat_block")

    result["pts_def_block"] = (
        _numeric_series(result, "pts_def_fg_block")
        + _numeric_series(result, "pts_def_punt_block")
        + _numeric_series(result, "pts_def_pat_block")
    )
    return result
