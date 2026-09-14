"""PFR weekly boxscore package overlays for the NFL supertable.

The package is built by ``scripts/build_pfr_supertable_update_package.py`` from
the local PFR weekly reconciliation audit.  This module intentionally does not
scrape or mutate Fly; it only consumes local package parquet files during a
supertable rebuild.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


ORGANIZED_ROOT = Path(
    os.environ.get(
        "FANTASY_FOOTBALL_DATA_ORGANIZED_ROOT",
        r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized",
    )
)
DEFAULT_PACKAGE_ROOT = ORGANIZED_ROOT / "_catalog"

PFR_SAFE_UPDATE_FILE = "pfr_supertable_safe_stat_updates_wide.parquet"
PFR_INSERT_FILE = "pfr_supertable_insert_ready_rows.parquet"

TEAM_CODE_ALIASES = {
    "GNB": "GB",
    "KAN": "KC",
    "NOR": "NO",
    "NWE": "NE",
    "SFO": "SF",
    "TAM": "TB",
}

PFR_WEEKLY_STAT_COLUMNS = [
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

PFR_DIRECT_INSERT_COLUMNS = {
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
}


def _log(log_fn: Callable[[str], None] | None, message: str) -> None:
    if log_fn:
        log_fn(message)


def _numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")


def _text_series(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].astype("string").str.strip().str.upper()


def resolve_pfr_package_dir(package_dir: Path | None = None) -> Path | None:
    """Resolve the package directory, preferring an explicit path/env var."""

    env_path = os.environ.get("PFR_SUPERTABLE_UPDATE_PACKAGE_DIR")
    if package_dir is not None:
        candidate = package_dir
    elif env_path:
        candidate = Path(env_path)
    else:
        dirs = sorted(
            DEFAULT_PACKAGE_ROOT.glob("pfr_supertable_update_package_*"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        candidate = next(
            (path for path in dirs if (path / PFR_SAFE_UPDATE_FILE).exists() and (path / PFR_INSERT_FILE).exists()),
            None,
        )

    if candidate is None:
        return None
    candidate = Path(candidate)
    if not (candidate / PFR_SAFE_UPDATE_FILE).exists() or not (candidate / PFR_INSERT_FILE).exists():
        return None
    return candidate


def _same_context_mask(existing: pd.DataFrame, package_rows: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=existing.index)
    for col in ("year", "week"):
        if col in existing.columns and col in package_rows.columns:
            mask &= _numeric_series(existing, col).eq(_numeric_series(package_rows, col))
    for col in ("season_type", "nfl_team", "opponent_nfl_team"):
        if col in existing.columns and col in package_rows.columns:
            left = _text_series(existing, col)
            right = _text_series(package_rows, col)
            if col in {"nfl_team", "opponent_nfl_team"}:
                left = left.replace(TEAM_CODE_ALIASES)
                right = right.replace(TEAM_CODE_ALIASES)
            mask &= left.ne("") & right.ne("") & left.eq(right)
    return mask.fillna(False)


def _load_package_parquet(package_dir: Path, file_name: str, columns: Iterable[str] | None = None) -> pd.DataFrame:
    path = package_dir / file_name
    if columns is None:
        return pd.read_parquet(path)
    available = set(pq.read_schema(path).names)
    read_cols = [col for col in columns if col in available]
    return pd.read_parquet(path, columns=read_cols)


def load_pfr_safe_updates(package_dir: Path) -> pd.DataFrame:
    columns = [
        "player_week",
        "year",
        "week",
        "season_type",
        "nfl_team",
        "opponent_nfl_team",
        *PFR_WEEKLY_STAT_COLUMNS,
    ]
    return _load_package_parquet(package_dir, PFR_SAFE_UPDATE_FILE, columns)


def load_pfr_insert_rows(package_dir: Path) -> pd.DataFrame:
    columns = [
        *PFR_DIRECT_INSERT_COLUMNS,
        *PFR_WEEKLY_STAT_COLUMNS,
    ]
    return _load_package_parquet(package_dir, PFR_INSERT_FILE, columns)


def apply_pfr_safe_stat_overlays(
    df: pd.DataFrame,
    *,
    package_dir: Path | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Overlay context-matched PFR weekly stat values onto existing rows."""

    out = df.copy()
    if "player_week" not in out.columns:
        _log(log_fn, "  Skipping PFR safe stat overlays: player_week missing")
        return out

    resolved_dir = resolve_pfr_package_dir(package_dir)
    if resolved_dir is None:
        _log(log_fn, "  Skipping PFR safe stat overlays: package files not found")
        return out

    updates = load_pfr_safe_updates(resolved_dir)
    if updates.empty:
        _log(log_fn, "  Skipping PFR safe stat overlays: package is empty")
        return out

    target_cols = [col for col in PFR_WEEKLY_STAT_COLUMNS if col in out.columns and col in updates.columns]
    if not target_cols:
        _log(log_fn, "  Skipping PFR safe stat overlays: no target columns present")
        return out
    for col in target_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    updates = updates.drop_duplicates("player_week").copy()
    updates["player_week"] = updates["player_week"].astype(str)
    indexed = updates.set_index("player_week")

    out_keys = out["player_week"].astype(str)
    matched = out_keys.isin(set(indexed.index))
    if not matched.any():
        _log(log_fn, "  PFR safe stat overlays found no matching player_week rows")
        return out

    match_idx = out.index[matched]
    matched_keys = out_keys.loc[matched].to_numpy()
    package_rows = indexed.reindex(matched_keys).reset_index(drop=True)
    package_rows.index = match_idx

    candidate = _same_context_mask(out.loc[matched], package_rows)
    if not candidate.any():
        _log(log_fn, "  PFR safe stat overlays found no context-matched rows")
        return out

    repair_idx = candidate[candidate].index
    changed = pd.Series(False, index=repair_idx)
    for col in target_cols:
        new_values = pd.to_numeric(package_rows.loc[repair_idx, col], errors="coerce").fillna(0.0)
        old_values = pd.to_numeric(out.loc[repair_idx, col], errors="coerce").fillna(0.0)
        changed |= old_values.ne(new_values)
        out.loc[repair_idx, col] = new_values.to_numpy()

    _log(
        log_fn,
        f"  Applied PFR safe stat overlays to {int(changed.sum()):,} changed rows "
        f"({len(repair_idx):,} context-matched rows checked)",
    )
    return out


def _initialized_output_frame(
    selected: pd.DataFrame,
    output_cols: list[str],
    existing: pd.DataFrame,
    schema_types: dict[str, str] | None = None,
) -> pd.DataFrame:
    schema_types = schema_types or {}
    out = pd.DataFrame(index=selected.index)
    for col in output_cols:
        dtype = str(schema_types.get(col, "")).upper()
        existing_is_numeric = col in existing.columns and pd.api.types.is_numeric_dtype(existing[col])
        existing_is_bool = col in existing.columns and pd.api.types.is_bool_dtype(existing[col])
        if dtype in {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
        }:
            out[col] = 0
        elif dtype in {"FLOAT", "REAL", "DOUBLE", "DECIMAL"} or dtype.startswith("DECIMAL") or existing_is_numeric:
            out[col] = 0.0
        elif dtype == "BOOLEAN" or existing_is_bool:
            out[col] = False
        else:
            out[col] = pd.NA
    return out


def build_pfr_insert_ready_rows(
    existing: pd.DataFrame,
    *,
    package_dir: Path | None = None,
    columns: Iterable[str] | None = None,
    schema_types: dict[str, str] | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Build aligned supertable rows from the package insert-ready rows."""

    output_cols = list(existing.columns if columns is None else columns)
    if "player_week" not in existing.columns:
        _log(log_fn, "  Skipping PFR insert-ready rows: existing player_week missing")
        return pd.DataFrame(columns=output_cols)

    resolved_dir = resolve_pfr_package_dir(package_dir)
    if resolved_dir is None:
        _log(log_fn, "  Skipping PFR insert-ready rows: package files not found")
        return pd.DataFrame(columns=output_cols)

    inserts = load_pfr_insert_rows(resolved_dir)
    if inserts.empty:
        _log(log_fn, "  Skipping PFR insert-ready rows: package is empty")
        return pd.DataFrame(columns=output_cols)

    existing_keys = set(existing["player_week"].dropna().astype(str))
    selected = inserts[~inserts["player_week"].astype(str).isin(existing_keys)].copy()
    if selected.empty:
        _log(log_fn, "  PFR insert-ready rows found no absent player_week rows")
        return pd.DataFrame(columns=output_cols)

    out = _initialized_output_frame(selected, output_cols, existing, schema_types)
    direct_cols = PFR_DIRECT_INSERT_COLUMNS.intersection(output_cols).intersection(selected.columns)
    for col in direct_cols:
        out[col] = selected[col].to_numpy()

    if "fantasy_position" in output_cols and "position" in selected.columns:
        out["fantasy_position"] = selected["position"].to_numpy()
    if "data_source" in output_cols:
        out["data_source"] = selected.get("data_source", "pfr_boxscore_weekly_insert")
    if "headshot_url" in output_cols:
        out["headshot_url"] = pd.NA

    for col in PFR_WEEKLY_STAT_COLUMNS:
        if col in output_cols and col in selected.columns:
            out[col] = pd.to_numeric(selected[col], errors="coerce").fillna(0.0).to_numpy()

    out = out.reset_index(drop=True)
    _log(log_fn, f"  Built PFR insert-ready rows: {len(out):,}")
    return out


def apply_pfr_supertable_update_package(
    df: pd.DataFrame,
    *,
    package_dir: Path | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Apply PFR safe overlays and append absent insert-ready rows."""

    resolved_dir = resolve_pfr_package_dir(package_dir)
    if resolved_dir is None:
        _log(log_fn, "  Skipping PFR supertable update package: package files not found")
        return df

    out = apply_pfr_safe_stat_overlays(df, package_dir=resolved_dir, log_fn=log_fn)
    insert_rows = build_pfr_insert_ready_rows(out, package_dir=resolved_dir, columns=out.columns, log_fn=log_fn)
    if not insert_rows.empty:
        before_rows = len(out)
        out = pd.concat([out, insert_rows], ignore_index=True, sort=False)
        _log(log_fn, f"  Added {len(out) - before_rows:,} PFR insert-ready rows")
    return out
