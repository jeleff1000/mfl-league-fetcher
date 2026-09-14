"""PBP-derived schema atoms for the NFL supertable.

These columns are not official box-score replacement values. They are
play-level atoms that the supertable previously did not carry at all, so they
can be safely added as an overlay by player_week.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from ..core.data_lake_paths import pbp_player_week_rollup_path


REPO_ROOT = Path(__file__).resolve().parents[3]
# Canonical D-drive data lake (overridable via LEAGUE_HISTORY_DATA_ROOT in
# data_lake_paths, or PBP_SCHEMA_BACKFILL_ROLLUP_PATH here). No C-drive default.
DEFAULT_ROLLUP_PATH = Path(
    os.environ.get(
        "PBP_SCHEMA_BACKFILL_ROLLUP_PATH",
        str(pbp_player_week_rollup_path()),
    )
)

PBP_SCHEMA_BACKFILL_COLUMNS = [
    "fumbles",
    "fumbles_lost",
    "fg_yards",
    "punts",
    "punt_yards",
    "punt_long",
    "punts_blocked",
    "kickoff_return_tds",
    "punt_return_tds",
    "def_int_ret_td",
]

PBP_SCHEMA_BACKFILL_COLUMN_TYPES = {col: "DOUBLE" for col in PBP_SCHEMA_BACKFILL_COLUMNS}

PBP_TRUTH_ATOM_SOURCE_MAP = {
    # Field-goal distance atoms.  The PBP rollup's fg_yards is made-FG
    # distance, so mirror it into fg_made_distance for yardage scoring.
    "fg_yards": "fg_yards",
    "fg_long": "fg_long",
    "fg_made_distance": "fg_yards",
    "fg_made_0_19": "fg_made_0_19",
    "fg_made_20_29": "fg_made_20_29",
    "fg_made_30_39": "fg_made_30_39",
    "fg_made_40_49": "fg_made_40_49",
    "fg_made_50_59": "fg_made_50_59",
    "fg_made_60_": "fg_made_60plus",
    "fg_made_60_plus_canonical": "fg_made_60plus",
    # Return atoms.
    "kickoff_returns": "kickoff_returns",
    "kickoff_return_yards": "kickoff_return_yards",
    "kickoff_return_tds": "kickoff_return_tds",
    "punt_returns": "punt_returns",
    "punt_return_yards": "punt_return_yards",
    "punt_return_tds": "punt_return_tds",
    "special_teams_tds": "special_teams_tds",
    # Long-play buckets derived from complete play yardage.
    "completions_40plus": "completions_40plus",
    "completions_50plus": "completions_50plus",
    "passing_tds_40plus": "passing_tds_40plus",
    "passing_tds_50plus": "passing_tds_50plus",
    "receptions_0_4": "receptions_0_4",
    "receptions_5_9": "receptions_5_9",
    "receptions_10_19": "receptions_10_19",
    "receptions_20_29": "receptions_20_29",
    "receptions_30_39": "receptions_30_39",
    "receptions_40plus": "receptions_40plus",
    "receiving_tds_40plus": "receiving_tds_40plus",
    "receiving_tds_50plus": "receiving_tds_50plus",
}

PBP_SAFE_MISSING_OFFENSE_SOURCE_MAP = {
    "attempts": "attempts",
    "completions": "completions",
    "passing_yards": "passing_yards",
    "passing_tds": "passing_tds",
    "passing_interceptions": "passing_interceptions",
    "sacks_suffered": "sacks_suffered",
    "carries": "carries",
    "rushing_yards": "rushing_yards",
    "rushing_tds": "rushing_tds",
    # Do not use PBP targets for pre-2009 truth; the audit showed completion-
    # only target coverage for 2003-2008 and similar risk in older rows.
    "receptions": "receptions",
    "receiving_yards": "receiving_yards",
    "receiving_tds": "receiving_tds",
    "fumbles": "fumbles",
    "fumbles_lost": "fumbles_lost",
}

PBP_SAFE_MISSING_KICKER_SOURCE_MAP = {
    "fg_att": "fg_att",
    "fg_made": "fg_made",
    "fg_missed": "fg_missed",
    "fg_blocked": "fg_blocked",
    "fg_yards": "fg_yards",
    "fg_long": "fg_long",
    "fg_made_distance": "fg_yards",
    "fg_made_0_19": "fg_made_0_19",
    "fg_made_20_29": "fg_made_20_29",
    "fg_made_30_39": "fg_made_30_39",
    "fg_made_40_49": "fg_made_40_49",
    "fg_made_50_59": "fg_made_50_59",
    "fg_made_60_": "fg_made_60plus",
    "fg_made_60_plus_canonical": "fg_made_60plus",
    "pat_att": "pat_att",
    "pat_made": "pat_made",
    "pat_missed": "pat_missed",
    "pat_blocked": "pat_blocked",
}

PBP_SAFE_MISSING_PUNTER_SOURCE_MAP = {
    "punts": "punts",
    "punt_yards": "punt_yards",
    "punt_long": "punt_long",
    "punts_blocked": "punts_blocked",
}

PBP_SAFE_MISSING_IDP_SOURCE_MAP = {
    "def_sacks": "def_sacks",
    "def_interceptions": "def_interceptions",
    "def_interception_yards": "def_interception_yards",
    "def_int_ret_td": "def_int_ret_td",
    "def_fumbles_forced": "def_fumbles_forced",
    "def_tackles_solo": "def_tackles_solo",
    "def_tackle_assists": "def_tackle_assists",
    "def_tackles_with_assist": "def_tackles_with_assist",
    "def_tackles_for_loss": "def_tackles_for_loss",
    "def_pass_defended": "def_pass_defended",
    "def_qb_hits": "def_qb_hits",
    "def_safeties": "def_safeties",
    "def_blk_kick": "def_blk_kick",
    "special_teams_tackles_solo": "special_teams_tackles_solo",
}

PBP_SAFE_MISSING_ROW_SOURCE_MAP = {
    **PBP_TRUTH_ATOM_SOURCE_MAP,
    **PBP_SAFE_MISSING_OFFENSE_SOURCE_MAP,
    **PBP_SAFE_MISSING_KICKER_SOURCE_MAP,
}

PBP_CONTEXT_COLUMNS = [
    "player_week",
    "NFL_player_id",
    "player",
    "position",
    "nfl_team",
    "opponent_nfl_team",
    "nfl_team_context_count",
    "opponent_context_count",
    "year",
    "week",
    "season_type",
    "event_roles",
    "pbp_player_id",
    "pbp_player_name",
    "pbp_source_systems",
    "player_id_namespaces",
]

PBP_OFFENSE_ROLES = ("passer", "rusher", "receiver")
PBP_RETURN_ROLES = ("kickoff_returner", "punt_returner")
PBP_UNSAFE_MISSING_ROLES = (
    "punter",
    "interceptor",
    "solo_tackle_1",
    "solo_tackle_2",
    "assist_tackle_1",
    "assist_tackle_2",
    "assist_tackle_3",
    "assist_tackle_4",
    "tackle_with_assist_1",
    "tackle_with_assist_2",
    "tackle_for_loss_1",
    "tackle_for_loss_2",
    "pass_defense_1",
    "pass_defense_2",
    "qb_hit_1",
    "qb_hit_2",
    "sack",
    "half_sack_1",
    "half_sack_2",
    "forced_fumble_1",
    "forced_fumble_2",
    "safety",
    "blocked_kick",
)


PBP_IDP_ROLES = (
    "interceptor",
    "solo_tackle_1",
    "solo_tackle_2",
    "assist_tackle_1",
    "assist_tackle_2",
    "assist_tackle_3",
    "assist_tackle_4",
    "tackle_with_assist_1",
    "tackle_with_assist_2",
    "tackle_for_loss_1",
    "tackle_for_loss_2",
    "pass_defense_1",
    "pass_defense_2",
    "qb_hit_1",
    "qb_hit_2",
    "sack",
    "half_sack_1",
    "half_sack_2",
    "forced_fumble_1",
    "forced_fumble_2",
    "safety",
    "blocked_kick",
)


def pbp_safe_missing_source_map(*, include_punters: bool = False, include_idp: bool = False) -> dict[str, str]:
    source_map = dict(PBP_SAFE_MISSING_ROW_SOURCE_MAP)
    if include_punters:
        source_map.update(PBP_SAFE_MISSING_PUNTER_SOURCE_MAP)
    if include_idp:
        source_map.update(PBP_SAFE_MISSING_IDP_SOURCE_MAP)
    return source_map


PBP_POSTSEASON_OFFICIAL_REPAIR_COLUMNS = [
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "fg_att",
    "fg_made",
    "fg_missed",
    "pat_att",
    "pat_made",
    "pat_missed",
]

PBP_POSTSEASON_REPAIR_YARD_COLUMNS = [
    "passing_yards",
    "rushing_yards",
    "receiving_yards",
]

PBP_POSTSEASON_REPAIR_VOLUME_COLUMNS = [
    "attempts",
    "completions",
    "carries",
    "receptions",
]

PBP_POSTSEASON_REPAIR_EVENT_COLUMNS = [
    "passing_tds",
    "passing_interceptions",
    "rushing_tds",
    "receiving_tds",
    "fg_att",
    "fg_made",
    "fg_missed",
    "pat_att",
    "pat_made",
    "pat_missed",
]

TEAM_CONTEXT_ALIASES = {
    "ARZ": "ARI",
    "CRD": "ARI",
    "PHO": "ARI",
    "BAL": "BAL",
    "BLT": "BAL",
    "CLT": "IND",
    "GNB": "GB",
    "KAN": "KC",
    "LAR": "LA",
    "RAM": "LA",
    "OAK": "LV",
    "RAI": "LV",
    "LVR": "LV",
    "SD": "LAC",
    "SDG": "LAC",
    "NWE": "NE",
    "NOR": "NO",
    "SFO": "SF",
    "TAM": "TB",
    "HOU": "TEN",  # pre-1999 Oilers; modern Texans are outside this repair window
    "OIL": "TEN",
}


def _log(log_fn: Callable[[str], None] | None, message: str) -> None:
    if log_fn is not None:
        log_fn(message)


def _team_context_token(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if not text or text == "NAN":
        return ""
    return TEAM_CONTEXT_ALIASES.get(text, text)


def _normalized_series(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series([""] * len(index), index=index, dtype="string")
    return series.map(_team_context_token).astype("string")


def _numeric_frame(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            col: pd.to_numeric(
                df[col] if col in df.columns else pd.Series(0, index=df.index),
                errors="coerce",
            ).fillna(0.0)
            for col in cols
        },
        index=df.index,
    )


def _numeric_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    source = df[col] if col in df.columns else pd.Series(default, index=df.index)
    return pd.to_numeric(source, errors="coerce").fillna(default)


def _text_series(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    source = df[col] if col in df.columns else pd.Series(default, index=df.index)
    return source.fillna(default).astype(str)


def _role_mask(roles: pd.Series, role: str) -> pd.Series:
    parts = roles.fillna("").astype(str).str.lower().str.split(";")
    return parts.map(lambda values: role in {value.strip() for value in values})


def _any_role_mask(roles: pd.Series, role_names: Iterable[str]) -> pd.Series:
    mask = pd.Series(False, index=roles.index)
    for role in role_names:
        mask |= _role_mask(roles, role)
    return mask


def _pure_role_mask(roles: pd.Series, role: str) -> pd.Series:
    parts = roles.fillna("").astype(str).str.lower().str.split(";")
    return parts.map(lambda values: {value.strip() for value in values if value.strip()} == {role})


def _same_context_mask(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    require_complete: bool = True,
) -> pd.Series:
    index = left.index
    same = pd.Series(True, index=index)

    if "year" in left.columns and "year" in right.columns:
        same &= _numeric_series(left, "year").eq(_numeric_series(right, "year"))
    if "week" in left.columns and "week" in right.columns:
        same &= _numeric_series(left, "week").eq(_numeric_series(right, "week"))
    if "season_type" in left.columns and "season_type" in right.columns:
        same &= (
            _text_series(left, "season_type", "REG")
            .str.upper()
            .eq(_text_series(right, "season_type", "REG").str.upper())
        )

    for col in ("nfl_team", "opponent_nfl_team"):
        if col in left.columns and col in right.columns:
            left_team = _normalized_series(left[col], index)
            right_team = _normalized_series(right[col], index)
            same &= left_team.eq(right_team)
            if require_complete:
                same &= left_team.ne("") & right_team.ne("")
        elif require_complete:
            same &= False

    return same


def _context_complete_mask(df: pd.DataFrame) -> pd.Series:
    mask = _normalized_series(df["nfl_team"] if "nfl_team" in df.columns else None, df.index).ne(
        ""
    ) & _normalized_series(df["opponent_nfl_team"] if "opponent_nfl_team" in df.columns else None, df.index).ne("")
    if "nfl_team_context_count" in df.columns:
        mask &= _numeric_series(df, "nfl_team_context_count").eq(1)
    if "opponent_context_count" in df.columns:
        mask &= _numeric_series(df, "opponent_context_count").eq(1)
    return mask


def _truth_rollup_columns(source_map: dict[str, str]) -> list[str]:
    return sorted(set(PBP_CONTEXT_COLUMNS).union(source_map.values()))


def ensure_pbp_schema_columns(df: pd.DataFrame, columns: Iterable[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    for col in columns or PBP_SCHEMA_BACKFILL_COLUMNS:
        if col not in out.columns:
            out[col] = 0.0
        else:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def load_pbp_schema_backfill(
    rollup_path: Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    path = rollup_path or DEFAULT_ROLLUP_PATH
    cols = list(columns or PBP_SCHEMA_BACKFILL_COLUMNS)
    if not path.exists():
        return pd.DataFrame(columns=["player_week", *cols])

    backfill = pd.read_parquet(path, columns=["player_week", *cols])
    backfill = backfill[backfill["player_week"].notna()].copy()
    backfill["player_week"] = backfill["player_week"].astype(str)
    for col in cols:
        backfill[col] = pd.to_numeric(backfill[col], errors="coerce").fillna(0.0)
    if backfill["player_week"].duplicated().any():
        aggregations = {col: ("max" if col in {"punt_long"} else "sum") for col in cols}
        backfill = backfill.groupby("player_week", as_index=False).agg(aggregations)
    return backfill


def load_pbp_truth_atom_rollup(
    rollup_path: Path | None = None,
    source_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Load PBP rows needed for guarded truth atom repairs."""

    path = rollup_path or DEFAULT_ROLLUP_PATH
    source_map = source_map or PBP_TRUTH_ATOM_SOURCE_MAP
    requested = _truth_rollup_columns(source_map)
    if not path.exists():
        return pd.DataFrame(columns=requested)

    available = set(pq.read_schema(path).names)
    read_cols = [col for col in requested if col in available]
    if "player_week" not in read_cols:
        return pd.DataFrame(columns=requested)

    rollup = pd.read_parquet(path, columns=read_cols)
    rollup = rollup[rollup["player_week"].notna()].copy()
    rollup["player_week"] = rollup["player_week"].astype(str)

    for col in set(source_map.values()).intersection(rollup.columns):
        rollup[col] = pd.to_numeric(rollup[col], errors="coerce").fillna(0.0)
    for col in ("year", "week", "nfl_team_context_count", "opponent_context_count"):
        if col in rollup.columns:
            rollup[col] = pd.to_numeric(rollup[col], errors="coerce")

    if rollup["player_week"].duplicated().any():
        aggregations: dict[str, str] = {}
        for col in rollup.columns:
            if col == "player_week":
                continue
            if col in {"fg_long", "punt_long"}:
                aggregations[col] = "max"
            elif col in source_map.values():
                aggregations[col] = "sum"
            else:
                aggregations[col] = "first"
        rollup = rollup.groupby("player_week", as_index=False).agg(aggregations)
    return rollup


def load_pbp_postseason_official_repair_rollup(
    rollup_path: Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    path = rollup_path or DEFAULT_ROLLUP_PATH
    cols = list(columns or PBP_POSTSEASON_OFFICIAL_REPAIR_COLUMNS)
    context_cols = [
        "player_week",
        "year",
        "week",
        "season_type",
        "nfl_team",
        "opponent_nfl_team",
    ]
    if not path.exists():
        return pd.DataFrame(columns=[*context_cols, *cols])

    available = set(pq.read_schema(path).names)
    read_cols = [col for col in [*context_cols, *cols] if col in available]
    if "player_week" not in read_cols:
        return pd.DataFrame(columns=[*context_cols, *cols])

    backfill = pd.read_parquet(path, columns=read_cols)
    backfill = backfill[backfill["player_week"].notna()].copy()
    backfill["player_week"] = backfill["player_week"].astype(str)
    for col in cols:
        if col not in backfill.columns:
            backfill[col] = 0.0
        backfill[col] = pd.to_numeric(backfill[col], errors="coerce").fillna(0.0)
    if "year" in backfill.columns:
        backfill["year"] = pd.to_numeric(backfill["year"], errors="coerce")
    if "week" in backfill.columns:
        backfill["week"] = pd.to_numeric(backfill["week"], errors="coerce")

    if backfill["player_week"].duplicated().any():
        aggregations = {
            **{col: "first" for col in context_cols if col in backfill.columns and col != "player_week"},
            **{col: "sum" for col in cols},
        }
        backfill = backfill.groupby("player_week", as_index=False).agg(aggregations)
    return backfill


def apply_pbp_schema_backfill(
    df: pd.DataFrame,
    *,
    rollup_path: Path | None = None,
    columns: Iterable[str] | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Overlay missing PBP-derived atoms onto an NFL stats frame by player_week."""

    cols = list(columns or PBP_SCHEMA_BACKFILL_COLUMNS)
    out = ensure_pbp_schema_columns(df, cols)
    if "player_week" not in out.columns:
        _log(log_fn, "  Skipping PBP schema backfill: player_week missing")
        return out

    backfill = load_pbp_schema_backfill(rollup_path, cols)
    if backfill.empty:
        _log(log_fn, "  Skipping PBP schema backfill: rollup file missing or empty")
        return out

    out_keys = out["player_week"].astype(str)
    matched = out_keys.isin(set(backfill["player_week"]))
    if not matched.any():
        _log(log_fn, "  PBP schema backfill found no matching player_week rows")
        return out

    indexed = backfill.set_index("player_week")
    matched_keys = out_keys.loc[matched]
    for col in cols:
        out.loc[matched, col] = matched_keys.map(indexed[col]).fillna(0.0).to_numpy()

    _log(log_fn, f"  Applied PBP schema backfill to {int(matched.sum()):,} rows")
    return out


def apply_pbp_truth_atom_overlays(
    df: pd.DataFrame,
    *,
    rollup_path: Path | None = None,
    source_map: dict[str, str] | None = None,
    min_year: int = 1978,
    max_year: int = 1998,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Overlay high-confidence PBP-derived atoms on existing pre-1999 rows.

    This intentionally avoids broad official box-score stats and targets.  It
    only repairs event atoms that the audit proved PBP is the better source for:
    FG distance/buckets, return stats, and long-play buckets.  Every row must
    match player_week, year/week, season_type, team, and opponent.
    """

    source_map = source_map or PBP_TRUTH_ATOM_SOURCE_MAP
    out = df.copy()
    if "player_week" not in out.columns:
        _log(log_fn, "  Skipping PBP truth atom overlay: player_week missing")
        return out

    target_cols = [col for col in source_map if col in out.columns]
    if not target_cols:
        _log(log_fn, "  Skipping PBP truth atom overlay: no target columns present")
        return out
    for col in target_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    rollup = load_pbp_truth_atom_rollup(rollup_path, source_map)
    if rollup.empty:
        _log(log_fn, "  Skipping PBP truth atom overlay: rollup file missing or empty")
        return out
    if "year" not in rollup.columns:
        _log(log_fn, "  Skipping PBP truth atom overlay: rollup year missing")
        return out

    rollup = rollup[_numeric_series(rollup, "year").between(min_year, max_year)].copy()
    if rollup.empty:
        _log(log_fn, "  PBP truth atom overlay found no pre-1999 rollup rows")
        return out

    indexed = rollup.set_index("player_week")
    out_keys = out["player_week"].astype(str)
    matched = out_keys.isin(set(indexed.index))
    if not matched.any():
        _log(log_fn, "  PBP truth atom overlay found no matching player_week rows")
        return out

    match_idx = out.index[matched]
    matched_keys = out_keys.loc[matched]
    pbp = indexed.loc[matched_keys].reset_index(drop=True)
    pbp.index = match_idx

    candidate = _same_context_mask(out.loc[matched], pbp, require_complete=True)
    if not candidate.any():
        _log(log_fn, "  PBP truth atom overlay found no context-complete matches")
        return out

    repair_idx = candidate[candidate].index
    changed = pd.Series(False, index=repair_idx)
    for target_col in target_cols:
        source_col = source_map[target_col]
        if source_col not in pbp.columns:
            continue
        new_values = pd.to_numeric(pbp.loc[repair_idx, source_col], errors="coerce").fillna(0.0)
        old_values = pd.to_numeric(out.loc[repair_idx, target_col], errors="coerce").fillna(0.0)
        changed |= old_values.ne(new_values)
        out.loc[repair_idx, target_col] = new_values.to_numpy()

    _log(
        log_fn,
        f"  Applied PBP truth atom overlay to {int(changed.sum()):,} changed rows "
        f"({len(repair_idx):,} context-matched rows checked)",
    )
    return out


def _safe_missing_row_mask(
    rollup: pd.DataFrame,
    min_year: int,
    max_year: int,
    *,
    include_punters: bool = False,
    punters_only: bool = False,
    include_idp: bool = False,
    idp_only: bool = False,
) -> tuple[pd.Series, pd.DataFrame]:
    include_punters = include_punters or punters_only
    include_idp = include_idp or idp_only
    years = _numeric_series(rollup, "year")
    roles = _text_series(rollup, "event_roles").str.lower()
    year_window = years.between(min_year, max_year)
    context_complete = _context_complete_mask(rollup)
    has_player_id = _text_series(rollup, "NFL_player_id").str.strip().ne("")
    unsafe_roles = PBP_UNSAFE_MISSING_ROLES
    if include_punters:
        unsafe_roles = tuple(role for role in unsafe_roles if role != "punter")
    if include_idp:
        unsafe_roles = tuple(role for role in unsafe_roles if role not in PBP_IDP_ROLES)
    unsafe = _any_role_mask(roles, unsafe_roles)
    pure_kicker = _pure_role_mask(roles, "kicker")
    punter = _any_role_mask(roles, ("punter",))
    idp = _any_role_mask(roles, PBP_IDP_ROLES)
    returner = _any_role_mask(roles, PBP_RETURN_ROLES) & ~unsafe
    offense = _any_role_mask(roles, PBP_OFFENSE_ROLES) & ~unsafe
    if idp_only:
        allowed_role = idp
    elif punters_only:
        allowed_role = punter
    else:
        allowed_role = pure_kicker | returner | offense
        if include_punters:
            allowed_role = allowed_role | punter
        if include_idp:
            allowed_role = allowed_role | idp
    selected = year_window & context_complete & has_player_id & allowed_role

    reason = pd.Series("excluded", index=rollup.index, dtype="object")
    reason.loc[offense & selected & ~returner & ~pure_kicker] = "offense"
    reason.loc[returner & selected] = "returner"
    reason.loc[pure_kicker & selected] = "pure_kicker"
    reason.loc[punter & selected] = "punter"
    if include_idp:
        reason.loc[idp & selected] = "idp"
    detail = pd.DataFrame(
        {
            "safe_missing_bucket": reason,
            "is_pure_kicker": pure_kicker,
            "is_punter": punter,
            "is_idp": idp,
            "is_returner": returner,
            "is_offense": offense,
            "has_unsafe_role": unsafe,
            "context_complete": context_complete,
            "has_player_id": has_player_id,
        },
        index=rollup.index,
    )
    return selected, detail


def build_pbp_safe_missing_rows(
    existing: pd.DataFrame,
    *,
    rollup_path: Path | None = None,
    columns: Iterable[str] | None = None,
    schema_types: dict[str, str] | None = None,
    min_year: int = 1978,
    max_year: int = 1998,
    include_punters: bool = False,
    punters_only: bool = False,
    include_idp: bool = False,
    idp_only: bool = False,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Build rebuild-safe missing rows from context-complete PBP.

    By default this excludes punters, IDP/defense-only rows, blocked-kick rows,
    and fumble-only rows.  Punter rows are opt-in so the K/return/offense and
    punter promotions can be audited independently.
    """

    include_punters = include_punters or punters_only
    include_idp = include_idp or idp_only
    output_cols = list(columns or existing.columns)
    if "player_week" not in existing.columns:
        _log(log_fn, "  Skipping PBP safe missing rows: existing player_week missing")
        return pd.DataFrame(columns=output_cols)

    source_map = pbp_safe_missing_source_map(include_punters=include_punters, include_idp=include_idp)
    rollup = load_pbp_truth_atom_rollup(rollup_path, source_map)
    if rollup.empty:
        _log(log_fn, "  Skipping PBP safe missing rows: rollup file missing or empty")
        return pd.DataFrame(columns=output_cols)

    existing_keys = set(existing["player_week"].dropna().astype(str))
    missing = rollup[~rollup["player_week"].astype(str).isin(existing_keys)].copy()
    if missing.empty:
        _log(log_fn, "  PBP safe missing rows found no absent player_week rows")
        return pd.DataFrame(columns=output_cols)

    selected_mask, detail = _safe_missing_row_mask(
        missing,
        min_year,
        max_year,
        include_punters=include_punters,
        punters_only=punters_only,
        include_idp=include_idp,
        idp_only=idp_only,
    )
    selected = missing.loc[selected_mask].copy()
    selected_detail = detail.loc[selected_mask].copy()
    if selected.empty:
        _log(log_fn, "  PBP safe missing rows found no eligible context-complete rows")
        return pd.DataFrame(columns=output_cols)

    if idp_only:
        trusted_source_values = PBP_SAFE_MISSING_IDP_SOURCE_MAP.values()
    elif punters_only:
        trusted_source_values = PBP_SAFE_MISSING_PUNTER_SOURCE_MAP.values()
    else:
        trusted_source_values = source_map.values()
    trusted_source_cols = [col for col in trusted_source_values if col in selected.columns]
    if trusted_source_cols:
        has_trusted_value = _numeric_frame(selected, trusted_source_cols).abs().sum(axis=1).gt(0)
        selected = selected.loc[has_trusted_value].copy()
        selected_detail = selected_detail.loc[has_trusted_value].copy()
        if selected.empty:
            _log(log_fn, "  PBP safe missing rows found no nonzero trusted atoms")
            return pd.DataFrame(columns=output_cols)

    out = pd.DataFrame(index=selected.index, columns=output_cols)
    schema_types = schema_types or {}

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

    direct_cols = {
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
    }
    for col in direct_cols.intersection(output_cols).intersection(selected.columns):
        out[col] = selected[col].to_numpy()

    if "nfl_position" in output_cols and "position" in selected.columns:
        out["nfl_position"] = selected["position"].to_numpy()
    if "fantasy_position" in output_cols and "position" in selected.columns:
        out["fantasy_position"] = selected["position"].to_numpy()
    if "data_source" in output_cols:
        out["data_source"] = "pbp_safe_missing_row"
    if "headshot_url" in output_cols:
        out["headshot_url"] = pd.NA

    for target_col, source_col in source_map.items():
        if target_col in output_cols and source_col in selected.columns:
            out[target_col] = pd.to_numeric(selected[source_col], errors="coerce").fillna(0.0).to_numpy()

    if "targets" in output_cols:
        out["targets"] = 0.0
    if "fg_yards_canonical" in output_cols and "fg_yards" in out.columns:
        out["fg_yards_canonical"] = pd.to_numeric(out["fg_yards"], errors="coerce").fillna(0.0)

    if "safe_missing_bucket" in output_cols:
        out["safe_missing_bucket"] = selected_detail["safe_missing_bucket"].to_numpy()

    out = out.reset_index(drop=True)
    bucket_counts = selected_detail["safe_missing_bucket"].value_counts().to_dict()
    _log(
        log_fn,
        "  Built PBP safe missing rows: "
        + ", ".join(f"{bucket}={count:,}" for bucket, count in sorted(bucket_counts.items())),
    )
    return out


def apply_pbp_postseason_official_repairs(
    df: pd.DataFrame,
    *,
    rollup_path: Path | None = None,
    columns: Iterable[str] | None = None,
    min_year: int = 1978,
    max_year: int = 1998,
    log_fn: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Repair high-confidence pre-1999 postseason official stat collisions.

    Some PFR-derived historical rows were keyed by calendar year for January
    playoff games, so a previous season playoff line can sit on the next
    season's week-18+ player_week. This uses the Stathead PBP rollup only for
    exact player_week rows whose team/opponent context still matches and whose
    box-score delta is large enough to indicate that calendar-year collision.
    """

    cols = list(columns or PBP_POSTSEASON_OFFICIAL_REPAIR_COLUMNS)
    out = df.copy()
    if "player_week" not in out.columns:
        _log(log_fn, "  Skipping PBP postseason official repair: player_week missing")
        return out

    for col in cols:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    backfill = load_pbp_postseason_official_repair_rollup(rollup_path, cols)
    if backfill.empty:
        _log(log_fn, "  Skipping PBP postseason official repair: rollup file missing or empty")
        return out

    if "year" not in backfill.columns or "week" not in backfill.columns:
        _log(log_fn, "  Skipping PBP postseason official repair: rollup context missing")
        return out

    backfill = backfill[backfill["year"].between(min_year, max_year) & backfill["week"].ge(18)].copy()
    if backfill.empty:
        _log(log_fn, "  PBP postseason official repair found no pre-1999 week-18+ rows")
        return out

    indexed = backfill.set_index("player_week")
    out_keys = out["player_week"].astype(str)
    matched = out_keys.isin(set(indexed.index))
    if not matched.any():
        _log(log_fn, "  PBP postseason official repair found no matching player_week rows")
        return out

    match_idx = out.index[matched]
    matched_keys = out_keys.loc[matched]
    pbp_context = indexed.loc[matched_keys].reset_index(drop=True)
    pbp_context.index = match_idx

    out_year = pd.to_numeric(out.loc[matched, "year"], errors="coerce") if "year" in out.columns else None
    out_week = pd.to_numeric(out.loc[matched, "week"], errors="coerce") if "week" in out.columns else None
    same_year_week = (
        out_year.eq(pbp_context["year"]) & out_week.eq(pbp_context["week"])
        if out_year is not None and out_week is not None
        else pd.Series(True, index=match_idx)
    )

    if "season_type" in out.columns and "season_type" in pbp_context.columns:
        out_phase = out.loc[matched, "season_type"].fillna("REG").astype(str).str.upper()
        pbp_phase = pbp_context["season_type"].fillna("REG").astype(str).str.upper()
        same_phase = out_phase.eq(pbp_phase)
        out_week_for_phase = out_week if out_week is not None else pd.Series(0, index=match_idx)
        regular_week18_phase_collision = out_phase.eq("POST") & pbp_phase.eq("REG") & out_week_for_phase.eq(18)
    else:
        same_phase = pd.Series(True, index=match_idx)
        regular_week18_phase_collision = pd.Series(False, index=match_idx)

    if "nfl_team" in out.columns and "nfl_team" in pbp_context.columns:
        same_team = _normalized_series(out.loc[matched, "nfl_team"], match_idx).eq(
            _normalized_series(pbp_context["nfl_team"], match_idx)
        )
    else:
        same_team = pd.Series(True, index=match_idx)

    if "opponent_nfl_team" in out.columns and "opponent_nfl_team" in pbp_context.columns:
        same_opp = _normalized_series(out.loc[matched, "opponent_nfl_team"], match_idx).eq(
            _normalized_series(pbp_context["opponent_nfl_team"], match_idx)
        )
    else:
        same_opp = pd.Series(True, index=match_idx)

    old_values = _numeric_frame(out.loc[matched], cols)
    new_values = _numeric_frame(pbp_context, cols)
    deltas = (new_values - old_values).abs()

    yard_signal = (
        deltas[[col for col in PBP_POSTSEASON_REPAIR_YARD_COLUMNS if col in deltas.columns]].max(axis=1).ge(40)
    )
    volume_signal = (
        deltas[[col for col in PBP_POSTSEASON_REPAIR_VOLUME_COLUMNS if col in deltas.columns]].max(axis=1).ge(5)
    )
    event_signal = (
        deltas[[col for col in PBP_POSTSEASON_REPAIR_EVENT_COLUMNS if col in deltas.columns]].max(axis=1).ge(1)
    )
    candidate = (
        same_year_week
        & (same_phase | regular_week18_phase_collision)
        & same_team
        & same_opp
        & (yard_signal | volume_signal | event_signal)
    )

    if not candidate.any():
        _log(log_fn, "  PBP postseason official repair found no high-confidence collision rows")
        return out

    repair_idx = candidate[candidate].index
    for col in cols:
        out.loc[repair_idx, col] = new_values.loc[repair_idx, col].to_numpy()
    if "season_type" in out.columns and "season_type" in pbp_context.columns:
        phase_collision_idx = repair_idx.intersection(
            regular_week18_phase_collision[regular_week18_phase_collision].index
        )
        if len(phase_collision_idx) > 0:
            out.loc[phase_collision_idx, "season_type"] = pbp_context.loc[phase_collision_idx, "season_type"].to_numpy()

    _log(log_fn, f"  Applied PBP postseason official repairs to {len(repair_idx):,} rows")
    return out
