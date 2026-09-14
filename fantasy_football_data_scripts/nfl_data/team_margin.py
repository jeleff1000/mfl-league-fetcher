"""Team-score and margin columns for DST scoring.

ESPN exposes team win/loss/tie, points scored, and margin buckets as scoring
rules. These are team/DST facts, so they belong on DEF rows in the super table.
"""

from __future__ import annotations

import pandas as pd

try:
    from nfl_data.nfl_franchises import get_nfl_franchise_number
except ImportError:  # pragma: no cover - fallback for direct module execution
    from .nfl_franchises import get_nfl_franchise_number


TEAM_MARGIN_COLUMNS: tuple[str, ...] = (
    "pts_def_team_win",
    "pts_def_team_loss",
    "pts_def_team_tie",
    "pts_def_team_pts",
    "pts_def_team_margin",
    "pts_def_team_win_margin_25p",
    "pts_def_team_win_margin_20_24",
    "pts_def_team_win_margin_15_19",
    "pts_def_team_win_margin_10_14",
    "pts_def_team_win_margin_5_9",
    "pts_def_team_win_margin_1_4",
    "pts_def_team_loss_margin_1_4",
    "pts_def_team_loss_margin_5_9",
    "pts_def_team_loss_margin_10_14",
    "pts_def_team_loss_margin_15_19",
    "pts_def_team_loss_margin_20_24",
    "pts_def_team_loss_margin_25p",
)


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _franchise_number(team: object, year: object) -> int | None:
    if pd.isna(team) or pd.isna(year):
        return None
    return get_nfl_franchise_number(str(team), int(year))


def team_margin_values(team_pts: float, opponent_pts: float) -> dict[str, float]:
    """Return raw DST team result/score/margin component values."""
    margin = float(team_pts) - float(opponent_pts)
    abs_margin = abs(margin)
    win = margin > 0
    loss = margin < 0
    tie = margin == 0
    return {
        "pts_def_team_win": float(win),
        "pts_def_team_loss": float(loss),
        "pts_def_team_tie": float(tie),
        "pts_def_team_pts": float(team_pts),
        "pts_def_team_margin": margin,
        "pts_def_team_win_margin_25p": float(win and abs_margin >= 25),
        "pts_def_team_win_margin_20_24": float(win and 20 <= abs_margin <= 24),
        "pts_def_team_win_margin_15_19": float(win and 15 <= abs_margin <= 19),
        "pts_def_team_win_margin_10_14": float(win and 10 <= abs_margin <= 14),
        "pts_def_team_win_margin_5_9": float(win and 5 <= abs_margin <= 9),
        "pts_def_team_win_margin_1_4": float(win and 1 <= abs_margin <= 4),
        "pts_def_team_loss_margin_1_4": float(loss and 1 <= abs_margin <= 4),
        "pts_def_team_loss_margin_5_9": float(loss and 5 <= abs_margin <= 9),
        "pts_def_team_loss_margin_10_14": float(loss and 10 <= abs_margin <= 14),
        "pts_def_team_loss_margin_15_19": float(loss and 15 <= abs_margin <= 19),
        "pts_def_team_loss_margin_20_24": float(loss and 20 <= abs_margin <= 24),
        "pts_def_team_loss_margin_25p": float(loss and abs_margin >= 25),
    }


def _team_margin_row(
    *,
    year: int,
    week: int,
    season_type: str,
    team: object,
    opponent: object,
    team_pts: float,
    opponent_pts: float,
) -> dict[str, object]:
    return {
        "year": int(year),
        "week": int(week),
        "season_type": season_type,
        "nfl_team": team,
        "opponent_nfl_team": opponent,
        "nfl_franchise_number": _franchise_number(team, year),
        "opponent_nfl_franchise_number": _franchise_number(opponent, year),
        **team_margin_values(team_pts, opponent_pts),
    }


def calculate_team_margin_stats_from_pbp(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Return one row per team game with team-score/margin DST columns."""
    required = {"season", "week", "home_team", "away_team", "home_score", "away_score"}
    if pbp_df is None or pbp_df.empty or not required.issubset(pbp_df.columns):
        return pd.DataFrame(columns=("year", "week", "season_type", *TEAM_MARGIN_COLUMNS))

    cols = ["season", "week", "home_team", "away_team", "home_score", "away_score"]
    if "season_type" in pbp_df.columns:
        cols.append("season_type")
    if "game_id" in pbp_df.columns:
        cols.append("game_id")

    games = pbp_df[cols].copy()
    games["home_score"] = _to_numeric(games["home_score"])
    games["away_score"] = _to_numeric(games["away_score"])
    games = games.dropna(subset=["season", "week", "home_team", "away_team", "home_score", "away_score"])
    if games.empty:
        return pd.DataFrame(columns=("year", "week", "season_type", *TEAM_MARGIN_COLUMNS))

    if "game_id" in games.columns:
        games = games.drop_duplicates("game_id", keep="last")
    else:
        games = games.drop_duplicates(["season", "week", "home_team", "away_team"], keep="last")

    if "season_type" not in games.columns:
        games["season_type"] = "REG"
    games["season_type"] = games["season_type"].fillna("REG")

    rows: list[dict[str, object]] = []
    for game in games.itertuples(index=False):
        year = int(game.season)
        week = int(game.week)
        season_type = str(game.season_type or "REG")
        home = game.home_team
        away = game.away_team
        home_score = float(game.home_score)
        away_score = float(game.away_score)
        rows.append(
            _team_margin_row(
                year=year,
                week=week,
                season_type=season_type,
                team=home,
                opponent=away,
                team_pts=home_score,
                opponent_pts=away_score,
            )
        )
        rows.append(
            _team_margin_row(
                year=year,
                week=week,
                season_type=season_type,
                team=away,
                opponent=home,
                team_pts=away_score,
                opponent_pts=home_score,
            )
        )

    out = pd.DataFrame(rows)
    return out[out["nfl_franchise_number"].notna()].reset_index(drop=True)


def ensure_team_margin_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in TEAM_MARGIN_COLUMNS:
        if col not in result.columns:
            result[col] = 0.0
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0.0)
    return result


def apply_team_margin_stats(defensive_df: pd.DataFrame, team_margin_df: pd.DataFrame | None) -> pd.DataFrame:
    """Merge team-score/margin columns onto DEF rows."""
    result = defensive_df.copy()
    if team_margin_df is None or team_margin_df.empty:
        return ensure_team_margin_columns(result)

    result = result.copy()
    if "defense_franchise_id" in result.columns:
        result["_team_margin_fid"] = pd.to_numeric(result["defense_franchise_id"], errors="coerce")
    elif "nfl_franchise_number" in result.columns:
        result["_team_margin_fid"] = pd.to_numeric(result["nfl_franchise_number"], errors="coerce")
    else:
        result["_team_margin_fid"] = result.apply(
            lambda row: _franchise_number(row.get("nfl_team"), row.get("year")),
            axis=1,
        )

    stage = team_margin_df.copy()
    stage["_team_margin_fid"] = pd.to_numeric(stage["nfl_franchise_number"], errors="coerce")
    merge_cols = ["year", "week", "season_type", "_team_margin_fid"]
    value_cols = list(TEAM_MARGIN_COLUMNS)
    before_cols = set(result.columns)
    result = result.merge(stage[merge_cols + value_cols], on=merge_cols, how="left", suffixes=("", "_team_margin"))

    for col in value_cols:
        merged_col = f"{col}_team_margin"
        if col in before_cols and merged_col in result.columns:
            result[col] = result[merged_col].combine_first(result[col])
            result = result.drop(columns=[merged_col])
        elif merged_col in result.columns:
            result[col] = result[merged_col]
            result = result.drop(columns=[merged_col])

    result = result.drop(columns=["_team_margin_fid"])
    return ensure_team_margin_columns(result)
