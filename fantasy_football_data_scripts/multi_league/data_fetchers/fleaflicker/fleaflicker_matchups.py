"""Fleaflicker matchup fetcher."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_matchup import apply_playoff_outcomes, normalize_matchup_df

from .fleaflicker_api_client import FleaflickerAPIClient, FleaflickerAPIError
from .fleaflicker_context import FleaflickerContext, YearFilter, resolve_years_to_fetch
from .fleaflicker_utils import (
    clean_id,
    result_flags,
    score_value,
    team_identity,
    team_lookup_from_standings,
    week_bounds_from_scoreboard,
)

logger = logging.getLogger(__name__)


def _flag(value: Any) -> bool:
    """Parse API booleans without treating the string ``"false"`` as true."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _matchup_id(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _side_row(
    *,
    year: int,
    week: int,
    league_id: str,
    game: dict[str, Any],
    side: str,
    opponent_side: str,
    team_lookup: dict[str, dict],
) -> dict[str, Any]:
    team = team_identity(game.get(side), team_lookup)
    opponent = team_identity(game.get(opponent_side), team_lookup)
    team_points = score_value(game.get(f"{side}Score"))
    opponent_points = score_value(game.get(f"{opponent_side}Score"))
    win, loss, tie = result_flags(game.get(f"{side}Result"), team_points, opponent_points)
    margin = team_points - opponent_points
    game_id = clean_id(game.get("id"))
    matchup_key = f"fleaflicker_{league_id}_{year}_{week}_{game_id or team['team_key']}"

    raw_playoffs = game["isPlayoffs"] if "isPlayoffs" in game else game.get("is_playoffs")
    raw_consolation = game["isConsolation"] if "isConsolation" in game else game.get("is_consolation")
    raw_championship = game["isChampionship"] if "isChampionship" in game else game.get("is_championship")
    is_playoffs = _flag(raw_playoffs)
    is_consolation = _flag(raw_consolation)
    # Fleaflicker exposes playoff/consolation classification but not always a
    # separate title-game field.  A non-consolation playoff game is the
    # championship bracket; preserve an explicit title flag when provided.
    is_championship = _flag(raw_championship) if raw_championship is not None else is_playoffs and not is_consolation

    return {
        "year": year,
        "week": week,
        "manager": team["manager"],
        "manager_guid": team["manager_guid"],
        "franchise_id": team["franchise_id"],
        "manager_week": f"{team['franchise_id']}_{year}_{week}" if team["franchise_id"] else None,
        "team_key": team["team_key"],
        "team_name": team["team_name"],
        "league_id": str(league_id),
        "platform": "fleaflicker",
        "opponent": opponent["manager"],
        "opponent_guid": opponent["manager_guid"],
        "opponent_franchise_id": opponent["franchise_id"],
        "matchup_id": _matchup_id(game_id),
        "matchup_key": matchup_key,
        "team_points": team_points,
        "opponent_points": opponent_points,
        "is_playoffs": is_playoffs,
        "is_consolation": is_consolation,
        "is_championship": is_championship,
        "team_logo": team["team_logo"],
        "division_id": team["division_id"],
        "waiver_rank": team["waiver_rank"],
        "win": win,
        "loss": loss,
        "tie": tie,
        "margin": margin,
        "total_matchup_score": team_points + opponent_points,
        "close_margin": 1 if abs(margin) <= 10 else 0,
    }


def rows_for_game(
    game: dict[str, Any], year: int, week: int, league_id: str, team_lookup: dict[str, dict]
) -> list[dict]:
    if not game.get("away") or not game.get("home"):
        return []
    return [
        _side_row(
            year=year,
            week=week,
            league_id=league_id,
            game=game,
            side="away",
            opponent_side="home",
            team_lookup=team_lookup,
        ),
        _side_row(
            year=year,
            week=week,
            league_id=league_id,
            game=game,
            side="home",
            opponent_side="away",
            team_lookup=team_lookup,
        ),
    ]


def fetch_fleaflicker_matchups(
    ctx: FleaflickerContext,
    year: int,
    client: FleaflickerAPIClient | None = None,
    weeks: list[int] | None = None,
) -> pd.DataFrame:
    client = client or FleaflickerAPIClient()
    league_id = ctx.get_league_id_for_year(year)
    if not league_id:
        return pd.DataFrame()

    try:
        standings = client.fetch_standings(league_id, season=year) or {}
    except FleaflickerAPIError as exc:
        # Historical Fleaflicker standings are sometimes forbidden while the
        # scoreboard remains public.  Matchup game objects carry the team and
        # owner identity needed by rows_for_game, so standings are enrichment,
        # not a prerequisite for recovering team outcomes.
        if exc.status_code == 403:
            logger.warning(
                "Fleaflicker standings forbidden for league=%s season=%s; "
                "using scoreboard team identity",
                league_id,
                year,
            )
            standings = {}
        else:
            raise
    team_lookup = team_lookup_from_standings(standings)
    if weeks is None:
        first_scoreboard = client.fetch_scoreboard(league_id, season=year, scoring_period=1) or {}
        start_week, end_week = week_bounds_from_scoreboard(first_scoreboard)
        weeks_to_fetch = list(range(start_week, end_week + 1))
    else:
        first_scoreboard = None
        weeks_to_fetch = sorted({int(w) for w in weeks if int(w) > 0})

    rows: list[dict] = []
    for week in weeks_to_fetch:
        scoreboard = (
            first_scoreboard
            if first_scoreboard is not None and week == 1
            else client.fetch_scoreboard(league_id, season=year, scoring_period=week)
        )
        if not scoreboard:
            continue
        for game in scoreboard.get("games") or []:
            rows.extend(rows_for_game(game, year, week, str(league_id), team_lookup))

    apply_playoff_outcomes(rows)
    return normalize_matchup_df(pd.DataFrame(rows), platform="fleaflicker", league_id=str(league_id))


def fetch_all_fleaflicker_matchups(
    ctx: FleaflickerContext,
    client: FleaflickerAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or FleaflickerAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_fleaflicker_matchups(ctx, year, client=client)
        if db is not None and not df.empty:
            db.save_table("matchup", df, year=year, platform="fleaflicker", league_id=ctx.get_league_id_for_year(year))
        frames.append(df)
        logger.info("Fetched Fleaflicker matchups for %s: %s rows", year, len(df))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
