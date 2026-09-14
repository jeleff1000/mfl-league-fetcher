"""Fleaflicker schedule fetcher."""

from __future__ import annotations

from typing import Any

import pandas as pd

from multi_league.core.canonical_schedule import normalize_schedule_df

from .fleaflicker_api_client import FleaflickerAPIClient
from .fleaflicker_context import FleaflickerContext, YearFilter, resolve_years_to_fetch
from .fleaflicker_matchups import rows_for_game
from .fleaflicker_utils import team_lookup_from_standings, week_bounds_from_scoreboard


def _schedule_rows_for_game(game: dict[str, Any], year: int, week: int, league_id: str, team_lookup: dict[str, dict]):
    rows = rows_for_game(game, year, week, league_id, team_lookup)
    for row in rows:
        row["manager_year"] = f"{row['franchise_id']}_{year}" if row.get("franchise_id") else None
        row["opponent_week"] = (
            f"{row['opponent_franchise_id']}_{year}_{week}" if row.get("opponent_franchise_id") else None
        )
        row["opponent_year"] = f"{row['opponent_franchise_id']}_{year}" if row.get("opponent_franchise_id") else None
        row["is_playoffs"] = int(bool(row.get("is_playoffs")))
        row["is_consolation"] = int(bool(row.get("is_consolation")))
        yield row


def fetch_fleaflicker_schedule(
    ctx: FleaflickerContext,
    year: int,
    client: FleaflickerAPIClient | None = None,
) -> pd.DataFrame:
    client = client or FleaflickerAPIClient()
    league_id = ctx.get_league_id_for_year(year)
    if not league_id:
        return pd.DataFrame()

    standings = client.fetch_standings(league_id, season=year) or {}
    team_lookup = team_lookup_from_standings(standings)
    first_scoreboard = client.fetch_scoreboard(league_id, season=year, scoring_period=1) or {}
    start_week, end_week = week_bounds_from_scoreboard(first_scoreboard)
    rows: list[dict] = []
    for week in range(start_week, end_week + 1):
        scoreboard = (
            first_scoreboard if week == 1 else client.fetch_scoreboard(league_id, season=year, scoring_period=week)
        )
        if not scoreboard:
            continue
        for game in scoreboard.get("games") or []:
            rows.extend(_schedule_rows_for_game(game, year, week, str(league_id), team_lookup))
    return normalize_schedule_df(pd.DataFrame(rows), platform="fleaflicker", league_id=str(league_id))


def fetch_all_fleaflicker_schedules(
    ctx: FleaflickerContext,
    client: FleaflickerAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or FleaflickerAPIClient()
    frames = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_fleaflicker_schedule(ctx, year, client=client)
        if db is not None and not df.empty:
            db.save_table("schedule", df, year=year, platform="fleaflicker", league_id=ctx.get_league_id_for_year(year))
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
