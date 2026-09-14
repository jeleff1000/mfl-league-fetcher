"""MFL schedule fetcher.

Reuses the matchup row builder over the (client-cached) weeklyResults
payloads, adding schedule-specific join keys — mirroring how the Fleaflicker
schedule fetcher reuses rows_for_game.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from multi_league.core.canonical_schedule import normalize_schedule_df

from .mfl_api_client import MFLAPIClient
from .mfl_context import MFLContext, YearFilter, resolve_years_to_fetch
from .mfl_matchups import (
    championship_bracket_pairs,
    last_regular_season_week,
    rows_for_week,
    week_bounds_from_league,
)
from .mfl_utils import franchise_lookup_from_league


def _schedule_rows_for_week(rows: list[dict[str, Any]], year: int, week: int):
    for row in rows:
        row["manager_year"] = f"{row['franchise_id']}_{year}" if row.get("franchise_id") else None
        row["opponent_week"] = (
            f"{row['opponent_franchise_id']}_{year}_{week}" if row.get("opponent_franchise_id") else None
        )
        row["opponent_year"] = f"{row['opponent_franchise_id']}_{year}" if row.get("opponent_franchise_id") else None
        row["is_playoffs"] = int(bool(row.get("is_playoffs")))
        row["is_consolation"] = int(bool(row.get("is_consolation")))
        yield row


def fetch_mfl_schedule(
    ctx: MFLContext,
    year: int,
    client: MFLAPIClient | None = None,
) -> pd.DataFrame:
    client = client or MFLAPIClient()
    league_id = ctx.get_league_id_for_year(year)
    if not league_id:
        return pd.DataFrame()

    league = client.fetch_league(league_id, year) or {}
    team_lookup = franchise_lookup_from_league(league)
    start_week, end_week = week_bounds_from_league(league)
    last_reg_week = last_regular_season_week(league)
    champ_pairs = championship_bracket_pairs(client, str(league_id), year)

    rows: list[dict] = []
    for week in range(start_week, end_week + 1):
        weekly_results = client.fetch_weekly_results(league_id, year, week)
        if not weekly_results:
            continue
        week_rows = rows_for_week(
            weekly_results,
            year=year,
            week=week,
            league_id=str(league_id),
            seed_league_id=str(ctx.league_id),
            team_lookup=team_lookup,
            last_reg_week=last_reg_week,
            champ_pairs=champ_pairs,
        )
        rows.extend(_schedule_rows_for_week(week_rows, year, week))
    return normalize_schedule_df(pd.DataFrame(rows), platform="mfl", league_id=str(league_id))


def fetch_all_mfl_schedules(
    ctx: MFLContext,
    client: MFLAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or MFLAPIClient()
    frames = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_mfl_schedule(ctx, year, client=client)
        if db is not None and not df.empty:
            db.save_table("schedule", df, year=year, platform="mfl", league_id=ctx.get_league_id_for_year(year))
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
