"""MFL weekly roster fetcher (player_fantasy rows).

Player-week rows come from TYPE=weeklyResults (same payload the matchup
fetcher uses; the client caches responses so no extra API calls are spent).
weeklyResults carries only MFL player ids; names/positions/teams resolve via
the once-per-season TYPE=players&DETAILS=1 player DB.

MFL reports lineup state as status "starter"/"nonstarter" (no slot labels),
so fantasy_position is the player's NFL position for starters and "BN" for
nonstarters; is_started is set explicitly. Single-threaded by design: MFL's
throttle forbids the per-team thread pools other platforms use.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_roster import normalize_roster_df
from multi_league.core.roster_slots import resolve as resolve_position

from .mfl_api_client import MFLAPIClient
from .mfl_context import MFLContext, YearFilter, resolve_years_to_fetch
from .mfl_matchups import week_bounds_from_league
from .mfl_utils import (
    as_list,
    clean_id,
    franchise_identity,
    franchise_lookup_from_league,
    player_fields,
    value_of,
)

logger = logging.getLogger(__name__)


def _franchise_entries(weekly_results: dict[str, Any] | None) -> list[dict[str, Any]]:
    """All franchise payloads for a week: matchup sides + flat bye entries."""
    entries: list[dict[str, Any]] = []
    for matchup in as_list((weekly_results or {}).get("matchup")):
        if not isinstance(matchup, dict):
            continue
        for side in as_list(matchup.get("franchise")):
            if isinstance(side, dict):
                entries.append(side)
    for franchise in as_list((weekly_results or {}).get("franchise")):
        if isinstance(franchise, dict):
            entries.append(franchise)
    return entries


def _rows_for_franchise_entry(
    entry: dict[str, Any],
    *,
    year: int,
    week: int,
    league_id: str,
    seed_league_id: str,
    team_lookup: dict[str, dict],
    players_db: dict[str, dict],
) -> list[dict[str, Any]]:
    team = franchise_identity({"id": entry.get("id")}, seed_league_id, team_lookup)
    rows: list[dict[str, Any]] = []
    for player_payload in as_list(entry.get("player")):
        if not isinstance(player_payload, dict):
            continue
        player_id = clean_id(player_payload.get("id"))
        if not player_id:
            continue
        fields = player_fields(player_id, players_db, year=year)
        is_started = 1 if str(player_payload.get("status") or "").strip().lower() == "starter" else 0
        if is_started and fields.get("position"):
            fantasy_position = resolve_position(str(fields["position"]))
        else:
            fantasy_position = "BN"
        rows.append(
            {
                "year": year,
                "week": week,
                "manager": team["manager"],
                "manager_guid": team["manager_guid"],
                "franchise_id": team["franchise_id"],
                "manager_week": f"{team['franchise_id']}_{year}_{week}" if team["franchise_id"] else None,
                "team_key": team["team_key"],
                "team_name": team["team_name"],
                "platform": "mfl",
                "league_id": str(league_id),
                "fantasy_position": fantasy_position,
                "fantasy_points": value_of(player_payload.get("score")),
                "projected_points": None,
                "is_started": is_started,
                "is_rostered": 1,
                **fields,
            }
        )
    return rows


def fetch_mfl_rosters(
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
    players_db = client.fetch_players(year, league_id)

    # Cache hit when the matchup fetcher already prefetched this year; otherwise one
    # YTD call replaces the ~17 per-week fetches below (fallback: per-week as before).
    client.prefetch_weekly_results_ytd(league_id, year)

    rows: list[dict[str, Any]] = []
    for week in range(start_week, end_week + 1):
        weekly_results = client.fetch_weekly_results(league_id, year, week)
        if not weekly_results:
            continue
        seen_franchises: set[str] = set()
        for entry in _franchise_entries(weekly_results):
            franchise_num = clean_id(entry.get("id"))
            if not franchise_num or franchise_num in seen_franchises:
                continue
            seen_franchises.add(franchise_num)
            rows.extend(
                _rows_for_franchise_entry(
                    entry,
                    year=year,
                    week=week,
                    league_id=str(league_id),
                    seed_league_id=str(ctx.league_id),
                    team_lookup=team_lookup,
                    players_db=players_db,
                )
            )

    return normalize_roster_df(pd.DataFrame(rows), platform="mfl", league_id=str(league_id))


def fetch_all_mfl_rosters(
    ctx: MFLContext,
    client: MFLAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or MFLAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_mfl_rosters(ctx, year, client=client)
        if db is not None:
            db.save_table(
                "player_fantasy",
                df,
                year=year,
                platform="mfl",
                league_id=ctx.get_league_id_for_year(year),
            )
        frames.append(df)
        logger.info("Fetched MFL rosters for %s: %s rows", year, len(df))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
