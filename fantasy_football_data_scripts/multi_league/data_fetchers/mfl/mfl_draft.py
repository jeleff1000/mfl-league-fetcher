"""MFL draft fetcher.

TYPE=draftResults returns draftUnit (dict or list — division/conference
drafts) each holding draftPick rows {round "01", pick "01", franchise,
player, timestamp}. Overall pick numbers are not in the payload, so they are
assigned by enumerating picks in (unit, round, pick-in-round) order.
Auction leagues use TYPE=auctionResults (winning bid = cost).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_draft import normalize_draft_df

from .mfl_api_client import MFLAPIClient
from .mfl_context import MFLContext, YearFilter, resolve_years_to_fetch
from .mfl_utils import (
    as_list,
    clean_id,
    franchise_identity,
    franchise_lookup_from_league,
    get_any,
    player_fields,
    value_of,
)

logger = logging.getLogger(__name__)


def _to_int(value) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _draft_pick_rows(
    draft_results: dict[str, Any],
    *,
    year: int,
    league_id: str,
    seed_league_id: str,
    team_lookup: dict[str, dict],
    players_db: dict[str, dict],
) -> list[dict[str, Any]]:
    picks: list[dict[str, Any]] = []
    for unit in as_list((draft_results or {}).get("draftUnit")):
        if not isinstance(unit, dict):
            continue
        for pick in as_list(unit.get("draftPick")):
            if isinstance(pick, dict):
                picks.append(pick)

    def sort_key(pick: dict) -> tuple[int, int]:
        return (_to_int(pick.get("round")) or 0, _to_int(pick.get("pick")) or 0)

    picks.sort(key=sort_key)

    rows: list[dict[str, Any]] = []
    draft_id = f"mfl_{league_id}_{year}"
    for overall, pick in enumerate(picks, start=1):
        player_id = clean_id(pick.get("player"))
        if not player_id:
            continue
        team = franchise_identity({"id": pick.get("franchise")}, seed_league_id, team_lookup)
        round_num = _to_int(pick.get("round"))
        pick_in_round = _to_int(pick.get("pick"))
        rows.append(
            {
                "year": year,
                "round": round_num,
                "pick": overall,
                "pick_in_round": pick_in_round,
                "draft_slot": pick_in_round,
                "manager": team["manager"],
                "manager_guid": team["manager_guid"],
                "franchise_id": team["franchise_id"],
                "team_key": team["team_key"],
                "team_name": team["team_name"],
                "platform": "mfl",
                "league_id": str(league_id),
                "cost": None,
                "draft_type": "snake",
                "is_keeper": 0,
                "draft_id": draft_id,
                **player_fields(player_id, players_db, year=year),
            }
        )
    return rows


def _auction_rows(
    auction_results: dict[str, Any],
    *,
    year: int,
    league_id: str,
    seed_league_id: str,
    team_lookup: dict[str, dict],
    players_db: dict[str, dict],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for unit in as_list((auction_results or {}).get("auctionUnit")):
        if not isinstance(unit, dict):
            continue
        for entry in as_list(get_any(unit, "auction", "auctionPick", "auction_pick")):
            if isinstance(entry, dict):
                entries.append(entry)

    rows: list[dict[str, Any]] = []
    draft_id = f"mfl_{league_id}_{year}"
    for index, entry in enumerate(entries, start=1):
        player_id = clean_id(get_any(entry, "player", "player_id"))
        if not player_id:
            continue
        team = franchise_identity(
            {"id": get_any(entry, "franchise", "franchise_id")}, seed_league_id, team_lookup
        )
        cost = value_of(get_any(entry, "winningBid", "winning_bid", "lastBid", "last_bid", "bid", "cost"))
        rows.append(
            {
                "year": year,
                "round": None,
                "pick": index,
                "pick_in_round": None,
                "draft_slot": None,
                "manager": team["manager"],
                "manager_guid": team["manager_guid"],
                "franchise_id": team["franchise_id"],
                "team_key": team["team_key"],
                "team_name": team["team_name"],
                "platform": "mfl",
                "league_id": str(league_id),
                "cost": cost,
                "draft_type": "auction",
                "is_keeper": 0,
                "draft_id": draft_id,
                **player_fields(player_id, players_db, year=year),
            }
        )
    return rows


def fetch_mfl_draft(
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
    players_db = client.fetch_players(year, league_id)

    draft_results = client.fetch_draft_results(league_id, year) or {}
    rows = _draft_pick_rows(
        draft_results,
        year=year,
        league_id=str(league_id),
        seed_league_id=str(ctx.league_id),
        team_lookup=team_lookup,
        players_db=players_db,
    )
    if not rows:
        auction_results = client.fetch_auction_results(league_id, year) or {}
        rows = _auction_rows(
            auction_results,
            year=year,
            league_id=str(league_id),
            seed_league_id=str(ctx.league_id),
            team_lookup=team_lookup,
            players_db=players_db,
        )

    df = pd.DataFrame(rows)
    if not df.empty and "cost" in df.columns and pd.to_numeric(df["cost"], errors="coerce").fillna(0).gt(0).any():
        df["draft_type"] = "auction"
    return normalize_draft_df(df, platform="mfl", league_id=str(league_id))


def fetch_all_mfl_drafts(
    ctx: MFLContext,
    client: MFLAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> list[pd.DataFrame]:
    client = client or MFLAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_mfl_draft(ctx, year, client=client)
        if db is not None:
            db.save_table("draft", df, year=year, platform="mfl", league_id=ctx.get_league_id_for_year(year))
        frames.append(df)
        logger.info("Fetched MFL draft for %s: %s rows", year, len(df))
    return frames
