"""Fleaflicker draft fetcher."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_draft import normalize_draft_df

from .fleaflicker_api_client import FleaflickerAPIClient
from .fleaflicker_context import FleaflickerContext, YearFilter, resolve_years_to_fetch
from .fleaflicker_utils import get_any, player_fields, team_identity, team_lookup_from_standings, value_of

logger = logging.getLogger(__name__)


def _rows_for_draft_board(
    board: dict[str, Any],
    *,
    year: int,
    league_id: str,
    team_lookup: dict[str, dict],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    draft_id = f"fleaflicker_{league_id}_{year}"

    for board_row in board.get("rows") or []:
        round_num = get_any(board_row, "round")
        for cell in board_row.get("cells") or []:
            if not isinstance(cell, dict):
                continue
            player_payload = get_any(cell, "player")
            if not isinstance(player_payload, dict):
                continue
            team = team_identity(get_any(cell, "team"), team_lookup)
            slot = get_any(cell, "slot") or {}
            cost = value_of(get_any(cell, "cost", "bid", "amount", "auctionAmount", "auction_amount"))
            rows.append(
                {
                    "year": year,
                    "round": get_any(slot, "round") or round_num,
                    "pick": get_any(slot, "overall"),
                    "pick_in_round": get_any(slot, "slot"),
                    "draft_slot": get_any(slot, "slot"),
                    "manager": team["manager"],
                    "manager_guid": team["manager_guid"],
                    "franchise_id": team["franchise_id"],
                    "team_key": team["team_key"],
                    "team_name": team["team_name"],
                    "platform": "fleaflicker",
                    "league_id": str(league_id),
                    "cost": cost,
                    "draft_type": "auction" if cost and cost > 0 else "snake",
                    "is_keeper": 0,
                    "draft_id": draft_id,
                    **player_fields(player_payload, year=year),
                }
            )

    return rows


def fetch_fleaflicker_draft(
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
    board = client.fetch_draft_board(league_id, season=year) or {}
    rows = _rows_for_draft_board(board, year=year, league_id=str(league_id), team_lookup=team_lookup)
    df = pd.DataFrame(rows)
    if not df.empty and "cost" in df.columns and pd.to_numeric(df["cost"], errors="coerce").fillna(0).gt(0).any():
        df["draft_type"] = "auction"
    return normalize_draft_df(df, platform="fleaflicker", league_id=str(league_id))


def fetch_all_fleaflicker_drafts(
    ctx: FleaflickerContext,
    client: FleaflickerAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> list[pd.DataFrame]:
    client = client or FleaflickerAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_fleaflicker_draft(ctx, year, client=client)
        if db is not None:
            db.save_table("draft", df, year=year, platform="fleaflicker", league_id=ctx.get_league_id_for_year(year))
        frames.append(df)
        logger.info("Fetched Fleaflicker draft for %s: %s rows", year, len(df))
    return frames
