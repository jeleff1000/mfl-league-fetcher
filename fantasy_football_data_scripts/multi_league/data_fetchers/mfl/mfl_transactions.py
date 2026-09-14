"""MFL transaction fetcher.

TYPE=transactions grammar (live-verified 2026-07-17):
- FREE_AGENT:  transaction = "added_ids|dropped_ids"       e.g. "16439,|" / "|15819,"
- BBID_WAIVER: transaction = "added_ids|bid|dropped_ids"   e.g. "15258,|11|16174,"
- WAIVER:      same "added|dropped" shape as FREE_AGENT (best-effort)
- TRADE:       franchise / franchise2 + franchise1_gave_up / franchise2_gave_up
               CSV lists mixing player ids and pick tokens (FP_/DP_...).
- IR / TAXI / LOCK_ALL_PLAYERS / UNLOCK_ALL_PLAYERS / LOAD_ROSTERS /
  BBID_AUTO_PROCESS_WAIVERS are roster-state noise and are skipped.

MFL has no schedule-period epochs, so week attribution uses calendar week
windows (see mfl_utils.season_week_starts) clamped to the league's weeks.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_transaction import normalize_transaction_df

from .mfl_api_client import MFLAPIClient
from .mfl_context import MFLContext, YearFilter, resolve_years_to_fetch
from .mfl_matchups import week_bounds_from_league
from .mfl_utils import (
    clean_id,
    franchise_identity,
    franchise_lookup_from_league,
    is_player_id,
    player_fields,
    season_week_starts,
    split_ids,
    stable_hash,
    value_of,
    week_for_timestamp,
)

logger = logging.getLogger(__name__)


SKIP_TYPES = {
    "IR",
    "TAXI",
    "LOCK_ALL_PLAYERS",
    "UNLOCK_ALL_PLAYERS",
    "BBID_AUTO_PROCESS_WAIVERS",
    "LOAD_ROSTERS",
    "AUCTION_INIT",
    "AUCTION_BID",
    "AUCTION_WON",
    "SURVIVOR_PICK",
    "POOL_PICK",
    "CALENDAR_EVENT",
}


def _epoch_s(value) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _transaction_datetime(timestamp_s: int | None) -> str | None:
    if timestamp_s is None:
        return None
    return dt.datetime.fromtimestamp(timestamp_s).strftime("%Y-%m-%d %H:%M:%S")


def _clamped_week(timestamp_s: int | None, period_starts, end_week: int) -> int:
    week = week_for_timestamp(timestamp_s, period_starts) or 1
    return max(1, min(int(week), int(end_week)))


def _base_row(
    *,
    transaction_id: str,
    sequence: int,
    year: int,
    week: int,
    timestamp_s: int | None,
    tx_type: str,
    league_id: str,
    team: dict[str, Any],
    player_id: str,
    players_db: dict[str, dict],
    source_type: str | None,
    destination: str | None,
    faab_bid: float | None = None,
) -> dict[str, Any]:
    row = {
        "transaction_id": transaction_id,
        "transaction_sequence": sequence,
        "year": year,
        "week": week,
        "timestamp": timestamp_s,
        "transaction_datetime": _transaction_datetime(timestamp_s),
        "status": "successful",
        "transaction_type": tx_type,
        "platform": "mfl",
        "league_id": str(league_id),
        "manager": team["manager"],
        "manager_guid": team["manager_guid"],
        "franchise_id": team["franchise_id"],
        "team_name": team["team_name"],
        "source_type": source_type,
        "destination": destination,
        **player_fields(player_id, players_db, year=year),
    }
    if faab_bid is not None:
        row["faab_bid"] = faab_bid
    return row


def _rows_for_item(
    item: dict[str, Any],
    *,
    year: int,
    league_id: str,
    seed_league_id: str,
    team_lookup: dict[str, dict],
    players_db: dict[str, dict],
    period_starts,
    end_week: int,
    sequence: int,
) -> list[dict[str, Any]]:
    raw_type = str(item.get("type") or "").strip().upper()
    if not raw_type or raw_type in SKIP_TYPES:
        return []

    timestamp_s = _epoch_s(item.get("timestamp"))
    week = _clamped_week(timestamp_s, period_starts, end_week)
    franchise_num = clean_id(item.get("franchise"))
    team = franchise_identity({"id": franchise_num}, seed_league_id, team_lookup) if franchise_num else None

    if raw_type == "TRADE":
        return _rows_for_trade(
            item,
            year=year,
            week=week,
            timestamp_s=timestamp_s,
            league_id=league_id,
            seed_league_id=seed_league_id,
            team_lookup=team_lookup,
            players_db=players_db,
            sequence=sequence,
        )

    if team is None or not team.get("franchise_id"):
        return []

    payload = str(item.get("transaction") or "")
    parts = payload.split("|")
    faab_bid: float | None = None
    if raw_type in {"BBID_WAIVER", "BBID_FCFS"} and len(parts) >= 3:
        added_ids = split_ids(parts[0])
        faab_bid = value_of(parts[1])
        dropped_ids = split_ids(parts[2])
        source_label = "waivers"
    elif raw_type in {"FREE_AGENT", "WAIVER", "WAIVER_REQUEST"}:
        added_ids = split_ids(parts[0]) if parts else []
        dropped_ids = split_ids(parts[1]) if len(parts) > 1 else []
        source_label = "waivers" if raw_type.startswith("WAIVER") else "freeagents"
    else:
        logger.debug("Skipping unsupported MFL transaction type %s", raw_type)
        return []

    transaction_id = stable_hash(league_id, timestamp_s, raw_type, franchise_num, sequence)
    rows: list[dict[str, Any]] = []
    for player_id in added_ids:
        if not is_player_id(player_id):
            continue
        rows.append(
            _base_row(
                transaction_id=transaction_id,
                sequence=sequence,
                year=year,
                week=week,
                timestamp_s=timestamp_s,
                tx_type="add",
                league_id=league_id,
                team=team,
                player_id=player_id,
                players_db=players_db,
                source_type=source_label,
                destination="team",
                faab_bid=faab_bid,
            )
        )
    for player_id in dropped_ids:
        if not is_player_id(player_id):
            continue
        rows.append(
            _base_row(
                transaction_id=transaction_id,
                sequence=sequence,
                year=year,
                week=week,
                timestamp_s=timestamp_s,
                tx_type="drop",
                league_id=league_id,
                team=team,
                player_id=player_id,
                players_db=players_db,
                source_type="team",
                destination="waivers",
            )
        )
    return rows


def _rows_for_trade(
    item: dict[str, Any],
    *,
    year: int,
    week: int,
    timestamp_s: int | None,
    league_id: str,
    seed_league_id: str,
    team_lookup: dict[str, dict],
    players_db: dict[str, dict],
    sequence: int,
) -> list[dict[str, Any]]:
    franchise1 = clean_id(item.get("franchise"))
    franchise2 = clean_id(item.get("franchise2"))
    if not franchise1 or not franchise2:
        return []
    team1 = franchise_identity({"id": franchise1}, seed_league_id, team_lookup)
    team2 = franchise_identity({"id": franchise2}, seed_league_id, team_lookup)
    transaction_id = f"trade_{timestamp_s or sequence}_{franchise1}_{franchise2}"

    rows: list[dict[str, Any]] = []
    sides = [
        # (giving team, receiving team, items given up)
        (team1, team2, split_ids(item.get("franchise1_gave_up"))),
        (team2, team1, split_ids(item.get("franchise2_gave_up"))),
    ]
    for giving, receiving, tokens in sides:
        for token in tokens:
            if not is_player_id(token):
                continue  # FP_/DP_ pick tokens: conveyed picks are out of scope
            row = _base_row(
                transaction_id=transaction_id,
                sequence=sequence,
                year=year,
                week=week,
                timestamp_s=timestamp_s,
                tx_type="trade",
                league_id=league_id,
                team=receiving,
                player_id=token,
                players_db=players_db,
                source_type="team",
                destination="team",
            )
            row.update(
                {
                    "source_manager": giving["manager"],
                    "source_manager_guid": giving["manager_guid"],
                    "source_franchise_id": giving["franchise_id"],
                    "source_team_name": giving["team_name"],
                    "destination_manager": receiving["manager"],
                    "destination_manager_guid": receiving["manager_guid"],
                    "destination_franchise_id": receiving["franchise_id"],
                    "destination_team_name": receiving["team_name"],
                }
            )
            rows.append(row)
    return rows


def fetch_mfl_transactions(
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
    _, end_week = week_bounds_from_league(league)
    players_db = client.fetch_players(year, league_id)
    period_starts = season_week_starts(year)

    items = client.fetch_transactions(league_id, year)
    rows: list[dict[str, Any]] = []
    for sequence, item in enumerate(items):
        rows.extend(
            _rows_for_item(
                item,
                year=year,
                league_id=str(league_id),
                seed_league_id=str(ctx.league_id),
                team_lookup=team_lookup,
                players_db=players_db,
                period_starts=period_starts,
                end_week=end_week,
                sequence=sequence,
            )
        )
    return normalize_transaction_df(pd.DataFrame(rows), platform="mfl", league_id=str(league_id))


def fetch_all_mfl_transactions(
    ctx: MFLContext,
    client: MFLAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> list[pd.DataFrame]:
    client = client or MFLAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_mfl_transactions(ctx, year, client=client)
        if db is not None:
            db.save_table(
                "transactions",
                df,
                year=year,
                platform="mfl",
                league_id=ctx.get_league_id_for_year(year),
            )
        frames.append(df)
        logger.info("Fetched MFL transactions for %s: %s rows", year, len(df))
    return frames
