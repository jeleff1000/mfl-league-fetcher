"""Fleaflicker transaction fetcher."""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_transaction import normalize_transaction_df

from .fleaflicker_api_client import FleaflickerAPIClient
from .fleaflicker_context import FleaflickerContext, YearFilter, resolve_years_to_fetch
from .fleaflicker_utils import (
    clean_id,
    get_any,
    period_starts_from_scoreboard,
    player_fields,
    stable_hash,
    team_identity,
    team_lookup_from_standings,
    week_for_timestamp,
)

logger = logging.getLogger(__name__)


TRANSACTION_TYPE_MAP = {
    "TRANSACTION_ADD": "add",
    "TRANSACTION_CLAIM": "add",
    "TRANSACTION_DROP": "drop",
    "TRANSACTION_TRADE": "trade",
    "TRANSACTION_TRADE_ACCEPT": "trade",
}


def _epoch_ms(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _year_from_ms(value: int | None, fallback_year: int) -> int:
    if value is None:
        return fallback_year
    date = dt.datetime.fromtimestamp(value / 1000, tz=dt.UTC)
    return date.year


def _transaction_datetime(value: int | None) -> str | None:
    if value is None:
        return None
    return dt.datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _source_destination(tx_type: str) -> tuple[str | None, str | None]:
    if tx_type == "add":
        return "waivers", "team"
    if tx_type == "drop":
        return "team", "waivers"
    return None, None


def _row_for_item(
    item: dict[str, Any],
    *,
    target_year: int,
    league_id: str,
    team_lookup: dict[str, dict],
    period_starts: list[tuple[int, int]],
    sequence: int,
) -> dict[str, Any] | None:
    transaction = get_any(item, "transaction") or {}
    player_payload = get_any(transaction, "player")
    if not isinstance(player_payload, dict):
        return None

    timestamp_ms = _epoch_ms(get_any(item, "timeEpochMilli", "time_epoch_milli"))
    year = _year_from_ms(timestamp_ms, target_year)
    if year != target_year:
        return None
    week = week_for_timestamp(timestamp_ms, period_starts) or 1
    raw_type = str(get_any(transaction, "type") or "TRANSACTION_ADD").upper()
    if raw_type == "TRANSACTION_IMPORT":
        return None
    tx_type = TRANSACTION_TYPE_MAP.get(raw_type, raw_type.lower().replace("transaction_", ""))
    if tx_type == "draft":
        return None
    source_type, destination = _source_destination(tx_type)
    team = team_identity(get_any(transaction, "team"), team_lookup)
    player = player_fields(player_payload, year=target_year)
    trade_id = clean_id(get_any(transaction, "tradeId", "trade_id"))
    transaction_id = (
        f"trade_{trade_id}"
        if tx_type == "trade" and trade_id
        else clean_id(get_any(transaction, "id"))
        or stable_hash(
            league_id,
            timestamp_ms,
            raw_type,
            team["team_key"],
            player.get("fleaflicker_player_id"),
            sequence,
        )
    )

    row = {
        "transaction_id": transaction_id,
        "transaction_sequence": sequence,
        "year": year,
        "week": week,
        "timestamp": int(timestamp_ms / 1000) if timestamp_ms is not None else None,
        "transaction_datetime": _transaction_datetime(timestamp_ms),
        "status": "successful",
        "transaction_type": tx_type,
        "platform": "fleaflicker",
        "league_id": str(league_id),
        "manager": team["manager"],
        "manager_guid": team["manager_guid"],
        "franchise_id": team["franchise_id"],
        "team_name": team["team_name"],
        "source_type": source_type,
        "destination": destination,
        **player,
    }

    if tx_type == "trade":
        row.update(
            {
                "_fleaflicker_trade_id": trade_id,
                "source_type": "team",
                "destination": "team",
                "destination_manager": team["manager"],
                "destination_manager_guid": team["manager_guid"],
                "destination_franchise_id": team["franchise_id"],
                "destination_team_name": team["team_name"],
            }
        )

    return row


def _trade_party_for_item(
    item: dict[str, Any],
    *,
    target_year: int,
    team_lookup: dict[str, dict],
) -> tuple[str, dict[str, Any]] | None:
    transaction = get_any(item, "transaction") or {}
    raw_type = str(get_any(transaction, "type") or "").upper()
    tx_type = TRANSACTION_TYPE_MAP.get(raw_type, raw_type.lower().replace("transaction_", ""))
    if tx_type != "trade":
        return None
    trade_id = clean_id(get_any(transaction, "tradeId", "trade_id"))
    if not trade_id:
        return None
    timestamp_ms = _epoch_ms(get_any(item, "timeEpochMilli", "time_epoch_milli"))
    year = _year_from_ms(timestamp_ms, target_year)
    if year != target_year:
        return None
    team = team_identity(get_any(transaction, "team"), team_lookup)
    franchise_id = team.get("franchise_id")
    if not franchise_id:
        return None
    return trade_id, {
        "manager": team.get("manager"),
        "manager_guid": team.get("manager_guid"),
        "franchise_id": franchise_id,
        "team_name": team.get("team_name"),
    }


def _collect_trade_parties(
    items: list[dict[str, Any]],
    *,
    target_year: int,
    team_lookup: dict[str, dict],
) -> dict[str, dict[str, dict[str, Any]]]:
    parties_by_trade_id: dict[str, dict[str, dict[str, Any]]] = {}
    for item in items:
        party = _trade_party_for_item(item, target_year=target_year, team_lookup=team_lookup)
        if party is None:
            continue
        trade_id, identity = party
        parties_by_trade_id.setdefault(trade_id, {})[str(identity["franchise_id"])] = identity
    return parties_by_trade_id


def _populate_trade_counterparties(
    rows: list[dict[str, Any]],
    parties_by_trade_id: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> None:
    trades_by_id: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        trade_id = row.get("_fleaflicker_trade_id")
        if row.get("transaction_type") == "trade" and trade_id:
            trades_by_id.setdefault(str(trade_id), []).append(row)

    for trade_id, trade_rows in trades_by_id.items():
        parties: dict[str, dict[str, Any]] = dict((parties_by_trade_id or {}).get(str(trade_id), {}))
        for row in trade_rows:
            franchise_id = row.get("franchise_id")
            if not franchise_id:
                continue
            parties[str(franchise_id)] = {
                "manager": row.get("manager"),
                "manager_guid": row.get("manager_guid"),
                "franchise_id": row.get("franchise_id"),
                "team_name": row.get("team_name"),
            }
        if len(parties) != 2:
            continue
        for row in trade_rows:
            franchise_id = str(row.get("franchise_id") or "")
            counterparty = next((party for fid, party in parties.items() if fid != franchise_id), None)
            if not counterparty:
                continue
            row["source_manager"] = counterparty["manager"]
            row["source_manager_guid"] = counterparty["manager_guid"]
            row["source_franchise_id"] = counterparty["franchise_id"]
            row["source_team_name"] = counterparty["team_name"]

    for row in rows:
        row.pop("_fleaflicker_trade_id", None)


def fetch_fleaflicker_transactions(
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
    scoreboard = client.fetch_scoreboard(league_id, season=year, scoring_period=1) or {}
    period_starts = period_starts_from_scoreboard(scoreboard)
    items = client.fetch_transactions(league_id)
    rows = [
        row
        for idx, item in enumerate(items)
        if (
            row := _row_for_item(
                item,
                target_year=year,
                league_id=str(league_id),
                team_lookup=team_lookup,
                period_starts=period_starts,
                sequence=idx,
            )
        )
        is not None
    ]
    trade_parties = _collect_trade_parties(items, target_year=year, team_lookup=team_lookup)
    _populate_trade_counterparties(rows, trade_parties)
    return normalize_transaction_df(pd.DataFrame(rows), platform="fleaflicker", league_id=str(league_id))


def fetch_all_fleaflicker_transactions(
    ctx: FleaflickerContext,
    client: FleaflickerAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> list[pd.DataFrame]:
    client = client or FleaflickerAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_fleaflicker_transactions(ctx, year, client=client)
        if db is not None:
            db.save_table(
                "transactions",
                df,
                year=year,
                platform="fleaflicker",
                league_id=ctx.get_league_id_for_year(year),
            )
        frames.append(df)
        logger.info("Fetched Fleaflicker transactions for %s: %s rows", year, len(df))
    return frames
