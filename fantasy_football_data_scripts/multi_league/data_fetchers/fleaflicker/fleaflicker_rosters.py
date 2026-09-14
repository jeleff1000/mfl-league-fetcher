"""Fleaflicker weekly roster fetcher."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_roster import normalize_roster_df
from multi_league.core.roster_slots import NON_STARTER_SLOTS, resolve as resolve_position

from .fleaflicker_api_client import FleaflickerAPIClient
from .fleaflicker_context import FleaflickerContext, YearFilter, resolve_years_to_fetch
from .fleaflicker_utils import (
    get_any,
    player_fields,
    team_identity,
    team_lookup_from_standings,
    value_of,
    week_bounds_from_scoreboard,
)

logger = logging.getLogger(__name__)


def _season_from_player_payload(league_player: dict[str, Any]) -> int | None:
    period = get_any(league_player, "requestedGamesPeriod", "requested_games_period") or {}
    raw_season = get_any(period, "season")
    try:
        return int(raw_season) if raw_season is not None else None
    except (TypeError, ValueError):
        return None


def _player_payload_matches_year(league_player: dict[str, Any], year: int) -> bool:
    """Reject current-roster payloads returned for unsupported historical seasons."""
    return _season_from_player_payload(league_player) == int(year)


def _player_points(league_player: dict[str, Any]) -> float | None:
    points = value_of(get_any(league_player, "viewingActualPoints", "viewing_actual_points"))
    if points is not None:
        return points
    requested_games = get_any(league_player, "requestedGames", "requested_games") or []
    total = 0.0
    found = False
    for game in requested_games if isinstance(requested_games, list) else []:
        game_points = value_of(get_any(game, "pointsActual", "points_actual"))
        if game_points is not None:
            total += game_points
            found = True
    return total if found else None


def _rows_for_roster_payload(
    payload: dict[str, Any],
    *,
    year: int,
    week: int,
    league_id: str,
    team_info: dict[str, Any],
) -> list[dict[str, Any]]:
    team = team_identity(team_info)
    rows: list[dict[str, Any]] = []
    for group in payload.get("groups") or []:
        if not isinstance(group, dict):
            continue
        for slot in group.get("slots") or []:
            if not isinstance(slot, dict):
                continue
            position_info = get_any(slot, "position") or {}
            slot_label = get_any(position_info, "label") or get_any(group, "group")
            if not slot_label:
                slot_label = "BN"
            fantasy_position = resolve_position(str(slot_label))
            league_player = get_any(slot, "leaguePlayer", "league_player", "player")
            if not isinstance(league_player, dict):
                continue
            if not _player_payload_matches_year(league_player, year):
                continue
            row = {
                "year": year,
                "week": week,
                "manager": team["manager"],
                "manager_guid": team["manager_guid"],
                "franchise_id": team["franchise_id"],
                "manager_week": f"{team['franchise_id']}_{year}_{week}" if team["franchise_id"] else None,
                "team_key": team["team_key"],
                "team_name": team["team_name"],
                "platform": "fleaflicker",
                "league_id": str(league_id),
                "fantasy_position": fantasy_position,
                "fantasy_points": _player_points(league_player),
                "projected_points": value_of(get_any(league_player, "viewingProjectedPoints", "projectedPoints")),
                "is_started": 0 if fantasy_position.upper() in {slot.upper() for slot in NON_STARTER_SLOTS} else 1,
                "is_rostered": 1,
                **player_fields(league_player, year=year),
            }
            rows.append(row)
    return rows


def _rows_for_league_rosters_payload(
    payload: dict[str, Any],
    *,
    year: int,
    week: int,
    league_id: str,
    team_lookup: dict[str, dict],
    team_filter: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for roster in payload.get("rosters") or []:
        if not isinstance(roster, dict):
            continue
        raw_team = get_any(roster, "team") or {}
        team = team_identity(raw_team, team_lookup)
        if team_filter and str(team["team_key"]) not in team_filter:
            continue
        for player_payload in roster.get("players") or []:
            if not isinstance(player_payload, dict):
                continue
            if not _player_payload_matches_year(player_payload, year):
                continue
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
                    "platform": "fleaflicker",
                    "league_id": str(league_id),
                    "fantasy_position": "BN",
                    "fantasy_points": _player_points(player_payload),
                    "projected_points": value_of(get_any(player_payload, "viewingProjectedPoints", "projectedPoints")),
                    "is_started": 0,
                    "is_rostered": 1,
                    **player_fields(player_payload, year=year),
                }
            )
    return rows


def fetch_fleaflicker_rosters(
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
    rows: list[dict[str, Any]] = []
    team_items = list(team_lookup.items())
    worker_count = min(max(int(ctx.max_workers or 1), 1), len(team_items) or 1)
    for week in range(start_week, end_week + 1):
        league_rosters = client.fetch_league_rosters(league_id, season=year, scoring_period=week)
        fallback_by_team: dict[str, list[dict[str, Any]]] = {}
        if league_rosters:
            for team_id, _team_info in team_items:
                team_id_str = str(team_id)
                fallback_rows = _rows_for_league_rosters_payload(
                    league_rosters,
                    year=year,
                    week=week,
                    league_id=str(league_id),
                    team_lookup=team_lookup,
                    team_filter={team_id_str},
                )
                if fallback_rows:
                    fallback_by_team[team_id_str] = fallback_rows

        week_rows_by_team: dict[str, list[dict[str, Any]]] = {}
        team_order = {str(team_id): index for index, (team_id, _) in enumerate(team_items)}

        def fetch_team_rows(team_id: str, team_info: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
            roster = client.fetch_roster(league_id, team_id=team_id, season=year, scoring_period=week)
            if not roster:
                return str(team_id), []
            return (
                str(team_id),
                _rows_for_roster_payload(
                    roster,
                    year=year,
                    week=week,
                    league_id=str(league_id),
                    team_info=team_info,
                ),
            )

        if worker_count <= 1:
            for team_id, team_info in team_items:
                team_id_str, row_batch = fetch_team_rows(str(team_id), team_info)
                if row_batch:
                    week_rows_by_team[team_id_str] = row_batch
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {
                    executor.submit(fetch_team_rows, str(team_id), team_info): str(team_id)
                    for team_id, team_info in team_items
                }
                for future in as_completed(futures):
                    team_id_str = futures[future]
                    try:
                        result_team_id, row_batch = future.result()
                    except Exception as exc:
                        logger.warning(
                            "Fleaflicker roster fetch failed for league=%s season=%s week=%s team=%s: %s",
                            league_id,
                            year,
                            week,
                            team_id_str,
                            exc,
                        )
                        continue
                    if row_batch:
                        week_rows_by_team[result_team_id] = row_batch

        failed_team_ids: set[str] = set()
        for team_id, _team_info in sorted(team_items, key=lambda item: team_order.get(str(item[0]), len(team_order))):
            team_id_str = str(team_id)
            row_batch = week_rows_by_team.get(team_id_str) or fallback_by_team.get(team_id_str) or []
            if not row_batch:
                failed_team_ids.add(team_id_str)
                continue
            rows.extend(row_batch)

        if failed_team_ids:
            if not week_rows_by_team and not fallback_by_team and len(failed_team_ids) == len(team_items):
                logger.warning(
                    "No accessible Fleaflicker roster rows for league=%s season=%s week=%s; "
                    "stopping roster fetch for this season",
                    league_id,
                    year,
                    week,
                )
                break
            logger.warning(
                "Missing Fleaflicker roster rows for league=%s season=%s week=%s teams=%s",
                league_id,
                year,
                week,
                sorted(failed_team_ids),
            )

    return normalize_roster_df(pd.DataFrame(rows), platform="fleaflicker", league_id=str(league_id))


def fetch_all_fleaflicker_rosters(
    ctx: FleaflickerContext,
    client: FleaflickerAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or FleaflickerAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_fleaflicker_rosters(ctx, year, client=client)
        if db is not None:
            db.save_table(
                "player_fantasy",
                df,
                year=year,
                platform="fleaflicker",
                league_id=ctx.get_league_id_for_year(year),
            )
        frames.append(df)
        logger.info("Fetched Fleaflicker rosters for %s: %s rows", year, len(df))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
