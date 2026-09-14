"""MFL matchup fetcher.

Built from TYPE=weeklyResults (the single player-week + matchup source).
Each week's payload has matchup pairs under ``matchup`` (each with a
``regularSeason`` "1"/"0" flag) plus a flat ``franchise`` list for teams
without a game that week (playoff byes) which carries no opponent and
therefore emits no matchup row.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from multi_league.core.canonical_matchup import apply_playoff_outcomes, normalize_matchup_df

from .mfl_api_client import MFLAPIClient
from .mfl_context import MFLContext, YearFilter, resolve_years_to_fetch
from .mfl_utils import (
    as_list,
    clean_id,
    franchise_identity,
    franchise_lookup_from_league,
    get_any,
    result_flags,
    value_of,
)

logger = logging.getLogger(__name__)


def _to_int(value, default: int | None = None) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def week_bounds_from_league(league: dict[str, Any] | None) -> tuple[int, int]:
    start_week = _to_int((league or {}).get("startWeek"), 1) or 1
    end_week = _to_int((league or {}).get("endWeek"), 17) or 17
    if end_week < start_week:
        end_week = start_week
    return start_week, end_week


def last_regular_season_week(league: dict[str, Any] | None) -> int | None:
    return _to_int((league or {}).get("lastRegularSeasonWeek"))


def championship_bracket_pairs(
    client: MFLAPIClient,
    league_id: str,
    year: int,
) -> set[tuple[int, frozenset[str]]]:
    """(week, {franchise ids}) pairs belonging to the championship bracket.

    Used to split playoff-week games into championship vs consolation:
    weeklyResults itself carries only the regularSeason flag, not which
    bracket a game belongs to.
    """
    brackets = client.fetch_playoff_brackets(league_id, year) or {}
    items = [item for item in as_list(brackets.get("playoffBracket")) if isinstance(item, dict)]
    if not items:
        return set()

    def bracket_sort_key(bracket: dict) -> int:
        return _to_int(get_any(bracket, "id"), 99) or 99

    champ = None
    for bracket in sorted(items, key=bracket_sort_key):
        title = f"{get_any(bracket, 'name') or ''} {get_any(bracket, 'bracketWinnerTitle') or ''}".lower()
        if "champ" in title:
            champ = bracket
            break
    if champ is None:
        champ = sorted(items, key=bracket_sort_key)[0]

    bracket_id = clean_id(get_any(champ, "id"))
    if not bracket_id:
        return set()
    detail = client.fetch_playoff_bracket(league_id, year, bracket_id) or {}
    pairs: set[tuple[int, frozenset[str]]] = set()
    for playoff_round in as_list(detail.get("playoffRound")):
        if not isinstance(playoff_round, dict):
            continue
        week = _to_int(playoff_round.get("week"))
        if week is None:
            continue
        for game in as_list(playoff_round.get("playoffGame")):
            if not isinstance(game, dict):
                continue
            fids = set()
            for side in ("home", "away"):
                side_payload = game.get(side) if isinstance(game.get(side), dict) else {}
                fid = clean_id(get_any(side_payload, "franchise_id", "franchiseId", "id"))
                if fid:
                    fids.add(fid)
            if len(fids) == 2:
                pairs.add((week, frozenset(fids)))
    return pairs


def _side_row(
    *,
    year: int,
    week: int,
    league_id: str,
    seed_league_id: str,
    side: dict[str, Any],
    opponent_side: dict[str, Any],
    team_lookup: dict[str, dict],
    is_playoffs: bool,
    is_consolation: bool,
    is_championship: bool,
) -> dict[str, Any]:
    team = franchise_identity({"id": side.get("id")}, seed_league_id, team_lookup)
    opponent = franchise_identity({"id": opponent_side.get("id")}, seed_league_id, team_lookup)
    team_points = value_of(side.get("score")) or 0.0
    opponent_points = value_of(opponent_side.get("score")) or 0.0
    win, loss, tie = result_flags(side.get("result"), team_points, opponent_points)
    margin = team_points - opponent_points
    pair_key = "_".join(sorted(str(fid) for fid in (team["team_key"], opponent["team_key"]) if fid))
    matchup_key = f"mfl_{league_id}_{year}_{week}_{pair_key}"

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
        "platform": "mfl",
        "opponent": opponent["manager"],
        "opponent_guid": opponent["manager_guid"],
        "opponent_franchise_id": opponent["franchise_id"],
        "matchup_id": None,
        "matchup_key": matchup_key,
        "team_points": team_points,
        "opponent_points": opponent_points,
        "optimal_points": value_of(side.get("opt_pts")),
        "is_playoffs": bool(is_playoffs),
        "is_consolation": bool(is_consolation),
        "is_championship": bool(is_championship),
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


def rows_for_week(
    weekly_results: dict[str, Any],
    *,
    year: int,
    week: int,
    league_id: str,
    seed_league_id: str,
    team_lookup: dict[str, dict],
    last_reg_week: int | None,
    champ_pairs: set[tuple[int, frozenset[str]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for matchup in as_list((weekly_results or {}).get("matchup")):
        if not isinstance(matchup, dict):
            continue
        sides = [side for side in as_list(matchup.get("franchise")) if isinstance(side, dict)]
        if len(sides) != 2:
            continue
        regular_flag = clean_id(matchup.get("regularSeason"))
        if regular_flag is not None:
            is_playoffs = regular_flag == "0"
        else:
            is_playoffs = bool(last_reg_week and week > last_reg_week)
        is_consolation = False
        fids = frozenset(fid for fid in (clean_id(sides[0].get("id")), clean_id(sides[1].get("id"))) if fid)
        is_championship = bool(is_playoffs and champ_pairs and (week, fids) in champ_pairs)
        if is_playoffs and champ_pairs:
            is_consolation = len(fids) == 2 and (week, fids) not in champ_pairs
        for side, opponent_side in ((sides[0], sides[1]), (sides[1], sides[0])):
            rows.append(
                _side_row(
                    year=year,
                    week=week,
                    league_id=league_id,
                    seed_league_id=seed_league_id,
                    side=side,
                    opponent_side=opponent_side,
                    team_lookup=team_lookup,
                    is_playoffs=is_playoffs,
                    is_consolation=is_consolation,
                    is_championship=is_championship,
                )
            )
    return rows


def fetch_mfl_matchups(
    ctx: MFLContext,
    year: int,
    client: MFLAPIClient | None = None,
    weeks: list[int] | None = None,
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

    if weeks is None:
        # One W=YTD call seeds every week below (17 -> 1 at the hard 15/min throttle);
        # unsupported years return [] and the loop pays per-week as before.
        client.prefetch_weekly_results_ytd(league_id, year)
        weeks_to_fetch = range(start_week, end_week + 1)
    else:
        weeks_to_fetch = sorted({int(w) for w in weeks if start_week <= int(w) <= end_week})

    rows: list[dict] = []
    for week in weeks_to_fetch:
        weekly_results = client.fetch_weekly_results(league_id, year, week)
        if not weekly_results:
            continue
        rows.extend(
            rows_for_week(
                weekly_results,
                year=year,
                week=week,
                league_id=str(league_id),
                seed_league_id=str(ctx.league_id),
                team_lookup=team_lookup,
                last_reg_week=last_reg_week,
                champ_pairs=champ_pairs,
            )
        )

    apply_playoff_outcomes(rows)
    return normalize_matchup_df(pd.DataFrame(rows), platform="mfl", league_id=str(league_id))


def fetch_all_mfl_matchups(
    ctx: MFLContext,
    client: MFLAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or MFLAPIClient()
    frames: list[pd.DataFrame] = []
    for year in resolve_years_to_fetch(ctx, year_filter):
        df = fetch_mfl_matchups(ctx, year, client=client)
        if db is not None and not df.empty:
            db.save_table("matchup", df, year=year, platform="mfl", league_id=ctx.get_league_id_for_year(year))
        frames.append(df)
        logger.info("Fetched MFL matchups for %s: %s rows", year, len(df))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
