"""Fleaflicker league settings fetcher."""

from __future__ import annotations

import logging

import pandas as pd

from multi_league.core.canonical_settings import flatten_settings
from multi_league.core.scoring_variant import derive_scoring_variant

from .fleaflicker_api_client import FleaflickerAPIClient
from .fleaflicker_context import FleaflickerContext, YearFilter, resolve_years_to_fetch
from .fleaflicker_utils import week_bounds_from_scoreboard

logger = logging.getLogger(__name__)


def _derive_settings_variant(row: dict) -> str:
    return derive_scoring_variant(
        num_teams=row.get("num_teams", 12),
        has_superflex=row.get("roster_SUPER_FLEX") or row.get("roster_OP") or row.get("roster_Q/W/R/T"),
        has_idp=row.get("roster_DL") or row.get("roster_LB") or row.get("roster_DB"),
        pass_td_pts=row.get("scoring_pass_td", 4),
        ppr=row.get("scoring_rec", 0),
        te_premium=row.get("scoring_bonus_rec_te", 0),
    )


def fetch_fleaflicker_settings(
    ctx: FleaflickerContext,
    year: int,
    client: FleaflickerAPIClient | None = None,
) -> dict:
    client = client or FleaflickerAPIClient()
    league_id = ctx.get_league_id_for_year(year)
    if not league_id:
        return {}
    first_scoreboard = client.fetch_scoreboard(league_id, season=year, scoring_period=1) or {}
    start_week, end_week = week_bounds_from_scoreboard(first_scoreboard)
    scoreboards = [first_scoreboard]
    for week in range(start_week, end_week + 1):
        if week == 1:
            continue
        scoreboard = client.fetch_scoreboard(league_id, season=year, scoring_period=week) or {}
        if scoreboard:
            scoreboards.append(scoreboard)
    raw = {
        "rules": client.fetch_rules(league_id) or {},
        "standings": client.fetch_standings(league_id, season=year) or {},
        "scoreboards": scoreboards,
    }
    flat = flatten_settings(raw, platform="fleaflicker", year=year, league_key=str(league_id))
    flat["import_mode"] = ctx.import_mode
    flat["first_active_year"] = ctx.start_year
    flat["last_active_year"] = ctx.end_year or year
    # Keeper leagues are NOT dynasty (research gates treat them oppositely: dynasty pools
    # for matchup but is excluded from the keeper gate). Fleaflicker has no dynasty flag;
    # dynasty = keep-(nearly)-all, so require max_keepers to cover most of the roster.
    rules_payload = raw.get("rules") or {}
    roster_cap = rules_payload.get("maxRosterSize") or (
        (rules_payload.get("numStarters") or 0) + (rules_payload.get("numBench") or 0)
    )
    max_keepers = flat.get("max_keepers") or 0
    flat["is_dynasty"] = bool(max_keepers and roster_cap and max_keepers >= max(15, int(0.7 * roster_cap)))
    flat["scoring_variant"] = _derive_settings_variant(flat)
    return flat


def fetch_all_fleaflicker_settings(
    ctx: FleaflickerContext,
    client: FleaflickerAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or FleaflickerAPIClient()
    rows = [fetch_fleaflicker_settings(ctx, year, client=client) for year in resolve_years_to_fetch(ctx, year_filter)]
    rows = [row for row in rows if row]
    df = pd.DataFrame(rows)
    if db is not None and not df.empty:
        if "year" in df.columns:
            for year, year_df in df.groupby("year", dropna=True):
                db.save_table("league_settings", year_df, year=int(year), platform="fleaflicker")
        else:
            db.save_table("league_settings", df, platform="fleaflicker")
    logger.info("Fetched Fleaflicker settings: %s rows", len(df))
    return df
