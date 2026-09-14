"""MFL league settings fetcher."""

from __future__ import annotations

import logging

import pandas as pd

from multi_league.core.canonical_settings import flatten_settings
from multi_league.core.scoring_variant import derive_scoring_variant

from .mfl_api_client import MFLAPIClient
from .mfl_context import MFLContext, YearFilter, resolve_years_to_fetch

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


def fetch_mfl_settings(
    ctx: MFLContext,
    year: int,
    client: MFLAPIClient | None = None,
) -> dict:
    client = client or MFLAPIClient()
    league_id = ctx.get_league_id_for_year(year)
    if not league_id:
        return {}
    league = client.fetch_league(league_id, year) or {}
    rules = client.fetch_rules(league_id, year) or {}
    brackets = client.fetch_playoff_brackets(league_id, year) or {}
    raw = {
        "league": league,
        "rules": rules,
        "brackets": brackets,
    }
    flat = flatten_settings(raw, platform="mfl", year=year, league_key=str(league_id))
    flat["import_mode"] = ctx.import_mode
    flat["first_active_year"] = ctx.start_year
    flat["last_active_year"] = ctx.end_year or year
    # MFL carries an explicit dynasty signal: rookie-only draft pools mean the
    # veteran roster carries over (dynasty). draftPlayerPool "Both" = redraft.
    flat["is_dynasty"] = str(league.get("draftPlayerPool") or "").strip().lower() == "rookie"
    flat["scoring_variant"] = _derive_settings_variant(flat)
    return flat


def fetch_all_mfl_settings(
    ctx: MFLContext,
    client: MFLAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    client = client or MFLAPIClient()
    rows = [fetch_mfl_settings(ctx, year, client=client) for year in resolve_years_to_fetch(ctx, year_filter)]
    rows = [row for row in rows if row]
    df = pd.DataFrame(rows)
    if db is not None and not df.empty:
        if "year" in df.columns:
            for year, year_df in df.groupby("year", dropna=True):
                db.save_table("league_settings", year_df, year=int(year), platform="mfl")
        else:
            db.save_table("league_settings", df, platform="mfl")
    logger.info("Fetched MFL settings: %s rows", len(df))
    return df
