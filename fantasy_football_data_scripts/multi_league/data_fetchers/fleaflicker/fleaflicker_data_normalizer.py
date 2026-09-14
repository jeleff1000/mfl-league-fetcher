"""Fleaflicker wrappers around canonical normalizers."""

from __future__ import annotations

from multi_league.core.canonical_draft import normalize_draft_df
from multi_league.core.canonical_matchup import normalize_matchup_df
from multi_league.core.canonical_roster import normalize_roster_df
from multi_league.core.canonical_schedule import normalize_schedule_df
from multi_league.core.canonical_transaction import normalize_transaction_df


def normalize_matchup_data(df, league_id: str | None = None):
    return normalize_matchup_df(df, platform="fleaflicker", league_id=league_id)


def normalize_roster_data(df, league_id: str | None = None):
    return normalize_roster_df(df, platform="fleaflicker", league_id=league_id)


def normalize_draft_data(df, league_id: str | None = None):
    return normalize_draft_df(df, platform="fleaflicker", league_id=league_id)


def normalize_transaction_data(df, league_id: str | None = None):
    return normalize_transaction_df(df, platform="fleaflicker", league_id=league_id)


def normalize_schedule_data(df, league_id: str | None = None):
    return normalize_schedule_df(df, platform="fleaflicker", league_id=league_id)
