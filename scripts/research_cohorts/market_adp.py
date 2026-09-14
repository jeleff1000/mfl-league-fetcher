"""Normalized market-ADP records and deterministic cohort-ladder selection.

This module is deliberately independent of Fly and filesystem layout. Source adapters feed
``MarketAdpRow`` records into it; builders can therefore exercise the same selection logic
against local fixture files, the local source archive, and the local ops cache.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


def normalize_yahoo_draft_rate(value: float | int | None) -> float | None:
    """Convert Yahoo's 0-1 percent_drafted fraction to a display percentage."""
    if value is None:
        return None
    numeric = float(value)
    return numeric * 100.0 if 0.0 <= numeric <= 1.0 else numeric


@dataclass(frozen=True)
class MarketAdpRow:
    year: int
    source: str
    market_format: str
    teams: int | None
    NFL_player_id: str
    adp: float
    stdev: float | None
    times_drafted: int | None
    pct_drafted: float | None
    source_player_id: str | None
    match_basis: str


@dataclass(frozen=True)
class MarketAdpSelection:
    adp: float
    basis: str
    effective_n: int
    sources: tuple[str, ...]


def _consensus(rows: list[MarketAdpRow], basis: str) -> MarketAdpSelection | None:
    usable = [row for row in rows if row.adp is not None and row.adp > 0]
    if not usable:
        return None
    weights = [max(int(row.times_drafted or 1), 1) for row in usable]
    weighted_adp = sum(row.adp * weight for row, weight in zip(usable, weights)) / sum(weights)
    return MarketAdpSelection(
        adp=weighted_adp,
        basis=basis,
        effective_n=sum(weights),
        sources=tuple(sorted({row.source for row in usable})),
    )


def select_market_adp(
    observations: Iterable[MarketAdpRow],
    *,
    teams: int,
    market_format: str,
) -> MarketAdpSelection | None:
    """Select the finest compatible external lane and form its sample-weighted consensus."""
    rows = list(observations)
    lanes = (
        (
            "external_exact_consensus",
            [row for row in rows if row.market_format == market_format and row.teams == teams],
        ),
        (
            "external_format_consensus",
            [row for row in rows if row.market_format == market_format],
        ),
        (
            "external_blind_consensus",
            [row for row in rows if row.market_format == "blind"],
        ),
    )
    for basis, candidates in lanes:
        selected = _consensus(candidates, basis)
        if selected is not None:
            return selected
    return None


def _position_group(position: object) -> str:
    normalized = str(position or "").upper()
    return normalized if normalized in {"K", "DEF"} else "SKILL"


def expand_market_only_players(
    existing_rows: list[dict],
    denominator_rows: list[dict],
    market_rows: list[dict],
) -> list[dict]:
    """Create zero-native-sample rows for every player in the compatible external lane.

    The returned rows match the native aggregate schema so the cohort builder can union them
    before applying the ADP ladder. Existing player/cohort rows are never duplicated.
    """
    if not existing_rows:
        return []
    fields = tuple(existing_rows[0])
    format_fields = tuple(
        field for field in ("league_type", "lineup_mode", "keeper_mode") if field in fields
    )
    key_fields = (
        "teams", "roster", "ppr", "td", *format_fields,
        "year", "NFL_player_id",
    )
    existing_keys = {tuple(row.get(key) for key in key_fields) for row in existing_rows}
    by_year: dict[int, list[dict]] = {}
    for row in market_rows:
        if row.get("adp") is not None:
            by_year.setdefault(int(row["year"]), []).append(row)

    expanded: list[dict] = []
    for denom in denominator_rows:
        year_rows = by_year.get(int(denom["year"]), [])
        market_format = (
            "sflx" if denom.get("roster") == "sflx"
            else str(denom.get("ppr")) if denom.get("ppr") in {"std", "half", "ppr"}
            else "blind"
        )
        teams_token = str(denom.get("teams", "ALL"))
        teams = int(teams_token.removesuffix("t")) if teams_token != "ALL" else None
        exact = [row for row in year_rows if row.get("market_format") == market_format and row.get("teams") == teams] if teams else []
        formatted = [row for row in year_rows if row.get("market_format") == market_format]
        blind = [row for row in year_rows if row.get("market_format") == "blind"]
        lane = exact or formatted or blind
        player_ids = {
            str(row["NFL_player_id"])
            for row in lane
            if _position_group(row.get("position")) == denom.get("pos_grp")
        }
        for player_id in sorted(player_ids):
            key = tuple(denom.get(field) if field != "NFL_player_id" else player_id for field in key_fields)
            if key in existing_keys:
                continue
            row = {field: None for field in fields}
            for field in ("teams", "roster", "ppr", "td", *format_fields, "year", "pos_grp"):
                row[field] = denom.get(field)
            row.update({"NFL_player_id": player_id, "n_drafted": 0, "n_auction_leagues": 0})
            expanded.append(row)
            existing_keys.add(key)
    return expanded
