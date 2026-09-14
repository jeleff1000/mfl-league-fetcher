"""Narrow MFL playoff evidence adapter for already-fetched roster rows."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def _team_id(value: object) -> str:
    token = str(value or "").strip()
    token = token.rsplit("_", 1)[-1]
    try:
        return str(int(token))
    except ValueError:
        return token


def build_mfl_evidence(
    rosters: pd.DataFrame,
    bracket_pairs: Iterable[tuple[int, frozenset[str]]],
    *,
    db_name: str,
    year: int,
) -> pd.DataFrame:
    """Convert MFL roster rows and championship bracket pairs to sidecar rows."""
    required = {"week", "NFL_player_id", "team_key"}
    missing = required - set(rosters.columns)
    if missing:
        raise ValueError(f"MFL roster rows are missing columns: {sorted(missing)}")
    pairs = {(int(week), frozenset(_team_id(team) for team in teams)) for week, teams in bracket_pairs}
    made_ids = set().union(*(set(teams) for _week, teams in pairs)) if pairs else set()
    week_ids: dict[int, set[str]] = {}
    for week, teams in pairs:
        week_ids.setdefault(week, set()).update(teams)
    rows = rosters.loc[:, ["week", "NFL_player_id", "team_key"]].copy()
    rows["week"] = rows["week"].astype(int)
    rows["team_id"] = rows["team_key"].map(_team_id)
    rows["made_po_bf"] = rows["team_id"].isin(made_ids).astype("int8")
    rows["is_playoffs_bf"] = [int(team in week_ids.get(week, set())) for week, team in zip(rows.week, rows.team_id)]
    rows["source_platform"] = "mfl"
    rows["source_id"] = rows["team_key"].astype(str)
    rows["evidence_kind"] = rows.apply(
        lambda r: "championship_bracket" if r.is_playoffs_bf else (
            "qualification_standings" if r.made_po_bf else "unresolved"
        ), axis=1,
    )
    rows["db_name"] = db_name
    rows["year"] = int(year)
    return rows[
        ["db_name", "year", "week", "NFL_player_id", "made_po_bf", "is_playoffs_bf",
         "source_platform", "evidence_kind", "source_id"]
    ].drop_duplicates(["db_name", "year", "week", "NFL_player_id"])
