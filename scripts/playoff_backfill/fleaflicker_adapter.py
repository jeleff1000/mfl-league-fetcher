"""Narrow Fleaflicker playoff evidence adapter for canonical schedule rows."""

from __future__ import annotations

import pandas as pd

from scripts.playoff_backfill.mfl_adapter import _team_id


def build_fleaflicker_evidence(
    rosters: pd.DataFrame,
    schedule: pd.DataFrame,
    *,
    db_name: str,
    year: int,
) -> pd.DataFrame:
    """Convert Fleaflicker roster/schedule rows to sidecar evidence."""
    missing_rosters = {"week", "NFL_player_id", "team_key"} - set(rosters.columns)
    missing_schedule = {"week", "team_key", "is_playoffs", "is_consolation"} - set(schedule.columns)
    if missing_rosters or missing_schedule:
        raise ValueError(
            f"missing Fleaflicker columns: rosters={sorted(missing_rosters)}, schedule={sorted(missing_schedule)}"
        )
    sch = schedule.copy()
    sch["week"] = sch["week"].astype(int)
    sch["team_id"] = sch["team_key"].map(_team_id)
    sch["is_playoffs"] = sch["is_playoffs"].fillna(0).astype(int)
    sch["is_consolation"] = sch["is_consolation"].fillna(0).astype(int)
    if "team_made_playoffs" in sch.columns:
        sch["team_made_playoffs"] = sch["team_made_playoffs"].fillna(0).astype(int)
    else:
        sch["team_made_playoffs"] = ((sch["is_playoffs"] == 1) & (sch["is_consolation"] == 0)).astype(int)
    made = sch.groupby("team_id")["team_made_playoffs"].max().to_dict()
    bracket = {
        (int(row.week), row.team_id)
        for row in sch.itertuples()
        if row.is_playoffs == 1 and row.is_consolation == 0
    }
    rows = rosters.loc[:, ["week", "NFL_player_id", "team_key"]].copy()
    rows["week"] = rows["week"].astype(int)
    rows["team_id"] = rows["team_key"].map(_team_id)
    rows["made_po_bf"] = rows["team_id"].map(made).fillna(0).astype("int8")
    rows["is_playoffs_bf"] = [int((int(week), team) in bracket) for week, team in zip(rows.week, rows.team_id)]
    rows["source_platform"] = "fleaflicker"
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
