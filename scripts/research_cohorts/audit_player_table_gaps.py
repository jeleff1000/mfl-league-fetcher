"""Player-table-only gap inventory for the canonical research lake.

This audit deliberately does not use ``matchup`` to define the population.
Its unit of record is a player row in ``public.player_fantasy`` and its
reported league-season inventory is derived from those rows only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def cols(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}


def first(candidates: tuple[str, ...], available: set[str]) -> str | None:
    return next((c for c in candidates if c in available), None)


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def audit(snapshot: Path) -> dict[str, object]:
    con = duckdb.connect(str(snapshot), read_only=True)
    tables = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public'").fetchall()}
    if "player_fantasy" not in tables:
        raise SystemExit("canonical lake has no public.player_fantasy table")

    available = cols(con)
    required = {"db_name", "year", "week", "is_started"}
    missing = required - available
    if missing:
        raise SystemExit(f"player_fantasy missing required columns: {sorted(missing)}")

    win = first(("win", "is_win", "won"), available)
    loss = first(("loss", "is_loss", "lost"), available)
    tie = first(("tie", "is_tie", "tied"), available)
    team_points = first(("team_points", "fantasy_team_points", "manager_points"), available)
    opponent_points = first(("opponent_points", "opp_points"), available)
    playoff = first(("is_playoffs", "is_playoff", "playoff", "playoff_start", "started_playoffs"), available)
    championship = first(("is_championship", "is_champ", "championship", "champ_start", "started_championship"), available)
    clutch = first(("clutch_equity", "clutch", "clutch_value"), available)

    # These are deliberately player-table fields. If a field is absent, the
    # report records that fact instead of pretending a matchup field resolves it.
    outcome_terms = []
    if win and loss:
        outcome_terms.append(f"({qident(win)} IS NOT NULL OR {qident(loss)} IS NOT NULL" + (f" OR {qident(tie)} IS NOT NULL" if tie else "") + ")")
    if team_points and opponent_points:
        outcome_terms.append(f"({qident(team_points)} IS NOT NULL AND {qident(opponent_points)} IS NOT NULL)")
    outcome_present = " OR ".join(outcome_terms) if outcome_terms else "FALSE"

    null_signal = lambda col: f"{qident(col)} IS NULL" if col else "FALSE"
    select = [
        "CAST(db_name AS VARCHAR) AS db_name",
        "CAST(year AS INTEGER) AS year",
        "COUNT(*) AS player_rows",
        "COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1) AS started_rows",
        "COUNT(*) FILTER (WHERE is_started IS NULL) AS null_start_rows",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND NOT ({outcome_present})) AS started_missing_outcome",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND {null_signal(playoff)}) AS started_missing_playoff",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND {null_signal(championship)}) AS started_missing_championship",
        f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND {null_signal(clutch)}) AS started_missing_clutch",
    ]
    if team_points and opponent_points:
        select.append(f"COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1 AND ({qident(team_points)} IS NULL OR {qident(opponent_points)} IS NULL)) AS started_missing_team_game_denominator")
    else:
        select.append("COUNT(*) AS started_missing_team_game_denominator")

    rows = con.execute("SELECT " + ", ".join(select) + " FROM public.player_fantasy GROUP BY 1,2 ORDER BY 1,2").fetchall()
    names = [d[0] for d in con.description]
    totals = con.execute("SELECT " + ", ".join(select[2:]) + " FROM public.player_fantasy").fetchone()
    total = dict(zip(names[2:], totals))
    result = {
        "population_source": "public.player_fantasy only",
        "player_rows": int(con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]),
        "player_league_years": int(con.execute("SELECT COUNT(*) FROM (SELECT db_name, year FROM public.player_fantasy GROUP BY 1,2)").fetchone()[0]),
        "resolved_columns": {"win": win, "loss": loss, "tie": tie, "team_points": team_points, "opponent_points": opponent_points, "playoff": playoff, "championship": championship, "clutch": clutch},
        "available_columns": sorted(available),
        "totals": total,
        "league_season_rows": [dict(zip(names, row)) for row in rows],
    }
    con.close()
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    result = audit(args.snapshot)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"player_rows": result["player_rows"], "player_league_years": result["player_league_years"], "totals": result["totals"], "resolved_columns": result["resolved_columns"]}, indent=2))


if __name__ == "__main__":
    main()
