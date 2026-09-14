"""Read-only structural and signal audit for research matchup cache lineages."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


REQUIRED_TABLES = {"league_settings", "player_fantasy", "matchup"}
OUTCOME_COLUMNS = {"opponent", "opponent_points", "win", "loss", "tie"}


def _tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public'"
        ).fetchall()
    }


def _columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {row[0] for row in con.execute(f"DESCRIBE public.{table}").fetchall()}


def audit_snapshot(root: Path) -> dict[str, object]:
    corpus = root / "corpus_snapshot.duckdb"
    con = duckdb.connect(str(corpus), read_only=True)
    tables = _tables(con)
    result: dict[str, object] = {
        "tables": sorted(tables),
        "missing_required_tables": sorted(REQUIRED_TABLES - tables),
    }
    if "league_settings" not in tables:
        con.close()
        return result

    result["league_settings_rows"] = con.execute(
        "SELECT COUNT(*) FROM public.league_settings"
    ).fetchone()[0]
    result["settings_league_years"] = con.execute(
        "SELECT COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) "
        "FROM public.league_settings"
    ).fetchone()[0]
    if "platform" in _columns(con, "league_settings"):
        result["platform_league_years"] = [
            {"platform": row[0], "league_years": row[1]}
            for row in con.execute(
                "SELECT LOWER(platform), COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) "
                "FROM public.league_settings GROUP BY 1 ORDER BY 1"
            ).fetchall()
        ]

    if "player_fantasy" in tables:
        pcols = _columns(con, "player_fantasy")
        result["player_columns"] = sorted(pcols)
        result["player_rows"] = con.execute(
            "SELECT COUNT(*) FROM public.player_fantasy"
        ).fetchone()[0]
        result["player_league_years"] = con.execute(
            "SELECT COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) "
            "FROM public.player_fantasy"
        ).fetchone()[0]
        for col in sorted({"win", "loss", "tie", "team_points", "opponent_points"} & pcols):
            result[f"null_player_{col}"] = con.execute(
                f"SELECT COUNT(*) FROM public.player_fantasy WHERE {col} IS NULL"
            ).fetchone()[0]

    if "matchup" not in tables:
        con.close()
        return result

    mcols = _columns(con, "matchup")
    result["matchup_columns"] = sorted(mcols)
    result["matchup_rows"] = con.execute(
        "SELECT COUNT(*) FROM public.matchup"
    ).fetchone()[0]
    result["matchup_league_years"] = con.execute(
        "SELECT COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR)) "
        "FROM public.matchup"
    ).fetchone()[0]
    if {"db_name", "year", "week", "manager"}.issubset(mcols):
        result["duplicate_team_week_rows"] = con.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT db_name || ':' || CAST(year AS VARCHAR) || ':' || "
            "CAST(week AS VARCHAR) || ':' || CAST(manager AS VARCHAR)) FROM public.matchup"
        ).fetchone()[0]
    result["missing_outcome_columns"] = sorted(OUTCOME_COLUMNS - mcols)

    for col in sorted(OUTCOME_COLUMNS & mcols):
        result[f"null_{col}"] = con.execute(
            f"SELECT COUNT(*) FROM public.matchup WHERE {col} IS NULL"
        ).fetchone()[0]

    if {"is_playoffs", "is_championship"}.issubset(mcols):
        result["playoff_league_years"] = con.execute(
            "SELECT COUNT(*) FROM (SELECT db_name, year FROM public.matchup "
            "WHERE CAST(is_playoffs AS INTEGER)=1 GROUP BY 1,2)"
        ).fetchone()[0]
        result["championship_league_years"] = con.execute(
            "SELECT COUNT(*) FROM (SELECT db_name, year FROM public.matchup "
            "WHERE CAST(is_championship AS INTEGER)=1 GROUP BY 1,2)"
        ).fetchone()[0]
        result["championship_outside_playoffs"] = con.execute(
            "SELECT COUNT(*) FROM public.matchup WHERE "
            "CAST(is_championship AS INTEGER)=1 AND "
            "COALESCE(CAST(is_playoffs AS INTEGER),0)<>1"
        ).fetchone()[0]
    else:
        result["playoff_league_years"] = None
        result["championship_league_years"] = None
        result["championship_outside_playoffs"] = None

    if {"is_playoffs", "is_championship", "week"}.issubset(mcols):
        team = "CAST(franchise_id AS VARCHAR)" if "franchise_id" in mcols else "CAST(manager AS VARCHAR)"
        result["premature_championship_teams"] = con.execute(
            f"""
            WITH ch AS (
              SELECT db_name, year, {team} AS team_id,
                     MAX(CAST(week AS INTEGER)) AS championship_week
              FROM public.matchup
              WHERE CAST(is_championship AS INTEGER)=1
              GROUP BY 1,2,3
            ), lp AS (
              SELECT db_name, year, {team} AS team_id,
                     MAX(CAST(week AS INTEGER)) FILTER
                       (WHERE CAST(is_playoffs AS INTEGER)=1) AS last_playoff_week
              FROM public.matchup
              GROUP BY 1,2,3
            )
            SELECT COUNT(*) FROM ch JOIN lp USING (db_name, year, team_id)
            WHERE last_playoff_week > championship_week
            """
        ).fetchone()[0]
    else:
        result["premature_championship_teams"] = None

    con.close()
    return result


def rank_lineages(records: list[dict[str, object]]) -> dict[str, object]:
    """Rank only quality-qualified lineages; never crown an invalid one."""
    scored = []
    for record in records:
        failures = []
        if record.get("missing_required_tables"):
            failures.append("missing_required_tables")
        if record.get("missing_outcome_columns"):
            failures.append("missing_outcome_columns")
        if record.get("championship_outside_playoffs") not in (0, None):
            failures.append("championship_outside_playoffs")
        if record.get("premature_championship_teams") not in (0, None):
            failures.append("premature_championship_teams")
        nulls = sum(
            int(record.get(key) or 0)
            for key in (
                "null_player_win", "null_player_loss", "null_player_tie",
                "null_player_team_points", "null_player_opponent_points",
                "null_opponent", "null_opponent_points", "null_win", "null_loss", "null_tie",
            )
        )
        score = (
            0 if failures else 1,
            int(record.get("matchup_league_years") or 0),
            int(record.get("player_league_years") or 0),
            -int(record.get("duplicate_team_week_rows") or 0),
            -nulls,
        )
        scored.append({"lineage": record.get("lineage"), "score": score, "failures": failures})
    scored.sort(key=lambda item: item["score"], reverse=True)
    winner = None
    if scored and not scored[0]["failures"]:
        if len(scored) == 1 or scored[0]["score"] > scored[1]["score"]:
            winner = scored[0]["lineage"]
    return {"winner": winner, "ranked": scored}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--lineage", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = {"lineage": args.lineage, **audit_snapshot(args.root)}
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
