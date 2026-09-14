"""Audit player-row completeness after temporary sidecar overlays.

The canonical DuckDB snapshot and both sidecars are read-only inputs.  The
overlay exists only as temporary DuckDB views; no cache, lineage, or schema is
written.  The population is ``public.player_fantasy`` rows, not matchup rows.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


SETTINGS_FIELDS = (
    "scoring_pass_td", "playoff_teams", "roster_FLX", "roster_SUPER_FLEX",
    "roster_IDP", "sleeper_best_ball",
)


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def path_sql(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def first(candidates: tuple[str, ...], available: set[str]) -> str | None:
    return next((x for x in candidates if x in available), None)


def audit(base: Path, settings: Path, position: Path, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH {path_sql(base)} AS lake (READ_ONLY)")
    try:
        pcols = {r[0] for r in con.execute("DESCRIBE lake.public.player_fantasy").fetchall()}
        scols = {r[0] for r in con.execute("DESCRIBE lake.public.league_settings").fetchall()}
        settings_overlay_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({path_sql(settings)})").fetchall()}
        position_overlay_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({path_sql(position)})").fetchall()}
        if not {"db_name", "year"} <= settings_overlay_cols:
            raise SystemExit("settings overlay lacks db_name/year")
        if not {"nfl_id", "season_year", "position_fill"} <= position_overlay_cols:
            raise SystemExit("position overlay lacks nfl_id/season_year/position_fill")

        settings_select = []
        for c in scols:
            if c in SETTINGS_FIELDS:
                settings_select.append(f"COALESCE(o.{ident(c)},s.{ident(c)}) AS {ident(c)}")
            else:
                settings_select.append(f"s.{ident(c)}")
        con.execute(f"""
          CREATE OR REPLACE TEMP VIEW settings_v AS
          SELECT {', '.join(settings_select)}
          FROM lake.public.league_settings s
          LEFT JOIN read_parquet({path_sql(settings)}) o
            ON CAST(s.db_name AS VARCHAR)=CAST(o.db_name AS VARCHAR)
           AND CAST(s.year AS INTEGER)=CAST(o.year AS INTEGER)
        """)

        player_select = []
        for c in pcols:
            if c == "position":
                player_select.append(
                    f"COALESCE(NULLIF(TRIM(CAST(p.{ident(c)} AS VARCHAR)),''),o.position_fill) AS {ident(c)}"
                )
            else:
                player_select.append(f"p.{ident(c)}")
        con.execute(f"""
          CREATE OR REPLACE TEMP VIEW player_v AS
          SELECT {', '.join(player_select)}
          FROM lake.public.player_fantasy p
          LEFT JOIN read_parquet({path_sql(position)}) o
            ON CAST(p.NFL_player_id AS VARCHAR)=CAST(o.nfl_id AS VARCHAR)
           AND CAST(p.year AS INTEGER)=CAST(o.season_year AS INTEGER)
        """)

        win = first(("win", "is_win", "won"), pcols)
        loss = first(("loss", "is_loss", "lost"), pcols)
        tie = first(("tie", "is_tie", "tied"), pcols)
        team_points = first(("team_points", "fantasy_team_points", "manager_points"), pcols)
        opponent_points = first(("opponent_points", "opp_points"), pcols)
        playoff = first(("is_playoffs", "is_playoff", "playoff", "started_playoffs"), pcols)
        championship = first(("is_championship", "is_champ", "championship", "champ_start", "started_championship"), pcols)
        clutch = first(("clutch_equity", "clutch", "clutch_value"), pcols)
        rostered = first(("is_rostered", "rostered"), pcols)

        outcome_terms = []
        if win and loss:
            flags = [f"p.{ident(win)} IS NOT NULL", f"p.{ident(loss)} IS NOT NULL"]
            if tie:
                flags.append(f"p.{ident(tie)} IS NOT NULL")
            outcome_terms.append("(" + " OR ".join(flags) + ")")
        if team_points and opponent_points:
            outcome_terms.append(f"(p.{ident(team_points)} IS NOT NULL AND p.{ident(opponent_points)} IS NOT NULL)")
        outcome_present = " OR ".join(outcome_terms) if outcome_terms else "FALSE"
        started = "CAST(p.is_started AS INTEGER)=1"
        def metric_filter(condition: str | None) -> str:
            return f"COUNT(*) FILTER (WHERE {condition})" if condition else "CAST(NULL AS BIGINT)"

        select = [
            "LOWER(TRIM(CAST(s.platform AS VARCHAR))) AS platform",
            "CAST(p.year AS INTEGER) AS year",
            "CAST(p.week AS INTEGER) AS week",
            "COUNT(DISTINCT p.db_name) AS league_count",
            "COUNT(*) AS player_rows",
            f"COUNT(*) FILTER (WHERE {started}) AS started_rows",
            f"COUNT(DISTINCT p.db_name) FILTER (WHERE {started}) AS started_leagues",
            f"COUNT(*) FILTER (WHERE {started} AND NOT ({outcome_present})) AS started_missing_outcome",
            f"{metric_filter(f'{started} AND p.' + ident(playoff) + ' IS NULL' if playoff else None)} AS started_missing_playoff",
            f"{metric_filter(f'{started} AND p.' + ident(championship) + ' IS NULL' if championship else None)} AS started_missing_championship",
            f"{metric_filter(f'{started} AND p.' + ident(clutch) + ' IS NULL' if clutch else None)} AS started_missing_clutch",
            f"{metric_filter(f'{started} AND p.' + ident(rostered) + ' IS NULL' if rostered else None)} AS started_missing_rostered",
            f"{metric_filter(f'{started} AND (p.' + ident(team_points) + ' IS NULL OR p.' + ident(opponent_points) + ' IS NULL)' if team_points and opponent_points else None)} AS started_missing_team_game_denominator",
            f"COUNT(*) FILTER (WHERE p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='') AS position_missing_after_overlay",
        ]
        group_sql = "SELECT " + ", ".join(select) + " FROM player_v p JOIN settings_v s USING (db_name,year) GROUP BY 1,2,3 ORDER BY 1,2,3"
        con.execute(f"COPY ({group_sql}) TO ? (FORMAT PARQUET)", [str(out / "league_week_audit.parquet")])
        con.execute(f"COPY (SELECT platform,year,COUNT(*) AS league_week_groups,SUM(league_count) AS league_count_sum,SUM(player_rows) AS player_rows,SUM(started_rows) AS started_rows,SUM(started_missing_outcome) AS started_missing_outcome,SUM(started_missing_playoff) AS started_missing_playoff,SUM(started_missing_championship) AS started_missing_championship,SUM(started_missing_clutch) AS started_missing_clutch,SUM(started_missing_rostered) AS started_missing_rostered,SUM(started_missing_team_game_denominator) AS started_missing_team_game_denominator,SUM(position_missing_after_overlay) AS position_missing_after_overlay FROM ({group_sql}) q GROUP BY 1,2 ORDER BY 1,2) TO ? (FORMAT PARQUET)", [str(out / "league_year_audit.parquet")])

        totals = con.execute(f"""
          SELECT COUNT(*) player_rows,
                 COUNT(DISTINCT (p.db_name,p.year)) league_years,
                 COUNT(*) FILTER (WHERE {started} AND NOT ({outcome_present})) started_missing_outcome,
                 {metric_filter(f'{started} AND p.' + ident(playoff) + ' IS NULL' if playoff else None)} started_missing_playoff,
                 {metric_filter(f'{started} AND p.' + ident(championship) + ' IS NULL' if championship else None)} started_missing_championship,
                 {metric_filter(f'{started} AND p.' + ident(clutch) + ' IS NULL' if clutch else None)} started_missing_clutch,
                 {metric_filter(f'{started} AND p.' + ident(rostered) + ' IS NULL' if rostered else None)} started_missing_rostered,
                 {metric_filter(f'{started} AND (p.' + ident(team_points) + ' IS NULL OR p.' + ident(opponent_points) + ' IS NULL)' if team_points and opponent_points else None)} started_missing_team_game_denominator,
                 COUNT(*) FILTER (WHERE p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='') position_missing_after_overlay
          FROM player_v p JOIN settings_v s USING (db_name,year)
        """).fetchone()
        names = [d[0] for d in con.description]
        report = {
            "population": "player_fantasy rows after temporary position/settings overlays",
            "cache_mutated": False,
            "new_lineage": False,
            "new_columns": [],
            "resolved_columns": {"win": win, "loss": loss, "tie": tie, "team_points": team_points, "opponent_points": opponent_points, "playoff": playoff, "championship": championship, "clutch": clutch, "rostered": rostered},
            "totals": dict(zip(names, [int(x) if isinstance(x, (int, float)) else None for x in totals])),
            "files": ["league_week_audit.parquet", "league_year_audit.parquet"],
        }
        (out / "player_row_audit_summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--settings", type=Path, required=True)
    ap.add_argument("--position", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    audit(args.base, args.settings, args.position, args.out)


if __name__ == "__main__":
    main()
