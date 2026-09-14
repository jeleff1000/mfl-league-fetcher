"""Build the exact MFL week inventory needing source recovery.

The target unit is (db_name, year, week).  A target is included when either
the canonical player table has fewer than three starter rows per configured
team or fewer than three winner rows per configured team.  This is an
inventory/manifest step only: it never mutates the research snapshot.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--platform", choices=["mfl", "fleaflicker", "sleeper"], default="mfl")
    ap.add_argument("--expected-groups", type=int, default=9915)
    ap.add_argument("--expected-league-years", type=int, default=2295)
    args = ap.parse_args()

    con = duckdb.connect(str(args.snapshot), read_only=True)
    pcols = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    scols = {r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()}
    required_p = {"db_name", "year", "week", "is_started", "win"}
    required_s = {"db_name", "year", "platform", "league_key", "num_teams"}
    missing = {
        "player_fantasy": sorted(required_p - pcols),
        "league_settings": sorted(required_s - scols),
    }
    if any(missing.values()):
        raise SystemExit(f"canonical schema missing required columns: {missing}")

    rows = con.execute(
        """
        WITH counts AS (
          SELECT
            CAST(p.db_name AS VARCHAR) AS db_name,
            CAST(p.year AS INTEGER) AS year,
            CAST(p.week AS INTEGER) AS week,
            MAX(CAST(s.num_teams AS INTEGER)) AS teams,
            MAX(CAST(s.league_key AS VARCHAR)) AS source_id,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER) = 1) AS starter_rows,
            COUNT(*) FILTER (WHERE CAST(p.win AS INTEGER) = 1) AS winner_rows,
            COUNT(*) FILTER (
              WHERE CAST(p.is_started AS INTEGER) = 1 AND p.win IS NULL
            ) AS started_missing_outcome_rows
          FROM public.player_fantasy p
          JOIN public.league_settings s
            ON s.db_name = p.db_name
           AND CAST(s.year AS INTEGER) = CAST(p.year AS INTEGER)
          WHERE LOWER(CAST(s.platform AS VARCHAR)) = ?
          GROUP BY 1, 2, 3
        )
        SELECT *,
          starter_rows < 3 * teams AS low_starters,
          winner_rows < 3 * teams AS low_winners
        FROM counts
        WHERE starter_rows < 3 * teams OR winner_rows < 3 * teams
        ORDER BY db_name, year, week
        """,
        [args.platform],
    ).fetchall()
    con.close()

    groups = []
    by_season: dict[tuple[str, int], dict] = {}
    for db, year, week, teams, source_id, starters, winners, missing_outcomes, low_s, low_w in rows:
        db = str(db)
        year = int(year)
        week = int(week)
        seed = re.fullmatch(r"smpl_mfl_(\d{4})_(\d+)", db)
        row = {
            "db_name": db,
            "year": year,
            "week": week,
            "teams": int(teams),
            "source_id": str(source_id or ""),
            "starter_rows": int(starters),
            "winner_rows": int(winners),
            "started_missing_outcome_rows": int(missing_outcomes),
            "low_starters": bool(low_s),
            "low_winners": bool(low_w),
            "source_seed_year": int(seed.group(1)) if seed else None,
            "source_seed_id": seed.group(2) if seed else None,
        }
        groups.append(row)
        key = (db, year)
        season = by_season.setdefault(
            key,
            {
                "db_name": db,
                "year": year,
                "platform": args.platform,
                "source_id": str(source_id or ""),
                "source_seed_year": row["source_seed_year"],
                "source_seed_id": row["source_seed_id"],
                "weeks": [],
                "target_groups": 0,
                "started_missing_outcome_rows": 0,
            },
        )
        season["weeks"].append(week)
        season["target_groups"] += 1
        season["started_missing_outcome_rows"] += int(missing_outcomes)

    league_years = list(by_season.values())
    for row in league_years:
        row["weeks"] = sorted(set(row["weeks"]))

    summary = {
        "schema_version": 1,
        "target_kind": f"{args.platform}-underpopulated-player-week-source-evidence",
        "unit": "db_name/year/week",
        "threshold": "starter_rows < 3*num_teams OR winner_rows < 3*num_teams",
        "group_count": len(groups),
        "league_year_count": len(league_years),
        "low_starter_groups": sum(r["low_starters"] for r in groups),
        "low_winner_groups": sum(r["low_winners"] for r in groups),
        "overlap_groups": sum(r["low_starters"] and r["low_winners"] for r in groups),
        "started_missing_outcome_rows": sum(r["started_missing_outcome_rows"] for r in groups),
        "targets": league_years,
        "groups": groups,
    }
    if args.expected_groups and len(groups) != args.expected_groups:
        raise SystemExit(f"fail-closed group count: expected {args.expected_groups}, got {len(groups)}")
    if args.expected_league_years and len(league_years) != args.expected_league_years:
        raise SystemExit(
            f"fail-closed league-year count: expected {args.expected_league_years}, got {len(league_years)}"
        )
    if any(not row["source_id"] for row in league_years):
        raise SystemExit(f"fail-closed inventory: one or more {args.platform} league-years lack source_id")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in summary if k not in {"targets", "groups"}}, sort_keys=True))


if __name__ == "__main__":
    main()
