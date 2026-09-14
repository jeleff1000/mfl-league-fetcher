"""
sota_recon/recon_stragglers.py  --  LANE: every player row must sit on a scheduled game.

Joe's check (2026-07-10): "if a team has one player in a week he's probably in the wrong
week -- we have the all-time NFL schedule, reconcile on it." Two straggler classes:

  ORPHAN_ROW    a player row on a (franchise, year, week) where the schedule says the team
                did NOT play -- the row is misdated, misattributed, or phantom. For each,
                report whether the player has a scheduled-game week nearby WITHOUT a row
                (the shift suggestion).
  LONE_PLAYERS  a scheduled team-game where v26 has only 1-2 player rows -- either the
                thin-game class (backfillable) or stray misweeked rows pretending to be
                a game.

Anchored on nfl_team_games_all (franchise-keyed, both season types).

    python -m scripts.sota_recon.recon_stragglers [--csv out.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .sources import TEAM_GAMES, latest_v26

LONE_MAX = 2


def run(src: str | None = None, csv: str | None = None) -> dict:
    vq = Path(src or latest_v26()).as_posix()
    tg = Path(TEAM_GAMES.path).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    con.execute(f"""
        CREATE TEMP TABLE sched AS
        SELECT DISTINCT team_fid, year, CAST(week AS INT) AS week, season_type
        FROM '{tg}'""")

    # ORPHAN_ROW: player rows whose (franchise, year, week, season_type) has no scheduled game
    orphans = con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE v.year < 1978),
               MIN(v.year), MAX(v.year)
        FROM '{vq}' v
        LEFT JOIN sched s ON s.team_fid = v.nfl_franchise_number AND s.year = v.year
          AND s.week = CAST(v.week AS INT) AND s.season_type = v.season_type
        WHERE v.nfl_franchise_number IS NOT NULL AND v.week IS NOT NULL
          AND s.team_fid IS NULL""").fetchone()

    orphan_rows = con.execute(f"""
        SELECT v.player_week, v.NFL_player_id, v.year, CAST(v.week AS INT) AS week,
               v.season_type, v.nfl_team, v.position,
               COALESCE(v.passing_yards,0)+COALESCE(v.rushing_yards,0)
                 +COALESCE(v.receiving_yards,0) AS yds,
               -- shift suggestion: nearest scheduled week for this franchise that year
               (SELECT MIN(s2.week) FROM sched s2
                WHERE s2.team_fid = v.nfl_franchise_number AND s2.year = v.year
                  AND s2.season_type = v.season_type
                  AND ABS(s2.week - CAST(v.week AS INT)) <= 2) AS nearby_scheduled_week
        FROM '{vq}' v
        LEFT JOIN sched s ON s.team_fid = v.nfl_franchise_number AND s.year = v.year
          AND s.week = CAST(v.week AS INT) AND s.season_type = v.season_type
        WHERE v.nfl_franchise_number IS NOT NULL AND v.week IS NOT NULL
          AND s.team_fid IS NULL
        ORDER BY v.year, v.week LIMIT 10000""").fetchall()

    # LONE_PLAYERS: scheduled games where v26 holds only 1-2 rows
    lone = con.execute(f"""
        WITH counts AS (
          SELECT s.team_fid, s.year, s.week, s.season_type, COUNT(v.player_week) AS n
          FROM sched s
          LEFT JOIN '{vq}' v ON v.nfl_franchise_number = s.team_fid AND v.year = s.year
            AND CAST(v.week AS INT) = s.week AND v.season_type = s.season_type
          GROUP BY 1, 2, 3, 4)
        SELECT COUNT(*) FILTER (WHERE n = 0) AS empty_games,
               COUNT(*) FILTER (WHERE n BETWEEN 1 AND {LONE_MAX}) AS lone_games,
               COUNT(*) FILTER (WHERE n BETWEEN 1 AND {LONE_MAX} AND year >= 1978) AS lone_modern
        FROM counts""").fetchone()
    con.close()

    if csv and orphan_rows:
        import csv as _csv
        with open(csv, "w", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            w.writerow(["player_week", "NFL_player_id", "year", "week", "season_type",
                        "nfl_team", "position", "yds", "nearby_scheduled_week"])
            w.writerows(orphan_rows)
    return {"orphan_rows": orphans[0], "orphan_pre1978": orphans[1],
            "orphan_years": (orphans[2], orphans[3]),
            "scheduled_games_with_zero_rows": lone[0],
            "scheduled_games_with_1_2_rows": lone[1],
            "lone_modern_1978plus": lone[2],
            "sample": orphan_rows[:8]}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--src", default=None)
    a = ap.parse_args()
    r = run(a.src, a.csv)
    print(f"ORPHAN player rows (no scheduled game): {r['orphan_rows']:,} "
          f"(pre-1978: {r['orphan_pre1978']:,}; years {r['orphan_years']})")
    print(f"scheduled team-games with ZERO v26 rows: {r['scheduled_games_with_zero_rows']:,}")
    print(f"scheduled team-games with 1-{LONE_MAX} rows: {r['scheduled_games_with_1_2_rows']:,} "
          f"(modern 1978+: {r['lone_modern_1978plus']:,})")
    for s in r["sample"]:
        print("  ", s)
