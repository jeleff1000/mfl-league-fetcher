"""
sota_recon/recon_negative_snaps.py  --  the NEGATIVE snap witness (2012+ disproof arm).

The appearance lanes used snap counts as POSITIVE evidence (participation -> row). This
lane runs the disproof direction no value comparison can see: a v26 row claiming
nonzero countable stats in a snap-covered game whose player appears in NEITHER team's
snap table. A player cannot accumulate stats on zero snaps -> every hit is a
misattribution candidate (wrong player, wrong week, or phantom row).

Guards (never over-claim):
  * only games where BOTH sides' snap tables have lines (scrape-coverage guard)
  * only bio-linked players (team/DST rows never join bio and are out of scope)
  * kickers/punters ARE in PFR snap tables via special_teams -- no position carve-out
  * stat predicate = core countable atoms only (yards can be negative; TDs/receptions/
    attempts/etc. > 0), so a 0-stat appearance row never trips it

READ-ONLY: enumerates candidates to CSV; adjudication is a wave's job.

    python -m scripts.sota_recon.recon_negative_snaps
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import sources as S

OUT_DIR = os.path.join(S.DATA_LAKE, "derived", "validation", "sota_recon_master",
                       "cell_witness")

ACTIVITY_COLS = ["completions", "attempts", "carries", "receptions", "targets",
                 "passing_tds", "rushing_tds", "receiving_tds", "fg_att", "pat_att",
                 "punts", "def_interceptions", "def_sacks", "def_tackles_solo",
                 "def_tackle_assists", "kickoff_returns", "punt_returns", "fumbles"]


def run() -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    os.makedirs(OUT_DIR, exist_ok=True)
    hs = Path(S.registry()["pfr_box_home_snaps"].path).as_posix()
    vs = Path(S.registry()["pfr_box_vis_snaps"].path).as_posix()
    tg = Path(S.TEAM_GAMES.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()

    activity = " + ".join(f"COALESCE(t.{c}, 0)" for c in ACTIVITY_COLS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE snaps AS
        SELECT regexp_extract(player_link_ids, '^([^,]+)', 1) AS pid, boxscore_id,
               CASE WHEN src = 'h' THEN TRUE ELSE FALSE END AS is_home
        FROM (SELECT *, 'h' AS src FROM '{hs}'
              UNION ALL SELECT *, 'v' AS src FROM '{vs}')
        WHERE player_link_ids IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE covered AS
        SELECT g.boxscore_id, g.year, CAST(g.week AS INT) AS week, g.season_type
        FROM (SELECT DISTINCT boxscore_id, year, week, season_type FROM '{tg}') g
        WHERE EXISTS (SELECT 1 FROM snaps s WHERE s.boxscore_id = g.boxscore_id
                      AND s.is_home)
          AND EXISTS (SELECT 1 FROM snaps s WHERE s.boxscore_id = g.boxscore_id
                      AND NOT s.is_home)""")
    hits = con.execute(f"""
        SELECT bio.pfr_id, t.player_week, t.year, CAST(t.week AS INT) AS week,
               t.season_type, t.nfl_team, ({activity}) AS activity_sum
        FROM '{v26}' t
        JOIN '{bio}' bio USING (NFL_player_id)
        JOIN covered c ON c.year = t.year AND c.week = CAST(t.week AS INT)
                      AND c.season_type = t.season_type
        WHERE t.year >= 2012 AND bio.pfr_id IS NOT NULL AND ({activity}) > 0
          AND NOT EXISTS (
            SELECT 1 FROM snaps s
            JOIN covered c2 ON c2.boxscore_id = s.boxscore_id
            WHERE s.pid = bio.pfr_id AND c2.year = t.year
              AND c2.week = CAST(t.week AS INT) AND c2.season_type = t.season_type)
        GROUP BY ALL""").fetchall()

    import csv
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    p = os.path.join(OUT_DIR, f"negative_snap_candidates_{stamp}.csv")
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pfr_id", "player_week", "year", "week", "season_type",
                    "nfl_team", "activity_sum"])
        w.writerows(hits)
    by_year = {}
    for r in hits:
        by_year[int(r[2])] = by_year.get(int(r[2]), 0) + 1
    out = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
           "candidates": len(hits), "by_year": by_year, "csv": p}
    with open(os.path.join(OUT_DIR, "NEGATIVE_SNAP_SUMMARY.json"), "w",
              encoding="utf-8") as f:
        json.dump(out, f, indent=1)
    con.close()
    return out


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    r = run()
    print(f"negative-snap candidates (stats with zero snaps, 2012+): {r['candidates']}")
    print("by year:", dict(sorted(r["by_year"].items())))
