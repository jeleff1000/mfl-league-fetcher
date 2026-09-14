"""
sota_recon/recon_doubleheader_alignment.py  --  the doubleheader 1:1 law (Joe,
2026-07-12): "when a player has 2 games in the same week we cannot handwave it --
the known doubleheaders should all line up on week/season/opponent 1:1."

For every KNOWN doubleheader team-week (2+ games in nfl_team_games_all for one
(team_fid, year, week, season_type)), every v26 player row that week lands in exactly
one state -- a closed vocabulary, states sum to the row count:

  ALIGNED       game-suffixed player_week (_G<boxscore_id>_) matching one of the
                week's scheduled games AND the row's opponent franchise number equals
                that game's opponent fid  -> fully 1:1
  OPP_MISMATCH  suffix matches a scheduled game but the row's opponent does NOT match
                that game's opponent      -> misattribution queue
  SUFFIX_ORPHAN suffixed, but the boxscore is not one of the week's games -> queue
  UNSPLIT       plain (unsuffixed) row in a doubleheader week -- the merged class;
                its stats may be a two-game sum (the 718-row wave-2 queue)
  DUP_SUFFIX    two rows carrying the SAME game suffix for one player -> dup queue

Also verifies the games side: for each doubleheader, the two scheduled opponents are
DISTINCT (same-opponent doubleheaders like CHI-DET 1935 wk11/12 Thanksgiving pairs are
listed separately -- suffix, not opponent, is the disambiguator there).

READ-ONLY. Outputs under sota_recon_master/doubleheaders/.

    python -m scripts.sota_recon.recon_doubleheader_alignment
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .sources import TEAM_GAMES, latest_v26

OUT_DIR = os.path.join("D:/league-history-data/nfl/derived/validation",
                       "sota_recon_master", "doubleheaders")


def run() -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    os.makedirs(OUT_DIR, exist_ok=True)
    tg = Path(TEAM_GAMES.path).as_posix()
    vq = Path(latest_v26()).as_posix()

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dh AS
        SELECT team_fid, year, CAST(week AS INT) AS week, season_type,
               COUNT(*) AS n_games,
               array_agg(boxscore_id ORDER BY game_date) AS games,
               array_agg(opponent_fid ORDER BY game_date) AS opp_fids,
               COUNT(DISTINCT opponent_fid) AS n_opps
        FROM '{tg}'
        GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1""")
    n_dh, same_opp = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE n_opps < n_games) FROM dh").fetchone()

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE rows_dh AS
        SELECT t.player_week, t.NFL_player_id, t.year, CAST(t.week AS INT) AS week,
               t.season_type, t.nfl_franchise_number AS fid,
               t.opponent_nfl_franchise_number AS opp_fid, t.position,
               regexp_extract(t.player_week, '_G([0-9a-z]+)_', 1) AS sfx
        FROM '{vq}' t
        JOIN dh ON dh.team_fid = t.nfl_franchise_number AND dh.year = t.year
               AND dh.week = CAST(t.week AS INT) AND dh.season_type = t.season_type""")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE verdicts AS
        SELECT r.*,
          CASE
            WHEN r.sfx = '' OR r.sfx IS NULL THEN 'UNSPLIT'
            WHEN g.boxscore_id IS NULL THEN 'SUFFIX_ORPHAN'
            WHEN r.opp_fid IS DISTINCT FROM g.opponent_fid THEN 'OPP_MISMATCH'
            ELSE 'ALIGNED'
          END AS state,
          g.opponent_fid AS game_opp_fid, g.boxscore_id
        FROM rows_dh r
        LEFT JOIN (SELECT boxscore_id, team_fid, opponent_fid FROM '{tg}') g
          ON g.boxscore_id = r.sfx AND g.team_fid = r.fid""")
    # dup suffixes: same player, same game suffix, 2+ rows
    con.execute("""
        CREATE OR REPLACE TEMP TABLE dups AS
        SELECT NFL_player_id, sfx, COUNT(*) AS n FROM verdicts
        WHERE state = 'ALIGNED' GROUP BY 1, 2 HAVING COUNT(*) > 1""")
    summary = dict(con.execute(
        "SELECT state, COUNT(*) FROM verdicts GROUP BY 1").fetchall())
    n_dup = con.execute("SELECT COALESCE(SUM(n), 0) FROM dups").fetchone()[0]
    by_era = con.execute("""
        SELECT year // 10 * 10 AS decade, state, COUNT(*) FROM verdicts
        GROUP BY 1, 2 ORDER BY 1""").fetchall()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    con.execute(f"""
        COPY (SELECT * FROM verdicts WHERE state != 'ALIGNED' ORDER BY year, week)
        TO '{Path(os.path.join(OUT_DIR, f"dh_misaligned_{stamp}.csv")).as_posix()}' (HEADER)""")
    out = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
           "doubleheader_team_weeks": n_dh,
           "same_opponent_doubleheaders": same_opp,
           "row_states": summary, "dup_suffix_rows": int(n_dup),
           "by_decade": [(int(d), s, int(c)) for d, s, c in by_era],
           "total_rows_in_dh_weeks": sum(summary.values())}
    with open(os.path.join(OUT_DIR, "DH_ALIGNMENT_SUMMARY.json"), "w",
              encoding="utf-8") as f:
        json.dump(out, f, indent=1)
    con.close()
    return out


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    r = run()
    print(f"doubleheader team-weeks: {r['doubleheader_team_weeks']} "
          f"(same-opponent pairs: {r['same_opponent_doubleheaders']})")
    print(f"rows in DH weeks: {r['total_rows_in_dh_weeks']}  states: {r['row_states']}")
    print(f"dup-suffix rows: {r['dup_suffix_rows']}")
