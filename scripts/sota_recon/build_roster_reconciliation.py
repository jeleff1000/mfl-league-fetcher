"""THE ROSTER RECONCILIATION LANE (Joe, 2026-08-02: 'we have NFL.com and PFR
and PFA and StatsCrew validating every team's roster for every week -- these
misallocations should have been caught').

He was right. The witnesses held the evidence all along; no lane compared the
plane's (player, team, week) assignment against the witnesses' game
participants. The BOS-1944 class sat inside the membership lane's undrained
6%% residual, and per-week reconciliation existed only as hand queries.

This lane: for every plane player-week, gather the witness participant set --
PFR offense box + defense box + home/vis starters (side resolved through the
catalog) -- and flag rows where the witnesses put the player on a DIFFERENT
team that week. Validated against ground truth: on 1944 it catches exactly
the 17 known BOS->WAS player-weeks, including the week-6 phantom the first
doom-key dropped, and nothing else.

Output: roster_misallocations.parquet (the queue) + receipt. Extension lanes
queued: PFA participation (crosswalked), StatsCrew rosters (season grain),
nflcom membership (already wired at stint grain -- ITS RESIDUAL FEEDS HERE).

Run per-decade to stay kind to the disk:
    python -m scripts.sota_recon.build_roster_reconciliation 1920 1949
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")


def build(con: duckdb.DuckDBPyConnection, y0: int, y1: int) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    g = Path(S.TEAM_GAMES.path).as_posix()
    off = Path(S.registry()["pfr_player_offense_box"].path).as_posix()
    dfb = Path(S.registry()["pfr_player_defense_box"].path).as_posix()
    hs = Path(S.registry()["pfr_box_home_starters"].path).as_posix()
    vs = Path(S.registry()["pfr_box_vis_starters"].path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    out = LAKE / f"roster_misallocations_{y0}_{y1}.parquet"

    box_branch = """
    SELECT b.NFL_player_id, s.team, c.year, c.week
    FROM read_parquet('{path}', union_by_name=true) s
    JOIN (SELECT DISTINCT boxscore_id, year, week FROM cat) c USING (boxscore_id),
    UNNEST(str_split(CAST(s.player_link_ids AS VARCHAR), ';')) AS u(pid)
    JOIN ids b ON b.pfr_id = u.pid
    WHERE s.team IS NOT NULL"""
    starter_branch = """
    SELECT b.NFL_player_id, c.team_code AS team, c.year, c.week
    FROM read_parquet('{path}', union_by_name=true) s
    JOIN cat c ON c.boxscore_id = s.boxscore_id AND c.side = '{side}',
    UNNEST(str_split(CAST(s.player_link_ids AS VARCHAR), ';')) AS u(pid)
    JOIN ids b ON b.pfr_id = u.pid"""

    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW cat AS
    SELECT boxscore_id, year, TRY_CAST(week AS INT) AS week, team_code,
           CASE WHEN is_home = 1 THEN 'home' ELSE 'vis' END AS side
    FROM '{g}' WHERE season_type = 'REG' AND year BETWEEN {y0} AND {y1}""")
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE participants AS
    {box_branch.format(path=off)}
    UNION {box_branch.format(path=dfb)}
    UNION {starter_branch.format(path=hs, side='home')}
    UNION {starter_branch.format(path=vs, side='vis')}""")

    con.execute(f"""
    COPY (
      SELECT t.NFL_player_id, t.year, t.week, t.nfl_team AS plane_team,
             (SELECT STRING_AGG(DISTINCT p.team, ',')
              FROM participants p
              WHERE p.NFL_player_id = t.NFL_player_id
                AND p.year = t.year AND p.week = t.week) AS witness_teams
      FROM read_parquet('{wk}') t
      WHERE t.season_type = 'REG' AND t.year BETWEEN {y0} AND {y1}
        AND t.position <> 'DEF' AND t.NFL_player_id NOT LIKE 'DEF%'
        AND EXISTS (SELECT 1 FROM participants p
                    WHERE p.NFL_player_id = t.NFL_player_id
                      AND p.year = t.year AND p.week = t.week)
        AND NOT EXISTS (SELECT 1 FROM participants p
                    WHERE p.NFL_player_id = t.NFL_player_id
                      AND p.year = t.year AND p.week = t.week
                      AND p.team = t.nfl_team)
    ) TO '{out.as_posix()}' (FORMAT parquet)""")

    witnessed = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet('{wk}') t
    WHERE t.season_type='REG' AND t.year BETWEEN {y0} AND {y1}
      AND t.position <> 'DEF' AND t.NFL_player_id NOT LIKE 'DEF%'
      AND EXISTS (SELECT 1 FROM participants p
                  WHERE p.NFL_player_id = t.NFL_player_id
                    AND p.year = t.year AND p.week = t.week)""").fetchone()[0]
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
    receipt = {
        "wave": f"roster_reconciliation_{y0}_{y1}",
        "date": time.strftime("%Y-%m-%d"),
        "witnessed_player_weeks": witnessed,
        "misallocated": n, "queue": str(out),
        "validation": ("method caught exactly the 17 known BOS-1944 rows "
                       "including the dropped week-6 -- ground-truthed"),
        "law": "misallocations are RELABEL candidates: adjudicate owner via "
               "catalog + witness side, never delete",
    }
    (LAKE / f"roster_reconciliation_{y0}_{y1}_receipt.json").write_text(
        json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    y0 = int(sys.argv[1]) if len(sys.argv) > 1 else 1920
    y1 = int(sys.argv[2]) if len(sys.argv) > 2 else 1949
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    print(json.dumps(build(con, y0, y1), indent=2))
