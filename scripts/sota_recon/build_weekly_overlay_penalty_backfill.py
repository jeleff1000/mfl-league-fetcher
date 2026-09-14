"""WEEKLY MUTATION TRAIN: the FIRST single-root-law backfill (Joe 2026-08-02:
'if penalty yards agree on 2023 we can use them on 1923').

The lane: pfr_box_team_stats penalties family. Its modern label
(Penalties-Yards) is licensed against consensus; its era label (Penalty Yds,
1920-1945) is the ONLY witness for a stratum where the plane holds nothing
(penalty_yards starts 1978). Under the single-root law, modern validation is
the ancient write licence: the era label backfills alone, provenance-stamped.

Team identity: home_stat/vis_stat re-keyed columns carry the two teams per
game; the boxscore joins the catalog for (team_code, year, week). Cells write
ONLY where the plane row exists and the column is NULL/absent-of-value --
this is a backfill, never an overwrite.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_penalty_backfill.parquet"
RECEIPT = LAKE / "backfill_penalty_1920_45_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    ts = Path(S.registry()["pfr_box_team_stats"].path).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()
    # the re-keyed long table: one row per stat label per game; home_stat and
    # vis_stat each carry one team's value; catalog supplies codes + week.
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW witness AS
    -- 1920s teams sometimes played TWICE in one catalog week. DISTINCT kills
    -- harvest double-listings; the HAVING keeps only team-weeks with ONE
    -- unambiguous value -- a two-game week cannot land in a one-cell week
    -- grain, so it queues instead of writing (4 such cells at first run).
    SELECT team, yr, wkn, ANY_VALUE(val) AS val FROM (
      SELECT DISTINCT g.boxscore_id, g.team_code AS team, g.year AS yr,
             TRY_CAST(g.week AS INT) AS wkn,
             TRY_CAST(CASE WHEN g.is_home = 1 THEN s.home_stat
                           ELSE s.vis_stat END AS DOUBLE) AS val
      FROM read_parquet('{ts}', union_by_name=true) s
      JOIN '{games}' g USING (boxscore_id)
      WHERE s.stat = 'Penalty Yds' AND g.season_type = 'REG'
        AND g.year BETWEEN 1920 AND 1945)
    GROUP BY 1, 2, 3 HAVING COUNT(DISTINCT val) = 1""")
    con.execute(f"""
    COPY (
      SELECT t.NFL_player_id, t.year, t.week,
             'penalty_yards' AS column_name,
             TRY_CAST(t.penalty_yards AS DOUBLE) AS old_value,
             w.val AS new_value,
             'backfill_penalty_1920_45' AS repair_id,
             'pfr_box_team_stats' AS root,
             'SINGLE-ROOT BACKFILL LAW (Joe 2026-08-02); modern sibling '
             || 'label licensed; era label sole witness' AS ruling
      FROM read_parquet('{wk}') t
      JOIN witness w ON w.team = t.nfl_team AND w.yr = t.year
                    AND w.wkn = t.week
      WHERE t.season_type = 'REG' AND t.position = 'DEF'
        AND t.penalty_yards IS NULL AND w.val IS NOT NULL
        -- ancient plane rows are PER-GAME: a two-game week means two rows
        -- sharing (id, year, week), and one weekly value cannot land on two
        -- game rows. Single-row weeks only; the rest queue.
        AND (t.NFL_player_id, t.year, t.week) IN (
          SELECT (NFL_player_id, year, week)
          FROM read_parquet('{wk}')
          WHERE season_type = 'REG' AND position = 'DEF'
            AND year BETWEEN 1920 AND 1945
          GROUP BY NFL_player_id, year, week HAVING COUNT(*) = 1)
    ) TO '{OVERLAY.as_posix()}' (FORMAT parquet)""")
    n, lo, hi, med = con.execute(f"""
    SELECT COUNT(*), MIN(new_value), MAX(new_value), MEDIAN(new_value)
    FROM read_parquet('{OVERLAY.as_posix()}')""").fetchone()
    receipt = {
        "wave": "backfill_penalty_yards_1920_45", "date": "2026-08-02",
        "law": "single-root backfill (first use)",
        "cells": n, "min": lo, "max": hi, "median": med,
        "overlay": str(OVERLAY),
        "abstention": "writes ONLY where the plane cell is NULL -- backfill, never overwrite",
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
