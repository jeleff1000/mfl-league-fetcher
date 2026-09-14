"""WEEKLY MUTATION TRAIN, car 2b: the 2025 wk14+ kicking distance atoms are
STALE (scoring_log_wave receipts: fg_made_distance did not get the late-season
re-derive; over_30 did). The licensed scoring log re-derives them.

Extraction: the boxscore Scoring log's '<N> yard field goal' sentences, scorer
= first link id, week via the catalog. DOOM KEY: 2025 REG week >= 14 kicker
cells where the scoring-log weekly sum differs from stored -- both directions
written (stale means stale, not merely low), old value kept for reversal.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_2b.parquet"
RECEIPT = LAKE / "repair_2b_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    sc = Path(S.registry()["pfr_box_scoring"].path).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW fg AS
    -- DISTINCT: the scoring log appears once per team perspective (and harvest
    -- shards can repeat) -- without this the sums double and yards-per-make
    -- reads 76, which the sanity gate caught before any write shipped.
    SELECT DISTINCT s.boxscore_id, s.description,
           regexp_extract(CAST(s.description_link_ids AS VARCHAR),
                          '^([^;,]+)', 1) AS pfr_id,
           TRY_CAST(g.week AS INT) AS wkn,
           TRY_CAST(regexp_extract(s.description,
                    '(\\d+) yard field goal', 1) AS DOUBLE) AS dist
    FROM read_parquet('{sc}', union_by_name=true) s
    JOIN '{games}' g USING (boxscore_id)
    WHERE g.year = 2025 AND g.season_type = 'REG'
      AND TRY_CAST(g.week AS INT) >= 14
      AND s.description LIKE '%yard field goal%'
      AND s.description NOT LIKE '%no good%'
      AND s.description_link_ids IS NOT NULL""")
    con.execute(f"""
    COPY (
      SELECT t.NFL_player_id, t.year, t.week,
             'fg_made_distance' AS column_name,
             TRY_CAST(t.fg_made_distance AS DOUBLE) AS old_value,
             w.v AS new_value, 'repair_2b' AS repair_id,
             'pfr_box_scoring' AS root,
             'scoring_log_wave stale wk14+ conviction + repair_signoff'
                 AS ruling
      FROM read_parquet('{wk}') t
      JOIN read_parquet('{bio}') b USING (NFL_player_id)
      JOIN (
        SELECT pfr_id, wkn, SUM(dist) AS v FROM fg
        WHERE dist IS NOT NULL GROUP BY 1, 2
      ) w ON w.pfr_id = b.pfr_id AND w.wkn = t.week
      WHERE t.year = 2025 AND t.season_type = 'REG' AND t.week >= 14
        AND COALESCE(TRY_CAST(t.fg_made_distance AS DOUBLE), -1) <> w.v
    ) TO '{OVERLAY.as_posix()}' (FORMAT parquet)""")
    # sanity gate: every new value must sit in [17, 66] yards per made FG
    bad = con.execute(f'''
    SELECT COUNT(*) FROM read_parquet('{OVERLAY.as_posix()}') o
    JOIN read_parquet('{wk}') t USING (NFL_player_id, year, week)
    WHERE t.season_type = 'REG'
      AND NOT (o.new_value BETWEEN 17 * TRY_CAST(t.fg_made AS DOUBLE)
                               AND 66 * TRY_CAST(t.fg_made AS DOUBLE))''').fetchone()[0]
    assert bad == 0, f"sanity gate REFUSED: {{bad}} cells outside yards-per-make bounds"
    n, changed_from_null, med_old, med_new = con.execute(f"""
    SELECT COUNT(*), COUNT(*) FILTER (WHERE old_value IS NULL),
           MEDIAN(old_value), MEDIAN(new_value)
    FROM read_parquet('{OVERLAY.as_posix()}')""").fetchone()
    receipt = {
        "wave": "repair_2b_kicking_2025_wk14plus", "date": "2026-08-02",
        "cells": n, "from_null": changed_from_null,
        "median_old": med_old, "median_new": med_new,
        "overlay": str(OVERLAY),
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
