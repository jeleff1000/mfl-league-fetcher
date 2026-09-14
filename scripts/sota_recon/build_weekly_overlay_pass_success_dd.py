"""Build the canonical down/distance success-count overlay for pass plays.

The stored ``pass_success`` atom is a COUNT of successful pass-play rows.  Its
rate is the separately derived ``pass_success / pass_success_plays`` value.
Success is model-free: 40% of yards-to-go on first down, 60% on second, and
100% on third/fourth.  This overlay migrates the weekly count to that contract
for every year with usable PBP, including rows that previously held EPA-sign
success counts.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_pass_success_down_distance.parquet"
RECEIPT = LAKE / "pass_success_down_distance_overlay_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.weekly_read_path()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    bio = Path(r"D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet").as_posix()
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE canon AS
    WITH raw AS (
      SELECT
        COALESCE(xw.NFL_player_id, regexp_replace(CAST(p.passer_player_id AS VARCHAR), '^pfr:', '')) AS NFL_player_id,
        CAST(p.season AS INTEGER) AS year, CAST(p.week AS INTEGER) AS week,
        COALESCE(NULLIF(CAST(p.season_type AS VARCHAR), ''), 'REG') AS season_type,
        SUM(CASE WHEN p.down IS NOT NULL AND p.ydstogo IS NOT NULL
                      AND p.yards_gained IS NOT NULL AND (
                    (CAST(p.down AS INTEGER) = 1 AND CAST(p.yards_gained AS DOUBLE) >= 0.4 * CAST(p.ydstogo AS DOUBLE))
                 OR (CAST(p.down AS INTEGER) = 2 AND CAST(p.yards_gained AS DOUBLE) >= 0.6 * CAST(p.ydstogo AS DOUBLE))
                 OR (CAST(p.down AS INTEGER) IN (3,4) AND CAST(p.yards_gained AS DOUBLE) >= CAST(p.ydstogo AS DOUBLE))
                      ) THEN 1 ELSE 0 END)::DOUBLE AS new_value
      FROM read_parquet('{pb}') p
      LEFT JOIN (SELECT DISTINCT CAST(pfr_id AS VARCHAR) AS pfr_id,
                        CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
                 FROM read_parquet('{bio}') WHERE pfr_id IS NOT NULL) xw
        ON regexp_replace(CAST(p.passer_player_id AS VARCHAR), '^pfr:', '') = xw.pfr_id
      WHERE p.passer_player_id IS NOT NULL
        AND COALESCE(CAST(p.two_point_attempt AS INTEGER), 0) = 0
        AND (COALESCE(CAST(p.pass_attempt AS INTEGER),0) = 1
             OR COALESCE(CAST(p.complete_pass AS INTEGER),0) = 1
             OR COALESCE(CAST(p.sack AS INTEGER),0) = 1
             OR COALESCE(CAST(p.interception AS INTEGER),0) = 1
             OR COALESCE(CAST(p.passing_yards AS DOUBLE),0) <> 0)
      GROUP BY 1,2,3,4
    )
    SELECT t.NFL_player_id, t.year, t.week, 'pass_success' AS column_name,
           TRY_CAST(t.pass_success AS DOUBLE) AS old_value,
           r.new_value, 'pass_success_down_distance_canon' AS repair_id,
           'pbp_merged_1978_2025' AS root,
           'CANON: successful pass play = 40/60/100 percent of yards-to-go by down; count atom' AS ruling
    FROM read_parquet('{wk}') t
    JOIN raw r USING (NFL_player_id, year, week)
    WHERE t.pass_success IS DISTINCT FROM r.new_value
      AND t.season_type = r.season_type
    """)
    con.execute(f"COPY (SELECT * FROM canon) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    cells, changed, old_null, lo, hi = con.execute(f"""
      SELECT COUNT(*), COUNT(*) FILTER (WHERE old_value IS DISTINCT FROM new_value),
             COUNT(*) FILTER (WHERE old_value IS NULL), MIN(new_value), MAX(new_value)
      FROM read_parquet('{OVERLAY.as_posix()}')""").fetchone()
    receipt = {
        "wave": "pass_success_down_distance_canon",
        "definition": "count successful pass-play rows; 40% first, 60% second, 100% third/fourth",
        "cells": cells, "changed": changed, "from_null": old_null,
        "new_min": lo, "new_max": hi, "overlay": str(OVERLAY),
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    c = duckdb.connect()
    c.execute("SET memory_limit='4GB'")
    print(json.dumps(build(c), indent=2, default=str))
