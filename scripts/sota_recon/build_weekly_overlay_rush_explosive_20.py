"""Build the 20+ explosive-rush migration overlay.

The physical column remains ``rush_explosive_10`` for compatibility with the
existing weekly schema; its locked semantic definition is now 20+ yards.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_rush_explosive_20.parquet"
RECEIPT = LAKE / "rush_explosive_20_overlay_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.weekly_read_path()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    bio = Path(r"D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet").as_posix()
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE canon AS
    WITH raw AS (
      SELECT COALESCE(xw.NFL_player_id,
                      regexp_replace(CAST(p.rusher_player_id AS VARCHAR), '^pfr:', '')) AS NFL_player_id,
             CAST(p.season AS INTEGER) AS year, CAST(p.week AS INTEGER) AS week,
             COALESCE(NULLIF(CAST(p.season_type AS VARCHAR), ''), 'REG') AS season_type,
             SUM(CASE WHEN COALESCE(CAST(p.rush_attempt AS INTEGER),0)=1
                           AND COALESCE(CAST(p.two_point_attempt AS INTEGER),0)=0
                           AND TRY_CAST(p.rushing_yards AS DOUBLE) >= 20
                      THEN 1 ELSE 0 END)::DOUBLE AS new_value
      FROM read_parquet('{pb}') p
      LEFT JOIN (SELECT DISTINCT CAST(pfr_id AS VARCHAR) AS pfr_id,
                        CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
                 FROM read_parquet('{bio}') WHERE pfr_id IS NOT NULL) xw
        ON regexp_replace(CAST(p.rusher_player_id AS VARCHAR), '^pfr:', '') = xw.pfr_id
      WHERE p.rusher_player_id IS NOT NULL
        AND COALESCE(CAST(p.rush_attempt AS INTEGER),0)=1
        AND COALESCE(CAST(p.two_point_attempt AS INTEGER),0)=0
      GROUP BY 1,2,3,4
    )
    SELECT t.NFL_player_id, t.year, t.week,
           'rush_explosive_10' AS column_name,
           TRY_CAST(t.rush_explosive_10 AS DOUBLE) AS old_value,
           r.new_value, 'rush_explosive_20_canon' AS repair_id,
           'pbp_merged_1978_2025' AS root,
           'CANON: explosive rush = 20+ rushing yards; matches NFL.com 20 bucket' AS ruling
    FROM read_parquet('{wk}') t JOIN raw r USING (NFL_player_id, year, week)
    WHERE t.rush_explosive_10 IS DISTINCT FROM r.new_value
      AND t.season_type = r.season_type
    """)
    con.execute(f"COPY (SELECT * FROM canon) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    row = con.execute(f"""SELECT COUNT(*), COUNT(*) FILTER (WHERE old_value IS NULL),
                                  MIN(new_value), MAX(new_value)
                           FROM read_parquet('{OVERLAY.as_posix()}')""").fetchone()
    receipt = {"wave":"rush_explosive_20_canon", "physical_column":"rush_explosive_10",
               "locked_definition":"20+ rushing yards", "cells":row[0], "from_null":row[1],
               "min":row[2], "max":row[3], "overlay":str(OVERLAY)}
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str), encoding='utf-8')
    return receipt


if __name__ == '__main__':
    c = duckdb.connect(); c.execute("SET memory_limit='4GB'")
    print(json.dumps(build(c), indent=2, default=str))
