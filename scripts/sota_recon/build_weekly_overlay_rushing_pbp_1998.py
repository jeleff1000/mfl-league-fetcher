"""Build the PBP-authoritative rushing/fumble repair overlay for 1998 onward.

The shipped weekly plane contains older non-null rushing first-down/fumble values.
This overlay replaces those cells only where the merged PBP has a player-attributed
event. Fumbles are partitioned into total, rushing, receiving, and sack classes;
either fumble slot may identify the player and ``rush_attempt=0`` no-play rows are
deliberately excluded.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.sota_recon import sources as S
from scripts.sota_recon.pbp_taxonomy import fumble_mentions_sql, official_play_sql

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_rushing_pbp_1998.parquet"
RECEIPT = LAKE / "rushing_pbp_1998_overlay_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.weekly_read_path()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE overlay AS
    WITH base AS (
      SELECT *
      FROM read_parquet('{pb}') p
      WHERE CAST(p.season AS INTEGER) >= 1998
        AND COALESCE(NULLIF(CAST(p.season_type AS VARCHAR), ''), 'REG') = 'REG'
        AND {official_play_sql('p')}
    ), coverage AS (
      SELECT rusher_player_id AS NFL_player_id, season, season_type FROM base WHERE rusher_player_id IS NOT NULL
      UNION SELECT receiver_player_id, season, season_type FROM base WHERE receiver_player_id IS NOT NULL
      UNION SELECT passer_player_id, season, season_type FROM base WHERE passer_player_id IS NOT NULL
      UNION SELECT fumbled_1_player_id, season, season_type FROM base WHERE fumble=1 AND fumbled_1_player_id IS NOT NULL
      UNION SELECT fumbled_2_player_id, season, season_type FROM base WHERE fumble=1 AND fumbled_2_player_id IS NOT NULL
    ), keys AS (
      SELECT rusher_player_id AS NFL_player_id, season, week, season_type
      FROM base WHERE rusher_player_id IS NOT NULL
      UNION
      SELECT receiver_player_id, season, week, season_type
      FROM base WHERE receiver_player_id IS NOT NULL
      UNION
      SELECT passer_player_id, season, week, season_type
      FROM base WHERE passer_player_id IS NOT NULL
      UNION
      SELECT fumbled_1_player_id, season, week, season_type
      FROM base WHERE fumble=1 AND fumbled_1_player_id IS NOT NULL
      UNION
      SELECT fumbled_2_player_id, season, week, season_type
      FROM base WHERE fumble=1 AND fumbled_2_player_id IS NOT NULL
    ), raw AS (
      SELECT
        regexp_replace(CAST(k.NFL_player_id AS VARCHAR), '^pfr:', '') AS NFL_player_id,
        CAST(k.season AS INTEGER) AS year, CAST(k.week AS INTEGER) AS week,
        COALESCE(NULLIF(CAST(k.season_type AS VARCHAR), ''), 'REG') AS season_type,
        SUM(CASE WHEN p.rusher_player_id=k.NFL_player_id
                      AND COALESCE(CAST(p.first_down_rush AS INTEGER), 0)=1
                 THEN 1 ELSE 0 END)::DOUBLE AS rushing_first_downs,
        SUM(CASE WHEN p.rusher_player_id=k.NFL_player_id
                      AND COALESCE(CAST(p.rush_attempt AS INTEGER), 0)=1
                      AND (p.fumbled_1_player_id=p.rusher_player_id
                           OR p.fumbled_2_player_id=p.rusher_player_id)
                 THEN 1 ELSE 0 END)::DOUBLE AS rushing_fumbles,
        SUM(CASE WHEN p.rusher_player_id=k.NFL_player_id
                      AND COALESCE(CAST(p.rush_attempt AS INTEGER), 0)=1
                      AND COALESCE(CAST(p.fumble_lost AS INTEGER), 0)=1
                      AND (p.fumbled_1_player_id=p.rusher_player_id
                           OR p.fumbled_2_player_id=p.rusher_player_id)
                 THEN 1 ELSE 0 END)::DOUBLE AS rushing_fumbles_lost
        ,SUM(CASE WHEN COALESCE(CAST(p.fumble AS INTEGER), 0)=1
                       AND (p.fumbled_1_player_id=k.NFL_player_id OR p.fumbled_2_player_id=k.NFL_player_id)
                  THEN CASE WHEN p.fumbled_1_player_id=k.NFL_player_id AND p.fumbled_2_player_id IS NULL
                            THEN {fumble_mentions_sql('p')} ELSE 1 END ELSE 0 END)::DOUBLE AS fumbles
        ,SUM(CASE WHEN COALESCE(CAST(p.fumble AS INTEGER), 0)=1
                       AND COALESCE(CAST(p.fumble_lost AS INTEGER), 0)=1
                       AND (p.fumbled_1_player_id=k.NFL_player_id OR p.fumbled_2_player_id=k.NFL_player_id)
                  THEN 1 ELSE 0 END)::DOUBLE AS fumbles_lost
        ,SUM(CASE WHEN COALESCE(CAST(p.complete_pass AS INTEGER), 0)=1
                       AND p.receiver_player_id=k.NFL_player_id
                       AND (p.fumbled_1_player_id=k.NFL_player_id OR p.fumbled_2_player_id=k.NFL_player_id)
                  THEN 1 ELSE 0 END)::DOUBLE AS receiving_fumbles
        ,SUM(CASE WHEN COALESCE(CAST(p.complete_pass AS INTEGER), 0)=1
                       AND COALESCE(CAST(p.fumble_lost AS INTEGER), 0)=1
                       AND p.receiver_player_id=k.NFL_player_id
                       AND (p.fumbled_1_player_id=k.NFL_player_id OR p.fumbled_2_player_id=k.NFL_player_id)
                  THEN 1 ELSE 0 END)::DOUBLE AS receiving_fumbles_lost
        ,SUM(CASE WHEN COALESCE(CAST(p.sack AS INTEGER), 0)=1
                       AND p.passer_player_id=k.NFL_player_id
                       AND (p.fumbled_1_player_id=k.NFL_player_id OR p.fumbled_2_player_id=k.NFL_player_id)
                  THEN 1 ELSE 0 END)::DOUBLE AS sack_fumbles
        ,SUM(CASE WHEN COALESCE(CAST(p.sack AS INTEGER), 0)=1
                       AND COALESCE(CAST(p.fumble_lost AS INTEGER), 0)=1
                       AND p.passer_player_id=k.NFL_player_id
                       AND (p.fumbled_1_player_id=k.NFL_player_id OR p.fumbled_2_player_id=k.NFL_player_id)
                  THEN 1 ELSE 0 END)::DOUBLE AS sack_fumbles_lost
      FROM keys k JOIN base p ON p.season=k.season AND p.week=k.week
        AND p.season_type=k.season_type
        AND (p.rusher_player_id=k.NFL_player_id OR p.receiver_player_id=k.NFL_player_id
             OR p.passer_player_id=k.NFL_player_id OR p.fumbled_1_player_id=k.NFL_player_id
             OR p.fumbled_2_player_id=k.NFL_player_id)
      GROUP BY 1,2,3,4
    )
    SELECT t.NFL_player_id, t.year, t.week, t.season_type,
           t.rushing_first_downs AS old_rushing_first_downs,
           t.rushing_fumbles AS old_rushing_fumbles,
           t.rushing_fumbles_lost AS old_rushing_fumbles_lost,
           t.fumbles AS old_fumbles, t.fumbles_lost AS old_fumbles_lost,
           t.receiving_fumbles AS old_receiving_fumbles,
           t.receiving_fumbles_lost AS old_receiving_fumbles_lost,
           t.sack_fumbles AS old_sack_fumbles,
           t.sack_fumbles_lost AS old_sack_fumbles_lost,
           r.rushing_first_downs, r.rushing_fumbles, r.rushing_fumbles_lost,
           r.fumbles, r.fumbles_lost, r.receiving_fumbles,
           r.receiving_fumbles_lost, r.sack_fumbles, r.sack_fumbles_lost,
           'rushing_pbp_1998_canon' AS repair_id,
           'PBP: valid rush attempt; fumble attribution accepts either fumble slot; no-play excluded' AS ruling
    FROM read_parquet('{wk}') t
    JOIN coverage c ON c.NFL_player_id=t.NFL_player_id AND c.season=t.year AND c.season_type=t.season_type
    LEFT JOIN raw r ON r.NFL_player_id=t.NFL_player_id AND r.year=t.year
       AND r.week=t.week AND r.season_type=t.season_type
    WHERE t.season_type='REG'
      AND (t.rushing_first_downs IS DISTINCT FROM COALESCE(r.rushing_first_downs, 0.0)
       OR t.rushing_fumbles IS DISTINCT FROM COALESCE(r.rushing_fumbles, 0.0)
       OR t.rushing_fumbles_lost IS DISTINCT FROM COALESCE(r.rushing_fumbles_lost, 0.0)
       OR t.fumbles IS DISTINCT FROM COALESCE(r.fumbles, 0.0)
       OR t.fumbles_lost IS DISTINCT FROM COALESCE(r.fumbles_lost, 0.0)
       OR t.receiving_fumbles IS DISTINCT FROM COALESCE(r.receiving_fumbles, 0.0)
       OR t.receiving_fumbles_lost IS DISTINCT FROM COALESCE(r.receiving_fumbles_lost, 0.0)
       OR t.sack_fumbles IS DISTINCT FROM COALESCE(r.sack_fumbles, 0.0)
       OR t.sack_fumbles_lost IS DISTINCT FROM COALESCE(r.sack_fumbles_lost, 0.0))
    """)
    con.execute(f"COPY (SELECT * FROM overlay) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OVERLAY.as_posix()}')").fetchone()[0]
    receipt = {"wave": "rushing_pbp_1998_canon", "cells": int(n), "overlay": str(OVERLAY),
               "columns": ["rushing_first_downs", "rushing_fumbles", "rushing_fumbles_lost", "fumbles", "fumbles_lost", "receiving_fumbles", "receiving_fumbles_lost", "sack_fumbles", "sack_fumbles_lost"],
               "rule": "PBP valid rush attempts; either fumble slot; no-play excluded"}
    RECEIPT.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2))
