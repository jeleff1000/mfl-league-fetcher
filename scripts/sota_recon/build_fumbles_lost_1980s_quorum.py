"""fumbles_lost 1980s quorum repair: overwrite ONLY where two independent
lineages agree against the plane.

Measured 2026-08-03 on the 183 disputed REG cells (nflcom gamelog vs plane,
1980-89): pbp sides with nflcom on 100 (SUPERTABLE_VALUE, cross-lineage
quorum -> overwrite), with us on 64 (nflcom outvoted -> ours stands,
dissent logged), three-way split on 19 (queued, no ruling). Median gap -1:
the plane runs one fumble HIGH -- the double-credit signature.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S
from scripts.sota_recon.vouch_2024 import lane_sql

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "weekly_overlay_fumbles_lost_1980s.parquet"
RECEIPT = LAKE / "fumbles_lost_1980s_quorum_receipt.json"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    reg = S.registry()
    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type='REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")
    sp = next(s for s in W.WITNESS_MAP
              if s.source_key == 'nflcom_player_logs'
              and s.v26_col == 'fumbles_lost'
              and s.source_table == 'Regular Season')
    nfl, keys = lane_sql(sp)
    assert keys == ("pid", "yr", "wk")
    rp = Path(reg['pbp_player_week_rollup'].path)
    rpp = (rp.as_posix() + "/*.parquet") if rp.is_dir() else rp.as_posix()
    con.execute(f"""
    COPY (
    WITH n AS ({nfl}),
    b AS (SELECT NFL_player_id AS pid, TRY_CAST(year AS INT) AS yr,
                 TRY_CAST(week AS INT) AS wk,
                 TRY_CAST(fumbles_lost AS DOUBLE) AS val
          FROM read_parquet('{rpp}') WHERE season_type='REG')
    SELECT t.NFL_player_id, t.year, t.week,
           'fumbles_lost' AS column_name,
           TRY_CAST(t.fumbles_lost AS DOUBLE) AS old_value,
           n.val AS new_value,
           'fl1980s_quorum' AS repair_id,
           'gamebook+pbp' AS root,
           'two independent lineages agree against the plane (2026-08-03)'
             AS ruling
    FROM n
    JOIN plane t ON t.NFL_player_id=n.pid AND t.year=n.yr AND t.week=n.wk
    JOIN b ON b.pid=n.pid AND b.yr=n.yr AND b.wk=n.wk
    WHERE n.yr BETWEEN 1980 AND 1989 AND t.fumbles_lost IS NOT NULL
      AND ABS(n.val - TRY_CAST(t.fumbles_lost AS DOUBLE)) > 0.5
      AND ABS(n.val - b.val) < 0.5
    QUALIFY COUNT(*) OVER (PARTITION BY t.NFL_player_id, t.year, t.week) = 1
    ) TO '{OUT.as_posix()}' (FORMAT parquet)""")
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUT.as_posix()}')").fetchone()[0]
    assert 90 <= n <= 100, f"expected ~100 quorum cells, got {n} -- refuse"
    RECEIPT.write_text(json.dumps({
        "date": time.strftime("%Y-%m-%d %H:%M"), "cells": n,
        "rule": "overwrite only on cross-lineage quorum (gamebook+pbp)",
        "dissents_kept": 64, "three_way_queued": 19,
        "overlay": str(OUT)}, indent=1), encoding="utf-8")
    print(f"quorum overlay: {n} cells")


if __name__ == "__main__":
    main()
