"""IDP TACKLE/SACK BACKFILL CARS, 1978-1998 (+ sacks 1978-1981).

Joe's July-16 and Aug-03 rulings converge here: pbp carries individual
tackle credits to 1978 and the plane is empty 1978-1999. This emits overlay
CARS (never an in-place plane write) with the placement PROVEN by
measurement on the plane's own populated era (2026-08-03):
    solo     = solo_tackle_{1,2} + tackle_with_assist_{1,2}   <- the July
               builder omitted TWA; fidelity testing caught it
    assists  = assist_tackle_{1..4}
    combined = solo + assists
    sacks    = sack_player_id (1.0) + half_sack_{1,2} (0.5), 1978-1981 only
NULL-only backfill (single-root law: pbp is the licensed lineage root for
its own era). Crosswalk pfr->NFL_player_id via bio (measured 99.9%).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S
from scripts.sota_recon.witness_map import _q

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "weekly_overlay_idp_tackles.parquet"
RECEIPT = LAKE / "idp_tackle_cars_receipt.json"

SOLO = ["solo_tackle_1_player_id", "solo_tackle_2_player_id",
        "tackle_with_assist_1_player_id", "tackle_with_assist_2_player_id"]
AST = ["assist_tackle_1_player_id", "assist_tackle_2_player_id",
       "assist_tackle_3_player_id", "assist_tackle_4_player_id"]


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    pbp = _q("pbp_merged_1978_2025")

    def u(roles, lo, hi):
        return " UNION ALL ".join(
            f"""SELECT REPLACE(CAST(r.{c} AS VARCHAR), 'pfr:', '') AS pfr_id,
                       TRY_CAST(r.season AS INT) AS yr,
                       TRY_CAST(r.week AS INT) AS wk
                FROM '{pbp}' r WHERE r.season_type = 'REG'
                  AND r.{c} IS NOT NULL AND CAST(r.{c} AS VARCHAR) <> ''
                  AND TRY_CAST(r.season AS INT) BETWEEN {lo} AND {hi}"""
            for c in roles)

    con.execute(f"""CREATE OR REPLACE TEMP TABLE tk AS
    WITH s AS (SELECT pfr_id, yr, wk, COUNT(*) n
               FROM ({u(SOLO, 1978, 1998)}) GROUP BY 1, 2, 3),
    a AS (SELECT pfr_id, yr, wk, COUNT(*) n
          FROM ({u(AST, 1978, 1998)}) GROUP BY 1, 2, 3)
    SELECT COALESCE(s.pfr_id, a.pfr_id) AS pfr_id,
           COALESCE(s.yr, a.yr) AS yr, COALESCE(s.wk, a.wk) AS wk,
           COALESCE(s.n, 0) AS solo, COALESCE(a.n, 0) AS ast
    FROM s FULL OUTER JOIN a
      ON s.pfr_id = a.pfr_id AND s.yr = a.yr AND s.wk = a.wk""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sk AS
    WITH f AS (SELECT pfr_id, yr, wk, COUNT(*) * 1.0 AS n
               FROM ({u(['sack_player_id'], 1978, 1981)}) GROUP BY 1, 2, 3),
    h AS (SELECT pfr_id, yr, wk, COUNT(*) * 0.5 AS n
          FROM ({u(['half_sack_1_player_id', 'half_sack_2_player_id'],
                   1978, 1981)}) GROUP BY 1, 2, 3)
    SELECT COALESCE(f.pfr_id, h.pfr_id) AS pfr_id,
           COALESCE(f.yr, h.yr) AS yr, COALESCE(f.wk, h.wk) AS wk,
           COALESCE(f.n, 0) + COALESCE(h.n, 0) AS sacks
    FROM f FULL OUTER JOIN h
      ON f.pfr_id = h.pfr_id AND f.yr = h.yr AND f.wk = h.wk""")

    parts = []
    for tbl, col, val in (("tk", "def_tackles_solo", "solo"),
                          ("tk", "def_tackle_assists", "ast"),
                          ("tk", "def_tackles_combined", "solo + ast"),
                          ("sk", "def_sacks", "sacks")):
        parts.append(f"""
        SELECT t.NFL_player_id, t.year, t.week, '{col}' AS column_name,
               TRY_CAST(t.{col} AS DOUBLE) AS old_value,
               CAST(d.{val} AS DOUBLE) AS new_value,
               'idp_backfill_1978' AS repair_id, 'pbp' AS root,
               'pbp credit arrays; placement proven on populated era '
               || '(solo includes TWA); Joe rulings 07-16 + 08-03' AS ruling
        FROM {tbl} d
        JOIN (SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
              WHERE pfr_id IS NOT NULL) b USING (pfr_id)
        JOIN read_parquet('{wk}') t ON t.NFL_player_id = b.NFL_player_id
          AND CAST(t.year AS INT) = d.yr AND TRY_CAST(t.week AS INT) = d.wk
          AND t.season_type = 'REG'
        WHERE t.{col} IS NULL AND ({val}) > 0
        QUALIFY COUNT(*) OVER (
          PARTITION BY t.NFL_player_id, t.year, t.week) = 1""")
    con.execute(f"""COPY ({' UNION ALL '.join(parts)})
    TO '{OUT.as_posix()}' (FORMAT parquet)""")
    stats = dict(con.execute(f"""SELECT column_name, COUNT(*)
    FROM read_parquet('{OUT.as_posix()}') GROUP BY 1""").fetchall())
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"),
         "cells_by_column": {k: int(v) for k, v in stats.items()},
         "law": ("NULL-only, positive-only backfill from the pbp lineage "
                 "root in its own era; TWA-in-solo proven by measurement")},
        indent=1), encoding="utf-8")
    print(json.dumps({k: int(v) for k, v in stats.items()}, indent=1))


if __name__ == "__main__":
    main()
