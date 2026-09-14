"""PBP TACKLE FIDELITY (Joe's era-1978 ruling: "pbp has them to 78").

Re-derives the tackle family from the raw pbp tackle-credit arrays per
player-week 1978-2000 and measures exact-match against the plane:
    solo     = count of solo_tackle_{1,2} credits + tackle_with_assist_{1,2}
    assists  = count of assist_tackle_{1..4} credits
    combined = solo + assists
If the plane reproduces its own lineage at >=99.9%, the family closes
1978+ by PBP-LINEAGE FIDELITY (the plane IS the pbp derivation, verified),
with 2001+ additionally official-witnessed and pre-1978 estimate-era per
the ruling. Writes a receipt; closure consumes it via FIDELITY_CLOSURES.
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
RECEIPT = LAKE / "pbp_tackle_fidelity_receipt.json"

SOLO_ROLES = ["solo_tackle_1_player_id", "solo_tackle_2_player_id",
              "tackle_with_assist_1_player_id",
              "tackle_with_assist_2_player_id"]
AST_ROLES = ["assist_tackle_1_player_id", "assist_tackle_2_player_id",
             "assist_tackle_3_player_id", "assist_tackle_4_player_id"]


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    pbp = _q("pbp_merged_1978_2025")

    # ID NAMESPACE (bug caught 2026-08-03): 1978-98 pbp ids are 'pfr:XxxxYy00',
    # 1999+ are gsis. The first run joined raw ids straight to NFL_player_id
    # and matched only year 2000 -- manufacturing a phantom "empty era".
    # Crosswalk BOTH namespaces through bio.
    def union(roles):
        return " UNION ALL ".join(
            f"""SELECT CAST(r.{role} AS VARCHAR) AS rawid,
                       TRY_CAST(r.season AS INT) AS yr,
                       TRY_CAST(r.week AS INT) AS wk
                FROM '{pbp}' r
                WHERE r.season_type = 'REG' AND r.{role} IS NOT NULL
                  AND TRY_CAST(r.season AS INT) BETWEEN 1978 AND 2000"""
            for role in roles)

    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP TABLE xw AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    def resolved(u_sql: str) -> str:
        return f"""
        SELECT COALESCE(x.NFL_player_id, u.rawid) AS pid, u.yr, u.wk
        FROM ({u_sql}) u
        LEFT JOIN xw x ON x.pfr_id = REPLACE(u.rawid, 'pfr:', '')"""

    con.execute(f"""CREATE OR REPLACE TEMP TABLE derived AS
    WITH s AS (SELECT pid, yr, wk, COUNT(*) AS solo
               FROM ({resolved(union(SOLO_ROLES))}) GROUP BY 1, 2, 3),
    a AS (SELECT pid, yr, wk, COUNT(*) AS ast
          FROM ({resolved(union(AST_ROLES))}) GROUP BY 1, 2, 3)
    SELECT COALESCE(s.pid, a.pid) AS pid, COALESCE(s.yr, a.yr) AS yr,
           COALESCE(s.wk, a.wk) AS wk,
           COALESCE(s.solo, 0) AS solo, COALESCE(a.ast, 0) AS ast
    FROM s FULL OUTER JOIN a
      ON s.pid = a.pid AND s.yr = a.yr AND s.wk = a.wk""")

    out = {}
    for col, expr in (("def_tackles_solo", "d.solo"),
                      ("def_tackle_assists", "d.ast"),
                      ("def_tackles_combined", "d.solo + d.ast")):
        n, ok = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (
          WHERE ABS(TRY_CAST(t.{col} AS DOUBLE) - ({expr})) < 0.5)
        FROM derived d
        JOIN read_parquet('{wk}') t ON t.NFL_player_id = d.pid
          AND CAST(t.year AS INT) = d.yr AND TRY_CAST(t.week AS INT) = d.wk
          AND t.season_type = 'REG'
        WHERE t.{col} IS NOT NULL""").fetchone()
        out[col] = {"n": n, "exact": ok,
                    "fidelity": round(ok / n, 5) if n else None}
        print(f"{col}: n={n}, fidelity={ok/n:.5f}" if n else (col, "n=0"),
              flush=True)

    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"), "era": "1978-2000",
         "derivation": {"solo": SOLO_ROLES, "assists": AST_ROLES,
                        "combined": "solo + assists"},
         "results": out,
         "ruling": ("Joe 2026-08-03: pbp has tackles to 1978; family is "
                    "all-phases; 1978+ closes by pbp-lineage fidelity, "
                    "pre-1978 estimate-era")}, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
