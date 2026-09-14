"""WEEKLY MUTATION TRAIN, car 2j (released 2026-08-02): weekly kickoff/punt
return longs are ZERO-FILLED on the very weeks of long return TDs (Hobbs 108
stored as 0.0). Adjudicated SUPERTABLE_VALUE; the pbp witness keeps the
licence and now supplies the repair.

Architecture: repairs write to the OVERLAY LEDGER (weekly_repairs_overlay
parquet in the lake) -- one row per cell edit with old/new/provenance. The
promote applies every overlay in one rebuild. No plane file is touched here.

DOOM KEY (deliberately narrow): stored IS NULL or 0 while pbp holds a
PENALTY-FREE return that week. Cells where stored > 0 but differs from pbp
are NOT repaired -- penalty-nullified returns legitimately stay below the
raw pbp max, so that class goes to arbitration, not to a write.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_repairs_overlay.parquet"
RECEIPT = LAKE / "repair_2j_receipt.json"

LANES = [
    ("kickoff_return_long", "kickoff_returner_player_id", "kickoff_attempt"),
    ("punt_return_long", "punt_returner_player_id", "punt_attempt"),
]


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    parts = []
    for col, retcol, attempt in LANES:
        parts.append(f"""
        SELECT t.NFL_player_id, t.year, t.week, '{col}' AS column_name,
               TRY_CAST(t.{col} AS DOUBLE) AS old_value,
               w.pbp_long AS new_value,
               'repair_2j' AS repair_id,
               'pbp_merged_1978_2025' AS root,
               'SUPERTABLE_VALUE adjudication 2026-08-01 + repair_signoff' AS ruling
        FROM read_parquet('{wk}') t
        JOIN (
          SELECT r.{retcol} AS pid, TRY_CAST(r.season AS INT) AS yr,
                 TRY_CAST(r.week AS INT) AS wkn,
                 MAX(TRY_CAST(r.return_yards AS DOUBLE)) AS pbp_long
          FROM read_parquet('{pb}') r
          WHERE r.{attempt} = 1 AND r.return_yards IS NOT NULL
            AND COALESCE(TRY_CAST(r.penalty AS INT), 0) = 0
            AND r.season_type = 'REG'
          GROUP BY 1, 2, 3
        ) w ON w.pid = t.NFL_player_id AND w.yr = t.year AND w.wkn = t.week
        WHERE t.season_type = 'REG'
          AND COALESCE(TRY_CAST(t.{col} AS DOUBLE), 0) = 0
          AND w.pbp_long > 0""")
    union = " UNION ALL ".join(parts)
    con.execute(f"""
    COPY ({union}) TO '{OVERLAY.as_posix()}' (FORMAT parquet)""")
    stats = con.execute(f"""
    SELECT column_name, COUNT(*), MIN(new_value), MAX(new_value),
           COUNT(*) FILTER (WHERE new_value > 100)
    FROM read_parquet('{OVERLAY.as_posix()}') GROUP BY 1""").fetchall()
    per = {r[0]: {"cells": r[1], "min": r[2], "max": r[3],
                  "over_100": r[4]} for r in stats}
    receipt = {
        "wave": "repair_2j_weekly_return_longs", "date": "2026-08-02",
        "architecture": "overlay ledger (first car of the weekly mutation train)",
        "doom_key": ("stored NULL/0 AND penalty-free pbp return > 0 that week; "
                     "stored>0 disagreements go to arbitration, never written"),
        "overlay": str(OVERLAY), "per_column": per,
        "gate": "every overlay row carries old_value for exact reversal",
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
