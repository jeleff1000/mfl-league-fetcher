"""WEEKLY MUTATION TRAIN, car 2f (Joe's ruling: penalties go to the PLAYER
and to TEAM DEFENSE): the plane's player-attributed penalty columns are
sparse/absent while pbp carries penalty_player_id + penalty_yards per flag
from 1978. Two lanes:

  * player lane: penalties / penalty_yards credited to the flagged player;
  * team-defense lane: the DEF row aggregates its team's flags (the existing
    penalty_yards team lane covers value; this adds the player attribution).

DOOM KEY: positive credits onto NULL cells + corrections where stored differs
from the pbp derivation. Zeros are never written onto empty cells. Nullified
plays still carry their accepted penalty (a nullified PLAY is how a penalty
happens), so no nullification filter here -- declined/offset penalties have
penalty=0 in pbp and never enter.

RUN AFTER the current apply lands (disk discipline) -- the builder is
armed, not fired.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_2f.parquet"
RECEIPT = LAKE / "repair_2f_receipt.json"

LANES = [
    ("penalties", "1"),
    ("penalty_yards", "TRY_CAST(r.penalty_yards AS DOUBLE)"),
]


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    plane_cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{wk}') LIMIT 0").fetchall()}
    lanes = [(c, e) for c, e in LANES if c in plane_cols]
    parts = []
    for col, expr in lanes:
        parts.append(f"""
        SELECT t.NFL_player_id, t.year, t.week, '{col}' AS column_name,
               TRY_CAST(t.{col} AS DOUBLE) AS old_value,
               w.v AS new_value, 'repair_2f' AS repair_id,
               'pbp_merged_1978_2025' AS root,
               'penalties dual-lane ruling (Joe) + repair_signoff' AS ruling
        FROM read_parquet('{wk}') t
        JOIN (
          SELECT r.penalty_player_id AS pid, TRY_CAST(r.season AS INT) AS yr,
                 TRY_CAST(r.week AS INT) AS wkn, SUM({expr}) AS v
          FROM read_parquet('{pb}') r
          WHERE COALESCE(TRY_CAST(r.penalty AS INT), 0) = 1
            AND r.penalty_player_id IS NOT NULL AND r.season_type = 'REG'
          GROUP BY 1, 2, 3
        ) w ON w.pid = t.NFL_player_id AND w.yr = t.year AND w.wkn = t.week
        WHERE t.season_type = 'REG' AND t.position <> 'DEF'
          AND w.v > 0
          AND ((t.{col} IS NULL)
               OR TRY_CAST(t.{col} AS DOUBLE) <> w.v)""")
    union = " UNION ALL ".join(parts)
    con.execute(f"COPY ({union}) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    stats = con.execute(f"""
    SELECT column_name, COUNT(*), COUNT(*) FILTER (WHERE old_value IS NULL),
           MAX(new_value)
    FROM read_parquet('{OVERLAY.as_posix()}') GROUP BY 1""").fetchall()
    per = {r[0]: {"cells": r[1], "from_null": r[2], "max": r[3]}
           for r in stats}
    for c, v in per.items():
        cap = 6 if c == "penalties" else 200
        assert (v["max"] or 0) <= cap, f"sanity REFUSED: {c} max {v['max']}"
    receipt = {"wave": "repair_2f_penalties_dual_lane", "date": "2026-08-02",
               "lanes_present_on_plane": [c for c, _ in lanes],
               "per_column": per, "overlay": str(OVERLAY)}
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
