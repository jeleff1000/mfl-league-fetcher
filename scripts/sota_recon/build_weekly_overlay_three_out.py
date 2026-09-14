"""WEEKLY MUTATION TRAIN: three_out re-derived to THE CANON (Joe's 08-02
ruling: 'pin to canon' -- a three-and-out is a drive of at most three
scrimmage plays ending in a punt). The stored column agreed only ~21%
cell-exact with drive-segmentation variants; the ruling dissolves the
ambiguity: one convention, re-derive both sides to it.

Derivation per team-week (credited to the DEFENSE that forced it):
  drives = pbp fixed_drive per game; qualifying drive = the OFFENSE ran <= 3
  scrimmage plays (pass/rush snaps; penalties that repeat a down ride along,
  spikes/kneels excluded per the 08-02 predicate ruling) AND the drive ends
  in a punt.

DOOM KEY: DEF-row cells where stored differs from the canon derivation --
the ruling is a DEFINITION MIGRATION, so both directions write; NULL cells
get positive values only.

ARMED, not fired -- runs when the disk frees.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_three_out.parquet"
RECEIPT = LAKE / "repair_three_out_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE canon AS
    WITH drives AS (
      SELECT r.game_id, r.fixed_drive, ANY_VALUE(r.defteam) AS defteam,
             ANY_VALUE(TRY_CAST(r.season AS INT)) AS yr,
             ANY_VALUE(TRY_CAST(r.week AS INT)) AS wkn,
             COUNT(*) FILTER (
               WHERE (TRY_CAST(r.pass_attempt AS INT) = 1
                      OR TRY_CAST(r.rush_attempt AS INT) = 1
                      OR TRY_CAST(r.sack AS INT) = 1)
                 AND COALESCE(TRY_CAST(r.qb_spike AS INT), 0) = 0
                 AND COALESCE(TRY_CAST(r.qb_kneel AS INT), 0) = 0
             ) AS scrimmage_plays,
             MAX(TRY_CAST(r.punt_attempt AS INT)) AS ended_in_punt
      FROM read_parquet('{pb}') r
      WHERE r.season_type = 'REG' AND r.fixed_drive IS NOT NULL
        AND r.defteam IS NOT NULL
      GROUP BY 1, 2)
    SELECT defteam AS team, yr, wkn,
           COUNT(*) FILTER (WHERE scrimmage_plays <= 3
                              AND ended_in_punt = 1)::DOUBLE AS v
    FROM drives GROUP BY 1, 2, 3""")
    con.execute(f"""
    COPY (
      SELECT t.NFL_player_id, t.year, t.week, 'three_out' AS column_name,
             TRY_CAST(t.three_out AS DOUBLE) AS old_value,
             c.v AS new_value, 'three_out_canon' AS repair_id,
             'pbp_merged_1978_2025' AS root,
             'CANON ruling (Joe 08-02): <=3 scrimmage plays ending in punt; '
             || 'spikes/kneels excluded per predicate ruling' AS ruling
      FROM read_parquet('{wk}') t
      JOIN canon c ON c.team = t.nfl_team AND c.yr = t.year AND c.wkn = t.week
      WHERE t.season_type = 'REG' AND t.position = 'DEF'
        AND ((t.three_out IS NULL AND c.v > 0)
             OR (t.three_out IS NOT NULL
                 AND TRY_CAST(t.three_out AS DOUBLE) <> c.v))
    ) TO '{OVERLAY.as_posix()}' (FORMAT parquet)""")
    n, mx, from_null = con.execute(f"""
    SELECT COUNT(*), MAX(new_value), COUNT(*) FILTER (WHERE old_value IS NULL)
    FROM read_parquet('{OVERLAY.as_posix()}')""").fetchone()
    assert (mx or 0) <= 12, f"sanity REFUSED: {mx} three-and-outs in one game"
    receipt = {"wave": "three_out_canon_migration", "date": "2026-08-02",
               "cells": n, "max": mx, "from_null": from_null,
               "overlay": str(OVERLAY)}
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
