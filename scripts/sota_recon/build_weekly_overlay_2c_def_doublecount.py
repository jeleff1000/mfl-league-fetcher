"""WEEKLY MUTATION TRAIN, car 2c: the team-DEF rows double-count the passing
*_allowed family (DST row + player rows summed at build; completions measured
2x while attempts matched -- team_stats_rekey receipts). The pbp team lanes
are the licensed repair source (supertable fault keeps the witness licence).

DOOM KEY: team-DEF-row cell whose stored value sits in [1.8x, 2.2x] of the
pbp team-week value -- the doubling signature and nothing else. Cells that
already match pbp (within 1) stay; cells that disagree OUTSIDE the doubling
band go to arbitration, never written.

Overlay file: weekly_overlay_2c.parquet (one file per car; the promote globs
weekly*overlay*.parquet).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_2c.parquet"
RECEIPT = LAKE / "repair_2c_receipt.json"

#: convicted column -> (pbp per-play value expr, play predicate)
LANES = {
    "def_targets_allowed": ("1", "r.pass_attempt=1"),
    "def_completions_allowed": ("1", "r.complete_pass=1"),
    "def_completion_yards_allowed": (
        "TRY_CAST(r.yards_gained AS DOUBLE)", "r.complete_pass=1"),
    "def_air_yards_allowed": (
        "TRY_CAST(r.air_yards AS DOUBLE)", "r.pass_attempt=1"),
    "def_yards_after_catch_allowed": (
        "TRY_CAST(r.yards_after_catch AS DOUBLE)", "r.complete_pass=1"),
}


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    parts = []
    for col, (expr, pred) in LANES.items():
        parts.append(f"""
        SELECT t.NFL_player_id, t.year, t.week, '{col}' AS column_name,
               TRY_CAST(t.{col} AS DOUBLE) AS old_value,
               w.v AS new_value, 'repair_2c' AS repair_id,
               'pbp_merged_1978_2025' AS root,
               'team_stats_rekey double-count conviction + repair_signoff'
                   AS ruling
        FROM read_parquet('{wk}') t
        JOIN (
          SELECT r.defteam AS team, TRY_CAST(r.season AS INT) AS yr,
                 TRY_CAST(r.week AS INT) AS wkn, SUM({expr}) AS v
          FROM read_parquet('{pb}') r
          WHERE {pred} AND r.season_type = 'REG' AND r.defteam IS NOT NULL
          GROUP BY 1, 2, 3
        ) w ON w.team = t.nfl_team AND w.yr = t.year AND w.wkn = t.week
        WHERE t.season_type = 'REG' AND t.position = 'DEF'
          AND TRY_CAST(t.{col} AS DOUBLE) BETWEEN 1.8 * w.v AND 2.2 * w.v
          AND w.v > 0""")
    union = " UNION ALL ".join(parts)
    con.execute(f"COPY ({union}) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    stats = con.execute(f"""
    SELECT column_name, COUNT(*), ROUND(MEDIAN(old_value / new_value), 3)
    FROM read_parquet('{OVERLAY.as_posix()}') GROUP BY 1""").fetchall()
    per = {r[0]: {"cells": r[1], "median_stored_over_pbp": float(r[2])}
           for r in stats}
    receipt = {
        "wave": "repair_2c_def_doublecount", "date": "2026-08-02",
        "doom_key": "team-DEF row stored in [1.8x, 2.2x] of pbp team-week value",
        "overlay": str(OVERLAY), "per_column": per,
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
