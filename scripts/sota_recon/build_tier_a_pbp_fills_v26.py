"""
sota_recon/build_tier_a_pbp_fills_v26.py -- era-rescue Tier A: extend three PBP-derived atoms 1999->1978.

Witness bases measured 2026-07-16 in pbp_merged (stathead 1978-98):
  * rushing_scrambles  : qb_scramble flag, 12,291 stathead plays (vs 22,675 modern) -- rusher-attributed.
  * penalties(+yards)  : penalty flag 69,851 / player-attributed 55,871 (80.0%; modern 95.2%).
                         NAMED-ONLY policy: unattributed penalties (team/unidentified) are NOT
                         zero-defaulted onto players; a player-week with no named penalty stays NULL
                         in 1978-98 (the 0-default would assert innocence the witness can't support).
  * fg_blocked         : field-goal plays with 'block' in desc, 528 stathead (~25/yr, plausible).
NOT derivable (documented bounds, do not chase in PBP): pat_blocked pre-1999 (1 text hit in 21 seasons),
def_tackles_for_loss_yards (multi-tackler credit semantics = open ruling), gwfg (scoring-log lane),
*_success family (separate basis recompute).

GATES per fill: control-era parity 1999-2002 (same derivation vs stored values; >=90% of shared nonzero
cells exact -- stored may come from a slightly different nflverse lane), fpts invariance (none of the
three feeds scoring; fg_blocked is a subset of fg_missed which is already scored), row count unchanged.
Fills write ONLY where the target cell is NULL in 1978-1998 -- never overwrites.

    python -m scripts.sota_recon.build_tier_a_pbp_fills_v26            # dry-run + gate report
    python -m scripts.sota_recon.build_tier_a_pbp_fills_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import safe_replace, utc_stamp
from .sources import latest_v26

PBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/**/*.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave63.tier_a_pbp_fills"
LO, HI = 1978, 1998
CTRL_LO, CTRL_HI = 1999, 2002

XW = (f"LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}') "
      f"WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b ON b.pfr_id = r.pid")


def _derive(con, lo: int, hi: int) -> None:
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scr AS
        SELECT COALESCE(b.NFL_player_id, r.pid) nfl_id, r.yr, r.wk, SUM(r.n) v FROM (
          SELECT CAST(season AS INT) yr, CAST(week AS INT) wk,
                 REPLACE(CAST(rusher_player_id AS VARCHAR),'pfr:','') pid, COUNT(*) n
          FROM read_parquet('{PBP}', union_by_name=true)
          WHERE qb_scramble=1 AND season BETWEEN {lo} AND {hi}
            AND rusher_player_id IS NOT NULL AND CAST(rusher_player_id AS VARCHAR)<>''
          GROUP BY 1,2,3) r {XW} GROUP BY 1,2,3""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pen AS
        SELECT COALESCE(b.NFL_player_id, r.pid) nfl_id, r.yr, r.wk, SUM(r.n) v, SUM(r.y) vy FROM (
          SELECT CAST(season AS INT) yr, CAST(week AS INT) wk,
                 REPLACE(CAST(penalty_player_id AS VARCHAR),'pfr:','') pid,
                 COUNT(*) n, SUM(COALESCE(TRY_CAST(penalty_yards AS DOUBLE),0)) y
          FROM read_parquet('{PBP}', union_by_name=true)
          WHERE penalty=1 AND season BETWEEN {lo} AND {hi}
            AND penalty_player_id IS NOT NULL AND CAST(penalty_player_id AS VARCHAR)<>''
          GROUP BY 1,2,3) r {XW} GROUP BY 1,2,3""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fgb AS
        SELECT COALESCE(b.NFL_player_id, r.pid) nfl_id, r.yr, r.wk, SUM(r.n) v FROM (
          SELECT CAST(season AS INT) yr, CAST(week AS INT) wk,
                 REPLACE(CAST(kicker_player_id AS VARCHAR),'pfr:','') pid, COUNT(*) n
          FROM read_parquet('{PBP}', union_by_name=true)
          WHERE field_goal_attempt=1 AND lower("desc") LIKE '%block%' AND season BETWEEN {lo} AND {hi}
            AND kicker_player_id IS NOT NULL AND CAST(kicker_player_id AS VARCHAR)<>''
          GROUP BY 1,2,3) r {XW} GROUP BY 1,2,3""")


def _parity(con, src: str, tmp_tbl: str, col: str, clo: int = CTRL_LO, chi: int = CTRL_HI) -> dict:
    r = con.execute(f"""SELECT
          COUNT(*) FILTER (WHERE s.sv IS NOT NULL AND d.v IS NOT NULL) shared,
          COUNT(*) FILTER (WHERE s.sv = d.v) agree
        FROM {tmp_tbl} d JOIN (
          SELECT CAST(NFL_player_id AS VARCHAR) nfl_id, CAST(year AS INT) yr, CAST(week AS INT) wk,
                 TRY_CAST({col} AS INT) sv
          FROM read_parquet('{src}')
          WHERE CAST(year AS INT) BETWEEN {clo} AND {chi} AND position <> 'DEF'
            AND TRY_CAST({col} AS DOUBLE) > 0) s
          ON s.nfl_id = CAST(d.nfl_id AS VARCHAR) AND s.yr = d.yr AND s.wk = d.wk""").fetchone()
    return {"shared": r[0], "agree": r[1], "pct": round(100 * r[1] / r[0], 2) if r[0] else None}


def run(apply: bool = False) -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='1500MB'")
    con.execute(f"SET temp_directory='{sp}'")
    src = Path(v26).as_posix()
    res: dict = {}

    # control-era derivation for parity gates. Scrambles' stored column only exists 2018+ (that IS the
    # floor being rescued) -> its control window is 2018-2021; penalties/fg_blocked use 1999-2002.
    _derive(con, CTRL_LO, CTRL_HI)
    res["parity_penalties"] = _parity(con, src, "pen", "penalties")
    res["parity_fg_blocked"] = _parity(con, src, "fgb", "fg_blocked")
    _derive(con, 2018, 2021)
    res["parity_scrambles"] = _parity(con, src, "scr", "rushing_scrambles", 2018, 2021)
    gates = {k: (v["shared"] or 0) > 50 and (v["agree"] / v["shared"]) >= 0.90
             for k, v in [("scrambles", res["parity_scrambles"]), ("penalties", res["parity_penalties"]),
                          ("fg_blocked", res["parity_fg_blocked"])]}
    res["parity_gates"] = gates

    # fill-era derivation
    _derive(con, LO, HI)
    res["fill_events"] = {
        "scrambles": int(con.execute("SELECT COALESCE(SUM(v),0) FROM scr").fetchone()[0]),
        "penalties": int(con.execute("SELECT COALESCE(SUM(v),0) FROM pen").fetchone()[0]),
        "fg_blocked": int(con.execute("SELECT COALESCE(SUM(v),0) FROM fgb").fetchone()[0])}

    if not apply:
        shutil.rmtree(sp, ignore_errors=True)
        return res

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{src}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_before = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)),2) FROM st").fetchone()[0]
    prov = ("recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
            f"THEN '{PROV}' ELSE recon_correction_log || ',{PROV}' END")
    for tbl, sets in (("scr", "rushing_scrambles = d.v"),
                      ("pen", "penalties = d.v, penalty_yards = d.vy"),
                      ("fgb", "fg_blocked = d.v")):
        tgt = sets.split(" = ")[0]
        con.execute(f"""UPDATE st SET {sets}, {prov}
            FROM {tbl} d WHERE st.position <> 'DEF' AND st.{tgt} IS NULL
              AND CAST(st.year AS INT) BETWEEN {LO} AND {HI}
              AND CAST(st.NFL_player_id AS VARCHAR) = CAST(d.nfl_id AS VARCHAR)
              AND CAST(st.year AS INT) = d.yr AND CAST(st.week AS INT) = d.wk""")
    after_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_after = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)),2) FROM st").fetchone()[0]
    filled = {c: con.execute(f"""SELECT COUNT(*) FROM st WHERE {c} IS NOT NULL
        AND CAST(year AS INT) BETWEEN {LO} AND {HI}""").fetchone()[0]
        for c in ("rushing_scrambles", "penalties", "fg_blocked")}
    res["filled_nonnull_1978_98"] = filled
    gate = all(gates.values()) and after_rows == before_rows and fpts_before == fpts_after
    res.update({"rows": f"{before_rows}->{after_rows}", "fpts_invariant": fpts_before == fpts_after,
                "gate_pass": bool(gate)})
    if gate:
        vp = Path(v26); tmp = vp.with_name(vp.stem + "_tiera.parquet")
        r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
        w = pq.ParquetWriter(str(tmp), r.schema)
        for b in r:
            w.write_batch(b)
        w.close()
        bk = vp.with_name(vp.stem + f"_pretiera_{stamp}.parquet")
        shutil.copy2(vp, bk); safe_replace(tmp, vp)
        res["backup"] = bk.name; res["swapped"] = True
    else:
        res["swapped"] = False
    con.close(); shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=1, default=str))
