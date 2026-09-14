"""
sota_recon/build_pbp_long_td_fix_v26.py -- resolve cross-entity offense mismatches from raw PBP (1978+).

Joe 2026-07-16: don't flag 1978+ cross-entity gaps as era-limits -- PBP has the play. Fix from PBP:
  * max_passing_long vs max_receiving_long: the longest completion is both. Set passing_long /
    receiving_long = GREATEST(box, PBP per-player-game longest completion). GREATEST never regresses a
    real box long PBP happened to miss; it repairs the side that under-recorded. POINTS-INVARIANT (long
    is not scored).
  * passing_tds vs receiving_tds: every TD pass is caught by someone. Credit the missing receiver/passer
    via GREATEST(box, PBP TD count) per player-game. This IS a scoring input -> re-run build_rescore_fpts
    AFTER this (fpts will move for the corrected rows). Gate here on rows-unchanged only; the rescore gate
    validates fpts.

pre-1978 has no PBP -> left as a documented era-limit (see cat-b-cross-entity-findings-2026-07-16.md).
id crosswalk mirrors the tackle backfill: gsis direct 1999+, pfr:id -> bio.pfr_id (COALESCE fallback to
c.pid) 1978-98.

    python -m scripts.sota_recon.build_pbp_long_td_fix_v26 [--apply]
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

PBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/**/*.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"


def _derive(con) -> None:
    # passer + receiver per (id, season, week): longest COMPLETION + TD-pass count. pfr: prefix stripped.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbp_pass AS
        SELECT REPLACE(CAST(passer_player_id AS VARCHAR),'pfr:','') pid,
               CAST(season AS INT) yr, CAST(week AS INT) wk,
               MAX(CASE WHEN {_c('complete_pass')}=1 THEN {_c('yards_gained')} END) long_,
               SUM(CASE WHEN {_c('pass_touchdown')}=1 THEN 1 ELSE 0 END) td_
        FROM read_parquet('{PBP}', union_by_name=true)
        WHERE season BETWEEN 1978 AND 2025 AND passer_player_id IS NOT NULL AND CAST(passer_player_id AS VARCHAR)<>''
        GROUP BY 1,2,3""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbp_recv AS
        SELECT REPLACE(CAST(receiver_player_id AS VARCHAR),'pfr:','') pid,
               CAST(season AS INT) yr, CAST(week AS INT) wk,
               MAX(CASE WHEN {_c('complete_pass')}=1 THEN {_c('yards_gained')} END) long_,
               SUM(CASE WHEN {_c('pass_touchdown')}=1 AND {_c('complete_pass')}=1 THEN 1 ELSE 0 END) td_
        FROM read_parquet('{PBP}', union_by_name=true)
        WHERE season BETWEEN 1978 AND 2025 AND receiver_player_id IS NOT NULL AND CAST(receiver_player_id AS VARCHAR)<>''
        GROUP BY 1,2,3""")
    # crosswalk each: COALESCE(bio.NFL_player_id, c.pid)
    for t in ("pbp_pass", "pbp_recv"):
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {t}_x AS
            SELECT COALESCE(b.NFL_player_id, c.pid) nfl_id, c.yr, c.wk,
                   COALESCE(c.long_,0) long_, COALESCE(c.td_,0) td_
            FROM {t} c LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}')
                                  WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b
              ON b.pfr_id = c.pid""")


_c = lambda col: f"COALESCE(TRY_CAST({col} AS DOUBLE),0)"
_D = lambda col: f"COALESCE(TRY_CAST({col} AS DOUBLE),0)"


def _mismatch_teamgames(con, src: str) -> int:
    return con.execute(f"""WITH t AS (SELECT year,week,nfl_team,
        MAX({_D('passing_long')}) pl, MAX({_D('receiving_long')}) rl
        FROM read_parquet('{src}') WHERE position<>'DEF' AND nfl_team IS NOT NULL AND year>=1978
          AND ({_D('passing_long')}>0 OR {_D('receiving_long')}>0) GROUP BY 1,2,3)
        SELECT COUNT(*) FROM t WHERE pl<>rl""").fetchone()[0]


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='3GB'")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")
    _derive(con)

    before_mm = _mismatch_teamgames(con, Path(v26).as_posix())
    if not apply:
        pr = con.execute("SELECT COUNT(*) FROM pbp_pass_x").fetchone()[0]
        rc = con.execute("SELECT COUNT(*) FROM pbp_recv_x").fetchone()[0]
        con.close(); shutil.rmtree(sp, ignore_errors=True)
        return {"pbp_passer_pg": pr, "pbp_receiver_pg": rc, "max_long_mismatch_1978plus_before": before_mm}

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    td_before = con.execute(f"SELECT ROUND(SUM({_D('receiving_tds')}),0) FROM st WHERE year>=1978 AND position<>'DEF'").fetchone()[0]
    # LONG: GREATEST(box, pbp) for 1978+
    con.execute(f"""UPDATE st SET passing_long = GREATEST({_D('passing_long')}, p.long_)
        FROM pbp_pass_x p WHERE st.year>=1978 AND st.position<>'DEF'
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(p.nfl_id AS VARCHAR) AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk""")
    con.execute(f"""UPDATE st SET receiving_long = GREATEST({_D('receiving_long')}, r.long_)
        FROM pbp_recv_x r WHERE st.year>=1978 AND st.position<>'DEF'
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(r.nfl_id AS VARCHAR) AND CAST(st.year AS INT)=r.yr AND CAST(st.week AS INT)=r.wk""")
    # TDs: GREATEST(box, pbp) -- credits the missing side (scoring input -> rescore after)
    con.execute(f"""UPDATE st SET passing_tds = GREATEST({_D('passing_tds')}, p.td_)
        FROM pbp_pass_x p WHERE st.year>=1978 AND st.position<>'DEF'
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(p.nfl_id AS VARCHAR) AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk""")
    con.execute(f"""UPDATE st SET receiving_tds = GREATEST({_D('receiving_tds')}, r.td_)
        FROM pbp_recv_x r WHERE st.year>=1978 AND st.position<>'DEF'
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(r.nfl_id AS VARCHAR) AND CAST(st.year AS INT)=r.yr AND CAST(st.week AS INT)=r.wk""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_pbplongtd.parquet")
    rb = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    td_after = con.execute(f"SELECT ROUND(SUM({_D('receiving_tds')}),0) FROM read_parquet('{tq}') WHERE year>=1978 AND position<>'DEF'").fetchone()[0]
    con.close()
    after_mm = _mismatch_teamgames(con=duckdb.connect(), src=tq) if False else None  # recomputed below
    con2 = duckdb.connect(); con2.execute("SET memory_limit='3GB'")
    after_mm = _mismatch_teamgames(con2, tq)
    con2.close()

    gate = (after_rows == before_rows and after_mm <= before_mm)
    res = {"max_long_mismatch_1978plus": f"{before_mm} -> {after_mm}",
           "recv_tds_1978plus": f"{td_before} -> {td_after}", "rows_unchanged": after_rows == before_rows,
           "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_prepbplongtd_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
