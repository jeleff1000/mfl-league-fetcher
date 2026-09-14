"""
sota_recon/build_nfl_position_complete_v26.py  --  complete nfl_position on the v26 super table.

nfl_position is the canonical fantasy-eligible position (QB/RB/WR/TE/OL/DL/LB/DB/K/P, + DEF for team rows).
It was NULL for 20,815 rows: single-wing/old codes (TB/BB/WB/FB/HB/B/LH/RH, ends E/LE/RE), granular line/
secondary codes (C/G/T/LT.., DE/LDE.., CB/S..), and a subset of standard codes left unmapped. This maps
every one from the natural `position` code, KEEPING `position` as the natural/official value untouched.

Mapping (only applied where nfl_position IS NULL; existing values kept):
  QB  <- QB, TB, BB, QB,K, TB/RE            (single-wing tailback/blocking-back were the passers)
  RB  <- RB, FB, HB, LH, RH, B
  WR  <- WR, FL, WB, SE
  TE  <- TE
  OL  <- OL, C, G, T, OT, OG, LG, RG, LT, RT
  DL  <- DL, DE, DT, NT, LDE, RDE, LDT, RDT, MG, DG
  LB  <- LB, MLB, LLB, RLB, OLB, ILB
  DB  <- DB, CB, S, SS, FS, LCB, RCB, LDH, DH, RH/DB
  K   <- K ;  P <- P
  E/LE/RE (two-way ends): WR if the row has receiving production, else DL   (offensive ends -> WR only)
  blank/junk position ('', '/', 'NA', NULL): left NULL (no derivable position)

Gate: golden_samples 56/56, rows unchanged, nfl_position NULL drops to the junk-only residual, `position`
column byte-identical. Backup + os.replace. Provenance -> recon_correction_log.

    python -m scripts.sota_recon.build_nfl_position_complete_v26 [--apply]
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

PROV = "wave54.nfl_position_complete"

# canonical fantasy position from the natural position code (applied only where nfl_position IS NULL)
POS_CASE = """
CASE
  WHEN nfl_position IS NOT NULL THEN nfl_position
  WHEN position IN ('QB','TB','BB','QB,K','TB/RE') THEN 'QB'
  WHEN position IN ('RB','FB','HB','LH','RH','B') THEN 'RB'
  WHEN position IN ('WR','FL','WB','SE') THEN 'WR'
  WHEN position = 'TE' THEN 'TE'
  WHEN position IN ('OL','C','G','T','OT','OG','LG','RG','LT','RT') THEN 'OL'
  WHEN position IN ('DL','DE','DT','NT','LDE','RDE','LDT','RDT','MG','DG') THEN 'DL'
  WHEN position IN ('LB','MLB','LLB','RLB','OLB','ILB') THEN 'LB'
  WHEN position IN ('DB','CB','S','SS','FS','LCB','RCB','LDH','DH','RH/DB') THEN 'DB'
  WHEN position = 'K' THEN 'K'
  WHEN position = 'P' THEN 'P'
  WHEN position IN ('E','LE','RE') THEN
    CASE WHEN COALESCE(receiving_yards,0)+COALESCE(receiving_tds,0) > 0 THEN 'WR' ELSE 'DL' END
  ELSE NULL
END
"""


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before_null = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE nfl_position IS NULL").fetchone()[0]
    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    if not apply:
        # preview: what the residual NULL (unmappable junk) would be
        resid = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE ({POS_CASE}) IS NULL").fetchone()[0]
        con.close()
        return {"rows": before_rows, "nfl_position_null_before": before_null, "would_map": before_null - resid,
                "residual_null_after": resid}

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    wkcols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = [f"({POS_CASE}) AS nfl_position"]
    if "recon_correction_log" in wkcols:
        repl.append(f"CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' THEN '{PROV}' "
                    f"ELSE recon_correction_log||',{PROV}' END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_nflpos.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_null = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE nfl_position IS NULL").fetchone()[0]
    # position (natural) must be byte-identical
    pos_changed = con.execute(f"""SELECT COUNT(*) FROM '{vq}' a JOIN '{tq}' b ON a.player_week=b.player_week
        WHERE a.position IS DISTINCT FROM b.position""").fetchone()[0]
    # every emitted nfl_position is canonical
    bad = con.execute(f"""SELECT COUNT(*) FROM '{tq}' WHERE nfl_position IS NOT NULL
        AND nfl_position NOT IN ('QB','RB','WR','TE','OL','DL','LB','DB','K','P','DEF')""").fetchone()[0]
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after_rows == before_rows and pos_changed == 0 and bad == 0
            and after_null < before_null)
    res = {"rows_before": before_rows, "rows_after": after_rows, "nfl_position_null_before": before_null,
           "nfl_position_null_after": after_null, "position_changed": pos_changed, "noncanonical": bad,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prenflpos_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
        res["backup"] = bk.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    r = run(apply=a.apply)
    print(r if not a.apply else
          f"nfl_position NULL {r['nfl_position_null_before']:,} -> {r['nfl_position_null_after']:,} | "
          f"position_changed={r['position_changed']} noncanonical={r['noncanonical']} golden {r['golden']} | "
          f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} {'SWAPPED' if r.get('swapped') else 'NOT swapped'}")
