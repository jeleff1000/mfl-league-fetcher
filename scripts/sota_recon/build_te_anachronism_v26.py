"""
sota_recon/build_te_anachronism_v26.py  --  fix anachronistic TE labels on the v26 super table.

The tight end position did not exist until ~1961 (Ditka/Mackey era). Pre-1961 receivers were "ends" -- the
ancestor of the modern WR (Don Hutson, 1935-45, is the prototype). The source data labels these pre-modern
ends as TE, so they wrongly appear as tight ends in rankings/eligibility. This corrects the canonical
nfl_position: nfl_position='TE' AND year<1961  ->  'WR'. Natural `position` (as-recorded) is left untouched.

Gate: golden_samples 56/56, rows unchanged, pre-1961 nfl_position=TE -> 0, Don Hutson -> WR, position column
byte-identical. Backup + os.replace. Provenance -> recon_correction_log.

    python -m scripts.sota_recon.build_te_anachronism_v26 [--apply]
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

PROV = "wave56.te_anachronism_pre1961_wr"
CUTOFF = 1961


def run(apply: bool = False) -> dict:
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE nfl_position='TE' AND year<{CUTOFF}").fetchone()[0]
    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    if not apply:
        con.close()
        return {"pre1961_TE_rows": before, "rows": before_rows}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    wkcols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    case = f"CASE WHEN nfl_position='TE' AND year<{CUTOFF} THEN 'WR' ELSE nfl_position END"
    repl = [f"({case}) AS nfl_position"]
    if "recon_correction_log" in wkcols:
        repl.append(f"CASE WHEN nfl_position='TE' AND year<{CUTOFF} THEN "
                    f"(CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' THEN '{PROV}' "
                    f"ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_teanach.parquet")
    rb = con.execute(out_sql).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_te = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE nfl_position='TE' AND year<{CUTOFF}").fetchone()[0]
    pos_changed = con.execute(f"""SELECT COUNT(*) FROM '{vq}' a JOIN '{tq}' b ON a.player_week=b.player_week
        WHERE a.position IS DISTINCT FROM b.position""").fetchone()[0]
    hutson = con.execute(f"SELECT DISTINCT nfl_position FROM '{tq}' WHERE NFL_player_id='HutsDo00'").fetchall()
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after_rows == before_rows and after_te == 0 and pos_changed == 0
            and hutson == [("WR",)])
    res = {"pre1961_TE_before": before, "pre1961_TE_after": after_te, "rows": after_rows,
           "position_changed": pos_changed, "hutson_nfl_position": hutson, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preteanach_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
        res["backup"] = bk.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
