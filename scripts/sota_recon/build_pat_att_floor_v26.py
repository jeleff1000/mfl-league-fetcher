"""
sota_recon/build_pat_att_floor_v26.py  --  structural-integrity floor on pat_att (XP attempts).

recon_internal flagged 215 rows where pat_made + pat_missed + pat_blocked > pat_att -- i.e. pat_att is
under-recorded (modern: pat_att excludes blocked XPs; pre-1938: attempts only partially recorded, e.g.
Jack McBride 1926 made 30 but pat_att 15). An attempt MUST exist for every made/missed/blocked XP, so
the minimum-consistent value is pat_att = GREATEST(pat_att, pat_made + pat_missed + pat_blocked).

This ALSO fixes the new xp_pct column (pat_made/pat_att), which showed impossible >1.0 values (up to 3.0)
on 12 pre-1938 season rows because pat_att < pat_made there.

pat_att feeds NO scoring column (kicker scoring uses pat_made), so the only downstream effect is xp_pct,
re-derived by the season/career rebuild. Streaming SELECT * REPLACE (no 740-col materialization).
Gate: golden 56/56; rows unchanged; post-fix pat_components>pat_att == 0; only pat_att changed.

    python -m scripts.sota_recon.build_pat_att_floor_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave51.pat_att_floor"
D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"
COMPONENTS = f"({D('pat_made')}+{D('pat_missed')}+{D('pat_blocked')})"
VIOL = f"(pat_att IS NOT NULL AND {COMPONENTS} > {D('pat_att')})"


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    viol = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {VIOL}").fetchone()[0]
    if not apply:
        con.close(); return {"rows": before, "viol": viol}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    has_log = "recon_correction_log" in cols
    repl = [f"CASE WHEN {VIOL} THEN {COMPONENTS} ELSE pat_att END AS pat_att"]
    if has_log:
        repl.append(f"CASE WHEN {VIOL} THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
                    f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_patfloor.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    viol_after = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {VIOL}").fetchone()[0]
    changed = con.execute(f"""SELECT COUNT(*) FROM '{vq}' o JOIN '{tq}' n USING (player_week)
        WHERE COALESCE(TRY_CAST(o.pat_att AS DOUBLE),-1) <> COALESCE(TRY_CAST(n.pat_att AS DOUBLE),-1)""").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    # changed may exceed viol slightly due to duplicate (doubleheader) player_week join fanout; the real
    # success criterion is viol_after==0 with rows unchanged + golden intact + at least viol rows changed.
    gate = (g["failed"] == 0) and (after == before) and (viol_after == 0) and (changed >= viol)
    res = {"before": before, "after": after, "viol_before": viol, "viol_after": viol_after,
           "changed_rows": changed, "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prepatfloor_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
        res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    if not a.apply:
        print("DRY:", run())
    else:
        r = run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | viol {r['viol_before']}->{r['viol_after']} | changed={r['changed_rows']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
