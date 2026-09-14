"""
sota_recon/build_structural_floors_v26.py  --  clear the last cross-column structural violations.

recon_internal's remaining structural flags after wave51 (pat_att floor):
  (1) fg_buckets_sum > fg_made (86 rows): the 6 distance buckets sum to MORE made FGs than the total.
      - 6 modern rows are MIS-TAGGED returners (Cromartie/Vasher/Hester...) whose missed-FG RETURNS got
        a spurious fg_made_60_=1 while fg_made=0 (they were also drawing 6 phantom kicker points).
      - 80 pre-1978 rows are bucket-reconstruction over-assignment (fg_made is the authoritative total).
      FIX: when the distribution is internally inconsistent (sum>made), NULL all 6 buckets so the kicker
      scoring falls back to the authoritative fg_made total. (pts_k_* is recomputed afterwards.)
  (2) punt_long > punt_yards (1 row, Bill Johnson 1970: long 39 > total 38): floor punt_yards to punt_long.

Streaming SELECT * REPLACE. Gate: golden 56/56; rows unchanged; both violations -> 0.

    python -m scripts.sota_recon.build_structural_floors_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave52.structural_floors"
D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"
BUCKETS = ["fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49", "fg_made_50_59", "fg_made_60_"]
BSUM = "(" + "+".join(D(b) for b in BUCKETS) + ")"
FGVIOL = f"({BSUM} > {D('fg_made')} AND fg_made IS NOT NULL)"
PUNTVIOL = f"(punt_long IS NOT NULL AND punt_yards IS NOT NULL AND {D('punt_long')} > {D('punt_yards')})"


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    fgv = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {FGVIOL}").fetchone()[0]
    pv = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {PUNTVIOL}").fetchone()[0]
    if not apply:
        con.close(); return {"rows": before, "fg_viol": fgv, "punt_viol": pv}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    has_log = "recon_correction_log" in cols
    repl = [f"CASE WHEN {FGVIOL} THEN NULL ELSE {b} END AS {b}" for b in BUCKETS if b in cols]
    if "punt_yards" in cols:
        repl.append(f"CASE WHEN {PUNTVIOL} THEN {D('punt_long')} ELSE punt_yards END AS punt_yards")
    if has_log:
        repl.append(f"CASE WHEN {FGVIOL} OR {PUNTVIOL} THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
                    f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_struct.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    fgv_a = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {FGVIOL}").fetchone()[0]
    pv_a = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {PUNTVIOL}").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (fgv_a == 0) and (pv_a == 0)
    res = {"before": before, "after": after, "fg_viol": f"{fgv}->{fgv_a}", "punt_viol": f"{pv}->{pv_a}",
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prestruct_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
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
        print(f"rows {r['before']:,}->{r['after']:,} | fg_viol {r['fg_viol']} | punt_viol {r['punt_viol']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
