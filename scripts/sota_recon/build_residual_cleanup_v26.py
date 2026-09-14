"""
sota_recon/build_residual_cleanup_v26.py  --  close the last two stat-table residuals on v26.

#1  targets < receptions (14 rows, 1933-1961, targets NOT NULL): a catch implies >= 1 target. Floor
    targets = receptions (minimum consistent) + catch_rate = 1.0 for those rows. (Universal invariant
    targets >= receptions now holds at ALL years; pre-1978 PBP-less rows keep targets NULL untouched.)
#2  5 player-weeks where wave46 clamped receiving_tds 0->1 (or 1->2) but weekly fpts was NOT recomputed
    -> fpts understated by 6 (a receiving TD is +6 in EVERY scoring variant). Add 6 to every fpts_* for
    those rows and recompute pts_rec_td_6 = receiving_tds*6 so the component is consistent.

Streaming SELECT * REPLACE (no materialization). Gate: golden 56/56; rows unchanged; post-fix
targets<receptions == 0; the 5 fpts rows increased by 6.

    python -m scripts.sota_recon.build_residual_cleanup_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave47.residual_cleanup"
# the 3 player-weeks whose receiving_tds was clamped up in wave46 (verified vs precensus backup on the
# UNIQUE key player_week+opponent+game_date -- all 3 are modern with a unique player_week, so no
# doubleheader fanout; Moss 2003, Stallworth 2003, Harris 1981).
RECTD_FIX = ("00-0011754_2003_7", "00-0021147_2003_16", "HAR374361_1981_18")
D = lambda c: f"TRY_CAST({c} AS DOUBLE)"


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    viol = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE receptions>targets AND targets IS NOT NULL").fetchone()[0]
    if not apply:
        con.close(); return {"rows": before, "targets_viol": viol, "rectd_rows": len(RECTD_FIX)}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    desc = con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()
    cols = [c[0] for c in desc]
    colset = set(cols); has_log = "recon_correction_log" in colset
    _num = ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGEINT")
    # numeric fpts point columns only (exclude the fpts_recomputed_at_* timestamp sentinel)
    fpts_cols = [r[0] for r in desc if r[0].startswith("fpts_") and any(x in str(r[1]).upper() for x in _num)]
    inlist = ",".join(f"'{p}'" for p in RECTD_FIX)
    tviol = f"(targets IS NOT NULL AND {D('receptions')} > {D('targets')})"
    rfix = f"player_week IN ({inlist})"

    repl = []
    # #1 targets floor + catch_rate
    repl.append(f"CASE WHEN {tviol} THEN {D('receptions')} ELSE targets END AS targets")
    if "catch_rate" in colset:
        repl.append(f"CASE WHEN {tviol} THEN 1.0 ELSE catch_rate END AS catch_rate")
    # #2 fpts += 6 for the clamped rec-TD rows (rec TD = +6 in every variant)
    for fc in fpts_cols:
        repl.append(f"CASE WHEN {rfix} THEN COALESCE({D(fc)},0)+6 ELSE {fc} END AS \"{fc}\"")
    # keep pts_rec_td_6 consistent = receiving_tds*6 for those rows
    if "pts_rec_td_6" in colset:
        repl.append(f"CASE WHEN {rfix} THEN {D('receiving_tds')}*6 ELSE pts_rec_td_6 END AS pts_rec_td_6")
    if has_log:
        repl.append(f"CASE WHEN {tviol} OR {rfix} THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
                    f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_resid.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    vafter = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE receptions>targets AND targets IS NOT NULL").fetchone()[0]
    # confirm the 5 rows' fpts rose by 6 vs original
    rose = con.execute(f"""SELECT COUNT(*) FROM '{vq}' o JOIN '{tq}' n USING(player_week)
        WHERE n.player_week IN ({inlist}) AND ABS((COALESCE({D('n.fpts_4pt_half')},0))-(COALESCE({D('o.fpts_4pt_half')},0)-0)-6) < 0.001""").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before and vafter == 0 and rose == len(RECTD_FIX))
    res = {"before": before, "after": after, "targets_viol_after": vafter, "fpts_rows_+6": rose,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preresid_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
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
        print(f"rows {r['before']:,}->{r['after']:,} | targets_viol_after={r['targets_viol_after']} | fpts_rows_+6={r['fpts_rows_+6']}/5 | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
