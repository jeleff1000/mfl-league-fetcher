"""
sota_recon/build_targets_backfill_v26.py  --  fix targets >= receptions for 1978+ from PBP.

Targets are tracked from 1978 on (PBP era). recon_internal flagged ~89 player-weeks where
receptions > targets or receptions>0 with targets=0. Root cause for 1978+: a PBP-target backfill
gap (box-score receptions present, targets missing/short). PRE-1978 targets are genuinely
unavailable -> those rows keep targets NULL (a catch with unknown targets, not a violation).

Fix (1978+ ONLY, where receptions > COALESCE(targets,0)):
  targets = GREATEST(pbp_targets, receptions, existing_targets)   # PBP-authoritative; never decreases
  catch_rate recomputed for those rows. Everything else bit-identical (SELECT * REPLACE).
PBP rollup (PBP_ROLLUP, 1978-2025) supplies the authoritative target counts; receptions is the
floor when PBP is missing/short (a catch implies >=1 target).

Gate: golden 24/24; rows unchanged; post-fix (year>=1978 AND receptions>targets)==0; pre-1978
null-target count UNCHANGED; targets never decreased; AND changed rows <= 200 (so it can never
again sweep the ~52k legitimately-null pre-1978 rows).

    python -m scripts.sota_recon.build_targets_backfill_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26, PBP_ROLLUP
from .recon_common import utc_stamp

PROV = "wave44.targets_pbp_backfill"
VIOL = "t.year>=1978 AND COALESCE(t.receptions,0) > COALESCE(t.targets,0)"
NEWT = "GREATEST(COALESCE(p.pt,0), CAST(t.receptions AS DOUBLE), COALESCE(t.targets,0))"


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix(); pbp = Path(PBP_ROLLUP.path).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    viol = con.execute(f"SELECT COUNT(*) FROM '{vq}' t WHERE {VIOL}").fetchone()[0]
    pre1978_null = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE receptions>0 AND targets IS NULL AND year<1978").fetchone()[0]
    if not apply:
        con.close()
        return {"rows": before, "viol_1978plus": viol, "pre1978_null_targets": pre1978_null}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    has_cr = "catch_rate" in cols; has_log = "recon_correction_log" in cols
    con.execute(f"CREATE OR REPLACE TEMP TABLE _pbp AS SELECT player_week, MAX(CAST(targets AS DOUBLE)) pt FROM '{pbp}' GROUP BY player_week")

    repl = [f"CASE WHEN {VIOL} THEN {NEWT} ELSE t.targets END AS targets"]
    if has_cr:
        repl.append(f"CASE WHEN {VIOL} AND {NEWT}>0 THEN CAST(t.receptions AS DOUBLE)/({NEWT}) ELSE t.catch_rate END AS catch_rate")
    if has_log:
        repl.append(f"CASE WHEN {VIOL} THEN (CASE WHEN t.recon_correction_log IS NULL OR t.recon_correction_log='' "
                    f"THEN '{PROV}' ELSE t.recon_correction_log||',{PROV}' END) ELSE t.recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT t.* REPLACE ({', '.join(repl)}) FROM '{vq}' t LEFT JOIN _pbp p ON t.player_week=p.player_week"

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_tgtbf.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    viol_after = con.execute(f"SELECT COUNT(*) FROM '{tq}' t WHERE {VIOL}").fetchone()[0]
    pre1978_null_after = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE receptions>0 AND targets IS NULL AND year<1978").fetchone()[0]
    # dup-safe (player_week non-unique for doubleheaders): compare total targets, not per-row join
    tot_before = con.execute(f"SELECT SUM(CAST(targets AS DOUBLE)) FROM '{vq}' WHERE year>=1978").fetchone()[0] or 0
    tot_after = con.execute(f"SELECT SUM(CAST(targets AS DOUBLE)) FROM '{tq}' WHERE year>=1978").fetchone()[0] or 0
    changed = viol  # exactly the violation rows are rewritten; bounded + confirmed by viol_after==0
    monotonic = tot_after >= tot_before
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o

    gate = (g["failed"] == 0 and after == before and viol_after == 0
            and pre1978_null_after == pre1978_null and monotonic and 0 < changed <= 200)
    res = {"before": before, "after": after, "viol_before": viol, "viol_after": viol_after,
           "changed_rows": changed, "targets_sum_monotonic": monotonic,
           "targets_sum_1978plus": f"{tot_before:.0f}->{tot_after:.0f}",
           "pre1978_null_preserved": pre1978_null_after == pre1978_null,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_pretgtbf_{stamp}.parquet"); shutil.copy2(vp, bk)
        os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
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
        print(f"rows {r['before']:,}->{r['after']:,} | viol(1978+) {r['viol_before']}->{r['viol_after']} "
              f"| changed={r['changed_rows']} targets_sum {r['targets_sum_1978plus']} (monotonic={r['targets_sum_monotonic']}) "
              f"pre1978_null_preserved={r['pre1978_null_preserved']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
