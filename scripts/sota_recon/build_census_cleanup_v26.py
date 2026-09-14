"""
sota_recon/build_census_cleanup_v26.py  --  fix the per-column census defects on v26.

From recon_column_census:
  (A) ceiling-pair impossibilities (the parent was undercounted; the specific event is reliable):
      fumbles      = max(fumbles, fumbles_lost)                 (29 rows; ST fumbles missed the total)
      completions  = max(completions, passing_tds)              (5 rows; pre-1950 TD passes, no cmp count)
      attempts     = max(attempts, completions_target)          (keep attempts >= completions)
      receiving_tds= max(receiving_tds, receiving_tds_40plus)   (3 rows; a 40+ TD missed the total)
      -> only the violating rows change (CASE-guarded); NULLs untouched.
  (B) numeric stats stored as VARCHAR -> coerce to DOUBLE (TRY_CAST; non-numeric -> NULL):
      three_out, fourth_down_stop, timeouts (used numerically) + raw passthrough pick6.
      (The legacy-vocab passthroughs 2pm/fg%/xp%/fgm/pts/sfty were dropped in wave65
      build_legacy_vocab_drop_v26 -- five contaminated Tommy Davis rows, carried canonically --
      so they are no longer coerced here; guarded below in case an older release is targeted.)
  (is_starter is genuine BOOLEAN and starter_position is categorical -> NOT touched; census refined.)

Streaming SELECT * REPLACE (no materialization). Gate: golden 56/56; rows unchanged; post-fix all
ceiling pairs == 0; coerced columns are DOUBLE-typed.
NOTE: receiving_tds feeds fpts on 3 rows -> their weekly fpts is understated by 6 until the next full
fpts pass (below materiality; season==Sumweekly still holds after re-derive).

    python -m scripts.sota_recon.build_census_cleanup_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave46.census_cleanup"
# The loop below guards each name with `if c in cols`, so dropped legacy-vocab columns
# (2pm/fg%/xp%/fgm/pts/sfty, removed in wave65) are simply skipped on a current release.
COERCE = ["three_out", "fourth_down_stop", "timeouts", "pick6"]
DBL = lambda c: f'TRY_CAST("{c}" AS DOUBLE)'


def _viol(con, src):
    D = lambda c: f"TRY_CAST({c} AS DOUBLE)"
    q = lambda a, b, extra="": con.execute(
        f"SELECT COUNT(*) FROM '{src}' WHERE {D(a)} > COALESCE({D(b)},0){extra}").fetchone()[0]
    return {"fum": q("fumbles_lost", "fumbles"), "ptd": q("passing_tds", "completions"),
            "rtd40": q("receiving_tds_40plus", "receiving_tds"), "cmp_att": q("completions", "attempts")}


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    if not apply:
        b = _viol(con, vq); con.close(); return {"rows": before, "violations": b}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    has_log = "recon_correction_log" in cols
    D = lambda c: f"TRY_CAST({c} AS DOUBLE)"
    cmp_target = f"GREATEST(COALESCE({D('completions')},0), COALESCE({D('passing_tds')},0))"
    repl = [
        f"CASE WHEN {D('fumbles_lost')} > COALESCE({D('fumbles')},0) THEN {D('fumbles_lost')} ELSE fumbles END AS fumbles",
        f"CASE WHEN {D('passing_tds')} > COALESCE({D('completions')},0) THEN {D('passing_tds')} ELSE completions END AS completions",
        f"CASE WHEN COALESCE({D('attempts')},0) < {cmp_target} AND {cmp_target} > 0 THEN {cmp_target} ELSE attempts END AS attempts",
        f"CASE WHEN {D('receiving_tds_40plus')} > COALESCE({D('receiving_tds')},0) THEN {D('receiving_tds_40plus')} ELSE receiving_tds END AS receiving_tds",
    ]
    for c in COERCE:
        if c in cols:
            repl.append(f'{DBL(c)} AS "{c}"')
    if has_log:
        viol_any = (f"({D('fumbles_lost')}>COALESCE({D('fumbles')},0) OR {D('passing_tds')}>COALESCE({D('completions')},0) "
                    f"OR {D('receiving_tds_40plus')}>COALESCE({D('receiving_tds')},0))")
        repl.append(f"CASE WHEN {viol_any} THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
                    f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_census.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    vafter = _viol(con, tq)
    dtypes = {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM '{tq}'").fetchall()}
    newtypes = {c: dtypes.get(c) for c in ["three_out", "fourth_down_stop", "timeouts"] if c in cols}
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    coerced_ok = all("DOUBLE" in t or "FLOAT" in t for t in newtypes.values())
    gate = (g["failed"] == 0 and after == before and sum(vafter.values()) == 0 and coerced_ok)
    res = {"before": before, "after": after, "viol_after": vafter, "coerced_types": newtypes,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_precensus_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
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
        print(f"rows {r['before']:,}->{r['after']:,} | violations after={r['viol_after']} | coerced={r['coerced_types']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
