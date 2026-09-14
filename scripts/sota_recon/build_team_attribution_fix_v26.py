"""
sota_recon/build_team_attribution_fix_v26.py  --  fix swapped nfl_team/opponent on misattributed rows.

recon_internal's comp_rec cross-flag surfaced 5 Mark Brunell games (2001 wk2/6/12/16, 2002 wk1) where
his row's nfl_team and opponent_nfl_team are SWAPPED: he played for JAX every week 2001-02 but these 5
rows tag him to the opponent (TEN/BUF/GNB/KAN/IND) with opponent=JAX. Symptom: those 5 JAX team-games
showed team completions=0 but receptions>5 (their passer was hiding in the opponent's bucket). Every
OTHER multi-team QB-season that year is a legitimate midseason trade (Mayfield, Flacco, Dobbs, ...),
so this is a surgical, enumerated fix -- NOT a heuristic that could catch real trades.

FIX: swap nfl_team<->opponent_nfl_team and nfl_franchise_number<->opponent_nfl_franchise_number on the
5 enumerated player_weeks. Streaming SELECT * REPLACE. Gate: golden 56/56; rows unchanged; post-fix the
5 rows have nfl_team='JAX'; the JAX comp_rec anomaly (team comp=0 & rec>5) is cleared for those games.

    python -m scripts.sota_recon.build_team_attribution_fix_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave53.team_attribution"
BAD = ("00-0002110_2001_2", "00-0002110_2001_6", "00-0002110_2001_12", "00-0002110_2001_16", "00-0002110_2002_1")


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    inlist = ",".join(f"'{p}'" for p in BAD)
    target = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE player_week IN ({inlist}) AND nfl_team<>'JAX'").fetchone()[0]
    if not apply:
        con.close(); return {"rows": before, "target_rows": target}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    has_log = "recon_correction_log" in cols
    bad = f"player_week IN ({inlist})"
    repl = [
        f"CASE WHEN {bad} THEN opponent_nfl_team ELSE nfl_team END AS nfl_team",
        f"CASE WHEN {bad} THEN nfl_team ELSE opponent_nfl_team END AS opponent_nfl_team",
    ]
    if "nfl_franchise_number" in cols and "opponent_nfl_franchise_number" in cols:
        repl += [
            f"CASE WHEN {bad} THEN opponent_nfl_franchise_number ELSE nfl_franchise_number END AS nfl_franchise_number",
            f"CASE WHEN {bad} THEN nfl_franchise_number ELSE opponent_nfl_franchise_number END AS opponent_nfl_franchise_number",
        ]
    if has_log:
        repl.append(f"CASE WHEN {bad} THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
                    f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_teamfix.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    now_jax = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE player_week IN ({inlist}) AND nfl_team='JAX'").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (now_jax == len(BAD))
    res = {"before": before, "after": after, "fixed_to_jax": now_jax, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preteamfix_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
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
        print(f"rows {r['before']:,}->{r['after']:,} | fixed_to_jax={r['fixed_to_jax']}/5 | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
