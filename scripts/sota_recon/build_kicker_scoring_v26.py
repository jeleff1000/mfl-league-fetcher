"""
sota_recon/build_kicker_scoring_v26.py  --  recompute the KICKER scoring layer on v26 (was STALE).

pts_k_std / pts_k_yds / pts_k_flat + the 13 L1.c kicker component cols were computed by
fantasy_points_calculator during the ORIGINAL super-table build, BEFORE later SOTA-recon waves
backfilled pre-1978 kicker FG atoms (fg_made + distance buckets) from PFR. Those waves never
recomputed the K scoring layer, so 1,532 pre-1994 kicker rows show only the -1 miss penalty
(made FGs scored 0). The calculator FORMULA is correct; the v26 data is just stale -- so this
re-applies the canonical calculator formulas (L638-717) to the now-complete atoms.

  pts_k_std = bucket*tier (3/3/3/4/5/6) when buckets present else fg_made*3, + xp*1 + miss*-1
                (60+ source = fg_made_60_ + fg_made_60_plus, matching calculator)
  pts_k_yds   = fg distance (actual -> bucket-midpoint -> 35yd fallback)*0.1 + xp*1
  pts_k_flat  = fg_made*3 + xp*1
  components  = atomic bucket x multiplier (60+ comps use fg_made_60_plus_canonical)

Scoped to nfl_position='K'; non-K rows unchanged. Streaming SELECT * REPLACE (no materialization).
Gate: golden 56/56; rows unchanged; the 1,532 stale rows now score made FGs (pts_k_std>0).

    python -m scripts.sota_recon.build_kicker_scoring_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave50.kicker_recompute"


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    # col-existence-guarded numeric accessor (calculator safe_col -> 0 for missing cols)
    g = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)" if c in cols else "0"
    fg0, fg2, fg3, fg4, fg5 = (g("fg_made_0_19"), g("fg_made_20_29"), g("fg_made_30_39"),
                               g("fg_made_40_49"), g("fg_made_50_59"))
    fg60 = f"({g('fg_made_60_')} + {g('fg_made_60_plus')})"          # pts_k_std 60+ union
    fg60c = g("fg_made_60_plus_canonical")                            # component 60+ source
    pat, patm, fgm, fgmade = g("pat_made"), g("pat_missed"), g("fg_missed"), g("fg_made")
    buckets = f"({fg0}+{fg2}+{fg3}+{fg4}+{fg5}+{fg60})"
    has_bucket = f"({buckets} > 0)"
    # pts_k_std: true modal league config (public.league_settings modes) --
    # FG 3/3/3/4/5/5 by distance (60+ is 5, the plurality, not 6), XP +1,
    # missed FG -1, missed XP -1.
    yahoo = (f"((CASE WHEN {has_bucket} THEN ({fg0}+{fg2}+{fg3})*3 + {fg4}*4 + {fg5}*5 + {fg60}*5 "
             f"ELSE {fgmade}*3 END) + {pat}*1 + {fgm}*-1 + {patm}*-1)")
    # pts_k_yds: actual distance -> bucket midpoints -> 35yd/FG fallback
    fg_actual = f"(CASE WHEN {g('fg_made_distance')}>0 THEN {g('fg_made_distance')} ELSE {g('fg_yards')} END)"
    yd_buckets = f"({fg0}*17 + {fg2}*25 + {fg3}*35 + {fg4}*45 + {fg5}*54 + {fg60}*62)"
    yd_est = f"(CASE WHEN {has_bucket} THEN {yd_buckets} ELSE {fgmade}*35 END)"
    fg_yards = f"(CASE WHEN {fg_actual}>0 THEN {fg_actual} ELSE {yd_est} END)"
    yds = f"({fg_yards}*0.1 + {pat}*1)"
    flat = f"({fgmade}*3 + {pat}*1)"
    # L1.c components
    COMPONENTS = {
        "pts_k_fgm_0_19_3": f"{fg0}*3", "pts_k_fgm_20_29_3": f"{fg2}*3", "pts_k_fgm_30_39_3": f"{fg3}*3",
        "pts_k_fgm_40_49_4": f"{fg4}*4", "pts_k_fgm_40_49_3": f"{fg4}*3", "pts_k_fgm_50_59_5": f"{fg5}*5",
        "pts_k_fgm_60p_6": f"{fg60c}*6", "pts_k_fgm_60p_5": f"{fg60c}*5",
        "pts_k_xpm_1": f"{pat}*1", "pts_k_xpmiss_n1": f"{patm}*-1", "pts_k_fgmiss_n1": f"{fgm}*-1",
        "pts_k_fgm_yd_p1": f"{g('fg_yards_canonical')}*0.1", "pts_k_fgm_yd_over30_p1": f"{g('fg_yards_over_30_canonical')}*0.1",
    }
    targets = {"pts_k_std": yahoo, "pts_k_yds": yds, "pts_k_flat": flat, **COMPONENTS}
    if not apply:
        con.close(); return {"rows": before, "k_cols": [c for c in targets if c in cols]}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    has_log = "recon_correction_log" in cols
    repl = [f"(CASE WHEN nfl_position='K' THEN ({expr}) ELSE {col} END) AS {col}"
            for col, expr in targets.items() if col in cols]
    if has_log:
        repl.append(f"CASE WHEN nfl_position='K' THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
                    f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log")
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_kick.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    Dk = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"
    # the stale cohort: rows w/ made FGs whose MADE-FG component is still 0 (net pts_k_std can be
    # legitimately <=0 from misses, so subtract the xp credit + add back BOTH miss penalties -- fg_missed
    # AND pat_missed -- to isolate the made-FG contribution, which MUST be > 0 when fg_made > 0). Omitting
    # pat_missed false-tripped legit high-PAT-miss games (e.g. Brett Maher 2022 wk19 record 4 missed XPs).
    stale_fixed = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE nfl_position='K' AND {Dk('fg_made')}>0 "
                              f"AND ({Dk('pts_k_std')}-{Dk('pat_made')}+{Dk('fg_missed')}+{Dk('pat_missed')})<=0").fetchone()[0]
    changed = con.execute(f"""SELECT COUNT(*) FROM '{vq}' o JOIN '{tq}' n USING (player_week)
        WHERE ABS(COALESCE(TRY_CAST(o.pts_k_std AS DOUBLE),0)-COALESCE(TRY_CAST(n.pts_k_std AS DOUBLE),0))>0.01""").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: gg = golden_samples.run()
    finally: S.latest_v26 = o
    gate = (gg["failed"] == 0) and (after == before) and (stale_fixed == 0) and (changed > 0)
    res = {"before": before, "after": after, "stale_still_zero": stale_fixed, "changed_rows": changed,
           "golden": f"{gg['passed']}/{gg['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prekick_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
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
        print(f"rows {r['before']:,}->{r['after']:,} | stale_still_zero={r['stale_still_zero']} changed={r['changed_rows']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
