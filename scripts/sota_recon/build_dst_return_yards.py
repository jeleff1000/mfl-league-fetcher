"""
sota_recon/build_dst_return_yards.py  --  populate the empty DST dst_return_yards from return yards

dst_return_yards (the unit's return yardage, for DST scoring) was entirely empty. It is the team's
own kickoff + punt return yards, so set the DST row = SUM over the team's players of
(kickoff_return_yards + punt_return_yards) per (year, week, franchise, opponent). Deterministic.

Gated: golden 24/24, DST dst_return_yards now == team return-yard sum (~100%), rows unchanged.

    python -m scripts.sota_recon.build_dst_return_yards [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
PROV = "wave29.dst_return_yards"; PROV_COL = "recon_correction_log"


def run(apply=False):
    v26 = latest_v26()
    if not apply:
        con = duckdb.connect(); con.execute("SET memory_limit='4GB'"); V = f"read_parquet('{v26}')"
        return con.execute(f"SELECT SUM(COALESCE(dst_return_yards,0)) FROM {V} WHERE position='DEF'").fetchone()[0]
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute("""CREATE TEMP TABLE ry AS SELECT nfl_franchise_number fr, year yr, week wk,
        opponent_nfl_franchise_number opp,
        SUM(COALESCE(kickoff_return_yards,0)+COALESCE(punt_return_yards,0)) v
        FROM st WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")
    con.execute(f"""UPDATE st SET dst_return_yards=CAST(r.v AS INT),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM ry r WHERE st.position='DEF' AND st.nfl_franchise_number=r.fr AND st.year=r.yr
          AND st.week=r.wk AND st.opponent_nfl_franchise_number IS NOT DISTINCT FROM r.opp
          AND COALESCE(st.dst_return_yards,0) IS DISTINCT FROM CAST(r.v AS INT)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    tot = con.execute("SELECT SUM(COALESCE(dst_return_yards,0)) FROM st WHERE position='DEF'").fetchone()[0]
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_dryt.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (tot > 0)
    res = {"before": before, "after": after, "dst_ret_yds_total": tot, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predry_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | dst_return_yards total now {r['dst_ret_yds_total']:,.0f} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        print("current dst_return_yards total:", run())
