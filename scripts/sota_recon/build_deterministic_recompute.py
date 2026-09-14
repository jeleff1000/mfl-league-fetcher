"""
sota_recon/build_deterministic_recompute.py  --  recompute deterministic decompositions from atoms

These columns are pure functions of the (now-reconciled) atoms -- threshold flags, arithmetic
remainders, and total-yards buckets. They were found STALE and even WRONG (e.g. bonus_pass_300yd=0
on a 554-yd game, =1 on a 285-yd game), so recomputing them from the corrected atoms is a real fix,
not just a refresh.

Player-row flags/arithmetic (position<>'DEF'):
  bonus_*  = achievement flags (yards/att/cmp/rec thresholds)
  fg_missed = fg_att-fg_made ; pat_missed = pat_att-pat_made
DEF-row total-yards buckets: yds_allow_<range> = (total_yds_allowed in range)

Gated: golden 24/24, each column == its definition post-recompute, row count unchanged.

    python -m scripts.sota_recon.build_deterministic_recompute            # dry-run (current mismatch)
    python -m scripts.sota_recon.build_deterministic_recompute --apply
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave28.deterministic_recompute"; PROV_COL = "recon_correction_log"

# player-row (non-DEF) deterministic formulas
PLAYER = {
    "bonus_pass_300yd": "CASE WHEN COALESCE(passing_yards,0)>=300 THEN 1 ELSE 0 END",
    "bonus_pass_400yd": "CASE WHEN COALESCE(passing_yards,0)>=400 THEN 1 ELSE 0 END",
    "bonus_pass_25cmp": "CASE WHEN COALESCE(completions,0)>=25 THEN 1 ELSE 0 END",
    "bonus_rush_100yd": "CASE WHEN COALESCE(rushing_yards,0)>=100 THEN 1 ELSE 0 END",
    "bonus_rush_200yd": "CASE WHEN COALESCE(rushing_yards,0)>=200 THEN 1 ELSE 0 END",
    "bonus_rush_20att": "CASE WHEN COALESCE(carries,0)>=20 THEN 1 ELSE 0 END",
    "bonus_rec_100yd": "CASE WHEN COALESCE(receiving_yards,0)>=100 THEN 1 ELSE 0 END",
    "bonus_rec_200yd": "CASE WHEN COALESCE(receiving_yards,0)>=200 THEN 1 ELSE 0 END",
    "bonus_rec_10rec": "CASE WHEN COALESCE(receptions,0)>=10 THEN 1 ELSE 0 END",
    "bonus_rush_rec_100yd": "CASE WHEN COALESCE(rushing_yards,0)+COALESCE(receiving_yards,0)>=100 THEN 1 ELSE 0 END",
    "bonus_rush_rec_200yd": "CASE WHEN COALESCE(rushing_yards,0)+COALESCE(receiving_yards,0)>=200 THEN 1 ELSE 0 END",
    "fg_missed": "GREATEST(COALESCE(fg_att,0)-COALESCE(fg_made,0),0)",
    "pat_missed": "GREATEST(COALESCE(pat_att,0)-COALESCE(pat_made,0),0)",
}
# DEF-row buckets of total_yds_allowed
DEFBUCKET = {
    "yds_allow_0_99": "total_yds_allowed < 100",
    "yds_allow_100_199": "total_yds_allowed BETWEEN 100 AND 199",
    "yds_allow_200_299": "total_yds_allowed BETWEEN 200 AND 299",
    "yds_allow_300_349": "total_yds_allowed BETWEEN 300 AND 349",
    "yds_allow_350_399": "total_yds_allowed BETWEEN 350 AND 399",
    "yds_allow_400_449": "total_yds_allowed BETWEEN 400 AND 449",
    "yds_allow_450_499": "total_yds_allowed BETWEEN 450 AND 499",
    "yds_allow_500_549": "total_yds_allowed BETWEEN 500 AND 549",
    "yds_allow_550_plus": "total_yds_allowed >= 550",
}


def _mismatch(con, tbl, have):
    out = {}
    for c, f in PLAYER.items():
        if c not in have:
            continue
        out[c] = con.execute(f"SELECT SUM(CASE WHEN COALESCE({c},0) IS DISTINCT FROM ({f}) THEN 1 ELSE 0 END) FROM {tbl} WHERE position<>'DEF'").fetchone()[0]
    for c, cond in DEFBUCKET.items():
        if c not in have:
            continue
        out[c] = con.execute(f"SELECT SUM(CASE WHEN COALESCE({c},0) IS DISTINCT FROM (CASE WHEN {cond} THEN 1 ELSE 0 END) THEN 1 ELSE 0 END) FROM {tbl} WHERE position='DEF' AND total_yds_allowed IS NOT NULL").fetchone()[0]
    return out


def run(apply=False):
    v26 = latest_v26(); have = set(pq.read_schema(v26).names)
    if not apply:
        con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        return _mismatch(con, f"read_parquet('{v26}')", have)
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
    prov = f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END"
    for c, f in PLAYER.items():
        if c in have:
            con.execute(f"UPDATE st SET {c}=({f}), {prov} WHERE position<>'DEF' AND COALESCE({c},0) IS DISTINCT FROM ({f})")
    for c, cond in DEFBUCKET.items():
        if c in have:
            con.execute(f"UPDATE st SET {c}=(CASE WHEN {cond} THEN 1 ELSE 0 END), {prov} WHERE position='DEF' AND total_yds_allowed IS NOT NULL AND COALESCE({c},0) IS DISTINCT FROM (CASE WHEN {cond} THEN 1 ELSE 0 END)")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_dettmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); post = _mismatch(con, "st", have); con.close(); shutil.rmtree(sp, ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and all(v == 0 for v in post.values())
    res = {"before": before, "after": after, "post": post,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predet_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        _pre = run(apply=False)
        r = run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | post-mismatch all 0: {all(v==0 for v in r['post'].values())}")
        fixed = sum(_pre.values())
        print(f"  cells corrected: {fixed:,} across {len(_pre)} columns")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED ({r['backup']})" if r["swapped"] else f"NOT swapped; {r['temp']}"))
    else:
        m = run(apply=False)
        print("current cells mismatching their definition (to be recomputed):")
        for c, n in sorted(m.items(), key=lambda x: -x[1]):
            print(f"  {c:24s} {n:,}")
