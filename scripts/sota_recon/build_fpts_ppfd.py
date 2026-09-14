"""
sota_recon/build_fpts_ppfd.py  --  add fpts_{4pt,5pt,6pt}_ppfd to v26 (PPFD research-preset basis)

PPFD (point-per-first-down) variant, population modal = HALF PPR + 0.5 per rush/rec first down.
Since fpts_*_half already exists and first downs are reconciled (wave35), this is exact:
    fpts_{td}pt_ppfd = fpts_{td}pt_half + 0.5*(rushing_first_downs + receiving_first_downs)
Pre-1978 first downs are empty (no pbp) so ppfd == half there (no FD data; PPFD leagues are modern).

Gated: golden_samples 24/24, rows unchanged, the 3 new cols populated (nonzero sum).

    python -m scripts.sota_recon.build_fpts_ppfd [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
PROV="wave38.fpts_ppfd"; PROV_COL="recon_correction_log"
COLS=["fpts_4pt_ppfd","fpts_5pt_ppfd","fpts_6pt_ppfd"]
FD="0.5*(COALESCE(TRY_CAST(rushing_first_downs AS DOUBLE),0)+COALESCE(TRY_CAST(receiving_first_downs AS DOUBLE),0))"


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("SET memory_limit='5GB'")
        n=con.execute(f"SELECT COUNT(*) FROM read_parquet('{v26}') WHERE position<>'DEF' AND TRY_CAST(fpts_4pt_half AS DOUBLE) IS NOT NULL").fetchone()[0]
        return n
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    existing=[c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in existing: con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    for c in COLS:
        if c not in existing: con.execute(f"ALTER TABLE st ADD COLUMN {c} DOUBLE")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    sets=", ".join(f"{c}=TRY_CAST(fpts_{c.split('_')[1]}_half AS DOUBLE)+{FD}" for c in COLS)
    con.execute(f"""UPDATE st SET {sets},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        WHERE position<>'DEF' AND TRY_CAST(fpts_4pt_half AS DOUBLE) IS NOT NULL""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    tot=con.execute(f"SELECT {', '.join(f'SUM({c})' for c in COLS)} FROM st").fetchone()
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_ppfd.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and all((x or 0)>0 for x in tot)
    res={"before":before,"after":after,"totals":dict(zip(COLS,tot)),"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_preppfd_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | totals {r['totals']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        print(f"would compute fpts_*_ppfd for {run():,} skill rows")
