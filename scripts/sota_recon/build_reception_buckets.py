"""
sota_recon/build_reception_buckets.py  --  recompute reception-by-yards buckets from pbp

receptions_0_4..40plus drifted from receptions (the by-yards decomposition). pbp records every
completion's yardage ("QB pass complete to RECEIVER for N yards") from 1966+, so recompute the
buckets per receiver-game from pbp. Excludes accepted-penalty-voided plays (lower(detail) LIKE
'%no play%'). 1994+ pbp is ~complete (buckets sum to receptions); 1966-93 ~68% (partial -> residual
= catches pbp lacks); pre-1966 no pbp (irreducible). fg-build-style: keep receptions, set buckets.

Gated: golden 24/24, reception bucket-sum mismatches drop sharply, rows unchanged.

    python -m scripts.sota_recon.build_reception_buckets [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
BOX="D:/league-history-data/nfl/raw/pfr/boxscores/tables"; TG="D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO="D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV="wave31.reception_buckets"; PROV_COL="recon_correction_log"
BUCKETS=["receptions_0_4","receptions_5_9","receptions_10_19","receptions_20_29","receptions_30_39","receptions_40plus"]


def _target(con):
    PBP=f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bion AS
        SELECT lower(regexp_replace(player,'[^A-Za-z]','','g')) nn, ANY_VALUE(NFL_player_id) nid
        FROM read_parquet('{BIO}') WHERE player IS NOT NULL GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cmp AS
        SELECT split_part(detail_link_ids,';',2) rec,
               lower(regexp_replace(split_part(detail_link_texts,';',2),'[^A-Za-z]','','g')) rnm,
               CASE WHEN detail LIKE '%for no gain%' THEN 0
                    ELSE TRY_CAST(regexp_extract(detail,'for (-?\\d+) yard',1) AS INT) END yds,
               boxscore_id
        FROM {PBP} WHERE detail LIKE '%pass complete%' AND lower(detail) NOT LIKE '%no play%'
          AND detail_link_ids IS NOT NULL
          AND (detail LIKE '%for no gain%' OR regexp_extract(detail,'for (-?\\d+) yard',1)<>'')""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT COALESCE(b.nid, bn.nid) AS NFL_player_id, t.yr, t.wk,
          SUM(CASE WHEN yds<5 THEN 1 ELSE 0 END) receptions_0_4,
          SUM(CASE WHEN yds BETWEEN 5 AND 9 THEN 1 ELSE 0 END) receptions_5_9,
          SUM(CASE WHEN yds BETWEEN 10 AND 19 THEN 1 ELSE 0 END) receptions_10_19,
          SUM(CASE WHEN yds BETWEEN 20 AND 29 THEN 1 ELSE 0 END) receptions_20_29,
          SUM(CASE WHEN yds BETWEEN 30 AND 39 THEN 1 ELSE 0 END) receptions_30_39,
          SUM(CASE WHEN yds>=40 THEN 1 ELSE 0 END) receptions_40plus, COUNT(*) pbp_rec
        FROM cmp JOIN tg t USING(boxscore_id)
          LEFT JOIN bio b ON b.pfr_id=cmp.rec LEFT JOIN bion bn ON bn.nn=cmp.rnm
        WHERE yds IS NOT NULL AND COALESCE(b.nid,bn.nid) IS NOT NULL GROUP BY 1,2,3""")


def _badsum(con, tbl):
    return con.execute(f"""SELECT SUM(CASE WHEN ({'+'.join('COALESCE('+c+',0)' for c in BUCKETS)}) <> COALESCE(receptions,0) THEN 1 ELSE 0 END)
        FROM {tbl} WHERE position<>'DEF' AND COALESCE(receptions,0)>0""").fetchone()[0]


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("SET memory_limit='5GB'"); return _badsum(con, f"read_parquet('{v26}')")
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]; pre=_badsum(con,"st")
    _target(con)
    setc=", ".join(f"{c}=t.{c}" for c in BUCKETS)
    con.execute("CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st WHERE position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1")
    # only set where pbp completion count == receptions (full attribution) so buckets sum exactly
    con.execute(f"""UPDATE st SET {setc},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
          AND st.position<>'DEF' AND t.pbp_rec=COALESCE(st.receptions,0)
          AND NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id AND m.year=st.year AND m.week=st.week)""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    post=_badsum(con,"st")
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_recbt.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and (post<pre)
    res={"before":before,"after":after,"pre_badsum":pre,"post_badsum":post,"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_prerecb_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | reception bucket-sum mismatches {r['pre_badsum']:,}->{r['post_badsum']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        print("current reception bucket-sum mismatches:", run())
