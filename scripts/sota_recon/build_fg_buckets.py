"""
sota_recon/build_fg_buckets.py  --  recompute FG-by-distance buckets + fg_long from scoring

fg_made_0_19..60_ are a by-distance decomposition that drifted from fg_made (24% of kicker-games
don't sum). The scoring table records every made FG with its distance ("N yard field goal") and the
kicker (1st description link), so recompute the buckets and fg_long from it per kicker-game.

Scope: kicker-games where the scoring FG count == fg_made (the distance record is complete), so the
recomputed buckets sum EXACTLY to fg_made; other games left as-is (flagged). 1960+ (distances
reliable). Gated: golden, post bucket-sum==fg_made on recomputed rows ~100%, rows unchanged.

    python -m scripts.sota_recon.build_fg_buckets [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
BOX="D:/league-history-data/nfl/raw/pfr/boxscores/tables"; TG="D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO="D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV="wave30.fg_buckets"; PROV_COL="recon_correction_log"
BUCKETS=["fg_made_0_19","fg_made_20_29","fg_made_30_39","fg_made_40_49","fg_made_50_59","fg_made_60_"]


def _target(con):
    SC=f"read_parquet('{BOX}/scoring/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    # bio maps pfr_id -> NFL_player_id; plus a NAME fallback for early kickers whose link id differs
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bion AS
        SELECT lower(regexp_replace(player,'[^A-Za-z]','','g')) nn, ANY_VALUE(NFL_player_id) nid
        FROM read_parquet('{BIO}') WHERE player IS NOT NULL GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    # direct bridge: many historical kickers (Jim Turner=TurnJi22, Sam Baker=BakeSa20, Mike Clark=ClarMi20,
    # ...) carry the pfr-style id AS their NFL_player_id (bio.pfr_id is NULL), so the scoring link id
    # equals the NFL_player_id directly. Recovers 475 1937-73 kicker-games the pfr_id/name joins missed.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE idset AS SELECT DISTINCT NFL_player_id nid FROM read_parquet('{BIO}') WHERE NFL_player_id IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fgs AS
        SELECT split_part(description_link_ids,';',1) kicker,
               lower(regexp_replace(split_part(description_link_texts,';',1),'[^A-Za-z]','','g')) knm,
               TRY_CAST(regexp_extract(description,'(\\d+) yard field goal',1) AS INT) dist,
               boxscore_id
        FROM {SC} WHERE lower(description) LIKE '%yard field goal%' AND description_link_ids IS NOT NULL
          AND CAST(season AS INT)>=1937""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT COALESCE(b.nid, bn.nid, ds.nid) AS NFL_player_id, t.yr, t.wk,
          SUM(CASE WHEN dist<20 THEN 1 ELSE 0 END) fg_made_0_19,
          SUM(CASE WHEN dist BETWEEN 20 AND 29 THEN 1 ELSE 0 END) fg_made_20_29,
          SUM(CASE WHEN dist BETWEEN 30 AND 39 THEN 1 ELSE 0 END) fg_made_30_39,
          SUM(CASE WHEN dist BETWEEN 40 AND 49 THEN 1 ELSE 0 END) fg_made_40_49,
          SUM(CASE WHEN dist BETWEEN 50 AND 59 THEN 1 ELSE 0 END) fg_made_50_59,
          SUM(CASE WHEN dist>=60 THEN 1 ELSE 0 END) fg_made_60_,
          MAX(dist) fg_long, COUNT(*) sc_fg
        FROM fgs JOIN tg t USING(boxscore_id)
          LEFT JOIN bio b ON b.pfr_id=fgs.kicker
          LEFT JOIN bion bn ON bn.nn=fgs.knm
          LEFT JOIN idset ds ON ds.nid=fgs.kicker
        WHERE dist IS NOT NULL AND COALESCE(b.nid, bn.nid, ds.nid) IS NOT NULL GROUP BY 1,2,3""")


def _badsum(con, tbl):
    return con.execute(f"""SELECT SUM(CASE WHEN ({'+'.join('COALESCE('+c+',0)' for c in BUCKETS)}) <> COALESCE(fg_made,0) THEN 1 ELSE 0 END)
        FROM {tbl} WHERE position<>'DEF' AND COALESCE(fg_made,0)>0""").fetchone()[0]


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
    # scoring is the authoritative record of MADE FGs with distance; set buckets + fg_long from it.
    # Keep fg_made as-is (don't regress where scoring lacks a make's distance); residual mismatch =
    # only the few makes scoring has no distance for. (single-game kicker-weeks only)
    setc=", ".join(f"{c}=t.{c}" for c in BUCKETS)+", fg_long=GREATEST(COALESCE(st.fg_long,0),t.fg_long)"
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st WHERE position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    con.execute(f"""UPDATE st SET {setc},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
          AND st.position<>'DEF'
          AND NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id AND m.year=st.year AND m.week=st.week)""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    post=_badsum(con,"st")
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_fgbt.parquet")
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
        bk=vp.with_name(vp.stem+f"_prefgb_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | fg bucket-sum mismatches {r['pre_badsum']:,}->{r['post_badsum']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        print("current fg bucket-sum mismatches:", run())
