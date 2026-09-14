"""
sota_recon/build_penalties_accepted.py  --  penalties = ACCEPTED only (from pbp), not all-flagged

v26 penalties counted ALL flagged penalties per player (+39% vs the team_stats ACCEPTED team total).
The standard stat is ACCEPTED penalties. pbp marks each "(accepted)"/"(no play)" (enforced) vs
"(declined)"/"(offset)" (not counted) and names the penalized player. Recompute v26 penalties +
penalty_yards from pbp accepted-only per player-game (1966+; pre-1966 has no pbp, left as-is).
Players whose penalties were all declined are zeroed.

Gated: golden 24/24, team penalty sum vs team_stats accepted total rises from ~41% toward ~100%
(1966+), rows unchanged.

    python -m scripts.sota_recon.build_penalties_accepted [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
BOX="D:/league-history-data/nfl/raw/pfr/boxscores/tables"; TG="D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO="D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV="wave32.penalties_accepted"; PROV_COL="recon_correction_log"; LO=1966


def _target(con):
    PBP=f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bion AS SELECT lower(regexp_replace(player,'[^A-Za-z]','','g')) nn, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE player IS NOT NULL GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pen AS
        SELECT split_part(detail_link_ids,';',1) pid,
               lower(regexp_replace(split_part(detail_link_texts,';',1),'[^A-Za-z]','','g')) pnm,
               TRY_CAST(regexp_extract(detail,', (\\d+) yard',1) AS INT) yds, boxscore_id
        FROM {PBP} WHERE detail LIKE 'Penalty on %' AND detail_link_ids IS NOT NULL
          AND (lower(detail) LIKE '%(accepted)%' OR lower(detail) LIKE '%(no play)%')""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT COALESCE(b.nid, bn.nid) AS NFL_player_id, t.yr, t.wk,
               COUNT(*) acc, SUM(COALESCE(yds,0)) acc_yds
        FROM pen JOIN tg t USING(boxscore_id)
          LEFT JOIN bio b ON b.pfr_id=pen.pid LEFT JOIN bion bn ON bn.nn=pen.pnm
        WHERE COALESCE(b.nid,bn.nid) IS NOT NULL GROUP BY 1,2,3""")


def _agree(con, tbl):
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tgp AS
        WITH j AS (SELECT g.yr,g.wk,g.fr, CASE WHEN g.is_home THEN ts.home_stat ELSE ts.vis_stat END val
                   FROM (SELECT boxscore_id,CAST(year AS INT) yr,CAST(week AS INT) wk,team_fid fr,is_home FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL) g
                   JOIN read_parquet('{BOX}/team_stats/_combined.parquet') ts ON ts.boxscore_id=g.boxscore_id AND ts.stat='Penalties-Yards')
        SELECT yr,wk,fr, TRY_CAST(string_split(val,'-')[1] AS INT) pen FROM j WHERE len(string_split(val,'-'))=2""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE vpn AS SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,nfl_franchise_number fr,
        SUM(COALESCE(penalties,0)) p FROM {tbl} GROUP BY 1,2,3""")
    return con.execute(f"""SELECT ROUND(100.0*SUM(CASE WHEN abs(COALESCE(vpn.p,0)-tgp.pen)<0.5 THEN 1 ELSE 0 END)/COUNT(*),1)
        FROM tgp LEFT JOIN vpn USING(yr,wk,fr) WHERE tgp.yr>={LO} AND tgp.pen IS NOT NULL""").fetchone()[0]


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("SET memory_limit='5GB'"); return _agree(con, f"read_parquet('{v26}')")
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]; pre=_agree(con,"st")
    _target(con)
    con.execute("CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st WHERE position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1")
    nm="NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id AND m.year=st.year AND m.week=st.week)"
    con.execute(f"""UPDATE st SET penalties=t.acc, penalty_yards=t.acc_yds,
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
          AND st.position<>'DEF' AND {nm}""")
    con.execute(f"""UPDATE st SET penalties=0, penalty_yards=0,
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        WHERE CAST(st.year AS INT)>={LO} AND st.position<>'DEF' AND COALESCE(st.penalties,0)>0 AND {nm}
          AND NOT EXISTS (SELECT 1 FROM tgt t WHERE t.NFL_player_id=st.NFL_player_id AND t.yr=CAST(st.year AS INT) AND t.wk=CAST(st.week AS INT))""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    post=_agree(con,"st")
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_pent.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and (post>=pre+20)
    res={"before":before,"after":after,"pre_agree":pre,"post_agree":post,"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_prepen_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | penalties vs team_stats accepted (1966+) {r['pre_agree']}->{r['post_agree']}% | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        print("current penalties vs team_stats accepted (1966+):", run(), "%")
