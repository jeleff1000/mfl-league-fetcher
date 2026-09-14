"""
sota_recon/build_misc_pbp_pre1998.py  --  backfill misc pbp atoms 1978-1997 (final pbp wave)

EMPTY-pre-1998 columns derivable from pbp:
  - receiving_target_interceptions : pass 'intended for X is intercepted' -> X      (1978-97)
  - fumble_recovery_own            : 'recovered by Y' where Y team == fumbler team
                                     (or self-recovery) -> Y                        (1978-97)
  - passing/rushing/receiving_2pt_conversions : 'Two Point Attempt: ... conversion
                                     succeeds' -> passer/receiver (pass) or rusher  (1994-97; 0 before)

Validated vs v26's populated 1998+: target_int 96% exact / 99.7% w1; passing_2pt 96/99.9; rushing_2pt
85/99.7; receiving_2pt 71/99.9; fumble_recovery_own 73/99.2. The two under-counters reflect pre-1998
roster limits (O-linemen who recover own fumbles aren't in the skill-position roster); all are far
beyond any existing service. Player id via per-boxscore name->pfr_id->bio (+ direct-id).

Scope 1978-1997. Single-row player-weeks only. Gated: golden_samples + golden_matrix, rows unchanged,
columns populated, pre-existing nonzero < 100.

    python -m scripts.sota_recon.build_misc_pbp_pre1998 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
BOX="D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG="D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO="D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV="wave37.misc_pbp_pre1998"; PROV_COL="recon_correction_log"
# 2pt splits DROPPED: the "Two Point Attempt:" pbp format that exists 1998+ is absent in 1994-97 pbp
# (the only pre-1998 years 2pt existed), so they'd write false 0s. Left NULL (honest, not derivable).
COLS=["receiving_target_interceptions","fumble_recovery_own"]
LO,HI=1978,1997
NN="lower(regexp_replace({},'[^A-Za-z]','','g'))"
NAME=r"([A-Z][A-Za-z.''\-]+(?: [A-Z][A-Za-z.''\-]+){0,2})"


def _targets(con):
    P=f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"CREATE OR REPLACE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL")
    con.execute(f"CREATE OR REPLACE TEMP TABLE bio AS SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1")
    con.execute(f"CREATE OR REPLACE TEMP TABLE idset AS SELECT DISTINCT NFL_player_id nid FROM read_parquet('{BIO}') WHERE NFL_player_id IS NOT NULL")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rnm AS SELECT boxscore_id, nn, ANY_VALUE(team) team FROM (
       SELECT boxscore_id, {NN.format('player')} nn, team FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player IS NOT NULL
       UNION ALL SELECT boxscore_id, {NN.format('player')} nn, team FROM read_parquet('{BOX}/player_defense/_combined.parquet') WHERE player IS NOT NULL) GROUP BY 1,2""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rpfr AS SELECT boxscore_id, nn, ANY_VALUE(pid) pid FROM (
       SELECT boxscore_id, {NN.format('player')} nn, split_part(player_link_ids,';',1) pid FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player IS NOT NULL AND player_link_ids IS NOT NULL
       UNION ALL SELECT boxscore_id, {NN.format('player')} nn, split_part(player_link_ids,';',1) pid FROM read_parquet('{BOX}/player_defense/_combined.parquet') WHERE player IS NOT NULL AND player_link_ids IS NOT NULL)
       GROUP BY 1,2 HAVING COUNT(DISTINCT pid)=1""")
    NID="COALESCE(b.nid, ds.nid)"
    MAP="LEFT JOIN rpfr rp ON rp.boxscore_id=s.boxscore_id AND rp.nn=s.who LEFT JOIN bio b ON b.pfr_id=rp.pid LEFT JOIN idset ds ON ds.nid=rp.pid"
    # target interceptions
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ti AS SELECT {NID} nid, s.yr, s.wk, COUNT(*) v FROM (
       SELECT t.yr,t.wk,p.boxscore_id, {NN.format("regexp_extract(p.detail,'intended for "+NAME+" is intercepted',1)")} who
       FROM {P} p JOIN tg t USING(boxscore_id) WHERE CAST(p.season AS INT) BETWEEN {LO} AND {HI} AND lower(p.detail) LIKE '%intended for%intercepted%') s
       {MAP} WHERE {NID} IS NOT NULL GROUP BY 1,2,3""")
    # fumble recovery own
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fr AS SELECT {NID} nid, s.yr, s.wk, COUNT(*) v FROM (
       SELECT t.yr,t.wk,p.boxscore_id, {NN.format("regexp_extract(p.detail,'"+NAME+" fumbles',1)")} fnn, {NN.format("regexp_extract(p.detail,'recovered by "+NAME+"',1)")} who
       FROM {P} p JOIN tg t USING(boxscore_id) WHERE CAST(p.season AS INT) BETWEEN {LO} AND {HI} AND lower(p.detail) LIKE '%fumble%' AND lower(p.detail) LIKE '%recovered by%') s
       LEFT JOIN rnm rf ON rf.boxscore_id=s.boxscore_id AND rf.nn=s.fnn
       LEFT JOIN rnm rr ON rr.boxscore_id=s.boxscore_id AND rr.nn=s.who
       {MAP} WHERE {NID} IS NOT NULL AND (s.fnn=s.who OR (rf.team IS NOT NULL AND rr.team IS NOT NULL AND rf.team=rr.team)) GROUP BY 1,2,3""")
    # 2pt: passer/receiver (pass) / rusher (rush) on 'conversion succeeds'
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tp AS SELECT t.yr,t.wk,p.boxscore_id, lower(p.detail) ld,
       {NN.format("regexp_extract(p.detail,'Two Point Attempt: "+NAME+"',1)")} a1, {NN.format("regexp_extract(p.detail,'complete to "+NAME+"',1)")} rcv
       FROM {P} p JOIN tg t USING(boxscore_id) WHERE CAST(p.season AS INT) BETWEEN {LO} AND {HI} AND p.detail LIKE 'Two Point Attempt:%' AND lower(p.detail) LIKE '%conversion succeeds%'""")
    for col,who,cond in [("p2","a1","ld LIKE '%pass%'"),("r2","a1","ld NOT LIKE '%pass%'"),("c2","rcv","ld LIKE '%pass%'")]:
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {col} AS SELECT {NID} nid, s.yr, s.wk, COUNT(*) v FROM (
           SELECT yr,wk,boxscore_id, {who} who FROM tp WHERE {cond}) s {MAP} WHERE {NID} IS NOT NULL GROUP BY 1,2,3""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
       SELECT nid, yr, wk,
         MAX(receiving_target_interceptions) receiving_target_interceptions, MAX(fumble_recovery_own) fumble_recovery_own,
         MAX(passing_2pt_conversions) passing_2pt_conversions, MAX(rushing_2pt_conversions) rushing_2pt_conversions, MAX(receiving_2pt_conversions) receiving_2pt_conversions
       FROM (
         SELECT nid,yr,wk, v receiving_target_interceptions, 0 fumble_recovery_own, 0 passing_2pt_conversions, 0 rushing_2pt_conversions, 0 receiving_2pt_conversions FROM ti
         UNION ALL SELECT nid,yr,wk, 0,v,0,0,0 FROM fr
         UNION ALL SELECT nid,yr,wk, 0,0,v,0,0 FROM p2
         UNION ALL SELECT nid,yr,wk, 0,0,0,v,0 FROM r2
         UNION ALL SELECT nid,yr,wk, 0,0,0,0,v FROM c2) GROUP BY 1,2,3""")


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'"); _targets(con)
        return con.execute(f"SELECT {', '.join('SUM('+c+')' for c in COLS)}, COUNT(*) FROM tgt").fetchone()
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    coltypes={c[0]:c[1] for c in con.execute("DESCRIBE st").fetchall()}
    if PROV_COL not in coltypes: con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre_nonzero=con.execute(f"""SELECT COUNT(*) FROM st WHERE position<>'DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI}
        AND ({ ' OR '.join(f'TRY_CAST({c} AS INT)>0' for c in COLS) })""").fetchone()[0]
    _targets(con)
    con.execute("CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st WHERE position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1")
    setc=", ".join(f"{c}=CAST(t.{c} AS {coltypes[c]})" for c in COLS)
    con.execute(f"""UPDATE st SET {setc},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.NFL_player_id=t.nid AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
          AND st.position<>'DEF' AND CAST(st.year AS INT) BETWEEN {LO} AND {HI}
          AND NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id AND m.year=st.year AND m.week=st.week)""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    tot=con.execute(f"SELECT {', '.join(f'SUM(TRY_CAST({c} AS INT))' for c in COLS)} FROM st WHERE position<>'DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI}").fetchone()
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_misc78.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and (pre_nonzero<100) and any((x or 0)>0 for x in tot)
    res={"before":before,"after":after,"pre_nonzero":pre_nonzero,"totals":dict(zip(COLS,tot)),"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_premisc78_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | pre-existing nonzero {r['pre_nonzero']} | totals {r['totals']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        t=run(); print("would populate "+", ".join(f"{c}={t[i]:,}" for i,c in enumerate(COLS))+f" across {t[len(COLS)]:,} player-weeks")
