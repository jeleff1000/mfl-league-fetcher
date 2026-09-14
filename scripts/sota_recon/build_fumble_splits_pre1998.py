"""
sota_recon/build_fumble_splits_pre1998.py  --  backfill fumble-type splits 1978-1997 from pbp

rushing/receiving/sack_fumbles (+ _lost) are EMPTY before 1998 in v26. pbp names the fumbler
("X fumbles") and the recoverer ("recovered by Y") on every fumble play 1978+. The play kind
(sack / reception / rush) sets which column; lost = recoverer's team != fumbler's team (per-boxscore
roster). Special-teams fumbles (punt/kick plays) and FG/XP are excluded.

Validated vs v26's populated 1998+ values: totals 90-104% (most ~100%), within-1 98.7-99.9%, exact
69-87%. Fumbler name -> NFL_player_id via per-boxscore name->pfr_id->bio (+ direct-id + unique-name).

Scope: 1978-1997 ONLY. Single-row player-weeks only. Gated: golden_samples + golden_matrix pass,
rows unchanged, columns populated (nonzero) 1978-97, pre-existing nonzero < 100.

    python -m scripts.sota_recon.build_fumble_splits_pre1998 [--apply]
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
PROV="wave36.fumble_splits_pre1998"; PROV_COL="recon_correction_log"
COLS=["rushing_fumbles","rushing_fumbles_lost","receiving_fumbles","receiving_fumbles_lost","sack_fumbles","sack_fumbles_lost"]
LO,HI=1978,1997
NN="lower(regexp_replace({},'[^A-Za-z]','','g'))"
NAME=r"([A-Z][A-Za-z.''\-]+(?: [A-Z][A-Za-z.''\-]+){0,2})"


def _targets(con):
    P=f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"CREATE OR REPLACE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL")
    con.execute(f"CREATE OR REPLACE TEMP TABLE bio AS SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1")
    con.execute(f"CREATE OR REPLACE TEMP TABLE idset AS SELECT DISTINCT NFL_player_id nid FROM read_parquet('{BIO}') WHERE NFL_player_id IS NOT NULL")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bion AS SELECT {NN.format('player')} nn, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE player IS NOT NULL GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rnm AS SELECT boxscore_id, nn, ANY_VALUE(team) team FROM (
       SELECT boxscore_id, {NN.format('player')} nn, team FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player IS NOT NULL
       UNION ALL SELECT boxscore_id, {NN.format('player')} nn, team FROM read_parquet('{BOX}/player_defense/_combined.parquet') WHERE player IS NOT NULL) GROUP BY 1,2""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rpfr AS SELECT boxscore_id, nn, ANY_VALUE(pid) pid FROM (
       SELECT boxscore_id, {NN.format('player')} nn, split_part(player_link_ids,';',1) pid FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player IS NOT NULL AND player_link_ids IS NOT NULL
       UNION ALL SELECT boxscore_id, {NN.format('player')} nn, split_part(player_link_ids,';',1) pid FROM read_parquet('{BOX}/player_defense/_combined.parquet') WHERE player IS NOT NULL AND player_link_ids IS NOT NULL)
       GROUP BY 1,2 HAVING COUNT(DISTINCT pid)=1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fm AS SELECT t.yr, t.wk, p.boxscore_id, lower(p.detail) ld,
       {NN.format("regexp_extract(p.detail,'"+NAME+" fumbles',1)")} fnn,
       {NN.format("regexp_extract(p.detail,'recovered by "+NAME+"',1)")} rnn
       FROM {P} p JOIN tg t USING(boxscore_id)
       WHERE CAST(p.season AS INT) BETWEEN {LO} AND {HI} AND lower(p.detail) LIKE '%fumble%' AND lower(p.detail) LIKE '%recovered by%'""")
    con.execute("""CREATE OR REPLACE TEMP TABLE fj AS SELECT fm.*, rf.team ftm, rr.team rtm,
       CASE WHEN ld LIKE '%sacked%' THEN 'sack' WHEN ld LIKE '%pass complete%' THEN 'rec'
            WHEN ld LIKE '%punt%' OR ld LIKE '%kick%' THEN 'st' ELSE 'rush' END kind,
       CASE WHEN ld LIKE '%field goal%' OR ld LIKE '%extra point%' THEN 1 ELSE 0 END skp
       FROM fm LEFT JOIN rnm rf ON rf.boxscore_id=fm.boxscore_id AND rf.nn=fm.fnn
               LEFT JOIN rnm rr ON rr.boxscore_id=fm.boxscore_id AND rr.nn=fm.rnn""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS SELECT COALESCE(b.nid, ds.nid, bn.nid) nid, yr, wk,
       SUM(CASE WHEN kind='rush' THEN 1 ELSE 0 END) rushing_fumbles,
       SUM(CASE WHEN kind='rush' AND ftm IS NOT NULL AND rtm IS NOT NULL AND ftm<>rtm THEN 1 ELSE 0 END) rushing_fumbles_lost,
       SUM(CASE WHEN kind='rec' THEN 1 ELSE 0 END) receiving_fumbles,
       SUM(CASE WHEN kind='rec' AND ftm IS NOT NULL AND rtm IS NOT NULL AND ftm<>rtm THEN 1 ELSE 0 END) receiving_fumbles_lost,
       SUM(CASE WHEN kind='sack' THEN 1 ELSE 0 END) sack_fumbles,
       SUM(CASE WHEN kind='sack' AND ftm IS NOT NULL AND rtm IS NOT NULL AND ftm<>rtm THEN 1 ELSE 0 END) sack_fumbles_lost
       FROM fj LEFT JOIN rpfr rp ON rp.boxscore_id=fj.boxscore_id AND rp.nn=fj.fnn
               LEFT JOIN bio b ON b.pfr_id=rp.pid LEFT JOIN idset ds ON ds.nid=rp.pid LEFT JOIN bion bn ON bn.nn=fj.fnn
       WHERE fj.skp=0 AND fj.kind<>'st' AND COALESCE(b.nid,ds.nid,bn.nid) IS NOT NULL GROUP BY 1,2,3""")


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
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_fum78.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and (pre_nonzero<100) and all((x or 0)>0 for x in tot)
    res={"before":before,"after":after,"pre_nonzero":pre_nonzero,"totals":tot,"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_prefum78_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | pre-existing nonzero {r['pre_nonzero']} | totals {dict(zip(COLS,r['totals']))} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        t=run(); print("would populate "+", ".join(f"{c}={t[i]:,}" for i,c in enumerate(COLS))+f" across {t[len(COLS)]:,} player-weeks")
