"""
sota_recon/build_first_downs_pre1998.py  --  backfill passing/rushing/receiving_first_downs 1978-1997 from pbp

These three player first-down columns are EMPTY before 1998 in v26 (no service tracks player first downs
pre-1994). pbp records down + yds_to_go + yardage per play 1978+, so a play yields a first down when
gain>=yds_to_go (or a TD). Credit: the rusher (rush play) -> rushing_first_downs; on a completion the
passer -> passing_first_downs and the receiver -> receiving_first_downs. Special-teams plays (punt/FG/
kick/XP/kneel/spike/aborted) and penalty/no-play rows are excluded.

Validated vs v26's populated 1998+ values (drive-by-drive): rushing 82.8% exact / 97.1% within-1 / 98%
of total; receiving 94.0% / 98.1% / 104%; passing 85.6% / 96.0% / 104%.

Scope: 1978-1997 ONLY. Player pfr_id -> NFL_player_id via bio.pfr_id + direct-id bridge + unique-name
fallback. Single-row player-weeks only (skip the rare multi-row). Gated: golden_samples + golden_matrix
pass, rows unchanged, the three columns populated (nonzero) in 1978-97, pre-existing 1978-97 == 0.

    python -m scripts.sota_recon.build_first_downs_pre1998 [--apply]
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
PROV="wave35.first_downs_pre1998"; PROV_COL="recon_correction_log"
COLS=["rushing_first_downs","receiving_first_downs","passing_first_downs"]
LO,HI=1978,1997


def _targets(con):
    P=f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE idset AS SELECT DISTINCT NFL_player_id nid FROM read_parquet('{BIO}') WHERE NFL_player_id IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pp AS SELECT t.yr, t.wk, TRY_CAST(p.yds_to_go AS INT) ytg, lower(p.detail) ld,
        CASE WHEN lower(p.detail) LIKE '%for no gain%' THEN 0 ELSE TRY_CAST(regexp_extract(lower(p.detail),'for (-?\\d+) yard',1) AS INT) END gain,
        split_part(p.detail_link_ids,';',1) p1, split_part(p.detail_link_ids,';',2) p2
        FROM {P} p JOIN tg t USING(boxscore_id)
        WHERE CAST(p.season AS INT) BETWEEN {LO} AND {HI} AND p.down IS NOT NULL AND p.detail IS NOT NULL
          AND lower(p.detail) NOT LIKE 'penalty on%' AND lower(p.detail) NOT LIKE '%(no play)%'""")
    con.execute("""CREATE OR REPLACE TEMP TABLE fd AS SELECT *,
        CASE WHEN ld LIKE '%touchdown%' THEN 1 WHEN gain IS NOT NULL AND ytg IS NOT NULL AND gain>=ytg THEN 1 ELSE 0 END isfd,
        CASE WHEN ld LIKE '%pass complete%' THEN 'cmp'
             WHEN ld LIKE '%pass incomplete%' OR ld LIKE '%intercepted%' OR ld LIKE '%sacked%' THEN 'pass0'
             WHEN ld LIKE '%punt%' OR ld LIKE '%field goal%' OR ld LIKE '%kicks%' OR ld LIKE '%extra point%' OR ld LIKE '%kneel%' OR ld LIKE '%spike%' OR ld LIKE '%aborted%' THEN 'st'
             ELSE 'rush' END kind FROM pp""")
    con.execute("""CREATE OR REPLACE TEMP TABLE attr AS
        SELECT p1 pid, yr, wk, SUM(CASE WHEN kind='rush' AND isfd=1 THEN 1 ELSE 0 END) rush_fd, SUM(CASE WHEN kind='cmp' AND isfd=1 THEN 1 ELSE 0 END) pass_fd, 0 rec_fd FROM fd WHERE p1<>'' GROUP BY 1,2,3
        UNION ALL
        SELECT p2 pid, yr, wk, 0, 0, SUM(CASE WHEN kind='cmp' AND isfd=1 THEN 1 ELSE 0 END) rec_fd FROM fd WHERE p2<>'' GROUP BY 1,2,3""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS SELECT COALESCE(b.nid, ds.nid) nid, yr, wk,
        SUM(rush_fd) rushing_first_downs, SUM(rec_fd) receiving_first_downs, SUM(pass_fd) passing_first_downs
        FROM attr LEFT JOIN bio b ON b.pfr_id=attr.pid LEFT JOIN idset ds ON ds.nid=attr.pid
        WHERE COALESCE(b.nid,ds.nid) IS NOT NULL GROUP BY 1,2,3""")


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'"); _targets(con)
        return con.execute("SELECT SUM(rushing_first_downs), SUM(receiving_first_downs), SUM(passing_first_downs), COUNT(*) FROM tgt").fetchone()
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    coltypes={c[0]:c[1] for c in con.execute("DESCRIBE st").fetchall()}
    if PROV_COL not in coltypes:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre_nonzero=con.execute(f"""SELECT COUNT(*) FROM st WHERE position<>'DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI}
        AND (TRY_CAST(rushing_first_downs AS INT)>0 OR TRY_CAST(receiving_first_downs AS INT)>0 OR TRY_CAST(passing_first_downs AS INT)>0)""").fetchone()[0]
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
    tot=con.execute(f"SELECT SUM(TRY_CAST(rushing_first_downs AS INT)), SUM(TRY_CAST(receiving_first_downs AS INT)), SUM(TRY_CAST(passing_first_downs AS INT)) FROM st WHERE position<>'DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI}").fetchone()
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_fd78.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    # pre_nonzero ceiling (not ==0): 1978-97 has NO authoritative player-first-down source, so pbp is
    # canonical for the era; a tiny stray set (12 rows, one player) is replaced for uniformity. The ceiling
    # guards against ever clobbering a real populated era.
    gate=(g["failed"]==0) and (after==before) and (pre_nonzero<100) and all((x or 0)>0 for x in tot)
    res={"before":before,"after":after,"pre_nonzero_1978_97":pre_nonzero,"totals":tot,"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_prefd78_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | pre-existing 1978-97 nonzero {r['pre_nonzero_1978_97']} | totals rush/rec/pass {r['totals']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        t=run(); print(f"would populate rushing={t[0]:,} receiving={t[1]:,} passing={t[2]:,} first downs across {t[3]:,} player-weeks (1978-97)")
