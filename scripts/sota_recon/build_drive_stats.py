"""
sota_recon/build_drive_stats.py  --  populate empty three_out / fourth_down_stop from the drives table

Both were empty. The drives table (1998+) gives them directly per team-game:
  three_out        = the team's own drives with <=3 plays ending in a Punt (three-and-out)
  fourth_down_stop = the OPPONENT's drives ending in "Downs" (turnover on downs = this defense stopped)
home_drives = home team, vis_drives = visitor (mapped to franchise via team_games is_home). Set on the
team's DST row. Pre-1998 has no drives table (left empty).

Gated: golden 24/24, both columns populated 1998+ (nonzero), rows unchanged.

    python -m scripts.sota_recon.build_drive_stats [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
BOX="D:/league-history-data/nfl/raw/pfr/boxscores/tables"; TG="D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
PROV="wave33.drive_stats"; PROV_COL="recon_correction_log"


def _drv(con):
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk,
        team_fid fr, opponent_fid ofr, is_home FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    # per-drive rows attributed to the franchise that had the ball (home_drives=home side)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE dr AS
        SELECT g.yr, g.wk, g.fr, g.ofr, TRY_CAST(d.play_count_tip AS INT) pc, d.end_event ev
        FROM read_parquet('{BOX}/home_drives/_combined.parquet') d
          JOIN tg g ON d.boxscore_id=g.boxscore_id AND g.is_home
        UNION ALL
        SELECT g.yr, g.wk, g.fr, g.ofr, TRY_CAST(d.play_count_tip AS INT) pc, d.end_event ev
        FROM read_parquet('{BOX}/vis_drives/_combined.parquet') d
          JOIN tg g ON d.boxscore_id=g.boxscore_id AND NOT g.is_home""")
    # three_out: own drives <=3 plays ending Punt ; fourth_down_stop: opponent's "Downs" drives
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        WITH own AS (SELECT yr,wk,fr, SUM(CASE WHEN pc<=3 AND ev='Punt' THEN 1 ELSE 0 END) three_out FROM dr GROUP BY 1,2,3),
             dn AS (SELECT yr,wk,ofr fr, SUM(CASE WHEN ev='Downs' THEN 1 ELSE 0 END) fds FROM dr GROUP BY 1,2,3)
        SELECT COALESCE(own.yr,dn.yr) yr, COALESCE(own.wk,dn.wk) wk, COALESCE(own.fr,dn.fr) fr,
               COALESCE(own.three_out,0) three_out, COALESCE(dn.fds,0) fourth_down_stop
        FROM own FULL JOIN dn USING(yr,wk,fr)""")


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("SET memory_limit='5GB'"); _drv(con)
        return con.execute("SELECT SUM(three_out), SUM(fourth_down_stop) FROM tgt").fetchone()
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    _drv(con)
    # columns are string-typed; write integer-as-string on the team DST row
    con.execute(f"""UPDATE st SET three_out=CAST(t.three_out AS VARCHAR), fourth_down_stop=CAST(t.fourth_down_stop AS VARCHAR),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.position='DEF' AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
          AND st.nfl_franchise_number=t.fr""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    tot=con.execute("SELECT SUM(TRY_CAST(three_out AS INT)), SUM(TRY_CAST(fourth_down_stop AS INT)) FROM st WHERE position='DEF'").fetchone()
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_drvt.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and (tot[0] or 0)>0 and (tot[1] or 0)>0
    res={"before":before,"after":after,"three_out_total":tot[0],"fds_total":tot[1],"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_predrv_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | three_out total {r['three_out_total']:,} | fourth_down_stop total {r['fds_total']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        t=run(); print(f"would populate three_out={t[0]:,} fourth_down_stop={t[1]:,}")
